"""gate/tier2: the S3 helper and the tier-two assertions, against an in-memory S3 (no AWS, no boto3).

Run: python3 -m unittest discover -s gate/tier2
"""
import contextlib
import datetime
import email.utils
import hashlib
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

import cas  # noqa: E402
import s3gate  # noqa: E402
import assert_tier2 as t  # noqa: E402

RUN = "t2-20260928-120000-4242"
BUCKET = s3gate.bucket_of(RUN)
WORK = s3gate.work_prefix_of(RUN)
WORKDIR = WORK + "work/"
WB = s3gate.WORK_BUCKET

A_BAM = b"BAM\nsample\tA\ndepth\t10\n"
B_BAM = b"BAM\nsample\tB\ndepth\t20\n"
SUMMARY = b"summary for A\n"
DETAIL = b"detail\n"
META_A = {"sample": "A", "single_end": False, "lane": 1, "nested": {"kit": "truseq", "ids": [1, 2]}}
META_B = {"sample": "B", "single_end": True, "lane": 2, "nested": {"kit": "nextera", "ids": [3]}}
# The DirectoryManifest tier one's local `cold` run wrote for qc/A/A_qc (a real plugin run, 2026-09-28).
TIER_ONE_A_QC = "bafyreigdnp3gpgk2zpyhbpzdssoov7nwuc5l22twvwhocn3vlbsqufvzli"


# --------------------------------------------------------------------------
# An S3 that answers from a dict
# --------------------------------------------------------------------------

class ClientError(Exception):
    def __init__(self, status, code):
        Exception.__init__(self, code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class Body(object):
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def iter_chunks(self, size):
        for i in range(0, len(self.data), size):
            yield self.data[i:i + size]


class Paginator(object):
    def __init__(self, s3, name):
        self.s3, self.name = s3, name

    def paginate(self, Bucket, Prefix=""):
        if Bucket in self.s3.missing:
            raise ClientError(404, "NoSuchBucket")
        if self.name == "list_multipart_uploads":
            yield {"Uploads": [u for u in self.s3.uploads if u["Bucket"] == Bucket and u["Key"].startswith(Prefix)]}
            return
        keys = sorted(k for (b, k) in self.s3.objects if b == Bucket and k.startswith(Prefix))
        yield {"Contents": [{"Key": k, "Size": len(self.s3.objects[(Bucket, k)]),
                             "LastModified": datetime.datetime.fromtimestamp(self.s3.modified[(Bucket, k)],
                                                                            datetime.timezone.utc)}
                            for k in keys],
               "ResponseMetadata": {"HTTPHeaders": {"date": email.utils.formatdate(self.s3.now, usegmt=True)}}}


class FakeS3(object):
    class exceptions(object):
        ClientError = ClientError

    def __init__(self):
        self.objects, self.metadata, self.deleted_buckets = {}, {}, []
        self.missing, self.uploads, self.undeletable, self.late = set(), [], set(), {}
        self.now, self.modified = 1759233600.0, {}      # S3's clock (2026-09-30T12:00Z): LastModified and Date

    def put(self, bucket, key, data, metadata=None):
        self.objects[(bucket, key)] = data
        self.metadata[(bucket, key)] = metadata or {}
        self.modified[(bucket, key)] = self.now

    def get_paginator(self, name):
        return Paginator(self, name)

    def _get(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError(404, "NoSuchKey")
        return self.objects[(Bucket, Key)]

    def get_object(self, Bucket, Key):
        return {"Body": Body(self._get(Bucket, Key))}

    def head_object(self, Bucket, Key, **_kw):
        data = self._get(Bucket, Key)
        return {"ContentLength": len(data), "Metadata": self.metadata.get((Bucket, Key), {})}

    def put_object(self, Bucket, Key, Body, IfMatch=None):
        if IfMatch is not None:
            current = self.objects.get((Bucket, Key))
            if current is None or '"%s"' % hashlib.md5(current).hexdigest() != IfMatch:
                raise ClientError(412, "PreconditionFailed")
        self.put(Bucket, Key, Body)
        return {"ETag": '"%s"' % hashlib.md5(Body).hexdigest()}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)

    def delete_objects(self, Bucket, Delete):
        errors = []
        for o in Delete["Objects"]:
            if (Bucket, o["Key"]) in self.undeletable:
                errors.append({"Key": o["Key"], "Code": "AccessDenied", "Message": "Access Denied"})
            else:
                self.objects.pop((Bucket, o["Key"]), None)
        for (bucket, key), data in list(self.late.items()):   # a writer that was still running
            if bucket == Bucket:
                self.put(bucket, key, data)
                del self.late[(bucket, key)]
        return {"Errors": errors} if errors else {}

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads = [u for u in self.uploads if u["UploadId"] != UploadId]

    def delete_bucket(self, Bucket):
        if Bucket in self.missing:
            raise ClientError(404, "NoSuchBucket")
        self.deleted_buckets.append(Bucket)


# --------------------------------------------------------------------------
# A harness run that went right, built by hand
# --------------------------------------------------------------------------

def sums(files):
    return "".join("%s  %s\n" % (hashlib.sha256(data).hexdigest(), path) for path, data in sorted(files.items())).encode()


FLAT_QC = {"A_qc/summary.txt": SUMMARY, "A_qc/alias.txt": SUMMARY, "A_qc/nested/detail.txt": DETAIL}
FUSION_QC = {"A_qc/summary.txt": SUMMARY, "A_qc/alias.txt": b"summary.txt",
             "A_qc/.fusion.symlinks": b"alias.txt\n", "A_qc/nested/detail.txt": DETAIL}
FUSION_QC_NODE = {"A_qc/summary.txt": SUMMARY, "A_qc/nested/detail.txt": DETAIL}   # find -type f: no link, no sidecar

# Ticket 18: cas-sarek's ts run. Nextflow's PublishOp writes a workflow output's Output Index File
# (ticket 26) from the head node straight into the output directory: it is never a task's own output, so
# it has no file in any task work dir and lives only in the member (a block plus a coords/ pointer).
TS_META = {"id": "sarek"}
MQ_REPORT = b"multiqc report\n"
MQ_DATA = b"multiqc data\n"
MQ_PLOT = b"multiqc plot\n"
INDEX_JSON = b'{"report": "multiqc"}\n'

TASKS = {
    "t1": {"11/aaaa": {"A.bam": A_BAM}, "12/bbbb": {"B.bam": B_BAM}, "13/cccc": FLAT_QC},
    "t2": {"21/aaaa": {"A.bam": A_BAM}, "22/bbbb": {"B.bam": B_BAM}, "23/cccc": FUSION_QC},
    "t2b": {"31/aaaa": {"A.bam": A_BAM}, "33/cccc": FUSION_QC},
}
NODE = {"t2": {"21/aaaa": {"A.bam": A_BAM}, "22/bbbb": {"B.bam": B_BAM}, "23/cccc": FUSION_QC_NODE}}


class World(object):
    """Every file tier2.sh leaves behind for a passing run: the work bucket, four members, logs, evidence."""

    def __init__(self):
        self.s3 = FakeS3()
        self.root = tempfile.mkdtemp()
        self.rts = 9000000000000
        self.cids = {}
        self._work()
        self._members()
        self._logs()
        build_t7(self)

    # -- the work bucket -------------------------------------------------
    def _work(self):
        for run, tasks in TASKS.items():
            rows = ["task_id\thash\tnative_id\tname\tstatus\texit\trealtime\tworkdir"]
            for i, (task, files) in enumerate(sorted(tasks.items())):
                prefix = WORKDIR + task + "/"
                for path, data in files.items():
                    self.s3.put(WB, prefix + path, data)
                self.s3.put(WB, prefix + ".command.run", b"#!/bin/bash\nnxf_s3_upload 'A.bam' s3://x\n")
                self.s3.put(WB, prefix + ".exitcode", b"0")
                if task in NODE.get(run, {}):
                    self.s3.put(WB, prefix + ".command.cas", sums(NODE[run][task]))
                rows.append("%d\t%s\tjob-%s-%d\tP (%d)\tCOMPLETED\t0\t1m 3s\ts3://%s/%s"
                            % (i + 1, task, run, i, i, WB, prefix.rstrip("/")))
            self.write("trace/%s.txt" % run, "\n".join(rows) + "\n")

    # -- members ----------------------------------------------------------
    def block(self, member, value):
        if isinstance(value, bytes):
            data, cid = value, cas.cid_raw(value)
        else:
            data = cas.encode(value)
            cid = cas.cid_dagcbor(data)
        self.s3.put(BUCKET, "%s/blocks/%s/%s" % (member, cid[-2:], cid), data)
        return cid

    def manifest(self, member, files, fusion):
        """The manifest's blocks, through the Gate's own encoder (pinned to the plugin's by test_fusion_manifest)."""
        def put(node_files):
            tree_cid, value = t.expected_manifest(node_files, fusion)
            for entry in value["entries"]:
                if entry["mode"] == "regular":
                    self.block(member, node_files[entry["name"]])
                elif entry["mode"] == "directory":
                    put({k[len(entry["name"]) + 1:]: v for k, v in node_files.items() if k.startswith(entry["name"] + "/")})
            self.block(member, value)
            return tree_cid
        return put({k[len("A_qc/"):]: v for k, v in files.items()})

    def item(self, member, meta, name, address, size):
        return self.block(member, {"kind": "OutputItem", "schema": 2, "value": [
            meta, {"kind": "Leaf", "name": name, "address": cas.Cid(address), "size": size, "reason": None}]})

    def run(self, member, name, collections, providers, nf_hash, pipeline="cas-test-pipeline", anomalies=None):
        """collections[output] is (items, paths) or (items, paths, index): index is an OutputCollection.index
        value (ticket 26), {"leaf": Leaf dict, "path": string}. anomalies, when given, is a RunCompletion
        Anomalies map (ticket 19 adds "unjoined"); omitted, as the other runs leave it, for no "anomalies" key."""
        manifest = self.block(member, {"kind": "RunManifest", "schema": 2, "run_name": name,
                                       "nf_run_hash": nf_hash, "pipeline": pipeline})
        links = []
        for output in sorted(collections):
            items, paths, *rest = collections[output]
            index = rest[0] if rest else None
            value = {"kind": "OutputCollection", "schema": 2, "asserted_by": "gate",
                     "run": cas.Cid(manifest), "name": output,
                     "items": [cas.Cid(i) for i in items], "paths": paths}
            if index is not None:
                value["index"] = index
            links.append(cas.Cid(self.block(member, value)))
        completion_value = {"kind": "RunCompletion", "schema": 2, "run": cas.Cid(manifest),
                            "collections": links, "status": "succeeded",
                            "providers": {k: [cas.Cid(c) for c in sorted(v)] for k, v in providers.items()}}
        if anomalies is not None:
            completion_value["anomalies"] = anomalies
        completion = self.block(member, completion_value)
        self.rts -= 1
        self.s3.put(BUCKET, "%s/log/%d-run-%s" % (member, self.rts, completion), b"")
        self.cids[(member, name)] = {"completion": completion, "manifest": manifest}
        return completion

    def coord(self, member, rel, cid, name):
        self.s3.put(BUCKET, "%s/coords/%s" % (member, rel), ("cas://%s/%s\n" % (cid, name)).encode())

    def snapshot(self, member, runs, written_at):
        path = os.path.join(self.root, "snap-%s.sqlite" % member)
        if os.path.exists(path):
            os.remove(path)
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
        con.execute("CREATE TABLE run(completion_cid TEXT, pipeline TEXT)")
        con.execute("INSERT INTO meta VALUES ('snapshot_written_at', ?)", (written_at,))
        con.executemany("INSERT INTO run VALUES (?, 'p')", [("r%d" % i,) for i in range(runs)])
        con.commit()
        con.close()
        with open(path, "rb") as fh:
            self.s3.put(BUCKET, "%s/index/v3.sqlite" % member, fh.read(), metadata={"runs": str(runs)})

    def _members(self):
        c = self.c = {}
        for member in ("cas", "cas-t2", "cas-t6"):
            c[(member, "A.bam")], c[(member, "B.bam")] = self.block(member, A_BAM), self.block(member, B_BAM)
            c[(member, "aligned A")] = self.item(member, META_A, "A.bam", cas.cid_raw(A_BAM), len(A_BAM))
            c[(member, "aligned B")] = self.item(member, META_B, "B.bam", cas.cid_raw(B_BAM), len(B_BAM))
        self.flat = self.manifest("cas", FLAT_QC, fusion=False)
        self.fusion = self.manifest("cas", FUSION_QC, fusion=True)
        self.manifest("cas-t2", FUSION_QC, fusion=True)
        qc_flat = self.item("cas", META_A, "A_qc", self.flat, None)
        qc_fusion = self.item("cas", META_A, "A_qc", self.fusion, None)
        qc_fusion_t2 = self.item("cas-t2", META_A, "A_qc", self.fusion, None)
        aligned = ([c[("cas", "aligned A")], c[("cas", "aligned B")]], [["aligned/A/A.bam"], ["aligned/B/B.bam"]])
        raw = [cas.cid_raw(A_BAM), cas.cid_raw(B_BAM)]
        self.run("cas", "t1", {"aligned": aligned, "qc": ([qc_flat], [["qc/A/A_qc"]])}, {"s3-copy": raw}, "h1")
        self.run("cas", "t2b", {"aligned": ([c[("cas", "aligned A")]], [["aligned/A/A.bam"]]),
                                "qc": ([qc_fusion], [["qc/A/A_qc"]])},
                 {"fusion-node": [cas.cid_raw(A_BAM)]}, "h2b", pipeline="cas-tier2-small")
        self.run("cas-t2", "t2", {"aligned": aligned, "qc": ([qc_fusion_t2], [["qc/A/A_qc"]])}, {"s3-copy": raw}, "h2")
        for member in ("cas", "cas-t2"):
            self.coord(member, "aligned/A/A.bam", cas.cid_raw(A_BAM), "A.bam")
            self.coord(member, "aligned/B/B.bam", cas.cid_raw(B_BAM), "B.bam")
        self.coord("cas", "qc/A/A_qc", self.fusion, "A_qc")         # t2b wrote it last
        self.coord("cas-t2", "qc/A/A_qc", self.fusion, "A_qc")
        self.s3.put(BUCKET, "cas/nf/h1/aligned/A/A.bam/.data.json", json.dumps({"kind": "FileOutput", "spec": {
            "path": "cas://%s/A.bam" % cas.cid_raw(A_BAM), "workflowRun": "lid://h1"}}).encode())
        self.s3.put(BUCKET, "cas/nf/aaaa/A.bam/.data.json", json.dumps({"kind": "FileOutput", "spec": {
            "path": "s3://%s/%s11/aaaa/A.bam" % (WB, WORKDIR), "workflowRun": "lid://h1"}}).encode())
        self.snapshot("cas", 2, "2026-09-28T12:00:00.000Z")
        for name, h in (("t6a", "h6a"), ("t6b", "h6b")):
            self.run("cas-t6", name, {"aligned": aligned}, {"s3-copy": raw}, h)
        self.coord("cas-t6", "aligned/A/A.bam", cas.cid_raw(A_BAM), "A.bam")
        self.snapshot("cas-t6", 2, "2026-09-28T12:30:00.000Z")
        self._sarek()
        self._consumer()

    # -- cas-sarek (ticket 18) --------------------------------------------
    def _sarek(self):
        """multiqc: 3 items (Meta Map {"id": "sarek"} plus a file Leaf) and an OutputIndex at
        multiqc/index.json; 5 more coords/reports/ pointers no item or index leaf references, so the
        independent count of unjoined publishes is 5."""
        member = "cas-sarek"
        for name, data in (("multiqc_report.html", MQ_REPORT), ("multiqc_data.json", MQ_DATA), ("plot.png", MQ_PLOT)):
            self.block(member, data)
            self.coord(member, "multiqc/%s" % name, cas.cid_raw(data), name)
        self.block(member, INDEX_JSON)
        self.coord(member, "multiqc/index.json", cas.cid_raw(INDEX_JSON), "index.json")
        for i, content in enumerate((b"extra report %d\n" % n for n in range(1, 6)), start=1):
            cid = self.block(member, content)
            self.coord(member, "reports/x%d.txt" % i, cid, "x%d.txt" % i)
        self.ts_items = [self.item(member, TS_META, name, cas.cid_raw(data), len(data))
                         for name, data in (("multiqc_report.html", MQ_REPORT), ("multiqc_data.json", MQ_DATA),
                                            ("plot.png", MQ_PLOT))]
        self.ts_paths = [["multiqc/multiqc_report.html"], ["multiqc/multiqc_data.json"], ["multiqc/plot.png"]]
        self.write_ts(unjoined=5)

    @staticmethod
    def index_of(address):
        return {"leaf": {"kind": "Leaf", "name": "index.json", "address": cas.Cid(address),
                        "size": len(INDEX_JSON), "reason": None},
                "path": "multiqc/index.json"}

    def write_ts(self, unjoined, index_address=None):
        """(Re)writes cas-sarek's ts RunCompletion; a test drop_run's the old one first to replace it
        with another anomalies.unjoined or another index leaf address."""
        member = "cas-sarek"
        order = sorted(range(len(self.ts_items)), key=lambda i: self.ts_items[i])
        index = self.index_of(index_address if index_address is not None else cas.cid_raw(INDEX_JSON))
        providers = {"s3-copy": [cas.cid_raw(d) for d in (MQ_REPORT, MQ_DATA, MQ_PLOT, INDEX_JSON)]}
        anomalies = {"unresolvable": 0, "unaddressed": 0, "declined": 0, "never_published": 0, "unjoined": unjoined}
        self.run(member, "ts", {"multiqc": ([self.ts_items[i] for i in order], [self.ts_paths[i] for i in order], index)},
                 providers, "hts", pipeline="cas-sarek", anomalies=anomalies)

    @staticmethod
    def consumer_texts():
        """{source: (published file name, text)} a passing consumer run publishes under hashes/."""
        a, b = hashlib.sha256(A_BAM).hexdigest(), hashlib.sha256(B_BAM).hexdigest()
        listing = t.gate_listing({k[len("A_qc/"):]: v for k, v in FUSION_QC.items()}, "A_qc", fusion=True)
        return {"lid": ("A.bam.sha256", "%s  A.bam\n" % a), "cas": ("A.bam.sha256", "%s  A.bam\n" % a),
                "fromstore": ("B.bam.sha256", "%s  B.bam\n" % b),
                "dir": ("A_qc.sha256", "".join("%s  %s\n" % (d, p) for p, d in listing))}

    def consumer_run(self, name, h, texts):
        items, paths = [], []
        for source in sorted(texts):
            file_name, text = texts[source]
            leaf = self.block("cas-out", text.encode())
            items.append(self.item("cas-out", source, file_name, leaf, len(text)))
            paths.append(["hashes/%s/%s" % (source, file_name)])
        order = sorted(range(len(items)), key=lambda i: items[i])
        self.run("cas-out", name, {"hashes": ([items[i] for i in order], [paths[i] for i in order])}, {}, h,
                 pipeline="cas-gate-consumer")

    def drop_run(self, member, name):
        """Removes a run's Store Log entry, so a test can write that run again differently."""
        completion = self.cids[(member, name)]["completion"]
        self.s3.objects = {k: v for k, v in self.s3.objects.items()
                           if not (k[0] == BUCKET and k[1].startswith(member + "/log/") and k[1].endswith(completion))}

    def _consumer(self):
        texts = self.consumer_texts()
        for name, h in (("t4", "c4"), ("t5", "c5")):
            self.consumer_run(name, h, texts)
        self.snapshot("cas-out", 2, "2026-09-28T13:00:00.000Z")
        cache = os.path.join(self.root, "cache-consumer", "nf-blocks")
        os.makedirs(cache)
        con = sqlite3.connect(os.path.join(cache, "abc.sqlite"))
        con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
        con.execute("CREATE TABLE run(pipeline TEXT)")
        con.execute("INSERT INTO meta VALUES ('seeded_from:lab', '2026-09-28T12:00:00.000Z')")
        con.executemany("INSERT INTO run VALUES (?)", [("cas-test-pipeline",), ("cas-gate-consumer",)])
        con.commit()
        con.close()

    # -- logs and evidence --------------------------------------------------
    def write(self, rel, text):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(text)

    def _logs(self):
        for run in ("t1", "t2", "t2b", "t4", "t5", "t6a", "t6b"):
            self.write("logs/%s/exit" % run, "0\n")
            self.write("logs/%s/nextflow.log" % run, "INFO nextflow.cas - nf-blocks: the head node read 0 bytes\n")
        self.write("logs/ts/exit", "0\n")
        self.write("logs/ts/nextflow.log",
                   "INFO nextflow.cas - nf-blocks: the head node read 0 bytes\n"
                   "WARN nextflow.cas - nf-blocks: process 'NFCORE_SAREK:SAREK:MULTIQC:MULTIQC' uses publishDir; "
                   "cas.enabled records it once, at completion\n"
                   "WARN nextflow.cas - nf-blocks: 5 published file(s) are in no workflow output, recorded as "
                   "unjoined publishes\n")
        self.write("evidence/refs-t4.env", "T2_DIR='%s'\n" % t.refs(self.ctx())["T2_DIR"])
        self.write("evidence/cas-out-after-t4.json", json.dumps({"meta_runs": 1, "count": 1}))
        self.write("evidence/if-match.json", json.dumps({"status": 412, "body": "second"}))
        occurrences = []
        for name in ("t6a", "t6b"):
            completion = cas.decode(self.s3.objects[(BUCKET, self.key("cas-t6", self.cids[("cas-t6", name)]["completion"]))])
            collection = completion["collections"][0].text
            occurrences.append("cas://%s/%s" % (collection, self.c[("cas-t6", "aligned A")]))
        self.write("logs/t6-items.txt", "\n".join(occurrences) + "\n")
        self.write("logs/t6-race-1-x.txt", "nf-blocks:snapshot: wrote 2 runs\n")
        self.write("logs/t6-race-1-y.txt", "nf-blocks:snapshot: wrote 2 runs\n")
        self.write("logs/t6-race-2-x.txt", "nf-blocks:snapshot: not rewritten: replaced_meanwhile\n")
        self.write("logs/t6-race-2-y.txt", "nf-blocks:snapshot: wrote 2 runs\n")

    @staticmethod
    def key(member, cid):
        return "%s/blocks/%s/%s" % (member, cid[-2:], cid)

    def ctx(self):
        cold = {"aligned": {cas.cid_raw(A_BAM)}, "qc": set(self.cold_qc())}
        return t.Ctx(self.s3, BUCKET, WORK, self.root, cold=cold,
                     local_coords={"qc/A/A_qc": "cas://%s/A_qc" % TIER_ONE_A_QC})

    def cold_qc(self):
        return [cas.cid_dagcbor(cas.encode({"kind": "OutputItem", "schema": 2, "value": [
            META_A, {"kind": "Leaf", "name": "A_qc", "address": cas.Cid(self.fusion), "size": None, "reason": None}]}))]

    def close(self):
        shutil.rmtree(self.root, ignore_errors=True)


# --------------------------------------------------------------------------
# T7: gate/retention's two runs in cas-t7 and the verbs' sequence, as an honest tier2.sh leaves them
# --------------------------------------------------------------------------

T7 = t.T7_MEMBER
T7_HOST = "%s.s3.us-east-1.amazonaws.com" % BUCKET


def sdk_line(method, path, headers=(), query=()):
    """One line of the AWS SDK's request log as Nextflow's file appender writes it."""
    return ("Sep-30 12:00:00.000 [main] DEBUG software.amazon.awssdk.request - Sending Request: "
            "DefaultSdkHttpFullRequest(httpMethod=%s, protocol=https, host=%s, encodedPath=%s, headers=[%s], "
            "queryParameters=[%s])\n" % (method, T7_HOST, path, ", ".join(["amz-sdk-invocation-id", "Content-Length"]
                                                                            + list(headers) + ["User-Agent"]),
                                          ", ".join(query)))


def lock_take(first=False):
    """The requests of SweepLock.take: If-None-Match, and when the lock exists, a read and an If-Match PUT."""
    put = ("PUT", "/%s/sweep.lock" % T7, ["Content-Type", "If-None-Match"])
    if first:
        return [put]
    return [put, ("HEAD", "/%s/sweep.lock" % T7), ("GET", "/%s/sweep.lock" % T7, ["If-Match"]),
            ("PUT", "/%s/sweep.lock" % T7, ["Content-Type", "If-Match"])]


LOCK_RELEASE = [("PUT", "/%s/sweep.lock" % T7, ["Content-Type", "If-Match"])]


def build_t7(w, trash_extra=None, bare_lock_put=False, single_deletes=False, wait=2400):
    """cas-t7 in the fake S3 and <T2_ROOT>/logs/t7, stepped the way tier2.sh steps the plugin, with every
    checkpoint written by assert_tier2.t7_checkpoint itself. trash_extra "shared_two" ledgers b's two.txt,
    which a shares, in sweeps 1 and 3 and deletes it in sweep 4. bare_lock_put: sweep-2 takes sweep.lock with a
    plain PUT. single_deletes: sweep-4 deletes each block with its own DELETE. wait: seconds of S3's clock
    between live/gate-fake's write and the sweep steps (T1 to TS)."""
    s3, logs = w.s3, "logs/t7/"
    blocks = {}

    def put(rel, data):
        s3.put(BUCKET, "%s/%s" % (T7, rel), data)

    def step(name, code, out="", requests=()):
        w.write(logs + name + ".exit", "%d\n" % code)
        w.write(logs + name + ".out", out)
        w.write(logs + name + ".err", "")
        w.write(logs + name + ".nextflow.log", "".join(sdk_line(*r) for r in requests))
        s3.now += 5

    def checkpoint(name):
        t.t7_checkpoint(w.ctx(), name)

    def claim(subject, verb, attribute, value, supersedes=()):
        cid = w.block(T7, {"kind": "Claim", "schema": 1, "asserted_by": "gate", "subject": cas.Cid(subject),
                           "verb": verb, "attribute": attribute, "value": value,
                           "supersedes": [cas.Cid(c) for c in supersedes], "timestamp": "2026-09-30T12:40:00.000Z"})
        w.rts -= 1
        put("log/%d-claim-%s" % (w.rts, cid), b"")
        return cid

    def run(tag):
        items = []
        for name in ("shared", "only_" + tag, "pin_" + tag):
            data = ("%s\n" % name).encode()
            blocks["%s_%s" % (tag, name)] = w.block(T7, data)
            items.append(w.item(T7, {"id": name}, name + ".txt", cas.cid_raw(data), len(data)))
            blocks["%s_item_%s" % (tag, name)] = items[-1]
        one = ("%s one\n" % tag).encode()
        blocks[tag + "_one"], blocks["shared_two"] = w.block(T7, one), w.block(T7, b"shared two\n")
        directory = w.block(T7, {"kind": "DirectoryManifest", "schema": 2, "entries": [
            {"name": "one.txt", "mode": "regular", "size": len(one), "address": cas.Cid(blocks[tag + "_one"]), "target": None},
            {"name": "two.txt", "mode": "regular", "size": 11, "address": cas.Cid(blocks["shared_two"]), "target": None}]})
        blocks[tag + "_dir"] = directory
        dir_item = w.item(T7, {"id": "dir_" + tag}, "dir_" + tag, directory, None)
        completion = w.run(T7, "t7" + tag, {"files": (items, [["files/%s" % n] for n in ("shared", "only_" + tag, "pin_" + tag)]),
                                            "dirs": ([dir_item], [["dirs/dir_" + tag]])},
                           {}, "h7" + tag, pipeline="cas-gate-retention")
        w.write("logs/t7%s/exit" % tag, "0\n")
        return completion

    run_b = run("b")
    run_a = run("a")
    unshared = [blocks["b_only_b"], blocks["b_dir"], blocks["b_one"]]
    trashed = unshared + ([blocks[trash_extra]] if trash_extra else [])
    checkpoint("after-runs")
    step("dry-1", 0, json.dumps({"applied": False, "dead": 0}) + "\n", [("GET", "/", [], ["list-type", "prefix"])])
    checkpoint("after-dry-1")
    put("live/" + t.T7_STALE, b'{"session":"gate-fake","run_name":"gate_fake_run"}')

    s3.now += wait                                                   # T1 to TS
    step("prune-dry", 0)
    step("prune", 0)
    release = claim(run_b, "set", "retain", "lineage")
    step("pin", 0)
    claim(blocks["b_item_pin_b"], "add", "pin", "figure 3")
    put("live/" + t.T7_FRESH, b'{"session":"gate-fresh","run_name":"gate_fresh_run"}')
    step("live-refused", 1, "", lock_take(first=True) + LOCK_RELEASE)
    w.write(logs + "live-refused.err", "nf-blocks:sweep: a pipeline is running: gate_fresh_run (session gate-fresh)\n")
    released = json.dumps({"sweep": "x", "state": "released", "started_at": "x", "beat": 0}).encode()
    put("sweep.lock", released)
    s3.delete_object(Bucket=BUCKET, Key="%s/live/%s" % (T7, t.T7_FRESH))
    checkpoint("before-sweep")

    def ledger(name, cids):
        put("trash/" + name, json.dumps({"sweep": name.split("-", 1)[1], "blocks": [{"cid": c, "size": 1} for c in sorted(cids)]}).encode())

    first = "1760400000000-20260930T124000Z-aaaaaaaa"
    step("sweep-1", 0, json.dumps({"applied": True, "ledger": first}) + "\n",
         lock_take() + [("PUT", "/%s/trash/%s" % (T7, first), ["Content-Type"]),
                        ("DELETE", "/%s/live/%s" % (T7, t.T7_STALE))] + LOCK_RELEASE)
    ledger(first, trashed)
    s3.delete_object(Bucket=BUCKET, Key="%s/live/%s" % (T7, t.T7_STALE))
    put("sweep.lock", released)
    checkpoint("after-sweep-1")

    step("untrash", 0, "", lock_take() + [("PUT", "/%s/trash/%s" % (T7, first), ["Content-Type"])] + LOCK_RELEASE)
    ledger(first, [c for c in trashed if c != blocks["b_one"]])
    put("sweep.lock", released)
    checkpoint("after-untrash")

    step("restore", 0)
    restore = claim(run_b, "del", "retain", None, [release])
    take = lock_take()
    if bare_lock_put:
        take = [("PUT", "/%s/sweep.lock" % T7, ["Content-Type"])]
    step("sweep-2", 0, json.dumps({"applied": True}) + "\n",
         take + [("DELETE", "/%s/trash/%s" % (T7, first))] + LOCK_RELEASE)
    s3.delete_object(Bucket=BUCKET, Key="%s/trash/%s" % (T7, first))
    put("sweep.lock", released)
    checkpoint("after-sweep-2")

    step("release-again", 0)
    claim(run_b, "set", "retain", "lineage", [restore])
    second = "1759236300000-20260930T124500Z-bbbbbbbb"
    step("sweep-3", 0, json.dumps({"applied": True, "ledger": second}) + "\n",
         lock_take() + [("PUT", "/%s/trash/%s" % (T7, second), ["Content-Type"])] + LOCK_RELEASE)
    ledger(second, trashed)
    put("sweep.lock", released)
    checkpoint("after-sweep-3")

    if single_deletes:
        deletion = [("DELETE", "/%s/blocks/%s/%s" % (T7, c[-2:], c)) for c in trashed]
    else:
        deletion = [("POST", "/", ["Content-MD5", "Content-Type"], ["delete"])]
    step("sweep-4", 0, json.dumps({"applied": True}) + "\n",
         lock_take() + deletion + [("DELETE", "/%s/trash/%s" % (T7, second))] + LOCK_RELEASE)
    for c in trashed:
        s3.delete_object(Bucket=BUCKET, Key=World.key(T7, c))
    s3.delete_object(Bucket=BUCKET, Key="%s/trash/%s" % (T7, second))
    put("sweep.lock", released)
    checkpoint("after-sweep-4")
    return blocks


class WorldTest(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.addCleanup(self.w.close)

    def status(self, fn):
        return fn(self.w.ctx())


# --------------------------------------------------------------------------
# s3gate
# --------------------------------------------------------------------------

class S3GateTest(unittest.TestCase):
    def test_completions_are_the_run_entries_of_the_store_log(self):
        s3 = FakeS3()
        for name in ("8209376805637-run-bafyA", "8209376805638-selection-bafyS", "8209376805639-run-bafyB"):
            s3.put(BUCKET, "cas/log/" + name, b"")
        self.assertEqual(s3gate.Member(s3, BUCKET, "cas").completions(), ["bafyA", "bafyB"])

    def test_work_objects_leave_out_command_files_and_exitcode(self):
        s3 = FakeS3()
        for rel in ("ab/cdef/A.bam", "ab/cdef/.command.run", "ab/cdef/.command.cas", "ab/cdef/.exitcode",
                    "ab/cdef/A_qc/nested/detail.txt", "ab/stray"):
            s3.put(WB, WORKDIR + rel, b"x")
        self.assertEqual(sorted(s3gate.work_objects(s3, WORKDIR)), ["A.bam", "A_qc/nested/detail.txt"])

    def test_stale_if_match_is_refused_and_keeps_the_second_body(self):
        s3 = FakeS3()
        self.assertEqual(s3gate.stale_if_match(s3, BUCKET), {"status": 412, "body": "second"})
        self.assertNotIn((BUCKET, "probe/if-match"), s3.objects)

    def teardown(self, s3, run=RUN, bucket=BUCKET, prefix=WORK):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            status = s3gate.teardown(run, bucket, prefix, s3=s3)
        return status, err.getvalue()

    def test_teardown_refuses_a_foreign_bucket_and_touches_nothing(self):
        s3 = FakeS3()
        s3.put("robs-real-data", "important", b"x")
        s3.put(WB, WORK + "work/ab/cdef/A.bam", b"x")
        status, err = self.teardown(s3, bucket="robs-real-data")
        self.assertEqual(status, 1)
        self.assertIn("refusing to delete bucket 'robs-real-data'", err)
        self.assertEqual(len(s3.objects), 2)
        self.assertEqual(s3.deleted_buckets, [])

    def test_teardown_refuses_a_foreign_prefix_and_touches_nothing(self):
        s3 = FakeS3()
        s3.put(BUCKET, "cas/blocks/aa/x", b"x")
        for prefix in ("", "robsyme/", s3gate.work_prefix_of("t2-20260928-120000-1")):
            s3.put(WB, prefix + "someone-else/data", b"x")
            status, err = self.teardown(s3, prefix=prefix)
            self.assertEqual(status, 1)
            self.assertIn("refusing to empty", err)
        self.assertEqual(len(s3.objects), 4)
        self.assertEqual(s3.deleted_buckets, [])

    def test_teardown_refuses_a_run_id_it_did_not_make(self):
        status, err = self.teardown(FakeS3(), run="", bucket="nf-blocks-t2-", prefix="robsyme/nf-blocks-gate//")
        self.assertEqual(status, 1)
        self.assertIn("not a tier-two run id", err)

    def test_teardown_empties_the_run_prefix_and_deletes_the_bucket(self):
        s3 = FakeS3()
        s3.put(WB, WORK + "work/ab/cdef/A.bam", b"x")
        s3.put(WB, "robsyme/nf-blocks-gate/t2-other/work/ab/cdef/A.bam", b"x")
        s3.put(BUCKET, "cas/blocks/aa/x", b"x")
        s3.uploads = [{"Bucket": BUCKET, "Key": "cas/blocks/bb/y", "UploadId": "u1"}]
        self.assertEqual(self.teardown(s3), (0, ""))
        self.assertEqual(list(s3.objects), [(WB, "robsyme/nf-blocks-gate/t2-other/work/ab/cdef/A.bam")])
        self.assertEqual((s3.uploads, s3.deleted_buckets), ([], [BUCKET]))

    def test_teardown_treats_a_missing_bucket_as_gone(self):
        s3 = FakeS3()
        s3.missing.add(BUCKET)
        s3.put(WB, WORK + "work/ab/cdef/A.bam", b"x")
        self.assertEqual(self.teardown(s3), (0, ""))
        self.assertEqual(s3.objects, {})

    def test_teardown_fails_on_a_key_delete_objects_did_not_delete(self):
        s3 = FakeS3()
        s3.put(WB, WORK + "work/ab/cdef/A.bam", b"x")
        s3.undeletable.add((WB, WORK + "work/ab/cdef/A.bam"))
        status, err = self.teardown(s3)
        self.assertEqual(status, 1)
        self.assertIn("1 key(s) not deleted", err)
        self.assertEqual(s3.deleted_buckets, [BUCKET])     # the other half still ran

    def test_teardown_fails_when_a_late_writer_left_an_object(self):
        s3 = FakeS3()
        s3.put(WB, WORK + "work/ab/cdef/A.bam", b"x")
        s3.late[(WB, WORK + "work/ab/cdef/.command.log")] = b"late"
        status, err = self.teardown(s3)
        self.assertEqual(status, 1)
        self.assertIn("still holds 1 object(s)", err)


# --------------------------------------------------------------------------
# The assertions
# --------------------------------------------------------------------------

class ManifestTest(unittest.TestCase):
    def test_fusion_manifest_decodes_the_sidecar_and_equals_the_plugins(self):
        cid, value = t.expected_manifest({"summary.txt": SUMMARY, "alias.txt": b"summary.txt",
                                          ".fusion.symlinks": b"alias.txt\n", "nested/detail.txt": DETAIL}, fusion=True)
        self.assertEqual([(e["name"], e["mode"], e["target"]) for e in value["entries"]],
                         [("alias.txt", "symlink", "summary.txt"), ("nested", "directory", None),
                          ("summary.txt", "regular", None)])
        self.assertEqual(cid, TIER_ONE_A_QC)

    def test_flattened_alias_is_a_regular_file_with_summarys_bytes(self):
        _cid, value = t.expected_manifest({"summary.txt": SUMMARY, "alias.txt": SUMMARY,
                                           "nested/detail.txt": DETAIL}, fusion=False)
        alias, summary = value["entries"][0], value["entries"][2]
        self.assertEqual((alias["mode"], alias["address"], alias["size"]), ("regular", summary["address"], 14))

    def test_listing_follows_the_link_and_leaves_out_the_sidecar(self):
        listing = t.gate_listing({"summary.txt": SUMMARY, "alias.txt": b"summary.txt",
                                  ".fusion.symlinks": b"alias.txt\n", "nested/detail.txt": DETAIL}, "A_qc", fusion=True)
        s, d = hashlib.sha256(SUMMARY).hexdigest(), hashlib.sha256(DETAIL).hexdigest()
        self.assertEqual(listing, [("A_qc/alias.txt", s), ("A_qc/nested/detail.txt", d), ("A_qc/summary.txt", s)])


class AssertionTest(WorldTest):
    def test_a_passing_run_passes_every_check(self):
        for code, _title, fn in t.CHECKS:
            status, message = self.status(fn)
            self.assertEqual(status, t.PASS, "%s: %s" % (code, message))

    def test_t1_fails_when_a_coordinate_names_another_cid(self):
        self.w.coord("cas", "aligned/A/A.bam", cas.cid_raw(B_BAM), "A.bam")
        status, message = self.status(t.t1)
        self.assertEqual(status, t.FAIL)
        self.assertIn("coords/aligned/A/A.bam", message)

    def test_t1_fails_when_a_command_run_names_the_scheme(self):
        self.w.s3.put(WB, WORKDIR + "11/aaaa/.command.run", b"nxf_s3_upload cas://x\n")
        self.assertEqual(self.status(t.t1)[0], t.FAIL)

    def replace_t2(self, aligned_items=None, provider="s3-copy"):
        """t2's RunCompletion again, with other aligned items or another provider for every file."""
        old = self.w.ctx().run("cas-t2", "t2").collections()
        aligned, qc = old["aligned"][1], old["qc"][1]
        items = aligned_items or [i.text for i in aligned["items"]]
        self.w.s3.objects = {k: v for k, v in self.w.s3.objects.items() if not k[1].startswith("cas-t2/log/")}
        self.w.run("cas-t2", "t2", {"aligned": (items, aligned["paths"]),
                                    "qc": ([i.text for i in qc["items"]], qc["paths"])},
                   {provider: [cas.cid_raw(A_BAM), cas.cid_raw(B_BAM)]}, "h2")

    def test_a_fusion_node_leaf_fails_t2_and_passes_t2b(self):
        self.replace_t2(provider="fusion-node")
        ctx = self.w.ctx()
        status, message = t.t2(ctx)
        self.assertEqual(status, t.FAIL)
        self.assertIn("not under s3-copy", message)
        self.assertEqual(t.provider_problems(ctx.run("cas-t2", "t2"), t.FUSION_NODE), [])
        self.assertEqual(t.t2b(ctx)[0], t.PASS)

    def test_t2_items_equal_t1s_aligned_and_tier_ones_qc_although_t1s_qc_differs(self):
        ctx = self.w.ctx()
        self.assertNotEqual(ctx.run("cas-t2", "t2").item_cids("qc"), ctx.run("cas", "t1").item_cids("qc"))
        self.assertEqual(t._item_problems(ctx, "t2", ctx.run("cas-t2", "t2")), [])

    def test_t2_fails_when_an_aligned_item_differs(self):
        other = self.w.item("cas-t2", dict(META_A, lane=9), "A.bam", cas.cid_raw(A_BAM), len(A_BAM))
        self.replace_t2(aligned_items=[other, self.w.c[("cas-t2", "aligned B")]])
        status, message = self.status(t.t2)
        self.assertEqual(status, t.FAIL)
        self.assertIn("aligned items", message)

    def test_t2_fails_when_a_command_cas_digest_is_not_the_gates(self):
        self.w.s3.put(WB, WORKDIR + "21/aaaa/.command.cas", ("%s  A.bam\n" % ("0" * 64)).encode())
        status, message = self.status(t.t2)
        self.assertEqual(status, t.FAIL)
        self.assertIn(".command.cas says", message)

    def test_refs_names_t2bs_manifest_whose_alias_is_a_symlink_as_an_occurrence(self):
        ctx = self.w.ctx()
        refs = t.refs(ctx)
        t2b_qc = ctx.run("cas", "t2b").collections()["qc"]
        self.assertEqual(refs["T2_DIR"], "cas://%s/%s/A_qc" % (t2b_qc[0], t2b_qc[1]["items"][0].text))
        manifest, name = t.occurrence_manifest(ctx.member("cas"), refs["T2_DIR"])
        self.assertEqual((manifest, name), (self.w.fusion, "A_qc"))
        self.assertEqual(t._entry(ctx.member("cas").decoded(manifest), "alias.txt")["mode"], "symlink")
        self.assertEqual(refs["T2_LID"], "lid://h1/aligned/A/A.bam")
        self.assertEqual(refs["T2_CAS"], "cas://%s/A.bam" % cas.cid_raw(A_BAM))

    def test_refs_falls_back_to_t1s_flattened_occurrence_without_t2b(self):
        self.w.s3.objects = {k: v for k, v in self.w.s3.objects.items()
                             if not (k[1].startswith("cas/log/") and self.w.cids[("cas", "t2b")]["completion"] in k[1])}
        ctx = self.w.ctx()
        manifest, _name = t.occurrence_manifest(ctx.member("cas"), t.refs(ctx)["T2_DIR"])
        self.assertEqual(manifest, self.w.flat)

    def test_occurrence_manifest_refuses_an_item_the_collection_does_not_list(self):
        ctx = self.w.ctx()
        t1_aligned = ctx.run("cas", "t1").collections()["aligned"][0]
        t2b_item = ctx.run("cas", "t2b").collections()["qc"][1]["items"][0].text
        self.assertEqual(t.occurrence_manifest(ctx.member("cas"), "cas://%s/%s/A_qc" % (t1_aligned, t2b_item)), (None, None))

    def test_t4_fails_when_dir_names_a_manifest_without_a_link(self):
        t1_qc = self.w.ctx().run("cas", "t1").collections()["qc"]
        self.w.write("evidence/refs-t4.env", "T2_DIR='cas://%s/%s/A_qc'\n" % (t1_qc[0], t1_qc[1]["items"][0].text))
        status, message = self.status(t.t4)
        self.assertEqual(status, t.FAIL)
        self.assertIn("ticket 15 decision 7", message)

    def test_t3_fails_when_t2s_manifest_is_not_tier_ones(self):
        ctx = self.w.ctx()
        ctx._local_coords = {"qc/A/A_qc": "cas://%s/A_qc" % self.w.flat}
        status, message = t.t3(ctx)
        self.assertEqual(status, t.FAIL)
        self.assertIn("the backend changed the address", message)

    def test_t5_fails_when_the_cache_was_not_seeded_from_labs_snapshot(self):
        self.w.snapshot("cas", 2, "2026-09-28T12:59:00.000Z")
        self.assertEqual(self.status(t.t5)[0], t.FAIL)

    def test_t6_race_skips_when_both_verbs_wrote(self):
        self.w.write("logs/t6-race-2-x.txt", "nf-blocks:snapshot: wrote 2 runs\n")
        status, message = self.status(t.t6)
        self.assertEqual(status, t.SKIP)
        self.assertIn("did not overlap in 3 attempts", message)

    def test_t6_fails_when_s3_let_the_stale_if_match_through(self):
        self.w.write("evidence/if-match.json", json.dumps({"status": 200, "body": "third"}))
        self.assertEqual(self.status(t.t6)[0], t.FAIL)

    def test_ts_passes_with_3_items_an_index_leaf_and_5_unjoined_coords(self):
        status, message = self.status(t.ts)
        self.assertEqual(status, t.PASS)
        self.assertIn("multiqc has 3 item(s)", message)
        self.assertIn("anomalies.unjoined 5 matches 5", message)


class FailPathTest(WorldTest):
    """Each check the paid run relies on, seen to FAIL on a crafted input (Task 14, final review)."""

    def replace_t2b(self, qc_item=None, provider="fusion-node"):
        c = self.w.c
        qc = qc_item or self.w.item("cas", META_A, "A_qc", self.w.fusion, None)
        self.w.drop_run("cas", "t2b")
        self.w.run("cas", "t2b", {"aligned": ([c[("cas", "aligned A")]], [["aligned/A/A.bam"]]),
                                  "qc": ([qc], [["qc/A/A_qc"]])},
                   {provider: [cas.cid_raw(A_BAM)]}, "h2b", pipeline="cas-tier2-small")

    def test_t2b_fails_when_a_file_leaf_is_not_fusion_node(self):
        self.replace_t2b(provider="s3-copy")
        status, message = self.status(t.t2b)
        self.assertEqual(status, t.FAIL)
        self.assertIn("t2b: 1 of 1 file leaves not under fusion-node", message)
        self.assertIn("is under s3-copy", message)

    def test_t2b_fails_when_its_qc_items_are_not_t2s(self):
        self.replace_t2b(qc_item=self.w.item("cas", META_A, "A_qc", self.w.flat, None))
        status, message = self.status(t.t2b)
        self.assertEqual(status, t.FAIL)
        self.assertIn("t2b's qc items", message)
        self.assertIn("are not among t2's", message)

    def replace_t1_qc(self, manifest):
        c = self.w.c
        aligned = ([c[("cas", "aligned A")], c[("cas", "aligned B")]], [["aligned/A/A.bam"], ["aligned/B/B.bam"]])
        self.w.drop_run("cas", "t1")
        self.w.run("cas", "t1", {"aligned": aligned, "qc": ([self.w.item("cas", META_A, "A_qc", manifest, None)], [["qc/A/A_qc"]])},
                   {"s3-copy": [cas.cid_raw(A_BAM), cas.cid_raw(B_BAM)]}, "h1")

    def test_t3_fails_when_t1s_alias_is_a_link_rather_than_flattened(self):
        self.replace_t1_qc(self.w.fusion)
        status, message = self.status(t.t3)
        self.assertEqual(status, t.FAIL)
        self.assertIn("t1's A_qc manifest", message)
        self.assertIn("nxf_s3_upload flattens it", message)

    def test_t3_fails_when_t1s_manifest_is_not_the_gates_walk_of_the_work_bucket(self):
        self.w.s3.put(WB, WORKDIR + "13/cccc/A_qc/nested/detail.txt", b"changed after the publish\n")
        status, message = self.status(t.t3)
        self.assertEqual(status, t.FAIL)
        self.assertIn("the Gate's walk of the work bucket gives", message)
        self.assertNotIn("nxf_s3_upload flattens it", message)

    def replace_t4(self, **changed):
        texts = dict(self.w.consumer_texts(), **changed)
        self.w.drop_run("cas-out", "t4")
        self.w.consumer_run("t4", "c4", texts)

    def test_t4_fails_when_a_staged_digest_is_not_the_work_buckets(self):
        self.replace_t4(lid=("A.bam.sha256", "%s  A.bam\n" % hashlib.sha256(B_BAM).hexdigest()))
        status, message = self.status(t.t4)
        self.assertEqual(status, t.FAIL)
        self.assertIn("lid staged", message)
        self.assertIn("expected only A.bam.sha256 of %s" % hashlib.sha256(A_BAM).hexdigest(), message)

    def test_t4_fails_when_the_staged_directory_listing_differs(self):
        listing = t.gate_listing({k[len("A_qc/"):]: v for k, v in FUSION_QC.items()}, "A_qc", fusion=True)
        without_alias = "".join("%s  %s\n" % (d, p) for p, d in listing if not p.endswith("alias.txt"))
        self.replace_t4(dir=("A_qc.sha256", without_alias))
        status, message = self.status(t.t4)
        self.assertEqual(status, t.FAIL)
        self.assertIn("dir staged", message)
        self.assertIn("alias.txt as summary.txt's bytes", message)

    def test_t6_fails_when_a_block_does_not_hash_to_its_cid(self):
        cid = cas.cid_raw(A_BAM)
        self.w.s3.put(BUCKET, World.key("cas-t6", cid), b"not the bytes the address names\n")
        status, message = self.status(t.t6)
        self.assertEqual(status, t.FAIL)
        self.assertIn("block %s" % cid, message)

    def test_ts_fails_when_unjoined_disagrees_with_the_independent_count(self):
        self.w.drop_run("cas-sarek", "ts")
        self.w.write_ts(unjoined=3)
        status, message = self.status(t.ts)
        self.assertEqual(status, t.FAIL)
        self.assertIn("anomalies.unjoined is 3; 5 coords/", message)

    def test_ts_fails_when_the_index_leaf_does_not_match_the_members_bytes(self):
        # A wrong index leaf address is also, itself, an unreferenced coords/multiqc/index.json pointer
        # (the real one): the independent count rises from 5 to 6, so unjoined is given as 6 to isolate
        # the index-leaf FAIL from an unjoined-count one. No block was ever written for this address, so
        # Member.verified (fetch + re-hash) fails to find it: the member's bytes don't back the claim.
        self.w.drop_run("cas-sarek", "ts")
        wrong = cas.cid_raw(b"not the index.json bytes\n")
        self.w.write_ts(unjoined=6, index_address=wrong)
        status, message = self.status(t.ts)
        self.assertEqual(status, t.FAIL)
        self.assertIn("index leaf %s: NoSuchKey" % wrong, message)

    def test_ts_fails_when_the_index_leaf_disagrees_with_its_coordinate(self):
        # The RunCompletion's index leaf keeps the real, correctly-hashing address; only the standalone
        # coords/multiqc/index.json pointer (a second, independently-written source) is rewritten to a
        # different, also validly-stored block, isolating the pointer cross-check from a bytes mismatch.
        other = self.w.block("cas-sarek", b"a different index.json\n")
        self.w.coord("cas-sarek", "multiqc/index.json", other, "index.json")
        status, message = self.status(t.ts)
        self.assertEqual(status, t.FAIL)
        self.assertIn("index leaf's address is", message)
        self.assertIn("coords/multiqc/index.json names %s" % other, message)

    def test_ts_fails_when_an_item_leafs_address_disagrees_with_its_coordinate(self):
        self.w.coord("cas-sarek", "multiqc/multiqc_report.html", cas.cid_raw(b"different bytes\n"), "multiqc_report.html")
        status, message = self.status(t.ts)
        self.assertEqual(status, t.FAIL)
        self.assertIn("coords/multiqc/multiqc_report.html names", message)


def t1_checkpoints():
    return t._tier_one().RETENTION_CHECKPOINTS


class T7Test(unittest.TestCase):
    """T7 over a fake cas-t7: one honest member, then one fault at a time (each a fresh World)."""

    def world(self, **faults):
        """A World whose cas-t7 and T7 logs and evidence are built again, with `faults`."""
        w = World()
        self.addCleanup(w.close)
        w.s3.objects = {k: v for k, v in w.s3.objects.items() if not (k[0] == BUCKET and k[1].startswith(T7 + "/"))}
        for rel in ("logs/t7", "evidence/t7"):
            shutil.rmtree(os.path.join(w.root, rel), ignore_errors=True)
        w.blocks = build_t7(w, **faults)
        return w

    def test_passes_on_an_honest_member(self):
        status, message = t.t7(self.world().ctx())
        self.assertEqual(status, t.PASS, message)
        self.assertIn("3 unshared blocks trashed", message)
        self.assertIn("1 DeleteObjects request(s)", message)

    def test_fails_when_a_shared_block_was_ledgered(self):
        status, message = t.t7(self.world(trash_extra="shared_two").ctx())
        self.assertEqual(status, t.FAIL)
        self.assertIn("sweep-1 trashed content shared with run a", message)

    def test_fails_when_sweep_lock_was_put_without_a_condition(self):
        status, message = t.t7(self.world(bare_lock_put=True).ctx())
        self.assertEqual(status, t.FAIL)
        self.assertIn("sweep-2 PUT sweep.lock 1 time(s) without If-None-Match or If-Match", message)

    def test_fails_when_blocks_were_deleted_one_at_a_time(self):
        status, message = t.t7(self.world(single_deletes=True).ctx())
        self.assertEqual(status, t.FAIL)
        self.assertIn("one DELETE at a time", message)
        self.assertIn("without a DeleteObjects request", message)

    def test_fails_when_the_stale_registration_was_not_eleven_minutes_old(self):
        status, message = t.t7(self.world(wait=300).ctx())
        self.assertEqual(status, t.FAIL)
        self.assertIn("live/gate-fake is 3", message)

    def test_refs_name_both_runs_the_pinned_item_and_the_current_retain_claims(self):
        w = self.world()
        refs = t.t7_refs(w.ctx())
        self.assertEqual(sorted(refs), ["T7_A", "T7_B", "T7_PIN_ITEM", "T7_RELEASE", "T7_RESTORE"])
        self.assertEqual(refs["T7_PIN_ITEM"], w.blocks["b_item_pin_b"])
        member = w.ctx().member(T7)
        self.assertEqual(member.decoded(refs["T7_RELEASE"])["supersedes"][0].text, refs["T7_RESTORE"])

    def test_wait_counts_down_by_s3s_clock_to_eleven_minutes(self):
        w = self.world()
        w.s3.put(BUCKET, "%s/live/%s" % (T7, t.T7_STALE), b"{}")         # written now, by S3's clock
        w.s3.now += 100
        seconds, why = t.t7_wait(w.ctx())
        self.assertEqual(seconds, t.T7_AGE - 100)
        self.assertIn("cannot be backdated", why)
        w.s3.now += t.T7_AGE
        self.assertEqual(t.t7_wait(w.ctx())[0], 0)

    def test_untrash_pick_is_bs_one_txt(self):
        w = self.world()
        # after sweep-4 there is no ledger and dir_b is gone; tier2.sh picks after sweep-1, when one ledger holds it
        w.s3.put(BUCKET, "%s/trash/1759236300000-x" % T7, json.dumps(
            {"sweep": "x", "blocks": [{"cid": w.blocks["b_one"], "size": 1}]}).encode())
        self.assertEqual(t.t7_untrash_pick(w.ctx()), w.blocks["b_one"])

    def test_only_after_runs_records_the_closures(self):
        w = self.world()
        for name in t1_checkpoints():
            data = t._json(os.path.join(w.root, "evidence", "t7", name + ".json"))
            self.assertEqual("closures" in data, name == "after-runs", name)
            self.assertNotIn("closures_error", data, name)

    def test_parses_the_sdks_request_line(self):
        [(method, path, headers, query)] = t.parse_sdk_requests(
            sdk_line("PUT", "/cas-t7/trash/1759236300000-x%3Ay", ["If-None-Match"]))
        self.assertEqual((method, path, query), ("PUT", "/cas-t7/trash/1759236300000-x:y", set()))
        self.assertIn("if-none-match", headers)


class VerdictTest(unittest.TestCase):
    def test_race_with_one_write_passes_and_with_two_skips(self):
        self.assertEqual(t.race_verdict(["not rewritten: replaced_meanwhile", "wrote"])[0], t.PASS)
        self.assertEqual(t.race_verdict(["wrote", "wrote"])[0], t.SKIP)
        self.assertEqual(t.race_verdict(["not rewritten: replaced_meanwhile"] * 2)[0], t.FAIL)

    def test_if_match_verdict(self):
        self.assertEqual(t.if_match_verdict({"status": 412, "body": "second"}), [])
        self.assertTrue(t.if_match_verdict({"status": 200, "body": "third"}))
        self.assertTrue(t.if_match_verdict(None))

    def test_trace_durations(self):
        self.assertEqual(t.duration_seconds("1m 3s"), 63)
        self.assertAlmostEqual(t.duration_seconds("850ms"), 0.85)
        self.assertEqual(t.duration_seconds("-"), 0)


if __name__ == "__main__":
    unittest.main()
