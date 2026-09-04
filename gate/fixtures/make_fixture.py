#!/usr/bin/env python3
"""Generate gate/fixtures/root: a hand-made GATE_ROOT for developing assert.py.

This is not a recording of a real run. It is what DESIGN.md says a correct
plugin must produce for the Test Pipeline, built with the Gate's own encoder,
so that assert.py can be exercised (and its PASS branches proved) before the
plugin exists.

    python3 gate/fixtures/make_fixture.py
    python3 gate/assert.py gate/fixtures/root --offline

All five outputs are modelled, including the multi-leaf `chunks` items and the
partial `reports` of the failed run. Everything it writes is deterministic:
re-running produces identical bytes.
"""

import datetime
import hashlib
import importlib.util
import json
import os
import shutil
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
GATE = os.path.dirname(HERE)
sys.path.insert(0, GATE)

import cas  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "gate_assert", os.path.join(GATE, "assert.py"))
gate_assert = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate_assert)

ROOT = os.path.join(HERE, "root")
ASSERTED_BY = "gate"
PIPELINE = "cas-test-pipeline"   # gate.config sets manifest.name to this

STATS_BYTES = b"schema\t1\nmetric\tvalue\ntotal\t42\n"
CHUNKS = [("chunk_1.txt", b"chunk one\n"),
          ("chunk_2.txt", b"chunk two\n"),
          ("chunk_3.txt", b"chunk three\n")]

SAMPLES = [
    ("A", 10, {"sample": "A", "single_end": False, "lane": 1,
               "nested": {"kit": "truseq", "ids": [1, 2]}}),
    ("B", 20, {"sample": "B", "single_end": True, "lane": 2,
               "nested": {"kit": "nextera", "ids": [3]}}),
    ("C", 30, {"sample": "C", "single_end": False, "lane": 1,
               "nested": {"kit": "truseq", "ids": [4, 5]}}),
]
META = {s: m for s, _d, m in SAMPLES}
DEPTH = {s: d for s, d, _m in SAMPLES}

# The failed run: MAYBE_FAIL exits 7 for sample B, so reports carries only C
# and qc never publishes. Entry counts measured on released Nextflow, issue 17.
FAIL_SAMPLES = ["A", "C"]
FAIL_REPORTS = ["C"]

RUNS = [
    ("cold", "a1" * 16, "11111111-1111-1111-1111-111111111111", False,
     "succeeded", 1767225600000),
    ("again", "a2" * 16, "22222222-2222-2222-2222-222222222222", False,
     "succeeded", 1767225660000),
    ("fail", "a3" * 16, "33333333-3333-3333-3333-333333333333", False,
     "failed", 1767225720000),
    ("resumed", "a4" * 16, "11111111-1111-1111-1111-111111111111", True,
     "succeeded", 1767225780000),
    ("elsewhere", "a5" * 16, "55555555-5555-5555-5555-555555555555", False,
     "succeeded", 1767225840000),
]


def bam_bytes(sample):
    return ("BAM\nsample\t%s\ndepth\t%s\n" % (sample, DEPTH[sample])).encode()


def report_bytes(sample):
    return ("report for %s\n" % sample).encode()


def summary_bytes(sample):
    return ("summary for %s\n" % sample).encode()


DETAIL_BYTES = b"detail\n"


# --------------------------------------------------------------------------

class Writer(object):
    def __init__(self, store_root):
        self.root = store_root

    def put_raw(self, data):
        return self._write(cas.cid_raw(data), data)

    def put_block(self, value):
        data = cas.encode(value)
        return self._write(cas.cid_dagcbor(data), data)

    def _write(self, cid, data):
        d = os.path.join(self.root, "blocks", cid[-2:])
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, cid)
        if not os.path.exists(path):
            with open(path, "wb") as fh:
                fh.write(data)
            os.chmod(path, 0o444)
        return cid

    def coord(self, rel_path, cid, name):
        path = os.path.join(self.root, "coords", *rel_path.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write("cas://%s/%s\n" % (cid, name))

    def run_log(self, finished_millis, completion_cid):
        d = os.path.join(self.root, "runs")
        os.makedirs(d, exist_ok=True)
        rts = "%013d" % (9999999999999 - finished_millis)
        open(os.path.join(d, "%s-%s" % (rts, completion_cid)), "w").close()

    def nf_record(self, key, kind, spec):
        d = os.path.join(self.root, "nf", *key.split("/"))
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, ".data.json"), "w") as fh:
            json.dump({"version": "lineage/v1beta1", "kind": kind, "spec": spec},
                      fh, indent=1, sort_keys=True)


def leaf(name, address, size):
    return {"kind": "Leaf", "name": name, "address": cas.Cid(address),
            "size": size, "provider": "head-node", "reason": None}


def write_work_tree(work_root, task_hash, files):
    """files: {relative path: bytes | ('link', target)}"""
    base = os.path.join(work_root, task_hash[:2], task_hash[2:])
    for rel, content in sorted(files.items()):
        path = os.path.join(base, *rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if isinstance(content, tuple):
            if os.path.lexists(path):
                os.remove(path)
            os.symlink(content[1], path)
        else:
            with open(path, "wb") as fh:
                fh.write(content)
    return base


def qc_files(sample):
    return {"summary.txt": summary_bytes(sample),
            "nested/detail.txt": DETAIL_BYTES,
            "alias.txt": ("link", "summary.txt")}


def build_qc_manifest(writer, sample):
    """<S>_qc: summary.txt, nested/detail.txt, alias.txt -> summary.txt."""
    summary = summary_bytes(sample)
    summary_cid = writer.put_raw(summary)
    detail_cid = writer.put_raw(DETAIL_BYTES)
    nested = writer.put_block({
        "kind": "DirectoryManifest", "schema": 1,
        "entries": [{"name": "detail.txt", "mode": "regular",
                     "size": len(DETAIL_BYTES), "address": cas.Cid(detail_cid),
                     "target": None}]})
    return writer.put_block({
        "kind": "DirectoryManifest", "schema": 1,
        "entries": [
            {"name": "alias.txt", "mode": "symlink", "size": len("summary.txt"),
             "address": None, "target": "summary.txt"},
            {"name": "nested", "mode": "directory", "size": 0,
             "address": cas.Cid(nested), "target": None},
            {"name": "summary.txt", "mode": "regular", "size": len(summary),
             "address": cas.Cid(summary_cid), "target": None},
        ]})


def task_hash(index):
    return ("%02x" % (index % 256)) + ("%030x" % index)


def main():
    if os.path.isdir(ROOT):
        shutil.rmtree(ROOT)
    store_root = os.path.join(ROOT, "store")
    writer = Writer(store_root)
    out_writer = Writer(os.path.join(ROOT, "store-out"))

    # ---- content blocks ------------------------------------------------
    stats_cid = writer.put_raw(STATS_BYTES)
    bam_cid = {s: writer.put_raw(bam_bytes(s)) for s in META}
    report_cid = {s: writer.put_raw(report_bytes(s)) for s in META}
    chunk_cid = {name: writer.put_raw(data) for name, data in CHUNKS}
    qc_cid = {s: build_qc_manifest(writer, s) for s in META}

    # ---- work trees, the bytes the Gate re-hashes for itself ------------
    locked = []
    for launch in ("pipeline-a", "pipeline-b"):
        work = os.path.join(ROOT, launch, "work")
        index = 0
        for sample in sorted(META):
            for files in ({"%s.bam" % sample: bam_bytes(sample)},
                          {"%s.stats" % sample: STATS_BYTES},
                          {"%s.report" % sample: report_bytes(sample)},
                          {name: data for name, data in CHUNKS},
                          {"%s_qc/%s" % (sample, k): v
                           for k, v in qc_files(sample).items()}):
                index += 1
                base = write_work_tree(work, task_hash(index), files)
                if launch == "pipeline-a":
                    for rel in sorted(files):
                        locked.append(os.path.relpath(os.path.join(base, *rel.split("/")),
                                                      ROOT))

    # ---- per-run metadata ----------------------------------------------
    per_run = {}
    for name, nf_hash, session, resumed, status, finished in RUNS:
        failed = status == "failed"
        manifest = {
            "kind": "RunManifest", "schema": 1, "asserted_by": ASSERTED_BY,
            "pipeline": PIPELINE, "repository": None, "revision": None,
            "commit_id": None, "run_name": name, "nf_run_hash": nf_hash,
            "session_id": session, "resumed": resumed,
            "nextflow_version": "26.04.6",
            # params and config as DESIGN section 6's portability scrub leaves
            # them: the cas, lineage, workDir, outputDir, launchDir, projectDir,
            # homeDir, configFiles, scriptFile, commandLine, runName and resume
            # scopes dropped, absolute paths and non-lid/cas URIs replaced with
            # "[redacted-location]", the OS user name with "[redacted-user]".
            "params": {"fail": failed, "qc_mode": "copy",
                       "refdir": "[redacted-location]"},
            "config": {"manifest": {"name": PIPELINE},
                       "process": {"executor": "local"},
                       "env": {"HOME": "[redacted-location]",
                               "SAMPLE_OWNER": "[redacted-user]"}},
            "script": None,
            "started_at": iso_utc(finished - 60000),
        }
        manifest_cid = writer.put_block(manifest)

        samples = FAIL_SAMPLES if failed else sorted(META)
        entries = {output: [] for output in
                   ("aligned", "stats", "qc", "chunks", "reports")}

        def add(output, sample, leaves, paths):
            item = writer.put_block({"kind": "OutputItem", "schema": 1,
                                     "value": [META[sample], leaves]})
            entries[output].append((item, paths))
            for one, path in zip(_flat_leaves(leaves), paths):
                writer.coord("/".join(path), one["address"].text, one["name"])

        for sample in samples:
            add("aligned", sample,
                leaf("%s.bam" % sample, bam_cid[sample], len(bam_bytes(sample))),
                [["aligned", sample, "%s.bam" % sample]])
            add("stats", sample,
                leaf("%s.stats" % sample, stats_cid, len(STATS_BYTES)),
                [["stats", sample, "%s.stats" % sample]])
            add("chunks", sample,
                [leaf(n, chunk_cid[n], len(d)) for n, d in CHUNKS],
                [["chunks", sample, n] for n, _d in CHUNKS])
            if not failed:
                add("qc", sample, leaf("%s_qc" % sample, qc_cid[sample], 0),
                    [["qc", sample, "%s_qc" % sample]])
        for sample in (FAIL_REPORTS if failed else samples):
            add("reports", sample,
                leaf("%s.report" % sample, report_cid[sample],
                     len(report_bytes(sample))),
                [["reports", sample, "%s.report" % sample]])

        collections = {}
        for output in sorted(entries):
            rows = sorted(entries[output], key=lambda e: e[0])
            collections[output] = writer.put_block({
                "kind": "OutputCollection", "schema": 1,
                "asserted_by": ASSERTED_BY, "run": cas.Cid(manifest_cid),
                "name": output,
                "items": [cas.Cid(c) for c, _p in rows],
                "paths": [p for _c, p in rows]})

        completion_cid = writer.put_block({
            "kind": "RunCompletion", "schema": 1, "asserted_by": ASSERTED_BY,
            "run": cas.Cid(manifest_cid),
            "collections": [cas.Cid(collections[k]) for k in sorted(collections)],
            "input_set": None,
            "status": status,
            "exit_status": 7 if failed else 0,
            "possibly_incomplete": failed,
            "started_at": iso_utc(finished - 60000),
            "finished_at": iso_utc(finished),
            "anomalies": {"unresolvable": 0, "unaddressed": 0,
                          "declined": 0, "never_published": 0},
            "error": "MAYBE_FAIL (2) exited 7" if failed else None,
        })
        writer.run_log(finished, completion_cid)
        per_run[name] = {"manifest_cid": manifest_cid, "manifest": manifest,
                         "completion_cid": completion_cid, "status": status,
                         "finished": finished, "collections": collections}

        # Nextflow's own records, DefaultLinStore layout under nf/.
        for sample in samples:
            writer.nf_record(
                "%s/stats/%s/%s.stats" % (nf_hash, sample, sample), "FileOutput",
                {"path": "work/%s.stats" % sample,
                 # three byte-identical files, three different fingerprints
                 "checksum": {"value": hashlib.md5(
                     ("%s-%s" % (nf_hash, sample)).encode()).hexdigest(),
                     "algorithm": "nextflow", "mode": "standard"},
                 "workflowRun": "lid://%s" % nf_hash,
                 "taskRun": "lid://%s" % task_hash(hash_index(sample)),
                 "size": len(STATS_BYTES), "labels": ["stats"]})
        for t in range(15 if not failed else 14):
            writer.nf_record("%s%030x" % (nf_hash[:2], t), "TaskRun",
                             {"name": "TASK (%d)" % t,
                              "workflowRun": "lid://%s" % nf_hash,
                              "sessionId": session})

    # ---- logs, as gate.sh keeps them ------------------------------------
    for name, *_rest in RUNS:
        d = os.path.join(ROOT, "logs", name)
        os.makedirs(d)
        with open(os.path.join(d, "exit"), "w") as fh:
            fh.write("1\n" if name == "fail" else "0\n")
        open(os.path.join(d, "stdout.log"), "w").close()
    with open(os.path.join(ROOT, "logs", "resumed", "sources-locked"), "w") as fh:
        fh.write("\n".join(sorted(locked)) + "\n")
    d = os.path.join(ROOT, "logs", "consumer")
    os.makedirs(d)
    with open(os.path.join(d, "exit"), "w") as fh:
        fh.write("0\n")

    # ---- the consumer's own store ---------------------------------------
    published = [("lid", ["A"]), ("cas", ["A"]), ("fromstore", ["B"]),
                 ("fromlineage", ["A", "B", "C"])]
    for source, samples in published:
        for sample in samples:
            digest = hashlib.sha256(bam_bytes(sample)).hexdigest()
            text = ("%s  %s.bam\n" % (digest, sample)).encode()
            cid = out_writer.put_raw(text)
            name = "%s.bam.sha256" % sample
            out_writer.coord("hashes/%s/%s" % (source, name), cid, name)

    # ---- the snapshots gate.sh takes between runs ------------------------
    # The fixture pretends `again` added nothing at all, which is the point.
    gate = gate_assert.Gate(ROOT)
    for snapshot in ("blocks-after-cold.txt", "blocks-after-again.txt"):
        gate_assert.write_snapshot(gate, os.path.join(ROOT, snapshot))

    build_index(store_root, per_run)
    print("fixture written to %s" % ROOT)


def hash_index(sample):
    return 5 * sorted(META).index(sample) + 2


def _flat_leaves(leaves):
    return leaves if isinstance(leaves, list) else [leaves]


INDEX_SCHEMA = """
CREATE TABLE schema_version (version INTEGER);
CREATE TABLE run (completion_cid TEXT PRIMARY KEY, manifest_cid TEXT,
    pipeline TEXT, revision TEXT, commit_id TEXT, nf_run_hash TEXT,
    session_id TEXT, run_name TEXT, asserted_by TEXT, status TEXT,
    possibly_incomplete INTEGER, finished_at TEXT, member TEXT);
CREATE TABLE collection (collection_cid TEXT PRIMARY KEY, completion_cid TEXT,
    output_name TEXT);
CREATE TABLE item (item_cid TEXT PRIMARY KEY);
CREATE TABLE collection_item (collection_cid TEXT, item_cid TEXT);
CREATE TABLE producer (content_cid TEXT, item_cid TEXT, collection_cid TEXT,
    completion_cid TEXT, filename TEXT);
CREATE TABLE consumer (content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT);
CREATE TABLE item_attr (item_cid TEXT, path TEXT, type TEXT, value TEXT,
    truncated INTEGER);
CREATE TABLE claim_current (subject_cid TEXT, attribute TEXT, value TEXT,
    claim_cid TEXT, conflicted INTEGER);
CREATE TABLE missing (have_cid TEXT, needed_cid TEXT);
CREATE TABLE nf_record (key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT,
    task_run TEXT, labels_json TEXT, block_cid TEXT);
CREATE INDEX producer_content ON producer(content_cid);
CREATE INDEX run_pipeline ON run(pipeline, status, finished_at DESC);
CREATE INDEX item_attr_lookup ON item_attr(path, type, value);
"""


def build_index(store_root, per_run):
    cache = os.path.join(ROOT, "cache", "nf-blocks")
    os.makedirs(cache)
    digest = hashlib.sha256(store_root.encode()).hexdigest()[:16]
    path = os.path.join(cache, digest + ".sqlite")
    con = sqlite3.connect(path)
    con.execute("PRAGMA page_size = 512")
    con.executescript(INDEX_SCHEMA)
    con.execute("INSERT INTO schema_version VALUES (1)")
    store = cas.Store(store_root)
    for name, info in sorted(per_run.items()):
        m = info["manifest"]
        con.execute(
            "INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (info["completion_cid"], info["manifest_cid"], m["pipeline"], None,
             None, m["nf_run_hash"], m["session_id"], name, ASSERTED_BY,
             info["status"], 1 if info["status"] == "failed" else 0,
             iso_utc(info["finished"]), "lab"))
        for output, collection_cid in sorted(info["collections"].items()):
            con.execute("INSERT INTO collection VALUES (?,?,?)",
                        (collection_cid, info["completion_cid"], output))
            block = store.read_block(collection_cid)
            for item_link in block["items"]:
                item_cid = item_link.text
                con.execute("INSERT OR IGNORE INTO item VALUES (?)", (item_cid,))
                con.execute("INSERT INTO collection_item VALUES (?,?)",
                            (collection_cid, item_cid))
                item = store.read_block(item_cid)
                for key, value in flatten(item["value"][0]):
                    con.execute("INSERT INTO item_attr VALUES (?,?,?,?,?)",
                                (item_cid, key, type_name(value),
                                 text_of(value), 0))
                for one in gate_assert._leaves(item["value"]):
                    if one.get("address") is not None:
                        con.execute("INSERT INTO producer VALUES (?,?,?,?,?)",
                                    (one["address"].text, item_cid, collection_cid,
                                     info["completion_cid"], one["name"]))
    for key, envelope in store.nf_envelopes():
        spec = envelope.get("spec") or {}
        con.execute("INSERT OR REPLACE INTO nf_record VALUES (?,?,?,?,?,?)",
                    (key, envelope.get("kind"), spec.get("workflowRun"),
                     spec.get("taskRun"), json.dumps(spec.get("labels")), None))
    con.commit()
    con.isolation_level = None
    con.execute("VACUUM")
    con.close()


def iso_utc(millis):
    """ISO-8601 UTC with millisecond precision, so finished_at sorts correctly."""
    base = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=millis)
    return base.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (millis % 1000)


def flatten(value, prefix=""):
    out = []
    if isinstance(value, dict):
        for key in sorted(value):
            out.extend(flatten(value[key],
                               key if not prefix else prefix + "." + key))
    elif isinstance(value, list):
        for element in value:
            out.extend(flatten(element, prefix))
    else:
        out.append((prefix, value))
    return out


def type_name(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def text_of(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


if __name__ == "__main__":
    main()
