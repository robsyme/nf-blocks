#!/usr/bin/env python3
"""Generate gate/fixtures/root: a hand-made GATE_ROOT for developing assert.py.

This is not a recording of a real run. It is what DESIGN.md says a correct
plugin must produce, built with the Gate's own encoder, so that assert.py can
be exercised (and its PASS branches proved) before the plugin exists.

    python3 gate/fixtures/make_fixture.py
    python3 gate/assert.py gate/fixtures/root --offline

Everything it writes is deterministic: re-running produces identical bytes.
"""

import hashlib
import json
import os
import shutil
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import cas  # noqa: E402

ROOT = os.path.join(HERE, "root")
ASSERTED_BY = "gate"
PIPELINE = "cas-test-pipeline"   # gate.config sets manifest.name to this

STATS_BYTES = b"schema\t1\nmetric\tvalue\ntotal\t42\n"
SAMPLES = [
    ("A", 10, {"sample": "A", "single_end": False, "lane": 1,
               "nested": {"kit": "truseq", "ids": [1, 2]}}),
    ("B", 20, {"sample": "B", "single_end": True, "lane": 2,
               "nested": {"kit": "nextera", "ids": [3]}}),
    ("C", 30, {"sample": "C", "single_end": False, "lane": 1,
               "nested": {"kit": "truseq", "ids": [4, 5]}}),
]

# name -> (nf_run_hash, session_id, resumed, status, finished_at_millis)
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


def bam_bytes(sample, depth):
    return ("BAM\nsample\t%s\ndepth\t%s\n" % (sample, depth)).encode()


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
    for rel, content in files.items():
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


def build_qc_manifest(writer, sample):
    """<S>_qc: summary.txt, nested/detail.txt, alias.txt -> summary.txt."""
    summary = ("summary for %s\n" % sample).encode()
    detail = b"detail\n"
    summary_cid = writer.put_raw(summary)
    detail_cid = writer.put_raw(detail)
    nested = writer.put_block({
        "kind": "DirectoryManifest", "schema": 1,
        "entries": [{"name": "detail.txt", "mode": "regular",
                     "size": len(detail), "address": cas.Cid(detail_cid),
                     "target": None}]})
    top = writer.put_block({
        "kind": "DirectoryManifest", "schema": 1,
        "entries": [
            {"name": "alias.txt", "mode": "symlink", "size": len("summary.txt"),
             "address": None, "target": "summary.txt"},
            {"name": "nested", "mode": "directory", "size": 0,
             "address": cas.Cid(nested), "target": None},
            {"name": "summary.txt", "mode": "regular", "size": len(summary),
             "address": cas.Cid(summary_cid), "target": None},
        ]})
    return top, {"summary.txt": summary, "nested/detail.txt": detail,
                 "alias.txt": ("link", "summary.txt")}


def main():
    if os.path.isdir(ROOT):
        shutil.rmtree(ROOT)
    store = os.path.join(ROOT, "store")
    store_out = os.path.join(ROOT, "store-out")
    writer = Writer(store)
    out_writer = Writer(store_out)

    stats_cid = writer.put_raw(STATS_BYTES)
    bam_cids = {s: writer.put_raw(bam_bytes(s, d)) for s, d, _ in SAMPLES}
    qc = {s: build_qc_manifest(writer, s) for s, _d, _m in SAMPLES}

    # ---- work trees, the bytes the Gate re-hashes for itself ----------
    for launch in ("pipeline-a", "pipeline-b"):
        work = os.path.join(ROOT, launch, "work")
        for i, (sample, depth, _meta) in enumerate(SAMPLES):
            h = ("%02x" % (i + 1)) + ("b%c" % sample.lower()) * 15
            write_work_tree(work, h, {"%s.bam" % sample: bam_bytes(sample, depth)})
            h = ("%02x" % (i + 4)) + ("c%c" % sample.lower()) * 15
            write_work_tree(work, h, {"%s.stats" % sample: STATS_BYTES})
        for i, (sample, _depth, _meta) in enumerate(SAMPLES):
            write_work_tree(work, ("%02x" % (i + 7)) + ("d%c" % sample.lower()) * 15,
                            {"%s_qc/%s" % (sample, k): v
                             for k, v in qc[sample][1].items()})

    # ---- per-run metadata ---------------------------------------------
    per_run = {}
    for name, nf_hash, session, resumed, status, finished in RUNS:
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
            "params": {"fail": status == "failed", "qc_mode": "copy",
                       "refdir": "[redacted-location]"},
            "config": {"manifest": {"name": PIPELINE},
                       "process": {"executor": "local"},
                       "env": {"HOME": "[redacted-location]",
                               "SAMPLE_OWNER": "[redacted-user]"}},
            "script": None,
            "started_at": "2026-01-01T00:00:00.000Z",
        }
        manifest_cid = writer.put_block(manifest)

        # The failed run publishes no qc and only two stats: partial.
        samples = SAMPLES if status != "failed" else [SAMPLES[0], SAMPLES[2]]
        collections = {}

        def collection(output, entries):
            entries = sorted(entries, key=lambda e: e[0])
            return writer.put_block({
                "kind": "OutputCollection", "schema": 1,
                "asserted_by": ASSERTED_BY, "run": cas.Cid(manifest_cid),
                "name": output,
                "items": [cas.Cid(c) for c, _p in entries],
                "paths": [p for _c, p in entries]})

        aligned = []
        for sample, depth, meta in samples:
            item = writer.put_block({
                "kind": "OutputItem", "schema": 1,
                "value": [meta, leaf("%s.bam" % sample, bam_cids[sample],
                                     len(bam_bytes(sample, depth)))]})
            aligned.append((item, [["aligned", sample, "%s.bam" % sample]]))
            writer.coord("aligned/%s/%s.bam" % (sample, sample),
                         bam_cids[sample], "%s.bam" % sample)
        collections["aligned"] = collection("aligned", aligned)

        stats = []
        for sample, _depth, meta in samples:
            item = writer.put_block({
                "kind": "OutputItem", "schema": 1,
                "value": [meta, leaf("%s.stats" % sample, stats_cid,
                                     len(STATS_BYTES))]})
            stats.append((item, [["stats", sample, "%s.stats" % sample]]))
            writer.coord("stats/%s/%s.stats" % (sample, sample),
                         stats_cid, "%s.stats" % sample)
        collections["stats"] = collection("stats", stats)

        if status != "failed":
            qc_entries = []
            for sample, _depth, meta in samples:
                item = writer.put_block({
                    "kind": "OutputItem", "schema": 1,
                    "value": [meta, leaf("%s_qc" % sample, qc[sample][0], 0)]})
                qc_entries.append((item, [["qc", sample, "%s_qc" % sample]]))
                writer.coord("qc/%s/%s_qc" % (sample, sample), qc[sample][0],
                             "%s_qc" % sample)
            collections["qc"] = collection("qc", qc_entries)

        completion_cid = writer.put_block({
            "kind": "RunCompletion", "schema": 1, "asserted_by": ASSERTED_BY,
            "run": cas.Cid(manifest_cid),
            "collections": [cas.Cid(collections[k]) for k in sorted(collections)],
            "input_set": None,
            "status": status,
            "exit_status": 7 if status == "failed" else 0,
            "possibly_incomplete": status == "failed",
            "started_at": "2026-01-01T00:00:00.000Z",
            "finished_at": "2026-01-01T00:01:00.000Z",
            "anomalies": {"unresolvable": 0, "unaddressed": 0,
                          "declined": 0, "never_published": 0},
            "error": "MAYBE_FAIL (2) exited 7" if status == "failed" else None,
        })
        writer.run_log(finished, completion_cid)
        per_run[name] = {
            "manifest_cid": manifest_cid, "completion_cid": completion_cid,
            "nf_hash": nf_hash, "status": status, "finished": finished,
            "collections": collections, "manifest": manifest,
        }

        # Nextflow's own records, DefaultLinStore layout under nf/.
        for j, (sample, _d, _m) in enumerate(samples):
            writer.nf_record(
                "%s/stats/%s/%s.stats" % (nf_hash, sample, sample), "FileOutput",
                {"path": "work/%s.stats" % sample,
                 # three byte-identical files, three different fingerprints
                 "checksum": {"value": hashlib.md5(
                     ("%s-%s" % (nf_hash, sample)).encode()).hexdigest(),
                     "algorithm": "nextflow", "mode": "standard"},
                 "workflowRun": "lid://%s" % nf_hash,
                 "taskRun": "lid://%s" % (("%02x" % (j + 4)) + ("c%c" % sample.lower()) * 15),
                 "size": len(STATS_BYTES), "labels": ["stats"]})
        n_tasks = 15 if name != "fail" else 14
        for t in range(n_tasks):
            writer.nf_record("%s%030x" % (nf_hash[:2], t), "TaskRun",
                             {"name": "TASK (%d)" % t,
                              "workflowRun": "lid://%s" % nf_hash,
                              "sessionId": session})

    # ---- snapshots gate.sh takes between runs -------------------------
    # (the fixture pretends `again` added no block at all, which is the point)
    blocks = sorted(p for _c, p in cas.Store(store).blocks())
    rel = [os.path.relpath(p, ROOT) for p in blocks]
    for snapshot in ("blocks-after-cold.txt", "blocks-after-again.txt"):
        with open(os.path.join(ROOT, snapshot), "w") as fh:
            fh.write("\n".join(rel) + "\n")

    # ---- the consumer pipeline's own store ----------------------------
    for tag, sample in (("lid", "A"), ("cas", "A"), ("fromstore", "B")):
        depth = dict((s, d) for s, d, _ in SAMPLES)[sample]
        digest = hashlib.sha256(bam_bytes(sample, depth)).hexdigest()
        text = ("%s  %s.bam\n" % (digest, sample)).encode()
        cid = out_writer.put_raw(text)
        out_writer.coord("hashes/%s/%s.sha256" % (tag, tag), cid,
                         "%s.sha256" % tag)

    # ---- logs, as gate.sh keeps them ----------------------------------
    for name, *_rest in RUNS:
        d = os.path.join(ROOT, "logs", name)
        os.makedirs(d)
        with open(os.path.join(d, "exit"), "w") as fh:
            fh.write("1\n" if name == "fail" else "0\n")
        open(os.path.join(d, "stdout.log"), "w").close()
    d = os.path.join(ROOT, "logs", "consumer")
    os.makedirs(d)
    with open(os.path.join(d, "exit"), "w") as fh:
        fh.write("0\n")

    # ---- the SQLite index --------------------------------------------
    build_index(store, per_run, stats_cid)
    print("fixture written to %s" % ROOT)


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


def build_index(store_root, per_run, stats_cid):
    cache = os.path.join(ROOT, "cache", "nf-blocks")
    os.makedirs(cache)
    digest = hashlib.sha256(store_root.encode()).hexdigest()[:16]
    path = os.path.join(cache, digest + ".sqlite")
    con = sqlite3.connect(path)
    con.execute("PRAGMA page_size = 512")
    con.executescript(INDEX_SCHEMA)
    con.execute("INSERT INTO schema_version VALUES (1)")
    store = cas.Store(store_root)
    for name, info in per_run.items():
        m = info["manifest"]
        con.execute(
            "INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (info["completion_cid"], info["manifest_cid"], m["pipeline"], None,
             None, m["nf_run_hash"], m["session_id"], name, ASSERTED_BY,
             info["status"], 1 if info["status"] == "failed" else 0,
             "2026-01-01T00:01:%02d.000Z" % (info["finished"] % 60), "lab"))
        for output, collection_cid in info["collections"].items():
            con.execute("INSERT INTO collection VALUES (?,?,?)",
                        (collection_cid, info["completion_cid"], output))
            block = store.read_block(collection_cid)
            for item_link, paths in zip(block["items"], block["paths"]):
                item_cid = item_link.text
                con.execute("INSERT OR IGNORE INTO item VALUES (?)", (item_cid,))
                con.execute("INSERT INTO collection_item VALUES (?,?)",
                            (collection_cid, item_cid))
                item = store.read_block(item_cid)
                meta = item["value"][0]
                for key, value in flatten(meta):
                    con.execute("INSERT INTO item_attr VALUES (?,?,?,?,?)",
                                (item_cid, key, type_name(value),
                                 text_of(value), 0))
                leaf_value = item["value"][1]
                filename = leaf_value["name"]
                address = leaf_value["address"]
                if address is not None:
                    con.execute("INSERT INTO producer VALUES (?,?,?,?,?)",
                                (address.text, item_cid, collection_cid,
                                 info["completion_cid"], filename))
                del paths
    for key, envelope in store.nf_envelopes():
        spec = envelope.get("spec") or {}
        con.execute("INSERT OR REPLACE INTO nf_record VALUES (?,?,?,?,?,?)",
                    (key, envelope.get("kind"), spec.get("workflowRun"),
                     spec.get("taskRun"), json.dumps(spec.get("labels")), None))
    con.commit()
    con.isolation_level = None
    con.execute("VACUUM")
    con.close()
    del stats_cid


def flatten(value, prefix=""):
    out = []
    if isinstance(value, dict):
        for key in sorted(value):
            out.extend(flatten(value[key], prefix + key if not prefix
                               else prefix + "." + key))
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
