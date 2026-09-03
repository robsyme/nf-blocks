#!/usr/bin/env python3
"""The Gate's assertions. Independent of the plugin by construction.

    python3 gate/assert.py <GATE_ROOT> [--offline] [--verbose]
    python3 gate/assert.py <GATE_ROOT> --refs      # shell vars for gate.sh

Every address this file checks is derived here, with hashlib, from the bytes
the pipeline actually produced in its work directory or from the bytes of a
block on disk. Nothing is taken on the plugin's word. Exit status is 1 if any
assertion FAILs; SKIP never fails the Gate.

Assertion numbers are the spec's (.scratch/content-addressed-lineage/spec.md
section 1.2). Numbers 8, 9, 11 and 12 are out of the Walking Skeleton and are
reported as SKIP with the spec's own wording, so the list stays complete.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cas  # noqa: E402

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"

STATS_BYTES = b"schema\t1\nmetric\tvalue\ntotal\t42\n"

# Pipeline Identity, fixed by `manifest.name` in gate/gate.config. DESIGN
# section 6 takes the first non-null of cas.pipeline, manifest.name and
# projectName; `nextflow run .` gives projectName the literal "main.nf", so the
# Gate names the pipeline explicitly and checks the plugin recorded that name.
PIPELINE_IDENTITY = "cas-test-pipeline"

REGISTRY = []


def assertion(number, title, online_only=False):
    def register(fn):
        REGISTRY.append({"number": number, "title": title, "fn": fn,
                         "online_only": online_only})
        return fn
    return register


# --------------------------------------------------------------------------
# What the Gate knows about a GATE_ROOT
# --------------------------------------------------------------------------

class Run(object):
    def __init__(self, name):
        self.name = name
        self.manifest_cid = None
        self.manifest = None
        self.completion_cid = None
        self.completion = None

    @property
    def nf_run_hash(self):
        return (self.manifest or {}).get("nf_run_hash")

    def collections(self, gate):
        """{output name: (cid, block)} for this run's OutputCollections."""
        out = {}
        if not self.completion:
            return out
        for link in self.completion.get("collections") or []:
            block = gate.block(link.text)
            if block:
                out[block.get("name")] = (link.text, block)
        return out

    def item_cids(self, gate):
        out = set()
        for _name, (_cid, block) in self.collections(gate).items():
            for link in block.get("items") or []:
                if link is not None:
                    out.add(link.text)
        return out


class Gate(object):
    def __init__(self, root, offline=False):
        self.root = os.path.abspath(root)
        self.offline = offline
        self.store = cas.Store(os.path.join(self.root, "store"))
        self.store_out = cas.Store(os.path.join(self.root, "store-out"))
        self.decode_errors = []
        self._blocks = None
        self._runs = None
        self._index = None
        self._index_error = None

    # -- blocks ---------------------------------------------------------
    @property
    def blocks(self):
        """{cid: decoded block} for every dag-cbor block that decodes."""
        if self._blocks is None:
            self._blocks = {}
            for cid, path in self.store.blocks("dagcbor"):
                try:
                    self._blocks[cid] = self.store.read_block(cid)
                except Exception as exc:
                    self.decode_errors.append("%s (%s): %s" % (cid, path, exc))
        return self._blocks

    def block(self, cid):
        return self.blocks.get(cid)

    def of_kind(self, kind):
        return {cid: b for cid, b in self.blocks.items()
                if isinstance(b, dict) and b.get("kind") == kind}

    # -- runs -----------------------------------------------------------
    @property
    def runs(self):
        """{run_name: Run}, assembled from the blocks alone."""
        if self._runs is None:
            self._runs = {}
            by_manifest = {}
            for cid, block in self.of_kind("RunManifest").items():
                name = block.get("run_name")
                run = self._runs.setdefault(name, Run(name))
                run.manifest_cid, run.manifest = cid, block
                by_manifest[cid] = run
            for cid, block in self.of_kind("RunCompletion").items():
                link = block.get("run")
                run = by_manifest.get(link.text) if isinstance(link, cas.Cid) else None
                if run is None:
                    run = self._runs.setdefault("<orphan:%s>" % cid[:12], Run(None))
                run.completion_cid, run.completion = cid, block
        return self._runs

    def run(self, name):
        run = self.runs.get(name)
        if run is None:
            raise cas.GateError(
                "no run named %r in %s (found: %s)"
                % (name, self.store.root,
                   ", ".join(sorted(self.runs)) or "no RunManifest blocks at all"))
        return run

    # -- index ----------------------------------------------------------
    @property
    def index(self):
        if self._index is None and self._index_error is None:
            try:
                self._index = cas.Index.open(os.path.join(self.root, "cache"))
            except Exception as exc:
                self._index_error = exc
        if self._index is None:
            raise cas.GateError("index unavailable: %s" % self._index_error)
        return self._index

    # -- the bytes the pipeline produced --------------------------------
    def work_files(self, launch, name_pattern):
        """Every path under <launch>/work matching a filename glob."""
        root = os.path.join(self.root, launch, "work")
        matcher = re.compile(_glob_to_regex(name_pattern))
        out = []
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                if matcher.match(name):
                    out.append(os.path.join(dirpath, name))
        return sorted(out)

    def work_dirs(self, launch, name):
        root = os.path.join(self.root, launch, "work")
        out = []
        for dirpath, dirnames, _filenames in os.walk(root):
            for d in dirnames:
                if d == name:
                    out.append(os.path.join(dirpath, d))
        return sorted(out)

    def read_snapshot(self, filename):
        """The set of block cids recorded in a `find blocks -type f` snapshot."""
        path = os.path.join(self.root, filename)
        if not os.path.isfile(path):
            raise cas.GateError("no snapshot at %s (gate.sh writes it)" % path)
        cids = set()
        with open(path) as fh:
            for line in fh:
                name = os.path.basename(line.strip())
                if cas.is_cid(name):
                    cids.add(name)
        return cids


def _glob_to_regex(pattern):
    out = ["^"]
    for ch in pattern:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    return "".join(out)


# --------------------------------------------------------------------------
# Assertion 1
# --------------------------------------------------------------------------

@assertion(1, "identical bytes, one address, three items")
def assert_one(gate):
    paths = gate.work_files("pipeline-a", "*.stats")
    if len(paths) < 3:
        return FAIL, ("expected at least 3 *.stats under %s/pipeline-a/work, "
                      "found %d: %s" % (gate.root, len(paths), paths))
    addresses = {}
    for path in paths:
        addresses.setdefault(cas.cid_of_file(path), []).append(path)
    if len(addresses) != 1:
        return FAIL, ("the .stats files are not byte-identical: %d addresses %s"
                      % (len(addresses),
                         {c: [os.path.relpath(p, gate.root) for p in v]
                          for c, v in addresses.items()}))
    content_cid = next(iter(addresses))
    problems = []
    notes = ["content %s from %d files" % (content_cid, len(paths))]

    block_path = gate.store.block_path(content_cid)
    if not os.path.isfile(block_path):
        problems.append("expected one block at %s, no such file" % block_path)
    else:
        gate.store.read(content_cid)   # verifies the address
        notes.append("block present and verified")

    cold = gate.run("cold")
    try:
        rows = gate.index.query(
            "SELECT item_cid, filename FROM producer "
            "WHERE content_cid = ? AND completion_cid = ?",
            (content_cid, cold.completion_cid))
        items = sorted({r["item_cid"] for r in rows})
        if len(rows) < 3 or len(items) != 3:
            problems.append(
                "expected >= 3 producer rows over 3 distinct item_cids for "
                "content %s in run cold (%s); found %d rows over %d items %s"
                % (content_cid, cold.completion_cid, len(rows), len(items), items))
        else:
            notes.append("%d producer rows, 3 items" % len(rows))
    except Exception as exc:
        problems.append("producer rows unavailable: %s" % exc)

    fingerprints = {}
    for key, spec in gate.store.nf_records("FileOutput"):
        if not key.endswith(".stats"):
            continue
        if cold.nf_run_hash and spec.get("workflowRun") != "lid://%s" % cold.nf_run_hash:
            continue
        value = ((spec.get("checksum") or {}).get("value"))
        fingerprints.setdefault(value, []).append(key)
    if len(fingerprints) < 3:
        problems.append(
            "expected 3 different Nextflow checksum.value over the .stats "
            "FileOutput records under %s/nf for run cold; found %d: %s"
            % (gate.store.root, len(fingerprints), fingerprints))
    else:
        notes.append("%d distinct nextflow fingerprints" % len(fingerprints))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, "; ".join(notes)


# --------------------------------------------------------------------------
# Assertion 2
# --------------------------------------------------------------------------

@assertion(2, "second run loses no record, duplicates no content")
def assert_two(gate):
    after_cold = gate.read_snapshot("blocks-after-cold.txt")
    after_again = gate.read_snapshot("blocks-after-again.txt")
    problems = []

    lost = sorted(after_cold - after_again)
    if lost:
        problems.append("%d block(s) present after cold and gone after again: %s"
                        % (len(lost), lost[:5]))

    raw_cold = {c for c in after_cold if cas.cid_codec(c) == cas.RAW}
    raw_again = {c for c in after_again if cas.cid_codec(c) == cas.RAW}
    new_raw = sorted(raw_again - raw_cold)
    if new_raw:
        problems.append(
            "run again wrote %d new content block(s), expected 0 (identical "
            "bytes must re-address): %s" % (len(new_raw), new_raw[:5]))

    unverified = []
    for cid, path in gate.store.blocks("raw"):
        try:
            gate.store.read(cid)          # re-hashes: the name must be the address
        except Exception as exc:
            unverified.append("%s: %s" % (path, exc))
    if unverified:
        problems.append("%d content block(s) do not hash to the address they are "
                        "filed under: %s" % (len(unverified), unverified[:3]))

    cold_items = gate.run("cold").item_cids(gate)
    again_items = gate.run("again").item_cids(gate)
    if not cold_items:
        problems.append("run cold has no OutputItem links at all")
    elif cold_items != again_items:
        problems.append(
            "OutputItem addresses differ between cold and again: only in cold %s; "
            "only in again %s" % (sorted(cold_items - again_items)[:5],
                                  sorted(again_items - cold_items)[:5]))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("%d blocks after cold, %d after again, %d raw unchanged, "
                  "%d identical OutputItems"
                  % (len(after_cold), len(after_again), len(raw_cold),
                     len(cold_items)))


# --------------------------------------------------------------------------
# Assertion 3
# --------------------------------------------------------------------------

@assertion(3, "failed run is marked failed and is not latest")
def assert_three(gate):
    run = gate.run("fail")
    if not run.completion:
        return FAIL, ("run fail has a RunManifest %s but no RunCompletion block; "
                      "a failed run must still get one" % run.manifest_cid)
    problems = []
    status = run.completion.get("status")
    if status != "failed":
        problems.append("RunCompletion %s: expected status 'failed', found %r"
                        % (run.completion_cid, status))
    if run.completion.get("possibly_incomplete") is not True:
        problems.append("RunCompletion %s: expected possibly_incomplete true, "
                        "found %r" % (run.completion_cid,
                                      run.completion.get("possibly_incomplete")))

    counts = {name: len(block.get("items") or [])
              for name, (_c, block) in run.collections(gate).items()}
    pipeline = (run.manifest or {}).get("pipeline")
    if pipeline != PIPELINE_IDENTITY:
        problems.append("RunManifest %s records pipeline %r; gate.config sets "
                        "manifest.name = %r, and DESIGN section 6 takes the "
                        "first non-null of cas.pipeline, manifest.name, "
                        "projectName" % (run.manifest_cid, pipeline,
                                         PIPELINE_IDENTITY))

    latest, how = _latest_successful(gate, pipeline)
    if latest is None:
        problems.append("could not determine the latest successful run for "
                        "pipeline %r (%s)" % (pipeline, how))
    elif latest == run.completion_cid:
        problems.append("latest successful run for pipeline %r is the failed run "
                        "%s (via %s)" % (pipeline, latest, how))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("status=failed, possibly_incomplete=true, partial collections "
                  "%s, latest successful is %s (via %s)"
                  % (counts, (latest or "")[:16] + "...", how))


def _latest_successful(gate, pipeline):
    try:
        rows = gate.index.query(
            "SELECT completion_cid FROM run WHERE pipeline = ? AND "
            "status = 'succeeded' AND possibly_incomplete = 0 "
            "ORDER BY finished_at DESC LIMIT 1", (pipeline,))
        if rows:
            return rows[0]["completion_cid"], "index run table"
    except Exception as exc:
        del exc
    for _rts, cid in gate.store.run_log():          # newest first
        block = gate.block(cid)
        if not isinstance(block, dict):
            continue
        if block.get("status") == "succeeded" and not block.get("possibly_incomplete"):
            manifest = gate.block(block["run"].text) if isinstance(
                block.get("run"), cas.Cid) else None
            if manifest is None or manifest.get("pipeline") == pipeline:
                return cid, "store run log"
    return None, "neither the index nor the run log named one"


# --------------------------------------------------------------------------
# Assertion 4
# --------------------------------------------------------------------------

@assertion(4, "resumed run has a complete output layer")
def assert_four_outputs(gate):
    run = gate.run("resumed")
    collections = run.collections(gate)
    if not collections:
        return FAIL, ("run resumed has no OutputCollection blocks (completion %s)"
                      % run.completion_cid)
    counts = {name: len(block.get("items") or [])
              for name, (_c, block) in collections.items()}
    wrong = {name: n for name, n in counts.items() if n != 3}
    if wrong:
        return FAIL, ("every collection of the resumed run must hold 3 items; "
                      "found %s (all: %s)" % (wrong, counts))
    return PASS, "%d collections, all with 3 items: %s" % (len(counts), sorted(counts))


@assertion(4, "resumed run has a populated task layer (onTaskCached)")
def assert_four_tasks(gate):
    run = gate.run("resumed")
    if not run.nf_run_hash:
        return FAIL, "run resumed has no nf_run_hash in its RunManifest %s" % run.manifest_cid
    lid = "lid://%s" % run.nf_run_hash
    records = [key for key, spec in gate.store.nf_records("TaskRun")
               if spec.get("workflowRun") == lid]
    if len(records) != 15:
        return FAIL, ("expected 15 TaskRun records naming workflowRun %s under "
                      "%s/nf (7 executed + 8 cached); found %d. Until the plugin "
                      "implements onTaskCached the cached tasks leave no record: "
                      "measured 7 on released Nextflow (issue 17)."
                      % (lid, gate.store.root, len(records)))
    return PASS, "15 TaskRun records name %s" % lid


# --------------------------------------------------------------------------
# Assertion 5
# --------------------------------------------------------------------------

@assertion(5, "published directory matches an independent walk")
def assert_five(gate):
    manifest_cid = _qc_manifest_cid(gate, "A")
    if manifest_cid is None:
        return FAIL, ("no DirectoryManifest address recorded for qc sample A: "
                      "neither the coords pointer %s/coords/qc/A/A_qc nor the qc "
                      "OutputCollection of run cold names one"
                      % gate.store.root)
    dirs = gate.work_dirs("pipeline-a", "A_qc")
    if not dirs:
        return FAIL, ("no A_qc directory under %s/pipeline-a/work to walk"
                      % gate.root)
    problems = _compare_manifest(gate, manifest_cid, dirs[0], "A_qc")
    if problems:
        return FAIL, ("manifest %s vs %s: %s"
                      % (manifest_cid, os.path.relpath(dirs[0], gate.root),
                         "; ".join(problems[:8])))
    block = gate.store.read_block(manifest_cid)
    names = [e.get("name") for e in block.get("entries") or []]
    return PASS, ("%s matches %s entry for entry (%s); alias.txt is a symlink to "
                  "summary.txt" % (manifest_cid[:16] + "...",
                                   os.path.relpath(dirs[0], gate.root), names))


def _qc_manifest_cid(gate, sample):
    pointer = gate.store.coords_pointer("qc/%s/%s_qc" % (sample, sample))
    if pointer:
        match = re.match(r"^cas://([^/]+)", pointer)
        if match and cas.is_cid(match.group(1)):
            return match.group(1)
    try:
        run = gate.run("cold")
    except cas.GateError:
        return None
    entry = run.collections(gate).get("qc")
    if not entry:
        return None
    for link in entry[1].get("items") or []:
        if link is None:
            continue
        item = gate.block(link.text)
        for leaf in _leaves(item.get("value")):
            address = leaf.get("address")
            if isinstance(address, cas.Cid) and address.codec == cas.DAG_CBOR:
                if leaf.get("name") in (None, "%s_qc" % sample):
                    return address.text
    return None


def _leaves(value):
    if isinstance(value, dict):
        if value.get("kind") == "Leaf":
            yield value
            return
        for element in value.values():
            for leaf in _leaves(element):
                yield leaf
    elif isinstance(value, list):
        for element in value:
            for leaf in _leaves(element):
                yield leaf


def _compare_manifest(gate, manifest_cid, dirpath, label, depth=0):
    """Walk dirpath ourselves and diff it against the recorded manifest."""
    problems = []
    if depth > 64:
        return ["depth limit 64 exceeded at %s" % label]
    try:
        block = gate.store.read_block(manifest_cid)
    except Exception as exc:
        return ["%s: cannot read manifest %s: %s" % (label, manifest_cid, exc)]
    if block.get("kind") != "DirectoryManifest":
        return ["%s: block %s has kind %r, expected DirectoryManifest"
                % (label, manifest_cid, block.get("kind"))]

    recorded = {}
    order = []
    for entry in block.get("entries") or []:
        recorded[entry.get("name")] = entry
        order.append(entry.get("name"))
    if order != sorted(order, key=lambda n: n.encode("utf-8")):
        problems.append("%s: entries are not sorted by the UTF-8 bytes of name: %s"
                        % (label, order))

    walked = _walk(dirpath)
    for name in sorted(set(walked) | set(recorded)):
        want = walked.get(name)
        got = recorded.get(name)
        if want is None:
            problems.append("%s/%s: in the manifest but not on disk" % (label, name))
            continue
        if got is None:
            problems.append("%s/%s: on disk but not in the manifest" % (label, name))
            continue
        for field in ("mode", "size", "target"):
            if want[field] != got.get(field):
                problems.append("%s/%s: %s expected %r, manifest says %r"
                                % (label, name, field, want[field], got.get(field)))
        address = got.get("address")
        address = address.text if isinstance(address, cas.Cid) else address
        if want["mode"] in ("regular", "executable"):
            if address != want["address"]:
                problems.append("%s/%s: address expected %s (sha256 of the file), "
                                "manifest says %s" % (label, name, want["address"],
                                                      address))
        elif want["mode"] == "directory":
            if not cas.is_cid(address or ""):
                problems.append("%s/%s: directory entry needs a manifest address, "
                                "found %r" % (label, name, address))
            else:
                problems.extend(_compare_manifest(
                    gate, address, os.path.join(dirpath, name),
                    "%s/%s" % (label, name), depth + 1))
        elif address is not None:
            problems.append("%s/%s: %s entry must have address null, found %s"
                            % (label, name, want["mode"], address))
    return problems


def _walk(dirpath):
    """{name: entry} for one directory, by the rules of DESIGN section 6."""
    out = {}
    for name in sorted(os.listdir(dirpath), key=lambda n: n.encode("utf-8")):
        full = os.path.join(dirpath, name)
        if os.path.islink(full):
            target = os.readlink(full)
            resolved = os.path.normpath(os.path.join(dirpath, target))
            inside = (not os.path.isabs(target)
                      and not os.path.relpath(resolved, dirpath).startswith(".."))
            if inside:
                out[name] = {"mode": "symlink", "size": len(target.encode()),
                             "address": None, "target": target}
                continue
            if not os.path.exists(full):
                out[name] = {"mode": "unresolvable", "size": len(target.encode()),
                             "address": None, "target": target}
                continue
        if os.path.isdir(full):
            out[name] = {"mode": "directory", "size": 0, "address": None,
                         "target": None}
        else:
            mode = "executable" if os.stat(full).st_mode & 0o111 else "regular"
            out[name] = {"mode": mode, "size": os.path.getsize(full),
                         "address": cas.cid_of_file(full), "target": None}
    return out


# --------------------------------------------------------------------------
# Assertions 6 and 7: the consumer pipeline
# --------------------------------------------------------------------------

def _consumer_hashes(gate):
    """{tag: sha256 hex} read out of the consumer's own cas:// store."""
    out = {}
    for rel in gate.store_out.coords_paths():
        if not rel.startswith("hashes/"):
            continue
        pointer = gate.store_out.coords_pointer(rel)
        match = re.match(r"^cas://([^/]+)", pointer or "")
        if not match or not cas.is_cid(match.group(1)):
            raise cas.GateError("coords/%s does not hold a Store URI: %r"
                                % (rel, pointer))
        text = gate.store_out.read(match.group(1)).decode("utf-8", "replace")
        digest = re.match(r"\s*([0-9a-f]{64})", text)
        if not digest:
            raise cas.GateError("coords/%s resolves to %r, which is not "
                                "sha256sum output" % (rel, text[:80]))
        out[rel.split("/")[1]] = digest.group(1)
    return out


def _bam_sha256(gate, sample):
    paths = gate.work_files("pipeline-a", "%s.bam" % sample)
    if not paths:
        raise cas.GateError("no %s.bam under %s/pipeline-a/work"
                            % (sample, gate.root))
    digests = {cas.sha256_of_file(p) for p in paths}
    if len(digests) != 1:
        raise cas.GateError("%s.bam differs between work directories: %s"
                            % (sample, digests))
    return next(iter(digests))


@assertion(6, "lid:// and cas:// references stage into a second pipeline",
           online_only=True)
def assert_six(gate):
    hashes = _consumer_hashes(gate)
    if not hashes:
        return FAIL, ("the consumer published nothing under hashes/ into %s; "
                      "see %s/logs/consumer/" % (gate.store_out.root, gate.root))
    expected = _bam_sha256(gate, "A")
    problems = []
    for tag in ("lid", "cas"):
        got = hashes.get(tag)
        if got is None:
            problems.append("no hashes/%s output in %s (found %s)"
                            % (tag, gate.store_out.root, sorted(hashes)))
        elif got != expected:
            problems.append("hashes/%s staged bytes hashing to %s, expected %s "
                            "(sha256 of A.bam in pipeline-a/work)"
                            % (tag, got, expected))
    if problems:
        return FAIL, ("; ".join(problems) + ". Not covered in the skeleton: the "
                      "run-rooted cas://<runCid>/aligned/A/A.bam form and a glob "
                      "over a manifest.")
    return PASS, ("lid:// and cas:// both staged bytes hashing to %s; the "
                  "run-rooted form and the manifest glob are not in the skeleton"
                  % expected)


@assertion(7, "fromStore where sample == 'B' returns exactly one item",
           online_only=True)
def assert_seven(gate):
    hashes = _consumer_hashes(gate)
    got = hashes.get("fromstore")
    expected = _bam_sha256(gate, "B")
    problems = []
    if got is None:
        problems.append("no hashes/fromstore output in %s (found %s); "
                        "channel.fromStore(where: [sample: 'B']) emitted nothing"
                        % (gate.store_out.root, sorted(hashes)))
    elif got != expected:
        problems.append("fromStore staged bytes hashing to %s, expected %s "
                        "(sha256 of B.bam in pipeline-a/work)" % (got, expected))

    count = _fromstore_item_count(gate)
    if count is not None and count != 1:
        problems.append("the index says %d aligned items have sample == 'B' in "
                        "run cold, expected exactly 1" % count)

    lineage = os.path.join(gate.root, "logs", "consumer", "lineage-find.txt")
    if os.path.isfile(lineage):
        with open(lineage) as fh:
            text = fh.read()
        note = ("`nextflow lineage find` returned %d line(s)"
                % len(text.strip().splitlines()))
        if "ERROR" in text or "Exception" in text:
            problems.append("`nextflow lineage find` errored: %s"
                            % text.strip().splitlines()[:2])
    else:
        note = ("channel.fromLineage not exercised: no %s (gate.sh writes it "
                "when `nextflow lineage find` is applicable)" % lineage)

    if problems:
        return FAIL, "; ".join(problems) + ". " + note
    return PASS, "one item, staged bytes hash to %s. %s" % (expected, note)


def _fromstore_item_count(gate):
    try:
        run = gate.run("cold")
        collection = run.collections(gate).get("aligned")
        if not collection:
            return None
        cids = [l.text for l in collection[1].get("items") or [] if l is not None]
        if not cids:
            return None
        marks = ",".join("?" * len(cids))
        rows = gate.index.query(
            "SELECT DISTINCT item_cid FROM item_attr WHERE path = 'sample' "
            "AND value = 'B' AND item_cid IN (%s)" % marks, tuple(cids))
        return len(rows)
    except Exception:
        return None


# --------------------------------------------------------------------------
# Assertion 10
# --------------------------------------------------------------------------

@assertion(10, "two launch directories, identical addresses, nothing local")
def assert_ten(gate):
    cold = gate.run("cold")
    elsewhere = gate.run("elsewhere")
    problems = []

    a_items, a_manifests = _closure(gate, cold)
    b_items, b_manifests = _closure(gate, elsewhere)
    if not a_items:
        problems.append("run cold has no OutputItems")
    if a_items != b_items:
        problems.append("OutputItem addresses differ between launch directories: "
                        "only in pipeline-a %s; only in pipeline-b %s"
                        % (sorted(a_items - b_items)[:5],
                           sorted(b_items - a_items)[:5]))
    if a_manifests != b_manifests:
        problems.append("DirectoryManifest addresses differ between launch "
                        "directories: only in pipeline-a %s; only in pipeline-b %s"
                        % (sorted(a_manifests - b_manifests)[:5],
                           sorted(b_manifests - a_manifests)[:5]))

    needles = {"the GATE_ROOT path": gate.root}
    user = os.environ.get("USER")
    if user:
        needles["the OS user name"] = user
    leaks = []
    for cid, block in gate.blocks.items():
        kind = block.get("kind") if isinstance(block, dict) else None
        subject = block
        if kind == "RunManifest":
            # DESIGN section 6 has RunManifest carry session.params and
            # session.config verbatim; everything else in it must be portable.
            subject = {k: v for k, v in block.items()
                       if k not in ("params", "config")}
        for what, needle in needles.items():
            where = _find_string(subject, needle)
            if where:
                leaks.append("%s block %s contains %s at %s"
                             % (kind, cid[:16] + "...", what, where))
    if leaks:
        problems.append("; ".join(sorted(leaks)[:5]))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("%d OutputItem and %d DirectoryManifest addresses identical "
                  "across pipeline-a and pipeline-b; no block names the launch "
                  "path or %r (RunManifest params/config exempt per DESIGN 6)"
                  % (len(a_items), len(a_manifests), user))


def _closure(gate, run):
    items = run.item_cids(gate)
    manifests = set()
    pending = list(items)
    seen = set()
    while pending:
        cid = pending.pop()
        if cid in seen:
            continue
        seen.add(cid)
        block = gate.block(cid)
        if not isinstance(block, dict):
            continue
        if block.get("kind") == "DirectoryManifest":
            manifests.add(cid)
            for entry in block.get("entries") or []:
                address = entry.get("address")
                if isinstance(address, cas.Cid) and address.codec == cas.DAG_CBOR:
                    pending.append(address.text)
        else:
            for leaf in _leaves(block.get("value")):
                address = leaf.get("address")
                if isinstance(address, cas.Cid) and address.codec == cas.DAG_CBOR:
                    pending.append(address.text)
    return items, manifests


def _find_string(value, needle, path="$"):
    if isinstance(value, str):
        return path if needle in value else None
    if isinstance(value, dict):
        for key in sorted(value):
            if needle in key:
                return "%s.%s (key)" % (path, key)
            found = _find_string(value[key], needle, "%s.%s" % (path, key))
            if found:
                return found
    elif isinstance(value, list):
        for i, element in enumerate(value):
            found = _find_string(element, needle, "%s[%d]" % (path, i))
            if found:
                return found
    return None


# --------------------------------------------------------------------------
# Out of the Walking Skeleton: reported, never run
# --------------------------------------------------------------------------

NOT_IN_SKELETON = [
    (8, "Input Set from four consumption shapes",
     "A second pipeline consuming the first run's outputs four ways, a "
     "projection symlink, a pointer file, a run-rooted reference and a copied "
     "file, records an Input Set whose entries resolve to the first run's "
     "addresses, the copied file recovered by hashing; a typed process input "
     "appears in it."),
    (9, "sweep and trash",
     "A dry-run sweep immediately after a run reports an empty dead set; a real "
     "sweep after deleting a default projection removes exactly that run's "
     "unshared content and none of its metadata."),
    (11, "Fusion node-side addressing",
     "Under Fusion, published files carry provider `fusion-node`, "
     "`.command.cas` verifies with `sha256sum -c`, and recorded addresses equal "
     "hashes the test computes."),
    (12, "cloud executor publish",
     "A cloud-executor publish to `cas://` arrives with lineage records that "
     "reference it; `getBashLib` and `getUploadCmd` are exercised."),
]


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def wrap(text, width, indent):
    words, lines, line = text.split(), [], ""
    for word in words:
        if line and len(line) + 1 + len(word) > width:
            lines.append(line)
            line = word
        else:
            line = (line + " " + word).strip()
    if line:
        lines.append(line)
    return ("\n" + " " * indent).join(lines)


def emit_refs(gate):
    """Print shell assignments gate.sh needs but can only learn from the store.

    The Pipeline Identity, one lid:// reference and one cas:// reference, all
    read out of the producer's store after it has run.
    """
    pipeline = lid = uri = ""
    try:
        cold = gate.run("cold")
        pipeline = (cold.manifest or {}).get("pipeline") or ""
        if cold.nf_run_hash:
            lid = "lid://%s/aligned/A/A.bam" % cold.nf_run_hash
    except Exception as exc:
        sys.stderr.write("refs: %s\n" % exc)
    pointer = gate.store.coords_pointer("aligned/A/A.bam")
    if pointer and pointer.startswith("cas://"):
        uri = pointer
    elif pointer:
        sys.stderr.write("refs: coords/aligned/A/A.bam holds %r\n" % pointer)
    else:
        sys.stderr.write("refs: no coords pointer at %s/coords/aligned/A/A.bam\n"
                         % gate.store.root)
    print("GATE_PIPELINE=%s" % _shquote(pipeline))
    print("GATE_LID=%s" % _shquote(lid))
    print("GATE_CAS=%s" % _shquote(uri))
    return 0


def _shquote(text):
    return "'" + text.replace("'", "'\\''") + "'"


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("--")]
    flags = {a for a in argv[1:] if a.startswith("--")}
    if len(args) != 1 or flags - {"--offline", "--verbose", "--refs"}:
        sys.stderr.write(__doc__)
        return 2
    root = args[0]
    offline = "--offline" in flags
    if not os.path.isdir(root):
        sys.stderr.write("no such GATE_ROOT: %s\n" % root)
        return 2

    gate = Gate(root, offline)
    if "--refs" in flags:
        return emit_refs(gate)
    print("GATE_ROOT  %s" % gate.root)
    print("store      %s (%d blocks, %d dag-cbor decoded)"
          % (gate.store.root, sum(1 for _ in gate.store.blocks()), len(gate.blocks)))
    if gate.decode_errors:
        print("!! %d dag-cbor block(s) failed to decode:" % len(gate.decode_errors))
        for line in gate.decode_errors[:10]:
            print("   %s" % line)
    print("runs       %s" % (", ".join(sorted(gate.runs)) or "none"))
    print("")

    results = []
    for entry in REGISTRY:
        if offline and entry["online_only"]:
            results.append((SKIP, entry["number"], entry["title"],
                            "--offline: needs a real consumer run"))
            continue
        try:
            status, message = entry["fn"](gate)
        except Exception as exc:
            status = FAIL
            message = "%s: %s" % (type(exc).__name__, exc)
        results.append((status, entry["number"], entry["title"], message))
    for number, title, wording in NOT_IN_SKELETON:
        results.append((SKIP, number, title, "not in skeleton: " + wording))

    results.sort(key=lambda r: r[1])
    width = max(len(r[2]) for r in results)
    failures = 0
    for status, number, title, message in results:
        indent = 4 + 2 + 2 + 2 + width + 2
        print("%-4s  %2d  %-*s  %s"
              % (status, number, width, title, wrap(message, 92, indent)))
        if status == FAIL:
            failures += 1

    print("")
    counts = {}
    for status, _n, _t, _m in results:
        counts[status] = counts.get(status, 0) + 1
    print("%d PASS, %d FAIL, %d SKIP"
          % (counts.get(PASS, 0), counts.get(FAIL, 0), counts.get(SKIP, 0)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
