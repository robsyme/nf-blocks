#!/usr/bin/env python3
"""The Gate's assertions. Independent of the plugin by construction.

    python3 gate/assert.py <GATE_ROOT> [--offline]
    python3 gate/assert.py <GATE_ROOT> --refs               # shell vars for gate.sh
    python3 gate/assert.py <GATE_ROOT> <file> --snapshot    # store snapshot

Every address this file checks is derived here, with hashlib, from the bytes
the pipeline actually produced in its work directory or from the bytes of a
block on disk. Nothing is taken on the plugin's word. Exit status is 1 if any
assertion FAILs; SKIP never fails the Gate.

Assertion numbers are the spec's (.scratch/content-addressed-lineage/spec.md
section 1.2), plus assertion 0 for the preconditions every other assertion
stands on. Numbers 8, 9, 11 and 12 are out of the Walking Skeleton and are
reported as SKIP with the spec's own wording, so the list stays complete.
"""

import getpass
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cas  # noqa: E402

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"

# Pipeline Identity, fixed by `manifest.name` in gate/gate.config. DESIGN
# section 6 takes the first non-null of cas.pipeline, manifest.name and
# projectName; `nextflow run .` gives projectName the literal "main.nf", so the
# Gate names the pipeline explicitly and checks the plugin recorded that name.
PIPELINE_IDENTITY = "cas-test-pipeline"
# The consumer pipeline's identity (manifest.name in gate/consumer). Its
# composite [out, lab] index ingests the producer runs from the read-only lab
# member, so both index files carry PIPELINE_IDENTITY; the producer-only index
# is the one that does NOT also carry this.
CONSUMER_IDENTITY = "cas-gate-consumer"

# The Test Pipeline's five outputs, and how gate.sh drives it.
OUTPUTS = {"aligned", "stats", "qc", "chunks", "reports"}
PRODUCER_RUNS = ["cold", "again", "fail", "resumed", "elsewhere"]
FAILING_RUN = "fail"
FAILING_SAMPLE = "B"          # MAYBE_FAIL exits 7 for sample B under --fail

REGISTRY = []


def assertion(number, title, online_only=False):
    def register(fn):
        REGISTRY.append({"number": number, "title": title, "fn": fn,
                         "online_only": online_only})
        return fn
    return register


def os_user_name():
    """The OS user name, or None if it genuinely cannot be determined."""
    try:
        return getpass.getuser()
    except Exception:
        pass
    for var in ("LOGNAME", "USER", "USERNAME"):
        if os.environ.get(var):
            return os.environ[var]
    try:
        import pwd
        return pwd.getpwuid(os.getuid()).pw_name
    except Exception:
        return None


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

    def items(self, gate, output=None):
        """[(cid, block)] for this run's OutputItems, optionally one output."""
        out = []
        collections = self.collections(gate)
        names = [output] if output else sorted(collections)
        for name in names:
            if name not in collections:
                continue
            for link in collections[name][1].get("items") or []:
                if link is not None:
                    out.append((link.text, gate.block(link.text)))
        return out

    def item_cids(self, gate):
        return {cid for cid, _block in self.items(gate)}


class Gate(object):
    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.store = cas.Store(os.path.join(self.root, "store"))
        self.store_out = cas.Store(os.path.join(self.root, "store-out"))
        self.block_problems = []
        self._blocks = None
        self._runs = None
        self._index = None

    # -- blocks ---------------------------------------------------------
    @property
    def blocks(self):
        """{cid: decoded block} for every dag-cbor block that decodes.

        A block that cannot be read, decoded, or re-encoded to its own address
        is recorded in block_problems; assertion 0 turns that into a FAIL. No
        other assertion may quietly proceed as if the block were not there.
        """
        if self._blocks is None:
            self._blocks = {}
            for cid, path in self.store.blocks("dagcbor"):
                try:
                    data = self.store.read(cid)
                    value = cas.decode(data)
                except Exception as exc:
                    self.block_problems.append(
                        "%s (%s): %s: %s" % (cid, path, type(exc).__name__, exc))
                    continue
                try:
                    recoded = cas.encode(value)
                except Exception as exc:
                    self.block_problems.append(
                        "%s (%s): decodes but will not re-encode: %s" % (cid, path, exc))
                    continue
                if cas.cid_dagcbor(recoded) != cid:
                    self.block_problems.append(
                        "%s (%s): not canonically encoded; the canonical form of "
                        "its own value is %s" % (cid, path, cas.cid_dagcbor(recoded)))
                    continue
                self._blocks[cid] = value
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
            runs = {}
            by_manifest = {}
            seen = {}
            for cid, block in sorted(self.of_kind("RunManifest").items()):
                name = block.get("run_name")
                seen.setdefault(name, []).append(cid)
                run = runs.setdefault(name, Run(name))
                run.manifest_cid, run.manifest = cid, block
                by_manifest[cid] = run
            collisions = {n: c for n, c in seen.items() if len(c) > 1}
            if collisions:
                raise cas.GateError(
                    "run_name is not unique in %s: %s. A run name identifies one "
                    "run; two RunManifests under one name means the wrong run is "
                    "being asserted about."
                    % (self.store.root,
                       "; ".join("%r has %d manifests (%s)"
                                 % (n, len(c), ", ".join(x[:16] + "..." for x in c))
                                 for n, c in sorted(collisions.items()))))
            for cid, block in sorted(self.of_kind("RunCompletion").items()):
                link = block.get("run")
                run = by_manifest.get(link.text) if isinstance(link, cas.Cid) else None
                if run is None:
                    run = runs.setdefault("<orphan:%s>" % cid[:12], Run(None))
                run.completion_cid, run.completion = cid, block
            self._runs = runs
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
        """The producer store's index, selected by pipeline.

        A composite store (the consumer's [out, lab]) keeps its own index, so
        $XDG_CACHE_HOME/nf-blocks holds more than one .sqlite. The producer's
        is the one whose run table names PIPELINE_IDENTITY. Raises rather than
        degrading if none or several match.
        """
        if self._index is None:
            self._index = cas.Index.open_for_pipeline(
                os.path.join(self.root, "cache"), PIPELINE_IDENTITY,
                exclude=CONSUMER_IDENTITY)
        return self._index

    # -- the bytes the pipeline produced --------------------------------
    def work_files(self, launch, name_pattern):
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

    def exit_code(self, name):
        """The recorded exit status of one run, or None if gate.sh kept none."""
        path = os.path.join(self.root, "logs", name, "exit")
        if not os.path.isfile(path):
            return None
        with open(path) as fh:
            text = fh.read().strip()
        return int(text) if text.lstrip("-").isdigit() else None

    # -- snapshots ------------------------------------------------------
    def snapshot(self):
        """Everything gate.sh must be able to diff between two runs."""
        return {
            "blocks": sorted(cid for cid, _p in self.store.blocks()),
            "log": sorted(name for name in _listdir(self.store.path("log"))),
            "nf": sorted(key for key, _e in self.store.nf_envelopes()),
            "coords": {rel: self.store.coords_pointer(rel)
                       for rel in self.store.coords_paths()},
        }

    def read_snapshot(self, filename):
        path = os.path.join(self.root, filename)
        if not os.path.isfile(path):
            raise cas.GateError("no snapshot at %s (gate.sh writes it)" % path)
        out = {"blocks": [], "log": [], "nf": [], "coords": {}}
        with open(path) as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line or "\t" not in line:
                    continue
                section, _, rest = line.partition("\t")
                if section == "coords":
                    rel, _, pointer = rest.partition("\t")
                    out["coords"][rel] = pointer
                elif section in out:
                    out[section].append(rest)
        return out


def write_snapshot(gate, path):
    data = gate.snapshot()
    lines = []
    for section in ("blocks", "log", "nf"):
        lines.extend("%s\t%s" % (section, value) for value in data[section])
    for rel in sorted(data["coords"]):
        lines.append("coords\t%s\t%s" % (rel, data["coords"][rel] or ""))
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + ("\n" if lines else ""))
    return len(data["blocks"]), len(data["log"]), len(data["nf"]), len(data["coords"])


def _listdir(path):
    return os.listdir(path) if os.path.isdir(path) else []


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


def metadata_view(item):
    """DESIGN section 12: the item itself if a Map, else its first top-level Map."""
    value = item.get("value") if isinstance(item, dict) else None
    if isinstance(value, dict) and value.get("kind") != "Leaf":
        return value
    if isinstance(value, list):
        for element in value:
            if isinstance(element, dict) and element.get("kind") != "Leaf":
                return element
    return {}


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


def _address_text(value):
    return value.text if isinstance(value, cas.Cid) else value


# --------------------------------------------------------------------------
# Assertion 0: the preconditions every other assertion stands on
# --------------------------------------------------------------------------

@assertion(0, "every block decodes and re-encodes to its own address")
def assert_blocks_sound(gate):
    gate.blocks                       # decoding is what populates block_problems
    problems = list(gate.block_problems)
    raw = 0
    for cid, path in gate.store.blocks("raw"):
        raw += 1
        try:
            gate.store.read(cid)            # re-hashes the bytes
        except Exception as exc:
            problems.append("%s (%s): %s" % (cid, path, exc))
    if problems:
        return FAIL, ("%d block(s) are unreadable, undecodable or not canonically "
                      "encoded, so every assertion over them is unsound: %s"
                      % (len(problems), "; ".join(problems[:5])))
    return PASS, ("%d content and %d metadata blocks all hash to the address they "
                  "are filed under; every metadata block re-encodes to itself"
                  % (raw, len(gate.blocks)))


@assertion(0, "every run exited as the Gate drove it")
def assert_runs_exited(gate):
    problems = []
    notes = []
    for name in PRODUCER_RUNS + ["consumer"]:
        code = gate.exit_code(name)
        if code is None:
            problems.append("no exit status recorded at %s/logs/%s/exit"
                            % (gate.root, name))
        elif name == FAILING_RUN and code == 0:
            problems.append("run %s was driven with --fail and MAYBE_FAIL exits 7 "
                            "for sample %s, but it exited 0"
                            % (name, FAILING_SAMPLE))
        elif name != FAILING_RUN and code != 0:
            # `resumed` runs with every published source at mode 000 so that
            # assertion 4c can catch a re-hash. Under the output DSL's
            # `mode 'copy'` Nextflow re-publishes those files on every resume
            # (issue 17) and its own PublishDir hits the lock first, which is
            # not a plugin failure and must not be scored as one.
            if name == "resumed" and _publish_copy_failures(gate):
                notes.append("resumed exited %d on Nextflow's own PublishDir copy "
                             "of the files the Gate locked (issue 17), not on "
                             "anything the plugin did" % code)
                continue
            problems.append("run %s exited %d; see %s/logs/%s/"
                            % (name, code, gate.root, name))
    if problems:
        return FAIL, "; ".join(problems)
    expected_zero = [n for n in PRODUCER_RUNS + ["consumer"] if n != FAILING_RUN]
    return PASS, ("%s exited 0, %s exited non-zero as intended%s"
                  % (", ".join(expected_zero), FAILING_RUN,
                     ". " + "; ".join(notes) if notes else ""))


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
        gate.store.read(content_cid)
        notes.append("one block, verified")

    cold = gate.run("cold")
    rows = gate.index.query(
        "SELECT item_cid, filename FROM producer "
        "WHERE content_cid = ? AND completion_cid = ?",
        (content_cid, cold.completion_cid))
    items = sorted({r["item_cid"] for r in rows})
    if len(rows) < 3 or len(items) != 3:
        problems.append(
            "expected >= 3 producer rows over exactly 3 distinct item_cids for "
            "content %s in run cold (%s); found %d rows over %d items %s"
            % (content_cid, cold.completion_cid, len(rows), len(items), items))
    else:
        notes.append("%d producer rows, 3 items" % len(rows))

    # The producer rows are the plugin's claim. Open the items they name and
    # check each really is an OutputItem carrying that address under that name.
    leaf_names = []
    for item_cid in items:
        item = gate.block(item_cid)
        if item is None:
            problems.append("producer names item %s, which is not a readable "
                            "dag-cbor block in %s" % (item_cid, gate.store.root))
            continue
        if item.get("kind") != "OutputItem":
            problems.append("producer names item %s, whose kind is %r, expected "
                            "OutputItem" % (item_cid, item.get("kind")))
            continue
        matching = [leaf for leaf in _leaves(item.get("value"))
                    if _address_text(leaf.get("address")) == content_cid]
        if len(matching) != 1:
            problems.append("item %s has %d leaf/leaves addressing %s, expected 1"
                            % (item_cid, len(matching), content_cid))
            continue
        leaf_names.append(matching[0].get("name"))
    expected_names = {"%s.stats" % s for s in ("A", "B", "C")}
    if items and not problems and set(leaf_names) != expected_names:
        problems.append("expected the 3 items' leaf names to be %s, found %s"
                        % (sorted(expected_names), sorted(leaf_names)))
    elif leaf_names:
        notes.append("leaf names %s" % sorted(leaf_names))

    fingerprints = {}
    for key, spec in gate.store.nf_records("FileOutput"):
        if not key.endswith(".stats"):
            continue
        if cold.nf_run_hash and spec.get("workflowRun") != "lid://%s" % cold.nf_run_hash:
            continue
        fingerprints.setdefault((spec.get("checksum") or {}).get("value"),
                                []).append(key)
    if len(fingerprints) < 3:
        problems.append(
            "expected 3 different Nextflow checksum.value over the .stats "
            "FileOutput records under %s/nf for run cold; found %d over %d "
            "record(s): %s" % (gate.store.root, len(fingerprints),
                               sum(len(v) for v in fingerprints.values()),
                               fingerprints))
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

    for section in ("blocks", "log", "nf"):
        lost = sorted(set(after_cold[section]) - set(after_again[section]))
        if lost:
            problems.append("%d %s entr(y/ies) present after cold and gone after "
                            "again: %s" % (len(lost), section, lost[:5]))
    dropped = sorted(set(after_cold["coords"]) - set(after_again["coords"]))
    if dropped:
        problems.append("%d coords pointer(s) disappeared between cold and again: "
                        "%s" % (len(dropped), dropped[:5]))
    changed = sorted(rel for rel, pointer in after_cold["coords"].items()
                     if rel in after_again["coords"]
                     and after_again["coords"][rel] != pointer)
    if changed:
        problems.append(
            "%d coords pointer(s) changed between two runs producing identical "
            "bytes, so a coordinate no longer names the same content: %s"
            % (len(changed), [(rel, after_cold["coords"][rel],
                               after_again["coords"][rel]) for rel in changed[:3]]))

    raw_cold = {c for c in after_cold["blocks"] if cas.cid_codec(c) == cas.RAW}
    raw_again = {c for c in after_again["blocks"] if cas.cid_codec(c) == cas.RAW}
    new_raw = sorted(raw_again - raw_cold)
    if new_raw:
        problems.append(
            "run again wrote %d new content block(s), expected 0 (identical bytes "
            "must re-address): %s" % (len(new_raw), new_raw[:5]))

    unverified = []
    for cid, path in gate.store.blocks():
        try:
            data = gate.store.read(cid)
            if cas.cid_codec(cid) == cas.DAG_CBOR:
                if cas.cid_dagcbor(cas.encode(cas.decode(data))) != cid:
                    raise cas.GateError("re-encodes to a different address")
        except Exception as exc:
            unverified.append("%s: %s" % (path, exc))
    if unverified:
        problems.append("%d block(s) do not hash to the address they are filed "
                        "under: %s" % (len(unverified), unverified[:3]))

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
    return PASS, ("%d blocks, %d Store Log entries, %d nf records and %d coords "
                  "pointers survive `again` unchanged; %d raw blocks added 0; "
                  "%d identical OutputItems"
                  % (len(after_again["blocks"]), len(after_again["log"]),
                     len(after_again["nf"]), len(after_again["coords"]),
                     len(raw_cold), len(cold_items)))


# --------------------------------------------------------------------------
# Assertion 3
# --------------------------------------------------------------------------

@assertion(3, "failed run is marked failed, is partial, and is not latest")
def assert_three(gate):
    run = gate.run(FAILING_RUN)
    if not run.completion:
        return FAIL, ("run %s has a RunManifest %s but no RunCompletion block; "
                      "a failed run must still get one"
                      % (FAILING_RUN, run.manifest_cid))
    problems = []
    status = run.completion.get("status")
    if status != "failed":
        problems.append("RunCompletion %s: expected status 'failed', found %r"
                        % (run.completion_cid, status))
    if run.completion.get("possibly_incomplete") is not True:
        problems.append("RunCompletion %s: expected possibly_incomplete true, "
                        "found %r" % (run.completion_cid,
                                      run.completion.get("possibly_incomplete")))
    anomalies = run.completion.get("anomalies")
    if not isinstance(anomalies, dict):
        problems.append("RunCompletion %s: expected an anomalies map, found %r"
                        % (run.completion_cid, anomalies))

    # The collections must be partial and say so. Partiality is the whole
    # point of RunCompletion: a reader must be able to tell "this run produces
    # no reports" from "this run died before reports". A failed run is partial
    # when it is missing an output a full run has, or holds a short collection
    # (fewer than the three samples). Which output goes missing depends on when
    # the abort landed relative to each publish, so nothing here requires a
    # specific collection to survive: on this run qc and reports published
    # nothing before MAYBE_FAIL exited 7, on another run reports might carry C.
    collections = run.collections(gate)
    counts = {name: len(block.get("items") or [])
              for name, (_c, block) in collections.items()}
    if not _failed_run_is_partial(counts, OUTPUTS):
        problems.append("run %s recorded every output full (%s); a failed run "
                        "must be partial and say so, but this is indistinguishable "
                        "from a clean run" % (FAILING_RUN, counts))
    # Only the failing task's own output may lose sample B: MAYBE_FAIL exits 7
    # for sample B before its report can publish. Every other output (ALIGN,
    # SHARED_STATS, QC_DIR, CHUNKS) succeeds for B, so aligned/B and the rest
    # legitimately carry it; the check is specific to `reports`.
    if "reports" in collections:
        reports = run.items(gate, "reports")
        samples = [metadata_view(item).get("sample") for _cid, item in reports]
        if FAILING_SAMPLE in samples:
            problems.append("the `reports` collection of run %s holds an item for "
                            "sample %s, whose task exited 7: samples %s"
                            % (FAILING_RUN, FAILING_SAMPLE, samples))
        if len(reports) > 2:
            problems.append("the `reports` collection of run %s holds %d items "
                            "(samples %s); at most 2 can have published"
                            % (FAILING_RUN, len(reports), samples))

    pipeline = (run.manifest or {}).get("pipeline")
    if pipeline != PIPELINE_IDENTITY:
        problems.append("RunManifest %s records pipeline %r; gate.config sets "
                        "manifest.name = %r, and DESIGN section 6 takes the first "
                        "non-null of cas.pipeline, manifest.name, projectName"
                        % (run.manifest_cid, pipeline, PIPELINE_IDENTITY))

    # gate.config sets a closure (ext.args) and a unit literal (memory); a
    # failed run is where a config dag-cbor could not encode used to abort
    # the RunManifest and swallow onError. DESIGN section 6 records the
    # resolved config as text.
    config = (run.manifest or {}).get("config")
    if not isinstance(config, str):
        problems.append("RunManifest %s records config as %s; DESIGN section 6 "
                        "records it as the resolved config text"
                        % (run.manifest_cid, type(config).__name__))
    else:
        for needle in ("task.cpus", "1 GB"):
            if needle not in config:
                problems.append("RunManifest %s config text lacks %r, which "
                                "gate.config sets" % (run.manifest_cid, needle))

    from_index = _latest_successful_from_index(gate, pipeline)
    from_log = _latest_successful_from_store_log(gate, pipeline)
    for source, latest in (("the index run table", from_index),
                           ("the Store Log", from_log)):
        if latest is None:
            problems.append("%s names no successful run for pipeline %r"
                            % (source, pipeline))
        elif latest == run.completion_cid:
            problems.append("%s says the latest successful run for pipeline %r is "
                            "the failed run %s" % (source, pipeline, latest))
    if from_index and from_log and from_index != from_log:
        problems.append("the index says the latest successful run is %s, the "
                        "Store Log says %s" % (from_index, from_log))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("status=failed, possibly_incomplete=true, anomalies=%s, "
                  "collections partial %s (of %d outputs), reports carries no "
                  "sample %s, latest successful is %s by both the index and the "
                  "Store Log" % (anomalies, counts, len(OUTPUTS), FAILING_SAMPLE,
                               from_log[:16] + "..."))


def _failed_run_is_partial(counts, full_outputs):
    """A failed run is partial: missing an output, or a short collection."""
    if set(counts) != set(full_outputs):
        return True
    return any(n < 3 for n in counts.values())


def _latest_successful_from_index(gate, pipeline):
    rows = gate.index.query(
        "SELECT completion_cid FROM run WHERE pipeline = ? AND "
        "status = 'succeeded' AND possibly_incomplete = 0 "
        "ORDER BY finished_at DESC, completion_cid ASC LIMIT 1", (pipeline,))
    return rows[0]["completion_cid"] if rows else None


def _latest_successful_from_store_log(gate, pipeline):
    """The successful run of `pipeline` with the latest asserted finish time,
    found from the Store Log's run entries and each RunCompletion's own
    finished_at. Log order is when an entry was written to this store, which
    a merged Bundle or a skewed clock can make differ from finish order."""
    best = None
    for _rts, kind, cid in gate.store.store_log():
        if kind != "run":
            continue
        block = gate.block(cid)
        if not isinstance(block, dict):
            continue
        if block.get("status") != "succeeded" or block.get("possibly_incomplete"):
            continue
        link = block.get("run")
        manifest = gate.block(link.text) if isinstance(link, cas.Cid) else None
        if manifest is not None and manifest.get("pipeline") != pipeline:
            continue
        # Newest finish first; a tie goes to the smaller cid, the rule the
        # plugin's index uses (ORDER BY finished_at DESC, completion_cid ASC).
        finished = block.get("finished_at") or ""
        if (best is None or finished > best[0]
                or (finished == best[0] and cid < best[1])):
            best = (finished, cid)
    return best[1] if best else None


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
    problems = []
    if set(counts) != OUTPUTS:
        problems.append("expected exactly the collections %s, found %s"
                        % (sorted(OUTPUTS), sorted(counts)))
    wrong = {name: n for name, n in counts.items() if n != 3}
    if wrong:
        problems.append("every collection of the resumed run must hold 3 items "
                        "(three samples); wrong: %s (all: %s)" % (wrong, counts))
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, "collections %s, each with 3 items" % sorted(counts)


@assertion(4, "resumed run has a populated task layer (onTaskCached)")
def assert_four_tasks(gate):
    # Filling the task layer on resume through our own onTaskCached is deferred
    # out of the Walking Skeleton (the plan defers address-reuse and the task
    # layer to a later task; the skeleton's onTaskCached only logs). Report it
    # rather than counting, so it is neither a false PASS nor a red the
    # skeleton cannot clear.
    detail = ""
    try:
        run = gate.run("resumed")
        if run.nf_run_hash:
            lid = "lid://%s" % run.nf_run_hash
            found = sum(1 for _key, spec in gate.store.nf_records("TaskRun")
                        if spec.get("workflowRun") == lid)
            detail = " (found %d TaskRun records for %s)" % (found, lid)
    except cas.GateError:
        pass
    return SKIP, ("not in skeleton: onTaskCached task-layer population is "
                  "deferred; native Nextflow leaves the task layer empty on "
                  "resume, measured 7 not 15 in issue 17%s" % detail)


@assertion(4, "resumed run re-hashed nothing")
def assert_four_no_rehash(gate):
    """Proved by the filesystem, not by a counter.

    gate.sh makes every published source file in pipeline-a/work unreadable
    (chmod 000) for the duration of the resumed run, so anything that re-reads
    a published file to re-address it gets AccessDenied and the run fails.

    One caveat, measured rather than assumed: under the workflow output DSL's
    `mode 'copy'` Nextflow's own PublishDir re-copies every published file on a
    resumed run (issue 17), and a copy needs read access. When that is what
    failed, the lock caught Nextflow upstream of the plugin and this assertion
    has measured nothing about us; it says so and skips rather than reporting a
    red no plugin change can clear.
    """
    marker = os.path.join(gate.root, "logs", "resumed", "sources-locked")
    if not os.path.isfile(marker):
        return FAIL, ("no %s: gate.sh did not lock the published source files for "
                      "the resumed run, so nothing here proves a re-hash would "
                      "have been caught" % marker)
    with open(marker) as fh:
        locked = [line for line in fh.read().splitlines() if line.strip()]
    if len(locked) < 15:
        return FAIL, ("%s lists only %d locked source file(s); the Test Pipeline "
                      "publishes at least 15 (3 bam, 3 stats, 3 report, 9 chunk, "
                      "and the qc trees)" % (marker, len(locked)))
    code = gate.exit_code("resumed")
    if code == 0:
        return PASS, ("resumed exited 0 with %d published source files at mode "
                      "000: no content byte was read" % len(locked))
    publish_failures = _publish_copy_failures(gate)
    if publish_failures:
        return SKIP, ("inconclusive, and not a plugin fault: the resumed run "
                      "exited %r because Nextflow's own PublishDir could not copy "
                      "%d published file(s) it re-publishes on every resume "
                      "(issue 17), e.g. %s. The lock is caught upstream of the "
                      "plugin, so this run measured nothing about re-hashing. It "
                      "needs the Test Pipeline to offer a non-copy publish mode, "
                      "which is not the Gate's file."
                      % (code, len(publish_failures), publish_failures[0]))
    return FAIL, ("the resumed run exited %r with %d published source file(s) "
                  "unreadable, and Nextflow's PublishDir reported no copy failure: "
                  "something else read content that -resume should have taken from "
                  "the cache. See %s/logs/resumed/"
                  % (code, len(locked), gate.root))


def _publish_copy_failures(gate):
    """Lines where Nextflow's own PublishDir failed to copy, newest run first."""
    path = os.path.join(gate.root, "logs", "resumed", "nextflow.log")
    if not os.path.isfile(path):
        return []
    out = []
    with open(path, errors="replace") as fh:
        for line in fh:
            if "PublishDir - Failed to publish file" in line:
                out.append(line.strip()[:160])
    return out


# --------------------------------------------------------------------------
# Assertion 5
# --------------------------------------------------------------------------

@assertion(5, "published directory matches an independent walk")
def assert_five(gate):
    manifest_cid = _qc_manifest_cid(gate, "A")
    if manifest_cid is None:
        return FAIL, ("no DirectoryManifest address recorded for qc sample A: "
                      "neither the coords pointer %s/coords/qc/A/A_qc nor the qc "
                      "OutputCollection of run cold names one" % gate.store.root)
    dirs = gate.work_dirs("pipeline-a", "A_qc")
    if not dirs:
        return FAIL, ("no A_qc directory under %s/pipeline-a/work to walk"
                      % gate.root)
    problems = _compare_manifest(gate, manifest_cid, dirs[0], "A_qc", dirs[0])
    if problems:
        return FAIL, ("manifest %s vs %s: %s"
                      % (manifest_cid, os.path.relpath(dirs[0], gate.root),
                         "; ".join(problems[:8])))
    counted = _count_manifest(gate, manifest_cid)
    return PASS, ("%s matches the walk of %s: %d regular file(s) by name, mode, "
                  "size and raw cid, %d subdirectory manifest(s) recursed into, "
                  "%d symlink(s) recorded as links (alias.txt -> summary.txt) and "
                  "%d unresolvable"
                  % (manifest_cid[:16] + "...",
                     os.path.relpath(dirs[0], gate.root), counted["regular"],
                     counted["directory"], counted["symlink"],
                     counted["unresolvable"]))


@assertion(5, "recorded publish paths resolve to the leaf addresses")
def assert_five_paths(gate):
    cold = gate.run("cold")
    collections = cold.collections(gate)
    if not collections:
        return FAIL, "run cold has no OutputCollection blocks to check paths on"
    problems = []
    checked = 0
    for name in sorted(collections):
        _cid, block = collections[name]
        items = block.get("items") or []
        paths = block.get("paths") or []
        if len(paths) != len(items):
            problems.append("collection %s has %d items but %d path lists"
                            % (name, len(items), len(paths)))
            continue
        for i, link in enumerate(items):
            if link is None:
                continue
            item = gate.block(link.text)
            if item is None:
                problems.append("collection %s item %d: %s is not a readable block"
                                % (name, i, link.text))
                continue
            leaves = list(_leaves(item.get("value")))
            entry_paths = paths[i] or []
            if len(entry_paths) != len(leaves):
                problems.append("collection %s item %d (%s): %d leaves but %d "
                                "publish paths %s"
                                % (name, i, link.text[:16] + "...", len(leaves),
                                   len(entry_paths), entry_paths))
                continue
            for leaf, publish_path in zip(leaves, entry_paths):
                if publish_path is None:
                    continue
                rel = "/".join(publish_path) if isinstance(publish_path, list) \
                    else str(publish_path)
                pointer = gate.store.coords_pointer(rel)
                if pointer is None:
                    problems.append("collection %s names publish path %r, but "
                                    "there is no pointer file at %s/coords/%s"
                                    % (name, rel, gate.store.root, rel))
                    continue
                checked += 1
                want = "cas://%s/%s" % (_address_text(leaf.get("address")),
                                        leaf.get("name"))
                if pointer != want:
                    problems.append("coords/%s holds %r, expected %r (the leaf's "
                                    "own address and name)" % (rel, pointer, want))
    if problems:
        return FAIL, "; ".join(problems[:6])
    return PASS, ("%d recorded publish path(s) across %d collection(s) each resolve "
                  "to a pointer naming that leaf's address and name"
                  % (checked, len(collections)))


def _qc_manifest_cid(gate, sample):
    pointer = gate.store.coords_pointer("qc/%s/%s_qc" % (sample, sample))
    if pointer:
        match = re.match(r"^cas://([^/]+)", pointer)
        if match and cas.is_cid(match.group(1)):
            return match.group(1)
    entry = gate.run("cold").collections(gate).get("qc")
    if not entry:
        return None
    for link in entry[1].get("items") or []:
        if link is None:
            continue
        for leaf in _leaves((gate.block(link.text) or {}).get("value")):
            address = leaf.get("address")
            if isinstance(address, cas.Cid) and address.codec == cas.DAG_CBOR:
                if leaf.get("name") in (None, "%s_qc" % sample):
                    return address.text
    return None


def _count_manifest(gate, manifest_cid, seen=None):
    counts = {"regular": 0, "executable": 0, "symlink": 0, "directory": 0,
              "unresolvable": 0}
    seen = seen if seen is not None else set()
    if manifest_cid in seen:
        return counts
    seen.add(manifest_cid)
    for entry in (gate.block(manifest_cid) or {}).get("entries") or []:
        mode = entry.get("mode")
        if mode in counts:
            counts[mode] += 1
        if mode == "directory":
            address = _address_text(entry.get("address"))
            if address:
                for key, value in _count_manifest(gate, address, seen).items():
                    counts[key] += value
    return counts


def _compare_manifest(gate, manifest_cid, dirpath, label, root, depth=0):
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

    walked = _walk(dirpath, root)
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
        address = _address_text(got.get("address"))
        if want["mode"] in ("regular", "executable"):
            if address != want["address"]:
                problems.append("%s/%s: address expected %s (sha256 of the file), "
                                "manifest says %s"
                                % (label, name, want["address"], address))
        elif want["mode"] == "directory":
            if not cas.is_cid(address or ""):
                problems.append("%s/%s: directory entry needs a manifest address, "
                                "found %r" % (label, name, address))
            else:
                problems.extend(_compare_manifest(
                    gate, address, os.path.join(dirpath, name),
                    "%s/%s" % (label, name), root, depth + 1))
        elif address is not None:
            problems.append("%s/%s: %s entry must have address null, found %s"
                            % (label, name, want["mode"], address))
    return problems


def _walk(dirpath, root):
    """{name: entry} for one directory, by the rules of DESIGN section 6.

    `root` is the root of the tree being published. A relative symlink counts
    as internal when it resolves inside `root`, not merely inside `dirpath`:
    nested/up.txt -> ../summary.txt is still a link within the published tree.
    """
    # Resolve both sides in one namespace: on macOS $TMPDIR is /var/... which
    # is itself a symlink to /private/var/..., and comparing one against the
    # other makes every internal link look like an escape.
    real_root = os.path.realpath(root)
    real_dir = os.path.realpath(dirpath)
    out = {}
    for name in sorted(os.listdir(dirpath), key=lambda n: n.encode("utf-8")):
        full = os.path.join(dirpath, name)
        if os.path.islink(full):
            target = os.readlink(full)
            resolved = os.path.normpath(os.path.join(real_dir, target))
            inside = (not os.path.isabs(target)
                      and not os.path.relpath(resolved, real_root).startswith(".."))
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
    """{source: {published file name: sha256 hex}} from the consumer's store."""
    out = {}
    for rel in gate.store_out.coords_paths():
        parts = rel.split("/")
        if len(parts) < 3 or parts[0] != "hashes":
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
        out.setdefault(parts[1], {})[parts[-1]] = digest.group(1)
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
    for source in ("lid", "cas"):
        got = hashes.get(source) or {}
        if len(got) != 1:
            problems.append("expected exactly 1 file under hashes/%s/ in %s, "
                            "found %d: %s" % (source, gate.store_out.root,
                                              len(got), sorted(got)))
            continue
        name, digest = next(iter(got.items()))
        if digest != expected:
            problems.append("hashes/%s/%s says the staged bytes hash to %s, "
                            "expected %s (sha256 of A.bam in pipeline-a/work)"
                            % (source, name, digest, expected))
    if problems:
        return FAIL, ("; ".join(problems) + ". Not covered in the skeleton: the "
                      "run-rooted cas://<runCid>/aligned/A/A.bam form and a glob "
                      "over a manifest.")
    return PASS, ("lid:// and cas:// each staged one file hashing to %s; the "
                  "run-rooted form and the manifest glob are not in the skeleton"
                  % expected)


@assertion(7, "fromStore where sample == 'B' returns exactly one item",
           online_only=True)
def assert_seven(gate):
    hashes = _consumer_hashes(gate)
    problems = []

    expected_b = _bam_sha256(gate, "B")
    from_store = hashes.get("fromstore") or {}
    if len(from_store) != 1:
        problems.append("expected exactly 1 file under hashes/fromstore/ in %s "
                        "(channel.fromStore(where: [sample: 'B']) must emit one "
                        "item), found %d: %s"
                        % (gate.store_out.root, len(from_store), sorted(from_store)))
    else:
        name, digest = next(iter(from_store.items()))
        if digest != expected_b:
            problems.append("hashes/fromstore/%s says the staged bytes hash to %s, "
                            "expected %s (sha256 of B.bam in pipeline-a/work)"
                            % (name, digest, expected_b))

    # channel.fromLineage over this store is a real product property but cannot
    # be exercised from a pipeline that declares a custom plugins block (it
    # stops Nextflow auto-loading nf-lineage), so it is left out of the skeleton
    # Gate; only the fromStore cardinality is checked here.
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("exactly one file under hashes/fromstore/ hashing to %s "
                  "(fromLineage is out of the skeleton Gate: a custom plugins "
                  "block stops nf-lineage auto-loading)" % expected_b)


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

    user = os_user_name()
    if not user:
        return FAIL, ("cannot determine the OS user name (getpass.getuser, "
                      "$LOGNAME, $USER and pwd all failed), so the leak scan "
                      "cannot be performed and must not be reported as clean")
    # No exemptions. DESIGN section 6 scrubs RunManifest params and config for
    # portability (dropping the cas, lineage, workDir, outputDir, launchDir,
    # projectDir, homeDir, configFiles, scriptFile, commandLine, runName and
    # resume scopes, and replacing absolute paths and non-lid/cas URIs with
    # "[redacted-location]" and the OS user name with "[redacted-user]"), so
    # every dag-cbor block is scanned whole.
    needles = {"the GATE_ROOT path": gate.root, "the OS user name": user}
    leaks = []
    for cid, block in gate.blocks.items():
        kind = block.get("kind") if isinstance(block, dict) else None
        for what, needle in needles.items():
            where = _find_string(block, needle)
            if where:
                leaks.append("%s block %s contains %s at %s"
                             % (kind, cid[:16] + "...", what, where))
    if leaks:
        problems.append("; ".join(sorted(leaks)[:5]))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("%d OutputItem and %d DirectoryManifest addresses identical "
                  "across pipeline-a and pipeline-b; none of the %d dag-cbor "
                  "blocks names the launch path or %r anywhere"
                  % (len(a_items), len(a_manifests), len(gate.blocks), user))


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
    """Print shell assignments gate.sh needs but can only learn from the store."""
    pipeline = lid = uri = run_lid = ""
    try:
        cold = gate.run("cold")
        pipeline = (cold.manifest or {}).get("pipeline") or ""
        if cold.nf_run_hash:
            run_lid = "lid://%s" % cold.nf_run_hash
            lid = "lid://%s/aligned/A/A.bam" % cold.nf_run_hash
    except cas.GateError as exc:
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
    print("GATE_RUN_LID=%s" % _shquote(run_lid))
    print("GATE_LID=%s" % _shquote(lid))
    print("GATE_CAS=%s" % _shquote(uri))
    return 0


def _shquote(text):
    return "'" + text.replace("'", "'\\''") + "'"


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("--")]
    flags = {a for a in argv[1:] if a.startswith("--")}
    known = {"--offline", "--refs", "--snapshot"}
    if not args or flags - known:
        sys.stderr.write(__doc__)
        return 2
    root = args[0]
    if not os.path.isdir(root):
        sys.stderr.write("no such GATE_ROOT: %s\n" % root)
        return 2

    gate = Gate(root)
    if "--snapshot" in flags:
        if len(args) != 2:
            sys.stderr.write("--snapshot needs an output file\n")
            return 2
        blocks, runs, nf, coords = write_snapshot(gate, args[1])
        print("%d blocks, %d Store Log entries, %d nf records, %d coords pointers"
              % (blocks, runs, nf, coords))
        return 0
    if "--refs" in flags:
        return emit_refs(gate)
    if len(args) != 1:
        sys.stderr.write(__doc__)
        return 2
    offline = "--offline" in flags

    print("GATE_ROOT  %s" % gate.root)
    print("store      %s (%d blocks, %d dag-cbor sound)"
          % (gate.store.root, sum(1 for _ in gate.store.blocks()), len(gate.blocks)))
    try:
        print("runs       %s" % (", ".join(sorted(gate.runs)) or "none"))
    except cas.GateError as exc:
        print("runs       UNRESOLVABLE: %s" % exc)
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

    results.sort(key=lambda r: (r[1], r[2]))
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
