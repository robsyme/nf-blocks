#!/usr/bin/env python3
"""The Gate's assertions. Independent of the plugin by construction.

    python3 gate/assert.py <GATE_ROOT> [--offline]
    python3 gate/assert.py <GATE_ROOT> --refs               # shell vars for gate.sh
    python3 gate/assert.py <GATE_ROOT> <file> --snapshot    # store snapshot
    python3 gate/assert.py <GATE_ROOT> --seeding-before     # seeding.json for assertion 13
    python3 gate/assert.py <GATE_ROOT> --delete-consumer-cache
    python3 gate/assert.py <GATE_ROOT> --seeding-lock       # blocks to lock for consumer-seeded

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
import json
import os
import re
import sqlite3
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

# Milestone 5 (plan 2026-09-29): gate/outputs and gate/outputs-badindex, run into their
# own store-outputs and never touched by PRODUCER_RUNS, OUTPUTS or PIPELINE_IDENTITY
# above (assertions 14 to 16 only).
OUTPUTS_RUNS = ["outputs", "outputs-badindex"]

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
        """{run_name: Run}, assembled from the blocks alone. Delegates to
        _assemble_runs, the same run-name/RunCompletion assembly store-outputs'
        _run_of uses, so gate.store and store-outputs can never drift apart."""
        if self._runs is None:
            self._runs = _assemble_runs(self.blocks, self.store.root)
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
        """Every directory named `name` under <launch>/work, or with `name`
        None every task directory (work/<2 hex>/<rest of the hash>)."""
        root = os.path.join(self.root, launch, "work")
        out = []
        if name is None:
            for shard in _listdir(root):
                shard_dir = os.path.join(root, shard)
                if len(shard) != 2 or not os.path.isdir(shard_dir):
                    continue
                for task in _listdir(shard_dir):
                    if os.path.isdir(os.path.join(shard_dir, task)):
                        out.append(os.path.join(shard_dir, task))
            return sorted(out)
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
    for name in PRODUCER_RUNS + OUTPUTS_RUNS + ["consumer"]:
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
    expected_zero = [n for n in PRODUCER_RUNS + OUTPUTS_RUNS + ["consumer"] if n != FAILING_RUN]
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

    # Ticket 16: `again` runs with cas.nodeHash = true (gate/node-hash.config),
    # so its file leaves are addressed from each task's .command.cas and
    # recorded fusion-node, while cold's were hashed on the head node. The
    # item addresses above must not care which.
    cold_rc, again_rc = gate.run("cold").completion or {}, gate.run("again").completion or {}
    for name, rc in (("cold", cold_rc), ("again", again_rc)):
        if rc.get("schema") != 2 or "providers" not in rc:
            problems.append("run %s's RunCompletion is schema %r without providers; "
                            "ticket 16 moves them there" % (name, rc.get("schema")))
    cold_p, again_p = _provider_of(cold_rc), _provider_of(again_rc)
    raw_leaves = {_address_text(leaf.get("address")) for _cid, item in gate.run("again").items(gate)
                  for leaf in _leaves(item.get("value")) if leaf.get("address") is not None
                  and cas.cid_codec(_address_text(leaf.get("address"))) == cas.RAW}
    not_node = sorted(a for a in raw_leaves if "fusion-node" not in again_p.get(a, set()))
    if not_node:
        problems.append("run again (cas.nodeHash = true) has %d file leaf address(es) "
                        "not under fusion-node: %s" % (len(not_node), not_node[:3]))
    problems.extend(_head_node_problems(raw_leaves, cold_p))
    with_provider = [cid for cid, item in gate.run("again").items(gate)
                     for leaf in _leaves(item.get("value")) if "provider" in leaf]
    if with_provider:
        problems.append("%d OutputItem(s) of again still carry a Leaf provider: %s"
                        % (len(with_provider), with_provider[:3]))
    problems.extend(_node_hash_problems(raw_leaves, gate.work_dirs("pipeline-a", None)))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("%d blocks, %d Store Log entries, %d nf records and %d coords "
                  "pointers survive `again` unchanged; %d raw blocks added 0; "
                  "%d identical OutputItems; again's %d file leaves came from "
                  ".command.cas (fusion-node), cold's from the head node, one item "
                  "address each"
                  % (len(after_again["blocks"]), len(after_again["log"]),
                     len(after_again["nf"]), len(after_again["coords"]),
                     len(raw_cold), len(cold_items), len(raw_leaves)))


def _provider_of(completion):
    """{address text: {provider, ...}} from a RunCompletion's providers."""
    out = {}
    for name, links in (completion.get("providers") or {}).items():
        for link in links:
            out.setdefault(_address_text(link), set()).add(name)
    return out


def _command_cas_problems(task_dir, verified=None):
    """Every .command.cas line must name a file in the task dir that hashes to its digest.

    `verified`, when given, collects the raw CID of every digest the Gate's own
    hash of the named file confirms.
    """
    path = os.path.join(task_dir, ".command.cas")
    problems = []
    with open(path, encoding="utf-8") as f:
        for line in f.read().splitlines():
            digest, name = line[:64], line[66:]
            full = os.path.join(task_dir, name)
            if not os.path.isfile(full):
                problems.append("%s: .command.cas names %s, which is not in the task "
                                "directory" % (task_dir, name))
                continue
            actual = cas.sha256_of_file(full)
            if actual != digest:
                problems.append("%s: .command.cas says %s is %s; it hashes to %s"
                                % (task_dir, name, digest, actual))
            elif verified is not None:
                verified.add(cas.cid_from_sha256(bytes.fromhex(actual), cas.RAW))
    return problems


def _node_hash_problems(raw_leaves, task_dirs):
    """Ties `again`'s file leaves to the task nodes' .command.cas files.

    Every .command.cas line is checked against the Gate's own hash of the file
    it names; the verified digests, as raw CIDs, must be non-empty and must
    cover every raw file leaf. The plugin's `providers` alone is its own claim.
    """
    problems, verified = [], set()
    for task_dir in task_dirs:
        if os.path.isfile(os.path.join(task_dir, ".command.cas")):
            problems.extend(_command_cas_problems(task_dir, verified)[:3])
    if not verified:
        problems.append("no task directory holds a .command.cas line the Gate could "
                        "verify, so nothing shows again's leaves were node-hashed")
        return problems
    uncovered = sorted(a for a in raw_leaves if a not in verified)
    if uncovered:
        problems.append("%d file leaf address(es) of again match no verified "
                        ".command.cas digest: %s" % (len(uncovered), uncovered[:3]))
    return problems


def _head_node_problems(raw_leaves, cold_providers):
    """Every file leaf must be under head-node in cold's providers, absent ones included."""
    not_head = sorted(a for a in raw_leaves
                      if "head-node" not in cold_providers.get(a, set()))
    if not_head:
        return ["run cold has %d file leaf address(es) not under head-node: %s"
                % (len(not_head), not_head[:3])]
    return []


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
    counts = {"regular": 0, "symlink": 0, "directory": 0, "unresolvable": 0}
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
        if want["mode"] == "regular":
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
            # A manifest records no execute bit (ticket 15 addendum): every
            # file is regular, whatever its permission bits.
            out[name] = {"mode": "regular", "size": os.path.getsize(full),
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


# The consumer's writable member (lineage.store.location in gate/consumer). Its
# config sets no outputDir, so the plugin must default it to this alias before
# WorkflowMetadata copies it, and Nextflow's WorkflowRun record must say so.
CONSUMER_OUTPUT_DIR = "cas://out"


def _consumer_output_dir(gate):
    """metadata.outputDir of the consumer's lineage WorkflowRun in store-out/nf/.

    Nextflow's LinObserver writes it through the plugin's lineage store into
    the writable member's nf/ tree; the Gate reads the JSON file directly.
    """
    runs = [spec for _key, spec in gate.store_out.nf_records("WorkflowRun")
            if spec.get("name") == "consumer"]
    if len(runs) != 1:
        raise cas.GateError("expected one WorkflowRun named 'consumer' under "
                            "%s, found %d"
                            % (gate.store_out.path("nf"), len(runs)))
    return (runs[0].get("metadata") or {}).get("outputDir")


# The plugin's own info line (CasObserverFactory.defaultOutputDir) when the
# consumer's config sets no outputDir. Its presence in nextflow.log is also
# the only Gate-visible proof that plugin log.* calls reach Nextflow's log at
# all (Task 1b): the plugin's isolated classloader used to bind every @Slf4j
# logger to the NOP implementation, silently dropping this and every other
# plugin log call. The line goes to the logger `nextflow.cas`, which
# Nextflow's console filter admits, so it must be on the consumer's console
# (stdout.log) as well: plugin loggers named robsyme.cas.* reach only
# nextflow.log.
CONSUMER_OUTPUT_DIR_LOG_LINE = "outputDir not set; publishing to %s" % CONSUMER_OUTPUT_DIR


def _consumer_log_has_output_dir_line(gate):
    """True if the consumer's nextflow.log carries the plugin's default line."""
    path = os.path.join(gate.root, "logs", "consumer", "nextflow.log")
    if not os.path.isfile(path):
        return False
    with open(path, errors="replace") as fh:
        return any(CONSUMER_OUTPUT_DIR_LOG_LINE in line for line in fh)


def _consumer_console_has_output_dir_line(gate):
    """True if the consumer's console (stdout.log or stderr.log) shows the plugin's default line."""
    for name in ("stdout.log", "stderr.log"):
        path = os.path.join(gate.root, "logs", "consumer", name)
        if os.path.isfile(path):
            with open(path, errors="replace") as fh:
                if any(CONSUMER_OUTPUT_DIR_LOG_LINE in line for line in fh):
                    return True
    return False


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
    output_dir = _consumer_output_dir(gate)
    if output_dir != CONSUMER_OUTPUT_DIR:
        problems.append("the consumer's WorkflowRun record says outputDir is %r, "
                        "expected %r: gate/consumer/nextflow.config sets no "
                        "outputDir, so it must default to the lineage alias "
                        "before WorkflowMetadata copies it (ticket 02)"
                        % (output_dir, CONSUMER_OUTPUT_DIR))
    if not _consumer_log_has_output_dir_line(gate):
        problems.append("the consumer's nextflow.log has no 'outputDir not set' "
                        "line from nf-blocks: plugin logging is not reaching "
                        "Nextflow's log")
    elif not _consumer_console_has_output_dir_line(gate):
        problems.append("the consumer's console (logs/consumer/stdout.log) has "
                        "no 'outputDir not set' line: nf-blocks logged it where "
                        "Nextflow's console filter drops it")
    if problems:
        return FAIL, ("; ".join(problems) + ". Not covered in the skeleton: the "
                      "run-rooted cas://<runCid>/aligned/A/A.bam form and a glob "
                      "over a manifest.")
    return PASS, ("lid:// and cas:// each staged one file hashing to %s; the "
                  "consumer set no outputDir, published into %s, and its "
                  "WorkflowRun names %s; the run-rooted form and the manifest "
                  "glob are not in the skeleton"
                  % (expected, gate.store_out.root, CONSUMER_OUTPUT_DIR))


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
# Assertion 13: a cold cache seeds from the Index Snapshot
# --------------------------------------------------------------------------

@assertion(13, "a cold cache seeds from the Index Snapshot")
def assert_thirteen(gate):
    """Ticket 04 decision 11. gate.sh deletes the consumer's cache twice.

    consumer-seeded runs with every RunManifest, RunCompletion and
    OutputCollection block of the runs lab's Store Log lists through the
    snapshot's watermark at mode 000 (and those must be the snapshot's runs): a
    catch-up that read one logs "could not be read (<path>)", and one that
    skipped them without seeding would lose the runs and change the answer.
    consumer-scan runs with lab's snapshot moved aside: it must scan, warn,
    and still give the same answer.
    """
    with open(os.path.join(gate.root, "seeding.json")) as fh:
        before = json.load(fh)
    problems = []
    baseline = _consumer_hashes_of_run(gate, "consumer")
    for name in ("consumer-seeded", "consumer-scan"):
        if gate.exit_code(name) != 0:
            problems.append("%s exited %s; see logs/%s/" % (name, gate.exit_code(name), name))
            continue
        got = _consumer_hashes_of_run(gate, name)
        if got != baseline:
            problems.append("%s staged %s, the first consumer %s: the answer changed"
                            % (name, got, baseline))
    lab = os.path.join(gate.store.root, "index", "v3.sqlite")
    seeding = _seeding_problems(gate.store, before["watermark"], _snapshot_completions(lab))
    problems.extend(seeding)
    seeded_log = _read(os.path.join(gate.root, "logs", "consumer-seeded", "nextflow.log"))
    locked = _read(os.path.join(gate.root, "logs", "consumer-seeded", "locked")).split()
    if not seeding:
        want = gate.store.metadata_blocks_of_runs(
            _store_log_runs_through(gate.store, before["watermark"]))
        if sorted(locked) != want:
            problems.append("logs/consumer-seeded/locked lists %d block(s), the Store "
                            "Log's runs through the watermark have %d metadata blocks"
                            % (len(locked), len(want)))
    denied = _permission_failures(seeded_log, locked)
    if denied:
        problems.append("the seeded run read a locked metadata block of a run the "
                        "snapshot holds: %s" % denied[:2])
    if "no usable Index Snapshot" in seeded_log:
        problems.append("the seeded run fell back to the full scan although lab's "
                        "snapshot was there")
    scan_out = (_read(os.path.join(gate.root, "logs", "consumer-scan", "stdout.log"))
                + _read(os.path.join(gate.root, "logs", "consumer-scan", "nextflow.log")))
    if not ("no usable Index Snapshot" in scan_out and "'lab'" in scan_out
            and "nf-blocks:snapshot" in scan_out):
        problems.append("consumer-scan printed no fallback warning naming lab and "
                        "nf-blocks:snapshot")
    out_runs = _snapshot_runs(os.path.join(gate.store_out.root, "index", "v3.sqlite"))
    if out_runs < before["out_runs_before"]:
        problems.append("store-out's snapshot fell from %d to %d runs"
                        % (before["out_runs_before"], out_runs))
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("with the cache deleted and %d metadata blocks of lab's %d snapshot "
                  "runs unreadable, the consumer seeded from the snapshot and staged "
                  "the same bytes; with the snapshot gone too it scanned, warned and "
                  "staged them again; store-out's snapshot has %d runs"
                  % (len(locked), before["runs"], out_runs))


# How the plugin reports a block it failed to read. Index.groovy's
# ingestTolerant logs "<kind> <cid> could not be read (<e.message>)"; a
# RunCompletion read in blockOfKind is caught there instead and logged as
# "block <cid> could not be decoded as <kind>: <e.message>" (measured on the
# Gate, 2026-09-28). An AccessDeniedException's message is the path alone.
_READ_FAILURE_PHRASES = ("could not be read", "could not be decoded as")


def _permission_failures(log_text, locked_paths):
    """Lines where the plugin failed to read one of the locked blocks."""
    return [line for line in log_text.splitlines()
            if any(phrase in line for phrase in _READ_FAILURE_PHRASES)
            and any(p in line for p in locked_paths)]


def _read(path):
    """The text of a file, or "" when it is absent."""
    if not os.path.isfile(path):
        return ""
    with open(path, errors="replace") as fh:
        return fh.read()


def _snapshot_runs(path):
    """count(*) of an Index Snapshot's run table, read-only; 0 when absent."""
    if not os.path.isfile(path):
        return 0
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return con.execute("SELECT count(*) FROM run").fetchone()[0]
    finally:
        con.close()


def _snapshot_meta(path):
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return dict(con.execute("SELECT key, value FROM meta").fetchall())
    finally:
        con.close()


def _snapshot_completions(path):
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return sorted(row[0] for row in con.execute("SELECT completion_cid FROM run"))
    finally:
        con.close()


def _consumer_hashes_of_run(gate, name):
    """{source: {published file name: sha256 hex}} for one consumer run.

    All three consumer runs publish to the same coordinates, so coords/ holds
    only the last. This reads run `name`'s own `hashes` OutputCollection in
    store-out instead: its publish paths give the source, and each item's leaf
    names the raw block whose text is the sha256sum line.
    """
    manifests, by_run = [], {}
    for cid, _path in gate.store_out.blocks("dagcbor"):
        block = gate.store_out.read_block(cid)
        if not isinstance(block, dict):
            continue
        if block.get("kind") == "RunManifest" and block.get("run_name") == name:
            manifests.append(cid)
        elif block.get("kind") == "RunCompletion" and isinstance(block.get("run"), cas.Cid):
            by_run.setdefault(block["run"].text, []).append(block)
    if len(manifests) != 1:
        raise cas.GateError("expected one RunManifest named %r in %s, found %d"
                            % (name, gate.store_out.root, len(manifests)))
    completions = by_run.get(manifests[0], [])
    if len(completions) != 1:
        raise cas.GateError("expected one RunCompletion for run %r in %s, found %d"
                            % (name, gate.store_out.root, len(completions)))
    out = {}
    for link in completions[0].get("collections") or []:
        collection = gate.store_out.read_block(link.text)
        if collection.get("name") != "hashes":
            continue
        items = collection.get("items") or []
        paths = collection.get("paths") or []
        for item_link, item_paths in zip(items, paths):
            if item_link is None:
                continue
            item = gate.store_out.read_block(item_link.text)
            for leaf, publish_path in zip(_leaves(item.get("value")), item_paths or []):
                if publish_path is None:
                    continue
                rel = "/".join(publish_path) if isinstance(publish_path, list) \
                    else str(publish_path)
                parts = rel.split("/")
                text = gate.store_out.read(_address_text(leaf.get("address"))) \
                    .decode("utf-8", "replace")
                digest = re.match(r"\s*([0-9a-f]{64})", text)
                if len(parts) < 3 or parts[0] != "hashes" or not digest:
                    raise cas.GateError("run %r published %r holding %r, not a "
                                        "sha256sum line under hashes/"
                                        % (name, rel, text[:80]))
                out.setdefault(parts[1], {})[parts[-1]] = digest.group(1)
    return out


def seeding_before(gate):
    """seeding.json: lab's snapshot watermark, run count and write time, and
    store-out's snapshot run count, before the two cold-cache consumers."""
    lab = os.path.join(gate.store.root, "index", "v3.sqlite")
    meta = _snapshot_meta(lab)
    return {"watermark": meta.get("store_log_watermark"),
            "runs": _snapshot_runs(lab),
            "written_at": meta.get("snapshot_written_at"),
            "out_runs_before": _snapshot_runs(
                os.path.join(gate.store_out.root, "index", "v3.sqlite"))}


def delete_consumer_cache(gate):
    """Remove the consumer's composite index (and its -wal/-shm); its path."""
    path = cas.Index.locate_for_pipeline(os.path.join(gate.root, "cache"),
                                         CONSUMER_IDENTITY)
    for each in (path, path + "-wal", path + "-shm"):
        if os.path.exists(each):
            os.remove(each)
    return path


def _store_log_runs_through(store, watermark):
    """Completion cids of the `run` entries of a Store Log at or before
    `watermark` (a Store Log entry name, `<reverse_ts>-<kind>-<cid>`), read
    from the log itself. A reverse timestamp grows into the past, so "at or
    before" is a reverse_ts no smaller than the watermark's."""
    parts = (watermark or "").split("-", 2)
    if len(parts) != 3 or len(parts[0]) != 13 or not parts[0].isdigit():
        raise cas.GateError("the snapshot's watermark %r is not a Store Log entry name"
                            % (watermark,))
    return sorted(cid for rts, kind, cid in store.store_log()
                  if kind == "run" and rts >= parts[0])


def _seeding_problems(store, watermark, snapshot_completions):
    """The snapshot's runs must be exactly the Store Log's runs through its watermark."""
    try:
        expected = set(_store_log_runs_through(store, watermark))
    except cas.GateError as exc:
        return [str(exc)]
    got = set(snapshot_completions)
    problems = []
    if not expected:
        problems.append("lab's Store Log has no run entry at or before the snapshot's "
                        "watermark %s" % watermark)
    if got != expected:
        problems.append("lab's snapshot holds runs %s the Store Log does not list "
                        "through its watermark and lacks %s it does"
                        % (sorted(got - expected)[:3], sorted(expected - got)[:3]))
    return problems


def seeding_lock(gate):
    """Block paths of every RunManifest, RunCompletion and OutputCollection of
    the runs lab's Store Log lists at or before its snapshot's watermark,
    derived from the log, not from the snapshot's own run rows."""
    lab = os.path.join(gate.store.root, "index", "v3.sqlite")
    watermark = _snapshot_meta(lab).get("store_log_watermark")
    return gate.store.metadata_blocks_of_runs(_store_log_runs_through(gate.store, watermark))


# --------------------------------------------------------------------------
# Assertions 14 to 16: milestone 5's outputs store (plan 2026-09-29)
#
# gate/outputs and gate/outputs-badindex run into store-outputs, a store of
# their own that gate.store never sees, so none of the above (Gate.blocks,
# Gate.runs, Gate.run) resolves a run in it. outputs_store and _run_of do for
# store-outputs what Gate.block/Gate.runs/Gate.run do for gate.store.
# --------------------------------------------------------------------------

def outputs_store(gate):
    """cas.Store for store-outputs: the outputs and outputs-badindex runs'
    own store, entirely separate from gate.store, so no existing assertion or
    browser tier ever sees them."""
    return cas.Store(os.path.join(gate.root, "store-outputs"))


def _assemble_runs(blocks, store_root):
    """{run_name: Run}, linking each RunCompletion to its RunManifest by
    run_name. Shared by Gate.runs (over gate.blocks) and store-outputs' own
    _run_of (over any {cid: block} dict), so the two can never drift apart."""
    runs = {}
    by_manifest = {}
    seen = {}
    for cid, block in sorted(blocks.items()):
        if not isinstance(block, dict) or block.get("kind") != "RunManifest":
            continue
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
            % (store_root, "; ".join("%r has %d manifests (%s)"
                                     % (n, len(c), ", ".join(x[:16] + "..." for x in c))
                                     for n, c in sorted(collisions.items()))))
    for cid, block in sorted(blocks.items()):
        if not isinstance(block, dict) or block.get("kind") != "RunCompletion":
            continue
        link = block.get("run")
        run = by_manifest.get(link.text) if isinstance(link, cas.Cid) else None
        if run is None:
            run = runs.setdefault("<orphan:%s>" % cid[:12], Run(None))
        run.completion_cid, run.completion = cid, block
    return runs


class _BlockLookup(object):
    """Duck-types Gate's block(cid) over a plain {cid: block} dict, so
    Run.collections/items (which call gate.block(cid)) work against any
    store's blocks, not only gate.store's."""

    def __init__(self, blocks):
        self._blocks = blocks

    def block(self, cid):
        return self._blocks.get(cid)


def _run_of(store, name):
    """(Run, block-lookup) for the one run named `name` in `store`. What
    Gate.run does for gate.store, generalised to any store: pass the returned
    lookup to run.collections(...)/run.items(...) in place of a Gate."""
    blocks = {}
    for cid, _path in store.blocks("dagcbor"):
        blocks[cid] = store.read_block(cid)
    runs = _assemble_runs(blocks, store.root)
    run = runs.get(name)
    if run is None:
        raise cas.GateError(
            "no run named %r in %s (found: %s)"
            % (name, store.root, ", ".join(sorted(runs)) or "no RunManifest blocks at all"))
    return run, _BlockLookup(blocks)


def _coords_raw_cid(store, rel_path):
    """The store's own coords/<rel_path> pointer, parsed to a raw block cid,
    or None if there is no such coordinate. Raises if the pointer text is not
    a Store URI naming a CID."""
    pointer = store.coords_pointer(rel_path)
    if pointer is None:
        return None
    match = re.match(r"^cas://([^/]+)", pointer)
    if not match or not cas.is_cid(match.group(1)):
        raise cas.GateError("coords/%s does not hold a Store URI: %r" % (rel_path, pointer))
    return match.group(1)


# The warning texts CasObserver.groovy logs on the `nextflow.cas` logger
# (Task 3): once per publishDir process with no workflow output, once per run
# naming the unjoined publishes it made. Nextflow reports a top-level
# process's simple name, so LEGACY here (not a fully-qualified name).
LEGACY_PUBLISHDIR_WARNING = (
    "nf-blocks: process 'LEGACY' uses publishDir; the files it publishes are "
    "stored but no run records them. Declare them as workflow outputs "
    "(output { }) to keep their lineage")
LEGACY_UNJOINED_WARNING = (
    "nf-blocks: 3 file(s) stored this run are in no workflow output, so no run "
    "records them: ")
# The files run "outputs" stores that no workflow output claims: LEGACY's two
# publishDir files (a publish event each) and the collectFile(storeDir:) file
# (no publish event at all, final review C1).
OUTPUTS_UNJOINED = ("collected/samples.txt", "legacy/A.legacy", "legacy/B.legacy")


@assertion(14, "workflow outputs with an index join, and each index file is linked by address")
def assert_fourteen(gate):
    """Tickets 26 answers 1, 2: both outputs join with their Meta Maps, and
    each collection's index leaf is the raw CID of the bytes at its own
    coordinate (hashed here, not trusted). Patch 0.3.0-beta.2: each records
    item's `input`, a path outside the store and the work dir, is a
    never_published Leaf named main.nf with a null publish path, and the
    run still has its RunCompletion."""
    store = outputs_store(gate)
    run, lookup = _run_of(store, "outputs")
    if not run.completion:
        return FAIL, "run outputs has a RunManifest but no RunCompletion block"
    collections = run.collections(lookup)
    problems = []
    for name in ("tuples", "records"):
        if name not in collections:
            problems.append("run outputs has no %r OutputCollection" % name)
    if problems:
        return FAIL, "; ".join(problems)

    for name in ("tuples", "records"):
        _cid, block = collections[name]
        items = run.items(lookup, name)
        for item_cid, item in items:
            meta = metadata_view(item) if item else {}
            if "id" not in meta:
                problems.append("%s item %s: no 'id' in its Meta Map (%r)"
                                % (name, item_cid, meta))
            if name == "records":
                value = (item or {}).get("value")
                outside = value.get("input") if isinstance(value, dict) else None
                if not (isinstance(outside, dict) and outside.get("kind") == "Leaf"
                        and outside.get("reason") == "never_published"
                        and outside.get("name") == "main.nf" and outside.get("address") is None):
                    problems.append("records item %s: its input (a path outside the store) "
                                    "should be a never_published Leaf named main.nf, found %r"
                                    % (item_cid, outside))
        if name == "records":
            for entry in block.get("paths") or []:
                if not (isinstance(entry, list) and None in entry):
                    problems.append("records paths entry %r has no null for the input "
                                    "outside the store" % (entry,))
        index = block.get("index")
        if not isinstance(index, dict):
            problems.append("%s: OutputCollection has no index (%r)" % (name, index))
            continue
        leaf = index.get("leaf") or {}
        path = index.get("path")
        raw_cid = _coords_raw_cid(store, path) if path else None
        if raw_cid is None:
            problems.append("%s: no coords/%s in %s" % (name, path, store.root))
            continue
        data = store.read(raw_cid)                  # re-hashes; raises if it does not match
        computed = cas.cid_raw(data)
        leaf_address = _address_text(leaf.get("address"))
        if computed != raw_cid or computed != leaf_address:
            problems.append("%s: the raw CID of coords/%s's bytes is %s, the coords "
                            "pointer names %s, the index leaf's address is %s: these "
                            "must all agree"
                            % (name, path, computed, raw_cid, leaf_address))
            continue
        if path == "tuples/index.json":
            try:
                rows = json.loads(data.decode("utf-8"))
            except Exception as exc:
                problems.append("tuples/index.json does not parse as JSON: %s" % exc)
            else:
                if not isinstance(rows, list) or len(rows) != 2:
                    problems.append("tuples/index.json: expected a 2-row JSON array, "
                                    "found %r" % (rows,))
        elif path == "records/index.csv":
            lines = data.decode("utf-8").splitlines()
            if len(lines) != 3 or "id" not in lines[0]:
                problems.append("records/index.csv: expected a header row and 2 data "
                                "rows, found %d line(s): %r" % (len(lines), lines))
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("tuples and records both join with Meta Maps carrying 'id'; each "
                  "records item's input outside the store is a never_published leaf; each "
                  "collection's index leaf is the raw CID of the bytes at its own "
                  "coordinate, matching the coords pointer; tuples/index.json has 2 "
                  "rows and records/index.csv has a header and 2 rows")


@assertion(15, "an index Nextflow fails to write is never_published while the run succeeds")
def assert_fifteen(gate):
    """Ticket 26 answer 4: run "outputs-badindex" exits 0 with status succeeded;
    its tuples collection has 2 items and an index leaf with reason
    never_published; anomalies.never_published >= 1; no coordinate
    tuples/index.csv exists in store-outputs, or, if it does, the leaf still
    is not addressed."""
    store = outputs_store(gate)
    run, lookup = _run_of(store, "outputs-badindex")
    if not run.completion:
        return FAIL, "run outputs-badindex has a RunManifest but no RunCompletion block"
    problems = []
    exit_code = gate.exit_code("outputs-badindex")
    if exit_code != 0:
        problems.append("run outputs-badindex exited %r, expected 0" % (exit_code,))
    if run.completion.get("status") != "succeeded":
        problems.append("run outputs-badindex: status is %r, expected 'succeeded'"
                        % run.completion.get("status"))

    collections = run.collections(lookup)
    if "tuples" not in collections:
        return FAIL, "; ".join(problems + ["run outputs-badindex has no 'tuples' "
                                           "OutputCollection"])
    _cid, block = collections["tuples"]
    items = run.items(lookup, "tuples")
    if len(items) != 2:
        problems.append("tuples: expected 2 items, found %d" % len(items))

    index = block.get("index")
    leaf = {}
    if not isinstance(index, dict):
        problems.append("tuples: OutputCollection has no index (%r)" % (index,))
    else:
        leaf = index.get("leaf") or {}
        if leaf.get("reason") != "never_published":
            problems.append("tuples index leaf reason is %r, expected 'never_published'"
                            % leaf.get("reason"))

    # Checked from the store itself, not just from what the RunCompletion says:
    # either no tuples/index.csv coordinate exists in store-outputs, or, if one
    # does, the index leaf must still not be addressed. Reading only
    # leaf.get("address") never touches store-outputs' coords/ tree at all, so
    # a plugin bug that wrote a stray coordinate while still (incorrectly)
    # marking the leaf never_published would sail through undetected.
    coord_cid = _coords_raw_cid(store, "tuples/index.csv")
    if leaf.get("address") is not None:
        if coord_cid is not None:
            problems.append("coords/tuples/index.csv exists (%s) in %s and the "
                            "tuples index leaf is addressed (%r): a coordinate "
                            "may exist only while the leaf still is not addressed"
                            % (coord_cid, store.root, leaf.get("address")))
        else:
            problems.append("tuples index leaf has address %r though CsvWriter never "
                            "wrote the file" % (leaf.get("address"),))

    anomalies = run.completion.get("anomalies") or {}
    never_published = anomalies.get("never_published")
    if not isinstance(never_published, int) or never_published < 1:
        problems.append("anomalies.never_published is %r, expected >= 1" % (never_published,))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("run outputs-badindex exited 0 with status succeeded; its tuples "
                  "index leaf is never_published (reason=%r, address=%r); "
                  "anomalies.never_published=%s"
                  % (leaf.get("reason"), leaf.get("address"), never_published))


@assertion(16, "a publishDir process warns, and every file no output claims counts as unjoined")
def assert_sixteen(gate):
    """Ticket 19 answer 2: run "outputs" logs "process 'LEGACY' uses
    publishDir" once and "3 file(s) stored this run are in no workflow
    output" once. The expected unjoined count is computed from the store:
    every coords/ pointer in store-outputs whose path no collection of
    either run names (an item's publish path or an index path). That set
    must be exactly LEGACY's two publishDir files and the collectFile
    (storeDir:) file, which Nextflow writes with no publish event; each is
    an addressed coordinate no item or index leaf's address names; and
    anomalies.unjoined must equal its size."""
    store = outputs_store(gate)
    run, lookup = _run_of(store, "outputs")
    if not run.completion:
        return FAIL, "run outputs has a RunManifest but no RunCompletion block"
    problems = []

    log_text = _read(os.path.join(gate.root, "logs", "outputs", "nextflow.log"))
    publishdir_hits = log_text.count(LEGACY_PUBLISHDIR_WARNING)
    if publishdir_hits != 1:
        problems.append("logs/outputs/nextflow.log has the LEGACY publishDir warning "
                        "%d time(s), expected 1" % publishdir_hits)
    unjoined_hits = log_text.count(LEGACY_UNJOINED_WARNING)
    if unjoined_hits != 1:
        problems.append("logs/outputs/nextflow.log has the unjoined-files warning %d "
                        "time(s), expected 1" % unjoined_hits)

    addressed = set()
    claimed = set()
    runs = [(run, lookup)]
    try:
        runs.append(_run_of(store, "outputs-badindex"))
    except cas.GateError:
        pass                                        # assertion 15 reports a missing run
    for each, each_lookup in runs:
        for out_name, (_cid, block) in each.collections(each_lookup).items():
            claimed.update(_strings(block.get("paths")))
            index = block.get("index")
            if isinstance(index, dict):
                if index.get("path"):
                    claimed.add(index["path"])
                if each is run:
                    address = _address_text((index.get("leaf") or {}).get("address"))
                    if address:
                        addressed.add(address)
            if each is not run:
                continue
            for _item_cid, item in each.items(each_lookup, out_name):
                if not item:
                    continue
                for leaf in _leaves(item.get("value")):
                    address = _address_text(leaf.get("address"))
                    if address:
                        addressed.add(address)

    unclaimed = [rel for rel in store.coords_paths() if rel not in claimed]
    if tuple(unclaimed) != OUTPUTS_UNJOINED:
        problems.append("the coords/ pointers no collection names are %r, expected %r"
                        % (unclaimed, list(OUTPUTS_UNJOINED)))
    anomalies = run.completion.get("anomalies") or {}
    if anomalies.get("unjoined") != len(unclaimed):
        problems.append("anomalies.unjoined is %r, but %d coords/ pointer(s) no collection "
                        "names: %s" % (anomalies.get("unjoined"), len(unclaimed),
                                       ", ".join(unclaimed) or "none"))

    for rel in OUTPUTS_UNJOINED:
        raw_cid = _coords_raw_cid(store, rel)
        if raw_cid is None:
            problems.append("no coords/%s in %s" % (rel, store.root))
            continue
        data = store.read(raw_cid)                  # re-hashes; raises if it does not match
        if cas.cid_raw(data) != raw_cid:
            problems.append("coords/%s: the bytes there do not hash to %s" % (rel, raw_cid))
        if raw_cid in addressed:
            problems.append("coords/%s (%s) is also an item or index leaf's address; "
                            "it should be unjoined" % (rel, raw_cid))

    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("logs/outputs/nextflow.log warns once about LEGACY's publishDir and "
                  "once about 3 unjoined files; anomalies.unjoined == %d, the coords/ "
                  "pointers no collection names (%s), each addressed and named by no "
                  "item or index leaf" % (len(unclaimed), ", ".join(unclaimed)))


def _strings(value):
    """Every string in a nested list (an OutputCollection's paths)."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for element in value:
            for text in _strings(element):
                yield text


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
     "Under Fusion, each published file's address comes from the task node's "
     "`.command.cas` or from S3's SHA-256 of a server-side copy (`s3-copy`), "
     "both checked against hashes the test computes; tier two runs it "
     "(`make gate-tier2`, T2 and T2b)."),
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
    known = {"--offline", "--refs", "--snapshot", "--seeding-before",
             "--delete-consumer-cache", "--seeding-lock"}
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
    if "--seeding-before" in flags:
        print(json.dumps(seeding_before(gate)))
        return 0
    if "--delete-consumer-cache" in flags:
        print(delete_consumer_cache(gate))
        return 0
    if "--seeding-lock" in flags:
        for path in seeding_lock(gate):
            print(path)
        return 0
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
