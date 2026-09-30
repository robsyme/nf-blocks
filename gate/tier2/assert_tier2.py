#!/usr/bin/env python3
# gate/tier2/assert_tier2.py -- the tier-two assertions (ticket 11), independent of the plugin.
#   assert_tier2.py refs <T2_ROOT> <GATE_ROOT>    shell lines T2_LID, T2_CAS, T2_DIR for the consumer; T2_DIR is
#                                                 the qc/A/A_qc manifest of cas, t2b's, whose alias.txt is symlink, as
#                                                 the Item Occurrence cas://<collection>/<item>/A_qc
#   assert_tier2.py t6-refs <T2_ROOT>             T6_RUNS
#   assert_tier2.py check <T2_ROOT> <GATE_ROOT>   the table; exit 1 on any FAIL
#   assert_tier2.py summary <T2_ROOT>             Batch job count, total job seconds, head-node lines (reported only)
"""Gate tier two's assertions (ticket 11 T1-T6, T2b).

Every address checked here is derived with hashlib from bytes read out of S3 by
boto3 (gate/tier2/s3gate.py): the work bucket's task outputs, the member's
blocks, the task nodes' .command.cas files. Nothing is taken on the plugin's
word (global constraint rule 6). The bucket and work prefix come from
<T2_ROOT>/ids.env, which tier2.sh writes.
"""

import importlib.util
import json
import os
import re
import shlex
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

import cas  # noqa: E402
import s3gate  # noqa: E402

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
PRODUCER = "cas-test-pipeline"
CONSUMER = "cas-gate-consumer"
S3_COPY, FUSION_NODE = "s3-copy", "fusion-node"
PRODUCERS = ("ts", "t1", "t2", "t2b", "t6a", "t6b")
RACE_MARKER = "not rewritten: replaced_meanwhile"
FALLBACK = "no usable Index Snapshot"
HEAD_NODE_LINE = "nf-blocks: the head node read"
SIDECAR = ".fusion.symlinks"


# --------------------------------------------------------------------------
# What tier two knows about one run of the harness
# --------------------------------------------------------------------------

def _tier_one():
    """gate/assert.py as a module (its name is a Python keyword, so it is loaded by path)."""
    spec = importlib.util.spec_from_file_location("gate_assert", os.path.join(os.path.dirname(HERE), "assert.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_ids(root):
    out = {}
    with open(os.path.join(root, "ids.env")) as fh:
        for line in fh:
            key, _, value = line.strip().partition("=")
            if key:
                out[key] = value
    return out


class Run(object):
    """A run read out of an S3 member: its RunCompletion and RunManifest, both verified."""

    def __init__(self, member, completion_cid, completion, manifest):
        self.member, self.completion_cid, self.completion, self.manifest = member, completion_cid, completion, manifest
        self._collections = None

    @property
    def nf_run_hash(self):
        return self.manifest.get("nf_run_hash")

    def collections(self):
        """{output name: (cid, block)}."""
        if self._collections is None:
            self._collections = {}
            for link in self.completion.get("collections") or []:
                block = self.member.decoded(link.text)
                self._collections[block.get("name")] = (link.text, block)
        return self._collections

    def items(self, output):
        entry = self.collections().get(output)
        if entry is None:
            return []
        return [(link.text, self.member.decoded(link.text)) for link in entry[1].get("items") or [] if link is not None]

    def item_cids(self, output):
        return {cid for cid, _block in self.items(output)}

    def leaves(self):
        return [leaf for name in sorted(self.collections()) for _cid, item in self.items(name)
                for leaf in _leaves(item.get("value"))]

    def raw_leaves(self):
        return [leaf for leaf in self.leaves() if _codec(leaf.get("address")) == cas.RAW]

    def providers(self):
        return {name: {_text(c) for c in cids or []} for name, cids in (self.completion.get("providers") or {}).items()}


class Ctx(object):
    """Everything an assertion reads. cold and local_coords stand in for tier one's GATE_ROOT in the unit tests."""

    def __init__(self, s3, bucket, work_prefix, root, gate_root=None, cold=None, local_coords=None):
        self.s3, self.bucket, self.root, self.gate_root = s3, bucket, root, gate_root
        self.work_prefix = work_prefix                  # robsyme/nf-blocks-gate/<run id>/, what teardown empties
        self.work_dir = work_prefix + "work/"           # batch.config's workDir
        self._cold, self._local_coords = cold, local_coords
        self._members, self._runs, self._sha, self._body, self._work = {}, {}, {}, {}, None

    @classmethod
    def from_root(cls, root, gate_root):
        ids = read_ids(root)
        return cls(s3gate.client(), ids["BUCKET"], ids["WORK"], root, gate_root)

    def member(self, name):
        if name not in self._members:
            self._members[name] = s3gate.Member(self.s3, self.bucket, name)
        return self._members[name]

    def run(self, member, name):
        """The run named `name` among the member's Store Log runs, or None."""
        key = (member, name)
        if key not in self._runs:
            m, found = self.member(member), []
            for cid in m.completions():
                completion = m.decoded(cid)
                manifest = m.decoded(completion["run"].text)
                if manifest.get("run_name") == name:
                    found.append(Run(m, cid, completion, manifest))
            if len(found) > 1:
                raise cas.GateError("%d RunCompletions of a run named %r in %s" % (len(found), name, member))
            self._runs[key] = found[0] if found else None
        return self._runs[key]

    def exit(self, run):
        path = os.path.join(self.root, "logs", run, "exit")
        if not os.path.isfile(path):
            return None
        with open(path) as fh:
            text = fh.read().strip()
        return int(text) if text.lstrip("-").isdigit() else None

    def text(self, *parts):
        path = os.path.join(self.root, *parts)
        if not os.path.isfile(path):
            return ""
        with open(path, errors="replace") as fh:
            return fh.read()

    def trace(self, run):
        return parse_trace(self.text("trace", "%s.txt" % run))

    def task_prefixes(self, run):
        """Each task directory of `run` as a key prefix in the work bucket, from its trace's workdir."""
        head = "s3://%s/" % s3gate.WORK_BUCKET
        return sorted({row["workdir"][len(head):].rstrip("/") + "/" for row in self.trace(run)
                       if row.get("workdir", "").startswith(head)})

    def work(self):
        if self._work is None:
            self._work = s3gate.work_objects(self.s3, self.work_dir)
        return self._work

    def sha(self, key):
        if key not in self._sha:
            self._sha[key] = s3gate.sha256_of_object(self.s3, s3gate.WORK_BUCKET, key)
        return self._sha[key]

    def body(self, key):
        if key not in self._body:
            self._body[key] = self.s3.get_object(Bucket=s3gate.WORK_BUCKET, Key=key)["Body"].read()
        return self._body[key]

    def listing(self, prefix):
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=s3gate.WORK_BUCKET, Prefix=prefix):
            out.extend(o["Key"] for o in page.get("Contents", []))
        return out

    def dir_files(self, run, name):
        """{path under <name>/: bytes} of the one task directory of `run` that holds <name>/."""
        found = {}
        for prefix in self.task_prefixes(run):
            keys = [k for k in self.listing(prefix + name + "/") if not k.endswith("/")]
            if keys:
                found[prefix] = {k[len(prefix + name + "/"):]: self.body(k) for k in keys}
        if len(found) != 1:
            raise cas.GateError("%d task directories of %s hold %s/ in the work bucket (want 1)" % (len(found), run, name))
        return list(found.values())[0]

    def evidence(self, *parts):
        path = os.path.join(self.root, "evidence", *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def cold_items(self, output):
        """Tier one's local `cold` run's item addresses for an output (its GATE_ROOT/store)."""
        if self._cold is None:
            t1 = _tier_one()
            gate = t1.Gate(self.gate_root)
            cold = gate.run("cold")
            self._cold = {name: {cid for cid, _b in cold.items(gate, name)} for name in ("aligned", "qc")}
        return set(self._cold.get(output) or ())

    def local_coord(self, rel):
        if self._local_coords is not None:
            return self._local_coords.get(rel)
        return cas.Store(os.path.join(self.gate_root, "store")).coords_pointer(rel)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

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


def _text(value):
    return value.text if isinstance(value, cas.Cid) else value


def _codec(value):
    text = _text(value)
    return cas.cid_codec(text) if cas.is_cid(text) else None


def _digest_cid(hexdigest):
    return cas.cid_from_sha256(bytes.fromhex(hexdigest), cas.RAW)


def _pointer(pointer):
    """(cid, name) of a cas://<cid>/<name> Store URI, or (None, None)."""
    m = re.match(r"^cas://([^/]+)/?(.*)$", pointer or "")
    if not m or not cas.is_cid(m.group(1)):
        return None, None
    return m.group(1), m.group(2)


_SUM = re.compile(r"^([0-9a-f]{64}) [ *](.+)$")


def parse_sums(text):
    """[(hex, path)] of sha256sum output lines; anything else is ignored."""
    return [(m.group(1), m.group(2)) for m in (_SUM.match(line) for line in text.splitlines()) if m]


def parse_trace(text):
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:]]


def _few(values, n=3):
    values = sorted(values)
    return ", ".join(str(v) for v in values[:n]) + (" and %d more" % (len(values) - n) if len(values) > n else "")


def _verdict(problems, message):
    return (FAIL, "; ".join(problems[:6])) if problems else (PASS, message)


def expected_manifest(files, fusion):
    """(cid, value) of the DirectoryManifest the Gate expects for a directory whose files are {path: bytes}.

    With fusion, a directory's .fusion.symlinks names its links (ticket 15): each is a symlink whose target
    is the object's body, and the sidecar is not an entry. Without, every object is a regular file, so a
    link nxf_s3_upload flattened is a regular file holding its target's bytes."""
    tree = {}
    for path, data in files.items():
        node, parts = tree, path.split("/")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = data

    def build(node):
        links = set()
        if fusion and isinstance(node.get(SIDECAR), bytes):
            links = {name for name in node[SIDECAR].decode("utf-8").rstrip("\n").split("\n") if name}
        entries = []
        for name in sorted((n for n in node if not (fusion and n == SIDECAR)), key=lambda n: n.encode("utf-8")):
            child = node[name]
            if isinstance(child, dict):
                entries.append({"name": name, "mode": "directory", "size": 0,
                                "address": cas.Cid(build(child)[0]), "target": None})
            elif name in links:
                entries.append({"name": name, "mode": "symlink", "size": len(child),
                                "address": None, "target": child.decode("utf-8")})
            else:
                entries.append({"name": name, "mode": "regular", "size": len(child),
                                "address": cas.Cid(cas.cid_raw(child)), "target": None})
        value = {"kind": "DirectoryManifest", "schema": 2, "entries": entries}
        return cas.cid_dagcbor(cas.encode(value)), value

    return build(tree)


def gate_listing(files, name, fusion):
    """[(path, sha256 hex)] a `find -L <name> -type f | LC_ALL=C sort | sha256sum` of the staged directory
    must print: links followed to their target's bytes, the sidecar absent."""
    import hashlib
    links = {}
    if fusion:
        for path, data in files.items():
            if path.rsplit("/", 1)[-1] == SIDECAR:
                parent = path.rsplit("/", 1)[0] if "/" in path else ""
                for link in data.decode("utf-8").rstrip("\n").split("\n"):
                    if link:
                        links[(parent + "/" if parent else "") + link] = parent
    out = []
    for path, data in files.items():
        if fusion and path.rsplit("/", 1)[-1] == SIDECAR:
            continue
        if path in links:
            target = os.path.normpath(os.path.join(links[path], data.decode("utf-8")))
            data = files.get(target)
            if data is None:
                continue                               # find -L -type f does not list a dangling link
        out.append(("%s/%s" % (name, path), hashlib.sha256(data).hexdigest()))
    return sorted(out, key=lambda pair: pair[0].encode("utf-8"))


def _manifest_files(member, cid, prefix):
    """[(path, raw cid)] of every regular file in a published directory, recursing into subdirectories."""
    out = []
    for entry in member.decoded(cid).get("entries") or []:
        path = "%s/%s" % (prefix, entry.get("name"))
        if entry.get("mode") == "directory":
            out.extend(_manifest_files(member, _text(entry.get("address")), path))
        elif entry.get("mode") == "regular":
            out.append((path, _text(entry.get("address"))))
    return out


def _dir_leaves(run, output, name):
    """[(item cid, manifest cid)] of the run's items of `output` with a directory leaf named `name`."""
    out = []
    for item_cid, item in run.items(output):
        for leaf in _leaves(item.get("value")):
            if leaf.get("name") == name and _codec(leaf.get("address")) == cas.DAG_CBOR:
                out.append((item_cid, _text(leaf.get("address"))))
    return out


def _dir_leaf(run, output, name):
    found = _dir_leaves(run, output, name)
    return found[0][1] if found else None


def occurrence_manifest(member, uri):
    """(manifest cid, name) that cas://<collection>/<item>/<name> names in the member, or (None, None)."""
    m = re.match(r"^cas://([^/]+)/([^/]+)/([^/]+)$", uri or "")
    if not m or not all(cas.is_cid(g) for g in m.groups()[:2]):
        return None, None
    collection, item, name = m.groups()
    if item not in {_text(link) for link in member.decoded(collection).get("items") or [] if link is not None}:
        return None, None
    for leaf in _leaves(member.decoded(item).get("value")):
        if leaf.get("name") == name and _codec(leaf.get("address")) == cas.DAG_CBOR:
            return _text(leaf.get("address")), name
    return None, None


def _entry(manifest, name):
    for entry in manifest.get("entries") or []:
        if entry.get("name") == name:
            return entry
    return None


def provider_problems(run, provider):
    """Raw file leaves of a run that its RunCompletion does not list under `provider`."""
    if run is None:
        return ["no such run"]
    providers, leaves = run.providers(), run.raw_leaves()
    if not leaves:
        return ["its RunCompletion has no raw file leaf"]
    wrong = []
    for leaf in leaves:
        address = _text(leaf.get("address"))
        if address not in providers.get(provider, set()):
            under = [name for name, cids in sorted(providers.items()) if address in cids] or ["no provider"]
            wrong.append("%s (%s) is under %s" % (leaf.get("name"), address[:16] + "...", "/".join(under)))
    return ["%d of %d file leaves not under %s: %s" % (len(wrong), len(leaves), provider, "; ".join(wrong[:3]))] if wrong else []


def _exit_problem(ctx, run):
    code = ctx.exit(run)
    return [] if code == 0 else ["%s exited %s; see logs/%s/" % (run, code, run)]


def _run_or_problem(ctx, member, name, problems):
    run = ctx.run(member, name)
    if run is None:
        problems.append("no RunCompletion of %s in s3://%s/%s's Store Log" % (name, ctx.bucket, member))
    return run


def run_hashes(ctx, member, name):
    """{source: {published file name: text}} of a consumer run's `hashes` collection, read from its own
    OutputCollection (coords/ keeps only the last run's)."""
    run = ctx.run(member, name)
    if run is None:
        raise cas.GateError("no RunCompletion of %s in %s" % (name, member))
    entry = run.collections().get("hashes")
    if entry is None:
        raise cas.GateError("%s has no hashes collection" % name)
    out = {}
    for link, item_paths in zip(entry[1].get("items") or [], entry[1].get("paths") or []):
        if link is None:
            continue
        item = run.member.decoded(link.text)
        for leaf, publish_path in zip(_leaves(item.get("value")), item_paths or []):
            if publish_path is None:
                continue
            rel = "/".join(publish_path) if isinstance(publish_path, list) else str(publish_path)
            parts = rel.split("/")
            if len(parts) < 3 or parts[0] != "hashes":
                raise cas.GateError("%s published %r, not a file under hashes/<source>/" % (name, rel))
            out.setdefault(parts[1], {})[parts[-1]] = run.member.verified(_text(leaf.get("address"))).decode("utf-8", "replace")
    return out


def _work_digest(ctx, name, problems):
    digests = {ctx.sha(k) for k in ctx.work().get(name, [])}
    if len(digests) != 1:
        problems.append("the work bucket holds %d distinct %s (want 1)" % (len(digests), name))
        return None
    return digests.pop()


def _sqlite_meta(path):
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return dict(con.execute("SELECT key, value FROM meta").fetchall())
    finally:
        con.close()


def _json(path):
    """A JSON file's value, or None when it is absent or empty."""
    if not os.path.isfile(path) or not os.path.getsize(path):
        return None
    with open(path) as fh:
        return json.load(fh)


def read_env(path):
    out = {}
    if os.path.isfile(path):
        with open(path) as fh:
            for line in fh:
                key, _, value = line.strip().partition("=")
                if key:
                    out[key] = (shlex.split(value) or [""])[0]
    return out


# --------------------------------------------------------------------------
# The assertions
# --------------------------------------------------------------------------

def t1(ctx):
    """Assertion 12: the Test Pipeline on Batch, Fusion off, into the S3 member."""
    problems = _exit_problem(ctx, "t1")
    m, work = ctx.member("cas"), ctx.work()
    coords = m.coords()
    with open(ctx.evidence("coords-cas.json"), "w") as fh:
        json.dump(coords, fh, indent=1, sort_keys=True)
    checked = 0
    for rel, pointer in sorted(coords.items()):
        cid, _name = _pointer(pointer)
        if cid is None:
            problems.append("coords/%s holds %r, not a cas:// Store URI" % (rel, pointer))
            continue
        if cas.cid_codec(cid) != cas.RAW:
            continue
        name = rel.split("/", 2)[-1]
        want = {_digest_cid(ctx.sha(k)) for k in work.get(name, [])}
        if cid not in want:
            problems.append("coords/%s names %s; the work bucket's %d object(s) named %s hash to %s"
                            % (rel, cid, len(work.get(name, [])), name, _few(want) or "nothing"))
        try:
            m.verified(cid)
        except Exception as exc:
            problems.append("coords/%s: the member's block %s: %s" % (rel, cid, exc))
        checked += 1
    if not checked:
        problems.append("no coords/ pointer in s3://%s/cas names a raw block" % ctx.bucket)
    outputs = [(key, spec) for key, spec in m.nf_records("FileOutput")
               if key.split("/")[0] == str(spec.get("workflowRun") or "")[len("lid://"):]]
    not_cas = [key for key, spec in outputs if not str(spec.get("path") or "").startswith("cas://")]
    if not outputs:
        problems.append("no workflow-output FileOutput record under s3://%s/cas/nf/" % ctx.bucket)
    elif not_cas:
        problems.append("%d FileOutput record(s) name no cas:// path: %s" % (len(not_cas), _few(not_cas)))
    run = _run_or_problem(ctx, "cas", "t1", problems)
    if run is not None:
        problems.extend("t1: " + p for p in provider_problems(run, S3_COPY))
    prefixes = ctx.task_prefixes("t1")
    if not prefixes:
        problems.append("trace/t1.txt names no task directory in the work bucket")
    for prefix in prefixes:
        try:
            if b"cas:" in ctx.body(prefix + ".command.run"):
                problems.append("s3://%s/%s.command.run contains cas:" % (s3gate.WORK_BUCKET, prefix))
        except Exception as exc:
            problems.append("no .command.run in s3://%s/%s (%s)" % (s3gate.WORK_BUCKET, prefix, exc))
    return _verdict(problems, "%d raw coordinate(s) hash to the work bucket's bytes; %d workflow-output FileOutput "
                              "records name cas://; every file leaf of t1 is s3-copy; no .command.run of %d task(s) "
                              "mentions cas:" % (checked, len(outputs), len(prefixes)))


def t2(ctx):
    """Assertion 11: Fusion on, a fresh member: s3-copy, and every .command.cas digest is the Gate's."""
    problems = _exit_problem(ctx, "t2")
    run = _run_or_problem(ctx, "cas-t2", "t2", problems)
    if run is not None:
        problems.extend("t2: " + p for p in provider_problems(run, S3_COPY))
    node, lines = set(), 0
    prefixes = ctx.task_prefixes("t2")
    if not prefixes:
        problems.append("trace/t2.txt names no task directory in the work bucket")
    for prefix in prefixes:
        try:
            body = ctx.body(prefix + ".command.cas")
        except Exception:
            problems.append("no .command.cas in s3://%s/%s" % (s3gate.WORK_BUCKET, prefix))
            continue
        with open(ctx.evidence("command-cas", "t2", prefix.rstrip("/").replace("/", "_") + ".command.cas"), "wb") as fh:
            fh.write(body)
        sums = parse_sums(body.decode("utf-8", "replace"))
        if not sums:
            problems.append("s3://%s/%s.command.cas lists no file" % (s3gate.WORK_BUCKET, prefix))
        for hexdigest, path in sums:
            lines += 1
            try:
                actual = ctx.sha(prefix + path)
            except Exception as exc:
                problems.append("%s.command.cas names %s, which is not in the work bucket (%s)" % (prefix, path, exc))
                continue
            if actual != hexdigest:
                problems.append("%s.command.cas says %s for %s; the Gate hashed %s" % (prefix, hexdigest, path, actual))
            node.add((path, _digest_cid(hexdigest)))
    published = 0
    if run is not None:
        for leaf in run.raw_leaves():
            published += 1
            if (leaf.get("name"), _text(leaf.get("address"))) not in node:
                problems.append("published %s is %s, which no .command.cas line gives" % (leaf.get("name"), _text(leaf.get("address"))))
        for leaf in run.leaves():
            if _codec(leaf.get("address")) == cas.DAG_CBOR:
                for path, cid in _manifest_files(run.member, _text(leaf.get("address")), leaf.get("name")):
                    published += 1
                    if (path, cid) not in node:
                        problems.append("published %s is %s, which no .command.cas line gives" % (path, cid))
        problems.extend(_item_problems(ctx, "t2", run))
    return _verdict(problems, "%d .command.cas line(s) over %d task(s) equal the Gate's hashes; %d published file(s) "
                              "carry their .command.cas digest under s3-copy; aligned items equal t1's, qc items equal "
                              "tier one's cold ones and not t1's flattened ones" % (lines, len(prefixes), published))


def _item_problems(ctx, name, run):
    t1_run = ctx.run("cas", "t1")
    if t1_run is None:
        return ["no t1 run in cas to compare items with"]
    problems = []
    aligned, t1_aligned = run.item_cids("aligned"), t1_run.item_cids("aligned")
    if not aligned or aligned != t1_aligned:
        problems.append("%s's aligned items %s differ from t1's %s" % (name, _few(aligned - t1_aligned) or "(none)", _few(t1_aligned - aligned) or "(none)"))
    qc, cold = run.item_cids("qc"), ctx.cold_items("qc")
    if not qc or qc != cold:
        problems.append("%s's qc items differ from tier one's cold ones (%s against %s)" % (name, _few(qc), _few(cold)))
    if qc and qc == t1_run.item_cids("qc"):
        problems.append("%s's qc items equal t1's, which flattened alias.txt (ticket 15 decision 5)" % name)
    return problems


def t2b(ctx):
    """T2b: Fusion on, into T1's member: fusion-node; its items are T1's (aligned) and t2's (qc)."""
    problems = _exit_problem(ctx, "t2b")
    run = _run_or_problem(ctx, "cas", "t2b", problems)
    t1_run, t2_run = ctx.run("cas", "t1"), ctx.run("cas-t2", "t2")
    if run is not None:
        problems.extend("t2b: " + p for p in provider_problems(run, FUSION_NODE))
        aligned, qc = run.item_cids("aligned"), run.item_cids("qc")
        if not aligned or t1_run is None or not aligned <= t1_run.item_cids("aligned"):
            problems.append("t2b's aligned items %s are not among t1's" % (_few(aligned) or "(none)"))
        if not qc or t2_run is None or not qc <= t2_run.item_cids("qc"):
            problems.append("t2b's qc items %s are not among t2's" % (_few(qc) or "(none)"))
        if not qc <= ctx.cold_items("qc"):
            problems.append("t2b's qc items %s are not among tier one's cold ones" % _few(qc))
    return _verdict(problems, "every file leaf of t2b is fusion-node; its aligned items are t1's, its qc items t2's "
                              "and tier one's cold ones")


def t3(ctx):
    """Assertion 5 in the cloud: each manifest equals the Gate's own walk of the work bucket."""
    problems, notes = [], []
    t1_run, t2_member = ctx.run("cas", "t1"), ctx.member("cas-t2")
    flat = _dir_leaf(t1_run, "qc", "A_qc") if t1_run else None
    if flat is None:
        problems.append("t1's RunCompletion has no qc item for A_qc")
    else:
        want, _value = expected_manifest(ctx.dir_files("t1", "A_qc"), fusion=False)
        got = ctx.member("cas").decoded(flat)
        alias, summary = _entry(got, "alias.txt"), _entry(got, "summary.txt")
        if not (alias and summary and alias.get("mode") == "regular" and alias.get("address") == summary.get("address")):
            problems.append("t1's A_qc manifest %s holds alias.txt as %s, expected a regular file with summary.txt's "
                            "address (nxf_s3_upload flattens it)" % (flat, alias))
        if flat != want:
            problems.append("t1's A_qc manifest is %s, the Gate's walk of the work bucket gives %s" % (flat, want))
        notes.append("t1 %s (alias.txt flattened)" % (flat[:16] + "..."))
    pointer = t2_member.coords().get("qc/A/A_qc")
    fusion, _name = _pointer(pointer)
    if fusion is None:
        problems.append("s3://%s/cas-t2/coords/qc/A/A_qc holds %r" % (ctx.bucket, pointer))
    else:
        want, _value = expected_manifest(ctx.dir_files("t2", "A_qc"), fusion=True)
        alias = _entry(t2_member.decoded(fusion), "alias.txt")
        if not alias or alias.get("mode") != "symlink" or alias.get("target") != "summary.txt":
            problems.append("t2's A_qc manifest %s holds alias.txt as %s, expected a symlink to summary.txt" % (fusion, alias))
        if fusion != want:
            problems.append("t2's A_qc manifest is %s, the Gate's decoding of .fusion.symlinks gives %s" % (fusion, want))
        local, _n = _pointer(ctx.local_coord("qc/A/A_qc"))
        if fusion != local:
            problems.append("t2's A_qc manifest %s is not tier one's local cold one %s: the backend changed the address" % (fusion, local))
        notes.append("t2 %s (alias.txt a link, equal to tier one's local manifest)" % (fusion[:16] + "..."))
    return _verdict(problems, "A_qc manifests equal the Gate's walk of the work bucket: " + "; ".join(notes))


def t4(ctx):
    """Assertion 6 in the cloud: the consumer on Batch reads back lid://, cas://, fromStore and a directory."""
    problems = []
    refs = read_env(os.path.join(ctx.root, "evidence", "refs-t4.env"))
    dir_cid, dir_name = occurrence_manifest(ctx.member("cas"), refs.get("T2_DIR"))
    if dir_cid is None:
        problems.append("t4 was given --dir %r, not an Item Occurrence of a directory in cas" % refs.get("T2_DIR"))
    else:
        alias = _entry(ctx.member("cas").decoded(dir_cid), "alias.txt")
        if not alias or alias.get("mode") != "symlink":
            problems.append("--dir %s holds alias.txt as %s; a directory without a symlink entry does not exercise "
                            "ticket 15 decision 7" % (refs.get("T2_DIR"), alias))
    problems.extend(_exit_problem(ctx, "t4"))
    if ctx.exit("t4") != 0:
        return FAIL, "; ".join(problems)
    hashes = run_hashes(ctx, "cas-out", "t4")
    a, b = _work_digest(ctx, "A.bam", problems), _work_digest(ctx, "B.bam", problems)
    for source, name, want in (("lid", "A.bam.sha256", a), ("cas", "A.bam.sha256", a), ("fromstore", "B.bam.sha256", b)):
        files = hashes.get(source, {})
        sums = parse_sums(files.get(name, ""))
        if set(files) != {name} or len(sums) != 1 or sums[0][0] != want:
            problems.append("%s staged %s, expected only %s of %s" % (source, {f: parse_sums(t) for f, t in files.items()}, name, want))
    listed = [(path, digest) for digest, path in parse_sums(hashes.get("dir", {}).get("%s.sha256" % dir_name, ""))]
    want_listing = gate_listing(ctx.dir_files("t2b", "A_qc"), "A_qc", fusion=True)
    if listed != want_listing:
        problems.append("dir staged %s, the Gate's listing of t2b's A_qc (alias.txt as summary.txt's bytes) is %s" % (listed, want_listing))
    return _verdict(problems, "lid:// and cas:// staged A.bam's bytes, fromStore B.bam's, and the directory %d files "
                              "with alias.txt staged as a copy of summary.txt" % len(listed))


def t5(ctx):
    """Ticket 04 on S3: the consumer again with its cache deleted seeds from lab's S3 snapshot."""
    problems = _exit_problem(ctx, "t5")
    if ctx.exit("t5") == 0:
        if run_hashes(ctx, "cas-out", "t5") != run_hashes(ctx, "cas-out", "t4"):
            problems.append("t5 staged other bytes than t4")
    cache = cas.Index.locate_for_pipeline(os.path.join(ctx.root, "cache-consumer"), CONSUMER)
    seeded = _sqlite_meta(cache).get("seeded_from:lab")
    lab = ctx.member("cas").download("index/v3.sqlite", ctx.evidence("cas-v3.sqlite"))
    written = _sqlite_meta(lab).get("snapshot_written_at")
    if not seeded or seeded != written:
        problems.append("t5's cache says seeded_from:lab %r; lab's snapshot was written at %r" % (seeded, written))
    if FALLBACK in ctx.text("logs", "t5", "nextflow.log") + ctx.text("logs", "t5", "stdout.log"):
        problems.append("t5 fell back to the full scan (%r in logs/t5/)" % FALLBACK)
    before_path = os.path.join(ctx.root, "evidence", "cas-out-after-t4.json")
    before = _json(before_path) or {}
    after = s3gate.snapshot(ctx.s3, ctx.bucket, "cas-out", ctx.evidence("cas-out-after-t5.sqlite"))
    for key in ("meta_runs", "count"):
        if before.get(key) is None or after.get(key) is None or after[key] < before[key]:
            problems.append("cas-out's snapshot %s went from %r after t4 to %r after t5" % (key, before.get(key), after.get(key)))
    return _verdict(problems, "t5 staged t4's bytes, seeded from lab's snapshot written at %s, no fallback; cas-out's "
                              "snapshot runs %s -> %s" % (written, before.get("count"), after.get("count")))


def if_match_verdict(probe):
    if probe == {"status": 412, "body": "second"}:
        return []
    return ["S3 answered the stale If-Match PutObject with %r (want 412, and the second body kept)" % (probe,)]


def race_verdict(texts):
    """(status, message) of the last attempt's two snapshot verbs."""
    skipped = sum(1 for text in texts if RACE_MARKER in text)
    if skipped == 1:
        return PASS, "one of two overlapping snapshot verbs was refused (%s)" % RACE_MARKER
    if skipped == 0:
        return SKIP, ("the two verbs did not overlap in 3 attempts; ticket 03 decision 6 was not observed through "
                      "the plugin on AWS")
    return FAIL, "both snapshot verbs printed %r: neither wrote the snapshot" % RACE_MARKER


def t6(ctx):
    """Ticket 03 decision 6: two writers into one fresh member."""
    problems = _exit_problem(ctx, "t6a") + _exit_problem(ctx, "t6b")
    m = ctx.member("cas-t6")
    runs = [_run_or_problem(ctx, "cas-t6", name, problems) for name in ("t6a", "t6b")]
    blocks = m.blocks()
    for cid in blocks:
        try:
            m.verified(cid)
        except Exception as exc:
            problems.append("block %s: %s" % (cid, exc))
    occurrences = [line.strip() for line in ctx.text("logs", "t6-items.txt").splitlines() if line.startswith("cas://")]
    listed = {occ[len("cas://"):].split("/")[0] for occ in occurrences}
    for run in runs:
        if run is not None and run.collections().get("aligned", (None,))[0] not in listed:
            problems.append("t6-items.txt lists no occurrence of %s's aligned collection" % run.manifest.get("run_name"))
    log_runs = len(m.completions())
    try:
        rows = s3gate.snapshot_runs(m.download("index/v3.sqlite", ctx.evidence("cas-t6-v3.sqlite")))
        if rows != log_runs:
            problems.append("cas-t6's snapshot has %d run rows, its Store Log %d run entries" % (rows, log_runs))
    except Exception as exc:
        problems.append("cas-t6's snapshot: %s" % exc)
    held = set(blocks)
    for rel, pointer in sorted(m.coords().items()):
        cid, _name = _pointer(pointer)
        if cid not in held:
            problems.append("cas-t6 coords/%s names %s, which the member does not hold" % (rel, pointer))
    probe_path = os.path.join(ctx.root, "evidence", "if-match.json")
    probe = _json(probe_path)
    problems.extend(if_match_verdict(probe))
    if problems:
        return FAIL, "; ".join(problems[:6])
    attempts = sorted({int(n) for n in (re.match(r"t6-race-(\d+)-", f).group(1) for f in os.listdir(os.path.join(ctx.root, "logs"))
                                       if re.match(r"t6-race-\d+-[xy]\.txt$", f))})
    last = attempts[-1] if attempts else 0
    status, race = race_verdict([ctx.text("logs", "t6-race-%d-%s.txt" % (last, side)) for side in ("x", "y")])
    message = ("both runs in the Store Log; %d blocks hash to their CIDs; items list both runs; snapshot %d runs; "
               "S3 refused a stale If-Match with 412 and kept the newer object; %s" % (len(blocks), log_runs, race))
    return status, message


def ts(ctx):
    """Ticket 18 on the queue: exit 0; the RunCompletion succeeded; collection multiqc has >= 1 item,
    each with a Meta Map carrying "id"; every item Leaf is independently verified against the member --
    a raw Leaf's block hashes to its address (Member.verified) and coords/ names the same CID for its
    recorded publish path; a directory Leaf's block decodes and hashes to its address (Member.decoded)
    -- and the index leaf the same way, against coords/<index.path> (ticket 26; the Output Index File is
    written by the head node straight to the output directory, so it is never a task's own output --
    Nextflow's PublishOp -- and is verified from the member, not the work bucket); anomalies.unjoined
    equals the number of coords/ keys whose pointer CID is neither an item leaf nor the index leaf, and
    is > 0; the log names at least one "uses publishDir" warning."""
    problems = _exit_problem(ctx, "ts")
    m = ctx.member("cas-sarek")
    run = _run_or_problem(ctx, "cas-sarek", "ts", problems)
    items, recorded, actual, index_leaf_address = [], None, None, None
    if run is not None:
        if run.completion.get("status") != "succeeded":
            problems.append("ts's RunCompletion status is %r, not succeeded" % run.completion.get("status"))
        coords = m.coords()
        referenced = set()
        entry = run.collections().get("multiqc")
        if entry is None:
            problems.append("ts's RunCompletion has no multiqc collection")
        else:
            _coll_cid, block = entry
            items = run.items("multiqc")
            if not items:
                problems.append("multiqc collection has no item")
            for (item_cid, item), item_paths in zip(items, block.get("paths") or []):
                value = item.get("value")
                meta = value[0] if isinstance(value, list) and value else None
                if not isinstance(meta, dict) or "id" not in meta:
                    problems.append("multiqc item %s carries no Meta Map with 'id': %r" % (item_cid, meta))
                for leaf, publish_path in zip(_leaves(value), item_paths or []):
                    address = _text(leaf.get("address"))
                    if address is None:
                        problems.append("multiqc item %s's %r leaf is not addressed (reason %r)"
                                        % (item_cid, leaf.get("name"), leaf.get("reason")))
                        continue
                    referenced.add(address)
                    try:
                        m.decoded(address) if _codec(leaf.get("address")) == cas.DAG_CBOR else m.verified(address)
                    except Exception as exc:
                        problems.append("multiqc item %s's %r leaf %s: %s" % (item_cid, leaf.get("name"), address, exc))
                    if publish_path is not None:
                        rel = "/".join(publish_path) if isinstance(publish_path, list) else str(publish_path)
                        coord_cid, _name = _pointer(coords.get(rel))
                        if coord_cid != address:
                            problems.append("multiqc item %s's %r leaf is %s; coords/%s names %s"
                                            % (item_cid, leaf.get("name"), address, rel, coord_cid))
            index = block.get("index")
            if not index or not index.get("leaf"):
                problems.append("multiqc collection has no index (index { path \"multiqc/index.json\" } expected)")
            else:
                index_path = index.get("path") or ""
                index_leaf_address = _text(index["leaf"].get("address"))
                try:
                    m.verified(index_leaf_address)
                except Exception as exc:
                    problems.append("index leaf %s: %s" % (index_leaf_address, exc))
                coord_cid, _name = _pointer(coords.get(index_path))
                if coord_cid != index_leaf_address:
                    problems.append("index leaf's address is %s; coords/%s names %s"
                                    % (index_leaf_address, index_path, coord_cid))
        if index_leaf_address is not None:
            referenced.add(index_leaf_address)
        unjoined_keys = [rel for rel, pointer in sorted(coords.items())
                         if _pointer(pointer)[0] is not None and _pointer(pointer)[0] not in referenced]
        actual = len(unjoined_keys)
        recorded = (run.completion.get("anomalies") or {}).get("unjoined") or 0
        if recorded != actual:
            problems.append("RunCompletion anomalies.unjoined is %d; %d coords/ key(s) name a block no item or index "
                            "leaf references: %s" % (recorded, actual, _few(unjoined_keys)))
        if not actual:
            problems.append("anomalies.unjoined is %r; want > 0 (sarek publishes files outside the workflow output)" % actual)
    if "uses publishDir" not in ctx.text("logs", "ts", "nextflow.log"):
        problems.append("logs/ts/nextflow.log names no 'uses publishDir' warning")
    return _verdict(problems, "ts exited 0; multiqc has %d item(s) each Meta-addressed and independently verified "
                              "against the member; its index leaf hashes to coords/multiqc/index.json's block; "
                              "anomalies.unjoined %s matches %s coords/ key(s) no leaf references; the log names a "
                              "publishDir warning" % (len(items), recorded, actual))


CHECKS = [("T1", "cloud executor publish (assertion 12)", t1),
          ("T2", "Fusion publish into a fresh member (assertion 11)", t2),
          ("T2b", "Fusion publish into T1's member", t2b),
          ("T3", "published directory matches the work bucket (assertion 5)", t3),
          ("T4", "the consumer on Batch reads back (assertion 6)", t4),
          ("T5", "a cold consumer cache seeds from S3 (ticket 04)", t5),
          ("T6", "two writers into one member (ticket 03 decision 6)", t6),
          ("TS", "nf-core/sarek: its workflow output joins and its publishDir files are counted (ticket 18)", ts)]


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------

def refs(ctx):
    """{T2_LID, T2_CAS, T2_DIR}. T2_DIR is the qc/A/A_qc manifest whose alias.txt is a symlink (F14): t2b's,
    as an Item Occurrence, cas://<collection>/<item>/A_qc. A directory's Store URI cas://<manifest> has no file
    name, so Nextflow's FilePorter would stage it into the cache directory itself and fail its integrity check
    forever; cas://<manifest>/A_qc is an entry named A_qc inside the manifest, which does not exist. The
    occurrence resolves to the manifest and is named A_qc. With no such item, t1's (flattened) occurrence, so
    T4's first check fails rather than the consumer being given nothing."""
    out = {"T2_LID": "", "T2_CAS": "", "T2_DIR": ""}
    m, t1_run = ctx.member("cas"), ctx.run("cas", "t1")
    if t1_run is not None and t1_run.nf_run_hash:
        out["T2_LID"] = "lid://%s/aligned/A/A.bam" % t1_run.nf_run_hash
    out["T2_CAS"] = m.coords().get("aligned/A/A.bam", "")
    candidates = []
    for run in (ctx.run("cas", "t2b"), t1_run):
        if run is not None and "qc" in run.collections():
            collection = run.collections()["qc"][0]
            candidates.extend((collection, item, manifest) for item, manifest in _dir_leaves(run, "qc", "A_qc"))
    for collection, item, manifest in candidates:
        alias = _entry(m.decoded(manifest), "alias.txt")
        if alias and alias.get("mode") == "symlink":
            out["T2_DIR"] = "cas://%s/%s/A_qc" % (collection, item)
            break
    else:
        if candidates:
            out["T2_DIR"] = "cas://%s/%s/A_qc" % candidates[-1][:2]
    return out


def t6_refs(ctx):
    hashes = []
    for name in ("t6a", "t6b"):
        run = ctx.run("cas-t6", name)
        if run is not None and run.nf_run_hash:
            hashes.append("lid://%s" % run.nf_run_hash)
    return {"T6_RUNS": ",".join(hashes)}


def check(ctx):
    results = []
    for code, title, fn in CHECKS:
        try:
            status, message = fn(ctx)
        except Exception as exc:
            status, message = FAIL, "%s: %s" % (type(exc).__name__, exc)
        results.append((status, code, title, message))
    width = max(len(r[2]) for r in results)
    indent = 4 + 2 + 3 + 2 + width + 2
    for status, code, title, message in results:
        print("%-4s  %-3s  %-*s  %s" % (status, code, width, title, _wrap(message, 92, indent)))
    counts = {s: sum(1 for r in results if r[0] == s) for s in (PASS, FAIL, SKIP)}
    print("")
    print("%d PASS, %d FAIL, %d SKIP" % (counts[PASS], counts[FAIL], counts[SKIP]))
    return 1 if counts[FAIL] else 0


def _wrap(text, width, indent):
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


_DURATION = re.compile(r"([\d.]+)\s*(ms|s|m|h|d)")
_SECONDS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def duration_seconds(text):
    """Seconds in a Nextflow trace duration such as '1m 3s' or '850ms'; 0 for '-'."""
    return sum(float(n) * _SECONDS[unit] for n, unit in _DURATION.findall(text or ""))


def summary(root):
    rows = []
    trace = os.path.join(root, "trace")
    for name in sorted(os.listdir(trace)) if os.path.isdir(trace) else []:
        with open(os.path.join(trace, name), errors="replace") as fh:
            rows.extend(parse_trace(fh.read()))
    jobs = [r for r in rows if r.get("native_id") not in (None, "", "-")]
    print("tier two: %d Batch job(s), %.0f job second(s) in all" % (len(jobs), sum(duration_seconds(r.get("realtime")) for r in jobs)))
    for run in PRODUCERS:
        path = os.path.join(root, "logs", run, "nextflow.log")
        lines = []
        if os.path.isfile(path):
            with open(path, errors="replace") as fh:
                lines = [line.strip() for line in fh]
        found = [line[line.index(HEAD_NODE_LINE):] for line in lines if HEAD_NODE_LINE in line]
        print("  %-4s %s" % (run, found[-1] if found else "(no head-node line in logs/%s/nextflow.log)" % run))
    print("  (the head node is a laptop nearest ca-central-1: every byte it read crossed regions)")
    return 0


def _shquote(text):
    return "'" + text.replace("'", "'\\''") + "'"


def main(argv):
    if len(argv) >= 3 and argv[1] == "refs":
        ids = read_ids(argv[2])
        for key, value in refs(Ctx(s3gate.client(), ids["BUCKET"], ids["WORK"], argv[2])).items():
            print("%s=%s" % (key, _shquote(value)))
        return 0
    if len(argv) >= 3 and argv[1] == "t6-refs":
        ids = read_ids(argv[2])
        for key, value in t6_refs(Ctx(s3gate.client(), ids["BUCKET"], ids["WORK"], argv[2])).items():
            print("%s=%s" % (key, _shquote(value)))
        return 0
    if len(argv) == 4 and argv[1] == "check":
        return check(Ctx.from_root(argv[2], argv[3]))
    if len(argv) == 3 and argv[1] == "summary":
        return summary(argv[2])
    sys.stderr.write(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
