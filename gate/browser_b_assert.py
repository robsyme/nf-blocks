#!/usr/bin/env python3
"""Gate browser tier B (block explorer spec section 1.3, assertions 8-16):
Selections and Claims written through the page, all local, over a
composition of a writable member and a read-only one.

    python3 gate/browser_b_assert.py prepare <GATE_ROOT>
    python3 gate/browser_b_assert.py probe <GATE_ROOT> <port> <token>
    python3 gate/browser_b_assert.py check <GATE_ROOT>

prepare copies this Gate run's store to $GATE_ROOT/browser-b/store (the
member `lab` of the explore server tier_b.sh starts, so tier B's writes never
touch the store tier A and the lineage tier read), builds a read-only member
`shared` beside it with the Gate's own encoder, works out which items and
files the scenario picks from the Gate's own read of the blocks and its own
hashes, and writes the scenario drive.mjs plays. probe runs while explore is
still up: it replays one of the page's own writes, sends three POSTs the
endpoint must refuse, dry-runs a Selection both members name, and fetches the
samplesheet export. check recomputes
every address with the Gate's encoder (gate/dagjson.py) and compares it, the
store, the probes and the selection pipeline's hashes with those answers.
Nothing the plugin or the page reports is taken on trust.
"""
import csv
import importlib.util
import json
import os
import re
import shutil
import sys
import urllib.error
import urllib.request
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cas  # noqa: E402
import dagjson  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
ASSERTED_BY = "gate"


def _load_assert():
    spec = importlib.util.spec_from_file_location("gate_assert", os.path.join(HERE, "assert.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


A = _load_assert()


def _write_json(path, value):
    with open(path, "w") as fh:
        json.dump(value, fh, indent=1)


def _read_json(path):
    with open(path) as fh:
        return json.load(fh)


# --------------------------------------------------------------------------
# prepare
# --------------------------------------------------------------------------

def _file(gate, name):
    """One work file the samplesheet must name: its sha256 over every copy in pipeline-a/work, and its raw CID."""
    paths = gate.work_files("pipeline-a", name)
    if not paths:
        raise cas.GateError("no %s under %s/pipeline-a/work" % (name, gate.root))
    digests = {cas.sha256_of_file(p) for p in paths}
    if len(digests) != 1:
        raise cas.GateError("%s differs between work directories: %s" % (name, sorted(digests)))
    return {"name": name, "sha256": next(iter(digests)), "cid": cas.cid_of_file(paths[0])}


SHARED_TS = "2026-01-01T00:00:00.000Z"
SHARED_MILLIS = 1767225600000
HORIZON_MILLIS = 9999999999999   # StoreLog.HORIZON_MILLIS


def _put_block(root, block):
    """Write the Gate's own encoding of `block` into a member directory, as a member stores it."""
    data = cas.encode(block)
    cid = cas.cid_dagcbor(data)
    path = os.path.join(root, "blocks", cid[-2:], cid)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    return cid


def _log(root, kind, cid, millis):
    """A Store Log entry: an empty file log/<13-digit reverse ts>-<kind>-<cid> (StoreLog.entryName)."""
    os.makedirs(os.path.join(root, "log"), exist_ok=True)
    open(os.path.join(root, "log", "%013d-%s-%s" % (HORIZON_MILLIS - millis, kind, cid)), "a").close()


def prepare(root):
    gate = A.Gate(root)
    out = os.path.join(root, "browser-b")
    shutil.rmtree(out, ignore_errors=True)
    store = os.path.join(out, "store")
    shutil.copytree(gate.store.root, store, ignore=shutil.ignore_patterns("index", "index.html"))
    os.makedirs(os.path.join(out, "cache"))
    os.makedirs(os.path.join(out, "store-out"))
    # `shared`: a second, read-only member (only blocks/ and log/, which is all a member's reader lists).
    shared = os.path.join(out, "shared")
    os.makedirs(shared)
    with open(os.path.join(out, "explore.config"), "w") as fh:
        fh.write("lineage.enabled = true\nlineage.store.location = 'cas://lab'\n"
                 "cas {\n  stores {\n    lab { location = '%s' }\n    shared { location = '%s' }\n  }\n"
                 "  asserted_by = '%s'\n  index { path = '%s' }\n}\n" % (store, shared, ASSERTED_BY, os.path.join(out, "cache", "index.sqlite")))
    cold, again = gate.run("cold"), gate.run("again")
    coll = {name: cid for name, (cid, _b) in cold.collections(gate).items()}
    again_aligned = again.collections(gate)["aligned"][0]

    def item(run, output, sample):
        for cid, block in run.items(gate, output):
            if A.metadata_view(block).get("sample") == sample:
                return cid
        raise cas.GateError("no %s item with sample %s in run %s" % (output, sample, run.name))

    a, b, c = item(cold, "aligned", "A"), item(cold, "aligned", "B"), item(cold, "stats", "C")
    if item(again, "aligned", "B") != b:
        raise cas.GateError("item B of `again` is not the block of `cold`; the nesting test needs one item in two collections")
    if again_aligned == coll["aligned"]:
        raise cas.GateError("`again`'s aligned collection is `cold`'s; B would not be picked through two collections")

    def claim(subject, value, supersedes):
        return dagjson.expected_claim({"subject": cas.Cid(subject), "verb": "set", "attribute": "name", "value": value,
                                       "supersedes": [cas.Cid(s) for s in supersedes], "timestamp": SHARED_TS}, ASSERTED_BY)

    def selection(members):
        return dagjson.expected_selection({"kind": "Selection", "members": members, "derived_from": []}, ASSERTED_BY)

    def deletion(subject):
        return dagjson.expected_claim({"subject": cas.Cid(subject), "verb": "delete", "attribute": None, "value": None,
                                       "supersedes": [], "timestamp": SHARED_TS}, ASSERTED_BY)

    # S3: what B.copy composes (A from cold's aligned), held only in shared, named there.
    s3 = _put_block(shared, selection([_item(a, [coll["aligned"]])]))
    _log(shared, "selection", s3, SHARED_MILLIS)
    n_s = _put_block(shared, claim(s3, "from-shared", []))
    _log(shared, "claim", n_s, SHARED_MILLIS)
    # S4: held and named in lab; shared renamed it, superseding lab's Claim.
    s4 = _put_block(store, selection([_item(c, [coll["stats"]])]))
    _log(store, "selection", s4, SHARED_MILLIS)
    n1 = _put_block(store, claim(s4, "lab-name", []))
    _log(store, "claim", n1, SHARED_MILLIS)
    n2 = _put_block(shared, claim(s4, "shared-name", [n1]))
    _log(shared, "claim", n2, SHARED_MILLIS)
    # S5: B from cold's aligned, held only in shared, named and deleted there (B.restore restores it).
    s5 = _put_block(shared, selection([_item(b, [coll["aligned"]])]))
    _log(shared, "selection", s5, SHARED_MILLIS)
    n5 = _put_block(shared, claim(s5, "restored-name", []))
    _log(shared, "claim", n5, SHARED_MILLIS)
    d5 = _put_block(shared, deletion(s5))
    _log(shared, "claim", d5, SHARED_MILLIS)
    # S6: B from again's aligned, held in lab, deleted in shared (B.deleted names the deletion).
    s6 = _put_block(store, selection([_item(b, [again_aligned])]))
    _log(store, "selection", s6, SHARED_MILLIS)
    d6 = _put_block(shared, deletion(s6))
    _log(shared, "claim", d6, SHARED_MILLIS)
    expected = {"items": {"A": a, "B": b, "C": c},
                "collections": {"aligned": coll["aligned"], "stats": coll["stats"], "again": again_aligned},
                "files": {"A": _file(gate, "A.bam"), "B": _file(gate, "B.bam"), "C": _file(gate, "C.stats")},
                "shared": {"s3": s3, "n_s": n_s, "s4": s4, "n1": n1, "n2": n2,
                           "s5": s5, "n5": n5, "d5": d5, "s6": s6, "d6": d6}}
    q = "?token={token}"
    steps = [
        {"id": "B.first", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (coll["aligned"], a),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/item/%s/%s" % (coll["aligned"], b)}, {"click": "[data-pick]"},
                     {"hash": "#/compose"}, {"fill": ["#compose-name", "first"]}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "after"}],
         "save": [["S1", "body", "data-written"]]},
        {"id": "B.second", "server": "explore", "path": "", "query": q, "hash": "#/selection/{S1}",
         "actions": [{"click": "[data-pick][data-kind=selection]"}, {"hash": "#/item/%s/%s" % (again_aligned, b)}, {"click": "[data-pick]"},
                     {"hash": "#/item/%s/%s" % (coll["stats"], c)}, {"click": "[data-pick]"}, {"hash": "#/compose"},
                     {"fill": ["#compose-name", "second"]}, {"click": "#compose-save"}, {"waitWrite": True}, {"extract": "after"}],
         "save": [["S2", "body", "data-written"]]},
        {"id": "B.rename", "server": "explore", "path": "", "query": q, "hash": "#/selection/{S2}",
         "actions": [{"fill": ["#rename-name", "second-renamed"]}, {"click": "#rename-save"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.delete", "server": "explore", "path": "", "query": q, "hash": "#/selection/{S2}",
         "actions": [{"click": "#delete"}, {"waitWrite": True}, {"hash": "#/selections"}, {"extract": "list"},
                     {"hash": "#/selections?deleted=1"}, {"extract": "deleted"}, {"hash": "#/selection/{S2}"},
                     {"click": "#undo"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.conflict", "server": "explore", "path": "", "query": q, "hash": "#/selection/{S2}", "pages": 2,
         "actions": [{"page": 0, "fill": ["#rename-name", "third-a"]}, {"page": 1, "fill": ["#rename-name", "third-b"]},
                     {"page": 0, "click": "#rename-save"}, {"page": 0, "waitWrite": True},
                     {"page": 1, "click": "#rename-save"}, {"page": 1, "waitWrite": True},
                     {"page": 0, "extract": "a"}, {"page": 1, "extract": "b"}]},
        {"id": "B.copy", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (coll["aligned"], a),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}, {"click": "#compose-copy"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.foreign", "server": "explore", "path": "", "query": q, "hash": "#/selection/%s" % s4,
         "actions": [{"extract": "before"}, {"fill": ["#rename-name", "lab-renamed"]}, {"click": "#rename-save"},
                     {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.restore", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (coll["aligned"], b),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}, {"click": "#compose-copy"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.deleted", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (again_aligned, b),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}, {"click": "#exists-restore"}, {"waitWrite": True}, {"extract": "after"}]},
    ]
    _write_json(os.path.join(out, "expected.json"), expected)
    _write_json(os.path.join(out, "scenario.json"), {"steps": steps})
    print("browser tier B: %d steps prepared over a copy of the store in %s" % (len(steps), store))
    return 0


# --------------------------------------------------------------------------
# probe (explore is up)
# --------------------------------------------------------------------------

def _observed(out):
    path = os.path.join(out, "observed.json")
    if not os.path.isfile(path):
        return {}
    return {s["id"]: s for s in _read_json(path)["steps"]}


def _posts(step):
    """The step's POST /api/put requests that write (not ?dry_run=true), in order."""
    out = []
    for r in step.get("requests") or []:
        path, _sep, query = (r.get("url") or "").partition("?")
        if r.get("method") != "POST" or not path.endswith("/api/put") or "dry_run=true" in query:
            continue
        out.append({"body": r.get("body"), "responseBody": r.get("responseBody"), "status": r.get("status"),
                    "page": r.get("page")})
    return out


def _kind(post):
    return json.loads(post["body"] or "{}").get("kind")


def _saved(observed, step_id):
    """The `written` address the step's `after` extract recorded."""
    return (((observed.get(step_id) or {}).get("extracts") or {}).get("after") or {}).get("written")


def _block_count(out):
    return sum(len(files) for _d, _s, files in os.walk(os.path.join(out, "store", "blocks")))


def _log_count(out):
    path = os.path.join(out, "store", "log")
    return len(os.listdir(path)) if os.path.isdir(path) else 0


def _request(url, data=None, headers=None, method="GET"):
    request = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _post(url, body, headers):
    status, data = _request(url, (body or "").encode("utf-8"), headers, "POST")
    return {"status": status, "body": data.decode("utf-8", "replace")}


def _get(url):
    return _request(url)


def probe(root, port, token):
    out = os.path.join(root, "browser-b")
    observed = _observed(out)
    base = "http://127.0.0.1:%d" % int(port)
    origin = base
    renames = _posts(observed.get("B.rename") or {})
    selections = [p for p in _posts(observed.get("B.first") or {}) if _kind(p) == "Selection"]
    if not renames or not selections:
        raise cas.GateError("the driver recorded no rename or no Selection POST to probe with (see browser-b/drive.log)")
    rename = renames[0]
    # The refusals send a Selection no step wrote ({A via aligned, C via stats};
    # {C via stats} alone is S4, which prepare put in lab), so a refused POST
    # that wrote anyway leaves a block check() can look for.
    expected = _read_json(os.path.join(out, "expected.json"))

    def member(key, coll):
        return {"item": {"address": {"/": expected["items"][key]}, "via": [{"/": expected["collections"][coll]}]}}

    refused_body = json.dumps({"kind": "Selection", "derived_from": [], "members": [member("A", "aligned"), member("C", "stats")]})
    refusal_address = dagjson.address(dagjson.expected_selection(dagjson.loads(refused_body), ASSERTED_BY))
    if cas.Store(os.path.join(out, "store")).has(refusal_address):
        raise cas.GateError("the refusal probe's Selection %s is already in the store; it would prove nothing" % refusal_address)
    put = base + "/api/put"
    results = {"blocks_before": _block_count(out), "log_before": _log_count(out), "refusal_address": refusal_address}
    # The page's own bytes, replayed exactly: idempotence first (decision 9).
    results["replay"] = _post(put, rename["body"], {"X-NF-Blocks-Token": token, "Content-Type": "application/vnd.ipld.dag-json",
                                                     "Origin": origin})
    results["blocks_after_replay"], results["log_after_replay"] = _block_count(out), _log_count(out)
    results["refusals"] = {
        "no_token": _post(put, refused_body, {"Content-Type": "application/json", "Origin": origin}),
        "foreign_origin": _post(put, refused_body, {"X-NF-Blocks-Token": token, "Content-Type": "application/json",
                                                      "Origin": "http://evil.example"}),
        "text_plain": _post(put, refused_body, {"X-NF-Blocks-Token": token, "Content-Type": "text/plain", "Origin": origin}),
    }
    results["blocks_after"], results["log_after"] = _block_count(out), _log_count(out)
    # S4's request as a dry run: the composition-wide answer once lab renamed it and shared's Claim still stands (B15).
    foreign_body = json.dumps({"kind": "Selection", "members": [member("C", "stats")], "derived_from": []})
    results["foreign_dry"] = _post(put + "?dry_run=true", foreign_body, {"X-NF-Blocks-Token": token,
                                   "Content-Type": "application/vnd.ipld.dag-json", "Origin": origin})
    s2 = _saved(observed, "B.second")
    for fmt in ("csv", "json"):
        status, body = _get("%s/api/samplesheet/%s.%s" % (base, s2, fmt)) if s2 else (None, b"")
        results["samplesheet_" + fmt] = status
        with open(os.path.join(out, "samplesheet." + fmt), "wb") as fh:
            fh.write(body)
    _write_json(os.path.join(out, "probes.json"), results)
    print("browser tier B: probed %s (replay %s; refusals %s; S4 dry run %s; samplesheet %s / %s)"
          % (base, results["replay"]["status"], [r["status"] for r in results["refusals"].values()],
             results["foreign_dry"]["status"], results["samplesheet_csv"], results["samplesheet_json"]))
    return 0


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------

def _response(post):
    """The endpoint's answer to one POST, decoded as DAG-JSON (links become cas.Cid)."""
    if not post.get("responseBody"):
        raise cas.GateError("no response body recorded for a POST that answered %s" % post.get("status"))
    return dagjson.loads(post["responseBody"])


def _text(value):
    return value.text if isinstance(value, cas.Cid) else value


def _item(cid, via):
    return {"item": {"address": cas.Cid(cid), "via": [cas.Cid(v) for v in sorted(via)]}}


def _members(entries):
    """Selection members in block order: sorted by the member address's string form (spec section 7.1)."""
    def key(m):
        return m["selection"].text if "selection" in m else m["item"]["address"].text
    return sorted(entries, key=key)


def _claims_about(store, subject):
    """Every Claim block in the store whose subject is `subject`, as dagjson.claim_state takes them."""
    out = []
    for cid, _path in store.blocks("dagcbor"):
        block = store.read_block(cid)
        if not isinstance(block, dict) or block.get("kind") != "Claim" or _text(block.get("subject")) != subject:
            continue
        out.append({"cid": cid, "verb": block.get("verb"), "attribute": block.get("attribute"), "value": block.get("value"),
                    "supersedes": [_text(s) for s in block.get("supersedes") or []]})
    return out


def _staged(root, source):
    """Sorted '<source>:<file name>' tags of every HASH task the selection pipeline ran for `source`, one per task.
    Published hashes are keyed by file name, so a file staged twice shows only here (each task's .command.run header)."""
    out = []
    for dirpath, _dirnames, filenames in os.walk(os.path.join(root, "selection", "work")):
        if ".command.run" not in filenames:
            continue
        with open(os.path.join(dirpath, ".command.run"), encoding="utf-8", errors="replace") as fh:
            for line in fh:
                match = re.match(r"^### name: 'HASH \((.*)\)'$", line.rstrip("\n"))
                if match:
                    if match.group(1).startswith(source + ":"):
                        out.append(match.group(1))
                    break
    return sorted(out)


def _read_sheet_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def evaluate(root):
    """[(status, number, title, message)] for assertions 8-16."""
    out = os.path.join(root, "browser-b")
    expected = _read_json(os.path.join(out, "expected.json"))
    observed = _observed(out)
    probes_path = os.path.join(out, "probes.json")
    probes = _read_json(probes_path) if os.path.isfile(probes_path) else None
    store = cas.Store(os.path.join(out, "store"))
    shared_store = cas.Store(os.path.join(out, "shared"))
    items, colls, files = expected["items"], expected["collections"], expected["files"]
    want_hashes = {"%s.sha256" % files[k]["name"]: files[k]["sha256"] for k in sorted(files)}

    def once_each(source):
        want = sorted("%s:%s" % (source, files[k]["name"]) for k in files)
        got = _staged(root, source)
        return [] if got == want else ["the pipeline hashed %s, expected each file once: %s" % (got, want)]

    def seen(step_id):
        if step_id not in observed:
            raise cas.GateError("the driver recorded no step %s (see browser-b/drive.log)" % step_id)
        s = observed[step_id]
        if s.get("driverError"):
            raise cas.GateError("%s: the driver failed: %s" % (step_id, s["driverError"][:300]))
        if s.get("pageErrors"):
            raise cas.GateError("%s: page errors %s" % (step_id, s["pageErrors"][:3]))
        return s

    def extract(step_id, name):
        got = (seen(step_id).get("extracts") or {}).get(name)
        if got is None:
            raise cas.GateError("%s recorded no %r extract" % (step_id, name))
        return got

    def probed():
        if probes is None:
            raise cas.GateError("no probes.json: the probe did not run (see browser-b/probe.log)")
        return probes

    def pipeline_hashes():
        exit_path = os.path.join(out, "selection.exit")
        status = "<none>"
        if os.path.isfile(exit_path):
            with open(exit_path) as fh:
                status = fh.read().strip()
        if status != "0":
            raise cas.GateError("the selection pipeline's exit status is %s (see browser-b/selection.log)" % status)
        return A._consumer_hashes(SimpleNamespace(store_out=cas.Store(os.path.join(out, "store-out"))))

    def verify_post(step_id, post, build):
        """The Gate's block for the request, and problems with its address, response and stored copy."""
        problems = []
        block = build(dagjson.loads(post["body"]), ASSERTED_BY)
        address = dagjson.address(block)
        if post.get("status") != 200:
            problems.append("%s: a %s POST answered %s: %s" % (step_id, block["kind"], post.get("status"), (post.get("responseBody") or "")[:200]))
            return address, block, problems
        answered = _text(_response(post).get("address"))
        if answered != address:
            problems.append("%s: the endpoint answered %s for a %s whose Gate address is %s" % (step_id, answered, block["kind"], address))
        try:
            stored = cas.decode(store.read(address))
        except cas.GateError as exc:
            problems.append("%s: %s" % (step_id, exc))
        else:
            if stored != block:
                problems.append("%s: block %s reads back as %r, the Gate built %r" % (step_id, address, stored, block))
        return address, block, problems

    results = []

    def run(number, title, fn):
        try:
            status, message = fn()
        except Exception as exc:  # noqa: BLE001, a crash in a check is a FAIL of that check
            status, message = FAIL, "%s: %s" % (type(exc).__name__, exc)
        results.append((status, number, title, message))

    def b8():
        problems, made = [], {}
        want_members = {
            "B.first": _members([_item(items["A"], [colls["aligned"]]), _item(items["B"], [colls["aligned"]])]),
        }
        for step_id in ("B.first", "B.second"):
            posts = [p for p in _posts(seen(step_id)) if _kind(p) == "Selection"]
            if len(posts) != 1:
                problems.append("%s: %d Selection POST(s) that write, expected 1" % (step_id, len(posts)))
                continue
            address, block, found = verify_post(step_id, posts[0], dagjson.expected_selection)
            problems += found
            written = extract(step_id, "after").get("written")
            if written != address:
                problems.append("%s: the page's data-written is %s, the Gate's address %s" % (step_id, written, address))
            made[step_id] = (address, block)
        if "B.first" in made:
            want_members["B.second"] = _members([{"selection": cas.Cid(made["B.first"][0])},
                                                 _item(items["B"], [colls["again"]]), _item(items["C"], [colls["stats"]])])
        for step_id, (address, block) in sorted(made.items()):
            if step_id in want_members and block["members"] != want_members[step_id]:
                problems.append("%s: members %r, the scenario picked %r" % (step_id, block["members"], want_members[step_id]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("both Selections (%s, and %s nesting it with B via `again` and C via `stats`) have the Gate's own "
                      "address, and read back from the store equal to the Gate's block"
                      % (made["B.first"][0][:16], made["B.second"][0][:16]))

    def b9():
        got = pipeline_hashes().get("fromstore") or {}
        problems = once_each("fromstore")
        if got != want_hashes:
            problems.append("fromStore(selection:) staged %s, expected exactly %s" % (json.dumps(got, sort_keys=True), json.dumps(want_hashes, sort_keys=True)))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("fromStore(selection: S2) staged A, B and C once each (A and B through the nested Selection, B also "
                      "directly) and every file hashes as pipeline-a's work file does")

    def b10():
        problems = []
        p = probed()
        s2_posts = [x for x in _posts(seen("B.second")) if _kind(x) == "Selection"]
        if len(s2_posts) != 1:
            raise cas.GateError("B.second recorded %d Selection POSTs that write, expected 1" % len(s2_posts))
        s2 = dagjson.address(dagjson.expected_selection(dagjson.loads(s2_posts[0]["body"]), ASSERTED_BY))
        verbs = {"B.rename": [("set", "name")], "B.delete": [("delete", None), ("del", None)]}
        claims = {}
        for step_id in ("B.first", "B.second", "B.rename", "B.delete"):
            posts = [x for x in _posts(seen(step_id)) if _kind(x) == "Claim"]
            if step_id in verbs:
                got = [(json.loads(x["body"]).get("verb"), json.loads(x["body"]).get("attribute")) for x in posts]
                if got != verbs[step_id]:
                    problems.append("%s: Claim POSTs %s, expected %s" % (step_id, got, verbs[step_id]))
            for post in posts:
                address, block, found = verify_post(step_id, post, dagjson.expected_claim)
                problems += found
                claims.setdefault(step_id, []).append(address)
                if step_id in verbs and _text(block["subject"]) != s2:
                    problems.append("%s: a %s Claim's subject is %s, not S2 %s" % (step_id, block["verb"], _text(block["subject"]), s2))
                if step_id == "B.delete" and block["verb"] == "del" and claims["B.delete"][0] not in [_text(x) for x in block["supersedes"]]:
                    problems.append("B.delete: the undo supersedes %s, not the delete Claim %s"
                                    % ([_text(x) for x in block["supersedes"]], claims["B.delete"][0]))
        rename = (claims.get("B.rename") or [None])[0]
        replay = p["replay"]
        if replay.get("status") != 200:
            problems.append("the replayed rename answered %s: %s" % (replay.get("status"), (replay.get("body") or "")[:200]))
        else:
            body = dagjson.loads(replay["body"])
            if body.get("written") is not False:
                problems.append("the replayed rename answered written: %r" % body.get("written"))
            if _text(body.get("address")) != rename:
                problems.append("the replay answered %s, the rename was %s" % (_text(body.get("address")), rename))
        if p.get("blocks_after_replay") != p.get("blocks_before"):
            problems.append("the replay changed the block count from %s to %s" % (p.get("blocks_before"), p.get("blocks_after_replay")))
        entries = [e for e in store.store_log() if e[2] == rename]
        if len(entries) != 1:
            problems.append("the store's log/ holds %d entries for the rename %s, expected 1" % (len(entries), rename))
        listed = {r["cid"] for r in extract("B.delete", "list").get("selections") or []}
        deleted = {r["cid"]: r["deletion"] for r in extract("B.delete", "deleted").get("selections") or []}
        if s2 in listed:
            problems.append("after the delete the Selection is still in the page's list")
        if deleted.get(s2) != "deleted":
            problems.append("after the delete the page's deleted list shows it as %r" % deleted.get(s2))
        if extract("B.delete", "after").get("deletion") != "none":
            problems.append("after the undo the page shows deletion %r" % extract("B.delete", "after").get("deletion"))
        state = dagjson.claim_state(_claims_about(store, s2))
        if state["deletion"] != "none":
            problems.append("the Gate's current state of S2 has deletion %r after the undo" % state["deletion"])
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("%d Claims (names, rename, delete, undo) at the Gate's addresses and in the store; the replayed "
                      "rename answered written: false with its address, no new block, one log entry"
                      % sum(len(v) for v in claims.values()))

    def b11():
        problems = []
        s2 = _saved(observed, "B.second")
        a, b = extract("B.conflict", "a"), extract("B.conflict", "b")
        if a.get("writeOutcome") != "written":
            problems.append("the first session's rename ended %r, expected written" % a.get("writeOutcome"))
        if b.get("writeOutcome") != "stale_supersedes":
            problems.append("the second session's rename ended %r, expected stale_supersedes" % b.get("writeOutcome"))
        if not any(e.get("error") == "stale_supersedes" for e in b.get("errors") or []):
            problems.append('the second session shows no [data-error="stale_supersedes"]: %s' % b.get("errors"))
        state = dagjson.claim_state(_claims_about(store, s2))
        if state["names"] != ["third-a"] or state["name_conflicted"]:
            problems.append("the Gate's current names of S2 are %s (conflicted: %s), expected [\"third-a\"]"
                            % (state["names"], state["name_conflicted"]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("the second session's rename was refused with stale_supersedes and shown; the Gate's own current "
                      "state of S2 is the one name third-a")

    def b12():
        p = probed()
        want = {"no_token": 403, "foreign_origin": 403, "text_plain": 415}
        got = {k: (p["refusals"].get(k) or {}).get("status") for k in want}
        problems = ["%s answered %s, expected %s" % (k, got[k], want[k]) for k in sorted(want) if got[k] != want[k]]
        fresh = p.get("refusal_address")
        if not fresh or not cas.is_cid(fresh):
            problems.append("probes.json names no address for the refused Selection (%r)" % fresh)
        elif store.has(fresh):
            problems.append("the store holds %s, the Selection only the refused POSTs sent" % fresh)
        if p.get("blocks_after") != p.get("blocks_after_replay"):
            problems.append("the refused POSTs changed the block count from %s to %s" % (p.get("blocks_after_replay"), p.get("blocks_after")))
        if p.get("log_after") != p.get("log_after_replay"):
            problems.append("the refused POSTs changed the log entry count from %s to %s" % (p.get("log_after_replay"), p.get("log_after")))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("no token 403, a foreign Origin 403, text/plain 415; the never-written Selection they sent is not "
                      "in the store, and no block or log entry was added")

    def b13():
        p = probed()
        problems = []
        for fmt in ("csv", "json"):
            if p.get("samplesheet_" + fmt) != 200:
                problems.append("the %s export answered %s" % (fmt, p.get("samplesheet_" + fmt)))
        want = ["cas://%s/%s" % (files[k]["cid"], files[k]["name"]) for k in sorted(items, key=lambda k: items[k])]
        if not problems:
            rows = _read_sheet_csv(os.path.join(out, "samplesheet.csv"))
            cells = [r.get("1") for r in rows]
            if cells != want:
                problems.append("the CSV's column 1 is %s, expected %s (one row per item, by item CID)" % (cells, want))
            rows = _read_json(os.path.join(out, "samplesheet.json"))
            cells = [r.get("1") for r in rows] if isinstance(rows, list) else rows
            if cells != want:
                problems.append("the JSON's \"1\" cells are %s, expected %s" % (cells, want))
        got = pipeline_hashes().get("samplesheet") or {}
        problems += once_each("samplesheet")
        if got != want_hashes:
            problems.append("the samplesheet's cells staged %s, expected %s" % (json.dumps(got, sort_keys=True), json.dumps(want_hashes, sort_keys=True)))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, "CSV and JSON list exactly A, B and C as cas://<cid>/<name>, and those cells stage and hash as expected"

    def b14():
        problems, sh = [], expected["shared"]
        offer = extract("B.copy", "offer")
        if offer.get("writeOutcome") != "elsewhere" or offer.get("heldElsewhere") != sh["s3"]:
            problems.append("the dry run offered %r for %r, expected elsewhere for S3 %s"
                            % (offer.get("writeOutcome"), offer.get("heldElsewhere"), sh["s3"]))
        if offer.get("composeName") != "from-shared":
            problems.append("the name field held %r, expected shared's name from-shared" % offer.get("composeName"))
        posts = _posts(seen("B.copy"))
        sels = [p for p in posts if _kind(p) == "Selection"]
        claims = [p for p in posts if _kind(p) == "Claim"]
        if len(sels) != 1 or len(claims) != 1:
            problems.append("B.copy: %d Selection and %d Claim POSTs that write, expected 1 and 1" % (len(sels), len(claims)))
        else:
            address, _block, found = verify_post("B.copy", sels[0], dagjson.expected_selection)
            problems += found
            if address != sh["s3"]:
                problems.append("B.copy wrote %s, the Selection shared holds is %s" % (address, sh["s3"]))
            _c, block, found = verify_post("B.copy", claims[0], dagjson.expected_claim)
            problems += found
            if block["value"] != "from-shared" or [_text(x) for x in block["supersedes"]] != [sh["n_s"]]:
                problems.append("the copy's name Claim is %r superseding %r, expected from-shared superseding %s"
                                % (block["value"], [_text(x) for x in block["supersedes"]], sh["n_s"]))
        if len([e for e in store.store_log() if e[1] == "selection" and e[2] == sh["s3"]]) != 1:
            problems.append("lab's log/ does not hold exactly one selection entry for S3")
        state = dagjson.claim_state(_claims_about(store, sh["s3"]) + _claims_about(shared_store, sh["s3"]))
        if state["names"] != ["from-shared"] or state["name_conflicted"]:
            problems.append("across both members S3 is named %r (conflicted %r), expected from-shared alone"
                            % (state["names"], state["name_conflicted"]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a Selection only shared held was offered as a copy with shared's name prefilled; one click wrote it "
                      "into lab at the Gate's address with a name Claim superseding shared's, one current name across both")

    def b15():
        problems, sh = [], expected["shared"]
        before = [(n["name"], n["claim"]) for n in extract("B.foreign", "before").get("names") or []]
        if before != [("lab-name", sh["n1"])]:
            problems.append("lab's view of S4 showed %r, expected lab-name from %s" % (before, sh["n1"]))
        claims = [p for p in _posts(seen("B.foreign")) if _kind(p) == "Claim"]
        if len(claims) != 1:
            problems.append("B.foreign: %d Claim POSTs that write, expected 1" % len(claims))
        else:
            _c, block, found = verify_post("B.foreign", claims[0], dagjson.expected_claim)
            problems += found
            if [_text(x) for x in block["supersedes"]] != [sh["n1"]]:
                problems.append("the rename supersedes %r, expected lab's %s" % ([_text(x) for x in block["supersedes"]], sh["n1"]))
        state = dagjson.claim_state(_claims_about(store, sh["s4"]) + _claims_about(shared_store, sh["s4"]))
        if sorted(state["names"]) != ["lab-renamed", "shared-name"] or not state["name_conflicted"]:
            problems.append("across both members S4 is named %r (conflicted %r), expected a conflict of lab-renamed and shared-name"
                            % (state["names"], state["name_conflicted"]))
        after = [(n["name"], n["conflicted"]) for n in extract("B.foreign", "after").get("names") or []]
        if after != [("lab-renamed", False)]:
            problems.append("lab's view after the rename showed %r, expected lab-renamed alone" % after)
        dry = probed().get("foreign_dry") or {}
        body = dagjson.loads(dry["body"]) if dry.get("status") == 200 else {}
        if body.get("here") is not True or sorted(body.get("names") or []) != ["lab-renamed", "shared-name"]:
            problems.append("the endpoint's dry run of S4 answered %s %r, expected here and names lab-renamed, shared-name"
                            % (dry.get("status"), body))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a rename in lab went through although shared holds a Claim superseding lab's; the composition "
                      "reports the two names as a conflict and lab's view shows its own")

    def b16():
        problems, sh = [], expected["shared"]
        offer = extract("B.restore", "offer")
        if offer.get("heldElsewhere") != sh["s5"] or offer.get("heldElsewhereDeletion") != "deleted":
            problems.append("the dry run offered %r (deletion %r), expected S5 %s deleted in shared"
                            % (offer.get("heldElsewhere"), offer.get("heldElsewhereDeletion"), sh["s5"]))
        if offer.get("copyLabel") != "Restore a copy here":
            problems.append("the copy button read %r, expected 'Restore a copy here'" % offer.get("copyLabel"))
        if offer.get("composeName") != "restored-name":
            problems.append("the name field held %r, expected shared's name restored-name" % offer.get("composeName"))
        posts = _posts(seen("B.restore"))
        sels = [p for p in posts if _kind(p) == "Selection"]
        claims = [p for p in posts if _kind(p) == "Claim"]
        verbs = [json.loads(p["body"]).get("verb") for p in claims]
        if len(sels) != 1 or verbs != ["set", "del"]:
            problems.append("B.restore: %d Selection POST(s) and Claim verbs %r, expected 1 and ['set', 'del']" % (len(sels), verbs))
        else:
            address, _b, found = verify_post("B.restore", sels[0], dagjson.expected_selection)
            problems += found
            if address != sh["s5"]:
                problems.append("B.restore wrote %s, the Selection shared holds is %s" % (address, sh["s5"]))
            for post in claims:
                _c, block, found = verify_post("B.restore", post, dagjson.expected_claim)
                problems += found
                if _text(block["subject"]) != sh["s5"]:
                    problems.append("a %s Claim is about %s, not S5" % (block["verb"], _text(block["subject"])))
                want = [sh["n5"]] if block["verb"] == "set" else [sh["d5"]]
                if [_text(x) for x in block["supersedes"]] != want:
                    problems.append("the %s Claim supersedes %r, expected %r" % (block["verb"], [_text(x) for x in block["supersedes"]], want))
        state = dagjson.claim_state(_claims_about(store, sh["s5"]) + _claims_about(shared_store, sh["s5"]))
        if state["names"] != ["restored-name"] or state["deletion"] != "none":
            problems.append("across both members S5 is named %r with deletion %r, expected restored-name and none"
                            % (state["names"], state["deletion"]))
        after = extract("B.restore", "after")
        if after.get("view") != sh["s5"] or after.get("deletion") != "none":
            problems.append("lab's view after the restore showed %r with deletion %r, expected S5 %s and none"
                            % (after.get("view"), after.get("deletion"), sh["s5"]))
        names = after.get("names") or []
        if [n["name"] for n in names] != ["restored-name"] or any(n["conflicted"] for n in names):
            problems.append("lab's view after the restore named %r, expected restored-name alone and unconflicted"
                            % [(n["name"], n["conflicted"]) for n in names])
        held = extract("B.deleted", "offer")
        if held.get("exists") != sh["s6"] or held.get("existsDeletion") != "deleted":
            problems.append("composing S6 showed exists %r with deletion %r, expected S6 %s deleted"
                            % (held.get("exists"), held.get("existsDeletion"), sh["s6"]))
        posts = _posts(seen("B.deleted"))
        verbs = [json.loads(p["body"]).get("verb") if _kind(p) == "Claim" else _kind(p) for p in posts]
        if verbs != ["del"]:
            problems.append("B.deleted: %d writing POST(s) %r, expected one del Claim" % (len(posts), verbs))
        else:
            _c, block, found = verify_post("B.deleted", posts[0], dagjson.expected_claim)
            problems += found
            if _text(block["subject"]) != sh["s6"]:
                problems.append("the exists path's del is about %s, not S6" % _text(block["subject"]))
            if [_text(x) for x in block["supersedes"]] != [sh["d6"]]:
                problems.append("the exists path's del supersedes %r, expected shared's %s"
                                % ([_text(x) for x in block["supersedes"]], sh["d6"]))
        state = dagjson.claim_state(_claims_about(store, sh["s6"]) + _claims_about(shared_store, sh["s6"]))
        if state["deletion"] != "none":
            problems.append("across both members S6 has deletion %r, expected none" % state["deletion"])
        restored = extract("B.deleted", "after")
        if restored.get("view") != sh["s6"] or restored.get("deletion") != "none":
            problems.append("lab's view after Restore on the exists path showed %r with deletion %r, expected S6 %s and none"
                            % (restored.get("view"), restored.get("deletion"), sh["s6"]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a Selection shared named and deleted was offered as 'Restore a copy here' with its name prefilled; one "
                      "click wrote it into lab with a name Claim and a del superseding shared's, live across both members; "
                      "composing a Selection lab holds and shared deleted named the deletion, and the 'already exists' "
                      "path's Restore wrote one del superseding shared's, live across both members")

    run(8, "a Selection made in the page has the Gate's own address", b8)
    run(9, "fromStore(selection:) receives each distinct item once, nested included", b9)
    run(10, "rename, delete and undo are Claims at the Gate's addresses; a replay writes nothing", b10)
    run(11, "two sessions renaming from one view: a surfaced conflict, not an overwrite", b11)
    run(12, "a POST without the token, from another Origin, or as text/plain is refused", b12)
    run(13, "the samplesheet lists exactly the Selection's items, and its cells stage", b13)
    run(14, "a Selection held only in a read-only member is copied and named", b14)
    run(15, "a Claim in another member does not lock a rename; the disagreement is a conflict", b15)
    run(16, "a copy deleted in another member is restored; a deletion held elsewhere is named", b16)
    return results


def check(root):
    results = evaluate(root)
    width = max(len(r[2]) for r in results)
    for status, number, title, message in results:
        print("%-4s  B%-2d %-*s  %s" % (status, number, width, title, A.wrap(message, 92, 4 + 2 + 3 + 1 + width + 2)))
    failures = sum(1 for r in results if r[0] == FAIL)
    print("")
    print("browser tier B: %d PASS, %d FAIL" % (len(results) - failures, failures))
    return 1 if failures else 0


def main(argv):
    if len(argv) == 3 and argv[1] in ("prepare", "check"):
        return {"prepare": prepare, "check": check}[argv[1]](argv[2])
    if len(argv) == 5 and argv[1] == "probe":
        return probe(argv[2], argv[3], argv[4])
    sys.stderr.write(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
