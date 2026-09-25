#!/usr/bin/env python3
"""Gate browser tier A, local part (block explorer spec section 1.3).

    python3 gate/browser_assert.py prepare <GATE_ROOT>
    python3 gate/browser_assert.py check <GATE_ROOT>

prepare lays out $GATE_ROOT/browser/site, works out every expected answer from
the Gate's own hashes and its own read of the blocks (never the plugin's index
or snapshot), and writes the scenario drive.mjs plays. check compares what the
browser saw with those answers. Request counts come from Playwright's network
events, not from the page.

Assertions 6 and 7 are the cloud part (gate/cloud/cloud.sh).
"""
import hashlib
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cas  # noqa: E402
import gen_year  # noqa: E402
import year_params  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
SNAPSHOT = "index/v2.sqlite"
POINT_LIMIT = (8, 65536)          # requests, bytes: spec section 1.3 assertion 2
QUERY3_LIMIT = (50, 524288)
YEAR_RUNS = 5 * 365

# The index's SQL, copied from Index.groovy so the Gate's year answers come from
# sqlite3, not from the page's queries.json.
YEAR_SQL = {
    "producersOf": "SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?",
    "latestSuccessfulRun": "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' "
                           "AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1",
    "itemsWhere": "SELECT ci.item_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid "
                  "WHERE c.completion_cid = ? AND c.output_name = ? AND EXISTS (SELECT 1 FROM item_attr a "
                  "WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?) "
                  "ORDER BY ci.item_cid",
}


def _load_assert():
    spec = importlib.util.spec_from_file_location("gate_assert", os.path.join(HERE, "assert.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


A = _load_assert()


# --------------------------------------------------------------------------
# prepare
# --------------------------------------------------------------------------

def member_copy(store_root, target, snapshot=None):
    """What a member publishes (DESIGN.md §15): blocks, log, snapshot, page. Never coords/ or nf/."""
    os.makedirs(os.path.join(target, "index"))
    shutil.copytree(os.path.join(store_root, "blocks"), os.path.join(target, "blocks"))
    shutil.copytree(os.path.join(store_root, "log"), os.path.join(target, "log"))
    shutil.copy(snapshot or os.path.join(store_root, SNAPSHOT), os.path.join(target, SNAPSHOT))
    page = os.path.join(store_root, "index.html")
    if not os.path.isfile(page):
        raise cas.GateError("no index.html beside the snapshot in %s; every run writes it (DESIGN.md §15)" % store_root)
    shutil.copy(page, os.path.join(target, "index.html"))
    return target


def tamper(path):
    """Changes one byte inside a string of the block, so it still decodes but no longer hashes."""
    os.chmod(path, 0o644)
    with open(path, "rb") as fh:
        data = fh.read()
    at = data.find(b"succeeded")
    if at < 0:
        raise cas.GateError("cannot tamper %s: no 'succeeded' in it" % path)
    with open(path, "wb") as fh:
        fh.write(data[:at] + b"succeedee" + data[at + 9:])


def snapshot_runs(path):
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return {r[0] for r in con.execute("SELECT completion_cid FROM run")}
    finally:
        con.close()


def year_snapshot(schema_path):
    """The year snapshot, cached by the generator's source and the schema, since it takes minutes."""
    cache = os.environ.get("GATE_YEAR_CACHE") or os.path.join(tempfile.gettempdir(), "nf-blocks-gate-year")
    os.makedirs(cache, exist_ok=True)
    with open(os.path.join(HERE, "gen_year.py"), "rb") as fh:
        key = hashlib.sha256(fh.read() + "\n".join(gen_year.ddl_of(schema_path)).encode()).hexdigest()[:16]
    path = os.path.join(cache, "year-%s.sqlite" % key)
    if not os.path.isfile(path):
        tmp = path + ".tmp"
        gen_year.build(schema_path, tmp, YEAR_RUNS)
        os.replace(tmp, path)
    return path


def year_answers(path):
    params = year_params.params(path, 0)
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        producers = sorted(list(r) for r in con.execute(YEAR_SQL["producersOf"], params["producersOf"]))
        latest = con.execute(YEAR_SQL["latestSuccessfulRun"], params["latestSuccessfulRun"]).fetchone()
        items = [r[0] for r in con.execute(YEAR_SQL["itemsWhere"], params["itemsWhere"])]
    finally:
        con.close()
    return {"params": params, "producers": producers, "latest": latest[0] if latest else None, "items": items}


def producers(gate, content):
    out = set()
    for run in gate.runs.values():
        if not run.completion:
            continue
        for _name, (collection_cid, block) in run.collections(gate).items():
            for link in block.get("items") or []:
                if link is None:
                    continue
                item = gate.block(link.text) or {}
                for leaf in A._leaves(item.get("value")):
                    if A._address_text(leaf.get("address")) == content:
                        out.add((content, link.text, collection_cid, run.completion_cid, leaf.get("name")))
    return sorted(list(t) for t in out)


def items(gate, run, output, key, value):
    return sorted(cid for cid, block in run.items(gate, output)
                  if isinstance(A.metadata_view(block).get(key), str) and A.metadata_view(block).get(key) == value)


def where(key, value):
    """The page's query 3 filter (DESIGN.md §15), URL-encoded for the location hash."""
    return "?where=" + urllib.parse.quote(json.dumps([[key, "string", value]], separators=(",", ":")), safe="")


def prepare(root):
    gate = A.Gate(root)
    out = os.path.join(root, "browser")
    site = os.path.join(out, "site")
    shutil.rmtree(site, ignore_errors=True)
    store = gate.store.root
    after_fail = os.path.join(root, "snapshot-after-fail.sqlite")
    if not os.path.isfile(after_fail):
        raise cas.GateError("no %s: gate.sh keeps the snapshot the `fail` run wrote" % after_fail)
    cold, resumed, elsewhere = gate.run("cold"), gate.run("resumed"), gate.run("elsewhere")
    held = snapshot_runs(after_fail)
    if resumed.completion_cid in held or elsewhere.completion_cid in held:
        raise cas.GateError("the snapshot kept after `fail` already holds a later run; it cannot be two runs stale")

    member_copy(store, os.path.join(site, "stores", "current"))
    member_copy(store, os.path.join(site, "stores", "stale"), after_fail)
    tampered = member_copy(store, os.path.join(site, "stores", "tampered"), after_fail)
    tamper(os.path.join(tampered, "blocks", elsewhere.completion_cid[-2:], elsewhere.completion_cid))
    year = year_snapshot(os.path.join(store, SNAPSHOT))
    os.makedirs(os.path.join(site, "stores", "year", "index"))
    os.symlink(year, os.path.join(site, "stores", "year", SNAPSHOT))

    bams = gate.work_files("pipeline-a", "A.bam")
    if not bams:
        raise cas.GateError("no A.bam under pipeline-a/work to hash")
    content = cas.cid_of_file(bams[0])
    y = year_answers(year)
    expected = {
        "content": content,
        "producers": producers(gate, content),
        "latest": A._latest_successful_from_store_log(gate, A.PIPELINE_IDENTITY),
        "items": items(gate, cold, "aligned", "sample", "B"),
        "stale_runs": sorted([resumed.completion_cid, elsewhere.completion_cid]),
        "stale_items": items(gate, elsewhere, "aligned", "sample", "B"),
        "tampered": elsewhere.completion_cid,
        "year": y,
    }
    p = y["params"]
    year_query = "?store={range}/stores/year/"
    steps = []
    for server, path in (("range", "stores/current/index.html"), ("explore", "")):
        steps += [
            {"id": "A1.producers.%s" % server, "server": server, "path": path, "hash": "#/content/%s" % content},
            {"id": "A1.latest.%s" % server, "server": server, "path": path, "hash": "#/latest/%s" % A.PIPELINE_IDENTITY},
            {"id": "A1.items.%s" % server, "server": server, "path": path,
             "hash": "#/items/%s/aligned%s" % (cold.completion_cid, where("sample", "B"))},
        ]
    steps += [
        {"id": "A2.producers", "server": "range", "path": "stores/current/index.html", "query": year_query, "idle": True,
         "hash": "#/content/%s" % p["producersOf"][0]},
        {"id": "A2.latest", "server": "range", "path": "stores/current/index.html", "query": year_query, "idle": True,
         "hash": "#/latest/%s" % p["latestSuccessfulRun"][0]},
        {"id": "A2.items", "server": "range", "path": "stores/current/index.html", "query": year_query, "idle": True,
         "hash": "#/items/%s/%s%s" % (p["itemsWhere"][0], p["itemsWhere"][1], where("sample", p["itemsWhere"][4]))},
        {"id": "A3.runs", "server": "range", "path": "stores/stale/index.html", "hash": "#/pipeline/%s" % A.PIPELINE_IDENTITY},
        {"id": "A3.items", "server": "range", "path": "stores/stale/index.html",
         "hash": "#/items/%s/aligned%s" % (elsewhere.completion_cid, where("sample", "B"))},
        {"id": "A4.tampered.home", "server": "range", "path": "stores/tampered/index.html", "hash": "#/"},
        {"id": "A4.tampered.runs", "server": "range", "path": "stores/tampered/index.html",
         "hash": "#/pipeline/%s" % A.PIPELINE_IDENTITY},
        {"id": "A5.whole", "server": "plain", "path": "stores/current/index.html", "hash": "#/content/%s" % content},
        {"id": "A5.over-cap", "server": "plain", "path": "stores/current/index.html",
         "query": "?store={plain}/stores/year/", "hash": "#/idle"},
    ]
    with open(os.path.join(out, "expected.json"), "w") as fh:
        json.dump(expected, fh, indent=1)
    with open(os.path.join(out, "scenario.json"), "w") as fh:
        json.dump({"steps": steps}, fh, indent=1)
    print("browser tier: %d steps prepared under %s" % (len(steps), site))
    return 0


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------

def query_cost(step, path_suffix):
    reads = [r for r in step.get("requests", [])
             if r.get("phase") == "query" and r.get("url", "").split("?")[0].endswith(path_suffix)]
    return len(reads), sum(r.get("bytes") or 0 for r in reads)


def within(cost, limit):
    return cost[0] <= limit[0] and cost[1] <= limit[1]


def unverified(step):
    """Blocks fetched with 200 whose hash the page neither accepted (verified)
    nor refused with hash_mismatch: both mean the page hashed the bytes."""
    fetched = set()
    for r in step.get("requests", []):
        parts = r.get("url", "").split("?")[0].split("/")
        if len(parts) >= 3 and parts[-3] == "blocks" and r.get("status") == 200 and cas.is_cid(parts[-1]):
            fetched.add(parts[-1])
    refused = {e.get("cid") for e in step.get("errors") or [] if e.get("error") == "hash_mismatch"}
    return sorted(fetched - set(step.get("verified") or []) - refused)


def producer_set(rows):
    return {(r["content"], r["item"], r["collection"], r["completion"], r["filename"]) for r in rows}


def check(root):
    out = os.path.join(root, "browser")
    with open(os.path.join(out, "expected.json")) as fh:
        expected = json.load(fh)
    observed = {}
    path = os.path.join(out, "observed.json")
    if os.path.isfile(path):
        with open(path) as fh:
            observed = {s["id"]: s for s in json.load(fh)["steps"]}

    def seen(step_id):
        if step_id not in observed:
            raise cas.GateError("the driver recorded no step %s (see browser/drive.log)" % step_id)
        s = observed[step_id]
        if s.get("pageErrors"):
            raise cas.GateError("%s: page errors %s" % (step_id, s["pageErrors"][:3]))
        return s

    results = []

    def run(number, title, fn):
        try:
            status, message = fn()
        except Exception as exc:  # noqa: BLE001, a crash in a check is a FAIL of that check
            status, message = FAIL, "%s: %s" % (type(exc).__name__, exc)
        results.append((status, number, title, message))

    def a1():
        problems = []
        want = {tuple(t) for t in expected["producers"]}
        for server in ("range", "explore"):
            s = seen("A1.producers.%s" % server)
            if s["state"] != "ready" or producer_set(s["producers"]) != want:
                problems.append("%s: producers-of gave %d rows (state %s), expected %d"
                                % (server, len(s["producers"]), s["state"], len(want)))
            s = seen("A1.latest.%s" % server)
            if s["latest"] != expected["latest"]:
                problems.append("%s: latest successful run %s, expected %s" % (server, s["latest"], expected["latest"]))
            s = seen("A1.items.%s" % server)
            if s["items"] != expected["items"]:
                problems.append("%s: query 3 gave %s, expected %s" % (server, s["items"], expected["items"]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("producers-of (%d rows), latest successful run and query 3 (%d items) equal the Gate's own "
                      "answers, through a static server and through nf-blocks:explore" % (len(want), len(expected["items"])))

    def a2():
        year = expected["year"]
        problems, costs = [], []
        for step_id, limit, key in (("A2.producers", POINT_LIMIT, "producers"), ("A2.latest", POINT_LIMIT, "latest"),
                                    ("A2.items", QUERY3_LIMIT, "items")):
            s = seen(step_id)
            cost = query_cost(s, "/stores/year/" + SNAPSHOT)
            costs.append("%s %d req / %d B" % (step_id.split(".")[1], cost[0], cost[1]))
            if not within(cost, limit):
                problems.append("%s cost %d requests / %d bytes, limit %d / %d" % (step_id, cost[0], cost[1], limit[0], limit[1]))
            if key == "producers":
                right = producer_set(s["producers"]) == {tuple(r) for r in year["producers"]}
                answer = "%d rows" % len(s["producers"])
            else:
                right = s[key] == year[key]
                answer = s[key]
            if not right:
                problems.append("%s answered %s, sqlite3 over the same file says %s"
                                % (step_id, str(answer)[:120], str(year[key])[:120]))
        return (FAIL, "; ".join(problems)) if problems else (PASS, "year snapshot, cold: " + ", ".join(costs))

    def a3():
        runs = seen("A3.runs")
        tail = {r["run"] for r in runs["runs"] if r["source"] == "tail"}
        problems = []
        if not set(expected["stale_runs"]) <= tail:
            problems.append("tail runs %s, expected %s among them" % (sorted(tail), expected["stale_runs"]))
        if runs.get("stale") != "2":
            problems.append("the stale notice counts %s, expected 2" % runs.get("stale"))
        s = seen("A3.items")
        if s["items"] != expected["stale_items"]:
            problems.append("query 3 over the stale run gave %s, expected %s" % (s["items"], expected["stale_items"]))
        return (FAIL, "; ".join(problems)) if problems else (PASS, "both runs newer than the snapshot listed, notice counts 2, "
                                                                   "query 3 over one of them right after fetching its closure")

    def a4():
        problems = []
        for step_id, s in sorted(observed.items()):
            missing = unverified(s)
            if missing:
                problems.append("%s fetched %d block(s) it never verified: %s" % (step_id, len(missing), missing[:2]))
        home = seen("A4.tampered.home")
        if not any(e["error"] == "hash_mismatch" and e["cid"] == expected["tampered"] for e in home["errors"]):
            problems.append("the tampered RunCompletion %s was not refused with hash_mismatch: %s"
                            % (expected["tampered"][:16], home["errors"]))
        if any(r["run"] == expected["tampered"] for r in seen("A4.tampered.runs")["runs"]):
            problems.append("the tampered run is listed as a run")
        verified = sum(len(s.get("verified") or []) for s in observed.values())
        return (FAIL, "; ".join(problems)) if problems else (PASS, "%d block fetches, every one hash-verified in the browser; "
                                                                   "a tampered block refused" % verified)

    def a5():
        problems = []
        s = seen("A5.whole")
        if s["mode"] != "whole" or producer_set(s["producers"]) != {tuple(t) for t in expected["producers"]}:
            problems.append("without Range: mode %s, %d producers" % (s["mode"], len(s["producers"])))
        if any(r.get("status") == 206 for r in s["requests"]):
            problems.append("a server without Range answered 206")
        s = seen("A5.over-cap")
        if s["state"] != "error" or not any(e["error"] == "no_range_over_cap" for e in s["errors"]):
            problems.append("the year snapshot without Range was not refused with no_range_over_cap: state %s, errors %s"
                            % (s["state"], s["errors"]))
        return (FAIL, "; ".join(problems)) if problems else (PASS, "without Range the snapshot is downloaded whole under the cap "
                                                                   "and refused over it, with the reason shown")

    run(1, "the page answers the three load-bearing queries", a1)
    run(2, "year-scale cold queries within the request and byte limits", a2)
    run(3, "a snapshot two runs stale", a3)
    run(4, "every fetched block is hash-verified in the browser", a4)
    run(5, "whole-file fallback without Range, refused over the cap", a5)

    width = max(len(r[2]) for r in results)
    for status, number, title, message in results:
        print("%-4s  A%d  %-*s  %s" % (status, number, width, title, A.wrap(message, 92, 4 + 2 + 2 + 2 + width + 2)))
    failures = sum(1 for r in results if r[0] == FAIL)
    print("")
    print("browser tier A (local): %d PASS, %d FAIL" % (len(results) - failures, failures))
    return 1 if failures else 0


def main(argv):
    if len(argv) != 3 or argv[1] not in ("prepare", "check"):
        sys.stderr.write(__doc__)
        return 2
    return prepare(argv[2]) if argv[1] == "prepare" else check(argv[2])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
