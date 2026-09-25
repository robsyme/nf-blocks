# Block Explorer Milestone 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every task is bound by `DESIGN.md`; when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets fixed.

**Goal:** A read-only browser explorer for a Block Store member: the Index Snapshot writer, our own HTTP VFS on `@sqlite.org/sqlite-wasm`, the page, `nf-blocks:explore` and `nf-blocks:snapshot`, and the Gate browser tier A (local and cloud parts), so milestone 1 of the block explorer spec passes its Gate tier.

**Architecture:** A run (and two new plugin verbs) copies the writable member's own rows out of the per-user cache index into `<member>/index/v2.sqlite` with `VACUUM INTO`, and writes the self-contained page beside it as `<member>/index.html`. The page opens that file over HTTP range requests through a read-only SQLite VFS running in a Worker with synchronous XHR, runs the same SQL the plugin runs (shared through `web/src/queries.json` and pinned by a Spock test), and fills in runs newer than the snapshot by listing the Store Log and fetching hash-verified blocks. `nf-blocks:explore` is a loopback JDK `HttpServer` that serves the page and every configured member with `Range`, reading local members from disk and private S3 members with the AWS SDK v2. The Gate drives Playwright's pinned Chromium against the store real Nextflow wrote and compares every answer with its own Python computation.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, `org.xerial:sqlite-jdbc` 3.50.3.0, Gradle with `io.nextflow.nextflow-plugin` 1.0.0-beta.15 and `com.github.node-gradle.node` 7.1.0, Nextflow 26.04.6, AWS SDK for Java v2 2.46.7, Node 24.21.0, esbuild 0.28.2, `@sqlite.org/sqlite-wasm` 3.53.4-build1, `@ipld/dag-cbor` 10.0.2, `cborg` 6.1.2, `@ipld/schema` 7.0.12, `multiformats` 14.0.5, Playwright 1.63.0, Python 3 stdlib for the Gate.

**Spec:** `../.scratch/block-explorer/spec.md` sections 1.1, 1.3, 1.4, 2, 4, 5 (5.1 to 5.5), 6, 12 and 15. Read sections 1, 4, 5 and 13 before starting. The v1 prerequisites (spec section 1.1 item 1) are already merged at `c9ca9dd`; do not redo them.

**Branch:** `git switch -c feat/explorer-m1 main` in `nf-blocks/`.

**Prototype branch:** `prototype/index-snapshot-http` holds `gen_year.py`, `serve.py`, `s3real.py`, the reference harness and the schema validators. Tasks 1, 2 and 13 move what milestone 1 keeps; read the originals with `git show prototype/index-snapshot-http:prototype-snapshot-http/<file>`. Do not merge or delete that branch; Task 14 asks Rob.

## Decisions this plan makes where the spec is silent

Each is also written into `DESIGN.md` §15 by Task 3, so reviewers can reject one there.

1. **Snapshot rows in milestone 1** are the runs whose `run.member` is the writable alias, with their collections, items, producers, attributes and `missing` rows. The spec's rule starts from `log_entry` rows with `member = M`; `log_entry` lands in milestone 2 (spec section 11), and `run.member` is the member whose Store Log or blocks the run was found in, so it is the same set in v1. The snapshot stores `run.member` as NULL, since an alias is a local label.
2. **`nf-blocks:explore` writes the writable member's snapshot at start and at exit**, at any size. The spec says "on exit", and its stale notice tells users that opening the member through `explore` rewrites the snapshot, which only holds if the rewrite also happens at start.
3. **The tail fetches a stale run's RunManifest as well as its RunCompletion**: the run list groups by pipeline, and the pipeline name is only in the manifest. Two blocks of 280 to 400 bytes instead of one.
4. **Query 3 in the page is per run** (a run picked from the list, or the latest successful run), which is `Index.items` and the query the reference numbers measured. Matching across every run of a pipeline needs milestone 2's `collection_item(item_cid)` index.
5. **The run list shows anomalies** from each visible run's RunCompletion, fetched and verified lazily, because the index carries no anomaly counts and a schema bump is out of scope.
6. **The page is one self-contained `index.html`**: the app, the Worker source and `sqlite3.wasm` (base64) are inlined, and the Worker is created from a `Blob`. Task 2 proves this; if it cannot work, Task 2 stops and says so.
7. **The page's whole-file cap** is 64 MiB (`cas.snapshot.maxBytes`' default), overridable per page load with `?cap=<bytes>`, because a static page cannot read the plugin's config.
8. **`explore` needs no launch token in milestone 1**: every request is a `GET` or `HEAD`, and the server refuses a foreign `Host` or `Origin` (spec section 9.5), which is what stops a DNS-rebinding page from reading a private bucket through it. The token arrives with the write endpoint in milestone 2.
9. **S3 members are read-only and explore-only in milestone 1** (Rob, 2026-09-25): `cas.stores.<alias>.location = 's3://bucket/prefix'` is accepted for a read-only member; a run leaves such members out of its default `resolve` list and refuses one named in `cas.resolve`, because no S3 `BlockStore` exists yet.
10. **Nothing is filtered by `delete` Claims yet.** Spec section 5.5 hides deleted Selections and runs by current `delete` Claims; Claims arrive with milestone 2's wave 0, so milestone 1 has none to apply and the page shows every run.

## Global Constraints

- Released dependencies only. Every npm dependency is pinned to an exact version in `package.json`, and `dependencyCheck` fails on a lockfile entry not resolved from `https://registry.npmjs.org/` (spec section 1.4).
- Nextflow facts are read from tag `v26.04.6` in `/Users/robsyme/dev/github.com/nextflow-io/nextflow`; the plugin builds against published artifacts only.
- The no-UI rule is lifted for the explorer only; nothing else gains a command-line or web surface (spec section 1.4, DESIGN §0 rule 5).
- One encoder: the page decodes and hashes blocks and never encodes one (spec section 1.4).
- The page never holds credentials. Private buckets go through `nf-blocks:explore` (spec section 1.4).
- One user, one machine: `explore` binds `InetAddress.getLoopbackAddress()` only (spec section 1.4).
- Snapshot file: `<member>/index/v<schema_version>.sqlite`, today `index/v2.sqlite`; `PRAGMA page_size=4096` before `VACUUM INTO`; a single rollback-journal file (header bytes 18 and 19 both `1`) with no `-wal`, `-shm` or `-journal` sidecar; replaced by an atomic move; mode 0644 (spec section 4).
- The page beside it: `<member>/index.html` (spec section 4).
- Snapshot `meta` keys: `store_log_watermark` (a Store Log entry name, absent when the member has no log) and `snapshot_written_at` (ISO-8601 UTC, millisecond precision).
- Cap: `cas.snapshot.maxBytes`, default `67108864` (64 MiB); a run writes the snapshot only while it is under the cap; `explore` and `snapshot` write at any size (spec section 4).
- Store Log overlap: `600000` ms before the watermark, floor clamped to the local clock, exactly as `StoreLog.entriesSince` (spec section 3).
- Stale notice: always shows the count of runs newer than the snapshot; past `20` stale runs, or when a query would need more than `2000` block fetches, it shows `nextflow plugin nf-blocks:snapshot` and `nextflow plugin nf-blocks:explore` (spec section 5.4).
- Gate limits on the year-scale snapshot, cold, per query: producers-of and latest successful run at most `8` requests and `65536` bytes each; query 3 at most `50` requests and `524288` bytes (spec section 1.3 assertion 2).
- Reference numbers the VFS must match (spec section 5.2, `sql.js-httpvfs` on the indexed year snapshot): producers-of 7 requests / 28 KB, latest successful run 5 / 20 KB, query 3 42 / 180 KB, warm 0 / 0.
- Year snapshot: `gate/gen_year.py`, `random.seed(42)`, 1,825 runs, 20 pipelines, 4 outputs, 100 items per run, 3 files per item, 15 Meta Map keys, page size 4096 (spec section 1.3 assertion 2).
- Every block the page uses is fetched from `<base>blocks/<last two chars of cid>/<cid>` and its SHA-256 checked against the requested CID before it is decoded (spec section 5.3).
- The page is plain DOM modules. No UI framework, no CSS framework, no router library.
- The page's SQL lives in `web/src/queries.json`; the three load-bearing queries there are character-for-character the `Index` constants (Task 4 test).
- Plugin verbs: `nextflow plugin nf-blocks:explore [--port <n>]` and `nextflow plugin nf-blocks:snapshot`. Exit status 0 on success, 1 on a failure the verb reports, 2 on a usage error.
- Failures in derived structures (index, Store Log, snapshot, page) log at warn and never abort a run (DESIGN §0 rule 3).
- Gate assertions never trust the plugin or the page: answers come from the Gate's own hashes and block reads, request counts from Playwright's network events (DESIGN §0 rule 6).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

The five inputs the spec implies, no task's main tests exercise, and a user is most likely to meet. Each line names the task whose test pins it.

1. **A snapshot answered `200` with no `Content-Length`** (a chunked static server or proxy): the page streams it and stops at the cap with `no_range_over_cap`, never buffering the whole file first. Pinned in Task 2 (`probe.test.mjs`).
2. **A Store Log entry for a run the snapshot already holds** (the same run inside the overlap window, or logged twice by a retried append): not counted as stale, not fetched, listed once. Pinned in Task 10 (`model.test.mjs`).
3. **A Meta Map float in a stale run that is integral or large** (`30.0`, `1.0E10`, `1.23456789E7`): query 3 over the stale run matches exactly what `Index.items` would, because the page reproduces Groovy's `float` typing and `Double.toString` text. Pinned in Task 10 (`metadata.test.mjs`).
4. **A request to `explore` that reaches outside what a member publishes** (a foreign `Host`, `..` in the path, `coords/`, `nf/`, a block path whose shard does not match its CID): `403` or `404`, never file bytes; `nf/` records hold absolute host paths. Pinned in Task 7 (`ExploreServerTest`).
5. **Two runs finishing at once, both writing the snapshot**: the file on disk is always a complete database that passes `PRAGMA integrity_check`, never a torn write. Pinned in Task 5 (`IndexSnapshotTest`).

## File Structure

```
nf-blocks/
  DESIGN.md                                   §2, §5, §12 amended; new §15 Explorer (Task 3)
  build.gradle                                node plugin, web tasks, AWS SDK, lockfile check (Tasks 8, 9)
  .gitignore                                  node_modules/, web/dist/, web/src/generated/ (Task 1)
  Makefile                                    gate-cloud target (Task 13)
  src/main/groovy/robsyme/cas/
    CasPlugin.groovy                          implements PluginExecAware (Task 6)
    CasConfig.groovy                          index override, snapshot cap, S3 members (Tasks 5, 8)
    CasConfigScope.groovy                     cas.snapshot scope (Task 5)
    CasSession.groovy                         openIndex(), snapshotWritable() (Task 5)
    core/Index.groovy                         SQL constants, ddl(), watermark() (Task 4)
    core/IndexSnapshot.groovy                 the snapshot writer (Task 5)
    trace/CasObserver.groovy                  writes the snapshot after indexing (Task 5)
    ext/CasExtension.groovy                   uses CasSession.openIndex() (Task 5)
    cli/UsageException.groovy                 a verb called wrongly: exit 2 (Task 6)
    cli/Options.groovy                        --flag value parsing (Task 6)
    cli/CasCommands.groovy                    verb dispatch, snapshot verb (Task 6)
    explore/MemberFiles.groovy                what the server needs of a member (Task 7)
    explore/LocalMemberFiles.groovy           a member on disk (Task 7)
    explore/ByteRange.groovy                  Range header parsing (Task 7)
    explore/ExploreServer.groovy              loopback HTTP server (Task 7)
    explore/ExploreCommand.groovy             the explore verb (Task 7)
    explore/S3MemberFiles.groovy              a member in a private bucket (Task 8)
  src/test/groovy/robsyme/cas/...             one Spock spec per class above
  web/
    package.json, package-lock.json           pinned npm project (Task 2)
    build.mjs                                 single-file bundler (Tasks 2, 10, 11)
    schema-gen.mjs                            DESIGN.md §6 IPLD Schema to src/generated/schema.json (Task 10)
    src/vfs/sources.js                        byte sources for the VFS (Task 2)
    src/vfs/httpvfs.js                        the read-only VFS (Task 2)
    src/worker.js                             SQLite in a Worker (Task 2)
    src/db.js                                 probe, whole-file fallback, worker RPC (Task 2)
    src/inline.js                             the inlined Worker and wasm (Task 2)
    src/config.js                             page constants (Task 2)
    src/queries.json                          the page's SQL (Task 4)
    src/storelog.js, cid.js, typed.js, metadata.js, schema.js, blocks.js, store.js, model.js   (Task 10)
    src/html.js, views.js, app.js, index.html (Task 11)
    bench/bench.html, bench/bench.js, bench/run.sh, bench/RESULTS.md   (Task 2)
    test/*.test.mjs                           node --test (Tasks 2, 5, 10)
    test/helpers.mjs, fixture.mjs             sqlite-wasm in Node; a small member built as the plugin builds one (Tasks 2, 10)
    test/fixtures/schema.sql                  the index DDL, pinned by ExplorerQueriesTest (Task 10)
    test/write-fixture.mjs                    the fixture as a member directory (Task 11)
  gate/
    gen_year.py, year_params.py               moved from the prototype (Task 1)
    test_gen_year.py, test_serve.py           (Task 1)
    browser/serve.py                          counting static server, Range optional (Task 1)
    browser/package.json, package-lock.json   pinned Playwright (Task 1)
    browser/bench.mjs                         the VFS spike's driver (Task 2)
    browser/page-smoke.mjs                    the page over the fixture member (Task 11)
    browser/drive.mjs                         the browser tier's driver (Task 12)
    browser/tier.sh                           the local browser tier (Task 12)
    browser_assert.py, test_browser_assert.py prepare and check (Tasks 12, 13)
    cloud/s3tier.py, cloud/cloud.sh           the cloud browser tier (Task 13)
    gate.sh                                   keeps the snapshot after `fail`, runs the browser tier (Task 12)
```

## Waves

| Wave | Tasks | Depends on | Notes |
|---|---|---|---|
| 0 | 1 year generator and counting server; then 2 the HTTP VFS spike | none | **Stop after Task 2** and show Rob the numbers in `web/bench/RESULTS.md` before any other task starts |
| 1 | 3 DESIGN.md contract | 2 | The contract every later task codes against |
| 2 | 4 shared SQL and the query-plan guard | 3 | |
| 3 | 5 snapshot writer; 10 page core modules | 4 | Disjoint files |
| 4 | 6 plugin verbs and `snapshot`; 11 views and app | 5; 10 | Disjoint files |
| 5 | 7 `explore` server; 9 web build in Gradle | 6; 11 | 9 touches only `build.gradle` and `web/build.mjs` |
| 6 | 8 private S3 members; 12 Gate local browser tier | 7, 9; 7, 9, 11 | 8 edits `build.gradle` after 9 |
| 7 | 13 Gate cloud browser tier | 8, 12 | Needs Rob's `scidev` SSO session |
| 8 | 14 acceptance | all | |

The build compiles and `make check` passes after every task.

---
### Task 1: Year generator and counting server move into the Gate

**Files:**
- Create: `gate/gen_year.py` (from `prototype-snapshot-http/gen_year.py`)
- Create: `gate/year_params.py` (the prototype's `paramsFrom`, from `drive.mjs` and `verify.py`)
- Create: `gate/browser/serve.py` (from `prototype-snapshot-http/serve.py`)
- Create: `gate/browser/package.json`, `gate/browser/package-lock.json`
- Create: `gate/test_gen_year.py`, `gate/test_serve.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing from this plan.
- Produces:
  - `python3 gate/gen_year.py <schema.sqlite> <out.sqlite>`: `<schema.sqlite>` is any schema-2 index or snapshot; `RUNS` env overrides 1825. Writes one snapshot-shaped file (page size 4096, rollback journal).
  - `gen_year.build(schema_path, out_path, runs)` and `gen_year.ddl_of(schema_path) -> [str]`.
  - `python3 gate/year_params.py <snapshot.sqlite> <offset>` prints JSON `{"producersOf": [content], "latestSuccessfulRun": [pipeline], "itemsWhere": [completion, output, "sample", "string", sample]}`; `year_params.params(path, offset) -> dict`.
  - `python3 gate/browser/serve.py <root> <port> [--no-range]`; `serve.make_server(root, port, no_range) -> ThreadingHTTPServer`; `GET /__stats` returns and resets `{"requests", "bytes", "log"}`.
  - `gate/browser/node_modules/playwright` 1.63.0 after `npm ci`.

- [ ] **Step 1: Write the failing tests**

```python
# gate/test_gen_year.py
"""gen_year builds a year-shaped Index Snapshot with the plugin's own DDL."""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gen_year  # noqa: E402

# A schema-2 stand-in with the tables gen_year writes, shaped as Index.groovy's.
SCHEMA = [
    "CREATE TABLE schema_version(version INTEGER NOT NULL)",
    "CREATE TABLE run(completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,"
    " commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,"
    " status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)",
    "CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)",
    "CREATE TABLE collection(collection_cid TEXT PRIMARY KEY, completion_cid TEXT, output_name TEXT)",
    "CREATE TABLE item(item_cid TEXT PRIMARY KEY)",
    "CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT)",
    "CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)",
    "CREATE INDEX collection_item_collection ON collection_item(collection_cid)",
    "CREATE TABLE producer(content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)",
    "CREATE INDEX producer_content_cid ON producer(content_cid)",
    "CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)",
    "CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)",
    "CREATE TABLE nf_record(key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,"
    " labels_json TEXT, block_cid TEXT)",
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)",
]


def make_schema(path, version=2):
    con = sqlite3.connect(path)
    for sql in SCHEMA:
        con.execute(sql)
    con.execute("INSERT INTO schema_version VALUES (?)", (version,))
    con.commit()
    con.close()


def dump(path):
    con = sqlite3.connect(path)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {t: con.execute("SELECT * FROM %s ORDER BY rowid" % t).fetchall() for t in tables}
    finally:
        con.close()


class GenYearTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.schema = os.path.join(self.tmp, "schema.sqlite")
        make_schema(self.schema)

    def build(self, name, runs=3):
        out = os.path.join(self.tmp, name)
        gen_year.build(self.schema, out, runs)
        return out

    def test_same_seed_same_rows(self):
        self.assertEqual(dump(self.build("a.sqlite")), dump(self.build("b.sqlite")))

    def test_scale_per_run(self):
        rows = dump(self.build("a.sqlite", runs=2))
        self.assertEqual(len(rows["run"]), 2)
        self.assertEqual(len(rows["collection"]), 8)
        self.assertEqual(len(rows["collection_item"]), 200)
        self.assertEqual(len(rows["producer"]), 600)
        self.assertEqual(len(rows["item_attr"]), 3000)
        self.assertEqual(rows["schema_version"], [(2,)])

    def test_snapshot_shape(self):
        out = self.build("a.sqlite")
        with open(out, "rb") as fh:
            header = fh.read(100)
        self.assertEqual(int.from_bytes(header[16:18], "big"), 4096)
        self.assertEqual((header[18], header[19]), (1, 1))
        for suffix in ("-wal", "-shm", "-journal"):
            self.assertFalse(os.path.exists(out + suffix), suffix)
        con = sqlite3.connect(out)
        self.assertIsNone(con.execute("SELECT member FROM run LIMIT 1").fetchone()[0])
        watermark = con.execute("SELECT value FROM meta WHERE key = 'store_log_watermark'").fetchone()[0]
        self.assertRegex(watermark, r"^\d{13}-run-bafyrei[a-z2-7]{52}$")
        con.close()

    def test_indexes_come_from_the_schema(self):
        out = self.build("a.sqlite")
        con = sqlite3.connect(out)
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        con.close()
        self.assertIn("collection_completion_output", names)
        self.assertIn("collection_item_collection", names)

    def test_refuses_another_schema_version(self):
        old = os.path.join(self.tmp, "old.sqlite")
        make_schema(old, version=1)
        with self.assertRaises(SystemExit):
            gen_year.build(old, os.path.join(self.tmp, "x.sqlite"), 1)


if __name__ == "__main__":
    unittest.main()
```

```python
# gate/test_serve.py
"""The counting static server the browser tier measures through."""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser"))

import serve  # noqa: E402


class ServeTest(unittest.TestCase):
    def start(self, no_range=False):
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "f.bin"), "wb") as fh:
            fh.write(bytes(range(256)) * 40)          # 10240 bytes
        server = serve.make_server(self.root, 0, no_range)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        self.base = "http://127.0.0.1:%d" % server.server_address[1]

    def get(self, path, headers=None):
        req = urllib.request.Request(self.base + path, headers=headers or {})
        try:
            with urllib.request.urlopen(req) as res:
                return res.status, dict(res.headers), res.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def stats(self):
        return json.loads(self.get("/__stats")[2])

    def test_range_is_206_with_exact_bytes(self):
        self.start()
        status, headers, body = self.get("/f.bin", {"Range": "bytes=4096-8191"})
        self.assertEqual(status, 206)
        self.assertEqual(headers["Content-Range"], "bytes 4096-8191/10240")
        self.assertEqual(body, (bytes(range(256)) * 40)[4096:8192])

    def test_suffix_and_open_ranges(self):
        self.start()
        self.assertEqual(len(self.get("/f.bin", {"Range": "bytes=-100"})[2]), 100)
        self.assertEqual(len(self.get("/f.bin", {"Range": "bytes=10000-"})[2]), 240)

    def test_range_past_the_end_is_416(self):
        self.start()
        self.assertEqual(self.get("/f.bin", {"Range": "bytes=20000-20001"})[0], 416)

    def test_no_range_mode_answers_200_with_the_whole_file(self):
        self.start(no_range=True)
        status, headers, body = self.get("/f.bin", {"Range": "bytes=0-99"})
        self.assertEqual(status, 200)
        self.assertEqual(len(body), 10240)
        self.assertNotIn("Accept-Ranges", headers)

    def test_stats_count_and_reset(self):
        self.start()
        self.get("/f.bin", {"Range": "bytes=0-4095"})
        self.get("/f.bin", {"Range": "bytes=4096-8191"})
        first = self.stats()
        self.assertEqual((first["requests"], first["bytes"]), (2, 8192))
        self.assertEqual(self.stats()["requests"], 0)

    def test_cors_exposes_content_range(self):
        self.start()
        headers = self.get("/f.bin", {"Range": "bytes=0-9", "Origin": "http://elsewhere"})[1]
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
        self.assertIn("Content-Range", headers["Access-Control-Expose-Headers"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd nf-blocks && python3 -m unittest gate.test_gen_year gate.test_serve -v` (or `python3 -m unittest discover -s gate -p 'test_*.py'`)
Expected: FAIL with `ModuleNotFoundError: No module named 'gen_year'` and `No module named 'serve'`.

- [ ] **Step 3: Write `gate/gen_year.py`**

Keep every `random` call in the prototype's order, or the synthetic rows change and the request counts stop being comparable with the prototype's `RESULTS.md`.

```python
#!/usr/bin/env python3
"""A year of synthetic runs as one Index Snapshot, for Gate browser assertion 2.

    python3 gate/gen_year.py <schema.sqlite> <out.sqlite>     # RUNS=1825 by default

<schema.sqlite> is any schema-2 index or snapshot. Only its DDL is read, so
the synthetic file has exactly the plugin's tables and indexes. Scale and seed
are the block explorer prototype's (spec section 1.3, assertion 2): 5 runs a
day for 365 days, 20 pipelines, 4 outputs, 100 items per run, 3 files per item,
15 Meta Map keys per item, random.seed(42). The random calls are made in the
prototype's order, so request counts stay comparable with its RESULTS.md
(branch prototype/index-snapshot-http).
"""
import json
import os
import random
import sqlite3
import sys

SCHEMA_VERSION = 2
ITEMS = 100
OUTPUTS = ['aligned', 'qc', 'variants', 'reports']
LEAVES = 3
PAGE_SIZE = 4096
HORIZON_MILLIS = 9999999999999
B32 = 'abcdefghijklmnopqrstuvwxyz234567'


def cid(prefix='bafyrei'):
    return prefix + ''.join(random.choice(B32) for _ in range(52))


def ddl_of(schema_path):
    """CREATE statements of a schema-2 database, in creation order."""
    con = sqlite3.connect('file:%s?mode=ro' % schema_path, uri=True)
    try:
        row = con.execute('SELECT version FROM schema_version').fetchone()
        if not row or row[0] != SCHEMA_VERSION:
            raise SystemExit('%s has schema version %r, not %d'
                             % (schema_path, row and row[0], SCHEMA_VERSION))
        return [r[0] for r in con.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")]
    finally:
        con.close()


def build(schema_path, out_path, runs):
    ddl = ddl_of(schema_path)
    live = out_path + '.live'
    for path in (live, live + '-wal', live + '-shm', out_path):
        if os.path.exists(path):
            os.remove(path)
    random.seed(42)
    con = sqlite3.connect(live)
    con.execute('PRAGMA journal_mode=wal')
    for sql in ddl:
        con.execute(sql)
    con.execute('INSERT INTO schema_version VALUES (?)', (SCHEMA_VERSION,))
    pipelines = ['pipeline-%02d' % i for i in range(20)]
    samples = ['S%05d' % i for i in range(20000)]
    t0 = 1_758_000_000_000
    content_pool = []
    comp = fin = None
    for r in range(runs):
        comp, man = cid(), cid()
        status = 'succeeded' if random.random() > 0.1 else 'failed'
        fin = t0 + r * 17_280_000
        con.execute('INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (comp, man, random.choice(pipelines), 'main', cid('')[:40], cid('')[:32],
                     cid('')[:36], 'run_%d' % r, 'lab', status, 0 if status == 'succeeded' else 1,
                     '2026-%02d-01T00:00:00.%03dZ' % (1 + r * 12 // runs, r % 1000), None))
        colls = {o: cid() for o in OUTPUTS}
        for o, c in colls.items():
            con.execute('INSERT INTO collection VALUES (?,?,?)', (c, comp, o))
        for i in range(ITEMS):
            item = cid()
            coll = colls[OUTPUTS[i % len(OUTPUTS)]]
            con.execute('INSERT OR IGNORE INTO item VALUES (?)', (item,))
            con.execute('INSERT INTO collection_item VALUES (?,?)', (coll, item))
            sample = random.choice(samples)
            attrs = [('id', 'string', sample), ('sample', 'string', sample),
                     ('lane', 'int', str(random.randint(1, 8))),
                     ('condition', 'string', random.choice(['tumour', 'normal', 'control'])),
                     ('library.strand', 'string', random.choice(['fwd', 'rev', 'none'])),
                     ('library.kit', 'string', random.choice(['kitA', 'kitB'])),
                     ('patient', 'string', 'P%04d' % random.randint(0, 5000)),
                     ('batch', 'int', str(r)), ('single_end', 'bool', 'false'),
                     ('read_group', 'string', '%s.L%d' % (sample, i % 8)),
                     ('platform', 'string', 'ILLUMINA'), ('genome', 'string', 'GRCh38'),
                     ('status', 'int', str(random.randint(0, 1))),
                     ('sex', 'string', random.choice(['XX', 'XY'])),
                     ('depth', 'float', '%.2f' % random.uniform(10, 60))]
            con.executemany('INSERT INTO item_attr VALUES (?,?,?,?,0)',
                            [(item, p, t, v) for p, t, v in attrs])
            for leaf in range(LEAVES):
                content = (random.choice(content_pool)
                           if content_pool and random.random() < 0.2 else cid('bafkrei'))
                content_pool.append(content)
                if len(content_pool) > 5000:
                    content_pool.pop(0)
                con.execute('INSERT INTO producer VALUES (?,?,?,?,?)',
                            (content, item, coll, comp, '%s.%d.dat' % (sample, leaf)))
        for k in range(30):
            con.execute('INSERT INTO nf_record VALUES (?,?,?,?,?,?)',
                        ('%s/%d' % (cid('')[:32], k), 'TaskRun', cid('')[:32], None,
                         json.dumps({'process': 'P%d' % k}), cid()))
        if r % 200 == 0:
            con.commit()
            print('runs', r, file=sys.stderr)
    if comp is not None:
        # The watermark a real snapshot carries: the newest Store Log entry name.
        con.execute("INSERT INTO meta VALUES ('store_log_watermark', ?)",
                    ('%013d-run-%s' % (HORIZON_MILLIS - fin, comp),))
    con.commit()
    con.execute('PRAGMA page_size=%d' % PAGE_SIZE)
    con.execute('VACUUM INTO ?', (out_path,))
    con.close()
    for path in (live, live + '-wal', live + '-shm'):
        if os.path.exists(path):
            os.remove(path)


def main(argv):
    if len(argv) != 3:
        sys.stderr.write(__doc__)
        return 2
    build(argv[1], argv[2], int(os.environ.get('RUNS', 5 * 365)))
    print(argv[2], os.path.getsize(argv[2]))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
```

The prototype wrote `member = 'lab'` and a watermark of the form `0000000000000-<cid>`; the snapshot contract (Task 3) stores `member` as NULL and a real entry name, so those two values change. Neither draws on `random`.

- [ ] **Step 4: Write `gate/year_params.py`**

```python
#!/usr/bin/env python3
"""Query parameters for the year snapshot, the prototype's formulas verbatim.

    python3 gate/year_params.py <snapshot.sqlite> <offset>

Offset 0 is the cold set, offset 3 the "other parameters" set, as in the
prototype's drive.mjs, so request counts compare with its RESULTS.md.
"""
import json
import sqlite3
import sys


def params(path, offset):
    con = sqlite3.connect('file:%s?mode=ro' % path, uri=True)
    try:
        def one(sql):
            return con.execute(sql).fetchone()
        content = one("SELECT content_cid FROM producer LIMIT 1 OFFSET (%d * 7919 %% "
                      "(SELECT count(*) FROM producer))" % offset)[0]
        pipeline = one("SELECT pipeline FROM run ORDER BY completion_cid LIMIT 1 OFFSET "
                       "(%d %% (SELECT count(*) FROM run))" % offset)[0]
        comp, out, sample = one(
            "SELECT c.completion_cid, c.output_name, a.value FROM collection c "
            "JOIN collection_item ci USING(collection_cid) "
            "JOIN item_attr a ON a.item_cid = ci.item_cid AND a.path = 'sample' "
            "WHERE c.rowid = (SELECT rowid FROM collection ORDER BY rowid LIMIT 1 OFFSET "
            "(%d * 37 %% (SELECT count(*) FROM collection))) LIMIT 1" % offset)
        return {'producersOf': [content], 'latestSuccessfulRun': [pipeline],
                'itemsWhere': [comp, out, 'sample', 'string', sample]}
    finally:
        con.close()


if __name__ == '__main__':
    print(json.dumps(params(sys.argv[1], int(sys.argv[2]))))
```

- [ ] **Step 5: Write `gate/browser/serve.py`**

```python
#!/usr/bin/env python3
"""A static file server with Range, CORS and request counting.

    python3 gate/browser/serve.py <root> <port> [--no-range]

Stock `python -m http.server` ignores Range and answers 200 with the whole
file, which is exactly the "plain static server" of spec section 6, so
--no-range reproduces it on purpose. GET /__stats returns and resets
{requests, bytes, log}; page assets (.html, .js, .wasm) are not counted.
"""
import json
import os
import re
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

RANGE = re.compile(r'^bytes=(\d*)-(\d*)$')


def make_server(root, port, no_range=False):
    lock = threading.Lock()
    stats = {'requests': 0, 'bytes': 0, 'log': []}

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=root, **k)

        def log_message(self, *a):
            pass

        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Headers', 'Range')
            self.send_header('Access-Control-Expose-Headers',
                             'Content-Range, Content-Length, Accept-Ranges, ETag')
            self.send_header('Cache-Control', 'no-store')
            super().end_headers()

        def do_OPTIONS(self):
            self.send_response(204)
            self.end_headers()

        def record(self, n, rng):
            path = self.path.split('?')[0]
            if path.startswith('/__') or path.endswith(('.js', '.wasm', '.html', '.map')):
                return
            with lock:
                stats['requests'] += 1
                stats['bytes'] += n
                stats['log'].append([self.command, path, rng, n])

        def file_path(self):
            return self.translate_path(self.path.split('?')[0])

        def do_HEAD(self):
            path = self.file_path()
            if not os.path.isfile(path):
                return super().do_HEAD()
            self.record(0, 'HEAD')
            self.send_response(200)
            self.send_header('Content-Length', str(os.path.getsize(path)))
            if not no_range:
                self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()

        def do_GET(self):
            if self.path == '/__stats':
                with lock:
                    body = json.dumps(stats).encode()
                    stats.update(requests=0, bytes=0, log=[])
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            path = self.file_path()
            header = self.headers.get('Range')
            if os.path.isfile(path) and header and not no_range:
                return self.ranged(path, header)
            if os.path.isfile(path):
                self.record(os.path.getsize(path), None)
            return super().do_GET()

        def ranged(self, path, header):
            size = os.path.getsize(path)
            m = RANGE.match(header.strip())
            if not m or (m.group(1) == '' and m.group(2) == ''):
                self.record(size, None)
                return super().do_GET()
            if m.group(1) == '':
                start, end = max(0, size - int(m.group(2))), size - 1
            else:
                start = int(m.group(1))
                end = min(int(m.group(2)) if m.group(2) else size - 1, size - 1)
            if start >= size or start > end:
                self.send_response(416)
                self.send_header('Content-Range', 'bytes */%d' % size)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            n = end - start + 1
            self.send_response(206)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, size))
            self.send_header('Content-Length', str(n))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            with open(path, 'rb') as fh:
                fh.seek(start)
                self.wfile.write(fh.read(n))
            self.record(n, header)

    return ThreadingHTTPServer(('127.0.0.1', port), Handler)


if __name__ == '__main__':
    make_server(sys.argv[1], int(sys.argv[2]), '--no-range' in sys.argv).serve_forever()
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd nf-blocks && python3 -m unittest discover -s gate -p 'test_*.py' -v`
Expected: PASS, the new tests and every existing `test_cas.py` / `test_assert.py` test.

- [ ] **Step 7: Pin Playwright for the Gate and ignore generated files**

```bash
cd nf-blocks/gate/browser
cat > package.json <<'EOF'
{
  "name": "nf-blocks-gate-browser",
  "private": true,
  "type": "module",
  "description": "The Gate's browser tier: Playwright's own pinned Chromium (block explorer spec section 1.3).",
  "dependencies": {
    "playwright": "1.63.0"
  }
}
EOF
npm install --no-audit --no-fund
npx playwright install chromium
node -e "import('playwright').then(p => p.chromium.launch()).then(b => b.close()).then(() => console.log('chromium ok'))"
```
Expected: `chromium ok`.

Append to `nf-blocks/.gitignore` (read it first; add these three lines at the end, do not rewrite it):

```
node_modules/
web/dist/
web/src/generated/
```

- [ ] **Step 8: Commit**

```bash
cd nf-blocks
git add gate/gen_year.py gate/year_params.py gate/browser/serve.py gate/browser/package.json \
        gate/browser/package-lock.json gate/test_gen_year.py gate/test_serve.py .gitignore
git commit -m "test(gate): move the year generator and the counting server in from the prototype

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Spike, the read-only HTTP VFS on `@sqlite.org/sqlite-wasm`

This is wave 0 (spec section 1.1 item 3, "must match the prototype's measured request counts"). Its code is not throwaway: `sources.js`, `httpvfs.js`, `worker.js`, `db.js` and `inline.js` are the page's reader. It ends with numbers Rob reviews before anything else starts.

**Files:**
- Create: `web/package.json`, `web/package-lock.json`, `web/build.mjs`
- Create: `web/src/config.js`, `web/src/vfs/sources.js`, `web/src/vfs/httpvfs.js`, `web/src/worker.js`, `web/src/db.js`, `web/src/inline.js`
- Create: `web/bench/bench.html`, `web/bench/bench.js`, `web/bench/run.sh`, `web/bench/RESULTS.md`
- Create: `web/test/vfs.test.mjs`, `web/test/probe.test.mjs`, `web/test/helpers.mjs`
- Create: `gate/browser/bench.mjs`

**Interfaces:**
- Consumes: `gate/gen_year.py`, `gate/year_params.py`, `gate/browser/serve.py` (Task 1).
- Produces (later tasks import these exact names):
  - `web/src/config.js`: `SCHEMA_VERSION = 2`, `SNAPSHOT_PATH = 'index/v2.sqlite'`, `DEFAULT_CAP_BYTES = 67108864`, `CHUNK_BYTES = 4096`, `OVERLAP_MILLIS = 600000`, `STALE_RUNS_NOTICE = 20`, `CLOSURE_FETCH_NOTICE = 2000`.
  - `web/src/vfs/sources.js`: `class Counter { requests; bytes; reset() -> {requests, bytes} }`, `class MemorySource(bytes)`, `class ChunkedSource(size, fetchRange, { chunk, maxChunks, head })`, `xhrRange(url, counter) -> (start, endInclusive) => Uint8Array`. A source is `{ size: number, read(offset, length) -> Uint8Array }`, synchronous.
  - `web/src/vfs/httpvfs.js`: `installReadOnlyVfs(sqlite3, name) -> { register(key, source), unregister(key), lastError }`. Open a registered source with `new sqlite3.oo1.DB({ filename: 'file:<key>?immutable=1', flags: 'r', vfs: name })`.
  - `web/src/db.js`: `class SnapshotError extends Error { code }` with codes `no_snapshot`, `no_range_over_cap`, `cors_headers`, `fetch_failed`, `query_failed`, `worker_failed`; `probeSnapshot(url, { cap, fetchFn }) -> { mode: 'range'|'whole', size, head?, bytes? }`; `openSnapshot(url, { cap, wasm, createWorker, fetchFn }) -> Snapshot { mode, size, query(sql, params) -> Promise<Array<object>>, stats() -> Promise<{requests, bytes}>, close() }`.
  - `web/src/inline.js`: `createInlineWorker() -> Worker`, `inlineWasm() -> Uint8Array`, reading the build-time globals `__WORKER_SOURCE__` and `__WASM_BASE64__`.
  - `web/build.mjs`: `node build.mjs` writes single-file pages into `web/dist/`; this task builds `dist/bench.html`.
  - `web/test/helpers.mjs`: `loadSqlite() -> Promise<sqlite3>` (Node, `wasmBinary` from `node_modules`), `makeDb(sqlite3, statements) -> Uint8Array` (an exported in-memory database with page size 4096).

- [ ] **Step 1: Create the npm project**

```bash
cd nf-blocks && mkdir -p web/src/vfs web/bench web/test
cat > web/package.json <<'EOF'
{
  "name": "nf-blocks-explorer",
  "private": true,
  "type": "module",
  "description": "The block explorer page (block explorer spec section 5). Built into the plugin zip; the output is not committed.",
  "scripts": {
    "build": "node build.mjs",
    "test": "node --test --test-reporter=spec \"test/**/*.test.mjs\""
  },
  "dependencies": {
    "@ipld/dag-cbor": "10.0.2",
    "@ipld/schema": "7.0.12",
    "@sqlite.org/sqlite-wasm": "3.53.4-build1",
    "cborg": "6.1.2",
    "multiformats": "14.0.5"
  },
  "devDependencies": {
    "esbuild": "0.28.2"
  }
}
EOF
cd web && npm install --no-audit --no-fund && node -e "console.log(require('./package-lock.json').lockfileVersion)"
```
Expected: `3`. Check that `npm ls cborg` shows exactly one `cborg@6.1.2` (the direct pin and `@ipld/dag-cbor`'s must agree, or float boxing in Task 10 wraps the wrong tokenizer).

- [ ] **Step 2: Write `web/src/config.js`**

```js
// Constants the page shares with the plugin. Each names the spec section or
// DESIGN.md section it comes from; a change here without one there is a bug.

/** Index schema version (DESIGN.md §12, Index.SCHEMA_VERSION). Task 5 pins them equal. */
export const SCHEMA_VERSION = 2
/** Where a member keeps its Index Snapshot (spec section 4). */
export const SNAPSHOT_PATH = `index/v${SCHEMA_VERSION}.sqlite`
/** The whole-file fallback's cap, cas.snapshot.maxBytes' default (spec section 4). */
export const DEFAULT_CAP_BYTES = 64 * 1024 * 1024
/** The snapshot's page size, and the unit the VFS fetches in (spec section 4). */
export const CHUNK_BYTES = 4096
/** How far before the watermark the tail re-reads (spec section 3). */
export const OVERLAP_MILLIS = 10 * 60 * 1000
/** Past this many stale runs the page shows the command that rewrites the snapshot (spec section 5.4). */
export const STALE_RUNS_NOTICE = 20
/** Past this many block fetches for one query, likewise (spec section 5.4). */
export const CLOSURE_FETCH_NOTICE = 2000
```

- [ ] **Step 3: Write the failing VFS tests**

```js
// web/test/helpers.mjs
import { readFileSync } from 'node:fs'
import sqlite3InitModule from '@sqlite.org/sqlite-wasm'

let loaded
/** sqlite-wasm in Node, given its wasm bytes the way the page gives them. */
export async function loadSqlite() {
  loaded ??= sqlite3InitModule({
    wasmBinary: readFileSync(new URL('../node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm', import.meta.url)),
    print: () => {},
    printErr: () => {},
  })
  return loaded
}

/** A database built in memory from SQL statements, exported as file bytes. */
export function makeDb(sqlite3, statements) {
  const db = new sqlite3.oo1.DB(':memory:')
  try {
    db.exec('PRAGMA page_size=4096')
    for (const sql of statements) db.exec(sql)
    return sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
}

/** A fetchRange over bytes in memory that counts what it is asked for. */
export function countingRange(bytes) {
  const calls = []
  const fn = (start, endInclusive) => {
    calls.push([start, endInclusive])
    return bytes.subarray(start, endInclusive + 1)
  }
  fn.calls = calls
  return fn
}
```

```js
// web/test/vfs.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadSqlite, makeDb, countingRange } from './helpers.mjs'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { ChunkedSource, MemorySource } from '../src/vfs/sources.js'

const ROWS = 2000
const statements = [
  'CREATE TABLE producer(content_cid TEXT, item_cid TEXT, filename TEXT)',
  'CREATE INDEX producer_content_cid ON producer(content_cid)',
  `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${ROWS})
   INSERT INTO producer SELECT 'c' || (i % 50), 'item' || i, 'f' || i || '.bam' FROM n`,
]

let counter = 0
async function openOver(source) {
  const sqlite3 = await loadSqlite()
  const vfs = installReadOnlyVfs(sqlite3, `test-${++counter}`)
  vfs.register('snap', source)
  const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: `test-${counter}` })
  return { db, vfs }
}

test('rows read through the VFS are the rows the SQL selects', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new ChunkedSource(bytes.length, countingRange(bytes)))
  const expected = Array.from({ length: ROWS }, (_, k) => k + 1)
    .filter((i) => i % 50 === 7)
    .map((i) => [`item${i}`, `f${i}.bam`])
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
  const sql = "SELECT item_cid, filename FROM producer WHERE content_cid = 'c7' ORDER BY item_cid"
  assert.deepEqual(db.selectArrays(sql), expected)
  db.close()
})

test('a warm query costs no fetches', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const range = countingRange(bytes)
  const { db } = await openOver(new ChunkedSource(bytes.length, range))
  const sql = "SELECT count(*) FROM producer WHERE content_cid = 'c3'"
  db.selectValue(sql)
  const cold = range.calls.length
  assert.ok(cold > 0)
  db.selectValue(sql)
  assert.equal(range.calls.length, cold)
  db.close()
})

test('an index lookup reads a few chunks, not the table', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const range = countingRange(bytes)
  const { db } = await openOver(new ChunkedSource(bytes.length, range))
  db.selectValue("SELECT count(*) FROM producer WHERE content_cid = 'c3'")
  const fetched = range.calls.reduce((n, [a, b]) => n + (b - a + 1), 0)
  assert.ok(fetched < bytes.length / 2, `fetched ${fetched} of ${bytes.length} bytes`)
  db.close()
})

test('reads spanning chunk boundaries come back intact', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new ChunkedSource(bytes.length, countingRange(bytes), { chunk: 1000 }))
  assert.equal(db.selectValue('SELECT count(*) FROM producer'), ROWS)
  db.close()
})

test('contiguous missing chunks are fetched in one request', () => {
  const bytes = new Uint8Array(40960).map((_, i) => i & 0xff)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range)
  assert.deepEqual([...source.read(4000, 9000)], [...bytes.subarray(4000, 13000)])
  assert.deepEqual(range.calls, [[0, 16383]])
  source.read(4096, 100)
  assert.equal(range.calls.length, 1)
})

test('the probe head seeds chunk 0 so it is not fetched again', () => {
  const bytes = new Uint8Array(8192).fill(7)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range, { head: bytes.subarray(0, 4096) })
  source.read(0, 100)
  assert.equal(range.calls.length, 0)
})

test('the chunk cache is bounded', () => {
  const bytes = new Uint8Array(4096 * 10)
  const range = countingRange(bytes)
  const source = new ChunkedSource(bytes.length, range, { maxChunks: 2 })
  for (let c = 0; c < 10; c++) source.read(c * 4096, 1)
  assert.ok(source.cachedChunks() <= 2)
})

test('a short range answer fails the read instead of returning holes', () => {
  const source = new ChunkedSource(8192, () => new Uint8Array(10))
  assert.throws(() => source.read(0, 100), /short range read/)
})

test('a failed fetch surfaces as a query error with the cause', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  let calls = 0
  const flaky = (a, b) => { if (++calls > 1) throw new Error('network down'); return bytes.subarray(a, b + 1) }
  const { db, vfs } = await openOver(new ChunkedSource(bytes.length, flaky))
  assert.throws(() => db.selectValue('SELECT count(*) FROM producer'))
  assert.match(String(vfs.lastError?.message), /network down/)
  db.close()
})

test('the whole-file source answers the same', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new MemorySource(bytes))
  assert.equal(db.selectValue('SELECT count(*) FROM producer'), ROWS)
  db.close()
})

test('nothing can be written', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, statements)
  const { db } = await openOver(new MemorySource(bytes))
  assert.throws(() => db.exec("INSERT INTO producer VALUES ('x', 'y', 'z')"), /readonly|read-only/i)
  db.close()
})
```

```js
// web/test/probe.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { probeSnapshot, SnapshotError } from '../src/db.js'

const URL_ = 'http://store.test/index/v2.sqlite'

function response(status, body, headers = {}) {
  return new Response(body, { status, headers })
}

/** A body that yields `total` bytes in 64 KiB pieces and records how much was read. */
function streamed(total) {
  let sent = 0
  const stream = new ReadableStream({
    pull(controller) {
      if (sent >= total) return controller.close()
      const n = Math.min(65536, total - sent)
      sent += n
      controller.enqueue(new Uint8Array(n))
    },
  })
  stream.sent = () => sent
  return stream
}

test('206 means range mode, with the size from Content-Range', async () => {
  const head = new Uint8Array(4096).fill(1)
  const fetchFn = async (url, init) => {
    assert.equal(init.headers.Range, 'bytes=0-4095')
    return response(206, head, { 'Content-Range': 'bytes 0-4095/580000000' })
  }
  const probe = await probeSnapshot(URL_, { cap: 1000, fetchFn })
  assert.equal(probe.mode, 'range')
  assert.equal(probe.size, 580000000)
  assert.equal(probe.head.length, 4096)
})

test('a 206 whose Content-Range is hidden by CORS is a configuration error', async () => {
  const fetchFn = async () => response(206, new Uint8Array(4096))
  await assert.rejects(probeSnapshot(URL_, { cap: 1000, fetchFn }), e => e instanceof SnapshotError && e.code === 'cors_headers')
})

test('200 under the cap downloads the whole file', async () => {
  const fetchFn = async () => response(200, new Uint8Array(5000), { 'Content-Length': '5000' })
  const probe = await probeSnapshot(URL_, { cap: 10000, fetchFn })
  assert.equal(probe.mode, 'whole')
  assert.equal(probe.bytes.length, 5000)
})

test('200 over the cap by Content-Length stops before reading the body', async () => {
  const body = streamed(20_000_000)
  const fetchFn = async () => response(200, body, { 'Content-Length': '20000000' })
  await assert.rejects(probeSnapshot(URL_, { cap: 1_000_000, fetchFn }),
    e => e.code === 'no_range_over_cap' && /does not support Range/.test(e.message))
  assert.ok(body.sent() <= 65536 * 2, `read ${body.sent()} bytes`)
})

test('200 with no Content-Length streams and stops at the cap (Review Focus 1)', async () => {
  const body = streamed(20_000_000)
  const fetchFn = async () => response(200, body)
  await assert.rejects(probeSnapshot(URL_, { cap: 1_000_000, fetchFn }), e => e.code === 'no_range_over_cap')
  assert.ok(body.sent() < 2_000_000, `read ${body.sent()} bytes`)
})

test('404 and 403 mean there is no snapshot here', async () => {
  for (const status of [403, 404]) {
    const fetchFn = async () => response(status, 'nope')
    await assert.rejects(probeSnapshot(URL_, { cap: 1, fetchFn }), e => e.code === 'no_snapshot')
  }
})

test('a network failure is fetch_failed, not a crash', async () => {
  const fetchFn = async () => { throw new TypeError('Failed to fetch') }
  await assert.rejects(probeSnapshot(URL_, { cap: 1, fetchFn }), e => e.code === 'fetch_failed')
})
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd nf-blocks/web && npm test`
Expected: FAIL, `Cannot find module '../src/vfs/httpvfs.js'` and `'../src/db.js'`.

- [ ] **Step 5: Write `web/src/vfs/sources.js`**

```js
// Byte sources the read-only VFS reads through (spec section 5.2). Every read
// is synchronous: the VFS runs inside SQLite's C call stack and cannot await,
// which is why the page reads the snapshot from a Worker with synchronous XHR.
import { CHUNK_BYTES } from '../config.js'

/** Requests and bytes a source has fetched since the last reset. */
export class Counter {
  constructor() { this.requests = 0; this.bytes = 0 }
  reset() {
    const seen = { requests: this.requests, bytes: this.bytes }
    this.requests = 0
    this.bytes = 0
    return seen
  }
}

/** A whole snapshot already in memory: the fallback for a server without Range. */
export class MemorySource {
  constructor(bytes) { this.bytes = bytes; this.size = bytes.length }
  read(offset, length) { return this.bytes.subarray(offset, Math.min(offset + length, this.size)) }
}

/**
 * A remote file read in fixed chunks, each fetched once and kept (least
 * recently used first out). Contiguous missing chunks go in one request.
 * `fetchRange(start, endInclusive)` must return exactly those bytes.
 */
export class ChunkedSource {
  constructor(size, fetchRange, { chunk = CHUNK_BYTES, maxChunks = 16384, head } = {}) {
    this.size = size
    this.fetchRange = fetchRange
    this.chunk = chunk
    this.maxChunks = maxChunks
    this.chunks = new Map()
    if (head && (head.length >= Math.min(chunk, size))) this.put(0, head.subarray(0, Math.min(chunk, size)))
  }

  cachedChunks() { return this.chunks.size }

  read(offset, length) {
    const end = Math.min(offset + length, this.size)
    if (offset >= end) return new Uint8Array(0)
    const first = Math.floor(offset / this.chunk)
    const last = Math.floor((end - 1) / this.chunk)
    const parts = this.fill(first, last)
    const out = new Uint8Array(end - offset)
    for (let c = first; c <= last; c++) {
      const bytes = parts.get(c)
      const chunkStart = c * this.chunk
      const from = Math.max(offset, chunkStart)
      const to = Math.min(end, chunkStart + bytes.length)
      out.set(bytes.subarray(from - chunkStart, to - chunkStart), from - offset)
    }
    return out
  }

  /** The chunks first..last, fetching the missing ones in contiguous runs. */
  fill(first, last) {
    const parts = new Map()
    let c = first
    while (c <= last) {
      const cached = this.chunks.get(c)
      if (cached) {
        this.chunks.delete(c)
        this.chunks.set(c, cached)
        parts.set(c, cached)
        c++
        continue
      }
      let run = c
      while (run + 1 <= last && !this.chunks.has(run + 1)) run++
      const start = c * this.chunk
      const endInclusive = Math.min((run + 1) * this.chunk, this.size) - 1
      const bytes = this.fetchRange(start, endInclusive)
      if (bytes.length !== endInclusive - start + 1)
        throw new Error(`short range read: asked for bytes ${start}-${endInclusive}, got ${bytes.length}`)
      for (let k = c; k <= run; k++) {
        const piece = bytes.subarray((k - c) * this.chunk, Math.min((k - c + 1) * this.chunk, bytes.length))
        this.put(k, piece)
        parts.set(k, piece)
      }
      c = run + 1
    }
    return parts
  }

  put(index, bytes) {
    this.chunks.set(index, bytes)
    while (this.chunks.size > this.maxChunks) this.chunks.delete(this.chunks.keys().next().value)
  }
}

/** A synchronous ranged GET. Only a Worker may do this; a window cannot set responseType on sync XHR. */
export function xhrRange(url, counter) {
  return (start, endInclusive) => {
    const xhr = new XMLHttpRequest()
    xhr.open('GET', url, false)
    xhr.responseType = 'arraybuffer'
    xhr.setRequestHeader('Range', `bytes=${start}-${endInclusive}`)
    xhr.send()
    if (xhr.status !== 206) throw new Error(`ranged read of ${url} answered ${xhr.status}, not 206`)
    const bytes = new Uint8Array(xhr.response)
    counter.requests++
    counter.bytes += bytes.length
    return bytes
  }
}
```

- [ ] **Step 6: Write `web/src/vfs/httpvfs.js`**

This follows the shape of the OPFS SAH-pool VFS inside `@sqlite.org/sqlite-wasm` 3.53.4 (`dist/index.mjs`, `installVfs` with `io` and `vfs` method maps), cut down to reads.

```js
// A read-only SQLite VFS over synchronous byte sources (spec section 5.2).
// SQLite asks for pages one at a time; each read goes to the source
// registered under the file's name. Nothing is ever written, locked or synced.

export function installReadOnlyVfs(sqlite3, name) {
  const { capi, wasm } = sqlite3
  const sources = new Map()   // file name -> source
  const open = new Map()      // sqlite3_file pointer -> source
  const state = { lastError: null }
  const fail = (error, rc) => { state.lastError = error; return rc }

  const io = new capi.sqlite3_io_methods()
  io.$iVersion = 1
  sqlite3.vfs.installVfs({
    io: {
      struct: io,
      methods: {
        xClose(pFile) { open.delete(pFile); return 0 },
        xRead(pFile, pDest, n, offset64) {
          try {
            const got = open.get(pFile).read(Number(offset64), n)
            const dest = Number(pDest)
            const heap = wasm.heap8u()
            heap.set(got, dest)
            if (got.length < n) {
              heap.fill(0, dest + got.length, dest + n)
              return capi.SQLITE_IOERR_SHORT_READ
            }
            return 0
          } catch (e) {
            return fail(e, capi.SQLITE_IOERR_READ)
          }
        },
        xWrite: () => capi.SQLITE_READONLY,
        xTruncate: () => capi.SQLITE_READONLY,
        xSync: () => 0,
        xFileSize(pFile, pSize) { wasm.poke64(pSize, BigInt(open.get(pFile).size)); return 0 },
        xLock: () => 0,
        xUnlock: () => 0,
        xCheckReservedLock(pFile, pOut) { wasm.poke32(pOut, 0); return 0 },
        xFileControl: () => capi.SQLITE_NOTFOUND,
        xSectorSize: () => 4096,
        xDeviceCharacteristics: () => capi.SQLITE_IOCAP_IMMUTABLE,
      },
    },
  })

  const vfs = new capi.sqlite3_vfs()
  const fallback = new capi.sqlite3_vfs(capi.sqlite3_vfs_find(null))
  vfs.$iVersion = 1
  vfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof
  vfs.$mxPathname = 1024
  vfs.$zName = wasm.allocCString(name)
  vfs.$xRandomness = fallback.$xRandomness
  vfs.$xSleep = fallback.$xSleep
  vfs.$xCurrentTime = fallback.$xCurrentTime
  vfs.$xCurrentTimeInt64 = fallback.$xCurrentTimeInt64
  fallback.dispose()
  sqlite3.vfs.installVfs({
    vfs: {
      struct: vfs,
      methods: {
        xOpen(pVfs, zName, pFile, flags, pOutFlags) {
          const key = zName ? wasm.cstrToJs(zName) : ''
          const source = sources.get(key)
          if (!source) return fail(new Error(`no source registered as '${key}'`), capi.SQLITE_CANTOPEN)
          open.set(pFile, source)
          const file = new capi.sqlite3_file(pFile)
          file.$pMethods = io.pointer
          file.dispose()
          wasm.poke32(pOutFlags, capi.SQLITE_OPEN_READONLY)
          return 0
        },
        xDelete: () => capi.SQLITE_IOERR_DELETE,
        xAccess(pVfs, zName, flags, pOut) { wasm.poke32(pOut, 0); return 0 },
        xFullPathname(pVfs, zName, nOut, pOut) {
          return wasm.cstrncpy(pOut, zName, nOut) < nOut ? 0 : capi.SQLITE_CANTOPEN
        },
        xGetLastError: () => 0,
      },
    },
  })

  return {
    register(key, source) { sources.set(key, source) },
    unregister(key) { sources.delete(key) },
    get lastError() { return state.lastError },
  }
}
```

- [ ] **Step 7: Write `web/src/db.js`, `web/src/worker.js` and `web/src/inline.js`**

```js
// web/src/db.js
// The snapshot as the page sees it (spec section 5.2): probe with one ranged
// GET, then read pages through the VFS in a Worker, or, when the server
// ignores Range, download the whole file if it is under the cap.
import { CHUNK_BYTES, DEFAULT_CAP_BYTES } from './config.js'

export class SnapshotError extends Error {
  constructor(code, message) { super(message); this.code = code }
}

export async function probeSnapshot(url, { cap = DEFAULT_CAP_BYTES, fetchFn = fetch } = {}) {
  const abort = new AbortController()
  let res
  try {
    res = await fetchFn(url, { headers: { Range: `bytes=0-${CHUNK_BYTES - 1}` }, signal: abort.signal, cache: 'no-store' })
  } catch (e) {
    throw new SnapshotError('fetch_failed', `could not fetch ${url}: ${e.message}`)
  }
  if (res.status === 206) {
    const range = res.headers.get('Content-Range')
    const total = range && /\/(\d+)$/.exec(range)
    if (!total)
      throw new SnapshotError('cors_headers', `${url} answered 206 but Content-Range is not readable; a bucket's CORS rule must expose Content-Range (DESIGN.md §15)`)
    return { mode: 'range', size: Number(total[1]), head: new Uint8Array(await res.arrayBuffer()) }
  }
  if (res.status === 200) return { mode: 'whole', ...(await readCapped(url, res, cap, abort)) }
  if (res.status === 403 || res.status === 404)
    throw new SnapshotError('no_snapshot', `no Index Snapshot at ${url} (HTTP ${res.status})`)
  throw new SnapshotError('fetch_failed', `${url} answered HTTP ${res.status}`)
}

async function readCapped(url, res, cap, abort) {
  const tooBig = (n) => new SnapshotError('no_range_over_cap',
    `${url} does not support Range requests, and the snapshot (${n}) is larger than the ${cap}-byte cap for downloading it whole. Serve it with Range (nf-blocks:explore does) or rewrite it smaller.`)
  const declared = Number(res.headers.get('Content-Length'))
  if (declared > cap) { abort.abort(); throw tooBig(`${declared} bytes`) }
  const reader = res.body.getReader()
  const parts = []
  let total = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.length
    if (total > cap) { abort.abort(); await reader.cancel().catch(() => {}); throw tooBig(`more than ${cap} bytes`) }
    parts.push(value)
  }
  const bytes = new Uint8Array(total)
  let at = 0
  for (const p of parts) { bytes.set(p, at); at += p.length }
  return { size: total, bytes }
}

export async function openSnapshot(url, { cap = DEFAULT_CAP_BYTES, wasm, createWorker, fetchFn = fetch } = {}) {
  const probe = await probeSnapshot(url, { cap, fetchFn })
  const worker = createWorker()
  const pending = new Map()
  let next = 0
  worker.onmessage = ({ data }) => {
    const waiting = pending.get(data.id)
    if (!waiting) return
    pending.delete(data.id)
    if (data.ok) waiting.resolve(data.result)
    else waiting.reject(new SnapshotError('query_failed', data.error))
  }
  worker.onerror = (event) => {
    for (const waiting of pending.values()) waiting.reject(new SnapshotError('worker_failed', event.message || 'the snapshot worker failed'))
    pending.clear()
  }
  const call = (message, transfer = []) => new Promise((resolve, reject) => {
    const id = ++next
    pending.set(id, { resolve, reject })
    worker.postMessage({ ...message, id }, transfer)
  })
  const wasmCopy = wasm.slice()
  const transfer = [wasmCopy.buffer]
  if (probe.bytes) transfer.push(probe.bytes.buffer)
  await call({ op: 'open', url: new URL(url, globalThis.location?.href).href, mode: probe.mode, size: probe.size, head: probe.head, bytes: probe.bytes, wasm: wasmCopy }, transfer)
  return {
    mode: probe.mode,
    size: probe.size,
    query: (sql, params = []) => call({ op: 'query', sql, params }),
    stats: () => call({ op: 'stats' }),
    close: async () => { await call({ op: 'close' }); worker.terminate() },
  }
}
```

```js
// web/src/worker.js
// SQLite in a Worker, reading the snapshot through the read-only VFS.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm'
import { installReadOnlyVfs } from './vfs/httpvfs.js'
import { ChunkedSource, Counter, MemorySource, xhrRange } from './vfs/sources.js'

const VFS = 'nfb-http'
const counter = new Counter()
let sqlite3
let vfs
let db

self.onmessage = async ({ data: message }) => {
  try {
    self.postMessage({ id: message.id, ok: true, result: await handle(message) })
  } catch (e) {
    const cause = vfs?.lastError ? ` (${vfs.lastError.message})` : ''
    self.postMessage({ id: message.id, ok: false, error: `${e?.message ?? e}${cause}` })
  }
}

async function handle(message) {
  switch (message.op) {
    case 'open': {
      if (!sqlite3) {
        sqlite3 = await sqlite3InitModule({ wasmBinary: message.wasm, print: () => {}, printErr: () => {} })
        vfs = installReadOnlyVfs(sqlite3, VFS)
      }
      const source = message.mode === 'whole'
        ? new MemorySource(message.bytes)
        : new ChunkedSource(message.size, xhrRange(message.url, counter), { head: message.head })
      vfs.register('snapshot', source)
      db = new sqlite3.oo1.DB({ filename: 'file:snapshot?immutable=1', flags: 'r', vfs: VFS })
      return { sqlite: sqlite3.version.libVersion }
    }
    case 'query':
      return db.selectObjects(message.sql, message.params).map(row => ({ ...row }))
    case 'stats':
      return counter.reset()
    case 'close':
      db?.close()
      db = null
      return null
    default:
      throw new Error(`unknown worker op ${message.op}`)
  }
}
```

```js
// web/src/inline.js
// The Worker source and sqlite3.wasm are inlined by build.mjs, so the page is
// one index.html a member can carry beside its snapshot (DESIGN.md §15).
/* global __WORKER_SOURCE__, __WASM_BASE64__ */

export function createInlineWorker() {
  const url = URL.createObjectURL(new Blob([__WORKER_SOURCE__], { type: 'text/javascript' }))
  return new Worker(url)
}

let wasm
export function inlineWasm() {
  if (!wasm) {
    const text = atob(__WASM_BASE64__)
    wasm = new Uint8Array(text.length)
    for (let i = 0; i < text.length; i++) wasm[i] = text.charCodeAt(i)
  }
  return wasm
}
```

- [ ] **Step 8: Run the unit tests to verify they pass**

Run: `cd nf-blocks/web && npm test`
Expected: PASS, all tests in `vfs.test.mjs` and `probe.test.mjs`.

- [ ] **Step 9: Write `web/build.mjs` and the bench page**

`@sqlite.org/sqlite-wasm` computes `new URL('sqlite3.wasm', import.meta.url)` even when `wasmBinary` is given, and a Worker created from a `Blob` has no usable base URL, so the Worker bundle defines `import.meta.url` as a fixed absolute URL. The wasm itself always comes from `wasmBinary`.

```js
// web/build.mjs
// Builds single-file pages into dist/: the app, the Worker source and
// sqlite3.wasm are inlined so a member can carry the page as one index.html
// (DESIGN.md §15). `node build.mjs` builds every page.
import * as esbuild from 'esbuild'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'

const here = new URL('.', import.meta.url)
const path = (p) => new URL(p, here).pathname

async function bundle(entry, define = {}) {
  const result = await esbuild.build({
    entryPoints: [path(entry)], bundle: true, format: 'iife', write: false, minify: true,
    target: 'es2022', define, logLevel: 'warning', legalComments: 'none',
  })
  return result.outputFiles[0].text
}

async function page(entry, template, out, define) {
  const js = await bundle(entry, define)
  const html = readFileSync(path(template), 'utf8')
    .replace('<!--APP-->', () => `<script>${js.replace(/<\/script/gi, '<\\/script')}</script>`)
  mkdirSync(path('dist'), { recursive: true })
  writeFileSync(path(`dist/${out}`), html)
  console.log(`dist/${out} ${html.length} bytes`)
}

const worker = await bundle('src/worker.js', { 'import.meta.url': JSON.stringify('https://nf-blocks.invalid/worker.js') })
const wasm = readFileSync(path('node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm'))
const inlined = { __WORKER_SOURCE__: JSON.stringify(worker), __WASM_BASE64__: JSON.stringify(wasm.toString('base64')) }

await page('bench/bench.js', 'bench/bench.html', 'bench.html', inlined)
```

```html
<!-- web/bench/bench.html -->
<!doctype html>
<meta charset="utf-8">
<title>VFS bench</title>
<pre id="out">loading</pre>
<!--APP-->
```

```js
// web/bench/bench.js
// The spike's harness: runs the three load-bearing queries through
// openSnapshot, calling window.__mark(step) after each so the driver can read
// its own network counters. SQL copied from Index.groovy on 2026-09-25; Task 4
// moves the page's SQL into src/queries.json.
import { openSnapshot } from '../src/db.js'
import { createInlineWorker, inlineWasm } from '../src/inline.js'

const SQL = {
  producersOf: 'SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?',
  latestSuccessfulRun: "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1",
  itemsWhere: 'SELECT ci.item_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ? AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?) ORDER BY ci.item_cid',
  watermark: "SELECT value FROM meta WHERE key = 'store_log_watermark'",
}

const mark = (step, extra) => (window.__mark ? window.__mark(step, extra ?? null) : Promise.resolve())

window.bench = async ({ url, params, params2, cap }) => {
  const out = { steps: [] }
  const db = await openSnapshot(url, { cap, wasm: inlineWasm(), createWorker: createInlineWorker })
  out.mode = db.mode
  await db.query(SQL.watermark)
  await mark('open')
  for (const [phase, set] of [['cold', params], ['warm', params], ['other', params2]]) {
    for (const name of ['producersOf', 'latestSuccessfulRun', 'itemsWhere']) {
      const t = performance.now()
      const rows = await db.query(SQL[name], set[name])
      const step = { step: `${phase}:${name}`, ms: Math.round(performance.now() - t), rows: rows.length, first: rows[0] ? Object.values(rows[0])[0] : null }
      out.steps.push(step)
      await mark(step.step, step)
    }
  }
  await db.close()
  document.getElementById('out').textContent = JSON.stringify(out, null, 2)
  return out
}
document.getElementById('out').textContent = 'ready'
```

Run: `cd nf-blocks/web && node build.mjs`
Expected: `dist/bench.html` of roughly 1.3 to 1.6 MB (the wasm is about 850 KB before base64).

- [ ] **Step 10: Write the driver `gate/browser/bench.mjs` and `web/bench/run.sh`**

```js
// gate/browser/bench.mjs
// Drives web/dist/bench.html in Playwright's pinned Chromium and counts the
// requests and bytes that reach the snapshot URL between marks, twice over:
// from the browser's network events (Content-Length of each response, since a
// synchronous XHR's body is not always readable from the driver) and from the
// counting server's own /__stats. Prints one JSON object.
//   node gate/browser/bench.mjs <bench page url> <snapshot url> <params.json> <params2.json>
import { chromium } from 'playwright'
import { readFileSync } from 'node:fs'

const [pageUrl, snapshotUrl, paramsFile, params2File] = process.argv.slice(2)
const statsUrl = new URL('/__stats', pageUrl).href
const browser = await chromium.launch()
const context = await browser.newContext()
const seen = { requests: 0, bytes: 0 }
context.on('requestfinished', async (request) => {
  if (!request.url().startsWith(snapshotUrl)) return
  const response = await request.response()
  const headers = await response.allHeaders()
  seen.requests++
  seen.bytes += Number(headers['content-length'] || 0)
})
const page = await context.newPage()
const steps = []
await page.exposeFunction('__mark', async (step, extra) => {
  await new Promise((r) => setTimeout(r, 50))       // let requestfinished land
  const server = await (await fetch(statsUrl)).json()
  steps.push({ step, requests: seen.requests, bytes: seen.bytes,
               server: { requests: server.requests, bytes: server.bytes }, ...(extra ?? {}) })
  seen.requests = 0
  seen.bytes = 0
})
page.on('pageerror', (e) => console.error('[pageerror]', String(e)))
await fetch(statsUrl)                                   // reset the server's counters
await page.goto(pageUrl)
await page.waitForFunction(() => window.bench)
const result = await page.evaluate((opts) => window.bench(opts), {
  url: snapshotUrl,
  params: JSON.parse(readFileSync(paramsFile, 'utf8')),
  params2: JSON.parse(readFileSync(params2File, 'utf8')),
})
console.log(JSON.stringify({ mode: result.mode, steps }, null, 2))
await browser.close()
```

```bash
#!/usr/bin/env bash
# web/bench/run.sh: the VFS spike, end to end, in one command (the Bash sandbox
# forbids a later command connecting to a server an earlier one started).
#   web/bench/run.sh <schema-2 index or snapshot> [port]
# SNAPSHOT=<file> measures that file instead of the generated year;
# NO_RANGE=1 serves it the way stock `python -m http.server` does.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SCHEMA="$1"; PORT="${2:-8811}"
WORK="${BENCH_DIR:-${TMPDIR:-/tmp}/nf-blocks-bench}"
mkdir -p "$WORK/site/snap/index"
TARGET="${SNAPSHOT:-$WORK/year-v2.sqlite}"
[[ -f "$TARGET" ]] || python3 "$REPO/gate/gen_year.py" "$SCHEMA" "$TARGET"
ln -sf "$TARGET" "$WORK/site/snap/index/v2.sqlite"
( cd "$REPO/web" && node build.mjs > /dev/null )
cp "$REPO/web/dist/bench.html" "$WORK/site/bench.html"
python3 "$REPO/gate/year_params.py" "$TARGET" 0 > "$WORK/params0.json"
python3 "$REPO/gate/year_params.py" "$TARGET" 3 > "$WORK/params3.json"
python3 "$REPO/gate/browser/serve.py" "$WORK/site" "$PORT" ${NO_RANGE:+--no-range} & SERVER=$!
trap 'kill $SERVER' EXIT
for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$PORT/bench.html" && break; sleep 0.1; done
node "$REPO/gate/browser/bench.mjs" "http://127.0.0.1:$PORT/bench.html" \
     "http://127.0.0.1:$PORT/snap/index/v2.sqlite" "$WORK/params0.json" "$WORK/params3.json"
```

- [ ] **Step 11: Run the spike against the year snapshot**

The generator needs a schema-2 index for its DDL. Get one from a Gate run on current `main` (it takes a few minutes):

```bash
cd nf-blocks
export GATE_ROOT="$SCRATCH/g"          # SCRATCH = your session scratchpad
make gate || true                       # assertion 4 may flake; the index is what is needed
SCHEMA=$(ls "$GATE_ROOT"/cache/nf-blocks/*.sqlite | head -1)
sqlite3 "file:$SCHEMA?mode=ro" 'select version from schema_version'     # must print 2
chmod +x web/bench/run.sh
BENCH_DIR="$SCRATCH/bench" web/bench/run.sh "$SCHEMA" 8811 | tee "$SCRATCH/bench/out.json"
```

Expected: a JSON object with `"mode": "range"` and steps `open`, `cold:*`, `warm:*`, `other:*`. Warm steps show `requests: 0`, and each step's `requests` equals its `server.requests`. Generating the year takes a few minutes the first time and is cached under `BENCH_DIR`.

Pass criteria, each checked against the output:

| step | requests at most | bytes at most | reference (sql.js-httpvfs) |
|---|---|---|---|
| cold:producersOf | 9 | 65536 | 7 / 28 KB |
| cold:latestSuccessfulRun | 7 | 65536 | 5 / 20 KB |
| cold:itemsWhere | 44 | 524288 | 42 / 180 KB |
| warm:* | 0 | 0 | 0 |

"At most reference + 2 requests" is the reading of "must match" this plan uses; the byte limits are the Gate's. Also compare the `rows` and `first` values with `python3 -c "import sqlite3, json; ..."` over the same file and SQL (the prototype's `verify.py` shape): they must be equal.

Then the whole-file mode, over a small snapshot-shaped copy of the Gate's index (the index itself is WAL, so copy it with `VACUUM INTO` first):

```bash
sqlite3 "$SCHEMA" "PRAGMA page_size=4096; VACUUM INTO '$SCRATCH/bench/small.sqlite'"
SNAPSHOT="$SCRATCH/bench/small.sqlite" NO_RANGE=1 BENCH_DIR="$SCRATCH/bench" web/bench/run.sh "$SCHEMA" 8812
```

Expected: `"mode": "whole"`, one counted request for the snapshot in the `open` step (the whole file), zero in every query step, and the same `rows` and `first` values that `sqlite3` gives for the same SQL and `params0.json`.

The browser's count and the server's `/__stats` count must agree in every step; if they differ, the network-event count is wrong and Task 12 must not rely on it until that is understood.

If any criterion fails, **stop**. Do not tune the reader to pass (read-ahead trades bytes for requests, which spec section 1.3 says must be revisited deliberately). Write what was measured into `RESULTS.md` and report to Rob.

- [ ] **Step 12: Record the results**

Write `web/bench/RESULTS.md` with: date, machine, Chromium build (`npx playwright --version` in `gate/browser`), SQLite version (the worker's `open` result), the year file's size and sha256, one table per mode with requests, bytes and milliseconds per step next to the reference column above, and the whole-file result. Keep it under a page.

- [ ] **Step 13: Commit, then stop for review**

```bash
cd nf-blocks
git add web/package.json web/package-lock.json web/build.mjs web/src web/bench web/test gate/browser/bench.mjs
git commit -m "feat(web): read-only HTTP VFS on sqlite-wasm, with the spike's measurements

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Show Rob `web/bench/RESULTS.md`. Wave 1 starts only after he accepts the numbers.

---
### Task 3: The explorer contract in DESIGN.md

Later tasks run in parallel and code against this section, so it lands alone.

**Files:**
- Modify: `DESIGN.md` §1 (packages), §2 (configuration), §5 (layout), §12 (index), and a new §15

**Interfaces:**
- Consumes: the measured results of Task 2 (`web/bench/RESULTS.md`), accepted by Rob.
- Produces: the contract text below, which Tasks 4 to 13 cite as "DESIGN.md §15".

- [ ] **Step 1: Amend §1, §2, §5 and §12**

In §1 "Packages", after the `robsyme.cas.CasSession` bullet, add:

```markdown
  - `robsyme.cas.cli` — `CasCommands` (the `nextflow plugin nf-blocks:<verb>` dispatch, §15) and `Options`.
  - `robsyme.cas.explore` — `ExploreServer`, `MemberFiles` and its local and S3 implementations (§15).
- The page is an npm project under `web/`, built by Gradle into the plugin jar as
  `robsyme/cas/explorer/index.html` (block explorer spec section 2). Its output is not committed.
```

In §2, append to the `cas { }` example block, before its closing brace:

```groovy
    snapshot { maxBytes = 64.MB }   // optional; a run writes the Index Snapshot only while it is under this (§15)
```

and append these bullets at the end of §2:

```markdown
- `cas.snapshot.maxBytes` (a number of bytes, a `MemoryUnit`, or a string such as
  `'64 MB'`; default 64 MiB): the cap under which a run rewrites its member's
  Index Snapshot at `onFlowComplete` (§15).
- *Amended 2026-09-25:* a read-only member's `location` may be an S3 URI,
  `s3://<bucket>[/<prefix>]`. Only `nf-blocks:explore` reads such a member (§15).
  A run leaves S3 members out of its default `resolve` list and refuses one named
  in `cas.resolve`, because no S3 `BlockStore` exists yet. The writable member is
  always a local directory.
```

In §5's layout block, after the `nf/<key>/.data.json` line, add:

```
index/v<N>.sqlite       Index Snapshot of this member's rows, N = Index.SCHEMA_VERSION (§15). Derived,
                        rewritten whole by an atomic move, mode 0644. Not a block, not a root.
index.html              The explorer page, self-contained (§15). Rewritten when its bytes differ.
```

In §12, after the "Queries:" bullet, add:

```markdown
- *Amended 2026-09-25:* the three load-bearing queries' SQL is held in public
  constants (`Index.SQL_PRODUCERS_OF`, `SQL_LATEST_SUCCESSFUL_RUN`,
  `SQL_ITEMS_BASE`, `SQL_ITEMS_PREDICATE`, `SQL_ITEMS_PREDICATE_NULL`,
  `SQL_ITEMS_ORDER`, `SQL_COLLECTIONS_OF`), and the page's copy in
  `web/src/queries.json` is pinned equal to them by `ExplorerQueriesTest`, which
  also refuses any page query whose plan scans a table other than `run` through
  a covering index (§15).
```

- [ ] **Step 2: Add §15**

Append to the end of `DESIGN.md`:

````markdown
## 15. The block explorer (milestone 1)

Specified in `../.scratch/block-explorer/spec.md`; this section fixes the names,
paths and seams its pieces share. Plan: `docs/plans/2026-09-25-explorer-milestone-1.md`.

### Index Snapshot

- Path `<member>/index/v<Index.SCHEMA_VERSION>.sqlite`, today `index/v2.sqlite`.
  Class `robsyme.cas.core.IndexSnapshot`.
- Rows, milestone 1: every `run` row whose `member` is the member's alias, and
  the `collection`, `collection_item`, `item`, `producer`, `item_attr`,
  `consumer` and `missing` rows reached from those runs. Copied by SQL from the
  cache index; no block is read. `run.member` is written as NULL. Milestone 2
  replaces the `run.member` rule with `log_entry.member` (spec section 4).
- Same DDL as the cache index (`Index.ddl()`), same `schema_version`.
- `meta` holds exactly `store_log_watermark` (the member's watermark in the
  cache index, a Store Log entry name; absent when the member has no log) and
  `snapshot_written_at` (ISO-8601 UTC, milliseconds).
- Written by inserting into a fresh database, `PRAGMA page_size=4096`, then
  `VACUUM INTO` a temp file in `<member>/index/`, then an atomic move over the
  old file. One rollback-journal file, no sidecars.
- Writers: a run's `onFlowComplete`, after indexing, only while the existing
  snapshot is under `cas.snapshot.maxBytes` and only if the new one is too;
  `nf-blocks:snapshot` at any size; `nf-blocks:explore` at start and at exit, at
  any size. Every writer writes only the writable member's snapshot.
- `<member>/index.html` is the page from the plugin jar
  (`/robsyme/cas/explorer/index.html`), written beside the snapshot whenever a
  snapshot is written and its bytes differ.

### Plugin verbs

`CasPlugin implements nextflow.cli.PluginExecAware` and delegates to
`robsyme.cas.cli.CasCommands`. It does not use `PluginAbstractExec`, which
swallows every exception and returns 0 (v26.04.6,
`modules/nextflow/src/main/groovy/nextflow/cli/PluginAbstractExec.groovy`). The
config comes from `ConfigBuilder` over the launch directory and `-c`, exactly as
`PluginAbstractExec` builds it; no `Session` is created.

```
nextflow [-c <config>] plugin nf-blocks:snapshot
nextflow [-c <config>] plugin nf-blocks:explore [--port <n>]
```

`CmdPlugin` turns `--name value` into the argument pair `--name`, `value` after
the positional arguments. Exit 0 on success, 1 on a failure the verb reports, 2
on a usage error. An unpinned plugin id makes Nextflow prefetch registry
metadata for `nf-blocks`; set `NXF_OFFLINE=true` when the plugin was installed
locally (the Gate does).

### What a member serves

Relative to a member's base URL:

```
index/v2.sqlite            the Index Snapshot, read with single-range GETs
index.html                 the page
blocks/<xx>/<cid>          a block, xx = the cid's last two characters
log/                       the Store Log listing (one of the three forms below)
```

The page lists `log/` in the first form that answers:

1. `GET <base>log/` answering `200` with `Content-Type: application/json`:
   `{"entries": ["<rts>-<kind>-<cid>", ...]}`, sorted ascending (newest first).
   This is what `nf-blocks:explore` serves.
2. `GET <base>log/` answering `200` with HTML: a static server's directory
   index; entry names are the `href` values that parse as Store Log names.
3. S3 `ListObjectsV2` at `<origin>/?list-type=2&prefix=<base path>log/`,
   following `NextContinuationToken`, stopping once a page's last key is older
   than the overlap floor. Virtual-hosted-style bucket URLs only.

If none answers, the member has no log and the tail is empty.

A directly browsed bucket needs the policy and CORS rule of spec section 6:
`s3:GetObject` and `s3:ListBucket` for `Principal "*"`, Block Public Access's
`BlockPublicPolicy` and `RestrictPublicBuckets` off, and a CORS rule allowing
`GET` and `HEAD` with `AllowedHeaders` including `range` and `ExposeHeaders`
`Content-Range`, `Content-Length`, `Accept-Ranges`, `ETag`.

### `nf-blocks:explore`

A JDK `HttpServer` bound to `InetAddress.getLoopbackAddress()`, port `--port`
or ephemeral. Prints `nf-blocks explorer: http://127.0.0.1:<port>/` on stdout
once it is listening, then blocks until the JVM is interrupted.

| Request | Answer |
|---|---|
| `GET /`, `GET /index.html` | the page |
| `GET /members.json` | `{"members": [{"alias", "writable", "base": "m/<alias>/"}, ...]}`, writable first |
| `GET` or `HEAD /m/<alias>/index/v<N>.sqlite` | the file, honouring one `Range` |
| `GET` or `HEAD /m/<alias>/blocks/<xx>/<cid>` | the block, honouring one `Range`; `xx` must equal the cid's last two characters |
| `GET /m/<alias>/log/` | listing form 1 |
| anything else | `404`; a method other than `GET` or `HEAD` is `405` |

`Host` must be `127.0.0.1:<port>` or `localhost:<port>`, and `Origin`, when
sent, `http://127.0.0.1:<port>` or `http://localhost:<port>`; otherwise `403`.
No CORS headers are sent. No other path under a member is ever served:
`coords/` and `nf/` hold host paths. Members are every configured store
(`cas.stores`), local ones read from disk, S3 ones with the AWS SDK default
credential chain (`AWS_PROFILE`, SSO) and ranged `GetObject`.

### The page

- Store: `?store=<base URL>` if given; else, if `./members.json` answers, the
  member `?member=<alias>` or the first listed, at `./m/<alias>/`; else the
  page's own directory (spec section 5.1).
- `?cap=<bytes>` overrides the 64 MiB whole-file cap.
- Routes (location hash): `#/` home, `#/idle` (opens the snapshot and does
  nothing else), `#/pipeline/<name>`, `#/run/<completion>`,
  `#/collection/<collection>`, `#/item/<collection>/<item>`,
  `#/content/<cid>` (query 1), `#/latest/<pipeline>` (query 2),
  `#/items/<completion>/<output>?where=<JSON [[path, type, value], ...]>`
  (query 3, per run; `type` is one of `string`, `int`, `float`, `bool`, `null`).
- SQL: only the statements in `web/src/queries.json`.
- Blocks: fetched from `<base>blocks/<xx>/<cid>`, SHA-256 checked against the
  requested CID, then decoded and checked against the IPLD Schema of §6
  (extracted from this file at build time), before use.
- Stale runs: Store Log `run` entries since the snapshot's watermark minus the
  10-minute overlap (floor clamped to the local clock), less those the snapshot
  holds. For each, the RunCompletion and its RunManifest are fetched; a run's
  collections and items only when it is opened or a query needs them.

The DOM the Gate reads, and nothing else it may rely on:

| Selector | Meaning |
|---|---|
| `body[data-state]` | `loading`, `ready` or `error`, for the current render |
| `body[data-render]` | a counter, incremented when a render finishes |
| `body[data-route]` | the current route's name |
| `#snapshot-mode[data-mode]` | `range` or `whole` |
| `#stale[data-stale-count]` | runs newer than the snapshot; `#stale [data-command]` once past the notice thresholds |
| `[data-run]` | one run: `data-run` completion cid, `data-pipeline`, `data-status`, `data-source` (`snapshot` or `tail`) |
| `[data-collection]` | one output of a run: `data-collection` cid, `data-output` |
| `[data-producer]` | one query 1 row: `data-content`, `data-item`, `data-collection`, `data-completion`, `data-filename` |
| `[data-latest]` | query 2's answer, a completion cid or empty |
| `[data-item-result]` | one query 3 item cid |
| `[data-error]` | an error: `data-error` code, `data-cid` when a block is to blame |
| `window.__nfBlocks.verified` | every cid whose bytes the page hashed and accepted |

Error codes: `no_snapshot`, `no_range_over_cap`, `cors_headers`,
`fetch_failed`, `query_failed`, `worker_failed`, `hash_mismatch`,
`schema_invalid`, `block_missing`, `not_found`, `bad_route`, `bad_predicate`.

The query views (query 1, 2 and 3) read the snapshot only through their own
statement, so Gate assertion 2's counts are the query's cost.

### Decisions made where the spec is silent (2026-09-25)

1. Milestone 1 selects snapshot rows by `run.member` (above).
2. `explore` rewrites the snapshot at start as well as at exit, so opening a
   member through it clears the stale notice (spec section 5.4).
3. The tail fetches a stale run's RunManifest with its RunCompletion, for the
   pipeline name.
4. Query 3 is per run; matching across runs waits for milestone 2's
   `collection_item(item_cid)` index.
5. Run-list anomalies come from each visible run's RunCompletion, fetched lazily.
6. The page is one self-contained `index.html`.
7. The whole-file cap is 64 MiB, `?cap=` per load.
8. No launch token until the write endpoint (milestone 2); `Host` and `Origin`
   checks from the start.
9. S3 members are read-only and explore-only.
10. Nothing is filtered by `delete` Claims until Claims exist (milestone 2).
````

- [ ] **Step 3: Check the amendments landed where intended**

Run: `cd nf-blocks && grep -n '^## \|^### ' DESIGN.md | tail -12 && grep -c 'index/v<N>.sqlite\|snapshot { maxBytes\|SQL_PRODUCERS_OF\|robsyme.cas.explore' DESIGN.md`
Expected: `## 15. The block explorer (milestone 1)` with its six `###` subsections at the end, and a count of `4`.

- [ ] **Step 4: Commit**

```bash
cd nf-blocks
git add DESIGN.md
git commit -m "docs(design): the block explorer's milestone 1 contract (section 15)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: One set of SQL for the plugin and the page, and the query-plan guard

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (constants; `ddl()`; `watermark(String)`; `items` and `collectionsOf` use the constants)
- Create: `web/src/queries.json`
- Create: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy`

**Interfaces:**
- Consumes: `Index` as it is on `main`.
- Produces:
  - `Index.SQL_PRODUCERS_OF`, `SQL_LATEST_SUCCESSFUL_RUN`, `SQL_ITEMS_BASE`, `SQL_ITEMS_PREDICATE`, `SQL_ITEMS_PREDICATE_NULL`, `SQL_ITEMS_ORDER`, `SQL_COLLECTIONS_OF`: `public static final String`.
  - `static List<String> Index.ddl()`: every CREATE statement of the schema, in order.
  - `String Index.watermark(String member)`: the member's Store Log watermark, or null.
  - `web/src/queries.json` keys: `producersOf`, `latestSuccessfulRun`, `itemsBase`, `itemsPredicate`, `itemsPredicateNull`, `itemsOrder`, `collectionsOf`, `watermark`, `pipelines`, `runsOfPipeline`, `runByCompletion`, `runsKnown`, `collectionByCid`, `collectionItems`. Parameters are positional `?`, in the order the SQL names them.

- [ ] **Step 1: Write `web/src/queries.json`**

```json
{
  "producersOf": "SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?",
  "latestSuccessfulRun": "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1",
  "itemsBase": "SELECT ci.item_cid, ci.collection_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?",
  "itemsPredicate": " AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?)",
  "itemsPredicateNull": " AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value IS NULL)",
  "itemsOrder": " ORDER BY ci.item_cid",
  "collectionsOf": "SELECT output_name, collection_cid FROM collection WHERE completion_cid = ? ORDER BY output_name",
  "watermark": "SELECT value FROM meta WHERE key = 'store_log_watermark'",
  "pipelines": "SELECT pipeline, count(*) AS runs, max(finished_at) AS latest FROM run GROUP BY pipeline ORDER BY pipeline",
  "runsOfPipeline": "SELECT completion_cid, manifest_cid, pipeline, run_name, status, possibly_incomplete, finished_at FROM run WHERE pipeline = ? ORDER BY finished_at DESC, completion_cid ASC LIMIT ? OFFSET ?",
  "runByCompletion": "SELECT completion_cid, manifest_cid, pipeline, run_name, status, possibly_incomplete, finished_at FROM run WHERE completion_cid = ?",
  "runsKnown": "SELECT completion_cid FROM run WHERE completion_cid IN (SELECT value FROM json_each(?))",
  "collectionByCid": "SELECT collection_cid, completion_cid, output_name FROM collection WHERE collection_cid = ?",
  "collectionItems": "SELECT item_cid FROM collection_item WHERE collection_cid = ? ORDER BY item_cid LIMIT ? OFFSET ?"
}
```

- [ ] **Step 2: Write the failing test**

```groovy
// src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The page runs the plugin's own SQL (block explorer spec section 14: "no
 * second query engine to drift"), and every page query must be answerable
 * from an index on a year-scale snapshot fetched page by page over HTTP. A
 * SCAN of item_attr there is 2.7 million rows (DESIGN.md §15).
 */
class ExplorerQueriesTest extends Specification {

    @Shared Map<String, String> page = (Map<String, String>) new JsonSlurper()
        .parse(Path.of('web/src/queries.json').toFile())

    @TempDir
    Path tempDir

    Connection connection

    def setup() {
        final Path file = tempDir.resolve('index.sqlite')
        Index.open(file).close()
        connection = DriverManager.getConnection("jdbc:sqlite:${file}")
    }

    def cleanup() {
        connection?.close()
    }

    def 'the load-bearing queries are the plugin constants, character for character'() {
        expect:
        page.producersOf == Index.SQL_PRODUCERS_OF
        page.latestSuccessfulRun == Index.SQL_LATEST_SUCCESSFUL_RUN
        page.itemsBase == Index.SQL_ITEMS_BASE
        page.itemsPredicate == Index.SQL_ITEMS_PREDICATE
        page.itemsPredicateNull == Index.SQL_ITEMS_PREDICATE_NULL
        page.itemsOrder == Index.SQL_ITEMS_ORDER
        page.collectionsOf == Index.SQL_COLLECTIONS_OF
    }

    def 'every page query runs against the real schema'() {
        expect:
        page.each { String name, String sql ->
            if( name.startsWith('itemsPredicate') || name == 'itemsOrder' )
                return
            plan(sql)     // throws on a syntax error or an unknown column
        }
    }

    def "no page query scans a table, except run through a covering index (#name)"() {
        when:
        final List<String> details = plan(sql)
        final List<String> scans = details.findAll { String d -> d.startsWith('SCAN ') && !ALLOWED_SCANS.any { d.startsWith(it) } }

        then:
        scans == []

        where:
        name                      | sql
        'producersOf'             | page.producersOf
        'latestSuccessfulRun'     | page.latestSuccessfulRun
        'items, no predicate'     | page.itemsBase + page.itemsOrder
        'items, one predicate'    | page.itemsBase + page.itemsPredicate + page.itemsOrder
        'items, two predicates'   | page.itemsBase + page.itemsPredicate + page.itemsPredicateNull + page.itemsOrder
        'collectionsOf'           | page.collectionsOf
        'watermark'               | page.watermark
        'pipelines'               | page.pipelines
        'runsOfPipeline'          | page.runsOfPipeline
        'runByCompletion'         | page.runByCompletion
        'runsKnown'               | page.runsKnown
        'collectionByCid'         | page.collectionByCid
        'collectionItems'         | page.collectionItems
    }

    def 'the guard itself catches a scan'() {
        expect:
        plan('SELECT path, type, value FROM item_attr WHERE item_cid = ?').any { it.startsWith('SCAN item_attr') }
    }

    def 'Index.items still answers through the shared constants'() {
        given:
        final Path storeRoot = tempDir.resolve('store')
        final LocalBlockStore store = new LocalBlockStore(storeRoot, 'lab', true)
        final Index index = Index.open(tempDir.resolve('items.sqlite'))
        final Cid content = Fixtures.contentCid('bytes of A')
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', content, 10L)]))
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [:]))
        index.ingestRun(store, completion, 'lab')

        expect:
        index.items(completion, 'aligned', [sample: 'A']) == [item]
        index.items(completion, 'aligned', [sample: 'B']) == []
        index.producersOf(content)*.itemCid == [item]
        index.collectionsOf(completion) == [aligned: collection]

        cleanup:
        index.close()
    }

    def 'the watermark is readable per member'() {
        given:
        final Index index = Index.open(tempDir.resolve('w.sqlite'))

        expect:
        index.watermark('lab') == null

        cleanup:
        index.close()
    }

    static final List<String> ALLOWED_SCANS = [
        'SCAN run USING COVERING INDEX',
        'SCAN json_each',
        'SCAN CONSTANT ROW',
    ]

    private List<String> plan(String sql) {
        final int parameters = sql.count('?')
        final def statement = connection.prepareStatement('EXPLAIN QUERY PLAN ' + sql)
        try {
            for( int i = 1; i <= parameters; i++ )
                statement.setObject(i, sql.contains('json_each') && i == 1 ? '["x"]' : 'x')
            final def rs = statement.executeQuery()
            final List<String> details = []
            while( rs.next() )
                details << rs.getString('detail')
            return details
        }
        finally {
            statement.close()
        }
    }
}
```

`Fixtures` is the existing helper in `src/test/groovy/robsyme/cas/core/Fixtures.groovy` (`contentCid`, `leaf(name, address, size)`, `outputItem(value)`, `runManifest(overrides)`, `outputCollection(run, name, [[itemCid, [paths]], ...])`, `runCompletion(run, collections, overrides)`).

A `where:` block can read only `@Shared` and static fields, which is why `page` is `@Shared`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd nf-blocks && ./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest`
Expected: FAIL to compile, `No such property: SQL_PRODUCERS_OF for class: robsyme.cas.core.Index` (and `ddl`, `watermark`).

- [ ] **Step 4: Add the constants, `ddl()` and `watermark()` to `Index`**

Add to `Index`, below `SCHEMA_VERSION`:

```groovy
    // The SQL the block explorer's page runs too (DESIGN.md §12, §15). The page
    // holds a copy in web/src/queries.json; ExplorerQueriesTest pins them equal.

    static final String SQL_PRODUCERS_OF =
        'SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?'

    static final String SQL_LATEST_SUCCESSFUL_RUN =
        "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1"

    // ci.collection_cid rides along so the page can link each item without a
    // second lookup; it is on the collection_item row already read, so it costs no page.
    static final String SQL_ITEMS_BASE =
        'SELECT ci.item_cid, ci.collection_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?'

    static final String SQL_ITEMS_PREDICATE =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?)'

    static final String SQL_ITEMS_PREDICATE_NULL =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value IS NULL)'

    static final String SQL_ITEMS_ORDER = ' ORDER BY ci.item_cid'

    static final String SQL_COLLECTIONS_OF =
        'SELECT output_name, collection_cid FROM collection WHERE completion_cid = ? ORDER BY output_name'
```

Split `createSchema` so the DDL list is its own method (the snapshot writer, Task 5, builds the same tables):

```groovy
    /** Every CREATE statement of the schema of DESIGN.md §12, in order. */
    static List<String> ddl() {
        return [
            // ... move the existing list literal from createSchema here, unchanged ...
        ]
    }

    private static void createSchema(Connection connection) {
        final Statement statement = connection.createStatement()
        try {
            for( String sql : ddl() )
                statement.executeUpdate(sql)
        }
        finally {
            statement.close()
        }
        // ... the existing schema_version insert, unchanged ...
    }
```

Move the list, do not retype it: cut the `final List<String> ddl = [ ... ]` literal out of `createSchema` and paste it as the body of `ddl()`.

Add below `isRunIndexed`:

```groovy
    /** The member's Store Log watermark, the newest entry name catch-up has read, or null. */
    String watermark(String member) {
        return meta(watermarkKey(member))
    }
```

Rewrite `producersOf`, `latestSuccessfulRun`, `items` and `collectionsOf` to use the constants, with the same behaviour:

```groovy
    List<ProducerRow> producersOf(Cid content) {
        final List<ProducerRow> rows = new ArrayList<ProducerRow>()
        query(SQL_PRODUCERS_OF, [content.toString()]) { ResultSet rs ->
            rows.add(new ProducerRow(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2)),
                Cid.parse(rs.getString(3)), Cid.parse(rs.getString(4)), rs.getString(5)))
        }
        return rows
    }

    Optional<Cid> latestSuccessfulRun(String pipeline) {
        return firstCid(SQL_LATEST_SUCCESSFUL_RUN, [pipeline])
    }

    List<Cid> items(Cid completion, String outputName, Map<String, Object> where) {
        final StringBuilder sql = new StringBuilder(SQL_ITEMS_BASE)
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        for( Map.Entry<String, Object> entry : (where ?: [:]).entrySet() ) {
            final AttrRow row = MetadataView.scalar(entry.key, entry.value)
            if( row.truncated ) {
                // A value stored as a digest refuses equality, so nothing can match.
                log.warn("the predicate on '${entry.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes and cannot be matched")
                return []
            }
            parameters.add(row.path)
            parameters.add(row.type)
            if( row.value == null ) {
                sql.append(SQL_ITEMS_PREDICATE_NULL)
            }
            else {
                sql.append(SQL_ITEMS_PREDICATE)
                parameters.add(row.value)
            }
        }
        sql.append(SQL_ITEMS_ORDER)
        final List<Cid> items = new ArrayList<Cid>()
        query(sql.toString(), parameters) { ResultSet rs -> items.add(Cid.parse(rs.getString(1))) }
        return items
    }

    Map<String, Cid> collectionsOf(Cid completion) {
        final Map<String, Cid> collections = new LinkedHashMap<String, Cid>()
        query(SQL_COLLECTIONS_OF, [completion.toString()]) { ResultSet rs ->
            collections.put(rs.getString(1), Cid.parse(rs.getString(2)))
        }
        return collections
    }
```

Keep the existing Javadoc comments on each method.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd nf-blocks && ./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest --tests robsyme.cas.core.IndexTest`
Expected: PASS. If a query-plan row fails, print `plan(sql)` for it: a page query that scans is a design bug to fix in `queries.json` (add the missing index to the query's shape, or fetch the block instead), never by widening `ALLOWED_SCANS`.

Then `./gradlew test` for the whole suite: PASS, the 458 existing tests plus the new ones.

- [ ] **Step 6: Commit**

```bash
cd nf-blocks
git add src/main/groovy/robsyme/cas/core/Index.groovy web/src/queries.json \
        src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
git commit -m "feat(core): the page and the index share one set of SQL, pinned with a query-plan guard

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 5: The Index Snapshot writer, written after every run

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy`
- Create: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasConfig.groovy` (`indexOverride`, `snapshotMaxBytes`)
- Modify: `src/main/groovy/robsyme/cas/CasConfigScope.groovy` (`cas.snapshot` scope)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`openIndex()`, `snapshotWritable()`)
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`indexRun` writes the snapshot; `openIndex` delegates)
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy` (`openIndex` delegates)
- Modify: `src/test/groovy/robsyme/cas/CasConfigTest.groovy`, `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`
- Create: `web/test/config.test.mjs`

**Interfaces:**
- Consumes: `Index.ddl()`, `Index.watermark(String)`, `Index.getFile()`, `Index.SCHEMA_VERSION` (Task 4).
- Produces:
  - `class IndexSnapshot` in `robsyme.cas.core`: `DEFAULT_MAX_BYTES = 64L * 1024 * 1024`, `PAGE_SIZE = 4096`, `WATERMARK_KEY = 'store_log_watermark'`, `WRITTEN_AT_KEY = 'snapshot_written_at'`, `PAGE_NAME = 'index.html'`, `PAGE_RESOURCE = '/robsyme/cas/explorer/index.html'`; `static String relativePath()`; `static Path pathIn(Path memberRoot)`; `static Result write(Index index, String member, Path memberRoot, long maxBytes)` (`maxBytes <= 0`: no cap); `static boolean writePage(Path memberRoot, byte[] page)`; `static byte[] bundledPage()` (null when the build has no page).
  - `IndexSnapshot.Result`: `boolean written`, `Path path`, `long bytes`, `int runs`, `String watermark`, `String skipped` (null, or `'over_cap'`).
  - `CasConfig.indexOverride` (String or null), `CasConfig.snapshotMaxBytes` (long).
  - `CasSession.openIndex() -> Index`, `CasSession.snapshotWritable(Index index, long maxBytes) -> IndexSnapshot.Result`.

- [ ] **Step 1: Write the failing snapshot tests**

```groovy
// src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §15: a member's Index Snapshot holds that member's runs and what
 * they reach, copied by SQL from the cache index into one rollback-journal
 * file of 4096-byte pages, replaced atomically.
 */
class IndexSnapshotTest extends Specification {

    @TempDir
    Path tempDir

    Path labRoot
    Path sharedRoot
    LocalBlockStore lab
    LocalBlockStore shared
    Index index

    def setup() {
        labRoot = tempDir.resolve('lab')
        sharedRoot = tempDir.resolve('shared')
        lab = new LocalBlockStore(labRoot, 'lab', true)
        shared = new LocalBlockStore(sharedRoot, 'shared', false)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    /** One run of one item per sample into a store; returns [completion, collection, items by sample]. */
    private List run(LocalBlockStore store, String runName, List<String> samples) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: runName, nf_run_hash: "hash-$runName"))
        final List<List> withPaths = []
        final Map<String, Cid> items = [:]
        for( String sample : samples ) {
            final Cid item = store.putDagCbor(Fixtures.outputItem(
                [[sample: sample], Fixtures.leaf("${sample}.bam".toString(), Fixtures.contentCid("bam-$sample"), 10L)]))
            items[sample] = item
            withPaths << [item, ["aligned/${sample}.bam".toString()]]
        }
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', withPaths))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, System.currentTimeMillis())
        return [completion, collection, items]
    }

    private void catchUp() {
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(shared, StoreLog.of(shared), 'shared')
    }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final int n = rs.metaData.columnCount
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..n).collect { int i -> rs.getObject(i) }
            return out
        }
        finally {
            c.close()
        }
    }

    def 'the snapshot holds the member own runs and only what they reach'() {
        given:
        final List a = run(lab, 'a', ['A', 'B'])
        final List b = run(shared, 'b', ['B', 'C'])       // item B is the same block in both runs
        catchUp()

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 0L)
        final Path file = result.path

        then:
        result.written
        result.runs == 1
        file == labRoot.resolve('index/v2.sqlite')
        rows(file, 'SELECT completion_cid, member FROM run') == [[a[0].toString(), null]]
        rows(file, 'SELECT collection_cid FROM collection')*.get(0) == [a[1].toString()]
        rows(file, 'SELECT DISTINCT completion_cid FROM producer')*.get(0) == [a[0].toString()]
        rows(file, 'SELECT item_cid FROM item ORDER BY item_cid')*.get(0) == ((Map) a[2]).values()*.toString().sort()
        rows(file, "SELECT count(*) FROM item_attr WHERE path = 'sample'") == [[2]]
        !rows(file, 'SELECT completion_cid FROM run')*.get(0).contains(b[0].toString())
    }

    def 'the snapshot is one rollback-journal file of 4096-byte pages with the cache schema'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path
        final byte[] header = Files.newInputStream(file).withCloseable { it.readNBytes(100) }

        then:
        (((header[16] & 0xff) << 8) | (header[17] & 0xff)) == 4096
        header[18] == 1 && header[19] == 1
        ['-wal', '-shm', '-journal'].every { !Files.exists(file.resolveSibling(file.fileName.toString() + it)) }
        rows(file, 'SELECT version FROM schema_version') == [[Index.SCHEMA_VERSION]]
        rows(file, "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name") ==
            rows(index.file, "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        Files.list(file.parent).withCloseable { it.toList() }*.fileName*.toString() == ['v2.sqlite']
    }

    def 'meta carries the member watermark and the time it was written'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path
        final Map meta = rows(file, 'SELECT key, value FROM meta').collectEntries { [(it[0]): it[1]] }

        then:
        meta.keySet() == ['store_log_watermark', 'snapshot_written_at'] as Set
        meta.store_log_watermark == index.watermark('lab')
        meta.store_log_watermark ==~ /\d{13}-run-b[a-z2-7]{58}/
        meta.snapshot_written_at ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
    }

    def 'a member with no log has no watermark key'() {
        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path

        then:
        rows(file, 'SELECT key FROM meta')*.get(0) == ['snapshot_written_at']
        rows(file, 'SELECT count(*) FROM run') == [[0]]
    }

    def 'an existing snapshot at or over the cap is left untouched'() {
        given:
        final Path file = IndexSnapshot.pathIn(labRoot)
        Files.createDirectories(file.parent)
        Files.write(file, ('x' * 100).bytes)

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 100L)

        then:
        !result.written
        result.skipped == 'over_cap'
        new String(Files.readAllBytes(file)) == 'x' * 100
    }

    def 'a new snapshot over the cap does not replace the old one'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()
        final Path file = IndexSnapshot.pathIn(labRoot)
        Files.createDirectories(file.parent)
        Files.write(file, 'old'.bytes)

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 1000L)

        then:
        result.skipped == 'over_cap'
        result.bytes > 1000L
        new String(Files.readAllBytes(file)) == 'old'
        Files.list(file.parent).withCloseable { it.toList() }.size() == 1
    }

    def 'concurrent writers leave a complete database, never a torn one (Review Focus 5)'() {
        given:
        ['A', 'B', 'C', 'D'].each { run(lab, "run-$it", [it]) }
        catchUp()
        final def pool = Executors.newFixedThreadPool(4)
        final CountDownLatch go = new CountDownLatch(1)
        final List<Throwable> failures = Collections.synchronizedList([])

        when:
        4.times {
            pool.submit {
                final Index own = Index.open(index.file)
                try {
                    go.await()
                    5.times { IndexSnapshot.write(own, 'lab', labRoot, 0L) }
                }
                catch( Throwable t ) {
                    failures << t
                }
                finally {
                    own.close()
                }
            }
        }
        go.countDown()
        pool.shutdown()
        pool.awaitTermination(60, TimeUnit.SECONDS)
        final Path file = IndexSnapshot.pathIn(labRoot)

        then:
        failures == []
        rows(file, 'PRAGMA integrity_check') == [['ok']]
        rows(file, 'SELECT count(*) FROM run') == [[4]]
        Files.list(file.parent).withCloseable { it.toList() }*.fileName*.toString() == ['v2.sqlite']
    }

    def 'the page is written beside the snapshot only when its bytes differ'() {
        given:
        final byte[] page = '<!doctype html><title>x</title>'.bytes

        expect:
        IndexSnapshot.writePage(labRoot, page)
        Files.readAllBytes(labRoot.resolve('index.html')) == page
        !IndexSnapshot.writePage(labRoot, page)
        IndexSnapshot.writePage(labRoot, '<!doctype html><title>y</title>'.bytes)
    }
}
```

Add to `CasConfigTest`:

```groovy
    def 'cas.snapshot.maxBytes defaults to 64 MiB and accepts bytes, MemoryUnit and text'() {
        expect:
        CasConfig.from(storeConfig(), 'cas://lab').snapshotMaxBytes == 64L * 1024 * 1024
        CasConfig.from(withSnapshot(1000), 'cas://lab').snapshotMaxBytes == 1000L
        CasConfig.from(withSnapshot(nextflow.util.MemoryUnit.of('2 MB')), 'cas://lab').snapshotMaxBytes == 2L * 1024 * 1024
        CasConfig.from(withSnapshot('3 MB'), 'cas://lab').snapshotMaxBytes == 3L * 1024 * 1024
    }

    def 'a non-positive snapshot cap is refused'() {
        when:
        CasConfig.from(withSnapshot(0), 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.snapshot.maxBytes')
    }

    def 'cas.index.path is carried as the index override'() {
        expect:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], index: [path: '/tmp/i.sqlite']]], 'cas://lab').indexOverride == '/tmp/i.sqlite'
        CasConfig.from(storeConfig(), 'cas://lab').indexOverride == null
    }

    private Map withSnapshot(Object maxBytes) {
        return [cas: [stores: [lab: [location: '/data/cas']], snapshot: [maxBytes: maxBytes]]]
    }
```

`storeConfig()` is the helper `CasConfigTest` already has; read it and use it as is.

Add to `CasObserverTest`:

```groovy
    private static List<List<Object>> snapshotRows(Path db, String sql) {
        final def c = java.sql.DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final List<List<Object>> out = []
            while( rs.next() )
                out << [rs.getObject(1)]
            return out
        }
        finally {
            c.close()
        }
    }

    def 'onFlowComplete writes the writable member Index Snapshot after indexing'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()
        final Path snapshot = tempDir.resolve('store/index/v2.sqlite')

        then:
        Files.isRegularFile(snapshot)
        snapshotRows(snapshot, 'SELECT count(*) FROM run') == [[1]]
    }

    def 'a snapshot already over cas.snapshot.maxBytes is not rewritten by a run'() {
        given:
        final Map cfg = config()
        ((Map) cfg.cas).snapshot = [maxBytes: 1]
        bind(cfg)
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        final Path snapshot = tempDir.resolve('store/index/v2.sqlite')
        Files.createDirectories(snapshot.parent)
        Files.write(snapshot, 'old'.bytes)

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        new String(Files.readAllBytes(snapshot)) == 'old'
    }

    def 'a snapshot failure does not fail the run'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        // A directory where the snapshot file must go makes the atomic move fail.
        Files.createDirectories(tempDir.resolve('store/index/v2.sqlite/blocker'))

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
    }
```

```js
// web/test/config.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { SCHEMA_VERSION, SNAPSHOT_PATH } from '../src/config.js'

test('the page reads the snapshot of the schema the plugin writes', () => {
  const index = readFileSync(new URL('../../src/main/groovy/robsyme/cas/core/Index.groovy', import.meta.url), 'utf8')
  const declared = Number(/static final int SCHEMA_VERSION = (\d+)/.exec(index)[1])
  assert.equal(SCHEMA_VERSION, declared)
  assert.equal(SNAPSHOT_PATH, `index/v${declared}.sqlite`)
})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd nf-blocks && ./gradlew test --tests robsyme.cas.core.IndexSnapshotTest --tests robsyme.cas.CasConfigTest --tests robsyme.cas.trace.CasObserverTest`
Expected: FAIL to compile, `unable to resolve class IndexSnapshot`, `No such property: snapshotMaxBytes`.

Run: `cd nf-blocks/web && npm test`
Expected: PASS already for `config.test.mjs` (both say 2); it exists to fail the day one side changes.

- [ ] **Step 3: Write `IndexSnapshot`**

```groovy
// src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy
package robsyme.cas.core

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.SecureRandom
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * A member's Index Snapshot (DESIGN.md §15, block explorer spec section 4):
 * the member's own rows, copied out of the per-user cache index by SQL into
 * `<member>/index/v<N>.sqlite`, so a static page can read them with HTTP range
 * requests. Derived and rebuildable; never a source of truth.
 */
@Slf4j
@CompileStatic
class IndexSnapshot {

    static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024
    static final int PAGE_SIZE = 4096
    static final String WATERMARK_KEY = 'store_log_watermark'
    static final String WRITTEN_AT_KEY = 'snapshot_written_at'
    static final String PAGE_NAME = 'index.html'
    static final String PAGE_RESOURCE = '/robsyme/cas/explorer/index.html'

    private static final SecureRandom RANDOM = new SecureRandom()
    private static final DateTimeFormatter MILLIS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    /** The member's runs; alias written as NULL, since an alias is a local label. */
    private static final String COPY_RUNS = '''
        INSERT INTO main.run
        SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
               run_name, asserted_by, status, possibly_incomplete, finished_at, NULL
        FROM src.run WHERE member = ?'''

    /** Everything those runs reach, in dependency order. */
    private static final List<String> COPY_CLOSURE = [
        'INSERT INTO main.collection SELECT * FROM src.collection WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.collection_item SELECT * FROM src.collection_item WHERE collection_cid IN (SELECT collection_cid FROM main.collection)',
        'INSERT INTO main.item SELECT DISTINCT item_cid FROM main.collection_item',
        'INSERT INTO main.producer SELECT * FROM src.producer WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.item_attr SELECT * FROM src.item_attr WHERE item_cid IN (SELECT item_cid FROM main.item)',
        'INSERT INTO main.consumer SELECT * FROM src.consumer WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        '''INSERT INTO main.missing SELECT * FROM src.missing
           WHERE have_cid IN (SELECT completion_cid FROM main.run UNION SELECT collection_cid FROM main.collection)''',
    ]

    /** What one write did. */
    @CompileStatic
    static final class Result {
        final boolean written
        final Path path
        final long bytes
        final int runs
        final String watermark
        /** Null when written; `over_cap` when the cap kept the old snapshot. */
        final String skipped

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped) {
            this.written = written
            this.path = path
            this.bytes = bytes
            this.runs = runs
            this.watermark = watermark
            this.skipped = skipped
        }

        @Override
        String toString() { "IndexSnapshot.Result[written=$written, path=$path, bytes=$bytes, runs=$runs, skipped=$skipped]" }
    }

    static String relativePath() { "index/v${Index.SCHEMA_VERSION}.sqlite" }

    static Path pathIn(Path memberRoot) { memberRoot.resolve(relativePath()) }

    /**
     * Writes the snapshot of {@code member} into {@code memberRoot}. With a
     * positive {@code maxBytes}, an existing snapshot at or over it is left
     * alone, and a new one over it is discarded, keeping the old.
     */
    static Result write(Index index, String member, Path memberRoot, long maxBytes) {
        final Path target = pathIn(memberRoot)
        if( maxBytes > 0 && Files.isRegularFile(target) && Files.size(target) >= maxBytes )
            return new Result(false, target, Files.size(target), -1, null, 'over_cap')
        Files.createDirectories(target.parent)
        final String token = token()
        final Path build = target.resolveSibling(".tmp-${token}.build")
        final Path vacuumed = target.resolveSibling(".tmp-${token}.sqlite")
        try {
            final String watermark = index.watermark(member)
            final int runs = buildAndVacuum(build, vacuumed, index.file, member, watermark)
            final long bytes = Files.size(vacuumed)
            if( maxBytes > 0 && bytes > maxBytes )
                return new Result(false, target, bytes, runs, watermark, 'over_cap')
            if( FileSystems.default.supportedFileAttributeViews().contains('posix') )
                Files.setPosixFilePermissions(vacuumed, PosixFilePermissions.fromString('rw-r--r--'))
            Files.move(vacuumed, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return new Result(true, target, bytes, runs, watermark, null)
        }
        finally {
            for( Path path : [build, vacuumed] )
                for( String suffix : ['', '-journal', '-wal', '-shm'] )
                    Files.deleteIfExists(path.resolveSibling(path.fileName.toString() + suffix))
        }
    }

    private static int buildAndVacuum(Path build, Path out, Path cacheFile, String member, String watermark) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${build.toAbsolutePath()}".toString())
        try {
            exec(c, "PRAGMA page_size=${PAGE_SIZE}".toString())
            exec(c, 'PRAGMA synchronous=OFF')
            for( String sql : Index.ddl() )
                exec(c, sql)
            update(c, 'INSERT INTO schema_version(version) VALUES (?)', [(Object) Index.SCHEMA_VERSION])
            // ATTACH is refused inside a transaction, so it comes first.
            update(c, 'ATTACH DATABASE ? AS src', [(Object) cacheFile.toAbsolutePath().toString()])
            c.setAutoCommit(false)
            update(c, COPY_RUNS, [(Object) member])
            for( String sql : COPY_CLOSURE )
                exec(c, sql)
            if( watermark != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WATERMARK_KEY, watermark])
            update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WRITTEN_AT_KEY, MILLIS.format(Instant.now())])
            c.commit()
            c.setAutoCommit(true)
            exec(c, 'DETACH DATABASE src')
            final int runs = count(c, 'SELECT count(*) FROM run')
            exec(c, "PRAGMA page_size=${PAGE_SIZE}".toString())
            update(c, 'VACUUM INTO ?', [(Object) out.toAbsolutePath().toString()])
            return runs
        }
        finally {
            c.close()
        }
    }

    /** Writes the page beside the snapshot when its bytes differ. True when it wrote. */
    static boolean writePage(Path memberRoot, byte[] page) {
        final Path target = memberRoot.resolve(PAGE_NAME)
        if( Files.isRegularFile(target) && Arrays.equals(Files.readAllBytes(target), page) )
            return false
        Files.createDirectories(memberRoot)
        final Path temp = memberRoot.resolve(".tmp-${token()}.html")
        try {
            Files.write(temp, page)
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return true
        }
        finally {
            Files.deleteIfExists(temp)
        }
    }

    /** The page this build of the plugin carries, or null in a build without one. */
    static byte[] bundledPage() {
        final InputStream input = IndexSnapshot.getResourceAsStream(PAGE_RESOURCE)
        if( input == null )
            return null
        try {
            return input.readAllBytes()
        }
        finally {
            input.close()
        }
    }

    private static void exec(Connection c, String sql) {
        final Statement s = c.createStatement()
        try {
            s.execute(sql)
        }
        finally {
            s.close()
        }
    }

    private static void update(Connection c, String sql, List<Object> parameters) {
        final PreparedStatement s = c.prepareStatement(sql)
        try {
            for( int i = 0; i < parameters.size(); i++ )
                s.setObject(i + 1, parameters[i])
            s.execute()
        }
        finally {
            s.close()
        }
    }

    private static int count(Connection c, String sql) {
        final Statement s = c.createStatement()
        try {
            final ResultSet rs = s.executeQuery(sql)
            return rs.next() ? rs.getInt(1) : 0
        }
        finally {
            s.close()
        }
    }

    private static String token() {
        final byte[] bytes = new byte[6]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }
}
```

- [ ] **Step 4: Read the cap and the index override in `CasConfig`, declare `cas.snapshot`**

In `CasConfig`, add the two fields and the constructor parameters, and parse them in `from()`:

```groovy
    /** `cas.index.path`, or null for the per-user cache path (DESIGN.md §12). */
    final String indexOverride

    /** `cas.snapshot.maxBytes`: a run rewrites the Index Snapshot only while under this (DESIGN.md §15). */
    final long snapshotMaxBytes
```

```groovy
        final Object indexScope = scope.get('index')
        final String indexOverride = indexScope instanceof Map ? ((Map) indexScope).get('path') as String : null
        final Object snapshotScope = scope.get('snapshot')
        final long snapshotMaxBytes = bytesOf(snapshotScope instanceof Map ? ((Map) snapshotScope).get('maxBytes') : null)
        return new CasConfig(alias, members, locations, assertedBy, indexOverride, snapshotMaxBytes)
```

```groovy
    private static long bytesOf(Object value) {
        if( value == null )
            return IndexSnapshot.DEFAULT_MAX_BYTES
        final long bytes
        if( value instanceof MemoryUnit )
            bytes = ((MemoryUnit) value).toBytes()
        else if( value instanceof Number )
            bytes = ((Number) value).longValue()
        else
            bytes = new MemoryUnit(value.toString()).toBytes()
        if( bytes <= 0 )
            throw new IllegalArgumentException("cas.snapshot.maxBytes must be a positive size, e.g. 64.MB -- offending value: ${value}")
        return bytes
    }
```

Imports: `nextflow.util.MemoryUnit`, `robsyme.cas.core.IndexSnapshot`. `robsyme.cas` may import Nextflow; `robsyme.cas.core` may not, which is why the parsing is here.

In `CasConfigScope`, add beside `index`:

```groovy
    CasSnapshotScope snapshot

    /** {@code cas.snapshot}: the Index Snapshot a member carries for the explorer (DESIGN.md §15). */
    @CompileStatic
    static class CasSnapshotScope implements ConfigScope {
        CasSnapshotScope() {}

        @ConfigOption
        @Description('A run rewrites its member Index Snapshot only while it is under this size. Defaults to 64 MB.')
        MemoryUnit maxBytes
    }
```

- [ ] **Step 5: Add `openIndex()` and `snapshotWritable()` to `CasSession`, and use them**

```groovy
    /** This composition's per-user cache index (DESIGN.md §12). The caller closes it. */
    Index openIndex() {
        final List<String> locations = config.members.collect { String alias -> config.locationOf(alias).toString() }
        return Index.open(IndexPaths.cachePath(locations, config.indexOverride))
    }

    /**
     * Rewrites the writable member's Index Snapshot from {@code index}, and the
     * page beside it when this build carries one (DESIGN.md §15).
     * {@code maxBytes <= 0} writes at any size.
     */
    IndexSnapshot.Result snapshotWritable(Index index, long maxBytes) {
        final IndexSnapshot.Result result = IndexSnapshot.write(index, config.writableAlias, config.writableLocation, maxBytes)
        if( result.written ) {
            final byte[] page = IndexSnapshot.bundledPage()
            if( page != null )
                IndexSnapshot.writePage(config.writableLocation, page)
            else
                warnOnce('this build of nf-blocks carries no explorer page; the snapshot was written without index.html')
        }
        return result
    }

    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    private static void warnOnce(String message) {
        if( WARNED.add(message) )
            log.warn(message)
    }
```

Imports: `robsyme.cas.core.IndexPaths`, `robsyme.cas.core.IndexSnapshot`.

In `CasObserver`, replace the body of `openIndex()` with `return cas.openIndex()` (keep the method: `CasObserverTest` overrides it), and make `indexRun` write the snapshot once the index is caught up:

```groovy
    private void indexRun(Cid completion) {
        Index index = null
        try {
            index = openIndex()
            index.ingestRun(cas.store, completion, cas.config.writableAlias)
            // Also fold in any read-only members' run logs, so this user's index
            // reflects the whole composition and not only what this run wrote.
            cas.catchUpIndex(index)
            writeSnapshot(index)
        }
        catch( Exception e ) {
            // ... unchanged ...
        }
        finally {
            index?.close()
        }
    }

    /** The member's Index Snapshot, under the cap (DESIGN.md §15). Derived: a failure only warns. */
    private void writeSnapshot(Index index) {
        try {
            final IndexSnapshot.Result result = cas.snapshotWritable(index, cas.config.snapshotMaxBytes)
            if( result.skipped )
                log.info("the Index Snapshot of store '${cas.config.writableAlias}' is over cas.snapshot.maxBytes " +
                    "(${cas.config.snapshotMaxBytes} bytes) and was not rewritten; `nextflow plugin nf-blocks:snapshot` rewrites it at any size")
        }
        catch( Exception e ) {
            log.warn("the Index Snapshot of store '${cas.config.writableAlias}' could not be written; it is derived: ${e.message}", e)
        }
    }
```

Remove the now-unused `IndexPaths` import from `CasObserver` if nothing else uses it. In `CasExtension`, replace `openIndex()`'s body with `return cas.openIndex()` and delete `navigate` if nothing else calls it.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd nf-blocks && ./gradlew test`
Expected: PASS, the whole suite, with the new `IndexSnapshotTest`, `CasConfigTest` and `CasObserverTest` features.

Then check the real thing once: `GATE_ROOT="$SCRATCH/g" make gate`, and afterwards
`sqlite3 "file:$SCRATCH/g/store/index/v2.sqlite?mode=ro" 'select count(*) from run; select * from meta'`
Expected: the producer runs of the Gate (5), a `store_log_watermark` and a `snapshot_written_at`. The lineage Gate table is unchanged (11 PASS, 0 FAIL, 6 SKIP; rerun once if assertion 4 flakes).

- [ ] **Step 7: Commit**

```bash
cd nf-blocks
git add src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy src/main/groovy/robsyme/cas/CasConfig.groovy \
        src/main/groovy/robsyme/cas/CasConfigScope.groovy src/main/groovy/robsyme/cas/CasSession.groovy \
        src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/main/groovy/robsyme/cas/ext/CasExtension.groovy \
        src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy src/test/groovy/robsyme/cas/CasConfigTest.groovy \
        src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy web/test/config.test.mjs
git commit -m "feat: every run writes its member's Index Snapshot under cas.snapshot.maxBytes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Plugin verbs and `nf-blocks:snapshot`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/CasPlugin.groovy`
- Create: `src/main/groovy/robsyme/cas/cli/UsageException.groovy`
- Create: `src/main/groovy/robsyme/cas/cli/Options.groovy`
- Create: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy`
- Create: `src/test/groovy/robsyme/cas/cli/OptionsTest.groovy`, `src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy`

**Interfaces:**
- Consumes: `CasConfig.fromSession(Map)`, `new CasSession(CasConfig)`, `CasSession.openIndex()`, `CasSession.catchUpIndex(Index)`, `CasSession.snapshotWritable(Index, long)` (Task 5).
- Produces:
  - `CasPlugin implements PluginExecAware`: `int exec(Launcher launcher, String pluginId, String cmd, List<String> args)`.
  - `class Options`: `static Options parse(List<String> args, Set<String> known)`; `List<String> positionals`; `String flag(String name)` (null when absent); `int intFlag(String name, int fallback)`. Unknown flag: `UsageException`.
  - `class UsageException extends RuntimeException` (in `robsyme.cas.cli`).
  - `class CasCommands`: `static final List<String> VERBS = ['explore', 'snapshot']`; `int exec(Launcher, String pluginId, String cmd, List<String> args)`; `int run(String cmd, List<String> args, Map config, PrintStream out, PrintStream err)` (the testable seam). `explore` is dispatched to `ExploreCommand.run(Options, Map, PrintStream, PrintStream)` from Task 7; until then `run('explore', ...)` prints `nf-blocks:explore is not built yet` and returns 1.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/cli/OptionsTest.groovy
package robsyme.cas.cli

import spock.lang.Specification

/** CmdPlugin hands a verb its positionals, then `--name`, `value` pairs (DESIGN.md §15). */
class OptionsTest extends Specification {

    def 'flags arrive as name and value pairs after the positionals'() {
        when:
        final Options o = Options.parse(['file.json', '--port', '8080'], ['port'] as Set)

        then:
        o.positionals == ['file.json']
        o.flag('port') == '8080'
        o.intFlag('port', 0) == 8080
        o.flag('missing') == null
        o.intFlag('missing', 7) == 7
    }

    def 'an unknown flag is a usage error naming it'() {
        when:
        Options.parse(['--prot', '8080'], ['port'] as Set)

        then:
        final UsageException e = thrown()
        e.message.contains('--prot')
    }

    def 'a flag with no value is a usage error'() {
        when:
        Options.parse(['--port'], ['port'] as Set)

        then:
        thrown(UsageException)
    }

    def 'a non-numeric port is a usage error'() {
        when:
        Options.parse(['--port', 'abc'], ['port'] as Set).intFlag('port', 0)

        then:
        thrown(UsageException)
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy
package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

class CasCommandsTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
            ],
        ]
    }

    private int run(String cmd, List<String> args = [], Map cfg = config()) {
        return new CasCommands().run(cmd, args, cfg, new PrintStream(out, true), new PrintStream(err, true))
    }

    private Cid logRun() {
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, System.currentTimeMillis())
        return completion
    }

    def 'snapshot catches the index up from the Store Log and writes the snapshot at any size'() {
        given:
        final Cid completion = logRun()

        when:
        final int status = run('snapshot')
        final Path file = tempDir.resolve('store/index/v2.sqlite')

        then:
        status == 0
        Files.isRegularFile(file)
        out.toString().contains(file.toString())
        out.toString().contains('1 runs')
        DriverManager.getConnection("jdbc:sqlite:${file}").withCloseable { c ->
            c.createStatement().executeQuery('SELECT completion_cid FROM run').with { next(); getString(1) }
        } == completion.toString()
    }

    def 'an unknown verb is a usage error listing the verbs'() {
        expect:
        run('frobnicate') == 2
        err.toString().contains('explore')
        err.toString().contains('snapshot')
    }

    def 'snapshot takes no flags'() {
        expect:
        run('snapshot', ['--port', '1']) == 2
        err.toString().contains('--port')
    }

    def 'a config without a cas lineage store is reported, not thrown'() {
        expect:
        run('snapshot', [], [:]) == 1
        err.toString().contains('lineage.store.location')
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd nf-blocks && ./gradlew test --tests 'robsyme.cas.cli.*'`
Expected: FAIL to compile, `unable to resolve class Options`, `CasCommands`.

- [ ] **Step 3: Write `UsageException`, `Options` and `CasCommands`**

```groovy
// src/main/groovy/robsyme/cas/cli/UsageException.groovy
package robsyme.cas.cli

import groovy.transform.CompileStatic

/** A verb was called wrongly; the message says how to call it. Exit status 2. */
@CompileStatic
class UsageException extends RuntimeException {
    UsageException(String message) { super(message) }
}
```

```groovy
// src/main/groovy/robsyme/cas/cli/Options.groovy
package robsyme.cas.cli

import groovy.transform.CompileStatic

/**
 * A verb's arguments as Nextflow's CmdPlugin hands them over: the positionals,
 * then each `--name value` as the pair `--name`, `value` (Launcher normalises
 * `--name value` to `--name=value`, CmdPlugin splits it again; DESIGN.md §15).
 */
@CompileStatic
class Options {

    final List<String> positionals
    private final Map<String, String> flags

    private Options(List<String> positionals, Map<String, String> flags) {
        this.positionals = Collections.unmodifiableList(positionals)
        this.flags = flags
    }

    static Options parse(List<String> args, Set<String> known) {
        final List<String> positionals = []
        final Map<String, String> flags = [:]
        for( int i = 0; i < (args ?: []).size(); i++ ) {
            final String arg = args[i]
            if( !arg.startsWith('--') ) {
                positionals << arg
                continue
            }
            final String name = arg.substring(2)
            if( !(name in known) )
                throw new UsageException("unknown option --${name}" + (known ? "; this verb takes ${known.collect { '--' + it }.join(', ')}" : '; this verb takes no options'))
            if( i + 1 >= args.size() )
                throw new UsageException("option --${name} needs a value")
            flags[name] = args[++i]
        }
        return new Options(positionals, flags)
    }

    String flag(String name) { flags.get(name) }

    int intFlag(String name, int fallback) {
        final String value = flags.get(name)
        if( value == null )
            return fallback
        if( !(value ==~ /\d+/) )
            throw new UsageException("option --${name} must be a whole number, got '${value}'")
        return Integer.parseInt(value)
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/cli/CasCommands.groovy
package robsyme.cas.cli

import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cli.Launcher
import nextflow.config.ConfigBuilder
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot

/**
 * `nextflow plugin nf-blocks:<verb>` (DESIGN.md §15). Builds the config the
 * way PluginAbstractExec does, but returns a real exit status: that trait
 * swallows every exception and returns 0.
 */
@Slf4j
@CompileStatic
class CasCommands {

    static final List<String> VERBS = ['explore', 'snapshot']

    int exec(Launcher launcher, String pluginId, String cmd, List<String> args) {
        final Map config
        try {
            config = new ConfigBuilder()
                .setOptions(launcher.options)
                .setBaseDir(Paths.get('.'))
                .build()
        }
        catch( Exception e ) {
            System.err.println("nf-blocks:${cmd}: could not read the Nextflow config: ${e.message}")
            return 1
        }
        return run(cmd, args, config, System.out, System.err)
    }

    int run(String cmd, List<String> args, Map config, PrintStream out, PrintStream err) {
        if( !(cmd in VERBS) ) {
            err.println(usage(cmd))
            return 2
        }
        try {
            switch( cmd ) {
                case 'snapshot':
                    return snapshot(Options.parse(args, [] as Set), config, out)
                case 'explore':
                    return explore(args, config, out, err)
            }
            return 2
        }
        catch( UsageException e ) {
            err.println("nf-blocks:${cmd}: ${e.message}")
            return 2
        }
        catch( Exception e ) {
            log.debug("nf-blocks:${cmd} failed", e)
            err.println("nf-blocks:${cmd}: ${e.message}")
            return 1
        }
    }

    private static String usage(String cmd) {
        final String head = cmd ? "unknown command 'nf-blocks:${cmd}'" : 'no command given'
        return "${head}; usage: nextflow plugin nf-blocks:<command>\ncommands:\n" +
            '  explore [--port <n>]   serve the explorer and this composition\'s members on loopback\n' +
            '  snapshot               rewrite the writable member\'s Index Snapshot at any size'
    }

    /** Catches the index up from every member's Store Log, then rewrites the writable member's snapshot. */
    private static int snapshot(Options options, Map config, PrintStream out) {
        if( options.positionals )
            throw new UsageException("snapshot takes no arguments, got ${options.positionals}")
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            cas.catchUpIndex(index)
            final IndexSnapshot.Result result = cas.snapshotWritable(index, 0L)
            out.println("wrote ${result.path} (${result.bytes} bytes, ${result.runs} runs, watermark ${result.watermark ?: 'none'})")
            return 0
        }
        finally {
            index.close()
        }
    }

    /** Replaced by ExploreCommand in Task 7. */
    private static int explore(List<String> args, Map config, PrintStream out, PrintStream err) {
        err.println('nf-blocks:explore is not built yet')
        return 1
    }
}
```

In `CasPlugin`, implement the interface:

```groovy
import nextflow.cli.Launcher
import nextflow.cli.PluginExecAware
import robsyme.cas.cli.CasCommands

@CompileStatic
class CasPlugin extends BasePlugin implements PluginExecAware {

    // ... existing constructor, start() and provider() unchanged ...

    /** `nextflow plugin nf-blocks:<cmd>` (DESIGN.md §15). */
    @Override
    int exec(Launcher launcher, String pluginId, String cmd, List<String> args) {
        return new CasCommands().exec(launcher, pluginId, cmd, args)
    }
}
```

`CasConfig.fromSession` of a config without `lineage.store.location` throws `IllegalArgumentException("lineage.store.location must be a 'cas://<alias>' location ...")`, which `run` reports with exit 1; that is what the last test checks.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd nf-blocks && ./gradlew test --tests 'robsyme.cas.cli.*'` then `./gradlew test`
Expected: PASS.

- [ ] **Step 5: Try the verb through real Nextflow**

The plugin id is unpinned on the command line, so Nextflow would prefetch registry metadata for `nf-blocks`; `NXF_OFFLINE=true` skips that for a locally installed plugin (`PluginsFacade.start`, v26.04.6). Run after a `make gate` so `$SCRATCH/g` holds a store, all in one command:

```bash
cd nf-blocks && REPO="$(pwd)" && ./gradlew -q assemble installPlugin && \
  export NXF_PLUGINS_DIR="$SCRATCH/g/plugins" GATE_STORE="$SCRATCH/g/store" XDG_CACHE_HOME="$SCRATCH/g/cache" && \
  ( cd "$SCRATCH/g/pipeline-a" && NXF_OFFLINE=true nextflow -c "$REPO/gate/gate.config" plugin nf-blocks:snapshot ); echo "exit $?"
```

Expected: `wrote <store>/index/v2.sqlite (... bytes, 5 runs, watermark ...)` and `exit 0`. Then `nextflow plugin nf-blocks:nope` (same environment): exit 2 and the usage text. If the offline start fails, record the exact message and the invocation that works in DESIGN.md §15 before going on; Task 12's `tier.sh` uses it.

- [ ] **Step 6: Commit**

```bash
cd nf-blocks
git add src/main/groovy/robsyme/cas/CasPlugin.groovy src/main/groovy/robsyme/cas/cli \
        src/test/groovy/robsyme/cas/cli
git commit -m "feat(cli): nextflow plugin nf-blocks:snapshot, with real exit statuses

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 7: `nf-blocks:explore`, the loopback server

**Files:**
- Create: `src/main/groovy/robsyme/cas/explore/MemberFiles.groovy`
- Create: `src/main/groovy/robsyme/cas/explore/LocalMemberFiles.groovy`
- Create: `src/main/groovy/robsyme/cas/explore/ByteRange.groovy`
- Create: `src/main/groovy/robsyme/cas/explore/ExploreServer.groovy`
- Create: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy`
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (dispatch `explore`)
- Create: `src/test/groovy/robsyme/cas/explore/ByteRangeTest.groovy`, `ExploreServerTest.groovy`, `ExploreCommandTest.groovy`, `RawHttp.groovy`

**Interfaces:**
- Consumes: `Options`, `UsageException` (Task 6); `CasConfig`, `CasSession.openIndex()`, `catchUpIndex()`, `snapshotWritable()`, `IndexSnapshot.bundledPage()` (Task 5).
- Produces:
  - `interface MemberFiles { Long size(String rel); InputStream open(String rel, long start, long length); List<String> list(String dirRel); String describe() }`. `size` is null when absent. `rel` is a `/`-separated path relative to the member root.
  - `class LocalMemberFiles implements MemberFiles { LocalMemberFiles(Path root) }`.
  - `final class ByteRange { long start; long end /* inclusive */; long getLength(); static ByteRange parse(String header, long size) throws ByteRange.Unsatisfiable }`: null when there is no usable single range.
  - `class ExploreServer { ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page); ExploreServer start(int port); int getPort(); String getUrl(); void stop() }`.
  - `class ExploreCommand { static final Set<String> FLAGS = ['port']; static Started start(List<String> args, Map config, PrintStream out, PrintStream err); static int run(List<String> args, Map config, PrintStream out, PrintStream err); static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config) }`, with `Started { ExploreServer server; CasSession cas }`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/explore/RawHttp.groovy
package robsyme.cas.explore

/**
 * HTTP/1.1 over a plain socket, so a test can send the Host and Origin headers
 * a browser on another origin, or a DNS-rebinding page, would send. The JDK
 * HttpClient refuses to set Host.
 */
class RawHttp {

    static class Response {
        int status
        Map<String, String> headers
        byte[] body
        String text() { new String(body, 'UTF-8') }
    }

    static Response send(int port, String method, String rawPath, Map<String, String> headers = [:]) {
        final Map<String, String> all = [Host: "127.0.0.1:${port}".toString()] + headers
        new Socket('127.0.0.1', port).withCloseable { Socket s ->
            final String head = "${method} ${rawPath} HTTP/1.1\r\n" +
                all.collect { k, v -> "${k}: ${v}\r\n" }.join('') + 'Connection: close\r\n\r\n'
            s.outputStream.write(head.getBytes('ISO-8859-1'))
            s.outputStream.flush()
            final byte[] raw = s.inputStream.readAllBytes()
            final String text = new String(raw, 'ISO-8859-1')
            final int split = text.indexOf('\r\n\r\n')
            final List<String> lines = text.substring(0, split).split('\r\n') as List<String>
            final Map<String, String> parsed = [:]
            lines.drop(1).each { String line ->
                final int colon = line.indexOf(':')
                parsed[line.substring(0, colon).trim().toLowerCase()] = line.substring(colon + 1).trim()
            }
            return new Response(status: lines[0].split(' ')[1] as int, headers: parsed,
                body: Arrays.copyOfRange(raw, split + 4, raw.length))
        }
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/explore/ByteRangeTest.groovy
package robsyme.cas.explore

import spock.lang.Specification

class ByteRangeTest extends Specification {

    def 'single ranges parse (#header)'() {
        when:
        final ByteRange range = ByteRange.parse(header, 1000)

        then:
        [range.start, range.end, range.length] == [start, end, end - start + 1]

        where:
        header          | start | end
        'bytes=0-99'    | 0     | 99
        'bytes=900-'    | 900   | 999
        'bytes=-100'    | 900   | 999
        'bytes=990-2000'| 990   | 999
        'bytes=-5000'   | 0     | 999
    }

    def 'no header, or a form this server does not honour, means the whole file (#header)'() {
        expect:
        ByteRange.parse(header, 1000) == null

        where:
        header << [null, '', 'bytes=0-1,5-6', 'items=0-1', 'bytes=-', 'bytes=50-10', 'bytes=99999999999999999999-']
    }

    def 'a range starting past the end is unsatisfiable (#header)'() {
        when:
        ByteRange.parse(header, 1000)

        then:
        thrown(ByteRange.Unsatisfiable)

        where:
        header << ['bytes=1000-', 'bytes=5000-6000', 'bytes=-0']
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/explore/ExploreServerTest.groovy
package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §15: what explore serves, and everything it refuses. */
class ExploreServerTest extends Specification {

    static final String CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
    static final String ENTRY = "8232774159999-run-${CID}"

    @TempDir
    Path tempDir

    ExploreServer server
    byte[] snapshot = (0..<10240).collect { (byte) (it & 0xff) } as byte[]
    byte[] page = '<!doctype html><title>explorer</title>'.getBytes('UTF-8')

    def setup() {
        final Path lab = tempDir.resolve('lab')
        Files.createDirectories(lab.resolve('index'))
        Files.write(lab.resolve('index/v2.sqlite'), snapshot)
        Files.createDirectories(lab.resolve("blocks/${CID[-2..-1]}"))
        Files.write(lab.resolve("blocks/${CID[-2..-1]}/${CID}"), [0xa0] as byte[])
        Files.createDirectories(lab.resolve('log'))
        Files.createFile(lab.resolve("log/${ENTRY}"))
        Files.createFile(lab.resolve('log/.DS_Store'))
        Files.createDirectories(lab.resolve('coords/aligned'))
        Files.write(lab.resolve('coords/aligned/A.bam'), 'cas://x/A.bam'.bytes)
        Files.createDirectories(lab.resolve('nf/abc'))
        Files.write(lab.resolve('nf/abc/.data.json'), '{"path": "/Users/someone/work"}'.bytes)
        final Path other = tempDir.resolve('other')
        Files.createDirectories(other)
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(lab))
        members.put('other', new LocalMemberFiles(other))
        server = new ExploreServer(members, 'lab', page).start(0)
    }

    def cleanup() {
        server?.stop()
    }

    private RawHttp.Response get(String path, Map<String, String> headers = [:]) {
        RawHttp.send(server.port, 'GET', path, headers)
    }

    def 'it listens on loopback only and prints its own URL'() {
        expect:
        server.url == "http://127.0.0.1:${server.port}/"
    }

    def 'the page is served at / and /index.html'() {
        expect:
        ['/', '/index.html'].every { String p ->
            final def r = get(p)
            r.status == 200 && r.headers['content-type'].startsWith('text/html') && r.body == page
        }
    }

    def 'members.json lists every member, writable first'() {
        when:
        final def r = get('/members.json')

        then:
        r.status == 200
        new JsonSlurper().parse(r.body) == [members: [
            [alias: 'lab', writable: true, base: 'm/lab/'],
            [alias: 'other', writable: false, base: 'm/other/'],
        ]]
    }

    def 'the snapshot is served whole, and by single ranges'() {
        expect:
        get('/m/lab/index/v2.sqlite').with { status == 200 && body == snapshot && headers['accept-ranges'] == 'bytes' }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=4096-8191']).with {
            status == 206 && headers['content-range'] == 'bytes 4096-8191/10240' &&
                body == Arrays.copyOfRange(snapshot, 4096, 8192)
        }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=-10']).with { status == 206 && body.length == 10 }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=20000-']).with { status == 416 && headers['content-range'] == 'bytes */10240' }
        RawHttp.send(server.port, 'HEAD', '/m/lab/index/v2.sqlite').with { status == 200 && body.length == 0 }
    }

    def 'a block is served when its shard matches its cid'() {
        expect:
        get("/m/lab/blocks/${CID[-2..-1]}/${CID}").with { status == 200 && body == ([0xa0] as byte[]) }
        get("/m/lab/blocks/aa/${CID}").status == 404
    }

    def 'the log listing is JSON of Store Log names only'() {
        when:
        final def r = get('/m/lab/log/')

        then:
        r.status == 200
        r.headers['content-type'].startsWith('application/json')
        new JsonSlurper().parse(r.body) == [entries: [ENTRY]]
        new JsonSlurper().parse(get('/m/other/log/').body) == [entries: []]
    }

    def 'nothing else under a member is ever served (Review Focus 4): #path'() {
        expect:
        get(path).status == 404

        where:
        path << ['/m/lab/coords/aligned/A.bam', '/m/lab/nf/abc/.data.json', '/m/lab/../../etc/passwd',
                 '/m/lab/%2e%2e/%2e%2e/etc/passwd', '/m/lab/index/../nf/abc/.data.json', '/m/ghost/index/v2.sqlite',
                 '/m/lab/index/v2.sqlite-wal', '/m/lab/log/.DS_Store', '/m/lab/blocks/', '/etc/passwd']
    }

    def 'a foreign Host or Origin is refused (Review Focus 4): #headers'() {
        expect:
        get('/m/lab/index/v2.sqlite', headers).status == 403

        where:
        headers << [[Host: 'attacker.example:80'], [Host: 'evil.test'], [Host: '127.0.0.1:1'],
                    [Origin: 'http://attacker.example'], [Origin: 'null']]
    }

    def 'its own origin is accepted under either loopback name'() {
        expect:
        get('/members.json', [Host: "localhost:${server.port}".toString(), Origin: "http://localhost:${server.port}".toString()]).status == 200
        get('/members.json', [Origin: "http://127.0.0.1:${server.port}".toString()]).status == 200
    }

    def 'it is read-only'() {
        expect:
        RawHttp.send(server.port, 'POST', '/m/lab/index/v2.sqlite').with { status == 405 && headers['allow'] == 'GET, HEAD' }
    }

    def 'no CORS headers are sent'() {
        expect:
        !get('/m/lab/index/v2.sqlite', [Origin: "http://127.0.0.1:${server.port}".toString()]).headers.keySet().any { it.startsWith('access-control') }
    }
}
```

The `127.0.0.1:1` row is a loopback name with the wrong port, which is still foreign.

```groovy
// src/test/groovy/robsyme/cas/explore/ExploreCommandTest.groovy
package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.cli.CasCommands
import spock.lang.Specification
import spock.lang.TempDir

class ExploreCommandTest extends Specification {

    @TempDir
    Path tempDir

    ExploreCommand.Started started
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    def cleanup() {
        started?.server?.stop()
    }

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('lab').toString()],
                         shared: [location: tempDir.resolve('shared').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
            ],
        ]
    }

    def 'explore rewrites the writable snapshot at start, then serves every member'() {
        given:
        final LocalBlockStore lab = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', []))
        final Cid completion = lab.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(lab, StoreLogKind.RUN, completion, System.currentTimeMillis())
        Files.createDirectories(tempDir.resolve('shared'))

        when:
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))

        then:
        Files.isRegularFile(tempDir.resolve('lab/index/v2.sqlite'))
        out.toString().trim() == "nf-blocks explorer: ${started.server.url}"
        RawHttp.send(started.server.port, 'GET', '/m/lab/index/v2.sqlite').status == 200
        RawHttp.send(started.server.port, 'GET', '/members.json').text().contains('"shared"')
    }

    def 'explore takes only --port'() {
        expect:
        new CasCommands().run('explore', ['--prot', '1'], config(), new PrintStream(out, true), new PrintStream(err, true)) == 2
        err.toString().contains('--prot')
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd nf-blocks && ./gradlew test --tests 'robsyme.cas.explore.*'`
Expected: FAIL to compile, `unable to resolve class ExploreServer` and the rest.

- [ ] **Step 3: Write `MemberFiles`, `LocalMemberFiles` and `ByteRange`**

```groovy
// src/main/groovy/robsyme/cas/explore/MemberFiles.groovy
package robsyme.cas.explore

/**
 * What the explorer's server needs of a store member (DESIGN.md §15): sizes,
 * ranged reads and one directory listing. Paths are relative to the member
 * root, `/`-separated; the server decides which ones may be asked for.
 */
interface MemberFiles {

    /** Size in bytes, or null when there is no such file. */
    Long size(String rel)

    /** The file's bytes from {@code start}; the caller reads at most {@code length}. */
    InputStream open(String rel, long start, long length)

    /** Names directly under {@code dirRel}, in no order; empty when there is no such directory. */
    List<String> list(String dirRel)

    /** For messages: where this member lives. */
    String describe()
}
```

```groovy
// src/main/groovy/robsyme/cas/explore/LocalMemberFiles.groovy
package robsyme.cas.explore

import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import groovy.transform.CompileStatic

/** A member in a local directory. */
@CompileStatic
class LocalMemberFiles implements MemberFiles {

    private final Path root

    LocalMemberFiles(Path root) {
        this.root = root.toAbsolutePath().normalize()
    }

    /** Refuses anything that is not a plain relative path inside the root. */
    private Path resolve(String rel) {
        if( !rel || rel.startsWith('/') || rel.split('/').any { String s -> s in ['', '.', '..'] } )
            throw new IllegalArgumentException("not a member path: '${rel}'")
        final Path path = root.resolve(rel).normalize()
        if( !path.startsWith(root) )
            throw new IllegalArgumentException("not a member path: '${rel}'")
        return path
    }

    @Override
    Long size(String rel) {
        final Path path = resolve(rel)
        return Files.isRegularFile(path) ? Files.size(path) : null
    }

    @Override
    InputStream open(String rel, long start, long length) {
        final FileChannel channel = FileChannel.open(resolve(rel), StandardOpenOption.READ)
        channel.position(start)
        return Channels.newInputStream(channel)
    }

    @Override
    List<String> list(String dirRel) {
        final Path dir = resolve(dirRel.replaceAll('/+$', ''))
        if( !Files.isDirectory(dir) )
            return []
        return Files.list(dir).withCloseable { stream ->
            stream.toList().collect { Object p -> ((Path) p).fileName.toString() }
        }
    }

    @Override
    String describe() { root.toString() }
}
```

```groovy
// src/main/groovy/robsyme/cas/explore/ByteRange.groovy
package robsyme.cas.explore

import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.CompileStatic

/**
 * One `Range: bytes=` range (RFC 9110 §14). Anything else, several ranges
 * included, is ignored and the whole file is served, which the RFC allows;
 * the page only ever asks for one range at a time.
 */
@CompileStatic
final class ByteRange {

    private static final Pattern SINGLE = ~/^bytes=(\d*)-(\d*)$/

    /** Thrown for a range that starts at or past the end: 416. */
    static class Unsatisfiable extends Exception {
        Unsatisfiable() { super('range not satisfiable') }
    }

    final long start
    /** Inclusive. */
    final long end

    private ByteRange(long start, long end) {
        this.start = start
        this.end = end
    }

    long getLength() { end - start + 1 }

    static ByteRange parse(String header, long size) throws Unsatisfiable {
        if( !header )
            return null
        final Matcher m = SINGLE.matcher(header.trim())
        if( !m.matches() )
            return null
        final String first = m.group(1)
        final String last = m.group(2)
        if( !first && !last )
            return null
        try {
            if( !first ) {
                final long suffix = Long.parseLong(last)
                if( suffix == 0 )
                    throw new Unsatisfiable()
                return new ByteRange(Math.max(0L, size - suffix), size - 1)
            }
            final long start = Long.parseLong(first)
            if( start >= size )
                throw new Unsatisfiable()
            final long end = last ? Math.min(Long.parseLong(last), size - 1) : size - 1
            return end < start ? null : new ByteRange(start, end)
        }
        catch( NumberFormatException e ) {
            return null
        }
    }
}
```

- [ ] **Step 4: Write `ExploreServer`**

```groovy
// src/main/groovy/robsyme/cas/explore/ExploreServer.groovy
package robsyme.cas.explore

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.sun.net.httpserver.Headers
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The explorer's loopback server (DESIGN.md §15, block explorer spec sections
 * 2 and 6): the page, and for every member only its snapshot, its blocks and
 * its Store Log listing, with Range. One user, one machine: it binds loopback,
 * answers only to its own origin, and sends no CORS headers.
 */
@Slf4j
@CompileStatic
class ExploreServer {

    private static final Pattern MEMBER = ~/^\/m\/([a-z][a-z0-9_-]{0,31})\/(.*)$/
    private static final Pattern BLOCK = ~/^blocks\/([a-z2-7]{2})\/(b[a-z2-7]{58})$/
    private static final Pattern SNAPSHOT = ~/^index\/v\d{1,4}\.sqlite$/
    private static final Pattern LOG_ENTRY = ~/^\d{13}-[a-z]+-b[a-z2-7]{58}$/
    private static final byte[] NO_PAGE = ('<!doctype html><meta charset="utf-8"><title>nf-blocks</title>' +
        '<p>This build of nf-blocks carries no explorer page. Build it with <code>./gradlew assemble</code>.').getBytes('UTF-8')

    private final LinkedHashMap<String, MemberFiles> members
    private final String writableAlias
    private final byte[] page
    private HttpServer server
    private ExecutorService executor

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page) {
        this.members = members
        this.writableAlias = writableAlias
        this.page = page ?: NO_PAGE
    }

    ExploreServer start(int port) {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0)
        executor = Executors.newFixedThreadPool(8, { Runnable r ->
            final Thread t = new Thread(r, 'nf-blocks-explore')
            t.daemon = true
            return t
        } as ThreadFactory)
        server.executor = executor
        server.createContext('/', { HttpExchange exchange -> handle(exchange) } as HttpHandler)
        server.start()
        return this
    }

    int getPort() { server.address.port }

    String getUrl() { "http://127.0.0.1:${port}/" }

    void stop() {
        server?.stop(0)
        executor?.shutdownNow()
    }

    private void handle(HttpExchange exchange) {
        try {
            if( !ownOrigin(exchange) ) {
                text(exchange, 403, 'refused: this server answers only to its own origin')
                return
            }
            if( !(exchange.requestMethod in ['GET', 'HEAD']) ) {
                exchange.responseHeaders.set('Allow', 'GET, HEAD')
                text(exchange, 405, 'the explorer server is read-only')
                return
            }
            // The raw path, so an encoded `..` is never decoded into a traversal.
            route(exchange, exchange.requestURI.rawPath)
        }
        catch( Exception e ) {
            log.warn("explore: ${exchange.requestMethod} ${exchange.requestURI} failed: ${e.message}", e)
            try {
                text(exchange, 500, 'internal error')
            }
            catch( Exception ignored ) {
                // The response had already started.
            }
        }
        finally {
            exchange.close()
        }
    }

    /** Host must be this server, and Origin, when sent, too (spec section 9.5): the DNS-rebinding guard. */
    private boolean ownOrigin(HttpExchange exchange) {
        final int port = getPort()
        final String host = exchange.requestHeaders.getFirst('Host')
        if( !(host in ["127.0.0.1:${port}".toString(), "localhost:${port}".toString()]) )
            return false
        final String origin = exchange.requestHeaders.getFirst('Origin')
        return origin == null || origin in ["http://127.0.0.1:${port}".toString(), "http://localhost:${port}".toString()]
    }

    private void route(HttpExchange exchange, String path) {
        if( path == '/' || path == '/index.html' ) {
            bytes(exchange, 200, 'text/html; charset=utf-8', page)
            return
        }
        if( path == '/members.json' ) {
            bytes(exchange, 200, 'application/json', membersJson())
            return
        }
        final Matcher member = MEMBER.matcher(path)
        if( !member.matches() || !members.containsKey(member.group(1)) ) {
            text(exchange, 404, 'not found')
            return
        }
        final MemberFiles files = members.get(member.group(1))
        final String rel = member.group(2)
        if( rel == 'log/' ) {
            bytes(exchange, 200, 'application/json', logJson(files))
            return
        }
        final Matcher block = BLOCK.matcher(rel)
        if( block.matches() && block.group(2).endsWith(block.group(1)) ) {
            file(exchange, files, rel, 'application/octet-stream', 'public, max-age=31536000, immutable')
            return
        }
        if( SNAPSHOT.matcher(rel).matches() ) {
            file(exchange, files, rel, 'application/vnd.sqlite3', 'no-cache')
            return
        }
        text(exchange, 404, 'not found')
    }

    private byte[] membersJson() {
        final List<Map> list = members.keySet().collect { String alias ->
            [alias: alias, writable: alias == writableAlias, base: "m/${alias}/".toString()] as Map
        }
        return JsonOutput.toJson([members: list]).getBytes('UTF-8')
    }

    private static byte[] logJson(MemberFiles files) {
        final List<String> names = files.list('log').findAll { String n -> LOG_ENTRY.matcher(n).matches() }.sort()
        return JsonOutput.toJson([entries: names]).getBytes('UTF-8')
    }

    private static void file(HttpExchange exchange, MemberFiles files, String rel, String type, String cache) {
        final Long size = files.size(rel)
        if( size == null ) {
            text(exchange, 404, 'not found')
            return
        }
        final Headers headers = exchange.responseHeaders
        headers.set('Content-Type', type)
        headers.set('Accept-Ranges', 'bytes')
        headers.set('Cache-Control', cache)
        final ByteRange range
        try {
            range = ByteRange.parse(exchange.requestHeaders.getFirst('Range'), size)
        }
        catch( ByteRange.Unsatisfiable e ) {
            headers.set('Content-Range', "bytes */${size}".toString())
            exchange.sendResponseHeaders(416, -1)
            return
        }
        final long start = range != null ? range.start : 0L
        final long length = range != null ? range.length : size
        final int status = range != null ? 206 : 200
        if( range != null )
            headers.set('Content-Range', "bytes ${range.start}-${range.end}/${size}".toString())
        if( exchange.requestMethod == 'HEAD' || length == 0 ) {
            exchange.sendResponseHeaders(status, -1)
            return
        }
        exchange.sendResponseHeaders(status, length)
        final InputStream input = files.open(rel, start, length)
        try {
            copy(input, exchange.responseBody, length)
        }
        finally {
            input.close()
        }
    }

    private static void copy(InputStream input, OutputStream output, long length) {
        final byte[] buffer = new byte[65536]
        long left = length
        while( left > 0 ) {
            final int n = input.read(buffer, 0, (int) Math.min((long) buffer.length, left))
            if( n < 0 )
                throw new IOException("the file ended ${left} bytes early")
            output.write(buffer, 0, n)
            left -= n
        }
    }

    private static void bytes(HttpExchange exchange, int status, String type, byte[] body) {
        exchange.responseHeaders.set('Content-Type', type)
        exchange.responseHeaders.set('Cache-Control', 'no-cache')
        if( exchange.requestMethod == 'HEAD' ) {
            exchange.sendResponseHeaders(status, -1)
            return
        }
        exchange.sendResponseHeaders(status, body.length)
        exchange.responseBody.write(body)
    }

    private static void text(HttpExchange exchange, int status, String message) {
        bytes(exchange, status, 'text/plain; charset=utf-8', (message + '\n').getBytes('UTF-8'))
    }
}
```

- [ ] **Step 5: Write `ExploreCommand` and dispatch to it**

```groovy
// src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy
package robsyme.cas.explore

import java.util.concurrent.CountDownLatch

import groovy.transform.CompileStatic
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.cli.Options
import robsyme.cas.cli.UsageException
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot

/**
 * `nextflow plugin nf-blocks:explore [--port <n>]` (DESIGN.md §15). Rewrites
 * the writable member's snapshot, serves until the JVM is interrupted, and
 * rewrites the snapshot again on the way out.
 */
@CompileStatic
class ExploreCommand {

    static final Set<String> FLAGS = ['port'] as Set

    @CompileStatic
    static class Started {
        final ExploreServer server
        final CasSession cas
        Started(ExploreServer server, CasSession cas) { this.server = server; this.cas = cas }
    }

    /** Blocks until interrupted. CmdPlugin calls System.exit when this returns. */
    static int run(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Started started = start(args, config, out, err)
        final CountDownLatch stopped = new CountDownLatch(1)
        Runtime.runtime.addShutdownHook(new Thread({
            try {
                started.server.stop()
                refresh(started.cas, err)
            }
            finally {
                stopped.countDown()
            }
        } as Runnable, 'nf-blocks-explore-exit'))
        stopped.await()
        return 0
    }

    static Started start(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Options options = Options.parse(args, FLAGS)
        if( options.positionals )
            throw new UsageException("explore takes no arguments, got ${options.positionals}")
        final CasConfig cas = CasConfig.fromSession(config)
        final CasSession session = new CasSession(cas)
        refresh(session, err)
        final ExploreServer server = new ExploreServer(membersOf(cas), cas.writableAlias, IndexSnapshot.bundledPage())
            .start(options.intFlag('port', 0))
        out.println("nf-blocks explorer: ${server.url}")
        out.flush()
        return new Started(server, session)
    }

    /** Catches the index up and rewrites the writable member's snapshot at any size. Derived: a failure warns. */
    static void refresh(CasSession cas, PrintStream err) {
        try {
            final Index index = cas.openIndex()
            try {
                cas.catchUpIndex(index)
                cas.snapshotWritable(index, 0L)
            }
            finally {
                index.close()
            }
        }
        catch( Exception e ) {
            err.println("nf-blocks:explore: could not rewrite the Index Snapshot (${e.message}); serving the one on disk")
        }
    }

    /** Every member the explorer serves, writable first. */
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.members )
            members.put(alias, new LocalMemberFiles(config.locationOf(alias)))
        return members
    }
}
```

In `CasCommands.run`, replace the `explore` case and delete the private `explore` stub:

```groovy
                case 'explore':
                    return ExploreCommand.run(args, config, out, err)
```

with `import robsyme.cas.explore.ExploreCommand`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd nf-blocks && ./gradlew test --tests 'robsyme.cas.explore.*' --tests 'robsyme.cas.cli.*'` then `./gradlew test`
Expected: PASS.

- [ ] **Step 7: Try it through real Nextflow, including the exit rewrite**

In one command (the sandbox rule), against the `$SCRATCH/g` store from an earlier `make gate`:

```bash
cd nf-blocks && REPO="$(pwd)" && ./gradlew -q assemble installPlugin && \
  export NXF_PLUGINS_DIR="$SCRATCH/g/plugins" GATE_STORE="$SCRATCH/g/store" XDG_CACHE_HOME="$SCRATCH/g/cache" && \
  cd "$SCRATCH/g/pipeline-a" && \
  ( NXF_OFFLINE=true nextflow -c "$REPO/gate/gate.config" plugin nf-blocks:explore --port 8820 > "$SCRATCH/explore.out" 2>&1 & echo $! > "$SCRATCH/explore.pid" ) && \
  for _ in $(seq 120); do grep -q 'nf-blocks explorer:' "$SCRATCH/explore.out" && break; sleep 0.5; done && \
  cat "$SCRATCH/explore.out" && \
  curl -s -o /dev/null -w '%{http_code} %{size_download}\n' -H 'Range: bytes=0-4095' http://127.0.0.1:8820/m/lab/index/v2.sqlite && \
  curl -s http://127.0.0.1:8820/m/lab/log/ | head -c 300; echo && \
  curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: evil.test' http://127.0.0.1:8820/members.json && \
  BEFORE=$(stat -f %m "$GATE_STORE/index/v2.sqlite") && sleep 1 && \
  pkill -INT -f 'nf-blocks:explore' ; sleep 5; AFTER=$(stat -f %m "$GATE_STORE/index/v2.sqlite"); echo "mtime $BEFORE -> $AFTER"
```

Expected: `nf-blocks explorer: http://127.0.0.1:8820/`, `206 4096`, a JSON listing of five `run` entries, `403`, and a later mtime after the interrupt (the exit rewrite). `pkill -INT -f` matches the Java process's command line; if the exit rewrite did not happen, look in `$SCRATCH/explore.out` and `.nextflow.log` for the shutdown hook's error. CmdPlugin registers its own hook that stops the plugins; if the two race and the exit rewrite fails for that reason, record it in DESIGN.md §15 ("the exit rewrite is best effort; the start rewrite is the one the stale notice relies on") rather than working around Nextflow's shutdown.

- [ ] **Step 8: Commit**

```bash
cd nf-blocks
git add src/main/groovy/robsyme/cas/explore src/main/groovy/robsyme/cas/cli/CasCommands.groovy \
        src/test/groovy/robsyme/cas/explore
git commit -m "feat(explore): nextflow plugin nf-blocks:explore serves the page and every member on loopback

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Private S3 members, read through `explore`

**Files:**
- Modify: `build.gradle` (AWS SDK v2)
- Modify: `src/main/groovy/robsyme/cas/CasConfig.groovy` (S3 locations)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`openIndex` over local members only)
- Create: `src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy`
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy` (`membersOf` serves every configured store)
- Create: `src/test/groovy/robsyme/cas/explore/FakeS3.groovy`, `S3MemberFilesTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/CasConfigTest.groovy`

**Interfaces:**
- Consumes: `MemberFiles`, `ExploreServer` (Task 7); the Gradle file as Task 9 left it.
- Produces:
  - `CasConfig.configuredAliases` (every `cas.stores` alias, writable first); `boolean isRemote(String alias)`; `URI remoteLocationOf(String alias)`; `List<String> localLocations()` (the path strings of `members`, for `IndexPaths`). `members` holds local aliases only.
  - `class S3MemberFiles implements MemberFiles { S3MemberFiles(S3Client client, String bucket, String prefix); static S3MemberFiles open(URI location); static S3Client defaultClient() }`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/explore/FakeS3.groovy
package robsyme.cas.explore

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

/**
 * Just enough of S3's REST API, path style, for S3MemberFiles through the real
 * SDK: HeadObject, GetObject with one Range, ListObjectsV2 with paging.
 */
class FakeS3 {

    final Map<String, byte[]> objects = [:]      // "bucket/key" -> bytes
    final List<String> requests = []
    int pageSize = 1000
    private HttpServer server

    FakeS3 start() {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext('/') { HttpExchange ex -> handle(ex) }
        server.start()
        return this
    }

    String getUrl() { "http://127.0.0.1:${server.address.port}" }

    void stop() { server?.stop(0) }

    private void handle(HttpExchange ex) {
        try {
            final String path = URLDecoder.decode(ex.requestURI.rawPath.substring(1), 'UTF-8')
            final Map<String, String> query = (ex.requestURI.rawQuery ?: '').split('&').findAll().collectEntries {
                final List<String> kv = it.split('=', 2) as List<String>
                [(URLDecoder.decode(kv[0], 'UTF-8')): kv.size() > 1 ? URLDecoder.decode(kv[1], 'UTF-8') : '']
            }
            requests << "${ex.requestMethod} /${path}${query ? '?' + query.keySet().sort().join('&') : ''}".toString()
            if( query['list-type'] == '2' )
                list(ex, path.replaceAll('/$', ''), query)
            else
                object(ex, path)
        }
        finally {
            ex.close()
        }
    }

    private void object(HttpExchange ex, String path) {
        final byte[] bytes = objects[path]
        if( bytes == null ) {
            final byte[] body = '<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>no</Message></Error>'.bytes
            ex.responseHeaders.set('Content-Type', 'application/xml')
            ex.sendResponseHeaders(404, ex.requestMethod == 'HEAD' ? -1 : body.length)
            if( ex.requestMethod != 'HEAD' ) ex.responseBody.write(body)
            return
        }
        ex.responseHeaders.set('Content-Type', 'application/octet-stream')
        ex.responseHeaders.set('Accept-Ranges', 'bytes')
        ex.responseHeaders.set('ETag', '"fake"')
        final String range = ex.requestHeaders.getFirst('Range')
        if( ex.requestMethod == 'HEAD' ) {
            ex.responseHeaders.set('Content-Length', String.valueOf(bytes.length))
            ex.sendResponseHeaders(200, -1)
            return
        }
        if( range ) {
            final def m = range =~ /^bytes=(\d+)-(\d+)$/
            m.matches()
            final int start = m.group(1) as int
            final int end = Math.min(m.group(2) as int, bytes.length - 1)
            ex.responseHeaders.set('Content-Range', "bytes ${start}-${end}/${bytes.length}")
            ex.sendResponseHeaders(206, end - start + 1)
            ex.responseBody.write(bytes, start, end - start + 1)
            return
        }
        ex.sendResponseHeaders(200, bytes.length)
        ex.responseBody.write(bytes)
    }

    private void list(HttpExchange ex, String bucket, Map<String, String> query) {
        final String prefix = query['prefix'] ?: ''
        final List<String> keys = objects.keySet().findAll { it.startsWith("${bucket}/${prefix}") }
            .collect { it.substring(bucket.length() + 1) }.sort()
        final int from = (query['continuation-token'] ?: '0') as int
        final List<String> page = keys.drop(from).take(pageSize)
        final boolean more = from + page.size() < keys.size()
        final String xml = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            "<Name>${bucket}</Name><Prefix>${prefix}</Prefix><KeyCount>${page.size()}</KeyCount>" +
            "<MaxKeys>${pageSize}</MaxKeys><IsTruncated>${more}</IsTruncated>" +
            (more ? "<NextContinuationToken>${from + page.size()}</NextContinuationToken>" : '') +
            page.collect { "<Contents><Key>${it}</Key><Size>${objects["${bucket}/${it}"].length}</Size></Contents>" }.join('') +
            '</ListBucketResult>'
        final byte[] body = xml.getBytes('UTF-8')
        ex.responseHeaders.set('Content-Type', 'application/xml')
        ex.sendResponseHeaders(200, body.length)
        ex.responseBody.write(body)
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/explore/S3MemberFilesTest.groovy
package robsyme.cas.explore

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Specification

/** S3MemberFiles through the real SDK, against a fake S3 on loopback. */
class S3MemberFilesTest extends Specification {

    static final String CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'

    FakeS3 s3
    S3Client client
    byte[] snapshot = (0..<10240).collect { (byte) (it & 0xff) } as byte[]

    def setup() {
        s3 = new FakeS3().start()
        client = S3Client.builder()
            .endpointOverride(URI.create(s3.url))
            .forcePathStyle(true)
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('test', 'test')))
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build()
        s3.objects['bucket/member/index/v2.sqlite'] = snapshot
        s3.objects["bucket/member/blocks/${CID[-2..-1]}/${CID}".toString()] = [0xa0] as byte[]
        (1..5).each { s3.objects["bucket/member/log/823277415999${it}-run-${CID}".toString()] = new byte[0] }
        s3.objects['bucket/elsewhere/log/x'] = new byte[0]
    }

    def cleanup() {
        client?.close()
        s3?.stop()
    }

    def 'size, ranged reads and a paged listing under the member prefix'() {
        given:
        s3.pageSize = 2
        final S3MemberFiles files = new S3MemberFiles(client, 'bucket', 'member/')

        expect:
        files.size('index/v2.sqlite') == 10240L
        files.size('index/v3.sqlite') == null
        files.open('index/v2.sqlite', 4096, 100).withCloseable { it.readAllBytes() } == Arrays.copyOfRange(snapshot, 4096, 4196)
        files.list('log').size() == 5
        files.list('log').every { it.endsWith(CID) && !it.contains('/') }
        s3.requests.count { it.startsWith('GET /bucket?') } == 3
    }

    def 'open parses bucket and prefix from the URI'() {
        expect:
        S3MemberFiles.bucketAndPrefix(URI.create(uri)) == expected

        where:
        uri                      | expected
        's3://bucket'            | ['bucket', '']
        's3://bucket/'           | ['bucket', '']
        's3://bucket/member'     | ['bucket', 'member/']
        's3://bucket/a/b/'       | ['bucket', 'a/b/']
    }

    def 'explore serves an S3 member with Range, end to end'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('priv', new S3MemberFiles(client, 'bucket', 'member/'))
        final ExploreServer server = new ExploreServer(members, 'lab', 'x'.bytes).start(0)

        when:
        final def r = RawHttp.send(server.port, 'GET', '/m/priv/index/v2.sqlite', [Range: 'bytes=0-4095'])
        final def log = RawHttp.send(server.port, 'GET', '/m/priv/log/')

        then:
        r.status == 206
        r.body == Arrays.copyOfRange(snapshot, 0, 4096)
        log.text().count(CID) == 5

        cleanup:
        server.stop()
    }
}
```

Add to `CasConfigTest`:

```groovy
    def 'an S3 location is a read-only member that runs leave out and explore serves'() {
        given:
        final Map cfg = [cas: [stores: [lab: [location: '/data/cas'], priv: [location: 's3://bucket/member']]]]

        when:
        final CasConfig config = CasConfig.from(cfg, 'cas://lab')

        then:
        config.members == ['lab']
        config.configuredAliases == ['lab', 'priv']
        config.isRemote('priv')
        !config.isRemote('lab')
        config.remoteLocationOf('priv') == URI.create('s3://bucket/member')
        config.localLocations() == ['/data/cas']
    }

    def 'naming an S3 member in cas.resolve is refused for runs'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas'], priv: [location: 's3://bucket']], resolve: ['lab', 'priv']]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('priv')
        e.message.contains('nf-blocks:explore')
    }

    def 'the writable member cannot be on S3'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: 's3://bucket']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('local directory')
    }
```

- [ ] **Step 2: Add the SDK to `build.gradle`**

In `dependencies`, after `sqlite-jdbc`:

```groovy
    // Private S3 members for nf-blocks:explore (DESIGN.md §15). URL-connection
    // client only: the Apache and Netty clients would add megabytes for nothing.
    implementation platform('software.amazon.awssdk:bom:2.46.7')
    implementation 'software.amazon.awssdk:s3'
    implementation 'software.amazon.awssdk:sso'
    implementation 'software.amazon.awssdk:ssooidc'
    implementation 'software.amazon.awssdk:url-connection-client'
```

and after the `dependencies` block:

```groovy
configurations.configureEach {
    exclude group: 'software.amazon.awssdk', module: 'apache-client'
    exclude group: 'software.amazon.awssdk', module: 'netty-nio-client'
}
```

Run: `cd nf-blocks && ls -l build/distributions/*.zip; ./gradlew -q assemble && ls -l build/distributions/*.zip && ./gradlew -q dependencyCheck`
Record the zip size before and after in the commit message. Expected growth: a few megabytes; if it is over 15 MB, list `unzip -l` of the new jars and stop to ask Rob before continuing.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd nf-blocks && ./gradlew test --tests robsyme.cas.explore.S3MemberFilesTest --tests robsyme.cas.CasConfigTest`
Expected: FAIL, `unable to resolve class S3MemberFiles`, `No such property: configuredAliases`.

- [ ] **Step 4: Accept S3 locations in `CasConfig`**

Keep `locations` (`Map<String, Path>`) for local members and add `remotes` (`Map<String, URI>`), both filled in `from()` in config order:

```groovy
    private static final Pattern S3_LOCATION = ~/^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](\/.*)?$/

    /** Every configured store alias, writable first: what nf-blocks:explore serves. */
    final List<String> configuredAliases

    private final Map<String, URI> remotes
```

In `from()`, replace the loop over `stores` and the member list with:

```groovy
        final Map<String, Path> locations = new LinkedHashMap<String, Path>()
        final Map<String, URI> remotes = new LinkedHashMap<String, URI>()
        for( Map.Entry entry : stores.entrySet() ) {
            final String name = entry.key as String
            checkAlias(name)
            final String location = locationText(name, entry.value)
            if( location.startsWith('s3://') ) {
                if( !S3_LOCATION.matcher(location).matches() )
                    throw new IllegalArgumentException("cas.stores.${name}.location is not an S3 URI of the form s3://<bucket>[/<prefix>] -- offending value: ${location}")
                remotes.put(name, URI.create(location))
            }
            else {
                locations.put(name, Path.of(location).toAbsolutePath().normalize())
            }
        }
        if( remotes.containsKey(alias) )
            throw new IllegalArgumentException("the writable member '${alias}' must be a local directory, not ${remotes.get(alias)}; S3 members are read-only and only nf-blocks:explore reads them")
        if( !locations.containsKey(alias) )
            throw new IllegalArgumentException("Missing store configuration 'cas.stores.${alias}' for the writable member '${alias}' named by lineage.store.location")

        final List<String> members = memberList(alias, scope.get('resolve'), locations.keySet(), remotes)
        final List<String> configured = [alias] + ((locations.keySet() + remotes.keySet()) - alias).toList()
```

`locationText(name, opts)` is the old `locationFor` returning the raw string (same "Missing 'cas.stores.<alias>.location'" error). `memberList` gains the `remotes` parameter; before its existing unknown-alias check, add:

```groovy
        for( String name : requested ) {
            if( remotes.containsKey(name) )
                throw new IllegalArgumentException("store '${name}' is an S3 member (${remotes.get(name)}), which only nf-blocks:explore reads so far; leave it out of cas.resolve")
        }
```

and let the default (no `resolve`) be the local aliases, which it already is once `known` is `locations.keySet()`. Add the accessors:

```groovy
    boolean isRemote(String alias) { remotes.containsKey(alias) }

    URI remoteLocationOf(String alias) { remotes.get(alias) }

    /** The resolvable members' local paths, as IndexPaths names the cache file by them. */
    List<String> localLocations() { members.collect { String a -> locations.get(a).toString() } }
```

and pass `configured` and `remotes` through the constructor. `CasSession.openIndex()` becomes:

```groovy
    Index openIndex() {
        return Index.open(IndexPaths.cachePath(config.localLocations(), config.indexOverride))
    }
```

The cache file name does not change for an existing local-only configuration: `localLocations()` is the same list `openIndex` built before.

- [ ] **Step 5: Write `S3MemberFiles`, and serve every configured store**

```groovy
// src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy
package robsyme.cas.explore

import groovy.transform.CompileStatic
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object

/**
 * A member in a private bucket, read with the user's own credentials so the
 * page never holds any (block explorer spec section 1.4). Ranged GetObject:
 * nf-amazon's newByteChannel downloads the whole object, which a 580 MB
 * snapshot cannot afford (v26.04.6, S3FileSystemProvider.newByteChannel).
 */
@CompileStatic
class S3MemberFiles implements MemberFiles {

    private final S3Client client
    private final String bucket
    private final String prefix

    S3MemberFiles(S3Client client, String bucket, String prefix) {
        this.client = client
        this.bucket = bucket
        this.prefix = prefix
    }

    static S3MemberFiles open(URI location) {
        final List<String> parts = bucketAndPrefix(location)
        return new S3MemberFiles(defaultClient(), parts[0], parts[1])
    }

    static List<String> bucketAndPrefix(URI location) {
        String prefix = (location.path ?: '').replaceAll('^/+', '')
        if( prefix && !prefix.endsWith('/') )
            prefix += '/'
        return [location.host, prefix]
    }

    /**
     * The default credential chain (AWS_PROFILE, SSO, environment, instance
     * role), any region, cross-region access on so the bucket's region need not
     * be configured.
     */
    static S3Client defaultClient() {
        return withPluginLoader {
            S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(defaultRegion())
                .crossRegionAccessEnabled(true)
                .build()
        }
    }

    private static Region defaultRegion() {
        try {
            return new DefaultAwsRegionProviderChain().region
        }
        catch( SdkClientException e ) {
            return Region.US_EAST_1
        }
    }

    /**
     * The SDK finds its HTTP client and the SSO credential classes through the
     * thread's context class loader, which inside a Nextflow plugin is not the
     * plugin's. Every SDK call runs with the plugin's loader in place.
     */
    private static <T> T withPluginLoader(Closure<T> body) {
        final Thread thread = Thread.currentThread()
        final ClassLoader previous = thread.contextClassLoader
        thread.contextClassLoader = S3MemberFiles.classLoader
        try {
            return body.call()
        }
        finally {
            thread.contextClassLoader = previous
        }
    }

    @Override
    Long size(String rel) {
        return withPluginLoader {
            try {
                return client.headObject(HeadObjectRequest.builder().bucket(bucket).key(prefix + rel).build()).contentLength()
            }
            catch( NoSuchKeyException e ) {
                return (Long) null
            }
            catch( S3Exception e ) {
                if( e.statusCode() == 404 )
                    return (Long) null
                throw e
            }
        }
    }

    @Override
    InputStream open(String rel, long start, long length) {
        return withPluginLoader {
            (InputStream) client.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(prefix + rel)
                .range("bytes=${start}-${start + length - 1}".toString())
                .build())
        }
    }

    @Override
    List<String> list(String dirRel) {
        final String under = prefix + dirRel.replaceAll('/+$', '') + '/'
        return withPluginLoader {
            final List<String> names = []
            for( S3Object object : client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(under).build()).contents() ) {
                final String name = object.key().substring(under.length())
                if( name && !name.contains('/') )
                    names.add(name)
            }
            return names
        }
    }

    @Override
    String describe() { "s3://${bucket}/${prefix}" }
}
```

In `ExploreCommand.membersOf`, serve every configured store:

```groovy
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.configuredAliases )
            members.put(alias, config.isRemote(alias)
                ? (MemberFiles) S3MemberFiles.open(config.remoteLocationOf(alias))
                : new LocalMemberFiles(config.locationOf(alias)))
        return members
    }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd nf-blocks && ./gradlew test --tests robsyme.cas.explore.S3MemberFilesTest --tests robsyme.cas.CasConfigTest` then `./gradlew check`
Expected: PASS. If the SDK rejects the fake's responses (a checksum or header it insists on), make the fake answer the way real S3 does, never the client laxer; the real-S3 check is Task 13.

- [ ] **Step 7: Commit**

```bash
cd nf-blocks
git add build.gradle src/main/groovy/robsyme/cas/CasConfig.groovy src/main/groovy/robsyme/cas/CasSession.groovy \
        src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy \
        src/test/groovy/robsyme/cas/explore/FakeS3.groovy src/test/groovy/robsyme/cas/explore/S3MemberFilesTest.groovy \
        src/test/groovy/robsyme/cas/CasConfigTest.groovy
git commit -m "feat(explore): private S3 members, read with the user's credentials through explore

Plugin zip: <before> -> <after> bytes.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 9: The page is built by Gradle into the plugin

**Files:**
- Modify: `build.gradle`

**Interfaces:**
- Consumes: `web/build.mjs` building `dist/index.html` (Task 11); `web/schema-gen.mjs` (Task 10).
- Produces: Gradle tasks `webBuild` (writes `web/dist/index.html`) and `webTest` (`npm test`), both on the node plugin's pinned Node; `processResources` puts the page at `robsyme/cas/explorer/index.html` in the plugin jar; `check` runs `webTest`; `dependencyCheck` also checks both npm lockfiles.

- [ ] **Step 1: Make `dependencyCheck` refuse an unpinned or non-registry npm dependency**

Write the failing check first: temporarily change `"esbuild": "0.28.2"` in `web/package.json` to `"^0.28.2"` and run `./gradlew dependencyCheck`. Expected today: PASS (nothing checks it). Add to the `dependencyCheck` task's `doLast`, before the final `if( offenders )`:

```groovy
        // Block explorer spec section 1.4: the npm lockfiles fall under this check too.
        ['web', 'gate/browser'].each { String dir ->
            final def manifest = file("${dir}/package.json")
            final def lock = file("${dir}/package-lock.json")
            if( !manifest.exists() )
                return
            final def pkg = new groovy.json.JsonSlurper().parse(manifest) as Map
            ['dependencies', 'devDependencies'].each { String section ->
                ((pkg[section] ?: [:]) as Map).each { name, version ->
                    if( !(version ==~ /\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?/) )
                        offenders << "${dir}/package.json: ${name} is '${version}', not an exact released version"
                }
            }
            if( !lock.exists() ) {
                offenders << "${dir}/package-lock.json is missing"
                return
            }
            final def packages = ((new groovy.json.JsonSlurper().parse(lock) as Map).packages ?: [:]) as Map
            packages.each { path, entry ->
                final def resolved = (entry as Map).resolved as String
                if( path && !(entry as Map).link && !(resolved?.startsWith('https://registry.npmjs.org/')) )
                    offenders << "${dir}/package-lock.json: ${path} resolves from '${resolved}', not https://registry.npmjs.org/"
            }
        }
```

Run: `./gradlew dependencyCheck`
Expected: FAIL naming `web/package.json: esbuild is '^0.28.2'`. Put `"0.28.2"` back, run again: PASS.

- [ ] **Step 2: Build and test the page through Gradle**

At the top of `build.gradle`, before `plugins {`:

```groovy
import com.github.gradle.node.npm.task.NpmTask
```

In `plugins {}` add:

```groovy
    id 'com.github.node-gradle.node' version '7.1.0'
```

After the `nextflowPlugin {}` block:

```groovy
// The explorer page (block explorer spec section 2): an npm project under web/,
// built on a pinned Node and packaged into the plugin jar. Its output is not committed.
node {
    download = true
    version = '24.21.0'
    nodeProjectDir = file('web')
    npmInstallCommand = 'ci'
}

tasks.register('webBuild', NpmTask) {
    description = 'Builds web/dist/index.html, the self-contained explorer page'
    group = 'build'
    dependsOn 'npmInstall'
    args = ['run', 'build']
    inputs.dir('web/src')
    inputs.files('web/build.mjs', 'web/schema-gen.mjs', 'web/package-lock.json', 'DESIGN.md')
    outputs.file('web/dist/index.html')
}

tasks.register('webTest', NpmTask) {
    description = 'Runs the page unit tests (node --test)'
    group = 'verification'
    dependsOn 'npmInstall'
    args = ['test']
    inputs.dir('web/src')
    inputs.dir('web/test')
    inputs.files('web/schema-gen.mjs', 'web/package-lock.json', 'DESIGN.md', 'src/main/groovy/robsyme/cas/core/Index.groovy')
    outputs.upToDateWhen { false }
}

processResources {
    dependsOn 'webBuild'
    from('web/dist/index.html') { into 'robsyme/cas/explorer' }
}
```

and change the last line to:

```groovy
check.dependsOn memoryBoundTest, dependencyCheck, webTest
```

`file('web')` is not `files(`, so the existing `dependencyCheck` needle does not trip on it.

- [ ] **Step 3: Check the jar carries the page and a run writes it**

Run: `cd nf-blocks && ./gradlew clean assemble && unzip -l build/libs/*.jar | grep explorer/index.html && ./gradlew check`
Expected: one `robsyme/cas/explorer/index.html` entry of about 1.5 MB, and `check` PASS including `webTest`.

Then `GATE_ROOT="$SCRATCH/g" make gate` and check `$SCRATCH/g/store/index.html` exists and equals `web/dist/index.html` (`cmp`). Open it in a browser through `nf-blocks:explore` or `python3 gate/browser/serve.py "$SCRATCH/g/store" 8830` and look at `http://127.0.0.1:8830/index.html`: the home view lists the Gate's pipeline.

- [ ] **Step 4: Commit**

```bash
cd nf-blocks
git add build.gradle
git commit -m "build: the explorer page is built on a pinned Node into the plugin jar; npm lockfiles under dependencyCheck

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: The page's core: Store Log tail, verified blocks, metadata, and the model

**Files:**
- Modify: `web/package.json`, `web/package-lock.json` (add `@noble/hashes` 2.4.0; `test` runs `schema-gen.mjs` first)
- Create: `web/schema-gen.mjs`
- Modify: `web/build.mjs` (generate the schema before bundling)
- Create: `web/src/storelog.js`, `web/src/cid.js`, `web/src/typed.js`, `web/src/metadata.js`, `web/src/schema.js`, `web/src/blocks.js`, `web/src/store.js`, `web/src/model.js`
- Create: `web/test/fixtures/schema.sql`, `web/test/fixture.mjs`
- Create: `web/test/storelog.test.mjs`, `cid.test.mjs`, `metadata.test.mjs`, `schema.test.mjs`, `blocks.test.mjs`, `store.test.mjs`, `model.test.mjs`
- Modify: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy` (pin `schema.sql`)

**Interfaces:**
- Consumes: `web/src/config.js`, `web/src/db.js`, `web/src/vfs/*` (Task 2); `web/src/queries.json` (Task 4).
- Produces:
  - `storelog.js`: `parseEntry(name) -> { name, rts, writtenAtMillis, kind, cid } | null`; `floorOf(watermark, nowMillis) -> number | null`; `entriesSince(names, watermark, nowMillis) -> Entry[]` (newest first); `listLog(base, { fetchFn, watermark, nowMillis }) -> Promise<string[]>`.
  - `cid.js`: `DAG_CBOR = 0x71`, `RAW = 0x55`; `cidFor(bytes, code) -> CID`; `verifies(cidText, bytes) -> boolean`; `blockPath(cidText) -> 'blocks/<xx>/<cid>'`.
  - `typed.js`: `class Float { value }`; `typedDecode(bytes)`: like `@ipld/dag-cbor`'s `decode`, with every CBOR float wrapped in `Float`.
  - `metadata.js`: `VALUE_CAP_BYTES = 1024`; `isLeaf(v)`; `metadataView(value)`; `leavesOf(value) -> Leaf[]`; `javaDouble(x) -> string`; `attrRows(view) -> [{ path, type, value, truncated }]`; `predicateRow(path, type, text) -> row` (throws `PredicateError`); `matches(rows, predicates) -> boolean`.
  - `schema.js`: `validBlock(value) -> boolean`, `validLeaf(value) -> boolean`.
  - `blocks.js`: `class BlockError extends Error { code, cid }`; `class BlockFetcher(base, { fetchFn })` with `get(cidText) -> Promise<{ cid, bytes, value }>`, `ofKind(cidText, kind)`, `verified: string[]`, `fetches: number`.
  - `store.js`: `resolveStore(href, { fetchFn }) -> Promise<{ base, members, member }>`.
  - `model.js`: `class Explorer` with `static open({ base, openDb, blocks, listFn, now })`, and `watermark`, `stale` (`StaleRun[]`), `staleCount`, `notice` (boolean), `pipelines()`, `runsOfPipeline(pipeline, { limit, offset })`, `run(completionCid)`, `completionOf(completionCid)`, `collection(collectionCid)`, `item(collectionCid, itemCid)`, `producersOf(contentCid, onProgress)`, `latestSuccessfulRun(pipeline) -> cid | null`, `items(completionCid, output, predicates, onProgress) -> { items: cid[], collection: cid | null }`. A run row is `{ completion_cid, manifest_cid, pipeline, run_name, status, possibly_incomplete, finished_at, source }`, `source` `'snapshot'` or `'tail'`. A `StaleRun` is `{ cid, entry, row, completion, manifest, error }`, `error` a `BlockError` or null.

- [ ] **Step 1: Add the dependency and the schema generator**

```bash
cd nf-blocks/web && npm install --save-exact @noble/hashes@2.4.0 --no-audit --no-fund
```

`multiformats`' browser SHA-256 calls `crypto.subtle`, which exists only in a secure context; a page opened over plain HTTP from a LAN host or an S3 website endpoint is not one. `@noble/hashes` is pure JavaScript, so hashing works everywhere the page loads.

```js
// web/schema-gen.mjs
// Extracts the IPLD Schema of DESIGN.md §6 (the one ```ipldsch block) into
// src/generated/schema.json for the page (block explorer spec section 12).
// @ipld/schema's typed builder names the prelude Any "$$Any", so value
// references are rewritten to it, as the prototype's validate.mjs did.
import { fromDSL } from '@ipld/schema/from-dsl.js'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'

export function extractSchema(design) {
  const block = /```ipldsch\n([\s\S]*?)```/.exec(design)
  if (!block) throw new Error('DESIGN.md has no ```ipldsch block')
  const parsed = fromDSL(block[1])
  return JSON.parse(JSON.stringify(parsed), (key, value) =>
    (value === 'Any' && (key === 'valueType' || key === 'type')) ? '$$Any' : value)
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const here = new URL('.', import.meta.url)
  const schema = extractSchema(readFileSync(new URL('../DESIGN.md', here), 'utf8'))
  mkdirSync(new URL('src/generated/', here), { recursive: true })
  writeFileSync(new URL('src/generated/schema.json', here), JSON.stringify(schema))
  console.log(`src/generated/schema.json: ${Object.keys(schema.types).length} types`)
}
```

In `web/package.json`, set:

```json
    "gen": "node schema-gen.mjs",
    "build": "node schema-gen.mjs && node build.mjs",
    "test": "node schema-gen.mjs && node --test --test-reporter=spec \"test/**/*.test.mjs\""
```

Run: `cd nf-blocks/web && node schema-gen.mjs`
Expected: `src/generated/schema.json: 19 types` (the count spec section 12 recorded).

- [ ] **Step 2: Pin the test schema to the index**

Generate the fixture from a fresh index (any `Index.open` result; the Gate's cache index from Task 5 works, since only the schema is read):

```bash
cd nf-blocks && mkdir -p web/test/fixtures && \
  sqlite3 "file:$(ls "$SCRATCH"/g/cache/nf-blocks/*.sqlite | head -1)?mode=ro" \
    "SELECT sql || ';' FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid" > web/test/fixtures/schema.sql
```

Add to `ExplorerQueriesTest`:

```groovy
    def 'the page tests build their databases from the index schema itself'() {
        given:
        final def rs = connection.createStatement().executeQuery(
            "SELECT sql || ';' FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")
        final List<String> statements = []
        while( rs.next() )
            statements << rs.getString(1)

        expect:
        Path.of('web/test/fixtures/schema.sql').text.trim() == statements.join('\n').trim()
    }
```

Run: `./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest`
Expected: PASS. (Delete a line from `schema.sql` and rerun to see it fail, then restore it.)

- [ ] **Step 3: Write the test fixture**

```js
// web/test/fixture.mjs
// A small member built the way the plugin builds one: real DAG-CBOR blocks,
// and a snapshot with the index schema holding run R1's rows. R2 and R3 are
// newer and only in the Store Log. Tests may encode; the page never does.
import { readFileSync } from 'node:fs'
import * as dagCbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from '@noble/hashes/sha2.js'
import { loadSqlite } from './helpers.mjs'

const HORIZON = 9999999999999
export const entryName = (millis, kind, cid) => `${String(HORIZON - millis).padStart(13, '0')}-${kind}-${cid}`
export const rawCid = (text) => CID.create(1, 0x55, Digest.create(0x12, sha256(new TextEncoder().encode(text))))

export function block(value) {
  const bytes = dagCbor.encode(value)
  return { cid: CID.create(1, 0x71, Digest.create(0x12, sha256(bytes))), bytes }
}

const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })

function manifest(runName) {
  return { kind: 'RunManifest', schema: 1, asserted_by: 'test', pipeline: 'demo', repository: null, revision: null,
    commit_id: null, run_name: runName, nf_run_hash: `hash-${runName}`, session_id: 's', resumed: false,
    nextflow_version: '26.04.6', params: {}, config: {}, script: null, started_at: '2026-09-01T00:00:00.000Z' }
}

function completion(run, collections, status, finishedAt) {
  return { kind: 'RunCompletion', schema: 1, asserted_by: 'test', run, collections, input_set: null, status,
    exit_status: status === 'succeeded' ? 0 : 1, possibly_incomplete: status !== 'succeeded',
    started_at: '2026-09-01T00:00:00.000Z', finished_at: finishedAt,
    anomalies: { unresolvable: 0, unaddressed: 1, declined: 0, never_published: 0 }, error: null }
}

export async function buildMember({ now = Date.now() } = {}) {
  const blocks = new Map()
  const put = (value) => { const b = block(value); blocks.set(b.cid.toString(), b.bytes); return b.cid }
  const content = { A: rawCid('bam-A'), B: rawCid('bam-B'), C: rawCid('bam-C') }
  const item = {
    A: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'A', lane: 1, depth: 1.5 }, leaf('A.bam', content.A)] }),
    B: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'B', lane: 2, depth: 2.5 }, leaf('B.bam', content.B)] }),
    C: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'C', lane: 3, depth: 3.5 }, leaf('C.bam', content.C)] }),
  }
  const byCid = (ids) => ids.map(k => item[k]).sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
  const runs = {}
  for (const [name, samples, status, finished] of [
    ['R1', ['A', 'B'], 'succeeded', '2026-09-01T10:00:00.000Z'],
    ['R2', ['B', 'C'], 'succeeded', '2026-09-02T10:00:00.000Z'],
    ['R3', ['C'], 'failed', '2026-09-03T10:00:00.000Z'],
  ]) {
    const m = put(manifest(name))
    const items = byCid(samples)
    const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: m, name: 'aligned', items,
      paths: items.map(i => [`aligned/${samples.find(s => item[s].equals(i))}.bam`]) })
    const comp = put(completion(m, [coll], status, finished))
    runs[name] = { manifest: m.toString(), collection: coll.toString(), completion: comp.toString(), samples, status, finished }
  }
  const at = { R0: now - 3 * 3600_000, R1: now - 3600_000, R2: now - 1800_000, R2again: now - 1500_000, R3: now - 1200_000 }
  const log = [
    entryName(at.R0, 'run', runs.R1.completion.replace(/.$/, 'q')),     // outside the overlap, never looked at
    entryName(at.R1, 'run', runs.R1.completion),
    entryName(at.R2, 'run', runs.R2.completion),
    entryName(at.R2again, 'run', runs.R2.completion),                    // the same run logged twice
    entryName(at.R3, 'run', runs.R3.completion),
    entryName(at.R3 + 1, 'selection', runs.R3.completion),               // not a run: ignored in milestone 1
  ]
  const watermark = entryName(at.R1, 'run', runs.R1.completion)
  const snapshot = await snapshotOf(runs.R1, item, content, watermark)
  return { blocks, log, watermark, runs, item: Object.fromEntries(Object.entries(item).map(([k, v]) => [k, v.toString()])),
    content: Object.fromEntries(Object.entries(content).map(([k, v]) => [k, v.toString()])), snapshot }
}

/** R1's rows, as Index.ingestRun and IndexSnapshot.write would leave them. */
async function snapshotOf(r1, item, content, watermark) {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    db.exec({ sql: 'INSERT INTO schema_version VALUES (2)' })
    db.exec({ sql: 'INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)', bind: [r1.completion, r1.manifest, 'demo', null, null,
      'hash-R1', 's', 'R1', 'test', 'succeeded', 0, r1.finished] })
    db.exec({ sql: 'INSERT INTO collection VALUES (?,?,?)', bind: [r1.collection, r1.completion, 'aligned'] })
    for (const [s, lane, depth] of [['A', 1, '1.5'], ['B', 2, '2.5']]) {
      const i = item[s].toString()
      db.exec({ sql: 'INSERT INTO item VALUES (?)', bind: [i] })
      db.exec({ sql: 'INSERT INTO collection_item VALUES (?,?)', bind: [r1.collection, i] })
      db.exec({ sql: 'INSERT INTO producer VALUES (?,?,?,?,?)', bind: [content[s].toString(), i, r1.collection, r1.completion, `${s}.bam`] })
      for (const [path, type, value] of [['sample', 'string', s], ['lane', 'int', String(lane)], ['depth', 'float', depth]])
        db.exec({ sql: 'INSERT INTO item_attr VALUES (?,?,?,?,0)', bind: [i, path, type, value] })
    }
    db.exec({ sql: "INSERT INTO meta VALUES ('store_log_watermark', ?)", bind: [watermark] })
    db.exec({ sql: "INSERT INTO meta VALUES ('snapshot_written_at', '2026-09-01T10:00:01.000Z')" })
    return sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
}

/** A fetch over the fixture's blocks, counting what is asked for. */
export function blockFetch(blocks) {
  const asked = []
  const fn = async (url) => {
    const cid = String(url).split('/').pop()
    asked.push(cid)
    const bytes = blocks.get(cid)
    return bytes ? new Response(bytes) : new Response('no such block', { status: 404 })
  }
  fn.asked = asked
  return fn
}
```

The `R0` entry names a CID that is not in the fixture: it sits outside the overlap window, so the model must never ask for it.

- [ ] **Step 4: Write the failing tests**

```js
// web/test/storelog.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { entriesSince, listLog, parseEntry } from '../src/storelog.js'
import { entryName } from './fixture.mjs'

const CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
const T = 1_758_000_000_000
const MIN = 60_000

test('entry names parse as StoreLog.parse does', () => {
  const e = parseEntry(entryName(T, 'run', CID))
  assert.deepEqual([e.kind, e.cid, e.writtenAtMillis], ['run', CID, T])
  for (const bad of ['.DS_Store', `123-run-${CID}`, `${'1'.repeat(13)}-pin-${CID}`, `${'1'.repeat(13)}-run-nope`])
    assert.equal(parseEntry(bad), null, bad)
})

test('since the watermark: newer, the watermark, and 10 minutes behind it; newest first', () => {
  const names = [entryName(T + MIN, 'run', CID), entryName(T, 'run', CID), entryName(T - 5 * MIN, 'run', CID),
    entryName(T - 11 * MIN, 'run', CID), 'junk']
  const since = entriesSince(names, entryName(T, 'run', CID), T + 60 * MIN)
  assert.deepEqual(since.map(e => e.writtenAtMillis), [T + MIN, T, T - 5 * MIN])
})

test('the floor is clamped to the local clock, as a watermark from a fast clock cannot hide entries', () => {
  const ahead = entryName(T + 60 * MIN, 'run', CID)
  const since = entriesSince([entryName(T - 5 * MIN, 'run', CID), entryName(T - 20 * MIN, 'run', CID)], ahead, T)
  assert.deepEqual(since.map(e => e.writtenAtMillis), [T - 5 * MIN])
})

test('no watermark means every entry', () => {
  assert.equal(entriesSince([entryName(T, 'run', CID), entryName(T - 99 * MIN, 'run', CID)], null, T).length, 2)
})

const json = (body) => new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })

test('explore answers log/ with JSON', async () => {
  const names = [entryName(T, 'run', CID)]
  const fetchFn = async (url) => { assert.equal(url, 'http://h/m/lab/log/'); return json({ entries: names }) }
  assert.deepEqual(await listLog('http://h/m/lab/', { fetchFn }), names)
})

test('a static server answers log/ with an HTML index', async () => {
  const name = entryName(T, 'run', CID)
  const html = `<html><body><ul><li><a href="${name}">${name}</a></li><li><a href="../">..</a></li></ul></body></html>`
  const fetchFn = async () => new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } })
  assert.deepEqual(await listLog('http://h/store/', { fetchFn }), [name])
})

test('S3 is listed with ListObjectsV2, paged, stopping once a page reaches past the floor', async () => {
  const pages = [
    [entryName(T + 2 * MIN, 'run', CID), entryName(T + MIN, 'run', CID)],
    [entryName(T, 'run', CID), entryName(T - 30 * MIN, 'run', CID)],
    [entryName(T - 60 * MIN, 'run', CID)],
  ]
  const asked = []
  const fetchFn = async (url) => {
    asked.push(url)
    if (url.endsWith('/log/')) return new Response('<Error><Code>AccessDenied</Code></Error>', { status: 403 })
    const u = new URL(url)
    assert.equal(u.searchParams.get('prefix'), 'member/log/')
    const i = Number(u.searchParams.get('continuation-token') ?? 0)
    const keys = pages[i].map(n => `<Contents><Key>member/log/${n}</Key></Contents>`).join('')
    const next = i + 1 < pages.length ? `<NextContinuationToken>${i + 1}</NextContinuationToken>` : ''
    return new Response(`<?xml version="1.0"?><ListBucketResult>${keys}${next}</ListBucketResult>`, { headers: { 'Content-Type': 'application/xml' } })
  }
  const names = await listLog('https://b.s3.ca-central-1.amazonaws.com/member/', { fetchFn, watermark: entryName(T, 'run', CID), nowMillis: T })
  assert.equal(names.length, 4)
  assert.equal(asked.filter(u => u.includes('list-type=2')).length, 2)
})

test('no listing at all is an empty log', async () => {
  const fetchFn = async () => new Response('nope', { status: 404 })
  assert.deepEqual(await listLog('http://h/store/', { fetchFn }), [])
})
```

```js
// web/test/cid.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { blockPath, verifies } from '../src/cid.js'

const EMPTY_MAP = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'   // DESIGN.md §3 vector
const HELLO = 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'

test('DESIGN.md §3 vectors verify, and a changed byte does not', () => {
  assert.equal(verifies(EMPTY_MAP, Uint8Array.of(0xa0)), true)
  assert.equal(verifies(HELLO, new TextEncoder().encode('hello\n')), true)
  assert.equal(verifies(HELLO, new TextEncoder().encode('hellO\n')), false)
  assert.equal(verifies(EMPTY_MAP, new TextEncoder().encode('hello\n')), false)
})

test('hashing does not need crypto.subtle (a page on plain http from a LAN host)', () => {
  // Node defines subtle on Crypto.prototype; an own property shadows it until deleted.
  Object.defineProperty(globalThis.crypto, 'subtle', { value: undefined, configurable: true })
  try {
    assert.equal(globalThis.crypto.subtle, undefined)
    assert.equal(verifies(EMPTY_MAP, Uint8Array.of(0xa0)), true)
  } finally {
    delete globalThis.crypto.subtle
  }
})

test('the block path is the last two characters of the cid', () => {
  assert.equal(blockPath(EMPTY_MAP), `blocks/ua/${EMPTY_MAP}`)
})
```

```js
// web/test/metadata.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Float, typedDecode } from '../src/typed.js'
import { attrRows, javaDouble, matches, metadataView, predicateRow, PredicateError } from '../src/metadata.js'

// Each right-hand side is Java 21's Double.toString, checked with `java` on 2026-09-25.
const JAVA = [[30.0, '30.0'], [1e10, '1.0E10'], [1e-4, '1.0E-4'], [0.001, '0.001'], [1234567, '1234567.0'],
  [12345678.9, '1.23456789E7'], [-0, '-0.0'], [0, '0.0'], [1.5, '1.5'], [1e7, '1.0E7'], [9999999, '9999999.0'],
  [0.1 + 0.2, '0.30000000000000004'], [1e21, '1.0E21'], [5e-324, '4.9E-324'], [1.7976931348623157e308, '1.7976931348623157E308'],
  [100, '100.0'], [2.5e-3, '0.0025'], [123456.789, '123456.789'], [-1.5e10, '-1.5E10']]

test('javaDouble is Double.toString', () => {
  for (const [x, java] of JAVA) assert.equal(javaDouble(x), java, String(x))
})

// {"a": 30 (unsigned int), "b": 30.0 (float64)}, by hand: an encoder would write 30.0 as an int.
const INT_AND_FLOAT = Uint8Array.from([0xa2, 0x61, 0x61, 0x18, 0x1e, 0x61, 0x62, 0xfb, 0x40, 0x3e, 0, 0, 0, 0, 0, 0])

test('typedDecode keeps a float a float, even when it is integral', () => {
  const v = typedDecode(INT_AND_FLOAT)
  assert.equal(v.a, 30)
  assert.ok(v.b instanceof Float)
  assert.equal(v.b.value, 30)
})

test('attribute rows are what Index.ingestItem writes (Review Focus 3)', () => {
  const value = [{ ...typedDecode(INT_AND_FLOAT), big: new Float(1e10), meta: { strand: 'fwd', tags: ['x', 'y'] }, ok: true,
    none: null, file: { kind: 'Leaf', name: 'A.bam', address: null, size: null, provider: null, reason: 'declined' } }, { other: 1 }]
  const rows = attrRows(metadataView(value)).map(r => [r.path, r.type, r.value, r.truncated]).sort()
  assert.deepEqual(rows, [
    ['a', 'int', '30', 0], ['b', 'float', '30.0', 0], ['big', 'float', '1.0E10', 0],
    ['meta.strand', 'string', 'fwd', 0], ['meta.tags', 'string', 'x', 0], ['meta.tags', 'string', 'y', 0],
    ['none', 'null', null, 0], ['ok', 'bool', 'true', 0],
  ].sort())
})

test('a string past 1024 UTF-8 bytes is stored as its digest', () => {
  const [row] = attrRows({ long: 'a'.repeat(1025) })
  assert.deepEqual(row, { path: 'long', type: 'string', value: 'sha256:4a82297889eb505cf6b5cbdf69977afab4632d6557539782f657bd7dc78091a5', truncated: 1 })
})

test('the metadata view is the item if a map, else its first non-Leaf map', () => {
  assert.deepEqual(metadataView({ s: 1 }), { s: 1 })
  assert.deepEqual(metadataView([{ kind: 'Leaf' }, 'x', { s: 2 }]), { s: 2 })
  assert.equal(metadataView({ kind: 'Leaf' }), null)
  assert.equal(metadataView('scalar'), null)
})

test('predicates normalise the way MetadataView.scalar types a value', () => {
  assert.deepEqual(predicateRow('lane', 'int', '007'), { path: 'lane', type: 'int', value: '7', truncated: 0 })
  assert.deepEqual(predicateRow('depth', 'float', '30'), { path: 'depth', type: 'float', value: '30.0', truncated: 0 })
  assert.deepEqual(predicateRow('ok', 'bool', 'true'), { path: 'ok', type: 'bool', value: 'true', truncated: 0 })
  assert.deepEqual(predicateRow('x', 'null', ''), { path: 'x', type: 'null', value: null, truncated: 0 })
  assert.throws(() => predicateRow('lane', 'int', '1.5'), PredicateError)
  assert.throws(() => predicateRow('ok', 'bool', 'yes'), PredicateError)
  assert.throws(() => predicateRow('x', 'date', '1'), PredicateError)
})

test('matching needs every predicate, and a truncated row never matches', () => {
  const rows = [{ path: 's', type: 'string', value: 'A', truncated: 0 }, { path: 'n', type: 'int', value: '1', truncated: 0 },
    { path: 'l', type: 'string', value: 'sha256:00', truncated: 1 }]
  assert.equal(matches(rows, [predicateRow('s', 'string', 'A'), predicateRow('n', 'int', '1')]), true)
  assert.equal(matches(rows, [predicateRow('s', 'string', 'A'), predicateRow('n', 'int', '2')]), false)
  assert.equal(matches(rows, [{ path: 'l', type: 'string', value: 'sha256:00', truncated: 0 }]), false)
  assert.equal(matches(rows, []), true)
})
```

```js
// web/test/schema.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { extractSchema } from '../schema-gen.mjs'
import { validBlock, validLeaf } from '../src/schema.js'
import { buildMember } from './fixture.mjs'
import * as dagCbor from '@ipld/dag-cbor'

test('DESIGN.md §6 parses to 19 types', () => {
  const schema = extractSchema(readFileSync(new URL('../../DESIGN.md', import.meta.url), 'utf8'))
  assert.equal(Object.keys(schema.types).length, 19)
})

test('every fixture block is valid, and broken ones are not', async () => {
  const { blocks, runs } = await buildMember()
  for (const bytes of blocks.values()) assert.equal(validBlock(dagCbor.decode(bytes)), true)
  const rc = dagCbor.decode(blocks.get(runs.R1.completion))
  assert.equal(validBlock({ ...rc, status: 'done' }), false)
  assert.equal(validBlock((({ finished_at, ...rest }) => rest)(rc)), false)
  assert.equal(validBlock({ ...rc, kind: 'Nope' }), false)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: null, reason: 'declined' }), true)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'laptop', reason: null }), false)
})
```

```js
// web/test/blocks.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { BlockFetcher } from '../src/blocks.js'
import { blockFetch, buildMember } from './fixture.mjs'

test('a block is fetched once, verified, decoded and recorded', async () => {
  const { blocks, runs } = await buildMember()
  const fetchFn = blockFetch(blocks)
  const fetcher = new BlockFetcher('http://h/m/lab/', { fetchFn })
  const a = await fetcher.ofKind(runs.R1.completion, 'RunCompletion')
  await fetcher.get(runs.R1.completion)
  assert.equal(a.value.status, 'succeeded')
  assert.deepEqual(fetchFn.asked, [runs.R1.completion])
  assert.deepEqual(fetcher.verified, [runs.R1.completion])
})

test('bytes that do not hash to the address are refused before decoding', async () => {
  const { blocks, runs } = await buildMember()
  const bad = new Map(blocks)
  const bytes = Uint8Array.from(blocks.get(runs.R2.completion))
  bytes[bytes.length - 3] ^= 1
  bad.set(runs.R2.completion, bytes)
  const fetcher = new BlockFetcher('http://h/', { fetchFn: blockFetch(bad) })
  await assert.rejects(fetcher.get(runs.R2.completion), e => e.code === 'hash_mismatch' && e.cid === runs.R2.completion)
  assert.deepEqual(fetcher.verified, [])
})

test('a block not in this member is block_missing, and the wrong kind is schema_invalid', async () => {
  const { blocks, runs } = await buildMember()
  const fetcher = new BlockFetcher('http://h/', { fetchFn: blockFetch(blocks) })
  await assert.rejects(fetcher.get('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'), e => e.code === 'block_missing')
  await assert.rejects(fetcher.ofKind(runs.R1.manifest, 'RunCompletion'), e => e.code === 'schema_invalid')
})
```

```js
// web/test/store.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { resolveStore } from '../src/store.js'

const members = { members: [{ alias: 'lab', writable: true, base: 'm/lab/' }, { alias: 'priv', writable: false, base: 'm/priv/' }] }
const explore = async () => new Response(JSON.stringify(members), { headers: { 'Content-Type': 'application/json' } })
const nothing = async () => new Response('no', { status: 404 })

test('?store= wins, with a trailing slash added', async () => {
  const s = await resolveStore('http://127.0.0.1:9/stores/current/index.html?store=https://b.s3.amazonaws.com/x#/', { fetchFn: explore })
  assert.equal(s.base, 'https://b.s3.amazonaws.com/x/')
})

test('under explore, the first member or ?member=', async () => {
  assert.equal((await resolveStore('http://127.0.0.1:9/', { fetchFn: explore })).base, 'http://127.0.0.1:9/m/lab/')
  const s = await resolveStore('http://127.0.0.1:9/?member=priv', { fetchFn: explore })
  assert.deepEqual([s.base, s.member, s.members.length], ['http://127.0.0.1:9/m/priv/', 'priv', 2])
})

test('otherwise the page directory', async () => {
  assert.equal((await resolveStore('http://h/stores/current/index.html#/run/x', { fetchFn: nothing })).base, 'http://h/stores/current/')
})
```

```js
// web/test/model.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { MemorySource } from '../src/vfs/sources.js'
import { loadSqlite } from './helpers.mjs'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'

let vfsCount = 0
async function snapshotDb(bytes) {
  const sqlite3 = await loadSqlite()
  const name = `model-${++vfsCount}`
  installReadOnlyVfs(sqlite3, name).register('snap', new MemorySource(bytes))
  const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: name })
  return { query: async (sql, params = []) => db.selectObjects(sql, params).map(r => ({ ...r })), close: async () => db.close() }
}

async function open(overrides = {}) {
  const now = Date.now()
  const member = await buildMember({ now })
  const blocks = overrides.blocks ?? member.blocks
  const fetchFn = blockFetch(blocks)
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    listFn: async () => overrides.log ?? member.log,
    now: () => now,
  })
  return { explorer, member, fetchFn }
}

test('stale runs are the logged runs past the watermark the snapshot does not hold, each once (Review Focus 2)', async () => {
  const { explorer, member, fetchFn } = await open()
  assert.deepEqual(explorer.stale.map(s => s.cid).sort(), [member.runs.R2.completion, member.runs.R3.completion].sort())
  assert.equal(explorer.staleCount, 2)
  assert.ok(!fetchFn.asked.includes(member.runs.R1.completion), 'R1 is in the snapshot and must not be fetched')
  assert.equal(fetchFn.asked.filter(c => c === member.runs.R2.completion).length, 1)
  assert.equal(explorer.notice, false)
})

test('the run list merges the snapshot and the tail, newest first', async () => {
  const { explorer, member } = await open()
  const rows = await explorer.runsOfPipeline('demo')
  assert.deepEqual(rows.map(r => [r.completion_cid, r.source]), [
    [member.runs.R3.completion, 'tail'], [member.runs.R2.completion, 'tail'], [member.runs.R1.completion, 'snapshot']])
  assert.deepEqual((await explorer.pipelines()).map(p => [p.pipeline, p.runs]), [['demo', 3]])
})

test('query 1 answers from the snapshot and from stale closures', async () => {
  const { explorer, member } = await open()
  const rows = await explorer.producersOf(member.content.B)
  assert.deepEqual(rows.map(r => [r.completion_cid, r.item_cid, r.filename]).sort(), [
    [member.runs.R1.completion, member.item.B, 'B.bam'], [member.runs.R2.completion, member.item.B, 'B.bam']].sort())
})

test('query 2 prefers a newer successful stale run and ignores a failed one', async () => {
  const { explorer, member } = await open()
  assert.equal(await explorer.latestSuccessfulRun('demo'), member.runs.R2.completion)
  assert.equal(await explorer.latestSuccessfulRun('other'), null)
})

test('query 3 over a snapshot run is the SQL; over a stale run it is the closure, typed the same', async () => {
  const { explorer, member } = await open()
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R1.completion, [['sample', 'string', 'B']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['depth', 'float', '1.5']]), [member.item.A])
  assert.deepEqual(await items(member.runs.R2.completion, [['depth', 'float', '3.5']]), [member.item.C])
  assert.deepEqual(await items(member.runs.R2.completion, [['lane', 'int', '2']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R2.completion, []), [member.item.B, member.item.C].sort())
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'x'.repeat(2000)]]), [])
  assert.equal((await explorer.items(member.runs.R1.completion, 'aligned', [])).collection, member.runs.R1.collection)
  assert.equal((await explorer.items(member.runs.R2.completion, 'aligned', [])).collection, member.runs.R2.collection)
})

test('a tampered stale RunCompletion is an error on that run, not a listed run', async () => {
  const member = await buildMember()
  const bad = new Map(member.blocks)
  const bytes = Uint8Array.from(member.blocks.get(member.runs.R2.completion))
  bytes[bytes.length - 3] ^= 1
  bad.set(member.runs.R2.completion, bytes)
  const { explorer } = await open({ blocks: bad })
  const r2 = explorer.stale.find(s => s.cid === member.runs.R2.completion)
  assert.equal(r2.error.code, 'hash_mismatch')
  assert.equal(r2.row, null)
  assert.ok(!(await explorer.runsOfPipeline('demo')).some(r => r.completion_cid === member.runs.R2.completion))
})

test('past 20 stale runs the notice is on', async () => {
  const member = await buildMember()
  const missing = Array.from({ length: 21 }, (_, i) => entryName(Date.now() - (i + 1) * 1000, 'run', rawCid(`missing-${i}`).toString()))
  const { explorer } = await open({ log: [...member.log, ...missing] })
  assert.equal(explorer.staleCount, 23)
  assert.ok(explorer.stale.filter(s => s.error?.code === 'block_missing').length === 21)
  assert.equal(explorer.notice, true)
})
```

The 21 extra entries name CIDs the member does not hold: each is a stale run whose RunCompletion is `block_missing`, which still counts toward the notice.

Run: `cd nf-blocks/web && npm test`
Expected: FAIL, `Cannot find module '../src/storelog.js'` and the rest.

- [ ] **Step 5: Write `storelog.js`, `cid.js` and `typed.js`**

```js
// web/src/storelog.js
// The Store Log as the page reads it (spec section 3, DESIGN.md §15): names
// parse exactly as StoreLog.parse, and the tail re-reads a 10-minute overlap
// before the watermark, its floor clamped to the local clock.
import { OVERLAP_MILLIS } from './config.js'

const HORIZON = 9999999999999
const NAME = /^(\d{13})-(run|selection|claim)-(b[a-z2-7]{58})$/

export function parseEntry(name) {
  const m = NAME.exec(name)
  return m ? { name, rts: m[1], writtenAtMillis: HORIZON - Number(m[1]), kind: m[2], cid: m[3] } : null
}

export function floorOf(watermark, nowMillis) {
  const mark = watermark ? parseEntry(watermark) : null
  return mark ? Math.min(mark.writtenAtMillis, nowMillis) - OVERLAP_MILLIS : null
}

export function entriesSince(names, watermark, nowMillis = Date.now()) {
  const entries = names.map(parseEntry).filter(Boolean).sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
  const floor = floorOf(watermark, nowMillis)
  return floor === null ? entries : entries.filter(e => e.writtenAtMillis >= floor)
}

/** Every entry name the member's log/ listing gives, in the first form that answers (DESIGN.md §15). */
export async function listLog(base, { fetchFn = fetch, watermark = null, nowMillis = Date.now() } = {}) {
  let res = null
  try {
    res = await fetchFn(new URL('log/', base).href, { headers: { Accept: 'application/json' }, cache: 'no-store' })
  } catch {
    res = null
  }
  if (res?.ok) {
    const type = res.headers.get('Content-Type') ?? ''
    if (type.includes('application/json')) return (await res.json()).entries ?? []
    if (type.includes('text/html')) return hrefs(await res.text())
  }
  return listS3(base, fetchFn, floorOf(watermark, nowMillis))
}

function hrefs(html) {
  return [...html.matchAll(/href="([^"?#]+)"/g)]
    .map(m => decodeURIComponent(m[1]).replace(/\/$/, ''))
    .filter(name => parseEntry(name))
}

const XML = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'" }
const unxml = (text) => text.replace(/&(amp|lt|gt|quot|apos);/g, m => XML[m])

/** ListObjectsV2, virtual-hosted style. New entries sort first, so paging stops at the floor. */
async function listS3(base, fetchFn, floor) {
  const url = new URL(base)
  const prefix = `${url.pathname.replace(/^\//, '')}log/`
  const names = []
  let token = null
  for (;;) {
    const query = new URLSearchParams({ 'list-type': '2', prefix })
    if (token) query.set('continuation-token', token)
    let res
    try {
      res = await fetchFn(`${url.origin}/?${query}`, { cache: 'no-store' })
    } catch {
      return names
    }
    if (!res.ok) return names
    const xml = await res.text()
    if (!xml.includes('<ListBucketResult')) return names
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => unxml(m[1]).slice(prefix.length))
    names.push(...keys)
    const next = /<NextContinuationToken>([^<]+)<\/NextContinuationToken>/.exec(xml)
    const last = parseEntry(keys[keys.length - 1] ?? '')
    if (!next || (floor !== null && last && last.writtenAtMillis < floor)) return names
    token = unxml(next[1])
  }
}
```

```js
// web/src/cid.js
// Content identity as DESIGN.md §3 has it, with a pure-JavaScript SHA-256:
// crypto.subtle exists only in a secure context, and a page on plain http from
// a LAN host or an S3 website endpoint is not one.
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from '@noble/hashes/sha2.js'

export const DAG_CBOR = 0x71
export const RAW = 0x55
const SHA2_256 = 0x12

export const cidFor = (bytes, code) => CID.create(1, code, Digest.create(SHA2_256, sha256(bytes)))

/** True when the bytes hash to the CID they were fetched as, codec included. */
export function verifies(cidText, bytes) {
  const want = CID.parse(cidText)
  return want.multihash.code === SHA2_256 && cidFor(bytes, want.code).equals(want)
}

export const blockPath = (cidText) => `blocks/${cidText.slice(-2)}/${cidText}`
```

```js
// web/src/typed.js
// DAG-CBOR decoded with its floats kept apart from its integers. JavaScript
// has one number type, so 30.0 and 30 decode alike; the index types them
// 'float' and 'int' (MetadataView.scalar), and query 3 over a stale run must
// type them the same way.
import { Token, Tokenizer, Type, decode } from 'cborg'
import * as dagCbor from '@ipld/dag-cbor'

export class Float {
  constructor(value) { this.value = value }
}

export function typedDecode(bytes) {
  const inner = new Tokenizer(bytes, dagCbor.decodeOptions)
  const tokenizer = {
    done: () => inner.done(),
    pos: () => inner.pos(),
    next() {
      const token = inner.next()
      return Type.equals(token.type, Type.float) ? new Token(Type.float, new Float(token.value), token.encodedLength) : token
    },
  }
  return decode(bytes, { ...dagCbor.decodeOptions, tokenizer })
}
```

- [ ] **Step 6: Write `metadata.js`, `schema.js`, `blocks.js` and `store.js`**

```js
// web/src/metadata.js
// The metadata view and its item_attr rows, a port of MetadataView.groovy
// (DESIGN.md §12), so query 3 over a stale run matches what the index would.
import { sha256 } from '@noble/hashes/sha2.js'
import { Float } from './typed.js'

export const VALUE_CAP_BYTES = 1024

export class PredicateError extends Error {}

const isCid = (v) => v !== null && typeof v === 'object' && v['/'] === v.bytes && typeof v.toV1 === 'function'
const isMap = (v) => v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Uint8Array) && !(v instanceof Float) && !isCid(v)

export const isLeaf = (v) => isMap(v) && v.kind === 'Leaf'

export function metadataView(value) {
  if (isLeaf(value)) return null
  if (isMap(value)) return value
  if (Array.isArray(value)) for (const e of value) if (isMap(e) && !isLeaf(e)) return e
  return null
}

export function leavesOf(value, out = []) {
  if (isLeaf(value)) out.push(value)
  else if (Array.isArray(value)) value.forEach(v => leavesOf(v, out))
  else if (isMap(value)) Object.values(value).forEach(v => leavesOf(v, out))
  return out
}

/** Java's Double.toString, which is what Groovy's toString of a Double gives. */
export function javaDouble(x) {
  if (Object.is(x, -0)) return '-0.0'
  if (x === 0) return '0.0'
  const abs = Math.abs(x)
  if (abs >= 1e-3 && abs < 1e7) {
    const s = String(x)
    return s.includes('.') ? s : `${s}.0`
  }
  let [mantissa, exponent] = x.toExponential().split('e')
  // Java keeps at least two significant digits, rounding to the nearer one.
  if (!mantissa.includes('.')) [mantissa, exponent] = x.toExponential(1).split('e')
  return `${mantissa}E${Number(exponent)}`
}

function textRow(path, text) {
  const bytes = new TextEncoder().encode(text)
  if (bytes.length <= VALUE_CAP_BYTES) return { path, type: 'string', value: text, truncated: 0 }
  const hex = [...sha256(bytes)].map(b => b.toString(16).padStart(2, '0')).join('')
  return { path, type: 'string', value: `sha256:${hex}`, truncated: 1 }
}

function scalarRow(path, v) {
  if (v === null || v === undefined) return { path, type: 'null', value: null, truncated: 0 }
  if (typeof v === 'boolean') return { path, type: 'bool', value: String(v), truncated: 0 }
  if (v instanceof Float) return { path, type: 'float', value: javaDouble(v.value), truncated: 0 }
  if (typeof v === 'number' || typeof v === 'bigint') return { path, type: 'int', value: String(v), truncated: 0 }
  return textRow(path, String(v))
}

export function attrRows(view) {
  const rows = []
  const walk = (value, path) => {
    if (isLeaf(value)) return
    if (Array.isArray(value)) return value.forEach(v => walk(v, path))
    if (isMap(value)) return Object.entries(value).forEach(([k, v]) => walk(v, path ? `${path}.${k}` : k))
    rows.push(scalarRow(path, value))
  }
  if (isMap(view) && !isLeaf(view)) Object.entries(view).forEach(([k, v]) => walk(v, k))
  return rows
}

/** The row a typed predicate from the page must equal, as MetadataView.scalar would type the value. */
export function predicateRow(path, type, text) {
  switch (type) {
    case 'string': return textRow(path, text)
    case 'int':
      if (!/^-?\d+$/.test(text)) throw new PredicateError(`'${text}' is not a whole number`)
      return { path, type, value: BigInt(text).toString(), truncated: 0 }
    case 'float': {
      const x = Number(text)
      if (text.trim() === '' || !Number.isFinite(x)) throw new PredicateError(`'${text}' is not a finite number`)
      return { path, type, value: javaDouble(x), truncated: 0 }
    }
    case 'bool':
      if (text !== 'true' && text !== 'false') throw new PredicateError(`'${text}' is not true or false`)
      return { path, type, value: text, truncated: 0 }
    case 'null': return { path, type, value: null, truncated: 0 }
    default: throw new PredicateError(`unknown type '${type}'; use string, int, float, bool or null`)
  }
}

export function matches(rows, predicates) {
  return predicates.every(p => rows.some(r => r.truncated === 0 && r.path === p.path && r.type === p.type &&
    (p.value === null ? r.value === null : r.value === p.value)))
}
```

```js
// web/src/schema.js
// Every fetched block is checked against the IPLD Schema of DESIGN.md §6,
// generated into generated/schema.json by schema-gen.mjs (spec section 12).
import { create } from '@ipld/schema/typed.js'
import schema from './generated/schema.json' with { type: 'json' }

const block = create(schema, 'Block')
const leaf = create(schema, 'Leaf')

export const validBlock = (value) => block.toTyped(value) !== undefined
export const validLeaf = (value) => leaf.toTyped(value) !== undefined
```

```js
// web/src/blocks.js
// Blocks as the page may use them: fetched from <base>blocks/<xx>/<cid>, hashed
// against the address they were asked for, then decoded and schema-checked,
// or refused (spec section 5.3, DESIGN.md §15).
import * as dagCbor from '@ipld/dag-cbor'
import { blockPath, verifies } from './cid.js'
import { validBlock, validLeaf } from './schema.js'
import { leavesOf } from './metadata.js'

export class BlockError extends Error {
  constructor(code, cid, message) { super(message); this.code = code; this.cid = cid }
}

export class BlockFetcher {
  constructor(base, { fetchFn = (...a) => fetch(...a) } = {}) {
    this.base = base
    this.fetchFn = fetchFn
    this.cache = new Map()
    this.verified = []
    this.fetches = 0
  }

  get(cidText) {
    if (!this.cache.has(cidText))
      this.cache.set(cidText, this.load(cidText).catch((e) => { this.cache.delete(cidText); throw e }))
    return this.cache.get(cidText)
  }

  async ofKind(cidText, kind) {
    const block = await this.get(cidText)
    if (block.value?.kind !== kind)
      throw new BlockError('schema_invalid', cidText, `block ${cidText} is a ${block.value?.kind ?? 'value with no kind'}, not a ${kind}`)
    return block
  }

  async load(cidText) {
    let res
    try {
      res = await this.fetchFn(new URL(blockPath(cidText), this.base).href)
    } catch (e) {
      throw new BlockError('fetch_failed', cidText, `could not fetch block ${cidText}: ${e.message}`)
    }
    this.fetches++
    if (res.status === 404 || res.status === 403)
      throw new BlockError('block_missing', cidText, `block ${cidText} is not in this member; it may be held in another one`)
    if (!res.ok) throw new BlockError('fetch_failed', cidText, `block ${cidText}: HTTP ${res.status}`)
    const bytes = new Uint8Array(await res.arrayBuffer())
    if (!verifies(cidText, bytes))
      throw new BlockError('hash_mismatch', cidText, `block ${cidText} does not hash to its address; it was refused`)
    this.verified.push(cidText)
    let value
    try {
      value = dagCbor.decode(bytes)
    } catch (e) {
      throw new BlockError('schema_invalid', cidText, `block ${cidText} is not DAG-CBOR: ${e.message}`)
    }
    if (!validBlock(value) || (value.kind === 'OutputItem' && !leavesOf(value.value).every(validLeaf)))
      throw new BlockError('schema_invalid', cidText, `block ${cidText} does not match the ${value?.kind ?? 'Block'} schema (DESIGN.md §6)`)
    return { cid: cidText, bytes, value }
  }
}
```

```js
// web/src/store.js
// Which member the page reads (spec section 5.1, DESIGN.md §15).

const withSlash = (href) => (href.endsWith('/') ? href : `${href}/`)

export async function resolveStore(href, { fetchFn = (...a) => fetch(...a) } = {}) {
  const url = new URL(href)
  const store = url.searchParams.get('store')
  if (store) return { base: withSlash(new URL(store, url).href), members: null, member: null }
  try {
    const res = await fetchFn(new URL('members.json', url).href, { cache: 'no-store' })
    if (res.ok && (res.headers.get('Content-Type') ?? '').includes('application/json')) {
      const { members } = await res.json()
      const wanted = url.searchParams.get('member')
      const member = members.find(m => m.alias === wanted) ?? members[0]
      return { base: new URL(member.base, url).href, members, member: member.alias }
    }
  } catch {
    // Not served by explore.
  }
  return { base: new URL('.', url).href, members: null, member: null }
}
```

- [ ] **Step 7: Write `model.js`**

```js
// web/src/model.js
// The explorer's answers: the snapshot's rows through the plugin's own SQL,
// and, for runs newer than the snapshot, the same answers computed from their
// verified blocks (spec sections 5.3 to 5.5).
import SQL from './queries.json' with { type: 'json' }
import { CLOSURE_FETCH_NOTICE, SNAPSHOT_PATH, STALE_RUNS_NOTICE } from './config.js'
import { BlockError } from './blocks.js'
import { entriesSince } from './storelog.js'
import { typedDecode } from './typed.js'
import { attrRows, leavesOf, matches, metadataView, predicateRow } from './metadata.js'

const byNewest = (a, b) => (a.finished_at > b.finished_at ? -1 : a.finished_at < b.finished_at ? 1
  : a.completion_cid < b.completion_cid ? -1 : a.completion_cid > b.completion_cid ? 1 : 0)
const text = (cid) => (cid === null || cid === undefined ? null : cid.toString())

export class Explorer {
  constructor({ base, db, blocks, now }) {
    this.base = base
    this.db = db
    this.blocks = blocks
    this.now = now
    this.watermark = null
    this.stale = []
    this.closures = new Map()
    this.fetchesForQuery = 0
  }

  static async open({ base, openDb, blocks, listFn, now = () => Date.now() }) {
    const db = await openDb(new URL(SNAPSHOT_PATH, base).href)
    const explorer = new Explorer({ base, db, blocks, now })
    explorer.watermark = (await db.query(SQL.watermark))[0]?.value ?? null
    await explorer.refreshTail(listFn)
    return explorer
  }

  get staleCount() { return this.stale.length }

  get notice() { return this.stale.length > STALE_RUNS_NOTICE || this.fetchesForQuery > CLOSURE_FETCH_NOTICE }

  async refreshTail(listFn) {
    const names = await listFn(this.base, { watermark: this.watermark, nowMillis: this.now() })
    const runs = entriesSince(names, this.watermark, this.now()).filter(e => e.kind === 'run')
    const unique = [...new Map(runs.map(e => [e.cid, e])).values()]
    const known = unique.length === 0 ? new Set()
      : new Set((await this.db.query(SQL.runsKnown, [JSON.stringify(unique.map(e => e.cid))])).map(r => r.completion_cid))
    this.stale = await Promise.all(unique.filter(e => !known.has(e.cid)).map(e => this.staleRun(e)))
  }

  async staleRun(entry) {
    try {
      const completion = (await this.blocks.ofKind(entry.cid, 'RunCompletion')).value
      const manifest = (await this.blocks.ofKind(text(completion.run), 'RunManifest')).value
      const row = { completion_cid: entry.cid, manifest_cid: text(completion.run), pipeline: manifest.pipeline,
        run_name: manifest.run_name, status: completion.status, possibly_incomplete: completion.possibly_incomplete ? 1 : 0,
        finished_at: completion.finished_at, source: 'tail' }
      return { cid: entry.cid, entry, row, completion, manifest, error: null }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      return { cid: entry.cid, entry, row: null, completion: null, manifest: null, error: e }
    }
  }

  staleRow(cid) { return this.stale.find(s => s.cid === cid && s.row) ?? null }

  async pipelines() {
    const out = new Map((await this.db.query(SQL.pipelines)).map(r => [r.pipeline, { ...r }]))
    for (const s of this.stale.filter(s => s.row)) {
      const p = out.get(s.row.pipeline) ?? { pipeline: s.row.pipeline, runs: 0, latest: null }
      p.runs += 1
      if (!p.latest || s.row.finished_at > p.latest) p.latest = s.row.finished_at
      out.set(p.pipeline, p)
    }
    return [...out.values()].sort((a, b) => (a.pipeline < b.pipeline ? -1 : 1))
  }

  async runsOfPipeline(pipeline, { limit = 50, offset = 0 } = {}) {
    const snapshot = (await this.db.query(SQL.runsOfPipeline, [pipeline, limit, offset])).map(r => ({ ...r, source: 'snapshot' }))
    const tail = offset === 0 ? this.stale.filter(s => s.row?.pipeline === pipeline).map(s => s.row) : []
    return [...tail, ...snapshot].sort(byNewest)
  }

  async runRow(cid) {
    const [row] = await this.db.query(SQL.runByCompletion, [cid])
    return row ? { ...row, source: 'snapshot' } : this.staleRow(cid)?.row ?? null
  }

  async completionOf(cid) { return (await this.blocks.ofKind(cid, 'RunCompletion')).value }

  async run(cid) {
    const row = await this.runRow(cid)
    if (!row) throw new BlockError('not_found', cid, `no run ${cid} in this member's snapshot or Store Log tail`)
    const completion = await this.completionOf(cid)
    const collections = row.source === 'snapshot'
      ? (await this.db.query(SQL.collectionsOf, [cid])).map(r => ({ output: r.output_name, cid: r.collection_cid }))
      : await Promise.all(completion.collections.map(async (c) =>
        ({ output: (await this.blocks.ofKind(text(c), 'OutputCollection')).value.name, cid: text(c) })))
    return { row, completion, collections }
  }

  async collection(cid, { limit = 500, offset = 0 } = {}) {
    const [row] = await this.db.query(SQL.collectionByCid, [cid])
    if (row) {
      const items = (await this.db.query(SQL.collectionItems, [cid, limit, offset])).map(r => r.item_cid)
      return { cid, output: row.output_name, completion: row.completion_cid, items }
    }
    const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
    const completion = this.stale.find(s => s.completion?.collections.some(c => text(c) === cid))?.cid ?? null
    return { cid, output: block.name, completion, items: block.items.filter(Boolean).map(text).slice(offset, offset + limit) }
  }

  async item(collectionCid, itemCid) {
    const { value } = (await this.blocks.ofKind(itemCid, 'OutputItem')).value
    return { collection: collectionCid, cid: itemCid, value, view: metadataView(value), leaves: leavesOf(value) }
  }

  /** A stale run's collections and items, fetched once and turned into index-shaped rows. */
  async closure(stale, onProgress = () => {}) {
    if (this.closures.has(stale.cid)) return this.closures.get(stale.cid)
    const collections = await Promise.all(stale.completion.collections.map(async (c) => ({ cid: text(c), block: (await this.blocks.ofKind(text(c), 'OutputCollection')).value })))
    const total = collections.reduce((n, c) => n + c.block.items.filter(Boolean).length, 0)
    this.fetchesForQuery += total
    let done = 0
    const outputs = new Map()
    const producers = []
    for (const c of collections) {
      const items = []
      for (const link of c.block.items.filter(Boolean)) {
        const block = await this.blocks.ofKind(text(link), 'OutputItem')
        const typed = typedDecode(block.bytes).value
        items.push({ cid: text(link), rows: attrRows(metadataView(typed)) })
        for (const leaf of leavesOf(block.value.value))
          if (leaf.address) producers.push({ content_cid: text(leaf.address), item_cid: text(link), collection_cid: c.cid, completion_cid: stale.cid, filename: leaf.name })
        onProgress(++done, total)
      }
      outputs.set(c.block.name, { cid: c.cid, items })
    }
    const result = { outputs, producers }
    this.closures.set(stale.cid, result)
    return result
  }

  async producersOf(contentCid, onProgress) {
    this.fetchesForQuery = 0
    const rows = await this.db.query(SQL.producersOf, [contentCid])
    for (const s of this.stale.filter(s => s.row))
      rows.push(...(await this.closure(s, onProgress)).producers.filter(p => p.content_cid === contentCid))
    return rows
  }

  /**
   * Query 2 over the snapshot and the tail. With no stale candidate the SQL's
   * answer stands, so the snapshot is not read again for its finish time.
   */
  async latestSuccessfulRun(pipeline) {
    const [best] = await this.db.query(SQL.latestSuccessfulRun, [pipeline])
    const stale = this.stale.filter(s => s.row && s.row.pipeline === pipeline && s.row.status === 'succeeded' && s.row.possibly_incomplete === 0)
    if (stale.length === 0) return best?.completion_cid ?? null
    const candidates = stale.map(s => s.row)
    if (best) candidates.push(await this.runRow(best.completion_cid))
    return candidates.sort(byNewest)[0].completion_cid
  }

  /**
   * Query 3 for one run: the index's SQL for a snapshot run, the closure for a
   * stale one. Which one is known from memory, so the snapshot is read only by
   * the query itself (Gate assertion 2 counts every page). Returns the items
   * and the collection they came from.
   */
  async items(completionCid, output, predicates, onProgress) {
    this.fetchesForQuery = 0
    const rows = predicates.map(([path, type, value]) => predicateRow(path, type, value))
    if (rows.some(r => r.truncated)) return { items: [], collection: null }
    const stale = this.staleRow(completionCid)
    if (stale) {
      const { outputs } = await this.closure(stale, onProgress)
      const found = outputs.get(output)
      return { items: (found?.items ?? []).filter(i => matches(i.rows, rows)).map(i => i.cid).sort(), collection: found?.cid ?? null }
    }
    let sql = SQL.itemsBase
    const params = [completionCid, output]
    for (const r of rows) {
      params.push(r.path, r.type)
      if (r.value === null) sql += SQL.itemsPredicateNull
      else { sql += SQL.itemsPredicate; params.push(r.value) }
    }
    const found = await this.db.query(sql + SQL.itemsOrder, params)
    return { items: found.map(r => r.item_cid), collection: found[0]?.collection_cid ?? null }
  }
}
```

- [ ] **Step 8: Generate the schema in the build**

`web/build.mjs` in full after this step:

```js
// web/build.mjs
// Builds single-file pages into dist/: the app, the Worker source and
// sqlite3.wasm are inlined so a member can carry the page as one index.html
// (DESIGN.md §15). The IPLD Schema is regenerated from DESIGN.md first.
import * as esbuild from 'esbuild'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { extractSchema } from './schema-gen.mjs'

const here = new URL('.', import.meta.url)
const path = (p) => new URL(p, here).pathname

mkdirSync(path('src/generated'), { recursive: true })
writeFileSync(path('src/generated/schema.json'), JSON.stringify(extractSchema(readFileSync(path('../DESIGN.md'), 'utf8'))))

async function bundle(entry, define = {}) {
  const result = await esbuild.build({
    entryPoints: [path(entry)], bundle: true, format: 'iife', write: false, minify: true,
    target: 'es2022', define, logLevel: 'warning', legalComments: 'none',
  })
  return result.outputFiles[0].text
}

async function page(entry, template, out, define) {
  const js = await bundle(entry, define)
  const html = readFileSync(path(template), 'utf8')
    .replace('<!--APP-->', () => `<script>${js.replace(/<\/script/gi, '<\\/script')}</script>`)
  mkdirSync(path('dist'), { recursive: true })
  writeFileSync(path(`dist/${out}`), html)
  console.log(`dist/${out} ${html.length} bytes`)
}

const worker = await bundle('src/worker.js', { 'import.meta.url': JSON.stringify('https://nf-blocks.invalid/worker.js') })
const wasm = readFileSync(path('node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm'))
const inlined = { __WORKER_SOURCE__: JSON.stringify(worker), __WASM_BASE64__: JSON.stringify(wasm.toString('base64')) }

await page('bench/bench.js', 'bench/bench.html', 'bench.html', inlined)
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `cd nf-blocks/web && npm test` then `cd .. && ./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest`
Expected: PASS. If `schema.test.mjs` rejects a fixture block, the fixture is wrong, not the schema: DESIGN.md §6 is the contract.

- [ ] **Step 10: Commit**

```bash
cd nf-blocks
git add web/package.json web/package-lock.json web/schema-gen.mjs web/build.mjs web/src web/test \
        src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
git commit -m "feat(web): the page's model: Store Log tail, verified blocks, and index-shaped answers for stale runs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 11: The views, the router and the page

**Files:**
- Create: `web/src/index.html`, `web/src/html.js`, `web/src/views.js`, `web/src/app.js`
- Modify: `web/build.mjs` (build `dist/index.html`)
- Create: `web/test/write-fixture.mjs`
- Create: `gate/browser/page-smoke.mjs`

**Interfaces:**
- Consumes: everything in Task 10; `openSnapshot`, `createInlineWorker`, `inlineWasm` (Task 2).
- Produces: `web/dist/index.html`, which honours the DOM contract of DESIGN.md §15 exactly (Task 12 relies on nothing else); `node web/test/write-fixture.mjs <dir>` writes the Task 10 fixture as a member directory and prints its expected answers as JSON; `node gate/browser/page-smoke.mjs` drives the page over that fixture.

- [ ] **Step 1: Write the failing smoke test**

```js
// web/test/write-fixture.mjs
// Writes the Task 10 fixture as a member directory a static server can serve,
// with the built page beside the snapshot, and prints the expected answers.
//   node web/test/write-fixture.mjs <dir>
import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { buildMember } from './fixture.mjs'

const dir = process.argv[2]
const member = await buildMember()
for (const [cid, bytes] of member.blocks) {
  mkdirSync(join(dir, 'blocks', cid.slice(-2)), { recursive: true })
  writeFileSync(join(dir, 'blocks', cid.slice(-2), cid), bytes)
}
mkdirSync(join(dir, 'log'), { recursive: true })
for (const name of member.log) writeFileSync(join(dir, 'log', name), '')
mkdirSync(join(dir, 'index'), { recursive: true })
writeFileSync(join(dir, 'index', 'v2.sqlite'), member.snapshot)
copyFileSync(new URL('../dist/index.html', import.meta.url), join(dir, 'index.html'))
console.log(JSON.stringify({ runs: member.runs, item: member.item, content: member.content }))
```

```js
// gate/browser/page-smoke.mjs
// The page over the Task 10 fixture, in Playwright's pinned Chromium, served
// with Range and without. Exits non-zero on the first wrong answer.
//   node gate/browser/page-smoke.mjs
import { chromium } from 'playwright'
import { execFileSync, spawn } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import assert from 'node:assert/strict'

const repo = new URL('../../', import.meta.url).pathname
const dir = mkdtempSync(join(tmpdir(), 'nfb-page-'))
const expected = JSON.parse(execFileSync('node', [join(repo, 'web/test/write-fixture.mjs'), dir]).toString())
const { runs, item, content } = expected

function serve(port, extra = []) {
  const child = spawn('python3', [join(repo, 'gate/browser/serve.py'), dir, String(port), ...extra], { stdio: 'inherit' })
  return child
}

async function waitFor(url) {
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(url)).ok) return } catch {}
    await new Promise(r => setTimeout(r, 100))
  }
  throw new Error(`no server at ${url}`)
}

/** Navigates to a route and waits for its render to finish. */
async function open(page, url) {
  const before = Number(await page.evaluate(() => document.body?.dataset.render ?? 0).catch(() => 0))
  await page.goto(url)
  await page.waitForFunction((n) => Number(document.body.dataset.render) > n && document.body.dataset.state !== 'loading', before)
}

async function go(page, hash) {
  const before = Number(await page.evaluate(() => document.body.dataset.render))
  await page.evaluate((h) => { location.hash = h }, hash)
  await page.waitForFunction((n) => Number(document.body.dataset.render) > n && document.body.dataset.state !== 'loading', before)
}

const all = (page, selector, attrs) => page.$$eval(selector, (els, names) => els.map(e => Object.fromEntries(names.map(n => [n, e.dataset[n] ?? null]))), attrs)

const servers = [serve(8841), serve(8842, ['--no-range'])]
const browser = await chromium.launch()
try {
  await waitFor('http://127.0.0.1:8841/index.html')
  await waitFor('http://127.0.0.1:8842/index.html')
  const context = await browser.newContext()
  const page = await context.newPage()
  const errors = []
  page.on('pageerror', e => errors.push(String(e)))
  const fetched = new Set()
  context.on('requestfinished', r => { const m = /\/blocks\/..\/(b[a-z2-7]{58})$/.exec(r.url()); if (m) fetched.add(m[1]) })

  await open(page, 'http://127.0.0.1:8841/index.html#/')
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'ready')
  assert.equal(await page.$eval('#snapshot-mode', e => e.dataset.mode), 'range')
  assert.equal(await page.$eval('#stale', e => e.dataset.staleCount), '2')

  await go(page, '#/pipeline/demo')
  assert.deepEqual((await all(page, '[data-run]', ['run', 'source', 'status'])).map(r => [r.run, r.source, r.status]), [
    [runs.R3.completion, 'tail', 'failed'], [runs.R2.completion, 'tail', 'succeeded'], [runs.R1.completion, 'snapshot', 'succeeded']])

  await go(page, `#/content/${content.B}`)
  assert.deepEqual((await all(page, '[data-producer]', ['completion', 'item', 'filename'])).map(p => p.completion).sort(),
    [runs.R1.completion, runs.R2.completion].sort())

  await go(page, '#/latest/demo')
  assert.equal(await page.$eval('[data-latest]', e => e.dataset.latest), runs.R2.completion)

  await go(page, `#/items/${runs.R2.completion}/aligned?where=${encodeURIComponent(JSON.stringify([['sample', 'string', 'C']]))}`)
  assert.deepEqual(await page.$$eval('[data-item-result]', els => els.map(e => e.dataset.itemResult)), [item.C])

  await go(page, `#/items/${runs.R1.completion}/aligned?where=${encodeURIComponent(JSON.stringify([['lane', 'int', '1']]))}`)
  assert.deepEqual(await page.$$eval('[data-item-result]', els => els.map(e => e.dataset.itemResult)), [item.A])

  await go(page, `#/run/${runs.R1.completion}`)
  assert.deepEqual(await all(page, '[data-collection]', ['collection', 'output']), [{ collection: runs.R1.collection, output: 'aligned' }])

  await go(page, `#/item/${runs.R1.collection}/${item.A}`)
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'ready')

  await go(page, '#/no/such/view')
  assert.equal(await page.evaluate(() => document.body.dataset.state), 'error')
  assert.equal(await page.$eval('[data-error]', e => e.dataset.error), 'bad_route')

  const verified = new Set(await page.evaluate(() => window.__nfBlocks.verified))
  for (const cid of fetched) assert.ok(verified.has(cid), `fetched ${cid} but never verified it`)
  assert.deepEqual(errors, [])

  const plain = await context.newPage()
  await open(plain, `http://127.0.0.1:8842/index.html#/content/${content.A}`)
  assert.equal(await plain.$eval('#snapshot-mode', e => e.dataset.mode), 'whole')
  assert.equal((await all(plain, '[data-producer]', ['completion'])).length, 1)

  console.log('page smoke: ok')
} finally {
  await browser.close()
  for (const s of servers) s.kill()
}
```

Run: `cd nf-blocks/web && node build.mjs && cd .. && node gate/browser/page-smoke.mjs`
Expected: FAIL, `dist/index.html` does not exist (`write-fixture.mjs` cannot copy it).

- [ ] **Step 2: Write the page template and `html.js`**

```html
<!-- web/src/index.html -->
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>nf-blocks explorer</title>
<style>
  :root { color-scheme: light dark; --fg: #1d1d1f; --bg: #ffffff; --muted: #6e6e73; --line: #d2d2d7; --bad: #b3261e; --warn: #7a4f00; }
  @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --bg: #161617; --muted: #a1a1a6; --line: #3a3a3c; --bad: #ff8a80; --warn: #ffd180; } }
  body { font: 14px/1.45 system-ui, sans-serif; color: var(--fg); background: var(--bg); margin: 0 auto; max-width: 72rem; padding: 0 16px 4rem; }
  header { display: flex; flex-wrap: wrap; gap: .5rem 1.5rem; align-items: baseline; border-bottom: 1px solid var(--line); padding: .75rem 0; }
  h1 { font-size: 1.3rem; } h2 { font-size: 1.05rem; }
  code, .cid { font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; overflow-wrap: anywhere; }
  table { border-collapse: collapse; width: 100%; }
  td, th { text-align: left; padding: .25rem .5rem; border-bottom: 1px solid var(--line); vertical-align: top; }
  .muted { color: var(--muted); } [data-error] { color: var(--bad); } #stale[data-notice] { color: var(--warn); }
  a { color: inherit; } form { display: grid; gap: .5rem; margin: 1rem 0; } fieldset { border: 1px solid var(--line); }
  .scroll { overflow-x: auto; }
</style>
</head>
<body data-state="loading" data-render="0" data-route="open">
<header>
  <strong>nf-blocks explorer</strong>
  <span id="where" class="muted"></span>
  <span id="snapshot-mode" class="muted"></span>
  <span id="stale"></span>
  <nav id="members"></nav>
</header>
<main id="main"><p class="muted">Opening the snapshot...</p></main>
<!--APP-->
</body>
</html>
```

```js
// web/src/html.js
// A DOM builder small enough to read in one sitting.

export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag)
  for (const [key, value] of Object.entries(attrs ?? {})) {
    if (value === null || value === undefined || value === false) continue
    if (key.startsWith('on')) el.addEventListener(key.slice(2), value)
    else el.setAttribute(key, value === true ? '' : String(value))
  }
  for (const child of children.flat(Infinity))
    if (child !== null && child !== undefined && child !== false) el.append(child instanceof Node ? child : String(child))
  return el
}

export const link = (href, ...children) => h('a', { href }, ...children)
export const cid = (text) => h('code', { class: 'cid', title: text }, text)
```

- [ ] **Step 3: Write `views.js`**

```js
// web/src/views.js
// One function per route (DESIGN.md §15). Each returns a node; the data-*
// attributes are the contract the Gate reads, the rest is for people.
import { h, link, cid } from './html.js'

const enc = encodeURIComponent

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
}

function table(head, rows) {
  return h('div', { class: 'scroll' }, h('table', {}, h('tr', {}, head.map(t => h('th', {}, t))), rows))
}

const shown = (value) => (value === null ? 'null' : typeof value === 'object' && typeof value.toString === 'function' && value['/']
  ? value.toString() : typeof value === 'object' ? JSON.stringify(value, (k, v) => (v && v['/'] ? v.toString() : v)) : String(value))

export function idle() {
  return h('p', { class: 'muted' }, 'Snapshot open.')
}

export async function home(ex) {
  const pipelines = await ex.pipelines()
  const unreadable = ex.stale.filter(s => s.error)
  return h('section', {},
    h('h1', {}, 'Pipelines'),
    pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs in this member yet.')
      : table(['pipeline', 'runs', 'latest finish'], pipelines.map(p => h('tr', {},
        h('td', {}, link(`#/pipeline/${enc(p.pipeline)}`, p.pipeline)), h('td', {}, String(p.runs)), h('td', {}, p.latest ?? '')))),
    unreadable.length ? h('section', {}, h('h2', {}, 'Runs in the Store Log that could not be read'), unreadable.map(s => errorNode(s.error))) : null)
}

export async function pipeline(ex, name) {
  const rows = await ex.runsOfPipeline(name)
  const node = h('section', {},
    h('h1', {}, name),
    h('p', {}, link(`#/latest/${enc(name)}`, 'Latest successful run')),
    table(['run', 'status', 'finished', 'anomalies', 'from'], rows.map(r => h('tr', {
      'data-run': r.completion_cid, 'data-pipeline': r.pipeline, 'data-status': r.status, 'data-source': r.source },
    h('td', {}, link(`#/run/${r.completion_cid}`, r.run_name ?? r.completion_cid)),
    h('td', {}, r.possibly_incomplete ? `${r.status}, possibly incomplete` : r.status),
    h('td', {}, r.finished_at ?? ''),
    h('td', { 'data-anomalies-for': r.completion_cid, class: 'muted' }, '...'),
    h('td', { class: 'muted' }, r.source === 'tail' ? 'newer than the snapshot' : 'snapshot')))))
  // Anomaly counts are only in each RunCompletion; fill them in without holding up the view.
  for (const r of rows) {
    ex.completionOf(r.completion_cid).then(c => {
      const cell = node.querySelector(`[data-anomalies-for="${r.completion_cid}"]`)
      const a = c.anomalies
      if (cell) cell.textContent = Object.entries(a).filter(([, n]) => n).map(([k, n]) => `${k.replace('_', ' ')} ${n}`).join(', ') || 'none'
    }, e => {
      const cell = node.querySelector(`[data-anomalies-for="${r.completion_cid}"]`)
      if (cell) cell.replaceChildren(errorNode(e))
    })
  }
  return node
}

export async function run(ex, completionCid) {
  const { row, completion, collections } = await ex.run(completionCid)
  const a = completion.anomalies
  return h('section', {},
    h('h1', {}, row.run_name ?? 'run'), cid(completionCid),
    h('dl', {},
      h('dt', {}, 'pipeline'), h('dd', {}, link(`#/pipeline/${enc(row.pipeline)}`, row.pipeline)),
      h('dt', {}, 'status'), h('dd', {}, completion.possibly_incomplete ? `${completion.status}, possibly incomplete` : completion.status),
      h('dt', {}, 'finished'), h('dd', {}, completion.finished_at),
      h('dt', {}, 'anomalies'), h('dd', {}, `unresolvable ${a.unresolvable}, unaddressed ${a.unaddressed}, declined ${a.declined}, never published ${a.never_published}`),
      completion.error ? [h('dt', {}, 'error'), h('dd', {}, completion.error)] : null),
    h('h2', {}, 'Outputs'),
    h('ul', {}, collections.map(c => h('li', { 'data-collection': c.cid, 'data-output': c.output },
      link(`#/collection/${c.cid}`, c.output), ' ', link(`#/items/${completionCid}/${enc(c.output)}`, '(filter by metadata)')))))
}

export async function collection(ex, collectionCid) {
  const c = await ex.collection(collectionCid)
  return h('section', {},
    h('h1', {}, c.output), cid(collectionCid),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    h('ul', {}, c.items.map(i => h('li', {}, link(`#/item/${collectionCid}/${i}`, h('code', { class: 'cid' }, `cas://${collectionCid}/${i}`))))))
}

export async function item(ex, collectionCid, itemCid) {
  const it = await ex.item(collectionCid, itemCid)
  const view = it.view ?? {}
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString())]))
  return h('section', {},
    h('h1', {}, 'Item'), h('p', {}, cid(`cas://${collectionCid}/${itemCid}`)),
    h('h2', {}, 'Meta Map'),
    Object.keys(view).length ? table(['key', 'value'], Object.entries(view).filter(([, v]) => !(v && v.kind === 'Leaf'))
      .map(([k, v]) => h('tr', {}, h('td', {}, k), h('td', {}, shown(v))))) : h('p', { class: 'muted' }, 'none'),
    h('h2', {}, 'Files'),
    table(['name', 'size', 'content', 'produced by'], it.leaves.map(l => h('tr', {},
      h('td', {}, l.name ?? ''), h('td', {}, l.size ?? ''),
      h('td', {}, l.address ? link(`#/content/${l.address}`, cid(l.address.toString())) : h('span', { class: 'muted' }, l.reason)),
      h('td', {}, (producers.find(([leaf]) => leaf === l)?.[1] ?? []).map(p => h('div', {}, link(`#/run/${p.completion_cid}`, p.filename ?? p.completion_cid))))))))
}

export async function content(ex, contentCid, ctx) {
  const rows = await ex.producersOf(contentCid, ctx.progress)
  return h('section', {},
    h('h1', {}, 'Every producer of this content'), cid(contentCid),
    rows.length === 0 ? h('p', { class: 'muted' }, 'No run in this member produced it.')
      : table(['file', 'item', 'run'], rows.map(p => h('tr', {
        'data-producer': '', 'data-content': p.content_cid, 'data-item': p.item_cid, 'data-collection': p.collection_cid,
        'data-completion': p.completion_cid, 'data-filename': p.filename },
      h('td', {}, p.filename ?? ''), h('td', {}, link(`#/item/${p.collection_cid}/${p.item_cid}`, cid(p.item_cid))),
      h('td', {}, link(`#/run/${p.completion_cid}`, cid(p.completion_cid)))))))
}

export async function latest(ex, pipelineName) {
  const best = await ex.latestSuccessfulRun(pipelineName)
  return h('section', {},
    h('h1', {}, `Latest successful run of ${pipelineName}`),
    h('p', { 'data-latest': best ?? '' }, best ? link(`#/run/${best}`, cid(best)) : 'None: no run of this pipeline succeeded without being possibly incomplete.'))
}

export async function items(ex, completionCid, output, whereText, ctx) {
  let where
  try {
    where = JSON.parse(whereText)
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && w.length === 3)) throw new Error('not a list of [path, type, value]')
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[path, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
  let found
  try {
    found = await ex.items(completionCid, output, where, ctx.progress)
  } catch (e) {
    if (e.constructor?.name === 'PredicateError') throw Object.assign(e, { code: 'bad_predicate' })
    throw e
  }
  const { items: results, collection: collectionCid } = found
  return h('section', {},
    h('h1', {}, `Items of ${output}`), h('p', {}, 'In ', link(`#/run/${completionCid}`, 'this run'), ', where every condition below holds.'),
    whereForm(completionCid, output, where),
    h('p', {}, `${results.length} item${results.length === 1 ? '' : 's'}`),
    h('ul', {}, results.map(i => h('li', { 'data-item-result': i },
      collectionCid ? link(`#/item/${collectionCid}/${i}`, cid(i)) : cid(i)))))
}

const TYPES = ['string', 'int', 'float', 'bool', 'null']

function whereForm(completionCid, output, where) {
  const rows = h('div', {})
  const addRow = ([path, type, value] = ['', 'string', '']) => rows.append(h('fieldset', {},
    h('input', { name: 'path', value: path, placeholder: 'Meta Map key, e.g. sample or library.kit' }),
    h('select', { name: 'type' }, TYPES.map(t => h('option', { value: t, selected: t === type }, t))),
    h('input', { name: 'value', value: value ?? '', placeholder: 'value' })))
  ;(where.length ? where : [undefined]).forEach(addRow)
  return h('form', { onsubmit: (event) => {
    event.preventDefault()
    const next = [...rows.querySelectorAll('fieldset')].map(f => [f.elements.path.value.trim(), f.elements.type.value, f.elements.value.value])
      .filter(([path]) => path)
    location.hash = `#/items/${completionCid}/${enc(output)}?where=${enc(JSON.stringify(next))}`
  } }, rows, h('div', {}, h('button', { type: 'button', onclick: () => addRow() }, 'Add a condition'), ' ', h('button', { type: 'submit' }, 'Filter')))
}
```

The query views (`content`, `latest`, `items`) run only their own query against the snapshot: Gate assertion 2 counts every page they read. Anything else a view wants (a run's name, its outputs) is either already in memory or is a link to another view.

- [ ] **Step 4: Write `app.js` and build the page**

```js
// web/src/app.js
// Opens the member's snapshot and routes the location hash to a view
// (DESIGN.md §15). Sets body[data-state], [data-route] and [data-render] for
// every render, so a driver can wait for one to finish.
import { DEFAULT_CAP_BYTES } from './config.js'
import { openSnapshot } from './db.js'
import { createInlineWorker, inlineWasm } from './inline.js'
import { BlockFetcher } from './blocks.js'
import { listLog } from './storelog.js'
import { resolveStore } from './store.js'
import { Explorer } from './model.js'
import { h } from './html.js'
import * as views from './views.js'

const ROUTES = [
  ['home', /^#?\/?$/, (ex) => views.home(ex)],
  ['idle', /^#\/idle$/, () => views.idle()],
  ['pipeline', /^#\/pipeline\/([^/]+)$/, (ex, m) => views.pipeline(ex, decodeURIComponent(m[1]))],
  ['run', /^#\/run\/([^/]+)$/, (ex, m) => views.run(ex, m[1])],
  ['collection', /^#\/collection\/([^/]+)$/, (ex, m) => views.collection(ex, m[1])],
  ['item', /^#\/item\/([^/]+)\/([^/]+)$/, (ex, m) => views.item(ex, m[1], m[2])],
  ['content', /^#\/content\/([^/]+)$/, (ex, m, ctx) => views.content(ex, m[1], ctx)],
  ['latest', /^#\/latest\/([^/]+)$/, (ex, m) => views.latest(ex, decodeURIComponent(m[1]))],
  ['items', /^#\/items\/([^/]+)\/([^/?]+)(?:\?where=(.*))?$/, (ex, m, ctx) =>
    views.items(ex, m[1], decodeURIComponent(m[2]), m[3] ? decodeURIComponent(m[3]) : '[]', ctx)],
]

let explorer = null
let sequence = 0

function finish(state) {
  document.body.dataset.state = state
  document.body.dataset.render = String(Number(document.body.dataset.render) + 1)
}

function updateStale() {
  const el = document.getElementById('stale')
  el.dataset.staleCount = String(explorer.staleCount)
  const parts = [`${explorer.staleCount} run${explorer.staleCount === 1 ? '' : 's'} newer than the snapshot`]
  if (explorer.notice) {
    el.dataset.notice = ''
    parts.push('; to rewrite it run ', h('code', { 'data-command': '' }, 'nextflow plugin nf-blocks:snapshot'),
      ' or open the member through ', h('code', {}, 'nextflow plugin nf-blocks:explore'))
  } else {
    delete el.dataset.notice
  }
  el.replaceChildren(...parts)
}

async function render() {
  const mine = ++sequence
  const main = document.getElementById('main')
  const hash = location.hash || '#/'
  const route = ROUTES.find(([, pattern]) => pattern.test(hash))
  document.body.dataset.state = 'loading'
  document.body.dataset.route = route ? route[0] : 'unknown'
  const progress = h('p', { id: 'progress', class: 'muted' })
  main.replaceChildren(h('p', { class: 'muted' }, 'Loading...'), progress)
  const ctx = { progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() } }
  try {
    if (!route) throw Object.assign(new Error(`there is no view for ${hash}`), { code: 'bad_route' })
    const node = await route[2](explorer, hash.match(route[1]), ctx)
    if (mine !== sequence) return
    main.replaceChildren(node)
    updateStale()
    finish('ready')
  } catch (e) {
    if (mine !== sequence) return
    main.replaceChildren(views.errorNode(e))
    finish('error')
  }
}

function renderMembers(store) {
  const nav = document.getElementById('members')
  nav.replaceChildren(...store.members.map(m => {
    const url = new URL(location.href)
    url.searchParams.set('member', m.alias)
    return h('a', { href: url.href, 'aria-current': m.alias === store.member ? 'page' : null, style: 'margin-right: .75rem' },
      m.alias, m.writable ? ' (writable)' : '')
  }))
}

async function start() {
  window.__nfBlocks = { verified: [] }
  try {
    const store = await resolveStore(location.href)
    const blocks = new BlockFetcher(store.base)
    window.__nfBlocks.verified = blocks.verified
    document.getElementById('where').textContent = store.base
    if (store.members) renderMembers(store)
    const cap = Number(new URL(location.href).searchParams.get('cap')) || DEFAULT_CAP_BYTES
    let mode = null
    explorer = await Explorer.open({
      base: store.base, blocks, listFn: listLog,
      openDb: async (url) => {
        const db = await openSnapshot(url, { cap, wasm: inlineWasm(), createWorker: createInlineWorker })
        mode = db.mode
        return db
      },
    })
    const modeEl = document.getElementById('snapshot-mode')
    modeEl.dataset.mode = mode
    modeEl.textContent = mode === 'whole' ? 'snapshot downloaded whole: this server ignores Range' : 'snapshot read by range'
    updateStale()
  } catch (e) {
    document.getElementById('main').replaceChildren(views.errorNode(e))
    finish('error')
    return
  }
  window.addEventListener('hashchange', render)
  await render()
}

start()
```

Add to the end of `web/build.mjs`:

```js
await page('src/app.js', 'src/index.html', 'index.html', inlined)
```

- [ ] **Step 5: Run the smoke test to verify it passes**

Run: `cd nf-blocks/web && npm test && node build.mjs && cd .. && node gate/browser/page-smoke.mjs`
Expected: `page smoke: ok`. Then open the fixture by hand once (`python3 gate/browser/serve.py <dir> 8841`, a browser at `http://127.0.0.1:8841/index.html`), click through every view, and fix anything that reads wrong to a person; the smoke test cannot judge that.

Size guard: `wc -l web/src/*.js web/src/vfs/*.js` should stay under 1,500 lines in total. The `blocks` attempt reached 7,700 lines of UI (spec section 14); if this page is heading that way, stop and ask Rob.

- [ ] **Step 6: Commit**

```bash
cd nf-blocks
git add web/src/index.html web/src/html.js web/src/views.js web/src/app.js web/build.mjs \
        web/test/write-fixture.mjs gate/browser/page-smoke.mjs
git commit -m "feat(web): the explorer's views, router and self-contained page

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 12: Gate browser tier A, local part

**Files:**
- Create: `gate/browser_assert.py`, `gate/test_browser_assert.py`
- Create: `gate/browser/drive.mjs`, `gate/browser/tier.sh`
- Modify: `gate/gate.sh` (keep the snapshot after `fail`; run the tier; combined exit status)
- Modify: `gate/README.md` (the browser tier)

**Interfaces:**
- Consumes: `gate/assert.py`'s `Gate`, `Run`, `metadata_view`, `_leaves`, `_address_text`, `_latest_successful_from_store_log`, `PIPELINE_IDENTITY`; `cas.cid_of_file`; `gen_year.build`, `gen_year.ddl_of`; `year_params.params` (Task 1); the page's DOM contract (DESIGN.md §15); `nf-blocks:explore` (Task 7); the page in the store, written by runs (Tasks 5, 9).
- Produces:
  - `python3 gate/browser_assert.py prepare <GATE_ROOT>`: writes `$GATE_ROOT/browser/site/stores/{current,stale,tampered,year}/`, `expected.json` and `scenario.json`.
  - `python3 gate/browser_assert.py check <GATE_ROOT>`: prints one line per assertion A1 to A5, exit 1 on any FAIL.
  - `node gate/browser/drive.mjs <scenario.json> <observed.json> <name>=<base url> ...`: one cold browser context per step; writes `observed.json`.
  - A scenario step: `{ "id", "server", "path", "query"?, "hash", "idle"?, "head"? }`; `{name}` in `query` and `head` is replaced by that server's base URL.
  - An observed step: `{ "id", "state", "route", "mode", "stale", "command", "runs", "producers", "latest", "items", "errors", "verified", "requests": [{ "url", "method", "status", "range", "bytes", "phase" }], "head", "consoleErrors", "pageErrors" }`; `phase` is `open` or `query`.
  - `gate/gate.sh` exits non-zero when either tier fails; `GATE_SKIP_BROWSER=1` skips the browser tier.

- [ ] **Step 1: Write the failing tests for the checker**

```python
# gate/test_browser_assert.py
"""The browser tier's checks, over synthetic observations."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import browser_assert as B  # noqa: E402

CID = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"
YEAR = "http://127.0.0.1:1/stores/year/index/v2.sqlite"


def step(**kw):
    base = {"state": "ready", "requests": [], "verified": [], "errors": [], "runs": [], "producers": [],
            "items": [], "latest": None, "pageErrors": [], "consoleErrors": []}
    base.update(kw)
    return base


def reads(n, size, phase="query", url=YEAR):
    return [{"url": url, "method": "GET", "status": 206, "range": "bytes=0-4095", "bytes": size, "phase": phase}] * n


class QueryCostTest(unittest.TestCase):
    def test_counts_only_query_phase_reads_of_the_snapshot(self):
        s = step(requests=reads(3, 4096, "open") + reads(7, 4096) + reads(2, 99, url="http://127.0.0.1:1/x"))
        self.assertEqual(B.query_cost(s, "/stores/year/index/v2.sqlite"), (7, 7 * 4096))

    def test_limits(self):
        self.assertTrue(B.within((8, 65536), (8, 65536)))
        self.assertFalse(B.within((9, 1000), (8, 65536)))
        self.assertFalse(B.within((2, 65537), (8, 65536)))


class VerifiedTest(unittest.TestCase):
    def test_every_fetched_block_must_be_verified(self):
        fetched = {"url": "http://h/stores/current/blocks/%s/%s" % (CID[-2:], CID), "method": "GET",
                   "status": 200, "range": None, "bytes": 1, "phase": "query"}
        self.assertEqual(B.unverified(step(requests=[fetched], verified=[CID])), [])
        self.assertEqual(B.unverified(step(requests=[fetched], verified=[])), [CID])

    def test_a_404_block_is_not_a_fetched_block(self):
        missing = {"url": "http://h/blocks/%s/%s" % (CID[-2:], CID), "method": "GET", "status": 404,
                   "range": None, "bytes": 0, "phase": "query"}
        self.assertEqual(B.unverified(step(requests=[missing])), [])


class ProducersTest(unittest.TestCase):
    def test_producer_rows_compare_as_sets_of_tuples(self):
        row = {"content": "c", "item": "i", "collection": "k", "completion": "r", "filename": "A.bam"}
        self.assertEqual(B.producer_set([row, row]), {("c", "i", "k", "r", "A.bam")})


if __name__ == "__main__":
    unittest.main()
```

Run: `cd nf-blocks && python3 -m unittest gate.test_browser_assert -v` (or `discover -s gate`)
Expected: FAIL, `No module named 'browser_assert'`.

- [ ] **Step 2: Write `gate/browser_assert.py`**

```python
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
    fetched = set()
    for r in step.get("requests", []):
        parts = r.get("url", "").split("?")[0].split("/")
        if len(parts) >= 3 and parts[-3] == "blocks" and r.get("status") == 200 and cas.is_cid(parts[-1]):
            fetched.add(parts[-1])
    return sorted(fetched - set(step.get("verified") or []))


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
```

Run: `cd nf-blocks && python3 -m unittest gate.test_browser_assert -v`
Expected: PASS.

- [ ] **Step 3: Write the driver `gate/browser/drive.mjs`**

```js
// gate/browser/drive.mjs
// Plays scenario.json in Playwright's pinned Chromium: one cold context per
// step, waiting on the page's body[data-render] counter, recording every
// request from the browser's own network events and reading back only the
// DOM contract of DESIGN.md §15. It judges nothing; browser_assert.py does.
//   node gate/browser/drive.mjs <scenario.json> <observed.json> <name>=<base url> ...
import { chromium } from 'playwright'
import { readFileSync, writeFileSync } from 'node:fs'

const [scenarioFile, observedFile, ...named] = process.argv.slice(2)
const servers = Object.fromEntries(named.map((arg) => { const i = arg.indexOf('='); return [arg.slice(0, i), arg.slice(i + 1).replace(/\/$/, '')] }))
const fill = (text) => (text ?? '').replace(/\{(\w+)\}/g, (_, name) => servers[name] ?? `{${name}}`)
const { steps } = JSON.parse(readFileSync(scenarioFile, 'utf8'))

async function waitRender(page, after) {
  await page.waitForFunction((n) => Number(document.body?.dataset.render ?? 0) > n && document.body.dataset.state !== 'loading',
    after, { timeout: 180_000 })
}

const settle = () => new Promise((r) => setTimeout(r, 250))

const EXTRACT = () => {
  const data = (selector, names) => [...document.querySelectorAll(selector)]
    .map((e) => Object.fromEntries(names.map((n) => [n, e.dataset[n] ?? null])))
  return {
    state: document.body.dataset.state,
    route: document.body.dataset.route,
    mode: document.getElementById('snapshot-mode')?.dataset.mode ?? null,
    stale: document.getElementById('stale')?.dataset.staleCount ?? null,
    command: !!document.querySelector('#stale [data-command]'),
    runs: data('[data-run]', ['run', 'pipeline', 'status', 'source']),
    producers: data('[data-producer]', ['content', 'item', 'collection', 'completion', 'filename']),
    latest: document.querySelector('[data-latest]')?.dataset.latest ?? null,
    items: [...document.querySelectorAll('[data-item-result]')].map((e) => e.dataset.itemResult),
    errors: data('[data-error]', ['error', 'cid']),
    verified: [...(window.__nfBlocks?.verified ?? [])],
  }
}

const browser = await chromium.launch()
const observed = []
for (const step of steps) {
  const context = await browser.newContext()
  const requests = []
  const consoleErrors = []
  const pageErrors = []
  let phase = 'open'
  context.on('requestfinished', async (request) => {
    const current = phase
    const response = await request.response()
    const headers = response ? await response.allHeaders() : {}
    requests.push({ url: request.url(), method: request.method(), status: response?.status() ?? null,
      range: request.headers().range ?? null, bytes: Number(headers['content-length'] || 0), phase: current })
  })
  context.on('requestfailed', (request) => {
    requests.push({ url: request.url(), method: request.method(), status: null, range: request.headers().range ?? null,
      bytes: 0, phase, failure: request.failure()?.errorText ?? null })
  })
  const page = await context.newPage()
  page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()) })
  page.on('pageerror', (e) => pageErrors.push(String(e)))
  const record = { id: step.id }
  try {
    const url = `${servers[step.server]}/${step.path}${fill(step.query)}${step.idle ? '#/idle' : step.hash}`
    await page.goto(url)
    await waitRender(page, 0)
    if (step.idle && (await page.evaluate(() => document.body.dataset.state)) === 'ready') {
      await settle()
      phase = 'query'
      const before = Number(await page.evaluate(() => document.body.dataset.render))
      await page.evaluate((h) => { location.hash = h }, step.hash)
      await waitRender(page, before)
    }
    await settle()
    Object.assign(record, await page.evaluate(EXTRACT))
    if (step.head) {
      record.head = await page.evaluate((u) => fetch(u, { method: 'HEAD', cache: 'no-store' })
        .then((r) => ({ status: r.status, length: r.headers.get('content-length') }))
        .catch((e) => ({ error: String(e) })), fill(step.head))
    }
  } catch (e) {
    record.state = 'driver_error'
    record.driverError = String(e)
  }
  Object.assign(record, { requests, consoleErrors, pageErrors })
  observed.push(record)
  console.log(`${step.id}: ${record.state}`)
  await context.close()
}
await browser.close()
writeFileSync(observedFile, JSON.stringify({ steps: observed }, null, 1))
```

- [ ] **Step 4: Write `gate/browser/tier.sh` and wire it into `gate.sh`**

```bash
#!/usr/bin/env bash
#
# Gate browser tier A, local part (block explorer spec section 1.3): the page
# in Playwright's pinned Chromium against the store this Gate run wrote, served
# three ways: a static server with Range, one without, and nf-blocks:explore.
#
#   gate/browser/tier.sh <GATE_ROOT>        # NEXTFLOW and NXF_PLUGINS_DIR from gate.sh
#
# Servers and driver run in this one script: the Bash sandbox forbids a later
# command connecting to a server an earlier command started.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
NEXTFLOW="${NEXTFLOW:-nextflow}"
B="$GATE_ROOT/browser"
mkdir -p "$B"
rm -f "$B/observed.json"

echo "--- browser tier: setup"
( cd "$REPO/gate/browser" && npm ci --no-audit --no-fund && npx playwright install chromium ) > "$B/setup.log" 2>&1 || {
    echo "browser tier: npm or Playwright setup failed, see $B/setup.log" >&2
    exit 2
}
python3 "$REPO/gate/browser_assert.py" prepare "$GATE_ROOT"

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
P_RANGE=$(free_port); P_PLAIN=$(free_port); P_EXPLORE=$(free_port)

python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_RANGE" > "$B/serve-range.log" 2>&1 & S1=$!
python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_PLAIN" --no-range > "$B/serve-plain.log" 2>&1 & S2=$!
( cd "$GATE_ROOT/pipeline-a" && \
  NXF_OFFLINE=true exec "$NEXTFLOW" -c "$REPO/gate/gate.config" plugin nf-blocks:explore --port "$P_EXPLORE" ) \
  > "$B/explore.log" 2>&1 & S3=$!
stop() { kill -INT "$S3" 2> /dev/null || true; kill "$S1" "$S2" 2> /dev/null || true; wait 2> /dev/null || true; }
trap stop EXIT

for _ in $(seq 240); do
    grep -q 'nf-blocks explorer:' "$B/explore.log" 2> /dev/null && break
    kill -0 "$S3" 2> /dev/null || { echo "browser tier: explore exited, see $B/explore.log" >&2; exit 2; }
    sleep 0.5
done
grep -q 'nf-blocks explorer:' "$B/explore.log" || { echo "browser tier: explore never listened, see $B/explore.log" >&2; exit 2; }
for port in "$P_RANGE" "$P_PLAIN"; do
    for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
done

echo "--- browser tier: driving $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["steps"]))' "$B/scenario.json") steps"
node "$REPO/gate/browser/drive.mjs" "$B/scenario.json" "$B/observed.json" \
     "range=http://127.0.0.1:$P_RANGE" "plain=http://127.0.0.1:$P_PLAIN" "explore=http://127.0.0.1:$P_EXPLORE" \
     > "$B/drive.log" 2>&1 || echo "browser tier: the driver failed, see $B/drive.log" >&2
echo
python3 "$REPO/gate/browser_assert.py" check "$GATE_ROOT"
```

In `gate/gate.sh`:

1. Add `"${GATE_ROOT:?}/browser" "${GATE_ROOT:?}/snapshot-after-fail.sqlite"` to the `rm -rf` of a reused `GATE_ROOT`.
2. Add `node` to the preconditions loop (`for tool in python3 unzip find node; do`).
3. After `run "$GATE_ROOT/pipeline-a" fail --fail`, add:

```bash
# Browser assertion A3 needs a snapshot two runs stale: keep the one `fail` wrote.
cp "$GATE_STORE"/index/v*.sqlite "$GATE_ROOT/snapshot-after-fail.sqlite" 2> /dev/null \
    || echo "    no Index Snapshot after fail; browser assertion A3 will fail"
```

4. Replace the last line (`python3 "$REPO/gate/assert.py" "$GATE_ROOT"`) with:

```bash
lineage=0
python3 "$REPO/gate/assert.py" "$GATE_ROOT" || lineage=$?
browser=0
if [[ -z "${GATE_SKIP_BROWSER:-}" ]]; then
    echo
    NEXTFLOW="$NEXTFLOW" "$REPO/gate/browser/tier.sh" "$GATE_ROOT" || browser=$?
fi
# Either tier failing fails the Gate.
exit $(( lineage != 0 ? lineage : browser ))
```

Make `tier.sh` executable: `chmod +x gate/browser/tier.sh`.

- [ ] **Step 5: Run the whole Gate**

Run: `cd nf-blocks && GATE_ROOT="$SCRATCH/g" make gate`
Expected: the lineage table as before (11 PASS, 0 FAIL, 6 SKIP), then

```
PASS  A1  the page answers the three load-bearing queries   ...
PASS  A2  year-scale cold queries within the request and byte limits   year snapshot, cold: producers N req / N B, ...
PASS  A3  a snapshot two runs stale   ...
PASS  A4  every fetched block is hash-verified in the browser   ...
PASS  A5  whole-file fallback without Range, refused over the cap   ...

browser tier A (local): 5 PASS, 0 FAIL
```

The first run generates the year snapshot (minutes); later runs reuse it. When a line fails, read `$SCRATCH/g/browser/observed.json` for that step and `drive.log`; fix the page or the plugin, never the expected answers, which come from the Gate's own reading of the store. If assertion 4 of the lineage tier flakes, rerun (known, by design).

Run it a second time with the same `GATE_ROOT` to show the tier is repeatable.

- [ ] **Step 6: Document the tier**

Add a section "The browser tier (block explorer)" to `gate/README.md`, after "What gate.sh does": what `tier.sh` serves and how (the table of the three servers and four stores), the assertion list A1 to A5 with the spec's wording, where the year snapshot is cached (`GATE_YEAR_CACHE`, default `$TMPDIR/nf-blocks-gate-year`), `GATE_SKIP_BROWSER=1`, and that Node plus `npm ci` in `gate/browser` are needed. Keep it under 60 lines.

- [ ] **Step 7: Commit**

```bash
cd nf-blocks
git add gate/browser_assert.py gate/test_browser_assert.py gate/browser/drive.mjs gate/browser/tier.sh \
        gate/gate.sh gate/README.md
git commit -m "test(gate): browser tier A, local part: the explorer in pinned Chromium against the Gate's own store

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Gate browser tier A, cloud part

Needs Rob: it runs with his `scidev` SSO session and creates, then deletes, two throwaway buckets.

**Files:**
- Create: `gate/cloud/s3tier.py` (from `prototype-snapshot-http/s3real.py`)
- Create: `gate/cloud/cloud.sh`
- Modify: `gate/browser_assert.py` (`cloud-prepare`, `cloud-check`)
- Modify: `Makefile` (`gate-cloud`)
- Modify: `gate/README.md`

**Interfaces:**
- Consumes: a `GATE_ROOT` that has passed the local tier (`browser/site/stores/current`, `browser/expected.json`); `drive.mjs` with `head` steps (Task 12); `explore` over an S3 member (Task 8).
- Produces: `gate/cloud/cloud.sh <GATE_ROOT>` printing A6 and A7; `make gate-cloud GATE_ROOT=<root>`; `python3 gate/cloud/s3tier.py setup <site dir> <region>` printing `<public bucket> <private bucket>`, and `teardown <bucket> <region>`.

- [ ] **Step 1: Write `gate/cloud/s3tier.py`**

```python
#!/usr/bin/env python3
"""Throwaway buckets for the cloud browser tier (block explorer spec section 1.3).

    python3 gate/cloud/s3tier.py setup <site dir> <region>    # prints "<public> <private>"
    python3 gate/cloud/s3tier.py teardown <bucket> <region>

The public bucket carries the policy and CORS rule of spec section 6 and holds
the Gate's store at its root and the year snapshot under year/. The private
bucket holds the same store with every public access blocked. Both are tagged
and deleted by teardown; setup deletes what it made if it fails half way.
Needs boto3 and an AWS session (AWS_PROFILE, default scidev).
"""
import json
import os
import sys
import time
import uuid

import boto3
from boto3.s3.transfer import TransferConfig

SESSION = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev"))
TAGS = [{"Key": "purpose", "Value": "nf-blocks Gate cloud browser tier - throwaway"},
        {"Key": "owner", "Value": os.environ.get("GATE_OWNER", "rob.syme")}]
CORS = {"CORSRules": [{"AllowedOrigins": ["*"], "AllowedMethods": ["GET", "HEAD"], "AllowedHeaders": ["range"],
                       "ExposeHeaders": ["Content-Range", "Content-Length", "Accept-Ranges", "ETag"],
                       "MaxAgeSeconds": 3000}]}


def create(s3, region, public):
    bucket = "nf-blocks-gate-%s-%s" % ("pub" if public else "priv", uuid.uuid4().hex[:10])
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": TAGS})
    if not public:
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        return bucket
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": False, "RestrictPublicBuckets": False})
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=CORS)
    policy = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::%s/*" % bucket},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::%s" % bucket}]}
    for _ in range(10):
        try:
            s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
            return bucket
        except s3.exceptions.ClientError as exc:
            print("policy retry: %s" % exc.response["Error"]["Code"], file=sys.stderr)
            time.sleep(3)
    raise SystemExit("could not set the public bucket's policy")


def upload(s3, bucket, root, prefix=""):
    config = TransferConfig(max_concurrency=16)
    count = total = 0
    started = time.time()
    for dirpath, _dirs, files in os.walk(root, followlinks=True):
        for name in files:
            full = os.path.join(dirpath, name)
            key = prefix + os.path.relpath(full, root).replace(os.sep, "/")
            s3.upload_file(full, bucket, key, Config=config)
            count += 1
            total += os.path.getsize(full)
    print("uploaded %d objects, %d MB to %s in %.0f s" % (count, total // 1_000_000, bucket, time.time() - started), file=sys.stderr)


def delete(region, bucket):
    b = SESSION.resource("s3", region_name=region).Bucket(bucket)
    b.objects.all().delete()
    b.delete()
    print("deleted %s" % bucket, file=sys.stderr)


def setup(site, region):
    s3 = SESSION.client("s3", region_name=region)
    made = []
    try:
        public = create(s3, region, True)
        made.append(public)
        private = create(s3, region, False)
        made.append(private)
        store = os.path.join(site, "stores", "current")
        upload(s3, public, store)
        upload(s3, public, os.path.join(site, "stores", "year"), prefix="year/")
        upload(s3, private, store)
    except BaseException:
        for bucket in made:
            delete(region, bucket)
        raise
    print("%s %s" % (public, private))


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] not in ("setup", "teardown"):
        sys.stderr.write(__doc__)
        sys.exit(2)
    if sys.argv[1] == "setup":
        setup(sys.argv[2], sys.argv[3])
    else:
        delete(sys.argv[3], sys.argv[2])
```

- [ ] **Step 2: Add `cloud-prepare` and `cloud-check` to `browser_assert.py`**

```python
def cloud_prepare(root, public, private, region):
    out = os.path.join(root, "browser")
    with open(os.path.join(out, "expected.json")) as fh:
        expected = json.load(fh)
    pub = "https://%s.s3.%s.amazonaws.com" % (public, region)
    priv = "https://%s.s3.%s.amazonaws.com" % (private, region)
    p = expected["year"]["params"]
    page = "stores/current/index.html"
    steps = [
        {"id": "A6.producers", "server": "local", "path": page, "query": "?store=%s/" % pub,
         "hash": "#/content/%s" % expected["content"], "head": "%s/%s" % (pub, SNAPSHOT)},
        {"id": "A6.home", "server": "local", "path": page, "query": "?store=%s/" % pub, "hash": "#/"},
        {"id": "A6.year.producers", "server": "local", "path": page, "query": "?store=%s/year/" % pub, "idle": True,
         "hash": "#/content/%s" % p["producersOf"][0]},
        {"id": "A6.year.items", "server": "local", "path": page, "query": "?store=%s/year/" % pub, "idle": True,
         "hash": "#/items/%s/%s%s" % (p["itemsWhere"][0], p["itemsWhere"][1], where("sample", p["itemsWhere"][4]))},
        {"id": "A7.explore", "server": "explore", "path": "", "query": "?member=priv",
         "hash": "#/content/%s" % expected["content"]},
        {"id": "A7.direct", "server": "local", "path": page, "query": "?store=%s/" % priv, "hash": "#/idle"},
    ]
    with open(os.path.join(out, "cloud-scenario.json"), "w") as fh:
        json.dump({"steps": steps}, fh, indent=1)
    return 0


def cloud_check(root):
    out = os.path.join(root, "browser")
    with open(os.path.join(out, "expected.json")) as fh:
        expected = json.load(fh)
    with open(os.path.join(out, "cloud-observed.json")) as fh:
        observed = {s["id"]: s for s in json.load(fh)["steps"]}
    want = {tuple(t) for t in expected["producers"]}
    results = []

    problems = []
    s = observed["A6.producers"]
    if producer_set(s["producers"]) != want:
        problems.append("producers-of from the public bucket gave %d rows, expected %d" % (len(s["producers"]), len(want)))
    if (s.get("head") or {}).get("status") != 200:
        problems.append("anonymous HEAD from the page answered %s" % s.get("head"))
    s3 = [r for step in observed.values() if step["id"].startswith("A6") for r in step["requests"] if "amazonaws.com" in r["url"]]
    if not any(r.get("status") == 206 for r in s3):
        problems.append("no ranged GET answered 206")
    if not any("list-type=2" in r["url"] and r.get("status") == 200 for r in s3):
        problems.append("no ListObjectsV2 answered 200")
    cors = [e for step in observed.values() if step["id"].startswith("A6") for e in step["consoleErrors"] if "CORS" in e]
    if cors:
        problems.append("CORS errors: %s" % cors[:2])
    costs = ["%s %d req / %d B" % (k, *query_cost(observed[k], "/year/" + SNAPSHOT)) for k in ("A6.year.producers", "A6.year.items")]
    if not within(query_cost(observed["A6.year.producers"], "/year/" + SNAPSHOT), POINT_LIMIT) or \
            not within(query_cost(observed["A6.year.items"], "/year/" + SNAPSHOT), QUERY3_LIMIT):
        problems.append("year-scale limits exceeded on real S3: %s" % costs)
    results.append((FAIL if problems else PASS, 6, "real S3 from a page on another origin",
                    "; ".join(problems) or "anonymous HEAD, ranged GET and ListObjectsV2 succeed with no CORS errors; " + ", ".join(costs)))

    problems = []
    if producer_set(observed["A7.explore"]["producers"]) != want:
        problems.append("through nf-blocks:explore the private member answered %d producers, expected %d"
                        % (len(observed["A7.explore"]["producers"]), len(want)))
    if observed["A7.direct"]["state"] != "error":
        problems.append("the private bucket was readable directly (state %s)" % observed["A7.direct"]["state"])
    results.append((FAIL if problems else PASS, 7, "a private bucket through explore only",
                    "; ".join(problems) or "browsable through nf-blocks:explore with the user's credentials, refused directly"))

    for status, number, title, message in results:
        print("%-4s  A%d  %-38s  %s" % (status, number, title, message))
    failures = sum(1 for r in results if r[0] == FAIL)
    print("\nbrowser tier A (cloud): %d PASS, %d FAIL" % (len(results) - failures, failures))
    return 1 if failures else 0
```

and extend `main`:

```python
def main(argv):
    if len(argv) == 3 and argv[1] in ("prepare", "check", "cloud-check"):
        return {"prepare": prepare, "check": check, "cloud-check": cloud_check}[argv[1]](argv[2])
    if len(argv) == 6 and argv[1] == "cloud-prepare":
        return cloud_prepare(argv[2], argv[3], argv[4], argv[5])
    sys.stderr.write(__doc__)
    return 2
```

Update the module docstring's usage lines to list all four subcommands.

- [ ] **Step 3: Write `gate/cloud/cloud.sh` and the Make target**

```bash
#!/usr/bin/env bash
#
# Gate browser tier A, cloud part (block explorer spec section 1.3), on demand:
# before each milestone is accepted and whenever the HTTP reader or the serving
# code changes. Creates two throwaway buckets in the scidev account, drives the
# page against them, and deletes them, also on failure.
#
#   GATE_PYTHON=<python with boto3> AWS_PROFILE=scidev gate/cloud/cloud.sh <GATE_ROOT>
#
# <GATE_ROOT> must have passed the local tier (it reuses browser/site and expected.json).

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
B="$GATE_ROOT/browser"
PY="${GATE_PYTHON:-python3}"
REGION="${GATE_CLOUD_REGION:-ca-central-1}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export AWS_PROFILE="${AWS_PROFILE:-scidev}"
export NXF_PLUGINS_DIR="$GATE_ROOT/plugins" XDG_CACHE_HOME="$GATE_ROOT/cache" GATE_STORE="$GATE_ROOT/store"

[[ -f "$B/expected.json" ]] || { echo "cloud tier: run the local tier on $GATE_ROOT first" >&2; exit 2; }
"$PY" -c 'import boto3' 2> /dev/null || { echo "cloud tier: $PY has no boto3; point GATE_PYTHON at a venv with it" >&2; exit 2; }

read -r PUBLIC PRIVATE < <("$PY" "$REPO/gate/cloud/s3tier.py" setup "$B/site" "$REGION") || true
# setup deletes whatever it made if it fails, so there is nothing to clean up here.
[[ -n "${PRIVATE:-}" ]] || { echo "cloud tier: bucket setup failed" >&2; exit 1; }
PORT_LOCAL=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
PORT_EXPLORE=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
cleanup() {
    # Never `kill 0`: that signals this whole process group.
    [[ -n "${EXPLORE:-}" ]] && kill -INT "$EXPLORE" 2> /dev/null || true
    [[ -n "${LOCAL:-}" ]] && kill "$LOCAL" 2> /dev/null || true
    "$PY" "$REPO/gate/cloud/s3tier.py" teardown "$PUBLIC" "$REGION" || true
    "$PY" "$REPO/gate/cloud/s3tier.py" teardown "$PRIVATE" "$REGION" || true
}
trap cleanup EXIT
echo "buckets: $PUBLIC (public), $PRIVATE (private), $REGION"

cat > "$B/cloud.config" <<EOF
includeConfig '$REPO/gate/gate.config'
cas.stores.priv.location = 's3://$PRIVATE/'
EOF

python3 "$REPO/gate/browser/serve.py" "$B/site" "$PORT_LOCAL" > "$B/cloud-serve.log" 2>&1 & LOCAL=$!
( cd "$GATE_ROOT/pipeline-a" && NXF_OFFLINE=true exec "$NEXTFLOW" -c "$B/cloud.config" plugin nf-blocks:explore --port "$PORT_EXPLORE" ) \
    > "$B/cloud-explore.log" 2>&1 & EXPLORE=$!
for _ in $(seq 240); do grep -q 'nf-blocks explorer:' "$B/cloud-explore.log" 2> /dev/null && break; sleep 0.5; done
grep -q 'nf-blocks explorer:' "$B/cloud-explore.log" || { echo "cloud tier: explore never listened, see $B/cloud-explore.log" >&2; exit 2; }
sleep 5    # a new bucket policy can take a few seconds to apply

python3 "$REPO/gate/browser_assert.py" cloud-prepare "$GATE_ROOT" "$PUBLIC" "$PRIVATE" "$REGION"
node "$REPO/gate/browser/drive.mjs" "$B/cloud-scenario.json" "$B/cloud-observed.json" \
     "local=http://127.0.0.1:$PORT_LOCAL" "explore=http://127.0.0.1:$PORT_EXPLORE" > "$B/cloud-drive.log" 2>&1 || true
python3 "$REPO/gate/browser_assert.py" cloud-check "$GATE_ROOT"
```

`chmod +x gate/cloud/cloud.sh`. In `Makefile`, after `gate:`:

```make
# The Gate's cloud browser tier (gate/README.md): throwaway buckets in the scidev
# account, a human's AWS SSO session. Run after `make gate` with the same GATE_ROOT.
.PHONY: gate-cloud
gate-cloud:
	./gate/cloud/cloud.sh "$(GATE_ROOT)"
```

- [ ] **Step 4: Run it with Rob**

This step needs Rob at the keyboard: his SSO session (`aws sso login --profile scidev` through the venv's boto3, since `/usr/local/bin/aws` does not run on this machine) and his go-ahead to create two buckets.

```bash
cd nf-blocks
GATE_ROOT="$SCRATCH/g" make gate          # the local tier must pass first
GATE_PYTHON="$SCRATCH/venv/bin/python" GATE_ROOT="$SCRATCH/g" make gate-cloud
```

Expected: `PASS A6 ...` with the year-scale costs on real S3 alongside, `PASS A7 ...`, and both buckets reported deleted. Afterwards, list the account's buckets with the venv's boto3 and confirm no `nf-blocks-gate-` bucket remains. If A7's explore log shows `Unable to load credentials` or an `SdkClientException` about the HTTP client, the context class loader wrapping in `S3MemberFiles` (Task 8) is missing a call path; fix it there.

- [ ] **Step 5: Document and commit**

Add "The cloud browser tier" to `gate/README.md`: when to run it, what it creates (two tagged buckets, deleted on exit), `GATE_PYTHON`, `AWS_PROFILE`, `GATE_CLOUD_REGION` (default `ca-central-1`), and the teardown check. Under 30 lines.

```bash
cd nf-blocks
git add gate/cloud gate/browser_assert.py Makefile gate/README.md
git commit -m "test(gate): browser tier A, cloud part: real S3 CORS and ranges, and a private bucket through explore

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Milestone 1 acceptance

**Files:**
- Modify: `DESIGN.md` §15 (status line), `README.md` (the two verbs, one paragraph)

- [ ] **Step 1: Everything green, twice**

```bash
cd nf-blocks
./gradlew clean check                      # unit tests, memory bound, dependencyCheck, webTest
node gate/browser/page-smoke.mjs
GATE_ROOT="$SCRATCH/g" make gate           # lineage tier and browser tier A (local)
GATE_ROOT="$SCRATCH/g" make gate           # again, same root
```

Expected: `check` PASS; `page smoke: ok`; both Gate runs end with the lineage table at 11 PASS, 0 FAIL, 6 SKIP and `browser tier A (local): 5 PASS, 0 FAIL`. The cloud tier result from Task 13 is the one on record for A6 and A7; rerun it if the reader or the server changed since.

- [ ] **Step 2: Record the milestone**

At the top of DESIGN.md §15 add: `*Status 2026-MM-DD: milestone 1 accepted; Gate browser tier A 7 of 7 (local 5, cloud 2).*` with the real date and numbers. In `README.md`, add a short "Browsing a store" paragraph: `nextflow plugin nf-blocks:explore`, `nextflow plugin nf-blocks:snapshot`, opening `<member>/index.html` directly, and the bucket policy and CORS rule pointer to DESIGN.md §15.

- [ ] **Step 3: Whole-branch review, then merge**

Run the final review the execution method calls for (a fresh reviewer over `git diff main...feat/explorer-m1` against this plan and the spec). Fix what it finds, rerun Step 1, then merge to `main` as the previous plans were merged.

- [ ] **Step 4: Ask Rob about the prototype branch**

Everything milestone 1 needed from `prototype/index-snapshot-http` has moved (`gen_year.py`, `serve.py`, `s3real.py`, the reference numbers now in `web/bench/RESULTS.md`, the schema check now in `web/test/schema.test.mjs`). Ask Rob whether to delete the branch; do not delete it without his answer. Milestone 2 (Selections) gets its own plan.

```bash
cd nf-blocks
git add DESIGN.md README.md
git commit -m "docs: block explorer milestone 1 accepted

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
