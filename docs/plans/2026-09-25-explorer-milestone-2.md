# Block Explorer Milestone 2 (Selections) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every task is bound by `DESIGN.md`; when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets fixed.

**Goal:** Selections and the Claims that name and delete them, written only by one Groovy builder, readable by `fromStore(selection:)`, exportable as a samplesheet, and composed, renamed, deleted and undone from the explorer page, accepted by Gate browser tier B (assertions 8 to 13).

**Architecture:** Wave 0 adds the Claim block and its index rows in `robsyme.cas.core` with no explorer dependency. Wave 1 adds the Selection block, its index rows and Item Occurrence URIs. Wave 2 adds one builder, `robsyme.cas.core.Put`, that turns a DAG-JSON request into a normalised, validated block, writes it to the writable member, appends its Store Log entry and ingests it; `nf-blocks:put` and `POST /api/put` on `nf-blocks:explore` both call it. Wave 3 reads Selections back (`fromStore(selection:)`, samplesheet export). Wave 4 teaches the page to list Selections and Claims from the snapshot and the Store Log tail, and to compose, rename, delete and undo through `POST /api/put`. Wave 5 is Gate browser tier B, all local, which recomputes every address with its own Python encoder.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, `org.xerial:sqlite-jdbc` 3.50.3.0, Nextflow 26.04.6, Node 24.21.0, esbuild 0.28.2, `@ipld/dag-cbor` 10.0.2, `@ipld/dag-json` 11.0.1 (new), `@ipld/schema` 7.0.12, `multiformats` 14.0.5, Playwright 1.63.0, Python 3 stdlib for the Gate.

**Spec:** `../.scratch/block-explorer/spec.md` sections 1.2, 1.3 (tier B), 1.4, 3, 4, 5.5, 5.6, 7, 8, 9, 10, 11 and 12. Read sections 1, 7, 8, 9 and 11 before starting. Milestone 1 is merged at `2bf7ae2`; its plan is `docs/plans/2026-09-25-explorer-milestone-1.md` and its contract is `DESIGN.md` §15. The tickets behind the decisions are `../.scratch/block-explorer/issues/01-post-gate-dependencies.md`, `02-selection-schema-and-root-log.md` and `05-write-endpoint-and-cli-verb.md`.

**Branch:** `git switch -c feat/explorer-m2 main` in `nf-blocks/`.

## Decisions this plan makes where the spec is silent

Each is written into `DESIGN.md` §16 by Task 17 (and into the section each task amends as it lands), so a reviewer can reject one there.

1. **The samplesheet export is served by `nf-blocks:explore`** at `GET /api/samplesheet/<selection cid>.csv` and `.json`, and linked from the page's Selection view when the page is served by `explore`. No new verb: spec section 2's verb table stays at three.
2. **Snapshot runs are those the member's Store Log announced, plus those found in its blocks.** The spec's rule starts from `log_entry` rows with `member = M`; a run found by the one-time block scan of a store written before the Store Log (DESIGN §12) has no log entry, so the snapshot also keeps runs whose `run.member = M`. This fixes milestone 1's decision 1 (a RunCompletion in two members now appears in both snapshots) without dropping pre-log runs.
3. **Four indexes beyond spec section 11**, so the page's queries pass `ExplorerQueriesTest`'s no-scan guard: `log_entry(cid, member)` unique, `log_entry(kind, written_at DESC)`, `claim(subject_cid)`, `claim_current(subject_cid, attribute)`. `log_entry.written_at` is ISO-8601 UTC with milliseconds, and `log_entry` keeps the earliest entry per `(cid, member)` ("first seen in this member").
4. **`claim.value` and `claim_current.value`** hold a string value as itself, null as NULL, and any other value as its DAG-JSON text.
5. **Current state** (spec section 8), one set of rules implemented twice, in `ClaimState.groovy` and `web/src/claims.js`, both pinned by `web/test/fixtures/claim-vectors.json`: the current Claims of a subject are those no Claim of that subject supersedes; they group by `attribute`, with `delete` and `del` (attribute null) forming the deletion group; a group with more than one current Claim is conflicted. Names are the values of the current `set name` Claims, in claim-address order. Deletion is `deleted` when the deletion group's one current Claim is a `delete`, `conflicted` when the group has more than one, `none` otherwise. Only `deleted` hides.
6. **Claim request rules**, checked by `Put` before writing: `set` needs an attribute and a non-null value; `delete` has neither; `del` has no value and supersedes at least one Claim; a `name` value is a non-empty string of at most 256 characters; `add` is refused (out of the slice, spec section 8); every superseded address must be a Claim about the same subject (`wrong_kind` otherwise), present and not already superseded (`stale_supersedes`).
7. **Error code `invalid`**, a ninth code beside spec section 9.4's eight, for a request that is not DAG-JSON or does not have the block's shape (a missing or mistyped field, an unknown key, a `/` map that is not a link or bytes, `verb: "add"`). Transport refusals stay plain text, as milestone 1's `403` is: no or wrong launch token `403`, foreign `Host` or `Origin` `403`, a content type other than JSON `415`, a body over 2 MiB `413`.
8. **`derived_from` is accepted as links or as bytes** in a request and always stored as bytes (binary CIDs), sorted by the CID's string form.
9. **Idempotence first.** `Put` computes the address before any semantic check. A block the writable member already holds is success with `"written": false`, without re-checking `supersedes`, the clock or membership, and reuses the block's existing Store Log entry (one is appended only if none exists). A retried request therefore never fails with `stale_supersedes` or `clock_skew` against itself (spec section 8).
10. **`dry_run` on the endpoint is the query parameter `?dry_run=true`**, since it is not block content. `exists` is true when any member of the composition holds the block.
11. **`put` reads a path.** `-` reads stdin when the verb is called in-process, but Nextflow 26.04.6's launcher refuses a bare `-` before any plugin runs (measured 2026-09-25: `Unknown option: - -- Check the available commands and options and syntax with 'help'`), so from the shell stdin is `/dev/stdin`. A bare `--dry-run` reaches the verb as `--dry-run`, `true` (`Launcher.normalizeArgs` appends `=true`).
12. **The launch token** is 26 base32 characters, printed as `?token=<t>` in the URL `explore` prints, and sent by the page as the header `X-NF-Blocks-Token`. Only `POST` needs it.
13. **A run's delete Claim names its RunCompletion.** Query 2's SQL excludes runs with a current `delete` Claim (conflicted included) and `Index.latestSuccessfulRun` warns about each conflicted one it leaves out. The page's run lists hide runs whose deletion is `deleted`, filtering the page of rows it shows and saying how many it hid; the UI offers no run deletion (spec section 5.6).
14. **The page sees its own write through the tail**: after a write it re-lists the writable member's Store Log. Composing works from any member's view; the Selection is written to the writable member, and after saving the page opens it there.
15. **The page keeps the tray of picked items in `sessionStorage`** (per tab, wrapped in `try`), so picks survive switching member; the page works without it.
16. **`fromStore(selection:)` and the samplesheet resolve nesting through the index's recursive CTE** (spec section 11), after catch-up, and fail naming any nested Selection the index does not hold rather than emitting a partial set. Items come out sorted by CID string. `run`, `output` and `where` are refused beside `selection`.
17. **An Item Occurrence without a leaf name is a directory of the item's leaves by leaf name**; a leaf name that two leaves of one item share is refused, naming both positions. Publish-path traversal of a collection root stays unsupported, and the error names both readings (DESIGN §7).
18. **Samplesheet specifics:** one row per distinct item, sorted by item CID. Meta Map columns in first-seen order, then file columns in first-seen order. A file column is named by the leaf's structural path in the item: tuple index or record key, nested positions joined by `.` (`1` for `[meta, bam]`, `1.0` for the first of `[meta, [chunks...]]`). An addressed raw leaf is `cas://<cid>/<name>`, a directory leaf `cas://<manifest cid>` (the form `fromStore` restores, DESIGN §13), any other leaf blank. In CSV a list value is its JSON text and scalars are the index's text forms (`MetadataView.scalar`). JSON is an array of objects: the Meta Map's keys with nesting, types and absence kept, then the file columns.
19. **Output Collection rows now carry `kind = 'output'` and `asserted_by`**; Selection rows `kind = 'selection'`, `completion_cid` and `output_name` NULL.
20. **Three of milestone 1's four parked minors are fixed where their code is touched**: dead `Explorer.closures` in Task 13, the pager's label past the end in Task 14, `Cache-Control: no-cache` documented and set on uploaded snapshots in Task 17. The fourth, "stale worker error code", is not specific enough to act on; Task 17 asks Rob.

## Global Constraints

- Released dependencies only. Every npm dependency pinned to an exact version in `package.json`; `dependencyCheck` fails on a lockfile entry not resolved from `https://registry.npmjs.org/` (spec section 1.4).
- Nextflow facts are read from tag `v26.04.6` in `/Users/robsyme/dev/github.com/nextflow-io/nextflow`; the plugin builds against published artifacts only.
- One encoder: blocks are built in Groovy. The page and the Gate may decode and hash; the page never encodes DAG-CBOR (spec section 1.4). The Gate's independent Python encoder exists only to recompute addresses.
- The page never holds credentials. The launch token is not a credential for any store; it only admits a `POST` to the loopback server that printed it.
- One user, one machine: `explore` binds `InetAddress.getLoopbackAddress()` only.
- Block kinds a client may build: `Selection` and `Claim` (spec section 9.1).
- Selection identity: `asserted_by`, `members`, `derived_from`, no timestamp. `members` sorted ascending by the member address's string form, each address once, each `via` and `derived_from` sorted likewise (spec section 7.1).
- An empty Selection is refused (`empty`); an encoded block over `1048576` bytes is refused (`too_large`) (spec sections 7.2, 9.4).
- Claim timestamp: ISO-8601 UTC, milliseconds (`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`), refused with `clock_skew` if more than `600000` ms from the server's clock (spec section 8).
- Error body: DAG-JSON `{"error": <code>, "message": <text>, "at": <JSON pointer into the request>}`; `400`, or `409` for `not_writable`. Codes: `not_found`, `wrong_kind`, `not_in_via`, `empty`, `stale_supersedes`, `clock_skew`, `too_large`, `not_writable`, and `invalid` (decision 7). The CLI prints the same body and exits 1.
- Response: `{"address": {"/": <cid>}, "block": <canonical block as DAG-JSON>, "entry": <Store Log entry name>, "written": <bool>}`; dry run `{"address", "exists", "names"}` (spec section 9.3).
- Accepted content types for `POST /api/put`: `application/json` and `application/vnd.ipld.dag-json`, parameters such as `charset` ignored (spec section 9.5).
- DAG-JSON as `@ipld/dag-json` 11.0.1 writes it (measured 2026-09-25): map keys sorted by UTF-8 bytes (not length first), links `{"/": "<cid>"}`, bytes `{"/": {"bytes": "<standard base64, no padding>"}}`; a decoder accepts padded base64. Unlike `@ipld/dag-json`, ours refuses any other map with a `/` key (DESIGN §4) and duplicate keys.
- Index schema version `3`; snapshot file `index/v3.sqlite`.
- Store Log overlap `600000` ms, floor clamped to the local clock, exactly `StoreLog.entriesSince` (spec section 3).
- Gate limits on the year snapshot stay: point queries at most `8` requests and `65536` bytes, query 3 at most `50` and `524288` (spec section 1.3 assertion 2). Query 2 gains a `NOT EXISTS` over `claim_current`; Task 4 re-measures it.
- Failures in derived structures (index, Store Log, snapshot, page) log at warn and never abort a run (DESIGN §0 rule 3). A `put` whose Store Log append fails after the block is written is a failure of the request (exit 1, `500`): the block is safe, and a retry appends the entry (decision 9).
- Gate assertions never trust the plugin or the page (DESIGN §0 rule 6).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

The five inputs the spec implies, no task's main tests exercise, and a user is most likely to meet. Each names the task whose test pins it.

1. **A retried request after the state it saw has moved on**: the same rename replayed after another rename superseded its `supersedes`, or replayed eleven minutes later. It succeeds with `written: false` and the same address, never `stale_supersedes` or `clock_skew`. Pinned in Task 8 (`PutTest`).
2. **The same item picked twice**: once from one run's collection and once from another's, or once as an occurrence URI and once as an `{"item": ...}` map, in either order. One member, `via` the union, the same address for every order. Pinned in Task 5 (`SelectionTest`) and Task 8 (`PutTest`).
3. **A name or Meta Map value with a comma, a quote, a newline or non-ASCII text**: it round-trips through DAG-JSON and the page, and the CSV cell is quoted per RFC 4180. Pinned in Task 2 (`DagJsonTest`) and Task 12 (`SamplesheetTest`).
4. **A hostile or broken body**: not UTF-8, not JSON, `{"/": 5}`, a duplicate key, 10,000 nested arrays, an unknown field. `400 invalid` with `at`, never a `500` or a stack overflow. Pinned in Task 2 (`DagJsonTest`) and Task 10 (`ExploreWriteTest`).
5. **A nested Selection whose block the composition does not hold**: `put` refuses it with `not_found` at its member's path, `fromStore` fails naming it, and the page shows the member as held in another member rather than failing the view. Pinned in Task 6 (`IndexSelectionsTest`), Task 8 (`PutTest`), Task 11 (`CasExtensionTest`) and Task 13 (`selections.test.mjs`).

## File Structure

```
nf-blocks/
  DESIGN.md                                    §5, §6, §7, §12, §13, §15 amended; new §16 Selections (Tasks 1-17)
  src/main/groovy/robsyme/cas/
    core/Index.groovy                          schema 3, log_entry, claims, selections, CTE (Tasks 1, 4, 6)
    core/IndexSnapshot.groovy                  milestone 2 row rule (Tasks 1, 4, 6)
    core/DagJson.groovy                        strict DAG-JSON codec (Task 2)
    core/Cid.groovy                            fromBytes (Task 5)
    core/Records.groovy                        CLAIM, SELECTION kind names (Tasks 3, 5)
    core/Claim.groovy                          the Claim block (Task 3)
    core/ClaimState.groovy                     current state of one subject (Task 3)
    core/ClaimCurrent.groovy                   claim_current rows from ClaimState, over any connection (Task 4)
    core/Selection.groovy                      the Selection block (Task 5)
    core/ItemOccurrence.groovy                 cas://<collection>/<item>[/<leaf>] (Task 5)
    core/PutError.groovy, PutResult.groovy     the builder's outcomes (Task 8)
    core/Put.groovy                            the one builder (Task 8)
    core/Samplesheet.groovy                    CSV and JSON export (Task 12)
    nio/CasFileSystemProvider.groovy           Item Occurrence resolution (Task 7)
    cli/CasCommands.groovy                     put verb (Task 9)
    explore/ExploreServer.groovy               POST /api/put, GET /api/samplesheet, token (Tasks 10, 12)
    explore/ExploreCommand.groovy              builds the Put, prints the token URL (Task 10)
    ext/CasExtension.groovy                    fromStore(selection:) (Task 11)
  src/test/groovy/robsyme/cas/...              IndexLogEntryTest, DagJsonTest, ClaimTest, ClaimStateTest,
                                               IndexClaimsTest, SelectionTest, ItemOccurrenceTest,
                                               IndexSelectionsTest, CasOccurrenceTest, PutTest,
                                               CasCommandsPutTest, ExploreWriteTest, SamplesheetTest;
                                               CasExtensionTest and IndexSnapshotTest extended
  web/
    package.json, package-lock.json            + @ipld/dag-json 11.0.1 (Task 2)
    src/config.js                              SCHEMA_VERSION 3 (Task 1)
    src/queries.json                           claims, selections, log_entry queries (Tasks 4, 13)
    src/claims.js                              current state, vector-pinned (Task 13)
    src/write.js                               the page's DAG-JSON requests and the POST (Task 13)
    src/tray.js                                picked items (Task 13)
    src/model.js                               tail kinds, selections, claims, hidden runs (Task 13)
    src/views.js, app.js, index.html           selections, compose, rename, delete, undo (Task 14)
    test/fixtures/claim-vectors.json           shared by ClaimStateTest and claims.test.mjs (Task 3)
    test/fixtures/dag-json-vectors.json        written by @ipld/dag-json, read by DagJsonTest (Task 2)
    test/dag-json-vectors.mjs                  regenerates the file above (Task 2)
    test/*.test.mjs                            claims, write, tray, selections (Task 13)
  gate/
    gen_year.py, browser_assert.py             schema 3 (Task 1)
    dagjson.py, test_dagjson.py                the Gate's DAG-JSON reader and expected blocks (Task 15)
    selection/main.nf, nextflow.config         the selection consumer pipeline (Task 15)
    browser/drive.mjs                          actions, two-page steps, POST bodies (Task 16)
    browser/tier_b.sh, browser_b_assert.py     tier B, assertions 8-13 (Task 16)
    test_browser_b_assert.py                   tier B's checks against hand-made observations (Task 16)
    cloud/s3tier.py                            Cache-Control on uploads (Task 17)
    gate.sh                                    runs tier B after tier A (Task 16)
```

## Waves

| Wave | Tasks | Depends on |
|---|---|---|
| 0, Claims slice | 1 schema 3 and `log_entry`; 2 DAG-JSON; 3 Claim and ClaimState; 4 Claims in the index | milestone 1 |
| 1, Selection kind | 5 Selection and Item Occurrence; 6 Selections in the index; 7 occurrence URIs in the provider | wave 0 |
| 2, Writing | 8 `Put`; 9 `nf-blocks:put`; 10 `POST /api/put` | waves 0-1 |
| 3, Consumption | 11 `fromStore(selection:)`; 12 samplesheet | waves 1-2 |
| 4, The page | 13 page core; 14 page views | waves 2-3 |
| 5, Gate tier B | 15 Gate encoder and selection consumer; 16 tier B; 17 docs, minors, acceptance | waves 0-4 |

Tasks within a wave touch disjoint files except `Index.groovy` (Tasks 1, 4, 6 run in order) and `DESIGN.md` (each task edits only its own section). Run them in number order.

Every Groovy task ends with `./gradlew test` green; every web task with `cd web && npm test` green; Tasks 1, 4, 10, 16 and 17 with `make gate` (Gate 11/0/6 or better, browser tier A 5/5, and from Task 16 tier B 6/6). Run the Gate with `GATE_ROOT` in the session scratchpad, and rerun once before debugging a Gate assertion 4 failure: it is intermittent by design (see `gate/README.md`).

---

### Task 1: Index schema 3 and the `log_entry` table

Bumps the index to spec section 11's schema in one step, so the later tasks fill tables that already exist. Fills `log_entry` from every Store Log entry, records `kind` and `asserted_by` on Output Collection rows, and switches the snapshot to decision 2's row rule.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`SCHEMA_VERSION`, `ddl()`, `ingestCollection`, `catchUp`, `retryMissingRuns`, `rebuild`; new `recordLogEntry`, `firstLogEntry`, `isoMillis`, `ingestLogged`)
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy` (`COPY_RUNS`, `COPY_CLOSURE`, `buildAndVacuum`)
- Modify: `web/src/config.js`, `web/test/fixtures/schema.sql`, `web/test/fixture.mjs`, `web/test/model.test.mjs`
- Modify: `gate/gen_year.py`, `gate/browser_assert.py`, `gate/test_gen_year.py`, `gate/test_browser_assert.py`, `gate/browser/page-smoke.mjs`, `gate/README.md`, `web/bench/run.sh`
- Modify: every test that names `v2.sqlite` (list in Step 7)
- Modify: `DESIGN.md` §5 (layout line), §12 (schema), §15 (snapshot rows)
- Create: `src/test/groovy/robsyme/cas/core/IndexLogEntryTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy`

**Interfaces:**
- Consumes: `StoreLog`, `StoreLogEntry(name, reverseTs, writtenAtMillis, kind, cid)`, `StoreLogKind.{RUN,SELECTION,CLAIM}` with `.token`.
- Produces:
  - `static final int Index.SCHEMA_VERSION = 3`
  - `void Index.recordLogEntry(StoreLogEntry entry, String member)`: upserts `log_entry`, keeping the earliest `written_at`.
  - `StoreLogEntry Index.firstLogEntry(Cid cid, String member)`: the earliest entry of `cid` in `member`, or null.
  - `static String Index.isoMillis(long millis)`: `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` in UTC.
  - `private void Index.ingestLogged(BlockStore store, StoreLogKind kind, Cid cid, String member)`: the one dispatch every catch-up, retry and scan path goes through. This task handles `RUN` and logs `SELECTION` and `CLAIM` at debug; Tasks 4 and 6 add those two branches.
  - Index Snapshot: runs whose `log_entry` names the member, or whose `run.member` is the member; `log_entry` rows of the member, `member` written NULL.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/IndexLogEntryTest.groovy
package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 11: log_entry gives "first seen in this member" without a listing. */
class IndexLogEntryTest extends Specification {

    static final long T0 = 1_758_000_000_000L          // 2025-09-16T05:20:00.000Z

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    private Cid run(String name) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-$name"))
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: name], Fixtures.leaf("${name}.bam".toString(), Fixtures.contentCid(name), 1L)]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ["aligned/${name}.bam".toString()]]]))
        return store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
    }

    private List<List<Object>> rows(String sql) {
        final def c = DriverManager.getConnection("jdbc:sqlite:${index.file}")
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

    def 'the schema is version 3 with the tables of spec section 11'() {
        expect:
        Index.SCHEMA_VERSION == 3
        rows("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")*.get(0).containsAll(
            ['collection', 'collection_item', 'selection_child', 'selection_derived', 'log_entry', 'claim', 'claim_supersedes', 'claim_current'])
        rows("SELECT name FROM pragma_table_info('collection')")*.get(0) == ['collection_cid', 'kind', 'completion_cid', 'output_name', 'asserted_by']
        rows("SELECT name FROM pragma_table_info('collection_item')")*.get(0) == ['collection_cid', 'item_cid', 'via_cid']
    }

    def 'catch-up records every entry, of every kind, keeping the first time the member saw it'() {
        given:
        final Cid completion = run('a')
        final Cid other = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final StoreLog log = StoreLog.of(store)
        log.append(StoreLogKind.RUN, completion, T0 + 60_000)
        log.append(StoreLogKind.RUN, completion, T0)                 // the same run, logged earlier
        log.append(StoreLogKind.SELECTION, other, T0 + 120_000)

        when:
        index.catchUp(store, log, 'lab')

        then:
        rows('SELECT cid, kind, member, written_at FROM log_entry ORDER BY written_at') == [
            [completion.toString(), 'run', 'lab', '2025-09-16T05:20:00.000Z'],
            [other.toString(), 'selection', 'lab', '2025-09-16T05:22:00.000Z'],
        ]
        index.firstLogEntry(completion, 'lab').name == StoreLog.entryName(StoreLogKind.RUN, completion, T0)
        index.firstLogEntry(completion, 'elsewhere') == null
    }

    def 'an entry for a run already indexed still gets its row'() {
        given:
        final Cid completion = run('a')
        index.ingestRun(store, completion, 'lab')
        StoreLog.append(store, StoreLogKind.RUN, completion, T0)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        rows('SELECT cid FROM log_entry')*.get(0) == [completion.toString()]
    }

    def 'output collections carry their kind and asserted_by'() {
        given:
        final Cid completion = run('a')

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        rows('SELECT kind, output_name, asserted_by FROM collection') == [['output', 'aligned', Fixtures.ASSERTED_BY]]
        rows('SELECT via_cid FROM collection_item') == [[null]]
    }

    def 'rebuild fills log_entry from the Store Log'() {
        given:
        final Cid completion = run('a')
        StoreLog.append(store, StoreLogKind.RUN, completion, T0)

        when:
        index.rebuild(store, 'lab')

        then:
        rows('SELECT cid, kind, member, written_at FROM log_entry') == [[completion.toString(), 'run', 'lab', '2025-09-16T05:20:00.000Z']]
    }

    def 'isoMillis is UTC with milliseconds'() {
        expect:
        Index.isoMillis(T0 + 7) == '2025-09-16T05:20:00.007Z'
    }
}
```

In `IndexSnapshotTest.groovy`, make `run()` also return the manifest, as a fourth element (its callers ignore extra elements):

```groovy
    /** One run of one item per sample into a store; returns [completion, collection, items by sample, manifest]. */
    private List run(LocalBlockStore store, String runName, List<String> samples) {
        // ...body unchanged...
        return [completion, collection, items, manifest]
    }
```

and add (the helper logs each run in the store it writes to):

```groovy
    def 'a run logged in two members is in both snapshots (milestone 1 decision 1, fixed)'() {
        given:
        final List a = run(lab, 'a', ['A'])
        // The same run's closure arrives in shared too, as a Bundle merge would leave it.
        for( Cid cid : [(Cid) a[0], (Cid) a[1], (Cid) a[3]] + ((Map<String, Cid>) a[2]).values() )
            shared.put(cid, lab.open(cid), lab.size(cid))
        StoreLog.append(shared, StoreLogKind.RUN, (Cid) a[0], System.currentTimeMillis())
        catchUp()

        expect:
        rows(IndexSnapshot.write(index, 'lab', labRoot, 0L).path, 'SELECT completion_cid FROM run') == [[a[0].toString()]]
        rows(IndexSnapshot.write(index, 'shared', sharedRoot, 0L).path, 'SELECT completion_cid FROM run') == [[a[0].toString()]]
    }

    def 'the snapshot carries the member own log_entry rows, member written as NULL'() {
        given:
        final List a = run(lab, 'a', ['A'])
        run(shared, 'b', ['B'])
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path

        then:
        rows(file, 'SELECT cid, kind, member FROM log_entry') == [[a[0].toString(), 'run', null]]
    }

    def 'a run found only by the block scan (no log entry) stays in its member snapshot'() {
        given:
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid completion = lab.putDagCbor(Fixtures.runCompletion(manifest, []))
        catchUp()                                      // the first catch-up scans lab's blocks once

        expect:
        rows(IndexSnapshot.write(index, 'lab', labRoot, 0L).path, 'SELECT completion_cid FROM run') == [[completion.toString()]]
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexLogEntryTest' --tests 'robsyme.cas.core.IndexSnapshotTest'`
Expected: FAIL: `Index.SCHEMA_VERSION == 3` is false, `firstLogEntry` and `isoMillis` do not exist, and `no such column: kind`.

- [ ] **Step 3: Implement the schema and `log_entry` in `Index.groovy`**

Set `static final int SCHEMA_VERSION = 3`. Replace `ddl()` with the list below (the order is the creation order `web/test/fixtures/schema.sql` pins):

```groovy
    /** Every CREATE statement of the schema of DESIGN.md §12 and block explorer spec section 11, in order. */
    static List<String> ddl() {
        return [
            'CREATE TABLE schema_version(version INTEGER NOT NULL)',
            '''CREATE TABLE run(
                 completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,
                 commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,
                 status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)''',
            'CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)',
            'CREATE INDEX run_nf_run_hash ON run(nf_run_hash)',
            'CREATE INDEX run_manifest_cid ON run(manifest_cid)',
            // kind: 'output' | 'selection'. A Selection has no run and no output name.
            '''CREATE TABLE collection(
                 collection_cid TEXT PRIMARY KEY, kind TEXT, completion_cid TEXT, output_name TEXT, asserted_by TEXT)''',
            'CREATE TABLE item(item_cid TEXT PRIMARY KEY)',
            // One row per (collection, item, via); via_cid is NULL for an Output
            // Collection and for a Selection item chosen by query.
            'CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT, via_cid TEXT)',
            'CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)',
            'CREATE INDEX collection_item_collection ON collection_item(collection_cid)',
            'CREATE INDEX collection_item_item ON collection_item(item_cid)',
            'CREATE TABLE selection_child(parent_cid TEXT, child_cid TEXT)',
            'CREATE INDEX selection_child_child ON selection_child(child_cid)',
            'CREATE TABLE selection_derived(selection_cid TEXT, derived_from_cid TEXT)',
            '''CREATE TABLE producer(
                 content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)''',
            'CREATE INDEX producer_content_cid ON producer(content_cid)',
            'CREATE TABLE consumer(content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT)',
            'CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)',
            'CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)',
            // written_at: ISO-8601 UTC, milliseconds; the earliest entry per (cid, member).
            'CREATE TABLE log_entry(cid TEXT, kind TEXT, member TEXT, written_at TEXT)',
            'CREATE UNIQUE INDEX log_entry_cid_member ON log_entry(cid, member)',
            'CREATE INDEX log_entry_kind_written_at ON log_entry(kind, written_at DESC)',
            '''CREATE TABLE claim(
                 claim_cid TEXT PRIMARY KEY, subject_cid TEXT, verb TEXT, attribute TEXT, value TEXT,
                 timestamp TEXT, asserted_by TEXT)''',
            'CREATE INDEX claim_subject ON claim(subject_cid)',
            'CREATE TABLE claim_supersedes(claim_cid TEXT, superseded_cid TEXT)',
            'CREATE INDEX claim_supersedes_superseded ON claim_supersedes(superseded_cid)',
            '''CREATE TABLE claim_current(
                 subject_cid TEXT, attribute TEXT, value TEXT, claim_cid TEXT, conflicted INTEGER)''',
            'CREATE INDEX claim_current_subject ON claim_current(subject_cid, attribute)',
            'CREATE TABLE missing(have_cid TEXT, needed_cid TEXT)',
            '''CREATE TABLE nf_record(
                 key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,
                 labels_json TEXT, block_cid TEXT)''',
            // Not in §12's schema: the watermark and the stale mark, which are
            // this build's own bookkeeping rather than indexed block content.
            'CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)',
        ]
    }
```

In `ingestCollection`, write the new columns:

```groovy
        update('INSERT OR REPLACE INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, ?, ?, ?, ?)',
            [collectionCid.toString(), 'output', completion.toString(), text(collection.get('name')), text(collection.get('asserted_by'))])
```

and name the columns in the membership insert, so a later column cannot shift it: `'INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, NULL)'`.

Add, beside `isRunIndexed`:

```groovy
    private static final java.time.format.DateTimeFormatter ISO_MILLIS =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC)

    /** ISO-8601 UTC with milliseconds, the form of every timestamp in a block and in log_entry. */
    static String isoMillis(long millis) {
        return ISO_MILLIS.format(java.time.Instant.ofEpochMilli(millis))
    }

    /**
     * Records that {@code member}'s Store Log announced {@code entry}. Idempotent;
     * keeps the earliest time, which is when the member first saw the block
     * (block explorer spec section 11). ISO text of one fixed width sorts as time.
     */
    void recordLogEntry(StoreLogEntry entry, String member) {
        update('''INSERT INTO log_entry(cid, kind, member, written_at) VALUES (?, ?, ?, ?)
                  ON CONFLICT(cid, member) DO UPDATE SET written_at = min(written_at, excluded.written_at)''',
            [entry.cid.toString(), entry.kind.token, member, isoMillis(entry.writtenAtMillis)])
    }

    /** The earliest Store Log entry of {@code cid} in {@code member}, rebuilt from its row, or null. */
    StoreLogEntry firstLogEntry(Cid cid, String member) {
        final List<StoreLogEntry> found = new ArrayList<StoreLogEntry>()
        query('SELECT kind, written_at FROM log_entry WHERE cid = ? AND member = ?', [cid.toString(), member]) { ResultSet rs ->
            final StoreLogKind kind = StoreLogKind.fromToken(rs.getString(1))
            final long millis = java.time.Instant.parse(rs.getString(2)).toEpochMilli()
            found.add(StoreLog.parse(StoreLog.entryName(kind, cid, millis)))
        }
        return found ? found[0] : null
    }
```

Replace the loop body of `catchUp` so every entry is recorded and ingest goes through one dispatch:

```groovy
        for( int i = entries.size() - 1; i >= 0; i-- ) {
            final StoreLogEntry entry = entries[i]
            recordLogEntry(entry, member)
            ingestLogged(store, entry.kind, entry.cid, member)
        }
        setMeta(key, entries[0].name)
```

```groovy
    /**
     * Ingests one block a Store Log announced, unless it is already indexed.
     * Every catch-up, retry and scan path comes through here, so a new kind
     * is one branch. A block that cannot be read is recorded as `missing`.
     */
    private void ingestLogged(BlockStore store, StoreLogKind kind, Cid cid, String member) {
        switch( kind ) {
            case StoreLogKind.RUN:
                if( !isRunIndexed(cid) )
                    ingestTolerant(store, cid, member)
                return
            default:
                log.debug("store log entry for ${cid} is a ${kind.token}; ingested from Task 4 and Task 6 on")
        }
    }
```

Replace `retryMissingRuns` with a version that dispatches by the kind the Store Log gave (a `missing` row with no `have_cid` is a logged block that had not arrived):

```groovy
    private void retryMissingLogged(BlockStore store, String member) {
        final Map<Cid, StoreLogKind> waiting = new LinkedHashMap<Cid, StoreLogKind>()
        query('''SELECT DISTINCT m.needed_cid, l.kind FROM missing m
                 LEFT JOIN log_entry l ON l.cid = m.needed_cid
                 WHERE m.have_cid IS NULL''', []) { ResultSet rs ->
            final StoreLogKind kind = rs.getString(2) == null ? StoreLogKind.RUN : StoreLogKind.fromToken(rs.getString(2))
            waiting.put(Cid.parse(rs.getString(1)), kind ?: StoreLogKind.RUN)
        }
        for( Map.Entry<Cid, StoreLogKind> each : waiting.entrySet() ) {
            try {
                if( store.has(each.key) )
                    ingestLogged(store, each.value, each.key, member)
            }
            catch( IOException | UncheckedIOException e ) {
                warnOnce(each.key.toString(), "block ${each.key} is still unreachable (${e.message}); will retry")
            }
        }
    }
```

and call it from `catchUp` in place of `retryMissingRuns(store, member)`. `ingestTolerant` keeps its body (it ingests a run); Task 4 generalises it.

In `rebuild`, after `fresh.setMeta(META_SCANNED + ...)` and before `carryWatermark`, fill `log_entry` from the whole Store Log:

```groovy
            try {
                for( StoreLogEntry entry : StoreLog.read(store) )
                    fresh.recordLogEntry(entry, member)
            }
            catch( Exception e ) {
                log.debug("could not read the Store Log to fill log_entry: ${e.message}")
            }
```

- [ ] **Step 4: Implement the snapshot row rule in `IndexSnapshot.groovy`**

```groovy
    /**
     * The member's runs: those its Store Log announced, and those its blocks
     * held when the index first scanned it (a store written before the Store
     * Log, DESIGN.md §12). Alias written as NULL, since an alias is a local label.
     */
    private static final String COPY_RUNS = '''
        INSERT INTO main.run
        SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
               run_name, asserted_by, status, possibly_incomplete, finished_at, NULL
        FROM src.run
        WHERE member = ? OR completion_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'run')'''

    /** The member's own Store Log rows. */
    private static final String COPY_LOG_ENTRIES =
        'INSERT INTO main.log_entry SELECT cid, kind, NULL, written_at FROM src.log_entry WHERE member = ?'
```

`COPY_CLOSURE` keeps its statements; change the collection copy to `WHERE kind = 'output' AND completion_cid IN (SELECT completion_cid FROM main.run)`. In `buildAndVacuum` bind the member twice for `COPY_RUNS` and run `COPY_LOG_ENTRIES` after the closure:

```groovy
            update(c, COPY_RUNS, [(Object) member, member])
            for( String sql : COPY_CLOSURE )
                exec(c, sql)
            update(c, COPY_LOG_ENTRIES, [(Object) member])
```

- [ ] **Step 5: Run the Groovy tests**

Run: `./gradlew test --tests 'robsyme.cas.core.*'`
Expected: `IndexLogEntryTest` and `IndexSnapshotTest` PASS. `ExplorerQueriesTest.'the page tests build their databases from the index schema itself'` FAILS until Step 6 regenerates `schema.sql`, and tests naming `v2.sqlite` fail until Step 7.

- [ ] **Step 6: Carry schema 3 into the page tests and the Gate**

Regenerate the page fixture schema from a fresh index (`ExplorerQueriesTest` pins it to `sqlite_master` in rowid order). Only Groovy can create the index, so add this generator as a throwaway spec, run it once, then delete it:

```groovy
// src/test/groovy/robsyme/cas/core/SchemaDumpOnce.groovy -- delete after running
package robsyme.cas.core
import java.nio.file.Files
import java.nio.file.Path
import spock.lang.Specification
class SchemaDumpOnce extends Specification {
    def 'dump'() {
        given:
        final Path file = Files.createTempFile('schema', '.sqlite')
        Files.delete(file)
        Index.open(file).close()
        final def c = java.sql.DriverManager.getConnection("jdbc:sqlite:${file}")
        final def rs = c.createStatement().executeQuery("SELECT sql || ';' FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")
        final List<String> out = []
        while( rs.next() ) out << rs.getString(1)
        c.close()
        Path.of('web/test/fixtures/schema.sql').text = out.join('\n') + '\n'
        expect: true
    }
}
```

Run `./gradlew test --tests 'robsyme.cas.core.SchemaDumpOnce'`, then delete the file (it was never committed) and check `git diff web/test/fixtures/schema.sql` shows only the new tables, columns and indexes.

Then:

- `web/src/config.js`: `export const SCHEMA_VERSION = 3`.
- `web/test/fixture.mjs`: `INSERT INTO schema_version VALUES (3)`; name the columns of the positional inserts: `INSERT INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, 'output', ?, ?, 'test')` and `INSERT INTO collection_item(collection_cid, item_cid) VALUES (?, ?)`.
- `web/test/model.test.mjs` line ~136: `"INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run001', 'aligned')"` and `INSERT INTO collection_item(collection_cid, item_cid) SELECT 'coll', printf('item%04d', i) FROM n`.
- `gate/gen_year.py`: `SCHEMA_VERSION = 3`; the two inserts become `con.execute('INSERT INTO collection VALUES (?,?,?,?,?)', (c, 'output', comp, o, 'lab'))` and `con.execute('INSERT INTO collection_item VALUES (?,?,?)', (coll, item, None))`. The random calls keep their order; do not add any. Update the docstring's "schema-2" to "schema-3".
- `gate/browser_assert.py`: `SNAPSHOT = "index/v3.sqlite"`.

- [ ] **Step 7: Replace every other `v2.sqlite` and schema-2 reference**

Run: `grep -rn 'v2\.sqlite\|schema-2\|SCHEMA_VERSION = 2\|VALUES (2)' src gate web/src web/test web/bench DESIGN.md README.md gate/README.md --include='*.groovy' --include='*.py' --include='*.mjs' --include='*.js' --include='*.md' --include='*.sh'`

Expected hits to change to `v3` / schema 3 (as of `2bf7ae2`): `CasObserverTest`, `IndexSnapshotTest`, `CasCommandsTest`, `ExploreServerTest`, `S3MemberFilesTest`, `ExploreCommandTest`, `gate/test_gen_year.py`, `gate/test_browser_assert.py`, `gate/README.md`, `gate/browser/page-smoke.mjs`, `web/test/write-fixture.mjs`, `web/test/probe.test.mjs`, `web/test/db.test.mjs`, `web/test/snapshot-change.test.mjs`, `web/bench/run.sh`, `DESIGN.md`. Leave `web/bench/RESULTS.md` alone: it records measurements of a v2 file. A test that builds a stand-in schema of its own (`test_gen_year.py`) must build the schema-3 columns of `collection` and `collection_item`.

- [ ] **Step 8: Amend `DESIGN.md`**

In §5's layout, `index/v<N>.sqlite` stays generic. In §12, replace the schema block's `collection` and `collection_item` lines and add the new tables and the four extra indexes, with a line: "*Amended 2026-09-25 (block explorer milestone 2): schema 3, spec section 11, plus the indexes of decision 3 of `docs/plans/2026-09-25-explorer-milestone-2.md`.*" Add to §12's catch-up bullet: "Every Store Log entry read is recorded in `log_entry` (earliest time per `(cid, member)`); rebuild fills it from the whole log." In §15 "Index Snapshot", replace the milestone 1 rows sentence with: "Rows: every `run` the member's Store Log announced (`log_entry.member`) or its blocks held at the first scan (`run.member`), and the rows reached from those runs, plus the member's `log_entry` rows with `member` NULL. Selections and Claims: Tasks 4 and 6." Replace "today `index/v2.sqlite`" with "today `index/v3.sqlite`".

- [ ] **Step 9: Run everything**

Run: `./gradlew test && (cd web && npm test) && python3 -m unittest discover -s gate -p 'test_*.py'`
Expected: all PASS.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage 11/0/6 and browser tier A 5/5. The year snapshot is regenerated (its cache key includes the DDL). Record A2's three costs from the output in the commit message; each must stay within 8 req / 64 KB (points) and 50 / 512 KB (query 3).

- [ ] **Step 10: Commit**

```bash
git add -A src web/src web/test gate DESIGN.md
git commit -m "feat(index): schema 3 and log_entry; snapshot runs by Store Log

Spec section 11's tables in one bump. Every Store Log entry read is
recorded in log_entry (earliest time per cid and member); output
collections carry kind and asserted_by. The Index Snapshot selects the
member's runs by log_entry or run.member, so a run in two members is in
both snapshots. Year costs after regeneration: <producers> / <latest> /
<query 3> requests.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: A strict DAG-JSON codec

The request and response format of the write path (spec section 9.1). Hand-written and strict, because a request is untrusted input: the decoder refuses what DESIGN §4 refuses and is safe against deep nesting. The `@ipld/dag-json` dependency joins the page here, only to write the vectors this codec is pinned against.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/DagJson.groovy`
- Create: `src/test/groovy/robsyme/cas/core/DagJsonTest.groovy`
- Create: `web/test/dag-json-vectors.mjs`, `web/test/fixtures/dag-json-vectors.json`
- Modify: `web/package.json`, `web/package-lock.json` (add `"@ipld/dag-json": "11.0.1"`)

**Interfaces:**
- Consumes: `Cid.parse`, `Cid.isCid`, `Cid.bytes()`.
- Produces:
  - `static Object DagJson.decode(byte[] utf8)` and `static Object DagJson.decode(String text)`: `LinkedHashMap`, `ArrayList`, `Long` (or `BigInteger` past `Long`), `Double`, `String`, `Boolean`, `null`, `Cid`, `byte[]`.
  - `static byte[] DagJson.encode(Object value)` and `static String DagJson.encodeToString(Object value)`: the canonical text (keys by UTF-8 bytes, no whitespace).
  - `static class DagJson.DagJsonException extends IllegalArgumentException` with `final String at` (a JSON pointer, `''` for the document).
  - `static final int DagJson.MAX_DEPTH = 64`.

- [ ] **Step 1: Pin `@ipld/dag-json` and write the vector generator**

In `web/package.json` add `"@ipld/dag-json": "11.0.1"` to `dependencies` (it needs `cborg ^6.1.1` and `multiformats ^14.0.0`, both already pinned), then `cd web && npm install --no-audit --no-fund` and check `git diff package-lock.json` resolves only from `https://registry.npmjs.org/`.

```js
// web/test/dag-json-vectors.mjs
// Writes fixtures/dag-json-vectors.json with @ipld/dag-json, the codec the page
// uses, so DagJsonTest pins the Groovy codec to it (spec section 9.1).
//   node test/dag-json-vectors.mjs
import { writeFileSync } from 'node:fs'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'

const link = CID.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')
const raw = CID.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')
const text = new TextDecoder()
// No floats: JavaScript writes 1e+21 where Groovy writes 1.0E21, both valid; floats are decode-only vectors.
const values = {
  empty: {},
  scalars: { t: true, f: false, n: null, i: 7, neg: -12, big: 9007199254740991, s: 'plain' },
  order: { bb: 1, a: 2, aaa: 3, B: 4, 'é': 5, z: 6 },
  escapes: { s: 'q"\\\n\r\t\b\f\u0001\u001fé€𝄞' },
  link: { l: link, r: raw, list: [link, raw] },
  bytes: { b: new Uint8Array([1, 2, 3, 250]), none: new Uint8Array([]) },
  selection: { kind: 'Selection', members: [{ item: { address: link, via: [link] } }, { selection: link }], derived_from: [link.bytes] },
  claim: { kind: 'Claim', subject: link, verb: 'set', attribute: 'name', value: 'tumour, "batch 2"', supersedes: [], timestamp: '2026-09-25T10:00:00.000Z' },
}
const vectors = Object.entries(values).map(([name, value]) => ({ name, json: text.decode(dagJson.encode(value)) }))
writeFileSync(new URL('./fixtures/dag-json-vectors.json', import.meta.url), JSON.stringify(vectors, null, 1) + '\n')
console.log(`wrote ${vectors.length} vectors`)
```

Run: `cd web && node test/dag-json-vectors.mjs`
Expected: `wrote 8 vectors`, and `fixtures/dag-json-vectors.json` whose `order` vector is `{"B":4,"a":2,"aaa":3,"bb":1,"z":6,"é":5}` (bytewise key order, measured 2026-09-25).

- [ ] **Step 2: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/DagJsonTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification

/** Spec section 9.1: requests and responses are DAG-JSON, as @ipld/dag-json writes it. */
class DagJsonTest extends Specification {

    static final String LINK = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'

    static List<Map> vectors() {
        (List<Map>) new JsonSlurper().parse(Path.of('web/test/fixtures/dag-json-vectors.json').toFile())
    }

    def 'every @ipld/dag-json vector decodes and re-encodes to the same text (#v.name)'() {
        expect:
        DagJson.encodeToString(DagJson.decode((String) v.json)) == v.json

        where:
        v << vectors()
    }

    def 'links and bytes decode to Cid and byte[]'() {
        when:
        final Map m = (Map) DagJson.decode('{"l":{"/":"' + LINK + '"},"b":{"/":{"bytes":"AQID+g"}},"p":{"/":{"bytes":"AQID+g=="}}}')

        then:
        m.l == Cid.parse(LINK)
        m.b == [1, 2, 3, (byte) 250] as byte[]
        m.p == [1, 2, 3, (byte) 250] as byte[]
    }

    def 'numbers: integers are Long, BigInteger past Long, anything with a point or exponent Double'() {
        expect:
        DagJson.decode('7') == 7L
        DagJson.decode('-9223372036854775808') == Long.MIN_VALUE
        DagJson.decode('18446744073709551615') == new BigInteger('18446744073709551615')
        DagJson.decode('1.5') == 1.5d
        DagJson.decode('1e21') == 1e21d
        DagJson.decode('1.0') instanceof Double
    }

    def 'strings keep every escape and every code point'() {
        expect:
        DagJson.decode('"q\\"\\\\\\n\\u0001\\u00e9\\ud834\\udd1e"') == 'q"\\\n\u0001é𝄞'
        DagJson.encodeToString('a\u001fb') == '"a\\u001fb"'
        DagJson.encodeToString('tumour, "batch 2"\n') == '"tumour, \\"batch 2\\"\\n"'
    }

    def 'maps encode with keys sorted by UTF-8 bytes'() {
        expect:
        DagJson.encodeToString([z: 1, 'é': 2, B: 3, a: [b: 1, a: 2]]) == '{"B":3,"a":{"a":2,"b":1},"z":1,"é":2}'
    }

    def 'refused, with where (#text)'() {
        when:
        DagJson.decode(text)

        then:
        final DagJson.DagJsonException e = thrown()
        e.at == at

        where:
        text                                    | at
        '{"/":5}'                               | ''
        '{"a":{"/":"not a cid"}}'               | '/a'
        '{"a":{"/":"' + LINK + '","x":1}}'      | '/a'
        '{"a":{"/":{"bytes":"@@"}}}'            | '/a'
        '{"a":1,"a":2}'                         | '/a'
        '[1,2,'                                 | '/2'
        '{"a":[1,{"b":tru}]}'                   | '/a/1/b'
        '"\u0001"'                              | ''
        '01'                                    | ''
        'NaN'                                   | ''
        '1 2'                                   | ''
        ''                                      | ''
        '{"k~/":x}'                             | '/k~0~1'
    }

    def 'deep nesting is refused, not a stack overflow (Review Focus 4)'() {
        when:
        DagJson.decode('[' * 10_000 + ']' * 10_000)

        then:
        final DagJson.DagJsonException e = thrown()
        e.message.contains('nested deeper than 64')
    }

    def 'bytes that are not UTF-8 are refused'() {
        when:
        DagJson.decode([0x22, 0xff, 0x22] as byte[])

        then:
        thrown(DagJson.DagJsonException)
    }

    def 'a "/" key and non-finite floats cannot be encoded'() {
        when:
        DagJson.encode(value)

        then:
        thrown(IllegalArgumentException)

        where:
        value << [['/': 'x'], Double.NaN, Double.POSITIVE_INFINITY]
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.DagJsonTest'`
Expected: FAIL to compile: `DagJson` does not exist.

- [ ] **Step 4: Implement `DagJson.groovy`**

```groovy
// src/main/groovy/robsyme/cas/core/DagJson.groovy
package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

import groovy.transform.CompileStatic

/**
 * DAG-JSON (block explorer spec section 9.1), the write path's wire format:
 * links as {"/": "<cid>"}, bytes as {"/": {"bytes": "<base64>"}}. Strict where
 * a request is untrusted: a map with a "/" key that is neither form, a
 * duplicate key, text that is not UTF-8 and nesting past {@link #MAX_DEPTH}
 * are refused, each naming where (a JSON pointer). Encoding is canonical as
 * @ipld/dag-json 11 writes it: keys by UTF-8 bytes, no whitespace.
 */
@CompileStatic
final class DagJson {

    static final int MAX_DEPTH = 64

    static class DagJsonException extends IllegalArgumentException {
        final String at
        DagJsonException(String message, String at) {
            super(message)
            this.at = at
        }
    }

    private DagJson() {}

    // ------------------------------------------------------------------ decode

    static Object decode(byte[] utf8) {
        final String text
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(utf8)).toString()
        }
        catch( CharacterCodingException e ) {
            throw new DagJsonException('the request is not UTF-8 text', '')
        }
        return decode(text)
    }

    static Object decode(String text) {
        final Parser parser = new Parser(text)
        parser.skipSpace()
        final Object value = parser.value('', 0)
        parser.skipSpace()
        if( parser.pos != text.length() )
            throw new DagJsonException("unexpected text after the document at offset ${parser.pos}", '')
        return value
    }

    @CompileStatic
    private static final class Parser {
        final String s
        int pos = 0

        Parser(String s) { this.s = s }

        void skipSpace() {
            while( pos < s.length() ) {
                final char c = s.charAt(pos)
                if( c == (char) ' ' || c == (char) '\t' || c == (char) '\n' || c == (char) '\r' ) pos++
                else break
            }
        }

        DagJsonException fail(String message, String at) {
            return new DagJsonException("${message} at offset ${pos}".toString(), at)
        }

        Object value(String at, int depth) {
            if( depth > MAX_DEPTH )
                throw new DagJsonException("the request is nested deeper than ${MAX_DEPTH}", at)
            if( pos >= s.length() )
                throw fail('the document ended early', at)
            final char c = s.charAt(pos)
            switch( c ) {
                case (char) '{': return object(at, depth)
                case (char) '[': return array(at, depth)
                case (char) '"': return string(at)
                case (char) 't': return literal('true', Boolean.TRUE, at)
                case (char) 'f': return literal('false', Boolean.FALSE, at)
                case (char) 'n': return literal('null', null, at)
                default:
                    if( c == (char) '-' || (c >= (char) '0' && c <= (char) '9') )
                        return number(at)
                    throw fail("unexpected character '${c}'", at)
            }
        }

        Object literal(String word, Object result, String at) {
            if( !s.startsWith(word, pos) )
                throw fail("expected ${word}", at)
            pos += word.length()
            return result
        }

        Object number(String at) {
            final int start = pos
            if( s.charAt(pos) == (char) '-' ) pos++
            if( pos >= s.length() ) throw fail('a number ended early', at)
            if( s.charAt(pos) == (char) '0' ) {
                pos++
                if( pos < s.length() && Character.isDigit(s.charAt(pos)) )
                    throw fail('a number may not start with 0', at)
            }
            else {
                if( !Character.isDigit(s.charAt(pos)) ) throw fail('expected a digit', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            boolean floating = false
            if( pos < s.length() && s.charAt(pos) == (char) '.' ) {
                floating = true
                pos++
                if( pos >= s.length() || !Character.isDigit(s.charAt(pos)) ) throw fail('expected a digit after the point', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            if( pos < s.length() && (s.charAt(pos) == (char) 'e' || s.charAt(pos) == (char) 'E') ) {
                floating = true
                pos++
                if( pos < s.length() && (s.charAt(pos) == (char) '+' || s.charAt(pos) == (char) '-') ) pos++
                if( pos >= s.length() || !Character.isDigit(s.charAt(pos)) ) throw fail('expected an exponent', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            final String text = s.substring(start, pos)
            if( floating ) {
                final double d = Double.parseDouble(text)
                if( Double.isInfinite(d) || Double.isNaN(d) )
                    throw fail('a float out of range', at)
                return d
            }
            final BigInteger big = new BigInteger(text)
            return big.bitLength() < 64 ? (Object) big.longValue() : (Object) big
        }

        String string(String at) {
            pos++    // opening quote
            final StringBuilder out = new StringBuilder()
            while( true ) {
                if( pos >= s.length() ) throw fail('a string ended early', at)
                final char c = s.charAt(pos++)
                if( c == (char) '"' ) return out.toString()
                if( c < (char) 0x20 ) throw fail('an unescaped control character in a string', at)
                if( c != (char) '\\' ) {
                    out.append(c)
                    continue
                }
                if( pos >= s.length() ) throw fail('a string ended early', at)
                final char e = s.charAt(pos++)
                switch( e ) {
                    case (char) '"': out.append('"'); break
                    case (char) '\\': out.append('\\'); break
                    case (char) '/': out.append('/'); break
                    case (char) 'b': out.append('\b'); break
                    case (char) 'f': out.append('\f'); break
                    case (char) 'n': out.append('\n'); break
                    case (char) 'r': out.append('\r'); break
                    case (char) 't': out.append('\t'); break
                    case (char) 'u':
                        if( pos + 4 > s.length() ) throw fail('a \\u escape ended early', at)
                        try {
                            out.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16))
                        }
                        catch( NumberFormatException x ) {
                            throw fail('a \\u escape with a non-hex digit', at)
                        }
                        pos += 4
                        break
                    default: throw fail("an unknown escape \\${e}", at)
                }
            }
        }

        List array(String at, int depth) {
            pos++
            final List<Object> out = new ArrayList<Object>()
            skipSpace()
            if( pos < s.length() && s.charAt(pos) == (char) ']' ) {
                pos++
                return out
            }
            while( true ) {
                skipSpace()
                out.add(value("${at}/${out.size()}".toString(), depth + 1))
                skipSpace()
                if( pos >= s.length() ) throw fail('an array ended early', "${at}/${out.size()}".toString())
                final char c = s.charAt(pos++)
                if( c == (char) ']' ) return out
                if( c != (char) ',' ) throw fail("expected , or ] in an array", at)
            }
        }

        Object object(String at, int depth) {
            pos++
            final LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>()
            skipSpace()
            if( pos < s.length() && s.charAt(pos) == (char) '}' ) {
                pos++
                return out
            }
            while( true ) {
                skipSpace()
                if( pos >= s.length() || s.charAt(pos) != (char) '"' ) throw fail('expected a string key', at)
                final String key = string(at)
                final String here = "${at}/${pointer(key)}".toString()
                if( out.containsKey(key) ) throw fail("the key '${key}' appears twice", here)
                skipSpace()
                if( pos >= s.length() || s.charAt(pos++) != (char) ':' ) throw fail('expected :', here)
                skipSpace()
                out.put(key, value(here, depth + 1))
                skipSpace()
                if( pos >= s.length() ) throw fail('an object ended early', at)
                final char c = s.charAt(pos++)
                if( c == (char) '}' ) break
                if( c != (char) ',' ) throw fail('expected , or } in an object', at)
            }
            return out.containsKey('/') ? special(out, at) : out
        }

        /** {"/": "<cid>"} is a link, {"/": {"bytes": "<base64>"}} bytes; any other "/" map is refused (DESIGN.md §4). */
        Object special(Map<String, Object> map, String at) {
            final Object slash = map.get('/')
            if( map.size() == 1 && slash instanceof String ) {
                if( !Cid.isCid((String) slash) )
                    throw new DagJsonException("'${slash}' is not a CID nf-blocks can hold", at)
                return Cid.parse((String) slash)
            }
            if( map.size() == 1 && slash instanceof Map && ((Map) slash).size() == 1 && ((Map) slash).get('bytes') instanceof String ) {
                try {
                    return Base64.decoder.decode((String) ((Map) slash).get('bytes'))
                }
                catch( IllegalArgumentException e ) {
                    throw new DagJsonException('bytes that are not base64', at)
                }
            }
            throw new DagJsonException('a map with a "/" key must be a link {"/": "<cid>"} or bytes {"/": {"bytes": "..."}}', at)
        }
    }

    /** RFC 6901 escaping of one pointer segment. */
    static String pointer(String key) {
        return key.replace('~', '~0').replace('/', '~1')
    }

    // ------------------------------------------------------------------ encode

    static byte[] encode(Object value) {
        return encodeToString(value).getBytes(StandardCharsets.UTF_8)
    }

    static String encodeToString(Object value) {
        final StringBuilder out = new StringBuilder()
        write(value, out)
        return out.toString()
    }

    private static final Comparator<String> BY_UTF8 = { String a, String b ->
        final byte[] x = a.getBytes(StandardCharsets.UTF_8)
        final byte[] y = b.getBytes(StandardCharsets.UTF_8)
        return Arrays.compareUnsigned(x, y)
    } as Comparator<String>

    private static void write(Object value, StringBuilder out) {
        if( value == null ) { out.append('null'); return }
        if( value instanceof Boolean ) { out.append(value.toString()); return }
        if( value instanceof Integer || value instanceof Long || value instanceof BigInteger || value instanceof Short || value instanceof Byte ) {
            out.append(value.toString()); return
        }
        if( value instanceof Double || value instanceof Float ) {
            final double d = ((Number) value).doubleValue()
            if( Double.isNaN(d) || Double.isInfinite(d) )
                throw new IllegalArgumentException('NaN and Infinity are not DAG-JSON')
            out.append(Double.toString(d)); return
        }
        if( value instanceof CharSequence ) { string(value.toString(), out); return }
        if( value instanceof Cid ) { out.append('{"/":'); string(value.toString(), out); out.append('}'); return }
        if( value instanceof byte[] ) {
            out.append('{"/":{"bytes":')
            string(Base64.encoder.withoutPadding().encodeToString((byte[]) value), out)
            out.append('}}'); return
        }
        if( value instanceof List ) {
            out.append('[')
            boolean first = true
            for( Object element : (List) value ) {
                if( !first ) out.append(',')
                first = false
                write(element, out)
            }
            out.append(']'); return
        }
        if( value instanceof Map ) {
            final Map map = (Map) value
            final List<String> keys = new ArrayList<String>()
            for( Object key : map.keySet() ) {
                if( !(key instanceof String) ) throw new IllegalArgumentException("a DAG-JSON map key must be a string, got ${key?.getClass()?.simpleName}")
                if( key == '/' ) throw new IllegalArgumentException('a map key "/" is reserved (DESIGN.md §4)')
                keys.add((String) key)
            }
            Collections.sort(keys, BY_UTF8)
            out.append('{')
            boolean first = true
            for( String key : keys ) {
                if( !first ) out.append(',')
                first = false
                string(key, out)
                out.append(':')
                write(map.get(key), out)
            }
            out.append('}'); return
        }
        throw new IllegalArgumentException("cannot encode ${value.getClass().name} as DAG-JSON")
    }

    private static void string(String text, StringBuilder out) {
        out.append('"')
        for( int i = 0; i < text.length(); i++ ) {
            final char c = text.charAt(i)
            switch( c ) {
                case (char) '"': out.append('\\"'); break
                case (char) '\\': out.append('\\\\'); break
                case (char) '\n': out.append('\\n'); break
                case (char) '\r': out.append('\\r'); break
                case (char) '\t': out.append('\\t'); break
                case (char) '\b': out.append('\\b'); break
                case (char) '\f': out.append('\\f'); break
                default:
                    if( c < (char) 0x20 )
                        out.append(String.format('\\u%04x', (int) c))
                    else
                        out.append(c)
            }
        }
        out.append('"')
    }
}
```

`Arrays.compareUnsigned(byte[], byte[])` is JDK 9+. If a vector's escapes disagree with `@ipld/dag-json` (for instance it writes `\u001f` in upper case), match the vector: the vector file is the reference, not this listing.

- [ ] **Step 5: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.DagJsonTest'`
Expected: PASS. If the `'{"k~/":x}'` row fails only on the pointer text, check `pointer()` escapes `~` before `/`.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/DagJson.groovy src/test/groovy/robsyme/cas/core/DagJsonTest.groovy \
        web/package.json web/package-lock.json web/test/dag-json-vectors.mjs web/test/fixtures/dag-json-vectors.json
git commit -m "feat(core): strict DAG-JSON codec, pinned to @ipld/dag-json vectors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The Claim block and its current state

The Claim of lineage spec §5 and block explorer spec section 8, and one function from a subject's Claims to its current state (decision 5). The rules are pinned by a vector file the page's `claims.js` (Task 13) runs too, so the two implementations cannot drift.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (add `static final String CLAIM = 'Claim'`)
- Create: `src/main/groovy/robsyme/cas/core/Claim.groovy`
- Create: `src/main/groovy/robsyme/cas/core/ClaimState.groovy`
- Create: `web/test/fixtures/claim-vectors.json`
- Modify: `src/test/groovy/robsyme/cas/core/Fixtures.groovy` (add `claim(...)`)
- Create: `src/test/groovy/robsyme/cas/core/ClaimTest.groovy`, `ClaimStateTest.groovy`

**Interfaces:**
- Consumes: `Records.head`, `Records.expectKind`, `Records.require`, `Records.string`, `Records.cid`, `DagCbor.encode`.
- Produces:
  - `class Claim` with `final String assertedBy, Cid subject, String verb, String attribute, Object value, List<Cid> supersedes, String timestamp`; constants `SET`, `ADD`, `DEL`, `DELETE`, `VERBS`, `NAME = 'name'`; constructor `Claim(String assertedBy, Cid subject, String verb, String attribute, Object value, List<Cid> supersedes, String timestamp)` (sorts and dedupes `supersedes` by CID string); `Map<String,Object> toCbor()`; `static Claim fromCbor(Map)`.
  - `final class ClaimState` with nested `static final class Row(String cid, String verb, String attribute, Object value, List<String> supersedes)`; constants `NONE`, `DELETED`, `CONFLICTED`; `static ClaimState of(Collection<Row> claims)`; fields `List<Row> current` (by cid), `List<String> names`, `List<String> nameClaims`, `boolean nameConflicted`, `String deletion`, `List<String> deletionClaims`; `boolean isHidden()`; `List<Map<String,Object>> currentRows()` each `[cid:, attribute:, value:, conflicted: boolean]`.
  - `Fixtures.claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes, String timestamp = '2026-09-25T10:00:00.000Z')` → a §6 map.

- [ ] **Step 1: Write the vectors**

```json
[
 {"name": "no claims", "claims": [],
  "expect": {"current": [], "names": [], "nameConflicted": false, "deletion": "none"}},
 {"name": "one name", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "first", "supersedes": []}],
  "expect": {"current": ["c1"], "names": ["first"], "nameConflicted": false, "deletion": "none"}},
 {"name": "a rename supersedes the old name", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "first", "supersedes": []},
   {"cid": "c2", "verb": "set", "attribute": "name", "value": "second", "supersedes": ["c1"]}],
  "expect": {"current": ["c2"], "names": ["second"], "nameConflicted": false, "deletion": "none"}},
 {"name": "two names nothing supersedes conflict, in claim-address order", "claims": [
   {"cid": "c2", "verb": "set", "attribute": "name", "value": "bravo", "supersedes": []},
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "alpha", "supersedes": []}],
  "expect": {"current": ["c1", "c2"], "names": ["alpha", "bravo"], "nameConflicted": true, "deletion": "none"}},
 {"name": "a rename superseding both names resolves the conflict", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "alpha", "supersedes": []},
   {"cid": "c2", "verb": "set", "attribute": "name", "value": "bravo", "supersedes": []},
   {"cid": "c3", "verb": "set", "attribute": "name", "value": "charlie", "supersedes": ["c1", "c2"]}],
  "expect": {"current": ["c3"], "names": ["charlie"], "nameConflicted": false, "deletion": "none"}},
 {"name": "a chain of renames leaves only the last", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "a", "supersedes": []},
   {"cid": "c2", "verb": "set", "attribute": "name", "value": "b", "supersedes": ["c1"]},
   {"cid": "c3", "verb": "set", "attribute": "name", "value": "c", "supersedes": ["c2"]}],
  "expect": {"current": ["c3"], "names": ["c"], "nameConflicted": false, "deletion": "none"}},
 {"name": "a delete hides", "claims": [
   {"cid": "c1", "verb": "delete", "attribute": null, "value": null, "supersedes": []}],
  "expect": {"current": ["c1"], "names": [], "nameConflicted": false, "deletion": "deleted"}},
 {"name": "undo is a del superseding the delete", "claims": [
   {"cid": "c1", "verb": "delete", "attribute": null, "value": null, "supersedes": []},
   {"cid": "c2", "verb": "del", "attribute": null, "value": null, "supersedes": ["c1"]}],
  "expect": {"current": ["c2"], "names": [], "nameConflicted": false, "deletion": "none"}},
 {"name": "delete again after an undo", "claims": [
   {"cid": "c1", "verb": "delete", "attribute": null, "value": null, "supersedes": []},
   {"cid": "c2", "verb": "del", "attribute": null, "value": null, "supersedes": ["c1"]},
   {"cid": "c3", "verb": "delete", "attribute": null, "value": null, "supersedes": ["c2"]}],
  "expect": {"current": ["c3"], "names": [], "nameConflicted": false, "deletion": "deleted"}},
 {"name": "an undo and a delete written without seeing each other conflict, and do not hide", "claims": [
   {"cid": "c1", "verb": "delete", "attribute": null, "value": null, "supersedes": []},
   {"cid": "c2", "verb": "del", "attribute": null, "value": null, "supersedes": ["c1"]},
   {"cid": "c3", "verb": "delete", "attribute": null, "value": null, "supersedes": []}],
  "expect": {"current": ["c2", "c3"], "names": [], "nameConflicted": false, "deletion": "conflicted"}},
 {"name": "two deletes nothing supersedes are a conflict too (spec section 8: more than one is a conflict)", "claims": [
   {"cid": "c1", "verb": "delete", "attribute": null, "value": null, "supersedes": []},
   {"cid": "c2", "verb": "delete", "attribute": null, "value": null, "supersedes": []}],
  "expect": {"current": ["c1", "c2"], "names": [], "nameConflicted": false, "deletion": "conflicted"}},
 {"name": "names and deletion are separate groups", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "kept", "supersedes": []},
   {"cid": "c2", "verb": "delete", "attribute": null, "value": null, "supersedes": []}],
  "expect": {"current": ["c1", "c2"], "names": ["kept"], "nameConflicted": false, "deletion": "deleted"}},
 {"name": "a del of the name removes it without a conflict", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "name", "value": "gone", "supersedes": []},
   {"cid": "c2", "verb": "del", "attribute": "name", "value": null, "supersedes": ["c1"]}],
  "expect": {"current": ["c2"], "names": [], "nameConflicted": false, "deletion": "none"}},
 {"name": "a supersedes naming a claim that is not in the set changes nothing", "claims": [
   {"cid": "c2", "verb": "set", "attribute": "name", "value": "only", "supersedes": ["c9"]}],
  "expect": {"current": ["c2"], "names": ["only"], "nameConflicted": false, "deletion": "none"}}
]
```

Save it as `web/test/fixtures/claim-vectors.json`. Vector CIDs are short strings compared as strings; keep them `c1` to `c9` so string order is numeric order.

- [ ] **Step 2: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/ClaimStateTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification

/** Decision 5 of the milestone 2 plan: current state, pinned by the vectors claims.js runs too. */
class ClaimStateTest extends Specification {

    static List<Map> vectors() {
        (List<Map>) new JsonSlurper().parse(Path.of('web/test/fixtures/claim-vectors.json').toFile())
    }

    def '#v.name'() {
        given:
        final List<ClaimState.Row> rows = ((List<Map>) v.claims).collect { Map c ->
            new ClaimState.Row((String) c.cid, (String) c.verb, (String) c.attribute, c.value, (List<String>) c.supersedes)
        }

        when:
        final ClaimState state = ClaimState.of(rows)
        final Map expect = (Map) v.expect

        then:
        state.current*.cid == expect.current
        state.names == expect.names
        state.nameConflicted == expect.nameConflicted
        state.deletion == expect.deletion
        state.hidden == (expect.deletion == 'deleted')

        where:
        v << vectors()
    }

    def 'currentRows flags every row of a conflicted group'() {
        given:
        final ClaimState state = ClaimState.of([
            new ClaimState.Row('c1', 'set', 'name', 'a', []),
            new ClaimState.Row('c2', 'set', 'name', 'b', []),
            new ClaimState.Row('c3', 'delete', null, null, []),
        ])

        expect:
        state.currentRows() == [
            [cid: 'c1', attribute: 'name', value: 'a', conflicted: true],
            [cid: 'c2', attribute: 'name', value: 'b', conflicted: true],
            [cid: 'c3', attribute: null, value: null, conflicted: false],
        ]
        state.nameClaims == ['c1', 'c2']
        state.deletionClaims == ['c3']
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/core/ClaimTest.groovy
package robsyme.cas.core

import spock.lang.Specification

/** Block explorer spec section 8 and DESIGN.md §6: the Claim block. */
class ClaimTest extends Specification {

    static final Cid SUBJECT = Fixtures.cidOf([kind: 'Selection', schema: 1])
    static final Cid A = Fixtures.cidOf([kind: 'Claim', n: 1])
    static final Cid B = Fixtures.cidOf([kind: 'Claim', n: 2])

    def 'the block is the §6 map, supersedes sorted and deduplicated'() {
        given:
        final List<Cid> given = [B, A, B]

        when:
        final Map block = new Claim('ada', SUBJECT, 'set', 'name', 'tumour', given, '2026-09-25T10:00:00.000Z').toCbor()

        then:
        block == [kind: 'Claim', schema: 1L, asserted_by: 'ada', subject: SUBJECT, verb: 'set', attribute: 'name',
                  value: 'tumour', supersedes: [A, B].sort { it.toString() }, timestamp: '2026-09-25T10:00:00.000Z']
        block.keySet().toList() == ['kind', 'schema', 'asserted_by', 'subject', 'verb', 'attribute', 'value', 'supersedes', 'timestamp']
    }

    def 'it round-trips through DAG-CBOR to the same address'() {
        given:
        final Claim claim = new Claim('ada', SUBJECT, 'delete', null, null, [], '2026-09-25T10:00:00.000Z')
        final byte[] bytes = DagCbor.encode(claim.toCbor())

        expect:
        Claim.fromCbor((Map) DagCbor.decode(bytes)) == claim
        DagCbor.encode(Claim.fromCbor((Map) DagCbor.decode(bytes)).toCbor()) == bytes
        Fixtures.cidOf(Fixtures.claim(SUBJECT, 'delete', null, null, [])) == DagCbor.cidOf(bytes)
    }

    def 'an unknown verb, a missing subject or no timestamp is refused'() {
        when:
        new Claim('ada', subject, verb, null, null, [], timestamp)

        then:
        thrown(IllegalArgumentException)

        where:
        subject | verb     | timestamp
        SUBJECT | 'rename' | '2026-09-25T10:00:00.000Z'
        null    | 'delete' | '2026-09-25T10:00:00.000Z'
        SUBJECT | 'delete' | ''
    }
}
```

Add to `Fixtures.groovy`:

```groovy
    /** A Claim as §6 specifies it; supersedes sorted by cid string. */
    static Map claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes,
                     String timestamp = '2026-09-25T10:00:00.000Z') {
        return [kind: 'Claim', schema: 1, asserted_by: ASSERTED_BY, subject: subject, verb: verb,
                attribute: attribute, value: value,
                supersedes: new ArrayList<Cid>(supersedes).sort { Cid c -> c.toString() },
                timestamp: timestamp]
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.ClaimTest' --tests 'robsyme.cas.core.ClaimStateTest'`
Expected: FAIL to compile: `Claim` and `ClaimState` do not exist.

- [ ] **Step 4: Implement**

Add `static final String CLAIM = 'Claim'` beside the other kind names in `Records`.

```groovy
// src/main/groovy/robsyme/cas/core/Claim.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * A statement about a subject (lineage spec §5, block explorer spec section 8):
 * rename is `set name`, delete is `delete`, undo is `del` superseding the
 * delete. Claims are blocks, not index rows, so they travel. The timestamp is
 * advisory and comes from the request, so a retried request is the same block.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Claim {

    static final String SET = 'set'
    static final String ADD = 'add'
    static final String DEL = 'del'
    static final String DELETE = 'delete'
    static final Set<String> VERBS = [SET, ADD, DEL, DELETE] as Set
    static final String NAME = 'name'

    final String assertedBy
    final Cid subject
    final String verb
    final String attribute
    final Object value
    final List<Cid> supersedes
    final String timestamp

    Claim(String assertedBy, Cid subject, String verb, String attribute, Object value, List<Cid> supersedes, String timestamp) {
        if( !assertedBy )
            throw new IllegalArgumentException('a claim needs an asserted_by')
        if( subject == null )
            throw new IllegalArgumentException('a claim needs a subject')
        if( !VERBS.contains(verb) )
            throw new IllegalArgumentException("unknown claim verb '${verb}'; one of ${VERBS.join(', ')}")
        if( !timestamp )
            throw new IllegalArgumentException('a claim needs a timestamp')
        this.assertedBy = assertedBy
        this.subject = subject
        this.verb = verb
        this.attribute = attribute
        this.value = value
        final TreeSet<Cid> sorted = new TreeSet<Cid>(supersedes ?: Collections.<Cid> emptyList())
        this.supersedes = Collections.unmodifiableList(new ArrayList<Cid>(sorted))
        this.timestamp = timestamp
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.CLAIM)
        map.put('asserted_by', assertedBy)
        map.put('subject', subject)
        map.put('verb', verb)
        map.put('attribute', attribute)
        map.put('value', value)
        map.put('supersedes', new ArrayList<Object>(supersedes))
        map.put('timestamp', timestamp)
        return map
    }

    static Claim fromCbor(Map block) {
        Records.expectKind(block, Records.CLAIM)
        final List supersedes = (List) Records.require(block, 'supersedes')
        return new Claim(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.cid(Records.require(block, 'subject'), 'subject'),
            Records.string(Records.require(block, 'verb'), 'verb'),
            Records.string(Records.require(block, 'attribute'), 'attribute'),
            Records.require(block, 'value'),
            supersedes.collect { Object c -> Records.cid(c, 'supersedes') },
            Records.string(Records.require(block, 'timestamp'), 'timestamp'))
    }
}
```

`Cid` is `Comparable` by its text, so the `TreeSet` sorts by CID string and drops duplicates.

```groovy
// src/main/groovy/robsyme/cas/core/ClaimState.groovy
package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The current state of one subject from its Claims (block explorer spec
 * section 8; decision 5 of the milestone 2 plan). Current = the Claims no
 * Claim in the set supersedes. They group by attribute, delete and del (no
 * attribute) forming the deletion group; a group of more than one is a
 * conflict, surfaced in claim-address order and never resolved here.
 * web/src/claims.js implements the same rules; web/test/fixtures/claim-vectors.json pins both.
 */
@CompileStatic
final class ClaimState {

    static final String NONE = 'none'
    static final String DELETED = 'deleted'
    static final String CONFLICTED = 'conflicted'

    @Canonical
    @CompileStatic
    static final class Row {
        String cid
        String verb
        String attribute
        Object value
        List<String> supersedes
    }

    final List<Row> current
    final List<String> names
    final List<String> nameClaims
    final boolean nameConflicted
    final String deletion
    final List<String> deletionClaims
    private final Set<String> conflictedGroups

    private ClaimState(List<Row> current, Set<String> conflictedGroups) {
        this.current = Collections.unmodifiableList(current)
        this.conflictedGroups = conflictedGroups
        final List<Row> nameGroup = current.findAll { Row r -> r.attribute == Claim.NAME }
        this.nameClaims = Collections.unmodifiableList(nameGroup*.cid)
        this.names = Collections.unmodifiableList(nameGroup.findAll { Row r -> r.verb == Claim.SET }.collect { Row r -> String.valueOf(r.value) })
        this.nameConflicted = nameGroup.size() > 1
        final List<Row> deletionGroup = current.findAll { Row r -> r.attribute == null && (r.verb == Claim.DELETE || r.verb == Claim.DEL) }
        this.deletionClaims = Collections.unmodifiableList(deletionGroup*.cid)
        this.deletion = deletionGroup.size() > 1 ? CONFLICTED
            : deletionGroup.size() == 1 && deletionGroup[0].verb == Claim.DELETE ? DELETED
            : NONE
    }

    static ClaimState of(Collection<Row> claims) {
        final Set<String> superseded = new HashSet<String>()
        for( Row row : claims )
            superseded.addAll(row.supersedes ?: Collections.<String> emptyList())
        final List<Row> current = new ArrayList<Row>(claims.findAll { Row r -> !superseded.contains(r.cid) })
        current.sort { Row a, Row b -> a.cid <=> b.cid }
        final Map<String, Integer> sizes = new HashMap<String, Integer>()
        for( Row row : current )
            sizes.merge(groupOf(row), 1, { Integer a, Integer b -> a + b })
        final Set<String> conflicted = sizes.findAll { String k, Integer n -> n > 1 }.keySet()
        return new ClaimState(current, new HashSet<String>(conflicted))
    }

    /** The attribute, or a marker for the deletion group (attribute null). */
    private static String groupOf(Row row) {
        return row.attribute == null ? '\u0000deletion' : row.attribute
    }

    boolean isHidden() { deletion == DELETED }

    /** One entry per current Claim, for claim_current. */
    List<Map<String, Object>> currentRows() {
        return current.collect { Row r ->
            [cid: r.cid, attribute: r.attribute, value: r.value, conflicted: conflictedGroups.contains(groupOf(r))] as Map<String, Object>
        }
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.ClaimTest' --tests 'robsyme.cas.core.ClaimStateTest'`
Expected: PASS, one row per vector.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Records.groovy src/main/groovy/robsyme/cas/core/Claim.groovy \
        src/main/groovy/robsyme/cas/core/ClaimState.groovy src/test/groovy/robsyme/cas/core/Fixtures.groovy \
        src/test/groovy/robsyme/cas/core/ClaimTest.groovy src/test/groovy/robsyme/cas/core/ClaimStateTest.groovy \
        web/test/fixtures/claim-vectors.json
git commit -m "feat(core): the Claim block and its current state, pinned by shared vectors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Claims in the index, the snapshot and query 2

Wave 0's last piece: catch-up ingests logged Claims into `claim` and `claim_supersedes`, keeps `claim_current` from `ClaimState`, a member's snapshot recomputes current state from that member's Claims alone (spec section 4), and query 2 leaves out runs with a current `delete` Claim (spec section 8).

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/ClaimCurrent.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`SQL_LATEST_SUCCESSFUL_RUN`, `latestSuccessfulRun`, `ingestLogged`, `ingestTolerant`, scan and rebuild; new `ingestClaim`, `isClaimIndexed`, `claimState`, `valueText`)
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy`
- Modify: `web/src/queries.json` (`latestSuccessfulRun`)
- Modify: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy` (nothing to add for the constant; the existing char-for-char check covers it)
- Create: `src/test/groovy/robsyme/cas/core/IndexClaimsTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy`
- Modify: `DESIGN.md` §12 (query 2's `successful`), §15 (snapshot Claims)

**Interfaces:**
- Consumes: `Claim.fromCbor`, `ClaimState.of`, `ClaimState.Row`, `ClaimState.currentRows()`, `DagJson.encodeToString`, Task 1's `ingestLogged` and `recordLogEntry`.
- Produces:
  - `static final String Index.SQL_LATEST_SUCCESSFUL_RUN` (new text below).
  - `void Index.ingestClaim(BlockStore store, Cid claim, String member)`.
  - `boolean Index.isClaimIndexed(Cid claim)`.
  - `ClaimState Index.claimState(Cid subject)`.
  - `static String Index.valueText(Object value)` (decision 4).
  - `final class ClaimCurrent` with `static ClaimState load(Connection c, String subject)` and `static void rewrite(Connection c, String subject)`.
  - Snapshot: `claim` and `claim_supersedes` rows of the Claims the member logged; `claim_current` recomputed from those alone.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/IndexClaimsTest.groovy
package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 8: Claims discovered through the Store Log, current state, query 2. */
class IndexClaimsTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    long clock = 1_758_000_000_000L

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, kind, cid, clock += 1000)
        return cid
    }

    private Cid claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = []) {
        return logged(StoreLogKind.CLAIM, Fixtures.claim(subject, verb, attribute, value, supersedes))
    }

    private Cid run(String name, String finishedAt) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-$name"))
        return logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [], [finished_at: finishedAt]))
    }

    private void catchUp() { index.catchUp(store, StoreLog.of(store), 'lab') }

    private List<List<Object>> rows(String sql) {
        final def c = DriverManager.getConnection("jdbc:sqlite:${index.file}")
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

    def 'a logged Claim is indexed with its supersedes, and claim_current follows ClaimState'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Cid first = claim(subject, 'set', 'name', 'first')
        final Cid second = claim(subject, 'set', 'name', 'second', [first])

        when:
        catchUp()

        then:
        rows('SELECT claim_cid, subject_cid, verb, attribute, value FROM claim ORDER BY timestamp, claim_cid').size() == 2
        rows('SELECT claim_cid, superseded_cid FROM claim_supersedes') == [[second.toString(), first.toString()]]
        rows('SELECT subject_cid, attribute, value, claim_cid, conflicted FROM claim_current') ==
            [[subject.toString(), 'name', 'second', second.toString(), 0]]
        index.claimState(subject).names == ['second']
        index.isClaimIndexed(first)
    }

    def 'two names nothing supersedes are both current and flagged'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Cid a = claim(subject, 'set', 'name', 'alpha')
        final Cid b = claim(subject, 'set', 'name', 'bravo')

        when:
        catchUp()

        then:
        rows('SELECT claim_cid, conflicted FROM claim_current ORDER BY claim_cid') == [a, b].sort { it.toString() }.collect { [it.toString(), 1] }
        index.claimState(subject).nameConflicted
    }

    def 'a non-string value is stored as its DAG-JSON text (decision 4)'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        claim(subject, 'set', 'rating', [stars: 3L])
        claim(subject, 'set', 'label', 'plain')

        when:
        catchUp()

        then:
        rows('SELECT attribute, value FROM claim ORDER BY attribute') == [['label', 'plain'], ['rating', '{"stars":3}']]
        Index.valueText(null) == null
    }

    def 'a Claim whose block has not arrived is missing, then indexed when it lands'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Map block = Fixtures.claim(subject, 'delete', null, null, [])
        final Cid absent = Fixtures.cidOf(block)
        StoreLog.append(store, StoreLogKind.CLAIM, absent, clock += 1000)

        when:
        catchUp()

        then:
        rows('SELECT have_cid, needed_cid FROM missing') == [[null, absent.toString()]]
        !index.isClaimIndexed(absent)

        when:
        store.putDagCbor(block)
        catchUp()

        then:
        index.isClaimIndexed(absent)
        rows('SELECT count(*) FROM missing') == [[0]]
        index.claimState(subject).hidden
    }

    def 'query 2 leaves out a run with a current delete Claim, and brings it back after an undo'() {
        given:
        final Cid older = run('older', '2026-09-01T10:00:00.000Z')
        final Cid newer = run('newer', '2026-09-02T10:00:00.000Z')
        final Cid delete = claim(newer, 'delete', null, null)
        catchUp()

        expect:
        index.latestSuccessfulRun('p') == Optional.of(older)

        when:
        claim(newer, 'del', null, null, [delete])
        catchUp()

        then:
        index.latestSuccessfulRun('p') == Optional.of(newer)
    }

    def 'query 2 leaves out a run whose deletion is conflicted too'() {
        given:
        final Cid older = run('older', '2026-09-01T10:00:00.000Z')
        final Cid newer = run('newer', '2026-09-02T10:00:00.000Z')
        final Cid delete = claim(newer, 'delete', null, null)
        claim(newer, 'del', null, null, [delete])
        claim(newer, 'delete', null, null)                  // written without seeing the undo
        catchUp()

        expect:
        index.claimState(newer).deletion == ClaimState.CONFLICTED
        index.latestSuccessfulRun('p') == Optional.of(older)
    }

    def 'rebuild indexes Claims from the blocks'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        claim(subject, 'set', 'name', 'kept')

        when:
        index.rebuild(store, 'lab')

        then:
        index.claimState(subject).names == ['kept']
    }
}
```

Add to `IndexSnapshotTest.groovy`:

```groovy
    def 'a member snapshot holds its own Claims, current state recomputed from them alone'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Cid first = lab.putDagCbor(Fixtures.claim(subject, 'set', 'name', 'first', []))
        StoreLog.append(lab, StoreLogKind.CLAIM, first, System.currentTimeMillis())
        // The rename is logged in shared only, so lab's snapshot must not see it.
        final Cid second = shared.putDagCbor(Fixtures.claim(subject, 'set', 'name', 'second', [first]))
        StoreLog.append(shared, StoreLogKind.CLAIM, second, System.currentTimeMillis())
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path

        then:
        index.claimState(subject).names == ['second']                       // the cache index sees both members
        rows(file, 'SELECT claim_cid FROM claim') == [[first.toString()]]
        rows(file, 'SELECT count(*) FROM claim_supersedes') == [[0]]
        rows(file, 'SELECT claim_cid, value, conflicted FROM claim_current') == [[first.toString(), 'first', 0]]
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexClaimsTest' --tests 'robsyme.cas.core.IndexSnapshotTest'`
Expected: FAIL: `claimState`, `isClaimIndexed`, `valueText` do not exist.

- [ ] **Step 3: Implement `ClaimCurrent.groovy`**

```groovy
// src/main/groovy/robsyme/cas/core/ClaimCurrent.groovy
package robsyme.cas.core

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

import groovy.transform.CompileStatic

/**
 * claim_current for one subject, from ClaimState over the Claims a database
 * holds for it. Shared by the cache index and the Index Snapshot, which must
 * recompute from the member's own Claims alone (block explorer spec section 4),
 * so both write the same rows by the same rules.
 */
@CompileStatic
final class ClaimCurrent {

    private ClaimCurrent() {}

    static ClaimState load(Connection c, String subject) {
        final Map<String, ClaimState.Row> rows = new LinkedHashMap<String, ClaimState.Row>()
        final PreparedStatement claims = c.prepareStatement('SELECT claim_cid, verb, attribute, value FROM claim WHERE subject_cid = ?')
        try {
            claims.setString(1, subject)
            final ResultSet rs = claims.executeQuery()
            while( rs.next() )
                rows.put(rs.getString(1), new ClaimState.Row(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), new ArrayList<String>()))
        }
        finally {
            claims.close()
        }
        final PreparedStatement supersedes = c.prepareStatement(
            'SELECT s.claim_cid, s.superseded_cid FROM claim_supersedes s JOIN claim k ON k.claim_cid = s.claim_cid WHERE k.subject_cid = ?')
        try {
            supersedes.setString(1, subject)
            final ResultSet rs = supersedes.executeQuery()
            while( rs.next() )
                rows.get(rs.getString(1))?.supersedes?.add(rs.getString(2))
        }
        finally {
            supersedes.close()
        }
        return ClaimState.of(rows.values())
    }

    /** Replaces the subject's claim_current rows. The caller owns the transaction. */
    static void rewrite(Connection c, String subject) {
        final ClaimState state = load(c, subject)
        final PreparedStatement delete = c.prepareStatement('DELETE FROM claim_current WHERE subject_cid = ?')
        try {
            delete.setString(1, subject)
            delete.executeUpdate()
        }
        finally {
            delete.close()
        }
        final PreparedStatement insert = c.prepareStatement(
            'INSERT INTO claim_current(subject_cid, attribute, value, claim_cid, conflicted) VALUES (?, ?, ?, ?, ?)')
        try {
            for( Map<String, Object> row : state.currentRows() ) {
                insert.setString(1, subject)
                insert.setObject(2, row.attribute)
                insert.setObject(3, row.value)
                insert.setString(4, (String) row.cid)
                insert.setInt(5, (Boolean) row.conflicted ? 1 : 0)
                insert.executeUpdate()
            }
        }
        finally {
            insert.close()
        }
    }
}
```

`claim.value` is already text (decision 4), so `ClaimState.Row.value` read back from the database is the stored text, which is what `claim_current.value` holds.

- [ ] **Step 4: Implement the index side**

In `Index.groovy`:

```groovy
    // Query 2 leaves out a run with a current delete Claim, a conflicted
    // deletion included (lineage spec §9, block explorer spec section 8).
    static final String SQL_LATEST_SUCCESSFUL_RUN =
        "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 AND NOT EXISTS (SELECT 1 FROM claim_current cc JOIN claim k ON k.claim_cid = cc.claim_cid WHERE cc.subject_cid = run.completion_cid AND cc.attribute IS NULL AND k.verb = 'delete') ORDER BY finished_at DESC, completion_cid ASC LIMIT 1"

    private static final String SQL_CONFLICTED_DELETIONS_OF_PIPELINE =
        "SELECT DISTINCT run.completion_cid FROM run JOIN claim_current cc ON cc.subject_cid = run.completion_cid WHERE run.pipeline = ? AND cc.attribute IS NULL AND cc.conflicted = 1"
```

```groovy
    Optional<Cid> latestSuccessfulRun(String pipeline) {
        query(SQL_CONFLICTED_DELETIONS_OF_PIPELINE, [pipeline]) { ResultSet rs ->
            warnOnce('conflicted-deletion:' + rs.getString(1),
                "run ${rs.getString(1)} of pipeline '${pipeline}' has a conflicted deletion (more than one current Claim); " +
                'the latest successful run leaves it out until a Claim superseding both resolves it')
        }
        return firstCid(SQL_LATEST_SUCCESSFUL_RUN, [pipeline])
    }
```

Add the Claim branch to `ingestLogged`, and make `ingestTolerant` take the kind:

```groovy
            case StoreLogKind.CLAIM:
                if( !isClaimIndexed(cid) )
                    ingestTolerant(store, kind, cid, member)
                return
```

```groovy
    private void ingestTolerant(BlockStore store, StoreLogKind kind, Cid cid, String member) {
        try {
            switch( kind ) {
                case StoreLogKind.RUN: ingestRun(store, cid, member); break
                case StoreLogKind.CLAIM: ingestClaim(store, cid, member); break
                default: return
            }
        }
        catch( IOException | UncheckedIOException e ) {
            warnOnce(cid.toString(), "${kind.token} ${cid} could not be read (${e.message}); it will be retried")
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [cid.toString()])
            insertMissing(null, cid)
        }
    }
```

(`scanOnce` calls `ingestTolerant(store, StoreLogKind.RUN, completion, member)`.)

```groovy
    /**
     * Indexes one Claim and rewrites its subject's claim_current. Idempotent.
     * An absent block is a `missing` row, retried by later catch-ups.
     */
    void ingestClaim(BlockStore store, Cid claimCid, String member) {
        final Map block = readBlock(store, claimCid, Records.CLAIM)
        withTransaction {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [claimCid.toString()])
            if( block == null ) {
                warnOnce(claimCid.toString(), "claim ${claimCid} has not arrived; it is indexed when it does")
                insertMissing(null, claimCid)
                return
            }
            final Claim claim = Claim.fromCbor(block)
            update('INSERT OR REPLACE INTO claim(claim_cid, subject_cid, verb, attribute, value, timestamp, asserted_by) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [claimCid.toString(), claim.subject.toString(), claim.verb, claim.attribute, valueText(claim.value), claim.timestamp, claim.assertedBy])
            update('DELETE FROM claim_supersedes WHERE claim_cid = ?', [claimCid.toString()])
            for( Cid superseded : claim.supersedes )
                update('INSERT INTO claim_supersedes(claim_cid, superseded_cid) VALUES (?, ?)', [claimCid.toString(), superseded.toString()])
            ClaimCurrent.rewrite(connection, claim.subject.toString())
        }
    }

    boolean isClaimIndexed(Cid claimCid) {
        boolean found = false
        query('SELECT 1 FROM claim WHERE claim_cid = ?', [claimCid.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /** The subject's current state from every Claim this index holds for it. */
    ClaimState claimState(Cid subject) {
        return ClaimCurrent.load(connection, subject.toString())
    }

    /** decision 4: a string as itself, null as NULL, anything else as DAG-JSON text. */
    static String valueText(Object value) {
        if( value == null )
            return null
        return value instanceof String ? (String) value : DagJson.encodeToString(value)
    }
```

Generalise the block scan so `scanOnce` and `rebuild` find Claims (and, in Task 6, Selections). Replace `runCompletionsIn` with:

```groovy
    /** The blocks of the kinds a store announces, found by decoding every `bafy…` block once. */
    private static Map<String, List<Cid>> blocksOfKinds(BlockStore store, Collection<String> kinds) {
        final Map<String, List<Cid>> found = new LinkedHashMap<String, List<Cid>>()
        for( String kind : kinds )
            found.put(kind, new ArrayList<Cid>())
        final Stream<Cid> blocks = store.listBlocks()
        try {
            for( Object element : blocks.toList() ) {
                final Cid cid = (Cid) element
                if( !cid.isDagCbor() )
                    continue
                final String kind = kindAt(store, cid)
                if( kind != null && found.containsKey(kind) )
                    found.get(kind).add(cid)
            }
        }
        finally {
            blocks.close()
        }
        for( List<Cid> list : found.values() )
            Collections.sort(list)
        return found
    }

    /** The kind of a metadata block, or null when it cannot be read or has none. */
    private static String kindAt(BlockStore store, Cid cid) {
        try {
            if( store.size(cid) > MAX_BLOCK_BYTES )
                return null
            final InputStream input = store.open(cid)
            try {
                final Object value = DagCbor.decode(input.readAllBytes())
                return value instanceof Map ? Records.kindOf((Map) value) : null
            }
            finally {
                input.close()
            }
        }
        catch( Exception e ) {
            return null
        }
    }

    /** What the scan and rebuild ingest, in dependency order: Claims refer to anything, so last. */
    private static final List<String> SCANNED_KINDS = [Records.RUN_COMPLETION, Records.CLAIM]
```

`scanOnce` becomes:

```groovy
        final Map<String, List<Cid>> found
        try {
            found = blocksOfKinds(store, SCANNED_KINDS)
        }
        catch( IOException | UncheckedIOException e ) {
            log.warn("could not scan the blocks of store member '$member' (${e.message}); will retry")
            return
        }
        for( Cid completion : found.get(Records.RUN_COMPLETION) )
            ingestLogged(store, StoreLogKind.RUN, completion, member)
        for( Cid claim : found.get(Records.CLAIM) )
            ingestLogged(store, StoreLogKind.CLAIM, claim, member)
        setMeta(key, 'done')
```

and `rebuild` ingests `found.get(Records.RUN_COMPLETION)` with `fresh.ingestRun` and `found.get(Records.CLAIM)` with `fresh.ingestClaim`, in that order. `Records.RUN_COMPLETION` is the existing kind-name constant.

- [ ] **Step 5: Copy the member's Claims into its snapshot**

In `IndexSnapshot.groovy`:

```groovy
    /** The Claims the member's Store Log announced; current state is recomputed from these alone (spec section 4). */
    private static final List<String> COPY_CLAIMS = [
        "INSERT INTO main.claim SELECT * FROM src.claim WHERE claim_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'claim')",
        'INSERT INTO main.claim_supersedes SELECT * FROM src.claim_supersedes WHERE claim_cid IN (SELECT claim_cid FROM main.claim)',
    ]
```

In `buildAndVacuum`, after `COPY_LOG_ENTRIES`: `update(c, COPY_CLAIMS[0], [(Object) member]); exec(c, COPY_CLAIMS[1])`. After `DETACH DATABASE src` and before counting runs, rewrite `claim_current` from the snapshot's own rows:

```groovy
            c.setAutoCommit(false)
            final List<String> subjects = new ArrayList<String>()
            final ResultSet rs = c.createStatement().executeQuery('SELECT DISTINCT subject_cid FROM claim')
            while( rs.next() )
                subjects.add(rs.getString(1))
            for( String subject : subjects )
                ClaimCurrent.rewrite(c, subject)
            c.commit()
            c.setAutoCommit(true)
```

Running it after the detach guarantees the unqualified table names in `ClaimCurrent` can only mean the snapshot's own tables.

- [ ] **Step 6: Update the page's copy of query 2 and re-run the guard**

Set `web/src/queries.json`'s `latestSuccessfulRun` to the new `SQL_LATEST_SUCCESSFUL_RUN` text, character for character.

Run: `./gradlew test --tests 'robsyme.cas.core.*'`
Expected: PASS, including `ExplorerQueriesTest` (the constants match, and the new subquery's plan searches `claim_current` through `claim_current_subject` and `claim` by its primary key, with no `SCAN`).

Run: `cd web && npm test`
Expected: PASS. The fixture snapshot has empty claim tables, so query 2's answers are unchanged.

- [ ] **Step 7: Amend `DESIGN.md` and run the Gate**

§12: replace "(delete claims arrive later)" with "and no current `delete` Claim names the RunCompletion, a conflicted deletion included; `latestSuccessfulRun` warns once per conflicted run it leaves out. Claims are ingested from `claim` Store Log entries into `claim` and `claim_supersedes`; `claim_current` is `ClaimState` (decision 5 of the milestone 2 plan) per subject." §15 Index Snapshot: "Claims: the `claim` and `claim_supersedes` rows of the Claims whose `log_entry` names the member, with `claim_current` recomputed from those alone."

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage 11/0/6, browser tier A 5/5. A2's `latest` cost must stay at or under 8 requests and 65536 bytes with the new `NOT EXISTS`; record the three costs in the commit message.

- [ ] **Step 8: Commit**

```bash
git add src/main/groovy/robsyme/cas/core src/test/groovy/robsyme/cas/core web/src/queries.json DESIGN.md
git commit -m "feat(index): Claims through the Store Log; claim_current; query 2 skips deleted runs

The Claims slice's index half (spec section 8): claim and
claim_supersedes from logged Claims, claim_current from ClaimState, a
member's snapshot recomputing current state from its own Claims alone,
and query 2 leaving out runs with a current delete Claim, conflicted
ones included with a warning. Year costs: <producers> / <latest> /
<query 3> requests.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The Selection block and Item Occurrences

The Selection of spec section 7.1 as a record whose constructor is the normalisation (sort, dedupe, merge `via`), so no caller can mint two addresses for one set of members. `ItemOccurrence` parses the `cas://<collection>/<item>[/<leaf>]` form (spec section 7.1a); `Cid.fromBytes` reads `derived_from`'s binary CIDs.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Cid.groovy` (add `fromBytes`)
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (add `static final String SELECTION = 'Selection'`)
- Create: `src/main/groovy/robsyme/cas/core/Selection.groovy`
- Create: `src/main/groovy/robsyme/cas/core/ItemOccurrence.groovy`
- Modify: `src/test/groovy/robsyme/cas/core/Fixtures.groovy` (add `selection(...)`)
- Create: `src/test/groovy/robsyme/cas/core/SelectionTest.groovy`, `ItemOccurrenceTest.groovy`

**Interfaces:**
- Consumes: `Records.head/expectKind/require/string/cid`, `Cid.bytes()`, `Multibase.base32Encode`.
- Produces:
  - `static Cid Cid.fromBytes(byte[] binary)`.
  - `class Selection` with nested `static final class Member(Cid address, boolean nested, List<Cid> via)`; factories `static Member Selection.item(Cid address, List<Cid> via)` and `static Member Selection.selection(Cid address)`; fields `String assertedBy`, `List<Member> members` (sorted by address string), `List<Cid> derivedFrom` (sorted); constructor `Selection(String assertedBy, List<Member> members, List<Cid> derivedFrom)` that merges, sorts and refuses an empty list or an address given both as an item and as a Selection (`IllegalArgumentException`); `Map<String,Object> toCbor()`; `static Selection fromCbor(Map)`.
  - `final class ItemOccurrence(Cid collection, Cid item, String leaf)` with `static ItemOccurrence parse(String text)` (null when the text is not of that shape) and `String toString()` (canonical).
  - `Fixtures.selection(List<Map> members, List<Cid> derivedFrom = [])` → a §6 map, members as given (tests build them sorted).

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/SelectionTest.groovy
package robsyme.cas.core

import spock.lang.Specification

/** Block explorer spec section 7: identity, order and the rules of a Selection. */
class SelectionTest extends Specification {

    static final Cid I1 = Fixtures.cidOf(Fixtures.outputItem([[sample: 'A']]))
    static final Cid I2 = Fixtures.cidOf(Fixtures.outputItem([[sample: 'B']]))
    static final Cid C1 = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
    static final Cid C2 = Fixtures.cidOf([kind: 'OutputCollection', n: 2])
    static final Cid S1 = Fixtures.cidOf([kind: 'Selection', n: 1])

    private static byte[] bytesOf(Selection s) { DagCbor.encode(s.toCbor()) }

    def 'members sort by address, each via is sorted, order given is not content'() {
        given:
        final Selection one = new Selection('ada', [Selection.item(I2, [C2, C1]), Selection.item(I1, []), Selection.selection(S1)], [])
        final Selection two = new Selection('ada', [Selection.selection(S1), Selection.item(I1, []), Selection.item(I2, [C1, C2])], [])

        expect:
        bytesOf(one) == bytesOf(two)
        one.members*.address == [I1, I2, S1].sort { it.toString() }
        one.members.find { it.address == I2 }.via == [C1, C2].sort { it.toString() }
    }

    def 'the same item picked twice is one member with the via of both (Review Focus 2)'() {
        given:
        final Selection merged = new Selection('ada', [Selection.item(I1, [C1]), Selection.item(I1, [C2]), Selection.item(I1, [C1])], [])

        expect:
        merged.members.size() == 1
        merged.members[0].via == [C1, C2].sort { it.toString() }
        bytesOf(merged) == bytesOf(new Selection('ada', [Selection.item(I1, [C2, C1])], []))
    }

    def 'the block is the §6 map, members a keyed union, derived_from binary cids'() {
        when:
        final Map block = new Selection('ada', [Selection.item(I1, [C1]), Selection.selection(S1)], [S1]).toCbor()

        then:
        block.kind == 'Selection'
        block.schema == 1L
        block.asserted_by == 'ada'
        block.members == [[item: [address: I1, via: [C1]]], [selection: S1]].sort { Map m -> ((m.item as Map)?.address ?: m.selection).toString() }
        (block.derived_from as List).size() == 1
        (block.derived_from as List)[0] == S1.bytes()
    }

    def 'it round-trips through DAG-CBOR and matches the Fixtures map'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(I1, [C1])], [S1])
        final byte[] bytes = bytesOf(s)

        expect:
        Selection.fromCbor((Map) DagCbor.decode(bytes)) == s
        DagCbor.cidOf(bytes) == Fixtures.cidOf(Fixtures.selection([[item: [address: I1, via: [C1]]]], [S1]))
    }

    def 'identity has no timestamp: two authors, two blocks; one author, one block'() {
        expect:
        bytesOf(new Selection('ada', [Selection.item(I1, [])], [])) == bytesOf(new Selection('ada', [Selection.item(I1, [])], []))
        bytesOf(new Selection('ada', [Selection.item(I1, [])], [])) != bytesOf(new Selection('bob', [Selection.item(I1, [])], []))
    }

    def 'refused: empty, and an address that is both an item and a Selection'() {
        when:
        new Selection('ada', members, [])

        then:
        thrown(IllegalArgumentException)

        where:
        members << [[], [Selection.item(I1, []), Selection.selection(I1)]]
    }

    def 'a member map with both keys, or neither, does not decode'() {
        when:
        Selection.fromCbor([kind: 'Selection', schema: 1L, asserted_by: 'ada', members: [member], derived_from: []])

        then:
        thrown(IllegalArgumentException)

        where:
        member << [[item: [address: I1, via: []], selection: S1], [other: I1]]
    }

    def 'Cid.fromBytes inverts bytes()'() {
        expect:
        Cid.fromBytes(I1.bytes()) == I1
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/core/ItemOccurrenceTest.groovy
package robsyme.cas.core

import spock.lang.Specification

/** Spec section 7.1a and DESIGN.md §7: cas://<OutputCollection cid>/<OutputItem cid>[/<leaf name>]. */
class ItemOccurrenceTest extends Specification {

    static final String C = Fixtures.cidOf([kind: 'OutputCollection', n: 1]).toString()
    static final String I = Fixtures.cidOf([kind: 'OutputItem', n: 1]).toString()

    def 'parses with and without a leaf, and prints canonically'() {
        expect:
        ItemOccurrence.parse("cas://$C/$I").with { it.collection.toString() == C && it.item.toString() == I && it.leaf == null }
        ItemOccurrence.parse("cas://$C/$I/A.bam").leaf == 'A.bam'
        ItemOccurrence.parse("cas://$C/$I/A.bam").toString() == "cas://$C/$I/A.bam"
    }

    def 'anything else is not an occurrence (#text)'() {
        expect:
        ItemOccurrence.parse(text) == null

        where:
        text << ["cas://$C", "cas://$C/aligned/A", "cas://$C/$I/", "cas://$C/$I/a/b", "lid://$C/$I",
                 "cas://lab/$I", "cas://${Fixtures.contentCid('x')}/$I", '', null]
    }
}
```

Add to `Fixtures.groovy`:

```groovy
    /** A Selection as §6 specifies it; the caller passes members already sorted by address. */
    static Map selection(List<Map> members, List<Cid> derivedFrom = []) {
        return [kind: 'Selection', schema: 1, asserted_by: ASSERTED_BY, members: members,
                derived_from: new ArrayList<Cid>(derivedFrom).sort { Cid c -> c.toString() }.collect { Cid c -> c.bytes() }]
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SelectionTest' --tests 'robsyme.cas.core.ItemOccurrenceTest'`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

In `Cid.groovy`:

```groovy
    /** The inverse of {@link #bytes()}: a binary CID, as a Selection's derived_from holds it. */
    static Cid fromBytes(byte[] binary) {
        if( binary == null || binary.length == 0 )
            throw new IllegalArgumentException('an empty binary cid')
        return parse(BASE32_PREFIX + Multibase.base32Encode(binary))
    }
```

(`BASE32_PREFIX` is the constant the private constructor already uses.)

```groovy
// src/main/groovy/robsyme/cas/core/Selection.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * A Collection a person assembles (block explorer spec section 7). Identity is
 * asserted_by, members and derived_from, with no timestamp. The constructor is
 * the normalisation: members sort by address string, each address appears
 * once (an item picked twice keeps the union of its via), each via and
 * derived_from sorts likewise. `via` is followed for metadata only;
 * `derived_from` holds weak references as bytes (spec section 7.3).
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Selection {

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Member {
        final Cid address
        /** True for a nested Selection, false for an Output Item. */
        final boolean nested
        /** The Output Collections the item was picked from; empty when chosen by a query, always empty when nested. */
        final List<Cid> via

        private Member(Cid address, boolean nested, Collection<Cid> via) {
            if( address == null )
                throw new IllegalArgumentException('a Selection member needs an address')
            this.address = address
            this.nested = nested
            this.via = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(via ?: Collections.<Cid> emptyList())))
        }
    }

    static Member item(Cid address, List<Cid> via) { new Member(address, false, via) }

    static Member selection(Cid address) { new Member(address, true, Collections.<Cid> emptyList()) }

    final String assertedBy
    final List<Member> members
    final List<Cid> derivedFrom

    Selection(String assertedBy, List<Member> members, List<Cid> derivedFrom) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Selection needs an asserted_by')
        if( !members )
            throw new IllegalArgumentException('an empty Selection is refused (block explorer spec section 7.2)')
        final TreeMap<Cid, Member> merged = new TreeMap<Cid, Member>()
        for( Member m : members ) {
            final Member seen = merged.get(m.address)
            if( seen == null ) {
                merged.put(m.address, m)
                continue
            }
            if( seen.nested != m.nested )
                throw new IllegalArgumentException("${m.address} is given both as an item and as a Selection")
            final List<Cid> via = new ArrayList<Cid>(seen.via)
            via.addAll(m.via)
            merged.put(m.address, new Member(m.address, m.nested, via))
        }
        this.assertedBy = assertedBy
        this.members = Collections.unmodifiableList(new ArrayList<Member>(merged.values()))
        this.derivedFrom = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(derivedFrom ?: Collections.<Cid> emptyList())))
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.SELECTION)
        map.put('asserted_by', assertedBy)
        map.put('members', members.collect { Member m ->
            m.nested
                ? ([selection: m.address] as Map<String, Object>)
                : ([item: [address: m.address, via: new ArrayList<Object>(m.via)] as Map<String, Object>] as Map<String, Object>)
        })
        map.put('derived_from', derivedFrom.collect { Cid c -> c.bytes() })
        return map
    }

    static Selection fromCbor(Map block) {
        Records.expectKind(block, Records.SELECTION)
        final List<Member> members = ((List) Records.require(block, 'members')).collect { Object entry ->
            if( !(entry instanceof Map) || ((Map) entry).size() != 1 )
                throw new IllegalArgumentException("a Selection member is a map with exactly one key, 'item' or 'selection': ${entry}")
            final Map m = (Map) entry
            if( m.containsKey('selection') )
                return selection(Records.cid(m.get('selection'), 'selection'))
            if( m.containsKey('item') && m.get('item') instanceof Map ) {
                final Map item = (Map) m.get('item')
                return Selection.item(Records.cid(Records.require(item, 'address'), 'address'),
                    ((List) Records.require(item, 'via')).collect { Object v -> Records.cid(v, 'via') })
            }
            throw new IllegalArgumentException("a Selection member is a map with exactly one key, 'item' or 'selection': ${entry}")
        }
        final List<Cid> derived = ((List) Records.require(block, 'derived_from')).collect { Object b ->
            if( !(b instanceof byte[]) )
                throw new IllegalArgumentException("derived_from holds binary cids, got ${b?.getClass()?.simpleName}")
            return Cid.fromBytes((byte[]) b)
        }
        return new Selection(Records.string(Records.require(block, 'asserted_by'), 'asserted_by'), members, derived)
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/ItemOccurrence.groovy
package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * One item as it appeared in one run's output (glossary: Item Occurrence;
 * DESIGN.md §7): cas://<OutputCollection cid>/<OutputItem cid>[/<leaf name>].
 * Parsing checks only the shape; whether the item is in the collection's
 * items is for the reader holding the collection to check.
 */
@Canonical
@CompileStatic
final class ItemOccurrence {

    static final String PREFIX = 'cas://'

    final Cid collection
    final Cid item
    final String leaf

    /** The occurrence this text names, or null when it is not of that shape. */
    static ItemOccurrence parse(String text) {
        if( text == null || !text.startsWith(PREFIX) )
            return null
        final String[] parts = text.substring(PREFIX.length()).split('/', -1)
        if( parts.length < 2 || parts.length > 3 )
            return null
        if( !Cid.isCid(parts[0]) || !Cid.isCid(parts[1]) )
            return null
        final Cid collection = Cid.parse(parts[0])
        final Cid item = Cid.parse(parts[1])
        if( !collection.isDagCbor() || !item.isDagCbor() )
            return null
        if( parts.length == 3 && parts[2].isEmpty() )
            return null
        return new ItemOccurrence(collection, item, parts.length == 3 ? parts[2] : null)
    }

    @Override
    String toString() {
        return PREFIX + collection + '/' + item + (leaf == null ? '' : '/' + leaf)
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.SelectionTest' --tests 'robsyme.cas.core.ItemOccurrenceTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Cid.groovy src/main/groovy/robsyme/cas/core/Records.groovy \
        src/main/groovy/robsyme/cas/core/Selection.groovy src/main/groovy/robsyme/cas/core/ItemOccurrence.groovy \
        src/test/groovy/robsyme/cas/core/Fixtures.groovy src/test/groovy/robsyme/cas/core/SelectionTest.groovy \
        src/test/groovy/robsyme/cas/core/ItemOccurrenceTest.groovy
git commit -m "feat(core): the Selection block, normalised by construction; Item Occurrence URIs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Selections in the index and the snapshot

Catch-up ingests logged Selections into `collection` (kind `selection`), `collection_item` (one row per item and `via`), `selection_child` and `selection_derived` (spec section 11). Nesting resolves at query time through a recursive CTE, and `selectionItems` fails naming any nested Selection the index does not hold (decision 16). A member's snapshot carries the Selections it logged, with membership rows but no metadata for items held elsewhere (spec section 4).

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy`
- Create: `src/test/groovy/robsyme/cas/core/IndexSelectionsTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy`
- Modify: `DESIGN.md` §12, §15

**Interfaces:**
- Consumes: `Selection.fromCbor`, `Selection.Member`, Task 1's `ingestLogged`, Task 4's `ingestTolerant(store, kind, cid, member)`, `blocksOfKinds`, `SCANNED_KINDS`.
- Produces:
  - `void Index.ingestSelection(BlockStore store, Cid selection, String member)`.
  - `boolean Index.isSelectionIndexed(Cid selection)`.
  - `static final String Index.SQL_SELECTION_ITEMS` (the CTE) and `List<Cid> Index.selectionItems(Cid selection)`: every distinct item reached, nested included, sorted by CID string; `IllegalStateException` naming each Selection reached but not indexed.
  - Snapshot: the Selections whose `log_entry` names the member, their `collection_item`, `selection_child` and `selection_derived` rows.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/IndexSelectionsTest.groovy
package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 11: Selections as Collections, nesting by recursive CTE. */
class IndexSelectionsTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    long clock = 1_758_000_000_000L

    Cid itemA, itemB, itemC, coll1, coll2

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        itemA = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        itemB = store.putDagCbor(Fixtures.outputItem([[sample: 'B']]))
        itemC = store.putDagCbor(Fixtures.outputItem([[sample: 'C']]))
        coll1 = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
        coll2 = Fixtures.cidOf([kind: 'OutputCollection', n: 2])
    }

    def cleanup() {
        index?.close()
    }

    private Cid logged(Selection s) {
        final Cid cid = store.putDagCbor(s.toCbor())
        StoreLog.append(store, StoreLogKind.SELECTION, cid, clock += 1000)
        return cid
    }

    private void catchUp() { index.catchUp(store, StoreLog.of(store), 'lab') }

    private List<List<Object>> rows(String sql) {
        final def c = DriverManager.getConnection("jdbc:sqlite:${index.file}")
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

    def 'a logged Selection is a collection of kind selection, one collection_item row per item and via'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(itemA, [coll1, coll2]), Selection.item(itemB, [])], [coll1])
        final Cid cid = logged(s)

        when:
        catchUp()

        then:
        rows("SELECT kind, completion_cid, output_name, asserted_by FROM collection WHERE collection_cid = '$cid'") ==
            [['selection', null, null, 'ada']]
        rows("SELECT item_cid, via_cid FROM collection_item WHERE collection_cid = '$cid' ORDER BY item_cid, via_cid") ==
            ([[itemA.toString(), coll1.toString()], [itemA.toString(), coll2.toString()]].sort { it[1] } + [[itemB.toString(), null]])
                .sort { a, b -> a[0] <=> b[0] ?: (a[1] ?: '') <=> (b[1] ?: '') }
        rows("SELECT derived_from_cid FROM selection_derived WHERE selection_cid = '$cid'") == [[coll1.toString()]]
        index.isSelectionIndexed(cid)
    }

    def 'nesting resolves through the CTE, each item once (spec section 10)'() {
        given:
        final Cid inner = logged(new Selection('ada', [Selection.item(itemA, [coll1]), Selection.item(itemB, [coll1])], []))
        final Cid outer = logged(new Selection('ada', [Selection.selection(inner), Selection.item(itemB, [coll2]), Selection.item(itemC, [])], []))
        catchUp()

        expect:
        rows("SELECT child_cid FROM selection_child WHERE parent_cid = '$outer'") == [[inner.toString()]]
        index.selectionItems(outer) == [itemA, itemB, itemC].sort { it.toString() }
        index.selectionItems(inner) == [itemA, itemB].sort { it.toString() }
    }

    def 'a nested Selection the index does not hold fails, naming it (Review Focus 5)'() {
        given:
        final Cid absent = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid outer = logged(new Selection('ada', [Selection.selection(absent), Selection.item(itemC, [])], []))
        catchUp()

        when:
        index.selectionItems(outer)

        then:
        final IllegalStateException e = thrown()
        e.message.contains(absent.toString())
    }

    def 'a Selection whose block has not arrived is missing, then indexed when it lands'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(itemA, [])], [])
        final Cid cid = DagCbor.cidOf(DagCbor.encode(s.toCbor()))
        StoreLog.append(store, StoreLogKind.SELECTION, cid, clock += 1000)
        catchUp()

        expect:
        !index.isSelectionIndexed(cid)
        rows('SELECT needed_cid FROM missing') == [[cid.toString()]]

        when:
        store.putDagCbor(s.toCbor())
        catchUp()

        then:
        index.isSelectionIndexed(cid)
        index.selectionItems(cid) == [itemA]
    }

    def 'rebuild indexes Selections from the blocks'() {
        given:
        final Cid cid = logged(new Selection('ada', [Selection.item(itemA, [])], []))

        when:
        index.rebuild(store, 'lab')

        then:
        index.selectionItems(cid) == [itemA]
    }
}
```

Add to `IndexSnapshotTest.groovy`:

```groovy
    def 'a member snapshot carries its own Selections; an item held elsewhere keeps its row but no metadata'() {
        given:
        final List a = run(lab, 'a', ['A'])
        final List b = run(shared, 'b', ['B'])
        final Cid itemA = ((Map<String, Cid>) a[2]).A
        final Cid itemB = ((Map<String, Cid>) b[2]).B
        final Cid mine = lab.putDagCbor(new Selection('ada', [Selection.item(itemA, [(Cid) a[1]]), Selection.item(itemB, [(Cid) b[1]])], []).toCbor())
        StoreLog.append(lab, StoreLogKind.SELECTION, mine, System.currentTimeMillis())
        final Cid theirs = shared.putDagCbor(new Selection('bob', [Selection.item(itemB, [])], []).toCbor())
        StoreLog.append(shared, StoreLogKind.SELECTION, theirs, System.currentTimeMillis())
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path

        then:
        rows(file, "SELECT collection_cid FROM collection WHERE kind = 'selection'") == [[mine.toString()]]
        rows(file, "SELECT item_cid FROM collection_item WHERE collection_cid = '${mine}' ORDER BY item_cid")*.get(0) ==
            [itemA.toString(), itemB.toString()].sort()
        rows(file, "SELECT DISTINCT item_cid FROM item_attr ORDER BY item_cid")*.get(0) == [itemA.toString()]
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexSelectionsTest' --tests 'robsyme.cas.core.IndexSnapshotTest'`
Expected: FAIL: `isSelectionIndexed`, `selectionItems` do not exist.

- [ ] **Step 3: Implement in `Index.groovy`**

```groovy
    /** Every item a Selection reaches, nested Selections included, each once (spec section 11). */
    static final String SQL_SELECTION_ITEMS = '''WITH RECURSIVE reach(cid) AS (
            SELECT ? UNION SELECT s.child_cid FROM selection_child s JOIN reach r ON s.parent_cid = r.cid)
        SELECT DISTINCT ci.item_cid FROM collection_item ci JOIN reach r ON ci.collection_cid = r.cid ORDER BY ci.item_cid'''

    private static final String SQL_SELECTIONS_NOT_INDEXED = '''WITH RECURSIVE reach(cid) AS (
            SELECT ? UNION SELECT s.child_cid FROM selection_child s JOIN reach r ON s.parent_cid = r.cid)
        SELECT r.cid FROM reach r LEFT JOIN collection c ON c.collection_cid = r.cid AND c.kind = 'selection'
        WHERE c.collection_cid IS NULL ORDER BY r.cid'''

    /**
     * Indexes one Selection. Idempotent: its rows are replaced. The item
     * blocks are not read here: an item's metadata is indexed with the run
     * that produced it, and a Selection may name items another member holds.
     */
    void ingestSelection(BlockStore store, Cid selectionCid, String member) {
        final Map block = readBlock(store, selectionCid, Records.SELECTION)
        withTransaction {
            final String text = selectionCid.toString()
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [text])
            update('DELETE FROM collection_item WHERE collection_cid = ?', [text])
            update('DELETE FROM selection_child WHERE parent_cid = ?', [text])
            update('DELETE FROM selection_derived WHERE selection_cid = ?', [text])
            update("DELETE FROM collection WHERE collection_cid = ? AND kind = 'selection'", [text])
            if( block == null ) {
                warnOnce(text, "selection ${selectionCid} has not arrived; it is indexed when it does")
                insertMissing(null, selectionCid)
                return
            }
            final Selection selection = Selection.fromCbor(block)
            update("INSERT INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, 'selection', NULL, NULL, ?)",
                [text, selection.assertedBy])
            for( Selection.Member m : selection.members ) {
                if( m.nested ) {
                    update('INSERT INTO selection_child(parent_cid, child_cid) VALUES (?, ?)', [text, m.address.toString()])
                    continue
                }
                update('INSERT OR IGNORE INTO item(item_cid) VALUES (?)', [m.address.toString()])
                if( m.via.isEmpty() )
                    update('INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, NULL)', [text, m.address.toString()])
                for( Cid via : m.via )
                    update('INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, ?)', [text, m.address.toString(), via.toString()])
            }
            for( Cid derived : selection.derivedFrom )
                update('INSERT INTO selection_derived(selection_cid, derived_from_cid) VALUES (?, ?)', [text, derived.toString()])
        }
    }

    boolean isSelectionIndexed(Cid selectionCid) {
        boolean found = false
        query("SELECT 1 FROM collection WHERE collection_cid = ? AND kind = 'selection'", [selectionCid.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Every distinct item the Selection reaches, sorted by CID string. Refuses
     * to answer with a partial set: a Selection reached but not indexed (not
     * arrived, or held in a member this composition does not include) fails.
     */
    List<Cid> selectionItems(Cid selectionCid) {
        final List<String> absent = new ArrayList<String>()
        query(SQL_SELECTIONS_NOT_INDEXED, [selectionCid.toString()]) { ResultSet rs -> absent.add(rs.getString(1)) }
        if( absent )
            throw new IllegalStateException("selection ${selectionCid} reaches ${absent.size() == 1 ? 'a Selection' : 'Selections'} " +
                "this index does not hold: ${absent.join(', ')}; it may be in a member this composition does not include")
        final List<Cid> items = new ArrayList<Cid>()
        query(SQL_SELECTION_ITEMS, [selectionCid.toString()]) { ResultSet rs -> items.add(Cid.parse(rs.getString(1))) }
        return items
    }
```

Add the branch to `ingestLogged` and `ingestTolerant`:

```groovy
            case StoreLogKind.SELECTION:
                if( !isSelectionIndexed(cid) )
                    ingestTolerant(store, kind, cid, member)
                return
```

```groovy
                case StoreLogKind.SELECTION: ingestSelection(store, cid, member); break
```

Set `SCANNED_KINDS = [Records.RUN_COMPLETION, Records.SELECTION, Records.CLAIM]` and ingest `found.get(Records.SELECTION)` between runs and Claims in `scanOnce` and `rebuild`.

`clearRun` deletes `collection_item` rows of the run's collections by `collection_cid IN (SELECT collection_cid FROM collection WHERE completion_cid = ?)`; Selection rows have a NULL `completion_cid`, so a run's re-ingest never touches them. Check that by reading `clearRun` once more; change nothing if so.

- [ ] **Step 4: Copy the member's Selections into its snapshot**

```groovy
    /** The Selections the member logged, after the run closure so item and item_attr stay the runs' (spec section 4). */
    private static final List<String> COPY_SELECTIONS = [
        "INSERT INTO main.collection SELECT * FROM src.collection WHERE kind = 'selection' AND collection_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'selection')",
        "INSERT INTO main.collection_item SELECT * FROM src.collection_item WHERE collection_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
        "INSERT INTO main.selection_child SELECT * FROM src.selection_child WHERE parent_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
        "INSERT INTO main.selection_derived SELECT * FROM src.selection_derived WHERE selection_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
    ]
```

In `buildAndVacuum`, after `COPY_LOG_ENTRIES` and before `COPY_CLAIMS`: `update(c, COPY_SELECTIONS[0], [(Object) member])`, then `exec` the other three. `COPY_CLOSURE`'s `main.item` and `main.item_attr` statements have already run by then, so they hold only the runs' items.

- [ ] **Step 5: Run the tests, amend `DESIGN.md`, commit**

Run: `./gradlew test --tests 'robsyme.cas.core.*'`
Expected: PASS.

§12: "Selections are ingested from `selection` Store Log entries (spec section 11); `Index.selectionItems` resolves nesting with `SQL_SELECTION_ITEMS` and refuses a partial answer." §15 Index Snapshot: "Selections: those whose `log_entry` names the member, with their `collection_item`, `selection_child` and `selection_derived` rows. An item another member produced keeps its membership row and has no `item_attr` rows; the page shows it as held elsewhere."

```bash
git add src/main/groovy/robsyme/cas/core src/test/groovy/robsyme/cas/core DESIGN.md
git commit -m "feat(index): Selections through the Store Log, nesting by recursive CTE, in the snapshot

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Item Occurrence URIs in the provider

DESIGN §7 puts `cas://<collection>/<item>[/<leaf>]` in milestone 2. The provider reads it: without a leaf name, a directory of the item's leaves by name; with one, that leaf (a file, or a directory manifest that can be walked further). Publish-path traversal of a collection root stays unsupported, and the error names both readings.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy` (`CasNode`, `resolveStoreUri`, `newDirectoryStream`, `download`; new `blockOf`, `occurrence`, `leavesByName`, `materialiseOccurrence`)
- Create: `src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy`
- Modify: `DESIGN.md` §7 (drop "Built with the explorer's milestone 2", say it is built)

**Interfaces:**
- Consumes: `OutputCollection.fromCbor`, `OutputItem.fromCbor` (its `value` holds `Leaf` instances), `Leaf.isAddressed()`, `Records.kindOf`, `Records.OUTPUT_COLLECTION`.
- Produces: `Files.readAttributes`, `Files.newDirectoryStream`, `Files.newInputStream` and `download()` over `cas://<collection>/<item>` and `cas://<collection>/<item>/<leaf>[/<entry>...]`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy
package robsyme.cas.nio

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.DirectoryManifest
import robsyme.cas.core.Leaf
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.ManifestEntry
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunManifest
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §7: an Item Occurrence read through the provider. */
class CasOccurrenceTest extends Specification {

    @TempDir Path tmp

    CasFileSystemProvider provider
    LocalBlockStore store
    Session session
    Cid bam, qcDir, collection, item, dupItem, dupCollection

    def setup() {
        final storeDir = tmp.resolve('store')
        Files.createDirectories(storeDir)
        final config = CasConfig.from(
                [lineage: [store: [location: 'cas://lab']], cas: [stores: [lab: [location: storeDir.toString()]]]],
                'cas://lab')
        store = new LocalBlockStore(storeDir, 'lab', true)
        session = Mock(Session)
        Global.session = session
        CasSession.bind(session, new CasSession(config, store, new CoordinateTree(storeDir.resolve('coords'))))
        provider = new CasFileSystemProvider()

        bam = store.putStreaming(new ByteArrayInputStream('BAM A\n'.bytes))
        final Cid summary = store.putStreaming(new ByteArrayInputStream('summary\n'.bytes))
        qcDir = store.putDagCbor(new DirectoryManifest([ManifestEntry.regular('summary.txt', summary, 8L)]).toCbor())
        final Cid manifest = store.putDagCbor(new RunManifest([assertedBy: 'test', pipeline: 'p', runName: 'r', nfRunHash: 'h',
            sessionId: 's', nextflowVersion: '26.04.6', params: [:], config: [:], startedAt: '2026-09-25T00:00:00.000Z']).toCbor())
        item = store.putDagCbor(OutputItem.of([[sample: 'A'], Leaf.of('A.bam', bam, 6L, 'head-node'), Leaf.of('A_qc', qcDir, 0L, 'head-node')]).toCbor())
        collection = store.putDagCbor(new OutputCollection('test', manifest, 'aligned', [item], [['aligned/A/A.bam', 'qc/A']]).toCbor())
        dupItem = store.putDagCbor(OutputItem.of([[sample: 'D'], Leaf.of('x.txt', bam, 6L, 'head-node'), Leaf.of('x.txt', bam, 6L, 'head-node')]).toCbor())
        dupCollection = store.putDagCbor(new OutputCollection('test', manifest, 'dups', [dupItem], [['d/1/x.txt', 'd/2/x.txt']]).toCbor())
    }

    def cleanup() {
        CasSession.unbind(session)
        Global.session = null
    }

    private CasPath p(String uri) { (CasPath) provider.getPath(URI.create(uri)) }

    def 'an occurrence with a leaf name is that file'() {
        when:
        final CasPath path = p("cas://${collection}/${item}/A.bam")

        then:
        provider.readAttributes(path, java.nio.file.attribute.BasicFileAttributes).isRegularFile()
        provider.newInputStream(path).text == 'BAM A\n'
    }

    def 'an occurrence without a leaf is a directory of its leaves by name (decision 17)'() {
        when:
        final CasPath path = p("cas://${collection}/${item}")
        final List<String> names = provider.newDirectoryStream(path, null).collect { it.fileName.toString() }.sort()

        then:
        provider.readAttributes(path, java.nio.file.attribute.BasicFileAttributes).isDirectory()
        names == ['A.bam', 'A_qc']
    }

    def 'a directory leaf is walked further'() {
        expect:
        provider.newInputStream(p("cas://${collection}/${item}/A_qc/summary.txt")).text == 'summary\n'
    }

    def 'download of an occurrence stages its leaves'() {
        given:
        final Path target = tmp.resolve('staged')

        when:
        provider.download(p("cas://${collection}/${item}"), target)

        then:
        Files.readString(target.resolve('A.bam')) == 'BAM A\n'
        Files.readString(target.resolve('A_qc/summary.txt')) == 'summary\n'
    }

    def 'a first segment that is not an item of the collection names both readings'() {
        when:
        provider.readAttributes(p("cas://${collection}/aligned/A/A.bam"), java.nio.file.attribute.BasicFileAttributes)

        then:
        final IOException e = thrown()
        e.message.contains('neither an item of collection')
        e.message.contains('publish path')
    }

    def 'an unknown leaf name is absent'() {
        when:
        provider.readAttributes(p("cas://${collection}/${item}/nope.bam"), java.nio.file.attribute.BasicFileAttributes)

        then:
        thrown(NoSuchFileException)
    }

    def 'a leaf name two leaves share is refused, naming both positions'() {
        when:
        provider.readAttributes(p("cas://${dupCollection}/${dupItem}/x.txt"), java.nio.file.attribute.BasicFileAttributes)

        then:
        final IOException e = thrown()
        e.message.contains("'x.txt'")
        e.message.contains('1') && e.message.contains('2')
    }
}
```

Check `DirectoryManifest`'s constructor and `RunManifest`'s map keys against `Records.groovy` before running; adjust the fixture calls to what the classes take (the test's intent is fixed, not these literal argument lists).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.nio.CasOccurrenceTest'`
Expected: FAIL: an Output Collection root is read as a Directory Manifest (`expected a DirectoryManifest block, got OutputCollection`).

- [ ] **Step 3: Implement**

Add a field to `CasNode`:

```groovy
        // An Item Occurrence without a leaf name: a directory of the item's leaves by name (DESIGN.md §7).
        Map<String, Leaf> occurrence
```

In `resolveStoreUri`, replace the dag-cbor tail:

```groovy
        // A dag-cbor root: a Directory Manifest, or an Output Collection naming an Item Occurrence (DESIGN.md §7).
        final Map root = blockOf(cid)
        if( root != null && Records.kindOf(root) == Records.OUTPUT_COLLECTION )
            return occurrence(cid, root, segs, p)
        if( segs.isEmpty() )
            return manifestNode(cid)
        return traverse(cid, segs, p)
```

```groovy
    /** A decoded metadata block, or null when the store does not hold it. */
    private Map blockOf(Cid cid) {
        if( !store().has(cid) )
            return null
        final InputStream input = store().open(cid)
        try {
            final Object value = DagCbor.decode(input.readAllBytes())
            return value instanceof Map ? (Map) value : null
        }
        finally {
            input.close()
        }
    }

    /**
     * cas://<collection>/<item>[/<leaf>[/<entry>...]]. The first segment names
     * an occurrence when it is one of the collection's items; publish-path
     * traversal of a collection is not built, so anything else is an error
     * naming both readings.
     */
    private CasNode occurrence(Cid collectionCid, Map root, List<String> segs, CasPath p) {
        final OutputCollection collection = OutputCollection.fromCbor(root)
        if( segs.isEmpty() )
            throw new IOException("cas: ${p} is an Output Collection; name one of its items, cas://${collectionCid}/<item>")
        final String first = segs[0]
        if( !Cid.isCid(first) || !collection.items.contains(Cid.parse(first)) )
            throw new IOException("cas: '${first}' in ${p} is neither an item of collection ${collectionCid} (an Item Occurrence) " +
                'nor a publish path this version can traverse')
        final Cid itemCid = Cid.parse(first)
        final Map itemBlock = blockOf(itemCid)
        if( itemBlock == null )
            return CasNode.absent()
        final Map<String, Leaf> leaves = leavesByName(OutputItem.fromCbor(itemBlock).value, p)
        if( segs.size() == 1 )
            return new CasNode(present: true, directory: true, size: 0L, mtime: store().lastModifiedMillis(itemCid), occurrence: leaves)
        final Leaf leaf = leaves.get(segs[1])
        if( leaf == null || !leaf.addressed )
            return CasNode.absent()
        if( leaf.address.isRaw() )
            return segs.size() == 2 ? fileNode(leaf.address) : CasNode.absent()
        return segs.size() == 2 ? manifestNode(leaf.address) : traverse(leaf.address, segs.subList(2, segs.size()), p)
    }

    /** The item's named leaves by name; a name two leaves share is refused, naming both positions. */
    private static Map<String, Leaf> leavesByName(Object value, CasPath p) {
        final Map<String, Leaf> byName = new LinkedHashMap<String, Leaf>()
        final Map<String, String> positions = new HashMap<String, String>()
        collectLeaves(value, '', byName, positions, p)
        return byName
    }

    private static void collectLeaves(Object value, String position, Map<String, Leaf> byName, Map<String, String> positions, CasPath p) {
        if( value instanceof Leaf ) {
            final Leaf leaf = (Leaf) value
            if( leaf.name == null )
                return
            if( byName.containsKey(leaf.name) )
                throw new IOException("cas: two leaves of the item in ${p} are named '${leaf.name}' (positions ${positions.get(leaf.name)} and ${position}); " +
                    'address one by its content, cas://<cid>/<name>')
            byName.put(leaf.name, leaf)
            positions.put(leaf.name, position)
            return
        }
        if( value instanceof Map ) {
            for( Map.Entry e : ((Map) value).entrySet() )
                collectLeaves(e.value, position ? "${position}.${e.key}".toString() : String.valueOf(e.key), byName, positions, p)
            return
        }
        if( value instanceof List ) {
            final List list = (List) value
            for( int i = 0; i < list.size(); i++ )
                collectLeaves(list[i], position ? "${position}.${i}".toString() : String.valueOf(i), byName, positions, p)
        }
    }
```

In `newDirectoryStream`, before `if( node.content != null )`:

```groovy
        if( node.occurrence != null ) {
            for( Map.Entry<String, Leaf> e : node.occurrence.entrySet() )
                if( e.value.addressed )
                    children.add(p.resolve(e.key))
        }
        else if( node.content != null ) {
```

(and turn the existing `if/else` into the `else if/else` tail). In `download`, before the `node.directory || node.dirPointer` branch:

```groovy
        if( node.occurrence != null ) {
            materialiseOccurrence(node.occurrence, target)
            return
        }
```

```groovy
    /** Stages each addressed leaf of an occurrence under {@code dir} by its name. */
    private void materialiseOccurrence(Map<String, Leaf> leaves, Path dir) throws IOException {
        Files.createDirectories(dir)
        for( Map.Entry<String, Leaf> e : leaves.entrySet() ) {
            final Leaf leaf = e.value
            if( !leaf.addressed )
                continue
            if( leaf.address.isRaw() )
                materialiseFile(leaf.address, dir.resolve(e.key), false, true)
            else
                materialiseDirectory(leaf.address, dir.resolve(e.key))
        }
    }
```

Any other method that assumes a directory node has `content` (search `node.content` and `node.directory`) must treat `occurrence != null` as a directory without content; `checkAccess` and `readAttributes` only read `present`, `directory`, `size` and `mtime`, which the occurrence node sets.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.nio.*'`
Expected: PASS, the existing provider tests unchanged.

- [ ] **Step 5: Amend `DESIGN.md` §7 and commit**

Replace "Built with the explorer's milestone 2 (block explorer spec section 7.4)." with "Built 2026-09-25 (milestone 2, Task 7): a leaf name two leaves of one item share is refused, naming both positions."

```bash
git add src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy DESIGN.md
git commit -m "feat(nio): Item Occurrence URIs, cas://<collection>/<item>[/<leaf>]

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `Put`, the one builder

Spec section 9: a block is a pure function of the request plus the server's `asserted_by`. `Put` decodes DAG-JSON, checks the shape, builds the record (whose constructor normalises), encodes, and knows the address before any semantic check. Then: dry run, or return an existing block unchanged (decision 9), or validate, write to the writable member, append the Store Log entry and ingest. `nf-blocks:put` (Task 9) and `POST /api/put` (Task 10) call only this.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/PutError.groovy`, `PutResult.groovy`, `Put.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (add `supersedersOf`)
- Create: `src/test/groovy/robsyme/cas/core/PutTest.groovy`

**Interfaces:**
- Consumes: `DagJson.decode`/`DagJsonException`, `Selection`, `Selection.item/selection`, `ItemOccurrence.parse`, `Claim`, `Cid.fromBytes`, `DagCbor.encode/cidOf/decode`, `StoreLog.append/entryName/of`, `Index.catchUp`, `Index.claimState`, `Index.firstLogEntry`, `BlockStore.has/put/open/size/isWritable/alias`.
- Produces:
  - `class PutError extends RuntimeException` with constants `NOT_FOUND`, `WRONG_KIND`, `NOT_IN_VIA`, `EMPTY`, `STALE_SUPERSEDES`, `CLOCK_SKEW`, `TOO_LARGE`, `NOT_WRITABLE`, `INVALID`; `final String code`, `final String at`; `int getStatus()` (409 for `not_writable`, else 400); `byte[] body()` (DAG-JSON `{error, message, at}`).
  - `final class PutResult` with `Cid address`, `boolean dryRun`, `boolean exists`, `List<String> names`, `Map<String,Object> block`, `String entry`, `boolean written`; `static PutResult dryRun(Cid, boolean exists, List<String> names)`; `static PutResult written(Cid, Map block, String entry, boolean written)`; `byte[] body()`.
  - `class Put` with `Put(BlockStore store, BlockStore writable, Index index, String assertedBy, Closure<Long> clock, Closure catchUp)`, `synchronized PutResult put(byte[] body, boolean dryRun)`, `synchronized PutResult put(Object request, boolean dryRun)`; constants `MAX_BLOCK_BYTES = 1048576`, `MAX_REQUEST_BYTES = 2097152`, `CLOCK_SKEW_MILLIS = 600000`, `MAX_NAME_CHARS = 256`. `store` is the composition (reads), `writable` its first member (writes); `catchUp` brings `index` up to date from every member.
  - `List<Cid> Index.supersedersOf(Cid claim)`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/PutTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 9: deterministic construction, validation, idempotence. */
class PutTest extends Specification {

    static final long NOW = 1_758_000_000_000L                // 2025-09-16T05:20:00.000Z
    static final String TS = '2025-09-16T05:20:00.000Z'

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    Put put
    long now = NOW
    Cid manifest, itemA, itemB, collA, collB

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        put = newPut(store)
        manifest = store.putDagCbor(Fixtures.runManifest())
        itemA = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        itemB = store.putDagCbor(Fixtures.outputItem([[sample: 'B'], Fixtures.leaf('B.bam', Fixtures.contentCid('B'), 1L)]))
        collA = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[itemA, ['aligned/A.bam']], [itemB, ['aligned/B.bam']]]))
        final Cid other = store.putDagCbor(Fixtures.runManifest(run_name: 'other', nf_run_hash: 'other'))
        collB = store.putDagCbor(Fixtures.outputCollection(other, 'aligned', [[itemA, ['x/A.bam']]]))
    }

    def cleanup() {
        index?.close()
    }

    private Put newPut(BlockStore s) {
        return new Put(s, s, index, 'ada', { now }, { index.catchUp(store, StoreLog.of(store), 'lab') })
    }

    private static String link(Cid c) { '{"/":"' + c + '"}' }

    private static String links(List<Cid> cs) { '[' + cs.collect { link(it) }.join(',') + ']' }

    private static String selection(String members) { '{"kind":"Selection","members":[' + members + '],"derived_from":[]}' }

    private static String item(Cid address, List<Cid> via) { '{"item":{"address":' + link(address) + ',"via":' + links(via) + '}}' }

    private static String claim(Cid subject, String verb, String attribute, String valueJson, List<Cid> supersedes, String ts = TS) {
        return '{"kind":"Claim","subject":' + link(subject) + ',"verb":"' + verb + '","attribute":' +
            (attribute == null ? 'null' : '"' + attribute + '"') + ',"value":' + valueJson +
            ',"supersedes":' + links(supersedes) + ',"timestamp":"' + ts + '"}'
    }

    private PutResult send(String json, boolean dry = false) { put.put(json.getBytes('UTF-8'), dry) }

    private PutError refused(String json) {
        try {
            send(json)
            return null
        }
        catch( PutError e ) {
            return e
        }
    }

    def 'a Selection is normalised, written, logged and indexed; its address is its canonical block address'() {
        when:
        final PutResult r = send(selection(item(itemB, [collA]) + ',' + item(itemA, [collB, collA])))
        final Map expected = new Selection('ada', [Selection.item(itemA, [collA, collB]), Selection.item(itemB, [collA])], []).toCbor()

        then:
        r.written
        r.address == DagCbor.cidOf(DagCbor.encode(expected))
        store.has(r.address)
        StoreLog.read(store)*.name == [r.entry]
        r.entry == StoreLog.entryName(StoreLogKind.SELECTION, r.address, NOW)
        index.isSelectionIndexed(r.address)
        ((Map) DagJson.decode(r.body())).block == expected
        ((Map) DagJson.decode(r.body())).address == r.address
    }

    def 'an occurrence URI and an item map for one item are one member, in either order (Review Focus 2)'() {
        given:
        final String occurrence = '"cas://' + collA + '/' + itemA + '"'

        expect:
        send(selection(occurrence + ',' + item(itemA, [collB])), true).address ==
            send(selection(item(itemA, [collB]) + ',' + occurrence), true).address
        send(selection(occurrence + ',' + item(itemA, [collB])), true).address ==
            DagCbor.cidOf(DagCbor.encode(new Selection('ada', [Selection.item(itemA, [collA, collB])], []).toCbor()))
    }

    def 'a retried identical request writes no second block and no second entry'() {
        given:
        final String json = selection(item(itemA, [collA]))
        final PutResult first = send(json)
        now += 60_000

        when:
        final PutResult again = send(json)

        then:
        !again.written
        again.address == first.address
        again.entry == first.entry
        StoreLog.read(store).size() == 1
    }

    def 'a dry run writes nothing and reports existence and current names'() {
        given:
        final String json = selection(item(itemA, [collA]))

        when:
        final PutResult dry = send(json, true)

        then:
        dry.dryRun
        !dry.exists
        dry.names == []
        !store.has(dry.address)
        StoreLog.read(store).isEmpty()

        when:
        final Cid written = send(json).address
        send(claim(written, 'set', 'name', '"first"', []))
        final PutResult again = send(json, true)

        then:
        again.exists
        again.names == ['first']
        ((Map) DagJson.decode(again.body())) == [address: written, exists: true, names: ['first']]
    }

    def 'rename, delete and undo, with supersedes checked against what the index knows'() {
        given:
        final Cid s = send(selection(item(itemA, [collA]))).address
        final Cid named = send(claim(s, 'set', 'name', '"first"', [])).address

        when:
        final Cid renamed = send(claim(s, 'set', 'name', '"second"', [named])).address
        final Cid deleted = send(claim(s, 'delete', null, 'null', [])).address

        then:
        index.claimState(s).names == ['second']
        index.claimState(s).hidden

        when:
        send(claim(s, 'del', null, 'null', [deleted]))

        then:
        !index.claimState(s).hidden

        when: 'a second rename from a window that still shows the first name'
        final PutError stale = refused(claim(s, 'set', 'name', '"third"', [named], '2025-09-16T05:20:00.001Z'))

        then:
        stale.code == 'stale_supersedes'
        stale.at == '/supersedes/0'
        stale.message.contains(renamed.toString())
    }

    def 'a retried rename after the state moved on still dedupes, even eleven minutes later (Review Focus 1)'() {
        given:
        final Cid s = send(selection(item(itemA, [collA]))).address
        final Cid named = send(claim(s, 'set', 'name', '"first"', [])).address
        final String rename = claim(s, 'set', 'name', '"second"', [named])
        final PutResult first = send(rename)
        send(claim(s, 'set', 'name', '"third"', [first.address], '2025-09-16T05:20:00.002Z'))

        when:
        final PutResult replayed = send(rename)
        now += 11 * 60_000
        final PutResult late = send(rename)

        then:
        !replayed.written && replayed.address == first.address
        !late.written && late.address == first.address
    }

    def 'refused before anything is written, each with its code and where'() {
        given:
        final Cid absentItem = Fixtures.cidOf(Fixtures.outputItem([[sample: 'Z']]))
        final Cid absentSelection = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid otherSubjectClaim = send(claim(itemB, 'set', 'name', '"b"', [])).address
        final int blocksBefore = store.listBlocks().withCloseable { it.count() } as int
        final List<List<String>> cases = [
            [selection(''), 'empty', '/members'],
            [selection(item(absentItem, [])), 'not_found', '/members/0/item/address'],
            [selection(item(manifest, [])), 'wrong_kind', '/members/0/item/address'],
            [selection(item(itemB, [collB])), 'not_in_via', '/members/0/item/via/0'],
            [selection(item(itemA, [itemB])), 'wrong_kind', '/members/0/item/via/0'],
            [selection('{"selection":' + link(absentSelection) + '}'), 'not_found', '/members/0/selection'],       // Review Focus 5
            [selection('"cas://' + collA + '/' + itemA + '/A.bam"'), 'invalid', '/members/0'],
            ['{"kind":"Selection","members":[],"extra":1}', 'invalid', '/extra'],
            ['{"kind":"RunCompletion"}', 'wrong_kind', '/kind'],
            ['{"members":[]}', 'invalid', '/kind'],
            ['[1]', 'invalid', ''],
            ['{"kind":"Selection","members":[{"/":5}]}', 'invalid', '/members/0'],
            ['{"kind":', 'invalid', '/kind'],
            [claim(itemA, 'add', 'tag', '"x"', []), 'invalid', '/verb'],
            [claim(itemA, 'rename', 'name', '"x"', []), 'invalid', '/verb'],
            [claim(itemA, 'set', null, '"x"', []), 'invalid', '/attribute'],
            [claim(itemA, 'set', 'name', '""', []), 'invalid', '/value'],
            [claim(itemA, 'set', 'name', '"' + 'x' * 257 + '"', []), 'invalid', '/value'],
            [claim(itemA, 'delete', 'name', 'null', []), 'invalid', '/attribute'],
            [claim(itemA, 'del', null, 'null', []), 'invalid', '/supersedes'],
            [claim(itemA, 'delete', null, 'null', [], '2025-09-16T05:20:00Z'), 'invalid', '/timestamp'],
            [claim(itemA, 'delete', null, 'null', [], '2025-09-16T05:30:00.001Z'), 'clock_skew', '/timestamp'],
            [claim(itemA, 'del', null, 'null', [absentSelection]), 'stale_supersedes', '/supersedes/0'],
            [claim(itemA, 'del', null, 'null', [itemB]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', null, 'null', [otherSubjectClaim]), 'wrong_kind', '/supersedes/0'],
        ]

        when:
        final List<String> wrong = cases.findResults { List<String> c ->
            final PutError e = refused(c[0])
            (e?.code == c[1] && e?.at == c[2]) ? null : "${c[0].take(80)}: got ${e?.code} at ${e?.at}, want ${c[1]} at ${c[2]}".toString()
        }

        then:
        wrong == []
        (store.listBlocks().withCloseable { it.count() } as int) == blocksBefore
    }

    def 'an encoded Selection over 1 MiB is too_large, before any member is looked up'() {
        given:
        final Random random = new Random(7)
        final List<Map> members = (1..20_000).collect {
            final byte[] digest = new byte[32]
            random.nextBytes(digest)
            [item: [address: Cid.of(Cid.DAG_CBOR, digest), via: []]] as Map
        }

        when:
        put.put([kind: 'Selection', members: members, derived_from: []], false)

        then:
        final PutError e = thrown()
        e.code == 'too_large'
        e.at == '/members'
    }

    def 'a writable member that is read-only is not_writable, a 409'() {
        given:
        final Put readOnly = newPut(new LocalBlockStore(tempDir.resolve('store'), 'lab', false))

        when:
        readOnly.put(selection(item(itemA, [collA])).getBytes('UTF-8'), false)

        then:
        final PutError e = thrown()
        e.code == 'not_writable'
        e.status == 409
    }

    def 'derived_from takes links or bytes and stores bytes'() {
        given:
        final Cid prior = send(selection(item(itemB, [collA]))).address
        final String asLink = '{"kind":"Selection","members":[' + item(itemA, [collA]) + '],"derived_from":[' + link(prior) + ']}'
        final String asBytes = '{"kind":"Selection","members":[' + item(itemA, [collA]) + '],"derived_from":[{"/":{"bytes":"' +
            Base64.encoder.withoutPadding().encodeToString(prior.bytes()) + '"}}]}'

        expect:
        send(asLink, true).address == send(asBytes, true).address
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.PutTest'`
Expected: FAIL to compile: `Put`, `PutResult`, `PutError` do not exist.

- [ ] **Step 3: Implement `PutError` and `PutResult`**

```groovy
// src/main/groovy/robsyme/cas/core/PutError.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** A refused request (block explorer spec section 9.4; decision 7 of the milestone 2 plan adds `invalid`). */
@CompileStatic
class PutError extends RuntimeException {

    static final String NOT_FOUND = 'not_found'
    static final String WRONG_KIND = 'wrong_kind'
    static final String NOT_IN_VIA = 'not_in_via'
    static final String EMPTY = 'empty'
    static final String STALE_SUPERSEDES = 'stale_supersedes'
    static final String CLOCK_SKEW = 'clock_skew'
    static final String TOO_LARGE = 'too_large'
    static final String NOT_WRITABLE = 'not_writable'
    static final String INVALID = 'invalid'

    final String code
    /** A JSON pointer into the request; '' for the whole request. */
    final String at

    PutError(String code, String message, String at) {
        super(message)
        this.code = code
        this.at = at
    }

    int getStatus() { code == NOT_WRITABLE ? 409 : 400 }

    byte[] body() {
        return DagJson.encode([error: code, message: message, at: at])
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/PutResult.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a put answered (block explorer spec sections 9.1 and 9.3). */
@CompileStatic
final class PutResult {

    final Cid address
    final boolean dryRun
    final boolean exists
    final List<String> names
    final Map<String, Object> block
    final String entry
    final boolean written

    private PutResult(Cid address, boolean dryRun, boolean exists, List<String> names, Map<String, Object> block, String entry, boolean written) {
        this.address = address
        this.dryRun = dryRun
        this.exists = exists
        this.names = names
        this.block = block
        this.entry = entry
        this.written = written
    }

    static PutResult dryRun(Cid address, boolean exists, List<String> names) {
        return new PutResult(address, true, exists, names, null, null, false)
    }

    static PutResult written(Cid address, Map<String, Object> block, String entry, boolean written) {
        return new PutResult(address, false, true, null, block, entry, written)
    }

    byte[] body() {
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        out.put('address', address)
        if( dryRun ) {
            out.put('exists', exists)
            out.put('names', names)
        }
        else {
            out.put('block', block)
            out.put('entry', entry)
            out.put('written', written)
        }
        return DagJson.encode(out)
    }
}
```

- [ ] **Step 4: Implement `Put`**

```groovy
// src/main/groovy/robsyme/cas/core/Put.groovy
package robsyme.cas.core

import java.nio.file.AccessDeniedException
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.regex.Pattern

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The one builder behind `nf-blocks:put` and `POST /api/put` (block explorer
 * spec section 9). A block is a pure function of the request and this
 * server's asserted_by, and its address is known before anything is checked
 * against the store. Order, which decision 9 of the milestone 2 plan fixes:
 * shape, address, dry run, "already written" (success, unchanged), then
 * membership, supersedes and clock, then write, Store Log entry, ingest.
 */
@Slf4j
@CompileStatic
class Put {

    static final long MAX_BLOCK_BYTES = 1024L * 1024
    static final long MAX_REQUEST_BYTES = 2L * 1024 * 1024
    static final long CLOCK_SKEW_MILLIS = 600_000L
    static final int MAX_NAME_CHARS = 256

    private static final Pattern TIMESTAMP = ~/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    private static final Set<String> SELECTION_KEYS = ['kind', 'members', 'derived_from'] as Set
    private static final Set<String> CLAIM_KEYS = ['kind', 'subject', 'verb', 'attribute', 'value', 'supersedes', 'timestamp'] as Set
    private static final Set<String> ITEM_KEYS = ['address', 'via'] as Set

    /** A request turned into a block, and what to check before writing it. */
    @Canonical
    @CompileStatic
    private static final class Draft {
        Map<String, Object> block
        StoreLogKind logKind
        /** Where a too_large refusal points. */
        String sizeAt
        Closure validate
    }

    /** One member entry as the request wrote it, with the pointers its errors name. */
    @Canonical
    @CompileStatic
    private static final class Entry {
        Selection.Member member
        String addressAt
        List<Cid> requestVia
        List<String> viaAt
    }

    private final BlockStore store
    private final BlockStore writable
    private final Index index
    private final String assertedBy
    private final Closure<Long> clock
    private final Closure catchUp

    Put(BlockStore store, BlockStore writable, Index index, String assertedBy, Closure<Long> clock, Closure catchUp) {
        this.store = store
        this.writable = writable
        this.index = index
        this.assertedBy = assertedBy
        this.clock = clock
        this.catchUp = catchUp
    }

    synchronized PutResult put(byte[] body, boolean dryRun) {
        if( body.length > MAX_REQUEST_BYTES )
            throw new PutError(PutError.TOO_LARGE, "the request is ${body.length} bytes, over ${MAX_REQUEST_BYTES}", '')
        final Object request
        try {
            request = DagJson.decode(body)
        }
        catch( DagJson.DagJsonException e ) {
            throw new PutError(PutError.INVALID, "the request is not DAG-JSON: ${e.message}", e.at)
        }
        return put(request, dryRun)
    }

    synchronized PutResult put(Object request, boolean dryRun) {
        if( !(request instanceof Map) )
            throw invalid("the request is a DAG-JSON map with a kind, 'Selection' or 'Claim'", '')
        final Map map = (Map) request
        final Object kind = map.get('kind')
        if( kind == null )
            throw invalid("the request needs a kind, 'Selection' or 'Claim'", '/kind')
        final Draft draft
        if( kind == Records.SELECTION )
            draft = selectionDraft(map)
        else if( kind == Records.CLAIM )
            draft = claimDraft(map)
        else
            throw new PutError(PutError.WRONG_KIND, "a client may build a Selection or a Claim, not ${kind}", '/kind')

        final byte[] bytes = DagCbor.encode(draft.block)
        if( bytes.length > MAX_BLOCK_BYTES )
            throw new PutError(PutError.TOO_LARGE, "the encoded ${kind} is ${bytes.length} bytes, over ${MAX_BLOCK_BYTES}; " +
                'split it into nested Selections', draft.sizeAt)
        final Cid address = DagCbor.cidOf(bytes)
        catchUp.call()
        if( dryRun )
            return PutResult.dryRun(address, store.has(address), index.claimState(address).names)
        if( !writable.isWritable() )
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' is not writable", '')
        if( writable.has(address) )
            return PutResult.written(address, draft.block, entryFor(address, draft.logKind), false)
        draft.validate.call()
        try {
            writable.put(address, new ByteArrayInputStream(bytes), (long) bytes.length)
        }
        catch( AccessDeniedException e ) {
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' refused the write: ${e.message}", '')
        }
        final String entry = append(address, draft.logKind)
        ingest()
        return PutResult.written(address, draft.block, entry, true)
    }

    // ------------------------------------------------------------------ Selection

    private Draft selectionDraft(Map map) {
        unknownKeys(map, SELECTION_KEYS, '')
        final Object members = map.get('members')
        if( !(members instanceof List) )
            throw invalid('members is a list', '/members')
        if( ((List) members).isEmpty() )
            throw new PutError(PutError.EMPTY, 'an empty Selection is refused (block explorer spec section 7.2)', '/members')
        final List<Entry> entries = new ArrayList<Entry>()
        final List list = (List) members
        for( int i = 0; i < list.size(); i++ )
            entries.add(entryOf(list[i], "/members/${i}".toString()))
        final List<Cid> derived = derivedFrom(map.get('derived_from'))
        final Selection selection
        try {
            selection = new Selection(assertedBy, entries.collect { Entry e -> e.member }, derived)
        }
        catch( IllegalArgumentException e ) {
            throw invalid(e.message, '/members')
        }
        return new Draft(selection.toCbor(), StoreLogKind.SELECTION, '/members', { -> validateSelection(entries) })
    }

    private static Entry entryOf(Object m, String at) {
        if( m instanceof String ) {
            final ItemOccurrence o = ItemOccurrence.parse((String) m)
            if( o == null || o.leaf != null )
                throw invalid("a member written as text is an Item Occurrence, cas://<collection>/<item>, got '${m}'", at)
            return new Entry(Selection.item(o.item, [o.collection]), at, [o.collection], [at])
        }
        if( !(m instanceof Map) || ((Map) m).size() != 1 )
            throw invalid('a member is {"item": {"address", "via"}}, {"selection": <link>} or an Item Occurrence URI', at)
        final Map entry = (Map) m
        if( entry.containsKey('selection') ) {
            if( !(entry.get('selection') instanceof Cid) )
                throw invalid('selection is a link', "${at}/selection".toString())
            return new Entry(Selection.selection((Cid) entry.get('selection')), "${at}/selection".toString(), [], [])
        }
        if( entry.get('item') instanceof Map ) {
            final Map item = (Map) entry.get('item')
            final String here = "${at}/item".toString()
            unknownKeys(item, ITEM_KEYS, here)
            if( !(item.get('address') instanceof Cid) )
                throw invalid('item.address is a link', "${here}/address".toString())
            final Object via = item.containsKey('via') ? item.get('via') : []
            if( !(via instanceof List) || !((List) via).every { Object v -> v instanceof Cid } )
                throw invalid('item.via is a list of links', "${here}/via".toString())
            final List<Cid> requestVia = (List<Cid>) via
            final List<String> viaAt = new ArrayList<String>()
            for( int j = 0; j < requestVia.size(); j++ )
                viaAt.add("${here}/via/${j}".toString())
            return new Entry(Selection.item((Cid) item.get('address'), requestVia), "${here}/address".toString(), requestVia, viaAt)
        }
        throw invalid('a member is {"item": {"address", "via"}}, {"selection": <link>} or an Item Occurrence URI', at)
    }

    /** decision 8: links or bytes in, binary CIDs out. */
    private static List<Cid> derivedFrom(Object value) {
        if( value == null )
            return []
        if( !(value instanceof List) )
            throw invalid('derived_from is a list of links', '/derived_from')
        final List<Cid> out = new ArrayList<Cid>()
        final List list = (List) value
        for( int i = 0; i < list.size(); i++ ) {
            final Object d = list[i]
            if( d instanceof Cid ) {
                out.add((Cid) d)
                continue
            }
            if( d instanceof byte[] ) {
                try {
                    out.add(Cid.fromBytes((byte[]) d))
                    continue
                }
                catch( IllegalArgumentException e ) {
                    throw invalid("derived_from/${i} is not a binary CID: ${e.message}", "/derived_from/${i}".toString())
                }
            }
            throw invalid('derived_from holds links or binary CIDs', "/derived_from/${i}".toString())
        }
        return out
    }

    private void validateSelection(List<Entry> entries) {
        for( Entry e : entries ) {
            final Selection.Member m = e.member
            final String kind = kindAt(m.address, e.addressAt)
            final String wanted = m.nested ? Records.SELECTION : Records.OUTPUT_ITEM
            if( kind != wanted )
                throw new PutError(PutError.WRONG_KIND, "${m.address} is ${describe(kind)}, not ${m.nested ? 'a Selection' : 'an Output Item'}", e.addressAt)
            for( int j = 0; j < e.requestVia.size(); j++ ) {
                final Cid via = e.requestVia[j]
                final Map collection = blockAt(via, e.viaAt[j])
                if( Records.kindOf(collection) != Records.OUTPUT_COLLECTION )
                    throw new PutError(PutError.WRONG_KIND, "${via} is ${describe(Records.kindOf(collection))}, not an Output Collection", e.viaAt[j])
                if( !((List) collection.get('items') ?: []).contains(m.address) )
                    throw new PutError(PutError.NOT_IN_VIA, "collection ${via} does not list item ${m.address}", e.viaAt[j])
            }
        }
    }

    // ---------------------------------------------------------------------- Claim

    private Draft claimDraft(Map map) {
        unknownKeys(map, CLAIM_KEYS, '')
        final Object subject = map.get('subject')
        if( !(subject instanceof Cid) )
            throw invalid('subject is a link', '/subject')
        final Object verb = map.get('verb')
        if( verb == Claim.ADD )
            throw invalid("verb 'add' is not built yet (block explorer spec section 8: out of the Claims slice)", '/verb')
        if( !(verb in [Claim.SET, Claim.DELETE, Claim.DEL]) )
            throw invalid('verb is one of set, delete, del', '/verb')
        final Object attribute = map.get('attribute')
        if( attribute != null && !(attribute instanceof String && ((String) attribute)) )
            throw invalid('attribute is a non-empty string or null', '/attribute')
        final Object value = map.get('value')
        final Object supersedesValue = map.containsKey('supersedes') ? map.get('supersedes') : []
        if( !(supersedesValue instanceof List) || !((List) supersedesValue).every { Object s -> s instanceof Cid } )
            throw invalid('supersedes is a list of links', '/supersedes')
        final List<Cid> supersedes = (List<Cid>) supersedesValue
        final Object timestamp = map.get('timestamp')
        if( !(timestamp instanceof String) || !TIMESTAMP.matcher((String) timestamp).matches() )
            throw invalid('timestamp is ISO-8601 UTC with milliseconds, e.g. 2026-09-25T10:00:00.000Z', '/timestamp')
        final long timestampMillis
        try {
            timestampMillis = Instant.parse((String) timestamp).toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            throw invalid("timestamp '${timestamp}' is not a real instant", '/timestamp')
        }
        switch( (String) verb ) {
            case Claim.SET:
                if( attribute == null ) throw invalid('set needs an attribute', '/attribute')
                if( value == null ) throw invalid('set needs a value', '/value')
                if( attribute == Claim.NAME && !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NAME_CHARS) )
                    throw invalid("a name is a non-blank string of at most ${MAX_NAME_CHARS} characters", '/value')
                break
            case Claim.DELETE:
                if( attribute != null ) throw invalid('delete names the subject itself, with no attribute', '/attribute')
                if( value != null ) throw invalid('delete has no value', '/value')
                break
            case Claim.DEL:
                if( value != null ) throw invalid('del has no value', '/value')
                if( supersedes.isEmpty() ) throw invalid('del supersedes the Claim it undoes', '/supersedes')
                break
        }
        final Claim claim = new Claim(assertedBy, (Cid) subject, (String) verb, (String) attribute, value, supersedes, (String) timestamp)
        return new Draft(claim.toCbor(), StoreLogKind.CLAIM, '', { -> validateClaim(claim, supersedes, timestampMillis) })
    }

    private void validateClaim(Claim claim, List<Cid> requested, long timestampMillis) {
        final long now = clock.call()
        if( Math.abs(now - timestampMillis) > CLOCK_SKEW_MILLIS )
            throw new PutError(PutError.CLOCK_SKEW, "timestamp ${claim.timestamp} is more than 10 minutes from this server's clock (${Index.isoMillis(now)})", '/timestamp')
        for( int j = 0; j < requested.size(); j++ ) {
            final Cid s = requested[j]
            final String at = "/supersedes/${j}".toString()
            if( !store.has(s) )
                throw new PutError(PutError.STALE_SUPERSEDES, "claim ${s} is not in this composition; reload and try again", at)
            final Map block = blockAt(s, at)
            if( Records.kindOf(block) != Records.CLAIM )
                throw new PutError(PutError.WRONG_KIND, "${s} is ${describe(Records.kindOf(block))}, not a Claim", at)
            if( block.get('subject') != claim.subject )
                throw new PutError(PutError.WRONG_KIND, "claim ${s} is about ${block.get('subject')}, not ${claim.subject}", at)
            final List<Cid> by = index.supersedersOf(s)
            if( by )
                throw new PutError(PutError.STALE_SUPERSEDES, "claim ${s} is already superseded by ${by.join(', ')}; reload and try again", at)
        }
    }

    // ------------------------------------------------------------------ plumbing

    private Map blockAt(Cid cid, String at) {
        if( !store.has(cid) )
            throw new PutError(PutError.NOT_FOUND, "${cid} is not in any member of this composition", at)
        if( cid.isRaw() )
            return [:]
        final InputStream input = store.open(cid)
        try {
            final Object value = DagCbor.decode(input.readAllBytes())
            return value instanceof Map ? (Map) value : [:]
        }
        finally {
            input.close()
        }
    }

    private String kindAt(Cid cid, String at) {
        return Records.kindOf(blockAt(cid, at))
    }

    private static String describe(String kind) {
        return kind == null ? 'not a metadata block' : "a ${kind}"
    }

    private String entryFor(Cid address, StoreLogKind kind) {
        final StoreLogEntry existing = index.firstLogEntry(address, writable.alias())
        return existing != null ? existing.name : append(address, kind)
    }

    private String append(Cid address, StoreLogKind kind) {
        final long now = clock.call()
        StoreLog.append(writable, kind, address, now)
        return StoreLog.entryName(kind, address, now)
    }

    /** The write is done; the index is derived, so a failure here only warns (DESIGN.md §0 rule 3). */
    private void ingest() {
        try {
            index.catchUp(writable, StoreLog.of(writable), writable.alias())
        }
        catch( Exception e ) {
            log.warn("the index could not ingest a block just written; it is derived and catches up later: ${e.message}")
        }
    }

    private static void unknownKeys(Map map, Set<String> known, String at) {
        for( Object key : map.keySet() )
            if( !known.contains(key) )
                throw invalid("unknown field '${key}'; this takes ${known.join(', ')}", "${at}/${DagJson.pointer(String.valueOf(key))}".toString())
    }

    private static PutError invalid(String message, String at) {
        return new PutError(PutError.INVALID, message, at)
    }
}
```

Add to `Index.groovy`:

```groovy
    /** The Claims this index holds that supersede {@code claim}. */
    List<Cid> supersedersOf(Cid claim) {
        final List<Cid> out = new ArrayList<Cid>()
        query('SELECT claim_cid FROM claim_supersedes WHERE superseded_cid = ? ORDER BY claim_cid', [claim.toString()]) { ResultSet rs ->
            out.add(Cid.parse(rs.getString(1)))
        }
        return out
    }
```

- [ ] **Step 5: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.PutTest'`
Expected: PASS. (`'{"kind":'` ends while reading the value of `kind`, so `DagJson` points at `/kind`.)

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Put.groovy src/main/groovy/robsyme/cas/core/PutError.groovy \
        src/main/groovy/robsyme/cas/core/PutResult.groovy src/main/groovy/robsyme/cas/core/Index.groovy \
        src/test/groovy/robsyme/cas/core/PutTest.groovy
git commit -m "feat(core): Put, the one builder for Selections and Claims

Address first, then dry run, then an existing block returned unchanged,
then membership, via, supersedes and clock, then write, Store Log entry
and ingest (spec section 9; decisions 6 to 10).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `nextflow plugin nf-blocks:put`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (add `newPut(Index)`)
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy`
- Create: `src/test/groovy/robsyme/cas/cli/CasCommandsPutTest.groovy`
- Modify: `DESIGN.md` §15 "Plugin verbs"

**Interfaces:**
- Consumes: `Put`, `PutResult.body()`, `PutError.body()`, `Options.parse`, `CasSession.openIndex/catchUpIndex/members`.
- Produces:
  - `Put CasSession.newPut(Index index)`: over the composition, writing to `members()[0]`, `asserted_by` from config, the system clock, catch-up of every member.
  - `nextflow plugin nf-blocks:put <file> [--dry-run]`: prints the response body (DAG-JSON) on stdout and exits 0; on a refusal prints the error body on stdout and exits 1; a usage error exits 2. `<file>` may be `-` for stdin (in-process callers; decision 11).
  - `int CasCommands.run(String cmd, List<String> args, Map config, PrintStream out, PrintStream err, InputStream stdin)` (the five-argument form delegates with `System.in`).

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/cli/CasCommandsPutTest.groovy
package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 2 and 9.2: `nf-blocks:put <file|->`, with --dry-run, calling the one builder. */
class CasCommandsPutTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    Cid item, collection

    def setup() {
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
    }

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
                asserted_by: 'ada',
            ],
        ]
    }

    private String request() { '{"kind":"Selection","members":["cas://' + collection + '/' + item + '"],"derived_from":[]}' }

    private int run(List<String> args, String stdin = '') {
        return new CasCommands().run('put', args, config(), new PrintStream(out, true), new PrintStream(err, true),
            new ByteArrayInputStream(stdin.getBytes('UTF-8')))
    }

    def 'put writes the block and prints the response'() {
        given:
        final Path file = tempDir.resolve('selection.json')
        file.text = request()

        when:
        final int status = run([file.toString()])
        final Map body = (Map) DagJson.decode(out.toString().trim())

        then:
        status == 0
        body.written == true
        body.address instanceof Cid
        new LocalBlockStore(tempDir.resolve('store'), 'lab', false).has((Cid) body.address)
        StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).size() == 1
    }

    def '--dry-run arrives as the pair --dry-run true and writes nothing'() {
        when:
        final int status = run(['-', '--dry-run', 'true'], request())
        final Map body = (Map) DagJson.decode(out.toString().trim())

        then:
        status == 0
        body.exists == false
        body.names == []
        StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).isEmpty()
    }

    def 'a refusal prints the error body and exits 1'() {
        when:
        final int status = run(['-'], '{"kind":"Selection","members":[],"derived_from":[]}')

        then:
        status == 1
        DagJson.decode(out.toString().trim()) == [error: 'empty', message: 'an empty Selection is refused (block explorer spec section 7.2)', at: '/members']
    }

    def 'usage errors exit 2'() {
        expect:
        run(args) == 2

        where:
        args << [[], ['a.json', 'b.json'], ['a.json', '--dry-run', 'maybe'], ['a.json', '--port', '1']]
    }

    def 'a file that does not exist is a failure the verb reports'() {
        expect:
        run([tempDir.resolve('nope.json').toString()]) == 1
        err.toString().contains('nope.json')
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.cli.CasCommandsPutTest'`
Expected: FAIL: no six-argument `run`, and `put` is an unknown verb.

- [ ] **Step 3: Implement**

In `CasSession.groovy`:

```groovy
    /**
     * The one builder over this composition, writing to the writable member
     * (block explorer spec section 9). The caller owns {@code index}; the
     * builder uses it under its own lock.
     */
    Put newPut(Index index) {
        return new Put(store, members()[0], index, assertedBy, { -> System.currentTimeMillis() }, { -> catchUpIndex(index) })
    }
```

In `CasCommands.groovy`: `static final List<String> VERBS = ['explore', 'put', 'snapshot']`; keep the five-argument `run` delegating to a six-argument one with `System.in`; add the case and the usage line (`'  put <file|-> [--dry-run]  build and write one Selection or Claim from DAG-JSON\n'`):

```groovy
                case 'put':
                    return put(Options.parse(args, ['dry-run'] as Set), config, out, stdin)
```

```groovy
    /** Builds and writes one client-constructible block (spec section 9.2); the body goes to stdout either way. */
    private static int put(Options options, Map config, PrintStream out, InputStream stdin) {
        if( options.positionals.size() != 1 )
            throw new UsageException("put takes one file (or - for stdin), got ${options.positionals ?: 'none'}")
        final String dry = options.flag('dry-run')
        if( !(dry in [null, 'true', 'false']) )
            throw new UsageException("--dry-run is a flag, got '${dry}'")
        final String source = options.positionals[0]
        final byte[] body = source == '-' ? readCapped(stdin, source) : readCapped(Files.newInputStream(Paths.get(source)), source)
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            out.println(new String(cas.newPut(index).put(body, dry == 'true').body(), 'UTF-8'))
            return 0
        }
        catch( PutError e ) {
            out.println(new String(e.body(), 'UTF-8'))
            return 1
        }
        finally {
            index.close()
        }
    }

    /** At most one byte past the builder's request cap, so the builder refuses it with too_large. */
    private static byte[] readCapped(InputStream input, String source) {
        try {
            return input.readNBytes((int) Put.MAX_REQUEST_BYTES + 1)
        }
        finally {
            input.close()
        }
    }
```

A missing file throws `NoSuchFileException` from `Files.newInputStream`, which the existing `catch( Exception e )` reports as `nf-blocks:put: <path>` and exit 1.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.cli.*'`
Expected: PASS, `CasCommandsTest` included (its unknown-verb test now also finds `put` in the usage text; extend it with `err.toString().contains('put')`).

- [ ] **Step 5: Amend `DESIGN.md` §15 and commit**

Under "Plugin verbs", add the line `nextflow [-c <config>] plugin nf-blocks:put <file> [--dry-run]` and: "`put` prints the response or error body (DAG-JSON) on stdout, exit 0 or 1. Nextflow 26.04.6's launcher refuses a bare `-` (`Unknown option: -`), so read stdin as `/dev/stdin`; `-` works for in-process callers. A bare `--dry-run` arrives as `--dry-run`, `true`."

```bash
git add src/main/groovy/robsyme/cas/CasSession.groovy src/main/groovy/robsyme/cas/cli/CasCommands.groovy \
        src/test/groovy/robsyme/cas/cli DESIGN.md
git commit -m "feat(cli): nf-blocks:put, the builder from the command line

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: `POST /api/put` on `nf-blocks:explore`

The page's write path (spec sections 9.2 and 9.5): the same `Put`, behind a per-launch token, JSON content types only, the existing `Host`/`Origin` guard, and a body cap.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreServer.groovy`
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy`
- Modify: `src/test/groovy/robsyme/cas/explore/RawHttp.groovy` (a body overload)
- Create: `src/test/groovy/robsyme/cas/explore/ExploreWriteTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/explore/ExploreServerTest.groovy`, `ExploreCommandTest.groovy` (constructor and printed URL)
- Modify: `DESIGN.md` §15 "`nf-blocks:explore`"

**Interfaces:**
- Consumes: `Put.put(byte[], boolean)`, `PutResult.body()`, `PutError.status/body()`, `CasSession.newPut`, `Multibase.base32Encode`.
- Produces:
  - `ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token)`; the old three-argument constructor stays for read-only use (`put` null, no token).
  - `static String ExploreServer.newToken()`: 16 random bytes, base32, 26 characters.
  - `String ExploreServer.getLaunchUrl()`: `http://127.0.0.1:<port>/?token=<token>` (or `getUrl()` without a token).
  - `POST /api/put[?dry_run=true]`: `403` without the right `X-NF-Blocks-Token`, `415` for a content type other than `application/json` or `application/vnd.ipld.dag-json`, `413` over 2 MiB, else the builder's status and DAG-JSON body with `Content-Type: application/vnd.ipld.dag-json`. `members.json` gains `"write": true|false`.
  - `explore` prints `nf-blocks explorer: http://127.0.0.1:<port>/?token=<token>`.

- [ ] **Step 1: Give `RawHttp` a body**

```groovy
    static Response send(int port, String method, String rawPath, Map<String, String> headers, byte[] body) {
        return send(port, method, rawPath, headers + ['Content-Length': String.valueOf(body.length)], body)
    }
```

Rework the existing `send` so the four-argument form calls a private five-argument one that writes `head` and then `body` (empty for the old callers) before flushing, and does not add `Content-Length` itself. Keep the old signature's behaviour byte for byte.

- [ ] **Step 2: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/explore/ExploreWriteTest.groovy
package robsyme.cas.explore

import java.nio.file.Path

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.StoreLog
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 9.5: the write endpoint's safety, and that it is the builder. */
class ExploreWriteTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    ExploreServer server
    String token = ExploreServer.newToken()
    Cid item, collection

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        item = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A']]]))
        final Put put = new Put(store, store, index, 'ada', { -> System.currentTimeMillis() }, { -> index.catchUp(store, StoreLog.of(store), 'lab') })
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        server = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, put, token).start(0)
    }

    def cleanup() {
        server?.stop()
        index?.close()
    }

    private byte[] request() {
        return ('{"kind":"Selection","members":["cas://' + collection + '/' + item + '"],"derived_from":[]}').getBytes('UTF-8')
    }

    private RawHttp.Response post(String path, Map<String, String> headers, byte[] body = request()) {
        return RawHttp.send(server.port, 'POST', path, headers, body)
    }

    private Map<String, String> good(Map<String, String> extra = [:]) {
        return ['Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': token,
                Origin: "http://127.0.0.1:${server.port}".toString()] + extra
    }

    def 'a POST with the token writes through the builder and answers DAG-JSON'() {
        when:
        final def r = post('/api/put', good())
        final Map body = (Map) DagJson.decode(r.body)

        then:
        r.status == 200
        r.headers['content-type'] == 'application/vnd.ipld.dag-json'
        body.written == true
        store.has((Cid) body.address)
    }

    def 'application/json with a charset is accepted, and dry_run=true writes nothing'() {
        when:
        final def r = post('/api/put?dry_run=true', good('Content-Type': 'application/json; charset=utf-8'))

        then:
        r.status == 200
        ((Map) DagJson.decode(r.body)).exists == false
        StoreLog.read(store).isEmpty()
    }

    def 'refused before the builder runs: #why'() {
        when:
        final def r = post('/api/put', headers(server.port, token))

        then:
        r.status == status
        StoreLog.read(store).isEmpty()

        where:
        why                    | status | headers
        'no token'             | 403    | { int p, String t -> ['Content-Type': 'application/json'] }
        'a wrong token'        | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': 'a' * 26] }
        'a foreign Origin'     | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Origin: 'http://evil.example'] }
        'text/plain'           | 415    | { int p, String t -> ['Content-Type': 'text/plain', 'X-NF-Blocks-Token': t] }
        'no content type'      | 415    | { int p, String t -> ['X-NF-Blocks-Token': t] }
    }

    def 'a body over 2 MiB is 413, and a broken body is 400 invalid, never 500 (Review Focus 4)'() {
        expect:
        post('/api/put', good(), new byte[2 * 1024 * 1024 + 1]).status == 413
        post('/api/put', good(), ('[' * 10_000).getBytes('UTF-8')).with {
            status == 400 && ((Map) DagJson.decode(body)).error == 'invalid'
        }
        post('/api/put', good(), [0x7b, 0xff] as byte[]).with {
            status == 400 && ((Map) DagJson.decode(body)).error == 'invalid'
        }
    }

    def 'a builder refusal keeps its status and body'() {
        when:
        final def r = post('/api/put', good(), '{"kind":"Selection","members":[],"derived_from":[]}'.getBytes('UTF-8'))

        then:
        r.status == 400
        ((Map) DagJson.decode(r.body)).error == 'empty'
    }

    def 'POST anywhere else is 405, and GET /api/put is 404'() {
        expect:
        post('/members.json', good()).status == 405
        RawHttp.send(server.port, 'GET', '/api/put').status == 404
    }

    def 'members.json says whether this server writes; the launch URL carries the token'() {
        expect:
        new JsonSlurper().parse(RawHttp.send(server.port, 'GET', '/members.json').body) ==
            [members: [[alias: 'lab', writable: true, base: 'm/lab/']], write: true]
        server.launchUrl == "http://127.0.0.1:${server.port}/?token=${token}"
        token ==~ /[a-z2-7]{26}/
    }
}
```

In `ExploreServerTest`, the read-only server keeps the three-argument constructor; its `members.json` expectation gains `write: false`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.explore.*'`
Expected: FAIL to compile: no five-argument constructor, no `newToken`.

- [ ] **Step 4: Implement in `ExploreServer.groovy`**

```groovy
    static final String TOKEN_HEADER = 'X-NF-Blocks-Token'
    private static final Set<String> JSON_TYPES = ['application/json', 'application/vnd.ipld.dag-json'] as Set
    private static final SecureRandom RANDOM = new SecureRandom()

    private final Put put
    private final String token

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page) {
        this(members, writableAlias, page, null, null)
    }

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token) {
        this.members = members
        this.writableAlias = writableAlias
        this.page = page ?: NO_PAGE
        this.put = put
        this.token = token
    }

    /** 16 random bytes as base32: what the printed URL carries (decision 12). */
    static String newToken() {
        final byte[] bytes = new byte[16]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }

    String getLaunchUrl() { token ? "${url}?token=${token}".toString() : url }
```

In `handle`, after the `ownOrigin` check, route `POST`:

```groovy
            if( exchange.requestMethod == 'POST' ) {
                if( exchange.requestURI.rawPath == '/api/put' )
                    write(exchange)
                else {
                    exchange.responseHeaders.set('Allow', 'GET, HEAD')
                    text(exchange, 405, 'only /api/put takes a POST')
                }
                return
            }
```

(keeping the existing `GET`/`HEAD` check after it), and add:

```groovy
    /** POST /api/put (spec sections 9.2 and 9.5): token, content type and size, then the builder. */
    private void write(HttpExchange exchange) {
        if( put == null || token == null ) {
            text(exchange, 405, 'this server was started without a writable member')
            return
        }
        final String given = exchange.requestHeaders.getFirst(TOKEN_HEADER)
        if( given == null || !MessageDigest.isEqual(given.getBytes('UTF-8'), token.getBytes('UTF-8')) ) {
            text(exchange, 403, 'refused: this needs the token in the URL nf-blocks:explore printed')
            return
        }
        final String type = (exchange.requestHeaders.getFirst('Content-Type') ?: '').split(';')[0].trim().toLowerCase()
        if( !JSON_TYPES.contains(type) ) {
            text(exchange, 415, 'refused: send application/json or application/vnd.ipld.dag-json')
            return
        }
        final byte[] body = exchange.requestBody.readNBytes((int) Put.MAX_REQUEST_BYTES + 1)
        if( body.length > Put.MAX_REQUEST_BYTES ) {
            text(exchange, 413, "refused: the body is over ${Put.MAX_REQUEST_BYTES} bytes")
            return
        }
        final boolean dryRun = (exchange.requestURI.rawQuery ?: '').split('&').contains('dry_run=true')
        try {
            dagJson(exchange, 200, put.put(body, dryRun).body())
        }
        catch( PutError e ) {
            dagJson(exchange, e.status, e.body())
        }
    }

    private static void dagJson(HttpExchange exchange, int status, byte[] body) {
        bytes(exchange, status, 'application/vnd.ipld.dag-json', body)
    }
```

and in `membersJson()` add `write: put != null` beside `members`. Any other exception from `put.put` (a failed Store Log append) falls to `handle`'s `catch`, a `500`.

- [ ] **Step 5: Build the `Put` and print the token in `ExploreCommand`**

`Started` gains `final Index index`. In `start`:

```groovy
        final Index index = session.openIndex()
        final String token = ExploreServer.newToken()
        final ExploreServer server = new ExploreServer(membersOf(cas), cas.writableAlias, IndexSnapshot.bundledPage(),
                session.newPut(index), token)
            .start(options.intFlag('port', 0))
        out.println("nf-blocks explorer: ${server.launchUrl}")
```

and in `run`'s shutdown hook, `started.index.close()` after `started.server.stop()` and before `refresh`. `ExploreCommandTest`'s expectation on the printed line becomes a match on `nf-blocks explorer: http://127.0.0.1:\d+/\?token=[a-z2-7]{26}`. `gate/browser/tier.sh` greps only `nf-blocks explorer:` and passes its own port, so it needs no change.

- [ ] **Step 6: Run the tests and the Gate**

Run: `./gradlew test`
Expected: PASS.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage 11/0/6, browser tier A 5/5 (A1's explore steps still read; nothing writes).

- [ ] **Step 7: Amend `DESIGN.md` §15 and commit**

In the `nf-blocks:explore` table add `POST /api/put[?dry_run=true]` (token header, JSON types, 2 MiB, the builder's answer) and `members.json`'s `write`. Replace the "No launch token until the write endpoint" decision line with: "The launch token (decision 12 of the milestone 2 plan) guards `POST`; `GET`/`HEAD` stay token-free behind the `Host`/`Origin` check." Change the printed line to include `?token=<token>`.

```bash
git add src/main/groovy/robsyme/cas/explore src/test/groovy/robsyme/cas/explore DESIGN.md
git commit -m "feat(explore): POST /api/put behind a launch token, JSON only, 2 MiB cap

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: `fromStore(selection:)`

Spec section 10: flatten nested Selections, emit each distinct item once in the shape a run's output has, and emit a deleted Selection's items with a warning.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy`
- Modify: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy` (new features beside the run ones, reusing its helpers)
- Modify: `DESIGN.md` §13

**Interfaces:**
- Consumes: `Index.selectionItems`, `Index.isSelectionIndexed`, `Index.ingestSelection`, `Index.claimState`, `Selection.fromCbor`, `ClaimState.hidden/deletion/deletionClaims`, the existing `restore`, `loadItem`, `loadBlock`, `openIndex`.
- Produces: `channel.fromStore(selection: '<cid>' | 'cas://<cid>')`, refusing `run`, `output`, `where` and `pipeline` beside it.

- [ ] **Step 1: Write the failing tests**

Add to `CasExtensionTest.groovy` (imports: `robsyme.cas.core.Selection`, `robsyme.cas.core.StoreLog`, `robsyme.cas.core.StoreLogKind`, `robsyme.cas.core.Fixtures`):

```groovy
    private Cid itemOf(Cid completion, String sample) {
        final Index index = Index.open(indexFile)
        try {
            return index.items(completion, 'aligned', [sample: sample])[0]
        }
        finally {
            index.close()
        }
    }

    private Cid alignedCollectionOf(Cid completion) {
        final Index index = Index.open(indexFile)
        try {
            return index.collectionsOf(completion).aligned
        }
        finally {
            index.close()
        }
    }

    private Cid storeSelection(List<Selection.Member> members, boolean logged = true) {
        final Cid cid = cas.store.putDagCbor(new Selection('test', members, []).toCbor())
        if( logged )
            StoreLog.append(cas.store, StoreLogKind.SELECTION, cid, System.currentTimeMillis())
        return cid
    }

    def 'fromStore(selection:) flattens nesting and emits each item once, restored (spec section 10)'() {
        given:
        final Map run = alignedRun()
        final Cid coll = alignedCollectionOf((Cid) run.completion)
        final Cid a = itemOf((Cid) run.completion, 'A'), b = itemOf((Cid) run.completion, 'B'), c = itemOf((Cid) run.completion, 'C')
        final Cid inner = storeSelection([Selection.item(a, [coll]), Selection.item(b, [coll])])
        final Cid outer = storeSelection([Selection.selection(inner), Selection.item(b, []), Selection.item(c, [coll])])

        when:
        final List items = drain(ext.fromStore(selection: "cas://${outer}".toString()))

        then: 'A, B and C once each, in item-CID order'
        final Map<Cid, Map> metaOf = [(a): [sample: 'A'], (b): [sample: 'B'], (c): [sample: 'C']]
        items.collect { (it as List)[0] } == [a, b, c].sort { it.toString() }.collect { metaOf[it] }
        items.every { (it as List)[1] instanceof CasPath }
    }

    def 'a bare cid works, and a Selection whose block was never logged is read too'() {
        given:
        final Map run = alignedRun()
        final Cid a = itemOf((Cid) run.completion, 'A')
        final Cid s = storeSelection([Selection.item(a, [])], false)

        expect:
        drain(ext.fromStore(selection: s.toString())).size() == 1
    }

    def 'a deleted Selection still emits its items (spec section 10: with a warning)'() {
        given:
        final Map run = alignedRun()
        final Cid s = storeSelection([Selection.item(itemOf((Cid) run.completion, 'A'), [])])
        final Cid delete = cas.store.putDagCbor(Fixtures.claim(s, 'delete', null, null, []))
        StoreLog.append(cas.store, StoreLogKind.CLAIM, delete, System.currentTimeMillis())

        expect:
        drain(ext.fromStore(selection: s.toString())).size() == 1
    }

    def 'a nested Selection the composition does not hold fails at the call, naming it (Review Focus 5)'() {
        given:
        final Map run = alignedRun()
        final Cid absent = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid s = storeSelection([Selection.selection(absent), Selection.item(itemOf((Cid) run.completion, 'A'), [])])

        when:
        ext.fromStore(selection: s.toString())

        then:
        final IllegalStateException e = thrown()
        e.message.contains(absent.toString())
    }

    def 'selection refuses run, output, where and pipeline beside it, and an address that is not a Selection'() {
        given:
        final Map run = alignedRun()

        when:
        ext.fromStore(opts)

        then:
        thrown(IllegalArgumentException)

        where:
        opts << [
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', output: 'aligned'],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', run: 'latest'],
            [selection: 'not a cid'],
        ]
    }

    def 'a RunCompletion address is not a Selection'() {
        given:
        final Map run = alignedRun()

        when:
        ext.fromStore(selection: run.completion.toString())

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('not a Selection')
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest'`
Expected: the new features FAIL (`channel.fromStore needs an 'output' name`).

- [ ] **Step 3: Implement**

At the top of `resolveItems`:

```groovy
        if( opts?.containsKey('selection') )
            return resolveSelection(opts)
```

```groovy
    /**
     * fromStore(selection:) (block explorer spec section 10): every distinct
     * item the Selection reaches, nested ones included, sorted by item CID,
     * restored as a run's output is. A deleted Selection still emits: its
     * address is an explicit, immutable request.
     */
    private List<Object> resolveSelection(Map opts) {
        final List<String> clashing = ['run', 'output', 'where', 'pipeline'].findAll { String k -> opts.containsKey(k) }
        if( clashing )
            throw new IllegalArgumentException("channel.fromStore(selection: ...) takes no ${clashing.join(', ')}: a Selection names its items itself")
        final Cid selection = selectionCid(opts.get('selection'))
        Index index = null
        try {
            index = openIndex()
            cas.catchUpIndex(index)
            final Map block = loadBlock(selection)
            if( block == null )
                throw new IllegalStateException("selection ${selection} is not in any member of this composition")
            if( Records.kindOf(block) != Records.SELECTION )
                throw new IllegalArgumentException("${selection} is a ${Records.kindOf(block) ?: 'block with no kind'}, not a Selection")
            ensureIndexed(index, selection, new HashSet<Cid>())
            final ClaimState state = index.claimState(selection)
            if( state.hidden )
                log.warn("selection ${selection} is hidden by a current delete Claim (${state.deletionClaims.join(', ')}); emitting its items anyway, as its address asks")
            else if( state.deletion == ClaimState.CONFLICTED )
                log.warn("selection ${selection} has a conflicted deletion (${state.deletionClaims.join(', ')}); emitting its items")
            final List<Object> items = new ArrayList<Object>()
            for( Cid itemCid : index.selectionItems(selection) ) {
                final OutputItem item = loadItem(itemCid)
                if( item == null )
                    throw new IllegalStateException("output item ${itemCid} of selection ${selection} is not in any member of this composition")
                items.add(restore(item.value, itemCid))
            }
            return items
        }
        finally {
            index?.close()
        }
    }

    private static Cid selectionCid(Object value) {
        String text = value?.toString()
        if( text?.startsWith(CAS_PREFIX) )
            text = text.substring(CAS_PREFIX.length())
        if( !text || !Cid.isCid(text) )
            throw new IllegalArgumentException("channel.fromStore(selection: ...) takes a Selection address, cas://<cid> or <cid>, got '${value}'")
        return Cid.parse(text)
    }

    /**
     * A Selection block present in the store but never logged (copied without
     * its Store Log entry) is indexed on the spot, and so is each nested one
     * the store holds; a nested one the store lacks is left for
     * selectionItems to name.
     */
    private void ensureIndexed(Index index, Cid selection, Set<Cid> seen) {
        if( !seen.add(selection) )
            return
        final Map block = loadBlock(selection)
        if( block == null || Records.kindOf(block) != Records.SELECTION )
            return
        if( !index.isSelectionIndexed(selection) )
            index.ingestSelection(cas.store, selection, cas.config.writableAlias)
        for( Selection.Member m : Selection.fromCbor(block).members )
            if( m.nested )
                ensureIndexed(index, m.address, seen)
    }
```

- [ ] **Step 4: Run the tests, amend `DESIGN.md` §13, commit**

Run: `./gradlew test --tests 'robsyme.cas.ext.*'`
Expected: PASS.

§13: "`channel.fromStore(selection: <cid or cas://cid>)` (block explorer spec section 10) emits every distinct item the Selection reaches through nesting (`Index.selectionItems`), sorted by item CID, restored as above; a Selection hidden by a current `delete` Claim emits with a warning; a nested Selection the composition lacks fails the call, naming it. `run`, `output`, `where` and `pipeline` are refused beside `selection`."

```bash
git add src/main/groovy/robsyme/cas/ext/CasExtension.groovy src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy DESIGN.md
git commit -m "feat(ext): fromStore(selection:), nested Selections flattened, each item once

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: The samplesheet export

Spec section 10's second consumer and decisions 1 and 18: CSV (flattened) and JSON (lossless) from a Selection's items, served by `explore` at `GET /api/samplesheet/<cid>.csv` and `.json`.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Samplesheet.groovy`
- Create: `src/test/groovy/robsyme/cas/core/SamplesheetTest.groovy`
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreServer.groovy` (an `Exporter` and the route)
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy` (builds the `Exporter`)
- Modify: `src/test/groovy/robsyme/cas/explore/ExploreWriteTest.groovy`
- Modify: `DESIGN.md` §15

**Interfaces:**
- Consumes: `OutputItem.fromCbor` (its `value` holds `Leaf` instances), `Leaf.addressed/address/name`, `MetadataView.scalar`, `Index.selectionItems`, `CasSession.catchUpIndex`.
- Produces:
  - `final class Samplesheet` with `static Samplesheet of(BlockStore store, List<Cid> items)`, `List<String> columns` (Meta Map columns then file columns), `List<Row> rows`, `String csv()`, `String json()`; `Row` has `Cid item`, `Map<String,Object> meta` (nested, no leaves), `Map<String,Object> flat` (dotted), `Map<String,String> files`.
  - `static interface ExploreServer.Exporter { byte[] samplesheet(Cid selection, String format) }`; constructor `ExploreServer(members, writableAlias, page, Put put, String token, Exporter exporter)` (the five-argument form passes null).
  - `GET /api/samplesheet/<cid>.csv` → `text/csv; charset=utf-8`, `.json` → `application/json`, each with `Content-Disposition: attachment; filename="selection-<first 16 of cid>.<ext>"`; `404` with the reason when the address is not a Selection the index holds or reaches one it does not.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/SamplesheetTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 10 and decision 18: one row per item, Meta Map columns, file columns by structural position. */
class SamplesheetTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Cid bamA, bamB, chunk1, chunk2, qc

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        bamA = Fixtures.contentCid('A')
        bamB = Fixtures.contentCid('B')
        chunk1 = Fixtures.contentCid('c1')
        chunk2 = Fixtures.contentCid('c2')
        qc = Fixtures.cidOf([kind: 'DirectoryManifest', schema: 1, entries: []])
    }

    private Cid item(Object value) { store.putDagCbor(Fixtures.outputItem(value)) }

    def 'tuple items: Meta Map columns by dotted path, then file columns by tuple position'() {
        given:
        final Cid a = item([[sample: 'A', lane: 1L, single_end: false, nested: [kit: 'truseq', ids: [1L, 2L]]], Fixtures.leaf('A.bam', bamA, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a])
        final List<String> lines = sheet.csv().readLines()

        then:
        sheet.columns == ['sample', 'lane', 'single_end', 'nested.kit', 'nested.ids', '1']
        sheet.rows[0].files == ['1': "cas://${bamA}/A.bam".toString()]
        sheet.rows[0].flat['nested.ids'] == [1L, 2L]
        lines == ['sample,lane,single_end,nested.kit,nested.ids,1', "A,1,false,truseq,\"[1,2]\",cas://${bamA}/A.bam".toString()]
    }

    def 'a key first seen in a later row is a later column, blank above it'() {
        given:
        final Cid a = item([[sample: 'A'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B', depth: 1.5d], Fixtures.leaf('B.bam', bamB, 1L)])
        final List<Cid> items = [a, b].sort { it.toString() }

        when:
        final Samplesheet sheet = Samplesheet.of(store, items)

        then:
        sheet.rows*.item == items
        sheet.columns == ['sample', 'depth', '1']      // every Meta Map column precedes every file column
        parse(sheet.csv()).find { it.sample == 'A' }.depth == ''
        parse(sheet.csv()).find { it.sample == 'B' }.depth == '1.5'
    }

    def 'a list of files is one column per position; a directory leaf is cas://<manifest>; an unaddressed leaf is blank'() {
        given:
        final Cid c = item([[sample: 'C'], [Fixtures.leaf('chunk_1.txt', chunk1, 1L), Fixtures.leaf('chunk_2.txt', chunk2, 1L)],
                            Fixtures.leaf('C_qc', qc, 0L), Fixtures.unaddressedLeaf('gone.txt')])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [c])

        then:
        sheet.columns == ['sample', '1.0', '1.1', '2', '3']
        sheet.rows[0].files == ['1.0': "cas://${chunk1}/chunk_1.txt".toString(), '1.1': "cas://${chunk2}/chunk_2.txt".toString(),
                                '2': "cas://${qc}".toString(), '3': '']
    }

    def 'a record item names its file columns by key'() {
        given:
        final Cid r = item([sample: 'R', bam: Fixtures.leaf('R.bam', bamA, 1L)])

        expect:
        Samplesheet.of(store, [r]).columns == ['sample', 'bam']
    }

    def 'CSV quotes commas, quotes and newlines; blank where a key is absent (Review Focus 3)'() {
        given:
        final Cid a = item([[sample: 'A', note: 'tumour, "batch 2"\nsecond line'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B', extra: 'x'], Fixtures.leaf('B.bam', bamB, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a, b].sort { it.toString() })
        final String csv = sheet.csv()

        then:
        csv.contains('"tumour, ""batch 2""\nsecond line"')
        parse(csv).collect { it.sample } as Set == ['A', 'B'] as Set
        parse(csv).find { it.sample == 'B' }.note == ''
        parse(csv).find { it.sample == 'A' }.extra == ''
    }

    def 'JSON keeps nesting, types and absence'() {
        given:
        final Cid a = item([[sample: 'A', lane: 1L, depth: 1.5d, nested: [kit: 'truseq']], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B'], Fixtures.leaf('B.bam', bamB, 1L)])

        when:
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(Samplesheet.of(store, [a, b].sort { it.toString() }).json())

        then:
        rows.find { it.sample == 'A' } == [sample: 'A', lane: 1, depth: 1.5, nested: [kit: 'truseq'], '1': "cas://${bamA}/A.bam".toString()]
        rows.find { it.sample == 'B' } == [sample: 'B', '1': "cas://${bamB}/B.bam".toString()]
    }

    def 'a file column named like a Meta Map column is written file.<position>'() {
        given:
        final Cid a = item([['1': 'one'], Fixtures.leaf('A.bam', bamA, 1L)])

        expect:
        Samplesheet.of(store, [a]).columns == ['1', 'file.1']
    }

    /** A small RFC 4180 reader, so the test does not trust the writer. */
    private static List<Map<String, String>> parse(String csv) {
        final List<List<String>> records = []
        List<String> record = []
        StringBuilder field = new StringBuilder()
        boolean quoted = false
        for( int i = 0; i < csv.length(); i++ ) {
            final char ch = csv.charAt(i)
            if( quoted ) {
                if( ch == '"' as char && i + 1 < csv.length() && csv.charAt(i + 1) == '"' as char ) { field.append('"'); i++ }
                else if( ch == '"' as char ) quoted = false
                else field.append(ch)
            }
            else if( ch == '"' as char ) quoted = true
            else if( ch == ',' as char ) { record << field.toString(); field = new StringBuilder() }
            else if( ch == '\n' as char ) { record << field.toString(); records << record; record = []; field = new StringBuilder() }
            else field.append(ch)
        }
        final List<String> header = records[0]
        return records.drop(1).collect { List<String> r -> [header, r].transpose().collectEntries() as Map<String, String> }
    }
}
```

Add to `ExploreWriteTest.groovy`:

```groovy
    def 'GET /api/samplesheet/<selection>.csv and .json answer the export'() {
        given:
        final Map written = (Map) DagJson.decode(post('/api/put', good()).body)
        final Cid selection = (Cid) written.address
        server.stop()
        final ExploreServer.Exporter exporter = { Cid s, String format ->
            index.catchUp(store, StoreLog.of(store), 'lab')
            final Samplesheet sheet = Samplesheet.of(store, index.selectionItems(s))
            return (format == 'csv' ? sheet.csv() : sheet.json()).getBytes('UTF-8')
        } as ExploreServer.Exporter
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        server = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, null, null, exporter).start(0)

        when:
        final def csv = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.csv")
        final def json = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.json")
        final def absent = RawHttp.send(server.port, 'GET', "/api/samplesheet/${Fixtures.cidOf([kind: 'Selection', n: 99])}.csv")

        then:
        csv.status == 200
        csv.headers['content-type'] == 'text/csv; charset=utf-8'
        csv.headers['content-disposition'] == "attachment; filename=\"selection-${selection.toString().take(16)}.csv\""
        csv.text().readLines()[0] == 'sample'
        json.status == 200
        new JsonSlurper().parseText(json.text()) == [[sample: 'A']]
        absent.status == 404
    }
```

(The test's item has only a Meta Map, so its CSV header is `sample` and it has no file column.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SamplesheetTest' --tests 'robsyme.cas.explore.ExploreWriteTest'`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `Samplesheet.groovy`**

```groovy
// src/main/groovy/robsyme/cas/core/Samplesheet.groovy
package robsyme.cas.core

import groovy.json.JsonOutput
import groovy.transform.CompileStatic

/**
 * A Selection's items as a samplesheet (block explorer spec section 10;
 * decision 18 of the milestone 2 plan). One row per item. Meta Map columns
 * by dotted path; file columns by the leaf's structural position (tuple
 * index or record key, never file name). A file cell is cas://<cid>/<name>
 * for a file, cas://<manifest> for a directory, blank for any leaf without
 * an address. CSV is the flattened copy, JSON the lossless one.
 */
@CompileStatic
final class Samplesheet {

    @CompileStatic
    static final class Row {
        final Cid item
        final Map<String, Object> meta
        final Map<String, Object> flat
        final Map<String, String> files

        Row(Cid item, Map<String, Object> meta, Map<String, Object> flat, Map<String, String> files) {
            this.item = item
            this.meta = meta
            this.flat = flat
            this.files = files
        }
    }

    final List<Row> rows
    final List<String> columns
    private final List<String> metaColumns
    private final Map<String, String> fileColumnNames

    private Samplesheet(List<Row> rows, List<String> metaColumns, Map<String, String> fileColumnNames) {
        this.rows = rows
        this.metaColumns = metaColumns
        this.fileColumnNames = fileColumnNames
        this.columns = Collections.unmodifiableList(metaColumns + new ArrayList<String>(fileColumnNames.values()))
    }

    static Samplesheet of(BlockStore store, List<Cid> items) {
        final List<Row> rows = new ArrayList<Row>()
        final LinkedHashSet<String> metaColumns = new LinkedHashSet<String>()
        final LinkedHashSet<String> positions = new LinkedHashSet<String>()
        for( Cid cid : items ) {
            final Object value = OutputItem.fromCbor(load(store, cid)).value
            final Object view = viewOf(value)
            final Map<String, Object> meta = view instanceof Map ? (Map<String, Object>) withoutLeaves(view) : new LinkedHashMap<String, Object>()
            final Map<String, Object> flat = new LinkedHashMap<String, Object>()
            flatten(meta, null, flat)
            final Map<String, String> files = new LinkedHashMap<String, String>()
            collectFiles(value, null, files)
            metaColumns.addAll(flat.keySet())
            positions.addAll(files.keySet())
            rows.add(new Row(cid, meta, flat, files))
        }
        // A position that is also a Meta Map column is written file.<position>.
        final Map<String, String> names = new LinkedHashMap<String, String>()
        for( String position : positions )
            names.put(position, metaColumns.contains(position) ? "file.${position}".toString() : position)
        return new Samplesheet(rows, new ArrayList<String>(metaColumns), names)
    }

    String csv() {
        final StringBuilder out = new StringBuilder()
        out.append(columns.collect { String c -> quote(c) }.join(',')).append('\n')
        for( Row row : rows ) {
            final List<String> cells = new ArrayList<String>()
            for( String column : metaColumns )
                cells.add(quote(text(column, row.flat.get(column))))
            for( Map.Entry<String, String> file : fileColumnNames.entrySet() )
                cells.add(quote(row.files.get(file.key) ?: ''))
            out.append(cells.join(',')).append('\n')
        }
        return out.toString()
    }

    String json() {
        final List<Map<String, Object>> out = new ArrayList<Map<String, Object>>()
        for( Row row : rows ) {
            final Map<String, Object> entry = new LinkedHashMap<String, Object>(row.meta)
            for( Map.Entry<String, String> file : row.files.entrySet() )
                entry.put(fileColumnNames.get(file.key), file.value)
            out.add(entry)
        }
        return JsonOutput.prettyPrint(JsonOutput.toJson(out)) + '\n'
    }

    // ------------------------------------------------------------------ plumbing

    private static Map load(BlockStore store, Cid cid) {
        if( !store.has(cid) )
            throw new IllegalStateException("output item ${cid} is not in any member of this composition")
        final InputStream input = store.open(cid)
        try {
            return (Map) DagCbor.decode(input.readAllBytes())
        }
        finally {
            input.close()
        }
    }

    /** MetadataView.of over a decoded item, whose leaves are Leaf objects rather than maps. */
    private static Object viewOf(Object value) {
        if( value instanceof Map )
            return value
        if( value instanceof List )
            for( Object element : (List) value )
                if( element instanceof Map )
                    return element
        return null
    }

    private static Object withoutLeaves(Object value) {
        if( value instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map) value).entrySet() )
                if( !(e.value instanceof Leaf) )
                    out.put(String.valueOf(e.key), withoutLeaves(e.value))
            return out
        }
        if( value instanceof List )
            return ((List) value).findAll { Object v -> !(v instanceof Leaf) }.collect { Object v -> withoutLeaves(v) }
        if( value instanceof Cid )
            return value.toString()
        return value
    }

    private static void flatten(Map<String, Object> map, String prefix, Map<String, Object> out) {
        for( Map.Entry<String, Object> e : map.entrySet() ) {
            final String path = prefix ? "${prefix}.${e.key}".toString() : e.key
            if( e.value instanceof Map )
                flatten((Map<String, Object>) e.value, path, out)
            else
                out.put(path, e.value)
        }
    }

    private static void collectFiles(Object value, String position, Map<String, String> files) {
        if( value instanceof Leaf ) {
            final Leaf leaf = (Leaf) value
            // A bare file item has no position; its one column is `file`.
            files.put(position ?: 'file', !leaf.addressed ? '' : leaf.address.isRaw()
                ? "cas://${leaf.address}/${leaf.name}".toString()
                : "cas://${leaf.address}".toString())
            return
        }
        if( value instanceof Map ) {
            for( Map.Entry e : ((Map) value).entrySet() )
                collectFiles(e.value, position ? "${position}.${e.key}".toString() : String.valueOf(e.key), files)
            return
        }
        if( value instanceof List ) {
            final List list = (List) value
            for( int i = 0; i < list.size(); i++ )
                collectFiles(list[i], position ? "${position}.${i}".toString() : String.valueOf(i), files)
        }
    }

    /** The index's text form for a scalar (MetadataView.scalar), JSON for a list or map, blank for absent or null. */
    private static String text(String path, Object value) {
        if( value == null )
            return ''
        if( value instanceof String )
            return (String) value
        if( value instanceof List || value instanceof Map )
            return JsonOutput.toJson(value)
        return MetadataView.scalar(path, value).value
    }

    /** RFC 4180: quote a field holding a comma, a quote, CR or LF, doubling its quotes. */
    private static String quote(String field) {
        if( field.indexOf(',') < 0 && field.indexOf('"') < 0 && field.indexOf('\n') < 0 && field.indexOf('\r') < 0 )
            return field
        return '"' + field.replace('"', '""') + '"'
    }
}
```

The `[meta, [chunks]]` item's metadata view is the first map; the file list at index 1 yields positions `1.0`, `1.1`. `collectFiles` also walks the Meta Map at index 0, which holds no leaves.

- [ ] **Step 4: Implement the route and the `Exporter`**

In `ExploreServer.groovy`:

```groovy
    /** The samplesheet export of a Selection (decision 1 of the milestone 2 plan). */
    static interface Exporter {
        byte[] samplesheet(Cid selection, String format)
    }

    private static final Pattern SAMPLESHEET = ~/^\/api\/samplesheet\/(b[a-z2-7]{58})\.(csv|json)$/
    private final Exporter exporter
```

Chain the constructors (three → five → six arguments, the new last one `Exporter exporter`). In `route`, before the `MEMBER` match:

```groovy
        final Matcher sheet = SAMPLESHEET.matcher(path)
        if( sheet.matches() && exporter != null ) {
            samplesheet(exchange, Cid.parse(sheet.group(1)), sheet.group(2))
            return
        }
```

```groovy
    private void samplesheet(HttpExchange exchange, Cid selection, String format) {
        final byte[] body
        try {
            body = exporter.samplesheet(selection, format)
        }
        catch( IllegalStateException | IllegalArgumentException e ) {
            text(exchange, 404, e.message)
            return
        }
        exchange.responseHeaders.set('Content-Disposition', "attachment; filename=\"selection-${selection.toString().take(16)}.${format}\"".toString())
        bytes(exchange, 200, format == 'csv' ? 'text/csv; charset=utf-8' : 'application/json', body)
    }
```

In `ExploreCommand.start`, build it over the same index and lock as the `Put`:

```groovy
        final Put put = session.newPut(index)
        final ExploreServer.Exporter exporter = { Cid selection, String format ->
            synchronized( put ) {
                session.catchUpIndex(index)
                final Samplesheet sheet = Samplesheet.of(session.store, index.selectionItems(selection))
                return (format == 'csv' ? sheet.csv() : sheet.json()).getBytes('UTF-8')
            }
        } as ExploreServer.Exporter
        final ExploreServer server = new ExploreServer(membersOf(cas), cas.writableAlias, IndexSnapshot.bundledPage(), put, token, exporter)
            .start(options.intFlag('port', 0))
```

`Put.put` is `synchronized` on the `Put` instance, so the export and a write never share the index connection at once.

- [ ] **Step 5: Run the tests, amend `DESIGN.md` §15, commit**

Run: `./gradlew test`
Expected: PASS.

§15, the `explore` table: `GET /api/samplesheet/<selection>.csv` and `.json` (decisions 1 and 18 of the milestone 2 plan), `404` naming the reason for an address that is not a Selection the index holds or that reaches one it does not.

```bash
git add src/main/groovy/robsyme/cas/core/Samplesheet.groovy src/test/groovy/robsyme/cas/core/SamplesheetTest.groovy \
        src/main/groovy/robsyme/cas/explore src/test/groovy/robsyme/cas/explore DESIGN.md
git commit -m "feat: samplesheet export of a Selection, CSV and JSON, served by explore

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: The page's core for Selections and Claims

The model and helpers the views of Task 14 need, all unit-tested in Node: Selections and Claims from the snapshot and the Store Log tail, current state by the shared vectors, hidden runs, the tray, and the DAG-JSON write client. No DOM here.

**Files:**
- Create: `web/src/claims.js`, `web/src/tray.js`, `web/src/write.js`
- Modify: `web/src/model.js`, `web/src/queries.json`, `web/src/config.js`, `web/src/store.js`
- Modify: `web/test/helpers.mjs` (export `snapshotDb`), `web/test/model.test.mjs` (import it), `web/test/fixture.mjs` (an `extra` hook)
- Create: `web/test/claims.test.mjs`, `web/test/tray.test.mjs`, `web/test/write.test.mjs`, `web/test/selections.test.mjs`
- Modify: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy` (the new queries in the no-scan table)

**Interfaces:**
- Consumes: `queries.json`, `BlockFetcher.ofKind`, `BlockError`, `entriesSince`, `@ipld/dag-json`, `multiformats/cid`, `POST /api/put` (Task 10), `members.json`'s `write` (Task 10).
- Produces:
  - `claims.js`: `NONE`, `DELETED`, `CONFLICTED`; `claimState(claims) -> { current, names, nameClaims, nameConflicted, deletion, deletionClaims, hidden }`, where each claim is `{ cid, verb, attribute, value, supersedes: [cid] }` plus any display fields, and each `current` entry gains `conflicted`.
  - `tray.js`: `class Tray(storage)` with `add({ address, via, kind })`, `remove(address)`, `clear()`, `has(address)`, `size`, `entries()` (sorted by address), `onlyOneSelection()`, `toMembers()` (DAG-JSON-ready, `CID` objects); `safeSessionStorage()`.
  - `write.js`: `class WriteError extends Error { code, at, status }`; `writer({ endpoint, token, fetchFn, now })` with `selection(members, { dryRun })`, `rename(subject, name, supersedes)`, `remove(subject, supersedes)`, `undo(subject, supersedes)`, each resolving to the response with `address` as a string.
  - `model.js` (`Explorer`): `tailSelections`, `tailClaims` (each `{ cid, entry, value, error }`); `claimStates(subjects) -> Map<subject, state & { claims }>`; `selectionPage({ offset, limit, showDeleted }) -> { rows, hiddenCount, first, last, total, next, prev }`, each row `{ cid, firstSeen, assertedBy, source, error, state }`; `selection(cid) -> { cid, block, state, firstSeen, members: [{ kind, address, via }] }`; `held(kind, address) -> 'here' | 'elsewhere'`; `selectionsHolding(itemCid) -> [cid]`; `runPage` rows exclude hidden runs and report `hidden`; `latestSuccessfulRun` respects tail delete Claims. `Explorer.closures` (dead since milestone 1) is removed.
  - `config.js`: `SELECTIONS_PAGE = 50`; `store.js`: `resolveStore` also returns `write` (true when `members.json` says so).

- [ ] **Step 1: Add the queries and extend the guard**

Add to `web/src/queries.json`:

```json
  "selectionsKnown": "SELECT collection_cid FROM collection WHERE collection_cid IN (SELECT value FROM json_each(?)) AND kind = 'selection'",
  "claimsKnown": "SELECT claim_cid FROM claim WHERE claim_cid IN (SELECT value FROM json_each(?))",
  "claimsOf": "SELECT claim_cid, subject_cid, verb, attribute, value, timestamp, asserted_by FROM claim WHERE subject_cid IN (SELECT value FROM json_each(?))",
  "supersedesOf": "SELECT claim_cid, superseded_cid FROM claim_supersedes WHERE superseded_cid IN (SELECT claim_cid FROM claim WHERE subject_cid IN (SELECT value FROM json_each(?)))",
  "selectionsPage": "SELECT l.cid AS selection_cid, l.written_at, c.asserted_by FROM log_entry l JOIN collection c ON c.collection_cid = l.cid WHERE l.kind = 'selection' ORDER BY l.written_at DESC, l.cid ASC LIMIT ? OFFSET ?",
  "selectionCount": "SELECT count(*) AS n FROM log_entry WHERE kind = 'selection'",
  "firstSeen": "SELECT written_at FROM log_entry WHERE cid = ? ORDER BY written_at LIMIT 1",
  "selectionsHolding": "SELECT DISTINCT ci.collection_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE ci.item_cid = ? AND c.kind = 'selection' ORDER BY ci.collection_cid",
  "successfulRunsOfPipeline": "SELECT completion_cid, finished_at FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 50"
```

`supersedesOf` finds the edges into a subject's Claims; the model keeps those whose superseding Claim is also one of that subject's, which is exactly `ClaimCurrent.load`'s set (it avoids a `claim_supersedes(claim_cid)` index the guard would otherwise need).

Add a row per new name to `ExplorerQueriesTest`'s `where:` table (`'selectionsKnown' | page.selectionsKnown` and so on). Run `./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`; expected PASS. If one plans a `SCAN`, fix the SQL, not the guard.

- [ ] **Step 2: Write the failing tests**

```js
// web/test/claims.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { claimState } from '../src/claims.js'

const vectors = JSON.parse(readFileSync(new URL('./fixtures/claim-vectors.json', import.meta.url), 'utf8'))

for (const v of vectors) {
  test(`claim state: ${v.name}`, () => {
    const s = claimState(v.claims)
    assert.deepEqual(s.current.map(c => c.cid), v.expect.current)
    assert.deepEqual(s.names, v.expect.names)
    assert.equal(s.nameConflicted, v.expect.nameConflicted)
    assert.equal(s.deletion, v.expect.deletion)
    assert.equal(s.hidden, v.expect.deletion === 'deleted')
  })
}
```

```js
// web/test/tray.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Tray } from '../src/tray.js'
import { block } from './fixture.mjs'

const I1 = block({ n: 1 }).cid.toString()
const I2 = block({ n: 2 }).cid.toString()
const C1 = block({ c: 1 }).cid.toString()
const C2 = block({ c: 2 }).cid.toString()

function memoryStorage() {
  const m = new Map()
  return { getItem: k => m.get(k) ?? null, setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }
}

test('an item picked twice is one entry with both vias (Review Focus 2)', () => {
  const t = new Tray(null)
  t.add({ address: I1, via: [C2], kind: 'item' })
  t.add({ address: I1, via: [C1], kind: 'item' })
  assert.equal(t.size, 1)
  assert.deepEqual(t.entries()[0].via, [C1, C2].sort())
})

test('toMembers is DAG-JSON-ready: CID objects, sorted by address', () => {
  const t = new Tray(null)
  t.add({ address: I2, via: [], kind: 'item' })
  t.add({ address: I1, via: [C1], kind: 'selection' })
  const addresses = t.toMembers().map(m => m.item?.address ?? m.selection)
  assert.ok(addresses.every(a => CID.asCID(a) !== null))
  assert.deepEqual(addresses.map(String), [I1, I2].sort())
  assert.deepEqual(t.toMembers().map(m => (m.item ? 'item' : 'selection')), [I1, I2].sort().map(a => (a === I1 ? 'selection' : 'item')))
})

test('a tray holding only one Selection is flagged, since the explorer does not offer that Selection', () => {
  const t = new Tray(null)
  t.add({ address: I1, via: [], kind: 'selection' })
  assert.equal(t.onlyOneSelection(), true)
  t.add({ address: I2, via: [], kind: 'item' })
  assert.equal(t.onlyOneSelection(), false)
})

test('the tray survives a reload through its storage, and works without one', () => {
  const storage = memoryStorage()
  new Tray(storage).add({ address: I1, via: [C1], kind: 'item' })
  assert.deepEqual(new Tray(storage).entries().map(e => e.address), [I1])
  const broken = { getItem: () => { throw new Error('denied') }, setItem: () => { throw new Error('denied') } }
  const t = new Tray(broken)
  t.add({ address: I1, via: [], kind: 'item' })
  assert.equal(t.size, 1)
})
```

```js
// web/test/write.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { writer, WriteError } from '../src/write.js'
import { block } from './fixture.mjs'

const S = block({ s: 1 }).cid
const N = block({ n: 1 }).cid

function fakeFetch(respond) {
  const calls = []
  const fn = async (url, init) => {
    calls.push({ url, init, body: dagJson.decode(init.body) })
    return respond(url, init)
  }
  fn.calls = calls
  return fn
}

const ok = (value) => new Response(dagJson.encode(value), { status: 200, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })

test('a rename posts the DAG-JSON Claim with the token, superseding what was shown', async () => {
  const fetchFn = fakeFetch(() => ok({ address: N, block: {}, entry: 'e', written: true }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't0k', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  const r = await w.rename(S.toString(), 'tumour, "batch 2"', [N.toString()])
  const [call] = fetchFn.calls
  assert.equal(call.url, 'http://h/api/put')
  assert.equal(call.init.method, 'POST')
  assert.equal(call.init.headers['Content-Type'], 'application/vnd.ipld.dag-json')
  assert.equal(call.init.headers['X-NF-Blocks-Token'], 't0k')
  assert.deepEqual(call.body, { kind: 'Claim', subject: S, verb: 'set', attribute: 'name', value: 'tumour, "batch 2"', supersedes: [N], timestamp: '2026-09-25T10:00:00.000Z' })
  assert.equal(r.address, N.toString())
})

test('delete and undo supersede the deletion Claims shown; a dry run asks with ?dry_run=true', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: false, names: [] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  await w.remove(S.toString(), [])
  await w.undo(S.toString(), [N.toString()])
  await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.equal(fetchFn.calls[0].body.verb, 'delete')
  assert.equal(fetchFn.calls[0].body.attribute, null)
  assert.deepEqual(fetchFn.calls[1].body.supersedes, [N])
  assert.equal(fetchFn.calls[1].body.verb, 'del')
  assert.equal(fetchFn.calls[2].url, 'http://h/api/put?dry_run=true')
  assert.deepEqual(fetchFn.calls[2].body, { kind: 'Selection', members: [{ item: { address: N, via: [] } }], derived_from: [] })
})

test('a refusal becomes a WriteError with its code and where; a plain-text refusal keeps its status', async () => {
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn: fakeFetch(() => new Response(
    dagJson.encode({ error: 'stale_supersedes', message: 'claim is already superseded', at: '/supersedes/0' }),
    { status: 400, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })) })
  await assert.rejects(w.rename(S.toString(), 'x', [N.toString()]), (e) => e instanceof WriteError && e.code === 'stale_supersedes' && e.at === '/supersedes/0' && e.status === 400)
  const plain = writer({ endpoint: 'http://h/api/put', token: 'wrong', fetchFn: fakeFetch(() => new Response('refused', { status: 403 })) })
  await assert.rejects(plain.remove(S.toString(), []), (e) => e instanceof WriteError && e.code === 'forbidden' && e.status === 403)
})
```

```js
// web/test/selections.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName } from './fixture.mjs'

const claim = (subject, verb, attribute, value, supersedes, timestamp = '2026-09-01T10:00:00.000Z') =>
  ({ kind: 'Claim', schema: 1, asserted_by: 'test', subject, verb, attribute, value, supersedes, timestamp })

/**
 * S1 (items A and B of R1) and its name N1 are in the snapshot. The tail holds
 * N2 renaming S1, S2 (nesting S1 and a Selection this member lacks, plus item
 * C chosen by query), a delete of S2, and a delete of run R2.
 */
async function open() {
  const now = Date.now()
  const ids = {}
  const member = await buildMember({
    now,
    extra: ({ put, runs, item, at }) => {
      // The fixture keeps run addresses as strings; inside a block a link must be a CID.
      const r1coll = runs.R1.collection
      ids.S1 = put({ kind: 'Selection', schema: 1, asserted_by: 'test', derived_from: [],
        members: [item.A, item.B].sort((a, b) => (a.toString() < b.toString() ? -1 : 1)).map(i => ({ item: { address: i, via: [CID.parse(r1coll)] } })) })
      ids.N1 = put(claim(ids.S1, 'set', 'name', 'first', []))
      ids.N2 = put(claim(ids.S1, 'set', 'name', 'second', [ids.N1]))
      ids.Sx = block({ kind: 'Selection', schema: 1, asserted_by: 'elsewhere', members: [], derived_from: [] }).cid     // never stored here
      ids.S2 = put({ kind: 'Selection', schema: 1, asserted_by: 'test', derived_from: [],
        members: [{ selection: ids.S1 }, { selection: ids.Sx }, { item: { address: item.C, via: [] } }]
          .sort((a, b) => { const k = (m) => (m.selection ?? m.item.address).toString(); return k(a) < k(b) ? -1 : 1 }) })
      ids.D = put(claim(ids.S2, 'delete', null, null, []))
      ids.DR = put(claim(CID.parse(runs.R2.completion), 'delete', null, null, []))
      const t = at.R1 + 1
      return {
        log: [entryName(t, 'selection', ids.S1), entryName(t, 'claim', ids.N1),
              entryName(now - 100_000, 'claim', ids.N2), entryName(now - 90_000, 'selection', ids.S2),
              entryName(now - 80_000, 'claim', ids.D), entryName(now - 70_000, 'claim', ids.DR)],
        rows: (db) => {
          const s1 = ids.S1.toString()
          db.exec({ sql: "INSERT INTO collection(collection_cid, kind, asserted_by) VALUES (?, 'selection', 'test')", bind: [s1] })
          for (const i of [item.A, item.B])
            db.exec({ sql: 'INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, ?)', bind: [s1, i.toString(), r1coll] })
          db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'selection', NULL, '2026-09-01T10:00:00.000Z')", bind: [s1] })
          db.exec({ sql: "INSERT INTO claim VALUES (?, ?, 'set', 'name', 'first', '2026-09-01T10:00:00.000Z', 'test')", bind: [ids.N1.toString(), s1] })
          db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'claim', NULL, '2026-09-01T10:00:00.000Z')", bind: [ids.N1.toString()] })
          db.exec({ sql: "INSERT INTO claim_current VALUES (?, 'name', 'first', ?, 0)", bind: [s1, ids.N1.toString()] })
        },
      }
    },
  })
  const text = Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()]))
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }),
    now: () => now,
  })
  return { explorer, member, ids: text }
}

test('the tail brings Selections and Claims the snapshot lacks, and only those', async () => {
  const { explorer, ids } = await open()
  assert.deepEqual(explorer.tailSelections.filter(t => t.value).map(t => t.cid), [ids.S2])
  assert.deepEqual(explorer.tailClaims.map(t => t.cid).sort(), [ids.N2, ids.D, ids.DR].sort())
})

test('a tail rename supersedes a snapshot name; a deleted Selection leaves the list for the deleted one', async () => {
  const { explorer, ids } = await open()
  const page = await explorer.selectionPage()
  const s1 = page.rows.find(r => r.cid === ids.S1)
  assert.deepEqual(s1.state.names, ['second'])
  assert.equal(s1.source, 'snapshot')
  assert.ok(!page.rows.some(r => r.cid === ids.S2))
  assert.equal(page.hiddenCount, 1)
  const deleted = await explorer.selectionPage({ showDeleted: true })
  assert.deepEqual(deleted.rows.map(r => [r.cid, r.state.deletion, r.source]), [[ids.S2, 'deleted', 'tail']])
})

test('a Selection view has its members, first-seen time and current state', async () => {
  const { explorer, member, ids } = await open()
  const s = await explorer.selection(ids.S2)
  assert.equal(s.state.deletion, 'deleted')
  assert.deepEqual(s.members.map(m => [m.kind, m.address]).sort(), [
    ['selection', ids.S1], ['selection', ids.Sx], ['item', member.item.C]].sort())
  assert.match(s.firstSeen, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
})

test('a nested Selection this member lacks is held elsewhere, not an error (Review Focus 5)', async () => {
  const { explorer, member, ids } = await open()
  assert.equal(await explorer.held('selection', ids.Sx), 'elsewhere')
  assert.equal(await explorer.held('selection', ids.S1), 'here')
  assert.equal(await explorer.held('item', member.item.C), 'here')
})

test('a run hidden by a tail delete Claim leaves the run list and query 2', async () => {
  const { explorer, member } = await open()
  const page = await explorer.runPage('demo')
  assert.ok(!page.rows.some(r => r.completion_cid === member.runs.R2.completion))
  assert.equal(page.hidden, 1)
  assert.equal(await explorer.latestSuccessfulRun('demo'), member.runs.R1.completion)
})

test('selectionsHolding finds the snapshot Selections an item is in', async () => {
  const { explorer, member, ids } = await open()
  assert.deepEqual(await explorer.selectionsHolding(member.item.A), [ids.S1])
})
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd web && npm test`
Expected: the four new files FAIL (modules missing; `buildMember` ignores `extra`).

- [ ] **Step 4: Extend the fixture and helpers**

Move `snapshotDb` from `model.test.mjs` into `helpers.mjs` as an export (give each VFS a unique name from a module-level counter, as it does now) and import it in `model.test.mjs`.

In `fixture.mjs`, `buildMember({ now = Date.now(), extra = null } = {})`: after the runs are put and `at` is computed, call `const more = extra ? extra({ put, runs, item, content, at }) : { log: [], rows: null }`, append `more.log` to `log`, and pass `more.rows` to `snapshotOf` as a fifth argument, which calls `rows(db)` (when given) before the `meta` inserts. The existing `entryName(at.R3 + 1, 'selection', runs.R3.completion)` entry stays: it now yields an unreadable tail Selection (a RunCompletion is not a Selection), which the views list with its error as they do an unreadable run. Update its comment to say so.

- [ ] **Step 5: Implement `claims.js`, `tray.js` and `write.js`**

```js
// web/src/claims.js
// A subject's current state from its Claims (block explorer spec section 8;
// decision 5 of the milestone 2 plan). The same rules as ClaimState.groovy;
// test/fixtures/claim-vectors.json pins both.
export const NONE = 'none'
export const DELETED = 'deleted'
export const CONFLICTED = 'conflicted'

const DELETION = '\u0000deletion'
const groupOf = (c) => (c.attribute === null || c.attribute === undefined ? DELETION : c.attribute)
const byCid = (a, b) => (a.cid < b.cid ? -1 : a.cid > b.cid ? 1 : 0)

export function claimState(claims) {
  const superseded = new Set(claims.flatMap(c => c.supersedes ?? []))
  const current = claims.filter(c => !superseded.has(c.cid)).sort(byCid)
  const sizes = new Map()
  for (const c of current) sizes.set(groupOf(c), (sizes.get(groupOf(c)) ?? 0) + 1)
  const nameGroup = current.filter(c => c.attribute === 'name')
  const deletionGroup = current.filter(c => groupOf(c) === DELETION && (c.verb === 'delete' || c.verb === 'del'))
  const deletion = deletionGroup.length > 1 ? CONFLICTED
    : deletionGroup.length === 1 && deletionGroup[0].verb === 'delete' ? DELETED : NONE
  return {
    current: current.map(c => ({ ...c, conflicted: sizes.get(groupOf(c)) > 1 })),
    names: nameGroup.filter(c => c.verb === 'set').map(c => String(c.value)),
    nameClaims: nameGroup.map(c => c.cid),
    nameConflicted: nameGroup.length > 1,
    deletion,
    deletionClaims: deletionGroup.map(c => c.cid),
    hidden: deletion === DELETED,
  }
}
```

```js
// web/src/tray.js
// The items picked for a Selection (spec section 5.6), kept per tab so picks
// survive switching member (decision 15). Storage may be absent or refuse
// every call; the tray then lives only as long as the page.
import { CID } from 'multiformats/cid'

const KEY = 'nf-blocks-tray'

export function safeSessionStorage() {
  try { return globalThis.sessionStorage ?? null } catch { return null }
}

export class Tray {
  constructor(storage = safeSessionStorage()) {
    this.storage = storage
    this.items = new Map()
    try {
      for (const e of JSON.parse(storage?.getItem(KEY) ?? '[]')) this.items.set(e.address, { ...e, via: new Set(e.via) })
    } catch {
      // No storage, or nothing readable in it.
    }
  }

  add({ address, via = [], kind = 'item' }) {
    const seen = this.items.get(address) ?? { address, kind, via: new Set() }
    for (const v of via) seen.via.add(v)
    this.items.set(address, seen)
    this.persist()
  }

  remove(address) { this.items.delete(address); this.persist() }

  clear() { this.items.clear(); this.persist() }

  has(address) { return this.items.has(address) }

  get size() { return this.items.size }

  entries() {
    return [...this.items.values()].map(e => ({ address: e.address, kind: e.kind, via: [...e.via].sort() }))
      .sort((a, b) => (a.address < b.address ? -1 : a.address > b.address ? 1 : 0))
  }

  /** Spec section 7.2: legal, but the explorer does not offer a Selection whose only member is a Selection. */
  onlyOneSelection() { return this.items.size === 1 && [...this.items.values()][0].kind === 'selection' }

  /** Members for a DAG-JSON Selection request; the server normalises again. */
  toMembers() {
    return this.entries().map(e => (e.kind === 'selection'
      ? { selection: CID.parse(e.address) }
      : { item: { address: CID.parse(e.address), via: e.via.map(v => CID.parse(v)) } }))
  }

  persist() {
    try {
      this.storage?.setItem(KEY, JSON.stringify(this.entries()))
    } catch {
      // Storage refused; the tray still works for this page.
    }
  }
}
```

```js
// web/src/write.js
// The page's only write path: DAG-JSON requests to nf-blocks:explore's
// POST /api/put (spec sections 9.1 and 9.5). The page builds request
// content, never a block: the server encodes (spec section 1.4).
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'

export class WriteError extends Error {
  constructor(code, message, at, status) { super(message); this.code = code; this.at = at; this.status = status }
}

const cid = (text) => CID.parse(text)
const TRANSPORT = { 403: 'forbidden', 413: 'too_large', 415: 'unsupported_media_type' }

export function writer({ endpoint, token, fetchFn = (...a) => fetch(...a), now = () => new Date() }) {
  async function post(request, dryRun = false) {
    let res
    try {
      res = await fetchFn(dryRun ? `${endpoint}?dry_run=true` : endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': token },
        body: dagJson.encode(request),
      })
    } catch (e) {
      throw new WriteError('write_failed', `could not reach the explore server: ${e.message}`, '', 0)
    }
    const type = res.headers.get('Content-Type') ?? ''
    if (type.includes('application/vnd.ipld.dag-json')) {
      const body = dagJson.decode(new Uint8Array(await res.arrayBuffer()))
      if (!res.ok) throw new WriteError(body.error, body.message, body.at, res.status)
      return { ...body, address: body.address.toString() }
    }
    throw new WriteError(TRANSPORT[res.status] ?? 'write_failed', (await res.text()).trim() || `HTTP ${res.status}`, '', res.status)
  }
  const claim = (subject, verb, attribute, value, supersedes) => post({
    kind: 'Claim', subject: cid(subject), verb, attribute, value, supersedes: supersedes.map(cid), timestamp: now().toISOString() })
  return {
    selection: (members, { dryRun = false } = {}) => post({ kind: 'Selection', members, derived_from: [] }, dryRun),
    rename: (subject, name, supersedes) => claim(subject, 'set', 'name', name, supersedes),
    remove: (subject, supersedes) => claim(subject, 'delete', null, null, supersedes),
    undo: (subject, supersedes) => claim(subject, 'del', null, null, supersedes),
  }
}
```

`remove` and `undo` both supersede every current Claim of the deletion group the page showed, so either resolves a conflicted deletion to one current Claim (the vectors "delete again after an undo" and "an undo and a delete ... conflict").

- [ ] **Step 6: Extend `model.js`, `store.js` and `config.js`**

`config.js`: `export const SELECTIONS_PAGE = 50`. `store.js`: when `members.json` answers, return `write: body.write === true` beside `members` and `member` (`false` in the other two branches).

In `model.js`, import `{ DELETED, claimState }` from `./claims.js` and `SELECTIONS_PAGE` from `./config.js`, drop `this.closures` (and its `set` in `buildClosure`), and add:

```js
const isoOf = (millis) => new Date(millis).toISOString()
```

Constructor: `this.tailSelections = []` and `this.tailClaims = []`.

```js
  async refreshTail(listFn) {
    const { names, readable } = await listFn(this.base, { watermark: this.watermark, nowMillis: this.now() })
    this.logReadable = readable
    const since = entriesSince(names, this.watermark, this.now())
    const unique = (kind) => [...new Map(since.filter(e => e.kind === kind).map(e => [e.cid, e])).values()]
    const known = async (sql, entries, column) => (entries.length === 0 ? new Set()
      : new Set((await this.db.query(sql, [JSON.stringify(entries.map(e => e.cid))])).map(r => r[column])))
    const runs = unique('run')
    const selections = unique('selection')
    const claims = unique('claim')
    const knownRuns = await known(SQL.runsKnown, runs, 'completion_cid')
    const knownSelections = await known(SQL.selectionsKnown, selections, 'collection_cid')
    const knownClaims = await known(SQL.claimsKnown, claims, 'claim_cid')
    this.stale = await Promise.all(runs.filter(e => !knownRuns.has(e.cid)).map(e => this.staleRun(e)))
    this.tailSelections = await Promise.all(selections.filter(e => !knownSelections.has(e.cid)).map(e => this.tailBlock(e, 'Selection')))
    this.tailClaims = await Promise.all(claims.filter(e => !knownClaims.has(e.cid)).map(e => this.tailBlock(e, 'Claim')))
  }

  async tailBlock(entry, kind) {
    try {
      return { cid: entry.cid, entry, value: (await this.blocks.ofKind(entry.cid, kind)).value, error: null }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      return { cid: entry.cid, entry, value: null, error: e }
    }
  }

  /** Every Claim this page can see about each subject: the snapshot's rows and the tail's blocks. */
  async claimsFor(subjects) {
    const wanted = [...new Set(subjects)]
    const out = new Map(wanted.map(s => [s, []]))
    if (wanted.length === 0) return out
    const json = JSON.stringify(wanted)
    const byCid = new Map()
    for (const r of await this.db.query(SQL.claimsOf, [json])) {
      const c = { cid: r.claim_cid, subject: r.subject_cid, verb: r.verb, attribute: r.attribute, value: r.value,
        timestamp: r.timestamp, asserted_by: r.asserted_by, supersedes: [], source: 'snapshot' }
      byCid.set(c.cid, c)
      out.get(c.subject).push(c)
    }
    for (const s of await this.db.query(SQL.supersedesOf, [json]))
      byCid.get(s.claim_cid)?.supersedes.push(s.superseded_cid)
    for (const t of this.tailClaims.filter(t => t.value)) {
      const subject = text(t.value.subject)
      if (!out.has(subject)) continue
      out.get(subject).push({ cid: t.cid, subject, verb: t.value.verb, attribute: t.value.attribute, value: t.value.value,
        timestamp: t.value.timestamp, asserted_by: t.value.asserted_by, supersedes: t.value.supersedes.map(text), source: 'tail' })
    }
    return out
  }

  async claimStates(subjects) {
    const claims = await this.claimsFor(subjects)
    return new Map([...claims].map(([subject, list]) => [subject, { ...claimState(list), claims: list }]))
  }

  /** A page of this member's Selections, newest first seen first; the tail's all on the first page. */
  async selectionPage({ offset = 0, limit = SELECTIONS_PAGE, showDeleted = false } = {}) {
    const snapshot = (await this.db.query(SQL.selectionsPage, [limit, offset]))
      .map(r => ({ cid: r.selection_cid, firstSeen: r.written_at, assertedBy: r.asserted_by, source: 'snapshot', error: null }))
    const tail = offset === 0 ? this.tailSelections.map(t => ({ cid: t.cid, firstSeen: isoOf(t.entry.writtenAtMillis),
      assertedBy: t.value?.asserted_by ?? null, source: 'tail', error: t.error })) : []
    const rows = [...tail.sort((a, b) => (a.firstSeen > b.firstSeen ? -1 : 1)), ...snapshot]
    const states = await this.claimStates(rows.map(r => r.cid))
    const all = rows.map(r => ({ ...r, state: states.get(r.cid) }))
    const shown = all.filter(r => (showDeleted ? r.state.deletion === DELETED : !r.state.hidden))
    const [{ n }] = await this.db.query(SQL.selectionCount)
    return { rows: shown, hiddenCount: all.length - shown.length,
      ...span({ offset, limit, shown: snapshot.length, count: n, tail: tail.length }) }
  }

  async selection(cid) {
    const block = (await this.blocks.ofKind(cid, 'Selection')).value
    const state = (await this.claimStates([cid])).get(cid)
    const [seen] = await this.db.query(SQL.firstSeen, [cid])
    const tail = this.tailSelections.find(t => t.cid === cid)
    return {
      cid, block, state,
      firstSeen: seen?.written_at ?? (tail ? isoOf(tail.entry.writtenAtMillis) : null),
      members: block.members.map(m => (m.item
        ? { kind: 'item', address: text(m.item.address), via: m.item.via.map(text) }
        : { kind: 'selection', address: text(m.selection) })),
    }
  }

  /** Whether this member holds a member's block (spec section 4: an item may be held in another member). */
  async held(kind, address) {
    try {
      await this.blocks.ofKind(address, kind === 'selection' ? 'Selection' : 'OutputItem')
      return 'here'
    } catch (e) {
      if (e instanceof BlockError && e.code === 'block_missing') return 'elsewhere'
      throw e
    }
  }

  async selectionsHolding(itemCid) {
    const snapshot = (await this.db.query(SQL.selectionsHolding, [itemCid])).map(r => r.collection_cid)
    const tail = this.tailSelections.filter(t => t.value?.members.some(m => text(m.item?.address) === itemCid)).map(t => t.cid)
    return [...new Set([...snapshot, ...tail])].sort()
  }
```

In `runPage`, after `rows` is computed:

```js
    const states = await this.claimStates(rows.map(r => r.completion_cid))
    const visible = rows.filter(r => !states.get(r.completion_cid).hidden)
```

and return `{ rows: visible, hidden: rows.length - visible.length, ...span(...) }` (the span still counts snapshot rows, so paging is unchanged).

In `latestSuccessfulRun`, leave the no-tail path exactly as it is (Gate assertion 2 counts it). When `this.tailClaims.length > 0`, compute the candidate list as now, then drop hidden ones and, if that empties it, fall back to the snapshot's next runs:

```js
    if (this.tailClaims.length === 0) return /* the existing answer */
    const states = await this.claimStates(candidates.map(c => c.completion_cid))
    const visible = candidates.filter(c => !states.get(c.completion_cid).hidden).sort(byNewest)
    if (visible.length) return visible[0].completion_cid
    const more = await this.db.query(SQL.successfulRunsOfPipeline, [pipeline])
    const moreStates = await this.claimStates(more.map(r => r.completion_cid))
    return more.find(r => !moreStates.get(r.completion_cid).hidden)?.completion_cid ?? null
```

Restructure the method so both paths share the candidate computation; the SQL answer (`best`) already excludes runs the snapshot's Claims delete.

- [ ] **Step 7: Run the tests**

Run: `cd web && npm test`
Expected: PASS, the milestone 1 model tests included.

- [ ] **Step 8: Commit**

```bash
git add web/src web/test src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
git commit -m "feat(web): Selections and Claims in the page's model; tray and write client

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: The page's views: Selections, composing, rename, delete, undo

Spec section 5.6 on top of Task 13's model. Every element the Gate reads is a `data-*` attribute listed in `DESIGN.md` §16.

**Files:**
- Modify: `web/src/views.js`, `web/src/app.js`, `web/src/index.html`
- Modify: `gate/browser/page-smoke.mjs` (one compose-and-rename pass against a stub `/api/put`)
- Modify: `DESIGN.md` §15 (routes, the DOM contract) and new §16 opening

**Interfaces:**
- Consumes: Task 13's `Explorer` methods, `Tray`, `writer`, `WriteError`, `resolveStore`'s `write`.
- Produces:
  - Routes: `#/selections[?offset=<n>][&deleted=1]`, `#/selection/<cid>`, `#/compose`; `#/item/-/<item>` for an item with no collection.
  - DOM contract (read by Task 16):

| Selector | Meaning |
|---|---|
| `body[data-write]` | `available` or `unavailable` |
| `body[data-write-seq]`, `body[data-write-outcome]` | a counter bumped when a write attempt ends, and how: `written`, `exists`, or an error code |
| `body[data-written]` | the address the last successful write made |
| `#tray[data-count]` | items in the tray |
| `[data-pick]` | a button adding `data-pick` (an address) with `data-via` (space-separated collections) and `data-kind` (`item` or `selection`) |
| `[data-tray-entry]` | one tray entry on `#/compose`: `data-tray-entry` address, `data-kind` |
| `#compose-name`, `#compose-save` | the new Selection's name, and save |
| `[data-exists]` | the dry run found the Selection: `data-exists` address, `data-names` JSON |
| `[data-unavailable]` | why composing, rename and delete are unavailable |
| `[data-selection]` | one row of `#/selections`: `data-selection` cid, `data-deletion`, `data-source`, `data-names` JSON |
| `[data-selection-view]` | the Selection view: `data-selection-view` cid, `data-deletion` |
| `[data-name]` | one current name: `data-name` value, `data-claim`, `data-conflicted` when in conflict |
| `[data-deletion-claim]` | one current deletion Claim: `data-deletion-claim` cid, `data-verb` |
| `[data-member]` | one member: `data-member` address, `data-kind`, `data-held` (`here` or `elsewhere`) |
| `#rename-name`, `#rename-save`, `#delete`, `#undo` | the actions |
| `[data-samplesheet]` | `csv` or `json` export link (served by `explore` only) |
| `[data-hidden-runs]` | on a pipeline page, how many runs a delete Claim hides |

- [ ] **Step 1: Write the failing smoke check**

`gate/browser/page-smoke.mjs` drives the page over the fixture member with Playwright. Add a pass that serves a `members.json` with `"write": true`, answers `POST /api/put` with a canned DAG-JSON response (`{"address": {"/": "<S1>"}, "block": {}, "entry": "e", "written": true}`, recording the request body), opens `/?token=t#/item/<coll>/<item>`, clicks `[data-pick]`, opens `#/compose`, fills `#compose-name`, clicks `#compose-save`, and asserts: two POSTs (the dry run with `?dry_run=true`, then the write), one more for the name Claim, `body[data-write-outcome="written"]`, and `#tray[data-count="0"]`. Read the existing file first and add the pass in its style (its server, its `waitRender`).

Run: `node gate/browser/page-smoke.mjs` (after `cd web && npm run build`)
Expected: FAIL: no `[data-pick]`.

- [ ] **Step 2: Implement the views**

In `views.js`, generalise `pager` so a route may already carry a query, and say so plainly past the end (milestone 1's parked "pager label past the end"):

```js
function pager(route, page, noun) {
  if (page.total === 0) return null
  const at = (offset) => `${route}${route.includes('?') ? '&' : '?'}offset=${offset}`
  const label = page.first === 0 ? `no ${noun} on this page; there are ${page.total}` : `showing ${page.first}-${page.last} of ${page.total} ${noun}`
  return h('p', { 'data-page': '', 'data-first': page.first, 'data-last': page.last, 'data-total': page.total, class: 'muted' },
    label,
    page.prev !== null ? [' ', h('a', { href: at(page.prev), 'data-page-prev': '' }, 'previous page')] : null,
    page.next !== null ? [' ', h('a', { href: at(page.next), 'data-page-next': '' }, 'next page')] : null)
}
```

Add:

```js
const flag = (text) => h('span', { class: 'warn' }, ` (${text})`)

export function pickButton(ctx, { address, via = [], kind = 'item' }) {
  const inTray = ctx.tray.has(address)
  return h('button', { type: 'button', 'data-pick': address, 'data-kind': kind, 'data-via': via.join(' '), disabled: inTray,
    onclick: (event) => {
      ctx.tray.add({ address, via, kind })
      ctx.trayChanged()
      event.currentTarget.textContent = 'In the tray'
      event.currentTarget.disabled = true
    } }, inTray ? 'In the tray' : kind === 'selection' ? 'Add this Selection to the tray' : 'Add to the tray')
}

export async function selections(ex, { offset = 0, deleted = false }, ctx) {
  const page = await ex.selectionPage({ offset, showDeleted: deleted })
  const route = deleted ? '#/selections?deleted=1' : '#/selections'
  return h('section', {},
    h('h1', {}, deleted ? 'Deleted Selections' : 'Selections'),
    h('p', {}, deleted ? link('#/selections', 'Show current Selections')
      : [link('#/selections?deleted=1', 'Show deleted'), page.hiddenCount ? ` (${page.hiddenCount} on this page)` : '']),
    pager(route, page, 'Selections'),
    page.rows.length === 0 ? h('p', { class: 'muted' }, deleted ? 'No deleted Selections.' : 'No Selections in this member yet.')
      : table(['name', 'first seen', 'by', 'state', ''], page.rows.map(r => h('tr', {
        'data-selection': r.cid, 'data-deletion': r.state.deletion, 'data-source': r.source, 'data-names': JSON.stringify(r.state.names) },
      h('td', {}, link(`#/selection/${r.cid}`, r.state.names.length ? r.state.names.join(' / ') : h('span', { class: 'muted' }, 'unnamed')),
        r.state.nameConflicted ? flag('names in conflict') : null, r.error ? errorNode(r.error) : null),
      h('td', {}, r.firstSeen ?? ''), h('td', {}, r.assertedBy ?? ''),
      h('td', {}, r.state.deletion === 'conflicted' ? flag('deletion in conflict') : r.state.deletion === 'deleted' ? 'deleted' : ''),
      h('td', {}, deleted ? undoButton(r.cid, r.state, ctx) : null)))))
}

function undoButton(cid, state, ctx) {
  if (!ctx.write.available) return null
  const status = h('span', {})
  return [h('button', { type: 'button', id: 'undo', onclick: () => ctx.write.run(status, async () => {
    await ctx.write.writer.undo(cid, state.deletionClaims)
    return { href: `#/selection/${cid}` }
  }) }, 'Undo'), status]
}

export async function selection(ex, selectionCid, ctx) {
  let s
  try {
    s = await ex.selection(selectionCid)
  } catch (e) {
    if (e.code === 'block_missing') e.message = `Selection ${selectionCid} is not in this member; it may be held in another one`
    throw e
  }
  const st = s.state
  const status = h('div', { id: 'write-status' })
  const byCid = new Map(st.current.map(c => [c.cid, c]))
  const members = await Promise.all(s.members.map(async (m) => {
    const held = await ex.held(m.kind, m.address)
    // Spec section 7.1a: members shown as Item Occurrences, copyable as links.
    const copy = (uri) => h('button', { type: 'button', onclick: () => navigator.clipboard?.writeText(uri).catch(() => {}) }, 'Copy')
    const where = m.kind === 'selection'
      ? link(`#/selection/${m.address}`, cid(m.address))
      : (m.via.length ? m.via : ['-']).map(v => h('div', {}, v === '-' ? [link(`#/item/-/${m.address}`, cid(m.address)), ' ', copy(`cas://${m.address}`)]
        : [link(`#/item/${v}/${m.address}`, h('code', { class: 'cid' }, `cas://${v}/${m.address}`)), ' ', copy(`cas://${v}/${m.address}`)]))
    return h('tr', { 'data-member': m.address, 'data-kind': m.kind, 'data-held': held },
      h('td', {}, where), h('td', {}, m.kind), h('td', {}, held === 'here' ? 'this member' : 'another member'))
  }))
  return h('section', { 'data-selection-view': selectionCid, 'data-deletion': st.deletion },
    h('h1', {}, st.names.length ? st.names.join(' / ') : 'Unnamed Selection'), cid(selectionCid),
    st.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    h('ul', {}, st.nameClaims.map(c => byCid.get(c)).filter(c => c.verb === 'set').map(c => h('li', {
      'data-name': c.value, 'data-claim': c.cid, 'data-conflicted': c.conflicted ? '' : null },
    String(c.value), ' ', h('span', { class: 'muted' }, `named by ${c.asserted_by ?? 'unknown'} at ${c.timestamp ?? 'unknown'}`)))),
    st.deletion === 'none' ? null : h('div', { class: 'warn' },
      h('p', {}, st.deletion === 'deleted' ? 'Deleted: hidden from the list of Selections until undone.'
        : 'Deletion in conflict: these Claims are all current, so it stays visible.'),
      h('ul', {}, st.deletionClaims.map(c => byCid.get(c)).map(c => h('li', { 'data-deletion-claim': c.cid, 'data-verb': c.verb },
        `${c.verb} by ${c.asserted_by ?? 'unknown'} at ${c.timestamp ?? 'unknown'}`)))),
    h('dl', {}, h('dt', {}, 'first seen in this member'), h('dd', {}, s.firstSeen ?? 'unknown'),
      h('dt', {}, 'assembled by'), h('dd', {}, s.block.asserted_by)),
    actions(selectionCid, st, ctx, status),
    status,
    h('h2', {}, `Members (${s.members.length})`),
    table(['member', 'kind', 'held in'], members),
    ctx.write.served ? h('p', {}, 'Samplesheet: ',
      h('a', { href: `api/samplesheet/${selectionCid}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
      h('a', { href: `api/samplesheet/${selectionCid}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null)
}

function actions(selectionCid, st, ctx, status) {
  if (!ctx.write.available) return h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason)
  const name = h('input', { id: 'rename-name', placeholder: 'New name', value: '' })
  return h('div', {},
    h('p', {}, name, ' ', h('button', { type: 'button', id: 'rename-save', onclick: () => ctx.write.run(status, async () => {
      if (!name.value.trim()) throw Object.assign(new Error('type a name first'), { code: 'invalid' })
      await ctx.write.writer.rename(selectionCid, name.value.trim(), st.nameClaims)
      return { href: `#/selection/${selectionCid}` }
    }) }, 'Rename')),
    h('p', {},
      st.deletion === 'deleted' ? null : h('button', { type: 'button', id: 'delete', onclick: () => ctx.write.run(status, async () => {
        await ctx.write.writer.remove(selectionCid, st.deletionClaims)
        return { href: `#/selection/${selectionCid}` }
      }) }, 'Delete'),
      st.deletion === 'none' ? null : [' ', h('button', { type: 'button', id: 'undo', onclick: () => ctx.write.run(status, async () => {
        await ctx.write.writer.undo(selectionCid, st.deletionClaims)
        return { href: `#/selection/${selectionCid}` }
      }) }, 'Undo delete')],
      ' ', pickButton(ctx, { address: selectionCid, kind: 'selection' })))
}

export function compose(ex, ctx) {
  const entries = ctx.tray.entries()
  const status = h('div', { id: 'write-status' })
  const name = h('input', { id: 'compose-name', placeholder: 'A name for this Selection' })
  const blocked = !ctx.write.available || entries.length === 0 || ctx.tray.onlyOneSelection()
  const save = h('button', { type: 'button', id: 'compose-save', disabled: blocked, onclick: () => ctx.write.run(status, async () => {
    const members = ctx.tray.toMembers()
    const dry = await ctx.write.writer.selection(members, { dryRun: true })
    if (dry.exists) {
      status.replaceChildren(h('p', { 'data-exists': dry.address, 'data-names': JSON.stringify(dry.names) },
        `This Selection already exists${dry.names.length ? ` as ${dry.names.join(', ')}` : ', unnamed'}. `,
        link(ctx.write.hrefFor(`#/selection/${dry.address}`), 'Open it to rename it'), ' or ',
        h('button', { type: 'button', onclick: () => status.replaceChildren() }, 'cancel'), '.'))
      return { outcome: 'exists' }
    }
    const written = await ctx.write.writer.selection(members)
    if (name.value.trim()) await ctx.write.writer.rename(written.address, name.value.trim(), [])
    ctx.tray.clear()
    ctx.trayChanged()
    return { address: written.address, href: `#/selection/${written.address}` }
  }) }, 'Save')
  return h('section', {},
    h('h1', {}, 'Compose a Selection'),
    ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
    entries.length === 0 ? h('p', { class: 'muted' }, 'The tray is empty. Add items from a run, a collection, an item or a query.')
      : table(['member', 'kind', 'picked from', ''], entries.map(e => h('tr', { 'data-tray-entry': e.address, 'data-kind': e.kind },
        h('td', {}, cid(e.address)), h('td', {}, e.kind), h('td', {}, e.via.length ? e.via.map(v => h('div', {}, cid(v))) : h('span', { class: 'muted' }, 'a query')),
        h('td', {}, h('button', { type: 'button', onclick: () => { ctx.tray.remove(e.address); ctx.trayChanged(); ctx.rerender() } }, 'Remove'))))),
    ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only member is another Selection is legal, but the explorer does not make one: add an item too.') : null,
    h('p', {}, name, ' ', save),
    status)
}
```

Existing views gain pick buttons and the hidden-runs note:

- `item(ex, collectionCid, itemCid, ctx)`: after the heading, `pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })`, and a "In Selections" list from `await ex.selectionsHolding(itemCid)` linking `#/selection/<cid>`. Show the occurrence heading only when `collectionCid !== '-'`.
- `collection(...)`: each `li` gains `' ', pickButton(ctx, { address: i, via: [collectionCid] })` (the function takes `ctx` as a new last argument; update its route).
- `items(...)` (query 3): each result `li` gains `pickButton(ctx, { address: i, via: [] })`: spec section 5.6 leaves `via` empty for items chosen by a query.
- `pipeline(...)`: when `page.hidden`, a `h('p', { 'data-hidden-runs': page.hidden, class: 'muted' }, `${page.hidden} run${page.hidden === 1 ? '' : 's'} on this page hidden by a delete Claim`)`.

- [ ] **Step 3: Wire routes, the tray and the write context in `app.js`**

Add routes (before `home` is matched, so order does not matter for these anchored patterns):

```js
  ['selections', /^#\/selections(?:\?(.*))?$/, (ex, m, ctx) => {
    const q = new URLSearchParams(m[1] ?? '')
    return views.selections(ex, { offset: Number(q.get('offset') ?? 0), deleted: q.get('deleted') === '1' }, ctx)
  }],
  ['selection', /^#\/selection\/([^/?]+)$/, (ex, m, ctx) => views.selection(ex, m[1], ctx)],
  ['compose', /^#\/compose$/, (ex, m, ctx) => views.compose(ex, ctx)],
```

Change `collection`'s route to pass `ctx`, and the `item` pattern to accept `-` (`/^#\/item\/([^/]+)\/([^/]+)$/` already does).

Module state: `let tray = null`, `let write = null`, `let store = null`. In `start()`, after `resolveStore`:

```js
    tray = new Tray()
    const token = new URL(location.href).searchParams.get('token')
    const writable = store.members?.find(m => m.writable)?.alias ?? null
    const reason = !store.members ? 'Composing, rename and delete need this page opened through `nextflow plugin nf-blocks:explore`, which holds the write endpoint.'
      : !store.write || !writable ? 'This explore server has no writable member.'
      : !token ? 'Open this page with the URL nf-blocks:explore printed; it carries the write token.'
      : null
    write = {
      available: reason === null, reason, served: !!store.members, writable,
      writer: reason === null ? writer({ endpoint: new URL('api/put', location.href).href.split('?')[0], token }) : null,
      hrefFor: (hash) => (store.member === writable ? hash : hrefIn(writable, hash)),
      run: runWrite,
    }
    document.body.dataset.write = write.available ? 'available' : 'unavailable'
    updateTray()
```

```js
/** The page URL for another member, keeping every other query parameter (the token among them). */
function hrefIn(alias, hash) {
  const url = new URL(location.href)
  url.searchParams.set('member', alias)
  url.hash = hash
  return url.href
}

function updateTray() {
  const el = document.getElementById('tray')
  el.dataset.count = String(tray.size)
  el.textContent = `Tray (${tray.size})`
}

function outcome(value) {
  document.body.dataset.writeOutcome = value
  document.body.dataset.writeSeq = String(Number(document.body.dataset.writeSeq ?? 0) + 1)
}

/**
 * One write attempt from a view: shows progress in `status`, refreshes the
 * tail so the page sees its own write (decision 14), opens `href`, then
 * records the outcome for a driver on body[data-write-*].
 */
async function runWrite(status, attempt) {
  status.replaceChildren(h('p', { class: 'muted' }, 'Saving...'))
  let done
  try {
    done = await attempt()
  } catch (e) {
    const node = views.errorNode(e)
    if (e.code === 'stale_supersedes') node.append(' Someone changed this since the page loaded; reload to see the current state.')
    status.replaceChildren(node)
    outcome(e.code ?? 'write_failed')
    return
  }
  if (done?.outcome === 'exists') {
    outcome('exists')
    return
  }
  if (done?.address) document.body.dataset.written = done.address
  if (store.member !== write.writable) {
    outcome('written')
    location.assign(hrefIn(write.writable, done.href))
    return
  }
  await explorer.refreshTail(listLog)
  history.pushState(null, '', done.href)
  await render()
  outcome('written')
}
```

`render()` builds `ctx` with `tray`, `write`, `trayChanged: updateTray` and `rerender: render` beside `progress`. Import `Tray` from `./tray.js` and `writer` from `./write.js`; `listLog` is already imported.

In `index.html`'s header add `<a href="#/selections">Selections</a>` and `<a id="tray" href="#/compose" data-count="0">Tray (0)</a>`, and a `.warn { color: var(--warn); }` rule.

- [ ] **Step 4: Build and run the smoke check and the page tests**

Run: `cd web && npm test && npm run build && node ../gate/browser/page-smoke.mjs`
Expected: PASS.

- [ ] **Step 5: Amend `DESIGN.md` and commit**

§15 "The page": add the three routes and `#/item/-/<item>`; add the rows of this task's DOM contract table to the table there, and the error codes `forbidden`, `unsupported_media_type`, `write_failed` plus every `PutError` code a write can show. Start §16 "Selections (milestone 2)" with one paragraph pointing at the plan and listing its decisions by number (Task 17 completes it).

```bash
git add web/src gate/browser/page-smoke.mjs DESIGN.md
git commit -m "feat(web): Selections, composing, rename, delete and undo in the page

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: The Gate's DAG-JSON reader, expected blocks, and the selection pipeline

What tier B needs before it can run: the Gate reads the page's DAG-JSON requests and rebuilds the blocks the plugin must have written with its own encoder (`gate/cas.py`), computes a subject's current state from the Claim blocks in the store, and has a pipeline that reads a Selection both ways (spec section 1.3 assertions 8 to 11 and 13).

**Files:**
- Create: `gate/dagjson.py`, `gate/test_dagjson.py`
- Create: `gate/selection/main.nf`, `gate/selection/nextflow.config`

**Interfaces:**
- Consumes: `cas.Cid`, `cas.encode`, `cas.cid_dagcbor`, `cas._b32_encode`, `web/test/fixtures/claim-vectors.json`, `web/test/fixtures/dag-json-vectors.json`.
- Produces:
  - `dagjson.loads(text) -> value` (links as `cas.Cid`, bytes as `bytes`).
  - `dagjson.expected_selection(request, asserted_by) -> dict` and `dagjson.expected_claim(request, asserted_by) -> dict`, the blocks under DESIGN §6 and the normalisation of spec section 7.1.
  - `dagjson.address(block) -> str`.
  - `dagjson.claim_state(claims) -> {"current", "names", "name_conflicted", "deletion"}` over `[{"cid", "verb", "attribute", "value", "supersedes"}]`.
  - `gate/selection`: `nextflow run . --selection <cid> --samplesheet <csv>` publishes `hashes/fromstore/<name>.sha256` for every file of every item `fromStore(selection:)` emits, and `hashes/samplesheet/<name>.sha256` for every `1` column cell of the samplesheet, into `cas://out` (`GATE_B_OUT`), reading the Selection from `lab` (`GATE_B_STORE`).

- [ ] **Step 1: Write the failing tests**

```python
# gate/test_dagjson.py
"""The Gate's DAG-JSON reader and expected-block builder (block explorer spec
section 1.3 assertions 8 and 10), and its current-state rules against the
vectors the plugin and the page share."""
import json
import os
import unittest

import cas
import dagjson

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "..", "web", "test", "fixtures")

I1 = cas.cid_dagcbor(cas.encode({"n": 1}))
I2 = cas.cid_dagcbor(cas.encode({"n": 2}))
C1 = cas.cid_dagcbor(cas.encode({"c": 1}))
C2 = cas.cid_dagcbor(cas.encode({"c": 2}))


def link(cid):
    return '{"/":"%s"}' % cid


class DagJsonTest(unittest.TestCase):
    def test_links_and_bytes(self):
        value = dagjson.loads('{"l":%s,"b":{"/":{"bytes":"AQID+g"}}}' % link(I1))
        self.assertEqual(value["l"], cas.Cid(I1))
        self.assertEqual(value["b"], bytes([1, 2, 3, 250]))

    def test_every_page_vector_loads(self):
        with open(os.path.join(FIXTURES, "dag-json-vectors.json")) as fh:
            for v in json.load(fh):
                dagjson.loads(v["json"])

    def test_a_slash_map_that_is_neither_form_is_refused(self):
        with self.assertRaises(cas.GateError):
            dagjson.loads('{"/":5}')

    def test_selection_normalisation_merges_orders_and_matches_a_hand_built_block(self):
        request = dagjson.loads('{"kind":"Selection","members":["cas://%s/%s",{"item":{"address":%s,"via":[%s]}},{"selection":%s}],"derived_from":[]}'
                                % (C1, I1, link(I1), link(C2), link(I2)))
        block = dagjson.expected_selection(request, "gate")
        members = sorted([(I1, {"item": {"address": cas.Cid(I1), "via": [cas.Cid(c) for c in sorted([C1, C2])]}}),
                          (I2, {"selection": cas.Cid(I2)})])
        self.assertEqual(block, {"kind": "Selection", "schema": 1, "asserted_by": "gate",
                                 "members": [m for _k, m in members], "derived_from": []})
        self.assertEqual(dagjson.address(block), cas.cid_dagcbor(cas.encode(block)))

    def test_member_order_is_not_content(self):
        a = dagjson.loads('{"kind":"Selection","members":[{"item":{"address":%s,"via":[]}},{"item":{"address":%s,"via":[]}}],"derived_from":[]}' % (link(I1), link(I2)))
        b = dagjson.loads('{"kind":"Selection","members":[{"item":{"address":%s,"via":[]}},{"item":{"address":%s,"via":[]}}],"derived_from":[]}' % (link(I2), link(I1)))
        self.assertEqual(dagjson.address(dagjson.expected_selection(a, "gate")), dagjson.address(dagjson.expected_selection(b, "gate")))

    def test_claim_supersedes_sorted_and_deduplicated(self):
        request = dagjson.loads('{"kind":"Claim","subject":%s,"verb":"set","attribute":"name","value":"x","supersedes":[%s,%s,%s],"timestamp":"2026-09-25T10:00:00.000Z"}'
                                % (link(I1), link(C2), link(C1), link(C2)))
        block = dagjson.expected_claim(request, "gate")
        self.assertEqual(block["supersedes"], [cas.Cid(c) for c in sorted([C1, C2])])
        self.assertEqual(list(sorted(block)), sorted(["kind", "schema", "asserted_by", "subject", "verb", "attribute", "value", "supersedes", "timestamp"]))

    def test_claim_state_passes_the_shared_vectors(self):
        with open(os.path.join(FIXTURES, "claim-vectors.json")) as fh:
            for v in json.load(fh):
                with self.subTest(v["name"]):
                    s = dagjson.claim_state(v["claims"])
                    self.assertEqual(s["current"], v["expect"]["current"])
                    self.assertEqual(s["names"], v["expect"]["names"])
                    self.assertEqual(s["name_conflicted"], v["expect"]["nameConflicted"])
                    self.assertEqual(s["deletion"], v["expect"]["deletion"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest gate/test_dagjson.py` (from `nf-blocks/`, with `gate` on `PYTHONPATH`, as the other Gate tests run: `cd gate && python3 -m unittest test_dagjson`)
Expected: FAIL: `No module named 'dagjson'`.

- [ ] **Step 3: Implement `gate/dagjson.py`**

```python
"""DAG-JSON as the Gate reads it, and the blocks the plugin must build from a
request (block explorer spec sections 7.1, 8 and 9.1), rebuilt with the Gate's
own encoder so no Selection or Claim address is taken on trust (spec section
1.3, assertions 8 and 10; DESIGN.md section 0 rule 6)."""
import base64
import json

import cas

OCCURRENCE = "cas://"


def loads(text):
    return _convert(json.loads(text))


def _convert(value):
    if isinstance(value, dict):
        if "/" in value:
            if len(value) != 1:
                raise cas.GateError('a map with a "/" key and others: %r' % (value,))
            inner = value["/"]
            if isinstance(inner, str):
                return cas.Cid(inner)
            if isinstance(inner, dict) and list(inner) == ["bytes"] and isinstance(inner["bytes"], str):
                text = inner["bytes"]
                return base64.b64decode(text + "=" * (-len(text) % 4))
            raise cas.GateError('a "/" map that is neither a link nor bytes: %r' % (value,))
        return {k: _convert(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert(v) for v in value]
    return value


def _text_of_binary(binary):
    return "b" + cas._b32_encode(binary)


def expected_selection(request, asserted_by):
    """Members merged by address (via unioned), sorted by address string; via and derived_from sorted likewise."""
    members = {}
    for m in request["members"]:
        if isinstance(m, str):
            parts = m[len(OCCURRENCE):].split("/")
            key, nested, via = parts[1], False, {parts[0]}
        elif "selection" in m:
            key, nested, via = m["selection"].text, True, set()
        else:
            key, nested, via = m["item"]["address"].text, False, {v.text for v in m["item"].get("via", [])}
        seen = members.setdefault(key, [nested, set()])
        if seen[0] != nested:
            raise cas.GateError("%s is both an item and a Selection" % key)
        seen[1].update(via)
    out = []
    for key in sorted(members):
        nested, via = members[key]
        out.append({"selection": cas.Cid(key)} if nested
                   else {"item": {"address": cas.Cid(key), "via": [cas.Cid(v) for v in sorted(via)]}})
    derived = set()
    for d in request.get("derived_from") or []:
        derived.add(d.text if isinstance(d, cas.Cid) else _text_of_binary(d))
    return {"kind": "Selection", "schema": 1, "asserted_by": asserted_by, "members": out,
            "derived_from": [cas.Cid(t).binary() for t in sorted(derived)]}


def expected_claim(request, asserted_by):
    supersedes = sorted({s.text for s in request.get("supersedes") or []})
    return {"kind": "Claim", "schema": 1, "asserted_by": asserted_by, "subject": request["subject"],
            "verb": request["verb"], "attribute": request.get("attribute"), "value": request.get("value"),
            "supersedes": [cas.Cid(s) for s in supersedes], "timestamp": request["timestamp"]}


def address(block):
    return cas.cid_dagcbor(cas.encode(block))


def claim_state(claims):
    """Decision 5 of the milestone 2 plan: the same rules as ClaimState.groovy and claims.js."""
    superseded = {s for c in claims for s in (c.get("supersedes") or [])}
    current = sorted((c for c in claims if c["cid"] not in superseded), key=lambda c: c["cid"])
    names = [c for c in current if c.get("attribute") == "name"]
    deletion = [c for c in current if c.get("attribute") is None and c["verb"] in ("delete", "del")]
    state = "conflicted" if len(deletion) > 1 else "deleted" if len(deletion) == 1 and deletion[0]["verb"] == "delete" else "none"
    return {"current": [c["cid"] for c in current],
            "names": [str(c["value"]) for c in names if c["verb"] == "set"],
            "name_conflicted": len(names) > 1,
            "deletion": state}
```

`cas.Cid(t).binary()` returns the binary CID (without the tag-42 `0x00`), as `Cid.bytes()` does in Groovy. Check `cas.encode` turns `bytes` into a CBOR byte string (it does: the `(bytes, bytearray)` branch).

- [ ] **Step 4: Write the selection pipeline**

```groovy
// gate/selection/main.nf
// Tier B's pipeline (block explorer spec section 1.3 assertions 9 and 13):
// read one Selection two ways and hash whatever Nextflow staged.
//
//   fromstore    channel.fromStore(selection: <cid>): every file of every item
//   samplesheet  the explorer's CSV export, column '1', staged with file()
//
// It computes nothing: gate/browser_b_assert.py compares the digests with its
// own hashes of pipeline-a's work files.

include { fromStore } from 'plugin/nf-blocks'

process HASH {
    tag "${source}:${staged.name}"

    input:
    tuple val(source), path(staged)

    output:
    tuple val(source), path("${staged.name}.sha256"), emit: sha

    script:
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum '${staged}' > '${staged.name}.sha256'
    else
        shasum -a 256 '${staged}' > '${staged.name}.sha256'
    fi
    """
}

/** Every file in a restored item, wherever it sits in the structure. */
def filesOf(value) {
    if( value instanceof java.nio.file.Path )
        return [value]
    if( value instanceof Map )
        return value.values().collectMany { v -> filesOf(v) }
    if( value instanceof List )
        return value.collectMany { v -> filesOf(v) }
    return []
}

workflow {
    main:
    if( !params.selection || !params.samplesheet )
        error "selection pipeline needs --selection and --samplesheet (tier_b.sh passes both)"

    ch_store = channel
        .fromStore(selection: params.selection)
        .flatMap { item -> filesOf(item).collect { f -> tuple('fromstore', f) } }

    ch_sheet = channel
        .fromPath(params.samplesheet)
        .splitCsv(header: true)
        .map { row -> tuple('samplesheet', file(row['1'])) }

    HASH(ch_store.mix(ch_sheet))

    publish:
    hashes = HASH.out.sha
}

output {
    hashes {
        path { source, _f -> "hashes/${source}" }
        mode 'copy'
    }
}
```

```groovy
// gate/selection/nextflow.config
// Two members, as the consumer pipeline has: its own writable `out`, and the
// tier B copy of the producer's store, `lab`, which holds the Selection.
plugins {
    id 'nf-blocks@0.1.0'
}

manifest.name = 'cas-gate-selection'

lineage.enabled = true
lineage.store.location = 'cas://out'
outputDir = 'cas://out'

cas {
    stores {
        out { location = System.getenv('GATE_B_OUT') }
        lab { location = System.getenv('GATE_B_STORE') }
    }
    resolve = ['out', 'lab']
    asserted_by = 'gate'
}

params {
    selection = null
    samplesheet = null
}
```

If Nextflow 26.04.6's strict syntax refuses the top-level `def filesOf`, read the parser's message, and move the recursion into a closure inside `workflow` (`def filesOf; filesOf = { v -> ... }`); record which form worked in the commit message.

- [ ] **Step 5: Run the tests**

Run: `cd gate && python3 -m unittest test_dagjson && nextflow lint selection/main.nf 2>&1 | tail -5`
Expected: PASS, and `nextflow lint` reports no errors (if `lint` is not a 26.04.6 command, skip it: Task 16 runs the pipeline for real).

- [ ] **Step 6: Commit**

```bash
git add gate/dagjson.py gate/test_dagjson.py gate/selection
git commit -m "test(gate): DAG-JSON reader, expected Selection and Claim blocks, selection pipeline

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: Gate browser tier B

Spec section 1.3 assertions 8 to 13, all local: the page, served by `nf-blocks:explore` over a copy of the Gate's store, composes two Selections (one nesting the other), renames, deletes and undoes, and races two sessions; the Gate replays a write, probes the endpoint's refusals, fetches the samplesheet, runs the selection pipeline, and checks everything against its own encoder and hashes.

**Files:**
- Modify: `gate/browser/drive.mjs` (actions, two-page steps, variables, POST bodies)
- Create: `gate/browser_b_assert.py` (`prepare`, `probe`, `check`), `gate/test_browser_b_assert.py`
- Create: `gate/browser/tier_b.sh`
- Modify: `gate/gate.sh` (run tier B after tier A), `gate/README.md`

**Interfaces:**
- Consumes: Task 14's DOM contract, Task 15's `dagjson`, `assert.Gate`, `assert.Run.collections/items`, `assert.metadata_view`, `assert._consumer_hashes`, `cas.Store`, `gate/browser/plugin-repo.sh`.
- Produces: `gate/browser/tier_b.sh <GATE_ROOT>` printing `B8` to `B13` and `browser tier B: <n> PASS, <m> FAIL`, exit non-zero on a failure; `gate.sh` fails when tier B does.

- [ ] **Step 1: Extend `drive.mjs`**

Keep every existing behaviour (tier A's scenario has no `actions`). Add:

```js
const vars = {}
const fill = (text) => (text ?? '').replace(/\{(\w+)\}/g, (_, name) => servers[name] ?? vars[name] ?? `{${name}}`)

const EXTRACT_B = () => ({
  write: document.body.dataset.write ?? null,
  writeOutcome: document.body.dataset.writeOutcome ?? null,
  written: document.body.dataset.written ?? null,
  tray: document.getElementById('tray')?.dataset.count ?? null,
  selections: [...document.querySelectorAll('[data-selection]')].map(e => ({ cid: e.dataset.selection, deletion: e.dataset.deletion,
    source: e.dataset.source, names: JSON.parse(e.dataset.names ?? '[]') })),
  view: document.querySelector('[data-selection-view]')?.dataset.selectionView ?? null,
  deletion: document.querySelector('[data-selection-view]')?.dataset.deletion ?? null,
  names: [...document.querySelectorAll('[data-name]')].map(e => ({ name: e.dataset.name, claim: e.dataset.claim, conflicted: 'conflicted' in e.dataset })),
  members: [...document.querySelectorAll('[data-member]')].map(e => ({ address: e.dataset.member, kind: e.dataset.kind, held: e.dataset.held })),
  errors: [...document.querySelectorAll('[data-error]')].map(e => ({ error: e.dataset.error, cid: e.dataset.cid ?? null })),
})

async function act(pages, action, record) {
  const page = pages[action.page ?? 0]
  if (action.hash !== undefined) {
    const before = Number(await page.evaluate(() => document.body.dataset.render))
    await page.evaluate((h) => { location.hash = h }, fill(action.hash))
    await waitRender(page, before)
  } else if (action.click) {
    await page.click(fill(action.click))
  } else if (action.fill) {
    await page.fill(fill(action.fill[0]), fill(action.fill[1]))
  } else if (action.waitWrite) {
    await page.waitForFunction((n) => Number(document.body.dataset.writeSeq ?? 0) > n, action.after ?? 0, { timeout: 60_000 })
  } else if (action.extract) {
    record.extracts[action.extract] = await page.evaluate(EXTRACT_B)
  }
}
```

The step's opening URL now passes `step.hash` through `fill` too (tier B's hashes carry `{S1}` and `{S2}`). A `waitWrite` must know the counter before the click: make `click` record `record.seq[page] = <writeSeq before>` and `waitWrite` wait past that (`action.after` is then filled from it), so a write that ends before `waitWrite` runs is still seen. Per step: create `step.pages ?? 1` pages in the context, navigate each to the step's URL and `waitRender` (every page), run `step.actions` in order, then for each `[name, selector, attribute]` in `step.save ?? []` set `vars[name]` from page 0 (`document.querySelector(selector)?.getAttribute(attribute)`), and record the extracts. For `POST` requests, record `request.postData()` as `body` and, in the `requestfinished` handler, `await response.text()` as `responseBody` when the URL path is `/api/put`.

- [ ] **Step 2: Write `browser_b_assert.py` and its unit tests**

`prepare(root)`:

```python
def prepare(root):
    gate = A.Gate(root)
    out = os.path.join(root, "browser-b")
    shutil.rmtree(out, ignore_errors=True)
    store = os.path.join(out, "store")
    shutil.copytree(gate.store.root, store, ignore=shutil.ignore_patterns("index", "index.html"))
    os.makedirs(os.path.join(out, "cache"))
    os.makedirs(os.path.join(out, "store-out"))
    with open(os.path.join(out, "explore.config"), "w") as fh:
        fh.write("lineage.enabled = true\nlineage.store.location = 'cas://lab'\n"
                 "cas {\n  stores { lab { location = '%s' } }\n  asserted_by = 'gate'\n"
                 "  index { path = '%s' }\n}\n" % (store, os.path.join(out, "cache", "index.sqlite")))
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
    expected = {"items": {"A": a, "B": b, "C": c}, "collections": {"aligned": coll["aligned"], "stats": coll["stats"], "again": again_aligned},
                "files": {"A": _file(gate, "A.bam"), "B": _file(gate, "B.bam"), "C": _file(gate, "C.stats")}}
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
    ]
    _write_json(os.path.join(out, "expected.json"), expected)
    _write_json(os.path.join(out, "scenario.json"), {"steps": steps})
    return 0
```

`_file(gate, name)` returns `{"name": name, "sha256": <one digest over every copy in pipeline-a/work>, "cid": cas.cid_of_file(path)}`, failing if copies differ (as `A._bam_sha256` does).

`probe(root, port, token)` (runs while `explore` is up):

```python
def probe(root, port, token):
    out = os.path.join(root, "browser-b")
    observed = _observed(out)
    base = "http://127.0.0.1:%d" % port
    origin = base
    rename = _posts(observed["B.rename"])[0]            # the page's own bytes, replayed exactly
    selection_body = _posts(observed["B.first"])[-1]["body"]
    before = _block_count(out)
    results = {"replay": _post(base + "/api/put", rename["body"], {"X-NF-Blocks-Token": token, "Content-Type": "application/vnd.ipld.dag-json", "Origin": origin}),
               "refusals": {
                   "no_token": _post(base + "/api/put", selection_body, {"Content-Type": "application/json", "Origin": origin}),
                   "foreign_origin": _post(base + "/api/put", selection_body, {"X-NF-Blocks-Token": token, "Content-Type": "application/json", "Origin": "http://evil.example"}),
                   "text_plain": _post(base + "/api/put", selection_body, {"X-NF-Blocks-Token": token, "Content-Type": "text/plain", "Origin": origin})},
               "blocks_before": before}
    results["blocks_after"] = _block_count(out)
    s2 = _saved(observed, "B.second")
    for fmt in ("csv", "json"):
        status, body = _get("%s/api/samplesheet/%s.%s" % (base, s2, fmt))
        results["samplesheet_" + fmt] = status
        with open(os.path.join(out, "samplesheet." + fmt), "wb") as fh:
            fh.write(body)
    _write_json(os.path.join(out, "probes.json"), results)
    return 0
```

`_posts(step)` is the step's non-dry-run `POST /api/put` requests (URL without `dry_run=true`) in order, each `{"body", "responseBody", "status"}`; `_post` and `_get` use `urllib.request` and return `{"status", "body"}` without raising on 4xx; `_block_count` counts files under `store/blocks/`; `_saved(observed, id)` is the `written` address that step's `after` extract recorded.

`check(root)` prints one line per assertion, as `browser_assert.check` does, from `expected.json`, `observed.json`, `probes.json`, the tier B store and the pipeline's `store-out`:

- **B8** (`a Selection made in the page has the Gate's own address`): for each Selection `POST` of `B.first` and `B.second`, `dagjson.address(dagjson.expected_selection(dagjson.loads(body), "gate"))` equals the response's `address` and the step's `written`; the block file in the tier B store reads back (`cas.Store.read` verifies its hash) and decodes equal to the expected block; `B.second`'s block has exactly the members `{selection: S1}`, `B` (via `again`'s aligned collection) and `C` (via `stats`).
- **B9** (`fromStore(selection:) receives each distinct item once, nested included`): the pipeline exited 0 and `A._consumer_hashes(SimpleNamespace(store_out=cas.Store(<browser-b/store-out>)))["fromstore"]` is exactly `{"A.bam.sha256": files.A.sha256, "B.bam.sha256": files.B.sha256, "C.stats.sha256": files.C.sha256}` (keys named as the published hash files are).
- **B10** (`rename, delete and undo are Claims at the Gate's addresses; a replay writes nothing`): each Claim `POST` of `B.rename` and `B.delete` has `dagjson.address(dagjson.expected_claim(...))` equal to its response address, and its block is in the store; the replay answered 200 with `written: false` and the rename's address; the tier B store's `log/` holds exactly one entry ending in that address.
- **B11** (`two sessions renaming from one view: a surfaced conflict, not an overwrite`): page 0's outcome is `written`, page 1's is `stale_supersedes` with a `[data-error="stale_supersedes"]`; the Gate's `dagjson.claim_state` over every Claim block in the tier B store whose subject is S2 has names `["third-a"]` and no name conflict.
- **B12** (`a POST without the token, from another Origin, or as text/plain is refused`): the three refusals are `403`, `403`, `415`, and `blocks_after == blocks_before`.
- **B13** (`the samplesheet lists exactly the Selection's items, and its cells stage`): both exports answered 200; the CSV (read with `csv.DictReader`) has three rows whose `1` cells are exactly `cas://<files.X.cid>/<files.X.name>` for A, B and C; the JSON rows carry the same cells under `"1"`; and the pipeline's `samplesheet` hashes equal B9's.

`gate/test_browser_b_assert.py` unit-tests `check` against small hand-made `observed.json`/`probes.json`/store fixtures in a temp dir (one passing and one failing case per assertion, as `test_browser_assert.py` does for tier A). Write them before `check`, run them failing, then implement.

- [ ] **Step 3: Write `tier_b.sh`**

```bash
#!/usr/bin/env bash
#
# Gate browser tier B (block explorer spec section 1.3, assertions 8-13): the
# page composes, renames, deletes and undoes through nf-blocks:explore over a
# copy of this Gate run's store; the Gate probes the write endpoint, fetches
# the samplesheet, runs gate/selection, and checks it all with its own encoder.
#
#   gate/browser/tier_b.sh <GATE_ROOT>        # NEXTFLOW and NXF_PLUGINS_DIR from gate.sh
#
# One script, since the Bash sandbox forbids a later command connecting to a
# server an earlier command started. Needs tier A's setup (npm ci, Playwright).

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
NEXTFLOW="${NEXTFLOW:-nextflow}"
B="$GATE_ROOT/browser-b"

python3 "$REPO/gate/browser_b_assert.py" prepare "$GATE_ROOT"
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$B")"
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')

S=''
stop() { if [[ -n "$S" ]]; then kill -TERM "$S" 2> /dev/null || true; wait "$S" 2> /dev/null || true; S=''; fi; }
trap stop EXIT

( cd "$B" && unset NXF_OFFLINE && XDG_CACHE_HOME="$B/cache" \
  NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
  exec "$NEXTFLOW" -c "$B/explore.config" plugin nf-blocks:explore --port "$port" ) > "$B/explore.log" 2>&1 & S=$!

for _ in $(seq 240); do
    grep -q 'nf-blocks explorer:' "$B/explore.log" 2> /dev/null && break
    kill -0 "$S" 2> /dev/null || { echo "browser tier B: explore exited, see $B/explore.log" >&2; exit 2; }
    sleep 0.5
done
url="$(grep -o 'http://127.0.0.1:[0-9]*/?token=[a-z2-7]*' "$B/explore.log" | head -1)"
[[ -n "$url" ]] || { echo "browser tier B: explore printed no URL with a token, see $B/explore.log" >&2; exit 2; }
token="${url##*token=}"

echo "--- browser tier B: driving"
node "$REPO/gate/browser/drive.mjs" "$B/scenario.json" "$B/observed.json" \
     "explore=http://127.0.0.1:$port" "token=$token" > "$B/drive.log" 2>&1 \
     || echo "browser tier B: the driver failed, see $B/drive.log" >&2
python3 "$REPO/gate/browser_b_assert.py" probe "$GATE_ROOT" "$port" "$token" > "$B/probe.log" 2>&1 \
     || echo "browser tier B: the probe failed, see $B/probe.log" >&2
stop

s2="$(python3 -c 'import json,sys; o={s["id"]: s for s in json.load(open(sys.argv[1]))["steps"]}; print(o["B.second"]["extracts"]["after"]["written"] or "")' "$B/observed.json" 2> /dev/null || true)"
echo "--- browser tier B: selection pipeline (selection ${s2:-<none>})"
rm -rf "$GATE_ROOT/selection" && mkdir -p "$GATE_ROOT/selection"
cp "$REPO/gate/selection/main.nf" "$REPO/gate/selection/nextflow.config" "$GATE_ROOT/selection/"
status=0
( cd "$GATE_ROOT/selection" && GATE_B_STORE="$B/store" GATE_B_OUT="$B/store-out" XDG_CACHE_HOME="$B/cache-run" \
  "$NEXTFLOW" run . -name selection --selection "$s2" --samplesheet "$B/samplesheet.csv" ) \
  > "$B/selection.log" 2>&1 || status=$?
echo "$status" > "$B/selection.exit"

echo
python3 "$REPO/gate/browser_b_assert.py" check "$GATE_ROOT"
```

`chmod +x gate/browser/tier_b.sh`. In `gate.sh`, after tier A: `browser_b=0; [[ -n "${GATE_SKIP_BROWSER:-}" ]] || NEXTFLOW="$NEXTFLOW" "$REPO/gate/browser/tier_b.sh" "$GATE_ROOT" || browser_b=$?`, clear `"${GATE_ROOT:?}/browser-b"` and `"${GATE_ROOT:?}/selection"` with the other directories at the start, and exit non-zero if any tier failed. `gate/README.md` gains a tier B section: what it does, that it is local, where its logs are (`browser-b/*.log`).

- [ ] **Step 4: Run tier B and the whole Gate**

Run: `cd gate && python3 -m unittest test_browser_b_assert test_dagjson`
Expected: PASS.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage 11/0/6, browser tier A 5/5, browser tier B 6/6. When a B assertion fails, read `browser-b/drive.log`, `observed.json` and the relevant log before changing code; a failure in the page or the plugin is fixed there, never by loosening a check.

- [ ] **Step 5: Commit**

```bash
git add gate
git commit -m "test(gate): browser tier B, assertions 8-13, all local

The page composes two Selections (one nested), renames, deletes and
undoes through nf-blocks:explore over a copy of the Gate's store, and two
sessions race a rename; the Gate recomputes every address with its own
encoder, replays a write, probes the endpoint's refusals, and stages the
Selection through fromStore and through the samplesheet.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 17: `DESIGN.md` §16, the remaining minor, and acceptance

**Files:**
- Modify: `DESIGN.md` (§16 complete; §15 status line)
- Modify: `gate/cloud/s3tier.py` (upload the snapshot with `CacheControl='no-cache'`), `DESIGN.md` §15 "What a member serves"
- Modify: `../.scratch/block-explorer/spec.md` section 1.2 (status line per item, as section 1.1 has)
- Modify: `README.md` (the `put` verb, Selections, the samplesheet)

- [ ] **Step 1: Finish `DESIGN.md` §16**

§16 "Selections (milestone 2)": a status line ("*Status <date>: milestone 2 accepted; Gate browser tier B 6 of 6.*"), the list of this plan's decisions 1 to 20 (one or two lines each, as §15's decision list is), the block kinds a client builds and the request and response shapes (spec section 9, decision 7's `invalid`), the samplesheet columns (decision 18), and the `put` verb. Keep facts only: no narrative of how tasks went.

- [ ] **Step 2: `Cache-Control` for directly browsed snapshots (milestone 1's parked minor)**

A snapshot uploaded to a bucket without `Cache-Control` may be served from a browser's heuristic cache after it is rewritten; the page then fails with `snapshot_changed` rather than answering wrongly, but a user sees an error they did not need. In §15 "What a member serves", add: "Upload `index/v<N>.sqlite` with `Cache-Control: no-cache` (a revalidation per load; blocks may be `immutable`)." In `gate/cloud/s3tier.py`, pass `CacheControl='no-cache'` (boto3 `ExtraArgs` or `put_object` argument, whichever the uploader uses) for the snapshot, and `'public, max-age=31536000, immutable'` for blocks, matching what `explore` sends. Run `python3 -m unittest gate/cloud/test_s3tier.py` (from `gate/`); expected PASS, updated if it pins upload arguments.

- [ ] **Step 3: Ask Rob about the fourth parked minor**

Milestone 1 parked "stale worker error code" for milestone 2 without saying which code or when it is stale. Ask Rob what he saw (a message, a step), fix it if it is small, and otherwise record it in §16 as open. Do not guess at a fix.

- [ ] **Step 4: Acceptance run**

Run: `./gradlew test && (cd web && npm test) && (cd gate && python3 -m unittest discover -p 'test_*.py')`
Expected: all PASS; record the Groovy test count.

Run: `GATE_ROOT=<scratchpad>/gate make gate` twice.
Expected both times: lineage 11/0/6 (assertion 4 may fail once, by design; rerun), tier A 5/5, tier B 6/6.

The cloud part of tier A (`make gate-cloud`, A6 and A7) needs Rob's `scidev` SSO session and runs when the HTTP reader or the serving code changes (spec section 1.3). This milestone changes `explore` (the write endpoint and the samplesheet route), so ask Rob to run it, or run it with his session if he says so, before calling the milestone accepted.

- [ ] **Step 5: Mark the spec and commit**

In `../.scratch/block-explorer/spec.md` section 1.2, add a "*Done <date>: ... at `<commit>`*" note to each of items 1 to 6, as section 1.1 item 1 has. `.scratch/` is not in the `nf-blocks` repo; edit it in place (read the file first, write the whole file back through a temporary file, per the append-safely rule).

```bash
git add DESIGN.md README.md gate/cloud
git commit -m "docs: block explorer milestone 2 accepted

Gate: lineage 11/0/6, browser tier A 5/5 (cloud A6-A7 <run or pending>),
tier B 6/6. <N> Groovy tests.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then use superpowers:finishing-a-development-branch to merge `feat/explorer-m2` into `main` with Rob's approval.
