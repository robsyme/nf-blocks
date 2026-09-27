# Block Explorer Milestone 3 (Picking Items for a Downstream Workflow) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every task is bound by `DESIGN.md`; when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets fixed.

**Goal:** A curator can pick people (items) from two runs and feed them to a downstream workflow without reading CIDs, hand-writing SQL or DAG-JSON, or guessing the `fromStore` spelling: item rows show their Meta Map, picks keep their run, a CLI verb lists and selects items, `put --name` names a Selection, `fromStore` can hand typed processes records, a consuming run needs no `outputDir`, and a missing `include` is explained.

**Architecture:** Wave 0 changes what a lineage run does at its edges: `CasObserverFactory` defaults an unset `outputDir` to the lineage alias, `CasObserver` warns with the fix when `fromStore` is missing and stops throwing from `onFlowComplete` on a run that is already failing, and `fromStore` gains `records: true`. Wave 1 is the command line: one run-reference resolver shared by `fromStore` and a new read-only verb `nf-blocks:items`, whose output is the samplesheet (`Samplesheet.of`) plus an `occurrence` column, and `put --name`. Wave 2 is the page: one item row (a labelled list with Meta Map pills, lazily filled from OutputItem blocks) used by query results, collection pages, the tray and Selection members; per-run query picks keep their collection as `via`; "Add all N" and checkboxes; consumer-code snippets on the Selection and run pages. Wave 3 is the Gate and the documents.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, `org.xerial:sqlite-jdbc` 3.50.3.0, Nextflow 26.04.6, Node 24, esbuild 0.28.2, `@ipld/dag-cbor` 10.0.2, `@ipld/dag-json` 11.0.1, `@ipld/schema` 7.0.12, Playwright 1.63.0, Python 3 stdlib for the Gate. No new dependency.

**Spec:** the decisions of the wayfinder map `../.scratch/block-explorer/ux/map.md`, one ticket per decision under `../.scratch/block-explorer/ux/issues/` (each ticket's `## Answer` is binding), drawn from the UX review `../.scratch/block-explorer/ux-review-2026-09-26.md` (R2 to R6; R1's upstream half is out of scope). Background contract: `../.scratch/block-explorer/spec.md` and `DESIGN.md` §2, §6, §13, §15, §16. Read the map, then each task's tickets, before starting.

**Branch:** `feat/explorer-m3`, branched from `fix/runmanifest-config-string` (`c4a68a8`, RunManifest `config` as text, Gate green), which merges to `main` with it. Tasks 3 and 11 rely on that fix (a Gate consumer config with a closure must not abort the run). Do not merge `prototype/item-previews`; it is a throwaway record of the prototypes behind tickets 03 and 10, useful only as a picture of the chosen row and pill styles (`web/src/prototype-previews.js`, `web/src/prototype-pairs.js` there).

## Decisions this plan carries (each from its ticket)

The ticket named in brackets holds the reasoning; Task 12 writes each into `DESIGN.md` §16 or the section it amends.

1. **Unset `outputDir` means the lineage alias** [02]. `CasObserverFactory.create(session)` sets `session.outputDir = FileHelper.toCanonicalPath('cas://<writableAlias>')` when lineage is enabled, `lineage.store.location` starts with `cas://`, and the config has no `outputDir`; it logs at info `outputDir not set; publishing to cas://<alias>`. It runs before `Session.groovy:472` copies `session.outputDir` into `WorkflowMetadata`, so `workflow.outputDir`, the lineage WorkflowRun record and Platform payloads agree. `session.config` is not modified. An explicit `outputDir` naming anything other than the alias still aborts in `onFlowCreate`. Research: `../.scratch/research/outputdir-read-timing.md`.
2. **A missing `fromStore` is explained** [06, 07]. `CasObserver.onFlowError` inspects `session.error`: a `MissingMethodException` (possibly the cause of a `MissingProcessException`) whose method is `fromStore` or `Channel.fromStore`. Untyped (method `Channel.fromStore`) and typed (method `fromStore`, type `nextflow.dataflow.ChannelNamespace` or `nextflow.script.types.Channel`) get different warnings; exact text in Task 2. Research: `../.scratch/research/missing-fromstore-exception-chain.md`.
3. **`onFlowComplete` does not throw on a failing run** [07 Q2]. When `session.error` is already set, `CasObserver.onFlowComplete` catches its own failures and logs them at warn, so `notifyError` (`Session.groovy:1125-1128`) still reaches every observer. A clean run keeps today's abort.
4. **`fromStore(..., records: true)`** [09]. Default `false` in every script mode, both entry points; a non-boolean is refused. Every non-Leaf map at any depth (inside lists too) is restored as `nextflow.util.RecordMap`; Leaves become paths as today.
5. **No-argument `fromStore` error**: "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output: <name>`; optionally `where: [...]` and `records: true`" [07 Q3].
6. **One run-reference resolver** [05 Q2], `robsyme.cas.core.RunRef`, used by `fromStore(run:)` and `nf-blocks:items --run`.
7. **`nf-blocks:items`**, a fourth verb, read-only [05]: `items <output> [<path>=<value> ...] --run <ref>[,<ref>...] [--pipeline <id>] [--format csv|json|occurrences|selection]`. Conditions split at the first `=` and match the index's value text regardless of type. `csv` (default) and `json` are `Samplesheet.of(store, items)` with a leading `occurrence` column; `occurrences` one `cas://<collection>/<item>` per line; `selection` a complete `put` request, members the union across runs. It reads the plugin's own index, caught up first.
8. **`put --name <name>`** [05 Q5], Selection requests only: a second request through the same `Put` builder writes a `set name` Claim superseding the dry run's `name_claims`, nothing when the one current name already equals `<name>`; both responses printed; "saved, naming failed" and exit 1 when the Claim fails; with `--dry-run`, the dry run and the Claim it would send. `items` never writes; the CLI route pipes `items ... --format selection` into `put /dev/stdin --name <name>`.
9. **Item rows** [03, 10]. Query results, collection pages, compose-tray entries and Selection members use one row: a bold label, file-name chips, the pick control at the right, the Meta Map as `key | value` pills beneath, the item CID (shortened, full in `title`) last. Label: up to three paths, skipping list-valued paths, string paths before numbers and booleans, then most distinct values among loaded previews, ties by path. Pills: the key is the last path segment (full path in `title`), a list's values in one pill; value colour by type (numbers blue, `true` green, `false` red, null grey italic, strings plain); a string that parses as a number is shown quoted. Clicking a value runs query 3 over the item's run and output with that pair added to the current conditions.
10. **Preview fetch budget** [03 Q3]: OutputItem blocks through `blocks.ofKind` (hash-checked, cached), at most 6 in flight, the first 100 rows of a view without a click, "show details" beyond. No DESIGN §12 change, no snapshot query.
11. **Runs are named, not shown as CIDs** [03 Q4]: "picked from", a Selection member's via and the item page's "produced by" read `<run_name> / <output>`, CID in `title`.
12. **Per-run query picks keep their collection** [04]. `via` is the Output Collection for every per-run query pick. "Add all N to the tray" (`[data-pick-all]`) adds every item of the query or the whole collection, not the page. Row checkboxes with "Add checked (k)". No client-side size check; the dry run's `too_large` covers it.
13. **Snippets** [07 Q4]: the Selection page shows the `include` line and `fromStore(selection: '<cid>')`; the run page shows the run's `lid://` under its name and, per output, `fromStore(run: 'lid://<hash>', output: '<name>')`. One "untyped | typed" toggle for every snippet, kept in `localStorage` key `nf-blocks.snippets` (wrapped in `try`); typed reads `nextflow.Channel.fromStore(..., records: true)`. Each snippet's call line carries `[data-snippet="untyped"|"typed"]`.
14. **The Gate runs the snippets** [07 Q6]: tier B9's consumer and the new typed consumer take their call line verbatim from the page's `[data-snippet]` text.

## Global Constraints

- Released dependencies only; no new npm or Maven dependency in this milestone.
- Nextflow facts are read from tag `v26.04.6` in `/Users/robsyme/dev/github.com/nextflow-io/nextflow` (`git show v26.04.6:<path>`); the plugin builds against published artifacts only.
- One encoder: blocks are built in Groovy by `Put`. `items` and the page never encode DAG-CBOR; `put` stays the only CLI write path.
- The page keeps every fetched block hash-checked through `BlockFetcher`/`blocks.ofKind`; previews read blocks only, never `index/v3.sqlite` beyond the queries already in `queries.json` plus those this plan adds, each covered by `ExplorerQueriesTest`'s no-scan guard.
- Gate tier A assertion A2's limits stay: point queries at most `8` requests and `65536` bytes, query 3 at most `50` and `524288` (previews are block fetches and do not count against A2).
- `records` accepts only `true` or `false`; `RecordMap` exists from Nextflow 26.01.1-edge; the plugin requires 26.04.6 (`build.gradle` `nextflowVersion`).
- The Nextflow launcher refuses a bare `-`; stdin is `/dev/stdin`. `CmdPlugin` keeps one value per `--flag`: repeated values are comma-joined (`--run a,b`) and conditions are positionals.
- Failures in derived structures (index, Store Log, snapshot, page) log at warn and never abort a run (DESIGN §0 rule 3).
- Gate assertions never trust the plugin or the page (DESIGN §0 rule 6).
- Plain prose in every user-facing string and document: no bold-first bullets, no em-dash chains.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **An item with no Meta Map, or only list values** (a bare file published alone; the Gate's `ids`). The row's label falls back to the file names, pills are absent rather than empty boxes, and `items --format csv` still writes one row with an `occurrence` and file columns. Pinned in Task 7 (`pairs.test.mjs`) and Task 5 (`ItemsCommandTest`).
2. **A condition that is not `path=value`**: `a=b=c` (split at the first `=`, value `b=c`), `=x` and `sample` (usage errors naming the argument, exit 2), a value with spaces or non-ASCII. Pinned in Task 5.
3. **`outputDir` set by `-output-dir` on the command line, or to `cas://<alias>/sub`**: both are explicit, so the factory leaves them alone and `onFlowCreate` judges them as today. Pinned in Task 1 (`CasObserverFactoryTest`).
4. **"Add all" on a collection far larger than a page** (15,000 items): it adds every item without fetching one preview, and the tray survives a reload. Pinned in Task 9 (`views.test.mjs`).
5. **A collection holding both the string `"2"` and the integer `2` under one path**: the pills differ (quoted and blue), and each pill's filter link carries its own type, so the query finds only its own. Pinned in Task 7.

## File Structure

```
nf-blocks/
  DESIGN.md                                   §0 rule 5 amendment, §2, §13, §15 (verbs, DOM contract), §16 (Task 12)
  README.md                                   consumer example, Selections via CLI (Task 12)
  src/main/groovy/robsyme/cas/
    trace/CasObserverFactory.groovy           default outputDir (Task 1)
    trace/CasObserver.groovy                  validateOutputDir reads session.outputDir (Task 1); onFlowError hint,
                                              onFlowComplete on a failing run (Task 2)
    trace/FromStoreHint.groovy                the hint text from an error, pure (Task 2)
    ext/CasExtension.groovy                   no-arg error (Task 2), records: true (Task 3), RunRef (Task 4)
    core/RunRef.groovy                        the one run-reference resolver (Task 4)
    core/Index.groovy                         itemHitsByText (Task 4)
    core/ItemHit.groovy                       (collection, item) pair (Task 4)
    core/Samplesheet.groovy                   of(store, items, occurrences) (Task 5)
    cli/ItemsCommand.groovy                   nf-blocks:items (Task 5)
    cli/CasCommands.groovy                    VERBS, usage, dispatch (Task 5); put --name (Task 6)
  src/test/groovy/robsyme/cas/...             CasObserverFactoryTest, FromStoreHintTest, CasObserverTest,
                                              CasExtensionTest, RunRefTest, IndexItemHitsTest,
                                              ItemsCommandTest, CasCommandsPutTest
  web/src/
    pairs.js                                  pairsOf, labelPaths, pairsNode, filterHref (Task 7)
    previews.js                               the lazy preview fetcher (Task 7)
    config.js                                 preview cap and concurrency constants (Task 7)
    tray.js                                   addMany, one storage write (Task 9)
    queries.json                              runByCompletion + nf_run_hash, collectionAllItems (Task 8)
    model.js                                  runLabel, allItems, nf_run_hash in tail rows, typed item view (Task 8)
    views.js                                  itemRows and every list that uses it, item page (Task 9)
    snippets.js                               consumer-code snippets and their toggle (Task 10)
    views.js                                  Selection and run page snippets (Task 10)
  web/test/                                   pairs, previews, model, views, snippets tests
  src/test/groovy/robsyme/cas/explore/ExplorerQueriesTest.groovy  new queries (Task 8)
  gate/
    consumer/nextflow.config                  no outputDir (Task 1)
    assert.py, test_assert.py, README.md      assertion 6: consumer WorkflowRun outputDir (Task 1)
    selection/main.nf                         call line from the page's snippet (Task 11)
    selection-typed/main.nf, nextflow.config  typed consumer, records: true (Task 11)
    browser/drive.mjs, tier_b.sh, browser_b_assert.py, test_browser_b_assert.py  B17, B18 (Task 11)
```

## Interfaces shared across tasks

Groovy (package `robsyme.cas`):

- `trace.CasObserverFactory.defaultOutputDir(Session session)`: `static void`; does nothing unless the three conditions of decision 1 hold. Called first thing in `create`.
- `trace.FromStoreHint.of(Throwable error)`: `static String`, the warning text for decision 2, or `null` when the error is not a missing `fromStore`. Walks `getCause()` to the first `MissingMethodException`.
- `core.RunRef.resolve(Index index, BlockStore store, String ref, String pipeline)`: `static Cid`, the RunCompletion address. `ref` is `latest` (needs `pipeline`), `lid://<hash>`, or a `cas://` RunCompletion or RunManifest. Throws `IllegalArgumentException` for a malformed ref and `IllegalStateException` for one the index cannot resolve; messages say "run reference", never "channel.fromStore". The index must already be caught up.
- `core.ItemHit`: value with `final Cid collection`, `final Cid item`, `Comparable` (item CID, then collection CID); `String occurrence()` returns `"cas://${collection}/${item}"`.
- `core.Samplesheet.of(BlockStore store, List<Cid> items, List<String> occurrences)`: the two-argument `of` plus a leading `occurrence` column; a Meta Map column named `occurrence` becomes `meta.occurrence`, a file position of that name `file.occurrence`.
- `cli.CasCommands.clock`: `Closure<Long>`, the name Claim's timestamp source (a test seam).
- `core.Index.itemHitsByText(Cid completion, String outputName, Map<String, String> conditions)`: `List<ItemHit>`, sorted by item CID then collection CID; a condition matches an `item_attr` row with that `path` and `value` text, any `type`, `truncated = 0`; every condition must match (AND).
- `cli.ItemsCommand.run(List<String> args, Map config, PrintStream out, PrintStream err)`: `static int`, exit status (0 ok, 1 failure, 2 usage), as `ExploreCommand.run`.
- `ext.CasExtension`: `restore(Object value, Cid itemCid, boolean records)` (private); `records` read from `opts.records` by a private `recordsOpt(Map opts)`.

Page (`web/src/`, ES modules):

- `pairs.js`: `pairsOf(view) -> [{path, type, values, types}]` (from `attrRows`, grouped by path, sorted by path, truncated rows skipped; `type` is `'mixed'` when a path's values differ in type); `labelPaths(previewList, n = 3) -> [path]` (decision 9's rule over `[{pairs}]`); `labelText(preview, paths) -> string` (falls back to file names joined by `, `); `filterHref(target, path, type, value) -> string|null` with `target = {completion, output, where}`; `pairsNode(pairs, target, {size: 'row'|'page'}) -> Node|null` (`null` for no pairs); `PAIRS_CSS` string.
- `previews.js`: `new Previews(ex, {cap = 100, concurrency = 6})`; `ask(collectionCid, itemCid)`; `askFirst(rows)`; `cancel()`; `cap`; `get(itemCid) -> {pairs, files} | {error, message} | undefined`; `onChange(fn)`; `stats -> {fetched, failed, inFlight, queued}`. `collectionCid` may be `null` (a Selection member with no via).
- `model.js` on `Explorer`: `runRow(cid)` rows gain `nf_run_hash`; `runLabel(collectionCid) -> Promise<{run_name, output, completion}|null>` (from `collectionByCid` then `runRow`; tail runs through `staleRow`); `allItems(collectionCid) -> Promise<string[]>` (every item CID, snapshot via `collectionAllItems`, tail via the collection block).
- `views.js`: `itemRows(ex, ctx, {items, collectionCid, completion, output, where, previews, total, results}) -> Node` (one list, checkboxes, "Add checked (k)", "Add all N" as `[data-pick-all]`); `itemRow(ex, ctx, {address, via, previews, target, ...}) -> Node` for the tray and Selection members; `pickAll(ex, tray, {items, collectionCid})`; `tray.addMany(picks)`.
- `snippets.js`: `snippetBlock({kind: 'selection', cid} | {kind: 'run', lid, output}) -> Node`; `snippetLines(spec, mode) -> {include, call}`; `snippetToggle() -> Node`; `snippetMode(storage?) -> 'untyped'|'typed'`; `setSnippetMode(mode, storage?)`; `SNIPPET_KEY`, `INCLUDE_LINE`.

DOM contract additions (DESIGN §15 table, Task 12): `[data-pick-all]` (button; `data-via` its one collection, `data-count` N; `#tray[data-count]` updates once its items have been added), `[data-preview-for="<item cid>"]` (the pills container of a row), `[data-snippet="untyped"|"typed"]` (the `<code>` holding exactly one call expression, `channel.fromStore(...)` or `nextflow.Channel.fromStore(..., records: true)`), `[data-snippet-mode="untyped"|"typed"]` (the toggle's buttons). The snippet mode is stored as the bare string `untyped` or `typed` under `localStorage` key `nf-blocks.snippets`. `[data-item-result]`, `[data-pick]` keep their meaning; `[data-pick]`'s `data-via` is now filled on query results.

## Waves

| Wave | Tasks | Depends on |
|---|---|---|
| 0, Runs | 1 default `outputDir`; 2 the missing-`fromStore` hint and failing-run safety; 3 `records: true` | `main` with the config fix |
| 1, Command line | 4 `RunRef` and `itemHitsByText`; 5 `nf-blocks:items`; 6 `put --name` | wave 0 (Task 4 edits `CasExtension` after Tasks 2 and 3) |
| 2, The page | 7 pills and previews; 8 model and queries; 9 item rows everywhere; 10 snippets | none of waves 0-1 (Task 10's typed snippet names Task 3's `records`) |
| 3, Gate and documents | 11 Gate tier B additions and the typed consumer; 12 DESIGN, spec, README, acceptance | waves 0-2 |

`CasObserver.groovy` is edited by Tasks 1 then 2; `CasExtension.groovy` by Tasks 2, 3, 4 in that order; `views.js` by Tasks 9 then 10; `CasCommands.groovy` by Tasks 5 then 6. Run tasks in number order within a wave; waves 0 and 2 may run in parallel.

Every Groovy task ends with `./gradlew test` green; every web task with `cd web && npm test` green; Tasks 1, 2, 11 and 12 with `make gate` (lineage 11/0/6 or better, browser tier A 5/5, tier B 9/9 before Task 11 and 11/11 after). Run the Gate with `GATE_ROOT` in the session scratchpad, and rerun once before debugging a Gate assertion 4 failure: it is intermittent by design (`gate/README.md`).

---

### Task 1: An unset `outputDir` means the lineage alias

Ticket 02. A consuming run with no `outputDir` line publishes into `cas://<writable alias>` rather than aborting. The default is set in the observer factory because factories run inside `createObserversV2()` (`Session.groovy:469`, loop at `512-515`), before `new WorkflowMetadata(this, scriptFile)` at `Session.groovy:472` copies `session.outputDir` into its own field (`WorkflowMetadata.groovy:271`). `LinObserver.storeWorkflowRun` (`LinObserver.groovy:164-180`) serialises that copy through `collectWorkflowMetadata` (`LinObserver.groovy:545-547`), turning the path into text with `FilesEx.toUriString` (`FilesEx.groovy:1605-1614`), which for a cas path is `CasPathFactory.toUriString`, which is `CasPath.toString()`: `cas://out`. That string is what the Gate reads back. The value is built with `FileHelper.toCanonicalPath`, as `Session.groovy:418` does for an explicit config value. `-output-dir` on the command line is copied into `config.outputDir` by `ConfigBuilder.groovy:570-571`, so the factory sees it as explicit and leaves it alone (Review Focus 3).

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy`
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`validateOutputDir` only; see the INTERFACE NOTE)
- Create: `src/test/groovy/robsyme/cas/trace/CasObserverFactoryTest.groovy`
- Modify: `gate/consumer/nextflow.config`
- Modify: `gate/assert.py` (assertion 6)
- Modify: `gate/test_assert.py`
- Modify: `gate/README.md` (assertion 6 row)

**Interfaces:**
- Consumes: `CasConfig.aliasOf(String)`, `FileHelper.toCanonicalPath(Object)`, `Session.getConfig()`, `Session.setOutputDir(Path)` / `getOutputDir()` (plain property, `Session.groovy:152`), `cas.Store.nf_records(kind)` in the Gate.
- Produces:
  - `static void CasObserverFactory.defaultOutputDir(Session session)`: sets `session.outputDir` to the canonical `cas://<alias>` path and logs at info `outputDir not set; publishing to cas://<alias>` when lineage is enabled, `lineage.store.location` is a bare `cas://<alias>`, and the config has no `outputDir`; otherwise does nothing. Never throws. Called first in `create`.
  - `CasObserver.validateOutputDir()` judges `session.config.outputDir` when set, else `session.outputDir` when it is a `CasPath`; the abort message is today's.
  - `gate/assert.py`: `_consumer_output_dir(gate) -> str|None`, the `metadata.outputDir` of the one `WorkflowRun` named `consumer` under `store-out/nf/`; raises `cas.GateError` when there is not exactly one. `CONSUMER_OUTPUT_DIR = "cas://out"`.

- [ ] **Step 1: Write the failing unit tests**

```groovy
// src/test/groovy/robsyme/cas/trace/CasObserverFactoryTest.groovy
package robsyme.cas.trace

import java.nio.file.Path

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import org.slf4j.LoggerFactory
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.nio.CasFileSystemProvider
import robsyme.cas.nio.CasPath
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Ticket 02: an unset outputDir defaults to the lineage alias, set in the
 * factory so WorkflowMetadata (Session.groovy:472) copies the default. Every
 * other case is left for CasObserver.onFlowCreate to judge as before.
 */
class CasObserverFactoryTest extends Specification {

    @TempDir
    Path tempDir

    Session session
    /** What the mock session's outputDir property holds; [0] is the value. */
    Path[] box
    Logger logger
    Level savedLevel
    ListAppender<ILoggingEvent> appender

    def setupSpec() {
        // FileHelper.toCanonicalPath('cas://lab') reaches CasPathFactory and
        // CasPlugin.provider(); prime the provider as CasLinStoreTest does.
        final field = CasPlugin.getDeclaredField('provider')
        field.setAccessible(true)
        field.set(null, new CasFileSystemProvider())
    }

    def setup() {
        logger = (Logger) LoggerFactory.getLogger(CasObserverFactory)
        savedLevel = logger.level
        logger.level = Level.INFO
        appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
    }

    def cleanup() {
        logger.detachAppender(appender)
        logger.level = savedLevel
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private Map config(Map overrides = [:]) {
        final Map cfg = [
            lineage: [enabled: true, store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                resolve: ['lab'],
                asserted_by: 'test',
                index: [path: tempDir.resolve('index.sqlite').toString()],
            ],
        ]
        cfg.putAll(overrides)
        return cfg
    }

    /**
     * A mock session whose outputDir behaves as the real property does. The
     * holder is a local so the mock's configuration closure cannot resolve it
     * against the mock itself.
     */
    private void bind(Map cfg, Path initial) {
        final Path[] holder = [initial] as Path[]
        session = Mock(Session) {
            getConfig() >> cfg
            getOutputDir() >> { holder[0] }
            setOutputDir(_) >> { Path p -> holder[0] = p }
            getRunName() >> 'test-run'
        }
        box = holder
        Global.session = session
    }

    private boolean logged(String text) {
        return appender.list.any { ILoggingEvent e -> e.level == Level.INFO && e.formattedMessage == text }
    }

    def 'an unset outputDir becomes the lineage alias, logged at info, and the config is left as written'() {
        given: 'what Session.create sets for an unset outputDir (Session.groovy:418)'
        bind(config(), tempDir.resolve('results'))

        when:
        CasObserverFactory.defaultOutputDir(session)

        then:
        box[0] instanceof CasPath
        box[0].toString() == 'cas://lab'
        logged('outputDir not set; publishing to cas://lab')
        !session.config.containsKey('outputDir')
    }

    def 'nothing changes when #why'() {
        given:
        final Path initial = tempDir.resolve('results')
        bind(config(overrides), initial)

        when:
        CasObserverFactory.defaultOutputDir(session)

        then:
        box[0] == initial
        appender.list.every { ILoggingEvent e -> !e.formattedMessage.contains('outputDir not set') }

        where:
        why                                                   | overrides
        'lineage is off'                                      | [lineage: [enabled: false, store: [location: 'cas://lab']]]
        'lineage.enabled is absent'                           | [lineage: [store: [location: 'cas://lab']]]
        'there is no lineage scope'                           | [lineage: null]
        'the lineage store is not a cas:// location'          | [lineage: [enabled: true, store: [location: 'file:///tmp/lineage']]]
        'the lineage store has no location'                   | [lineage: [enabled: true]]
        'the lineage location is not a bare alias'            | [lineage: [enabled: true, store: [location: 'cas://lab/sub']]]
        'the lineage location has an empty alias'             | [lineage: [enabled: true, store: [location: 'cas://']]]
        'outputDir is a local path'                           | [outputDir: 'results']
        'outputDir already names the alias'                   | [outputDir: 'cas://lab']
        'outputDir came from -output-dir (ConfigBuilder:570)' | [outputDir: '/data/out']
        'outputDir is a directory inside the alias'           | [outputDir: 'cas://lab/sub']
        'outputDir names another alias'                       | [outputDir: 'cas://other']
    }

    def 'create defaults the outputDir first, and the observer then accepts the run at onFlowCreate'() {
        given:
        bind(config(), tempDir.resolve('results'))

        when:
        final List<Object> observers = new ArrayList<Object>(new CasObserverFactory().create(session))
        (observers[0] as CasObserver).onFlowCreate(session)

        then:
        noExceptionThrown()
        observers.size() == 1
        observers[0] instanceof CasObserver
        box[0].toString() == 'cas://lab'
    }

    def 'an explicit #value is left alone and still aborts at onFlowCreate (Review Focus 3)'() {
        given:
        bind(config(outputDir: value), tempDir.resolve('explicit'))
        final CasObserver observer = new CasObserverFactory().create(session)[0] as CasObserver

        when:
        observer.onFlowCreate(session)

        then:
        final AbortRunException e = thrown()
        e.message == "outputDir must publish through the lineage store 'cas://lab', but is '${value}'".toString()
        box[0] == tempDir.resolve('explicit')

        where:
        value << ['cas://lab/sub', '/data/out', 'cas://other']
    }

    def 'with lineage off, an unset outputDir is not defaulted and onFlowCreate still aborts naming it unset'() {
        given:
        bind(config(lineage: [store: [location: 'cas://lab']]), tempDir.resolve('results'))
        final CasObserver observer = new CasObserverFactory().create(session)[0] as CasObserver

        when:
        observer.onFlowCreate(session)

        then:
        final AbortRunException e = thrown()
        e.message == "outputDir must publish through the lineage store 'cas://lab', but is 'unset'"
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverFactoryTest'`
Expected: FAIL. The default and no-op features fail with `MissingMethodException: No signature of method: static robsyme.cas.trace.CasObserverFactory.defaultOutputDir()`; the "accepts the run" feature fails with `AbortRunException ... but is 'unset'`. The two abort features pass already.

- [ ] **Step 3: Implement the factory**

Replace `src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy`:

```groovy
package robsyme.cas.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2
import robsyme.cas.CasConfig

/**
 * Registers the one {@link CasObserver} of a run (DESIGN.md §11), after giving
 * an unset {@code outputDir} its default (ticket 02). Listed in
 * {@code extensionPoints}; {@code @Extension} alone registers nothing.
 */
@Slf4j
@CompileStatic
class CasObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        defaultOutputDir(session)
        return Collections.<TraceObserverV2> singletonList(new CasObserver())
    }

    /**
     * An unset {@code outputDir} means the writable member's alias. This runs
     * inside {@code Session.init}'s {@code createObserversV2()}
     * (Session.groovy:469, 514), before {@code new WorkflowMetadata} copies
     * {@code session.outputDir} (Session.groovy:472, WorkflowMetadata.groovy:271),
     * so {@code workflow.outputDir}, the lineage WorkflowRun record and Platform
     * payloads all see one value.
     *
     * It changes nothing unless lineage is enabled, {@code lineage.store.location}
     * is a bare {@code cas://<alias>} (the CasLinStoreFactory.canOpen test plus
     * CasConfig's alias rule) and the config sets no {@code outputDir};
     * {@code -output-dir} counts as set, since ConfigBuilder.groovy:570-571 copies
     * it into the config. {@code session.config} is left as written, so the
     * RunManifest records the config the user wrote. It never throws: a failure
     * leaves {@code outputDir} alone and {@code CasObserver.onFlowCreate} judges
     * the run as before.
     */
    static void defaultOutputDir(Session session) {
        final Map config = session?.config
        if( config == null || config.get('outputDir') )
            return
        final Object lineage = config.get('lineage')
        if( !(lineage instanceof Map) || !isTrue(((Map) lineage).get('enabled')) )
            return
        final Object store = ((Map) lineage).get('store')
        final String location = store instanceof Map ? ((Map) store).get('location') as String : null
        if( !location?.startsWith('cas://') )
            return
        final String alias = CasConfig.aliasOf(location)
        if( !alias )
            return
        final String target = "cas://${alias}".toString()
        try {
            // The same conversion Session.groovy:418 applies to an explicit value.
            session.outputDir = FileHelper.toCanonicalPath(target)
        }
        catch( Exception e ) {
            log.warn("outputDir not set, and ${target} could not be resolved as its default: ${e.message}", e)
            return
        }
        log.info("outputDir not set; publishing to ${target}")
    }

    private static boolean isTrue(Object value) {
        return value instanceof Boolean ? (Boolean) value : value?.toString() == 'true'
    }
}
```

- [ ] **Step 4: Let `onFlowCreate` accept the default**

In `src/main/groovy/robsyme/cas/trace/CasObserver.groovy`, add `import robsyme.cas.nio.CasPath` beside the other `robsyme.cas` imports and replace `validateOutputDir` (lines 66-76):

```groovy
    /**
     * The writable member is the alias in {@code lineage.store.location};
     * {@code outputDir} must name the same alias or provenance would split
     * across two stores (DESIGN.md §2). An unset {@code outputDir} was given
     * the alias by {@link CasObserverFactory#defaultOutputDir}, which sets
     * {@code session.outputDir} and leaves the config as written, so that is
     * what is judged when the config names none.
     */
    private void validateOutputDir() {
        final String configured = session.config?.get('outputDir') as String
        final String outputDir = configured ?: defaultedOutputDir()
        final String outputAlias = CasConfig.aliasOf(outputDir)
        if( outputAlias != cas.config.writableAlias )
            throw new AbortRunException("outputDir must publish through the lineage store 'cas://${cas.config.writableAlias}', but is '${configured ?: 'unset'}'")
    }

    /** {@code session.outputDir} when the factory defaulted it to a cas path, else null. */
    private String defaultedOutputDir() {
        final Path current = session.outputDir
        return current instanceof CasPath ? current.toString() : null
    }
```

`CasObserverTest`'s mock sessions never stub `getOutputDir()`, so it returns null there and every existing feature is judged by its configured value, as before.

- [ ] **Step 5: Run the unit tests**

Run: `./gradlew test --tests 'robsyme.cas.trace.*'`
Expected: PASS (`CasObserverFactoryTest`, `CasObserverTest`, `JoinTest`).

- [ ] **Step 6: Write the failing Gate test**

In `gate/test_assert.py`, add `import json` to the imports and this class before `if __name__ == "__main__":`

```python
class TestConsumerOutputDir(TempTree):
    """Assertion 6 reads the consumer's lineage WorkflowRun from store-out/nf/."""

    def _workflow_run(self, key, name, metadata):
        envelope = {"version": "lineage/v1beta1", "kind": "WorkflowRun",
                    "spec": {"name": name, "sessionId": "s", "params": [],
                             "config": {}, "metadata": metadata}}
        self.write("store-out/nf/%s/.data.json" % key,
                   json.dumps(envelope).encode("utf-8"))

    def test_reads_the_consumer_runs_output_dir(self):
        self._workflow_run("c0" * 16, "consumer", {"outputDir": "cas://out"})
        self._workflow_run("c1" * 16, "someone-else", {"outputDir": "/tmp/results"})
        self.write("store-out/nf/c0c0/task/.data.json",
                   json.dumps({"kind": "TaskRun", "spec": {"name": "consumer"}}).encode("utf-8"))
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._consumer_output_dir(gate), "cas://out")

    def test_a_record_without_metadata_reads_as_none(self):
        self._workflow_run("c0" * 16, "consumer", None)
        gate = gate_assert.Gate(self.tmp)
        self.assertIsNone(gate_assert._consumer_output_dir(gate))

    def test_no_consumer_record_is_an_error(self):
        self._workflow_run("c1" * 16, "someone-else", {"outputDir": "cas://out"})
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError) as ctx:
            gate_assert._consumer_output_dir(gate)
        self.assertIn("consumer", str(ctx.exception))
        self.assertIn("found 0", str(ctx.exception))

    def test_two_consumer_records_is_an_error(self):
        self._workflow_run("c0" * 16, "consumer", {"outputDir": "cas://out"})
        self._workflow_run("c2" * 16, "consumer", {"outputDir": "cas://out"})
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError) as ctx:
            gate_assert._consumer_output_dir(gate)
        self.assertIn("found 2", str(ctx.exception))
```

Run: `python3 -m unittest discover -s gate -p 'test_assert.py'`
Expected: FAIL, four errors: `AttributeError: module 'gate_assert' has no attribute '_consumer_output_dir'`.

- [ ] **Step 7: Implement the Gate check and drop the consumer's `outputDir`**

In `gate/consumer/nextflow.config`, delete line 14, `outputDir = 'cas://out'`, and replace the file's opening comment with:

```groovy
// Two members: the producer's store read-only, and the consumer's own
// writable store. DESIGN.md section 2 has no read-only flag: the writable
// member is the alias named by lineage.store.location, every other member of
// `resolve` is read-only, and writes go to resolve[0].
//
// No outputDir line: an unset outputDir means the writable member's alias
// (ticket 02), so workflow outputs publish into cas://out. Assertion 6 checks
// that the lineage WorkflowRun record names it.
```

In `gate/assert.py`, below `_bam_sha256`, add:

```python
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
```

and replace the tail of `assert_six`, from `if problems:` to the end of the function, with:

```python
    output_dir = _consumer_output_dir(gate)
    if output_dir != CONSUMER_OUTPUT_DIR:
        problems.append("the consumer's WorkflowRun record says outputDir is %r, "
                        "expected %r: gate/consumer/nextflow.config sets no "
                        "outputDir, so it must default to the lineage alias "
                        "before WorkflowMetadata copies it (ticket 02)"
                        % (output_dir, CONSUMER_OUTPUT_DIR))
    if problems:
        return FAIL, ("; ".join(problems) + ". Not covered in the skeleton: the "
                      "run-rooted cas://<runCid>/aligned/A/A.bam form and a glob "
                      "over a manifest.")
    return PASS, ("lid:// and cas:// each staged one file hashing to %s; the "
                  "consumer set no outputDir, published into %s, and its "
                  "WorkflowRun names %s; the run-rooted form and the manifest "
                  "glob are not in the skeleton"
                  % (expected, gate.store_out.root, CONSUMER_OUTPUT_DIR))
```

The "outputs land in the store" half of ticket 02's Gate check is the existing `_consumer_hashes` read: every file it finds came from a `coords/hashes/...` pointer in `store-out`, so a consumer that published elsewhere fails the `if not hashes` branch at the top of `assert_six`.

In `gate/README.md`, replace the assertion 6 row of the assertion table with:

```markdown
| 6 | `lid://` and `cas://` each stage into a second pipeline and hash to the expected address; the consumer, with no `outputDir` line, publishes into its store | requires exactly one file under each of `hashes/lid/` and `hashes/cas/` in `store-out`, compares its digest against the Gate's own sha256 of `A.bam`, and requires the consumer's lineage `WorkflowRun` (`store-out/nf/*/.data.json`, `name` `consumer`) to have `metadata.outputDir` `cas://out` |
```

- [ ] **Step 8: Run the Gate tests and the Gate**

Run: `python3 -m unittest discover -s gate`
Expected: `OK`.

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage `11 PASS, 0 FAIL, 6 SKIP`, assertion 6's PASS line naming `cas://out`; browser tier A 5/5; tier B 9/9. `grep 'outputDir not set; publishing to cas://out' <scratchpad>/gate/logs/consumer/nextflow.log` prints one line.

- [ ] **Step 9: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy \
        src/main/groovy/robsyme/cas/trace/CasObserver.groovy \
        src/test/groovy/robsyme/cas/trace/CasObserverFactoryTest.groovy \
        gate/consumer/nextflow.config gate/assert.py gate/test_assert.py gate/README.md
git commit -m "feat(trace): an unset outputDir publishes to the lineage alias

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Explain a missing `fromStore`, and keep a failing run's error handling intact

Tickets 06 and 07 (Q1 to Q3). What the observer sees was measured (`.scratch/research/missing-fromstore-exception-chain.md`): `ScriptRunner.execute` catches the script's error and calls `session.abort(e)` (`ScriptRunner.groovy:138-139`), which sets `session.error` (`Session.groovy:819`, read through `getError()` at `307`), runs `shutdown0()` and so every observer's `onFlowComplete` (`830`), then `notifyError(null)` (`831`), which calls each V2 observer's `onFlowError(new TaskEvent(null, null))` (`1106`); the launcher prints the error only afterwards. The error is a `MissingProcessException` (`WorkflowDef.groovy:189-190`) whose cause is a `groovy.lang.MissingMethodException`. Untyped scripts give method `Channel.fromStore`, type `java.lang.Object` (`PluginExtensionProvider.groovy:289`); typed scripts give method `fromStore`, type `nextflow.dataflow.ChannelNamespace` for `channel.` and `nextflow.script.types.Channel` for `Channel.`. `notifyEvent` rethrows an `AbortRunException` from any observer (`Session.groovy:1125-1128`), which leaves the abort block before `notifyError`, so `onFlowComplete` must not throw on a run that is already failing.

**Files:**
- Create: `src/main/groovy/robsyme/cas/trace/FromStoreHint.groovy`
- Create: `src/test/groovy/robsyme/cas/trace/FromStoreHintTest.groovy`
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`onFlowError`, `onFlowComplete`)
- Modify: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy` (no-argument error)
- Modify: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`

**Interfaces:**
- Consumes: `Session.getError()`, `MissingMethodException.getMethod()` / `getType()`, `TraceObserverV2.onFlowError(TaskEvent)` (`TraceObserverV2.groovy:121`).
- Produces:
  - `static String FromStoreHint.of(Throwable error)`: `FromStoreHint.UNTYPED`, `FromStoreHint.TYPED` or `null`. Walks `getCause()` (cycle-safe) to the first `MissingMethodException`; `Channel.fromStore` is untyped, `fromStore` on one of the two typed receivers is typed, anything else is `null`.
  - `CasObserver.onFlowError(TaskEvent)`: logs the hint for `session.error` at warn, once per run.
  - `CasObserver.onFlowComplete()`: when `session.error` is set, catches its own `Exception` and logs it at warn; with no `session.error` it throws as today.
  - `CasExtension.USAGE`, thrown as `IllegalArgumentException` when `fromStore` gets none of `selection`, `run`, `output`.

- [ ] **Step 1: Write the failing `FromStoreHint` tests**

```groovy
// src/test/groovy/robsyme/cas/trace/FromStoreHintTest.groovy
package robsyme.cas.trace

import nextflow.dataflow.ChannelNamespace
import nextflow.exception.MissingProcessException
import nextflow.exception.ScriptCompilationException
import nextflow.script.ScriptMeta
import nextflow.script.WorkflowBinding
import nextflow.script.types.Channel as TypedChannel
import spock.lang.Specification

/**
 * The chains measured on Nextflow 26.04.6 (research: missing-fromstore-exception-chain):
 * a MissingProcessException, wrapped by WorkflowDef.run around the
 * MissingMethodException the call raised.
 */
class FromStoreHintTest extends Specification {

    private static final Object[] ARGS = [[selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua']] as Object[]

    /** What WorkflowDef.groovy:189-190 throws around the call's own exception. */
    private MissingProcessException wrapped(MissingMethodException cause) {
        final ScriptMeta meta = Stub(ScriptMeta) {
            getAllNames() >> new HashSet<String>()
        }
        return new MissingProcessException(meta, cause)
    }

    def 'an untyped channel.fromStore with no include gets the include line'() {
        given: 'PluginExtensionProvider.groovy:289'
        final error = wrapped(new MissingMethodException('Channel.fromStore', Object, ARGS))

        expect:
        FromStoreHint.of(error) == FromStoreHint.UNTYPED
        FromStoreHint.UNTYPED == "`fromStore` comes from the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'` at the top of the script."
    }

    def 'a typed #call gets the nextflow.Channel workaround'() {
        given:
        final error = wrapped(new MissingMethodException('fromStore', receiver, ARGS, true))

        expect:
        FromStoreHint.of(error) == FromStoreHint.TYPED
        FromStoreHint.TYPED == "In a typed script, `channel.fromStore` can't be reached until nextflow-io/nextflow#7694 is fixed. " +
            "Add the include and call `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed inputs."

        where:
        call                  | receiver
        'channel.fromStore'   | ChannelNamespace
        'Channel.fromStore'   | TypedChannel
    }

    def 'the MissingMethodException is found unwrapped, and under more than one wrapper'() {
        expect:
        FromStoreHint.of(new MissingMethodException('Channel.fromStore', Object, ARGS)) == FromStoreHint.UNTYPED
        FromStoreHint.of(new RuntimeException('outer', wrapped(new MissingMethodException('fromStore', ChannelNamespace, ARGS, true)))) == FromStoreHint.TYPED
    }

    def 'no hint for #why'() {
        expect:
        FromStoreHint.of(error) == null

        where:
        why                                              | error
        'no error'                                       | null
        'another missing channel factory'                | new MissingMethodException('Channel.fromLineage', Object, ARGS)
        'another missing typed member'                   | new MissingMethodException('fromPathz', ChannelNamespace, ARGS, true)
        'a bare fromStore call (WorkflowBinding:115)'    | new MissingMethodException('fromStore', WorkflowBinding, ARGS)
        'a compile error'                                | new ScriptCompilationException('Script compilation failed')
        'a task failure'                                 | new IllegalStateException('Process `HASH` terminated with an error exit status (1)')
    }

    def 'no hint for an unrelated missing method wrapped the same way'() {
        expect:
        FromStoreHint.of(wrapped(new MissingMethodException('HASHH', Object, ARGS))) == null
    }

    def 'a cause chain that loops ends the walk'() {
        given:
        final RuntimeException a = new RuntimeException('a')
        final RuntimeException b = new RuntimeException('b')
        a.initCause(b)
        b.initCause(a)

        expect:
        FromStoreHint.of(a) == null
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.FromStoreHintTest'`
Expected: FAIL at `compileTestGroovy`: `unable to resolve class FromStoreHint`.

- [ ] **Step 3: Implement `FromStoreHint`**

```groovy
// src/main/groovy/robsyme/cas/trace/FromStoreHint.groovy
package robsyme.cas.trace

import groovy.transform.CompileStatic

/**
 * The warning for a script that calls {@code fromStore} where Nextflow cannot
 * reach it (tickets 06 and 07). Pure: it reads only the error.
 *
 * Nextflow 26.04.6 wraps the call's {@link MissingMethodException} in a
 * MissingProcessException (WorkflowDef.groovy:189-190). An untyped script's
 * {@code channel} is {@code nextflow.Channel}, whose missing-method hook throws
 * with method {@code Channel.fromStore} when no include loaded the factory
 * (PluginExtensionProvider.groovy:289). A typed script's {@code channel} is
 * {@code nextflow.dataflow.ChannelNamespace} and its {@code Channel} is
 * {@code nextflow.script.types.Channel}; neither consults plugin factories, so
 * Groovy's own error names method {@code fromStore} with or without the include.
 */
@CompileStatic
class FromStoreHint {

    static final String UNTYPED =
        "`fromStore` comes from the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'` at the top of the script."

    static final String TYPED =
        "In a typed script, `channel.fromStore` can't be reached until nextflow-io/nextflow#7694 is fixed. " +
        "Add the include and call `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed inputs."

    private static final String UNTYPED_METHOD = 'Channel.fromStore'
    private static final String TYPED_METHOD = 'fromStore'
    private static final Set<String> TYPED_RECEIVERS = Collections.unmodifiableSet(
        ['nextflow.dataflow.ChannelNamespace', 'nextflow.script.types.Channel'] as Set<String>)

    private FromStoreHint() {}

    /** The warning for {@code error}, or null when it is not a missing {@code fromStore}. */
    static String of(Throwable error) {
        final MissingMethodException missing = firstMissingMethod(error)
        if( missing == null )
            return null
        if( missing.method == UNTYPED_METHOD )
            return UNTYPED
        if( missing.method == TYPED_METHOD && TYPED_RECEIVERS.contains(missing.type?.name) )
            return TYPED
        return null
    }

    private static MissingMethodException firstMissingMethod(Throwable error) {
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>())
        Throwable current = error
        while( current != null && seen.add(current) ) {
            if( current instanceof MissingMethodException )
                return (MissingMethodException) current
            current = current.cause
        }
        return null
    }
}
```

Run: `./gradlew test --tests 'robsyme.cas.trace.FromStoreHintTest'`
Expected: PASS.

- [ ] **Step 4: Write the failing observer tests**

In `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`, add these imports:

```groovy
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import nextflow.dataflow.ChannelNamespace
import nextflow.exception.MissingProcessException
import nextflow.script.ScriptMeta
import nextflow.trace.event.TaskEvent
import org.slf4j.LoggerFactory
```

replace `cleanup()` with:

```groovy
    private final List<ListAppender<ILoggingEvent>> appenders = []

    def cleanup() {
        final Logger logger = (Logger) LoggerFactory.getLogger(CasObserver)
        appenders.each { logger.detachAppender(it) }
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private ListAppender<ILoggingEvent> capture() {
        final Logger logger = (Logger) LoggerFactory.getLogger(CasObserver)
        final ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        appenders << appender
        return appender
    }

    private static int warnings(ListAppender<ILoggingEvent> appender, String containing) {
        return appender.list.count { ILoggingEvent e -> e.level == Level.WARN && e.formattedMessage.contains(containing) } as int
    }

    /** The measured chain: MissingProcessException around the call's MissingMethodException. */
    private Throwable missingFromStore(String method, Class receiver) {
        final ScriptMeta meta = Stub(ScriptMeta) {
            getAllNames() >> new HashSet<String>()
        }
        final Object[] args = [[selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua']] as Object[]
        return new MissingProcessException(meta, new MissingMethodException(method, receiver, args, receiver != Object))
    }

    private void makeBlocksUnwritable(List<Path> into) {
        final Path blocks = tempDir.resolve('store').resolve('blocks')
        into.addAll(Files.walk(blocks).filter { Path p -> Files.isDirectory(p) }.toList())
        into.each { Path d -> d.toFile().setWritable(false, false) }
    }
```

and add these features at the end of the class:

```groovy
    def 'onFlowError warns once with the include line for an untyped script missing it'() {
        given:
        bind(config())
        session.getError() >> missingFromStore('Channel.fromStore', Object)
        final appender = capture()
        observer.onFlowCreate(session)

        when: 'notified twice, as a later task error would'
        observer.onFlowError(new TaskEvent(null, null))
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, FromStoreHint.UNTYPED) == 1
    }

    def 'onFlowError gives a typed script the nextflow.Channel workaround'() {
        given:
        bind(config())
        session.getError() >> missingFromStore('fromStore', ChannelNamespace)
        final appender = capture()
        observer.onFlowCreate(session)

        when:
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, FromStoreHint.TYPED) == 1
    }

    def 'onFlowError says nothing about fromStore for any other error, or none'() {
        given:
        bind(config())
        session.getError() >> error
        final appender = capture()
        observer.onFlowCreate(session)

        when:
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, 'fromStore') == 0

        where:
        error << [null, new IllegalStateException('Process `HASH` terminated with an error exit status (1)')]
    }

    def 'a failing run still gets its failed RunCompletion when the store is fine'() {
        given:
        bind(config(), meta(1))
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        session.getError() >> missingFromStore('Channel.fromStore', Object)

        when:
        observer.onFlowCreate(session)
        observer.onFlowComplete()

        then:
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
        blocksOfKind('RunCompletion')[0].get('status') == 'failed'
    }

    def 'on a run that is already failing, a provenance write failure is logged, not thrown, so onFlowError still runs'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        session.getError() >> missingFromStore('Channel.fromStore', Object)
        final appender = capture()
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        final List<Path> dirs = []
        makeBlocksUnwritable(dirs)

        when: 'the order Session.abort uses (Session.groovy:830-831)'
        observer.onFlowComplete()
        observer.onFlowError(new TaskEvent(null, null))

        then:
        noExceptionThrown()
        warnings(appender, 'the run is already failing') == 1
        warnings(appender, FromStoreHint.UNTYPED) == 1
        cas.awaitCompletionWritten(0)

        cleanup:
        dirs.each { Path d -> d.toFile().setWritable(true, false) }
    }
```

The existing feature 'a failure while writing the completion reaches the caller and still releases the waiting notification' stays as it is: its mock `getError()` returns null, so it pins that a run with no error still aborts on a failed provenance write.

- [ ] **Step 5: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest'`
Expected: FAIL. The two hint features find 0 warnings (the default `onFlowError` does nothing); 'on a run that is already failing...' throws `AbortRunException: Unable to write the RunCompletion provenance block`. The "any other error" and "store is fine" features pass already.

- [ ] **Step 6: Implement the observer changes**

In `src/main/groovy/robsyme/cas/trace/CasObserver.groovy`, add `import java.util.concurrent.atomic.AtomicBoolean`, add a field below `labels`:

```groovy
    /** The missing-fromStore hint is logged once per run, however often onFlowError fires. */
    private final AtomicBoolean hinted = new AtomicBoolean(false)
```

replace `onFlowComplete` (lines 118-136) with:

```groovy
    /**
     * On a run that is already failing ({@code session.error} set by
     * Session.abort, Session.groovy:819, before it notifies completion at 830),
     * a failure here is logged rather than thrown: an AbortRunException would
     * stop Session.notifyEvent (Session.groovy:1125-1128) before notifyError
     * (831) reaches any observer, losing the fromStore hint and the user's own
     * onError (ticket 07 Q2). A run with no error keeps rule 3's abort.
     */
    @Override
    void onFlowComplete() {
        final Throwable failing = session?.error
        if( failing == null ) {
            completeRun()
            return
        }
        try {
            completeRun()
        }
        catch( Exception e ) {
            log.warn("the run is already failing (${failing.message ?: failing.class.name}), and nf-blocks could not record it either: ${e.message}", e)
        }
    }

    private void completeRun() {
        // Fires twice on a failed run (no barrier on that path); the latch keeps
        // exactly one RunCompletion.
        if( !cas.claimCompletion() ) {
            // The other notification is writing the RunCompletion. A failed
            // run's second notification comes from Session.destroy on main,
            // which reaches System.exit next, so wait until the write is done.
            if( !cas.awaitCompletionWritten(COMPLETION_WAIT_MILLIS) )
                log.warn("the run's RunCompletion was still being written after ${COMPLETION_WAIT_MILLIS} ms; not waiting longer")
            return
        }
        try {
            runUninterrupted { writeCompletion() }
        }
        finally {
            cas.completionWritten()
        }
    }

    /**
     * Session.abort calls this after onFlowComplete with {@code session.error}
     * already set (Session.groovy:830-831), before the launcher prints the
     * error, so the hint is read next to it. The event carries no handler
     * there, so the error is read from the session.
     */
    @Override
    void onFlowError(TaskEvent event) {
        final String hint = FromStoreHint.of(session?.error)
        if( hint != null && hinted.compareAndSet(false, true) )
            log.warn(hint)
    }
```

`TaskEvent` is already imported (line 16).

- [ ] **Step 7: Write the failing no-argument test**

In `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`, add:

```groovy
    def 'fromStore with none of selection, run or output says what it takes (ticket 07 Q3): #opts'() {
        when:
        ext.fromStore(opts)

        then:
        final IllegalArgumentException e = thrown()
        e.message == "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output: <name>`; optionally `where: [...]` and `records: true`."
        e.message == CasExtension.USAGE

        where:
        opts << [null, [:], [where: [sample: 'A']], [pipeline: 'p'], [records: true]]
    }

    def 'a run with no output, and an output with no run, keep their own messages'() {
        when:
        ext.fromStore(run: 'latest', pipeline: 'p')

        then:
        final IllegalArgumentException noOutput = thrown()
        noOutput.message.contains("'output'")

        when:
        ext.fromStore(output: 'aligned')

        then:
        final IllegalArgumentException noRun = thrown()
        noRun.message.contains("'run'")
    }
```

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest'`
Expected: FAIL. Every row of the first feature gets `channel.fromStore needs an 'output' name` instead of the usage text (and `CasExtension.USAGE` does not exist yet); the second feature passes already.

- [ ] **Step 8: Implement the no-argument error**

In `src/main/groovy/robsyme/cas/ext/CasExtension.groovy`, add below `LATEST`:

```groovy
    /** What fromStore takes, for a call that names none of it (ticket 07 Q3). */
    static final String USAGE =
        "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output: <name>`; optionally `where: [...]` and `records: true`."

    private static final List<String> ENTRY_KEYS = ['selection', 'run', 'output']
```

and make the first lines of `resolveItems` read:

```groovy
    private List<Object> resolveItems(Map opts) {
        if( opts == null || !ENTRY_KEYS.any { String k -> opts.containsKey(k) } )
            throw new IllegalArgumentException(USAGE)
        if( opts.containsKey('selection') )
            return resolveSelection(opts)
        final String output = opts.get('output') as String
```

(the rest of `resolveItems` is unchanged; `opts?.` becomes `opts.` in the two lines shown, since `opts` is now known non-null). A script calling `channel.fromStore()` with no arguments reaches the one-`Map` method with `opts == null`, which is the first case.

- [ ] **Step 9: Run the tests and the Gate**

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage `11 PASS, 0 FAIL, 6 SKIP`, browser tier A 5/5, tier B 9/9.

Then check the hint end to end against the Gate's store, in both script modes (`G` is the same `GATE_ROOT`; `nextflow` is the 26.04.6 binary the Gate used):

```bash
G=<scratchpad>/gate
for mode in untyped typed; do
    rm -rf "$G/hint-$mode" && mkdir -p "$G/hint-$mode/out"
    cp gate/consumer/nextflow.config "$G/hint-$mode/"
    {
        if [[ $mode == typed ]]; then echo 'nextflow.enable.types = true'; fi
        echo 'workflow {'
        echo "    channel.fromStore(run: 'latest', pipeline: 'cas-test-pipeline', output: 'aligned').view()"
        echo '}'
    } > "$G/hint-$mode/main.nf"
    ( cd "$G/hint-$mode" && NXF_PLUGINS_DIR="$G/plugins" XDG_CACHE_HOME="$G/hint-$mode/cache" \
        GATE_STORE="$G/store" GATE_STORE_OUT="$G/hint-$mode/out" NXF_ANSI_LOG=false \
        nextflow run . -name "hint-$mode" ); echo "exit $?"
    grep -n -e 'nf-blocks plugin: add `include' -e 'In a typed script' -e 'Missing process or function' "$G/hint-$mode/.nextflow.log"
done
```

Expected: each run exits 1. For `untyped`, the log shows one `WARN` line with `FromStoreHint.UNTYPED`'s text, at a lower line number than the `Missing process or function Channel.fromStore(...)` error. For `typed`, one `WARN` line with `FromStoreHint.TYPED`'s text, before `Missing process or function fromStore(...)`. Neither log contains `Abort exception produced when notifying an event`.

- [ ] **Step 10: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/FromStoreHint.groovy \
        src/main/groovy/robsyme/cas/trace/CasObserver.groovy \
        src/main/groovy/robsyme/cas/ext/CasExtension.groovy \
        src/test/groovy/robsyme/cas/trace/FromStoreHintTest.groovy \
        src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy \
        src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy
git commit -m "feat(trace): name the fix when fromStore is missing; a failing run no longer skips onFlowError

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `fromStore(..., records: true)`

Ticket 09. With `records: true`, every non-Leaf map of a restored item, at any depth and inside lists too, is a `nextflow.util.RecordMap`; Leaves still become paths and lists stay lists. The default is `false` in every script mode, on both entry points, and anything but `true` or `false` is refused. `RecordMap(Map)` (`RecordMap.java:35`) copies through `LinkedHashMap`'s constructor, which does not call the overridden `put` (`RecordMap.java:49`), so building a plain `LinkedHashMap` and wrapping it once is safe; the wrapped map is then immutable (`put`, `putAll`, `remove`, `clear` throw `UnsupportedOperationException`). Its key type is `String`; DAG-CBOR map keys are strings already (`Records.groovy:591` encodes them with `String.valueOf`, `:609` decodes the same way), so keying the rebuilt map by `String.valueOf(key)` changes nothing for the default path. `TaskProcessor.isAssignableFrom` (`TaskProcessor.groovy:1886-1891`) treats any two `Record` types as compatible, which is what stops the "invalid argument type" warning for a record-typed input.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy`
- Modify: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`

**Interfaces:**
- Consumes: `nextflow.util.RecordMap` (nf-commons, on the plugin's compile classpath through `io.nextflow:nextflow`, as `nextflow.util.MemoryUnit` already is for `CasConfig`), Task 2's `USAGE` guard at the top of `resolveItems`.
- Produces:
  - `fromStore(selection: ..., records: true|false)` and `fromStore(run: ..., output: ..., records: true|false)`.
  - `private static boolean recordsOpt(Map opts)`: `false` when absent; the `Boolean` when one is given; otherwise `IllegalArgumentException` naming `records`.
  - `private Object restore(Object value, Cid itemCid, boolean records)`.

- [ ] **Step 1: Write the failing tests**

In `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`, add `import nextflow.util.RecordMap`, and let `storeRun` take an item's whole shape: replace line 108,

```groovy
            final OutputItem item = OutputItem.of([spec.meta, leaf])
```

with

```groovy
            // `shape`, when given, builds the item's whole value around its leaf.
            final OutputItem item = OutputItem.of(spec.shape ? ((Closure) spec.shape).call(leaf) : [spec.meta, leaf])
```

Then add:

```groovy
    // ------------------------------------------------------------ records: true

    /** One item whose value is a map holding a nested map, a list of maps and a Leaf inside a list. */
    private Map peopleRun() {
        return storeRun(
            pipeline: 'p', nfHash: 'nfhashP', output: 'people', withCompletion: true,
            items: [[name: 'A.bam', content: rawCid(4), size: 40L, path: 'people/A/A.bam',
                     shape: { Leaf bam -> [id: 'A', person: [name: 'Ada', langs: [[lang: 'en'], [lang: 'fr']]], files: [bam]] }]])
    }

    private Cid peopleItemOf(Cid completion) {
        final Index index = Index.open(indexFile)
        try {
            return index.items(completion, 'people', [:])[0]
        }
        finally {
            index.close()
        }
    }

    private static void assertRecordsThroughout(Map item) {
        assert item instanceof RecordMap
        assert item.person instanceof RecordMap
        assert (item.person as Map).langs instanceof List
        assert ((item.person as Map).langs as List).every { it instanceof RecordMap }
        assert item.files instanceof List
        assert (item.files as List)[0] instanceof CasPath
        assert (item.files as List)[0].toString() == "cas://${rawCid(4)}/A.bam".toString()
        assert item == [id: 'A', person: [name: 'Ada', langs: [[lang: 'en'], [lang: 'fr']]], files: [(item.files as List)[0]]]
    }

    def 'records: true restores every non-Leaf map as a RecordMap at any depth; leaves are still paths'() {
        given:
        peopleRun()

        when:
        final List items = drain(ext.fromStore(run: 'lid://nfhashP', output: 'people', records: true))

        then:
        items.size() == 1
        assertRecordsThroughout(items[0] as Map)
    }

    def 'records: true on a tuple item: the list stays a list, the map in it is a RecordMap'() {
        given:
        alignedRun('p', 'nfhashT')

        when:
        final List items = drain(ext.fromStore(run: 'lid://nfhashT', output: 'aligned', where: [sample: 'B'], records: true))
        final List tuple = items[0] as List

        then:
        items.size() == 1
        tuple.getClass() == ArrayList
        tuple[0] instanceof RecordMap
        tuple[0] == [sample: 'B']
        tuple[1] instanceof CasPath
    }

    def 'records: true through selection: gives the same records'() {
        given:
        final Map run = peopleRun()
        final Cid s = storeSelection([Selection.item(peopleItemOf((Cid) run.completion), [])])

        when:
        final List items = drain(ext.fromStore(selection: s.toString(), records: true))

        then:
        items.size() == 1
        assertRecordsThroughout(items[0] as Map)
    }

    def 'a restored record is immutable: put throws UnsupportedOperationException'() {
        given:
        peopleRun()
        final Map item = drain(ext.fromStore(run: 'lid://nfhashP', output: 'people', records: true))[0] as Map

        when:
        item.put('x', 1)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'without records, or with records: false, maps are plain LinkedHashMaps on both entry points (#how)'() {
        given:
        final Map run = peopleRun()
        final Cid s = storeSelection([Selection.item(peopleItemOf((Cid) run.completion), [])])

        when:
        final Map viaRun = drain(ext.fromStore([run: 'lid://nfhashP', output: 'people'] + extra))[0] as Map
        final Map viaSelection = drain(ext.fromStore([selection: s.toString()] + extra))[0] as Map

        then:
        [viaRun, viaSelection].every { Map item ->
            item.getClass() == LinkedHashMap &&
                (item.person as Map).getClass() == LinkedHashMap &&
                ((item.person as Map).langs as List).every { it.getClass() == LinkedHashMap } &&
                (item.files as List)[0] instanceof CasPath
        }
        viaRun.put('x', 1) == null

        where:
        how               | extra
        'absent'          | [:]
        'records: false'  | [records: false]
    }

    def 'records refuses anything but true or false, before reading the store: #opts'() {
        when:
        ext.fromStore(opts)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('records')
        e.message.contains('true or false')

        where:
        opts << [
            [run: 'lid://no-such-run', output: 'aligned', records: 'true'],
            [run: 'lid://no-such-run', output: 'aligned', records: 1],
            [run: 'lid://no-such-run', output: 'aligned', records: null],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', records: 'yes'],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', records: [true]],
        ]
    }
```

`lid://no-such-run` is never recorded and the Selection CID is not in the store, so an `IllegalArgumentException` naming `records` (rather than the `IllegalStateException` those references would raise) shows the option is checked first.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest'`
Expected: FAIL. The `records: true` features find `LinkedHashMap` where `RecordMap` is asserted, the immutability feature throws nothing, and the refusal rows raise `IllegalStateException` (`no run with nextflow run hash 'no-such-run' is recorded`, `selection ... is not in any member`). The default-mode rows pass already.

- [ ] **Step 3: Implement**

In `src/main/groovy/robsyme/cas/ext/CasExtension.groovy`, add `import nextflow.util.RecordMap`. In the class comment, after the sentence ending "an error naming the item.", add: "With {@code records: true} every non-Leaf map is a {@code nextflow.util.RecordMap} instead, at any depth, for typed processes with record inputs (ticket 09)."

Replace `resolveItems` with:

```groovy
    private List<Object> resolveItems(Map opts) {
        if( opts == null || !ENTRY_KEYS.any { String k -> opts.containsKey(k) } )
            throw new IllegalArgumentException(USAGE)
        final boolean records = recordsOpt(opts)
        if( opts.containsKey('selection') )
            return resolveSelection(opts, records)
        final String output = opts.get('output') as String
        if( !output )
            throw new IllegalArgumentException("channel.fromStore needs an 'output' name")
        final Map<String, Object> where = (opts.get('where') ?: [:]) as Map<String, Object>

        Index index = null
        try {
            index = openIndex()
            // Read across the whole composition: bring the index up to date from
            // every member's run log, so a run recorded only in a read-only member
            // (the producer's store, mounted read-only here) is visible to `latest`.
            cas.catchUpIndex(index)
            final Cid completion = resolveRun(index, opts)
            final List<Object> items = new ArrayList<Object>()
            for( Cid itemCid : index.items(completion, output, where) ) {
                final OutputItem item = loadItem(itemCid)
                if( item == null )
                    throw new IllegalStateException("output item ${itemCid} of output '${output}' is not in the store")
                items.add(restore(item.value, itemCid, records))
            }
            return items
        }
        finally {
            index?.close()
        }
    }

    /**
     * {@code records:} (ticket 09): false when absent, the given Boolean
     * otherwise. Anything else is refused, so a typo such as
     * {@code records: 'true'} is not quietly read as false.
     */
    private static boolean recordsOpt(Map opts) {
        if( !opts.containsKey('records') )
            return false
        final Object value = opts.get('records')
        if( value instanceof Boolean )
            return (Boolean) value
        final String type = value == null ? 'null' : value.getClass().simpleName
        throw new IllegalArgumentException("fromStore's `records` takes true or false, got '${value}' (${type})")
    }
```

Change `resolveSelection`'s signature and its one `restore` call:

```groovy
    private List<Object> resolveSelection(Map opts, boolean records) {
```

```groovy
                items.add(restore(item.value, itemCid, records))
```

and replace `restore` (lines 178-195) with:

```groovy
    /**
     * Rebuilds an item's published structure, turning each leaf into a path or
     * null. With {@code records}, every map (a Leaf is decoded to {@link Leaf}
     * before this, so it is never one) is returned as an immutable RecordMap;
     * lists stay lists either way.
     */
    private Object restore(Object value, Cid itemCid, boolean records) {
        if( value instanceof Leaf )
            return pathFor((Leaf) value, itemCid)
        if( value instanceof Map ) {
            final LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map) value).entrySet() )
                out.put(String.valueOf(e.key), restore(e.value, itemCid, records))
            return records ? new RecordMap(out) : out
        }
        if( value instanceof List ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object element : (List) value )
                out.add(restore(element, itemCid, records))
            return out
        }
        return value
    }
```

`resolveSelection`'s refusal list (`run`, `output`, `where`, `pipeline`) is unchanged, so `records` is allowed beside `selection`.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest'`
Expected: PASS.

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/ext/CasExtension.groovy src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy
git commit -m "feat(ext): fromStore(records: true) restores maps as immutable records

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: One run-reference resolver, `RunRef`, and `Index.itemHitsByText`

Decision 6 and the first half of decision 7. Right now `CasExtension.resolveRun` (`CasExtension.groovy:95-125` on `fix/runmanifest-config-string`) is the only code that turns a run reference into a RunCompletion address. It moves to `core/RunRef` so that `fromStore(run:)` and `nf-blocks:items --run` share it, and its messages stop naming `channel.fromStore`. `Index` gains a query that matches conditions as text and returns each hit together with its collection. Ticket 05 Q2 asks `Index.items` to "return the collection it already selects". That is this new method: `Index.items` keeps its typed signature because `fromStore(where:)` keeps Groovy types (ticket 05 Q4).

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/RunRef.groovy`
- Create: `src/main/groovy/robsyme/cas/core/ItemHit.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (three private SQL constants, `itemHitsByText`)
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy` (`resolveRun` delegates to `RunRef`)
- Create: `src/test/groovy/robsyme/cas/core/RunRefTest.groovy`
- Create: `src/test/groovy/robsyme/cas/core/IndexItemHitsTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy` (one test)

**Interfaces:**
- Consumes: `Index.latestSuccessfulRun`, `Index.runByNextflowHash`, `Index.runByManifest`, `StoreRef.parse`, `Records.kindOf`, `DagCbor.decode`, `BlockStore.has/open`, `ItemOccurrence.PREFIX`.
- Produces:
  - `static Cid RunRef.resolve(Index index, BlockStore store, String ref, String pipeline)`. `ref` is `latest` (needs `pipeline`), `lid://<hash>`, or a `cas://` RunCompletion or RunManifest. It throws `IllegalArgumentException` for a malformed reference and `IllegalStateException` for one the index or the composition cannot resolve. Every message contains "run reference" and none contains "channel.fromStore". The index must already be caught up. Constants `RunRef.LATEST`, `RunRef.LID_PREFIX`, `RunRef.CAS_PREFIX`.
  - `final class ItemHit implements Comparable<ItemHit>` with `final Cid collection`, `final Cid item`, `String occurrence()` returning `cas://<collection>/<item>`, value equality, ordering by item CID then collection CID.
  - `List<ItemHit> Index.itemHitsByText(Cid completion, String outputName, Map<String, String> conditions)`, sorted by item CID then collection CID, one hit per distinct (collection, item). A condition matches an `item_attr` row with that `path` and `value` text, any `type`, `truncated = 0`. Every condition must match. An empty or null map means every item of the output. A null value matches nothing.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/RunRefTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Decision 6 of the milestone 3 plan: the one run-reference resolver behind fromStore(run:) and nf-blocks:items --run. */
class RunRefTest extends Specification {

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

    /** One run of pipeline `p` with an empty `aligned` output; ingested unless {@code complete} is false. */
    private Map run(String hash, String finishedAt, boolean complete = true) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(nf_run_hash: hash, run_name: "run-${hash}".toString()))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', []))
        if( !complete )
            return [manifest: manifest, collection: collection]
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [finished_at: finishedAt]))
        index.ingestRun(store, completion, 'lab')
        return [manifest: manifest, collection: collection, completion: completion]
    }

    def 'latest with a pipeline is the most recent successful run; lid:// and both cas:// kinds name one run'() {
        given:
        final Map older = run('h1', '2026-09-03T10:05:00.000Z')
        final Map newer = run('h2', '2026-09-04T10:05:00.000Z')

        expect:
        RunRef.resolve(index, store, 'latest', 'p') == newer.completion
        RunRef.resolve(index, store, 'lid://h1', null) == older.completion
        RunRef.resolve(index, store, "cas://${older.completion}".toString(), null) == older.completion
        RunRef.resolve(index, store, "cas://${newer.manifest}".toString(), null) == newer.completion
    }

    def 'a malformed reference is an IllegalArgumentException that says run reference'() {
        when:
        RunRef.resolve(index, store, ref, pipeline)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('run reference')
        !e.message.contains('channel.fromStore')

        where:
        ref               | pipeline
        null              | null
        ''                | null
        'latest'          | null
        'lid://'          | null
        'bogus'           | null
        'cas://not-a-cid' | null
    }

    def 'a cas:// reference to a block that is not a run is an IllegalArgumentException naming its kind'() {
        given:
        final Map r = run('h1', '2026-09-03T10:05:00.000Z')

        when:
        RunRef.resolve(index, store, "cas://${r.collection}".toString(), null)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('OutputCollection')
        e.message.contains('run reference')
    }

    def 'a reference the index or the composition cannot resolve is an IllegalStateException that says run reference'() {
        given:
        run('h1', '2026-09-03T10:05:00.000Z')
        final Map unfinished = run('h3', null, false)
        final Cid absent = Fixtures.cidOf([kind: 'RunCompletion', n: 1])

        when:
        RunRef.resolve(index, store, ref.call(unfinished, absent) as String, pipeline)

        then:
        final IllegalStateException e = thrown()
        e.message.contains('run reference')
        !e.message.contains('channel.fromStore')
        e.message.contains(named.call(unfinished, absent) as String)

        where:
        ref                                    | pipeline | named
        { Map u, Cid a -> 'latest' }           | 'nobody' | { Map u, Cid a -> 'nobody' }
        { Map u, Cid a -> 'lid://nope' }       | null     | { Map u, Cid a -> 'nope' }
        { Map u, Cid a -> "cas://${u.manifest}" } | null  | { Map u, Cid a -> 'did not finish' }
        { Map u, Cid a -> "cas://${a}" }       | null     | { Map u, Cid a -> a.toString() }
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/core/IndexItemHitsTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Decision 7 of the milestone 3 plan (ticket 05 Q4): nf-blocks:items matches a
 * condition's value as text against any type, every condition must match, and
 * a truncated row never matches. Each hit carries its collection.
 */
class IndexItemHitsTest extends Specification {

    static final String LONG = 'x' * 2000

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    Cid laneInt, laneString, depthFloat, pairedBool, lanesList, longNote, nestedKit
    Cid run1, aligned1, stats1

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        laneInt = item([sample: 'A', lane: 2L])
        laneString = item([sample: 'B', lane: '2'])
        depthFloat = item([sample: 'C', depth: 1.5d])
        pairedBool = item([sample: 'D', paired: true])
        lanesList = item([sample: 'E', lanes: [2L, 3L]])
        longNote = item([sample: 'F', note: LONG])
        nestedKit = item([sample: 'G', kit: [name: 'truseq']])
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        aligned1 = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned',
            [laneInt, laneString, depthFloat, pairedBool, lanesList, longNote, nestedKit].collect { Cid i -> [i, ["aligned/${i}".toString()]] }))
        stats1 = store.putDagCbor(Fixtures.outputCollection(manifest, 'stats', [[laneInt, ['stats/A.stats']]]))
        run1 = store.putDagCbor(Fixtures.runCompletion(manifest, [aligned1, stats1]))
        index.ingestRun(store, run1, 'lab')
    }

    def cleanup() {
        index?.close()
    }

    private Cid item(Map meta) {
        return store.putDagCbor(Fixtures.outputItem(
            [meta, Fixtures.leaf("${meta.sample}.bam".toString(), Fixtures.contentCid("bam-${meta.sample}".toString()), 1L)]))
    }

    private List<ItemHit> hits(Map<String, String> conditions) {
        return index.itemHitsByText(run1, 'aligned', conditions)
    }

    def 'a condition matches the value text whatever its type'() {
        expect:
        hits([lane: '2'])*.item == [laneInt, laneString].sort()
        hits([depth: '1.5'])*.item == [depthFloat]
        hits([paired: 'true'])*.item == [pairedBool]
        hits([lanes: '3'])*.item == [lanesList]
        hits(['kit.name': 'truseq'])*.item == [nestedKit]
        hits([lane: '9']) == []
    }

    def 'every condition must match'() {
        expect:
        hits([lane: '2', sample: 'B'])*.item == [laneString]
        hits([lane: '2', sample: 'C']) == []
    }

    def 'no condition is every item of the output, each hit carrying its collection'() {
        expect:
        hits([:]).size() == 7
        hits(null).size() == 7
        hits([:])*.collection.unique() == [aligned1]
        index.itemHitsByText(run1, 'stats', [:]) == [new ItemHit(stats1, laneInt)]
        index.itemHitsByText(run1, 'nothing', [:]) == []
    }

    def 'a truncated row is ignored, by its digest or by the long text'() {
        expect:
        hits([note: MetadataView.digestOf(LONG.getBytes('UTF-8'))]) == []
        hits([note: LONG]) == []
        hits([sample: 'F'])*.item == [longNote]
    }

    def 'the same item in another run is its own hit; listed twice in one collection, it is one hit'() {
        given:
        final Cid manifest2 = store.putDagCbor(Fixtures.runManifest(nf_run_hash: 'other', run_name: 'r2'))
        final Cid aligned2 = store.putDagCbor(Fixtures.outputCollection(manifest2, 'aligned',
            [[laneInt, ['aligned/A-1.bam']], [laneInt, ['aligned/A-2.bam']]]))
        final Cid run2 = store.putDagCbor(Fixtures.runCompletion(manifest2, [aligned2]))
        index.ingestRun(store, run2, 'lab')

        expect:
        index.itemHitsByText(run2, 'aligned', [lane: '2']) == [new ItemHit(aligned2, laneInt)]
        index.itemHitsByText(run2, 'aligned', [lane: '2'])[0].occurrence() == "cas://${aligned2}/${laneInt}".toString()
        hits([lane: '2'])*.collection == [aligned1, aligned1]
    }
}
```

Add to `CasExtensionTest.groovy`, next to the existing run-reference tests:

```groovy
    def "fromStore(run: 'latest') without a pipeline is refused by the shared resolver"() {
        given:
        alignedRun()

        when:
        ext.fromStore(run: 'latest', output: 'aligned')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains("run reference 'latest' needs a pipeline")
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.RunRefTest' --tests 'robsyme.cas.core.IndexItemHitsTest' --tests 'robsyme.cas.ext.CasExtensionTest'`
Expected: FAIL to compile (`RunRef`, `ItemHit` and `Index.itemHitsByText` do not exist).

- [ ] **Step 3: Implement `RunRef` and `ItemHit`**

```groovy
// src/main/groovy/robsyme/cas/core/RunRef.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The one run-reference resolver (decision 6 of the milestone 3 plan; ticket
 * 05 Q2), shared by {@code fromStore(run:)} and {@code nf-blocks:items --run}.
 * A reference is {@code latest} with a pipeline identity, a
 * {@code lid://<nextflow run hash>}, or a {@code cas://} Store URI naming a
 * RunCompletion or a RunManifest. The answer is the RunCompletion address.
 * The caller catches the index up first.
 */
@CompileStatic
final class RunRef {

    static final String LATEST = 'latest'
    static final String LID_PREFIX = 'lid://'
    static final String CAS_PREFIX = 'cas://'

    private RunRef() {}

    /**
     * @throws IllegalArgumentException the reference is malformed, or names a block that is not a run
     * @throws IllegalStateException the index or the composition cannot resolve it
     */
    static Cid resolve(Index index, BlockStore store, String ref, String pipeline) {
        if( !ref )
            throw new IllegalArgumentException("a run reference is needed: a cas:// Store URI, a lid://<hash>, or 'latest'")
        if( ref == LATEST ) {
            if( !pipeline )
                throw new IllegalArgumentException("run reference 'latest' needs a pipeline identity: pipeline: '<id>' in fromStore, --pipeline <id> for nf-blocks:items")
            return index.latestSuccessfulRun(pipeline)
                .orElseThrow { new IllegalStateException("run reference 'latest': no successful run of pipeline '${pipeline}' is recorded") }
        }
        if( ref.startsWith(LID_PREFIX) ) {
            final String hash = ref.substring(LID_PREFIX.length())
            if( !hash )
                throw new IllegalArgumentException("run reference '${ref}' has no run hash after lid://")
            return index.runByNextflowHash(hash)
                .orElseThrow { new IllegalStateException("run reference '${ref}': no run with nextflow run hash '${hash}' is recorded") }
        }
        if( ref.startsWith(CAS_PREFIX) )
            return fromStoreUri(index, store, ref)
        throw new IllegalArgumentException("unrecognised run reference '${ref}': expected a cas:// Store URI, a lid://<hash>, or 'latest'")
    }

    private static Cid fromStoreUri(Index index, BlockStore store, String ref) {
        Cid cid = null
        try {
            cid = StoreRef.parse(ref).cid
        }
        catch( IllegalArgumentException e ) {
            throw new IllegalArgumentException("run reference '${ref}' is not a Store URI: ${e.message}", e)
        }
        if( !store.has(cid) )
            throw new IllegalStateException("run reference '${ref}' names ${cid}, a block this composition does not hold")
        final String kind = kindOf(store, cid)
        if( kind == Records.RUN_COMPLETION )
            return cid
        if( kind == Records.RUN_MANIFEST )
            return index.runByManifest(cid)
                .orElseThrow { new IllegalStateException("run reference '${ref}' is a RunManifest with no RunCompletion; the run did not finish") }
        throw new IllegalArgumentException("run reference '${ref}' is ${kind ? "a ${kind}" : 'not a metadata'} block, not a run")
    }

    private static String kindOf(BlockStore store, Cid cid) {
        if( !cid.isDagCbor() )
            return null
        final InputStream input = store.open(cid)
        try {
            final Object decoded = DagCbor.decode(input.readAllBytes())
            return decoded instanceof Map ? Records.kindOf((Map) decoded) : null
        }
        finally {
            input.close()
        }
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/ItemHit.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/**
 * One item of one Output Collection, as nf-blocks:items finds it (decision 7
 * of the milestone 3 plan). Its occurrence is the Item Occurrence URI a
 * Selection request may list as a member (Put.groovy, entryOf).
 */
@CompileStatic
@EqualsAndHashCode
final class ItemHit implements Comparable<ItemHit> {

    final Cid collection
    final Cid item

    ItemHit(Cid collection, Cid item) {
        if( collection == null || item == null )
            throw new IllegalArgumentException('an item hit needs a collection and an item')
        this.collection = collection
        this.item = item
    }

    /** cas://<collection>/<item> */
    String occurrence() {
        return ItemOccurrence.PREFIX + collection + '/' + item
    }

    /** By item CID, then collection CID, the order Index.itemHitsByText answers in. */
    @Override
    int compareTo(ItemHit other) {
        final int byItem = item.compareTo(other.item)
        return byItem != 0 ? byItem : collection.compareTo(other.collection)
    }

    @Override
    String toString() { occurrence() }
}
```

- [ ] **Step 4: Implement `Index.itemHitsByText`**

In `Index.groovy`, after `SQL_COLLECTIONS_OF`. These constants are private: the page does not run this query, so `ExplorerQueriesTest` does not pin it.

```groovy
    // nf-blocks:items (decision 7 of the milestone 3 plan): conditions match
    // the value text of any type. DISTINCT, since a collection may list an item twice.
    private static final String SQL_ITEM_HITS_BASE =
        'SELECT DISTINCT ci.collection_cid, ci.item_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?'

    private static final String SQL_ITEM_HITS_TEXT =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.value = ?)'

    private static final String SQL_ITEM_HITS_ORDER = ' ORDER BY ci.item_cid, ci.collection_cid'
```

After `items(...)`:

```groovy
    /**
     * The items of one output of one run whose metadata view holds, for every
     * condition, a row with that path and that value text, of any type
     * (ticket 05 Q4: {@code lane=2} finds the int 2 and the string "2").
     * A truncated row never matches. Each hit carries its collection. Sorted
     * by item CID, then collection CID.
     */
    List<ItemHit> itemHitsByText(Cid completion, String outputName, Map<String, String> conditions) {
        final StringBuilder sql = new StringBuilder(SQL_ITEM_HITS_BASE)
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        final Map<String, String> all = conditions ?: Collections.<String, String> emptyMap()
        for( Map.Entry<String, String> condition : all.entrySet() ) {
            if( condition.value != null && condition.value.getBytes('UTF-8').length > MetadataView.VALUE_CAP_BYTES ) {
                // Stored as a digest with truncated = 1, which never matches.
                log.warn("the condition on '${condition.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes and cannot be matched")
                return []
            }
            sql.append(SQL_ITEM_HITS_TEXT)
            parameters.add(condition.key)
            parameters.add(condition.value)
        }
        sql.append(SQL_ITEM_HITS_ORDER)
        final List<ItemHit> hits = new ArrayList<ItemHit>()
        query(sql.toString(), parameters) { ResultSet rs ->
            hits.add(new ItemHit(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2))))
        }
        return hits
    }
```

A null condition value binds SQL `NULL`, and `a.value = NULL` is never true, so it matches nothing. The CLI never produces one.

- [ ] **Step 5: Make `CasExtension.resolveRun` delegate**

Tasks 2 and 3 edit `CasExtension.groovy` first, so the line numbers below (today's code) shift. The method is still named `resolveRun(Index, Map)`. Replace its body so it reads:

```groovy
    /** The RunCompletion address for the run reference in {@code opts.run} (the shared resolver, RunRef). */
    private Cid resolveRun(Index index, Map opts) {
        final String run = opts?.get('run') as String
        if( !run )
            throw new IllegalArgumentException("channel.fromStore needs a 'run' reference")
        return RunRef.resolve(index, cas.store, run, opts?.get('pipeline') as String)
    }
```

Add `import robsyme.cas.core.RunRef`. Then delete the constants `LID_PREFIX` and `LATEST` and the import `robsyme.cas.core.StoreRef`, which only the old body used. Check that nothing else still uses them:

Run: `grep -n 'LID_PREFIX\|LATEST\|StoreRef' src/main/groovy/robsyme/cas/ext/CasExtension.groovy`
Expected: no output. `CAS_PREFIX` stays, because `selectionCid` and `pathFor` use it.

- [ ] **Step 6: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.RunRefTest' --tests 'robsyme.cas.core.IndexItemHitsTest' --tests 'robsyme.cas.ext.*'`
Expected: PASS. That includes every existing `CasExtensionTest` run-reference test, which only asserts exception types.

Run: `./gradlew test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/RunRef.groovy src/main/groovy/robsyme/cas/core/ItemHit.groovy \
        src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/ext/CasExtension.groovy \
        src/test/groovy/robsyme/cas/core/RunRefTest.groovy src/test/groovy/robsyme/cas/core/IndexItemHitsTest.groovy \
        src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy
git commit -m "feat(core): RunRef, the one run-reference resolver, and Index.itemHitsByText

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `nextflow plugin nf-blocks:items`

Decision 7 and ticket 05 Q1 to Q4 and Q6. A fourth verb, read-only. It catches the plugin's own index up with `cas.openIndex()` and `catchUpIndex`, as `put` and `snapshot` do, and never reads or writes the Index Snapshot. Its `csv` and `json` output is `Samplesheet.of` over the matched items plus a leading `occurrence` column, so there is one column rule, in `Samplesheet`, for the page's download and for this verb.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Samplesheet.groovy` (the three-argument `of`, the `occurrence` column)
- Modify: `src/test/groovy/robsyme/cas/core/SamplesheetTest.groovy`
- Create: `src/main/groovy/robsyme/cas/cli/ItemsCommand.groovy`
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (`VERBS`, usage, dispatch)
- Create: `src/test/groovy/robsyme/cas/cli/ItemsCommandTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy`

**Interfaces:**
- Consumes: `RunRef.resolve`, `Index.itemHitsByText`, `Index.collectionsOf`, `ItemHit`, `Samplesheet.of`, `DagJson.encodeToString`, `Options.parse`, `UsageException`, `CasSession.openIndex/catchUpIndex/store`.
- Produces:
  - `static Samplesheet Samplesheet.of(BlockStore store, List<Cid> items, List<String> occurrences)` (see the INTERFACE NOTE).
  - `static int ItemsCommand.run(List<String> args, Map config, PrintStream out, PrintStream err)`, which throws `UsageException` for a usage error (exit 2 through `CasCommands`), as `ExploreCommand.run` does. Also `static ItemsCommand.Request parse(List<String> args)`, `static List<ItemHit> hits(Index, BlockStore, Request)` and `static String render(BlockStore, List<ItemHit>, String format)`.
  - `nextflow plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...] [--pipeline <id>] [--format csv|json|occurrences|selection]`. Output goes to stdout with exit 0. No match prints a note on stderr: exit 0 for `csv` (header only), `json` (`[]`) and `occurrences` (nothing), exit 1 for `selection` (nothing printed, since an empty Selection is refused). An unresolvable run or a missing output exits 1, and a usage error exits 2.
  - `CasCommands.VERBS == ['explore', 'items', 'put', 'snapshot']`.

The `selection` format is a complete `put` request. `Put.entryOf` (`Put.groovy:162-168`) takes an Item Occurrence string as a member and turns it into `Selection.item(o.item, [o.collection])`. `derived_from` is optional there (null means `[]`), and `asserted_by` comes from the server's config, never from the request. The page's `writer.selection` sends `{kind: 'Selection', members, derived_from: []}`, so `items` sends the same three keys. `DagJson` writes keys in UTF-8 order: `{"derived_from":[],"kind":"Selection","members":["cas://<collection>/<item>",...]}`. When one item occurs in two runs it is listed twice, once per collection, and `Selection`'s constructor merges the entries into one member whose `via` holds both collections.

- [ ] **Step 1: Write the failing tests**

Add to `SamplesheetTest.groovy`:

```groovy
    def 'with occurrences: a leading occurrence column, one per row, in CSV and JSON; without, nothing changes'() {
        given:
        final Cid a = item([[sample: 'A'], Fixtures.leaf('A.bam', bamA, 1L)])
        final String occ = "cas://${Fixtures.cidOf([kind: 'OutputCollection', n: 1])}/${a}".toString()

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a], [occ])

        then:
        sheet.columns == ['occurrence', 'sample', '1']
        sheet.csv().readLines() == ['occurrence,sample,1', "${occ},A,cas://${bamA}/A.bam".toString()]
        new JsonSlurper().parseText(sheet.json()) == [[occurrence: occ, sample: 'A', '1': "cas://${bamA}/A.bam".toString()]]
        Samplesheet.of(store, [a]).columns == ['sample', '1']
    }

    def 'with occurrences, a Meta Map key named occurrence is meta.occurrence and a file position so named is file.occurrence'() {
        given:
        final Cid t = item([[occurrence: 'first'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid r = item([sample: 'R', occurrence: Fixtures.leaf('R.bam', bamB, 1L)])
        final Cid coll = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
        final List<Cid> items = [t, r].sort { it.toString() }
        final List<String> occs = items.collect { Cid i -> "cas://${coll}/${i}".toString() }

        when:
        final Samplesheet sheet = Samplesheet.of(store, items, occs)
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(sheet.json())

        then:
        sheet.columns[0] == 'occurrence'
        sheet.columns as Set == ['occurrence', 'meta.occurrence', 'sample', '1', 'file.occurrence'] as Set
        rows.find { it['meta.occurrence'] == 'first' }.occurrence == "cas://${coll}/${t}".toString()
        rows.find { it.sample == 'R' }['file.occurrence'] == "cas://${bamB}/R.bam".toString()
        Samplesheet.of(store, [t]).columns == ['occurrence', '1']
    }

    def 'occurrences must pair with items one to one'() {
        when:
        Samplesheet.of(store, [item([[sample: 'A']])], [])

        then:
        thrown(IllegalArgumentException)
    }
```

```groovy
// src/test/groovy/robsyme/cas/cli/ItemsCommandTest.groovy
package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Samplesheet
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** Decision 7 of the milestone 3 plan, ticket 05: nf-blocks:items, read-only, four formats, several runs. */
class ItemsCommandTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    LocalBlockStore store
    long clock = 1_758_000_000_000L
    Cid itemA, itemB, itemC, bare, itemD
    Cid run1, coll1, run2, coll2

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        itemA = item([[sample: 'A', lane: 2L, note: 'tumour batch'], Fixtures.leaf('A.txt', Fixtures.contentCid('A'), 1L)])
        itemB = item([[sample: 'B', lane: '2', formula: 'x=y'], Fixtures.leaf('B.txt', Fixtures.contentCid('B'), 1L)])
        itemC = item([[sample: 'Zoë', lane: 1L], Fixtures.leaf('C.txt', Fixtures.contentCid('C'), 1L)])
        // Review Focus 1: a bare file published alone (no Meta Map), and one whose Meta Map holds only a list.
        bare = item(Fixtures.leaf('bare.txt', Fixtures.contentCid('bare'), 1L))
        itemD = item([[ids: [1L, 2L]], Fixtures.leaf('D.txt', Fixtures.contentCid('D'), 1L)])
        final List<Cid> first = logRun('hash1', 'r1', '2026-09-03T10:05:00.000Z', [itemA, itemB, itemC, bare])
        run1 = first[0]
        coll1 = first[1]
        final List<Cid> second = logRun('hash2', 'r2', '2026-09-04T10:05:00.000Z', [itemA, itemD])
        run2 = second[0]
        coll2 = second[1]
    }

    private Cid item(Object value) { store.putDagCbor(Fixtures.outputItem(value)) }

    /** One run of pipeline `p` with one output, `greetings`, logged so catch-up finds it. [completion, collection]. */
    private List<Cid> logRun(String hash, String runName, String finishedAt, List<Cid> items) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(nf_run_hash: hash, run_name: runName))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'greetings',
            items.collect { Cid i -> [i, ["greetings/${i}".toString()]] }))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [finished_at: finishedAt]))
        StoreLog.append(store, StoreLogKind.RUN, completion, clock += 1000)
        return [completion, collection]
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

    private int run(List<String> args) {
        return new CasCommands().run('items', args, config(), new PrintStream(out, true, 'UTF-8'), new PrintStream(err, true, 'UTF-8'))
    }

    private String stdout() { out.toString('UTF-8') }

    private static String occ(Cid collection, Cid item) { "cas://${collection}/${item}".toString() }

    /** The CSV as rows by header; these fixtures hold no comma or quote, so a plain split reads them. */
    private static List<Map<String, String>> table(String csv) {
        assert !csv.contains('"')
        final List<String> lines = csv.readLines()
        final List<String> header = lines[0].split(',', -1) as List<String>
        return lines.drop(1).collect { String line ->
            [header, line.split(',', -1) as List<String>].transpose().collectEntries() as Map<String, String>
        }
    }

    def 'csv is the samplesheet of the matched items with a leading occurrence column'() {
        when:
        final int status = run(['greetings', 'sample=A', '--run', 'lid://hash1'])
        final List<String> lines = stdout().readLines()

        then:
        status == 0
        // Meta Map columns in canonical DAG-CBOR key order: lane, note (4), sample (6).
        lines == ['occurrence,lane,note,sample,1', "${occ(coll1, itemA)},2,tumour batch,A,cas://${Fixtures.contentCid('A')}/A.txt".toString()]
        lines.collect { String l -> l.substring(l.indexOf(',') + 1) } == Samplesheet.of(store, [itemA]).csv().readLines()
    }

    def 'a condition matches as text across types, and conditions AND'() {
        expect:
        run(['greetings', 'lane=2', '--run', 'lid://hash1', '--format', 'occurrences']) == 0
        stdout().readLines() == [itemA, itemB].sort().collect { Cid i -> occ(coll1, i) }

        when:
        out.reset()
        run(['greetings', 'lane=2', 'sample=B', '--run', 'lid://hash1', '--format', 'occurrences'])

        then:
        stdout().readLines() == [occ(coll1, itemB)]
    }

    def 'a condition splits at the first =, and a value may hold spaces or non-ASCII (Review Focus 2)'() {
        expect:
        run(['greetings', condition, '--run', 'lid://hash1', '--format', 'occurrences']) == 0
        stdout().readLines() == [occ(coll1, expected.call(this) as Cid)]

        where:
        condition           | expected
        'formula=x=y'       | { ItemsCommandTest t -> t.itemB }
        'note=tumour batch' | { ItemsCommandTest t -> t.itemA }
        'sample=Zoë'        | { ItemsCommandTest t -> t.itemC }
    }

    def 'an item with no Meta Map still has its row: an occurrence and its file column (Review Focus 1)'() {
        when:
        final int status = run(['greetings', '--run', 'lid://hash1'])
        final List<Map<String, String>> rows = table(stdout())
        final Map<String, String> row = rows.find { it.occurrence == occ(coll1, bare) }

        then:
        status == 0
        rows.size() == 4
        row.file == "cas://${Fixtures.contentCid('bare')}/bare.txt".toString()
        row.sample == ''
        row['1'] == ''
        rows.find { it.occurrence == occ(coll1, itemA) }.file == ''
    }

    def 'json is the lossless samplesheet with an occurrence key; a list-only Meta Map keeps its list (Review Focus 1)'() {
        when:
        final int status = run(['greetings', '--run', 'lid://hash2', '--format', 'json'])
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(stdout())

        then:
        status == 0
        rows*.occurrence == [itemA, itemD].sort().collect { Cid i -> occ(coll2, i) }
        rows.find { it.occurrence == occ(coll2, itemD) } == [occurrence: occ(coll2, itemD), ids: [1, 2], '1': "cas://${Fixtures.contentCid('D')}/D.txt".toString()]
    }

    def 'several runs: the union, sorted by item then collection; latest takes --pipeline; a run given twice counts once'() {
        expect:
        run(['greetings', 'sample=A', '--run', 'lid://hash1,latest', '--pipeline', 'p', '--format', 'occurrences']) == 0
        stdout().readLines() == [coll1, coll2].sort().collect { Cid c -> occ(c, itemA) }

        when:
        out.reset()
        run(['greetings', 'sample=A', '--run', "lid://hash1,cas://${run1}".toString(), '--format', 'occurrences'])

        then:
        stdout().readLines() == [occ(coll1, itemA)]
    }

    def 'selection is a complete put request that put accepts, one member per item with every via'() {
        when:
        final int status = run(['greetings', 'sample=A', '--run', 'lid://hash1,lid://hash2', '--format', 'selection'])
        final String request = stdout()

        then:
        status == 0
        DagJson.decode(request.trim()) == [derived_from: [], kind: 'Selection', members: [coll1, coll2].sort().collect { Cid c -> occ(c, itemA) }]

        when:
        out.reset()
        final int put = new CasCommands().run('put', ['-'], config(), new PrintStream(out, true, 'UTF-8'), new PrintStream(err, true, 'UTF-8'),
            new ByteArrayInputStream(request.getBytes('UTF-8')))
        final Map written = (Map) DagJson.decode(stdout().trim())

        then:
        put == 0
        written.written == true
        written.block.members == [[item: [address: itemA, via: [coll1, coll2].sort()]]]
    }

    def 'items writes nothing: no Store Log entry, no block, no Index Snapshot'() {
        given:
        final int entries = StoreLog.read(store).size()
        final long blocks = Files.walk(tempDir.resolve('store')).count()

        when:
        final int status = run(['greetings', '--run', 'lid://hash1,lid://hash2', '--format', 'selection'])

        then:
        status == 0
        StoreLog.read(store).size() == entries
        Files.walk(tempDir.resolve('store')).count() == blocks
        !Files.exists(tempDir.resolve('store/index'))
    }

    def 'no match: a header, an empty list or nothing, exit 0; for selection nothing and exit 1'() {
        expect:
        run(['greetings', 'sample=nobody', '--run', 'lid://hash1', '--format', format]) == status
        stdout() == printed
        err.toString('UTF-8').contains("no item of output 'greetings' matches")

        where:
        format        | status | printed
        'csv'         | 0      | 'occurrence\n'
        'json'        | 0      | '[]\n'
        'occurrences' | 0      | ''
        'selection'   | 1      | ''
    }

    def 'a run without the output, or a reference the index cannot resolve, is a failure the verb reports'() {
        expect:
        run(args) == 1
        err.toString('UTF-8').contains(named)

        where:
        args                                  | named
        ['nothing', '--run', 'lid://hash1']   | "has no output 'nothing'; its outputs are greetings"
        ['greetings', '--run', 'lid://nope']  | "no run with nextflow run hash 'nope'"
    }

    def 'usage errors exit 2 and name the argument (Review Focus 2)'() {
        expect:
        run(args) == 2
        err.toString('UTF-8').contains(named)

        where:
        args                                                              | named
        []                                                                | 'output name'
        ['greetings']                                                     | '--run'
        ['greetings', 'sample', '--run', 'lid://hash1']                   | "'sample'"
        ['greetings', '=x', '--run', 'lid://hash1']                       | "'=x'"
        ['greetings', 'sample=A', 'sample=B', '--run', 'lid://hash1']     | "'sample'"
        ['sample=A', '--run', 'lid://hash1']                              | "'sample=A'"
        ['greetings', '--run', 'lid://hash1', '--format', 'table']        | "'table'"
        ['greetings', '--run', 'latest']                                  | 'pipeline'
        ['greetings', '--run', 'lid://hash1,,lid://hash2']                | 'empty run reference'
        ['greetings', '--run', 'bogus']                                   | "'bogus'"
        ['greetings', '--run', 'lid://hash1', '--pipeline', 'p']          | '--pipeline goes with --run latest'
        ['greetings', '--run', 'lid://hash1', '--port', '1']              | '--port'
    }
}
```

In `CasCommandsTest.groovy`, extend `'an unknown verb is a usage error listing the verbs'` with `err.toString().contains('items')`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SamplesheetTest' --tests 'robsyme.cas.cli.*'`
Expected: FAIL. `SamplesheetTest` does not compile (no three-argument `of`), and `ItemsCommandTest` exits 2 with "unknown command 'nf-blocks:items'".

- [ ] **Step 3: Add the occurrence column to `Samplesheet`**

In `Samplesheet.groovy`, replace the fields, the constructor, `of`, `csv()` and `json()` with the code below. `Row`, `GENERATOR` and everything under `// ---- plumbing` stay as they are. Also extend the class comment with: "With occurrences, a leading `occurrence` column (decision 7 of the milestone 3 plan)."

```groovy
    static final String OCCURRENCE = 'occurrence'

    final List<Row> rows
    final List<String> columns
    /** Null, or one Item Occurrence per row, written as the leading column. */
    private final List<String> occurrences
    /** Dotted path -> header, in column order. */
    private final Map<String, String> metaColumnNames
    /** Structural position -> header, in column order. */
    private final Map<String, String> fileColumnNames

    private Samplesheet(List<Row> rows, List<String> occurrences, Map<String, String> metaColumnNames, Map<String, String> fileColumnNames) {
        this.rows = rows
        this.occurrences = occurrences
        this.metaColumnNames = metaColumnNames
        this.fileColumnNames = fileColumnNames
        final List<String> header = new ArrayList<String>()
        if( occurrences != null )
            header.add(OCCURRENCE)
        header.addAll(metaColumnNames.values())
        header.addAll(fileColumnNames.values())
        this.columns = Collections.unmodifiableList(header)
    }

    static Samplesheet of(BlockStore store, List<Cid> items) {
        return of(store, items, null)
    }

    /**
     * The samplesheet with a leading {@code occurrence} column, {@code occurrences[i]}
     * for {@code items[i]} (nf-blocks:items, decision 7 of the milestone 3 plan).
     * A Meta Map column whose path is {@code occurrence} is then written
     * {@code meta.occurrence}, and a file position so named {@code file.occurrence},
     * the rule a file position named like a Meta Map column already follows.
     * {@code occurrences} null is the plain samplesheet.
     */
    static Samplesheet of(BlockStore store, List<Cid> items, List<String> occurrences) {
        if( occurrences != null && occurrences.size() != items.size() )
            throw new IllegalArgumentException("${items.size()} items but ${occurrences.size()} occurrences")
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
        final boolean leading = occurrences != null
        final Map<String, String> metaNames = new LinkedHashMap<String, String>()
        for( String column : metaColumns )
            metaNames.put(column, leading && column == OCCURRENCE ? "meta.${column}".toString() : column)
        // A position that is also a Meta Map column (or the occurrence column) is written file.<position>.
        final Map<String, String> names = new LinkedHashMap<String, String>()
        for( String position : positions )
            names.put(position, metaColumns.contains(position) || (leading && position == OCCURRENCE) ? "file.${position}".toString() : position)
        return new Samplesheet(rows, leading ? new ArrayList<String>(occurrences) : null, metaNames, names)
    }

    String csv() {
        final StringBuilder out = new StringBuilder()
        out.append(columns.collect { String c -> quote(c) }.join(',')).append('\n')
        for( int r = 0; r < rows.size(); r++ ) {
            final Row row = rows[r]
            final List<String> cells = new ArrayList<String>()
            if( occurrences != null )
                cells.add(quote(occurrences[r]))
            for( String column : metaColumnNames.keySet() )
                cells.add(quote(text(column, row.flat.get(column))))
            for( Map.Entry<String, String> file : fileColumnNames.entrySet() )
                cells.add(quote(row.files.get(file.key) ?: ''))
            out.append(cells.join(',')).append('\n')
        }
        return out.toString()
    }

    String json() {
        final List<Map<String, Object>> out = new ArrayList<Map<String, Object>>()
        for( int r = 0; r < rows.size(); r++ ) {
            final Row row = rows[r]
            final Map<String, Object> entry = new LinkedHashMap<String, Object>()
            if( occurrences != null )
                entry.put(OCCURRENCE, occurrences[r])
            for( Map.Entry<String, Object> m : row.meta.entrySet() )
                entry.put(occurrences != null && m.key == OCCURRENCE ? "meta.${OCCURRENCE}".toString() : m.key, m.value)
            for( Map.Entry<String, String> file : row.files.entrySet() )
                entry.put(fileColumnNames.get(file.key), file.value)
            out.add(entry)
        }
        // JsonOutput.prettyPrint re-lexes the text and re-escapes non-ASCII
        // characters regardless of GENERATOR's own options, undoing the fix
        // above; GENERATOR's own (compact) output is used as-is instead.
        return GENERATOR.toJson(out) + '\n'
    }
```

In JSON the rename applies to the top-level key `occurrence` whatever its value. A nested map there, `occurrence: [x: 1]`, flattens to the CSV column `occurrence.x`, which does not clash and keeps its name. The JSON key would still clash, so it becomes `meta.occurrence`.

- [ ] **Step 4: Implement `ItemsCommand`**

```groovy
// src/main/groovy/robsyme/cas/cli/ItemsCommand.groovy
package robsyme.cas.cli

import groovy.transform.CompileStatic
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Index
import robsyme.cas.core.ItemHit
import robsyme.cas.core.Records
import robsyme.cas.core.RunRef
import robsyme.cas.core.Samplesheet

/**
 * `nextflow plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...]
 * [--pipeline <id>] [--format csv|json|occurrences|selection]` (decision 7 of the
 * milestone 3 plan; ticket 05). Read-only: it catches the plugin's own index up
 * from every member's Store Log, as put and snapshot do, and writes nothing.
 * Conditions are positionals split at the first '='; several runs are one
 * comma-joined --run, because CmdPlugin keeps one value per flag.
 */
@CompileStatic
class ItemsCommand {

    static final Set<String> FLAGS = ['run', 'pipeline', 'format'] as Set
    static final List<String> FORMATS = ['csv', 'json', 'occurrences', 'selection']

    /** The arguments, checked; every usage error but a malformed run reference is found here. */
    @CompileStatic
    static final class Request {
        final String output
        final Map<String, String> conditions
        final List<String> runs
        final String pipeline
        final String format

        Request(String output, Map<String, String> conditions, List<String> runs, String pipeline, String format) {
            this.output = output
            this.conditions = conditions
            this.runs = runs
            this.pipeline = pipeline
            this.format = format
        }
    }

    static int run(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Request request = parse(args)
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            cas.catchUpIndex(index)
            final List<ItemHit> hits = hits(index, cas.store, request)
            if( hits.isEmpty() ) {
                err.println("nf-blocks:items: no item of output '${request.output}' matches " +
                    "${request.conditions ? request.conditions.collect { String k, String v -> "${k}=${v}" }.join(' ') : 'at all'} " +
                    "in ${request.runs.join(', ')}" + (request.format == 'selection' ? '; an empty Selection is refused, so nothing is printed' : ''))
                if( request.format == 'selection' )
                    return 1
            }
            out.print(render(cas.store, hits, request.format))
            out.flush()
            return 0
        }
        finally {
            index.close()
        }
    }

    static Request parse(List<String> args) {
        final Options options = Options.parse(args, FLAGS)
        final List<String> positionals = options.positionals
        if( positionals.isEmpty() )
            throw new UsageException('items takes an output name, then any <path>=<value> conditions')
        final String output = positionals[0]
        if( output.contains('=') )
            throw new UsageException("items takes the output name first, then conditions; got the condition '${output}' where the output name goes")
        final Map<String, String> conditions = new LinkedHashMap<String, String>()
        for( String arg : positionals.drop(1) ) {
            final int eq = arg.indexOf('=')
            if( eq < 0 )
                throw new UsageException("condition '${arg}' is not <path>=<value>")
            if( eq == 0 )
                throw new UsageException("condition '${arg}' has no path before the '='")
            final String path = arg.substring(0, eq)
            if( conditions.containsKey(path) )
                throw new UsageException("condition '${arg}': the path '${path}' is already given, and each path takes one value")
            conditions.put(path, arg.substring(eq + 1))
        }
        final String run = options.flag('run')
        if( !run )
            throw new UsageException('items needs --run <ref>[,<ref>...]: a lid://<hash>, a cas:// RunCompletion or RunManifest, or latest with --pipeline')
        final List<String> runs = Arrays.asList(run.split(',', -1)).collect { String r -> r.trim() }
        if( runs.any { String r -> !r } )
            throw new UsageException("--run '${run}' has an empty run reference")
        final String pipeline = options.flag('pipeline')
        if( pipeline != null && !runs.contains(RunRef.LATEST) )
            throw new UsageException('--pipeline goes with --run latest')
        final String format = options.flag('format') ?: 'csv'
        if( !FORMATS.contains(format) )
            throw new UsageException("--format is one of ${FORMATS.join(', ')}, got '${format}'")
        return new Request(output, conditions, runs, pipeline, format)
    }

    /** The union over every run, each (collection, item) once, sorted by item CID then collection CID. */
    static List<ItemHit> hits(Index index, BlockStore store, Request request) {
        final LinkedHashSet<Cid> completions = new LinkedHashSet<Cid>()
        for( String ref : request.runs ) {
            try {
                completions.add(RunRef.resolve(index, store, ref, request.pipeline))
            }
            catch( IllegalArgumentException e ) {
                throw new UsageException("--run: ${e.message}")
            }
        }
        final TreeSet<ItemHit> union = new TreeSet<ItemHit>()
        for( Cid completion : completions ) {
            final Map<String, Cid> outputs = index.collectionsOf(completion)
            if( !outputs.containsKey(request.output) )
                throw new IllegalStateException("run ${completion} has no output '${request.output}'; its outputs are ${outputs.keySet().join(', ') ?: 'none'}")
            union.addAll(index.itemHitsByText(completion, request.output, request.conditions))
        }
        return new ArrayList<ItemHit>(union)
    }

    static String render(BlockStore store, List<ItemHit> hits, String format) {
        final List<String> occurrences = hits.collect { ItemHit h -> h.occurrence() }
        if( format == 'occurrences' )
            return occurrences.collect { String o -> o + '\n' }.join('')
        if( format == 'selection' ) {
            // The request the page's writer.selection sends: asserted_by is the server's.
            final Map<String, Object> request = new LinkedHashMap<String, Object>()
            request.put('kind', Records.SELECTION)
            request.put('members', occurrences)
            request.put('derived_from', new ArrayList<Object>())
            return DagJson.encodeToString(request) + '\n'
        }
        final Samplesheet sheet = Samplesheet.of(store, hits.collect { ItemHit h -> h.item }, occurrences)
        return format == 'json' ? sheet.json() : sheet.csv()
    }
}
```

A `UsageException` thrown from `run` reaches `CasCommands.run`, which prints `nf-blocks:items: <message>` and exits 2. An `IllegalStateException` (a run the index does not hold, a missing output, an item block that has not arrived) reaches its `catch( Exception e )` and exits 1.

- [ ] **Step 5: Register the verb in `CasCommands`**

```groovy
    static final List<String> VERBS = ['explore', 'items', 'put', 'snapshot']
```

In `run`'s `switch`:

```groovy
                case 'items':
                    return ItemsCommand.run(args, config, out, err)
```

In `usage`, between the `explore` and `put` lines:

```groovy
            '  items <output> [<path>=<value> ...] --run <ref>[,<ref>...] [--pipeline <id>] [--format csv|json|occurrences|selection]\n' +
            '                         one output\'s items across runs, as a samplesheet, occurrences or a put request; read-only\n' +
```

- [ ] **Step 6: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.SamplesheetTest' --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.explore.ExploreWriteTest'`
Expected: PASS. `ExploreWriteTest` runs the page's samplesheet export through the two-argument `of`, which is unchanged.

Run: `./gradlew test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Samplesheet.groovy src/test/groovy/robsyme/cas/core/SamplesheetTest.groovy \
        src/main/groovy/robsyme/cas/cli/ItemsCommand.groovy src/main/groovy/robsyme/cas/cli/CasCommands.groovy \
        src/test/groovy/robsyme/cas/cli/ItemsCommandTest.groovy src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy
git commit -m "feat(cli): nf-blocks:items, one output's items across runs, read-only

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: `put --name <name>`

Decision 8 and ticket 05 Q5 to Q7. `--name` is for Selection requests only. The Selection is written, or found already written, and then a second request through the same `Put` builder writes a `set name` Claim. That Claim supersedes the `name_claims` the Selection's dry run reports, so it also resolves a conflicted name. No Claim is written when the Selection has exactly one current name Claim and its name equals `<name>`. The Claim request is the one the page sends (`web/src/write.js`, `claim(subject, 'set', 'name', name, supersedes)`): `{kind: 'Claim', subject, verb: 'set', attribute: 'name', value, supersedes, timestamp}`, with `timestamp` taken from the verb's clock as ISO-8601 UTC with milliseconds. The builder fills in `asserted_by`. This task changes the `put` line of the usage text. `DESIGN.md` and `README.md` are Task 12's.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (`clock` property, `put` with `--name`, usage line)
- Modify: `src/test/groovy/robsyme/cas/cli/CasCommandsPutTest.groovy`

**Interfaces:**
- Consumes: `Put.put(byte[], boolean)`, `Put.put(Object, boolean)`, `PutResult.address/names/nameClaims/body()`, `PutError.body()`, `Put.MAX_NAME_CHARS`, `Claim.SET`, `Claim.NAME`, `Records.SELECTION`, `Records.CLAIM`, `Index.isoMillis`, `DagJson.decode/encodeToString`.
- Produces:
  - `nextflow plugin nf-blocks:put <file|-> [--dry-run] [--name <name>]`. With `--name`, stdout gets the Selection's response body on one line and then the Claim's response body on a second line, exit 0. When the name is already current, stdout gets only the Selection's body, stderr gets a note, and the exit is 0. When the Claim is refused, stdout gets the Selection's body and then the error body, stderr gets `nf-blocks:put: saved <address>, naming failed: <message>`, and the exit is 1. With `--dry-run --name`, stdout gets the dry run's body and then the Claim request that would be sent (DAG-JSON), and nothing is written. `--name` on a request whose kind is not `Selection`, or a blank name, or a name over 256 characters is a usage error: exit 2, before anything is written.
  - `Closure<Long> CasCommands.clock`, a property defaulting to `System.currentTimeMillis()`, which stamps the name Claim.

- [ ] **Step 1: Write the failing tests**

Add to `CasCommandsPutTest.groovy`. The `run` helper gains a `CasCommands` parameter, and the file gains two imports:

```groovy
import robsyme.cas.core.Claim
import robsyme.cas.core.PutError
```

```groovy
    private int run(List<String> args, String stdin = '', CasCommands commands = new CasCommands()) {
        return commands.run('put', args, config(), new PrintStream(out, true), new PrintStream(err, true),
            new ByteArrayInputStream(stdin.getBytes('UTF-8')))
    }

    /** Each line of stdout, decoded; --name prints one body per request. */
    private List<Map> bodies() {
        return out.toString().readLines().findAll { String l -> l.trim() }.collect { String l -> (Map) DagJson.decode(l) }
    }

    private int logSize() { StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).size() }

    def '--name writes the Selection, then a set name Claim about it through the same builder'() {
        when:
        final int status = run(['-', '--name', 'greetings'], request())
        final List<Map> printed = bodies()
        final Map claim = (Map) printed[1].block

        then:
        status == 0
        printed.size() == 2
        printed[0].written == true
        printed[1].written == true
        claim.kind == 'Claim'
        claim.subject == printed[0].address
        claim.verb == Claim.SET
        claim.attribute == Claim.NAME
        claim.value == 'greetings'
        claim.supersedes == []
        claim.asserted_by == 'ada'
        claim.timestamp ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
        logSize() == 2
    }

    def 'a new --name on a Selection already written supersedes its current name Claim'() {
        given:
        run(['-', '--name', 'first'], request())
        final Cid first = (Cid) bodies()[1].address
        out.reset()

        when:
        final int status = run(['-', '--name', 'second'], request())
        final List<Map> printed = bodies()

        then:
        status == 0
        printed[0].written == false
        printed[1].block.value == 'second'
        printed[1].block.supersedes == [first]
        logSize() == 3
    }

    def 'the one current name already equal writes no Claim'() {
        given:
        run(['-', '--name', 'greetings'], request())
        out.reset()

        when:
        final int status = run(['-', '--name', ' greetings '], request())

        then:
        status == 0
        bodies().size() == 1
        bodies()[0].written == false
        err.toString().contains("already named 'greetings'")
        logSize() == 2
    }

    def '--dry-run --name prints the dry run and the Claim it would send, and writes nothing'() {
        when:
        final int status = run(['-', '--dry-run', 'true', '--name', 'greetings'], request())
        final List<Map> printed = bodies()

        then:
        status == 0
        printed.size() == 2
        printed[0].exists == false
        printed[1].subMap(['kind', 'subject', 'verb', 'attribute', 'value', 'supersedes']) ==
            [kind: 'Claim', subject: printed[0].address, verb: 'set', attribute: 'name', value: 'greetings', supersedes: []]
        printed[1].timestamp ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
        logSize() == 0
    }

    def 'a Claim the builder refuses is "saved, naming failed", exit 1, the Selection kept'() {
        when:
        // The verb's clock an hour behind the builder's: the builder refuses the Claim with clock_skew.
        final int status = run(['-', '--name', 'greetings'], request(), new CasCommands(clock: { -> System.currentTimeMillis() - 3_600_000L }))
        final List<Map> printed = bodies()

        then:
        status == 1
        printed[0].written == true
        printed[1].error == PutError.CLOCK_SKEW
        err.toString().contains("saved ${printed[0].address}, naming failed")
        logSize() == 1
    }

    def '--name is for a Selection and a non-blank name; a usage error writes nothing'() {
        given:
        final String claimRequest = '{"kind":"Claim","subject":{"/":"' + item + '"},"verb":"delete","attribute":null,"value":null,"supersedes":[],"timestamp":"2026-09-25T10:00:00.000Z"}'

        expect:
        run(['-', '--name', 'x'], claimRequest) == 2
        err.toString().contains('--name names a Selection')
        run(['-', '--name', '   '], request()) == 2
        run(['-', '--name', 'n' * 257], request()) == 2
        logSize() == 0
    }
```

The existing `'usage errors exit 2'` table stays as it is. Its `--port` row now reports `this verb takes --dry-run, --name`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.cli.CasCommandsPutTest'`
Expected: FAIL. The new tests exit 2 with "unknown option --name", and `new CasCommands(clock: ...)` fails with `MissingPropertyException`.

- [ ] **Step 3: Implement**

In `CasCommands.groovy`, add the imports:

```groovy
import robsyme.cas.core.Cid
import robsyme.cas.core.Claim
import robsyme.cas.core.DagJson
import robsyme.cas.core.PutResult
import robsyme.cas.core.Records
```

Add the property, just after `VERBS`:

```groovy
    /** The verb's clock, which stamps the name Claim of put --name; a test seam. */
    Closure<Long> clock = { -> System.currentTimeMillis() } as Closure<Long>
```

Change the `put` case:

```groovy
                case 'put':
                    return put(Options.parse(args, ['dry-run', 'name'] as Set), config, out, err, stdin, clock)
```

Replace the `put` line of `usage` with:

```groovy
            '  put <file|-> [--dry-run] [--name <name>]  build and write one Selection or Claim from DAG-JSON;\n' +
            '                         a member may be an Item Occurrence, cas://<collection>/<item>; --name then names the Selection:\n' +
            '                         nf-blocks:items ... --format selection | nextflow plugin nf-blocks:put /dev/stdin --name <name>\n' +
```

Replace `put` and add its helpers:

```groovy
    /**
     * Builds and writes one client-constructible block (spec section 9.2); the body goes to stdout either way.
     * With --name, the Selection is then named (decision 8 of the milestone 3 plan).
     */
    private static int put(Options options, Map config, PrintStream out, PrintStream err, InputStream stdin, Closure<Long> clock) {
        if( options.positionals.size() != 1 )
            throw new UsageException("put takes one file (or - for stdin), got ${options.positionals ?: 'none'}")
        final String dry = options.flag('dry-run')
        if( !(dry in [null, 'true', 'false']) )
            throw new UsageException("--dry-run is a flag, got '${dry}'")
        final String name = nameOption(options.flag('name'))
        final String source = options.positionals[0]
        final byte[] body = source == '-' ? readCapped(stdin) : readCapped(Files.newInputStream(Paths.get(source)))
        if( name != null ) {
            final String kind = requestKind(body)
            if( kind != null && kind != Records.SELECTION )
                throw new UsageException("--name names a Selection; this request is a ${kind}")
        }
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            final Put builder = cas.newPut(index)
            PutResult saved = null
            try {
                saved = builder.put(body, dry == 'true')
            }
            catch( PutError e ) {
                out.println(new String(e.body(), 'UTF-8'))
                return 1
            }
            out.println(new String(saved.body(), 'UTF-8'))
            return name == null ? 0 : nameSelection(builder, saved, body, name, dry == 'true', out, err, clock)
        }
        finally {
            index.close()
        }
    }

    /** --name's value, trimmed as the page trims a typed name; null when --name is absent. */
    private static String nameOption(String value) {
        if( value == null )
            return null
        final String name = value.trim()
        if( !name )
            throw new UsageException('--name needs a non-blank name')
        if( name.length() > Put.MAX_NAME_CHARS )
            throw new UsageException("--name is at most ${Put.MAX_NAME_CHARS} characters, got ${name.length()}")
        return name
    }

    /** The request's kind, or null when it is not a DAG-JSON map with a string kind (the builder then refuses it). */
    private static String requestKind(byte[] body) {
        Object request = null
        try {
            request = DagJson.decode(body)
        }
        catch( Exception e ) {
            return null
        }
        final Object kind = request instanceof Map ? ((Map) request).get('kind') : null
        return kind instanceof String ? (String) kind : null
    }

    /**
     * The set name Claim after a Selection (ticket 05 Q5), through the same
     * builder, superseding the current name Claims the Selection's dry run
     * reports; nothing when its one current name already equals {@code name}.
     * The request is the page's (web/src/write.js, rename).
     */
    private static int nameSelection(Put builder, PutResult saved, byte[] body, String name, boolean dryRun,
                                     PrintStream out, PrintStream err, Closure<Long> clock) {
        try {
            // A dry run already holds the current names; after a write, ask the builder for them.
            final PutResult state = dryRun ? saved : builder.put(body, true)
            if( state.names.size() == 1 && state.names[0] == name && state.nameClaims.size() == 1 ) {
                err.println("nf-blocks:put: ${saved.address} is already named '${name}'; no name Claim ${dryRun ? 'would be' : 'is'} written")
                return 0
            }
            final Map<String, Object> claim = new LinkedHashMap<String, Object>()
            claim.put('kind', Records.CLAIM)
            claim.put('subject', saved.address)
            claim.put('verb', Claim.SET)
            claim.put('attribute', Claim.NAME)
            claim.put('value', name)
            claim.put('supersedes', new ArrayList<Cid>(state.nameClaims))
            claim.put('timestamp', Index.isoMillis(clock.call()))
            if( dryRun ) {
                out.println(DagJson.encodeToString(claim))
                return 0
            }
            out.println(new String(builder.put((Object) claim, false).body(), 'UTF-8'))
            return 0
        }
        catch( PutError e ) {
            out.println(new String(e.body(), 'UTF-8'))
            err.println("nf-blocks:put: saved ${saved.address}, naming failed: ${e.message}")
            return 1
        }
    }
```

The `put` flow keeps today's shape when `--name` is absent: one body, exit 0 or 1. The Claim goes through `Put.put(Object, boolean)`, whose `claimDraft` checks the shape and whose `validateClaim` checks the clock (10 minutes) and that each superseded Claim is current in the writable member. A stale `name_claims`, left behind when someone else renamed the Selection between the dry run and the Claim, therefore surfaces as `stale_supersedes` and "saved, naming failed". Rerunning the same command then supersedes the newer Claim.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.cli.*'`
Expected: PASS, including the earlier `CasCommandsPutTest` cases and `ItemsCommandTest`'s pipe into `put`.

Run: `./gradlew test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/cli/CasCommands.groovy src/test/groovy/robsyme/cas/cli/CasCommandsPutTest.groovy
git commit -m "feat(cli): put --name names a Selection with a set name Claim through the same builder

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Meta Map pills and the preview fetcher

Two pure modules the rows of Task 9 are drawn from: `pairs.js` turns an item's metadata view into pairs, a label and pills, and `previews.js` fetches OutputItem blocks lazily, six at a time, the first hundred rows without a click. Both are tested in Node; only `pairsNode` touches the DOM, and it returns `null` before touching it when there is nothing to draw.

**Files:**
- Create: `web/src/pairs.js`, `web/src/previews.js`
- Modify: `web/src/config.js` (`PREVIEW_CAP`, `PREVIEW_CONCURRENCY`)
- Create: `web/test/pairs.test.mjs`, `web/test/previews.test.mjs`

**Interfaces:**
- Consumes: `attrRows` (`metadata.js`), `h` (`html.js`), `Explorer.item(collectionCid, itemCid) -> {view, leaves}`, `Explorer.items` (in the Review Focus 5 test), `Float` (`typed.js`).
- Produces:
  - `pairs.js`: `pairsOf(view) -> [{path, type, values, types}]`; `labelPaths(previews, n = 3) -> [path]`; `labelText(preview, paths) -> string`; `valueText(type, value) -> string`; `filterHref(target, path, type, value) -> string|null`; `pillsOf(pairs, target) -> [{path, key, values: [{text, type, cls, href, title}]}]`; `pairsNode(pairs, target, {size: 'row'|'page'}) -> Node|null`; `PAIRS_CSS`.
  - `previews.js`: `new Previews(ex, {cap, concurrency})` with `cap`, `ask(collectionCid, itemCid)`, `askFirst(rows)`, `get(itemCid) -> {pairs, files} | {error, message} | undefined`, `onChange(fn) -> unsubscribe`, `cancel()`, `stats -> {fetched, failed, inFlight, queued}`.
  - `config.js`: `PREVIEW_CAP = 100`, `PREVIEW_CONCURRENCY = 6`.

- [ ] **Step 1: Write the failing tests**

```js
// web/test/pairs.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Float } from '../src/typed.js'
import { filterHref, labelPaths, labelText, pairsNode, pairsOf, pillsOf, valueText } from '../src/pairs.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'

const leaf = (name) => ({ kind: 'Leaf', name, address: null, size: null, provider: null, reason: 'declined' })
const preview = (view, files = []) => ({ pairs: pairsOf(view), files })

test('pairsOf: one entry per path, sorted by path, a list\'s values together, floats typed as the index types them', () => {
  const pairs = pairsOf({ sample: 'A', lane: 1, depth: new Float(30), library: { kit: 'truseq' }, ids: ['x', 'y'],
    ok: true, none: null, reads: leaf('A.bam') })
  assert.deepEqual(pairs, [
    { path: 'depth', type: 'float', values: ['30.0'], types: ['float'] },
    { path: 'ids', type: 'string', values: ['x', 'y'], types: ['string', 'string'] },
    { path: 'lane', type: 'int', values: ['1'], types: ['int'] },
    { path: 'library.kit', type: 'string', values: ['truseq'], types: ['string'] },
    { path: 'none', type: 'null', values: [null], types: ['null'] },
    { path: 'ok', type: 'bool', values: ['true'], types: ['bool'] },
    { path: 'sample', type: 'string', values: ['A'], types: ['string'] },
  ])
})

test('pairsOf: a path whose values differ in type is mixed, each value keeping its own; a truncated value is left out', () => {
  assert.deepEqual(pairsOf({ tags: ['2', 2], long: 'a'.repeat(1025) }),
    [{ path: 'tags', type: 'mixed', values: ['2', '2'], types: ['string', 'int'] }])
})

test('an item with no Meta Map has no pairs and no pills, and its label is its file names (Review Focus 1)', () => {
  const bare = preview(null, ['A.bam', 'A.bam.bai'])
  assert.deepEqual(bare.pairs, [])
  assert.equal(pairsNode(bare.pairs, null), null)
  assert.deepEqual(labelPaths([bare]), [])
  assert.equal(labelText(bare, labelPaths([bare])), 'A.bam, A.bam.bai')
  assert.equal(labelText(preview(null), []), '')
})

test('an item whose Meta Map holds only lists keeps its pills, but its label is still its file names (Review Focus 1)', () => {
  const ids = preview({ ids: ['a', 'b', 'c'] }, ['ids.txt'])
  assert.equal(ids.pairs.length, 1)
  assert.deepEqual(labelPaths([ids]), [])
  assert.equal(labelText(ids, labelPaths([ids])), 'ids.txt')
})

test('the label: string paths first, then most distinct values, ties by path, list-valued paths skipped (ticket 10 Q3)', () => {
  const gate = ['A', 'B', 'C'].map(s => preview({ sample: s, lane: 1, library: { kit: 'truseq' }, ids: ['x', 'y'] }, [`${s}.bam`]))
  const paths = labelPaths(gate)
  assert.deepEqual(paths, ['sample', 'library.kit', 'lane'])
  assert.equal(labelText(gate[2], paths), 'C · truseq · 1')
  assert.deepEqual(labelPaths(gate, 1), ['sample'])
  const people = [['Femi', 'Adeyemi', 'Lagos', 'Africa'], ['Ana', 'Silva', 'Lisbon', 'Europe'], ['Bo', 'Chen', 'Lagos', 'Africa']]
    .map(([firstName, lastName, city, region]) => preview({ person: { firstName, lastName, location: { city, region }, age: 40 } }))
  const named = labelPaths(people)
  assert.deepEqual(named, ['person.firstName', 'person.lastName', 'person.location.city'])
  assert.equal(labelText(people[0], named), 'Femi · Adeyemi · Lagos')
  // A path that is a list in any loaded preview is not a label path, even where it is a single value.
  assert.deepEqual(labelPaths([preview({ tag: 'x' }), preview({ tag: ['x', 'y'] })]), [])
})

test('a string that reads as a number is shown quoted; null is shown as null', () => {
  assert.equal(valueText('string', '2'), '"2"')
  assert.equal(valueText('string', '-1.5e3'), '"-1.5e3"')
  assert.equal(valueText('string', '2a'), '2a')
  assert.equal(valueText('string', ''), '')
  assert.equal(valueText('int', '2'), '2')
  assert.equal(valueText('float', '1.0E10'), '1.0E10')
  assert.equal(valueText('null', null), 'null')
})

test('a pill: the last path segment as its key, a list\'s values in one pill, the value classed by type', () => {
  const [kit] = pillsOf(pairsOf({ library: { kit: 'truseq' } }), null)
  assert.deepEqual([kit.path, kit.key], ['library.kit', 'kit'])
  const [ids] = pillsOf(pairsOf({ ids: ['x', 'y'] }), null)
  assert.deepEqual(ids.values.map(v => v.text), ['x', 'y'])
  const classes = pillsOf(pairsOf({ a: 1, b: new Float(2.5), c: true, d: false, e: null, f: 's' }), null).map(p => p.values[0].cls)
  assert.deepEqual(classes, ['pv-num', 'pv-num', 'pv-true', 'pv-false', 'pv-null', 'pv-str'])
  const [off] = pillsOf(pairsOf({ ok: false }), null)
  assert.deepEqual(off.values[0], { text: 'false', type: 'bool', cls: 'pv-false', href: null, title: 'ok is false' })
})

test('a filter link adds its pair to the current conditions once, and needs a run and an output', () => {
  const target = { completion: 'run1', output: 'my reads', where: [['sample', 'string', 'A']] }
  const at = (where) => `#/items/run1/my%20reads?where=${encodeURIComponent(JSON.stringify(where))}`
  assert.equal(filterHref(target, 'lane', 'int', '1'), at([['sample', 'string', 'A'], ['lane', 'int', '1']]))
  assert.equal(filterHref(target, 'sample', 'string', 'A'), at([['sample', 'string', 'A']]))
  assert.equal(filterHref(null, 'lane', 'int', '1'), null)
  assert.equal(filterHref({ completion: null, output: 'x', where: [] }, 'lane', 'int', '1'), null)
  const [none] = pillsOf(pairsOf({ none: null }), { completion: 'r', output: 'o', where: [] })
  assert.deepEqual(none.values[0], { text: 'null', type: 'null', cls: 'pv-null',
    href: `#/items/r/o?where=${encodeURIComponent('[["none","null",null]]')}`, title: 'list the items of o in this run where none is null' })
})

test('the string "2" and the integer 2 draw differently, and each filter link finds only its own item (Review Focus 5)', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('run1', 'p', 'R1', 'succeeded', 0, '2026-09-01T00:00:00.000Z')",
    "INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run1', 'reads')",
    "INSERT INTO collection_item(collection_cid, item_cid) VALUES ('coll', 'itemStr'), ('coll', 'itemInt')",
    "INSERT INTO item_attr VALUES ('itemStr', 'n', 'string', '2', 0), ('itemInt', 'n', 'int', '2', 0)"])
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: async () => new Response('', { status: 404 }) }),
    listFn: async () => ({ names: [], readable: true }) })
  const target = { completion: 'run1', output: 'reads', where: [] }
  const [str] = pillsOf(pairsOf({ n: '2' }), target)
  const [int] = pillsOf(pairsOf({ n: 2 }), target)
  assert.deepEqual([str.key, str.values[0].text, str.values[0].cls], ['n', '"2"', 'pv-str'])
  assert.deepEqual([int.key, int.values[0].text, int.values[0].cls], ['n', '2', 'pv-num'])
  const whereOf = (href) => JSON.parse(decodeURIComponent(href.match(/^#\/items\/run1\/reads\?where=(.*)$/)[1]))
  assert.deepEqual(whereOf(str.values[0].href), [['n', 'string', '2']])
  assert.deepEqual(whereOf(int.values[0].href), [['n', 'int', '2']])
  assert.deepEqual((await ex.items('run1', 'reads', whereOf(str.values[0].href))).items, ['itemStr'])
  assert.deepEqual((await ex.items('run1', 'reads', whereOf(int.values[0].href))).items, ['itemInt'])
})
```

```js
// web/test/previews.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Previews } from '../src/previews.js'
import { BlockError } from '../src/blocks.js'

const tick = () => new Promise((resolve) => setImmediate(resolve))

/** An `ex` whose item() answers only when the test says so, recording the most it had open at once. */
function fakeEx() {
  const open = []
  const asked = []
  let most = 0
  const ex = {
    item(collectionCid, itemCid) {
      asked.push([collectionCid, itemCid])
      return new Promise((resolve, reject) => {
        open.push({ itemCid, resolve: () => resolve({ view: { sample: itemCid, lane: 1 }, leaves: [{ name: `${itemCid}.bam` }] }), reject })
        most = Math.max(most, open.length)
      })
    },
  }
  const settle = async (n = open.length, how = 'resolve') => {
    for (const o of open.splice(0, n)) {
      if (how === 'resolve') o.resolve()
      else o.reject(new BlockError('block_missing', o.itemCid, `block ${o.itemCid} is not in this member`))
    }
    await tick()
  }
  return { ex, asked, open, most: () => most, settle }
}

test('at most six previews are fetched at once; the rest wait their turn (ticket 03 Q3)', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  for (let i = 0; i < 10; i++) p.ask('coll', `i${i}`)
  assert.deepEqual(p.stats, { fetched: 0, failed: 0, inFlight: 6, queued: 4 })
  assert.equal(f.asked.length, 6)
  await f.settle(1)
  assert.deepEqual(p.stats, { fetched: 1, failed: 0, inFlight: 6, queued: 3 })
  while (f.open.length) await f.settle()
  assert.deepEqual(p.stats, { fetched: 10, failed: 0, inFlight: 0, queued: 0 })
  assert.equal(f.most(), 6)
  assert.deepEqual(p.get('i3'), {
    pairs: [{ path: 'lane', type: 'int', values: ['1'], types: ['int'] }, { path: 'sample', type: 'string', values: ['i3'], types: ['string'] }],
    files: ['i3.bam'] })
})

test('askFirst asks for the first hundred rows only; the rest wait for a click (ticket 03 Q3)', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const rows = Array.from({ length: 150 }, (_, i) => ({ collection: 'coll', item: `i${i}` }))
  assert.equal(p.cap, 100)
  assert.equal(p.askFirst(rows), 100)
  while (f.open.length) await f.settle()
  assert.equal(f.asked.length, 100)
  assert.equal(p.get('i99')?.files[0], 'i99.bam')
  assert.equal(p.get('i100'), undefined)
  p.ask('coll', 'i100')
  await f.settle()
  assert.equal(p.get('i100')?.files[0], 'i100.bam')
  assert.equal(new Previews(f.ex, { cap: 2 }).askFirst(rows), 2)
})

test('an item asked for twice, or again once it has arrived, is fetched once', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  p.ask('coll', 'i0')
  p.ask('coll', 'i0')
  p.ask(null, 'i0')
  await f.settle()
  p.ask('coll', 'i0')
  assert.deepEqual(f.asked, [['coll', 'i0']])
})

test('a preview that fails is recorded with its code and counted, and listeners hear of it', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const told = []
  p.onChange((itemCid) => told.push(itemCid))
  p.ask(null, 'gone')
  await f.settle(1, 'reject')
  assert.equal(p.get('gone').error, 'block_missing')
  assert.deepEqual(p.stats, { fetched: 0, failed: 1, inFlight: 0, queued: 0 })
  assert.deepEqual(told, ['gone'])
  assert.deepEqual(f.asked, [[null, 'gone']])
  const throwing = new Previews({ item: () => { throw new Error('boom') } })
  throwing.ask(null, 'x')
  await tick()
  assert.deepEqual(throwing.get('x'), { error: 'query_failed', message: 'boom' })
})

test('cancel drops the queue and keeps what is in flight; an unsubscribed listener hears nothing', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const told = []
  const off = p.onChange((itemCid) => told.push(itemCid))
  for (let i = 0; i < 8; i++) p.ask('coll', `i${i}`)
  p.cancel()
  assert.deepEqual(p.stats, { fetched: 0, failed: 0, inFlight: 6, queued: 0 })
  off()
  while (f.open.length) await f.settle()
  assert.equal(p.stats.fetched, 6)
  assert.deepEqual(told, [])
  assert.equal(p.get('i7'), undefined)
  p.ask('coll', 'i7')
  assert.equal(p.stats.inFlight, 1)
})
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd web && npm run gen && node --test --test-reporter=spec test/pairs.test.mjs test/previews.test.mjs`
Expected: FAIL, both files with `ERR_MODULE_NOT_FOUND` naming `web/src/pairs.js` and `web/src/previews.js`.

- [ ] **Step 3: Add the budget constants**

Append to `web/src/config.js`:

```js
/** Item rows whose previews load without a click (DESIGN.md §16 decision 10, ticket 03 Q3). */
export const PREVIEW_CAP = 100
/** OutputItem fetches for previews in flight at once (DESIGN.md §16 decision 10). */
export const PREVIEW_CONCURRENCY = 6
```

- [ ] **Step 4: Write `pairs.js`**

```js
// web/src/pairs.js
// A Meta Map drawn as `key | value` pills, and the label an item row leads
// with (DESIGN.md §16 decision 9, tickets 03 and 10). Pairs are the index's
// own item_attr rows (metadata.js attrRows), so a pill's filter link carries
// the type query 3 matches.
import { h } from './html.js'
import { attrRows } from './metadata.js'

const enc = encodeURIComponent
const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const DOT = ' · '

/** The view's item_attr rows grouped by path, sorted by path; truncated values are left out. */
export function pairsOf(view) {
  const out = new Map()
  for (const r of attrRows(view ?? null)) {
    if (r.truncated) continue
    const e = out.get(r.path) ?? { path: r.path, type: r.type, values: [], types: [] }
    e.values.push(r.value)
    e.types.push(r.type)
    if (e.type !== r.type) e.type = 'mixed'
    out.set(r.path, e)
  }
  return [...out.values()].sort((a, b) => byText(a.path, b.path))
}

/**
 * Up to `n` label paths over the previews loaded so far (ticket 10 Q3):
 * a path that is a list in any of them is skipped; string paths come before
 * the rest, then the most distinct values, ties by path.
 */
export function labelPaths(previews, n = 3) {
  const stats = new Map()
  for (const p of previews) {
    for (const e of p.pairs ?? []) {
      const s = stats.get(e.path) ?? { path: e.path, list: false, string: true, distinct: new Set() }
      if (e.values.length > 1) s.list = true
      if (e.type !== 'string') s.string = false
      s.distinct.add(JSON.stringify([e.type, e.values]))
      stats.set(e.path, s)
    }
  }
  return [...stats.values()].filter(s => !s.list)
    .sort((a, b) => (Number(b.string) - Number(a.string)) || (b.distinct.size - a.distinct.size) || byText(a.path, b.path))
    .slice(0, n).map(s => s.path)
}

/** The row's label: the preview's values at `paths`, or its file names when it has none of them. */
export function labelText(preview, paths) {
  const byPath = new Map((preview?.pairs ?? []).map(e => [e.path, e]))
  const values = paths.map(p => byPath.get(p)).filter(Boolean).map(e => e.values.map(v => v ?? 'null').join(', '))
  return values.length ? values.join(DOT) : (preview?.files ?? []).join(', ')
}

const NUMERIC = /^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/

/** A value as a pill shows it: a string that reads as a number is quoted, since the where form is typed (ticket 10 Q4). */
export const valueText = (type, value) => (type === 'null' ? 'null' : type === 'string' && NUMERIC.test(value) ? `"${value}"` : value)

const valueClass = (type, value) => (type === 'int' || type === 'float' ? 'pv-num'
  : type === 'bool' ? `pv-${value}` : type === 'null' ? 'pv-null' : 'pv-str')

/**
 * Query 3 over `target`'s run and output with one more condition (ticket 10
 * Q2): the pair is added to `target.where` unless it is already there. Null
 * when the run or the output is not known, so the pill is not a link.
 */
export function filterHref(target, path, type, value) {
  if (!target?.completion || !target.output) return null
  const kept = (target.where ?? []).filter(([p, t, v]) => !(p === path && t === type && v === value))
  const where = [...kept, [path, type, value]]
  return `#/items/${target.completion}/${enc(target.output)}?where=${enc(JSON.stringify(where))}`
}

/** What pairsNode draws, without a DOM. */
export function pillsOf(pairs, target) {
  return pairs.map(e => ({
    path: e.path,
    key: e.path.split('.').at(-1),
    values: e.values.map((value, i) => {
      const type = e.types[i]
      const text = valueText(type, value)
      const href = filterHref(target, e.path, type, value)
      return { text, type, cls: valueClass(type, value), href,
        title: href ? `list the items of ${target.output} in this run where ${e.path} is ${text}` : `${e.path} is ${text}` }
    }),
  }))
}

/** The pills, or null when there are no pairs (an item with no Meta Map shows none, not an empty box). */
export function pairsNode(pairs, target, { size = 'row' } = {}) {
  if (!pairs?.length) return null
  return h('span', { class: `pills pills-${size}` }, pillsOf(pairs, target).map(p => h('span', { class: 'pill', title: p.path },
    h('span', { class: 'pill-key' }, p.key),
    p.values.map(v => (v.href
      ? h('a', { class: `pill-val ${v.cls}`, href: v.href, title: v.title }, v.text)
      : h('span', { class: `pill-val ${v.cls}`, title: v.title }, v.text))))))
}

export const PAIRS_CSS = `
  .pills { display: inline-flex; flex-wrap: wrap; gap: 4px; }
  .pill { display: inline-flex; max-width: 100%; border: 1px solid var(--line); border-radius: 999px; overflow: hidden; font-size: 12px; }
  .pill-key { background: var(--pill-key); color: var(--muted); padding: 0 6px; }
  .pill-val { padding: 0 6px; border-left: 1px solid var(--line); color: inherit; text-decoration: none;
    max-width: 24rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  a.pill-val:hover { text-decoration: underline; }
  .pv-num { color: var(--num); font-variant-numeric: tabular-nums; } .pv-true { color: var(--true); }
  .pv-false { color: var(--false); } .pv-null { color: var(--muted); font-style: italic; }
  .pills-page .pill { font-size: 14px; } .pills-page .pill-key, .pills-page .pill-val { padding: 1px 8px; }
`
```

- [ ] **Step 5: Write `previews.js`**

```js
// web/src/previews.js
// Item previews for the page's item rows (DESIGN.md §16 decision 10, ticket
// 03 Q3): OutputItem blocks fetched lazily through Explorer.item, which reads
// them through BlockFetcher (hash-checked, cached), a few at a time. No
// snapshot query: a preview never costs a range read of the index.
import { PREVIEW_CAP, PREVIEW_CONCURRENCY } from './config.js'
import { pairsOf } from './pairs.js'

export class Previews {
  constructor(ex, { cap = PREVIEW_CAP, concurrency = PREVIEW_CONCURRENCY } = {}) {
    this.ex = ex
    this.cap = cap
    this.concurrency = concurrency
    this.done = new Map()
    this.waiting = new Set()
    this.queue = []
    this.listeners = new Set()
    this.inFlight = 0
    this.fetched = 0
    this.failed = 0
  }

  /** Queues one item's preview unless it has arrived or is on its way. `collectionCid` may be null (a member with no via). */
  ask(collectionCid, itemCid) {
    if (this.done.has(itemCid) || this.waiting.has(itemCid)) return
    this.waiting.add(itemCid)
    this.queue.push({ collectionCid, itemCid })
    this.pump()
  }

  /** Asks for the first `cap` of `rows` (`{collection, item}`) and says how many; the rest wait for "show details". */
  askFirst(rows) {
    const first = rows.slice(0, this.cap)
    for (const r of first) this.ask(r.collection, r.item)
    return first.length
  }

  get(itemCid) { return this.done.get(itemCid) }

  onChange(fn) {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  /** Drops what has not started, for a view that is no longer shown; fetches in flight finish and are kept. */
  cancel() {
    for (const q of this.queue) this.waiting.delete(q.itemCid)
    this.queue = []
  }

  get stats() { return { fetched: this.fetched, failed: this.failed, inFlight: this.inFlight, queued: this.queue.length } }

  pump() {
    while (this.inFlight < this.concurrency && this.queue.length) {
      const { collectionCid, itemCid } = this.queue.shift()
      this.inFlight++
      let answer
      try {
        answer = Promise.resolve(this.ex.item(collectionCid, itemCid))
      } catch (e) {
        answer = Promise.reject(e)
      }
      answer.then(
        (it) => { this.done.set(itemCid, { pairs: pairsOf(it.view), files: it.leaves.map(l => l.name).filter(Boolean) }); this.fetched++ },
        (e) => { this.done.set(itemCid, { error: e?.code ?? 'query_failed', message: e?.message ?? String(e) }); this.failed++ })
        .finally(() => {
          this.inFlight--
          this.waiting.delete(itemCid)
          for (const fn of this.listeners) {
            try { fn(itemCid) } catch { /* one row's redraw must not stop the queue */ }
          }
          this.pump()
        })
    }
  }
}
```

- [ ] **Step 6: Run the tests**

Run: `cd web && npm test`
Expected: PASS; the spec reporter's summary reads `fail 0`, with the new `pairs.test.mjs` and `previews.test.mjs` tests listed.

- [ ] **Step 7: Commit**

```bash
git add web/src/pairs.js web/src/previews.js web/src/config.js web/test/pairs.test.mjs web/test/previews.test.mjs
git commit -m "feat(web): Meta Map pills, row labels and a capped preview fetcher

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: The model: run labels, every item of a collection, a run's lineage hash

What Task 9's rows and Task 10's snippets read from the model. A collection is named by its run (`<run_name> / <output>`), "Add all" lists every item of a collection without fetching one OutputItem, and a run row carries `nf_run_hash` for its `lid://`. One new query, `collectionAllItems`, joins `ExplorerQueriesTest`'s no-scan guard.

**Files:**
- Modify: `web/src/queries.json` (`runByCompletion` gains `nf_run_hash`; new `collectionAllItems`)
- Modify: `web/src/model.js` (`runLabel`, `allItems`, `nf_run_hash` in `staleRun`'s row, typed `item().view`)
- Modify: `web/test/model.test.mjs`
- Modify: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy` (the new query in the no-scan table)

**Interfaces:**
- Consumes: `SQL.collectionByCid`, `Explorer.runRow`, `Explorer.stale`, `BlockFetcher.ofKind`, `typedDecode`.
- Produces (on `Explorer`):
  - `runRow(cid)` rows gain `nf_run_hash` (snapshot from `runByCompletion`, tail from the RunManifest).
  - `runLabel(collectionCid) -> Promise<{run_name, output, completion} | null>`: one lookup per collection (the same promise for repeated calls); null when neither the snapshot nor the tail knows the collection, and not cached, so a later tail refresh can find it.
  - `allItems(collectionCid) -> Promise<string[]>`: every item CID, the snapshot's in item CID order, a tail collection's in block order.
  - `item(collectionCid, itemCid).view`: the metadata view of the typed decoding.

- [ ] **Step 1: Extend the no-scan guard first**

In `ExplorerQueriesTest`'s `where:` table, after the `collectionItems` row, add:

```groovy
        'collectionAllItems'      | page.collectionAllItems
```

Run: `./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`
Expected: FAIL; the `collectionAllItems` iteration throws a `NullPointerException` in `plan` (the query does not exist yet).

- [ ] **Step 2: Add the query and the column**

In `web/src/queries.json`, replace `runByCompletion` and add `collectionAllItems` after `collectionItems`:

```json
  "runByCompletion": "SELECT completion_cid, manifest_cid, pipeline, run_name, nf_run_hash, status, possibly_incomplete, finished_at FROM run WHERE completion_cid = ?",
```

```json
  "collectionAllItems": "SELECT item_cid FROM collection_item WHERE collection_cid = ? ORDER BY item_cid",
```

`collectionAllItems` is `collectionItems` without its page limit; it searches `collection_item_collection` as that query does. `runsOfPipeline` is unchanged: no run list shows the hash.

Run: `./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`
Expected: PASS (`BUILD SUCCESSFUL`). If `collectionAllItems` plans a `SCAN`, fix the SQL, not the guard.

- [ ] **Step 3: Write the failing model tests**

Add to the imports of `web/test/model.test.mjs`:

```js
import { Float } from '../src/typed.js'
import { attrRows } from '../src/metadata.js'
```

and append:

```js
test('a run row carries its nf_run_hash, from the snapshot and from a tail run\'s RunManifest (ticket 07 Q4)', async () => {
  const { explorer, member } = await open()
  assert.equal((await explorer.runRow(member.runs.R1.completion)).nf_run_hash, 'hash-R1')
  assert.equal((await explorer.runRow(member.runs.R2.completion)).nf_run_hash, 'hash-R2')
})

test('runLabel names a collection by its run and output, from the snapshot or the tail, and null when neither knows it (decision 11)', async () => {
  const { explorer, member } = await open()
  assert.deepEqual(await explorer.runLabel(member.runs.R1.collection),
    { run_name: 'R1', output: 'aligned', completion: member.runs.R1.completion })
  assert.deepEqual(await explorer.runLabel(member.runs.R2.collection),
    { run_name: 'R2', output: 'aligned', completion: member.runs.R2.completion })
  assert.equal(await explorer.runLabel(rawCid('held elsewhere').toString()), null)
  assert.equal(await explorer.runLabel(null), null)
  assert.equal(explorer.runLabel(member.runs.R1.collection), explorer.runLabel(member.runs.R1.collection), 'one lookup per collection')
})

test('allItems lists every item of a collection past the page size without fetching an OutputItem (decision 12)', async () => {
  const explorer = await openBig()
  const all = await explorer.allItems('coll')
  assert.equal(all.length, 1200)
  assert.deepEqual([all[0], all[1199]], ['item0001', 'item1200'])
  assert.equal(explorer.blocks.fetches, 0)
  const { explorer: tail, member } = await open()
  assert.deepEqual(await tail.allItems(member.runs.R2.collection), [member.item.B, member.item.C].sort())
})

test('an item\'s view keeps its floats floats, so its pairs are typed as the index types them', async () => {
  const { explorer, member } = await open()
  const it = await explorer.item(member.runs.R1.collection, member.item.A)
  assert.ok(it.view.depth instanceof Float)
  assert.deepEqual(attrRows(it.view).map(r => [r.path, r.type, r.value]).sort(),
    [['depth', 'float', '1.5'], ['lane', 'int', '1'], ['sample', 'string', 'A']])
  assert.equal(it.leaves[0].name, 'A.bam')
})
```

Run: `cd web && npm run gen && node --test --test-reporter=spec test/model.test.mjs`
Expected: FAIL; the tail row has no `nf_run_hash` (`undefined !== 'hash-R2'`), `explorer.runLabel is not a function`, `explorer.allItems is not a function`, and `it.view.depth instanceof Float` is false.

- [ ] **Step 4: Implement**

In `web/src/model.js`, in the constructor add:

```js
    this.runLabels = new Map()
```

In `staleRun`, the row gains the hash:

```js
      const row = { completion_cid: entry.cid, manifest_cid: text(completion.run), pipeline: manifest.pipeline,
        run_name: manifest.run_name, nf_run_hash: manifest.nf_run_hash ?? null, status: completion.status,
        possibly_incomplete: completion.possibly_incomplete ? 1 : 0, finished_at: completion.finished_at, source: 'tail' }
```

Replace `item`:

```js
  /** An item's block. `view` is read from the typed decoding, so its pairs type floats as the index does (metadata.js). */
  async item(collectionCid, itemCid) {
    const block = await this.blocks.ofKind(itemCid, 'OutputItem')
    const { value } = block.value
    return { collection: collectionCid, cid: itemCid, value, view: metadataView(typedDecode(block.bytes).value), leaves: leavesOf(value) }
  }
```

After `collection`, add:

```js
  /**
   * Every item CID of a collection, for "Add all N to the tray" (DESIGN.md
   * §16 decision 12): the snapshot's rows, or a tail collection's block. No
   * OutputItem is fetched.
   */
  async allItems(collectionCid) {
    const [row] = await this.db.query(SQL.collectionByCid, [collectionCid])
    if (row) return (await this.db.query(SQL.collectionAllItems, [collectionCid])).map(r => r.item_cid)
    return (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value.items.filter(Boolean).map(text)
  }

  /**
   * The run a collection came from, named (DESIGN.md §16 decision 11): one
   * lookup per collection however many rows ask. A collection neither the
   * snapshot nor the tail knows (held in another member) is null, and is
   * asked again next time, since a tail refresh may find it.
   */
  runLabel(collectionCid) {
    if (!collectionCid) return Promise.resolve(null)
    if (!this.runLabels.has(collectionCid)) {
      this.runLabels.set(collectionCid, this.findRunLabel(collectionCid).then(
        (label) => { if (!label) this.runLabels.delete(collectionCid); return label },
        (e) => { this.runLabels.delete(collectionCid); throw e }))
    }
    return this.runLabels.get(collectionCid)
  }

  async findRunLabel(collectionCid) {
    const [row] = await this.db.query(SQL.collectionByCid, [collectionCid])
    let completion = row?.completion_cid ?? null
    let output = row?.output_name ?? null
    if (!row) {
      const stale = this.stale.find(s => s.completion?.collections.some(c => text(c) === collectionCid))
      if (!stale) return null
      completion = stale.cid
      try {
        output = (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value.name
      } catch (e) {
        if (!(e instanceof BlockError)) throw e
        return null
      }
    }
    if (!completion) return null
    const run = await this.runRow(completion)
    return { run_name: run?.run_name ?? null, output, completion }
  }
```

`typedDecode`, `metadataView`, `leavesOf` and `BlockError` are already imported.

- [ ] **Step 5: Run the tests**

Run: `cd web && npm test && cd .. && ./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`
Expected: PASS; `fail 0` from the spec reporter, the milestone 1 and 2 model tests included, then `BUILD SUCCESSFUL`.

- [ ] **Step 6: Commit**

```bash
git add web/src/queries.json web/src/model.js web/test/model.test.mjs src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
git commit -m "feat(web): run labels, every item of a collection, and a run's nf_run_hash in the model

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: One item row everywhere items are listed

Query results, collection pages, the compose tray and Selection members all draw the same row (decision 9): a checkbox where picking applies, the label, file-name chips, the action at the right, the Meta Map pills beneath, the via line where there is one, the shortened CID last. Query picks keep their collection as `via` (decision 12), "Add all N to the tray" adds the whole query or collection, and the item page shows its Meta Map as larger pills and "produced by" as run labels (decision 11). Every selector the Gate reads keeps its meaning: `[data-item-result]` on each query result, `[data-pick]` still the first pick button on an item page (the rows add none there), `[data-member]` with `data-kind` and `data-held`, `[data-tray-entry]`, `[data-producer]` (the content page is unchanged).

**Files:**
- Modify: `web/src/views.js` (row helpers, `collection`, `item`, `items`, `selection`, `compose`)
- Modify: `web/src/tray.js` (`addMany`)
- Modify: `web/src/app.js` (inject `PAIRS_CSS`)
- Modify: `web/src/index.html` (colour tokens and row styles)
- Modify: `web/test/views.test.mjs`, `web/test/tray.test.mjs`

**Interfaces:**
- Consumes: `Previews` (Task 7), `pairsOf`, `labelPaths`, `labelText`, `pairsNode`, `PAIRS_CSS` (Task 7), `Explorer.runLabel`, `Explorer.allItems` (Task 8), `Explorer.item`, `Explorer.items`, `Explorer.collection`, `Explorer.selection`, `Explorer.held`, `pickButton`, `ctx.tray`, `ctx.trayChanged`, `ctx.rerender`.
- Produces:
  - `tray.js`: `Tray.addMany(picks)`; `add(pick)` is `addMany([pick])`.
  - `views.js`: `itemRows(ex, ctx, {items, total, collectionCid, completion, output, where, previews, results}) -> Node`; `itemRow(ex, ctx, {address, via, previews, target, index, attrs, lead, action, viaLabel, noVia, perVia}) -> Node`; `pickAll(ex, tray, {items, collectionCid}) -> Promise<number>`; `runLabelText(label, collectionCid) -> string`.
  - DOM: `[data-pick-all]` (button, `data-via` its collection, `data-count` N), `[data-preview-for="<item cid>"]` (the pills container of a row); `[data-pick]`'s `data-via` is filled on query results.

- [ ] **Step 1: Write the failing tests**

Add to `web/test/tray.test.mjs`:

```js
test('addMany merges vias as add does and writes storage once', () => {
  const writes = []
  const storage = { getItem: () => null, setItem: (k, v) => writes.push([k, v]) }
  const t = new Tray(storage)
  t.addMany([{ address: I1, via: [C1] }, { address: I2 }, { address: I1, via: [C2] }])
  assert.equal(t.size, 2)
  assert.deepEqual(t.entries().find(e => e.address === I1).via, [C1, C2].sort())
  assert.equal(writes.length, 1)
})
```

Replace the header comment and imports of `web/test/views.test.mjs`, and append the new tests:

```js
// views.js needs a DOM (`document`) to build its nodes, which this Node test
// runner does not have, so only its pure helpers are unit-tested here (the
// page minors of ticket 11, "Add all" and run labels); the rendering itself
// is covered by the Gate's browser tier.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { copyOutcome, copyText, pickAll, runLabelText, undoNote } from '../src/views.js'
import { Tray } from '../src/tray.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
```

```js
function countingStorage() {
  const m = new Map()
  const s = { writes: 0, getItem: k => m.get(k) ?? null, setItem: (k, v) => { s.writes++; m.set(k, String(v)) }, removeItem: k => m.delete(k) }
  return s
}

test('Add all on a 15,000-item collection puts every item in the tray with its collection, fetches no block, and survives a reload (Review Focus 4)', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('run1', 'big', 'R1', 'succeeded', 0, '2026-09-01T00:00:00.000Z')",
    "INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run1', 'aligned')",
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 15000)
     INSERT INTO collection_item(collection_cid, item_cid) SELECT 'coll', printf('item%05d', i) FROM n`])
  const asked = []
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: async (url) => { asked.push(String(url)); return new Response('', { status: 404 }) } }),
    listFn: async () => ({ names: [], readable: true }) })
  ex.item = () => { throw new Error('Add all must not fetch a preview') }
  const storage = countingStorage()
  const tray = new Tray(storage)
  assert.equal(await pickAll(ex, tray, { items: null, collectionCid: 'coll' }), 15000)
  assert.equal(tray.size, 15000)
  assert.deepEqual(asked, [])
  assert.equal(storage.writes, 1, 'one write to storage, not one per item')
  const reloaded = new Tray(storage)
  assert.equal(reloaded.size, 15000)
  assert.deepEqual(reloaded.entries()[14999], { address: 'item15000', kind: 'item', via: ['coll'] })
})

test('Add all on query results adds the results it is given, each with the run\'s collection, without listing the collection', async () => {
  const ex = { allItems: async () => { throw new Error('query results already are every item') } }
  const tray = new Tray(null)
  assert.equal(await pickAll(ex, tray, { items: ['i2', 'i1'], collectionCid: 'coll' }), 2)
  assert.deepEqual(tray.entries(), [{ address: 'i1', kind: 'item', via: ['coll'] }, { address: 'i2', kind: 'item', via: ['coll'] }])
  const bare = new Tray(null)
  await pickAll(ex, bare, { items: ['i1'], collectionCid: null })
  assert.deepEqual(bare.entries()[0].via, [])
})

test('a run is named `<run_name> / <output>`, or shown by its collection when this member does not know it (decision 11)', () => {
  assert.equal(runLabelText({ run_name: 'cold', output: 'aligned', completion: 'c' }, 'coll'), 'cold / aligned')
  assert.equal(runLabelText({ run_name: null, output: 'aligned', completion: 'c' }, 'coll'), 'unnamed run / aligned')
  assert.equal(runLabelText(null, 'coll'), 'coll')
})
```

The three existing tests (`copyText`, `copyOutcome`, `undoNote`) stay as they are.

Run: `cd web && npm run gen && node --test --test-reporter=spec test/views.test.mjs test/tray.test.mjs`
Expected: FAIL; `views.test.mjs` with `SyntaxError: The requested module '../src/views.js' does not provide an export named 'pickAll'`, `tray.test.mjs` with `t.addMany is not a function`.

- [ ] **Step 2: `Tray.addMany`**

In `web/src/tray.js`, replace `add`:

```js
  add(pick) { this.addMany([pick]) }

  /** Many picks with one write to storage: "Add all" may add a whole collection (DESIGN.md §16 decision 12). */
  addMany(picks) {
    for (const { address, via = [], kind = 'item' } of picks) {
      const seen = this.items.get(address) ?? { address, kind, via: new Set() }
      for (const v of via) seen.via.add(v)
      this.items.set(address, seen)
    }
    this.persist()
  }
```

- [ ] **Step 3: The row helpers in `views.js`**

Change the imports at the top of `web/src/views.js`:

```js
import { h, link, cid } from './html.js'
import { saveChoice } from './save-choice.js'
import { saveSequence, retryRestore } from './save-flow.js'
import { Previews } from './previews.js'
import { labelPaths, labelText, pairsNode, pairsOf } from './pairs.js'
```

Delete the `shown` helper (the item page's JSON cell was its only use). After `pickButton`, add:

```js
const shortCid = (text) => (text.length > 20 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text)

/** `<run_name> / <output>` (DESIGN.md §16 decision 11), or the collection CID when this member does not know its run. */
export const runLabelText = (label, collectionCid) => (label ? `${label.run_name ?? 'unnamed run'} / ${label.output}` : collectionCid)

/**
 * A collection shown by its run: the short CID until Explorer.runLabel
 * answers, then `<run_name> / <output>` linking to the run, the collection
 * CID in `title`. With `completionCid` the placeholder already links there.
 */
function runLabelNode(ex, collectionCid, completionCid = null) {
  const placeholder = h('code', { class: 'cid' }, shortCid(completionCid ?? collectionCid))
  const node = h('span', { title: collectionCid }, completionCid ? link(`#/run/${completionCid}`, placeholder) : placeholder)
  ex.runLabel(collectionCid).then((label) => {
    if (!label) return
    const text = runLabelText(label, collectionCid)
    node.replaceChildren(label.completion ? link(`#/run/${label.completion}`, text) : text)
  }, () => {})
  return node
}

/**
 * "Add all N to the tray" (DESIGN.md §16 decision 12): `items` when the
 * caller holds every one (query results), else every item of the collection
 * from the model; each picked with the collection as via, one storage
 * write, no preview fetched.
 */
export async function pickAll(ex, tray, { items = null, collectionCid }) {
  const all = items ?? await ex.allItems(collectionCid)
  const via = collectionCid ? [collectionCid] : []
  tray.addMany(all.map(address => ({ address, via, kind: 'item' })))
  return all.length
}

function markInTray(list, addresses) {
  const only = addresses ? new Set(addresses) : null
  for (const button of list.querySelectorAll('[data-pick]')) {
    if (only && !only.has(button.dataset.pick)) continue
    button.textContent = 'In the tray'
    button.disabled = true
  }
}

/** Calls `fn` once on the next frame however often it is asked in between. */
function nextFrame(fn) {
  let queued = false
  const later = globalThis.requestAnimationFrame ?? ((f) => setTimeout(f, 16))
  return () => {
    if (queued) return
    queued = true
    later(() => { queued = false; fn() })
  }
}

// Each row's parts, kept beside its node so a list can redraw it.
const rowOf = new WeakMap()

/**
 * One item row (DESIGN.md §16 decision 9): `lead` (a checkbox) and the label,
 * file-name chips, `action` at the right; the Meta Map pills beneath in
 * `[data-preview-for]`; a via line when `viaLabel` is set, each via named by
 * its run with `perVia(via)` after it; the short CID last, linking to the
 * item page. The preview is drawn by `watchRows`; a row at `index` past
 * `previews.cap` waits for "show details".
 */
export function itemRow(ex, ctx, { address, via = [], previews, target = null, index = 0, attrs = {}, lead = null, action = null,
  viaLabel = null, noVia = 'a query', perVia = () => null }) {
  const collection = via[0] ?? null
  const row = { address, collection, target, drawn: undefined,
    label: h('strong', { class: 'row-label' }), files: h('span', { class: 'row-files' }),
    pairs: h('div', { class: 'row-pairs', 'data-preview-for': address }) }
  const loading = () => h('span', { class: 'muted' }, 'loading...')
  row.label.append(index < previews.cap ? loading()
    : h('button', { type: 'button', onclick: () => { row.label.replaceChildren(loading()); previews.ask(collection, address) } }, 'show details'))
  const viaLine = viaLabel === null ? null : h('div', { class: 'row-via muted' }, `${viaLabel} `,
    via.length ? via.map((v, i) => [i ? '; ' : null, runLabelNode(ex, v), ' ', perVia(v)]) : [noVia, ' ', perVia('-')])
  const li = h('li', { class: 'row', ...attrs },
    h('div', { class: 'row-head' }, lead, row.label, row.files, h('span', { class: 'row-action' }, action)),
    row.pairs, viaLine,
    h('div', { class: 'row-cid' }, link(`#/item/${collection ?? '-'}/${address}`, h('code', { class: 'cid', title: address }, shortCid(address)))))
  rowOf.set(li, row)
  return li
}

function fillRow(row, preview, paths) {
  if (preview.error) {
    row.label.replaceChildren(h('span', { class: 'muted', title: preview.message ?? '' },
      preview.error === 'block_missing' ? 'not held in this member' : `no preview (${preview.error})`))
    return
  }
  row.label.replaceChildren(labelText(preview, paths) || h('span', { class: 'muted' }, 'no Meta Map'))
  row.files.replaceChildren(...preview.files.map(f => h('span', { class: 'chip' }, f)))
  const pills = pairsNode(preview.pairs, row.target, { size: 'row' })
  row.pairs.replaceChildren(...(pills ? [pills] : []))
}

/**
 * Asks for the first rows' previews and redraws rows as they arrive, one
 * frame at a time; the label paths are chosen across the list's loaded
 * previews, so every row is relabelled when they change. Once the list has
 * been shown and is gone (another route rendered), the queue is dropped.
 */
function watchRows(list, previews, lis) {
  const rows = lis.map(li => rowOf.get(li))
  previews.askFirst(rows.map(r => ({ collection: r.collection, item: r.address })))
  let shown = false
  let lastPaths = null
  const redraw = nextFrame(() => {
    if (list.isConnected) shown = true
    else if (shown) { off(); previews.cancel(); return }
    const paths = labelPaths(rows.map(r => previews.get(r.address)).filter(p => p?.pairs))
    const key = JSON.stringify(paths)
    const relabel = key !== lastPaths
    lastPaths = key
    for (const r of rows) {
      const p = previews.get(r.address)
      if (p === undefined || (p === r.drawn && !relabel)) continue
      r.drawn = p
      fillRow(r, p, paths)
    }
  })
  const off = previews.onChange(redraw)
  redraw()
}

/**
 * The labelled list for query results and collection pages: a checkbox per
 * row with "Add checked (k)", "Add all N to the tray" ([data-pick-all]) for
 * the whole query or collection, and each row's own Add. Every pick records
 * `collectionCid` as via (decision 12). Pills link to query 3 over
 * `completion` and `output` with `where` plus the pair.
 */
export function itemRows(ex, ctx, { items, total = items.length, collectionCid = null, completion = null, output = null, where = [],
  previews, results = false }) {
  const via = collectionCid ? [collectionCid] : []
  const target = { completion, output, where }
  const checked = new Set()
  const status = h('span', { class: 'muted' })
  const addChecked = h('button', { type: 'button', disabled: true }, 'Add checked (0)')
  const showChecked = () => {
    addChecked.textContent = `Add checked (${checked.size})`
    addChecked.disabled = checked.size === 0
  }
  const lis = items.map((address, index) => itemRow(ex, ctx, { address, via, previews, target, index,
    attrs: results ? { 'data-item-result': address } : {},
    lead: h('input', { type: 'checkbox', 'aria-label': 'check this item', onchange: (event) => {
      if (event.currentTarget.checked) checked.add(address)
      else checked.delete(address)
      showChecked()
    } }),
    action: pickButton(ctx, { address, via }) }))
  const list = h('ol', { class: 'rows' }, lis)
  addChecked.addEventListener('click', () => {
    const picked = [...checked]
    ctx.tray.addMany(picked.map(address => ({ address, via, kind: 'item' })))
    ctx.trayChanged()
    markInTray(list, picked)
    for (const box of list.querySelectorAll('input[type=checkbox]')) box.checked = false
    checked.clear()
    showChecked()
    status.textContent = `Added ${picked.length} to the tray.`
  })
  const addAll = h('button', { type: 'button', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': total, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    status.textContent = total > items.length ? `Reading all ${total} items...` : ''
    try {
      const n = await pickAll(ex, ctx.tray, { items: total === items.length ? items : null, collectionCid })
      ctx.trayChanged()
      markInTray(list, null)
      status.textContent = `Added ${n} to the tray.`
    } catch (e) {
      status.replaceChildren(errorNode(e))
    } finally {
      button.disabled = false
    }
  } }, `Add all ${total} to the tray`)
  watchRows(list, previews, lis)
  return h('div', { class: 'item-rows' }, h('p', { class: 'row-actions' }, addChecked, ' ', addAll, ' ', status), list)
}
```

- [ ] **Step 4: Query results and collection pages use `itemRows`**

Replace `collection`:

```js
export async function collection(ex, collectionCid, offset = 0, ctx) {
  const c = await ex.collection(collectionCid, { offset })
  return h('section', {},
    h('h1', {}, c.output), cid(collectionCid),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    pager(`#/collection/${collectionCid}`, c, 'items'),
    c.total === 0 ? h('p', { class: 'muted' }, 'This collection has no items.')
      : itemRows(ex, ctx, { items: c.items, total: c.total, collectionCid, completion: c.completion, output: c.output, where: [],
        previews: new Previews(ex) }))
}
```

In `items`, replace the returned section:

```js
  const { items: results, collection: collectionCid } = found
  return h('section', {},
    h('h1', {}, `Items of ${output}`), h('p', {}, 'In ', link(`#/run/${completionCid}`, 'this run'), ', where every condition below holds.'),
    whereForm(completionCid, output, where),
    h('p', {}, `${results.length} item${results.length === 1 ? '' : 's'}`),
    // Decision 12: a per-run query's pick records the run's Output Collection as via.
    results.length === 0 ? null : itemRows(ex, ctx, { items: results, collectionCid, completion: completionCid, output, where,
      previews: new Previews(ex), results: true }))
```

The items view asks no new snapshot query (the run and output are in the route), so Gate assertion A2's costs are unchanged; previews are block fetches.

- [ ] **Step 5: The item page's pills and run labels**

Replace `item`:

```js
export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  // Ticket 10 Q2: on the item page a value's pair is the query's only condition.
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const pills = pairsNode(pairsOf(it.view), target, { size: 'page' })
  return h('section', {},
    h('h1', {}, 'Item'),
    // `-` is an item reached with no collection (a Selection member picked by a query).
    h('p', {}, collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
    from ? h('p', { title: collectionCid }, 'From ', from.completion ? link(`#/run/${from.completion}`, runLabelText(from, collectionCid)) : runLabelText(from, collectionCid)) : null,
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
    h('h2', {}, 'Meta Map'),
    pills ? [pills, target ? h('p', { class: 'muted' }, 'Click a value to list the items of ', h('code', {}, target.output), ' in this run that share it.') : null]
      : h('p', { class: 'muted' }, 'none'),
    h('h2', {}, 'Files'),
    table(['name', 'size', 'content', 'produced by'], it.leaves.map(l => h('tr', {},
      h('td', {}, l.name ?? ''), h('td', {}, l.size ?? ''),
      h('td', {}, l.address ? link(`#/content/${l.address}`, cid(l.address.toString())) : h('span', { class: 'muted' }, l.reason)),
      h('td', {}, (producers.find(([leaf]) => leaf === l)?.[1] ?? []).map(p => h('div', {}, runLabelNode(ex, p.collection_cid, p.completion_cid))))))))
}
```

The page's own pick button is still the first `[data-pick]` on it, which tier B's steps click.

- [ ] **Step 6: Selection members and tray entries through `itemRow`**

In `selection`, replace the `members` construction and its `for` loop:

```js
  const previews = new Previews(ex)
  const copy = (text) => h('button', { type: 'button', onclick: async (event) => {
    event.currentTarget.textContent = await copyOutcome(navigator.clipboard, text)
  } }, 'Copy')
  // A Selection may have thousands of members: the list renders at once and
  // each row's "held in" fills in when its own lookup answers, so one slow or
  // failing member (a hash mismatch, say) never holds up the view or fails it
  // outright (final review finding 2). Spec section 7.1a: each via's Copy
  // copies the member as an Item Occurrence.
  const members = s.members.map((m, index) => {
    const held = h('span', { 'data-held-for': m.address, class: 'muted' }, '...')
    const node = m.kind === 'selection'
      ? h('li', { class: 'row', 'data-member': m.address, 'data-kind': m.kind },
        h('div', { class: 'row-head' }, h('strong', {}, 'Selection'), link(`#/selection/${m.address}`, cid(m.address)), h('span', { class: 'row-action' }, held)))
      : itemRow(ex, ctx, { address: m.address, via: m.via, previews, index, attrs: { 'data-member': m.address, 'data-kind': m.kind },
        action: held, viaLabel: 'via', noVia: 'no run (picked by a query across runs)', perVia: (v) => copy(copyText(m.address, v)) })
    return { m, held, node }
  })
  for (const { m, held, node } of members) {
    ex.held(m.kind, m.address).then(
      (state) => {
        node.dataset.held = state
        held.textContent = state === 'here' ? 'held in this member' : 'held in another member'
        held.classList.remove('muted')
      },
      (e) => {
        const error = errorNode(e)
        error.textContent = `unavailable (${error.textContent})`
        held.replaceChildren(error)
        held.classList.remove('muted')
      })
  }
  const memberList = h('ol', { class: 'rows' }, members.map(({ node }) => node))
  watchRows(memberList, previews, members.filter(({ m }) => m.kind === 'item').map(({ node }) => node))
```

and in the returned section replace `table(['member', 'kind', 'held in'], members.map(({ tr }) => tr)),` with `memberList,`.

In `compose`, before `return`, add:

```js
  const previews = new Previews(ex)
  const entryNodes = entries.map((e, index) => {
    const remove = h('button', { type: 'button', onclick: () => { ctx.tray.remove(e.address); ctx.trayChanged(); ctx.rerender() } }, 'Remove')
    return e.kind === 'selection'
      ? h('li', { class: 'row', 'data-tray-entry': e.address, 'data-kind': e.kind },
        h('div', { class: 'row-head' }, h('strong', {}, 'Selection'), link(`#/selection/${e.address}`, cid(e.address)), h('span', { class: 'row-action' }, remove)))
      : itemRow(ex, ctx, { address: e.address, via: e.via, previews, index, attrs: { 'data-tray-entry': e.address, 'data-kind': e.kind },
        action: remove, viaLabel: 'picked from', noVia: 'a query' })
  })
  const entryList = h('ol', { class: 'rows' }, entryNodes)
  watchRows(entryList, previews, entryNodes.filter((node, i) => entries[i].kind === 'item'))
```

and replace the tray table (`table(['member', 'kind', 'picked from', ''], entries.map(...))`) with `entryList`, keeping the empty-tray paragraph:

```js
    entries.length === 0 ? h('p', { class: 'muted' }, 'The tray is empty. Add items from a run, a collection, an item or a query.') : entryList,
```

- [ ] **Step 7: Styles**

In `web/src/index.html`, extend both `:root` token rules and add the row rules to the `<style>` block:

```css
  :root { color-scheme: light dark; --fg: #1d1d1f; --bg: #ffffff; --muted: #6e6e73; --line: #d2d2d7; --bad: #b3261e; --warn: #7a4f00;
    --num: #0550ae; --true: #116329; --false: #cf222e; --pill-key: #f6f8fa; --chip: #eef1ff; }
  @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --bg: #161617; --muted: #a1a1a6; --line: #3a3a3c; --bad: #ff8a80; --warn: #ffd180;
    --num: #79c0ff; --true: #56d364; --false: #ff7b72; --pill-key: #2c2c2e; --chip: #2a2f45; } }
  .rows { list-style: none; padding: 0; margin: .5rem 0; } .row { padding: .4rem 0; border-bottom: 1px solid var(--line); }
  .row-head { display: flex; flex-wrap: wrap; gap: .4rem; align-items: baseline; } .row-action { margin-left: auto; }
  .row-pairs, .row-via, .row-cid { margin: .15rem 0 0 1.6rem; } .row-pairs:empty { display: none; }
  .chip { background: var(--chip); border-radius: 4px; padding: 0 5px; font-size: 12px; }
  .row-actions { display: flex; flex-wrap: wrap; gap: .5rem; align-items: baseline; }
```

In `web/src/app.js`, import the pill styles and add them once at the start of `start()`:

```js
import { PAIRS_CSS } from './pairs.js'
```

```js
async function start() {
  document.head.append(h('style', {}, PAIRS_CSS))
  window.__nfBlocks = { verified: [] }
```

- [ ] **Step 8: Run the tests and build**

Run: `cd web && npm test && npm run build`
Expected: PASS (`fail 0`), then `dist/index.html <n> bytes`.

- [ ] **Step 9: Look at it, then run the Gate**

Serve a member with `make explore CONFIG=<a config naming the demo store>` and open the printed URL. On a run's "(filter by metadata)" page: each row shows a label ("Femi · Adeyemi · Lagos" on the demo store), file chips, "Add to the tray" at the right, pills beneath, the short CID last; clicking a pill value lists that run's items with the pair added to the conditions; "Add all N to the tray" sets the header's Tray count to N; `#/compose` names each entry's run under "picked from". The rows touch every selector tier B reads, so run it:

Run: `GATE_ROOT=<session scratchpad>/gate make gate`
Expected: lineage 11/0/6 or better, browser tier A 5/5, tier B 9/9. Rerun once before debugging an assertion 4 failure (`gate/README.md`).

- [ ] **Step 10: Commit**

```bash
git add web/src/views.js web/src/tray.js web/src/app.js web/src/index.html web/test/views.test.mjs web/test/tray.test.mjs
git commit -m "feat(web): one item row for query results, collections, the tray and Selection members

Query picks keep their collection as via; Add all N and checked rows go to
the tray; the item page draws its Meta Map as pills and names producing runs.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Consumer snippets on the Selection and run pages

A curator copies the two lines a downstream workflow needs instead of guessing the `fromStore` spelling (decision 13). The Selection page shows the include and `fromStore(selection: '<cid>')`; the run page shows the run's `lid://` under its name and, per output, `fromStore(run: 'lid://<hash>', output: '<name>')`. One "untyped | typed" toggle switches every snippet on the page and is remembered in `localStorage` under `nf-blocks.snippets`. Each call line is a `<code data-snippet="untyped"|"typed">`, which Task 11's Gate copies verbatim into its consumers.

**Files:**
- Create: `web/src/snippets.js`, `web/test/snippets.test.mjs`
- Modify: `web/src/views.js` (`run`, `selection`)
- Modify: `web/src/index.html` (snippet styles)

**Interfaces:**
- Consumes: `h` (`html.js`), `Explorer.run(...).row.nf_run_hash` (Task 8), `records: true` (Task 3, named in the typed text only).
- Produces:
  - `snippets.js`: `INCLUDE_LINE`, `SNIPPET_KEY = 'nf-blocks.snippets'`; `snippetLines({kind: 'selection', cid} | {kind: 'run', lid, output}, mode) -> {include, call}`; `snippetMode(storage?) -> 'untyped'|'typed'`; `setSnippetMode(mode, storage?)` (throws on another mode; storage that refuses is ignored); `snippetBlock(spec) -> Node`; `snippetToggle() -> Node`.
  - DOM: `[data-snippet="untyped"|"typed"]` on the `<code>` holding a call line; `[data-snippet-mode]` on the toggle's two buttons.

- [ ] **Step 1: Write the failing tests**

```js
// web/test/snippets.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { INCLUDE_LINE, SNIPPET_KEY, setSnippetMode, snippetLines, snippetMode } from '../src/snippets.js'

const SELECTION = 'bafyreib7lhx4cekrw4appaxcfb5r4yxk5amw2p6yrnnv2evci3niqplcbu'
const LID = 'lid://4f1b2c3d4e5f60718293a4b5c6d7e8f9'

function memoryStorage() {
  const m = new Map()
  return { getItem: k => m.get(k) ?? null, setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }
}

test('the Selection snippet, untyped and typed (decision 13)', () => {
  assert.equal(INCLUDE_LINE, "include { fromStore } from 'plugin/nf-blocks'")
  assert.deepEqual(snippetLines({ kind: 'selection', cid: SELECTION }, 'untyped'),
    { include: INCLUDE_LINE, call: `channel.fromStore(selection: '${SELECTION}')` })
  assert.deepEqual(snippetLines({ kind: 'selection', cid: SELECTION }, 'typed'),
    { include: INCLUDE_LINE, call: `nextflow.Channel.fromStore(selection: '${SELECTION}', records: true)` })
})

test('the run snippet names the run by its lid:// and the output by name (decision 13)', () => {
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'aligned' }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'aligned')`)
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'aligned' }, 'typed').call,
    `nextflow.Channel.fromStore(run: '${LID}', output: 'aligned', records: true)`)
})

test('a quote or a backslash in an output name stays inside its Groovy string', () => {
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: "it's\\here" }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'it\\'s\\\\here')`)
})

test('the mode is untyped unless the viewer chose typed', () => {
  assert.equal(SNIPPET_KEY, 'nf-blocks.snippets')
  assert.equal(snippetMode(memoryStorage()), 'untyped')
  assert.equal(snippetMode(null), 'untyped')
  const odd = memoryStorage()
  odd.setItem(SNIPPET_KEY, 'strict')
  assert.equal(snippetMode(odd), 'untyped')
})

test('choosing a mode remembers it under nf-blocks.snippets', () => {
  const storage = memoryStorage()
  setSnippetMode('typed', storage)
  assert.equal(storage.getItem(SNIPPET_KEY), 'typed')
  assert.equal(snippetMode(storage), 'typed')
  setSnippetMode('untyped', storage)
  assert.equal(snippetMode(storage), 'untyped')
})

test('storage that refuses every call costs only the memory, and another mode is refused', () => {
  const broken = { getItem: () => { throw new Error('denied') }, setItem: () => { throw new Error('denied') } }
  assert.equal(snippetMode(broken), 'untyped')
  assert.doesNotThrow(() => setSnippetMode('typed', broken))
  assert.throws(() => setSnippetMode('strict', memoryStorage()), /untyped or typed/)
})
```

Run: `cd web && npm run gen && node --test --test-reporter=spec test/snippets.test.mjs`
Expected: FAIL with `ERR_MODULE_NOT_FOUND` naming `web/src/snippets.js`.

- [ ] **Step 2: Write `snippets.js`**

```js
// web/src/snippets.js
// The consumer code the Selection and run pages offer (DESIGN.md §16
// decision 13, ticket 07 Q4): the include line and one fromStore call,
// untyped or typed. The mode is one choice for every snippet on the page,
// remembered per viewer in localStorage where the browser allows it. The
// Gate runs these call lines verbatim (decision 14).
import { h } from './html.js'

export const SNIPPET_KEY = 'nf-blocks.snippets'
export const INCLUDE_LINE = "include { fromStore } from 'plugin/nf-blocks'"
const MODES = ['untyped', 'typed']

function safeLocalStorage() {
  try { return globalThis.localStorage ?? null } catch { return null }
}

/** The remembered mode: 'typed' only when storage says so; anything else, or no storage, is 'untyped'. */
export function snippetMode(storage = safeLocalStorage()) {
  try {
    return storage?.getItem(SNIPPET_KEY) === 'typed' ? 'typed' : 'untyped'
  } catch {
    return 'untyped'
  }
}

// The mode chosen on this page, which holds even when storage refused it,
// and every snippet and toggle drawn, redrawn when the mode changes.
let chosen = null
const live = new Set()
const current = () => chosen ?? snippetMode()

/** Sets the mode for every snippet on the page and remembers it; storage that refuses loses only the memory. */
export function setSnippetMode(mode, storage = safeLocalStorage()) {
  if (!MODES.includes(mode)) throw new Error(`a snippet is untyped or typed, not ${mode}`)
  chosen = mode
  try {
    storage?.setItem(SNIPPET_KEY, mode)
  } catch {
    // Not remembered past this page.
  }
  for (const shown of [...live]) {
    if (shown.node.isConnected) shown.draw(mode)
    else live.delete(shown)
  }
}

const quote = (text) => `'${String(text).replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`

/** The two lines of a snippet: the include, and the call for `mode`. */
export function snippetLines(spec, mode) {
  const args = spec.kind === 'selection' ? `selection: ${quote(spec.cid)}` : `run: ${quote(spec.lid)}, output: ${quote(spec.output)}`
  return { include: INCLUDE_LINE, call: mode === 'typed' ? `nextflow.Channel.fromStore(${args}, records: true)` : `channel.fromStore(${args})` }
}

async function copied(text) {
  try {
    await globalThis.navigator.clipboard.writeText(text)
    return 'Copied'
  } catch {
    return 'Copy failed'
  }
}

/** A snippet with a Copy button; its call line is `<code data-snippet="untyped"|"typed">`. */
export function snippetBlock(spec) {
  const call = h('code', {})
  const copy = h('button', { type: 'button', onclick: async (event) => {
    const button = event.currentTarget
    button.textContent = await copied(`${INCLUDE_LINE}\n${call.textContent}`)
  } }, 'Copy')
  const node = h('div', { class: 'snippet' }, h('pre', {}, h('code', {}, INCLUDE_LINE), '\n', call), copy)
  const draw = (mode) => {
    call.dataset.snippet = mode
    call.textContent = snippetLines(spec, mode).call
    copy.textContent = 'Copy'
  }
  draw(current())
  live.add({ node, draw })
  return node
}

/** The one "untyped | typed" choice for every snippet on the page. */
export function snippetToggle() {
  const buttons = MODES.map(mode => h('button', { type: 'button', 'data-snippet-mode': mode, 'aria-pressed': String(current() === mode),
    title: mode === 'typed' ? 'For scripts with nextflow.enable.types: processes get records.' : 'For scripts without nextflow.enable.types.',
    onclick: () => setSnippetMode(mode) }, mode))
  const node = h('span', { class: 'snippet-toggle', role: 'group', 'aria-label': 'script kind' }, buttons[0], ' | ', buttons[1])
  live.add({ node, draw: (mode) => buttons.forEach(b => b.setAttribute('aria-pressed', String(b.dataset.snippetMode === mode))) })
  return node
}
```

Run: `cd web && npm run gen && node --test --test-reporter=spec test/snippets.test.mjs`
Expected: PASS, 6 tests.

- [ ] **Step 3: Show them on the run and Selection pages**

In `web/src/views.js`, add the import:

```js
import { snippetBlock, snippetToggle } from './snippets.js'
```

Replace `run`:

```js
export async function run(ex, completionCid) {
  const { row, completion, collections } = await ex.run(completionCid)
  const a = completion.anomalies
  // Decision 13: the run's lineage ID, which fromStore(run:) and nf-blocks:items --run accept.
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  return h('section', {},
    h('h1', {}, row.run_name ?? 'run'),
    lid ? h('p', {}, h('code', { title: 'this run\'s lineage ID' }, lid)) : null,
    cid(completionCid),
    h('dl', {},
      h('dt', {}, 'pipeline'), h('dd', {}, link(`#/pipeline/${enc(row.pipeline)}`, row.pipeline)),
      h('dt', {}, 'status'), h('dd', {}, completion.possibly_incomplete ? `${completion.status}, possibly incomplete` : completion.status),
      h('dt', {}, 'finished'), h('dd', {}, completion.finished_at),
      h('dt', {}, 'anomalies'), h('dd', {}, `unresolvable ${a.unresolvable}, unaddressed ${a.unaddressed}, declined ${a.declined}, never published ${a.never_published}`),
      completion.error ? [h('dt', {}, 'error'), h('dd', {}, completion.error)] : null),
    h('h2', {}, 'Outputs'),
    lid && collections.length ? h('p', {}, 'Read an output in a downstream workflow. Script kind: ', snippetToggle()) : null,
    h('ul', {}, collections.map(c => h('li', { 'data-collection': c.cid, 'data-output': c.output },
      link(`#/collection/${c.cid}`, c.output), ' ', link(`#/items/${completionCid}/${enc(c.output)}`, '(filter by metadata)'),
      lid ? snippetBlock({ kind: 'run', lid, output: c.output }) : null))))
}
```

In `selection`, replace the samplesheet paragraph at the end of the returned section with the paragraph followed by the snippet:

```js
    ctx.write.served ? h('p', {}, 'Samplesheet: ',
      h('a', { href: `api/samplesheet/${selectionCid}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
      h('a', { href: `api/samplesheet/${selectionCid}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null,
    h('h2', {}, 'Use it in a workflow'),
    h('p', {}, 'Read this Selection in a downstream workflow. Script kind: ', snippetToggle()),
    snippetBlock({ kind: 'selection', cid: selectionCid }))
```

In `web/src/index.html`, add to the `<style>` block:

```css
  .snippet { display: flex; gap: .5rem; align-items: flex-start; margin: .35rem 0; }
  .snippet pre { margin: 0; padding: .4rem .6rem; border: 1px solid var(--line); overflow-x: auto; flex: 1; }
  .snippet-toggle button[aria-pressed=true] { font-weight: 600; }
```

- [ ] **Step 4: Run the tests and build**

Run: `cd web && npm test && npm run build`
Expected: PASS (`fail 0`), then `dist/index.html <n> bytes`.

Open a run page through `make explore`: the `lid://` sits under the run's name, and each output has a two-line snippet. Choosing "typed" switches every snippet to `nextflow.Channel.fromStore(..., records: true)`, and a reload keeps it. A Selection page shows `channel.fromStore(selection: '<its cid>')` under "Use it in a workflow".

- [ ] **Step 5: Commit**

```bash
git add web/src/snippets.js web/src/views.js web/src/index.html web/test/snippets.test.mjs
git commit -m "feat(web): fromStore snippets on the Selection and run pages, untyped or typed

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Gate tier B, B17 (picks keep their collection) and B18 (the typed consumer)

Tickets 04 Q5, 07 Q6 and 09 Q5. Two assertions join tier B, over the same fixture: B17 picks item A from a per-run query and every item of a collection with "Add all", saves them, and checks that each member kept its collection; B18 runs a typed consumer (`nextflow.enable.types = true`, a record type, a record-typed process input) on the page's typed snippet and checks that no task saw a plain map. B9's consumer now takes its call line from the page's untyped snippet too, so a broken snippet fails the Gate.

**Files:**
- Modify: `gate/browser/drive.mjs` (extract picks, `[data-pick-all]` and `[data-snippet]`; actions `waitTray` and `snippetMode`)
- Modify: `gate/browser_b_assert.py` (steps `B.picks` and `B.snippets`, `substitute`, `snippet_call`, the `consumer` subcommand, B9 extended, B17, B18)
- Modify: `gate/test_browser_b_assert.py`
- Modify: `gate/selection/main.nf` (the call line marked `// @snippet`)
- Create: `gate/selection-typed/main.nf`, `gate/selection-typed/nextflow.config`
- Modify: `gate/browser/tier_b.sh`, `gate/gate.sh`, `gate/README.md`

**Interfaces:**
- Consumes: Task 1's unset-`outputDir` default (the typed consumer's config has no `outputDir`); Task 3's `records: true`; Task 9's `[data-pick]` with `data-via` on query results and `[data-pick-all]` (`data-via`, `data-count`); Task 10's `[data-snippet="untyped"|"typed"]` and `localStorage` key `nf-blocks.snippets`; `assert.Gate`, `assert.Run.items`, `assert._consumer_hashes`, `browser_b_assert._staged`, `verify_post`, `_members`, `_item`.
- Produces: `browser_b_assert.py consumer <GATE_ROOT> <untyped|typed> <src dir> <dest dir>` (exit 0, or 2 when the page showed no usable snippet); `browser_b_assert.substitute(template, call) -> str` and `snippet_call(path) -> str|None`; `tier_b.sh` prints `B8` to `B18` and `browser tier B: <n> PASS, <m> FAIL`.

- [ ] **Step 1: Write the failing unit tests**

In `gate/test_browser_b_assert.py`, change the module docstring's first line to `"""Browser tier B's checks (block explorer spec section 1.3, assertions 8-18)`. Add below `STAMP`:

```python
STATS = sorted([ITEMS["C"], dcid("stats A"), dcid("stats B")])   # every item of cold's stats; C is one of them
UNTYPED_TEMPLATE = ("include { fromStore } from 'plugin/nf-blocks'\n\nworkflow {\n"
                    "    ch_items = channel.fromStore(selection: params.selection)   // @snippet\n}\n")
TYPED_TEMPLATE = ("nextflow.enable.types = true\n\ninclude { fromStore } from 'plugin/nf-blocks'\n\nworkflow {\n"
                  "    ch_items = nextflow.Channel.fromStore(selection: params.selection, records: true)   // @snippet\n}\n")


def stats_member(cid):
    return {"item": {"address": link(cid), "via": [link(COLLS["stats"])]}}
```

In `World.__init__`, set `self.expected["stats_items"] = list(STATS)` right after `self.expected = {...}`, and after `self.build_shared()` add:

```python
        self.build_picks()
        self.untyped = "channel.fromStore(selection: '%s')" % self.s2
        self.typed = "nextflow.Channel.fromStore(selection: '%s', records: true)" % self.s2
        self.ran = {"selection": None, "selection-typed": None}   # None: main.nf runs the page's snippet
        self.store_typed = os.path.join(self.out, "store-typed")
        self.typed_hashes = {"%s.sha256" % f["name"]: f["sha256"] for f in FILES.values()}
        self.typed_exit = "0"
        self.typed_tasks = ["typed:%s" % f["name"] for f in FILES.values()]
        self.typed_log = ("Sep-27 10:00:00.000 [main] INFO  nextflow.Session - Session start\n"
                          "Sep-27 10:00:02.000 [Task submitter] INFO  nextflow.Session - [ab/cdef01] Submitted process > HASH (typed:A.bam)\n")
```

Add a method after `build_deleted`:

```python
    def build_picks(self):
        """B.picks: A from query 3 over cold's aligned, then Add all on cold's stats; saved as `picked`."""
        self.picked = self.compose("B.picks", [item_member("A", "aligned")] + [stats_member(s) for s in STATS], "picked")
        self.steps["B.picks"].pop("saved", None)
        self.steps["B.picks"]["extracts"].update(
            query=self.extract(picks=[{"address": ITEMS["A"], "kind": "item", "via": COLLS["aligned"]}],
                               pickAll=[{"via": COLLS["aligned"], "count": "1"}]),
            collection=self.extract(pickAll=[{"via": COLLS["stats"], "count": "3"}]),
            all=self.extract(tray="4"))
```

In `World.extract`, add `"picks": [], "pickAll": [], "snippets": {"untyped": None, "typed": None}` to `base`. Replace the part of `World.write` from `shutil.rmtree(self.store_out)` to the end of the method with:

```python
        self.steps["B.snippets"] = {"id": "B.snippets", "requests": [], "pageErrors": [], "consoleErrors": [],
                                    "extracts": {"untyped": self.extract(snippets={"untyped": self.untyped, "typed": None}),
                                                 "typed": self.extract(snippets={"untyped": None, "typed": self.typed})}}
        dump("observed.json", {"steps": list(self.steps.values())})
        self.hash_store(self.store_out, self.hashes)
        self.hash_store(self.store_typed, {"typed": self.typed_hashes})
        with open(os.path.join(self.out, "selection-typed.exit"), "w") as fh:
            fh.write(self.typed_exit + "\n")
        log = os.path.join(self.out, "selection-typed-nextflow.log")
        if self.typed_log is None:
            if os.path.exists(log):
                os.remove(log)
        else:
            with open(log, "w") as fh:
                fh.write(self.typed_log)
        for launch, template, call, tasks in (("selection", UNTYPED_TEMPLATE, self.untyped, self.tasks),
                                              ("selection-typed", TYPED_TEMPLATE, self.typed, self.typed_tasks)):
            os.makedirs(os.path.join(self.root, launch), exist_ok=True)
            with open(os.path.join(self.root, launch, "main.nf"), "w") as fh:
                fh.write(B.substitute(template, self.ran[launch] or call))
            work = os.path.join(self.root, launch, "work")
            shutil.rmtree(work, ignore_errors=True)
            for n, tag in enumerate(tasks):
                task = os.path.join(work, "%02x" % n, "task%d" % n)
                os.makedirs(task)
                with open(os.path.join(task, ".command.run"), "w") as fh:
                    fh.write("#!/bin/bash\n### ---\n### name: 'HASH (%s)'\n### outputs:\n" % tag)
        return self.root

    @staticmethod
    def hash_store(root, hashes):
        """A consumer's member: each published hashes/<source>/<name> pointer resolving to sha256sum output."""
        shutil.rmtree(root, ignore_errors=True)
        os.makedirs(root)
        for source, files in hashes.items():
            for name, digest in files.items():
                data = ("%s  %s\n" % (digest, name[:-len(".sha256")])).encode()
                cid = cas.cid_raw(data)
                path = os.path.join(root, "blocks", cid[-2:], cid)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as fh:
                    fh.write(data)
                pointer = os.path.join(root, "coords", "hashes", source, name)
                os.makedirs(os.path.dirname(pointer), exist_ok=True)
                with open(pointer, "w") as fh:
                    fh.write("cas://%s\n" % cid)
```

The `dump("observed.json", ...)` line earlier in `write` stays where it is; the second one above rewrites it with `B.snippets` added, so a test that edits `self.untyped` or `self.typed` before `results()` changes what the page showed.

In `CheckTest`, change `test_the_whole_world_passes` to expect `list(range(8, 19))`, and `test_check_prints_every_line_and_the_total` to loop `for n in range(8, 19)` and expect `"browser tier B: 11 PASS, 0 FAIL"`. Add after the B9 tests:

```python
    def test_b9_a_consumer_not_running_the_pages_snippet_fails(self):
        self.w.ran["selection"] = "channel.fromStore(selection: params.selection)"
        self.assertFail(9, "not the page's untyped snippet")

    def test_b9_a_snippet_naming_another_selection_fails(self):
        self.w.untyped = "channel.fromStore(selection: '%s')" % self.w.s1
        self.assertFail(9, "does not name S2")

    def test_b9_no_snippet_shown_fails(self):
        self.w.untyped = None
        self.w.ran["selection"] = "channel.fromStore(selection: params.selection)"
        self.assertFail(9, 'no [data-snippet="untyped"]')
```

and after the B16 tests:

```python
    # -- B17 --------------------------------------------------------------
    def test_b17_the_whole_world_passes(self):
        self.assertPass(17)

    def test_b17_a_query_pick_without_its_collection_fails(self):
        # Milestone 2's behaviour: an item chosen by a per-run query was picked with no via.
        self.w.steps["B.picks"]["extracts"]["query"]["picks"][0]["via"] = ""
        self.assertFail(17, "data-via")

    def test_b17_a_member_saved_without_its_collection_fails(self):
        step = self.w.steps["B.picks"]
        request = {"kind": "Selection", "members": [{"item": {"address": link(ITEMS["A"]), "via": []}}]
                   + [stats_member(s) for s in STATS], "derived_from": []}
        cid = self.w.put_block(dagjson.expected_selection(dagjson.loads(json.dumps(request)), "gate"))
        step["requests"][1].update(body=json.dumps(request), responseBody=self.w.response(cid))
        step["extracts"]["after"]["written"] = cid
        self.assertFail(17, "members")

    def test_b17_add_all_adding_only_part_of_the_collection_fails(self):
        step = self.w.steps["B.picks"]
        request = {"kind": "Selection", "members": [item_member("A", "aligned")] + [stats_member(s) for s in STATS[:2]],
                   "derived_from": []}
        cid = self.w.put_block(dagjson.expected_selection(dagjson.loads(json.dumps(request)), "gate"))
        step["requests"][1].update(body=json.dumps(request), responseBody=self.w.response(cid))
        step["extracts"]["after"]["written"] = cid
        step["extracts"]["all"]["tray"] = "3"
        status, message = self.status(17)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("members", message)
        self.assertIn("the tray holds '3'", message)

    def test_b17_a_pick_all_count_other_than_the_collections_fails(self):
        self.w.steps["B.picks"]["extracts"]["collection"]["pickAll"][0]["count"] = "2"
        self.assertFail(17, "data-count 3")

    def test_b17_no_pick_all_on_query_results_fails(self):
        self.w.steps["B.picks"]["extracts"]["query"]["pickAll"] = []
        self.assertFail(17, "B.picks query")

    # -- B18 --------------------------------------------------------------
    def test_b18_the_whole_world_passes(self):
        self.assertPass(18)

    def test_b18_an_invalid_argument_type_warning_fails(self):
        # What a typed input logs when fromStore hands it a LinkedHashMap (TaskProcessor.groovy:1880).
        self.w.typed_log += ("Sep-27 10:00:03.000 [Actor Thread 3] WARN  nextflow.processor.TaskProcessor - [HASH (typed:A.bam)] "
                             "invalid argument type at index 0 -- expected a Sample but got a LinkedHashMap\n")
        self.assertFail(18, "invalid argument type")

    def test_b18_a_typed_snippet_without_records_fails(self):
        self.w.typed = "nextflow.Channel.fromStore(selection: '%s')" % self.w.s2
        self.assertFail(18, "records: true")

    def test_b18_a_typed_snippet_through_the_channel_namespace_fails(self):
        self.w.typed = "channel.fromStore(selection: '%s', records: true)" % self.w.s2
        self.assertFail(18, "nextflow.Channel.fromStore")

    def test_b18_a_consumer_not_running_the_pages_snippet_fails(self):
        self.w.ran["selection-typed"] = "nextflow.Channel.fromStore(selection: params.selection, records: true)"
        self.assertFail(18, "not the page's typed snippet")

    def test_b18_a_failed_run_fails(self):
        self.w.typed_exit = "1"
        self.assertFail(18, "exit")

    def test_b18_no_log_fails(self):
        self.w.typed_log = None
        self.assertFail(18, "selection-typed-nextflow.log")

    def test_b18_a_wrong_digest_fails(self):
        self.w.typed_hashes["C.stats.sha256"] = "3" * 64
        self.assertFail(18, "staged")

    def test_b18_an_item_staged_twice_fails(self):
        self.w.typed_tasks.append("typed:B.bam")
        self.assertFail(18, "typed:B.bam")
```

Add a test class before `MemberWriteTest`:

```python
class SubstituteTest(unittest.TestCase):
    def test_the_marked_line_takes_the_call_and_keeps_its_indent_name_and_mark(self):
        text = B.substitute(UNTYPED_TEMPLATE, "  channel.fromStore(selection: 'bafyx')  ")
        self.assertIn("\n    ch_items = channel.fromStore(selection: 'bafyx')   // @snippet\n", text)
        self.assertNotIn("params.selection", text)
        path = os.path.join(tempfile.mkdtemp(), "main.nf")
        try:
            with open(path, "w") as fh:
                fh.write(text)
            self.assertEqual(B.snippet_call(path), "channel.fromStore(selection: 'bafyx')")
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_no_call_is_refused(self):
        for call in (None, "", "   "):
            with self.assertRaises(cas.GateError):
                B.substitute(UNTYPED_TEMPLATE, call)

    def test_a_call_of_more_than_one_line_is_refused(self):
        with self.assertRaisesRegex(cas.GateError, "one call line"):
            B.substitute(UNTYPED_TEMPLATE, "channel\n.fromStore(selection: 'x')")

    def test_a_template_without_exactly_one_mark_is_refused(self):
        with self.assertRaisesRegex(cas.GateError, "expected 1"):
            B.substitute("workflow {\n}\n", "channel.fromStore(selection: 'x')")
        twice = UNTYPED_TEMPLATE.replace("}\n", "    ch_other = channel.empty()   // @snippet\n}\n")
        with self.assertRaisesRegex(cas.GateError, "expected 1"):
            B.substitute(twice, "channel.fromStore(selection: 'x')")

    def test_a_marked_line_that_is_not_an_assignment_is_refused(self):
        with self.assertRaisesRegex(cas.GateError, "not `<name> = <call>`"):
            B.substitute("workflow {\n    channel.empty()   // @snippet\n}\n", "channel.fromStore(selection: 'x')")
```

Run: `cd gate && python3 -m unittest test_browser_b_assert`
Expected: FAIL (`AttributeError: module 'browser_b_assert' has no attribute 'substitute'`, and B17/B18 missing from the results).

- [ ] **Step 2: Extend `drive.mjs`**

In the header comment, replace

```js
// Tier B's steps add `actions` (hash, click, fill, waitWrite, extract), may
```

with

```js
// Tier B's steps add `actions` (hash, click, fill, waitWrite, waitTray,
// snippetMode, extract), may
```

In `EXTRACT_B`, replace

```js
  copyLabel: document.getElementById('compose-copy')?.textContent ?? null,
})
```

with

```js
  copyLabel: document.getElementById('compose-copy')?.textContent ?? null,
  // Milestone 3 (DESIGN.md §15): picks carry their collection, "Add all" names its own, and the consumer snippets.
  picks: [...document.querySelectorAll('[data-pick]')].map((e) => ({ address: e.dataset.pick, kind: e.dataset.kind ?? null,
    via: e.dataset.via ?? null })),
  pickAll: [...document.querySelectorAll('[data-pick-all]')].map((e) => ({ via: e.dataset.via ?? null, count: e.dataset.count ?? null })),
  snippets: Object.fromEntries(['untyped', 'typed'].map((m) => [m, document.querySelector(`[data-snippet="${m}"]`)?.textContent ?? null])),
})
```

In `act`, replace

```js
  } else if (action.extract) {
    record.extracts[action.extract] = await page.evaluate(EXTRACT_B)
```

with

```js
  } else if (action.waitTray !== undefined) {
    // "Add all" resolves the collection's items before the tray changes.
    await page.waitForFunction((n) => document.getElementById('tray')?.dataset.count === String(n), action.waitTray, { timeout: 60_000 })
  } else if (action.snippetMode) {
    // The untyped | typed choice lives in localStorage (DESIGN.md §15, [data-snippet]); set it and reload.
    await page.evaluate((m) => localStorage.setItem('nf-blocks.snippets', m), action.snippetMode)
    await page.reload()
    await waitRender(page, 0)
  } else if (action.extract) {
    record.extracts[action.extract] = await page.evaluate(EXTRACT_B)
```

- [ ] **Step 3: Add the steps, the substitution and the checks to `browser_b_assert.py`**

Docstring: replace `(block explorer spec section 1.3, assertions 8-16):` with `(block explorer spec section 1.3, assertions 8-18):`, and replace

```python
    python3 gate/browser_b_assert.py check <GATE_ROOT>
```

with

```python
    python3 gate/browser_b_assert.py consumer <GATE_ROOT> <untyped|typed> <src dir> <dest dir>
    python3 gate/browser_b_assert.py check <GATE_ROOT>
```

and, in the prose, replace `samplesheet export. check recomputes` with `samplesheet export. consumer copies a consumer pipeline with its marked call line replaced by the page's snippet, verbatim. check recomputes`.

Imports: add `import urllib.parse` after `import urllib.error`.

After `_log`, add:

```python
def _where(key, value):
    """The page's query 3 filter (DESIGN.md §15), URL-encoded for the location hash, as browser_assert.where."""
    return "?where=" + urllib.parse.quote(json.dumps([[key, "string", value]], separators=(",", ":")), safe="")


SNIPPET_MARK = "// @snippet"


def substitute(template, call):
    """`template` with its one line ending in SNIPPET_MARK re-assigned to `call`, the page's [data-snippet] text, verbatim."""
    if call is None or not call.strip():
        raise cas.GateError("the page showed no snippet to run")
    call = call.strip()
    if "\n" in call or "\r" in call:
        raise cas.GateError("the snippet is %d lines, expected one call line: %r" % (len(call.splitlines()), call))
    lines = template.split("\n")
    marked = [i for i, line in enumerate(lines) if line.rstrip().endswith(SNIPPET_MARK)]
    if len(marked) != 1:
        raise cas.GateError("the consumer template has %d lines ending in %r, expected 1" % (len(marked), SNIPPET_MARK))
    line = lines[marked[0]]
    indent = line[:len(line) - len(line.lstrip())]
    name, sep, _rest = line.strip().partition("=")
    if not sep or not name.strip():
        raise cas.GateError("the marked line %r is not `<name> = <call>`" % line.strip())
    lines[marked[0]] = "%s%s = %s   %s" % (indent, name.strip(), call, SNIPPET_MARK)
    return "\n".join(lines)


def snippet_call(path):
    """The call a consumer's main.nf ran: the right-hand side of its marked line, without the mark; None without one."""
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.rstrip().endswith(SNIPPET_MARK):
                return line.strip()[:-len(SNIPPET_MARK)].rstrip().partition("=")[2].strip()
    return None
```

In `prepare`, after `os.makedirs(os.path.join(out, "store-out"))`, add `os.makedirs(os.path.join(out, "store-typed"))`. After `a, b, c = item(...)`, add:

```python
    stats_items = sorted(cid for cid, _b in cold.items(gate, "stats"))
    if c not in stats_items or len(stats_items) < 2:
        raise cas.GateError("cold's stats holds %r; B17's Add all needs C and at least one more item" % stats_items)
```

In `expected`, add `"stats_items": stats_items,` after the `"files"` entry. Append two steps to `steps`, after `B.deleted`:

```python
        # B17: a per-run query pick keeps its collection; Add all adds every item of a collection with it.
        {"id": "B.picks", "server": "explore", "path": "", "query": q,
         "hash": "#/items/%s/aligned%s" % (cold.completion_cid, _where("sample", "A")),
         "actions": [{"extract": "query"}, {"click": '[data-pick="%s"]' % a},
                     {"hash": "#/collection/%s" % coll["stats"]}, {"extract": "collection"},
                     {"click": '[data-pick-all][data-via="%s"]' % coll["stats"]}, {"waitTray": 1 + len(stats_items)},
                     {"extract": "all"}, {"hash": "#/compose"}, {"fill": ["#compose-name", "picked"]},
                     {"click": "#compose-save"}, {"waitWrite": True}, {"extract": "after"}]},
        # B9 and B18 run what the Selection view shows for S2, untyped and typed.
        {"id": "B.snippets", "server": "explore", "path": "", "query": q, "hash": "#/selection/{S2}",
         "actions": [{"snippetMode": "untyped"}, {"extract": "untyped"}, {"snippetMode": "typed"}, {"extract": "typed"}]},
```

After `probe`, add:

```python
# --------------------------------------------------------------------------
# consumer (explore has stopped)
# --------------------------------------------------------------------------

def consumer(root, mode, src, dest):
    """Copy the consumer pipeline in `src` to `dest`, its marked line re-assigned to the page's [data-snippet=<mode>] text."""
    out = os.path.join(root, "browser-b")
    extracts = (_observed(out).get("B.snippets") or {}).get("extracts") or {}
    call = ((extracts.get(mode) or {}).get("snippets") or {}).get(mode)
    try:
        with open(os.path.join(src, "main.nf"), encoding="utf-8") as fh:
            text = substitute(fh.read(), call)
    except cas.GateError as exc:
        sys.stderr.write("browser tier B: no %s consumer: %s (see browser-b/drive.log)\n" % (mode, exc))
        return 2
    os.makedirs(dest, exist_ok=True)
    with open(os.path.join(dest, "main.nf"), "w", encoding="utf-8") as fh:
        fh.write(text)
    shutil.copy(os.path.join(src, "nextflow.config"), os.path.join(dest, "nextflow.config"))
    print("browser tier B: the %s consumer calls %s" % (mode, call.strip()))
    return 0
```

Change `_staged` to take the launch directory:

```python
def _staged(root, source, launch="selection"):
    """Sorted '<source>:<file name>' tags of every HASH task the pipeline in `launch` ran for `source`, one per task.
    Published hashes are keyed by file name, so a file staged twice shows only here (each task's .command.run header)."""
    out = []
    for dirpath, _dirnames, filenames in os.walk(os.path.join(root, launch, "work")):
```

(the rest of its body unchanged). In `evaluate`, change its docstring to `"""[(status, number, title, message)] for assertions 8-18."""`, and replace `once_each` and `pipeline_hashes` with:

```python
    def once_each(source, launch="selection"):
        want = sorted("%s:%s" % (source, files[k]["name"]) for k in files)
        got = _staged(root, source, launch)
        return [] if got == want else ["the pipeline hashed %s, expected each file once: %s" % (got, want)]
```

```python
    def pipeline_hashes(launch="selection", store_out="store-out"):
        exit_path = os.path.join(out, launch + ".exit")
        status = "<none>"
        if os.path.isfile(exit_path):
            with open(exit_path) as fh:
                status = fh.read().strip()
        if status != "0":
            raise cas.GateError("the %s pipeline's exit status is %s (see browser-b/%s.log)" % (launch, status, launch))
        return A._consumer_hashes(SimpleNamespace(store_out=cas.Store(os.path.join(out, store_out))))

    def ran_snippet(mode, launch):
        """Problems with the call `launch`'s main.nf ran, and the page's text: it must be [data-snippet=<mode>], naming S2."""
        s2 = _saved(observed, "B.second")
        shown = ((extract("B.snippets", mode).get("snippets") or {}).get(mode) or "").strip()
        ran = snippet_call(os.path.join(root, launch, "main.nf"))
        problems = []
        if not shown:
            problems.append('the Selection view of S2 showed no [data-snippet="%s"]' % mode)
        elif ran != shown:
            problems.append("%s/main.nf ran %r, not the page's %s snippet %r" % (launch, ran, mode, shown))
        if shown and (not s2 or "'%s'" % s2 not in shown):
            problems.append("the page's %s snippet %r does not name S2 %s" % (mode, shown, s2))
        return problems, shown
```

Replace `b9` with:

```python
    def b9():
        problems, shown = ran_snippet("untyped", "selection")
        got = pipeline_hashes().get("fromstore") or {}
        problems += once_each("fromstore")
        if got != want_hashes:
            problems.append("fromStore(selection:) staged %s, expected exactly %s" % (json.dumps(got, sort_keys=True), json.dumps(want_hashes, sort_keys=True)))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("the page's untyped snippet (%s), run verbatim, staged A, B and C once each (A and B through the "
                      "nested Selection, B also directly) and every file hashes as pipeline-a's work file does" % shown)
```

Add after `b16`:

```python
    def b17():
        problems = []
        aligned, stats, stats_items = colls["aligned"], colls["stats"], expected["stats_items"]
        query = extract("B.picks", "query")
        picks = [p for p in query.get("picks") or [] if p.get("address") == items["A"]]
        if len(picks) != 1 or (picks[0].get("via") or "").split() != [aligned]:
            problems.append("query 3's [data-pick] for A is %r, expected one with data-via %s (cold's aligned)" % (picks, aligned))
        for label, ex, via, count in (("B.picks query", query, aligned, "1"),
                                      ("B.picks collection", extract("B.picks", "collection"), stats, str(len(stats_items)))):
            got = [(x.get("via"), x.get("count")) for x in ex.get("pickAll") or []]
            if got != [(via, count)]:
                problems.append("%s: [data-pick-all] is %r, expected one with data-via %s and data-count %s" % (label, got, via, count))
        tray = extract("B.picks", "all").get("tray")
        if tray != str(1 + len(stats_items)):
            problems.append("after Add all the tray holds %r, expected %d (A and every item of stats)" % (tray, 1 + len(stats_items)))
        posts = [p for p in _posts(seen("B.picks")) if _kind(p) == "Selection"]
        if len(posts) != 1:
            problems.append("B.picks: %d Selection POST(s) that write, expected 1" % len(posts))
        else:
            address, block, found = verify_post("B.picks", posts[0], dagjson.expected_selection)
            problems += found
            want = _members([_item(items["A"], [aligned])] + [_item(s, [stats]) for s in stats_items])
            if block["members"] != want:
                problems.append("B.picks: members %r, expected A via aligned and every stats item via stats: %r" % (block["members"], want))
            written = extract("B.picks", "after").get("written")
            if written != address:
                problems.append("B.picks: the page's data-written is %s, the Gate's address %s" % (written, address))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a pick from query 3 carried cold's aligned as its via, Add all put all %d items of stats in the tray "
                      "with stats as theirs, and the saved Selection has the Gate's address with every via kept" % len(stats_items))

    def b18():
        problems, shown = ran_snippet("typed", "selection-typed")
        if shown and not shown.startswith("nextflow.Channel.fromStore("):
            problems.append("the page's typed snippet %r does not call nextflow.Channel.fromStore (DESIGN.md §13)" % shown)
        if shown and "records: true" not in shown:
            problems.append("the page's typed snippet %r does not pass records: true" % shown)
        log = os.path.join(out, "selection-typed-nextflow.log")
        if not os.path.isfile(log):
            problems.append("no browser-b/selection-typed-nextflow.log: the typed consumer never started (see selection-typed.log)")
        else:
            with open(log, encoding="utf-8", errors="replace") as fh:
                warned = [line.strip() for line in fh if "invalid argument type" in line]
            if warned:
                problems.append("the typed consumer's .nextflow.log warns %d time(s): %s" % (len(warned), warned[:2]))
        got = pipeline_hashes("selection-typed", "store-typed").get("typed") or {}
        problems += once_each("typed", "selection-typed")
        if got != want_hashes:
            problems.append("the typed consumer staged %s, expected %s" % (json.dumps(got, sort_keys=True), json.dumps(want_hashes, sort_keys=True)))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("the page's typed snippet (%s), run verbatim with nextflow.enable.types, handed a Sample and a Kit record "
                      "to every task without an invalid argument type warning, and A, B and C hash as expected" % shown)
```

and after `run(16, ...)`:

```python
    run(17, "a per-run query pick keeps its collection, and Add all adds every item with it", b17)
    run(18, "the typed consumer receives records", b18)
```

In `main`, before the `sys.stderr.write(__doc__)` line, add:

```python
    if len(argv) == 6 and argv[1] == "consumer" and argv[3] in ("untyped", "typed"):
        return consumer(argv[2], argv[3], argv[4], argv[5])
```

Run: `cd gate && python3 -m unittest test_browser_b_assert`
Expected: PASS (every B8 to B18 case, `SubstituteTest`, `MemberWriteTest`, `PostsTest`).

- [ ] **Step 4: Mark B9's call line and write the typed consumer**

Replace `gate/selection/main.nf` with:

```nextflow
// gate/selection/main.nf
// Tier B's pipeline (block explorer spec section 1.3 assertions 9 and 13):
// read one Selection two ways and hash whatever Nextflow staged.
//
//   fromstore    the Selection view's untyped snippet: every file of every item
//   samplesheet  the explorer's CSV export, column '1', staged with file()
//
// tier_b.sh copies this file through `browser_b_assert.py consumer`, which
// replaces the line ending in `// @snippet` with the page's
// [data-snippet="untyped"] text, verbatim (DESIGN.md §15): a broken snippet
// fails B9. Run by hand, the line reads params.selection.
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

    ch_items = channel.fromStore(selection: params.selection)   // @snippet

    ch_store = ch_items
        .flatMap { item -> filesOf(item).collect { f -> tuple('fromstore', f) } }

    ch_sheet = channel
        .fromPath(params.samplesheet)
        .splitCsv(header: true, quote: '"')   // RFC 4180: list cells are quoted JSON with commas
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

Create `gate/selection-typed/main.nf`:

```nextflow
// gate/selection-typed/main.nf
// Tier B's typed consumer (block explorer spec section 1.3, assertion B18;
// UX map tickets 07 Q6 and 09 Q5): read the Selection S2 in a typed script
// and hand each item to a process whose input is typed with record types.
//
// tier_b.sh copies this file through `browser_b_assert.py consumer`, which
// replaces the line ending in `// @snippet` with the page's
// [data-snippet="typed"] text, verbatim. That text calls
// nextflow.Channel.fromStore(..., records: true): under
// nextflow.enable.types a plugin factory cannot be reached as
// channel.fromStore (nextflow-io/nextflow#7694, DESIGN.md §13).
//
// Every item of S2 is [meta, file]. The process takes the meta map as a
// Sample and its nested map as a Kit, so TaskProcessor checks both (v26.04.6
// TaskProcessor.groovy:1872-1891): a plain LinkedHashMap logs "invalid
// argument type", a RecordMap passes. browser_b_assert.py reads this run's
// .nextflow.log for that warning and compares the published digests with its
// own hashes of pipeline-a's work files.
//
// nextflow.config has no outputDir: unset, it is the writable member's alias
// (DESIGN.md §2).

nextflow.enable.types = true

include { fromStore } from 'plugin/nf-blocks'

params {
    selection: String
}

record Kit {
    kit: String
}

record Sample {
    sample: String
    lane: Integer
    nested: Kit
}

process HASH {
    tag "typed:${staged.name}"

    input:
    tuple(meta: Sample, kit: Kit, staged: Path)

    output:
    file("${staged.name}.sha256")

    script:
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum '${staged}' > '${staged.name}.sha256'
    else
        shasum -a 256 '${staged}' > '${staged.name}.sha256'
    fi
    """
}

workflow {
    main:
    ch_items = nextflow.Channel.fromStore(selection: params.selection, records: true)   // @snippet

    ch_hashes = HASH(ch_items.map { item -> tuple(item[0], item[0].nested, item[1]) })

    publish:
    hashes = ch_hashes
}

output {
    hashes {
        path 'hashes/typed'
        mode 'copy'
    }
}
```

Create `gate/selection-typed/nextflow.config`:

```groovy
// gate/selection-typed/nextflow.config
// The typed consumer's two members: its own writable `out` (tier_b.sh points
// it at browser-b/store-typed, apart from B9's store-out), and the tier B copy
// of the producer's store, `lab`, which holds the Selection. No outputDir line:
// unset, it means the writable member's alias, cas://out (DESIGN.md §2).
plugins {
    id 'nf-blocks@0.1.0'
}

manifest.name = 'cas-gate-selection-typed'

lineage.enabled = true
lineage.store.location = 'cas://out'

cas {
    stores {
        out { location = System.getenv('GATE_B_OUT') }
        lab { location = System.getenv('GATE_B_STORE') }
    }
    resolve = ['out', 'lab']
    asserted_by = 'gate'
}
```

- [ ] **Step 5: Run both consumers from `tier_b.sh`**

Header comment: replace `(block explorer spec section 1.3, assertions 8-16): the` with `(block explorer spec section 1.3, assertions 8-18): the`, and replace `# the samplesheet, runs gate/selection, and checks it all with its own encoder.` with:

```bash
# the samplesheet, runs gate/selection and gate/selection-typed on the page's
# own snippets, and checks it all with its own encoder.
```

Replace everything from the line `echo "--- browser tier B: selection pipeline (selection ${s2:-<none>})"` to the end of the file with:

```bash
# Each consumer runs the call line the page's Selection view showed for S2
# (step B.snippets), put verbatim on its `// @snippet` line; without one it
# does not run, and B9 or B18 fails on the exit status recorded here.
run_consumer() {   # <untyped|typed> <gate dir> <launch dir> <store-out> <cache> [args...]
    local mode="$1" src="$2" launch="$3" store_out="$4" cache="$5"; shift 5
    local name; name="$(basename "$launch")"
    local status=0
    echo "--- browser tier B: $name (selection ${s2:-<none>}, the page's $mode snippet)"
    rm -rf "${launch:?}" && mkdir -p "$launch" "$store_out"
    if python3 "$REPO/gate/browser_b_assert.py" consumer "$GATE_ROOT" "$mode" "$src" "$launch"; then
        ( cd "$launch" && GATE_B_STORE="$B/store" GATE_B_OUT="$store_out" XDG_CACHE_HOME="$cache" \
          "$NEXTFLOW" run . -name "$name" --selection "$s2" ${1+"$@"} ) > "$B/$name.log" 2>&1 || status=$?
    else
        status=no-snippet
    fi
    echo "$status" > "$B/$name.exit"
    if [[ -f "$launch/.nextflow.log" ]]; then cp "$launch/.nextflow.log" "$B/$name-nextflow.log"; fi
}

run_consumer untyped "$REPO/gate/selection" "$GATE_ROOT/selection" "$B/store-out" "$B/cache-run" \
    --samplesheet "$B/samplesheet.csv"
run_consumer typed "$REPO/gate/selection-typed" "$GATE_ROOT/selection-typed" "$B/store-typed" "$B/cache-typed"

echo
python3 "$REPO/gate/browser_b_assert.py" check "$GATE_ROOT"
```

The `s2=...` line above it stays. `${1+"$@"}` rather than `"$@"`: macOS's bash 3.2 treats an empty `"$@"` as unbound under `set -u`. In `gate/gate.sh`, replace

```bash
       "${GATE_ROOT:?}/browser-b" "${GATE_ROOT:?}/selection"
```

with

```bash
       "${GATE_ROOT:?}/browser-b" "${GATE_ROOT:?}/selection" "${GATE_ROOT:?}/selection-typed"
```

- [ ] **Step 6: Document B17 and B18 in `gate/README.md`**

Replace

```
`browser/`, `browser-b/`, `selection/` and the snapshots first. Every one of them is evidence, and stale
```

with

```
`browser/`, `browser-b/`, `selection/`, `selection-typed/` and the snapshots first. Every one of them is evidence, and stale
```

Replace `Tier B, milestone 2, assertions 8 to 16 of spec section 1.3. It is local:` with `Tier B, milestones 2 and 3, assertions 8 to 18 of spec section 1.3. It is local:`.

Replace `` `drive.mjs` then plays nine steps with the launch token `explore` printed:`` with `` `drive.mjs` then plays eleven steps with the launch token `explore` printed:``.

Replace

```
compose S5 and restore the copy the page offers; and compose S6. While `explore` is still up, `browser_b_assert.py probe`
```

with

```
compose S5 and restore the copy the page offers; compose S6; pick A from
query 3 over `cold`'s `aligned` (`sample` is `A`), add every item of `cold`'s
`stats` with Add all, and save them as `picked`; and read the Selection view
of `second` in both snippet modes, untyped and typed. While `explore` is still up, `browser_b_assert.py probe`
```

Replace

```
`gate/selection` runs in `$GATE_ROOT/selection` over the copy (member `lab`)
and its own `browser-b/store-out` (member `out`), staging `second` through
`fromStore(selection:)` and through the CSV's `1` column, and publishing the
sha256 of each staged file. `browser_b_assert.py check` recomputes every
address with `gate/dagjson.py` and the Gate's DAG-CBOR encoder:
```

with

```
`gate/selection` runs in `$GATE_ROOT/selection` over the copy (member `lab`)
and its own `browser-b/store-out` (member `out`), staging `second` through
the page's untyped snippet and through the CSV's `1` column, and publishing
the sha256 of each staged file. `browser_b_assert.py consumer` copies the
pipeline first, putting the page's `[data-snippet]` text verbatim on the line
of `main.nf` that ends in `// @snippet`, so a broken snippet fails the Gate.
`gate/selection-typed` then runs the page's typed snippet the same way in
`$GATE_ROOT/selection-typed`, into its own `browser-b/store-typed` and with
no `outputDir` line: a typed script (`nextflow.enable.types = true`) whose
process takes each item as `tuple(meta: Sample, kit: Kit, staged: Path)`.
`browser_b_assert.py check` recomputes every address with `gate/dagjson.py`
and the Gate's DAG-CBOR encoder:
```

Replace

```
  Restore writes one `del` superseding `shared`'s, so S6 is live across both
  members.

Logs are in `browser-b/`: `explore.log`, `drive.log`, `probe.log`,
`selection.log` (and `selection-nextflow.log`), beside `scenario.json`,
```

with

```
  Restore writes one `del` superseding `shared`'s, so S6 is live across both
  members.
- B17: query 3's `[data-pick]` for A carries `cold`'s `aligned` in
  `data-via`; `[data-pick-all]` names the query's collection with count 1 on
  the query page and `stats` with its item count on the collection page; Add
  all fills the tray with every item of `stats`; and the saved Selection has
  the Gate's address, with A via `aligned` and each `stats` item via `stats`.
- B18: the typed snippet calls `nextflow.Channel.fromStore` with `records:
  true` and names `second`; the typed consumer ran it verbatim, exited 0,
  logged no `invalid argument type` warning in its `.nextflow.log`, ran one
  task per file, and each published digest matches pipeline-a's work file.
  B9 likewise requires `gate/selection` to have run the untyped snippet,
  naming `second`.

Logs are in `browser-b/`: `explore.log`, `drive.log`, `probe.log`,
`selection.log` (and `selection-nextflow.log`), `selection-typed.log` (and
`selection-typed-nextflow.log`), beside `scenario.json`,
```

In the GATE_ROOT tree, replace

```
selection/               tier B's selection pipeline launch directory
```

with

```
selection/               tier B's selection pipeline launch directory
selection-typed/         tier B's typed consumer launch directory
```

- [ ] **Step 7: Run tier B and the whole Gate**

Run: `cd gate && python3 -m unittest test_browser_b_assert test_dagjson`
Expected: PASS.

Run: `GATE_ROOT=<scratchpad>/gate make gate`
Expected: lineage 11/0/6 (or better), browser tier A 5/5, browser tier B 11/11, with `B17` and `B18` lines. When a B assertion fails, read `browser-b/drive.log`, the step in `observed.json`, and `selection-typed.log` or `selection-typed-nextflow.log` before changing code. A typed-script compile error (the record or tuple syntax) is fixed in `gate/selection-typed/main.nf` against `v26.04.6:tests/record-types.nf` and `docs/process-typed.md`; an `invalid argument type` warning is a `records: true` bug in Task 3 or a typed snippet without it in Task 10, fixed there, never by loosening B18.

- [ ] **Step 8: Commit**

```bash
git add gate
git commit -m "test(gate): tier B17 picks keep their collection, B18 typed consumer

B17 picks an item from a per-run query and a whole collection with Add
all, and checks each saved member kept its collection. B18 runs a typed
consumer (nextflow.enable.types, record-typed inputs) on the Selection
view's typed snippet and fails on any invalid argument type warning. B9's
consumer now runs the untyped snippet verbatim, so a broken snippet fails
the Gate.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: `DESIGN.md`, spec, README, and acceptance

Every decision of this plan's header lands in the document it amends; `DESIGN.md` §16 gains a Milestone 3 subsection that lists the fourteen with their tickets. Each step below quotes the existing text and gives its replacement in full. Keep facts only.

**Files:**
- Modify: `DESIGN.md` (§0 rule 5, §2, §11, §13, §15 verbs and DOM contract, §16)
- Modify: `README.md` (the stale status paragraph, Get Started, a consumer example, Selections)
- Modify, in place (not in the repo): `../.scratch/block-explorer/spec.md` (sections 2, 5.6, 10), `../.scratch/block-explorer/map.md` (Q15 b)

- [ ] **Step 1: `DESIGN.md` §0, §2, §11, §13**

§0 rule 5. Replace

```
5. No command-line or web surface. No participation in task hashing or
   `-resume` identity. *Amended 2026-09-24: the command-line and web surface
   rule is lifted for the block explorer alone, specified in
   `../.scratch/block-explorer/spec.md`.*
```

with

```
5. No command-line or web surface. No participation in task hashing or
   `-resume` identity. *Amended 2026-09-24: the command-line and web surface
   rule is lifted for the block explorer alone, specified in
   `../.scratch/block-explorer/spec.md`.* *Widened 2026-09-27: the lifting
   also covers `nf-blocks:items`, a read-only verb that lists a run's items
   from the plugin's own index so a downstream workflow can use them (§15,
   §16 Milestone 3). `put` stays the only command-line write path.*
```

§2, the example block. Replace

```
outputDir = 'cas://lab'                // publish through the store
```

with

```
outputDir = 'cas://lab'                // optional: unset means the writable member's alias
```

§2, the rule. Replace

```
- The writable member is the alias in `lineage.store.location`; `outputDir`
  must name the same alias (checked at run start, abort otherwise).
```

with

```
- The writable member is the alias in `lineage.store.location`. *Amended
  2026-09-27:* `outputDir` unset means the writable member's alias; set to
  anything else, the run aborts (checked at run start, in
  `CasObserver.onFlowCreate`). `CasObserverFactory.create(session)` sets
  `session.outputDir = FileHelper.toCanonicalPath('cas://<alias>')` (as
  `Session.groovy:418` does) when lineage is enabled,
  `lineage.store.location` starts with `cas://`, and the config has no
  `outputDir`, and logs at info `outputDir not set; publishing to
  cas://<alias>`. It runs before `Session.groovy:472` copies
  `session.outputDir` into `WorkflowMetadata`, so `workflow.outputDir`, the
  lineage WorkflowRun record and Platform payloads agree. `session.config` is
  not changed, so the RunManifest records the config as written. An
  `outputDir` from `-output-dir`, or `cas://<alias>/sub`, is explicit: the
  factory leaves it to `onFlowCreate`'s check.
```

§11. Replace

```
  exception included, reaches the caller as `AbortRunException` (§0 rule 3);
  before this change a checked exception was swallowed at debug level.
```

with

```
  exception included, reaches the caller as `AbortRunException` (§0 rule 3);
  before this change a checked exception was swallowed at debug level.
  *Amended 2026-09-27:* when `session.error` is already set, `onFlowComplete`
  catches its own failures and logs them at warn instead of throwing, since
  an `AbortRunException` there skips `notifyError` (`Session.groovy:1125-1128`)
  for every observer, losing the user's `onError` and the hint below. A clean
  run keeps the abort.
- `onFlowError(event)` (2026-09-27): when `session.error`, or a cause of it,
  is a `MissingMethodException` whose method is `fromStore` or
  `Channel.fromStore`, log at warn the hint `FromStoreHint.of(error)` builds
  (§13); anything else, nothing.
```

§13. Replace the section's last paragraph

```
`channel.fromStore(selection: <cid or cas://cid>)` (block explorer spec
section 10) emits every distinct item the Selection reaches through nesting
(`Index.selectionItems`), sorted by item CID, restored as above; a Selection
hidden by a current `delete` Claim emits with a warning; a nested Selection
the composition lacks fails the call, naming it. `run`, `output`, `where` and
`pipeline` are refused beside `selection`.
```

with

```
`channel.fromStore(selection: <cid or cas://cid>)` (block explorer spec
section 10) emits every distinct item the Selection reaches through nesting
(`Index.selectionItems`), sorted by item CID, restored as above; a Selection
hidden by a current `delete` Claim emits with a warning; a nested Selection
the composition lacks fails the call, naming it. `run`, `output`, `where` and
`pipeline` are refused beside `selection`.

*Amended 2026-09-27 (milestone 3):*

- `records: true` (default `false`, in every script mode, beside `selection:`
  and beside `run:` with `output:`) restores every map that is not a Leaf, at
  any depth and inside lists, as `nextflow.util.RecordMap`; Leaves become
  paths as above and lists stay lists. Any value other than `true` or
  `false` is refused, naming `records`. A restored record is immutable:
  `item + [x: 1]` returns a new map, and `put` throws Nextflow's
  `UnsupportedOperationException`. `RecordMap` exists from Nextflow
  26.01.1-edge; the plugin requires 26.04.6.
- In a typed script (`nextflow.enable.types = true`) a plugin factory cannot
  be reached as `channel.fromStore` or `Channel.fromStore` in 26.04.6:
  `channel` is `nextflow.dataflow.ChannelNamespace`, which has a fixed
  factory list (nextflow-io/nextflow#7694). The spelling that works keeps the
  include and calls `nextflow.Channel.fromStore(..., records: true)`, which
  a record-typed process input accepts without TaskProcessor's `invalid
  argument type` warning. Gate tier B18 runs it.
- A run reference (`run:`) is resolved by `robsyme.cas.core.RunRef.resolve`,
  which `nf-blocks:items --run` shares (§15); its errors say "run reference".
- With none of `selection`, `run` or `output`, the call fails with
  "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output:
  <name>`; optionally `where: [...]` and `records: true`".
- A run that fails because `fromStore` cannot be found ends with a warning
  from `CasObserver.onFlowError` (§11), chosen by the exception, not the
  script mode. Untyped (method `Channel.fromStore`): "`fromStore` comes from
  the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'`
  at the top of the script." Typed (method `fromStore`, type
  `nextflow.dataflow.ChannelNamespace` or `nextflow.script.types.Channel`):
  "In a typed script, `channel.fromStore` can't be reached until
  nextflow-io/nextflow#7694 is fixed. Add the include and call
  `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed
  inputs."
```

Then compare the two quoted hints and the no-argument message with the strings in `src/main/groovy/robsyme/cas/trace/FromStoreHint.groovy` and `ext/CasExtension.groovy` as Task 2 built them (`grep -n "nf-blocks plugin\|typed script\|takes \`selection" src/main/groovy/robsyme/cas/trace/FromStoreHint.groovy src/main/groovy/robsyme/cas/ext/CasExtension.groovy`). They must be the same characters; if Task 2's differ from ticket 07's, the code is wrong and is fixed there, with its test.

- [ ] **Step 2: `DESIGN.md` §15, the verbs and the DOM contract**

Replace

```
nextflow [-c <config>] plugin nf-blocks:snapshot
nextflow [-c <config>] plugin nf-blocks:explore [--port <n>]
nextflow [-c <config>] plugin nf-blocks:put <file> [--dry-run]
```

with

```
nextflow [-c <config>] plugin nf-blocks:snapshot
nextflow [-c <config>] plugin nf-blocks:explore [--port <n>]
nextflow [-c <config>] plugin nf-blocks:put <file> [--dry-run] [--name <name>]
nextflow [-c <config>] plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...]
                                              [--pipeline <id>] [--format csv|json|occurrences|selection]
```

Replace

```
`put` prints the response or error body (DAG-JSON) on stdout, exit 0 or 1.
Nextflow 26.04.6's launcher refuses a bare `-` (`Unknown option: -`), so read
stdin as `/dev/stdin`; `-` works for in-process callers. A bare `--dry-run`
arrives as `--dry-run`, `true`.
```

with

```
`put` prints the response or error body (DAG-JSON) on stdout, exit 0 or 1.
Nextflow 26.04.6's launcher refuses a bare `-` (`Unknown option: -`), so read
stdin as `/dev/stdin`; `-` works for in-process callers. A bare `--dry-run`
arrives as `--dry-run`, `true`.

`put --name <name>` (milestone 3) takes Selection requests only. Once the
Selection is written or found, a second request through the same `Put`
builder writes a `set name` Claim, timestamped by the verb's clock,
superseding the dry run's `name_claims`; nothing is written when the one
current name already equals `<name>`. Both responses are printed. When the
Claim fails the verb prints "saved, naming failed" and exits 1. With
`--dry-run` it prints the dry run and the Claim it would send. Renaming is a
re-put with a new `--name`; there is no `rename` verb.

`items` (milestone 3) is read-only. It lists the items of one output of one
or more runs from the plugin's own index, caught up first with
`cas.openIndex()` as `put` and `snapshot` do; it never reads an Index
Snapshot and never writes. `<ref>` is anything `fromStore(run:)` takes
(`latest` needs `--pipeline`), resolved by `RunRef`; several runs are
comma-joined, because `CmdPlugin` keeps one value per flag. A condition is a
positional `<path>=<value>` split at the first `=` (`a=b=c` is the path `a`
and the value `b=c`); it matches an `item_attr` row of that path whose value
text equals `<value>`, whatever its type, so `lane=2` finds the integer 2 and
the string "2" (`Index.itemHitsByText`). Every condition must hold. `=x` or
an argument without `=` is a usage error naming it, exit 2. Rows are sorted
by item CID, then collection CID. `--format csv` (the default) and `json` are
`Samplesheet.of(store, items)` (§16) with a leading `occurrence` column,
`cas://<collection>/<item>`; a Meta Map key named `occurrence` is renamed by
the samplesheet's existing prefix rule. `occurrences` prints one occurrence
per line. `selection` prints a complete `put` request whose members are
those occurrence strings, the union across the runs, so the command-line
route to a named Selection is `items ... --format selection | nextflow
plugin nf-blocks:put /dev/stdin --name <name>`.
```

In the DOM table, replace

```
| `[data-item-result]` | one query 3 item cid |
```

with

```
| `[data-item-result]` | one query 3 item cid |
| `[data-preview-for]` | the Meta Map pills container of one item row: `data-preview-for` the item cid. Absent when the item has no Meta Map (milestone 3) |
| `[data-pick-all]` | "Add all N to the tray" on query results and collection pages: `data-via` the collection, `data-count` N; adds every item of the query or collection, not only the page (milestone 3) |
```

Replace

```
| `[data-pick]` | a button adding `data-pick` (an address) with `data-via` (space-separated collections) and `data-kind` (`item` or `selection`) |
```

with

```
| `[data-pick]` | a button adding `data-pick` (an address) with `data-via` (space-separated collections) and `data-kind` (`item` or `selection`); on query 3 results `data-via` is the run's Output Collection (milestone 3) |
```

Replace

```
| `[data-samplesheet]` | `csv` or `json` export link (served by `explore` only) |
```

with

```
| `[data-samplesheet]` | `csv` or `json` export link (served by `explore` only) |
| `[data-snippet]` | the `<code>` holding one consumer call line on the Selection and run pages: `untyped`, `channel.fromStore(...)`, or `typed`, `nextflow.Channel.fromStore(..., records: true)`. The mode is the bare string in `localStorage` key `nf-blocks.snippets` (milestone 3) |
| `[data-snippet-mode]` | one of the two toggle buttons, `untyped` or `typed`, that switch every snippet on the page; `aria-pressed` marks the current one (milestone 3) |
```

- [ ] **Step 3: `DESIGN.md` §16**

Replace the heading `## 16. Selections (milestone 2)` with `## 16. Selections (milestones 2 and 3)`.

Replace

```
only (decision 1; no new verb, spec section 2's table stays at three), linked
```

with

```
only (decision 1; no new verb for it; milestone 3's `items`, §15, prints the same columns from the command line), linked
```

Replace

```
nextflow [-c <config>] plugin nf-blocks:put <file|-> [--dry-run]
```

with

```
nextflow [-c <config>] plugin nf-blocks:put <file|-> [--dry-run] [--name <name>]
```

Replace

```
conflict; the dry run reports `here` and `name_claims`, and `deletion`, `deletion_claims`.
```

with

```
conflict; the dry run reports `here` and `name_claims`, and `deletion`, `deletion_claims`.
A member may be written as an Item Occurrence string, `cas://<collection>/<item>`,
meaning that item via that collection (`Put.groovy:162-168`); `items --format
selection` writes its members that way. `--name` is in §15.
```

Replace

```
Nine assertions (spec section 1.3, tier B), all local: a Selection made in
```

with

```
Eleven assertions (spec section 1.3, tier B), all local: a Selection made in
```

and

```
another member does not lock a rename (15); a copy deleted in another member is
restored (16). B12 probes with a Selection no
```

with

```
another member does not lock a rename (15); a copy deleted in another member is
restored (16); a per-run query pick keeps its collection, and Add all adds
every item with it (17); the typed consumer receives records (18). B9's and
B18's consumers run the call line the Selection view shows (`[data-snippet]`)
verbatim, so a broken snippet fails the Gate. B12 probes with a Selection no
```

At the end of the file, after the `### Resolved` item, append:

```
### Milestone 3: picking items for a downstream workflow

Plan `docs/plans/2026-09-27-explorer-milestone-3.md`, from the UX map
`../.scratch/block-explorer/ux/map.md`; each ticket's `## Answer` holds the
reasoning.

1. An unset `outputDir` means the writable member's alias, set by
   `CasObserverFactory` before `WorkflowMetadata` copies it (§2).
   [02](../.scratch/block-explorer/ux/issues/02-unset-outputdir.md), research
   [01](../.scratch/block-explorer/ux/issues/01-outputdir-read-before-flowcreate.md).
2. A run that fails because `fromStore` cannot be found ends with a warning
   naming the fix, untyped and typed apart, chosen by the exception (§11,
   §13). [06](../.scratch/block-explorer/ux/issues/06-missing-fromstore-exception-chain.md),
   [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q1.
3. On a run that is already failing, `onFlowComplete` logs its own failures
   instead of throwing, so `notifyError` still reaches every observer (§11).
   [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q2.
4. `fromStore(..., records: true)` restores every non-Leaf map as
   `RecordMap`; default `false` in every script mode (§13).
   [09](../.scratch/block-explorer/ux/issues/09-opt-in-record-restore.md).
5. `fromStore` with no `selection`, `run` or `output` says what it takes
   (§13). [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q3.
6. One run-reference resolver, `RunRef`, for `fromStore(run:)` and
   `items --run` (§13, §15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q2.
7. `nf-blocks:items`, a fourth verb, read-only, over the caught-up local
   index; conditions match as text; formats `csv`, `json`, `occurrences`,
   `selection` (§0 rule 5, §15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q1-Q4, Q6.
8. `put --name <name>` names a Selection with one `set name` Claim (§15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q5.
9. Query results, collection pages, the tray and Selection members share one
   item row: a label of up to three paths (list-valued paths skipped, strings
   first, then most distinct values among loaded previews, ties by path),
   file-name chips, the pick control, the Meta Map as `key | value` pills
   coloured by type (numbers blue, `true` green, `false` red, null grey
   italic; a number-like string quoted), and the shortened item CID. A pill's
   value runs query 3 with that pair added.
   [03](../.scratch/block-explorer/ux/issues/03-item-previews.md),
   [10](../.scratch/block-explorer/ux/issues/10-meta-map-pairs.md).
10. Previews are OutputItem blocks fetched through `blocks.ofKind`
    (hash-checked, cached), at most 6 at once, the first 100 rows of a view
    without a click; no §12 change and no snapshot query.
    [03](../.scratch/block-explorer/ux/issues/03-item-previews.md) Q3.
11. Runs are shown as `<run_name> / <output>`, the CID in `title`: a tray
    entry's "picked from", a Selection member's via, the item page's
    "produced by". [03](../.scratch/block-explorer/ux/issues/03-item-previews.md) Q4.
12. A per-run query pick records its Output Collection as `via`; "Add all N
    to the tray" (`[data-pick-all]`) adds every item of the query or
    collection; row checkboxes add the checked ones; no client-side size
    check, since the dry run's `too_large` covers it (spec section 5.6).
    [04](../.scratch/block-explorer/ux/issues/04-query-results-keep-run.md).
13. The Selection page shows the `include` line and the
    `fromStore(selection:)` call; the run page shows its `lid://` and, per
    output, the `fromStore(run:, output:)` call; one untyped | typed toggle
    for every snippet, remembered per viewer (§15 `[data-snippet]`).
    [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q4.
14. The Gate runs the snippets: tier B9's consumer and B18's typed consumer
    take their call line verbatim from the page (§14, Gate browser tier B).
    [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q6.

Left out, per the map: plugin factories on the typed `channel` namespace
(nextflow-io/nextflow#7694, upstream), self-registration of `fromStore` so
the include is optional, and a "one of" condition in the page and in
`fromStore(where:)`.
```

After Step 7 passes, insert below the new heading the status line `*Status <acceptance date, YYYY-MM-DD>: milestone 3 accepted; Gate lineage 11/0/6, browser tier A 5/5, tier B 11/11 (all local).*`, with the cloud tier's result appended when Step 7 ran it (`; cloud tier A6-A7 passed on <commit>`).

- [ ] **Step 4: The spec and the old map, in place**

`../.scratch/block-explorer/` is not in the `nf-blocks` repo. For each file, read it whole, make the replacements in memory, write the result to a temporary file beside it, check that the section headings (`grep -c '^## ' <file>`) and line count moved only as expected, then move it over the original (the append-safely rule).

`spec.md` section 2. Replace

```
| `nextflow plugin nf-blocks:put <file\|->` | Builds and writes one client-constructible block from DAG-JSON; `--dry-run` |
| `nextflow plugin nf-blocks:snapshot` | Rewrites the writable member's Index Snapshot at any size |
```

with

```
| `nextflow plugin nf-blocks:put <file\|->` | Builds and writes one client-constructible block from DAG-JSON; `--dry-run`; `--name <name>` names a Selection with one `set name` Claim (milestone 3) |
| `nextflow plugin nf-blocks:snapshot` | Rewrites the writable member's Index Snapshot at any size |
| `nextflow plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...]` | Lists the items of a run's output from the plugin's own index, read-only; `--pipeline`, `--format csv\|json\|occurrences\|selection` (milestone 3, UX map ticket 05) |
```

`spec.md` section 5.6. Replace

```
The page builds a Selection from items picked in any view, recording `via`
as the Output Collection each was picked from, or leaving it empty for items
chosen by a query. Before saving it always dry-runs (section 9.3); if the
```

with

```
The page builds a Selection from items picked in any view, recording `via`
as the Output Collection each was picked from, including a per-run query's;
empty only for items chosen by a query across runs. Before saving it always dry-runs (section 9.3); if the
```

`spec.md` section 10. Replace

```
  `delete` Claim it emits with a warning (map Q19; Selection-dependencies
  ticket, Q9).
```

with

```
  `delete` Claim it emits with a warning (map Q19; Selection-dependencies
  ticket, Q9).
- `fromStore(..., records: true)`, with a Selection or a run, restores every
  map that is not a Leaf, at any depth, as a Nextflow record (`RecordMap`),
  so a record-typed process input accepts it; the default, `false`, keeps
  plain maps in every script mode. A typed script calls it as
  `nextflow.Channel.fromStore(...)` until nextflow-io/nextflow#7694 is fixed
  (UX map tickets 07 and 09).
- The page's Selection view shows the `include` line and the
  `fromStore(selection:)` call, untyped or typed, and the run page the
  `fromStore(run:, output:)` call per output (UX map ticket 07).
```

`map.md` (the old map). Replace

```
  carries the run the curator saw rather than every run that produced the
  same bytes. (Q15 b)
```

with

```
  carries the run the curator saw rather than every run that produced the
  same bytes. (Q15 b) *Amended 2026-09-27 by UX map ticket 04: a pick from a
  per-run query result records that run's Output Collection as `via` too;
  `via` is empty only for an item chosen by a query across runs. The intent
  holds, since query 3 is per run.*
```

- [ ] **Step 5: `README.md`**

Replace

```
This is the walking skeleton. The boundary with Nextflow is in place and
proven on released Nextflow 26.04.6: the plugin loads, publishes through
`cas://`, and keeps `lid://` reads working. The block store itself, the
DAG-CBOR lineage records, the SQLite index and the `fromStore` channel factory
arrive in the tasks that follow; see `DESIGN.md` for the contract they are
built against.
```

with

```
It is built and tested against released Nextflow 26.04.6. A run writes
DAG-CBOR lineage records into the store beside Nextflow's own `lid://`
records, and keeps a SQLite index of them; another pipeline reads the outputs
back with the `fromStore` channel factory (below), and `nf-blocks:explore`
browses them and saves Selections. `DESIGN.md` is the contract the code is
built against.
```

Replace

```
outputDir = 'cas://lab'                // publish through the store
```

with

```
outputDir = 'cas://lab'                // optional: unset, it is the lineage alias
```

Replace

```
An alias matches `^[a-z][a-z0-9_-]{0,31}$` and must not parse as a content
address. The alias in `outputDir` must be the same one as in
`lineage.store.location`.
```

with

```
An alias matches `^[a-z][a-z0-9_-]{0,31}$` and must not parse as a content
address. `outputDir` may be left out: unset, it means the alias in
`lineage.store.location`. When set, it must name that alias, or the run
aborts.
```

Insert a new section before `## Browsing a store`:

````
## Reading outputs in another pipeline

A second pipeline reads a run's outputs, or a Selection, back out of the
store with `fromStore`. Its config names the producer's store as a read-only
member beside its own writable one:

```groovy
plugins {
    id 'nf-blocks@0.1.0'
}

manifest.name = 'downstream'           // or cas.pipeline = 'downstream'

lineage.enabled = true
lineage.store.location = 'cas://mine'

cas {
    stores {
        mine { location = '/data/cas-downstream' }   // writable: this pipeline's own outputs
        lab  { location = '/data/cas' }              // the producer's store, read-only here
    }
    resolve = ['mine', 'lab']
}
```

No `outputDir` line is needed; unset, it is `cas://mine`. Set
`manifest.name` (or `cas.pipeline`) so the consuming runs are filed under a
Pipeline Identity of their own rather than as `main.nf`.

`fromStore` comes from the plugin, so the script includes it:

```nextflow
include { fromStore } from 'plugin/nf-blocks'

workflow {
    main:
    picked  = channel.fromStore(selection: '<selection address>')
    aligned = channel.fromStore(run: 'latest', pipeline: 'cas-test-pipeline', output: 'aligned', where: [sample: 'B'])

    picked.mix(aligned).view()
}
```

Each item arrives in the shape the producer published it, here
`[meta, bam]`, with every file a `cas://` path Nextflow stages like any
other. `run:` also takes a `lid://<run hash>` or a `cas://` RunCompletion.

In a typed script (`nextflow.enable.types = true`), Nextflow 26.04.6 cannot
reach a plugin factory through `channel.` (nextflow-io/nextflow#7694). Keep
the include and call it through `nextflow.Channel`, with `records: true` so a
record-typed input receives records rather than plain maps:

```nextflow
nextflow.enable.types = true

include { fromStore } from 'plugin/nf-blocks'

record Sample {
    sample: String
    lane: Integer
}

process COUNT_LINES {
    input:
    tuple(meta: Sample, bam: Path)

    output:
    stdout()

    script:
    """
    printf '%s\\t' '${meta.sample}'
    wc -l < '${bam}'
    """
}

workflow {
    COUNT_LINES(nextflow.Channel.fromStore(selection: '<selection address>', records: true))
        .view()
}
```

A restored record is immutable: `meta + [x: 1]` returns a new map, and
`meta.put('x', 1)` throws `UnsupportedOperationException`. Without
`records: true` every map arrives as an ordinary mutable map, typed script
or not.

The explorer's Selection and run pages show these call lines for what you
are looking at, with a toggle between the untyped and typed forms. A run that
fails because the include is missing ends with a warning saying what to add.
````

In `## Selections`, replace

```
`nextflow plugin nf-blocks:put <file|-> [--dry-run]` builds and writes the
```

with

```
`nextflow plugin nf-blocks:put <file|-> [--dry-run] [--name <name>]` builds and writes the
```

and after the paragraph ending `` `-plugins nf-blocks` to stage. See `DESIGN.md` §16 for the full contract.`` add:

````
From the command line, `nf-blocks:items` finds items and `put --name` saves
them as a named Selection:

```bash
nextflow plugin nf-blocks:items aligned nested.kit=truseq --run latest --pipeline cas-test-pipeline
nextflow plugin nf-blocks:items aligned lane=2 --run lid://<run hash>,lid://<other run hash> --format selection \
  | nextflow plugin nf-blocks:put /dev/stdin --name lane-2
```

`items` never writes. By default it prints the samplesheet above with a
leading `occurrence` column; `--format json` prints the same rows as JSON,
`--format occurrences` one `cas://<collection>/<item>` per line, and
`--format selection` a complete `put` request. A condition is
`<path>=<value>`, split at the first `=`, with dotted paths for nested keys;
it matches the value as text whatever its type, so `lane=2` finds the number
and the string. In a `put` request a member may be written as such an
occurrence string instead of `{"item": {"address": ..., "via": [...]}}`,
meaning the item as picked from that collection. `put --name` writes the name
Claim after the Selection, and nothing when the Selection already has that
one name; renaming is the same command with a new name.
````

- [ ] **Step 6: Check the prose**

Run: `git diff -U0 DESIGN.md README.md | grep -n -E '^\+.*(—|serves as|stands as)|^\+ *- \*\*'`
Expected: no output (no em dashes, no "serves as", no bold-first bullets in the added lines). Read the new §16 subsection and the README section once through for repeated sentence openings and filler, and fix them.

- [ ] **Step 7: Acceptance**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL; record the test count.

Run: `cd web && npm test`
Expected: all pass.

Run: `cd gate && python3 -m unittest discover -p 'test_*.py'`
Expected: OK.

Run: `GATE_ROOT=<scratchpad>/gate make gate` twice.
Expected both times: `11 PASS, 0 FAIL, 6 SKIP` from the lineage tier (or more PASS if Task 1's assertion 6 counts separately; never a FAIL), `browser tier A (local): 5 PASS, 0 FAIL`, `browser tier B: 11 PASS, 0 FAIL`. Lineage assertion 4 may fail once, by design (`gate/README.md`); rerun once before debugging it.

Optional, with Rob's `scidev` SSO session: `GATE_PYTHON=<venv python with boto3> make gate-cloud GATE_ROOT=<scratchpad>/gate`. This milestone does not change the HTTP reader or how `explore` serves members (previews are block fetches through the existing routes), so the cloud tier is not required for acceptance; ask Rob whether to run it, and record the answer in the status line.

- [ ] **Step 8: Status line and commit**

Add the §16 status line (Step 3) with the date and the tallies of Step 7.

```bash
git add DESIGN.md README.md
git commit -m "docs: block explorer milestone 3 accepted

DESIGN.md: unset outputDir means the lineage alias (§2), onFlowError's
fromStore hint and onFlowComplete on a failing run (§11), records: true
and the typed-script spelling (§13), nf-blocks:items and put --name, and
[data-pick-all], [data-preview-for], [data-snippet] (§15), and §16's
Milestone 3 decisions with their tickets. README: a consumer example and
Selections from the command line.

Gate: lineage 11/0/6, browser tier A 5/5, tier B 11/11. <N> Groovy tests.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Replace `<N>` with Step 7's count before committing. Then use superpowers:finishing-a-development-branch to merge `feat/explorer-m3` into `main` with Rob's approval.
