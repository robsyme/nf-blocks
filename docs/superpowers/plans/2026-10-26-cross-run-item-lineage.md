# Cross-run Item Lineage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record, for every input a run received through `fromStore`, the upstream OutputItem, OutputCollection and Selection it came from, and for every OutputItem a run published, which of the run's inputs and which tasks made it; index both, and show them on the explorer's item page as an upstream Lineage step and a "Used by" panel.

**Architecture:** `fromStore` fills a per-run map on `CasSession` (emitted Path to Origins); `InputRecorder` attaches those Origins to InputSet inputs (InputSet schema 2). The observer keeps each publish's work-dir source and each task's work dir and staged inputs, walks them at completion into a new `ItemLineage` block linked from RunCompletion schema 3. Mark and the copy closure keep both as run metadata and follow nothing under them. The Index (schema 7) adds `run_input.position`, `input_origin`, `item_input` and `item_task`, copied into snapshots; the page reads them, or the blocks for tail runs.

**Tech Stack:** Groovy 4 / Spock (`./gradlew test`), SQLite via JDBC, Nextflow 26.04.6 APIs (`TaskRun`, `FilePublishEvent`), plain ES modules in `web/src` with `node:test` and the fake DOM in `web/test/dom.mjs`, the Gate's Python (`python3 -m unittest discover -s gate`).

**Spec:** `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md` (approved). DESIGN.md is the project contract; every task that changes behaviour leaves DESIGN.md's ipldsch block or prose true (Task D1 owns the prose).

## Global Constraints

- Clean code over compatibility: the user is the only user. Bump versions and rebuild; no migrations or shims beyond "readers accept InputSet schema 1 and 2" and `RunCompletion.READABLE = [1, 2, 3]`.
- Task hashes must not change: nothing goes into the emitted `cas://` path; Origins live only in the side map (spec §4).
- Sweep (Mark) and the copy closure never follow `Input.from` or anything under `item_lineage` (spec §4, §5.3).
- The explorer never names an upstream item it did not read from a record; an input with no Origin shows the file only (spec §4).
- Sort orders, verbatim from the spec: Origins by (item, collection, selection) as CID bytes, nulls first, no two equal; `Input` sort key gains `from` last; `ItemLineage.items` by item CID bytes; `Derived.inputs` ascending, no duplicates; `Derived.tasks` by hash.
- `TaskRef.hash` is 32 lowercase hex; `TaskRef.process` is the fully qualified process name (`task.processor.name`).
- Index `SCHEMA_VERSION = 7`, snapshot `index/v7.sqlite`, the web adds `index/v6.sqlite` to `OLDER_SNAPSHOT_PATHS`.
- Never symlink `web/node_modules` between worktrees (`npm ci` wipes the target); run `npm ci` in each worktree, or `cp -R` it.
- Any run that opens an Index (explore, snapshot, put, a pipeline run) sets `XDG_CACHE_HOME` to a scratch directory and a scratch `NXF_HOME`; never write under `~/.nextflow` or `~/.cache`.
- The real store `/tmp/cas-exploration/store` is never opened by anything that can write: no `sqlite3` without `-readonly`, no Index or CasSession, no `nf-blocks:*` command, no test pointed at it. Copy it (`cp -R`) into a scratch directory for acceptance.
- Quote heredoc delimiters (`<<'EOF'`) whenever the body has backticks or `$`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Implementers commit on their own worktree branch; the orchestrator integrates by cherry-pick.
- Test commands: Spock with `./gradlew test --tests '<FQCN>'`; web with `cd web && npm ci && npm run gen && node --test test/<name>.test.mjs` (`npm run gen` writes `web/src/generated/schema.json` from DESIGN.md; `npm test` does gen plus every test); Gate Python with `python3 -m unittest gate.<module>` from the repo root. `make test` is `./gradlew test`, which does not run the web suite; run `cd web && npm test` as well.
- `web/test/fixtures/schema.sql` must equal the index DDL text exactly (ExplorerQueriesTest "the page tests build their databases from the index schema itself"). Regenerate it from a fresh `Index.open` with `sqlite3 <scratch>/i.sqlite "SELECT sql || ';' FROM sqlite_master WHERE sql IS NOT NULL AND tbl_name NOT IN ('block_absent','block_present') ORDER BY rowid"` on a scratch file the test itself creates, or copy the expected text from the failing assertion.

## Review Focus

- A directory input from two items holding identical directories: `fromStore` emits `cas://<item1>/<name>` and `cas://<item2>/<name>`, so the InputSet holds two Inputs differing only in `from`, at two positions, and each consumer item's lineage names only the item it read. Pinned in Task A2 (`two directory paths from two items stay two inputs`) and Task I1 (`input_origin is keyed by position, so identical directories keep their own item`).
- A task that publishes one of its own staged inputs unchanged (`<workDir>/<name>` is the staged link): the item maps to that one input, not to every input the task staged. Pinned in Task B1 (`a published staged link maps to that input only`).
- `-resume` with every task cached: `onTaskCached` and the already-exists publish path still feed the walk, so the InputSet and ItemLineage CIDs equal the first run's. Pinned in Task B2 (`a cached task's lineage equals the completed task's`) and acceptance step 2.
- An ItemLineage whose `input_set` is not the RunCompletion's `input_set` (positions would point into the wrong list): the index writes no `item_input`/`item_task` rows for it and warns once. Pinned in Task I1 (`an item lineage naming another input set is skipped`).
- An item page whose item CID was published by several runs (the three `scratch-workflow` runs share `people · Dev`): Lineage and Used by show only the run named by the URL's collection; `-` shows every run, grouped. Pinned in Task W3 (`lineage is scoped to the URL's run`) and Task W4 (`used by lists only consumers that read this run's collection`).

---

## Dependency graph

Lanes run in parallel once their inputs have landed. An arrow means "must be integrated before".

```
F1 InputSet schema 2 ----+--> A1 fromStore origins --------+
                         +--> A2 InputRecorder origins ----+--> B2 observer writes ItemLineage --> G1 Gate
F2 ItemLineage + RC 3 ---+--> B1 Join keys + ItemDerivation+
                         +--> C1 Mark + CopyClosure
F1 + F2 -----------------+--> I1 Index v7 ingest --+--> I2 snapshot copy + seed
                                                   +--> I3 page queries --> W2 model methods
F3 ipldsch + web validity --> W3 Lineage panel --> W4 Used by panel
F3 -----------------------> D1 DESIGN prose + README
everything ---------------> Final acceptance (orchestrator checklist)
```

| Task | Needs | Files it owns |
|---|---|---|
| F1 | none | `core/InputSet.groovy`, `InputSetTest` (new) |
| F2 | none | `core/ItemLineage.groovy` (new), `core/Records.groovy` (RunCompletion, `ITEM_LINEAGE`), `ItemLineageTest` (new), `RecordsTest` |
| F3 | none | DESIGN.md ipldsch block only, `web/src/blocks.js`, `web/test/blocks.test.mjs` |
| A1 | F1 | `ext/CasExtension.groovy`, `CasSession.groovy`, `core/Index.groovy` (item refs, selection vias only), `CasExtensionTest`, `IndexSelectionsTest` |
| A2 | F1 | `trace/InputRecorder.groovy`, `InputRecorderTest` |
| B1 | F1, F2 | `trace/Join.groovy`, `trace/ItemDerivation.groovy` (new), `JoinTest`, `ItemDerivationTest` (new) |
| B2 | A1, A2, B1 | `trace/CasObserver.groovy`, `CasObserverTest` |
| C1 | F2 | `core/Mark.groovy`, `core/CopyClosure.groovy`, `MarkTest`, `CopyClosureTest` |
| I1 | F1, F2 | `core/Index.groovy` (DDL, ingest, clear, forget), `web/src/config.js`, `web/test/fixtures/schema.sql`, `IndexItemLineageTest` (new), version pins |
| I2 | I1 | `core/IndexSnapshot.groovy`, `core/Index.groovy` (seed constants), `IndexSnapshotTest`, `IndexSeedTest` |
| I3 | I1 | `web/src/queries.json`, `ExplorerQueriesTest` |
| W2 | F3, I3 | `web/src/model.js`, `web/test/item-lineage-model.test.mjs` (new) |
| W3 | F3 | `web/src/views/item.js`, `web/test/item-lineage-view.test.mjs` (new) |
| W4 | W3 | `web/src/views/item.js`, `web/test/item-lineage-view.test.mjs` |
| D1 | F3 | DESIGN.md prose, README.md |
| G1 | B2 | `gate/browser_b_assert.py`, `gate/test_browser_b_assert.py` |

A1 and I1 both edit `core/Index.groovy`, in different regions (A1: the `items`/`selectionItems` methods near lines 1811 and 2138 and one SQL constant near line 51; I1: DDL, ingest, `clearRun`, `forget`). Cherry-pick A1 before I1 if both are ready; the conflict, if any, is textual.

---

## Foundation

### Task F1: InputSet schema 2 with `Input.from` and `Origin`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/InputSet.groovy`
- Test: `src/test/groovy/robsyme/cas/core/InputSetTest.groovy` (new)

**Interfaces:**
- Consumes: `Records.head(kind, schema)`, `Records.schemaOf`, `Records.cid`, `Cid.bytes()`.
- Produces:
  - `InputSet.SCHEMA = 2`, `InputSet.READABLE = [1, 2]`.
  - `@Canonical static final class InputSet.Origin implements Comparable<Origin> { Cid item; Cid collection; Cid selection }`; `item` required; `compareTo` by (item, collection, selection) with `Arrays.compareUnsigned(a.bytes(), b.bytes())`, nulls first; `Map<String, Object> toCbor()` (`item`, `collection`, `selection`, nulls kept); `static Origin fromCbor(Object)`.
  - `InputSet.Input` gains a 7th property `List<Origin> from` (null when absent). Existing 6-argument construction keeps working (`from == null`).
  - `InputSet.Input withFrom(Collection<Origin> origins)`: a copy whose `from` is sorted, without repeats, and null when `origins` is null or empty. Every producer of an Input with Origins goes through it.
  - `int InputSet.positionOf(InputSet.Input input)`: the index of an equal input in `inputs` (binary search on `compareTo`), or -1.

- [ ] **Step 1: Write the failing tests** in `InputSetTest.groovy` (Spock; CIDs from `Fixtures.contentCid` and `Fixtures.cidOf`)

```groovy
def item1 = Fixtures.cidOf([kind: 'OutputItem', n: 1]); def item2 = Fixtures.cidOf([kind: 'OutputItem', n: 2])
def coll = Fixtures.cidOf([kind: 'OutputCollection']); def sel = Fixtures.cidOf([kind: 'Selection'])
def dat = Fixtures.contentCid('Dev_Patel.dat')

def 'an input with origins writes schema 2 and reads back to itself'() {
    given:
    def input = new InputSet.Input('task', 'Dev_Patel.dat', dat, 12L, 'Dev', null).withFrom([new InputSet.Origin(item1, coll, sel)])
    def set = new InputSet('rob', [input])
    expect:
    set.toCbor().schema == 2
    set.toCbor().inputs[0].from == [[item: item1, collection: coll, selection: sel]]
    InputSet.fromCbor(set.toCbor()) == set
}

def 'an input without origins has no from key'() {
    expect:
    !new InputSet('rob', [new InputSet.Input('task', 'a', dat, 1L, null, null)]).toCbor().inputs[0].containsKey('from')
    new InputSet.Input('task', 'a', dat, 1L, null, null).withFrom([]).from == null
}

def 'a schema 1 block still reads'() {
    given:
    def block = [kind: 'InputSet', schema: 1, asserted_by: 'rob', inputs: [[role: 'task', name: 'a', address: dat, size: 1, sample: null, reason: null]]]
    expect:
    InputSet.fromCbor(block).inputs[0].from == null
}

def 'origins sort by item, collection, selection as CID bytes, nulls first, without repeats'() {
    given:
    def raw = [new InputSet.Origin(item2, coll, sel), new InputSet.Origin(item1, coll, null), new InputSet.Origin(item1, null, sel), new InputSet.Origin(item1, coll, null)]
    def sorted = new InputSet.Input('task', 'a', dat, 1L, null, null).withFrom(raw).from
    expect:
    sorted.size() == 3
    sorted*.item.collect { it.bytes() } == sorted*.item.collect { it.bytes() }.toSorted { a, b -> Arrays.compareUnsigned(a, b) }
    sorted[0].collection == null
}

def 'two inputs differing only in from are two inputs, ordered by from'() {
    given:
    def a = new InputSet.Input('task', 'qc', null, null, null, 'unaddressed').withFrom([new InputSet.Origin(item1, coll, null)])
    def b = new InputSet.Input('task', 'qc', null, null, null, 'unaddressed').withFrom([new InputSet.Origin(item2, coll, null)])
    def set = new InputSet('rob', [b, a])
    expect:
    set.inputs.size() == 2
    set.positionOf(a) == (Arrays.compareUnsigned(item1.bytes(), item2.bytes()) < 0 ? 0 : 1)
    set.positionOf(new InputSet.Input('task', 'zz', dat, 1L, null, null)) == -1
}
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.InputSetTest'`
Expected: FAIL, `withFrom` and `Origin` do not exist.

- [ ] **Step 3: Implement** `Origin`, `Input.from`, `withFrom`, `positionOf` in `core/InputSet.groovy`

`toCbor` writes `Records.head(Records.INPUT_SET, SCHEMA)` and puts `from` only when non-null. `fromCbor` accepts `READABLE`; at schema 2 reads an optional `from` list through `withFrom`; at schema 1 ignores it. `compareTo` appends `from` last: null before non-null, then element-wise `Origin.compareTo`, then shorter list first. Update the class comment's sort description.

- [ ] **Step 4: Run to see them pass, plus the InputSet users**

Run: `./gradlew test --tests 'robsyme.cas.core.InputSetTest' --tests 'robsyme.cas.core.P4BlocksTest' --tests 'robsyme.cas.core.IndexRunInputTest' --tests 'robsyme.cas.trace.InputRecorderTest'`
Expected: PASS. A test that pins `schema == 1` on a written InputSet is updated to 2.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/InputSet.groovy src/test/groovy/robsyme/cas/core/InputSetTest.groovy
git commit -m "core: InputSet schema 2, inputs carry the fromStore items they came from"
```

### Task F2: ItemLineage record and RunCompletion schema 3

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/ItemLineage.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (constant near line 35; `RunCompletion` at lines 1087-1199)
- Test: `src/test/groovy/robsyme/cas/core/ItemLineageTest.groovy` (new), `src/test/groovy/robsyme/cas/core/RecordsTest.groovy`

**Interfaces:**
- Produces:
  - `Records.ITEM_LINEAGE = 'ItemLineage'`.
  - `class ItemLineage` (`@CompileStatic @EqualsAndHashCode`), `SCHEMA = 1`, fields `final String assertedBy; final Cid inputSet; final List<Derived> items`; constructor `ItemLineage(String assertedBy, Cid inputSet, Collection<Derived> items)` sorts `items` by item CID bytes and refuses two entries for one item, a blank `assertedBy` and a null `inputSet`; `Map<String, Object> toCbor()`; `static ItemLineage fromCbor(Map block)`; `Derived itemFor(Cid item)` (null when absent).
  - `@Canonical static final class ItemLineage.Derived { Cid item; List<Integer> inputs; List<TaskRef> tasks }`; `static Derived of(Cid item, Collection<Integer> inputs, Collection<TaskRef> tasks)` sorts and dedupes both lists; the ItemLineage constructor refuses a Derived with no inputs and no tasks, a negative position, and a null item.
  - `@Canonical static final class ItemLineage.TaskRef implements Comparable<TaskRef> { String hash; String process; String tag }`; `hash` must match `/[0-9a-f]{32}/`, `process` non-blank, `tag` nullable; sorted by hash, then process, then tag (nulls first).
  - `RunCompletion.SCHEMA = 3`, `READABLE = [1, 2, 3]`, new field `final Cid itemLineage` (constructor key `itemLineage`), written as `item_lineage` (null allowed) at schema 3; `fromCbor` requires `item_lineage` at schema 3 and leaves it null at 1 and 2.

- [ ] **Step 1: Write the failing tests**

`ItemLineageTest.groovy`:

```groovy
def set = Fixtures.cidOf([kind: 'InputSet']); def intro = Fixtures.cidOf([kind: 'OutputItem', n: 'intros-Dev'])
def circle = Fixtures.cidOf([kind: 'OutputItem', n: 'circle'])
def introduce = new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', 'Introduce', 'Dev')
def friend = new ItemLineage.TaskRef('fedcba9876543210fedcba9876543210', 'FriendCircle', null)

def 'an ItemLineage writes the spec shape and reads back to itself'() {
    given:
    def l = new ItemLineage('rob', set, [ItemLineage.Derived.of(circle, [3, 0, 2, 1, 2], [friend]), ItemLineage.Derived.of(intro, [1], [introduce])])
    def cbor = l.toCbor()
    expect:
    cbor.kind == 'ItemLineage' && cbor.schema == 1 && cbor.asserted_by == 'rob' && cbor.input_set == set
    cbor.items*.item.collect { it.bytes() } == [circle, intro].collect { it.bytes() }.toSorted { a, b -> Arrays.compareUnsigned(a, b) }
    l.itemFor(circle).inputs == [0, 1, 2, 3]
    l.itemFor(intro).tasks == [introduce]
    cbor.items.find { it.item == intro }.tasks == [[hash: introduce.hash, process: 'Introduce', tag: 'Dev']]
    ItemLineage.fromCbor(cbor) == l
}

def 'an empty items list is a valid ItemLineage'() {
    expect:
    new ItemLineage('rob', set, []).toCbor().items == []
}

def 'refuses an item with neither input nor task, a repeated item, a bad hash'() {
    when: new ItemLineage('rob', set, [ItemLineage.Derived.of(intro, [], [])])
    then: thrown(IllegalArgumentException)
    when: new ItemLineage('rob', set, [ItemLineage.Derived.of(intro, [0], []), ItemLineage.Derived.of(intro, [1], [])])
    then: thrown(IllegalArgumentException)
    when: new ItemLineage.TaskRef('ABC', 'P', null)
    then: thrown(IllegalArgumentException)
}
```

Add to `RecordsTest.groovy`:

```groovy
def 'a RunCompletion is written at schema 3 with item_lineage, null allowed'() {
    // build with the file's existing RunCompletion fixture args plus itemLineage: lineage / null
    expect:
    withLineage.toCbor().schema == 3
    withLineage.toCbor().item_lineage == lineage
    withoutLineage.toCbor().containsKey('item_lineage') && withoutLineage.toCbor().item_lineage == null
    RunCompletion.fromCbor(withLineage.toCbor()).itemLineage == lineage
}

def 'a schema 2 RunCompletion still reads, with no item lineage'() {
    expect:
    RunCompletion.fromCbor(schema2Block).itemLineage == null   // schema2Block: the file's existing schema 2 fixture map
}
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.ItemLineageTest' --tests 'robsyme.cas.core.RecordsTest'`
Expected: FAIL, `ItemLineage` does not exist; schema is 2.

- [ ] **Step 3: Implement** `core/ItemLineage.groovy` after the pattern of `core/InputSet.groovy` (class comment naming spec §5.2 and DESIGN.md §6), and RunCompletion schema 3 in `Records.groovy`

`toCbor` writes `inputs` as a list of `Long` and `tasks` as maps with `hash`, `process`, `tag` (null kept). Update the RunCompletion class comment (`Written at schema 3 since 2026-10-26: it links item_lineage`).

- [ ] **Step 4: Run to see them pass; fix every pin on RunCompletion schema 2**

Run: `grep -rn "schema == 2\|SCHEMA == 2" src/test/groovy | grep -i completion` and update each pin on a written RunCompletion to 3. Then `./gradlew test --tests 'robsyme.cas.core.*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/ItemLineage.groovy src/main/groovy/robsyme/cas/core/Records.groovy src/test/groovy/robsyme/cas/core/ItemLineageTest.groovy src/test/groovy/robsyme/cas/core/RecordsTest.groovy
git commit -m "core: ItemLineage record, RunCompletion schema 3 links it"
```

### Task F3: DESIGN.md ipldsch block and the page's block validity

**Files:**
- Modify: `DESIGN.md` ipldsch block only (lines 421-645: `Block` union at 422-437, `RunCompletion` at ~534, `InputSet`/`Input` at ~618-631)
- Modify: `web/src/blocks.js` (`validKind`, lines 26-41)
- Test: `web/test/blocks.test.mjs` (existing file; add the tests below, merging the imports with the file's own)

**Interfaces:**
- Produces: the schema types exactly as spec §5.1 and §5.2 write them, with `from optional [Origin]` on `Input`, `item_lineage optional nullable &ItemLineage` on `RunCompletion`, `ItemLineage "ItemLineage"` in the `Block` union. `validKind` rules: RunCompletion schema 1 has no `providers` and no `item_lineage`; schema 2 has `providers` and no `item_lineage`; schema 3 has both keys (`item_lineage` may be null). InputSet schema 1 has no input with `from`; schema 2 any. ItemLineage schema 1 only. Export `validKind` from `blocks.js` (it is a module-private `function` at line 26 today) so the test can call it.

- [ ] **Step 1: Write the failing tests** in `web/test/blocks.test.mjs`

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { validBlock } from '../src/schema.js'
import { validKind } from '../src/blocks.js'
import { block, rawCid } from './fixture.mjs'

const item = block({ kind: 'OutputItem', n: 1 }).cid
const coll = block({ kind: 'OutputCollection', n: 1 }).cid
const sel = block({ kind: 'Selection', n: 1 }).cid
const set = block({ kind: 'InputSet', n: 1 }).cid
const input = (extra = {}) => ({ role: 'task', name: 'Dev_Patel.dat', address: rawCid('dev'), size: 12, sample: 'Dev', reason: null, ...extra })
const inputSet = (schema, inputs) => ({ kind: 'InputSet', schema, asserted_by: 'rob', inputs })
const lineage = { kind: 'ItemLineage', schema: 1, asserted_by: 'rob', input_set: set,
  items: [{ item, inputs: [0], tasks: [{ hash: '0123456789abcdef0123456789abcdef', process: 'Introduce', tag: 'Dev' }] }] }

test('an InputSet input may carry from at schema 2; schema 1 may not', () => {
  const withFrom = input({ from: [{ item, collection: coll, selection: sel }, { item, collection: null, selection: null }] })
  assert.equal(validBlock(inputSet(2, [withFrom])) && validKind(inputSet(2, [withFrom])), true)
  assert.equal(validBlock(inputSet(1, [input()])) && validKind(inputSet(1, [input()])), true)
  assert.equal(validKind(inputSet(1, [withFrom])), false)
})

test('an ItemLineage block validates; a TaskRef without process does not', () => {
  assert.equal(validBlock(lineage), true)
  assert.equal(validBlock({ ...lineage, items: [{ item, inputs: [0], tasks: [{ hash: 'x', tag: null }] }] }), false)
})

test('a RunCompletion carries item_lineage exactly at schema 3, null allowed', () => {
  const rc = (schema, extra) => ({ kind: 'RunCompletion', schema, providers: {}, ...extra })
  assert.equal(validKind(rc(3, { item_lineage: null })), true)
  assert.equal(validKind(rc(3, { item_lineage: block(lineage).cid })), true)
  assert.equal(validKind(rc(3, {})), false)
  assert.equal(validKind(rc(2, { item_lineage: null })), false)
  assert.equal(validKind(rc(2, {})), true)
})
```

Also assert that a full schema 3 RunCompletion built like the existing ones in `web/test/fixture.mjs` plus `item_lineage` passes `validBlock`.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm ci && npm run gen && node --test test/blocks.test.mjs`
Expected: FAIL on `validBlock(lineage)` and the schema 3 rules.

- [ ] **Step 3: Edit the ipldsch block and `validKind`**

Copy the spec's types into the block with its comments; keep `InputSet.schema`'s comment as `# 1 or 2; 2 adds Input.from`. Keep the block the single ```` ```ipldsch ```` fence `schema-gen.mjs` extracts.

- [ ] **Step 4: Run to see them pass, then the whole web suite**

Run: `node --test test/blocks.test.mjs` then `npm test`
Expected: PASS, 0 failing.

- [ ] **Step 5: Commit**

```bash
git add DESIGN.md web/src/blocks.js web/test/blocks.test.mjs
git commit -m "schema: ItemLineage, Input.from and RunCompletion.item_lineage in DESIGN.md §6; the page accepts them"
```

---

## Plugin lane A: origins

### Task A1: `fromStore` records each emitted path's Origins on CasSession

**Files:**
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (per-run maps near line 221)
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`items` at ~2138, `selectionItems` at ~1811, a SQL constant beside `SQL_SELECTION_ITEMS` at ~51)
- Modify: `src/main/groovy/robsyme/cas/ext/CasExtension.groovy` (`resolveItems` 75-106, `resolveSelection` 137-169, `restore`/`pathFor` 188-241)
- Test: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`, `src/test/groovy/robsyme/cas/core/IndexSelectionsTest.groovy`

**Interfaces:**
- Consumes: `InputSet.Origin(Cid item, Cid collection, Cid selection)` (F1).
- Produces:
  - `CasSession`: `final ConcurrentHashMap<Path, Set<InputSet.Origin>> origins`; `void addOrigins(Path path, Collection<InputSet.Origin> origins)` (merges into a concurrent set); B2 passes `cas.origins` to `InputRecorder.collect`.
  - `Index`: `@Canonical static final class ItemRef { Cid item; Cid collection }`; `List<ItemRef> itemRefs(Cid completion, String outputName, Map<String, Object> where)`; `items(...)` becomes `itemRefs(...)*.item`. `Map<Cid, List<Cid>> selectionVias(Cid selection)`: every item the Selection reaches (nested included), keys in `selectionItems` order, each value the distinct non-null `via_cid`s of its `collection_item` rows, sorted, or `[]` when it was picked by query only; same "not indexed" refusal as `selectionItems`. New `private static final String SQL_SELECTION_VIAS` (the recursive `reach` CTE of `SQL_SELECTION_ITEMS`, selecting `DISTINCT ci.item_cid, ci.via_cid`).
  - `fromStore` behaviour: `run:`/`output:` gives `Origin(item, collection, null)`; `selection:` gives one `Origin(item, via, selection)` per via, or one `Origin(item, null, selection)` when the item has no via. `selection` is always the Selection the call named (the outer one for nested Selections). Every Path `pathFor` returns for that item gets the item's Origins; a declined leaf (`null`) gets none.

- [ ] **Step 1: Write the failing tests**

In `IndexSelectionsTest.groovy`, beside the nested test at ~line 84:

```groovy
def 'selectionVias gives each item its via collections, [] when picked by query'() {
    // reuse the fixture that builds outer/inner and itemA..C; give itemA a via (collA) and itemC none
    expect:
    index.selectionVias(outer).keySet() as List == index.selectionItems(outer)
    index.selectionVias(outer)[itemA] == [collA]
    index.selectionVias(outer)[itemC] == []
}
```

In `CasExtensionTest.groovy` (existing Mock(Session) setup, `ext.init(session)`, store fixtures):

```groovy
def 'run/output: each emitted path carries the item and its collection, no selection'() {
    when: def emitted = paths(ext.fromStore(run: runRef, output: 'people'))
    then: emitted.every { cas.origins[it] == [new InputSet.Origin(itemOf(it), peopleCollection, null)] as Set }
}

def 'selection: one origin per via, the selection the call named'() {
    when: paths(ext.fromStore(selection: outer.toString()))
    then:
    cas.origins[pathOfDev] == [new InputSet.Origin(devItem, peopleCollection, outer)] as Set
    cas.origins[pathOfTwoVias] == [new InputSet.Origin(twoViaItem, collA, outer), new InputSet.Origin(twoViaItem, collB, outer)] as Set
    cas.origins[pathOfQueryPick] == [new InputSet.Origin(queryItem, null, outer)] as Set
}

def 'a raw leaf two items share gives one path with both origins'() {
    expect: cas.origins[sharedPath]*.item as Set == [itemOne, itemTwo] as Set
}

def 'the emitted cas:// paths are unchanged'() {
    expect: emitted*.toUri()*.toString() == expectedUris   // the same URIs the existing tests pin: cas://<leaf>/<name>, cas://<item>/<dir name>
}
```

`paths(channel)` and the fixture names follow the file's existing helpers; build the extra items and Selections the same way the nesting test at ~line 381 does.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest' --tests 'robsyme.cas.core.IndexSelectionsTest'`
Expected: FAIL, `origins` and `selectionVias` do not exist.

- [ ] **Step 3: Implement**

`resolveItems` iterates `index.itemRefs(...)`; `resolveSelection` iterates `index.selectionVias(selection)`. `restore` gains a `Collection<InputSet.Origin> origins` argument threaded to `pathFor`, which calls `cas.addOrigins(path, origins)` on every non-null Path it returns. `ExploreCommand` and `IndexSnapshot` keep calling `selectionItems` unchanged.

- [ ] **Step 4: Run to see them pass, plus Index's item tests**

Run: `./gradlew test --tests 'robsyme.cas.ext.CasExtensionTest' --tests 'robsyme.cas.core.IndexSelectionsTest' --tests 'robsyme.cas.core.IndexTest' --tests 'robsyme.cas.trace.FromStoreHintTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/CasSession.groovy src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/ext/CasExtension.groovy src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy src/test/groovy/robsyme/cas/core/IndexSelectionsTest.groovy
git commit -m "fromStore: remember which item, output and Selection gave each emitted path"
```

### Task A2: InputRecorder attaches Origins, reports what each staged file became, reads `params.input` safely

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/InputRecorder.groovy` (constructor ~203, `recordTask` 215-220, `recordSamplesheet` ~269, `record` 291-316, `put`/`Entry` 133-147 and ~460-471, `collect` 520-576)
- Test: `src/test/groovy/robsyme/cas/trace/InputRecorderTest.groovy`

**Interfaces:**
- Consumes: `InputSet.Origin`, `Input.withFrom` (F1).
- Produces:
  - `List<Staged> recordTask(TaskRun task)`: returns the list it recorded (empty for a null task). B2 hands it to `ItemDerivation.noteTask`.
  - `List<InputSet.Input> collect(long waitMillis, Map<Path, Set<InputSet.Origin>> origins)`; `collect(long)` calls it with an empty map. An entry whose `origin` Path is a key of `origins` becomes `input.withFrom(origins[origin])`.
  - `Map<Staged, InputSet.Input> getCollectedByStaged()`: valid after `collect`; each `Staged` that `record` turned into a kept entry maps to the exact Input `collect` returned for it. A Staged that was skipped (under the run's work dir, under a storeDir, not a Path) is absent. `record` remembers the Staged that created or matched each entry key; several Staged may share one entry.
  - `params.input` is read only when `params.containsKey('input')`, in the constructor and in `recordSamplesheet`.

- [ ] **Step 1: Write the failing tests** in `InputRecorderTest.groovy` (beside `'a cas:// input keeps its known address without hashing'` at ~line 480, same `CasFileSystemProvider().getPath(URI.create(...))` construction)

```groovy
def 'a cas:// input from fromStore lists its origins'() {
    given:
    def path = new CasFileSystemProvider().getPath(URI.create("cas://${dat}/Dev_Patel.dat"))
    def recorder = new InputRecorder(workDir, [:], true)
    def staged = [new InputRecorder.Staged(path, workDir.resolve('ab/cdef/Dev_Patel.dat'), 'Dev')]
    recorder.recordStaged(staged)
    when:
    def inputs = recorder.collect(-1, [(path): [new InputSet.Origin(devItem, peopleColl, sel)] as Set])
    then:
    inputs*.from == [[new InputSet.Origin(devItem, peopleColl, sel)]]
    recorder.collectedByStaged[staged[0]].is(inputs[0])
}

def 'two directory paths from two items stay two inputs'() {
    given: 'cas://<item1>/qc and cas://<item2>/qc, the same manifest behind both'
    // stub the provider attributes as the existing directory-input test does
    when:
    def set = new InputSet('rob', recorder.collect(-1, [(p1): [o1] as Set, (p2): [o2] as Set]))
    then:
    set.inputs.size() == 2
    set.inputs*.from == [[o1], [o2]].sort { a, b -> a[0] <=> b[0] }
}

def 'a file under the run work dir is not in collectedByStaged'() {
    given:
    def intermediate = new InputRecorder.Staged(workDir.resolve('aa/bbbb/Dev_intro.txt'), workDir.resolve('cc/dddd/Dev_intro.txt'), 'Dev')
    recorder.recordStaged([intermediate])
    expect:
    recorder.collect(-1, [:]) == []
    !recorder.collectedByStaged.containsKey(intermediate)
}

def 'recordTask returns what it staged'() {
    expect: recorder.recordTask(taskWith(path)) == [new InputRecorder.Staged(path, storePath, null)]   // taskWith: the file's existing TaskRun helper
}

def 'a params map without input is never asked for it'() {
    given:
    def params = Mock(Map) { containsKey('input') >> false; entrySet() >> [].toSet() }
    when:
    def recorder = new InputRecorder(workDir, null, params, true)
    recorder.recordSamplesheet()
    then:
    0 * params.get('input')
}
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.InputRecorderTest'`
Expected: FAIL, `collect(long, Map)` does not exist; `params.get('input')` is called.

- [ ] **Step 3: Implement** the four changes in `InputRecorder.groovy`. `collect` builds `collectedByStaged` in the same loop that builds `out`, after the samplesheet de-duplication (a removed Input leaves no Staged pointing at it). Store the map as a field replaced on each `collect`.

- [ ] **Step 4: Run to see them pass, plus the special-files spec**

Run: `./gradlew test --tests 'robsyme.cas.trace.InputRecorderTest' --tests 'robsyme.cas.trace.InputRecorderSpecialFilesTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/InputRecorder.groovy src/test/groovy/robsyme/cas/trace/InputRecorderTest.groovy
git commit -m "InputRecorder: inputs carry fromStore origins; read params.input only when declared"
```

---

## Plugin lane B: derivation

### Task B1: Join exposes each item's join keys; ItemDerivation walks tasks to run inputs

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/Join.groovy` (`JoinedOutput` 39-51, `join` 102-139, `build`/`leafFor` 146-212)
- Create: `src/main/groovy/robsyme/cas/trace/ItemDerivation.groovy`
- Test: `src/test/groovy/robsyme/cas/trace/JoinTest.groovy`, `src/test/groovy/robsyme/cas/trace/ItemDerivationTest.groovy` (new)

**Interfaces:**
- Consumes: `InputRecorder.Staged` (existing, `@Canonical(source, store, sample)`), `InputSet`/`InputSet.Input`/`positionOf` (F1), `ItemLineage.Derived`/`TaskRef` (F2).
- Produces:
  - `Join.JoinedOutput` gains `final List<Cid> itemCids` and `final List<List<String>> itemKeys`, both aligned with `items` (arrival order). `itemKeys[i]` holds the join key (`Coordinates.key`) of each of item i's `cas://` leaves in leaf order; a non-`cas://` leaf contributes nothing.
  - `class ItemDerivation` (thread-safe; `ConcurrentHashMap` inside):
    - `void notePublish(String joinKey, Path source)`: keeps `source.toAbsolutePath().normalize()`; the last call for a key wins.
    - `void noteTask(String hash, Path workDir, String process, String tag, List<InputRecorder.Staged> staged)`: keyed by hash; keeps only the arguments (never a TaskRun).
    - `void noteTask(TaskRun task, List<InputRecorder.Staged> staged)`: reads `task.hash.toString()`, `task.workDir`, `task.processor?.name`, and the tag as `task.config.containsKey('tag') ? String.valueOf(task.config.tag).trim() : null` inside a try that gives null; ignores a task with no hash or no work dir.
    - `List<ItemLineage.Derived> derive(List<Join.JoinedOutput> outputs, InputSet set, Map<InputRecorder.Staged, InputSet.Input> runInputs)`: one Derived per distinct item CID that has at least one input position or one task, merged across outputs.

Walk, per item (spec §5.2 step 3):

```
for each join key of the item:
    source = published[key]; if none: next
    task = taskHolding(source)                // walk source.parent upward until a noted workDir matches; none -> next
    tasks += TaskRef(task)
    follow(task, source)
follow(task, source):
    if (task.hash, passthrough-or-'*') already visited for this item: return
    picked = task.staged where normalize(s.store) == source   // a published staged link
    for s in (picked ?: task.staged):
        input = runInputs[s]
        if input: positions += set.positionOf(input)          // skip -1
        else if s.source is a Path and taskHolding(s.source): follow(taskHolding(s.source), normalize(s.source))
```

The visited key is `hash + '\u0000' + (picked ? source : '*')`. Work dirs are compared normalized and absolute; no file system access.

- [ ] **Step 1: Write the failing tests**

`JoinTest.groovy`:

```groovy
def 'each joined item carries its cid and its leaves join keys'() {
    // reuse the file's two-leaf item fixture
    expect:
    out.itemCids == out.items.collect { DagCbor.cidOf(DagCbor.encode(it.toCbor())) }
    out.itemKeys[0] == [Coordinates.key(bamPath), Coordinates.key(baiPath)]
}
```

`ItemDerivationTest.groovy` (pure; Paths under a `@TempDir` that need not exist; the spec's consumer run: four `Introduce` tasks each stage one `cas://` datafile and publish `<name>_intro.txt`; `FriendCircle` stages the four intro files and publishes `circle.txt`):

```groovy
def 'intros name their one datafile and Introduce; circle names all four through the intermediate tasks'() {
    given:
    def d = new ItemDerivation()
    def names = ['Alice', 'Bob', 'Carol', 'Dev']
    def staged = names.collectEntries { n -> [(n): new InputRecorder.Staged(casPath(n), work("i${n}").resolve("${n}_Patel.dat"), n)] }
    names.each { n ->
        d.noteTask(hashOf(n), work("i${n}"), 'Introduce', n, [staged[n]])
        d.notePublish("out/intros/${n}_intro.txt", work("i${n}").resolve("${n}_intro.txt"))
    }
    d.noteTask(hashOf('circle'), work('c'), 'FriendCircle', null,
        names.collect { n -> new InputRecorder.Staged(work("i${n}").resolve("${n}_intro.txt"), work('c').resolve("${n}_intro.txt"), null) })
    d.notePublish('out/circle/circle.txt', work('c').resolve('circle.txt'))
    def inputs = names.collect { n -> new InputSet.Input('task', "${n}_Patel.dat", raw(n), 10L, n, null) }
    def set = new InputSet('rob', inputs)
    def runInputs = names.collectEntries { n -> [(staged[n]): inputs[names.indexOf(n)]] }
    when:
    def derived = d.derive([joined('intros', names.collect { n -> [item(n), ["out/intros/${n}_intro.txt"]] }),
                            joined('circle', [[item('circle'), ['out/circle/circle.txt']]])], set, runInputs)
    then:
    derived.find { it.item == item('Dev') }.inputs == [set.positionOf(inputs[3])]
    derived.find { it.item == item('Dev') }.tasks == [new ItemLineage.TaskRef(hashOf('Dev'), 'Introduce', 'Dev')]
    derived.find { it.item == item('circle') }.inputs == [0, 1, 2, 3]
    derived.find { it.item == item('circle') }.tasks*.process == ['FriendCircle']
}

def 'a published staged link maps to that input only'() {
    // task P stages in.dat and ref.fa (both run inputs) and publishes <workDir>/in.dat, the staged link
    expect: derived.find { it.item == passItem }.inputs == [set.positionOf(inDat)]
}

def 'a source outside every task gives no entry; a value-only item gives no entry'() {
    expect: derived*.item.every { it != outsideItem && it != valuesItem }
}

def 'two tasks staging each other end'() {
    // A stages a file under B's work dir and B one under A's; derive returns
    expect: derived.find { it.item == loopItem }.inputs == [set.positionOf(seed)]
}

def 'one item in two outputs is one entry with the union'() {
    expect: derived.count { it.item == shared } == 1
}

def 'noteTask(TaskRun) copies hash, work dir, process and tag'() {
    // build a real TaskRun as CasObserverTest.taskEvent does (~line 1753), set task.workDir and a config tag 'Dev'
    expect: noted.hash ==~ /[0-9a-f]{32}/ && noted.process == 'Introduce' && noted.tag == 'Dev'
}
```

`work(name)`, `casPath(name)`, `raw(name)`, `item(name)`, `hashOf(name)` (32 hex chars from the name's md5) and `joined(name, [[cid, keys]])` (a `JoinedOutput` built directly) are local helpers. Expose the noted task for the last test through a package-scope accessor.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.JoinTest' --tests 'robsyme.cas.trace.ItemDerivationTest'`
Expected: FAIL, `itemKeys` and `ItemDerivation` do not exist.

- [ ] **Step 3: Implement** the Join fields (collect keys in `leafFor` next to `leafPaths`) and `ItemDerivation` with the walk above.

- [ ] **Step 4: Run to see them pass**

Run: `./gradlew test --tests 'robsyme.cas.trace.JoinTest' --tests 'robsyme.cas.trace.ItemDerivationTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/Join.groovy src/main/groovy/robsyme/cas/trace/ItemDerivation.groovy src/test/groovy/robsyme/cas/trace/JoinTest.groovy src/test/groovy/robsyme/cas/trace/ItemDerivationTest.groovy
git commit -m "trace: derive each published item's run inputs and publishing tasks"
```

### Task B2: The observer writes ItemLineage before the RunCompletion

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`onFilePublish` 167-185, `recordInputs` 229-238, `writeInputSet`/`inputSetBlock` 275-311, `writeCompletion` ~575-628)
- Test: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`

**Interfaces:**
- Consumes: `cas.origins` (A1); `recordTask` returning `List<Staged>`, `collect(long, Map)`, `collectedByStaged` (A2); `ItemDerivation`, `JoinedOutput.itemKeys` (B1); `ItemLineage`, `RunCompletion` `itemLineage` (F2).
- Produces: every RunCompletion at schema 3. `item_lineage` is the CID of `new ItemLineage(cas.assertedBy, inputSetCid, derivation.derive(joined.outputs, inputSet, byStaged))`, written with `putBlock(cbor, 'ItemLineage')` after the InputSet and before the RunCompletion; null when the InputSet is null or deriving throws (warn on the console as the InputSet does: `nf-blocks: the run's item lineage could not be made, so its RunCompletion records none: <message>`). A failed `putBlock` aborts the run (rule 3), as for the InputSet.

Wiring: `onFilePublish` calls `derivation.notePublish(key, event.source)` when the source is non-null, after the `isCasTarget` check and before the provenance checks. `recordInputs` keeps the returned staged list and calls `derivation.noteTask(task, staged)` inside its own try (a failure warns). `inputSetBlock` returns the `InputSet` and the `collectedByStaged` map (a private holder) instead of the CBOR map, and passes `cas.origins` to `collect`.

- [ ] **Step 1: Write the failing tests** in `CasObserverTest.groovy` (its `bind` session, `taskEvent` helper at ~1753 with `task.workDir` set, and a `FilePublishEvent(source, target, null)` with a work-dir source)

```groovy
def 'a run with a published task output writes an ItemLineage linked from a schema 3 RunCompletion'() {
    // one task: stages cas://<dat>/Dev_Patel.dat (origin Dev item via people, Selection S), publishes Dev_intro.txt as output intros
    when: observer.onFlowComplete()
    then:
    completion.schema == 3
    def lineage = ItemLineage.fromCbor(read(completion.item_lineage))
    lineage.inputSet == completion.input_set
    def set = InputSet.fromCbor(read(completion.input_set))
    set.schema == 2
    set.inputs.find { it.name == 'Dev_Patel.dat' }.from == [new InputSet.Origin(devItem, peopleColl, selection)]
    lineage.items*.inputs == [[set.positionOf(set.inputs.find { it.name == 'Dev_Patel.dat' })]]
    lineage.items[0].tasks*.process == ['Introduce']
}

def 'a cached task gives the same lineage as a completed one'() {
    // the same events, onTaskCached instead of onTaskComplete, publish through the already-exists path
    expect: cachedLineageCid == completedLineageCid && cachedInputSetCid == completedInputSetCid
}

def 'no InputSet means item_lineage null'() {
    // newInputRecorder seam returns a recorder whose collect throws
    expect: completion.input_set == null && completion.containsKey('item_lineage') && completion.item_lineage == null
}

def 'the Output Index File publish (no source) is ignored'() {
    when: observer.onFilePublish(new FilePublishEvent(null, indexTarget, null))
    then: noExceptionThrown()
}
```

`read(cid)` decodes a block from the test's store, as the file's other completion tests do.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest'`
Expected: FAIL, the RunCompletion is schema 2 with no `item_lineage`.

- [ ] **Step 3: Implement** the wiring above. Existing tests that read `completion.schema == 2` move to 3.

- [ ] **Step 4: Run to see them pass, then the trace package**

Run: `./gradlew test --tests 'robsyme.cas.trace.*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy
git commit -m "observer: write the run's ItemLineage and link it from RunCompletion schema 3"
```

---

## Plugin lane C: retention and copy

### Task C1: Mark and the copy closure keep `item_lineage` like `input_set`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Mark.groovy` (`RUN_COMPLETION` case, 456-462; `default:` comment at ~504)
- Modify: `src/main/groovy/robsyme/cas/core/CopyClosure.groovy` (`input_set` placement, 588-598)
- Test: `src/test/groovy/robsyme/cas/core/MarkTest.groovy`, `src/test/groovy/robsyme/cas/core/CopyClosureTest.groovy`

**Interfaces:**
- Consumes: `Records.ITEM_LINEAGE` (F2); RunCompletion maps with `item_lineage`.
- Produces: Mark: `meta(block.get('item_lineage'), META, v.cid, next)`; the `default:` comment lists ItemLineage. CopyClosure: `item_lineage` placed in `RUN_RECORDS` with kind read from the block, nothing under it followed, at every depth.

- [ ] **Step 1: Write the failing tests**

`MarkTest.groovy` (its existing InputSet test is the model):

```groovy
def 'a run keeps its ItemLineage as itself and follows nothing under it'() {
    // a schema 3 RunCompletion whose item_lineage names an InputSet and an upstream OutputItem in another run
    expect:
    marked.contains(lineageCid)
    !marked.contains(upstreamItemCid)          // named by the InputSet's from and not otherwise held
}
```

`CopyClosureTest.groovy`:

```groovy
def 'item_lineage travels in run_records at every depth, nothing under it'() {
    expect:
    [Depth.RESULTS, Depth.LINEAGE].every { depth -> plan(depth).find { it.cid == lineageCid }?.group == 'run_records' }
    plan(Depth.RESULTS).every { it.cid != upstreamItemCid }
}
```

Use the depth names the file already uses.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.MarkTest' --tests 'robsyme.cas.core.CopyClosureTest'`
Expected: FAIL, the ItemLineage is not marked and not in the plan.

- [ ] **Step 3: Implement** the two cases.

- [ ] **Step 4: Run to see them pass, plus sweep and copy**

Run: `./gradlew test --tests 'robsyme.cas.core.MarkTest' --tests 'robsyme.cas.core.CopyClosureTest' --tests 'robsyme.cas.core.SweepTest' --tests 'robsyme.cas.CopyServiceTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Mark.groovy src/main/groovy/robsyme/cas/core/CopyClosure.groovy src/test/groovy/robsyme/cas/core/MarkTest.groovy src/test/groovy/robsyme/cas/core/CopyClosureTest.groovy
git commit -m "sweep and copy: a run's ItemLineage is run metadata, nothing under it followed"
```

---

## Index lane

### Task I1: Index schema 7, ingest of Origins and ItemLineage

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`SCHEMA_VERSION` line 37; `ddl()` 467-563; `ingestRun` 609-638; `ingestInputSet` 709-754; `retryMissingInputSets` 763-790 and its call at ~979; `clearRun` 935-948; `forget` ~2375-2402)
- Modify: `web/src/config.js` (`SCHEMA_VERSION = 7`, `OLDER_SNAPSHOT_PATHS = ['index/v6.sqlite', 'index/v5.sqlite', 'index/v4.sqlite', 'index/v3.sqlite']`)
- Modify: `web/test/fixtures/schema.sql` (regenerated)
- Modify: version pins `src/test/groovy/robsyme/cas/core/IndexTest.groovy:732`, `IndexLogEntryTest.groovy:53`, `IndexSchemaVersionTest.groovy:100`
- Test: `src/test/groovy/robsyme/cas/core/IndexItemLineageTest.groovy` (new; set up as `IndexRunInputTest` does)

**Interfaces:**
- Consumes: `InputSet` schema 2 (F1), `ItemLineage.fromCbor` (F2), `RunCompletion.item_lineage`.
- Produces (DDL, verbatim):

```sql
CREATE TABLE run_input(completion_cid TEXT, position INTEGER, role TEXT, name TEXT, address TEXT, size INTEGER, sample TEXT)
CREATE TABLE input_origin(completion_cid TEXT, position INTEGER, item_cid TEXT, collection_cid TEXT, selection_cid TEXT)
CREATE INDEX input_origin_item ON input_origin(item_cid)
CREATE INDEX input_origin_completion ON input_origin(completion_cid, position)
CREATE TABLE item_input(completion_cid TEXT, item_cid TEXT, positions TEXT)
CREATE INDEX item_input_item ON item_input(item_cid)
CREATE INDEX item_input_completion ON item_input(completion_cid)
CREATE TABLE item_task(completion_cid TEXT, item_cid TEXT, task_hash TEXT, process TEXT, tag TEXT)
CREATE INDEX item_task_item ON item_task(item_cid)
CREATE INDEX item_task_completion ON item_task(completion_cid)
```

  `run_input`'s two existing indexes stay. `positions` is the JSON array text of `Derived.inputs` (`[0,3]`, no spaces; `groovy.json.JsonOutput.toJson`).
- `@PackageScope static final String ITEMLINEAGE_WAIT = 'itemlineage_wait:'` (I2 copies it).
- `ingestInputSet` writes `position` (index in `set.inputs`) and one `input_origin` row per Origin (`collection_cid`/`selection_cid` NULL when null).
- `private void ingestItemLineage(BlockStore store, Cid completion, Cid lineageCid, Cid inputSetCid)`: same shape as `ingestInputSet` (size-capped read, wait record `itemlineage_wait:<completion>` when absent or unreadable for now, warn-once and skip when malformed). When `lineage.inputSet != inputSetCid`, warn once (`item lineage <cid> of run <completion> names input set <x>, not the run's <y>; the run is indexed without item lineage`) and write nothing. Writes one `item_input` row per Derived and one `item_task` row per (Derived, TaskRef). Never writes `block`, `block_ref` or `missing`.
- `private void retryMissingItemLineages(BlockStore store, String member)`, called beside `retryMissingInputSets`.
- `clearRun` and `forget` delete the run's `input_origin`, `item_input`, `item_task` rows and its `itemlineage_wait:` key.

- [ ] **Step 1: Write the failing tests** in `IndexItemLineageTest.groovy`

```groovy
def 'a schema 3 run gives run_input positions, input_origin and item_input rows'() {
    // InputSet: Alice..Dev datafiles, each from = [Origin(<person item>, people, S)]; ItemLineage: intros-Dev -> [3], circle -> [0,1,2,3]
    when: index.ingestRun(store, completion, 'lab')
    then:
    rows('SELECT position, name FROM run_input WHERE completion_cid = ? ORDER BY position', c) == set.inputs.withIndex().collect { i, n -> [n, i.name] }
    rows('SELECT position, item_cid, collection_cid, selection_cid FROM input_origin WHERE completion_cid = ? ORDER BY position', c)[3] == [3, devItem.toString(), people.toString(), sel.toString()]
    rows('SELECT item_cid, positions FROM item_input WHERE completion_cid = ? ORDER BY item_cid', c) as Set == [[introDev.toString(), '[3]'], [circle.toString(), '[0,1,2,3]']] as Set
    rows('SELECT item_cid, task_hash, process, tag FROM item_task WHERE completion_cid = ?', c).find { it[0] == introDev.toString() }[2..3] == ['Introduce', 'Dev']
    rows('SELECT count(*) FROM missing', null) == [[0]]
}

def 'input_origin is keyed by position, so identical directories keep their own item'() {
    // two inputs 'qc' with the same manifest address, from item1 and from item2
    expect: rows('SELECT position, item_cid FROM input_origin WHERE completion_cid = ? ORDER BY position', c) == [[0, item1.toString()], [1, item2.toString()]].sort { it[0] }
}

def 'an ItemLineage that has not arrived waits, and is ingested when it arrives'() {
    when: index.ingestRun(store, completion, 'lab')
    then: meta("itemlineage_wait:${completion}") == lineageCid.toString()
    when: store.put(lineageBytes); index.catchUp(store, storeLog, 'lab')     // Index.catchUp(BlockStore, StoreLog, String), as the InputSet wait tests call it
    then: meta("itemlineage_wait:${completion}") == null && count('item_input') == 2
}

def 'an item lineage naming another input set is skipped'() {
    expect: count('item_input') == 0 && count('item_task') == 0 && meta("itemlineage_wait:${completion}") == null
}

def 're-ingesting and forgetting a run clear its rows'() {
    when: index.ingestRun(store, completion, 'lab'); index.ingestRun(store, completion, 'lab')
    then: count('input_origin') == 4 && count('item_input') == 2
    when: index.forget([completion])
    then: count('input_origin') == 0 && count('item_input') == 0 && count('item_task') == 0
}

def 'a schema 2 run (no item_lineage) indexes as before'() {
    expect: count('item_input') == 0 && rows('SELECT count(*) FROM run_input', null) == [[set.inputs.size()]]
}
```

Bump the three version pins to `== 7`.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexItemLineageTest'`
Expected: FAIL, no such table `input_origin`.

- [ ] **Step 3: Implement** the DDL, ingest, wait, clear and forget changes; bump `SCHEMA_VERSION` and `web/src/config.js`; regenerate `web/test/fixtures/schema.sql`.

- [ ] **Step 4: Run to see them pass, then the core package and the web config test**

Run: `./gradlew test --tests 'robsyme.cas.core.*'` then `cd web && npm ci && npm run gen && node --test test/config.test.mjs`
Expected: PASS (ExplorerQueriesTest's schema.sql comparison included).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy web/src/config.js web/test/fixtures/schema.sql src/test/groovy/robsyme/cas/core/IndexItemLineageTest.groovy src/test/groovy/robsyme/cas/core/IndexTest.groovy src/test/groovy/robsyme/cas/core/IndexLogEntryTest.groovy src/test/groovy/robsyme/cas/core/IndexSchemaVersionTest.groovy
git commit -m "index v7: input positions, input_origin, item_input, item_task"
```

### Task I2: Snapshot copy and seed carry the new rows

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy` (`COPY_P4` 174-178, `COPY_INPUTSET_WAITS` 185-187)
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`SEED_RUN_INPUTS` ~1164, `SEED_INPUTSET_WAITS` ~1167, their use at ~1222)
- Test: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy`, `src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy`

**Interfaces:**
- Consumes: the I1 tables and `Index.ITEMLINEAGE_WAIT`.
- Produces: `COPY_P4` gains `INSERT INTO main.<t> SELECT * FROM src.<t> WHERE completion_cid IN (SELECT completion_cid FROM main.run)` for `input_origin`, `item_input`, `item_task`; a `COPY_ITEMLINEAGE_WAITS` mirrors `COPY_INPUTSET_WAITS`. Seeding: `SEED_RUN_INPUTS` becomes a list `SEED_RUN_RECORDS` covering `run_input`, `input_origin`, `item_input`, `item_task` (`WHERE completion_cid IN seed_run`), plus `SEED_ITEMLINEAGE_WAITS`.

- [ ] **Step 1: Write the failing tests**

```groovy
// IndexSnapshotTest
def 'a snapshot carries the runs input origins and item lineage, and nothing of runs it leaves out'() {
    expect:
    snap('SELECT count(*) FROM input_origin') == [[4]]
    snap('SELECT positions FROM item_input WHERE item_cid = ?', circle) == [['[0,1,2,3]']]
    snap('SELECT count(*) FROM item_task') == [[2]]
    snap('SELECT count(*) FROM item_input WHERE completion_cid = ?', otherMembersRun) == [[0]]
    snap("SELECT count(*) FROM meta WHERE key GLOB 'itemlineage_wait:*'") == [[waitingRuns]]
}

// IndexSeedTest
def 'seeding copies input_origin, item_input, item_task and the item lineage waits'() {
    expect:
    seeded('SELECT count(*) FROM input_origin') == [[4]]
    seeded('SELECT count(*) FROM item_input') == [[2]]
    seeded("SELECT value FROM meta WHERE key = ?", "itemlineage_wait:${waiting}") == [[lineageCid.toString()]]
}
```

Build the run with the I1 test's fixture (copy the helper, or move it to `Fixtures.groovy` if both tests need it).

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexSnapshotTest' --tests 'robsyme.cas.core.IndexSeedTest'`
Expected: FAIL, counts are 0.

- [ ] **Step 3: Implement** the copy and seed statements.

- [ ] **Step 4: Run to see them pass, then every snapshot spec**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexSnapshot*' --tests 'robsyme.cas.core.IndexSeedTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy src/main/groovy/robsyme/cas/core/Index.groovy src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy
git commit -m "snapshot: copy and seed input origins and item lineage"
```

### Task I3: Page queries for Lineage and Used by

**Files:**
- Modify: `web/src/queries.json`
- Test: `src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy` (the `where:` table at ~68-123)

**Interfaces:**
- Produces (exact SQL text in `queries.json`):

```json
"itemInputsOf": "SELECT completion_cid, positions FROM item_input WHERE item_cid = ? ORDER BY completion_cid",
"runInputsAt": "SELECT position, role, name, address, size, sample FROM run_input WHERE completion_cid = ? AND position IN (SELECT value FROM json_each(?)) ORDER BY position",
"originsAt": "SELECT position, item_cid, collection_cid, selection_cid FROM input_origin WHERE completion_cid = ? AND position IN (SELECT value FROM json_each(?)) ORDER BY position, item_cid, collection_cid, selection_cid",
"usedByOf": "SELECT DISTINCT o.completion_cid, o.collection_cid AS via_cid, o.selection_cid, d.item_cid, ci.collection_cid FROM input_origin o JOIN item_input d ON d.completion_cid = o.completion_cid JOIN collection c ON c.completion_cid = o.completion_cid JOIN collection_item ci ON ci.collection_cid = c.collection_cid AND ci.item_cid = d.item_cid WHERE o.item_cid = ? AND EXISTS (SELECT 1 FROM json_each(d.positions) p WHERE p.value = o.position) ORDER BY o.completion_cid, ci.collection_cid, d.item_cid"
```

  W2 binds `itemInputsOf(item)`, `runInputsAt(completion, positionsJson)`, `originsAt(completion, positionsJson)`, `usedByOf(item)`.

- [ ] **Step 1: Write the failing tests**: add four rows to the scan table (`'itemInputsOf' | page.itemInputsOf`, and the other three).

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`
Expected: FAIL, `page.itemInputsOf` is null.

- [ ] **Step 3: Add the four queries.** If the scan check finds a `SCAN` in `usedByOf`, change the query's join order or add the missing index in `Index.ddl()` (and regenerate `schema.sql`), not the allowed-scans list.

- [ ] **Step 4: Run to see them pass**

Run: `./gradlew test --tests 'robsyme.cas.core.ExplorerQueriesTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/queries.json src/test/groovy/robsyme/cas/core/ExplorerQueriesTest.groovy
git commit -m "page queries: an item's inputs and origins, and the items that used it"
```

---

## Web lane

### Task W2: Model methods `itemLineage` and `usedBy`

**Files:**
- Modify: `web/src/model.js` (beside `runInputs`, ~415-442)
- Test: `web/test/item-lineage-model.test.mjs` (new; snapshot built with `makeDb` over `fixtures/schema.sql` as `model.test.mjs:132` does, tail runs with `buildMember`/`block` from `fixture.mjs`)

**Interfaces:**
- Consumes: the I3 queries; `staleRow(cid).completion` (decoded RunCompletion, `item_lineage` and `input_set` CIDs); `blocks.ofKind(cid, 'ItemLineage' | 'InputSet')`.
- Produces:
  - `async itemLineage(itemCid, completionCid = null)` -> `[{ completion_cid, source: 'snapshot'|'tail', inputs: [{ position, role, name, address, size, sample, origins: [{ item_cid, collection_cid, selection_cid }] }] }]`, sorted by `completion_cid`, `inputs` by position, `[]` when no run recorded lineage for the item. With `completionCid`, only that run. Snapshot runs come from the three queries; a run in the tail (or a wanted completion the snapshot has no `item_input` row for) reads its RunCompletion's `item_lineage` and `input_set` blocks. A `BlockError` on either block leaves that run out. CIDs are text.
  - `async usedBy(itemCid)` -> `[{ completion_cid, via_cid, selection_cid, item_cid, collection_cid }]` from `usedByOf`, snapshot only (spec §5.4).

- [ ] **Step 1: Write the failing tests**

```js
test('snapshot: an intros item names its datafile and the upstream people item', async () => {
  const rows = await ex.itemLineage(INTRO_DEV, CONSUMER)
  assert.deepEqual(rows.map(r => r.completion_cid), [CONSUMER])
  assert.deepEqual(rows[0].inputs.map(i => [i.name, i.origins.map(o => [o.item_cid, o.collection_cid, o.selection_cid])]),
    [['Dev_Patel.dat', [[PEOPLE_DEV, PEOPLE, FRIENDS]]]])
})

test('snapshot: circle names all four inputs', async () => {
  assert.equal((await ex.itemLineage(CIRCLE, CONSUMER))[0].inputs.length, 4)
})

test('without a completion every run that recorded lineage for the item answers, sorted', async () => {
  assert.deepEqual((await ex.itemLineage(INTRO_DEV)).map(r => r.completion_cid), [CONSUMER, CONSUMER_2].sort())
})

test('a tail run reads its ItemLineage and InputSet blocks', async () => {
  const [run] = await ex.itemLineage(INTRO_DEV, TAIL_CONSUMER)
  assert.equal(run.source, 'tail')
  assert.equal(run.inputs[0].origins[0].item_cid, PEOPLE_DEV)
})

test('a run without lineage, or with item_lineage null, answers []', async () => {
  assert.deepEqual(await ex.itemLineage(OLD_ITEM, OLD_RUN), [])
})

test('usedBy lists each derived item with its collection, the via and the Selection', async () => {
  const rows = await ex.usedBy(PEOPLE_DEV)
  assert.deepEqual(rows.map(r => [r.item_cid, r.via_cid, r.selection_cid]).sort(),
    [[CIRCLE, PEOPLE, FRIENDS], [INTRO_DEV, PEOPLE, FRIENDS]].sort())
})
```

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm ci && npm run gen && node --test test/item-lineage-model.test.mjs`
Expected: FAIL, `ex.itemLineage is not a function`.

- [ ] **Step 3: Implement both methods** with the existing `text()` helper for CIDs.

- [ ] **Step 4: Run to see them pass, then the suite**

Run: `node --test test/item-lineage-model.test.mjs` then `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/model.js web/test/item-lineage-model.test.mjs
git commit -m "model: an item's recorded lineage and the items that used it"
```

### Task W3: The Lineage panel shows the step back

**Files:**
- Modify: `web/src/views/item.js` (the lineage block at ~283-286; export a title helper)
- Test: `web/test/item-lineage-view.test.mjs` (new; fake Explorer and ctx in the style of `web/test/item-view.test.mjs`)

**Interfaces:**
- Consumes: `ex.itemLineage?.(itemCid, completion|null)` (W2 shape; optional chaining, so old fakes still render); for each Origin, the holding member's Explorer: `ex.item(coll, item)`, `ex.runLabel(coll)` (`{ completion, run_name, status, ... }`), `ex.claimStates([selection])` (`names`); presence `pres.states(cid, { hereHeld })`; `ctx.members.explorer(alias)`; `hrefIn(ctx, alias, hash)`; `runFact`; `thread`.
- Produces: `export function itemTitle(it)`: the title computation the page already does (`labelText(...)`, fallback `'Item'`), moved out so upstream nodes reuse it. The panel, per run (grouped under a run chip only when `-` gives several runs):
  - For each input with Origins, one upstream node per Origin above the file node: text `<output> · <itemTitle>` linked with `hrefIn(ctx, alias, '#/item/<collection>/<item>')` where `alias` is the first member presence reports `held` (the open member when it holds it, with no `?member`); under it `runFact(...)` for the run and `via Selection "<name>"` (short CID when unnamed; nothing when `selection_cid` is null).
  - An Origin with `collection_cid` null: link `#/item/-/<item>`, text `<itemTitle> · picked by query`, the Selection, no run chip.
  - No member holds the item block: the item's short CID and `not in any store here`, no link; run chip and Selection name still shown when the open member can read them.
  - The file node: `<name>` linked `#/content/<address>`, `ext` (dashed) when presence says no member holds `address`; no link when `address` is null.
  - The never-published inputs line stays; the item node stays `here`.
  - `ex.itemLineage` answering `[]` (or absent): the panel exactly as today.

- [ ] **Step 1: Write the failing tests**

```js
test('intros · Dev shows people · Dev, its run and Selection, above Dev_Patel.dat', async () => {
  const node = await item(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  const nodes = [...node.querySelectorAll('.thread .node')].map(n => n.textContent)
  assert.match(nodes[0], /people · Dev/)
  assert.match(nodes[0], /infallible_spence/)
  assert.match(nodes[0], /via Selection "friends"/)
  assert.match(nodes[1], /Dev_Patel\.dat/)
  assert.match(nodes.at(-1), /intros · Dev/)
  assert.equal(node.querySelector('.thread a[href$="#/item/' + PEOPLE + '/' + PEOPLE_DEV + '"]') !== null, true)
  assert.equal(node.querySelector(`.thread a[href="#/content/${DEV_DAT}"]`) !== null, true)
})

test('circle shows four upstream nodes', async () => {
  const node = await item(lineageEx(), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.equal([...node.querySelectorAll('.thread .node')].filter(n => /people ·/.test(n.textContent)).length, 4)
})

test('an upstream item held only in member B links there with ?member=B', async () => {
  const ctx = ctxWith({ held: { [PEOPLE_DEV]: ['B'] }, hrefIn: (alias, h) => `?member=${alias}${h}` })
  const node = await item(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(node.querySelector('.thread a[href^="?member=B#/item/"]'))
})

test('held nowhere: short CID, "not in any store here", no link; the file node is dashed', async () => {
  const node = await item(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  const first = node.querySelector('.thread .node')
  assert.match(first.textContent, /not in any store here/)
  assert.equal(first.querySelector('a[href*="#/item/"]'), null)
  assert.ok(node.querySelector('.thread .node.ext'))
})

test('a Selection pick by query links #/item/-/ with no run chip', async () => {
  const node = await item(lineageEx({ collection: null }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.ok(node.querySelector(`.thread a[href="#/item/-/${PEOPLE_DEV}"]`))
  assert.match(node.querySelector('.thread .node').textContent, /picked by query/)
  assert.equal(node.querySelector('.thread .node .sealed-fact'), null)
})

test('lineage is scoped to the URL run; - groups every run under its chip', async () => {
  const scoped = await item(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(calls.itemLineage.at(-1), [INTRO_DEV, CONSUMER])
  const across = await item(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), '-', INTRO_DEV, ctxWith({ held: allHeld }))
  assert.equal(across.querySelectorAll('[data-lineage-run]').length, 2)
})

test('a run without lineage shows the panel as today', async () => {
  const node = await item(lineageEx({ runs: [] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.deepEqual([...node.querySelectorAll('.thread .node')].map(n => n.textContent), ['intros · Dev'])
})
```

`lineageEx(opts)` is a fake Explorer returning the W2 shape for the spec's consumer (`intros` items, `circle`, Selection `friends`, run `infallible_spence`); `ctxWith({ held, hrefIn })` adds `presence` (an object with `states(cid)` resolving `[{ alias, state: 'held'|'not', here }]` from `held`, and `dots()` returning a span), `members.explorer(alias)` returning the same fake, and `store.hrefIn`. Each run group carries `data-lineage-run="<completion>"`.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm ci && npm run gen && node --test test/item-lineage-view.test.mjs`
Expected: FAIL, no upstream node.

- [ ] **Step 3: Implement** in `views/item.js` within the visual language spec (`docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md`): pass nodes (not `href`) to `thread` where a node holds several links.

- [ ] **Step 4: Run to see them pass, then the suite**

Run: `node --test test/item-lineage-view.test.mjs test/item-view.test.mjs` then `npm test`
Expected: PASS; `item-view.test.mjs` unchanged and passing.

- [ ] **Step 5: Commit**

```bash
git add web/src/views/item.js web/test/item-lineage-view.test.mjs
git commit -m "item page: the Lineage panel shows the upstream item, its run and Selection"
```

### Task W4: The Used by panel

**Files:**
- Modify: `web/src/views/item.js`
- Test: `web/test/item-lineage-view.test.mjs`

**Interfaces:**
- Consumes: `ex.usedBy?.(itemCid)` (W2 shape), `fromEach(ctx.members, (ex) => ex.usedBy?.(itemCid) ?? [])` when `ctx.members?.list?.length > 1`, `hrefIn`, `runLabel`, `itemTitle` (W3).
- Produces: a panel titled `Used by`, placed in the `grid-2` beside or below Lineage, listing derived items as links `<output> · <itemTitle>` to `hrefIn(ctx, alias, '#/item/<collection_cid>/<item_cid>')`, grouped under each consumer run's chip, with `via Selection "<name>"` when `selection_cid` is set. With a collection in the URL it keeps rows whose `via_cid` equals that collection; with `-` it keeps every row. No rows: no panel.

- [ ] **Step 1: Write the failing tests**

```js
test('people · Dev from infallible_spence is used by intros · Dev and circle under the consumer run', async () => {
  const node = await item(usedByEx(), PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld }))
  const panel = [...node.querySelectorAll('section.panel')].find(p => /Used by/.test(p.querySelector('h2')?.textContent))
  assert.ok(panel)
  assert.match(panel.textContent, /intros · Dev/)
  assert.match(panel.textContent, /circle/)
  assert.match(panel.textContent, new RegExp(CONSUMER_RUN_NAME))   // the run name usedByEx's runLabel gives the consumer
})

test('used by lists only consumers that read this run collection', async () => {
  const node = await item(usedByEx(), OTHER_PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld }))   // same item CID, another scratch-workflow run
  assert.equal([...node.querySelectorAll('section.panel h2')].some(h => /Used by/.test(h.textContent)), false)
})

test('a consumer recorded in another member links there', async () => {
  const node = await item(usedByEx({ inMember: 'A' }), PEOPLE, PEOPLE_DEV, ctxWith({ members: ['B', 'A'], hrefIn: (a, h) => `?member=${a}${h}` }))
  assert.ok(node.querySelector('a[href^="?member=A#/item/"]'))
})
```

- [ ] **Step 2: Run to see them fail**

Run: `node --test test/item-lineage-view.test.mjs`
Expected: FAIL, no Used by panel.

- [ ] **Step 3: Implement** the panel.

- [ ] **Step 4: Run to see them pass, then the suite**

Run: `node --test test/item-lineage-view.test.mjs test/item-view.test.mjs` then `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/views/item.js web/test/item-lineage-view.test.mjs
git commit -m "item page: Used by lists the downstream items made from this one"
```

---

## Docs and Gate

### Task D1: DESIGN.md prose and README

**Files:**
- Modify: `DESIGN.md` prose (not the ipldsch block, which F3 owns), `README.md`

**Interfaces:**
- Consumes: the decisions in this plan. Write each as a dated (2026-10-26) amendment in the section's existing style, citing the spec path.

- [ ] **Step 1: Amend DESIGN.md**
  - §6 RunCompletion prose (line 1237) and the InputSet prose (1252-1269): schema 3 and `item_lineage` (null when it could not be made, ItemLineage only with an InputSet, empty `items` allowed); InputSet schema 2, `Input.from`, Origin sort order; ItemLineage as a new kind, its sort orders and positions.
  - §11 CasObserver (1718): publish sources and per-task notes kept in memory keyed by task hash, the walk, the passthrough rule, ItemLineage written between the InputSet and the RunCompletion.
  - §12 Index (1860) and §22 Index (5407): schema 7; `run_input.position`; `input_origin`, `item_input` (packed JSON positions), `item_task` with their indexes, including `item_input_completion` and `item_task_completion`, added for `usedByOf` and `clearRun`; wait record `itemlineage_wait:`; an ItemLineage naming another InputSet is skipped; Used by reads snapshots only (tail runs' derived items appear once the snapshot catches up).
  - §13 fromStore (2132): Origins recorded per emitted path; the Selection recorded is the one the call named (outer, for nested Selections); a via-less Selection item gives `collection` null.
  - §15 or §22 explorer: the Lineage panel's upstream nodes, "picked by query", "not in any store here", run scoping (`-` groups by run), and Used by scoping (a collection in the URL keeps rows whose via is that collection; `-` keeps all, so a consumer that picked the item by query shows only there).
  - §19 decision 4 and §22 Sweep (5514): Mark keeps `item_lineage` as metadata and follows nothing under it; `Input.from` is not followed.
  - §21 closure (4706) and §22 Copy (5541): `item_lineage` in `run_records` at every depth, nothing under it followed.
  - Line ~4815 (the "1,905 of 1,905" measurement): `fromStore` inputs are `cas://` paths in TaskRun records, and `TaskLineage` stops at them by design.
- [ ] **Step 2: Amend README "Reading outputs in another pipeline" (line 299 on, and the note at 43-46 and 353)**: on Nextflow 26.10.4 `channel.fromStore` still fails with MissingProcessException (checked 2026-10-26), so a typed script calls `nextflow.Channel.fromStore`; record-collection inputs (`Bag<Record>`) stage files from 26.10.4.
- [ ] **Step 3: Check**: `cd web && npm ci && npm run gen` succeeds (the ipldsch fence is untouched), and the paragraph found by `grep -n "1,905 of 1,905" DESIGN.md` now says `fromStore` inputs are `cas://` paths.
- [ ] **Step 4: Commit**

```bash
git add DESIGN.md README.md
git commit -m "DESIGN, README: cross-run item lineage"
```

### Task G1: The Gate's typed consumer asserts Origins and ItemLineage

**Files:**
- Modify: `gate/browser_b_assert.py` (`b18`, ~915-935)
- Test: `gate/test_browser_b_assert.py` (fixture at ~360-390)

**Interfaces:**
- Consumes: the `store-typed` member the typed consumer writes; `cas.Store(...).blocks('RunCompletion')`, `read_block`; S2's address and members as `b18`'s neighbours already obtain them (`_saved(observed, "B.second")`).
- Produces: `b18` also fails when the typed run's RunCompletion is not schema 3, its InputSet is not schema 2, any input with role `task` lacks exactly one Origin whose `item` is a member item of S2 and whose `selection` is S2, or its ItemLineage does not list every published `hashes` item with exactly one input position and one task whose process ends in `HASH`. The PASS message names the Origins and the ItemLineage.

- [ ] **Step 1: Write the failing tests** in `test_browser_b_assert.py`: the fixture's `store-typed` gains a schema 3 RunCompletion, a schema 2 InputSet with `from` per input and an ItemLineage (encode with `cas.encode`); `test_b18_passes_with_origins` expects PASS; `test_b18_fails_without_origins` (InputSet schema 2 with no `from`) expects FAIL naming the input; `test_b18_fails_without_item_lineage` (`item_lineage: None`) expects FAIL.
- [ ] **Step 2: Run to see them fail**

Run: `python3 -m unittest gate.test_browser_b_assert`
Expected: FAIL on the two new negative cases.

- [ ] **Step 3: Implement** the checks in `b18`.
- [ ] **Step 4: Run to see them pass, then the Gate's unit tests**

Run: `python3 -m unittest gate.test_browser_b_assert` then `python3 -m unittest discover -s gate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gate/browser_b_assert.py gate/test_browser_b_assert.py
git commit -m "gate: the typed consumer's InputSet names its upstream items and ItemLineage"
```

---

## Final: acceptance (orchestrator checklist, spec §6)

Run after every task is integrated on `cross-run-lineage`. Not an implementer task. Every command below runs with a scratch `XDG_CACHE_HOME` and `NXF_HOME`; nothing opens `/tmp/cas-exploration/store`.

- [ ] Set up: `S=$(mktemp -d)`; `cp -R /tmp/cas-exploration/store "$S/roblab"`; `export XDG_CACHE_HOME="$S/cache" NXF_HOME="$S/nxf"`; build and install the plugin into a throwaway `NXF_PLUGINS_DIR` as `gate/smoke.sh` does; Nextflow 26.10.4. Point `scratch-consumer-workflow`'s config at `$S/roblab` (a scratch copy of its config, never the original).
- [ ] 1. Run `scratch-consumer-workflow` against `$S/roblab`. With `sqlite3 -readonly` on the scratch cache or by decoding blocks: the RunCompletion is schema 3 and links an InputSet (schema 2) and an ItemLineage; each of the four datafile inputs has exactly one Origin naming the `people` item from `infallible_spence`, its `people` OutputCollection and Selection `bafyreih4xlqr6dec4bqqilk7kv6brh6ux5ggkod3cot4znfr7k6lhrbpf4`; ItemLineage lists five items, each `intros` item one input, `circle` all four.
- [ ] 2. Re-run with `-resume`: every task cached; the InputSet and ItemLineage CIDs equal step 1's.
- [ ] 3. `nextflow plugin nf-blocks:snapshot` then `nf-blocks:explore` on `$S/roblab`: the `intros · Dev` page shows `people · Dev` (linked, run `infallible_spence`, the Selection) above `Dev_Patel.dat` above the item; clicking `people · Dev` opens its page.
- [ ] 4. Member A: a fresh empty scratch store, writable. Member B: `$S/roblab`, read-only. Run the consumer with `resolve = ['A', 'B']`; explore A+B: the upstream node links into B (`?member=B`), run chip and Selection name read from B. Explore A alone: the node shows the short CID and "not in any store here"; the file node is dashed. No blocks deleted by hand.
- [ ] 5. A run written by the previous build shows the Lineage panel unchanged. `people · Dev` from `infallible_spence` shows Used by with `intros · Dev` and `circle` under the consumer run; the same item from another `scratch-workflow` run shows no Used by panel. Each ItemLineage entry names its publishing task: `Introduce` with its tag for each `intros` item, `FriendCircle` for `circle`.
- [ ] 6. The typed consumer's `.nextflow.log` has no `Access to undefined parameter 'input'`.
- [ ] 7. `make test` and `cd web && npm test` pass; `make gate` (or the Tier B run that includes `selection-typed`) passes B18 with the new assertions.
