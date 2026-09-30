# Milestone 5 (nf-core pipelines in the store) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an nf-core run, which mostly publishes with `publishDir`, get what it can from nf-blocks: lineage for its workflow outputs (including outputs with an `index {}` block and their Output Index File), a loud and counted account of the `publishDir` files no run records, and nf-core/sarek in Gate tier two.

**Architecture:** Two optional fields join the blocks (`Anomalies.unjoined`, `OutputCollection.index`), the pure `Join` learns index files and empty value outputs, and `CasObserver` counts publishes that joined to no item and warns about `publishDir` processes. The explorer shows both. The Gate gains a small in-repo pipeline for the new cases (the Test Pipeline is not the Gate's file and must stay in step with tier two's `small`), and tier two gains a sarek run.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, Nextflow 26.04.6 plugin API, DAG-CBOR, the explorer page (plain JS, `node --test`), Python 3 Gate harness (unittest), bash, AWS Batch via tier two.

**Spec:** the decisions this plan implements are the `## Answer` sections of
`../.scratch/post-gate/issues/19-publishdir-publishes-in-the-closure.md`,
`../.scratch/post-gate/issues/26-workflow-outputs-with-an-index-block.md` and
`../.scratch/post-gate/issues/18-measure-sarek-on-the-queue.md`, summarised in
`../.scratch/post-gate/roadmap.md` ("Milestone 5"). Glossary: `../CONTEXT.md`
(Output Index File, Output Collection, Publish Coordinate). Contract: `DESIGN.md`.

**Branch:** create `feat/m5-nfcore` from `main` at `e5fefff` before Task 1 (`git switch -c feat/m5-nfcore`). The version stays `0.2.0-beta.1` until the release after acceptance.

## Global Constraints

- DESIGN §0 rule 1: released Nextflow 26.04.6 only; no `includeBuild`, no absolute paths in `build.gradle`.
- DESIGN §0 rule 2: memory bound (1 MiB buffer per hash, at most 32); nothing in this plan reads file content into memory.
- DESIGN §0 rule 3: a failure that would lose provenance throws `AbortRunException`; derived structures (index, Store Log, warnings) log and continue. The new anomaly and warnings never abort a run.
- Every new or changed block field is added to the IPLD Schema in DESIGN.md §6 (the ```ipldsch block from line ~344): the page validates every block against it (`web/schema-gen.mjs` -> `web/src/generated/schema.json`, `web/src/blocks.js:84`) and refuses a block with an unknown field.
- A block written without the new fields must encode byte-identically to today (an OutputCollection with no index keeps its address; old RunCompletions still decode).
- All main classes `@CompileStatic`. Only `robsyme.cas.s3` imports `software.amazon.awssdk.*`.
- Lineage comes from workflow outputs only (ticket 19). Nothing in this plan builds items from `publishDir`.
- Console warnings go through `robsyme.cas.trace.ConsoleLog.LOG` (logger `nextflow.cas`) so Nextflow prints them.
- Do not edit `../.scratch/content-addressed-lineage/test-pipeline/` or `gate/tier2/small/`.

## Review Focus

1. A file published by both a workflow output and a `publishDir` to the same coordinate is joined, never counted unjoined. (Task 3 test `a coordinate published by a workflow output is not unjoined even when publishDir also wrote it`.)
2. On `-resume`, a cached `publishDir` task's publish fires `onFilePublish` with no `upload()` (the pointer fallback); it must still count as unjoined. (Task 3 test `a publish known only by its pointer file still counts as unjoined`.)
3. An output with `index {}` and zero items (an empty channel) still writes its index file; the collection has 0 items and an addressed `index` Leaf. (Task 2 test `an output with an index and no items keeps its index leaf`.)
4. Old blocks decode: a RunCompletion whose anomalies lack `unjoined` reads as 0, and an OutputCollection without `index` encodes to the same bytes as before this change. (Task 1 tests.)
5. A process whose `publishDir` targets a local path (not `cas://`) still gets the warning, but its files are not counted, since they never reach the store. (Task 3 test `a publishDir to a local path warns but counts nothing`.)

---

### Task 1: The two optional block fields

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (class `Anomalies` ~474-530, class `OutputCollection` ~722-807; add class `OutputIndex` after `OutputCollection`)
- Modify: `DESIGN.md` §6 IPLD Schema (`type OutputCollection` ~412-419, `type Anomalies` ~465-470) and the prose blocks (`### OutputCollection` ~557, `### RunCompletion` ~635-649)
- Test: `src/test/groovy/robsyme/cas/core/RecordsTest.groovy`
- Test: `web/test/schema.test.mjs` (create if absent; else add to the file that tests `validBlock`)

**Interfaces:**
- Produces: `Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished, int unjoined)`; the existing 4-arg constructor stays and sets `unjoined = 0`; field `final int unjoined`; `toCbor()` always writes `unjoined`; `fromCbor` treats a missing `unjoined` as 0; `plus` and `isEmpty` include it.
- Produces: `class OutputIndex { final Leaf leaf; final String path; OutputIndex(Leaf leaf, String path); Map toCbor(); static OutputIndex fromCbor(Map) }`.
- Produces: `OutputCollection(String assertedBy, Cid run, String name, List<Cid> items, List<List<String>> paths, OutputIndex index)`; the 5-arg constructor stays (index null); field `final OutputIndex index` (nullable); `toCbor()` writes `index` only when non-null; `fromCbor` reads it when present.

- [ ] **Step 1: Write the failing tests** (append to `RecordsTest.groovy`)

```groovy
    def 'anomalies carry unjoined, and an older block without it reads as zero'() {
        given:
        final withIt = new Anomalies(1, 2, 3, 4, 5)

        expect:
        withIt.toCbor() == [unresolvable: 1L, unaddressed: 2L, declined: 3L, never_published: 4L, unjoined: 5L]
        Anomalies.fromCbor(withIt.toCbor()).unjoined == 5
        Anomalies.fromCbor([unresolvable: 0L, unaddressed: 0L, declined: 0L, never_published: 0L]).unjoined == 0
        new Anomalies(0, 0, 0, 0).unjoined == 0
        !new Anomalies(0, 0, 0, 0, 1).isEmpty()
        new Anomalies(0, 0, 0, 0, 1).plus(new Anomalies(0, 0, 0, 0, 2)).unjoined == 3
    }

    def 'an output collection without an index encodes exactly as before'() {
        given:
        final run = Cid.of(Cid.DAG_CBOR, new byte[32])
        final plain = new OutputCollection('test', run, 'aligned', [], [])

        expect: 'no index key at all, so existing collection addresses do not move'
        !plain.toCbor().containsKey('index')
        OutputCollection.fromCbor(plain.toCbor()).index == null
    }

    def 'an output collection keeps its Output Index File as a leaf and a publish path'() {
        given:
        final run = Cid.of(Cid.DAG_CBOR, new byte[32])
        final file = Cid.of(Cid.RAW, ([7] * 32) as byte[])
        final index = new OutputIndex(Leaf.of('index.json', file, 312L), 'multiqc/index.json')
        final c = new OutputCollection('test', run, 'multiqc', [], [], index)

        when:
        final back = OutputCollection.fromCbor((Map) DagCbor.decode(DagCbor.encode(c.toCbor())))

        then:
        c.toCbor().index == [leaf: Leaf.of('index.json', file, 312L).toCbor(), path: 'multiqc/index.json']
        back.index.path == 'multiqc/index.json'
        back.index.leaf.address == file
        back.index.leaf.size == 312L
    }

    def 'a never-written index is a leaf with a reason'() {
        given:
        final run = Cid.of(Cid.DAG_CBOR, new byte[32])
        final index = new OutputIndex(Leaf.without('index.csv', Leaf.NEVER_PUBLISHED), 'bad/index.csv')

        expect:
        OutputCollection.fromCbor(new OutputCollection('test', run, 'bad', [], [], index).toCbor()).index.leaf.reason == Leaf.NEVER_PUBLISHED
    }
```

(Match the file's existing imports: `Leaf`, `OutputIndex`, `DagCbor`, `Cid` from `robsyme.cas.core`.)

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.RecordsTest'`
Expected: FAIL, compilation errors (`OutputIndex` unknown, no 5-arg `Anomalies` constructor).

- [ ] **Step 3: Implement**

In `Anomalies`:

```groovy
    static final Anomalies NONE = new Anomalies(0, 0, 0, 0, 0)

    final int unresolvable
    final int unaddressed
    final int declined
    final int neverPublished
    /** Publishes into the store that joined to no workflow output item (ticket 19): publishDir's files. */
    final int unjoined

    Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished) {
        this(unresolvable, unaddressed, declined, neverPublished, 0)
    }

    Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished, int unjoined) {
        this.unresolvable = unresolvable
        this.unaddressed = unaddressed
        this.declined = declined
        this.neverPublished = neverPublished
        this.unjoined = unjoined
    }

    static Anomalies unjoined(int count) {
        return new Anomalies(0, 0, 0, 0, count)
    }
```

`plus` adds `unjoined + other.unjoined`; `isEmpty` adds `&& unjoined == 0`; `toCbor` adds `map.put('unjoined', (long) unjoined)` last; `fromCbor` adds a fifth argument
`counts.containsKey('unjoined') ? (int) Records.number(counts.get('unjoined'), 'unjoined') : 0`.
Keep `unresolvable(int)` as it is.

New class after `OutputCollection`:

```groovy
/**
 * A workflow output's Output Index File (glossary; ticket 26): the file Nextflow
 * writes for an output that declares `index {}`. Linked from its collection so it
 * is in the closure; its content names Publish Coordinates, so it is a view, never
 * provenance. The leaf is addressed only from a publish made in the same run.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class OutputIndex {
    final Leaf leaf
    /** The index file's publish path, relative to outputDir, '/'-joined. */
    final String path

    OutputIndex(Leaf leaf, String path) {
        if( leaf == null )
            throw new IllegalArgumentException('an output index needs its leaf')
        if( !path )
            throw new IllegalArgumentException('an output index needs its publish path')
        this.leaf = leaf
        this.path = path
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('leaf', leaf.toCbor())
        map.put('path', path)
        return map
    }

    static OutputIndex fromCbor(Map map) {
        return new OutputIndex(
            Leaf.fromCbor((Map) Records.require(map, 'leaf')),
            Records.string(Records.require(map, 'path'), 'path'))
    }
}
```

In `OutputCollection`: add `final OutputIndex index`; the existing 5-arg constructor becomes `this(assertedBy, run, name, items, paths, null)` and the body moves to a new 6-arg constructor that also sets `this.index = index`. `toCbor()` appends `if( index != null ) map.put('index', index.toCbor())` after `paths`. `fromCbor` passes `block.containsKey('index') ? OutputIndex.fromCbor((Map) block.get('index')) : null`.

In DESIGN.md §6 IPLD Schema:

```
type OutputCollection struct {
  schema Int
  asserted_by String
  run &RunManifest
  name String
  items [nullable &OutputItem]  # sorted by cid string
  paths [[nullable String]]
  index optional OutputIndex     # 2026-09-29 (ticket 26): the Output Index File of an output with index {}
}

type OutputIndex struct {
  leaf Leaf
  path String
}
```

and in `type Anomalies struct` add `unjoined optional Int  # 2026-09-29 (ticket 19): publishes that joined no item; absent reads as 0`. Update the prose blocks: `### OutputCollection` gains the `index` line; `### RunCompletion`'s anomalies line gains `unjoined: int`. Neither schema number changes: the fields are optional and absent in every earlier block.

- [ ] **Step 4: Web schema test** (create `web/test/schema.test.mjs` if no file already tests `validBlock`)

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { validBlock } from '../src/schema.js'
import { buildMember } from './fixture.mjs'

// buildMember() (web/test/fixture.mjs:36) returns real encoded blocks; take its first
// OutputCollection and RunCompletion values as the base literals so the CIDs are real.
test('an OutputCollection with an index validates, and one without still does', async () => {
  const m = await buildMember()
  const collection = m.valueOfKind('OutputCollection')   // see note below
  assert.equal(validBlock(collection), true)
  const file = collection.items[0]                        // any real CID will do as the index file's address
  const leaf = { kind: 'Leaf', name: 'index.json', address: file, size: 312, reason: null }
  assert.equal(validBlock({ ...collection, index: { leaf, path: 'multiqc/index.json' } }), true)
  const missing = { kind: 'Leaf', name: 'index.csv', address: null, size: null, reason: 'never_published' }
  assert.equal(validBlock({ ...collection, index: { leaf: missing, path: 'bad/index.csv' } }), true)
  assert.equal(validBlock({ ...collection, index: { path: 'no-leaf' } }), false)
})

test('a RunCompletion anomalies map may carry unjoined, and still validates without it', async () => {
  const m = await buildMember()
  const completion = m.valueOfKind('RunCompletion')
  assert.equal(validBlock(completion), true)
  assert.equal(validBlock({ ...completion, anomalies: { ...completion.anomalies, unjoined: 2 } }), true)
})
```

`buildMember` may not expose decoded values by kind; if it does not, add a small `valueOfKind(kind)` to its returned object in `fixture.mjs` that returns the first block value with that `kind` (the fixture keeps the values it encodes), rather than building CIDs by hand.

- [ ] **Step 5: Run everything touched**

Run: `./gradlew test --tests 'robsyme.cas.core.RecordsTest' && (cd web && npm test)`
Expected: PASS. Then `./gradlew test`: fix any test that compares an `Anomalies.toCbor()` map literally (they now carry `unjoined: 0L`); grep `src/test` for `never_published:` and add `unjoined: 0L` where a whole map is compared.

- [ ] **Step 6: Gate fixtures**

Run: `python3 -m unittest discover -s gate`
If `gate/assert.py` or `gate/test_assert.py` compares a full anomalies map, extend it with `unjoined`; if `gate/fixtures/make_fixture.py` writes anomalies maps, add `"unjoined": 0` and regenerate with `python3 gate/fixtures/make_fixture.py` (see its docstring for the exact invocation). Expected: all OK.

- [ ] **Step 7: Commit**

```bash
git add -A src/main/groovy/robsyme/cas/core/Records.groovy src/test DESIGN.md web gate
git commit -s -m "feat(core): OutputCollection.index and Anomalies.unjoined (tickets 19, 26)"
```

---

### Task 2: The join learns index files and empty value outputs

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/Join.groovy`
- Test: `src/test/groovy/robsyme/cas/trace/JoinTest.groovy`

**Interfaces:**
- Consumes: `OutputIndex`, the 6-arg `OutputCollection`, `Anomalies` (Task 1).
- Produces: `static Result join(Map<String, Object> captured, Map<String, Path> indexes, CasSession session)`; the 2-arg `join(captured, session)` stays and calls it with `[:]`.
- Produces: `Result.joinedKeys` (`Set<String>`, join keys of every Leaf built, addressed or not, and of every index file).

- [ ] **Step 1: Write the failing tests** (append to `JoinTest.groovy`, reusing its `coord`, `rawCid`, `dagCid`, `sessionWith`)

```groovy
    def 'an output with an index links its Output Index File, addressed from this run\'s publish'() {
        given:
        final bam = rawCid(1)
        final idx = rawCid(3)
        final run = dagCid(9)
        final session = sessionWith([
            'cas://lab/aligned/A/A.bam'     : new CasSession.Publish(new StoreRef(bam, 'A.bam'), 100L, 'head-node'),
            'cas://lab/aligned/index.json'  : new CasSession.Publish(new StoreRef(idx, 'index.json'), 40L, 'head-node'),
        ], run)
        final captured = [aligned: [[[id: 'A'], coord('cas://lab/aligned/A/A.bam')]]] as Map<String, Object>
        final indexes = [aligned: coord('cas://lab/aligned/index.json')] as Map<String, Path>

        when:
        final result = Join.join(captured, indexes, session)

        then:
        final c = result.outputs[0].collection
        c.index.path == 'aligned/index.json'
        c.index.leaf.name == 'index.json'
        c.index.leaf.address == idx
        c.index.leaf.size == 40L
        result.joinedKeys == ['cas://lab/aligned/A/A.bam', 'cas://lab/aligned/index.json'] as Set
        result.providers['head-node'].contains(idx)
        result.anomalies.neverPublished == 0
    }

    def 'an index file with no publish this run is never_published'() {
        given:
        final session = sessionWith([:], dagCid(9))

        when:
        final result = Join.join([bad: []] as Map<String, Object>, [bad: coord('cas://lab/bad/index.csv')] as Map<String, Path>, session)

        then:
        result.outputs[0].collection.index.leaf.reason == Leaf.NEVER_PUBLISHED
        result.outputs[0].collection.index.leaf.address == null
        result.anomalies.neverPublished == 1
        result.joinedKeys == ['cas://lab/bad/index.csv'] as Set
    }

    def 'an output with an index and no items keeps its index leaf'() {
        given:
        final idx = rawCid(4)
        final session = sessionWith(['cas://lab/none/index.json': new CasSession.Publish(new StoreRef(idx, 'index.json'), 2L, 'head-node')], dagCid(9))

        when:
        final result = Join.join([none: []] as Map<String, Object>, [none: coord('cas://lab/none/index.json')] as Map<String, Path>, session)

        then:
        result.outputs[0].collection.items.isEmpty()
        result.outputs[0].collection.index.leaf.address == idx
    }

    def 'a value output that emitted nothing is an empty collection, not an unaddressed anomaly'() {
        given:
        final session = sessionWith([:], dagCid(9))

        when:
        final result = Join.join([summary: null] as Map<String, Object>, session)

        then:
        result.outputs.size() == 1
        result.outputs[0].name == 'summary'
        result.outputs[0].collection.items.isEmpty()
        result.anomalies.unaddressed == 0
    }
```

Remove or rewrite the existing JoinTest case that expects a null value to count `unaddressed` (search for `unaddressed == 1`); it describes the premise ticket 26 overturned.

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.JoinTest'`
Expected: FAIL (no 3-arg `join`, no `joinedKeys`).

- [ ] **Step 3: Implement**

- `Result` gains `final Set<String> joinedKeys` (constructor parameter, stored unmodifiable).
- New `join(Map<String, Object> captured, Map<String, Path> indexes, CasSession session)`; `join(captured, session)` returns `join(captured, Collections.<String, Path>emptyMap(), session)`.
- A `Set<String> joined = new LinkedHashSet<String>()` is threaded into `build`/`leafFor`; `leafFor` adds `key` before either branch.
- The `value == null` branch builds an empty collection instead of counting `unaddressed`: `rawItems = []`.
- After the items of an output: `final Path indexPath = indexes.get(name)`; when non-null, `final OutputIndex index = indexFor(indexPath, counters, session, byProvider, joined)` and the collection is `new OutputCollection(assertedBy, run, name, itemCids, itemPaths, index)`.
- `indexFor`: reuse `leafFor` to build the Leaf, with a throwaway paths list, then take the publish path as `segmentsOf(Coordinates.key(indexPath)).join('/')`:

```groovy
    private static OutputIndex indexFor(Path indexPath, Counters counters, CasSession session,
                                        Map<String, TreeMap<String, Cid>> byProvider, Set<String> joined) {
        final List<String> ignored = new ArrayList<String>()
        final Leaf leaf = leafFor(indexPath, ignored, counters, session, byProvider, joined)
        return new OutputIndex(leaf, segmentsOf(Coordinates.key(indexPath)).join('/'))
    }
```

  `leafFor` already reads only `session.publishFor(key)` (this run's publishes), never the Pointer File, which is ticket 26 answer 4.
- Update the class comment: an `index {}` does not null the value at 26.04.6 (`PublishOp.groovy:219-229`); a null value is a value channel that emitted nothing.

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.trace.JoinTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/Join.groovy src/test/groovy/robsyme/cas/trace/JoinTest.groovy
git commit -s -m "feat(trace): the join links Output Index Files and keeps empty value outputs (ticket 26)"
```

---

### Task 3: The observer counts unjoined publishes and warns about publishDir

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy`
- Test: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`

**Interfaces:**
- Consumes: `Join.join(captured, indexes, session)`, `Result.joinedKeys` (Task 2); `Anomalies.unjoined(int)`, `plus` (Task 1).
- Produces: nothing new for later tasks beyond the RunCompletion's `anomalies.unjoined` and two console warnings, whose exact texts the Gate (Task 6, 7) greps:
  - per process: `nf-blocks: process '<name>' uses publishDir; the files it publishes are stored but no run records them. Declare them as workflow outputs (output { }) to keep their lineage`
  - per run: `nf-blocks: <N> published file(s) are in no workflow output, so no run records them: <first 3 coordinates, comma-separated>[, ...]. Declare them as workflow outputs (output { }) to keep their lineage`

- [ ] **Step 1: Write the failing tests** (append to `CasObserverTest.groovy`; follow how the file's existing tests publish a file and complete a run: find the helper that calls `cas.recordPublish(...)` then `observer.onFilePublish(new FilePublishEvent(...))` and `observer.onFlowComplete()`, and reuse it)

```groovy
    def 'a publish that joins to no workflow output counts as unjoined and is warned once'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        publish('cas://lab/legacy/A.txt', 'A\n')        // recordPublish + onFilePublish, as a publishDir would
        publish('cas://lab/legacy/B.txt', 'B\n')

        when:
        observer.onFlowComplete()

        then:
        final completion = readBlock(latestCompletion())
        completion.anomalies.unjoined == 2L
        warnings(console, '2 published file(s) are in no workflow output') == 1
        console.list.find { it.formattedMessage.contains('published file(s)') }.formattedMessage.contains('cas://lab/legacy/A.txt')
    }

    def 'a coordinate published by a workflow output is not unjoined even when publishDir also wrote it'() {
        given:
        bind(config())
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/aligned/A/A.bam', 'bam\n')
        publish('cas://lab/aligned/A/A.bam', 'bam\n')   // a second publish event for the same coordinate
        observer.onWorkflowOutput(new WorkflowOutputEvent('aligned', [[[id: 'A'], a]], null))

        when:
        observer.onFlowComplete()

        then:
        readBlock(latestCompletion()).anomalies.unjoined == 0L
    }

    def 'a publish known only by its pointer file still counts as unjoined'() {
        given: 'a resumed publishDir task: the pointer exists from an earlier run, and this run made no upload'
        bind(config())
        observer.onFlowCreate(session)
        writePointerOnly('legacy/C.txt', 'C\n')            // coordinates tree holds cas://<cid>/C.txt, no recordPublish
        observer.onFilePublish(new FilePublishEvent(null, coord('cas://lab/legacy/C.txt'), null))

        when:
        observer.onFlowComplete()

        then:
        readBlock(latestCompletion()).anomalies.unjoined == 1L
    }

    def 'an output with an index is recorded with its index leaf, and the index is not unjoined'() {
        given:
        bind(config())
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/tuples/A/A.txt', 'A\n')
        final Path idx = publish('cas://lab/tuples/index.json', '[]\n')
        observer.onWorkflowOutput(new WorkflowOutputEvent('tuples', [[[id: 'A'], a]], idx))

        when:
        observer.onFlowComplete()

        then:
        final completion = readBlock(latestCompletion())
        completion.anomalies.unjoined == 0L
        final collection = readBlock((Cid) (completion.collections as List)[0])
        (collection.index as Map).path == 'tuples/index.json'
    }

    def 'a process that declares publishDir is warned about once'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final TaskProcessor p = Stub(TaskProcessor) {
            getName() >> 'LEGACY'
            getConfig() >> Stub(ProcessConfig) { get('publishDir') >> [[path: 'cas://lab/legacy']] }
        }

        when:
        observer.onProcessCreate(p)
        observer.onProcessCreate(p)

        then:
        warnings(console, "process 'LEGACY' uses publishDir") == 1
    }

    def 'a publishDir to a local path warns but counts nothing'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final TaskProcessor p = Stub(TaskProcessor) {
            getName() >> 'LOCAL'
            getConfig() >> Stub(ProcessConfig) { get('publishDir') >> [[path: 'local_results']] }
        }
        observer.onProcessCreate(p)
        observer.onFilePublish(new FilePublishEvent(null, tempDir.resolve('local_results/x.txt'), null))

        when:
        observer.onFlowComplete()

        then:
        warnings(console, "process 'LOCAL' uses publishDir") == 1
        readBlock(latestCompletion()).anomalies.unjoined == 0L
    }
```

Add private helpers to the spec if the file has none equivalent: `publish(String uri, String text)` (write the bytes through the provider, as the file's existing publish tests do, so `recordPublish` runs, then call `observer.onFilePublish(new FilePublishEvent(null, path, null))` and return the path), `writePointerOnly(String rel, String text)` (put the bytes as a block with `cas.store.put...`, then `cas.coordinates.write(rel, new StoreRef(cid, name))` using the `CoordinateTree` API), `coord(String uri)`, and `latestCompletion()` (the newest `StoreLog.of(cas.store)` entry of kind `RUN`). Import `nextflow.trace.event.FilePublishEvent` and `WorkflowOutputEvent`.

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest'`
Expected: FAIL (`unjoined` 0 or absent; no warning).

- [ ] **Step 3: Implement**

Fields:

```groovy
    /** output name -> the Output Index File Nextflow named for it (ticket 26). */
    private final Map<String, Path> capturedIndexes = new LinkedHashMap<String, Path>()

    /** Every cas:// coordinate a publish event named this run, for the unjoined count (ticket 19). */
    private final Set<String> publishedKeys = ConcurrentHashMap.newKeySet()

    /** Processes already warned about publishDir, so each is warned once. */
    private final Set<String> publishDirWarned = ConcurrentHashMap.newKeySet()
```

`onFilePublish`: right after `final String key = Coordinates.key(target)`, add `publishedKeys.add(key)`. Nothing else changes, so the pointer fallback (Review Focus 2) is still accepted and counted.

`onWorkflowOutput`:

```groovy
    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {
        capturedOutputs.put(event.name, event.value)
        if( event.index != null )
            capturedIndexes.put(event.name, event.index)
    }
```

`onProcessCreate`: the publishDir warning comes first and does not depend on node hashing:

```groovy
    @Override
    void onProcessCreate(TaskProcessor process) {
        warnPublishDir(process)
        warnNodeHash(process)          // the existing body, moved unchanged into this method
    }

    /** Ticket 19 answer 2: lineage comes from workflow outputs only, so a publishDir process is told once. */
    private void warnPublishDir(TaskProcessor process) {
        final String name = process?.name
        if( name == null )
            return
        final Object declared = process.config?.get('publishDir')
        if( !declared || !publishDirWarned.add(name) )
            return
        ConsoleLog.LOG.warn("nf-blocks: process '${name}' uses publishDir; the files it publishes are stored but no run records them. " +
            "Declare them as workflow outputs (output { }) to keep their lineage")
    }
```

`writeCompletion`: replace `Join.join(capturedOutputs, cas)` with `Join.join(capturedOutputs, capturedIndexes, cas)`, then:

```groovy
        final List<String> unjoined = new ArrayList<String>(publishedKeys)
        unjoined.removeAll(joined.joinedKeys)
        Collections.sort(unjoined)
        final Anomalies anomalies = (joined.anomalies ?: Anomalies.NONE).plus(Anomalies.unjoined(unjoined.size()))
        if( !unjoined.isEmpty() )
            ConsoleLog.LOG.warn("nf-blocks: ${unjoined.size()} published file(s) are in no workflow output, so no run records them: " +
                "${unjoined.take(3).join(', ')}${unjoined.size() > 3 ? ', ...' : ''}. Declare them as workflow outputs (output { }) to keep their lineage")
```

and pass `anomalies` to the RunCompletion's `anomalies:`. The run's status is untouched.

Keep `onTaskCached` as it is (milestone 7).

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest'`
Expected: PASS, and the file's existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy
git commit -s -m "feat(trace): count publishes no workflow output joined, and warn about publishDir (ticket 19)"
```

---

### Task 4: The explorer shows the index file and the unjoined count

**Files:**
- Modify: `web/src/model.js` (`Explorer.collection`, ~234-246)
- Modify: `web/src/views.js` (`run()` anomalies line ~103-114; `collection()` ~125-134)
- Modify: `DESIGN.md` §15 selector table (~1492-1533)
- Test: `web/test/views.test.mjs`, `web/test/fixture.mjs`

**Interfaces:**
- Consumes: the `index` field and `anomalies.unjoined` (Task 1).
- Produces: `Explorer.collection(cid, opts)` returns `index: { leaf, path } | null` in its result; markup `[data-output-index]` (value: the index leaf's address) with an `<a download>` child, or `[data-output-index-missing]` (value: the publish path).

- [ ] **Step 1: Write the failing tests** (in `web/test/views.test.mjs`, using `buildMember({ extra })` from `web/test/fixture.mjs` to add a collection that carries an index; read how the existing collection tests at ~:145 build and render a collection page, and follow the same calls)

```js
test('a collection with an Output Index File offers it as a download', async () => {
  const { ex, ids } = await memberWithIndexedCollection({ indexName: 'index.json', indexPath: 'multiqc/index.json' })
  const page = await render(views.collection(ex, ids.collection, 0, ctx()))
  const p = page.querySelector('[data-output-index]')
  assert.ok(p, 'the index paragraph is shown')
  assert.equal(p.getAttribute('data-output-index'), ids.indexFile)
  const a = p.querySelector('a[download]')
  assert.equal(a.getAttribute('download'), 'index.json')
  assert.match(a.getAttribute('href'), new RegExp(`blocks/${ids.indexFile.slice(-2)}/${ids.indexFile}$`))
  assert.match(p.textContent, /Nextflow's index/)
})

test('a collection whose index was never written says so', async () => {
  const { ex, ids } = await memberWithIndexedCollection({ indexName: 'index.csv', indexPath: 'bad/index.csv', neverPublished: true })
  const page = await render(views.collection(ex, ids.collection, 0, ctx()))
  assert.equal(page.querySelector('[data-output-index]'), null)
  assert.equal(page.querySelector('[data-output-index-missing]').getAttribute('data-output-index-missing'), 'bad/index.csv')
})

test('the run page counts unjoined publishes', async () => {
  const { ex, ids } = await memberWithUnjoinedRun(2)
  const page = await render(views.run(ex, ids.completion, ctx()))
  assert.match(page.textContent, /unjoined 2/)
})
```

Write `memberWithIndexedCollection` and `memberWithUnjoinedRun` in `fixture.mjs` on top of `buildMember`'s `extra` hook: a raw block for the index file, an OutputCollection with `index: { leaf: { kind: 'Leaf', name, address, size, reason: null }, path }` (or `address: null, size: null, reason: 'never_published'`), a RunCompletion linking it, and Store Log entries so the member is a tail member (no snapshot). Use the helper names the existing tests use for `render` and `ctx`.

- [ ] **Step 2: Run to verify they fail**

Run: `cd web && npm test`
Expected: FAIL (no `[data-output-index]`, no "unjoined").

- [ ] **Step 3: Implement**

`model.js`, `collection()`: after both branches have their row data, read the block for the index, since the snapshot's `collection` table carries no index column and the page must not need a schema change:

```js
    const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
    const index = block.index ?? null
```

For the tail branch reuse the block it already decoded. Return `index` alongside `output`, `completion`, `items`.

`views.js`, `collection()`: after the "Output of this run" link:

```js
    const idx = c.index
    const indexLine = !idx ? null
      : idx.leaf.address
        ? h('p', { 'data-output-index': String(idx.leaf.address) },
            "Nextflow's index file for this output: ",
            h('a', { href: blockHref(String(idx.leaf.address)), download: idx.leaf.name }, idx.leaf.name),
            ` (published at ${idx.path})`)
        : h('p', { 'data-output-index-missing': idx.path },
            `Nextflow's index file for this output (${idx.path}) was never written.`)
```

`blockHref` must produce the same URL the page's `BlockFetcher` fetches a block from (read `web/src/blocks.js` `load()`: if it prefixes `blockPath(cid)` with a member base, do the same; export a small `blockHref(cidText)` from `blocks.js` so both use one function).

`views.js`, `run()`: extend the anomalies text with `, unjoined ${a.unjoined ?? 0}`.

DESIGN §15 selector table: add two rows, `[data-output-index]` (a collection's Output Index File: value its address, an `<a download>` to its block) and `[data-output-index-missing]` (value the index file's publish path when it was never written).

- [ ] **Step 4: Run to verify they pass**

Run: `cd web && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web DESIGN.md
git commit -s -m "feat(web): a collection offers its Output Index File; the run page counts unjoined publishes (tickets 19, 26)"
```

---

### Task 5: Two small fixes found by the sarek measurement

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`catchUp(... SnapshotStorage, Path)` ~432, `warnUnusable` ~457)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`catchUpIndex` ~206; a new `noteLogged(Cid)`)
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`appendStoreLog`)
- Modify: `src/main/groovy/robsyme/cas/lineage/CasLinStore.groovy` (`save`, ~139-155)
- Test: `src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy`, `src/test/groovy/robsyme/cas/lineage/CasLinStoreTest.groovy`

**Interfaces:**
- Produces: `Index.catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir, Set<String> quietFor)`; the 5-arg overload stays and passes an empty set.
- Produces: `CasSession.noteLogged(Cid entry)`; `catchUpIndex` passes the noted set for the writable member.

**Fix A, the empty-store warning.** A run appends its own Store Log entry before it catches the index up, so on a brand-new store the member's log is never empty by then, and `warnUnusable` warns "no usable Index Snapshot" on the very first run (seen in the 0.2.0-beta.1 clean-install check). The warning is only useful when the log holds entries this process did not just write.

- [ ] **Step 1: Failing test** (in `IndexSeedTest.groovy`, following how it builds a member with a Store Log and no snapshot and captures `nextflow.cas` warnings)

```groovy
    def 'no snapshot warning when the only Store Log entries are the ones this run wrote'() {
        given:
        final member = memberWithOneRun()                 // one run entry, no snapshot (reuse the file's builder)
        final String only = StoreLog.of(member).read()[0].cid.toString()
        final console = capture('nextflow.cas')

        when:
        index.catchUp(member, StoreLog.of(member), 'lab', emptySnapshots(), tempDir, [only] as Set)

        then:
        console.list.findAll { it.formattedMessage.contains('no usable Index Snapshot') }.isEmpty()
    }

    def 'the snapshot warning still fires when an older entry is there'() {
        given:
        final member = memberWithTwoRuns()
        final String newest = StoreLog.of(member).read()[0].cid.toString()
        final console = capture('nextflow.cas')

        when:
        index.catchUp(member, StoreLog.of(member), 'lab', emptySnapshots(), tempDir, [newest] as Set)

        then:
        console.list.count { it.formattedMessage.contains('no usable Index Snapshot') } == 1
    }
```

- [ ] **Step 2: Run, verify FAIL**, `./gradlew test --tests 'robsyme.cas.core.IndexSeedTest'`.

- [ ] **Step 3: Implement.** `warnUnusable(StoreLog storeLog, String member, String reason, Set<String> quietFor)` warns only when `storeLog.read().any { StoreLogEntry e -> !quietFor.contains(e.cid.toString()) }`. The new 6-arg `catchUp` threads `quietFor` to both `warnUnusable` calls. `CasSession` keeps `private final Set<String> loggedThisRun = ConcurrentHashMap.newKeySet()` with `void noteLogged(Cid cid) { loggedThisRun.add(cid.toString()) }`, and `catchUpIndex` passes `member.alias() == config.writableAlias ? loggedThisRun : Collections.<String>emptySet()`. `CasObserver.appendStoreLog` calls `cas.noteLogged(completion)` after a successful append.

- [ ] **Step 4: Run, verify PASS.**

**Fix B, the misleading abort on a failed run.** On a failed run Nextflow interrupts the finalizer thread while `LinObserver.onFlowComplete` saves `#output` through `CasLinStore`; the S3 client throws `AbortedException: Thread was interrupted` inside an `IOException`, `save` turns it into `AbortRunException`, and Nextflow logs `ERROR ... Abort exception produced when notifying an event`. The second notification (from `Session.destroy`) saves the record, so nothing is lost, but the log says otherwise (ticket 18, attempt 2).

- [ ] **Step 5: Failing test** (in `CasLinStoreTest.groovy`, following how it builds a `CasLinStore` over a delegate that throws)

```groovy
    def 'an interrupted save while the run is already failing warns instead of aborting'() {
        given:
        final failing = Mock(Session) { getError() >> new RuntimeException('task failed'); getConfig() >> [:] }
        Global.session = failing
        final store = storeWithDelegateThrowing(new IOException('Exception calling listObject', new InterruptedException('Thread was interrupted')))

        when:
        store.save('abc#output', someRecord())

        then:
        noExceptionThrown()
    }

    def 'the same failure on a run that is not failing still aborts'() {
        given:
        Global.session = Mock(Session) { getError() >> null; getConfig() >> [:] }
        final store = storeWithDelegateThrowing(new IOException('boom', new InterruptedException('x')))

        when:
        store.save('abc#output', someRecord())

        then:
        thrown(AbortRunException)
    }
```

- [ ] **Step 6: Implement.** In `save`, catch `IOException e`: when `interrupted(e) && failingSession()`, `log.warn("nf-blocks: saving lineage record '${key}' was interrupted while the run was already failing; Nextflow saves its records again when the run ends")` and return; otherwise throw as today. `interrupted(Throwable t)` walks the cause chain for `InterruptedException`, `java.nio.channels.ClosedByInterruptException`, or a class whose simple name is `AbortedException` (the AWS SDK's; matched by name so this package does not import the SDK), and also returns true when `Thread.currentThread().isInterrupted()`. `failingSession()` is `(Global.session as Session)?.error != null`.

- [ ] **Step 7: Run, verify PASS**: `./gradlew test --tests 'robsyme.cas.lineage.CasLinStoreTest' --tests 'robsyme.cas.core.IndexSeedTest'`.

- [ ] **Step 8: Commit**

```bash
git add -A src
git commit -s -m "fix: no snapshot warning on a store's first run; an interrupted save on a failing run warns (ticket 18)"
```

---

### Task 6: Gate tier one, the milestone 5 pipeline and assertions 14 to 16

**Files:**
- Create: `gate/outputs/main.nf`, `gate/outputs/nextflow.config`, `gate/outputs/overlay.config`
- Create: `gate/outputs-badindex/main.nf`, `gate/outputs-badindex/nextflow.config`
- Modify: `gate/gate.sh` (two runs after `elsewhere`, before the consumer, into their own store)
- Modify: `gate/assert.py` (three assertions; `assert_runs_exited` gains the two runs)
- Modify: `gate/test_assert.py`
- Modify: `gate/README.md` (assertion table rows 14, 15, 16)

**Interfaces:**
- Consumes: the plugin behaviour of Tasks 1 to 3 and the warning texts in Task 3's Interfaces.
- Produces: runs `outputs` and `outputs-badindex` with logs under `$GATE_ROOT/logs/<name>/`, in the store `$GATE_ROOT/store-outputs` (its own store, so no existing assertion or browser tier sees them).

- [ ] **Step 1: The pipelines**

`gate/outputs/main.nf`:

```groovy
// Milestone 5's Gate pipeline (plan 2026-09-29): workflow outputs with a JSON and a CSV
// index, and one publishDir process whose files no run records. Its own store, so the
// Test Pipeline's assertions and the browser tiers never see it.
params.outdir = 'cas://lab'

process SAMPLE {
    input:
    val sample

    output:
    tuple val(meta), path("${sample}.txt")

    script:
    meta = [id: sample]
    """
    printf 'sample %s\\n' ${sample} > ${sample}.txt
    """
}

process LEGACY {
    publishDir "${params.outdir}/legacy", mode: 'copy'

    input:
    val sample

    output:
    path "${sample}.legacy"

    script:
    """
    printf 'legacy %s\\n' ${sample} > ${sample}.legacy
    """
}

workflow {
    main:
    samples = channel.of('A', 'B')
    tuples = SAMPLE(samples)
    LEGACY(samples)

    publish:
    tuples = tuples
    records = tuples.map { meta, f -> [id: meta.id, file: f] }
}

output {
    tuples {
        path { meta, f -> "tuples/${meta.id}" }
        index { path 'tuples/index.json' }
    }
    records {
        path { r -> "records/${r.id}" }
        index { path 'records/index.csv'; header true }
    }
}
```

`gate/outputs/nextflow.config`: `lineage.enabled = true` and nothing else (gate.config supplies the store). `gate/outputs/overlay.config`: `manifest.name = 'cas-gate-outputs'`.

`gate/outputs-badindex/main.nf`: the same `SAMPLE` process, `publish: tuples = SAMPLE(channel.of('A', 'B'))`, and

```groovy
output {
    tuples {
        path { meta, f -> "tuples/${meta.id}" }
        // A CSV header needs map records; a tuple channel makes Nextflow's CsvWriter throw
        // while the run still exits 0 (research output-dsl-meta-maps.md §2.2).
        index { path 'tuples/index.csv'; header true }
    }
}
```

with the same one-line `nextflow.config`.

- [ ] **Step 2: Wire the runs into `gate.sh`** after the `elsewhere` run:

```bash
# Milestone 5 (plan 2026-09-29): index files and a publishDir process, in a store of their own.
for p in outputs outputs-badindex; do
    rm -rf "$GATE_ROOT/$p"; mkdir -p "$GATE_ROOT/$p"
    cp "$REPO/gate/$p/main.nf" "$REPO/gate/$p/nextflow.config" "$GATE_ROOT/$p/"
done
GATE_STORE="$GATE_ROOT/store-outputs" run "$GATE_ROOT/outputs" outputs -c "$REPO/gate/outputs/overlay.config"
GATE_STORE="$GATE_ROOT/store-outputs" run "$GATE_ROOT/outputs-badindex" outputs-badindex -c "$REPO/gate/outputs/overlay.config"
```

Check that `run()` inherits the environment (it runs `"$NEXTFLOW"` in a subshell, so an assignment prefix on `run` applies); if `gate.sh` exports `GATE_STORE` globally, the prefix still wins for that call.

- [ ] **Step 3: Write the failing Gate unit tests** in `gate/test_assert.py`, following `TestRunExitAssertion` and the `StoreBuilder` pattern: a fake `store-outputs` with a RunManifest `run_name: outputs`, a RunCompletion whose `anomalies.unjoined` is 2 and whose collections `tuples` (with `index: {leaf: {...address...}, path: 'tuples/index.json'}`) and `records` (CSV index) link items `[{"id": "A"}, Leaf]`; coords pointers for `tuples/index.json`, `records/index.csv`, `legacy/A.legacy`, `legacy/B.legacy`; the raw blocks the pointers name; and `logs/outputs/nextflow.log` containing both warning texts. One test per assertion for PASS, and one FAIL case each:
  - 14 FAILs when the index leaf's address differs from the raw CID of the bytes the pointer names;
  - 15 FAILs when the bad run's index leaf is addressed;
  - 16 FAILs when `unjoined` is 1 while two `legacy/` pointers name blocks no leaf references.

- [ ] **Step 4: Implement the assertions** in `gate/assert.py`. A helper opens the second store: `outputs_store(gate)` returns `cas.Store(os.path.join(gate.root, "store-outputs"))`, and a small `Run`-like lookup by `run_name` over its Store Log, reusing whatever `Gate.run` uses internally (factor it into a function that takes a store if it is a method today).

```python
@assertion(14, "workflow outputs with an index join, and each index file is linked by address")
def assert_fourteen(gate):
    """Tickets 26 answers 1, 2: both outputs join with their Meta Maps, and each collection's
    index leaf is the raw CID of the bytes at its own coordinate (hashed here, not trusted)."""
    # For run "outputs": collections {"tuples", "records"} exist; every item carries {"id": ...};
    # for each: index.path == "<name>/index.<ext>", pointer = store.coords_pointer(index.path),
    # bytes = store.read(pointer cid), cas.cid_raw(bytes) == index.leaf.address == pointer cid.
    # tuples/index.json parses as JSON with 2 rows; records/index.csv has a header row and 2 rows.


@assertion(15, "an index Nextflow fails to write is never_published while the run succeeds")
def assert_fifteen(gate):
    """Ticket 26 answer 4: run "outputs-badindex" exits 0 with status succeeded; its tuples
    collection has 2 items and an index leaf with reason never_published; anomalies.never_published >= 1;
    no coordinate tuples/index.csv exists in store-outputs, or, if it does, the leaf still is not addressed."""


@assertion(16, "a publishDir process warns, and its files count as unjoined")
def assert_sixteen(gate):
    """Ticket 19 answer 2: run "outputs" logs "process 'LEGACY' uses publishDir" once and
    "2 published file(s) are in no workflow output"; anomalies.unjoined == 2; the coords
    legacy/A.legacy and legacy/B.legacy exist and neither's CID is any item leaf's or index leaf's
    address. Computed from the store, not from the plugin's count alone."""
```

Write each body out in full following the comments (the existing assertions show how to read completions, collections, items, `_leaves`, coords and logs). Add `"outputs"` and `"outputs-badindex"` to the list `assert_runs_exited` checks for exit 0.

- [ ] **Step 5: Run the Gate's unit tests, then the Gate**

Run: `python3 -m unittest discover -s gate` (expected OK), then `GATE_ROOT=/Users/robsyme/.claude/jobs/989dda24/tmp/gate-m5 make gate` (expected: lineage 15 PASS, 0 FAIL, 6 SKIP; browser A 5/5; B 12/12). Gate assertion 4 can fail intermittently by design (DESIGN/Gate README); rerun before debugging it.

- [ ] **Step 6: Docs and commit.** `gate/README.md`'s assertion table gains rows 14, 15, 16 in the same style as the existing rows.

```bash
git add gate
git commit -s -m "test(gate): index files and a publishDir process, assertions 14 to 16 (milestone 5)"
```

---

### Task 7: Gate tier two runs nf-core/sarek

**Files:**
- Create: `gate/tier2/sarek.config`, `gate/tier2/gatk4-quay.config`
- Modify: `gate/tier2/tier2.sh` (a sarek run started first, in the background, into member `cas-sarek`)
- Modify: `gate/tier2/assert_tier2.py` (check `TS`)
- Modify: `gate/tier2/test_assert_tier2.py`
- Modify: `gate/tier2/README.md`

**Interfaces:**
- Consumes: the warning texts (Task 3), `anomalies.unjoined` and `OutputCollection.index` (Task 1).
- Produces: run `ts` (log `$T2/logs/ts/`, trace `$T2/trace/ts.txt`) and check `TS`.

- [ ] **Step 1: The configs.** `gate/tier2/gatk4-quay.config` is `prototype/18/gatk4-quay.config` from branch `prototype/18-sarek` (commit `17596c5`), verbatim. `gate/tier2/sarek.config`:

```groovy
// gate/tier2/sarek.config -- tier two's nf-core/sarek run (ticket 18). A -c plugins block
// replaces the pipeline's own list (measured), so sarek 3.10.0's pins are repeated here.
plugins {
    id "nf-blocks@${System.getenv('T2_PLUGIN_VERSION')}"
    id 'nf-amazon'
    id 'nf-core-utils@0.4.0'
    id 'nf-fgbio@1.0.0'
    id 'nf-prov@1.7.0'
    id 'nf-schema@2.7.2'
}
lineage.enabled = true
lineage.store.location = 'cas://lab'
outputDir = 'cas://lab'
params.outdir = 'cas://lab'
cas {
    stores { lab { location = "s3://${System.getenv('T2_BUCKET')}/cas-sarek" } }
    asserted_by = 'gate-tier2'
}
workDir = "s3://scidev-playground-us-east-1/robsyme/nf-blocks-gate/${System.getenv('T2_RUN_ID')}/work"
process {
    executor = 'awsbatch'
    queue = 'TowerForge-3skcexigeJwK0Jb71pThbJ'
}
aws { region = 'us-east-1'; profile = System.getenv('AWS_PROFILE') ?: 'scidev' }
wave.enabled = true
fusion.enabled = true
trace { enabled = true; overwrite = true; file = System.getenv('T2_TRACE'); fields = 'task_id,hash,native_id,name,status,exit,realtime,workdir' }
```

(No `gate/gate.config` and no `batch.config`: gate.config would rename the pipeline and drop its pins; batch.config forces an ubuntu container on every process.)

- [ ] **Step 2: `tier2.sh`.** After the bucket is set up and before `produce t1`:

```bash
# Ticket 18: nf-core/sarek 3.10.0's test profile, started first because it is the longest run
# (about 15 min), into its own member. Not produce(): sarek is fetched, not copied, and takes neither
# gate.config nor batch.config.
mkdir -p "$T2/ts" "$T2/logs/ts"
run_nf "$T2/logs/ts/stdout.log" "$T2/ts" T2_TRACE="$T2/trace/ts.txt" XDG_CACHE_HOME="$T2/cache-ts" \
    "$NEXTFLOW" -log "$T2/logs/ts/nextflow.log" run nf-core/sarek -r 3.10.0 -profile test,docker -name ts \
    -c "$REPO/gate/tier2/sarek.config" -c "$REPO/gate/tier2/gatk4-quay.config" &
TS_PID=$!
```

Read `run_nf` first: it already backgrounds and records the PID (so the watchdog and the cleanup trap cover sarek); if it does, drop the trailing `&`/`$!` and capture the PID the way `t6a`/`t6b` do. Before `assert_tier2.py check`, wait for it and write `$T2/logs/ts/exit` the same way `produce` does. The 2700 s watchdog stays: sarek runs beside t1 to t6.

- [ ] **Step 3: Failing unit tests** in `gate/tier2/test_assert_tier2.py`, following `AssertionTest`/`FailPathTest` and the `World` fake: a `cas-sarek` member with a succeeded RunCompletion (run_name `ts`), a `multiqc` collection of 3 items `[{"id": "sarek"}, Leaf]` and an index leaf matching `coords/multiqc/index.json`'s block, 5 extra coords under `reports/` whose blocks no leaf references, `anomalies.unjoined == 5`, and a `logs/ts/nextflow.log` with the publishDir warning. PASS case, plus FAIL when `unjoined` disagrees with the independent count, and FAIL when the index leaf's address is not the SHA-256 of the object at its coordinate.

- [ ] **Step 4: Implement `TS`** in `assert_tier2.py` and add `("TS", "nf-core/sarek: its workflow output joins and its publishDir files are counted (ticket 18)", ts)` to `CHECKS`, and `"ts"` to `PRODUCERS`:

```python
def ts(ctx):
    """Ticket 18 on the queue: exit 0; the RunCompletion succeeded; collection multiqc has >= 1 item,
    each with a Meta Map carrying "id", and every item Leaf addressed; its index leaf's address equals
    the SHA-256 CID of the object at coords/<index.path> (ctx.sha); anomalies.unjoined equals the number
    of coords/ keys whose pointer CID is neither an item leaf nor the index leaf, and is > 0; the log
    names at least one "uses publishDir" warning."""
```

Write the body in full following the comment and the existing checks' use of `ctx.member`, `ctx.run`, `ctx.exit`, `ctx.text`, `ctx.sha` and `_verdict`.

- [ ] **Step 5: Run the tier-two unit tests.** `python3 -m unittest discover -s gate/tier2` (expected OK). The paid run itself is Rob's, at acceptance (Task 8).

- [ ] **Step 6: README and commit.** `gate/tier2/README.md`: add `ts` to the runs table (Pipeline nf-core/sarek 3.10.0, Fusion on, member `cas-sarek`) and `TS` to the checks, with the cost (about 23 Batch jobs, 15 min, $0.10 to $0.20, estimate) and the GATK note (sarek's `gatk4_gcnvkernel` image has a 1.98 GB layer that Cloudflare does not cache, so its GATK4 modules run on `quay.io/biocontainers/gatk4:4.6.2.0--py310hdfd78af_1` until a smaller Wave image exists).

```bash
git add gate/tier2
git commit -s -m "test(tier2): nf-core/sarek 3.10.0 on the queue, check TS (ticket 18)"
```

---

### Task 8: Documentation and acceptance

**Files:**
- Modify: `DESIGN.md` (§11 `onFilePublish`, `onWorkflowOutput`, `onProcessCreate`, `onFlowComplete` bullets; new §18 "Milestone 5: nf-core pipelines in the store (2026-09-29)")
- Modify: `README.md` ("Status" list; a new section "Pipelines that use publishDir"; the `-c` plugins note already there)

- [ ] **Step 1: DESIGN §11.** Amend, each as a dated `*Amended 2026-09-29 (milestone 5):*` note: `onFilePublish` records every cas:// key in `publishedKeys`; `onWorkflowOutput` also captures `event.index` (the "an index nulls the value" sentence is corrected: at 26.04.6 `PublishOp.groovy:219-229` passes both); `onProcessCreate` warns once per process declaring `publishDir`; `onFlowComplete` counts `unjoined` and warns. `Join` builds an empty collection for a null value and an `index` Leaf from this run's publish only.

- [ ] **Step 2: DESIGN §18,** following §17's shape: what the milestone contains, each decision with its ticket (19, 26, 18), the status line with test counts, and the acceptance runs.

- [ ] **Step 3: README.** Status list: nf-blocks records lineage from workflow outputs only; a `publishDir` process is warned about, its files are stored and counted as `unjoined`, not recorded; outputs with `index {}` record their index file. New section "Pipelines that use publishDir" with the two warnings' wording and the pointer to the output DSL.

- [ ] **Step 4: Full local acceptance**

Run: `./gradlew check` (unit tests, `memoryBoundTest`, `dependencyCheck`, `webTest`), then `GATE_ROOT=<fresh dir in the scratchpad> make gate`.
Expected: all green; lineage 15 PASS, 0 FAIL, 6 SKIP; browser A 5/5; B 12/12. Record the numbers in DESIGN §18's status line.

- [ ] **Step 5: Commit**

```bash
git add DESIGN.md README.md
git commit -s -m "docs: milestone 5, nf-core pipelines in the store"
```

- [ ] **Step 6: Rob's paid acceptance (not an agent step).** With the scidev SSO session: `GATE_PYTHON=<venv python> AWS_PROFILE=scidev make gate-tier2 GATE_ROOT=<the same root>`, expecting T1 to T6, T2b and TS PASS; then merge `feat/m5-nfcore` to `main`.
