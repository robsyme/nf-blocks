# Store Log and Query Indexes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every task is bound by `DESIGN.md`; when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets fixed.

**Goal:** Replace the Run Log (`runs/<rts>-<cid>`) with the Store Log (`log/<rts>-<kind>-<cid>`, write-time timestamps, a 10-minute catch-up overlap) and add the two indexes query 3 needs, keeping the Gate green.

**Architecture:** The log classes in `robsyme.cas.core` are renamed and gain a `kind` and an overlap-aware `entriesSince`. `Index` moves to schema version 2 (an old index is recreated empty on open, and the first catch-up for each member then finds its runs from the blocks; see the final-review fix), adds two indexes, and catches up with the overlap, skipping entries it has already indexed so a catch-up with nothing new still reads no blocks. The observer writes entries at write time. The Gate's Python reads the new layout and ranks "latest successful run" by each RunCompletion's `finished_at`, not by log order.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, `org.xerial:sqlite-jdbc`, Gradle 8.14, Nextflow 26.04.6, Python 3 stdlib for the Gate.

**Spec:** `../.scratch/block-explorer/spec.md` section 3 (Store Log) and section 13 (v1 changes); `DESIGN.md` §5 and §12 amendments (commit `a054221` on branch `design/block-explorer-amendments`).

**Branch:** start from `design/block-explorer-amendments`, which carries the DESIGN.md amendments: `git switch -c feat/store-log design/block-explorer-amendments`.

## Global Constraints

- Released Nextflow 26.04.6 only; no new dependencies (`dependencyCheck` must stay green).
- Store Log entry name: `<rts>-<kind>-<cid>` under `log/`, `rts = String.format('%013d', 9999999999999L - writtenAtMillis)`, `kind ∈ run | selection | claim`.
- `rts` is the moment the entry is written to the member, never `finished_at`.
- Catch-up overlap: exactly `10 * 60 * 1000L` ms before the watermark.
- `runs/` is dropped with no compatibility read. Existing stores keep their runs because each member's first catch-up scans its blocks once.
- New indexes, exact names: `collection_completion_output ON collection(completion_cid, output_name)` and `collection_item_collection ON collection_item(collection_cid)`.
- Index `SCHEMA_VERSION` becomes `2`; the watermark meta key becomes `store_log_watermark:<member>`.
- Failures in derived structures (index, Store Log) log at warn and continue (DESIGN §0 rule 3).
- Only `run` entries are ingested in this plan; `selection` and `claim` entries are listed, skipped at debug level, and counted toward the watermark. Their ingest arrives with the explorer's milestone 2.
- Gate assertions never trust the plugin (DESIGN §0 rule 6).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A store written by v1** (`runs/` only, no `log/`) opened by the new plugin: its runs still answer queries, because the first catch-up for a member scans its blocks once. Pinned by the final-review fix (`a v1 index is recreated on open and the first catch-up finds its runs from the blocks`); the Task 2 test only exercised `rebuild`.
2. **An unknown or malformed entry in `log/`** (a future kind such as `pin`, a stray file): ignored with a warning, never an exception, and the rest of the log still ingests. Pinned in Task 1.
3. **The same run logged twice** (a retried append at a later millisecond gives a second name for the same CID): one `run` row, no duplicate producers. Pinned in Task 2.
4. **A log entry whose block is absent** (a partial Bundle merge): catch-up does not abort, the other entries ingest, and the watermark still advances. Pinned in Task 2.
5. **An entry written behind the watermark by a skewed clock**: picked up by the next catch-up without re-reading blocks of runs already indexed. Pinned in Task 2.

**Waves:**

| Wave | Tasks | Depends on |
|---|---|---|
| 1 | 1 Store Log core; 4 Gate Python (code against this plan's entry format) | — |
| 2 | 2 Index and session catch-up | 1 |
| 3 | 3 Observer writes the Store Log; delete the Run Log classes | 2 |
| 4 | 5 Regenerate the fixture, run the Gate, fix until green | 3, 4 |

Tasks in one wave touch disjoint files. The old `RunLog*` classes stay in the
tree until Task 3, so the build compiles after every task.

---

### Task 1: Store Log core

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/StoreLogKind.groovy`
- Create: `src/main/groovy/robsyme/cas/core/StoreLog.groovy` (replaces `RunLog.groovy`)
- Create: `src/main/groovy/robsyme/cas/core/StoreLogEntry.groovy` (replaces `RunLogEntry.groovy`)
- Create: `src/main/groovy/robsyme/cas/core/StoreLogStorage.groovy` (replaces `RunLogStorage.groovy`)
- Create: `src/main/groovy/robsyme/cas/core/LocalStoreLogStorage.groovy` (replaces `LocalRunLogStorage.groovy`)
- Create: `src/test/groovy/robsyme/cas/core/StoreLogTest.groovy`

Leave the existing `RunLog*` classes and `RunLogTest` alone; Task 3 deletes
them once nothing uses them.

**Interfaces:**
- Consumes: `Cid` (`Cid.parse`, `Cid.isCid`, `Cid.of`), `BlockStore`, `LocalBlockStore.getRoot()`, `CompositeStore.getMembers()`, `Hashing.sha256`.
- Produces:
  - `enum StoreLogKind { RUN, SELECTION, CLAIM; String getToken(); static StoreLogKind fromToken(String) /* null if unknown */ }`
  - `final class StoreLogEntry implements Comparable<StoreLogEntry> { String name; String reverseTs; long writtenAtMillis; StoreLogKind kind; Cid cid }`
  - `interface StoreLogStorage { void putEntry(String name); List<String> listEntries() }`
  - `class LocalStoreLogStorage implements StoreLogStorage { LocalStoreLogStorage(Path storeRoot); Path getDirectory() /* <root>/log */ }`
  - `class StoreLog`:
    - `static final long HORIZON_MILLIS = 9999999999999L`
    - `static final long OVERLAP_MILLIS = 600_000L`
    - `static StoreLog of(BlockStore store)`
    - `static void append(BlockStore store, StoreLogKind kind, Cid cid, long writtenAtMillis)`
    - `static List<StoreLogEntry> read(BlockStore store)`
    - `static String entryName(StoreLogKind kind, Cid cid, long writtenAtMillis)`
    - `static String reverseTimestamp(long writtenAtMillis)`
    - `static StoreLogEntry parse(String name) /* null if malformed or unknown kind */`
    - `void append(StoreLogKind kind, Cid cid, long writtenAtMillis)`
    - `List<StoreLogEntry> read() /* newest first */`
    - `List<StoreLogEntry> entriesSince(String watermark) /* newest first; everything when watermark is null or empty */`

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/StoreLogTest.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §5 as amended: `log/<rts>-<kind>-<cid>`, one empty file per block
 * written outside a run's closure, `rts` the reverse of the moment the entry
 * was written, so a lexicographic listing is newest first.
 */
class StoreLogTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
    }

    static Cid cidOf(String text) {
        return Cid.of(Cid.DAG_CBOR, Hashing.sha256(text.getBytes('UTF-8')))
    }

    private List<String> namesOnDisk() {
        final Path log = tempDir.resolve('store').resolve('log')
        return Files.list(log).map { Path p -> p.fileName.toString() }.sorted().toList()
    }

    def 'append writes an empty file named by reverse timestamp, kind and cid'() {
        given:
        def cid = cidOf('one')

        when:
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_700_000_000_000L)

        then:
        namesOnDisk() == ["${String.format('%013d', 9999999999999L - 1_700_000_000_000L)}-run-${cid}".toString()]
        Files.size(tempDir.resolve('store/log').resolve(namesOnDisk()[0])) == 0
    }

    def 'nothing is written under runs/'() {
        when:
        StoreLog.append(store, StoreLogKind.RUN, cidOf('one'), 1_000L)

        then:
        !Files.exists(tempDir.resolve('store/runs'))
    }

    def 'every kind round-trips through its token'() {
        expect:
        StoreLogKind.fromToken(kind.token) == kind
        StoreLog.parse(StoreLog.entryName(kind, cidOf('x'), 5_000L)).kind == kind

        where:
        kind << StoreLogKind.values().toList()
    }

    def 'parse recovers the written time and the cid'() {
        given:
        def cid = cidOf('p')

        when:
        def entry = StoreLog.parse(StoreLog.entryName(StoreLogKind.SELECTION, cid, 1_234_567L))

        then:
        entry.writtenAtMillis == 1_234_567L
        entry.cid == cid
        entry.kind == StoreLogKind.SELECTION
    }

    def 'read lists the newest entry first'() {
        given:
        def older = cidOf('older')
        def newer = cidOf('newer')
        StoreLog.append(store, StoreLogKind.RUN, older, 1_000L)
        StoreLog.append(store, StoreLogKind.CLAIM, newer, 2_000L)

        expect:
        StoreLog.read(store)*.cid == [newer, older]
    }

    def 'appending the same entry twice is success'() {
        given:
        def cid = cidOf('twice')

        when:
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_000L)
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_000L)

        then:
        namesOnDisk().size() == 1
    }

    def 'unknown kinds and malformed names are ignored, the rest still reads'() {
        given:
        def good = cidOf('good')
        StoreLog.append(store, StoreLogKind.RUN, good, 1_000L)
        def dir = tempDir.resolve('store/log')
        Files.createFile(dir.resolve("${String.format('%013d', 9999999999999L - 2_000L)}-pin-${cidOf('future')}"))
        Files.createFile(dir.resolve('README'))
        Files.createFile(dir.resolve("123-run-${cidOf('short')}"))
        Files.createFile(dir.resolve("${String.format('%013d', 9999999999999L - 3_000L)}-run-notacid"))

        expect:
        StoreLog.read(store)*.cid == [good]
    }

    def 'entriesSince returns everything newer than the watermark plus the overlap window'() {
        given: 'entries at 0, 20 min, 25 min and 40 min'
        def base = 1_700_000_000_000L
        def e0 = cidOf('e0'); def e20 = cidOf('e20'); def e25 = cidOf('e25'); def e40 = cidOf('e40')
        StoreLog.append(store, StoreLogKind.RUN, e0, base)
        StoreLog.append(store, StoreLogKind.RUN, e20, base + 20 * 60_000L)
        StoreLog.append(store, StoreLogKind.RUN, e25, base + 25 * 60_000L)
        StoreLog.append(store, StoreLogKind.RUN, e40, base + 40 * 60_000L)
        def log = StoreLog.of(store)
        def watermark = StoreLog.entryName(StoreLogKind.RUN, e25, base + 25 * 60_000L)

        expect: 'the 40-minute entry, the watermark itself and the 20-minute entry inside the 10-minute window'
        log.entriesSince(watermark)*.cid == [e40, e25, e20]
    }

    def 'entriesSince with no watermark is everything'() {
        given:
        StoreLog.append(store, StoreLogKind.RUN, cidOf('a'), 1_000L)
        StoreLog.append(store, StoreLogKind.RUN, cidOf('b'), 2_000L)

        expect:
        StoreLog.of(store).entriesSince(null).size() == 2
        StoreLog.of(store).entriesSince('').size() == 2
    }

    def 'an empty log reads as nothing'() {
        expect:
        StoreLog.read(store) == []
    }

    def 'the log of a composite store is the log of its writable member'() {
        given:
        def other = new LocalBlockStore(tempDir.resolve('other'), 'other', false)
        def composite = new CompositeStore([store, other] as List<BlockStore>)

        when:
        StoreLog.append(composite, StoreLogKind.RUN, cidOf('c'), 1_000L)

        then:
        StoreLog.read(store).size() == 1
        !Files.exists(tempDir.resolve('other/log'))
    }

    def 'a timestamp outside the encodable range is refused'() {
        when:
        StoreLog.reverseTimestamp(-1L)

        then:
        thrown(IllegalArgumentException)
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.StoreLogTest'`
Expected: compilation failure, `unable to resolve class StoreLog` / `StoreLogKind`.

- [ ] **Step 3: Write the implementation**

```groovy
// src/main/groovy/robsyme/cas/core/StoreLogKind.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a Store Log entry announces (block explorer spec section 3). */
@CompileStatic
enum StoreLogKind {
    RUN('run'), SELECTION('selection'), CLAIM('claim')

    final String token

    StoreLogKind(String token) { this.token = token }

    /** The kind for a token, or null when the token is not one we know. */
    static StoreLogKind fromToken(String token) {
        for( StoreLogKind kind : values() )
            if( kind.token == token )
                return kind
        return null
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/StoreLogEntry.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** One Store Log entry: `<rts>-<kind>-<cid>` decomposed (DESIGN.md §5). */
@CompileStatic
final class StoreLogEntry implements Comparable<StoreLogEntry> {

    /** The entry name as it appears in the store; also the watermark form. */
    final String name
    /** The 13-digit reverse timestamp, so lexicographic order is newest first. */
    final String reverseTs
    /** When the entry was written to this member. */
    final long writtenAtMillis
    final StoreLogKind kind
    /** The block this entry announces. */
    final Cid cid

    StoreLogEntry(String name, String reverseTs, long writtenAtMillis, StoreLogKind kind, Cid cid) {
        this.name = name
        this.reverseTs = reverseTs
        this.writtenAtMillis = writtenAtMillis
        this.kind = kind
        this.cid = cid
    }

    @Override
    int compareTo(StoreLogEntry other) { name <=> other.name }

    @Override
    String toString() { "StoreLogEntry[$name]" }

    @Override
    boolean equals(Object other) {
        return other instanceof StoreLogEntry && ((StoreLogEntry) other).name == name
    }

    @Override
    int hashCode() { name.hashCode() }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/StoreLogStorage.groovy
package robsyme.cas.core

/**
 * The one thing the Store Log needs of a store: write-once empty entries under
 * a `log/` prefix and list them (DESIGN.md §5). Kept separate from
 * {@link BlockStore} because an entry is not a block.
 */
interface StoreLogStorage {

    /** Creates the empty entry. Already present is success. */
    void putEntry(String name)

    /** Every entry name, in no particular order. */
    List<String> listEntries()
}
```

```groovy
// src/main/groovy/robsyme/cas/core/LocalStoreLogStorage.groovy
package robsyme.cas.core

import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.CompileStatic

/** The Store Log of a local store root: empty files under `log/` (DESIGN.md §5). */
@CompileStatic
class LocalStoreLogStorage implements StoreLogStorage {

    private static final String LOG = 'log'

    private final Path root

    LocalStoreLogStorage(Path storeRoot) {
        this.root = storeRoot
    }

    Path getDirectory() { root.resolve(LOG) }

    @Override
    void putEntry(String name) {
        final Path directory = getDirectory()
        Files.createDirectories(directory)
        try {
            Files.createFile(directory.resolve(name))
        }
        catch( FileAlreadyExistsException e ) {
            // Write-once: the same entry written twice is the same entry.
        }
    }

    @Override
    List<String> listEntries() {
        final Path directory = getDirectory()
        if( !Files.isDirectory(directory) )
            return []
        final List<String> names = new ArrayList<String>()
        Files.list(directory).withCloseable { stream ->
            for( Object p : stream.toList() )
                names.add(((Path) p).fileName.toString())
        }
        return names
    }

    @Override
    String toString() { "LocalStoreLogStorage[${getDirectory()}]" }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/StoreLog.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The Store Log (DESIGN.md §5, block explorer spec section 3): one write-once
 * empty entry per block written outside a run's closure, under `log/`, named
 * `String.format('%013d', 9999999999999L - writtenAtMillis)`, then the kind,
 * then the address, so a listing sorts newest first. `writtenAtMillis` is when
 * the entry was written to this member, so a merged Bundle's runs sort as new.
 *
 * Derived, like the index: a failure here is logged and never aborts a run
 * (DESIGN.md §0 rule 3).
 */
@Slf4j
@CompileStatic
class StoreLog {

    /** One past the last millisecond this encoding can carry. */
    static final long HORIZON_MILLIS = 9999999999999L

    /**
     * How far before the watermark a catch-up re-reads, so an entry written
     * behind it by a host with a skewed clock is not skipped.
     */
    static final long OVERLAP_MILLIS = 600_000L

    private final StoreLogStorage storage

    StoreLog(StoreLogStorage storage) {
        this.storage = storage
    }

    /** The Store Log of a store's writable member. */
    static StoreLog of(BlockStore store) {
        return new StoreLog(storageOf(store))
    }

    private static StoreLogStorage storageOf(BlockStore store) {
        if( store instanceof LocalBlockStore )
            return new LocalStoreLogStorage(((LocalBlockStore) store).getRoot())
        if( store instanceof CompositeStore )
            return storageOf(((CompositeStore) store).getMembers()[0])
        throw new IllegalArgumentException("no store log storage for ${store?.getClass()?.name}")
    }

    static void append(BlockStore store, StoreLogKind kind, Cid cid, long writtenAtMillis) {
        of(store).append(kind, cid, writtenAtMillis)
    }

    static List<StoreLogEntry> read(BlockStore store) {
        return of(store).read()
    }

    /** The entry name, which is also its watermark form. */
    static String entryName(StoreLogKind kind, Cid cid, long writtenAtMillis) {
        return reverseTimestamp(writtenAtMillis) + '-' + kind.token + '-' + cid.toString()
    }

    static String reverseTimestamp(long writtenAtMillis) {
        final long reverse = HORIZON_MILLIS - writtenAtMillis
        if( reverse < 0 || writtenAtMillis < 0 )
            throw new IllegalArgumentException("a store log timestamp must fall between 0 and $HORIZON_MILLIS, got $writtenAtMillis")
        return String.format('%013d', reverse)
    }

    void append(StoreLogKind kind, Cid cid, long writtenAtMillis) {
        storage.putEntry(entryName(kind, cid, writtenAtMillis))
    }

    /** Every entry, newest first. Names that do not parse are ignored. */
    List<StoreLogEntry> read() {
        final List<StoreLogEntry> entries = new ArrayList<StoreLogEntry>()
        for( String name : storage.listEntries() ) {
            final StoreLogEntry entry = parse(name)
            if( entry != null )
                entries.add(entry)
        }
        Collections.sort(entries)
        return entries
    }

    /**
     * The entries a catch-up must consider, newest first: everything newer
     * than the watermark, the watermark itself, and everything written up to
     * {@link #OVERLAP_MILLIS} before it. A null or empty watermark means
     * everything. The caller skips what it has already ingested.
     */
    List<StoreLogEntry> entriesSince(String watermark) {
        final List<StoreLogEntry> entries = read()
        if( !watermark )
            return entries
        final StoreLogEntry mark = parse(watermark)
        if( mark == null ) {
            log.warn("ignoring an unreadable store log watermark '$watermark'; reading the whole log")
            return entries
        }
        final long floor = mark.writtenAtMillis - OVERLAP_MILLIS
        final List<StoreLogEntry> since = new ArrayList<StoreLogEntry>()
        for( StoreLogEntry entry : entries )
            if( entry.writtenAtMillis >= floor )
                since.add(entry)
        return since
    }

    static StoreLogEntry parse(String name) {
        final String[] parts = name.split('-', 3)
        if( parts.length != 3 || !parts[0].matches(/\d{13}/) ) {
            log.warn("ignoring a store log entry that is not <13 digits>-<kind>-<cid>: '$name'")
            return null
        }
        final StoreLogKind kind = StoreLogKind.fromToken(parts[1])
        if( kind == null ) {
            log.warn("ignoring a store log entry of unknown kind '${parts[1]}': '$name'")
            return null
        }
        if( !Cid.isCid(parts[2]) ) {
            log.warn("ignoring a store log entry whose name does not end in a cid: '$name'")
            return null
        }
        final long written = HORIZON_MILLIS - Long.parseLong(parts[0])
        return new StoreLogEntry(name, parts[0], written, kind, Cid.parse(parts[2]))
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.StoreLogTest'`
Expected: PASS, 13 tests (the `where:` block reports as three).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/StoreLog*.groovy src/main/groovy/robsyme/cas/core/LocalStoreLogStorage.groovy src/test/groovy/robsyme/cas/core/StoreLogTest.groovy
git commit -m "feat(core): Store Log replaces the Run Log: log/<rts>-<kind>-<cid>, write-time rts, overlap-aware entriesSince

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Index and session catch-up on the Store Log, schema 2, two indexes

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (constants at lines 34-36; DDL list at lines 148-173; `catchUp` at 316-328; `watermarkKey` at 330-332; `carryWatermark` at 363-374)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`catchUpIndex` at 124-140, one call and its Javadoc)
- Modify: `src/test/groovy/robsyme/cas/core/IndexTest.groovy` (every `RunLog` reference: lines 516, 525, 528, 529, 541, 543, 554, 595, 603; the schema test near line 134)

**Interfaces:**
- Consumes (Task 1): `StoreLog`, `StoreLogEntry`, `StoreLogKind`, `StoreLog.append(BlockStore, StoreLogKind, Cid, long)`, `StoreLog.read(BlockStore)`, `StoreLog.of(BlockStore)`, `log.entriesSince(String)`.
- Produces:
  - `static final int SCHEMA_VERSION = 2`
  - `void catchUp(BlockStore store, StoreLog log, String member)`
  - `boolean isRunIndexed(Cid completion)`

- [ ] **Step 1: Update existing tests to the new API and add failing ones**

In `IndexTest.groovy`, replace `import`s and calls mechanically:

```
RunLog.append(X, cid, t)   ->  StoreLog.append(X, StoreLogKind.RUN, cid, t)
RunLog.of(X)               ->  StoreLog.of(X)
RunLog.read(X)             ->  StoreLog.read(X)
```

In the schema test near line 134, add the two index names to whatever list of index names it asserts (or add a new assertion if it lists only tables):

```groovy
    def 'schema 2 carries the two query 3 indexes'() {
        expect:
        indexNames().containsAll(['collection_completion_output', 'collection_item_collection'])
        Index.SCHEMA_VERSION == 2
    }

    private List<String> indexNames() {
        final List<String> names = []
        java.sql.DriverManager.getConnection("jdbc:sqlite:${dbFile}").withCloseable { c ->
            c.createStatement().executeQuery("SELECT name FROM sqlite_master WHERE type = 'index'").withCloseable { rs ->
                while( rs.next() ) names << rs.getString(1)
            }
        }
        return names
    }

    def 'query 3 is served by the collection indexes, not a scan'() {
        given:
        final String sql = '''EXPLAIN QUERY PLAN SELECT ci.item_cid FROM collection_item ci
               JOIN collection c ON c.collection_cid = ci.collection_cid
               WHERE c.completion_cid = ? AND c.output_name = ?'''
        final List<String> plan = []
        java.sql.DriverManager.getConnection("jdbc:sqlite:${dbFile}").withCloseable { c ->
            c.prepareStatement(sql).withCloseable { st ->
                st.setString(1, 'x'); st.setString(2, 'y')
                st.executeQuery().withCloseable { rs -> while( rs.next() ) plan << rs.getString('detail') }
            }
        }

        expect:
        plan.any { it.contains('collection_completion_output') }
        plan.any { it.contains('collection_item_collection') }
        !plan.any { it.startsWith('SCAN ci') || it.startsWith('SCAN collection_item') }
    }
```

Rename the existing `'catchUp ingests only the run log entries past the watermark'` test to `'catchUp ingests only entries it has not indexed'` and keep its body, including the final `counting.opens == 0`. That assertion must still hold: the overlap re-lists entries but skips runs already indexed, so it reads no blocks.

Add the Review Focus tests:

```groovy
    def 'an entry written behind the watermark by a skewed clock is picked up'() {
        given: 'a run logged and caught up at t = 20 min'
        def first = buildRun()
        StoreLog.append(store, StoreLogKind.RUN, first, 20 * 60_000L)
        index.catchUp(store, StoreLog.of(store), 'lab')

        when: 'another host logs a run stamped 5 minutes earlier'
        def skewed = buildOtherRun([finished_at: '2026-09-03T11:00:00.000Z'], 'skewed')
        StoreLog.append(store, StoreLogKind.RUN, skewed, 15 * 60_000L)
        def counting = new Counting(store)
        index.catchUp(counting, StoreLog.of(store), 'lab')

        then: 'the skewed run is indexed, and the already-indexed run is not re-read'
        count('run') == 2
        counting.opened.contains(skewed)
        !counting.opened.contains(first)
    }

    def 'the same run logged twice yields one run row'() {
        given:
        def completion = buildRun()
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_000L)
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_001L)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        count('run') == 1
    }

    def 'an entry whose block is absent does not stop catch-up'() {
        given:
        def present = buildRun()
        def absent = Cid.of(Cid.DAG_CBOR, Hashing.sha256('never stored'.getBytes('UTF-8')))
        StoreLog.append(store, StoreLogKind.RUN, present, 1_000_000L)
        StoreLog.append(store, StoreLogKind.RUN, absent, 2_000_000L)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        noExceptionThrown()
        count('run') == 1
        and: 'the watermark still advanced past the absent entry'
        meta('store_log_watermark:lab') == StoreLog.entryName(StoreLogKind.RUN, absent, 2_000_000L)
    }

    def 'selection and claim entries are listed but not ingested yet'() {
        given:
        def completion = buildRun()
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_000L)
        StoreLog.append(store, StoreLogKind.SELECTION, Cid.of(Cid.DAG_CBOR, Hashing.sha256('s'.getBytes('UTF-8'))), 2_000_000L)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        noExceptionThrown()
        count('run') == 1
    }

    def 'a store with only a v1 runs/ directory is still answered after the schema 2 rebuild'() {
        given: 'a run whose blocks exist, logged only in the old layout'
        def completion = buildRun()
        def runs = tempDir.resolve('store').resolve('runs')
        java.nio.file.Files.createDirectories(runs)
        java.nio.file.Files.createFile(runs.resolve("${String.format('%013d', 9999999999999L - 1_000_000L)}-${completion}"))

        when: 'the index is rebuilt, as a schema bump does'
        index.rebuild(store, 'lab')

        then:
        count('run') == 1
    }
```

If `IndexTest` has no `meta(String)` helper, add one next to `count`:

```groovy
    private String meta(String key) {
        String value = null
        java.sql.DriverManager.getConnection("jdbc:sqlite:${dbFile}").withCloseable { c ->
            c.prepareStatement('SELECT value FROM meta WHERE key = ?').withCloseable { st ->
                st.setString(1, key)
                st.executeQuery().withCloseable { rs -> if( rs.next() ) value = rs.getString(1) }
            }
        }
        return value
    }
```

`buildRun()`, `buildOtherRun(...)`, `Counting`, `count(...)`, `store`, `storeRoot`, `dbFile` and `tempDir` already exist in `IndexTest` (`setup()` at lines 33-37); reuse them. In the v1-layout test, use `storeRoot.resolve('runs')` rather than `tempDir.resolve('store').resolve('runs')`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexTest'`
Expected: FAIL on `schema 2 carries the two query 3 indexes`, `query 3 is served by the collection indexes`, the skewed-clock test and the absent-block test's watermark key.

- [ ] **Step 3: Implement**

In `Index.groovy`:

```groovy
    static final int SCHEMA_VERSION = 2
    private static final String META_WATERMARK = 'store_log_watermark'
```

Add to the DDL list, after `'CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT)',`:

```groovy
            'CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)',
            'CREATE INDEX collection_item_collection ON collection_item(collection_cid)',
```

Replace `catchUp`. The parameter is `storeLog`, not `log`, because `log` is the `@Slf4j` logger:

```groovy
    /**
     * Ingests every Store Log entry this index has not seen, oldest first,
     * then advances the watermark to the newest entry listed. Re-reads the
     * overlap window before the watermark (StoreLog.OVERLAP_MILLIS) so an entry
     * written behind it is not skipped, and skips runs already indexed so the
     * overlap costs a listing, never a block read. Only `run` entries are
     * ingested for now; Selections and Claims arrive with the explorer.
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member) {
        // The watermark is per member: in a composition each member's log
        // advances independently (DESIGN.md §12).
        final String key = watermarkKey(member)
        final List<StoreLogEntry> entries = storeLog.entriesSince(meta(key))
        if( !entries )
            return
        // entriesSince is newest first; ingest in the order they were written.
        for( int i = entries.size() - 1; i >= 0; i-- ) {
            final StoreLogEntry entry = entries[i]
            if( entry.kind != StoreLogKind.RUN ) {
                log.debug("store log entry ${entry.name} is a ${entry.kind.token}; not ingested until the explorer lands")
                continue
            }
            if( isRunIndexed(entry.cid) )
                continue
            // An absent RunCompletion is not an error: ingestRun records a
            // `missing` row and returns, and the run is indexed when it arrives.
            ingestRun(store, entry.cid, member)
        }
        setMeta(key, entries[0].name)
    }

    /** True when a RunCompletion already has its `run` row. */
    boolean isRunIndexed(Cid completion) {
        boolean found = false
        query('SELECT 1 FROM run WHERE completion_cid = ?', [completion.toString()]) { ResultSet rs -> found = true }
        return found
    }
```

`StoreLog`, `StoreLogEntry` and `StoreLogKind` are in the same package, so no imports are needed. In `carryWatermark`, replace `RunLog.read(store)` with `StoreLog.read(store)` and the `List<RunLogEntry>` type with `List<StoreLogEntry>`. Update the comments that say "run log" to "Store Log".

In `CasSession.groovy`, change `index.catchUp(member, RunLog.of(member), member.alias())` to `index.catchUp(member, StoreLog.of(member), member.alias())`, replace the `RunLog` import with `StoreLog`, and change "run log" to "Store Log" in the Javadoc of `catchUpIndex`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew test`
Expected: PASS, including the new `IndexTest` cases. The whole suite compiles because the old `RunLog*` classes are still present; `CasObserver` still uses them until Task 3.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/CasSession.groovy src/test/groovy/robsyme/cas/core/IndexTest.groovy
git commit -m "feat(core): index schema 2 on the Store Log, with the two query 3 indexes and a catch-up overlap

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The observer writes the Store Log; the Run Log classes go

**Files:**
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (line 151 call; `appendRunLog` at 229-236)
- Modify: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy` (import at line 13; assertion at line 140)
- Delete: `src/main/groovy/robsyme/cas/core/RunLog.groovy`, `RunLogEntry.groovy`, `RunLogStorage.groovy`, `LocalRunLogStorage.groovy`; `src/test/groovy/robsyme/cas/core/RunLogTest.groovy`

**Interfaces:**
- Consumes (Task 1): `StoreLog.append(BlockStore, StoreLogKind, Cid, long)`, `StoreLog.read(BlockStore)`.
- Produces: `protected long nowMillis()` on `CasObserver`, the test seam for write time.

- [ ] **Step 1: Write the failing test**

In `CasObserverTest.groovy`, replace `import robsyme.cas.core.RunLog` with `import robsyme.cas.core.StoreLog` and `import robsyme.cas.core.StoreLogKind`, change line 140 to `StoreLog.read(cas.store).size() == 1`, and add after the test `'onFlowComplete twice writes exactly one RunCompletion and one run log entry'` (rename that test's title to `'... and one Store Log entry'`):

```groovy
    def 'the Store Log entry is stamped with the time it was written, not the run finish time'() {
        given: 'the usual binding, then an observer whose clock is fixed'
        final long fixed = 1_800_000_000_000L
        bind(config())
        observer = new CasObserver() {
            @Override
            protected long nowMillis() { return fixed }
        }
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final entries = StoreLog.read(cas.store)
        entries.size() == 1
        entries[0].kind == StoreLogKind.RUN
        entries[0].writtenAtMillis == fixed
    }
```

`bind(config())` builds a fresh `CasObserver`; the test replaces it with the subclass before driving it.

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest'`
Expected: FAIL; `nowMillis` does not exist, or the entry's time equals the run's `complete` time.

- [ ] **Step 3: Implement**

In `CasObserver.groovy`, change line 151 from `appendRunLog(completion, epochMillis(meta?.complete))` to `appendStoreLog(completion)`, and replace `appendRunLog` with:

```groovy
    private void appendStoreLog(Cid completion) {
        try {
            StoreLog.append(cas.store, StoreLogKind.RUN, completion, nowMillis())
        }
        catch( Exception e ) {
            log.warn("the store log entry for ${completion} could not be written; it is derived: ${e.message}", e)
        }
    }

    /** When the entry is written; overridable so a test can fix the clock. */
    protected long nowMillis() {
        return System.currentTimeMillis()
    }
```

Replace the `RunLog` import with `StoreLog` and `StoreLogKind`. If `epochMillis` has no other caller after this change, delete it.

Then delete the Run Log classes, which nothing uses any more:

```bash
git rm src/main/groovy/robsyme/cas/core/RunLog.groovy \
       src/main/groovy/robsyme/cas/core/RunLogEntry.groovy \
       src/main/groovy/robsyme/cas/core/RunLogStorage.groovy \
       src/main/groovy/robsyme/cas/core/LocalRunLogStorage.groovy \
       src/test/groovy/robsyme/cas/core/RunLogTest.groovy
```

- [ ] **Step 4: Run the whole suite**

Run: `./gradlew test`
Expected: PASS. No file under `src/` mentions `RunLog`: `grep -rn 'RunLog' src/` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy
git commit -m "feat: the observer writes run entries to the Store Log at write time; the Run Log classes are removed

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The Gate reads the Store Log

**Files:**
- Modify: `gate/cas.py` (`run_log` at lines 449-462)
- Modify: `gate/assert.py` (`snapshot` at 256-264, `read_snapshot` at 266-282, `write_snapshot` at 285-294, the `"runs"` loop at 505, assertion 3's use at 628-639, `_latest_successful_from_run_log` at 666-677)
- Modify: `gate/fixtures/make_fixture.py` (`run_log` at 123-127 and its callers)
- Modify: `gate/test_cas.py` (285-297), `gate/test_assert.py` (57, 302-310, 325-345)
- Modify: `gate/README.md` (mentions of `runs/`)

Python only; no Groovy. Code against the entry format in Global Constraints.

**Interfaces:**
- Produces: `Store.store_log() -> [(rts, kind, cid)]` newest first; `Gate.snapshot()["log"]`; `_latest_successful_from_store_log(gate, pipeline)`; fixture builder `store_log(written_millis, kind, cid)`.

- [ ] **Step 1: Write the failing tests**

In `gate/test_cas.py`, replace the run-log tests with:

```python
    def test_store_log_reads_newest_first_with_kind(self):
        log = os.path.join(self.root, "log")
        os.makedirs(log)
        older = "%013d" % (9999999999999 - 1000)
        newer = "%013d" % (9999999999999 - 5000)
        open(os.path.join(log, "%s-run-%s" % (newer, CID_DAGCBOR_EMPTY_MAP)), "w").close()
        open(os.path.join(log, "%s-claim-%s" % (older, CID_RAW_EMPTY)), "w").close()
        open(os.path.join(log, "%s-pin-%s" % (older, CID_RAW_EMPTY)), "w").close()   # unknown kind: ignored
        open(os.path.join(log, "README"), "w").close()                               # stray file: ignored
        self.assertEqual([(kind, cid) for _rts, kind, cid in self.store.store_log()],
                         [("run", CID_DAGCBOR_EMPTY_MAP), ("claim", CID_RAW_EMPTY)])

    def test_store_log_on_missing_directory_is_empty(self):
        self.assertEqual(self.store.store_log(), [])

    def test_runs_directory_is_not_read(self):
        runs = os.path.join(self.root, "runs")
        os.makedirs(runs)
        open(os.path.join(runs, "%013d-%s" % (1, CID_DAGCBOR_EMPTY_MAP)), "w").close()
        self.assertEqual(self.store.store_log(), [])
```

In `gate/test_assert.py`, change the fixture helper at line 336 to `self.builder.store_log(finished, "run", completion)`, and replace the latest-run test with two:

```python
    def test_store_log_returns_the_newest_successful_run(self):
        older = self._run("cold", "succeeded", 1000)
        newer = self._run("again", "succeeded", 2000)
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"), newer)
        self.assertNotEqual(newer, older)

    def test_latest_is_by_finished_at_not_by_log_order(self):
        # The run that finished later was logged first (a merged Bundle, or a
        # skewed clock): latest must still be the later finish.
        later_finish = self._run("cold", "succeeded", 1000, finished_at="2026-01-01T02:00:00.000Z")
        earlier_finish = self._run("again", "succeeded", 2000, finished_at="2026-01-01T01:00:00.000Z")
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"), later_finish)
```

Give `_run` a `finished_at="2026-01-01T00:01:00.000Z"` keyword argument and pass it into the RunCompletion's `finished_at` field. Adjust the `gate.runs` test at 302-310 only if it depends on the `runs/` directory; it reads RunManifest blocks, so it most likely needs no change.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd gate && python3 -m unittest test_cas test_assert -v`
Expected: FAIL, `AttributeError: 'Store' object has no attribute 'store_log'`.

- [ ] **Step 3: Implement**

`gate/cas.py`, replacing `run_log`:

```python
    STORE_LOG_KINDS = ("run", "selection", "claim")

    def store_log(self):
        """[(reverse_ts, kind, cid)] from log/<rts>-<kind>-<cid>, newest first.
        Unknown kinds and malformed names are ignored, as the plugin does."""
        root = os.path.join(self.root, "log")
        if not os.path.isdir(root):
            return []
        out = []
        for name in sorted(os.listdir(root)):
            parts = name.split("-", 2)
            if len(parts) != 3 or len(parts[0]) != 13 or not parts[0].isdigit():
                continue
            rts, kind, cid = parts
            if kind not in self.STORE_LOG_KINDS or not is_cid(cid):
                continue
            out.append((rts, kind, cid))
        return out
```

(`STORE_LOG_KINDS` is a class attribute on the same class that defines `run_log` today.)

`gate/assert.py`:

- In `snapshot`, replace the `"runs"` key with `"log": sorted(name for name in _listdir(self.store.path("log"))),`.
- In `read_snapshot`, change `out = {"blocks": [], "runs": [], "nf": [], "coords": {}}` to use `"log"`.
- In `write_snapshot` and the comparison loop at line 505, change `("blocks", "runs", "nf")` to `("blocks", "log", "nf")`; `write_snapshot`'s return tuple uses `data["log"]`.
- Replace `_latest_successful_from_run_log` with:

```python
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
        key = (block.get("finished_at") or "", cid)
        if best is None or key > best[0]:
            best = (key, cid)
    return best[1] if best else None
```

- In assertion 3 (lines 628-647), rename the call to `_latest_successful_from_store_log`, and change the wording `"the store run log"` to `"the Store Log"` and `"run log"` to `"Store Log"` in its messages.

`gate/fixtures/make_fixture.py`, replacing `run_log`:

```python
    def store_log(self, written_millis, kind, cid):
        d = os.path.join(self.root, "log")
        os.makedirs(d, exist_ok=True)
        rts = "%013d" % (9999999999999 - written_millis)
        open(os.path.join(d, "%s-%s-%s" % (rts, kind, cid)), "w").close()
```

and change every `run_log(finished_millis, completion)` call in the file to `store_log(finished_millis, "run", completion)`. The fixture's written time may equal its finish time; the fixture is deterministic either way.

`gate/README.md`: replace `runs/` with `log/` and "run log" with "Store Log".

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd gate && python3 -m unittest test_cas test_assert -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gate/cas.py gate/assert.py gate/fixtures/make_fixture.py gate/test_cas.py gate/test_assert.py gate/README.md
git commit -m "test(gate): read the Store Log; latest successful run ranks by finished_at, not log order

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Regenerate the fixture and run the Gate

**Files:**
- Regenerate: `gate/fixtures/root/**` via `gate/fixtures/make_fixture.py`

**Interfaces:**
- Consumes: Tasks 1 to 4. The branch compiles and its unit tests pass before this task starts.

- [ ] **Step 1: Regenerate the fixture and check offline**

```bash
python3 gate/fixtures/make_fixture.py
test ! -d gate/fixtures/root/store/runs && ls gate/fixtures/root/store/log | head -3
python3 gate/assert.py gate/fixtures/root --offline
```

Expected: `log/` entries named `<13 digits>-run-bafyrei…`, no `runs/`, and the same PASS/SKIP counts as before this plan (README: 10 PASS, 0 FAIL, 7 SKIP offline).

- [ ] **Step 2: Run the unit tests and the smoke test**

```bash
./gradlew test
make smoke
```

Expected: all green.

- [ ] **Step 3: Run the real Gate**

```bash
make gate
```

Expected: `11 PASS, 0 FAIL, 6 SKIP`, the same as on 2026-09-24 before this plan. If an assertion fails, fix the cause in the task that owns the file, never by weakening the assertion. Then check the store it wrote:

```bash
ls "$GATE_ROOT"/store/log | head -3        # GATE_ROOT is printed at the top of the Gate's output
test ! -d "$GATE_ROOT"/store/runs && echo "no runs/"
sqlite3 "$GATE_ROOT"/cache/nf-blocks/*.sqlite "SELECT version FROM schema_version; SELECT name FROM sqlite_master WHERE name LIKE 'collection_%';"
```

Expected: `log/…-run-…` entries, `no runs/`, version `2`, and both index names.

- [ ] **Step 4: Commit the regenerated fixture**

```bash
git add -A gate/fixtures/root
git commit -m "test(gate): regenerate the fixture on the Store Log layout

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
