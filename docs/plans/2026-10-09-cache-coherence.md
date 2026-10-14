# Cache coherence (DESIGN §20) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A per-user cache index notices when a store member was replaced or partly swept, and corrects itself, so no cache shows or re-exports runs whose blocks are gone.

**Architecture:** Each member gets a `store-id` file (created once, create-if-absent). `CasSession.catchUpIndex` reads every member's id before catching up and lets `Index.reconcileStoreIds` reset the cache when one changed. A sweep writes a `Forget` block and a `log/<rts>-forget-<cid>` entry after each delete batch; catch-up applies them with `Index.forget`, so every cache forgets what any sweep deleted. The Index Snapshot carries `store_id`; seeding and the snapshot guard check it, the guard compares watermarks, and `fewer_runs` adds back runs that `forget` entries removed. A `reindex` verb drops a cache and rebuilds it. Gate assertions 17 and 18 prove it from outside.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, SQLite (xerial), Nextflow 26.04.6 plugin API, AWS SDK v2 via nf-amazon 3.9.2 (only in `robsyme.cas.s3`), the explorer page (`node --test`), Python 3 Gate harness (stdlib only), bash.

**Spec:** `DESIGN.md` §20 (commit `5475339`), with the pointers it added to §5, §12, §14 and §15. Read §20 whole before Task 1; the decision numbers below are §20's.

**Branch:** `design/cache-coherence`, already created from `main` at `98bde42`; §20 is its first commit. The version stays `0.5.0-beta.1` until the release after acceptance.

## Decisions this plan makes where §20 is silent or where it deviates

Task 10 records each in DESIGN.md §20 under "Decisions made while building". A reviewer may reject any of them.

1. **A changed store id resets the whole cache, not only that member's rows (deviates from decision 4).** Per-member dropping needs the closure of every run (collections, items, attrs, producers) minus rows another member still announces; the cache has no per-member ownership for most tables. A reset (every table emptied, `meta` cleared) followed by the usual cold catch-up of every member is simple and always right; the other members reseed from their snapshots. An id changes only when a member is deleted and recreated, so the cost is rare.
2. **Ids are reconciled for every member before any member is caught up** (`CasSession.catchUpIndex`), so a reset triggered by the second member never discards rows the first one just caught up.
3. **A member whose `store-id` cannot be read (an S3 403, the network) is left out of reconciliation** for that call: no reset, no adoption. A failure to look is not evidence of replacement.
4. **The cache adopts an id only when it can show its rows are from this incarnation**: it holds nothing for the member (no watermark and no `block_scan` mark), or its watermark entry is still in the member's log. A cache with rows but no watermark (a pre-Store-Log member that was scanned) cannot show it, so it resets. When the log check itself fails, the member is left out (decision 3).
5. **The writable member's id is created by `CasSession.writableStoreId()`**, called from `catchUpIndex` and from `CasObserver.onFlowCreate` before the Live Writer registration. Every verb that holds the member writable goes through `catchUpIndex`, so decision 2 of §20 needs no per-verb code. `items` on a writable config therefore also creates the id; it is the writable member, so that is allowed.
6. **A Forget block lists every deleted block whose CID codec is dag-cbor (deviates from decision 6).** The deleted blocks are dead, so the mark never decoded them and the sweep does not know their kinds without reading each one. Filtering by codec costs nothing; it lets DirectoryManifests in, which have no index rows, so forgetting them is a no-op. A sweep that deletes only raw blocks writes no entry. Gate assertion 9 deletes released DirectoryManifests, so it now writes `forget` entries (deviates from decision 6's last sentence); Task 9 makes its checkpoints ignore Forget blocks.
7. **Store Log entries of deleted blocks are still deleted once, after every batch (deviates from decision 7's per-batch order).** Reading the whole log per batch is a listing per batch on S3. The `forget` entry is written right after its batch, which is the part other caches depend on.
8. **The sweeper's own cache forgets through catch-up**, as decision 7 says, except for cids whose `forget` entry could not be written (`SweepReport.unannounced`): `RetentionCommands.forgetAndSnapshot` forgets those directly and the sweep warns that other caches need `reindex`.
9. **Catch-up applies a `forget` entry once per member**, recorded in `meta` as `forget:<member>:<forget cid>` = `<entry name> <runs forgotten>`. A cid any member of the composition holds now (`CompositeStore.has`) is not forgotten: it is still held elsewhere, or was written again since the sweep (an OutputItem a later run reproduced). That is one `has` per listed cid per cache, once.
10. **No schema version bump.** The forgotten-run counts live in `meta`, not a new table (§20 decision 10 named a `forgotten` table; a new table changes `Index.ddl()`, which is also the snapshot's DDL, and would force `index/v4.sqlite` on the page and the Gate).
11. **`SnapshotBase` carries `storeId` and `watermark`**, and `SnapshotStorage.replace` takes a `SnapshotBase` describing what it writes (`replace(Path built, SnapshotBase written, SnapshotBase base)`). On S3 they ride as `x-amz-meta-store-id` and `x-amz-meta-watermark` beside `x-amz-meta-runs`; an object without `runs` is still downloaded once, and then all three are read from the file.
12. **Snapshot guard order:** `over_cap` (base), `stale_store`, then build, `over_cap` (built), `older_watermark`, `fewer_runs`, `replaced_meanwhile`. Watermarks compare by their 13-digit reverse timestamp; a smaller one is newer; equal is not newer. A base with a watermark beats a build with none.
13. **The page does not apply `forget` entries.** Its Store Log regex keeps ignoring unknown kinds; it reads the snapshot, which the sweep and every later writer rebuild after applying them. Only `web/src/generated/schema.json` changes.
14. **`reindex --seed false`** passes no snapshot storage to the cold catch-up, so every member is scanned.

## Global Constraints

- DESIGN §0 rule 1: released Nextflow 26.04.6 only; no `includeBuild`, no absolute paths in `build.gradle`.
- DESIGN §0 rule 2: memory bound. A Forget block is at most one delete batch (`Sweep.BATCH` = 1000 cids, about 40 KB); nothing here reads file content.
- DESIGN §0 rule 3: the index, the snapshot, `store-id` reconciliation and `forget` entries are derived; a failure in any of them logs and continues and never aborts a run. Creating the writable member's `store-id` at `onFlowCreate` warns and continues on failure.
- §20 decision 1, verbatim: `store-id` is "one line, a random 128-bit id as 32 lowercase hex characters, then `\n`. Not a block, not a root, not swept, never rewritten."
- §6: blocks carry nothing store-local: no timestamps (except run facts and a Claim's advisory one), no sweep ids, no aliases. A Forget block is `{kind: 'Forget', schema: 1, asserted_by, deleted: [cid...]}`, `deleted` sorted by cid string, no duplicates.
- Store Log kind token is `forget`: `log/<13-digit rts>-forget-<cid>`.
- All main classes `@CompileStatic`. Only `robsyme.cas.s3` imports `software.amazon.awssdk.*`. `robsyme.cas.core` imports nothing from Nextflow.
- Console output from a run goes through the `nextflow.cas` logger (`ConsoleLog.LOG`, or `Index`'s `CONSOLE`).
- The shell's `grep` is an ugrep alias that misbehaves, and `core/Records.groovy` holds a binary byte: use `/usr/bin/grep -a`.
- Subagents never search the whole filesystem (`find /`, anything rooted at `/` or `~`) and stop every background process they start before reporting.
- Do not edit `../.scratch/content-addressed-lineage/test-pipeline/` or `gate/tier2/small/`.
- Every commit is `git commit -s` and its message ends with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (the steps show the subject only).
- Unit tests: `./gradlew test --tests '<Spec>'` for one spec; `./gradlew check` for all. Web: `cd web && npm test`. Gate unit tests: `python3 -m unittest discover -s gate`.

## Review Focus

1. **A member whose `store-id` cannot be read must not reset the cache** (an S3 403 on the member root, a flaky network): the next call tries again. Test `an unreadable store-id is left out: no reset, no adoption` in Task 2.
2. **Two writers creating `store-id` at once end with one id, and both use it.** Tests `two writers racing to create store-id agree on one id` (local, Task 1) and `S3: a lost If-None-Match race returns the winner's id` (Task 1).
3. **An item reproduced after a sweep keeps its rows when a cache applies the old `forget` entry.** Test `a listed cid the store holds again is not forgotten` in Task 5.
4. **A `forget` entry naming a block a read-only member still holds leaves its rows.** Test `a listed cid another member holds is not forgotten` in Task 5.
5. **A cache that missed a sweep cannot overwrite the sweeper's snapshot with the swept runs**, and the sweeper's own rewrite is not refused as `fewer_runs`. Tests `a sweep that deletes a run rewrites the snapshot` (Task 6) and `a cache behind the stored snapshot's watermark does not replace it (older_watermark)` (Task 7); Gate assertion 18.

---

### Task 1: `store-id` storage and the Store Log `has` check

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/StoreIdStorage.groovy`
- Create: `src/main/groovy/robsyme/cas/core/IdentifiedStore.groovy`
- Create: `src/main/groovy/robsyme/cas/core/StoreIds.groovy`
- Create: `src/main/groovy/robsyme/cas/core/LocalStoreIdStorage.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3StoreIdStorage.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/LocalBlockStore.groovy` (implements `IdentifiedStore`)
- Modify: `src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy` (implements `IdentifiedStore`)
- Modify: `src/main/groovy/robsyme/cas/core/StoreLogStorage.groovy`, `LocalStoreLogStorage.groovy`, `src/main/groovy/robsyme/cas/s3/S3StoreLogStorage.groovy`, `src/main/groovy/robsyme/cas/core/StoreLog.groovy`
- Test: `src/test/groovy/robsyme/cas/core/StoreIdsTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3StoreIdStorageTest.groovy`, `src/test/groovy/robsyme/cas/core/StoreLogTest.groovy` (append), `src/test/groovy/robsyme/cas/s3/S3StoreLogStorageTest.groovy` (append)

**Interfaces:**
- Produces `interface StoreIdStorage { String read(); String create(String id); String describe() }`: `read()` is the member's id, or null when absent or malformed (malformed warns once); `create(id)` writes `id + '\n'` only if absent and returns the id now there (ours, or the winner's).
- Produces `interface IdentifiedStore { StoreIdStorage storeIdStorage() }`, implemented by `LocalBlockStore` and `S3BlockStore`.
- Produces `class StoreIds { static final String FILE = 'store-id'; static String newId(); static boolean valid(String id); static String parse(byte[] body); static String ensure(StoreIdStorage s); static String read(BlockStore b) }`. `ensure` reads, creates when absent, returns the id. `read(b)` is null when `b` is not an `IdentifiedStore`.
- Produces `boolean StoreLogStorage.hasEntry(String name)` and `boolean StoreLog.has(String name)`.

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/robsyme/cas/core/StoreIdsTest.groovy`:

```groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.Executors

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20 decisions 1 and 2. */
class StoreIdsTest extends Specification {

    @TempDir Path tmp

    def 'a new id is 32 lowercase hex characters, and two are different'() {
        expect:
        StoreIds.newId() ==~ /[0-9a-f]{32}/
        StoreIds.newId() != StoreIds.newId()
    }

    def 'absent, the id reads null; ensure creates it once and reads it back'() {
        given:
        final LocalBlockStore store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)

        expect:
        store.storeIdStorage().read() == null

        when:
        final String id = StoreIds.ensure(store.storeIdStorage())

        then:
        id ==~ /[0-9a-f]{32}/
        Files.readString(tmp.resolve('lab/store-id')) == id + '\n'
        StoreIds.ensure(store.storeIdStorage()) == id
        StoreIds.read(store) == id
    }

    def 'create never rewrites: the second id is refused and the first returned'() {
        given:
        final LocalStoreIdStorage s = new LocalStoreIdStorage(tmp.resolve('lab'))
        final String first = s.create('a' * 32)

        expect:
        first == 'a' * 32
        s.create('b' * 32) == 'a' * 32
        s.read() == 'a' * 32
    }

    def 'two writers racing to create store-id agree on one id'() {
        given:
        final def pool = Executors.newFixedThreadPool(8)
        final List<Callable<String>> calls = (1..8).collect { int i ->
            ({ -> new LocalStoreIdStorage(tmp.resolve('lab')).create(StoreIds.newId()) } as Callable<String>)
        }

        when:
        final Set<String> seen = pool.invokeAll(calls)*.get() as Set

        then:
        seen.size() == 1
        new LocalStoreIdStorage(tmp.resolve('lab')).read() == seen[0]

        cleanup:
        pool.shutdownNow()
    }

    def 'a malformed store-id reads as null and leaves no temp file behind'() {
        given:
        Files.createDirectories(tmp.resolve('lab'))
        Files.writeString(tmp.resolve('lab/store-id'), 'not an id\n')

        expect:
        new LocalStoreIdStorage(tmp.resolve('lab')).read() == null
        Files.list(tmp.resolve('lab')).withCloseable { it.toList() }*.fileName*.toString() == ['store-id']
    }

    def 'a store that is not identified has no id'() {
        expect:
        StoreIds.read(Mock(BlockStore)) == null
    }
}
```

`src/test/groovy/robsyme/cas/s3/S3StoreIdStorageTest.groovy`:

```groovy
package robsyme.cas.s3

import spock.lang.Specification

class S3StoreIdStorageTest extends Specification {

    MemoryS3Ops s3 = new MemoryS3Ops('member')
    S3StoreIdStorage storage = new S3StoreIdStorage(s3, 'cas/')

    def 'absent reads null; create puts the id with a newline under the member prefix'() {
        expect:
        storage.read() == null
        storage.create('a' * 32) == 'a' * 32
        s3.objects['cas/store-id'].text() == 'a' * 32 + '\n'
        storage.read() == 'a' * 32
    }

    def 'S3: a lost If-None-Match race returns the winner\'s id'() {
        given:
        storage.create('a' * 32)

        expect:
        storage.create('b' * 32) == 'a' * 32
        s3.objects['cas/store-id'].text() == 'a' * 32 + '\n'
    }
}
```

Append to `src/test/groovy/robsyme/cas/core/StoreLogTest.groovy` (inside the class):

```groovy
    def 'has answers whether one entry name is in the log'() {
        given:
        final LocalBlockStore store = new LocalBlockStore(tmp.resolve('has'), 'lab', true)
        final Cid c = store.putDagCbor(Fixtures.runManifest())
        StoreLog.append(store, StoreLogKind.RUN, c, 1_790_000_000_000L)
        final String name = StoreLog.entryName(StoreLogKind.RUN, c, 1_790_000_000_000L)

        expect:
        StoreLog.of(store).has(name)
        !StoreLog.of(store).has(StoreLog.entryName(StoreLogKind.RUN, c, 1_790_000_000_001L))
    }
```

(If `StoreLogTest` names its temp directory differently, use its field.)

Append to `src/test/groovy/robsyme/cas/s3/S3StoreLogStorageTest.groovy` (use the spec's existing `MemoryS3Ops` and storage fields; if it has none, construct `new S3StoreLogStorage(new MemoryS3Ops('member'), 'cas/')`):

```groovy
    def 'hasEntry is one HEAD on log/<name>'() {
        given:
        final MemoryS3Ops ops = new MemoryS3Ops('member')
        final S3StoreLogStorage log = new S3StoreLogStorage(ops, 'cas/')
        log.putEntry('9999999999998-run-x')

        expect:
        log.hasEntry('9999999999998-run-x')
        !log.hasEntry('9999999999997-run-x')
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.StoreIdsTest' --tests 'robsyme.cas.s3.S3StoreIdStorageTest' --tests 'robsyme.cas.core.StoreLogTest' --tests 'robsyme.cas.s3.S3StoreLogStorageTest'`
Expected: compilation failure (`StoreIds`, `LocalStoreIdStorage`, `S3StoreIdStorage`, `hasEntry`, `has` not defined).

- [ ] **Step 3: Implement**

`src/main/groovy/robsyme/cas/core/StoreIdStorage.groovy`:

```groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** A member's `store-id` (DESIGN.md §20 decision 1): not a block, written once, never rewritten. */
@CompileStatic
interface StoreIdStorage {
    /** The member's id, or null when there is none or it does not parse. */
    String read()
    /** Writes `id` only when the member has none; the id there afterwards, ours or the winner's. */
    String create(String id)
    String describe()
}
```

`src/main/groovy/robsyme/cas/core/IdentifiedStore.groovy`:

```groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** A block store that knows where its member's `store-id` lives (as LoggedStore is for the Store Log). */
@CompileStatic
interface IdentifiedStore {
    StoreIdStorage storeIdStorage()
}
```

`src/main/groovy/robsyme/cas/core/StoreIds.groovy`:

```groovy
package robsyme.cas.core

import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/** Ids of member incarnations (DESIGN.md §20 decisions 1 and 2). */
@Slf4j
@CompileStatic
class StoreIds {

    static final String FILE = 'store-id'
    private static final SecureRandom RANDOM = new SecureRandom()
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    static String newId() {
        final byte[] bytes = new byte[16]
        RANDOM.nextBytes(bytes)
        final StringBuilder hex = new StringBuilder(32)
        for( byte b : bytes )
            hex.append(Integer.toHexString((b & 0xff) | 0x100).substring(1))
        return hex.toString()
    }

    static boolean valid(String id) { id != null && id ==~ /[0-9a-f]{32}/ }

    /** The id in a `store-id` body, or null (warned once per describe()) when it is not one. */
    static String parse(byte[] body, String where) {
        final String text = new String(body, 'UTF-8').trim()
        if( valid(text) )
            return text
        if( WARNED.add(where) )
            log.warn("ignoring ${where}: it does not hold a store id (32 lowercase hex characters)")
        return null
    }

    /** Reads the id, creating it when absent. */
    static String ensure(StoreIdStorage storage) {
        final String id = storage.read()
        return id != null ? id : storage.create(newId())
    }

    /** The member's id, or null when the store keeps none or has none. */
    static String read(BlockStore store) {
        return store instanceof IdentifiedStore ? ((IdentifiedStore) store).storeIdStorage().read() : null
    }
}
```

`src/main/groovy/robsyme/cas/core/LocalStoreIdStorage.groovy` (temp file, then an atomic create-if-absent link, the block write's rule, §5):

```groovy
package robsyme.cas.core

import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import groovy.transform.CompileStatic

/** `<member>/store-id` on a local filesystem. */
@CompileStatic
class LocalStoreIdStorage implements StoreIdStorage {

    private final Path root

    LocalStoreIdStorage(Path root) { this.root = root }

    private Path file() { root.resolve(StoreIds.FILE) }

    @Override
    String read() {
        try {
            return StoreIds.parse(Files.readAllBytes(file()), file().toString())
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    String create(String id) {
        if( !StoreIds.valid(id) )
            throw new IllegalArgumentException("not a store id: '${id}'")
        Files.createDirectories(root)
        final Path temp = root.resolve(".tmp-store-id-${UUID.randomUUID()}")
        try {
            Files.write(temp, (id + '\n').getBytes('UTF-8'), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
            try {
                Files.createLink(file(), temp)
            }
            catch( FileAlreadyExistsException e ) {
                // Another writer won; its id stands.
            }
        }
        finally {
            Files.deleteIfExists(temp)
        }
        return read()
    }

    @Override
    String describe() { file().toString() }
}
```

`src/main/groovy/robsyme/cas/s3/S3StoreIdStorage.groovy` (follow `S3RetentionStorage`'s STANDARD class for small rewritten objects; `put` with `ifNoneMatch()`; a 412 or 409 means another writer won, so read it back):

```groovy
package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.StoreIdStorage
import robsyme.cas.core.StoreIds

/** `<prefix>store-id` in an S3 member: created with If-None-Match: *, never rewritten. */
@CompileStatic
class S3StoreIdStorage implements StoreIdStorage {

    private final S3Ops ops
    private final String key

    S3StoreIdStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.key = (prefix ?: '') + StoreIds.FILE
    }

    @Override
    String read() {
        final InputStream in = ops.get(key, null, 0L, -1L)
        if( in == null )
            return null
        final byte[] body = in.withCloseable { InputStream s -> s.readAllBytes() }
        return StoreIds.parse(body, describe())
    }

    @Override
    String create(String id) {
        if( !StoreIds.valid(id) )
            throw new IllegalArgumentException("not a store id: '${id}'")
        ops.put(key, S3Body.ofBytes((id + '\n').getBytes('UTF-8')),
            S3PutOptions.create().ifNoneMatch().contentType('text/plain; charset=utf-8').storageClass(S3RetentionStorage.RETENTION_CLASS))
        return read()
    }

    @Override
    String describe() { "${ops.describe()}/${key}".toString() }
}
```

Check `S3PutOptions` for the storage-class setter `S3RetentionStorage.retention()` uses (`/usr/bin/grep -a -n "retention()" -A4 src/main/groovy/robsyme/cas/s3/S3RetentionStorage.groovy`) and use the same call; if it has none, drop `.storageClass(...)`.

`LocalBlockStore`: add `IdentifiedStore` to its `implements` list and, beside `storeLogStorage()`:

```groovy
    @Override
    StoreIdStorage storeIdStorage() { new LocalStoreIdStorage(root) }
```

`S3BlockStore`: add `IdentifiedStore` and, beside its `storeLogStorage()`:

```groovy
    @Override StoreIdStorage storeIdStorage() { new S3StoreIdStorage(ops, prefix) }
```

(import `robsyme.cas.core.IdentifiedStore` and `robsyme.cas.core.StoreIdStorage`).

`StoreLogStorage`: add

```groovy
    /** Whether the entry is there now. */
    boolean hasEntry(String name)
```

`LocalStoreLogStorage`:

```groovy
    @Override
    boolean hasEntry(String name) { Files.isRegularFile(getDirectory().resolve(name)) }
```

`S3StoreLogStorage` (its prefix field is `under`, the key prefix ending `log/`):

```groovy
    @Override
    boolean hasEntry(String name) { ops.head(under + name) != null }
```

`StoreLog`: beside `read()`, add (use the field that holds the `StoreLogStorage`; `append` uses it as `storage`):

```groovy
    /** Whether this exact entry is in the log now (DESIGN.md §20 decisions 3 and 9). */
    boolean has(String name) { storage.hasEntry(name) }
```

Search the tests for other `StoreLogStorage` implementations (`/usr/bin/grep -a -rln "StoreLogStorage" src/test`) and give each `hasEntry` (a fake can answer `listEntries().contains(name)`).

- [ ] **Step 4: Run the tests**

Run: the command of Step 2.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/{StoreIdStorage,IdentifiedStore,StoreIds,LocalStoreIdStorage,LocalBlockStore,StoreLogStorage,LocalStoreLogStorage,StoreLog}.groovy \
        src/main/groovy/robsyme/cas/s3/{S3StoreIdStorage,S3BlockStore,S3StoreLogStorage}.groovy src/test
git commit -s -m "feat(core): store-id per member and a Store Log entry check (§20 decisions 1, 2)"
```

---

### Task 2: The cache reconciles store ids before catching up

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy`
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`onFlowCreate`)
- Test: `src/test/groovy/robsyme/cas/core/IndexStoreIdTest.groovy` (create), `src/test/groovy/robsyme/cas/CacheCoherenceTest.groovy` (create)

**Interfaces:**
- Consumes: `StoreIds`, `IdentifiedStore`, `StoreLog.has` (Task 1).
- Produces on `Index`: `String storeIdOf(String member)` (the cached `store_id:<member>`, or null); `void reset()`; `String reconcileStoreIds(Map<String, String> ids, Closure<Boolean> watermarkLogged)`. `ids`: alias to the member's id now, null when the member has none; an alias whose id could not be read is absent from the map. `watermarkLogged.call(alias, watermark)` answers true, false, or null when it could not tell. Returns the reset reason, or null when nothing was reset.
- Produces on `CasSession`: `String writableStoreId()` (ensures and caches; null when the writable member is not an `IdentifiedStore` or the write failed).

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/robsyme/cas/core/IndexStoreIdTest.groovy`:

```groovy
package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20 decisions 3 and 4; plan decisions 1 to 4. */
class IndexStoreIdTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore store
    Index index

    static final String A = 'a' * 32
    static final String B = 'b' * 32

    def setup() {
        store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private Cid addRun(String name) {
        final Cid m = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(store, StoreLogKind.RUN, rc, System.currentTimeMillis())
        index.catchUp(store, StoreLog.of(store), 'lab')
        return rc
    }

    private static Closure<Boolean> logged(Boolean answer) { { String a, String w -> answer } as Closure<Boolean> }

    def 'an empty cache adopts the id and keeps nothing to reset'() {
        expect:
        index.reconcileStoreIds([lab: A], logged(false)) == null
        index.storeIdOf('lab') == A
    }

    def 'the same id changes nothing'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))

        expect:
        index.reconcileStoreIds([lab: A], logged(false)) == null
        index.countRows('run') == 1
    }

    def 'a changed id resets the cache and records the new one'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))

        when:
        final String reason = index.reconcileStoreIds([lab: B], logged(true))

        then:
        reason.contains('lab') && reason.contains(A) && reason.contains(B)
        index.countRows('run') == 0
        index.countRows('log_entry') == 0
        index.watermark('lab') == null
        index.storeIdOf('lab') == B
    }

    def 'an id that disappears resets the cache'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))

        expect:
        index.reconcileStoreIds([lab: null], logged(true)) != null
        index.countRows('run') == 0
        index.storeIdOf('lab') == null
    }

    def 'a cache from before store-id adopts when its watermark entry is still logged'() {
        given:
        addRun('r1')

        expect:
        index.reconcileStoreIds([lab: A], logged(true)) == null
        index.countRows('run') == 1
        index.storeIdOf('lab') == A
    }

    def 'a cache from before store-id resets when its watermark entry is gone'() {
        given:
        addRun('r1')

        expect:
        index.reconcileStoreIds([lab: A], logged(false)) != null
        index.countRows('run') == 0
        index.storeIdOf('lab') == A
    }

    def 'an unreadable store-id is left out: no reset, no adoption'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))

        expect: 'the member is absent from the map'
        index.reconcileStoreIds([:], logged(true)) == null
        index.countRows('run') == 1
        index.storeIdOf('lab') == A
    }

    def 'when the watermark check cannot tell, the member is left out'() {
        given:
        addRun('r1')

        expect:
        index.reconcileStoreIds([lab: A], logged(null)) == null
        index.countRows('run') == 1
        index.storeIdOf('lab') == null
    }

    def 'neither side holding an id changes nothing'() {
        given:
        addRun('r1')

        expect:
        index.reconcileStoreIds([lab: null], logged(false)) == null
        index.countRows('run') == 1
    }
}
```

`src/test/groovy/robsyme/cas/CacheCoherenceTest.groovy` (the `scratch-workflow` case of 2026-10-09, end to end through `CasSession`):

```groovy
package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20: a member deleted and recreated at the same location. */
class CacheCoherenceTest extends Specification {

    @TempDir Path tmp

    private CasSession session() {
        new CasSession(CasConfig.from([
            cas: [
                stores: [lab: [location: tmp.resolve('store').toString()]],
                index : [path: tmp.resolve('cache.sqlite').toString()],
            ],
        ], 'cas://lab'))
    }

    private Cid logRun(String name) {
        final LocalBlockStore store = new LocalBlockStore(tmp.resolve('store'), 'lab', true)
        final Cid m = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(store, StoreLogKind.RUN, rc, System.currentTimeMillis())
        return rc
    }

    private List<String> runsIn(Index index) {
        final List<String> out = []
        for( String p : index.pipelines() )
            out.addAll(index.runsOf(p)*.completion*.toString())
        return out
    }

    def 'catching up creates the writable member\'s store-id and records it'() {
        given:
        logRun('r1')
        final CasSession cas = session()
        final Index index = cas.openIndex()

        when:
        cas.catchUpIndex(index)

        then:
        final String id = Files.readString(tmp.resolve('store/store-id')).trim()
        index.storeIdOf('lab') == id
        cas.writableStoreId() == id

        cleanup:
        index.close()
    }

    def 'a recreated member shows only its own runs'() {
        given: 'two runs in the first incarnation, caught up'
        logRun('old1'); logRun('old2')
        final CasSession first = session()
        Index index = first.openIndex()
        first.catchUpIndex(index)
        assert runsIn(index).size() == 2
        index.close()

        when: 'the member is deleted and one run written at the same location'
        tmp.resolve('store').toFile().deleteDir()
        final Cid fresh = logRun('new1')
        final CasSession cas = session()
        index = cas.openIndex()
        cas.catchUpIndex(index)

        then:
        runsIn(index) == [fresh.toString()]
        index.storeIdOf('lab') == Files.readString(tmp.resolve('store/store-id')).trim()

        cleanup:
        index?.close()
    }
}
```

Check the getter names on `IndexedRun` (`/usr/bin/grep -a -n "final" src/main/groovy/robsyme/cas/core/IndexedRun.groovy`) and use them in `runsIn`.

- [ ] **Step 2: Run them to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexStoreIdTest' --tests 'robsyme.cas.CacheCoherenceTest'`
Expected: compilation failure (`reconcileStoreIds`, `storeIdOf`, `writableStoreId` not defined).

- [ ] **Step 3: Implement `Index`**

Beside the other `META_*` constants:

```groovy
    private static final String META_STORE_ID = 'store_id'
```

In the queries/bookkeeping area, beside `watermark(String)`:

```groovy
    /** The member's store-id as this cache last recorded it, or null (DESIGN.md §20 decision 3). */
    String storeIdOf(String member) { meta(META_STORE_ID + ':' + member) }

    /** Empties every table but schema_version, meta included: a cold cache (plan decision 1). */
    void reset() {
        withTransaction {
            for( String sql : ddl() ) {
                final java.util.regex.Matcher m = sql =~ /^CREATE TABLE (\w+)\(/
                if( m.find() && m.group(1) != 'schema_version' )
                    update("DELETE FROM ${m.group(1)}".toString(), [])
            }
        }
    }

    /**
     * DESIGN.md §20 decision 3 and plan decisions 1 to 4. {@code ids}: alias to
     * the member's id now, null when it has none; a member whose id could not be
     * read is left out. {@code watermarkLogged(alias, watermark)} answers whether
     * that entry is still in the member's log, or null when it could not tell.
     * Resets the whole cache when any member was replaced, then records every
     * id it may. Returns why it reset, or null.
     */
    String reconcileStoreIds(Map<String, String> ids, Closure<Boolean> watermarkLogged) {
        String reason = null
        final Map<String, String> record = new LinkedHashMap<String, String>()
        for( Map.Entry<String, String> e : ids.entrySet() ) {
            final String alias = e.key
            final String now = e.value
            final String cached = storeIdOf(alias)
            if( cached != null ) {
                if( now != cached && reason == null )
                    reason = "store member '${alias}' was replaced (store-id ${cached} is now ${now ?: 'absent'})".toString()
                if( now != null ) record.put(alias, now)
                continue
            }
            if( now == null )
                continue
            final String wm = watermark(alias)
            final boolean holdsNothing = wm == null && meta(META_SCANNED + ':' + alias) == null
            if( holdsNothing ) {
                record.put(alias, now)
                continue
            }
            final Boolean logged = wm == null ? Boolean.FALSE : watermarkLogged.call(alias, wm)
            if( logged == null )
                continue
            if( !logged && reason == null )
                reason = "store member '${alias}' has a store-id this cache never saw, and its rows cannot be shown to be from it (watermark ${wm ?: 'none'} is not in its Store Log)".toString()
            record.put(alias, now)
        }
        if( reason != null ) {
            CONSOLE.info("rebuilding the cache index ${file}: ${reason}")
            reset()
        }
        for( Map.Entry<String, String> e : record.entrySet() )
            setMeta(META_STORE_ID + ':' + e.key, e.value)
        return reason
    }
```

Note the "id disappears" case: `cached != null`, `now == null`, so the reason is set and nothing is recorded; after `reset()` the old key is gone.

- [ ] **Step 4: Implement `CasSession`**

Add a field and method beside `catchUpIndex`:

```groovy
    private volatile String writableId

    /**
     * The writable member's store-id, created when absent (DESIGN.md §20
     * decision 2, plan decision 5). Null when the member keeps none or it could
     * not be written; derived, so a failure warns once and the caller goes on.
     */
    String writableStoreId() {
        if( writableId != null )
            return writableId
        final BlockStore writable = members()[0]
        if( !(writable instanceof IdentifiedStore) || !writable.isWritable() )
            return null
        try {
            writableId = StoreIds.ensure(((IdentifiedStore) writable).storeIdStorage())
        }
        catch( Exception e ) {
            warnOnce("could not create the store-id of '${writable.alias()}'; caches cannot tell if it is replaced: ${e.message}".toString())
        }
        return writableId
    }

    /**
     * Every member's id now (null when it has none); a member whose id could not
     * be read is left out (plan decision 3), and so is the writable member when
     * its id could not be created, which would otherwise read as "the id vanished".
     */
    private Map<String, String> memberStoreIds() {
        final Map<String, String> ids = new LinkedHashMap<String, String>()
        for( BlockStore member : members() ) {
            if( !(member instanceof IdentifiedStore) )
                continue
            if( member.alias() == config.writableAlias && member.isWritable() ) {
                final String id = writableStoreId()
                if( id != null )
                    ids.put(member.alias(), id)
                continue
            }
            try {
                ids.put(member.alias(), StoreIds.read(member))
            }
            catch( Exception e ) {
                log.debug("could not read the store-id of '${member.alias()}': ${e.message}")
            }
        }
        return ids
    }

    private Boolean watermarkLogged(String alias, String watermark) {
        final BlockStore member = members().find { BlockStore m -> m.alias() == alias }
        if( member == null )
            return null
        try {
            return StoreLog.of(member).has(watermark)
        }
        catch( Exception e ) {
            log.debug("could not look for ${watermark} in the Store Log of '${alias}': ${e.message}")
            return null
        }
    }
```

At the top of `catchUpIndex(Index index)`, before the loop:

```groovy
        try {
            index.reconcileStoreIds(memberStoreIds(), { String a, String w -> watermarkLogged(a, w) } as Closure<Boolean>)
        }
        catch( Exception e ) {
            log.warn("could not compare this cache with the store's ids; it is derived: ${e.message}", e)
        }
```

Imports: `robsyme.cas.core.IdentifiedStore`, `robsyme.cas.core.StoreIds`.

- [ ] **Step 5: Create the id before a run registers**

In `CasObserver.onFlowCreate`, after `cas.checkClock()` and before `cas.startLiveWriter(...)`:

```groovy
        // DESIGN.md §20 decision 2: the member's id exists before anything else this run writes there.
        cas.writableStoreId()
```

- [ ] **Step 6: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexStoreIdTest' --tests 'robsyme.cas.CacheCoherenceTest' --tests 'robsyme.cas.CasSessionTest' --tests 'robsyme.cas.CasSessionS3Test' --tests 'robsyme.cas.trace.*'`
Expected: PASS. If an observer test fails because a mocked `CasSession` lacks `writableStoreId`, stub it to return null.

- [ ] **Step 7: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/CasSession.groovy \
        src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/test
git commit -s -m "feat: the cache resets when a member's store-id changes (§20 decisions 3, 4)"
```

---

### Task 3: The snapshot carries `store_id`; seeding checks it

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy` (`build`, `buildAndVacuum`, constants)
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`seedFrom`, the 6-arg `catchUp`)
- Test: `src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy` (append), `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy` (append)

**Interfaces:**
- Consumes: `Index.storeIdOf`, `Index.reconcileStoreIds` (Task 2), `StoreLog.has` (Task 1).
- Produces: `IndexSnapshot.STORE_ID_KEY = 'store_id'`; the snapshot's `meta` holds it when the cache knows the member's id. `Index.seedFrom(Path, String member, StoreLog log)`; the 2-arg `seedFrom` is `seedFrom(file, member, null)` (no log check).

- [ ] **Step 1: Write the failing tests**

Append to `IndexSnapshotTest` (use its store/index fields and `addRun`-style helper; if it has none, copy `addRun` from `SnapshotGuardTest`):

```groovy
    def 'the snapshot meta holds store_id when the cache knows the member\'s id'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: 'a' * 32], { a, w -> true } as Closure<Boolean>)

        when:
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(store.root), 0L, null, tmp)

        then:
        java.sql.DriverManager.getConnection("jdbc:sqlite:${IndexSnapshot.pathIn(store.root)}").withCloseable { c ->
            c.createStatement().executeQuery("SELECT value FROM meta WHERE key = 'store_id'").with { next(); getString(1) }
        } == 'a' * 32
    }
```

Append to `IndexSeedTest` (its helpers: `run(name, sample, at)` logs a run in `lab`, `producer()` returns a caught-up `Index`):

```groovy
    private static Closure<Boolean> logged(Boolean answer) { { String a, String w -> answer } as Closure<Boolean> }

    /** A snapshot of `lab` written by a cache that recorded `id` (or none). */
    private void snapshotWith(String id) {
        run('r1', 'A', System.currentTimeMillis() - 1000)
        final Index source = producer()
        if( id != null )
            source.reconcileStoreIds([lab: id], logged(true))
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        source.close()
    }

    private String snapshotMeta(String key) {
        return DriverManager.getConnection("jdbc:sqlite:${IndexSnapshot.pathIn(lab.root)}").withCloseable { Connection c ->
            final java.sql.PreparedStatement s = c.prepareStatement('SELECT value FROM meta WHERE key = ?')
            s.setString(1, key)
            final java.sql.ResultSet rs = s.executeQuery()
            rs.next() ? rs.getString(1) : null
        }
    }

    def 'a snapshot from another incarnation does not seed'() {
        given:
        snapshotWith('a' * 32)
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.reconcileStoreIds([lab: 'b' * 32], logged(true))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab', StoreLog.of(lab))

        then:
        final Index.SnapshotUnusable e = thrown()
        e.reason.contains('another incarnation')

        cleanup:
        cold.close()
    }

    def 'a snapshot without store_id does not seed once its watermark entry is gone'() {
        given:
        snapshotWith(null)
        final String wm = snapshotMeta(IndexSnapshot.WATERMARK_KEY)
        Files.delete(lab.root.resolve('log').resolve(wm))
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab', StoreLog.of(lab))

        then:
        final Index.SnapshotUnusable e = thrown()
        e.reason.contains('watermark')

        cleanup:
        cold.close()
    }

    def 'a snapshot without store_id whose watermark is logged still seeds'() {
        given:
        snapshotWith(null)
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        expect:
        snapshotMeta(IndexSnapshot.STORE_ID_KEY) == null
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab', StoreLog.of(lab))
        cold.countRows('run') == 1

        cleanup:
        cold.close()
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexSeedTest' --tests 'robsyme.cas.core.IndexSnapshotTest'`
Expected: compilation failure (`seedFrom` 3-arg), then assertion failures.

- [ ] **Step 3: Implement**

`IndexSnapshot`: add `static final String STORE_ID_KEY = 'store_id'`. In `build`, read `final String storeId = index.storeIdOf(member)` beside the watermark and pass it to `buildAndVacuum` as a new last parameter; in `buildAndVacuum`, after the watermark insert:

```groovy
            if( storeId != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) STORE_ID_KEY, storeId])
```

Give `Built` a `storeId` field (constructor last argument) so Task 7 can describe the write.

`Index.seedFrom`: rename the current method body into `boolean seedFrom(Path snapshotFile, String member, StoreLog storeLog)`, and keep `boolean seedFrom(Path snapshotFile, String member) { seedFrom(snapshotFile, member, null) }`. In the 3-arg body, read `snapMeta` right after the schema-version check (move the `query('SELECT key, value FROM snap.meta' ...)` out of the inner try, keeping its own catch to `SnapshotUnusable('unreadable', e)`), then before the transaction:

```groovy
            final String snapId = snapMeta[IndexSnapshot.STORE_ID_KEY]
            final String memberId = storeIdOf(member)
            if( snapId != null && memberId != null && snapId != memberId )
                throw new SnapshotUnusable("written for another incarnation of the member (store_id ${snapId}, the member's is ${memberId})")
            final String snapWatermark = snapMeta[IndexSnapshot.WATERMARK_KEY]
            if( snapId == null && snapWatermark != null && storeLog != null && !storeLog.has(snapWatermark) )
                throw new SnapshotUnusable("its watermark ${snapWatermark} is not in the member's Store Log, so it may be from an earlier incarnation")
```

`storeLog.has` throws on an S3 failure: let it propagate; the caller's `catch( Exception e )` already turns that into `unreadable` and the full scan.

In the 6-arg `catchUp`, change `seedFrom(file, member)` to `seedFrom(file, member, storeLog)`.

- [ ] **Step 4: Run the tests**

Run: the command of Step 2, then `./gradlew test --tests 'robsyme.cas.core.*'`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/{IndexSnapshot,Index}.groovy src/test
git commit -s -m "feat(core): the snapshot carries store_id and seeding checks it (§20 decisions 5, 9)"
```

---

### Task 4: The Forget block and the `forget` Store Log kind

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Forget.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (constant), `StoreLogKind.groovy`
- Modify: `DESIGN.md` §6 (the ```` ```ipldsch ```` block: the `Block` union and a `Forget` struct)
- Modify: `web/src/generated/schema.json` (regenerated), `web/test/schema.test.mjs`
- Test: `src/test/groovy/robsyme/cas/core/ForgetTest.groovy` (create), `src/test/groovy/robsyme/cas/core/StoreLogTest.groovy` (append)

**Interfaces:**
- Produces `Records.FORGET = 'Forget'`, `StoreLogKind.FORGET('forget')`.
- Produces `class Forget { final String assertedBy; final List<Cid> deleted; Forget(String assertedBy, Collection<Cid> deleted); Map<String, Object> toCbor(); static Forget fromCbor(Map block) }`. The constructor sorts by cid string and drops duplicates; empty `deleted` is refused.

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/robsyme/cas/core/ForgetTest.groovy`:

```groovy
package robsyme.cas.core

import spock.lang.Specification

class ForgetTest extends Specification {

    def 'deleted is sorted and unique, and identical sets give one block'() {
        given:
        final Cid x = Fixtures.cidOf([kind: 'X', n: 1])
        final Cid y = Fixtures.cidOf([kind: 'X', n: 2])
        final Forget f = new Forget('lab', [y, x, y])

        expect:
        f.deleted == [x, y].sort { it.toString() }
        f.toCbor() == [kind: 'Forget', schema: 1L, asserted_by: 'lab', deleted: f.deleted]
        Fixtures.cidOf(new Forget('lab', [x, y]).toCbor()) == Fixtures.cidOf(new Forget('lab', [y, x]).toCbor())
        Forget.fromCbor(f.toCbor()).deleted == f.deleted
    }

    def 'an empty Forget is refused, and so is another kind'() {
        when:
        new Forget('lab', [])

        then:
        thrown(IllegalArgumentException)

        when:
        Forget.fromCbor([kind: 'Claim', schema: 1L])

        then:
        thrown(IllegalArgumentException)
    }
}
```

Append to `StoreLogTest`:

```groovy
    def 'a forget entry parses with its kind'() {
        given:
        final Cid c = Fixtures.cidOf([kind: 'Forget', n: 1])
        final String name = StoreLog.entryName(StoreLogKind.FORGET, c, 1_790_000_000_000L)

        expect:
        name.contains('-forget-')
        StoreLog.parse(name).kind == StoreLogKind.FORGET
    }
```

Edit `web/test/schema.test.mjs`: change `'DESIGN.md §6 parses to 23 types'` to 24 and its expected count to `24`, and add:

```js
test('a Forget block validates, and one without deleted does not', async () => {
  const { blocks } = await buildMember()
  const some = [...blocks.keys()].slice(0, 2).map(k => dagCbor.decode(blocks.get(k)))
  const { CID } = await import('multiformats/cid')
  const cids = [...blocks.keys()].slice(0, 2).map(k => CID.parse(k))
  assert.equal(validBlock({ kind: 'Forget', schema: 1, asserted_by: 'lab', deleted: cids }), true)
  assert.equal(validBlock({ kind: 'Forget', schema: 1, asserted_by: 'lab' }), false)
  assert.ok(some.length === 2)
})
```

(If `blocks` is keyed by `CID` objects rather than strings, build `cids` from the keys directly; check `web/test/fixture.mjs`.)

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.ForgetTest' --tests 'robsyme.cas.core.StoreLogTest'` and `cd web && node --test test/schema.test.mjs`
Expected: compilation failure; the web test fails on the count and on `Forget`.

- [ ] **Step 3: Implement**

`Records`: beside `SELECTION`, `static final String FORGET = 'Forget'`.

`StoreLogKind`: `RUN('run'), SELECTION('selection'), CLAIM('claim'), FORGET('forget')`.

`src/main/groovy/robsyme/cas/core/Forget.groovy`:

```groovy
package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * What one sweep batch deleted (DESIGN.md §20 decision 6), so every cache can
 * forget its rows. No timestamp and no sweep id (§6): two batches that delete
 * the same set write one block.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Forget {

    final String assertedBy
    final List<Cid> deleted

    Forget(String assertedBy, Collection<Cid> deleted) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Forget needs an asserted_by')
        if( !deleted )
            throw new IllegalArgumentException('an empty Forget is refused')
        this.assertedBy = assertedBy
        this.deleted = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(deleted)))
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.FORGET)
        map.put('asserted_by', assertedBy)
        map.put('deleted', new ArrayList<Object>(deleted))
        return map
    }

    static Forget fromCbor(Map block) {
        Records.expectKind(block, Records.FORGET)
        final List deleted = (List) Records.require(block, 'deleted')
        return new Forget(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            deleted.collect { Object c -> Records.cid(c, 'deleted') })
    }
}
```

(`Cid.compareTo` orders by text, so `TreeSet` sorts by cid string. Check `Records.expectKind` throws `IllegalArgumentException`; `ClaimTest` relies on it.)

`DESIGN.md` §6 ipldsch block: add `  | Forget "Forget"` to the `Block` union after `| Claim "Claim"`, and after the `ClaimVerb` enum:

```
type Forget struct {
  schema Int
  asserted_by String
  deleted [&Any]                # sorted by cid string, no duplicates (§20)
}
```

Regenerate: `cd web && node schema-gen.mjs` (prints `24 types`).

- [ ] **Step 4: Run the tests**

Run: the commands of Step 2, then `cd web && npm test`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/{Forget,Records,StoreLogKind}.groovy DESIGN.md web/src/generated/schema.json web/test/schema.test.mjs src/test
git commit -s -m "feat(core): the Forget block and the forget Store Log kind (§20 decision 6)"
```

---

### Task 5: Catch-up applies `forget` entries

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`catchUp` overloads, `ingestLogged`, `ingestTolerant`, `retryMissingLogged`, new `applyForget`, `forgottenRunsSince`)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`catchUpIndex` passes the composite store)
- Test: `src/test/groovy/robsyme/cas/core/IndexForgetTest.groovy` (create)

**Interfaces:**
- Consumes: `Forget`, `StoreLogKind.FORGET` (Task 4).
- Produces on `Index`: `void catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir, Set<String> quietFor, BlockStore holders)`; the existing overloads pass `store` as `holders`. `int forgottenRunsSince(String member, String baseWatermark)`: the runs `forget` entries of `member` removed whose entry is newer than `baseWatermark` (all of them when it is null).
- `meta` key `forget:<member>:<forget cid>` = `<entry name> <runs>` (plan decision 9).

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/robsyme/cas/core/IndexForgetTest.groovy`:

```groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20 decision 8, plan decision 9. */
class IndexForgetTest extends Specification {

    @TempDir Path tmp
    RetentionFixture f
    Index index

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private void catchUp(BlockStore holders = f.store) {
        index.catchUp(f.store, StoreLog.of(f.store), 'lab', null, tmp, Collections.<String> emptySet(), holders)
    }

    /** Deletes the blocks the way a sweep does and logs one forget entry for them. */
    private Cid sweepAway(List<Cid> cids) {
        for( Cid c : cids )
            Files.deleteIfExists(((LocalBlockStore) f.store).blockPath(c))
        final Cid forget = f.store.putDagCbor(new Forget('test', cids).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, forget, ++f.clock)
        return forget
    }

    def 'a forget entry removes the rows of what it lists'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        final Map kept = f.run('kept', [[k: f.raw('k')]])
        catchUp()

        when:
        sweepAway([gone.completion, gone.collection] + gone.items as List<Cid>)
        catchUp()

        then:
        !index.isRunIndexed((Cid) gone.completion)
        index.isRunIndexed((Cid) kept.completion)
        index.forgottenRunsSince('lab', null) == 1
    }

    def 'a listed cid the store holds again is not forgotten'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        catchUp()
        final Cid item = ((List<Cid>) run.items)[0]
        final byte[] bytes = Files.readAllBytes(((LocalBlockStore) f.store).blockPath(item))
        sweepAway([item])
        f.store.put(item, new ByteArrayInputStream(bytes), bytes.length)

        when:
        catchUp()

        then:
        index.items((Cid) run.completion, 'out', [:]) == [item]
    }

    def 'a listed cid another member holds is not forgotten'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        catchUp()
        final LocalBlockStore other = new LocalBlockStore(tmp.resolve('other'), 'other', false)
        final Path from = ((LocalBlockStore) f.store).blockPath((Cid) run.completion)
        final Path to = other.blockPath((Cid) run.completion)
        Files.createDirectories(to.parent)
        Files.copy(from, to)
        sweepAway([(Cid) run.completion])

        when:
        catchUp(new CompositeStore([f.store, other]))

        then:
        index.isRunIndexed((Cid) run.completion)
    }

    def 'a forget entry is applied once: the overlap re-read changes nothing'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        catchUp()
        sweepAway([(Cid) gone.completion])
        catchUp()

        when:
        catchUp()

        then:
        index.forgottenRunsSince('lab', null) == 1
    }

    def 'an unreadable Forget block is retried once it can be read'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        catchUp()
        final Cid forget = sweepAway([(Cid) gone.completion])
        final Path p = ((LocalBlockStore) f.store).blockPath(forget)
        final byte[] bytes = Files.readAllBytes(p)
        Files.delete(p)

        when:
        catchUp()

        then:
        index.isRunIndexed((Cid) gone.completion)

        when:
        f.store.put(forget, new ByteArrayInputStream(bytes), bytes.length)
        catchUp()

        then:
        !index.isRunIndexed((Cid) gone.completion)
    }

    def 'forgottenRunsSince counts only entries newer than the base watermark'() {
        given:
        final Map one = f.run('one', [[a: f.raw('a')]])
        final Map two = f.run('two', [[b: f.raw('b')]])
        catchUp()
        sweepAway([(Cid) one.completion])
        catchUp()
        final String base = index.watermark('lab')
        sweepAway([(Cid) two.completion])
        catchUp()

        expect:
        index.forgottenRunsSince('lab', base) == 1
        index.forgottenRunsSince('lab', null) == 2
    }
}
```

`LocalBlockStore.blockPath(Cid)` is used by `SweepTest`; `RetentionFixture.clock` is a public `long`.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexForgetTest'`
Expected: compilation failure (7-arg `catchUp`, `forgottenRunsSince`).

- [ ] **Step 3: Implement**

Thread a `BlockStore holders` parameter through the catch-up paths:

- `catchUp(BlockStore store, StoreLog storeLog, String member)` becomes `catchUp(store, storeLog, member, store)` delegating to a new private `catchUpTail(BlockStore store, StoreLog storeLog, String member, BlockStore holders)` holding today's body, with `retryMissingLogged(store, member, holders)` and `ingestLogged(store, entry.kind, entry.cid, member, holders)`.
- The 5-arg and 6-arg overloads delegate to the 7-arg one with `store` as `holders`; the 7-arg overload holds today's 6-arg body and ends with `catchUpTail(store, storeLog, member, holders)`.
- `scanOnce` passes `store` as `holders` (it never sees a forget entry).
- `ingestLogged(..., BlockStore holders)` gains:

```groovy
            case StoreLogKind.FORGET:
                if( meta(forgetKey(member, cid)) == null )
                    ingestTolerant(store, kind, cid, member, holders)
                return
```

- `ingestTolerant(..., BlockStore holders)` gains `case StoreLogKind.FORGET: applyForget(store, holders, cid, member); break`.
- `retryMissingLogged(store, member, holders)` passes `holders` on.

Then add:

```groovy
    private static String forgetKey(String member, Cid forget) { "forget:${member}:${forget}".toString() }

    /**
     * DESIGN.md §20 decision 8: forgets what one sweep batch deleted, except a
     * cid some member holds now (still elsewhere, or written again since).
     * Recorded per member, so the overlap re-read and a retry apply it once.
     */
    private void applyForget(BlockStore store, BlockStore holders, Cid forgetCid, String member) {
        final Map block = readBlock(store, forgetCid, Records.FORGET)
        if( block == null ) {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [forgetCid.toString()])
            insertMissing(null, forgetCid)
            return
        }
        final List<Cid> gone = new ArrayList<Cid>()
        for( Cid c : Forget.fromCbor(block).deleted )
            if( !holders.has(c) )
                gone.add(c)
        int runs = 0
        for( Cid c : gone )
            if( isRunIndexed(c) )
                runs++
        final StoreLogEntry entry = firstLogEntry(forgetCid, member)
        forget(gone)
        setMeta(forgetKey(member, forgetCid), "${entry?.name ?: ''} ${runs}".toString())
        update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [forgetCid.toString()])
    }

    /**
     * Runs that `forget` entries of {@code member} removed, counting only entries
     * newer than {@code baseWatermark} (all of them when it is null): what the
     * snapshot guard adds back before comparing run counts (§20 decision 10).
     */
    int forgottenRunsSince(String member, String baseWatermark) {
        final String floor = baseWatermark == null ? null : StoreLog.parse(baseWatermark)?.reverseTs
        final String prefix = "forget:${member}:".toString()
        int n = 0
        query('SELECT value FROM meta WHERE substr(key, 1, ?) = ?', [(Object) prefix.length(), prefix]) { ResultSet rs ->
            final String[] parts = rs.getString(1).split(' ')
            if( parts.length != 2 )
                return
            final String rts = parts[0] ? StoreLog.parse(parts[0])?.reverseTs : null
            if( floor == null || (rts != null && rts < floor) )
                n += parts[1] as int
        }
        return n
    }
```

`readBlock` returns null for a block that is absent; an `IOException` from `holders.has` reaches `ingestTolerant`, which records `missing` and the retry picks it up. `Index.forget` deletes `log_entry` rows of the listed cids, not of the forget entry, so the forget entry's own `log_entry` row stays (the snapshot copies it, and a cache seeded from it then skips the entry only when the `meta` key is set; a seeded cache has no `forget:` keys, so it applies the entry again, which is a no-op on rows the snapshot already lacks).

`Index.reset()` (Task 2) clears `meta`, so it clears the `forget:` keys too.

`CasSession.catchUpIndex`: call the 7-arg overload with `store` (the composite) as `holders`:

```groovy
                index.catchUp(member, StoreLog.of(member), member.alias(), snapshotsOf(member.alias()), config.tmpDir, quietFor, store)
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexForgetTest' --tests 'robsyme.cas.core.*' --tests 'robsyme.cas.CasSession*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/CasSession.groovy src/test
git commit -s -m "feat(core): catch-up applies forget entries (§20 decision 8)"
```

---

### Task 6: The sweep writes `forget` entries; the mark roots them

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Sweep.groovy` (constructor, `deleteDue`, new `announce`)
- Modify: `src/main/groovy/robsyme/cas/core/SweepReport.groovy` (`forgets`, `unannounced`)
- Modify: `src/main/groovy/robsyme/cas/core/Mark.groovy` (`extend`)
- Modify: `src/main/groovy/robsyme/cas/cli/RetentionCommands.groovy` (`new Sweep(...)`, `forgetAndSnapshot`)
- Test: `src/test/groovy/robsyme/cas/core/SweepTest.groovy` (append), `src/test/groovy/robsyme/cas/core/MarkTest.groovy` (append), `src/test/groovy/robsyme/cas/cli/RetentionCommandsTest.groovy` (append)

**Interfaces:**
- Consumes: `Forget`, `StoreLogKind.FORGET` (Task 4); catch-up applying them (Task 5).
- Produces: `Sweep(BlockStore store, SweepPolicy policy, String assertedBy)`; the 2-arg constructor passes `'nf-blocks'`. `SweepReport.forgets` (`List<Cid>`, the Forget blocks written) and `SweepReport.unannounced` (`List<Cid>`, deleted dag-cbor cids no entry announces). Neither is added to `toJson()`.

- [ ] **Step 1: Write the failing tests**

Append to `SweepTest`:

```groovy
    def 'each delete batch of metadata is announced by one forget entry'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.forgets.size() >= 1
        final List<StoreLogEntry> entries = StoreLog.read(f.store).findAll { it.kind == StoreLogKind.FORGET }
        entries*.cid as Set == r.forgets as Set
        final Set<Cid> listed = r.forgets.collectMany { Cid c ->
            Forget.fromCbor((Map) DagCbor.decode(f.store.open(c).withCloseable { it.readAllBytes() })).deleted } as Set
        listed.contains(gone.completion)
        listed.every { Cid c -> c.isDagCbor() }
        r.unannounced == []
    }

    def 'a sweep that deletes only raw content writes no forget entry'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.forgets == []
        !StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET }
    }

    def 'a later sweep keeps the Forget blocks its entries name'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final SweepReport first = sweep().apply(false, 0L, { -> false }, say, sleeper)
        ageAll(15 * SweepPolicy.DAY)

        when:
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        first.forgets.every { Cid c -> f.store.has(c) }
    }

    def 'a forget entry that cannot be written leaves its cids unannounced and warns'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final Path log = root.resolve('log')
        log.toFile().setWritable(false)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.unannounced.contains(gone.completion)
        r.warnings.any { it.contains('reindex') }

        cleanup:
        log.toFile().setWritable(true)
    }
```

The last test removes write permission from `log/`, which also stops the sweep from deleting log entries; if that makes `apply` stop before `announce`, make the forget append fail instead by subclassing nothing and wrapping: construct the `Sweep` over a `LocalBlockStore` subclass whose `storeLogStorage()` returns a `StoreLogStorage` whose `putEntry` throws `IOException('denied')` and otherwise delegates. Pick whichever keeps the rest of the sweep working; the assertions stay the same.

Append to `MarkTest` (follow its existing construction of a `RetentionFixture` and `Mark.of(f.store, f.logs(), 4)`):

```groovy
    def 'a forget entry roots its block; an entry whose block is gone is dangling'() {
        given:
        final Cid kept = f.store.putDagCbor(new Forget('test', [Fixtures.cidOf([kind: 'X'])]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, kept, ++f.clock)
        final Cid lost = Fixtures.cidOf(new Forget('test', [Fixtures.cidOf([kind: 'Y'])]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, lost, ++f.clock)

        when:
        final Mark m = Mark.of(f.store, f.logs(), 4)

        then:
        m.isLive(kept)
        !m.isLive(lost)
        m.danglingEntries.any { it.contains(lost.toString()) }
    }
```

Append to `RetentionCommandsTest` (its `setup` logs run `a` as `theRun`; `ageAll`, `expireLedgers` and `run(cmd, args)` are its helpers). The existing test `'an applied sweep that deletes a hidden run ... rewrites the snapshot'` passes today only because no snapshot existed before the sweep; a real store has one from every run's `onFlowComplete`, which is what this test adds. It pins Review Focus 5:

```groovy
    private List<String> runsIn(Path sqlite) {
        return java.sql.DriverManager.getConnection("jdbc:sqlite:${sqlite}").withCloseable { c ->
            final java.sql.ResultSet rs = c.createStatement().executeQuery('SELECT completion_cid FROM run ORDER BY 1')
            final List<String> out = []
            while( rs.next() ) out << rs.getString(1)
            out
        }
    }

    def 'a sweep that deletes a run rewrites an existing snapshot, and the cache forgets the run through its own catch-up'() {
        given: 'runs a (deleted) and kept, a snapshot of both, a sweep that trashed a, its ledger expired'
        final Map kept = f.run('kept', [[k: f.raw('k')]])
        run('snapshot')
        f.claim(theRun.completion, 'delete', null, null)
        ageAll()
        run('sweep', ['--apply', 'true'])
        expireLedgers()
        out.reset()

        when:
        final int status = run('sweep', ['--apply', 'true', '--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        status == 0
        ((String) json.snapshot).startsWith('wrote')
        runsIn(store.resolve('index/v3.sqlite')) == [kept.completion.toString()]
        runsIn(indexPath) == [kept.completion.toString()]
        StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET }
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SweepTest' --tests 'robsyme.cas.core.MarkTest' --tests 'robsyme.cas.cli.RetentionCommandsTest'`
Expected: compilation failure (`forgets`, `unannounced`), then failures.

- [ ] **Step 3: Implement**

`SweepReport`: beside `deleted`:

```groovy
    /** The Forget blocks this sweep wrote, one per delete batch with metadata in it (DESIGN.md §20 decision 7). */
    List<Cid> forgets = []
    /** Deleted dag-cbor blocks no forget entry announces, because writing it failed (plan decision 8). */
    List<Cid> unannounced = []
```

`Sweep`: add `private final String assertedBy`; the existing constructor becomes `Sweep(BlockStore store, SweepPolicy policy) { this(store, policy, 'nf-blocks') }` and the new 3-arg one holds today's body plus `this.assertedBy = assertedBy ?: 'nf-blocks'`. In `deleteDue`, after the loop that adds each non-failed block to `deleted`:

```groovy
            announce(r, batch*.cid.findAll { Cid c -> !failedSet.contains(c) } as List<Cid>)
```

Add:

```groovy
    /**
     * DESIGN.md §20 decision 7 (plan decisions 6 to 8): one Forget block and
     * one `forget` entry for the dag-cbor blocks a batch deleted, so every
     * cache forgets their rows. Derived: a failure warns, and the caller's own
     * cache forgets them directly.
     */
    private void announce(SweepReport r, List<Cid> went) {
        final List<Cid> metadata = went.findAll { Cid c -> c.isDagCbor() } as List<Cid>
        if( metadata.isEmpty() )
            return
        try {
            final Cid block = writable.putDagCbor(new Forget(assertedBy, metadata).toCbor())
            StoreLog.append(writable, StoreLogKind.FORGET, block, System.currentTimeMillis())
            r.forgets.add(block)
        }
        catch( Exception e ) {
            r.unannounced.addAll(metadata)
            r.warnings.add("could not record what ${metadata.size()} deleted block(s) were (${e.message}); other caches keep their rows until `nextflow plugin nf-blocks:reindex`".toString())
        }
    }
```

`Mark`: add `private final Set<Cid> forgets = new LinkedHashSet<Cid>()`. In `extend`, declare `final List<Visit> newForgets = new ArrayList<Visit>()` beside `newClaims`, and in the switch:

```groovy
                        case StoreLogKind.FORGET:
                            // Rooted itself only (DESIGN.md §20 decision 8): its entry names it,
                            // and a missing block makes the entry dangling.
                            if( forgets.add(e.cid) ) newForgets.add(new Visit(e.cid, SELF, false, null))
                            break
```

After `root(touched)`: `walk(newForgets)`.

`RetentionCommands`: construct the sweep with `new Sweep(cas.store, policy, cas.assertedBy)` (find the `new Sweep(` call). In `forgetAndSnapshot`, replace `index.forget(r.deleted)` with `index.forget(r.unannounced)`; the `catchUpIndex` call after it applies the sweep's own `forget` entries.

- [ ] **Step 4: Run the tests**

Run: the command of Step 2, then `./gradlew test --tests 'robsyme.cas.core.*' --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.s3.Sweep*'`.
Expected: PASS. A `SweepS3Test` that compares the member's object listing before and after a sweep now sees `log/…-forget-…` and a Forget block; update its expectation to allow them, naming §20 decision 7 in a comment.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/{Sweep,SweepReport,Mark}.groovy src/main/groovy/robsyme/cas/cli/RetentionCommands.groovy src/test
git commit -s -m "feat(retention): each delete batch writes a forget entry every cache applies (§20 decision 7)"
```

---

### Task 7: The snapshot guard knows store ids and watermarks

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/SnapshotStorage.groovy` (`SnapshotBase` fields, `replace` signature)
- Modify: `src/main/groovy/robsyme/cas/core/LocalSnapshotStorage.groovy`, `src/main/groovy/robsyme/cas/s3/S3SnapshotStorage.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy` (`write`, `baseOf`, reasons)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`snapshotWritable`)
- Test: `src/test/groovy/robsyme/cas/core/SnapshotGuardTest.groovy` (append), `src/test/groovy/robsyme/cas/s3/S3SnapshotStorageTest.groovy` (update), `src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy` (check)

**Interfaces:**
- Consumes: `Index.storeIdOf` (Task 2), `Built.storeId` (Task 3), `Index.forgottenRunsSince` (Task 5).
- Produces: `SnapshotBase(String tag, long bytes, int runs, String storeId = null, String watermark = null)`. `SnapshotStorage.replace(Path built, SnapshotBase written, SnapshotBase base)`. `IndexSnapshot.STALE_STORE = 'stale_store'`, `IndexSnapshot.OLDER_WATERMARK = 'older_watermark'`. `IndexSnapshot.write(Index, String member, SnapshotStorage, long maxBytes, SnapshotBase base, Path tempDir, String memberStoreId)`; the 6-arg `write` passes `null`. `static SnapshotBase IndexSnapshot.baseOf(Path sqlite, String tag, long bytes)`. `static boolean IndexSnapshot.newer(String a, String b)` (true when watermark `a` is newer than `b`; a non-null `a` is newer than a null `b`).

- [ ] **Step 1: Write the failing tests**

Append to `SnapshotGuardTest`:

```groovy
    def 'a cache that does not hold the member\'s current store-id does not replace the snapshot (stale_store)'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: 'a' * 32], { a, w -> true } as Closure<Boolean>)
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)

        expect:
        IndexSnapshot.write(index, 'lab', storage, 0L, storage.base(tmp), tmp, 'b' * 32).skipped == IndexSnapshot.STALE_STORE
        IndexSnapshot.write(index, 'lab', storage, 0L, storage.base(tmp), tmp, 'a' * 32).written
    }

    def 'a cache behind the stored snapshot\'s watermark does not replace it (older_watermark)'() {
        given: 'a cache that saw r1 only, and a snapshot written after r2'
        addRun('r1')
        final Index behind = Index.open(tmp.resolve('behind.sqlite'))
        behind.catchUp(store, StoreLog.of(store), 'lab')
        Thread.sleep(2)
        addRun('r2')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)

        expect:
        storage.base(tmp).watermark == index.watermark('lab')
        IndexSnapshot.write(behind, 'lab', storage, 0L, storage.base(tmp), tmp).skipped == IndexSnapshot.OLDER_WATERMARK

        cleanup:
        behind.close()
    }

    def 'runs a forget entry removed since the base are added back before fewer_runs is judged'() {
        given:
        final RetentionFixture rf = new RetentionFixture(store)
        final Map gone = rf.run('gone', [[g: rf.raw('g')]])
        rf.run('kept', [[k: rf.raw('k')]])
        index.catchUp(store, StoreLog.of(store), 'lab')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final SnapshotBase base = storage.base(tmp)
        java.nio.file.Files.delete(store.blockPath((Cid) gone.completion))
        final Cid f = store.putDagCbor(new Forget('test', [(Cid) gone.completion]).toCbor())
        StoreLog.append(store, StoreLogKind.FORGET, f, System.currentTimeMillis() + 1)
        index.catchUp(store, StoreLog.of(store), 'lab')

        expect:
        base.runs == 2
        IndexSnapshot.write(index, 'lab', storage, 0L, base, tmp).written
        storage.base(tmp).runs == 1
    }

    def 'a base read from a file without meta still counts its runs'() {
        given:
        final java.nio.file.Path file = tmp.resolve('bare.sqlite')
        java.sql.DriverManager.getConnection("jdbc:sqlite:${file}").withCloseable { c ->
            c.createStatement().withCloseable { st ->
                st.executeUpdate('CREATE TABLE run (id INTEGER PRIMARY KEY)')
                st.executeUpdate('INSERT INTO run VALUES (1), (2)')
            }
        }

        expect:
        IndexSnapshot.baseOf(file, 't', 1L) == new SnapshotBase('t', 1L, 2, null, null)
    }

    def 'newer compares by reverse timestamp, and anything beats nothing'() {
        expect:
        IndexSnapshot.newer('9999999999990-run-x', '9999999999991-run-y')
        !IndexSnapshot.newer('9999999999991-run-y', '9999999999990-run-x')
        !IndexSnapshot.newer('9999999999990-run-x', '9999999999990-claim-z')
        IndexSnapshot.newer('9999999999990-run-x', null)
        !IndexSnapshot.newer(null, '9999999999990-run-x')
    }
```

`RetentionFixture(BlockStore)` needs a `LoggedStore`; `LocalBlockStore` is one. The entry names in the last test are not valid `StoreLog.parse` names (the cid part); `newer` must compare the leading 13 digits directly (`name.substring(0, 13)`) rather than parse, so it works on any entry name.

Update `S3SnapshotStorageTest`: every `storage.replace(built(x), n, base)` becomes `storage.replace(built(x), new SnapshotBase(null, 0L, n), base)`, and add:

```groovy
    def 'store id and watermark ride as metadata and come back in base'() {
        expect:
        storage.replace(built('one'), new SnapshotBase(null, 0L, 2, 'a' * 32, '9999999999990-run-x'), null)
        s3.objects['cas/index/v3.sqlite'].metadata == [runs: '2', 'store-id': 'a' * 32, watermark: '9999999999990-run-x']
        storage.base(tmp) == new SnapshotBase(s3.objects['cas/index/v3.sqlite'].etag, s3.objects['cas/index/v3.sqlite'].size, 2, 'a' * 32, '9999999999990-run-x')
    }
```

(Check the `MemoryS3Ops.Obj` size field name.)

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SnapshotGuardTest' --tests 'robsyme.cas.s3.S3SnapshotStorageTest'`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`SnapshotBase` (in `SnapshotStorage.groovy`): add `String storeId` and `String watermark` after `runs`; `@Canonical` keeps the 3-arg constructor. `SnapshotStorage.replace` becomes `boolean replace(Path built, SnapshotBase written, SnapshotBase base)` (javadoc: `written` describes the file: its runs, store id and watermark; its tag and bytes are ignored).

`IndexSnapshot`:

```groovy
    static final String STALE_STORE = 'stale_store'
    static final String OLDER_WATERMARK = 'older_watermark'

    /** True when Store Log entry name a is newer than b (smaller reverse timestamp); a non-null name is newer than none. */
    static boolean newer(String a, String b) {
        if( a == null || a.length() < 13 ) return false
        if( b == null || b.length() < 13 ) return true
        return a.substring(0, 13) < b.substring(0, 13)
    }

    /** What a snapshot file says of itself: run rows (-1 when uncountable), store_id and watermark when its meta has them. */
    static SnapshotBase baseOf(Path sqlite, String tag, long bytes) {
        final int runs = countRuns(sqlite)
        String storeId = null, watermark = null
        try {
            DriverManager.getConnection("jdbc:sqlite:file:${sqlite.toAbsolutePath()}?mode=ro".toString()).withCloseable { Connection c ->
                final ResultSet rs = c.createStatement().executeQuery('SELECT key, value FROM meta')
                while( rs.next() ) {
                    if( rs.getString(1) == STORE_ID_KEY ) storeId = rs.getString(2)
                    if( rs.getString(1) == WATERMARK_KEY ) watermark = rs.getString(2)
                }
            }
        }
        catch( SQLException e ) {
            // No meta table: a snapshot from before §15's meta, or a hand-made one.
        }
        return new SnapshotBase(tag, bytes, runs, storeId, watermark)
    }
```

Replace the 6-arg `write` with a 7-arg one, and keep the 6-arg as `write(index, member, storage, maxBytes, base, tempDir, null)`:

```groovy
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir, String memberStoreId) {
        if( maxBytes > 0 && base != null && base.bytes >= maxBytes )
            return new Result(false, null, base.bytes, -1, null, OVER_CAP)
        if( memberStoreId != null && index.storeIdOf(member) != memberStoreId )
            return new Result(false, null, 0L, -1, index.watermark(member), STALE_STORE)
        final Built built = build(index, member, tempDir)
        try {
            if( maxBytes > 0 && built.bytes > maxBytes )
                return new Result(false, null, built.bytes, built.runs, built.watermark, OVER_CAP)
            if( base != null && newer(base.watermark, built.watermark) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, OLDER_WATERMARK)
            if( base != null && base.runs > built.runs + index.forgottenRunsSince(member, base.watermark) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, FEWER_RUNS)
            final SnapshotBase written = new SnapshotBase(null, built.bytes, built.runs, built.storeId, built.watermark)
            if( !storage.replace(built.file, written, base) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, REPLACED_MEANWHILE)
            return new Result(true, null, built.bytes, built.runs, built.watermark, null)
        }
        finally {
            Files.deleteIfExists(built.file)
        }
    }
```

Update the `Result.skipped` javadoc to list the two new reasons.

`LocalSnapshotStorage.base`: `return IndexSnapshot.baseOf(target(), "${a.size()}:${a.lastModifiedTime().toMillis()}:${a.fileKey()}".toString(), a.size())`. `replace(Path built, SnapshotBase written, SnapshotBase base)`: same body (the file carries its own meta).

`S3SnapshotStorage.base`: read `runs`, `store-id` and `watermark` from `head.metadata`; when `runs` is an integer return `new SnapshotBase(head.etag, head.size, runs as int, meta['store-id'], meta['watermark'])`; otherwise download once and return `IndexSnapshot.baseOf(copy, head.etag, head.size)` (`runs` -1 when the copy is null, as today). `replace`:

```groovy
    @Override
    boolean replace(Path built, SnapshotBase written, SnapshotBase base) {
        final S3PutOptions o = S3PutOptions.create().sha256().cacheControl(NO_CACHE)
            .contentType('application/vnd.sqlite3').meta('runs', String.valueOf(written.runs))
        if( written.storeId != null ) o.meta('store-id', written.storeId)
        if( written.watermark != null ) o.meta('watermark', written.watermark)
        if( base == null ) o.ifNoneMatch() else o.ifMatch(base.tag)
        final S3Written w = ops.put(snapshotKey, S3Body.ofFile(built, 0L, Files.size(built)), o)
        return w.status == S3Written.Status.WRITTEN
    }
```

Check `SdkS3Ops` copies every `S3PutOptions.metadata` entry into the request and `head` returns them (it does for `runs`; the map is general). Search other callers of `replace(` (`/usr/bin/grep -a -rn "\.replace(.*, base)" src`) and of `SnapshotStorage` fakes in tests, and update them.

`CasSession.snapshotWritable`: call the 7-arg `write` with `writableStoreId()` as `memberStoreId`.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.*' --tests 'robsyme.cas.s3.*' --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.CasSession*'`
Expected: PASS. `CasCommandsTest` `'snapshot says not rewritten: fewer_runs ...'` hand-writes a snapshot with a bare `run` table and no `meta`: its base has no watermark, so the guard still reaches `fewer_runs` (3 > 1 + 0). If it reports another reason, read why before changing the test; the test's intent (a cache with fewer runs keeps the stored snapshot) must still hold.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/{SnapshotStorage,LocalSnapshotStorage,IndexSnapshot}.groovy \
        src/main/groovy/robsyme/cas/s3/S3SnapshotStorage.groovy src/main/groovy/robsyme/cas/CasSession.groovy src/test
git commit -s -m "feat: the snapshot guard checks store id and watermark; fewer_runs counts forgotten runs (§20 decision 10)"
```

---

### Task 8: `nf-blocks:reindex`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (`VERBS`, `run`, `usage`, new `reindex`)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`catchUpIndex(Index, boolean seed)`)
- Modify: `README.md` (beside the `nf-blocks:snapshot` paragraph near line 390)
- Test: `src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy` (append)

**Interfaces:**
- Consumes: everything above.
- Produces: `Set<String> CasSession.catchUpIndex(Index index, boolean seed)`; the 1-arg form is `catchUpIndex(index, true)`. With `seed` false, `null` is passed for `snapshots`.

- [ ] **Step 1: Write the failing tests**

Append to `CasCommandsTest`:

```groovy
    def 'reindex drops the cache and rebuilds it, forgetting rows the store no longer backs'() {
        given: 'a cache holding a run whose blocks were then removed by hand'
        final Cid completion = logRun()
        run('snapshot')
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        Files.delete(store.blockPath(completion))
        Files.list(tempDir.resolve('store/log')).withCloseable { it.toList() }.each { Files.delete(it) }
        out.reset()

        when:
        final int status = run('reindex', ['--seed', 'false'])

        then:
        status == 0
        out.toString().contains('reindexed')
        DriverManager.getConnection("jdbc:sqlite:${tempDir.resolve('cache/index.sqlite')}").withCloseable { c ->
            c.createStatement().executeQuery('SELECT count(*) FROM run').with { next(); getInt(1) }
        } == 0
    }

    def 'reindex takes only --seed, as true or false'() {
        expect:
        run('reindex', ['--seed', 'maybe']) == 2
        run('reindex', ['extra']) == 2
    }

    def 'the usage lists reindex'() {
        expect:
        run('frobnicate') == 2
        err.toString().contains('reindex [--seed false]')
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.cli.CasCommandsTest'`
Expected: FAIL (unknown verb `reindex`).

- [ ] **Step 3: Implement**

`CasSession`: rename the body of `catchUpIndex(Index)` into `catchUpIndex(Index index, boolean seed)`, passing `seed ? snapshotsOf(member.alias()) : null`; `catchUpIndex(Index index)` returns `catchUpIndex(index, true)`.

`CasCommands`: add `'reindex'` to `VERBS` (keep it sorted: `['explore', 'items', 'prune', 'put', 'reindex', 'snapshot', 'sweep', 'untrash']`); in `run`, `case 'reindex': return reindex(Options.parse(args, ['seed'] as Set), config, out)`; in `usage`, after the `snapshot` line:

```groovy
            '  reindex [--seed false]  drop this composition\'s cache index and rebuild it from the store; --seed false scans every block\n' +
```

and:

```groovy
    /**
     * DESIGN.md §20 decision 12: deletes the cache file of this composition and
     * catches every member up cold, then rewrites the writable member's snapshot.
     */
    private static int reindex(Options options, Map config, PrintStream out) {
        if( options.positionals )
            throw new UsageException("reindex takes no arguments, got ${options.positionals}")
        final String seed = options.flag('seed')
        if( !(seed in [null, 'true', 'false']) )
            throw new UsageException("--seed is true or false, got '${seed}'")
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final java.nio.file.Path file = robsyme.cas.core.IndexPaths.cachePath(cas.config.locationTexts(), cas.config.indexOverride)
        for( String suffix : ['', '-wal', '-shm', '-journal'] )
            java.nio.file.Files.deleteIfExists(file.resolveSibling(file.fileName.toString() + suffix))
        final SnapshotBase base = cas.snapshotBase()
        final Index index = cas.openIndex()
        try {
            final Set<String> failed = cas.catchUpIndex(index, seed != 'false')
            final IndexSnapshot.Result result = cas.snapshotWritable(index, 0L, base, failed)
            out.println("reindexed ${file}${failed ? '; could not catch up ' + failed.join(', ') : ''}")
            out.println(result.skipped ? "snapshot not rewritten: ${result.skipped}" :
                "wrote ${cas.snapshotsOf(cas.config.writableAlias).describe()} (${result.runs} runs)")
            return failed ? 1 : 0
        }
        finally {
            index.close()
        }
    }
```

A rebuilt cache has fewer runs than a snapshot that still lists swept runs, so the rewrite can be skipped as `fewer_runs`; that is reported, not an error. Check the `Options.flag` name and `UsageException` handling against `snapshot`/`put`.

`README.md`: after the paragraph that introduces `nextflow plugin nf-blocks:snapshot` (around line 390), add:

```markdown
`nextflow plugin nf-blocks:reindex` deletes this machine's cache index for the
configured store and rebuilds it, seeding from each member's Index Snapshot
(`--seed false` scans every block instead). The cache corrects itself when a
member is deleted and recreated, or when a sweep deletes runs; reindex is for
blocks removed by hand or by a bucket lifecycle rule.
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.CasPluginTest'`
Expected: PASS (`CasPluginTest` may pin the verb list; update it if so).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/cli/CasCommands.groovy src/main/groovy/robsyme/cas/CasSession.groovy README.md src/test
git commit -s -m "feat(cli): nf-blocks:reindex (§20 decision 12)"
```

---

### Task 9: Gate assertions 17 and 18

**Files:**
- Modify: `gate/cas.py` (`STORE_LOG_KINDS`)
- Modify: `gate/assert.py` (two assertions, `--coherence-refs`, `--coherence-age`, the retention checkpoint's block list)
- Modify: `gate/gate.sh` (a section after assertion 9's last checkpoint, before the consumer)
- Modify: `gate/README.md` (assertion list)
- Test: `gate/test_assert.py` (append)

**Interfaces:**
- Consumes: the built plugin (Tasks 1 to 8).
- Produces in `assert.py`: pure judges `coherence_17_problems(state)` and `coherence_18_problems(state)` returning a list of problem strings; gatherers `coherence_17_state(gate)` and `coherence_18_state(gate)` returning dicts; `@assertion(17, "a recreated member shows only its own runs")` and `@assertion(18, "a sweep reaches every cache")`.

- [ ] **Step 1: Write the failing unit tests**

Append to `gate/test_assert.py` (inside a new `unittest.TestCase`; import `assert` the way the file already does):

```python
class CoherenceJudges(unittest.TestCase):

    def state17(self, **over):
        s = {"new_run": "bafyNEW", "cache_runs": ["bafyNEW"], "store_id": "a" * 32,
             "cache_store_ids": ["a" * 32], "snapshot_runs": ["bafyNEW"], "snapshot_store_id": "a" * 32,
             "cold_runs": ["bafyNEW"], "exits": {"recreate-1": 0, "recreate-2": 0, "snap-2": 0, "cold": 0}}
        s.update(over)
        return s

    def test_17_passes_on_one_new_run_everywhere(self):
        self.assertEqual(A.coherence_17_problems(self.state17()), [])

    def test_17_names_an_old_run_left_in_the_cache(self):
        p = A.coherence_17_problems(self.state17(cache_runs=["bafyNEW", "bafyOLD"]))
        self.assertTrue(any("bafyOLD" in x and "cache" in x for x in p))

    def test_17_needs_the_snapshot_to_carry_the_member_id(self):
        p = A.coherence_17_problems(self.state17(snapshot_store_id=None))
        self.assertTrue(any("store_id" in x for x in p))

    def state18(self, **over):
        s = {"r1": "bafyR1", "r2": "bafyR2", "listed": ["bafyR1", "bafyCOLL"],
             "sweep_json": {"applied": True, "snapshot": "wrote x (1 runs)"},
             "b_rows": [], "b_runs": ["bafyR2"], "snapshot_runs": ["bafyR2"], "c_runs": ["bafyR2"],
             "exits": {"sweep-2": 0, "b-1": 0, "c-1": 0}}
        s.update(over)
        return s

    def test_18_passes_when_every_cache_forgot_r1(self):
        self.assertEqual(A.coherence_18_problems(self.state18()), [])

    def test_18_needs_a_forget_entry_listing_r1(self):
        p = A.coherence_18_problems(self.state18(listed=[]))
        self.assertTrue(any("forget" in x for x in p))

    def test_18_refuses_a_fewer_runs_rewrite(self):
        p = A.coherence_18_problems(self.state18(sweep_json={"applied": True, "snapshot": "not rewritten: fewer_runs"}))
        self.assertTrue(any("fewer_runs" in x for x in p))

    def test_18_names_rows_cache_b_kept(self):
        p = A.coherence_18_problems(self.state18(b_rows=[("run", "bafyR1")]))
        self.assertTrue(any("bafyR1" in x for x in p))
```

(`A` is however `test_assert.py` already imports `gate/assert.py`; check its top lines.)

- [ ] **Step 2: Run to see them fail**

Run: `python3 -m unittest gate.test_assert -k Coherence` (or `cd gate && python3 -m unittest test_assert`)
Expected: AttributeError (`coherence_17_problems`).

- [ ] **Step 3: Implement the judges, gatherers and flags**

`gate/cas.py`: `STORE_LOG_KINDS = ("run", "selection", "claim", "forget")`.

`gate/assert.py`, near assertion 9:

```python
COHERENCE_RECREATE = "store-recreate"
COHERENCE_STORE = "store-coherence"


def _cache_runs(cache_home):
    return sorted(r["completion_cid"] for r in cas.Index.open(cache_home).query("SELECT completion_cid FROM run"))


def _snapshot(store_root):
    path = os.path.join(store_root, "index", "v3.sqlite")
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        runs = sorted(r[0] for r in con.execute("SELECT completion_cid FROM run"))
        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
    finally:
        con.close()
    return runs, meta.get("store_id")


def _store_id(store_root):
    try:
        with open(os.path.join(store_root, "store-id")) as fh:
            return fh.read().strip()
    except OSError:
        return None


def coherence_17_state(gate):
    root = os.path.join(gate.root, COHERENCE_RECREATE)
    store = cas.Store(root)
    runs = [cid for _rts, kind, cid in store.store_log() if kind == "run"]
    index = cas.Index.open(os.path.join(gate.root, "cache-recreate"))
    snap_runs, snap_id = _snapshot(root)
    return {"new_run": runs[0] if len(runs) == 1 else None,
            "cache_runs": _cache_runs(os.path.join(gate.root, "cache-recreate")),
            "store_id": _store_id(root),
            "cache_store_ids": [r["value"] for r in index.query("SELECT value FROM meta WHERE key LIKE 'store_id:%'")],
            "snapshot_runs": snap_runs, "snapshot_store_id": snap_id,
            "cold_runs": _cache_runs(os.path.join(gate.root, "cache-recreate-c")),
            "exits": {name: _coherence_exit(gate, name) for name in ("recreate-1", "recreate-2", "snap-2", "cold")}}


def coherence_17_problems(s):
    problems = ["%s exited %s; see logs/coherence/" % (n, c) for n, c in sorted(s["exits"].items()) if c != 0]
    if not s["new_run"]:
        return problems + ["the recreated member's Store Log does not hold exactly one run"]
    for label, runs in (("the cache", s["cache_runs"]), ("the member's snapshot", s["snapshot_runs"]),
                        ("a cold cache seeded from it", s["cold_runs"])):
        if runs != [s["new_run"]]:
            problems.append("%s holds %s, not only the new run %s" % (label, runs, s["new_run"]))
    if not s["store_id"]:
        problems.append("the recreated member has no store-id")
    elif s["cache_store_ids"] != [s["store_id"]]:
        problems.append("the cache records store ids %s, the member's is %s" % (s["cache_store_ids"], s["store_id"]))
    if s["snapshot_store_id"] != s["store_id"]:
        problems.append("the snapshot's store_id is %s, the member's %s" % (s["snapshot_store_id"], s["store_id"]))
    return problems


def coherence_18_state(gate):
    root = os.path.join(gate.root, COHERENCE_STORE)
    store = cas.Store(root)
    runs = retention_runs(store)
    listed = []
    for _rts, kind, cid in store.store_log():
        if kind == "forget" and store.has(cid):
            listed.extend(str(c) for c in store.read_block(cid).get("deleted") or [])
    b = cas.Index.open(os.path.join(gate.root, "cache-coh-b"))
    rows = []
    for table, column in (("run", "completion_cid"), ("collection", "collection_cid"), ("item", "item_cid"),
                          ("claim", "claim_cid"), ("log_entry", "cid")):
        for cid in listed:
            if b.scalar("SELECT count(*) FROM %s WHERE %s = ?" % (table, column), (cid,)):
                rows.append((table, cid))
    snap_runs, _ = _snapshot(root)
    with open(os.path.join(gate.root, "logs", "coherence", "sweep-2.out")) as fh:
        sweep_json = json.loads(fh.read().strip().splitlines()[-1])
    return {"r1": runs["a"][0], "r2": runs["b"][0], "listed": listed, "sweep_json": sweep_json,
            "b_rows": rows, "b_runs": _cache_runs(os.path.join(gate.root, "cache-coh-b")),
            "snapshot_runs": snap_runs, "c_runs": _cache_runs(os.path.join(gate.root, "cache-coh-c")),
            "exits": {name: _coherence_exit(gate, name) for name in ("sweep-2", "b-1", "c-1")}}


def coherence_18_problems(s):
    problems = ["%s exited %s; see logs/coherence/" % (n, c) for n, c in sorted(s["exits"].items()) if c != 0]
    if s["r1"] not in s["listed"]:
        problems.append("no forget entry in the member's log lists run a's RunCompletion %s" % s["r1"])
    if not s["sweep_json"].get("applied"):
        problems.append("the deleting sweep did not apply")
    if not str(s["sweep_json"].get("snapshot", "")).startswith("wrote"):
        problems.append("the deleting sweep's snapshot rewrite said %r" % s["sweep_json"].get("snapshot"))
    for table, cid in s["b_rows"]:
        problems.append("cache B still has a %s row for %s, which a forget entry lists" % (table, cid))
    for label, runs in (("cache B", s["b_runs"]), ("the member's snapshot", s["snapshot_runs"]),
                        ("cold cache C", s["c_runs"])):
        if s["r1"] in runs:
            problems.append("%s still holds the swept run %s" % (label, s["r1"]))
        if s["r2"] not in runs:
            problems.append("%s lost the kept run %s" % (label, s["r2"]))
    return problems


def _coherence_exit(gate, step):
    try:
        with open(os.path.join(gate.root, "logs", "coherence", step + ".exit")) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return None


@assertion(17, "a recreated member shows only its own runs")
def assert_seventeen(gate):
    try:
        problems = coherence_17_problems(coherence_17_state(gate))
    except Exception as exc:
        return FAIL, "could not read the recreated member or its caches: %s: %s" % (type(exc).__name__, exc)
    return (FAIL, "; ".join(problems[:5])) if problems else (PASS, "one run in the cache, the snapshot and a seeded cache; ids agree")


@assertion(18, "a sweep reaches every cache")
def assert_eighteen(gate):
    try:
        problems = coherence_18_problems(coherence_18_state(gate))
    except Exception as exc:
        return FAIL, "could not read store-coherence or its caches: %s: %s" % (type(exc).__name__, exc)
    return (FAIL, "; ".join(problems[:5])) if problems else (PASS, "run a forgotten by caches A, B and C and by the snapshot; run b kept")
```

Check `retention_runs` returns `{'a': (cid, block), 'b': (cid, block)}` as its docstring says, `Store.read_block` returns the decoded map with `deleted` as CID objects (`str()` gives the text), and `cas.Index.scalar` exists (it does). `sweep-2.out` holds the verb's stdout; the JSON object is its last line.

`main()`: add `"--coherence-refs"` and `"--coherence-age"` to `known`, and before the `--retention-age` branch:

```python
    if "--coherence-refs" in flags:
        try:
            runs = retention_runs(cas.Store(os.path.join(root, COHERENCE_STORE)))
            print("COH_R1=%s" % _shquote(runs["a"][0]))
        except Exception as exc:
            sys.stderr.write("coherence-refs: %s: %s\n" % (type(exc).__name__, exc))
            print("COH_R1=''")
        return 0
    if "--coherence-age" in flags:
        import time
        when = time.time() - float(args[1]) * 86400
        count = 0
        for _cid, path in cas.Store(os.path.join(root, COHERENCE_STORE)).blocks():
            os.utime(path, (when, when))
            count += 1
        print("    backdated %d block(s) of %s by %s days" % (count, COHERENCE_STORE, args[1]))
        return 0
```

`retention_checkpoint`: make `"blocks"` leave out Forget blocks (plan decision 6), since sweeps now write them:

```python
    forgets = {cid for cid, _p in store.blocks(kind="Forget")}
    data = {"name": name,
            "blocks": sorted(cid for cid, _p in store.blocks() if cid not in forgets),
            ...
```

(Check `Store.blocks(kind=...)` yields `(cid, path)` filtered by the decoded `kind`.)

- [ ] **Step 4: Add the steps to `gate.sh`**

After `checkpoint after-sweep-4` and before the consumer section:

```bash
# --------------------------------------------------------------------------
# Assertions 17 and 18 (DESIGN §20): cache coherence
# --------------------------------------------------------------------------

mkdir -p "$GATE_ROOT/logs/coherence"
coh() {   # <store dir> <cache dir> <step> [-c extra.config] <verb and args...>: never aborts
    local store="$1" cache="$2" step="$3"; shift 3
    local extra=()
    if [[ "${1:-}" == "-c" ]]; then extra=(-c "$2"); shift 2; fi
    local log="$GATE_ROOT/logs/coherence/$step" status=0
    ( cd "$RET" && unset NXF_OFFLINE && export GATE_STORE="$GATE_ROOT/$store" \
        NXF_PLUGINS_TEST_REPOSITORY="file://$ret_json" XDG_CACHE_HOME="$GATE_ROOT/$cache" \
      && "$NEXTFLOW" -q -c "$REPO/gate/gate.config" -c "$REPO/gate/retention/overlay.config" "${extra[@]+"${extra[@]}"}" \
           plugin "nf-blocks:$1" "${@:2}" ) > "$log.out" 2> "$log.err" || status=$?
    echo "$status" > "$log.exit"
    echo "    nf-blocks:$1 ($step) exit $status"
}
coh_run() {   # <step> <tag>: the retention pipeline into store-recreate with cache-recreate
    local status=0
    GATE_STORE="$GATE_ROOT/store-recreate" XDG_CACHE_HOME="$GATE_ROOT/cache-recreate" \
        run "$RET" "$1" --tag "$2" -c "$REPO/gate/retention/overlay.config"
    cp "$GATE_ROOT/logs/$1/exit" "$GATE_ROOT/logs/coherence/$1.exit"
}

# 17: a member deleted and recreated at the same location, with the same cache.
rm -rf "$GATE_ROOT/store-recreate" "$GATE_ROOT/store-recreate-old" "$GATE_ROOT"/cache-recreate*
mkdir -p "$GATE_ROOT/store-recreate" "$GATE_ROOT/cache-recreate"
coh_run recreate-1 r1
mv "$GATE_ROOT/store-recreate" "$GATE_ROOT/store-recreate-old"
mkdir -p "$GATE_ROOT/store-recreate"
coh_run recreate-2 r2
coh store-recreate cache-recreate snap-2 snapshot
coh store-recreate cache-recreate-c cold snapshot

# 18: a copy of store-retention as a fresh incarnation, two caches, a sweep through A.
rm -rf "$GATE_ROOT/store-coherence" "$GATE_ROOT"/cache-coh-*
cp -Rp "$GATE_ROOT/store-retention" "$GATE_ROOT/store-coherence"
rm -rf "$GATE_ROOT/store-coherence/store-id" "$GATE_ROOT/store-coherence/index" "$GATE_ROOT/store-coherence/index.html"
coh store-coherence cache-coh-a a-0 snapshot
coh store-coherence cache-coh-b b-0 snapshot
eval "$(python3 "$REPO/gate/assert.py" "$GATE_ROOT" --coherence-refs)"
printf '{"kind":"Claim","subject":{"/":"%s"},"verb":"delete","attribute":null,"value":null,"supersedes":[],"timestamp":"%s"}\n' \
    "$COH_R1" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" > "$GATE_ROOT/logs/coherence/delete-r1.request"
coh store-coherence cache-coh-a delete-r1 put "$GATE_ROOT/logs/coherence/delete-r1.request"
python3 "$REPO/gate/assert.py" "$GATE_ROOT" --coherence-age 15
coh store-coherence cache-coh-a sweep-1 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
coh store-coherence cache-coh-a sweep-2 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
coh store-coherence cache-coh-b b-1 snapshot
coh store-coherence cache-coh-c c-1 snapshot
```

Check against the retention section: `run`'s extra arguments after the name go to `nextflow run`; `--tag` is the retention pipeline's parameter, and `-c overlay.config` is passed as assertion 9 passes it. If `run` needs `GATE_STORE` to exist first, the `mkdir` above covers it.

`gate/README.md`: add 17 and 18 to the assertion list in the style of the existing entries, and change the lineage-tier total to 18 PASS.

- [ ] **Step 5: Run the unit tests, then the Gate**

Run: `python3 -m unittest discover -s gate` — Expected: all OK.
Run: `make gate` on a fresh `GATE_ROOT` (about 5 minutes; assertion 4's resumed-run flake may need one rerun).
Expected: lineage 18 PASS, 0 FAIL, 5 SKIP; browser tier A 5/5; tier B 14/14. If assertion 9 fails, read its message: a Forget block named as "a block outside run b's unshared content" means a judging path reads blocks other than the checkpoint's `blocks` list; filter Forget blocks there the same way.

- [ ] **Step 6: Commit**

```bash
git add gate/cas.py gate/assert.py gate/gate.sh gate/README.md gate/test_assert.py
git commit -s -m "gate: assertions 17 and 18, cache coherence (§20)"
```

---

### Task 10: DESIGN.md records what was built

**Files:**
- Modify: `DESIGN.md` (§20 status, a "Decisions made while building" subsection, §0 verb rule if it lists verbs, §14 lineage-tier count)

- [ ] **Step 1: Update §20**

Replace the status line with `*Status: built on design/cache-coherence; <test counts and Gate result from Task 9's run>.*`. Add `### Decisions made while building` after `### Open questions`, one numbered entry per plan decision 1 to 14 above, each a short paragraph saying what was built and, for 1, 6, 7 and 10, which §20 decision it changes. Edit §20 decisions 4, 6, 7 and 10 in place to say "Amended by decision N below" where they no longer match.

- [ ] **Step 2: Update the rest**

§14: lineage tier `18 PASS, 0 FAIL, 5 SKIP`, and replace "Proposed 2026-10-09: assertions 17 and 18 (§20)." with one sentence per assertion. §0 or §15 "Plugin verbs": add `reindex` wherever the verbs are listed (`/usr/bin/grep -a -n "untrash" DESIGN.md`). The "Proposed 2026-10-09" pointers in §5, §12 and §15 become "Since 2026-10-09".

- [ ] **Step 3: Run every check**

Run: `./gradlew check`, `cd web && npm test`, `python3 -m unittest discover -s gate`.
Expected: all green (the web schema test reads DESIGN.md; it must still find 24 types).

- [ ] **Step 4: Commit**

```bash
git add DESIGN.md
git commit -s -m "docs(design): §20 as built"
```
