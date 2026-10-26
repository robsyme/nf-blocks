package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 3 follow-up: a build records no presence (block_present) while
 * the writable member's sweep.lock says a sweep may be deleting, read before
 * the build reads the roots and again just before it records. The first
 * feature is the integrator's repro (ReviewPresentRaceTest): a restore and a
 * build inside a sweep's delete batch, with the real Sweep.
 */
class IndexSnapshotSweepLockTest extends Specification {
    @TempDir Path root

    /** A delegating RetentionStorage whose deleteBlocks runs a hook first. */
    static class HookedRetention implements RetentionStorage {
        @Delegate RetentionStorage inner
        Closure hook
        @Override
        List<Cid> deleteBlocks(Collection<Cid> cids) {
            if( hook != null ) { final Closure h = hook; hook = null; h.call(cids) }
            return inner.deleteBlocks(cids)
        }
    }

    static class HookedStore extends LocalBlockStore {
        Closure hook
        HookedStore(Path root) { super(root, 'lab', true) }
        @Override
        RetentionStorage retentionStorage() { new HookedRetention(inner: super.retentionStorage(), hook: hook) }
    }

    /** A delegating RetentionStorage that counts lock reads, and fails them when told to. */
    static class CountingRetention implements RetentionStorage {
        @Delegate RetentionStorage inner
        int reads
        boolean failing
        @Override
        Versioned readLock() {
            reads++
            if( failing ) throw new IOException('sweep.lock: 503 Slow Down')
            return inner.readLock()
        }
    }

    private static List<Map> rows(Path db, String sql) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def rs = c.createStatement().executeQuery(sql); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    private Map write(Index index, Closure<Boolean> present, RetentionStorage sweepLock, String out = 'out') {
        def r = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve(out)), 0L, null, root.resolve('t'), null, false, 1000L,
            present, sweepLock)
        assert r.written
        rows(IndexSnapshot.pathIn(root.resolve(out)), 'SELECT stored_bytes, held_bytes, pinned_bytes, releasing_bytes FROM store_stats')[0]
    }

    private Map write(BlockStore store, Index index, String out = 'out') {
        write(index, { Cid c -> store.has(c) } as Closure<Boolean>, ((RetainedStore) store).retentionStorage(), out)
    }

    private static final def SAY = { String msg -> null } as Closure<Void>
    private static final def SLEEPER = { long ms -> null } as Closure<Void>

    /** Ages every block past the floor, sweeps, then moves the new ledgers past their deadline. */
    private static void sweepOnce(BlockStore store) {
        long aged = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        ((LocalBlockStore) store).listBlocks().withCloseable { it.toList() }.each { Cid c ->
            Files.setLastModifiedTime(((LocalBlockStore) store).blockPath(c), FileTime.fromMillis(aged)) }
        new Sweep(store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, SAY, SLEEPER)
        RetentionStorage s = ((RetainedStore) store).retentionStorage()
        for( String name : s.listLedgers() ) {
            final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
            final TrashLedger past = new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)
            s.writeLedger(past.name, past.toJson()); if( past.name != name ) s.deleteLedger(name)
        }
    }

    private Path lockFile() { root.resolve('lab').resolve('sweep.lock') }

    /** Rewrites sweep.lock with this body, last modified `ageMillis` ago. */
    private void lock(String body, long ageMillis) {
        Files.write(lockFile(), body.getBytes('UTF-8'))
        Files.setLastModifiedTime(lockFile(), FileTime.fromMillis(System.currentTimeMillis() - ageMillis))
    }

    /**
     * Run a (shared) and run b (shared + only), b released, swept (only is
     * gone), then restored; a cache that never saw the sweep. Its first
     * build with no lock in the way records `shared` present.
     */
    private Map restored() {
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid shared = f.raw('shared')
        Cid only = f.raw('only b')
        Map a = f.run('a', [[s: shared]])
        Map b = f.run('b', [[s: shared, u: only]])
        Cid release = f.claim(b.completion, 'set', 'retain', 'lineage')
        sweepOnce(f.store)
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, SAY, SLEEPER)
        assert !f.store.has(only)
        f.claim(b.completion, 'del', 'retain', null, [release])
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        [f: f, index: index, shared: shared, only: only]
    }

    def 'a restore and a build inside a sweep batch record no presence for a block the batch then deletes'() {
        given: 'run a (shared) and run b (shared + only); b released; a first sweep trashes only; its ledger is past due'
        HookedStore store = new HookedStore(root.resolve('lab'))
        RetentionFixture f = new RetentionFixture(store)
        Cid shared = f.raw('shared')
        Cid only = f.raw('only b')
        Map a = f.run('a', [[s: shared]])
        Map b = f.run('b', [[s: shared, u: only]])
        Cid release = f.claim(b.completion, 'set', 'retain', 'lineage')
        sweepOnce(store)
        assert store.has(only)
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(store, StoreLog.of(store), 'lab')

        and: 'inside the delete batch (after the sweep re-checked the Store Log): b is restored, the cache catches up and builds'
        Map during = null
        store.hook = { Collection<Cid> batch ->
            assert batch.contains(only)
            f.claim(b.completion, 'del', 'retain', null, [release])
            index.catchUp(store, StoreLog.of(store), 'lab')
            during = write(store, index)
        }

        when: 'the second sweep deletes the due block'
        new Sweep(store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, SAY, SLEEPER)

        then: 'the block is gone, and the build in the window, which saw the sweep holding the lock, recorded no presence'
        during != null
        !store.has(only)
        index.presentBlocks('lab').isEmpty()

        when: 'later builds of the same cache, with no Claim since; and a fresh cache as control'
        index.catchUp(store, StoreLog.of(store), 'lab')
        Map after1 = write(store, index)
        index.catchUp(store, StoreLog.of(store), 'lab')
        Map after2 = write(store, index)
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        fresh.catchUp(store, StoreLog.of(store), 'lab')
        Map control = write(store, fresh, 'out2')
        fresh.close()

        then: 'as a fresh cache says: the deleted file counts in no figure'
        control.stored_bytes == 1
        after1 == control
        after2 == control

        cleanup:
        index?.close()
    }

    def 'with no sweep.lock, a build records presences as before'() {
        given:
        Map l = restored()
        Files.delete(lockFile())
        CountingRetention storage = new CountingRetention(inner: l.f.store.retentionStorage())

        when:
        Map s = write(l.index, { Cid c -> l.f.store.has(c) } as Closure<Boolean>, storage)

        then: 'the lock is read at build start and again before recording'
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        l.index.presentBlocks('lab') == [l.shared.toString()] as Set
        l.index.absentBlocks('lab') == [l.only.toString()] as Set
        storage.reads == 2

        cleanup:
        l?.index?.close()
    }

    def 'a lock released a while ago is no lock: presences are recorded'() {
        given:
        Map l = restored()
        lock('{"sweep":"s1","state":"released","started_at":"2026-10-24T00:00:00.000Z","beat":3}', 2 * SweepLock.HEARTBEAT_MILLIS)

        when:
        Map s = write(l.f.store, l.index)

        then:
        s.stored_bytes == 1
        l.index.presentBlocks('lab') == [l.shared.toString()] as Set

        cleanup:
        l?.index?.close()
    }

    def 'a lock that cannot be read records nothing; lookups and figures are as without it'() {
        given:
        Map l = restored()
        Files.delete(lockFile())
        CountingRetention storage = new CountingRetention(inner: l.f.store.retentionStorage(), failing: true)
        AtomicInteger asked = new AtomicInteger()

        when:
        Map s = write(l.index, { Cid c -> asked.incrementAndGet(); l.f.store.has(c) } as Closure<Boolean>, storage)

        then:
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        asked.get() == 2
        l.index.presentBlocks('lab').isEmpty()
        l.index.absentBlocks('lab') == [l.only.toString()] as Set
        storage.reads == 1

        cleanup:
        l?.index?.close()
    }

    def 'no retention storage records nothing; lookups and figures are as without it'() {
        given:
        Map l = restored()
        Files.delete(lockFile())

        when:
        Map s = write(l.index, { Cid c -> l.f.store.has(c) } as Closure<Boolean>, null)

        then:
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        l.index.presentBlocks('lab').isEmpty()
        l.index.absentBlocks('lab') == [l.only.toString()] as Set

        cleanup:
        l?.index?.close()
    }

    def 'a lock at build start records nothing, whether fresh, stale, unparseable or just released'() {
        given:
        Map l = restored()
        lock(body, ageMillis)

        when:
        Map s = write(l.f.store, l.index)

        then:
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        l.index.presentBlocks('lab').isEmpty()
        l.index.absentBlocks('lab') == [l.only.toString()] as Set

        cleanup:
        l?.index?.close()

        where:
        body                                                    | ageMillis
        '{"sweep":"s1","state":"held","beat":0}'                | 0L
        '{"sweep":"s1","state":"held","beat":0}'                | 10 * SweepLock.STALE_MILLIS
        'not json'                                              | 10 * SweepLock.STALE_MILLIS
        '[1, 2]'                                                | 10 * SweepLock.STALE_MILLIS
        '{"sweep":"s1","state":"released","beat":4}'            | 0L
    }

    def 'a lock taken after the build started records nothing'() {
        given:
        Map l = restored()
        Files.delete(lockFile())
        RetentionStorage storage = l.f.store.retentionStorage()
        SweepLock late = new SweepLock(storage, 'late', { -> System.currentTimeMillis() } as Closure<Long>)

        when: 'a sweep takes the lock while the build looks blocks up'
        Map s = write(l.index, { Cid c ->
            synchronized( late ) { if( !late.held ) late.take() }
            l.f.store.has(c)
        } as Closure<Boolean>, storage)

        then:
        late.held
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        l.index.presentBlocks('lab').isEmpty()

        cleanup:
        l?.index?.close()
    }

    def 'a build with nothing to record reads the lock once, at build start'() {
        given: 'a store no sweep touched: nothing is looked up'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        f.run('a', [[s: f.raw('shared')]])
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        CountingRetention storage = new CountingRetention(inner: f.store.retentionStorage())
        AtomicInteger asked = new AtomicInteger()

        when:
        write(index, { Cid c -> asked.incrementAndGet(); f.store.has(c) } as Closure<Boolean>, storage)

        then:
        asked.get() == 0
        storage.reads == 1

        cleanup:
        index?.close()
    }
}
