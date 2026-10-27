package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 04 decision 3 and ticket 03 decision 1, local member. */
class SnapshotGuardTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore store
    Index index

    def setup() {
        store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    /** One run in the store, logged and indexed (the IndexSnapshotTest helpers do the same). */
    private void addRun(String name) {
        final Cid m = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(store, StoreLogKind.RUN, rc, System.currentTimeMillis())
        index.catchUp(store, StoreLog.of(store), 'lab')
    }

    def 'a snapshot is written and counted; its base says how many runs it has'() {
        given:
        addRun('r1'); addRun('r2')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)

        when:
        final IndexSnapshot.Result r = IndexSnapshot.write(index, 'lab', storage, 0L, storage.base(tmp), tmp)

        then:
        r.written && r.runs == 2
        storage.base(tmp).runs == 2
        IndexSnapshot.countRuns(IndexSnapshot.pathIn(store.root)) == 2
    }

    def 'a snapshot with fewer runs than the one it would replace is not written (fewer_runs)'() {
        given:
        addRun('r1'); addRun('r2')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final Index fresh = Index.open(tmp.resolve('fresh.sqlite'))
        final SnapshotBase b = storage.base(tmp)

        when: 'a cache that saw only an empty log, against a base that records no watermark (older_watermark would answer first otherwise)'
        final IndexSnapshot.Result r = IndexSnapshot.write(fresh, 'lab', storage, 0L, new SnapshotBase(b.tag, b.bytes, b.runs), tmp)

        then:
        !r.written
        r.skipped == IndexSnapshot.FEWER_RUNS
        storage.base(tmp).runs == 2

        cleanup:
        fresh.close()
    }

    def 'the cap still keeps a large snapshot'() {
        given:
        addRun('r1')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)

        expect:
        IndexSnapshot.write(index, 'lab', storage, 1L, storage.base(tmp), tmp).skipped == IndexSnapshot.OVER_CAP
    }

    def 'no temp file is left in the member or in tempDir'() {
        given:
        addRun('r1')

        when:
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(store.root), 0L, null, tmp.resolve('t'))

        then:
        Files.list(store.root.resolve('index')).toList()*.fileName*.toString() == ['v7.sqlite']
        Files.list(tmp.resolve('t')).count() == 0
    }

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

    def 'fewer_runs against a base with a watermark the cache has reached, not counting a forget entry older than the base'() {
        given: 'runs one, two and three; a forget entry removing one; then run four, so the forget entry is older than the watermark'
        final RetentionFixture rf = new RetentionFixture(store)
        final Map one = rf.run('one', [[a: rf.raw('one-a')]])
        rf.run('two', [[b: rf.raw('two-b')]])
        rf.run('three', [[c: rf.raw('three-c')]])
        index.catchUp(store, StoreLog.of(store), 'lab')
        Files.delete(store.blockPath((Cid) one.completion))
        final Cid f = store.putDagCbor(new Forget('test', [(Cid) one.completion]).toCbor())
        StoreLog.append(store, StoreLogKind.FORGET, f, ++rf.clock)
        rf.run('four', [[d: rf.raw('four-d')]])
        index.catchUp(store, StoreLog.of(store), 'lab')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final SnapshotBase written = storage.base(tmp)

        when: 'a base at the cache\'s own watermark that counts one run more than the cache holds'
        final SnapshotBase base = new SnapshotBase(written.tag, written.bytes, written.runs + 1, written.storeId, index.watermark('lab'))
        final IndexSnapshot.Result r = IndexSnapshot.write(index, 'lab', storage, 0L, base, tmp)

        then: 'the forget entry is older than the base, so its run is not added back'
        written.runs == 3
        index.forgottenRunsSince('lab', null) == 1
        index.forgottenRunsSince('lab', base.watermark) == 0
        r.skipped == IndexSnapshot.FEWER_RUNS
    }

    def 'a base written for another incarnation of the member is not judged by watermark or run count'() {
        given: 'a cache holding the member\'s id, and a base from another id that is newer and counts more runs'
        addRun('r1')
        index.reconcileStoreIds([lab: 'a' * 32], { a, w -> true } as Closure<Boolean>)
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final SnapshotBase b = storage.base(tmp)
        final SnapshotBase other = new SnapshotBase(b.tag, b.bytes, 9, 'c' * 32, '0000000000000-run-x')

        expect:
        IndexSnapshot.write(index, 'lab', storage, 0L, other, tmp, 'a' * 32).written
        storage.base(tmp).storeId == 'a' * 32
    }

    def 'a base of the same incarnation is still judged by watermark and run count'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: 'a' * 32], { a, w -> true } as Closure<Boolean>)
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final SnapshotBase b = storage.base(tmp)

        expect:
        IndexSnapshot.write(index, 'lab', storage, 0L, new SnapshotBase(b.tag, b.bytes, 9, 'a' * 32, '0000000000000-run-x'), tmp, 'a' * 32).skipped == IndexSnapshot.OLDER_WATERMARK
        IndexSnapshot.write(index, 'lab', storage, 0L, new SnapshotBase(b.tag, b.bytes, 9, 'a' * 32, b.watermark), tmp, 'a' * 32).skipped == IndexSnapshot.FEWER_RUNS
    }

    /** Storage another writer always got to first. */
    static class ReplacedMeanwhile implements SnapshotStorage {
        @Delegate(excludes = ['replace']) final SnapshotStorage inner
        ReplacedMeanwhile(SnapshotStorage inner) { this.inner = inner }
        boolean replace(Path built, SnapshotBase written, SnapshotBase base) { false }
    }

    def 'a rebuild waives older_watermark and fewer_runs, and keeps stale_store and replaced_meanwhile'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: 'a' * 32], { a, w -> true } as Closure<Boolean>)
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final SnapshotBase b = storage.base(tmp)

        expect:
        IndexSnapshot.write(index, 'lab', storage, 0L, new SnapshotBase(b.tag, b.bytes, 9, 'a' * 32, '0000000000000-run-x'), tmp, 'b' * 32, true).skipped == IndexSnapshot.STALE_STORE
        IndexSnapshot.write(index, 'lab', new ReplacedMeanwhile(storage), 0L, new SnapshotBase(b.tag, b.bytes, 9, 'a' * 32, '0000000000000-run-x'), tmp, 'a' * 32, true).skipped == IndexSnapshot.REPLACED_MEANWHILE
        IndexSnapshot.write(index, 'lab', storage, 0L, new SnapshotBase(b.tag, b.bytes, 9, 'a' * 32, '0000000000000-run-x'), tmp, 'a' * 32, true).written
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
}
