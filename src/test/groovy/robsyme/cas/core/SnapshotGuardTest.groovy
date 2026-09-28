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

        when: 'a cache that saw only an empty log'
        final IndexSnapshot.Result r = IndexSnapshot.write(fresh, 'lab', storage, 0L, storage.base(tmp), tmp)

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
        Files.list(store.root.resolve('index')).toList()*.fileName*.toString() == ['v3.sqlite']
        Files.list(tmp.resolve('t')).count() == 0
    }
}
