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
