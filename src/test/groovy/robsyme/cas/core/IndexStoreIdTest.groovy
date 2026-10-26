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

    // P3 plan T10: a member the cache holds nothing of gains its first id with
    // no reset, whether or not its blocks were scanned. Once a scan of it found
    // anything (block_scan 'done'), its rows are its run rows (run.member), its
    // log_entry rows and every Selection, Claim or Transfer no log announces. A
    // scan that found nothing ('empty') and rows a writer ingested itself count
    // for nothing: the writer created the id first (DESIGN.md §20 decision 2).

    private LocalBlockStore org() { new LocalBlockStore(tmp.resolve('org'), 'org', true) }

    private void scanOrg() {
        final LocalBlockStore org = org()
        index.catchUp(org, StoreLog.of(org), 'org')
        assert index.metaValue('block_scan:org') != null
    }

    def 'a member scanned while empty and without an id takes its first id with no reset: no run, log_entry or scanned row is attributed to it'() {
        given: 'lab caught up under its id, org scanned while it held nothing'
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        scanOrg()

        when: 'org gains an id (a first copy into it); its log has no watermark to check'
        final String reason = index.reconcileStoreIds([lab: A, org: B], logged(false))

        then:
        reason == null
        index.storeIdOf('org') == B
        index.storeIdOf('lab') == A
        index.countRows('run') == 1
        index.watermark('lab') != null
    }

    def 'an empty scan of a member is no row of it even when another member brought a Selection no log announces'() {
        given: 'lab holds a Selection with no Store Log entry, found by its own scan, and a logged run'
        store.putDagCbor(new Selection('ada', [Selection.item(Fixtures.contentCid('i'), [])], []).toCbor())
        addRun('r1')
        assert index.reconcileStoreIds([lab: A], logged(true)) == null
        assert index.countRows('collection') == 1
        scanOrg()

        expect:
        index.reconcileStoreIds([lab: A, org: B], logged(false)) == null
        index.storeIdOf('org') == B
        index.countRows('collection') == 1
    }

    def 'a run a writer ingested itself after an empty scan is not a row of the member: no reset at its first id'() {
        given: 'org scanned empty, then a run written there ingests itself before any catch-up'
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        scanOrg()
        final Cid m = org().putDagCbor(Fixtures.runManifest(run_name: 'o1', nf_run_hash: 'o1'))
        index.ingestRun(org(), org().putDagCbor(Fixtures.runCompletion(m, [])), 'org')
        assert index.countRows('run') == 2

        expect:
        index.reconcileStoreIds([lab: A, org: B], logged(false)) == null
        index.storeIdOf('org') == B
        index.countRows('run') == 2
    }

    def 'a cache whose scan of a member predates the empty mark takes its id with no reset when it holds no Selection, Claim or Transfer no log announces'() {
        given: 'org scanned empty, its flag as a build before T10 left it'
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        scanOrg()
        index.setMetaValue('block_scan:org', 'done')

        expect:
        index.reconcileStoreIds([lab: A, org: B], logged(false)) == null
        index.storeIdOf('org') == B
        index.countRows('run') == 1
    }

    def 'a member scanned without an id that holds a run (run.member) still resets when it gains an id with no watermark'() {
        given: 'a run in org with no Store Log entry, found by the scan'
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        final Cid m = org().putDagCbor(Fixtures.runManifest(run_name: 'o1', nf_run_hash: 'o1'))
        org().putDagCbor(Fixtures.runCompletion(m, []))
        scanOrg()
        assert index.countRows('run') == 2
        assert index.watermark('org') == null

        when:
        final String reason = index.reconcileStoreIds([lab: A, org: B], logged(false))

        then:
        reason != null && reason.contains("'org'")
        index.countRows('run') == 0
        index.storeIdOf('org') == B
    }

    def 'a member whose scan found only a #kind no log announces still resets when it gains an id'() {
        given:
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        org().putDagCbor(block)
        scanOrg()
        assert index.countRows(table) == 1

        expect:
        index.reconcileStoreIds([lab: A, org: B], logged(false)) != null
        index.countRows(table) == 0
        index.countRows('run') == 0

        where:
        kind        | table        | block
        'Selection' | 'collection' | new Selection('ada', [Selection.item(Fixtures.contentCid('i'), [])], []).toCbor()
        'Claim'     | 'claim'      | Fixtures.claim(Fixtures.contentCid('t'), 'add', 'pin', 'keep', [])
        'Transfer'  | 'transfer'   | new Transfer('rob@lab', [Fixtures.contentCid('r')], 'lab', 'org', 'all', [], [], 1L, 1L, 10L,
                                            '2026-10-24T10:00:00.000Z').toCbor()
    }

    def "a member whose scan said '#flag' and whose log catch-up stopped before a watermark still resets at its first id"() {
        given: 'org scanned, then its log catch-up recorded an entry and threw before setting a watermark'
        addRun('r1')
        index.reconcileStoreIds([lab: A], logged(true))
        scanOrg()
        index.setMetaValue('block_scan:org', flag)
        index.recordLogEntry(StoreLog.parse(StoreLog.entryName(StoreLogKind.RUN, Fixtures.contentCid('o'), 1_758_000_000_000L)), 'org')
        assert index.watermark('org') == null

        expect:
        index.reconcileStoreIds([lab: A, org: B], logged(false)) != null
        index.countRows('log_entry') == 0
        index.storeIdOf('org') == B

        where:
        flag << ['empty', 'done']
    }
}
