package robsyme.cas.trace

import java.nio.file.Path

import robsyme.cas.CasSession
import robsyme.cas.core.Anomalies
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputItem
import robsyme.cas.core.StoreRef
import robsyme.cas.nio.CasFileSystemProvider
import spock.lang.Specification

/**
 * The join is exercised without a Nextflow Session: it is handed the captured
 * output values (whose file leaves are real {@code cas://} coordinate paths)
 * and a {@link CasSession} stub that answers {@code publishFor}. DESIGN.md §11.
 */
class JoinTest extends Specification {

    private CasFileSystemProvider provider = new CasFileSystemProvider()

    private Path coord(String uri) {
        return provider.getPath(URI.create(uri))
    }

    private static Cid rawCid(int fill) {
        final byte[] digest = new byte[32]
        Arrays.fill(digest, (byte) fill)
        return Cid.of(Cid.RAW, digest)
    }

    private static Cid dagCid(int fill) {
        final byte[] digest = new byte[32]
        Arrays.fill(digest, (byte) fill)
        return Cid.of(Cid.DAG_CBOR, digest)
    }

    private CasSession sessionWith(Map<String, CasSession.Publish> publishes,
                                   Cid run,
                                   Map<String, Anomalies> uploaded = [:]) {
        final CasSession session = Stub(CasSession)
        session.getAssertedBy() >> 'test'
        session.getRunManifest() >> run
        publishes.each { String key, CasSession.Publish p -> session.publishFor(key) >> p }
        session.publishFor(_) >> { String key -> publishes.get(key) }
        session.uploadAnomaliesFor(_) >> { String key -> uploaded.get(key) }
        return session
    }

    def 'joins a channel output into items with addressed leaf maps and aligned paths'() {
        given:
        final cidA = rawCid(1)
        final cidB = rawCid(2)
        final run = dagCid(9)
        final publishes = [
            'cas://lab/aligned/A/A.bam': new CasSession.Publish(new StoreRef(cidA, 'A.bam'), 100L, 'head-node'),
            'cas://lab/aligned/B/B.bam': new CasSession.Publish(new StoreRef(cidB, 'B.bam'), 200L, 'head-node'),
        ]
        final session = sessionWith(publishes, run)
        final captured = [
            aligned: [
                [[sample: 'A'], coord('cas://lab/aligned/A/A.bam')],
                [[sample: 'B'], coord('cas://lab/aligned/B/B.bam')],
            ],
        ] as Map<String, Object>

        when:
        final result = Join.join(captured, session)

        then: 'one output, two items, each a [meta, Leaf] tuple'
        result.outputs.size() == 1
        final out = result.outputs[0]
        out.name == 'aligned'
        out.items.size() == 2
        final leavesByName = out.items.collectMany { it.leaves() }.collectEntries { [(it.name): it] }
        leavesByName['A.bam'].address == cidA
        leavesByName['A.bam'].size == 100L
        leavesByName['A.bam'].reason == null
        leavesByName['B.bam'].address == cidB

        and: 'the item value mirrors the tuple with the path replaced by a Leaf'
        final itemA = out.items.find { (it.value as List)[0] == [sample: 'A'] }
        (itemA.value as List)[1] instanceof Leaf

        and: 'collection items are the OutputItem block cids, sorted, with paths aligned'
        out.collection.name == 'aligned'
        out.collection.run == run
        out.collection.assertedBy == 'test'
        final itemCids = out.items.collect { DagCbor.cidOf(DagCbor.encode(it.toCbor())) }
        out.collection.items == itemCids.toSorted()
        // paths[i] belongs to items[i]; find A's item position and check its path
        final cidItemA = DagCbor.cidOf(DagCbor.encode(itemA.toCbor()))
        final posA = out.collection.items.indexOf(cidItemA)
        out.collection.paths[posA] == ['aligned/A/A.bam']

        and: 'no anomalies'
        result.anomalies == Anomalies.NONE
    }

    def 'a null is a declined leaf and a path with no publish is never_published'() {
        given:
        final run = dagCid(9)
        final session = sessionWith([:], run)
        final captured = [
            misc: [
                [[sample: 'A'], null],
                [[sample: 'Z'], coord('cas://lab/misc/Z/Z.bam')],
            ],
        ] as Map<String, Object>

        when:
        final result = Join.join(captured, session)

        then:
        final leaves = result.outputs[0].items.collectMany { it.leaves() }
        final declined = leaves.find { it.reason == Leaf.DECLINED }
        declined != null
        declined.name == null
        declined.address == null
        final never = leaves.find { it.reason == Leaf.NEVER_PUBLISHED }
        never.name == 'Z.bam'
        never.address == null

        and:
        result.anomalies.declined == 1
        result.anomalies.neverPublished == 1
    }

    def 'identical bytes under three metas become three items with one content address'() {
        given:
        final stats = rawCid(7)
        final run = dagCid(9)
        final publishes = [
            'cas://lab/stats/A/A.stats': new CasSession.Publish(new StoreRef(stats, 'A.stats'), 10L, 'head-node'),
            'cas://lab/stats/B/B.stats': new CasSession.Publish(new StoreRef(stats, 'B.stats'), 10L, 'head-node'),
            'cas://lab/stats/C/C.stats': new CasSession.Publish(new StoreRef(stats, 'C.stats'), 10L, 'head-node'),
        ]
        final session = sessionWith(publishes, run)
        final captured = [
            stats: [
                [[sample: 'A'], coord('cas://lab/stats/A/A.stats')],
                [[sample: 'B'], coord('cas://lab/stats/B/B.stats')],
                [[sample: 'C'], coord('cas://lab/stats/C/C.stats')],
            ],
        ] as Map<String, Object>

        when:
        final result = Join.join(captured, session)

        then: 'three distinct items'
        final out = result.outputs[0]
        out.items.size() == 3
        out.collection.items.toSet().size() == 3

        and: 'but one content address across their leaves'
        out.items.collectMany { it.leaves() }*.address.toSet() == [stats].toSet()
    }

    def 'a directory leaf carries the manifest address and a null size, folding upload anomalies'() {
        given:
        final manifest = dagCid(5)
        final run = dagCid(9)
        final publishes = [
            'cas://lab/qc/A/A_qc': new CasSession.Publish(new StoreRef(manifest, 'A_qc'), 4242L, 'head-node'),
        ]
        final uploaded = ['cas://lab/qc/A/A_qc': Anomalies.unresolvable(1)]
        final session = sessionWith(publishes, run, uploaded)
        final captured = [
            qc: [[[sample: 'A'], coord('cas://lab/qc/A/A_qc')]],
        ] as Map<String, Object>

        when:
        final result = Join.join(captured, session)

        then:
        final leaf = result.outputs[0].items[0].leaves()[0]
        leaf.address == manifest
        leaf.size == null
        leaf.reason == null

        and: 'the directory publish anomaly is folded into the totals'
        result.anomalies.unresolvable == 1
    }

    def 'providers maps each provider to its leaf addresses'() {
        given:
        final cidA = rawCid(1)
        final cidB = rawCid(2)
        final dir = dagCid(3)
        final session = sessionWith([
            'cas://lab/aligned/A/A.bam': new CasSession.Publish(new StoreRef(cidA, 'A.bam'), 1L, 's3-copy'),
            'cas://lab/aligned/B/B.bam': new CasSession.Publish(new StoreRef(cidB, 'B.bam'), 1L, 'fusion-node'),
            'cas://lab/qc/A/A_qc'      : new CasSession.Publish(new StoreRef(dir, 'A_qc'), 1L, 'head-node'),
        ], dagCid(9))

        when:
        final result = Join.join([
            aligned: [[[sample: 'A'], coord('cas://lab/aligned/A/A.bam')], [[sample: 'B'], coord('cas://lab/aligned/B/B.bam')]],
            qc     : [[[sample: 'A'], coord('cas://lab/qc/A/A_qc')]],
        ] as Map<String, Object>, session)

        then:
        result.providers == ['fusion-node': [cidB], 'head-node': [dir], 's3-copy': [cidA]]
    }

    def 'the same bytes published by two providers in one run: one item address, the address under both (Review Focus 2)'() {
        given:
        final cid = rawCid(7)
        final session = sessionWith([
            'cas://lab/a/x.txt': new CasSession.Publish(new StoreRef(cid, 'x.txt'), 5L, 'head-node'),
            'cas://lab/b/x.txt': new CasSession.Publish(new StoreRef(cid, 'x.txt'), 5L, 's3-copy'),
        ], dagCid(9))

        when:
        final result = Join.join([
            a: [[[id: 1], coord('cas://lab/a/x.txt')]],
            b: [[[id: 1], coord('cas://lab/b/x.txt')]],
        ] as Map<String, Object>, session)

        then:
        result.outputs[0].collection.items == result.outputs[1].collection.items
        result.providers == ['head-node': [cid], 's3-copy': [cid]]
    }

    def 'providers also covers the files inside a published directory (ticket 16 decision 1)'() {
        given:
        final dir = dagCid(3)
        final inA = rawCid(4)
        final inB = rawCid(5)
        final session = sessionWith([
            'cas://lab/qc/A/A_qc': new CasSession.Publish(new StoreRef(dir, 'A_qc'), 1L, 'head-node',
                ['s3-copy': [inA], 'head-node': [inB]]),
        ], dagCid(9))

        when:
        final result = Join.join([qc: [[[sample: 'A'], coord('cas://lab/qc/A/A_qc')]]] as Map<String, Object>, session)

        then:
        result.providers == ['head-node': [dir, inB].sort { it.toString() }, 's3-copy': [inA]]
    }
}
