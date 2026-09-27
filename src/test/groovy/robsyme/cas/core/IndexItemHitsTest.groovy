package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Decision 7 of the milestone 3 plan (ticket 05 Q4): nf-blocks:items matches a
 * condition's value as text against any type, every condition must match, and
 * a truncated row never matches. Each hit carries its collection.
 */
class IndexItemHitsTest extends Specification {

    static final String LONG = 'x' * 2000

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    Cid laneInt, laneString, depthFloat, pairedBool, lanesList, longNote, nestedKit
    Cid run1, aligned1, stats1

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        laneInt = item([sample: 'A', lane: 2L])
        laneString = item([sample: 'B', lane: '2'])
        depthFloat = item([sample: 'C', depth: 1.5d])
        pairedBool = item([sample: 'D', paired: true])
        lanesList = item([sample: 'E', lanes: [2L, 3L]])
        longNote = item([sample: 'F', note: LONG])
        nestedKit = item([sample: 'G', kit: [name: 'truseq']])
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        aligned1 = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned',
            [laneInt, laneString, depthFloat, pairedBool, lanesList, longNote, nestedKit].collect { Cid i -> [i, ["aligned/${i}".toString()]] }))
        stats1 = store.putDagCbor(Fixtures.outputCollection(manifest, 'stats', [[laneInt, ['stats/A.stats']]]))
        run1 = store.putDagCbor(Fixtures.runCompletion(manifest, [aligned1, stats1]))
        index.ingestRun(store, run1, 'lab')
    }

    def cleanup() {
        index?.close()
    }

    private Cid item(Map meta) {
        return store.putDagCbor(Fixtures.outputItem(
            [meta, Fixtures.leaf("${meta.sample}.bam".toString(), Fixtures.contentCid("bam-${meta.sample}".toString()), 1L)]))
    }

    private List<ItemHit> hits(Map<String, String> conditions) {
        return index.itemHitsByText(run1, 'aligned', conditions)
    }

    def 'a condition matches the value text whatever its type'() {
        expect:
        hits([lane: '2'])*.item == [laneInt, laneString].sort()
        hits([depth: '1.5'])*.item == [depthFloat]
        hits([paired: 'true'])*.item == [pairedBool]
        hits([lanes: '3'])*.item == [lanesList]
        hits(['kit.name': 'truseq'])*.item == [nestedKit]
        hits([lane: '9']) == []
    }

    def 'every condition must match'() {
        expect:
        hits([lane: '2', sample: 'B'])*.item == [laneString]
        hits([lane: '2', sample: 'C']) == []
    }

    def 'no condition is every item of the output, each hit carrying its collection'() {
        expect:
        hits([:]).size() == 7
        hits(null).size() == 7
        hits([:])*.collection.unique() == [aligned1]
        index.itemHitsByText(run1, 'stats', [:]) == [new ItemHit(stats1, laneInt)]
        index.itemHitsByText(run1, 'nothing', [:]) == []
    }

    def 'a truncated row is ignored, by its digest or by the long text'() {
        expect:
        hits([note: MetadataView.digestOf(LONG.getBytes('UTF-8'))]) == []
        hits([note: LONG]) == []
        hits([sample: 'F'])*.item == [longNote]
    }

    def 'the same item in another run is its own hit; listed twice in one collection, it is one hit'() {
        given:
        final Cid manifest2 = store.putDagCbor(Fixtures.runManifest(nf_run_hash: 'other', run_name: 'r2'))
        final Cid aligned2 = store.putDagCbor(Fixtures.outputCollection(manifest2, 'aligned',
            [[laneInt, ['aligned/A-1.bam']], [laneInt, ['aligned/A-2.bam']]]))
        final Cid run2 = store.putDagCbor(Fixtures.runCompletion(manifest2, [aligned2]))
        index.ingestRun(store, run2, 'lab')

        expect:
        index.itemHitsByText(run2, 'aligned', [lane: '2']) == [new ItemHit(aligned2, laneInt)]
        index.itemHitsByText(run2, 'aligned', [lane: '2'])[0].occurrence() == "cas://${aligned2}/${laneInt}".toString()
        hits([lane: '2'])*.collection == [aligned1, aligned1]
    }
}
