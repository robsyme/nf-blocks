package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** The index lookups `CopyClosure` makes (P3 plan 1.4): block sizes, join keys, an item's runs, and Claims per member. */
class IndexCopyQueriesTest extends Specification {

    @TempDir Path tmp

    LocalBlockStore lab
    LocalBlockStore other
    Index index
    long clock = 1_758_000_000_000L

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        other = new LocalBlockStore(tmp.resolve('other'), 'other', true)
        index = Index.open(tmp.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    private Cid logged(BlockStore store, StoreLogKind kind, Map block) {
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, kind, cid, clock += 1000)
        return cid
    }

    private void catchUp() {
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(other, StoreLog.of(other), 'other')
    }

    def 'blockSize: the block table row, null when the index has none'() {
        given:
        final Cid file = lab.putStreaming(new ByteArrayInputStream('hello'.bytes))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([f: Fixtures.leaf2('h.txt', file, 5)]))
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'o', [[item, ['h.txt']]]))
        logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(manifest, [collection]))
        catchUp()

        expect:
        index.blockSize(file) == 5L
        index.blockSize(Fixtures.contentCid('never')) == null
    }

    def 'attrsOf and runItemsWith: the join key of an item, and the items of a run that share it'() {
        given:
        final Cid a = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], n: 1L]))
        final Cid b = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], n: 2L]))
        final Cid c = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], n: 3L]))
        final Cid d = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 1L], n: 4L]))
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid one = lab.putDagCbor(Fixtures.outputCollection(manifest, 'one', [[a, []], [c, []]]))
        final Cid two = lab.putDagCbor(Fixtures.outputCollection(manifest, 'two', [[b, []], [d, []]]))
        final Cid run = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(manifest, [one, two]))
        final Cid otherManifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'x', nf_run_hash: 'x'))
        final Cid elsewhere = lab.putDagCbor(Fixtures.outputCollection(otherManifest, 'one', [[a, []]]))
        final Cid otherRun = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(otherManifest, [elsewhere]))
        catchUp()

        expect:
        index.attrsOf(a, 'meta.id') == [new AttrRow('meta.id', 'string', 's1', 0)]
        index.attrsOf(d, 'meta.id') == [new AttrRow('meta.id', 'int', '1', 0)]
        index.attrsOf(a, 'meta.nope') == []
        index.runItemsWith(run, 'meta.id', 'string', 's1') == [a, b].sort { it.toString() }
        index.runItemsWith(run, 'meta.id', 'string', '1') == []
        index.runItemsWith(run, 'meta.id', 'int', '1') == [d]
        index.runItemsWith(otherRun, 'meta.id', 'string', 's1') == [a]

        and: 'the output collections holding an item, with their runs'
        index.outputCollectionsOf(a) == [(one): run, (elsewhere): otherRun].sort { it.key.toString() }
        index.outputCollectionsOf(c) == [(one): run]
    }

    def 'claimsLoggedIn: only Claims whose log entry names the member, by subject, with their supersedes'() {
        given:
        final Cid subject = Fixtures.contentCid('subject')
        final Cid first = logged(lab, StoreLogKind.CLAIM, Fixtures.claim(subject, 'set', 'name', 'a', []))
        final Cid second = logged(lab, StoreLogKind.CLAIM, Fixtures.claim(subject, 'set', 'name', 'b', [first]))
        final Cid onlyOther = logged(other, StoreLogKind.CLAIM, Fixtures.claim(subject, 'del', 'name', null, [second]))
        catchUp()

        when:
        final Map<Cid, List<ClaimState.Row>> lab = index.claimsLoggedIn('lab')
        final Map<Cid, List<ClaimState.Row>> elsewhere = index.claimsLoggedIn('other')

        then:
        lab.keySet() == [subject] as Set
        lab[subject]*.cid as Set == [first, second]*.toString() as Set
        lab[subject].find { it.cid == second.toString() }.supersedes == [first.toString()]
        lab[subject].find { it.cid == second.toString() }.value == 'b'
        ClaimState.of(lab[subject]).current*.cid == [second.toString()]
        elsewhere[subject]*.cid == [onlyOther.toString()]

        and: 'one Claim as the index holds it'
        index.claimOf(second) == new Claim('ada', subject, 'set', 'name', 'b', [first], '2026-09-25T10:00:00.000Z')
        index.claimOf(Fixtures.contentCid('nothing')) == null
    }
}
