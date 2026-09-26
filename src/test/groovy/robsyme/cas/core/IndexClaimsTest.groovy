package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 8: Claims discovered through the Store Log, current state, query 2. */
class IndexClaimsTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    long clock = 1_758_000_000_000L

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, kind, cid, clock += 1000)
        return cid
    }

    private Cid claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = [],
                      String timestamp = '2026-09-25T10:00:00.000Z') {
        return logged(StoreLogKind.CLAIM, Fixtures.claim(subject, verb, attribute, value, supersedes, timestamp))
    }

    private Cid run(String name, String finishedAt) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-$name"))
        return logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [], [finished_at: finishedAt]))
    }

    private void catchUp() { index.catchUp(store, StoreLog.of(store), 'lab') }

    private List<List<Object>> rows(String sql) {
        final def c = DriverManager.getConnection("jdbc:sqlite:${index.file}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final int n = rs.metaData.columnCount
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..n).collect { int i -> rs.getObject(i) }
            return out
        }
        finally {
            c.close()
        }
    }

    def 'a logged Claim is indexed with its supersedes, and claim_current follows ClaimState'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Cid first = claim(subject, 'set', 'name', 'first')
        final Cid second = claim(subject, 'set', 'name', 'second', [first])

        when:
        catchUp()

        then:
        rows('SELECT claim_cid, subject_cid, verb, attribute, value FROM claim ORDER BY timestamp, claim_cid').size() == 2
        rows('SELECT claim_cid, superseded_cid FROM claim_supersedes') == [[second.toString(), first.toString()]]
        rows('SELECT subject_cid, attribute, value, claim_cid, conflicted FROM claim_current') ==
            [[subject.toString(), 'name', 'second', second.toString(), 0]]
        index.claimState(subject).names == ['second']
        index.isClaimIndexed(first)
    }

    def 'two names nothing supersedes are both current and flagged'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Cid a = claim(subject, 'set', 'name', 'alpha')
        final Cid b = claim(subject, 'set', 'name', 'bravo')

        when:
        catchUp()

        then:
        rows('SELECT claim_cid, conflicted FROM claim_current ORDER BY claim_cid') == [a, b].sort { it.toString() }.collect { [it.toString(), 1] }
        index.claimState(subject).nameConflicted
    }

    def 'a non-string value is stored as its DAG-JSON text (decision 4)'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        claim(subject, 'set', 'rating', [stars: 3L])
        claim(subject, 'set', 'label', 'plain')

        when:
        catchUp()

        then:
        rows('SELECT attribute, value FROM claim ORDER BY attribute') == [['label', 'plain'], ['rating', '{"stars":3}']]
        Index.valueText(null) == null
    }

    def 'a Claim whose block has not arrived is missing, then indexed when it lands'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final Map block = Fixtures.claim(subject, 'delete', null, null, [])
        final Cid absent = Fixtures.cidOf(block)
        StoreLog.append(store, StoreLogKind.CLAIM, absent, clock += 1000)

        when:
        catchUp()

        then:
        rows('SELECT have_cid, needed_cid FROM missing') == [[null, absent.toString()]]
        !index.isClaimIndexed(absent)

        when:
        store.putDagCbor(block)
        catchUp()

        then:
        index.isClaimIndexed(absent)
        rows('SELECT count(*) FROM missing') == [[0]]
        index.claimState(subject).hidden
    }

    def 'query 2 leaves out a run with a current delete Claim, and brings it back after an undo'() {
        given:
        final Cid older = run('older', '2026-09-01T10:00:00.000Z')
        final Cid newer = run('newer', '2026-09-02T10:00:00.000Z')
        final Cid delete = claim(newer, 'delete', null, null)
        catchUp()

        expect:
        index.latestSuccessfulRun('p') == Optional.of(older)

        when:
        claim(newer, 'del', null, null, [delete])
        catchUp()

        then:
        index.latestSuccessfulRun('p') == Optional.of(newer)
    }

    def 'query 2 leaves out a run whose deletion is conflicted too'() {
        given:
        final Cid older = run('older', '2026-09-01T10:00:00.000Z')
        final Cid newer = run('newer', '2026-09-02T10:00:00.000Z')
        final Cid delete = claim(newer, 'delete', null, null)
        claim(newer, 'del', null, null, [delete])
        // A distinct timestamp, so this is a genuinely different (content-addressed)
        // Claim from `delete` above, not the same block re-logged.
        claim(newer, 'delete', null, null, [], '2026-09-25T11:00:00.000Z')  // written without seeing the undo
        catchUp()

        expect:
        index.claimState(newer).deletion == ClaimState.CONFLICTED
        index.latestSuccessfulRun('p') == Optional.of(older)
    }

    def 'rebuild indexes Claims from the blocks'() {
        given:
        final Cid subject = Fixtures.cidOf([kind: 'Selection', schema: 1])
        claim(subject, 'set', 'name', 'kept')

        when:
        index.rebuild(store, 'lab')

        then:
        index.claimState(subject).names == ['kept']
    }
}
