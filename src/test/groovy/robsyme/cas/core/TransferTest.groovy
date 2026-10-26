package robsyme.cas.core

import spock.lang.Specification

/** The Transfer block of the P3 plan, section 1.5 (with D8's `runs`). */
class TransferTest extends Specification {

    static final Cid A = Fixtures.cidOf([kind: 'Selection', n: 1])
    static final Cid B = Fixtures.cidOf([kind: 'Selection', n: 2])
    static final Cid R1 = Fixtures.cidOf([kind: 'RunCompletion', n: 1])
    static final Cid R2 = Fixtures.cidOf([kind: 'RunCompletion', n: 2])

    static Transfer transfer(Map overrides = [:]) {
        final Map a = [assertedBy: 'rob@lab', roots: [B, A], from: 'roblab', to: 'org-s3', depth: 'results',
                       claims: ['projects', 'names'], runs: [R2, R1], blocks: 15L, copied: 13L, bytes: 1186000L,
                       timestamp: '2026-10-24T10:00:00.000Z'] + overrides
        return new Transfer((String) a.assertedBy, (List<Cid>) a.roots, (String) a.from, (String) a.to, (String) a.depth,
            (List<String>) a.claims, (List<Cid>) a.runs, (long) a.blocks, (long) a.copied, (long) a.bytes, (String) a.timestamp)
    }

    def 'the block holds the 1.5 fields in snake_case, with roots, runs and claims sorted'() {
        given:
        final Transfer t = transfer()

        expect:
        t.toCbor() == [kind: 'Transfer', schema: 1L, asserted_by: 'rob@lab',
                       roots: [A, B].sort { it.toString() }, from: 'roblab', to: 'org-s3', depth: 'results',
                       claims: ['names', 'projects'], runs: [R1, R2].sort { it.toString() },
                       blocks: 15L, copied: 13L, bytes: 1186000L, timestamp: '2026-10-24T10:00:00.000Z']
        Records.TRANSFER == 'Transfer'
    }

    def 'a decoded block equals what was written, and the order given does not change the address'() {
        given:
        final Transfer t = transfer()
        final Map decoded = (Map) DagCbor.decode(DagCbor.encode(t.toCbor()))

        expect:
        Transfer.fromCbor(decoded) == t
        Fixtures.cidOf(Transfer.fromCbor(decoded).toCbor()) == Fixtures.cidOf(t.toCbor())
        Fixtures.cidOf(transfer(roots: [A, B], runs: [R1, R2], claims: ['names', 'projects']).toCbor()) == Fixtures.cidOf(t.toCbor())
    }

    def 'fromCbor sorts roots, runs and claims a writer left unsorted'() {
        given:
        final Map block = transfer().toCbor()
        block.roots = [B, A].sort { -it.toString().hashCode() }
        block.runs = [R2, R1]
        block.claims = ['projects', 'names']

        when:
        final Transfer t = Transfer.fromCbor(block)

        then:
        t.roots == [A, B].sort { it.toString() }
        t.runs == [R1, R2].sort { it.toString() }
        t.claims == ['names', 'projects']
    }

    def 'no runs and no claims are allowed: a dry copy of a bare Selection still names its root'() {
        expect:
        Transfer.fromCbor(transfer(runs: [], claims: []).toCbor()).runs == []
    }

    def 'the constructor refuses what the block may not hold'() {
        when:
        transfer(overrides)

        then:
        thrown(IllegalArgumentException)

        where:
        overrides << [
            [assertedBy: ''], [assertedBy: null], [roots: []], [roots: null], [from: ''], [to: null],
            [depth: ''], [timestamp: ''], [blocks: -1L], [copied: -1L], [bytes: -1L],
            // The index keeps the categories comma-joined: a blank one or one holding ',' would not read back.
            [claims: ['']], [claims: [' ']], [claims: ['names,pins']], [claims: ['names', null]],
        ]
    }

    def 'fromCbor refuses another kind, another schema and a wrong shape'() {
        when:
        Transfer.fromCbor(block)

        then:
        thrown(IllegalArgumentException)

        where:
        block << [
            null,
            [kind: 'Claim', schema: 1L],
            transfer().toCbor() + [schema: 2L],
            transfer().toCbor().findAll { k, v -> k != 'schema' },
            transfer().toCbor().findAll { k, v -> k != 'runs' },
            transfer().toCbor().findAll { k, v -> k != 'timestamp' },
            transfer().toCbor() + [roots: 'not a list'],
            transfer().toCbor() + [roots: ['not a cid']],
            transfer().toCbor() + [runs: [R1.toString()]],
            transfer().toCbor() + [claims: [1L]],
            transfer().toCbor() + [claims: 'names'],
            transfer().toCbor() + [claims: ['names,pins']],
            transfer().toCbor() + [depth: 3L],
            transfer().toCbor() + [blocks: '15'],
            transfer().toCbor() + [copied: null],
            transfer().toCbor() + [from: null],
        ]
    }
}
