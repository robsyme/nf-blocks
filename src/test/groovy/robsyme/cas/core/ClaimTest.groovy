package robsyme.cas.core

import spock.lang.Specification

/** Block explorer spec section 8 and DESIGN.md §6: the Claim block. */
class ClaimTest extends Specification {

    static final Cid SUBJECT = Fixtures.cidOf([kind: 'Selection', schema: 1])
    static final Cid A = Fixtures.cidOf([kind: 'Claim', n: 1])
    static final Cid B = Fixtures.cidOf([kind: 'Claim', n: 2])

    def 'the block is the §6 map, supersedes sorted and deduplicated'() {
        given:
        final List<Cid> given = [B, A, B]

        when:
        final Map block = new Claim('ada', SUBJECT, 'set', 'name', 'tumour', given, '2026-09-25T10:00:00.000Z').toCbor()

        then:
        block == [kind: 'Claim', schema: 1L, asserted_by: 'ada', subject: SUBJECT, verb: 'set', attribute: 'name',
                  value: 'tumour', supersedes: [A, B].sort { it.toString() }, timestamp: '2026-09-25T10:00:00.000Z']
        block.keySet().toList() == ['kind', 'schema', 'asserted_by', 'subject', 'verb', 'attribute', 'value', 'supersedes', 'timestamp']
    }

    def 'it round-trips through DAG-CBOR to the same address'() {
        given:
        final Claim claim = new Claim('ada', SUBJECT, 'delete', null, null, [], '2026-09-25T10:00:00.000Z')
        final byte[] bytes = DagCbor.encode(claim.toCbor())

        expect:
        Claim.fromCbor((Map) DagCbor.decode(bytes)) == claim
        DagCbor.encode(Claim.fromCbor((Map) DagCbor.decode(bytes)).toCbor()) == bytes
        Fixtures.cidOf(Fixtures.claim(SUBJECT, 'delete', null, null, [])) == DagCbor.cidOf(bytes)
    }

    def 'an unknown verb, a missing subject or no timestamp is refused'() {
        when:
        new Claim('ada', subject, verb, null, null, [], timestamp)

        then:
        thrown(IllegalArgumentException)

        where:
        subject | verb     | timestamp
        SUBJECT | 'rename' | '2026-09-25T10:00:00.000Z'
        null    | 'delete' | '2026-09-25T10:00:00.000Z'
        SUBJECT | 'delete' | ''
    }
}
