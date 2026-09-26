package robsyme.cas.core

import spock.lang.Specification

/** Block explorer spec section 7: identity, order and the rules of a Selection. */
class SelectionTest extends Specification {

    static final Cid I1 = Fixtures.cidOf(Fixtures.outputItem([[sample: 'A']]))
    static final Cid I2 = Fixtures.cidOf(Fixtures.outputItem([[sample: 'B']]))
    static final Cid C1 = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
    static final Cid C2 = Fixtures.cidOf([kind: 'OutputCollection', n: 2])
    static final Cid S1 = Fixtures.cidOf([kind: 'Selection', n: 1])

    private static byte[] bytesOf(Selection s) { DagCbor.encode(s.toCbor()) }

    def 'members sort by address, each via is sorted, order given is not content'() {
        given:
        final Selection one = new Selection('ada', [Selection.item(I2, [C2, C1]), Selection.item(I1, []), Selection.selection(S1)], [])
        final Selection two = new Selection('ada', [Selection.selection(S1), Selection.item(I1, []), Selection.item(I2, [C1, C2])], [])

        expect:
        bytesOf(one) == bytesOf(two)
        one.members*.address == [I1, I2, S1].sort { it.toString() }
        one.members.find { it.address == I2 }.via == [C1, C2].sort { it.toString() }
    }

    def 'the same item picked twice is one member with the via of both (Review Focus 2)'() {
        given:
        final Selection merged = new Selection('ada', [Selection.item(I1, [C1]), Selection.item(I1, [C2]), Selection.item(I1, [C1])], [])

        expect:
        merged.members.size() == 1
        merged.members[0].via == [C1, C2].sort { it.toString() }
        bytesOf(merged) == bytesOf(new Selection('ada', [Selection.item(I1, [C2, C1])], []))
    }

    def 'the block is the §6 map, members a keyed union, derived_from binary cids'() {
        when:
        final Map block = new Selection('ada', [Selection.item(I1, [C1]), Selection.selection(S1)], [S1]).toCbor()

        then:
        block.kind == 'Selection'
        block.schema == 1L
        block.asserted_by == 'ada'
        block.members == [[item: [address: I1, via: [C1]]], [selection: S1]].sort { Map m -> ((m.item as Map)?.address ?: m.selection).toString() }
        (block.derived_from as List).size() == 1
        (block.derived_from as List)[0] == S1.bytes()
    }

    def 'it round-trips through DAG-CBOR and matches the Fixtures map'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(I1, [C1])], [S1])
        final byte[] bytes = bytesOf(s)

        expect:
        Selection.fromCbor((Map) DagCbor.decode(bytes)) == s
        DagCbor.cidOf(bytes) == Fixtures.cidOf(Fixtures.selection([[item: [address: I1, via: [C1]]]], [S1]))
    }

    def 'identity has no timestamp: two authors, two blocks; one author, one block'() {
        expect:
        bytesOf(new Selection('ada', [Selection.item(I1, [])], [])) == bytesOf(new Selection('ada', [Selection.item(I1, [])], []))
        bytesOf(new Selection('ada', [Selection.item(I1, [])], [])) != bytesOf(new Selection('bob', [Selection.item(I1, [])], []))
    }

    def 'refused: empty, and an address that is both an item and a Selection'() {
        when:
        new Selection('ada', members, [])

        then:
        thrown(IllegalArgumentException)

        where:
        members << [[], [Selection.item(I1, []), Selection.selection(I1)]]
    }

    def 'a member map with both keys, or neither, does not decode'() {
        when:
        Selection.fromCbor([kind: 'Selection', schema: 1L, asserted_by: 'ada', members: [member], derived_from: []])

        then:
        thrown(IllegalArgumentException)

        where:
        member << [[item: [address: I1, via: []], selection: S1], [other: I1]]
    }

    def 'Cid.fromBytes inverts bytes()'() {
        expect:
        Cid.fromBytes(I1.bytes()) == I1
    }
}
