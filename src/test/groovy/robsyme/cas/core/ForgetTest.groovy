package robsyme.cas.core

import spock.lang.Specification

class ForgetTest extends Specification {

    def 'deleted is sorted and unique, and identical sets give one block'() {
        given:
        final Cid x = Fixtures.cidOf([kind: 'X', n: 1])
        final Cid y = Fixtures.cidOf([kind: 'X', n: 2])
        final Forget f = new Forget('lab', [y, x, y])

        expect:
        f.deleted == [x, y].sort { it.toString() }
        f.toCbor() == [kind: 'Forget', schema: 1L, asserted_by: 'lab', deleted: f.deleted]
        Fixtures.cidOf(new Forget('lab', [x, y]).toCbor()) == Fixtures.cidOf(new Forget('lab', [y, x]).toCbor())
        Forget.fromCbor(f.toCbor()).deleted == f.deleted
    }

    def 'an empty Forget is refused, and so is another kind'() {
        when:
        new Forget('lab', [])

        then:
        thrown(IllegalArgumentException)

        when:
        Forget.fromCbor([kind: 'Claim', schema: 1L])

        then:
        thrown(IllegalArgumentException)
    }
}
