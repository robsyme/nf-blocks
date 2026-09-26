package robsyme.cas.core

import spock.lang.Specification

/** Spec section 7.1a and DESIGN.md §7: cas://<OutputCollection cid>/<OutputItem cid>[/<leaf name>]. */
class ItemOccurrenceTest extends Specification {

    static final String C = Fixtures.cidOf([kind: 'OutputCollection', n: 1]).toString()
    static final String I = Fixtures.cidOf([kind: 'OutputItem', n: 1]).toString()

    def 'parses with and without a leaf, and prints canonically'() {
        expect:
        ItemOccurrence.parse("cas://$C/$I").with { it.collection.toString() == C && it.item.toString() == I && it.leaf == null }
        ItemOccurrence.parse("cas://$C/$I/A.bam").leaf == 'A.bam'
        ItemOccurrence.parse("cas://$C/$I/A.bam").toString() == "cas://$C/$I/A.bam"
    }

    def 'anything else is not an occurrence (#text)'() {
        expect:
        ItemOccurrence.parse(text) == null

        where:
        text << ["cas://$C", "cas://$C/aligned/A", "cas://$C/$I/", "cas://$C/$I/a/b", "lid://$C/$I",
                 "cas://lab/$I", "cas://${Fixtures.contentCid('x')}/$I", '', null]
    }
}
