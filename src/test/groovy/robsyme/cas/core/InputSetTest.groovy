package robsyme.cas.core

import spock.lang.Specification

/** InputSet schema 2: an input names the fromStore items it was read from. */
class InputSetTest extends Specification {

    def item1 = Fixtures.cidOf([kind: 'OutputItem', n: 1])
    def item2 = Fixtures.cidOf([kind: 'OutputItem', n: 2])
    def coll = Fixtures.cidOf([kind: 'OutputCollection'])
    def sel = Fixtures.cidOf([kind: 'Selection'])
    def dat = Fixtures.contentCid('Dev_Patel.dat')

    def 'an input with origins writes schema 2 and reads back to itself'() {
        given:
        def input = new InputSet.Input('task', 'Dev_Patel.dat', dat, 12L, 'Dev', null).withFrom([new InputSet.Origin(item1, coll, sel)])
        def set = new InputSet('rob', [input])
        expect:
        set.toCbor().schema == 2
        set.toCbor().inputs[0].from == [[item: item1, collection: coll, selection: sel]]
        InputSet.fromCbor(set.toCbor()) == set
    }

    def 'an input without origins has no from key'() {
        expect:
        !new InputSet('rob', [new InputSet.Input('task', 'a', dat, 1L, null, null)]).toCbor().inputs[0].containsKey('from')
        new InputSet.Input('task', 'a', dat, 1L, null, null).withFrom([]).from == null
    }

    def 'a schema 1 block still reads'() {
        given:
        def block = [kind: 'InputSet', schema: 1, asserted_by: 'rob', inputs: [[role: 'task', name: 'a', address: dat, size: 1, sample: null, reason: null]]]
        expect:
        InputSet.fromCbor(block).inputs[0].from == null
    }

    def 'origins sort by item, collection, selection as CID bytes, nulls first, without repeats'() {
        given:
        def raw = [new InputSet.Origin(item2, coll, sel), new InputSet.Origin(item1, coll, null), new InputSet.Origin(item1, null, sel), new InputSet.Origin(item1, coll, null)]
        def sorted = new InputSet.Input('task', 'a', dat, 1L, null, null).withFrom(raw).from
        expect:
        sorted.size() == 3
        sorted*.item.collect { it.bytes() } == sorted*.item.collect { it.bytes() }.toSorted { a, b -> Arrays.compareUnsigned(a, b) }
        sorted[0].collection == null
    }

    def 'two inputs differing only in from are two inputs, ordered by from'() {
        given:
        def a = new InputSet.Input('task', 'qc', null, null, null, 'unaddressed').withFrom([new InputSet.Origin(item1, coll, null)])
        def b = new InputSet.Input('task', 'qc', null, null, null, 'unaddressed').withFrom([new InputSet.Origin(item2, coll, null)])
        def set = new InputSet('rob', [b, a])
        expect:
        set.inputs.size() == 2
        set.positionOf(a) == (Arrays.compareUnsigned(item1.bytes(), item2.bytes()) < 0 ? 0 : 1)
        set.positionOf(new InputSet.Input('task', 'zz', dat, 1L, null, null)) == -1
    }
}
