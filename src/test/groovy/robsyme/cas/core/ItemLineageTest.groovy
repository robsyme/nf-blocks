package robsyme.cas.core

import spock.lang.Specification

class ItemLineageTest extends Specification {

    def set = Fixtures.cidOf([kind: 'InputSet'])
    def intro = Fixtures.cidOf([kind: 'OutputItem', n: 'intros-Dev'])
    def circle = Fixtures.cidOf([kind: 'OutputItem', n: 'circle'])
    def introduce = new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', 'Introduce', 'Dev')
    def friend = new ItemLineage.TaskRef('fedcba9876543210fedcba9876543210', 'FriendCircle', null)

    def 'an ItemLineage writes the spec shape and reads back to itself'() {
        given:
        def l = new ItemLineage('rob', set, [ItemLineage.Derived.of(circle, [3, 0, 2, 1, 2], [friend]), ItemLineage.Derived.of(intro, [1], [introduce])])
        def cbor = l.toCbor()
        expect:
        cbor.kind == 'ItemLineage' && cbor.schema == 1 && cbor.asserted_by == 'rob' && cbor.input_set == set
        cbor.items*.item.collect { it.bytes() } == [circle, intro].collect { it.bytes() }.toSorted { a, b -> Arrays.compareUnsigned(a, b) }
        l.itemFor(circle).inputs == [0, 1, 2, 3]
        l.itemFor(intro).tasks == [introduce]
        cbor.items.find { it.item == intro }.tasks == [[hash: introduce.hash, process: 'Introduce', tag: 'Dev']]
        ItemLineage.fromCbor(cbor) == l
    }

    def 'an empty items list is a valid ItemLineage'() {
        expect:
        new ItemLineage('rob', set, []).toCbor().items == []
    }

    def 'refuses an item with neither input nor task, a repeated item, a bad hash'() {
        when: new ItemLineage('rob', set, [ItemLineage.Derived.of(intro, [], [])])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage('rob', set, [ItemLineage.Derived.of(intro, [0], []), ItemLineage.Derived.of(intro, [1], [])])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage.TaskRef('ABC', 'P', null)
        then: thrown(IllegalArgumentException)
    }

    def 'refuses a blank asserted_by, a null input set, a negative position, a null item'() {
        when: new ItemLineage('', set, [])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage('rob', null, [])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage('rob', set, [ItemLineage.Derived.of(intro, [-1], [])])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage('rob', set, [ItemLineage.Derived.of(null, [0], [])])
        then: thrown(IllegalArgumentException)
        when: new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', ' ', null)
        then: thrown(IllegalArgumentException)
    }

    def 'tasks sort by hash, process, then tag with nulls first, and repeats drop'() {
        given:
        def a = new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', 'P', 'x')
        def b = new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', 'P', null)
        def c = new ItemLineage.TaskRef('0123456789abcdef0123456789abcdef', 'A', 'z')
        expect:
        ItemLineage.Derived.of(intro, [0], [friend, a, b, c, a]).tasks == [c, b, a, friend]
    }

    def 'a lineage rejects a block of another kind or schema'() {
        when: ItemLineage.fromCbor([kind: 'InputSet', schema: 1L])
        then: thrown(IllegalArgumentException)
        when: ItemLineage.fromCbor([kind: 'ItemLineage', schema: 2L, asserted_by: 'rob', input_set: set, items: []])
        then: thrown(IllegalArgumentException)
    }

    def 'itemFor is null for an item the lineage does not name'() {
        expect:
        new ItemLineage('rob', set, []).itemFor(intro) == null
    }
}
