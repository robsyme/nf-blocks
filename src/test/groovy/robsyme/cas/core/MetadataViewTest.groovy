package robsyme.cas.core

import spock.lang.Specification

/**
 * DESIGN.md §12 and CONTEXT.md "Meta Map": a bare query key resolves against
 * the item's metadata view, which is the item itself when it is a map and
 * otherwise the first top-level map in the tuple. Every scalar leaf of that
 * view becomes an `item_attr` row under its dotted path.
 */
class MetadataViewTest extends Specification {

    static Map leaf(String name) {
        [kind: 'Leaf', name: name, address: null, size: null, provider: null, reason: 'declined']
    }

    def 'the view of a tuple is its first top-level map'() {
        given:
        def meta = [sample: 'A']

        expect:
        MetadataView.of([meta, leaf('a.bam')]).is(meta)
    }

    def 'the view of a map item is the item itself'() {
        given:
        def item = [sample: 'A', bam: leaf('a.bam')]

        expect:
        MetadataView.of(item).is(item)
    }

    def 'a Leaf map is never the view'() {
        expect:
        MetadataView.of(leaf('a.bam')) == null
        MetadataView.of([leaf('a.bam'), [sample: 'A']]) == [sample: 'A']
    }

    def 'an item with no map has no view'() {
        expect:
        MetadataView.of([leaf('a.bam'), leaf('b.bam')]) == null
        MetadataView.of('just a string') == null
        MetadataView.of(null) == null
    }

    def 'scalars flatten to typed rows under dotted paths'() {
        given:
        def view = [sample: 'A', lane: 1, single_end: false, nested: [kit: 'truseq', ids: [1, 2]]]

        when:
        def rows = MetadataView.flatten(view)

        then:
        rows.collect { "$it.path $it.type $it.value" } == [
            'sample string A',
            'lane int 1',
            'single_end bool false',
            'nested.kit string truseq',
            'nested.ids int 1',
            'nested.ids int 2',
        ]
        rows.every { it.truncated == 0 }
    }

    def 'a null value keeps its own type'() {
        when:
        def rows = MetadataView.flatten([sample: null])

        then:
        rows.size() == 1
        rows[0].path == 'sample'
        rows[0].type == 'null'
        rows[0].value == null
    }

    def 'a Double is a float and an integer is an int'() {
        when:
        def rows = MetadataView.flatten([coverage: 31.5d, lane: 2L])

        then:
        rows.collect { "$it.path $it.type $it.value" } == ['coverage float 31.5', 'lane int 2']
    }

    def 'a string longer than 1024 bytes is stored as its digest'() {
        given:
        def long_ = 'x' * 2000

        when:
        def rows = MetadataView.flatten([note: long_])

        then:
        rows.size() == 1
        rows[0].type == 'string'
        rows[0].truncated == 1
        rows[0].value == 'sha256:' + Hashing.sha256(long_.getBytes('UTF-8')).encodeHex().toString()
    }

    def 'a string of exactly 1024 bytes is kept whole'() {
        when:
        def rows = MetadataView.flatten([note: 'x' * 1024])

        then:
        rows[0].value == 'x' * 1024
        rows[0].truncated == 0
    }

    def 'Leaf maps are skipped, they are not metadata'() {
        when:
        def rows = MetadataView.flatten([sample: 'A', bam: leaf('a.bam'), bams: [leaf('a.bam'), leaf('b.bam')]])

        then:
        rows.collect { it.path } == ['sample']
    }

    def 'maps inside arrays flatten under the array path'() {
        when:
        def rows = MetadataView.flatten([reads: [[lane: 1], [lane: 2]]])

        then:
        rows.collect { "$it.path $it.type $it.value" } == ['reads.lane int 1', 'reads.lane int 2']
    }

    def 'a null view flattens to nothing'() {
        expect:
        MetadataView.flatten(null) == []
        MetadataView.flatten('not a map') == []
    }
}
