package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 11: Selections as Collections, nesting by recursive CTE. */
class IndexSelectionsTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    long clock = 1_758_000_000_000L

    Cid itemA, itemB, itemC, coll1, coll2

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        itemA = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        itemB = store.putDagCbor(Fixtures.outputItem([[sample: 'B']]))
        itemC = store.putDagCbor(Fixtures.outputItem([[sample: 'C']]))
        coll1 = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
        coll2 = Fixtures.cidOf([kind: 'OutputCollection', n: 2])
    }

    def cleanup() {
        index?.close()
    }

    private Cid logged(Selection s) {
        final Cid cid = store.putDagCbor(s.toCbor())
        StoreLog.append(store, StoreLogKind.SELECTION, cid, clock += 1000)
        return cid
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

    def 'a logged Selection is a collection of kind selection, one collection_item row per item and via'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(itemA, [coll1, coll2]), Selection.item(itemB, [])], [coll1])
        final Cid cid = logged(s)

        when:
        catchUp()

        then:
        rows("SELECT kind, completion_cid, output_name, asserted_by FROM collection WHERE collection_cid = '$cid'") ==
            [['selection', null, null, 'ada']]
        rows("SELECT item_cid, via_cid FROM collection_item WHERE collection_cid = '$cid' ORDER BY item_cid, via_cid") ==
            ([[itemA.toString(), coll1.toString()], [itemA.toString(), coll2.toString()]].sort { it[1] } + [[itemB.toString(), null]])
                .sort { a, b -> a[0] <=> b[0] ?: (a[1] ?: '') <=> (b[1] ?: '') }
        rows("SELECT derived_from_cid FROM selection_derived WHERE selection_cid = '$cid'") == [[coll1.toString()]]
        index.isSelectionIndexed(cid)
    }

    def 'nesting resolves through the CTE, each item once (spec section 10)'() {
        given:
        final Cid inner = logged(new Selection('ada', [Selection.item(itemA, [coll1]), Selection.item(itemB, [coll1])], []))
        final Cid outer = logged(new Selection('ada', [Selection.selection(inner), Selection.item(itemB, [coll2]), Selection.item(itemC, [])], []))
        catchUp()

        expect:
        rows("SELECT child_cid FROM selection_child WHERE parent_cid = '$outer'") == [[inner.toString()]]
        index.selectionItems(outer) == [itemA, itemB, itemC].sort { it.toString() }
        index.selectionItems(inner) == [itemA, itemB].sort { it.toString() }
    }

    def 'a nested Selection the index does not hold fails, naming it (Review Focus 5)'() {
        given:
        final Cid absent = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid outer = logged(new Selection('ada', [Selection.selection(absent), Selection.item(itemC, [])], []))
        catchUp()

        when:
        index.selectionItems(outer)

        then:
        final IllegalStateException e = thrown()
        e.message.contains(absent.toString())
    }

    def 'a Selection whose block has not arrived is missing, then indexed when it lands'() {
        given:
        final Selection s = new Selection('ada', [Selection.item(itemA, [])], [])
        final Cid cid = DagCbor.cidOf(DagCbor.encode(s.toCbor()))
        StoreLog.append(store, StoreLogKind.SELECTION, cid, clock += 1000)
        catchUp()

        expect:
        !index.isSelectionIndexed(cid)
        rows('SELECT needed_cid FROM missing') == [[cid.toString()]]

        when:
        store.putDagCbor(s.toCbor())
        catchUp()

        then:
        index.isSelectionIndexed(cid)
        index.selectionItems(cid) == [itemA]
    }

    def 'rebuild indexes Selections from the blocks'() {
        given:
        final Cid cid = logged(new Selection('ada', [Selection.item(itemA, [])], []))

        when:
        index.rebuild(store, 'lab')

        then:
        index.selectionItems(cid) == [itemA]
    }
}
