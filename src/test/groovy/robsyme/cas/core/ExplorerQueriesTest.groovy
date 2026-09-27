package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The page runs the plugin's own SQL (block explorer spec section 14: "no
 * second query engine to drift"), and every page query must be answerable
 * from an index on a year-scale snapshot fetched page by page over HTTP. A
 * SCAN of item_attr there is 2.7 million rows (DESIGN.md §15).
 */
class ExplorerQueriesTest extends Specification {

    @Shared Map<String, String> page = (Map<String, String>) new JsonSlurper()
        .parse(Path.of('web/src/queries.json').toFile())

    @TempDir
    Path tempDir

    Connection connection

    def setup() {
        final Path file = tempDir.resolve('index.sqlite')
        Index.open(file).close()
        connection = DriverManager.getConnection("jdbc:sqlite:${file}")
    }

    def cleanup() {
        connection?.close()
    }

    def 'the load-bearing queries are the plugin constants, character for character'() {
        expect:
        page.producersOf == Index.SQL_PRODUCERS_OF
        page.latestSuccessfulRun == Index.SQL_LATEST_SUCCESSFUL_RUN
        page.itemsBase == Index.SQL_ITEMS_BASE
        page.itemsPredicate == Index.SQL_ITEMS_PREDICATE
        page.itemsPredicateNull == Index.SQL_ITEMS_PREDICATE_NULL
        page.itemsOrder == Index.SQL_ITEMS_ORDER
        page.collectionsOf == Index.SQL_COLLECTIONS_OF
    }

    def 'every page query runs against the real schema'() {
        expect:
        page.each { String name, String sql ->
            if( name.startsWith('itemsPredicate') || name == 'itemsOrder' )
                return
            plan(sql)     // throws on a syntax error or an unknown column
        }
    }

    def "no page query scans a table, except run through a covering index (#name)"() {
        when:
        final List<String> details = plan(sql)
        final List<String> scans = details.findAll { String d -> d.startsWith('SCAN ') && !ALLOWED_SCANS.any { d.startsWith(it) } }

        then:
        scans == []

        where:
        name                      | sql
        'producersOf'             | page.producersOf
        'latestSuccessfulRun'     | page.latestSuccessfulRun
        'items, no predicate'     | page.itemsBase + page.itemsOrder
        'items, one predicate'    | page.itemsBase + page.itemsPredicate + page.itemsOrder
        'items, two predicates'   | page.itemsBase + page.itemsPredicate + page.itemsPredicateNull + page.itemsOrder
        'collectionsOf'           | page.collectionsOf
        'watermark'               | page.watermark
        'pipelines'               | page.pipelines
        'runsOfPipeline'          | page.runsOfPipeline
        'runByCompletion'         | page.runByCompletion
        'runsKnown'               | page.runsKnown
        'collectionByCid'         | page.collectionByCid
        'collectionItems'         | page.collectionItems
        'collectionAllItems'      | page.collectionAllItems
        'runCount'                | page.runCount
        'collectionItemCount'     | page.collectionItemCount
        'selectionsKnown'         | page.selectionsKnown
        'claimsKnown'             | page.claimsKnown
        'claimsOf'                | page.claimsOf
        'supersedesOf'            | page.supersedesOf
        'selectionsPage'          | page.selectionsPage
        'selectionCount'          | page.selectionCount
        'firstSeen'               | page.firstSeen
        'selectionsHolding'       | page.selectionsHolding
        'successfulRunsPage'      | page.successfulRunsPage
    }

    def 'the guard itself catches a scan'() {
        expect:
        plan('SELECT path, type, value FROM item_attr WHERE item_cid = ?').any { it.startsWith('SCAN item_attr') }
    }

    def 'Index.items still answers through the shared constants'() {
        given:
        final Path storeRoot = tempDir.resolve('store')
        final LocalBlockStore store = new LocalBlockStore(storeRoot, 'lab', true)
        final Index index = Index.open(tempDir.resolve('items.sqlite'))
        final Cid content = Fixtures.contentCid('bytes of A')
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', content, 10L)]))
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [:]))
        index.ingestRun(store, completion, 'lab')

        expect:
        index.items(completion, 'aligned', [sample: 'A']) == [item]
        index.items(completion, 'aligned', [sample: 'B']) == []
        index.producersOf(content)*.itemCid == [item]
        index.collectionsOf(completion) == [aligned: collection]

        cleanup:
        index.close()
    }

    def 'the page tests build their databases from the index schema itself'() {
        given:
        final def rs = connection.createStatement().executeQuery(
            "SELECT sql || ';' FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")
        final List<String> statements = []
        while( rs.next() )
            statements << rs.getString(1)

        expect:
        Path.of('web/test/fixtures/schema.sql').text.trim() == statements.join('\n').trim()
    }

    def 'the watermark is readable per member'() {
        given:
        final Index index = Index.open(tempDir.resolve('w.sqlite'))

        expect:
        index.watermark('lab') == null

        cleanup:
        index.close()
    }

    static final List<String> ALLOWED_SCANS = [
        'SCAN run USING COVERING INDEX',
        'SCAN json_each',
        'SCAN CONSTANT ROW',
    ]

    private List<String> plan(String sql) {
        final int parameters = sql.count('?')
        final def statement = connection.prepareStatement('EXPLAIN QUERY PLAN ' + sql)
        try {
            for( int i = 1; i <= parameters; i++ )
                statement.setObject(i, sql.contains('json_each') && i == 1 ? '["x"]' : 'x')
            final def rs = statement.executeQuery()
            final List<String> details = []
            while( rs.next() )
                details << rs.getString('detail')
            return details
        }
        finally {
            statement.close()
        }
    }
}
