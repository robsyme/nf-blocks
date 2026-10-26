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
        page.itemsRange == Index.SQL_ITEMS_RANGE
        page.itemsNotEqual == Index.SQL_ITEMS_NOT_EQUAL
        page.itemsOrder == Index.SQL_ITEMS_ORDER
        page.collectionsOf == Index.SQL_COLLECTIONS_OF
    }

    def 'every page query runs against the real schema'() {
        expect:
        page.each { String name, Object sql ->
            if( name.startsWith('itemsPredicate') || name == 'itemsRange' || name == 'itemsNotEqual' || name == 'itemsOrder' )
                return
            plan((String) sql)     // throws on a syntax error or an unknown column
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
        'items, a range (>=)'     | page.itemsBase + page.itemsRange['>='] + page.itemsOrder
        'items, a range (<)'      | page.itemsBase + page.itemsPredicate + page.itemsRange['<'] + page.itemsOrder
        'items, not equal'        | page.itemsBase + page.itemsNotEqual + page.itemsOrder
        'items, = and !='         | page.itemsBase + page.itemsPredicate + page.itemsNotEqual + page.itemsOrder
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
        'searchSampleValues'      | page.searchSampleValues
        'searchSampleRows'        | page.searchSampleRows
        'searchRunCids'           | page.searchRunCids
        'searchCollectionCids'    | page.searchCollectionCids
        'searchItemCids'          | page.searchItemCids
        'searchContentCids'       | page.searchContentCids
        'runsOfPipelineBy'        | page.runsOfPipelineBy
        'runCountBy'              | page.runCountBy
        'selectionsPageBy'        | page.selectionsPageBy
        'selectionCountBy'        | page.selectionCountBy
        'transfersKnown'          | page.transfersKnown
        'transfersOf'             | page.transfersOf
        'projectsKnown'           | page.projectsKnown
        'projectByCid'            | page.projectByCid
        'projectStats'            | page.projectStats
        'projectStatsOf'          | page.projectStatsOf
        'projectMemberSubjects'   | page.projectMemberSubjects
        'projectMembershipSubjects' | page.projectMembershipSubjects
        'projectRemovedSubjects'  | page.projectRemovedSubjects
        'itemCollectionsOf'       | page.itemCollectionsOf
        'subjectKinds'            | page.subjectKinds
        'runInputsOf'             | page.runInputsOf
        'sharedInputRows'         | page.sharedInputRows
        'handoffsKnown'           | page.handoffsKnown
    }

    def 'shared input addresses are found from the run_input index alone'() {
        // The page's project suggestions read every task input address once; each row it
        // reads must come from the index, never a table lookup per row (a range over HTTP).
        expect:
        plan(page.sharedInputRows).any { String d -> d.startsWith('SEARCH run_input USING COVERING INDEX run_input_address') }
    }

    def "a storage query never scans block_ref, pinned_overlap, root or root_shared, and scans only small tables (#name)"() {
        given:
        final String sql = page[name]
        final Connection year = yearShaped()

        expect:
        for( Connection db : [connection, year] ) {
            final Set<String> scanned = scannedTables(db, sql)
            assert scanned.intersect(NEVER_SCANNED) == [] as Set
            assert (scanned - SMALL_TABLES - (allowed as Set)) == [] as Set
        }

        cleanup:
        year?.close()

        where:
        name              | allowed
        'storeStats'      | []
        'snapshotGrace'   | []
        'asserterStats'   | []
        // The Storage view lists every run, so it reads the run table once; root is looked up per run by root_cid.
        'storageRuns'     | ['run']
        'rootsOf'         | []
        'rootShared'      | []
        'pinnedOverlapOf' | []
        'kindStats'       | []
        'pipelineStats'   | []
        'collectionSize'  | []
        'itemSize'        | []
        'pinRoots'        | []
    }

    def 'pinRoots finds the pin roots through root_kind, on an empty and a year-shaped index'() {
        given:
        final Connection year = yearShaped()

        expect:
        for( Connection db : [connection, year] )
            assert plan(page.pinRoots, db).any { String d -> d.startsWith('SEARCH o USING INDEX root_kind (kind=?)') }

        cleanup:
        year?.close()
    }

    def 'the storage guard sees through aliases'() {
        expect:
        scannedTables(connection, 'SELECT o.cid FROM root o WHERE o.state = ?') == ['root'] as Set
        scannedTables(connection, 'SELECT x.cid FROM root_shared s JOIN root x ON x.id = s.other_root_id') == ['root_shared'] as Set
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
        given: 'the snapshot schema: the cache, less the tables only the cache keeps'
        final List<String> cacheOnly = Index.CACHE_ONLY_DDL.collect { String sql -> (sql =~ /CREATE TABLE IF NOT EXISTS (\w+)\(/)[0][1] as String }
        final def rs = connection.createStatement().executeQuery(
            "SELECT sql || ';', tbl_name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")
        final List<String> statements = []
        while( rs.next() )
            if( !cacheOnly.contains(rs.getString(2)) )
                statements << rs.getString(1)
        assert cacheOnly == ['block_absent', 'block_present']

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

    /** The tables a year-scale snapshot holds millions of rows of, or one row per root (P2 task 5). */
    static final Set<String> NEVER_SCANNED = ['block_ref', 'pinned_overlap', 'root', 'root_shared'] as Set

    /** One row, or one per asserter, pipeline or extension: well under a hundred rows in a year. */
    static final Set<String> SMALL_TABLES = ['store_stats', 'asserter_stats', 'pipeline_stats', 'kind_stats'] as Set

    static final Set<String> KEYWORDS = ['ON', 'WHERE', 'JOIN', 'CROSS', 'LEFT', 'INNER', 'GROUP', 'ORDER', 'LIMIT', 'USING', 'UNION'] as Set

    /** Row counts for a year of runs (gate/gen_year.py's 1825), so the planner chooses as it would on a real snapshot. */
    static final List<List<String>> YEAR_STATS = [
        ['run', 'sqlite_autoindex_run_1', '1825 1'],
        ['run', 'run_pipeline_status_finished_at', '1825 365 300 1'],
        ['root', 'root_cid', '4000 2'],
        ['root', 'root_kind', '4000 1333'],
        ['root_stats', null, '4000'],
        ['root_shared', 'root_shared_root', '20000 5'],
        ['block', 'sqlite_autoindex_block_1', '3000000 1'],
        ['block_ref', 'block_ref_owner', '5000000 4'],
        ['pinned_overlap', 'pinned_overlap_subject', '50000 2'],
        ['collection_item', 'collection_item_collection', '2000000 500'],
        ['collection_item', 'collection_item_item', '2000000 3'],
        ['collection_size', 'sqlite_autoindex_collection_size_1', '20000 1'],
        ['item_size', 'sqlite_autoindex_item_size_1', '1000000 1'],
        ['store_stats', null, '1'],
        ['asserter_stats', 'sqlite_autoindex_asserter_stats_1', '20 1'],
        ['pipeline_stats', 'sqlite_autoindex_pipeline_stats_1', '30 1'],
        ['kind_stats', 'sqlite_autoindex_kind_stats_1', '60 1'],
        ['meta', 'sqlite_autoindex_meta_1', '5 1'],
    ]

    static final List<String> ALLOWED_SCANS = [
        'SCAN run USING COVERING INDEX',
        'SCAN json_each',
        'SCAN CONSTANT ROW',
    ]

    /** An empty index whose sqlite_stat1 says it holds a year of runs. */
    private Connection yearShaped() {
        final Path file = tempDir.resolve('year.sqlite')
        Index.open(file).close()
        Connection c = DriverManager.getConnection("jdbc:sqlite:${file}")
        try {
            c.createStatement().execute('ANALYZE')
            final def insert = c.prepareStatement('INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES (?, ?, ?)')
            for( List<String> row : YEAR_STATS ) {
                insert.setString(1, row[0]); insert.setString(2, row[1]); insert.setString(3, row[2])
                insert.executeUpdate()
            }
        }
        finally {
            c.close()
        }
        // The planner reads sqlite_stat1 when a connection opens.
        DriverManager.getConnection("jdbc:sqlite:${file}")
    }

    /** The tables a plan SCANs, with each alias in the SQL read back to its table. */
    private Set<String> scannedTables(Connection db, String sql) {
        final Map<String, String> alias = [:]
        (sql =~ /(?i)\b(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?/).each { List<String> g ->
            alias[g[2] && !KEYWORDS.contains(g[2].toUpperCase()) ? g[2] : g[1]] = g[1]
        }
        plan(sql, db).findAll { String d -> d.startsWith('SCAN ') }
            .collect { String d -> final String name = d.split(' ')[1]; alias[name] ?: name } as Set
    }

    private List<String> plan(String sql, Connection db = connection) {
        // Numbered parameters (?1 used three times) bind once each.
        final List<Integer> numbered = (sql =~ /\?(\d+)/).collect { List<String> g -> g[1] as Integer }
        final int parameters = numbered ? numbered.max() : sql.count('?')
        final def statement = db.prepareStatement('EXPLAIN QUERY PLAN ' + sql)
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
