package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.stream.Stream

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §12. The counts are asserted through a second connection so the
 * table and column names an `sqlite3` user would type are what is verified.
 */
class IndexTest extends Specification {

    @TempDir
    Path tempDir

    Path storeRoot
    LocalBlockStore store
    Path dbFile
    Index index

    Cid manifestCid
    Cid completionCid
    Cid alignedCollection
    Cid statsCollection
    Cid statsContent
    Map<String, Cid> alignedBySample = [:]
    Map<String, Cid> statsBySample = [:]

    def setup() {
        storeRoot = tempDir.resolve('store')
        store = new LocalBlockStore(storeRoot, 'lab', true)
        dbFile = tempDir.resolve('cache').resolve('index.sqlite')
        index = Index.open(dbFile)
    }

    def cleanup() {
        index?.close()
    }

    // ------------------------------------------------------------ fixtures

    /** The run of the plan: pipeline `p`, `aligned` and `stats`, three samples each. */
    private Cid buildRun(Map completionOverrides = [:], Map manifestOverrides = [:]) {
        manifestCid = store.putDagCbor(Fixtures.runManifest(manifestOverrides))
        statsContent = Fixtures.contentCid('one set of stats for every sample')

        final List<List> aligned = []
        final List<List> stats = []
        ['A', 'B', 'C'].eachWithIndex { String sample, int i ->
            final Map meta = [sample: sample, lane: i + 1]

            final Cid alignedItem = store.putDagCbor(Fixtures.outputItem(
                [meta, Fixtures.leaf("${sample}.bam".toString(), Fixtures.contentCid("bam-$sample"), 100L)]))
            alignedBySample[sample] = alignedItem
            aligned << [alignedItem, ["aligned/${sample}.bam".toString()]]

            final Cid statsItem = store.putDagCbor(Fixtures.outputItem(
                [meta, Fixtures.leaf("${sample}.stats".toString(), statsContent, 42L)]))
            statsBySample[sample] = statsItem
            stats << [statsItem, ["stats/${sample}.stats".toString()]]
        }

        alignedCollection = store.putDagCbor(Fixtures.outputCollection(manifestCid, 'aligned', aligned))
        statsCollection = store.putDagCbor(Fixtures.outputCollection(manifestCid, 'stats', stats))
        // §6: collections sorted by output name.
        completionCid = store.putDagCbor(
            Fixtures.runCompletion(manifestCid, [alignedCollection, statsCollection], completionOverrides))
        return completionCid
    }

    /** A second run of the same pipeline, distinguished by its run name. */
    private Cid buildOtherRun(Map completionOverrides, String runName) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: runName, nf_run_hash: "hash-$runName"))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', []))
        return store.putDagCbor(Fixtures.runCompletion(manifest, [collection], completionOverrides))
    }

    // ------------------------------------------------------------ probing

    private Object scalar(String sql) {
        final def connection = DriverManager.getConnection("jdbc:sqlite:${dbFile}".toString())
        try {
            final def statement = connection.createStatement()
            try {
                final def rs = statement.executeQuery(sql)
                return rs.next() ? rs.getObject(1) : null
            }
            finally { statement.close() }
        }
        finally { connection.close() }
    }

    private void eachRow(String sql, Closure consume) {
        final def connection = DriverManager.getConnection("jdbc:sqlite:${dbFile}".toString())
        try {
            final def statement = connection.createStatement()
            try {
                final def rs = statement.executeQuery(sql)
                while( rs.next() )
                    consume.call(rs)
            }
            finally { statement.close() }
        }
        finally { connection.close() }
    }

    private long count(String table) {
        return ((Number) scalar("SELECT COUNT(*) FROM $table")).longValue()
    }

    private Map<String, Long> counts() {
        return ['run', 'collection', 'item', 'collection_item', 'producer', 'item_attr', 'missing']
            .collectEntries { [(it): count(it)] } as Map<String, Long>
    }

    // ------------------------------------------------------------ the tests

    /** The schema of DESIGN §12, table by table, exactly as an `sqlite3` user would type it. */
    static final Map<String, List<String>> SCHEMA = [
        schema_version : ['version'],
        run            : ['completion_cid', 'manifest_cid', 'pipeline', 'revision', 'commit_id',
                          'nf_run_hash', 'session_id', 'run_name', 'asserted_by',
                          'status', 'possibly_incomplete', 'finished_at', 'member'],
        collection     : ['collection_cid', 'completion_cid', 'output_name'],
        item           : ['item_cid'],
        collection_item: ['collection_cid', 'item_cid'],
        producer       : ['content_cid', 'item_cid', 'collection_cid', 'completion_cid', 'filename'],
        consumer       : ['content_cid', 'completion_cid', 'name', 'how'],
        item_attr      : ['item_cid', 'path', 'type', 'value', 'truncated'],
        claim_current  : ['subject_cid', 'attribute', 'value', 'claim_cid', 'conflicted'],
        missing        : ['have_cid', 'needed_cid'],
        nf_record      : ['key', 'kind', 'workflow_run', 'task_run', 'labels_json', 'block_cid'],
    ]

    def 'open creates the schema of DESIGN 12 in WAL mode'() {
        expect: 'WAL, so a run writing rows does not block a reader'
        scalar("PRAGMA journal_mode") == 'wal'
        scalar("SELECT version FROM schema_version") == Index.SCHEMA_VERSION

        and: 'every table with exactly the specified columns'
        SCHEMA.every { String table, List<String> columns ->
            assert scalar("SELECT COUNT(*) FROM $table WHERE " + columns.collect { "$it IS NULL" }.join(' AND ')) == 0
            assert columnsOf(table) == columns.toSet()
            true
        }

        and: 'the composite indexes the three queries lean on'
        indexedColumns('run').contains(['pipeline', 'status', 'finished_at'])
        indexedColumns('run').contains(['nf_run_hash'])
        indexedColumns('run').contains(['manifest_cid'])
        indexedColumns('producer').contains(['content_cid'])
        indexedColumns('item_attr').contains(['path', 'type', 'value'])
    }

    private Set<String> columnsOf(String table) {
        final Set<String> names = new HashSet<String>()
        eachRow("PRAGMA table_info('$table')") { rs -> names.add(rs.getString('name')) }
        return names
    }

    private List<List<String>> indexedColumns(String table) {
        final List<String> indexes = []
        eachRow("PRAGMA index_list('$table')") { rs -> indexes.add(rs.getString('name')) }
        return indexes.collect { String name ->
            final List<String> columns = []
            eachRow("PRAGMA index_info('$name')") { rs -> columns.add(rs.getString('name')) }
            return columns
        }
    }

    def 'ingestRun writes every table of the run'() {
        given:
        def completion = buildRun()

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        counts() == [run: 1L, collection: 2L, item: 6L, collection_item: 6L,
                     producer: 6L, item_attr: 12L, missing: 0L]

        and: 'the run row carries what the manifest and the completion assert'
        scalar("SELECT pipeline FROM run") == 'p'
        scalar("SELECT manifest_cid FROM run") == manifestCid.toString()
        scalar("SELECT nf_run_hash FROM run") == 'aa11bb22'
        scalar("SELECT session_id FROM run") == 'sess-1'
        scalar("SELECT run_name FROM run") == 'grave_curie'
        scalar("SELECT revision FROM run") == 'main'
        scalar("SELECT commit_id FROM run") == 'c0ffee'
        scalar("SELECT asserted_by FROM run") == 'ada'
        scalar("SELECT status FROM run") == 'succeeded'
        scalar("SELECT possibly_incomplete FROM run") == 0
        scalar("SELECT finished_at FROM run") == '2026-09-03T10:05:00.000Z'
        scalar("SELECT member FROM run") == 'lab'

        and: 'the collections are named by their output name'
        scalar("SELECT output_name FROM collection WHERE collection_cid = '$alignedCollection'") == 'aligned'
        scalar("SELECT output_name FROM collection WHERE collection_cid = '$statsCollection'") == 'stats'
    }

    def 'producersOf answers with every item that carries the content'() {
        given:
        index.ingestRun(store, buildRun(), 'lab')

        when:
        def rows = index.producersOf(statsContent)

        then:
        rows.size() == 3
        rows*.itemCid.toSet().size() == 3
        rows*.filename.sort() == ['A.stats', 'B.stats', 'C.stats']
        rows.every { it.completionCid == completionCid && it.collectionCid == statsCollection }
        rows.every { it.contentCid == statsContent }

        and: 'a content address with one producer has one row'
        index.producersOf(Fixtures.contentCid('bam-A')).size() == 1

        and: 'an address nothing produced has none'
        index.producersOf(Fixtures.contentCid('never seen')) == []
    }

    def 'latestSuccessfulRun skips a failed run and a possibly incomplete one'() {
        given:
        def succeeded = buildRun()
        def failed = buildOtherRun([status: 'failed', exit_status: 1, possibly_incomplete: true,
                                    finished_at: '2026-09-03T11:00:00.000Z'], 'failed_run')
        def incomplete = buildOtherRun([status: 'succeeded', possibly_incomplete: true,
                                        finished_at: '2026-09-03T12:00:00.000Z'], 'incomplete_run')

        when:
        [succeeded, failed, incomplete].each { index.ingestRun(store, it, 'lab') }

        then:
        count('run') == 3
        index.latestSuccessfulRun('p') == Optional.of(succeeded)

        and: 'a pipeline nobody ran has no latest run'
        index.latestSuccessfulRun('other') == Optional.empty()

        when: 'a later run of the same pipeline succeeds'
        def later = buildOtherRun([finished_at: '2026-09-03T13:00:00.000Z'], 'later_run')
        index.ingestRun(store, later, 'lab')

        then:
        index.latestSuccessfulRun('p') == Optional.of(later)
    }

    def 'a tie on the finish time breaks by address'() {
        given:
        def one = buildOtherRun([finished_at: '2026-09-03T10:05:00.000Z'], 'one')
        def two = buildOtherRun([finished_at: '2026-09-03T10:05:00.000Z'], 'two')
        [one, two].each { index.ingestRun(store, it, 'lab') }

        expect:
        index.latestSuccessfulRun('p') == Optional.of([one, two].min { it.toString() })
    }

    def 'items filters a collection by the metadata view'() {
        given:
        def completion = buildRun()
        index.ingestRun(store, completion, 'lab')

        expect: 'one key'
        index.items(completion, 'aligned', [sample: 'B']) == [alignedBySample['B']]

        and: 'several keys intersect'
        index.items(completion, 'aligned', [sample: 'B', lane: 2]) == [alignedBySample['B']]

        and: 'a key that does not match removes the row'
        index.items(completion, 'aligned', [sample: 'B', lane: 99]) == []

        and: 'no predicate is every item of the collection'
        index.items(completion, 'aligned', null).toSet() == alignedBySample.values().toSet()
        index.items(completion, 'aligned', [:]).size() == 3

        and: 'the predicate is scoped to the named output'
        index.items(completion, 'stats', [sample: 'B']) == [statsBySample['B']]

        and: 'an output the run does not have is empty'
        index.items(completion, 'nothing', [:]) == []
    }

    def 'a predicate value of the wrong type does not match'() {
        given:
        def completion = buildRun()
        index.ingestRun(store, completion, 'lab')

        expect:
        index.items(completion, 'aligned', [lane: '2']) == []
        index.items(completion, 'aligned', [lane: 2]).size() == 1
    }

    def 'a run is reachable by nextflow hash, by manifest and by its collections'() {
        given:
        def completion = buildRun()
        index.ingestRun(store, completion, 'lab')

        expect:
        index.runByNextflowHash('aa11bb22') == Optional.of(completion)
        index.runByNextflowHash('nothing') == Optional.empty()
        index.runByManifest(manifestCid) == Optional.of(completion)
        index.runByManifest(Fixtures.contentCid('not a manifest')) == Optional.empty()
        index.collectionsOf(completion) == [aligned: alignedCollection, stats: statsCollection]
        index.collectionsOf(Fixtures.contentCid('not a run')) == [:]
    }

    def 'ingesting the same run twice changes nothing'() {
        given:
        def completion = buildRun()
        index.ingestRun(store, completion, 'lab')
        def first = counts()

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        counts() == first
        index.producersOf(statsContent).size() == 3
        index.items(completion, 'aligned', [sample: 'B']).size() == 1
    }

    def 'rebuild from blocks alone reproduces the incremental ingest'() {
        given: 'a store holding two runs and a raw block that is not metadata'
        def completion = buildRun()
        def other = buildOtherRun([finished_at: '2026-09-03T11:00:00.000Z'], 'other')
        store.putStreaming(new ByteArrayInputStream('some published bytes'.getBytes('UTF-8')))
        index.ingestRun(store, completion, 'lab')
        index.ingestRun(store, other, 'lab')
        def incremental = counts()

        when: 'the index is thrown away and rebuilt from the store'
        index.close()
        Files.delete(dbFile)
        index = Index.open(dbFile)
        index.rebuild(store, 'lab')

        then:
        counts() == incremental
        index.latestSuccessfulRun('p') == Optional.of(other)
        index.producersOf(statsContent).size() == 3

        and: 'the rebuilt file is the one the index still writes to'
        index.file == dbFile
        Files.isRegularFile(dbFile)
    }

    def 'rebuild replaces what was there'() {
        given:
        def completion = buildRun()
        index.ingestRun(store, completion, 'lab')
        index.markStale()

        when: 'the blocks of that run go away and the index is rebuilt'
        def emptyStore = new LocalBlockStore(tempDir.resolve('empty'), 'empty', true)
        index.rebuild(emptyStore, 'lab')

        then:
        count('run') == 0
        !index.isStale()
    }

    def 'a stale index says so until it is rebuilt'() {
        expect:
        !index.isStale()

        when:
        index.markStale()

        then:
        index.isStale()

        and: 'the mark survives a close'
        index.close()
        (index = Index.open(dbFile)).isStale()
    }

    def 'a schema version mismatch recreates the index'() {
        given:
        index.ingestRun(store, buildRun(), 'lab')
        index.close()

        when: 'something wrote a version this build does not know'
        def connection = DriverManager.getConnection("jdbc:sqlite:${dbFile}".toString())
        connection.createStatement().executeUpdate("UPDATE schema_version SET version = 999")
        connection.close()
        index = Index.open(dbFile)

        then:
        scalar("SELECT version FROM schema_version") == Index.SCHEMA_VERSION
        count('run') == 0
    }

    def 'a file that is not a database is recreated'() {
        given:
        index.close()
        Files.write(dbFile, 'not a database at all'.getBytes('UTF-8'))

        when:
        index = Index.open(dbFile)

        then:
        scalar("SELECT version FROM schema_version") == Index.SCHEMA_VERSION
        count('run') == 0
    }

    def 'a referenced block that has not arrived leaves a missing row'() {
        given: 'a completion naming a collection whose block was never stored'
        manifestCid = store.putDagCbor(Fixtures.runManifest())
        def absent = Fixtures.cidOf(Fixtures.outputCollection(manifestCid, 'aligned', []))
        def present = store.putDagCbor(Fixtures.outputCollection(manifestCid, 'stats', []))
        def completion = store.putDagCbor(Fixtures.runCompletion(manifestCid, [absent, present]))

        when:
        index.ingestRun(store, completion, 'lab')

        then: 'what arrived is indexed and the gap is recorded'
        count('run') == 1
        count('collection') == 1
        count('missing') == 1
        scalar("SELECT have_cid FROM missing") == completion.toString()
        scalar("SELECT needed_cid FROM missing") == absent.toString()

        and: 're-ingesting does not double the gap'
        index.ingestRun(store, completion, 'lab')
        count('missing') == 1

        and: 'and once the block arrives the gap closes'
        store.putDagCbor(Fixtures.outputCollection(manifestCid, 'aligned', []))
        index.ingestRun(store, completion, 'lab')
        count('missing') == 0
        count('collection') == 2
    }

    def 'a run completion that has not arrived is recorded rather than rejected'() {
        given: 'a completion known by address only, as a partial sync leaves it'
        manifestCid = store.putDagCbor(Fixtures.runManifest())
        def absent = Fixtures.cidOf(Fixtures.runCompletion(manifestCid, []))

        when:
        index.ingestRun(store, absent, 'lab')

        then:
        count('run') == 0
        count('missing') == 1
        scalar("SELECT have_cid FROM missing") == null
        scalar("SELECT needed_cid FROM missing") == absent.toString()

        and: 'once it arrives the run is indexed and the gap closes'
        store.putDagCbor(Fixtures.runCompletion(manifestCid, []))
        index.ingestRun(store, absent, 'lab')
        count('run') == 1
        count('missing') == 0
    }

    def 'an item whose block has not arrived is still known to be in the collection'() {
        given:
        manifestCid = store.putDagCbor(Fixtures.runManifest())
        def absentItem = Fixtures.cidOf(Fixtures.outputItem([[sample: 'Z'], Fixtures.unaddressedLeaf('z.bam')]))
        def collection = store.putDagCbor(
            Fixtures.outputCollection(manifestCid, 'aligned', [[absentItem, ['aligned/z.bam']]]))
        def completion = store.putDagCbor(Fixtures.runCompletion(manifestCid, [collection]))

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        count('item') == 1
        count('collection_item') == 1
        count('item_attr') == 0
        count('producer') == 0
        scalar("SELECT have_cid FROM missing") == collection.toString()
        scalar("SELECT needed_cid FROM missing") == absentItem.toString()
    }

    def 'an unaddressed leaf contributes no producer row'() {
        given:
        manifestCid = store.putDagCbor(Fixtures.runManifest())
        def item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.unaddressedLeaf('A.bam')]))
        def collection = store.putDagCbor(
            Fixtures.outputCollection(manifestCid, 'aligned', [[item, [null]]]))
        def completion = store.putDagCbor(Fixtures.runCompletion(manifestCid, [collection]))

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        count('item') == 1
        count('item_attr') == 1
        count('producer') == 0
    }

    def 'a null item in a collection is a hole, not a row'() {
        given:
        manifestCid = store.putDagCbor(Fixtures.runManifest())
        def item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.unaddressedLeaf('A.bam')]))
        def collection = store.putDagCbor([kind: 'OutputCollection', schema: 1, asserted_by: 'ada',
                                           run: manifestCid, name: 'aligned',
                                           items: [null, item], paths: [null, [null]]])
        def completion = store.putDagCbor(Fixtures.runCompletion(manifestCid, [collection]))

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        count('item') == 1
        count('collection_item') == 1
    }

    def 'catchUp ingests only the run log entries past the watermark'() {
        given:
        def first = buildRun()
        RunLog.append(store, first, 1_000_000L)
        def counting = new Counting(store)
        def log = RunLog.of(store)

        when:
        index.catchUp(counting, log, 'lab')

        then:
        count('run') == 1
        counting.opens > 0

        when: 'a second run is logged'
        def second = buildOtherRun([finished_at: '2026-09-03T11:00:00.000Z'], 'second')
        RunLog.append(store, second, 2_000_000L)
        counting.opens = 0
        counting.opened.clear()
        index.catchUp(counting, log, 'lab')

        then: 'only the new run is read'
        count('run') == 2
        !counting.opened.contains(first)
        counting.opened.contains(second)

        when: 'nothing new has been logged'
        counting.opens = 0
        index.catchUp(counting, log, 'lab')

        then:
        counting.opens == 0
        count('run') == 2
    }

    def 'two threads ingesting concurrently against one index file both succeed'() {
        given: 'two runs in the store and a second connection on the same index file'
        final run1 = buildRun()
        final run2 = buildOtherRun([finished_at: '2026-09-03T11:00:00.000Z'], 'other')
        final index2 = Index.open(dbFile)

        when: 'each connection ingests one run on its own thread'
        final errors = Collections.synchronizedList(new ArrayList())
        final t1 = Thread.start { try { index.ingestRun(store, run1, 'lab') } catch( Throwable e ) { errors << e } }
        final t2 = Thread.start { try { index2.ingestRun(store, run2, 'lab') } catch( Throwable e ) { errors << e } }
        t1.join()
        t2.join()
        index2.close()

        then: 'busy_timeout made the second writer wait rather than fail'
        errors.isEmpty()
        count('run') == 2
    }

    def 'rebuild carries the run log watermark forward so catchUp does not re-scan'() {
        given: 'a logged, ingested run'
        final completion = buildRun()
        RunLog.append(store, completion, 1_000_000L)
        index.ingestRun(store, completion, 'lab')

        when: 'the index is rebuilt from the store'
        index.rebuild(store, 'lab')

        and: 'catchUp runs against a store that records every read'
        final counting = new Counting(store)
        index.catchUp(counting, RunLog.of(store), 'lab')

        then: 'the already-logged run is behind the carried watermark, so nothing is re-read'
        counting.opens == 0
        count('run') == 1
    }

    /** Records which blocks the index actually reads. */
    static class Counting implements BlockStore {
        final BlockStore delegate
        int opens = 0
        final Set<Cid> opened = new HashSet<Cid>()

        Counting(BlockStore delegate) { this.delegate = delegate }

        @Override String alias() { delegate.alias() }
        @Override boolean has(Cid cid) { delegate.has(cid) }
        @Override long size(Cid cid) { delegate.size(cid) }
        @Override InputStream open(Cid cid) { opens++; opened.add(cid); delegate.open(cid) }
        @Override long lastModifiedMillis(Cid cid) { delegate.lastModifiedMillis(cid) }
        @Override void put(Cid cid, InputStream input, long expectedSize) { delegate.put(cid, input, expectedSize) }
        @Override Cid putStreaming(InputStream input) { delegate.putStreaming(input) }
        @Override Cid putDagCbor(Object value) { delegate.putDagCbor(value) }
        @Override Stream<Cid> listBlocks() { delegate.listBlocks() }
        @Override boolean isWritable() { delegate.isWritable() }
    }
}
