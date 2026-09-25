package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 11: log_entry gives "first seen in this member" without a listing. */
class IndexLogEntryTest extends Specification {

    static final long T0 = 1_758_000_000_000L          // 2025-09-16T05:20:00.000Z

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    private Cid run(String name) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-$name"))
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: name], Fixtures.leaf("${name}.bam".toString(), Fixtures.contentCid(name), 1L)]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ["aligned/${name}.bam".toString()]]]))
        return store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
    }

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

    def 'the schema is version 3 with the tables of spec section 11'() {
        expect:
        Index.SCHEMA_VERSION == 3
        rows("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")*.get(0).containsAll(
            ['collection', 'collection_item', 'selection_child', 'selection_derived', 'log_entry', 'claim', 'claim_supersedes', 'claim_current'])
        rows("SELECT name FROM pragma_table_info('collection')")*.get(0) == ['collection_cid', 'kind', 'completion_cid', 'output_name', 'asserted_by']
        rows("SELECT name FROM pragma_table_info('collection_item')")*.get(0) == ['collection_cid', 'item_cid', 'via_cid']
    }

    def 'catch-up records every entry, of every kind, keeping the first time the member saw it'() {
        given:
        final Cid completion = run('a')
        final Cid other = Fixtures.cidOf([kind: 'Selection', schema: 1])
        final StoreLog log = StoreLog.of(store)
        log.append(StoreLogKind.RUN, completion, T0 + 60_000)
        log.append(StoreLogKind.RUN, completion, T0)                 // the same run, logged earlier
        log.append(StoreLogKind.SELECTION, other, T0 + 120_000)

        when:
        index.catchUp(store, log, 'lab')

        then:
        rows('SELECT cid, kind, member, written_at FROM log_entry ORDER BY written_at') == [
            [completion.toString(), 'run', 'lab', '2025-09-16T05:20:00.000Z'],
            [other.toString(), 'selection', 'lab', '2025-09-16T05:22:00.000Z'],
        ]
        index.firstLogEntry(completion, 'lab').name == StoreLog.entryName(StoreLogKind.RUN, completion, T0)
        index.firstLogEntry(completion, 'elsewhere') == null
    }

    def 'an entry for a run already indexed still gets its row'() {
        given:
        final Cid completion = run('a')
        index.ingestRun(store, completion, 'lab')
        StoreLog.append(store, StoreLogKind.RUN, completion, T0)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        rows('SELECT cid FROM log_entry')*.get(0) == [completion.toString()]
    }

    def 'output collections carry their kind and asserted_by'() {
        given:
        final Cid completion = run('a')

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        rows('SELECT kind, output_name, asserted_by FROM collection') == [['output', 'aligned', Fixtures.ASSERTED_BY]]
        rows('SELECT via_cid FROM collection_item') == [[null]]
    }

    def 'rebuild fills log_entry from the Store Log'() {
        given:
        final Cid completion = run('a')
        StoreLog.append(store, StoreLogKind.RUN, completion, T0)

        when:
        index.rebuild(store, 'lab')

        then:
        rows('SELECT cid, kind, member, written_at FROM log_entry') == [[completion.toString(), 'run', 'lab', '2025-09-16T05:20:00.000Z']]
    }

    def 'isoMillis is UTC with milliseconds'() {
        expect:
        Index.isoMillis(T0 + 7) == '2025-09-16T05:20:00.007Z'
    }
}
