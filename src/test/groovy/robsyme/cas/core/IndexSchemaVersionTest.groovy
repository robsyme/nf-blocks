package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Schema 6 (DESIGN.md §12): every table, the transfer and P4 ones included,
 * is in the base schema. A cache of an older version (v4, v5) is recreated
 * and such a snapshot is not seeded from, so a member is read again from its
 * blocks and its Store Log, and every row the older build may have skipped,
 * or a Handoff's file rows a v5 build did not record, is ingested.
 */
class IndexSchemaVersionTest extends Specification {

    static final String WHO = 'p4-test@example.org'

    @TempDir Path tmp
    RetentionFixture f

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
    }

    private void catchUp(Index into) {
        into.catchUp(f.store, StoreLog.of(f.store), 'lab')
    }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
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

    private static void exec(Path db, List<String> statements) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            for( String sql : statements )
                c.createStatement().executeUpdate(sql)
        }
        finally {
            c.close()
        }
    }

    /** A logged Transfer of {@code root} carrying {@code run}. */
    private Cid transfer(Cid root, Cid run) {
        final Cid cid = f.store.putDagCbor(new Transfer(WHO, [root], 'roblab', 'lab', 'results', ['names'],
            [run], 3L, 3L, 30L, '2026-10-25T10:00:00.000Z').toCbor())
        StoreLog.append(f.store, StoreLogKind.TRANSFER, cid, ++f.clock)
        return cid
    }

    /** A run with an InputSet, a Selection of its item, a Transfer, a Project and a Handoff, all logged. */
    private Map member() {
        final Cid input = f.raw('reads another lab keeps')
        final Map run = f.run('r', [[x: f.raw('x')]], [inputSet: f.inputSet([input])])
        final Cid sel = f.selection(((List<Cid>) run.items)[0], (Cid) run.collection)
        final Cid project = f.project('atlas')
        return [run: run, input: input, transfer: transfer(sel, (Cid) run.completion), project: project,
                handoff: f.handoff(sel, project, ['samplesheet.csv': f.raw('sample\nA\n')])]
    }

    /** Every row the transfer and P4 tables hold for {@code m}, as a catch-up writes them. */
    private static void expectEveryRow(Path db, Map m) {
        assert rows(db, 'SELECT cid FROM transfer') == [[m.transfer.toString()]]
        assert rows(db, 'SELECT role FROM transfer_root ORDER BY role') == [['root'], ['run']]
        assert rows(db, 'SELECT project_cid FROM project') == [[m.project.toString()]]
        assert rows(db, 'SELECT cid, project_cid FROM handoff') == [[m.handoff.toString(), m.project.toString()]]
        assert rows(db, 'SELECT completion_cid, address FROM run_input') == [[m.run.completion.toString(), m.input.toString()]]
        assert rows(db, "SELECT kind FROM log_entry WHERE kind IN ('transfer', 'project', 'handoff') ORDER BY kind") ==
            [['handoff'], ['project'], ['transfer']]
    }

    /** Leaves {@code db} as a v4 build that knew none of the transfer and P4 kinds left it: their rows gone, the watermark past them. */
    private static void asV4(Path db) {
        exec(db, ['DELETE FROM transfer_root', 'DELETE FROM transfer', 'DELETE FROM project', 'DELETE FROM handoff', 'DELETE FROM run_input',
                  "DELETE FROM log_entry WHERE kind IN ('transfer', 'project', 'handoff')",
                  'UPDATE schema_version SET version = 4'])
    }

    def 'the schema version is 6'() {
        // 6 (P4 second follow-up): a v5 cache that ingested a Handoff before its files were recorded
        // as content (1f994d4) has no block_ref rows for them; a version bump rebuilds it.
        expect:
        Index.SCHEMA_VERSION == 6
        IndexSnapshot.relativePath() == 'index/v6.sqlite'
    }

    def 'a cache of an older version (v4) is recreated as the current one when opened, and its catch-up ingests Transfers, Projects, Handoffs and InputSets'() {
        given: 'a cache a v4 build caught up past every entry'
        final Map m = member()
        final Path cache = tmp.resolve('cache.sqlite')
        final Index old = Index.open(cache)
        catchUp(old)
        old.close()
        asV4(cache)
        assert rows(cache, "SELECT value FROM meta WHERE key = 'store_log_watermark:lab'") == [[StoreLog.read(f.store)[0].name]]

        when:
        final Index index = Index.open(cache)

        then: 'a new, empty cache of this version'
        rows(cache, 'SELECT version FROM schema_version') == [[Index.SCHEMA_VERSION]]
        index.countRows('run') == 0
        index.watermark('lab') == null

        when:
        catchUp(index)

        then:
        index.isRunIndexed((Cid) m.run.completion)
        expectEveryRow(cache, m)

        cleanup:
        index?.close()
    }

    def 'a v4 snapshot is not seeded from: a cold cache reads every block and the whole Store Log'() {
        given: "the member's snapshot as a v4 build wrote it, at the v4 path and at this build's"
        final Map m = member()
        final Path producerFile = tmp.resolve('producer.sqlite')
        final Index producer = Index.open(producerFile)
        catchUp(producer)
        IndexSnapshot.write(producer, 'lab', new LocalSnapshotStorage(f.root), 0L, null, tmp.resolve('t'))
        producer.close()
        final Path snapshot = IndexSnapshot.pathIn(f.root)
        asV4(snapshot)
        Files.copy(snapshot, f.root.resolve('index/v4.sqlite'), StandardCopyOption.REPLACE_EXISTING)

        when:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(f.root), tmp.resolve('t'))

        then: 'not seeded: the full catch-up found every row'
        cold.metaValue('seeded_from:lab') == null
        cold.isRunIndexed((Cid) m.run.completion)
        expectEveryRow(cold.file, m)

        when: 'only the v4 path holds one'
        Files.delete(snapshot)
        final Index colder = Index.open(tmp.resolve('colder.sqlite'))
        colder.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(f.root), tmp.resolve('t'))

        then:
        colder.metaValue('seeded_from:lab') == null
        expectEveryRow(colder.file, m)

        cleanup:
        cold?.close(); colder?.close()
    }

    def 'a run the block scan finds without a Store Log entry has its InputSet rows on a fresh cache'() {
        given: 'a RunCompletion linking an InputSet, written before the Store Log, so only the block scan finds it'
        final Cid input = f.raw('reads')
        final Cid manifest = f.store.putDagCbor(RetentionFixture.manifest('unlogged', f.raw('script')))
        final Cid completion = f.store.putDagCbor(RetentionFixture.completion(manifest, [], [inputSet: f.inputSet([input])]))
        assert StoreLog.read(f.store).isEmpty()

        when:
        final Index index = Index.open(tmp.resolve('fresh.sqlite'))
        catchUp(index)

        then:
        index.isRunIndexed(completion)
        rows(index.file, 'SELECT completion_cid, address FROM run_input') == [[completion.toString(), input.toString()]]
        rows(index.file, 'SELECT content_cid, completion_cid FROM consumer') == [[input.toString(), completion.toString()]]

        cleanup:
        index?.close()
    }
}
