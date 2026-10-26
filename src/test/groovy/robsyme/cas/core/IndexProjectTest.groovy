package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** P4 plan 1.4, 1.5: `project` and `handoff` entries fill `project` and `handoff`, in the cache and in a member's snapshot. */
class IndexProjectTest extends Specification {

    static final String WHO = 'p4-test@example.org'

    @TempDir Path tmp
    RetentionFixture f
    Index index

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private void catchUp(Index into = index, BlockStore store = f.store) {
        into.catchUp(store, StoreLog.of(store), store.alias())
    }

    private Cid project(BlockStore store = f.store, String name = 'RNA atlas', boolean log = true) {
        final Cid cid = store.putDagCbor(new Project(WHO, name, 'bulk RNA-seq of the atlas', '2026-10-25T09:00:00.000Z').toCbor())
        if( log )
            StoreLog.append(store, StoreLogKind.PROJECT, cid, ++f.clock)
        return cid
    }

    private Cid handoff(Cid selection, Cid project, BlockStore store = f.store, boolean log = true) {
        final Cid sheet = f.raw("sample,condition\nA,${selection}\n")
        final Handoff h = new Handoff(WHO, 'nf-core/differentialabundance', selection, project,
            [Leaf.of('samplesheet.csv', sheet, 20L)], [input: "cas://${sheet}/samplesheet.csv".toString()], '2026-10-25T10:00:00.000Z')
        final Cid cid = store.putDagCbor(h.toCbor())
        if( log )
            StoreLog.append(store, StoreLogKind.HANDOFF, cid, ++f.clock)
        return cid
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

    def 'a project entry fills project and log_entry'() {
        given:
        final Cid p = project()

        when:
        catchUp()

        then:
        rows(index.file, 'SELECT * FROM project') == [[p.toString(), 'RNA atlas', 'bulk RNA-seq of the atlas', WHO, '2026-10-25T09:00:00.000Z']]
        rows(index.file, "SELECT kind, member FROM log_entry WHERE cid = '${p}'") == [['project', 'lab']]
        index.isProjectIndexed(p)
        !index.isHandoffIndexed(p)
    }

    def 'a handoff entry fills handoff and log_entry, with or without a project'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid s = f.selection((Cid) run.items[0], (Cid) run.collection)
        final Cid p = project()
        final Cid withProject = handoff(s, p)
        final Cid alone = handoff(s, null)

        when:
        catchUp()

        then:
        rows(index.file, 'SELECT * FROM handoff ORDER BY project_cid IS NULL') == [
            [withProject.toString(), 'nf-core/differentialabundance', s.toString(), p.toString(), WHO, '2026-10-25T10:00:00.000Z'],
            [alone.toString(), 'nf-core/differentialabundance', s.toString(), null, WHO, '2026-10-25T10:00:00.000Z']]
        rows(index.file, "SELECT kind FROM log_entry WHERE cid = '${withProject}'") == [['handoff']]
        index.isHandoffIndexed(withProject)
        index.isHandoffIndexed(alone)
        !index.isProjectIndexed(withProject)
    }

    def 'catch-up is idempotent across the overlap window, and ingesting again leaves one set of rows'() {
        given:
        final Cid p = project()
        final Cid h = handoff(Fixtures.cidOf([kind: 'Selection', n: 1]), p)

        when:
        catchUp()
        final List<List<Object>> projects = rows(index.file, 'SELECT * FROM project')
        final List<List<Object>> handoffs = rows(index.file, 'SELECT * FROM handoff')
        index.setMetaValue('store_log_watermark:lab', null)
        catchUp()
        index.ingestTolerant(f.store, StoreLogKind.PROJECT, p, 'lab', f.store)
        index.ingestTolerant(f.store, StoreLogKind.HANDOFF, h, 'lab', f.store)

        then:
        rows(index.file, 'SELECT * FROM project') == projects
        rows(index.file, 'SELECT * FROM handoff') == handoffs
        index.countRows('log_entry') == 2
    }

    def 'a block not yet arrived leaves missing(NULL, cid) and is ingested once it lands'() {
        given:
        final Cid p = project()
        final Cid h = handoff(Fixtures.cidOf([kind: 'Selection', n: 1]), p)
        final Map<Cid, byte[]> held = [:]
        for( Cid c : [p, h] ) {
            final Path path = ((LocalBlockStore) f.store).blockPath(c)
            held[c] = Files.readAllBytes(path)
            Files.delete(path)
        }

        when:
        catchUp()

        then:
        !index.isProjectIndexed(p)
        !index.isHandoffIndexed(h)
        rows(index.file, 'SELECT have_cid, needed_cid FROM missing ORDER BY needed_cid') ==
            [p, h].sort { it.toString() }.collect { [null, it.toString()] }

        when: 'they arrive after the watermark has passed their entries'
        held.each { Cid c, byte[] bytes -> f.store.put(c, new ByteArrayInputStream(bytes), bytes.length) }
        catchUp()

        then:
        index.isProjectIndexed(p)
        index.isHandoffIndexed(h)
        index.countRows('missing') == 0
    }

    def 'a malformed Project or Handoff block is skipped, not retried, and does not stop the catch-up'() {
        given:
        final Cid badProject = f.store.putDagCbor([kind: 'Project', schema: 1L, asserted_by: WHO, name: '', description: '', created_at: 'x'])
        StoreLog.append(f.store, StoreLogKind.PROJECT, badProject, ++f.clock)
        final Cid badHandoff = f.store.putDagCbor([kind: 'Handoff', schema: 1L, asserted_by: WHO, template: 't', files: 'not a list'])
        StoreLog.append(f.store, StoreLogKind.HANDOFF, badHandoff, ++f.clock)
        final Cid notAProject = f.raw('a raw block is not a Project')
        StoreLog.append(f.store, StoreLogKind.PROJECT, notAProject, ++f.clock)
        final Map run = f.run('after', [[x: f.raw('x')]])

        when:
        catchUp()
        catchUp()

        then:
        !index.isProjectIndexed(badProject)
        !index.isHandoffIndexed(badHandoff)
        !index.isProjectIndexed(notAProject)
        index.isRunIndexed((Cid) run.completion)
        index.countRows('missing') == 0
        index.countRows('project') == 0
        index.countRows('handoff') == 0
    }

    def 'a rebuild and a first block scan both find Projects and Handoffs'() {
        given:
        final Cid p = project()
        final Cid h = handoff(Fixtures.cidOf([kind: 'Selection', n: 1]), p)

        when:
        index.rebuild(f.store, 'lab')

        then:
        index.isProjectIndexed(p)
        index.isHandoffIndexed(h)
        rows(index.file, "SELECT kind FROM log_entry WHERE cid = '${p}'") == [['project']]

        when: 'blocks with no entry yet (a writer that stopped before logging them)'
        final Cid unloggedProject = project(f.store, 'unlogged', false)
        final Cid unloggedHandoff = handoff(Fixtures.cidOf([kind: 'Selection', n: 2]), null, f.store, false)
        final Index scanned = Index.open(tmp.resolve('scanned.sqlite'))
        catchUp(scanned)

        then:
        scanned.isProjectIndexed(p)
        scanned.isHandoffIndexed(h)
        scanned.isProjectIndexed(unloggedProject)
        scanned.isHandoffIndexed(unloggedHandoff)

        cleanup:
        scanned?.close()
    }

    def 'forgetting a Project or Handoff cid removes its row, and a forget entry does too'() {
        given:
        final Cid p = project()
        final Cid other = project(f.store, 'other')
        final Cid h = handoff(Fixtures.cidOf([kind: 'Selection', n: 1]), p)
        catchUp()

        when:
        Files.delete(((LocalBlockStore) f.store).blockPath(p))
        Files.delete(((LocalBlockStore) f.store).blockPath(h))
        index.forget([p, h])

        then:
        rows(index.file, 'SELECT project_cid FROM project') == [[other.toString()]]
        index.countRows('handoff') == 0

        when: 'a sweep deletes the other and logs a forget for it'
        Files.delete(((LocalBlockStore) f.store).blockPath(other))
        final Cid forget = f.store.putDagCbor(new Forget('test', [other]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, forget, ++f.clock)
        catchUp()

        then:
        index.countRows('project') == 0
    }

    def "a member's snapshot carries the project and handoff rows its Store Log names, and a seeded cache has them"() {
        given:
        final LocalBlockStore shared = new LocalBlockStore(tmp.resolve('shared'), 'shared', true)
        f.run('r', [[x: f.raw('x')]])
        final Cid p = project()
        final Cid h = handoff(Fixtures.cidOf([kind: 'Selection', n: 1]), p)
        final Cid sharedProject = project(shared, 'shared project')
        catchUp()
        catchUp(index, shared)
        assert index.countRows('project') == 2

        when:
        final IndexSnapshot.Result written = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        final Path snapshot = tmp.resolve('lab').resolve(IndexSnapshot.relativePath())

        then:
        written.written
        rows(snapshot, 'SELECT project_cid FROM project') == [[p.toString()]]
        rows(snapshot, 'SELECT cid FROM handoff') == [[h.toString()]]
        !rows(snapshot, 'SELECT project_cid FROM project').contains([sharedProject.toString()])

        when: 'a cold cache seeds from it'
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        rows(cold.file, 'SELECT * FROM project') == rows(index.file, "SELECT * FROM project WHERE project_cid = '${p}'")
        rows(cold.file, 'SELECT * FROM handoff') == rows(index.file, 'SELECT * FROM handoff')

        cleanup:
        cold?.close()
    }

    def 'a seeded Project whose block the snapshot writer could not read is retried after the seed'() {
        given:
        final Cid p = project()
        final Path path = ((LocalBlockStore) f.store).blockPath(p)
        final byte[] bytes = Files.readAllBytes(path)
        Files.delete(path)
        catchUp()
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        f.store.put(p, new ByteArrayInputStream(bytes), bytes.length)

        when:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        cold.isProjectIndexed(p)

        cleanup:
        cold?.close()
    }
}
