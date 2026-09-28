package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 04: a cold cache seeds from the member's snapshot and reads only the tail. */
class IndexSeedTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore lab
    List<Cid> opened = []

    /** Counts every metadata block the index reads, which is what seeding must avoid. */
    BlockStore counting

    ListAppender<ILoggingEvent> console
    int runs = 0

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        counting = new CompositeStore([lab]) {
            @Override InputStream open(Cid cid) { opened << cid; super.open(cid) }
        }
        console = new ListAppender<ILoggingEvent>(); console.start()
        ((Logger) LoggerFactory.getLogger('nextflow.cas')).addAppender(console)
    }

    def cleanup() { ((Logger) LoggerFactory.getLogger('nextflow.cas')).detachAppender(console) }

    /** A succeeded run with one item, logged at `at`; returns its completion. */
    private Cid run(String name, String sample, long at) {
        final Cid content = lab.putStreaming(new ByteArrayInputStream("bam-${sample}".bytes))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([[sample: sample], Fixtures.leaf2("${sample}.bam", content, 5)]))
        final Cid m = lab.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(m, 'aligned', [[item, ["aligned/${sample}.bam".toString()]]]))
        // finished_at in call order, so the last run() is the latest run.
        final Cid rc = lab.putDagCbor(Fixtures.runCompletion(m, [coll], [finished_at: String.format('2026-09-28T10:%02d:00.000Z', runs++)]))
        StoreLog.append(lab, StoreLogKind.RUN, rc, at)
        return rc
    }

    private Index producer(LocalBlockStore member = lab) {
        final Index index = Index.open(tmp.resolve("producer-${UUID.randomUUID()}.sqlite"))
        index.catchUp(member, StoreLog.of(member), member.alias())
        return index
    }

    /** Every table of Index.ddl() but the bookkeeping ones, which a seeded and a scanned cache may differ in. */
    private static List<String> contentTables() {
        return Index.ddl().findResults { String sql ->
            final java.util.regex.Matcher m = sql =~ /^CREATE TABLE (\w+)\(/
            m.find() ? m.group(1) : null
        }.findAll { it != 'meta' && it != 'schema_version' } as List<String>
    }

    /** A table's rows, each as its columns' text, sorted, so two indexes compare as multisets. */
    private static List<String> rowsOf(Index index, String table) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${index.file.toAbsolutePath()}".toString())
        try {
            final java.sql.ResultSet rs = c.createStatement().executeQuery("SELECT * FROM ${table}".toString())
            final int n = rs.metaData.columnCount
            final List<String> rows = []
            while( rs.next() )
                rows << (1..n).collect { int i -> String.valueOf(rs.getString(i)) }.join('|')
            return rows.sort()
        }
        finally {
            c.close()
        }
    }

    /** The tables whose rows differ between two indexes; empty when a seed matches a full scan. */
    private static List<String> differingTables(Index a, Index b) {
        return contentTables().findAll { String t -> rowsOf(a, t) != rowsOf(b, t) }
    }

    def 'a cold cache seeds, answers like the producer, and reads no block of a run the snapshot holds'() {
        given:
        final long now = System.currentTimeMillis()
        run('r1', 'A', now - 3_600_000L)
        final Cid r2 = run('r2', 'B', now - 3_000_000L)
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Cid r3 = run('r3', 'C', now)                        // after the snapshot: the tail
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        final Index full = producer()

        when:
        opened.clear()
        cold.catchUp(counting, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then: 'the same answers as a full ingest'
        cold.items(r2, 'aligned', [sample: 'B']).size() == 1
        cold.latestSuccessfulRun('p') == Optional.of(r3)
        cold.isRunIndexed(r2) && cold.isRunIndexed(r3)
        cold.metaValue('seeded_from:lab') != null
        cold.metaValue('block_scan:lab') == 'done'

        and: 'every table holds what a full scan of the same member holds'
        differingTables(cold, full) == []

        and: 'only the tail run was read (ticket 04 decision 11)'
        !opened.contains(r2)
        opened.contains(r3)
        console.list.isEmpty()

        cleanup:
        source?.close(); cold?.close(); full?.close()
    }

    def 'claim_current is recomputed, never copied (ticket 04 decision 9)'() {
        given:
        final Cid rc = run('r1', 'A', System.currentTimeMillis())
        final Cid claim = lab.putDagCbor(Fixtures.claim(rc, 'delete', null, null, []))
        StoreLog.append(lab, StoreLogKind.CLAIM, claim, System.currentTimeMillis())
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')

        then:
        cold.claimState(rc).deletion == 'deleted'
        cold.latestSuccessfulRun('p') == Optional.empty()

        cleanup:
        source?.close(); cold?.close()
    }

    def 'seeding the same snapshot twice duplicates no row'() {
        given:
        run('r1', 'A', System.currentTimeMillis())
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')

        then:
        cold.countRows('collection_item') == source.countRows('collection_item')
        cold.countRows('item_attr') == source.countRows('item_attr')

        cleanup:
        source?.close(); cold?.close()
    }

    def 'two members sharing a run, each with its own Claim on it, seed into one cache as a full scan of both would'() {
        given: 'r1 in lab and in core; a Claim on it in each'
        final long now = System.currentTimeMillis()
        final Cid r1 = run('r1', 'A', now - 3_600_000L)
        LocalBlockStore core = new LocalBlockStore(tmp.resolve('core'), 'core', true)
        lab.listBlocks().withCloseable { it.forEach { Cid b -> core.put(b, lab.open(b), lab.size(b)) } }
        StoreLog.append(core, StoreLogKind.RUN, r1, now - 3_000_000L)
        final Cid labClaim = lab.putDagCbor(Fixtures.claim(r1, 'set', 'name', 'tumour', []))
        StoreLog.append(lab, StoreLogKind.CLAIM, labClaim, now - 2_000_000L)
        final Cid coreClaim = core.putDagCbor(Fixtures.claim(r1, 'set', 'name', 'normal', []))
        StoreLog.append(core, StoreLogKind.CLAIM, coreClaim, now - 1_000_000L)

        and: 'each member snapshotted by its own writer'
        final Index labWriter = producer(lab)
        final Index coreWriter = producer(core)
        IndexSnapshot.write(labWriter, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        IndexSnapshot.write(coreWriter, 'core', new LocalSnapshotStorage(core.root), 0L, null, tmp.resolve('t'))

        and: 'a full scan of both, lab first'
        Index full = Index.open(tmp.resolve('full.sqlite'))
        full.catchUp(lab, StoreLog.of(lab), 'lab')
        full.catchUp(core, StoreLog.of(core), 'core')
        Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))
        cold.catchUp(core, StoreLog.of(core), 'core', new LocalSnapshotStorage(core.root), tmp.resolve('t'))

        then: 'both were seeded, and no row is duplicated'
        cold.metaValue('seeded_from:lab') != null
        cold.metaValue('seeded_from:core') != null
        ['run', 'collection_item', 'item_attr', 'claim', 'log_entry'].every { String t -> cold.countRows(t) == full.countRows(t) }
        cold.countRows('run') == 1
        cold.countRows('claim') == 2

        and: 'the run keeps the member that indexed it first, as ingest does'
        rowsOf(cold, 'run').collect { it.split('\\|')[-1] } == ['lab']

        and: 'one log_entry per (cid, member), under the right alias'
        cidMembers(cold) == [
            "${r1}|lab".toString(), "${r1}|core".toString(),
            "${labClaim}|lab".toString(), "${coreClaim}|core".toString(),
        ].sort()

        and: 'claim_current reflects both Claims: two values of name, in conflict'
        cold.claimState(r1).nameConflicted
        cold.claimState(r1).nameClaims.toSet() == [labClaim.toString(), coreClaim.toString()].toSet()

        and: 'every table holds what the full scan holds'
        differingTables(cold, full) == []
        console.list.isEmpty()

        cleanup:
        labWriter?.close(); coreWriter?.close(); full?.close(); cold?.close()
    }

    /** log_entry as cid|member, sorted. */
    private static List<String> cidMembers(Index index) {
        return rowsOf(index, 'log_entry').collect { String row -> final String[] c = row.split('\\|'); "${c[0]}|${c[2]}".toString() }.sort()
    }

    def 'a run whose block had not arrived when the snapshot was built is indexed once it arrives'() {
        given: 'a logged run whose completion block is absent while the snapshot is built'
        final long now = System.currentTimeMillis()
        final Cid late = run('late', 'L', now - 3_600_000L)       // far outside the overlap window
        final Path block = lab.blockPath(late)
        final Path held = tmp.resolve('held-block')
        Files.move(block, held)
        run('r2', 'B', now)                                        // the watermark entry
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))

        and: 'the block arrives afterwards'
        Files.move(held, block)
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        final Index full = producer()

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then:
        !source.isRunIndexed(late)
        cold.metaValue('seeded_from:lab') != null
        differingTables(cold, full) == []
        cold.isRunIndexed(late)

        cleanup:
        source?.close(); cold?.close(); full?.close()
    }

    def 'no usable snapshot (#why): full scan, same answer, one warning naming the member and the fix'() {
        given:
        final Cid rc = run('r1', 'A', System.currentTimeMillis())
        prepare.call(lab.root)
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then:
        cold.isRunIndexed(rc)
        cold.metaValue('seeded_from:lab') == null
        console.list.size() == 1
        console.list[0].formattedMessage.contains("'lab'")
        console.list[0].formattedMessage.contains(why)
        console.list[0].formattedMessage.contains('nf-blocks:snapshot')

        cleanup:
        cold?.close()

        where:
        why                        | prepare
        'absent'                   | { Path root -> }
        'unreadable'               | { Path root -> Files.createDirectories(root.resolve('index')); Files.writeString(IndexSnapshot.pathIn(root), 'not sqlite') }
        'schema_version 99'        | { Path root -> snapshotAtVersion(root, 99) }
    }

    def 'a snapshot storage that throws #failure.class.simpleName is unreadable: the full scan still runs, with the warning (final review I4)'() {
        given:
        final Cid rc = run('r1', 'A', System.currentTimeMillis())
        RuntimeException raised = failure
        final SnapshotStorage throwing = new LocalSnapshotStorage(lab.root) {
            @Override Path fetch(Path tempDir) { throw raised }
        }
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', throwing, tmp.resolve('t'))

        then:
        cold.isRunIndexed(rc)
        cold.metaValue('seeded_from:lab') == null
        console.list.size() == 1
        console.list[0].formattedMessage.contains("'lab'")
        console.list[0].formattedMessage.contains('unreadable')

        cleanup:
        cold?.close()

        where:
        failure << [new IllegalStateException('Access Denied (Service: S3, Status Code: 403)'), new UncheckedIOException(new IOException('reset'))]
    }

    /** A snapshot of the member at root whose schema_version row then says `version`. */
    private static void snapshotAtVersion(Path root, int version) {
        final LocalBlockStore store = new LocalBlockStore(root, 'lab', true)
        final Index p = Index.open(root.resolveSibling('version-producer.sqlite'))
        try {
            p.catchUp(store, StoreLog.of(store), 'lab')
            IndexSnapshot.write(p, 'lab', new LocalSnapshotStorage(root), 0L, null, root.resolveSibling('version-t'))
        }
        finally {
            p.close()
        }
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${IndexSnapshot.pathIn(root).toAbsolutePath()}".toString())
        try {
            c.createStatement().executeUpdate("UPDATE schema_version SET version = ${version}".toString())
        }
        finally {
            c.close()
        }
    }

    def 'an empty member warns about nothing'() {
        given:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then:
        console.list.isEmpty()

        cleanup:
        cold?.close()
    }
}
