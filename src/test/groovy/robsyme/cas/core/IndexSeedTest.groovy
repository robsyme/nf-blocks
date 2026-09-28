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

    private Index producer() {
        final Index index = Index.open(tmp.resolve("producer-${UUID.randomUUID()}.sqlite"))
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        return index
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

        when:
        opened.clear()
        cold.catchUp(counting, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then: 'the same answers as a full ingest'
        cold.items(r2, 'aligned', [sample: 'B']).size() == 1
        cold.latestSuccessfulRun('p') == Optional.of(r3)
        cold.isRunIndexed(r2) && cold.isRunIndexed(r3)
        cold.metaValue('seeded_from:lab') != null
        cold.metaValue('block_scan:lab') == 'done'

        and: 'only the tail run was read (ticket 04 decision 11)'
        !opened.contains(r2)
        opened.contains(r3)
        console.list.isEmpty()

        cleanup:
        source?.close(); cold?.close()
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

    def 'seeding the same snapshot twice, or two members, duplicates no row'() {
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
