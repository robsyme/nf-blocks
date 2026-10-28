package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CancellationException

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

/** Background store indexing, Task C1: which members the cache holds, seeding without a scan, and one member's scan with progress. */
class IndexMemberScanTest extends Specification {

    @TempDir Path tmp
    Path cloudRoot
    LocalBlockStore cloud
    Index index
    int runs = 0
    /** Each run's one item, by completion. */
    Map<Cid, Cid> items = [:]
    /** Each run's one OutputCollection, by completion. */
    Map<Cid, Cid> collections = [:]

    def setup() {
        cloudRoot = tmp.resolve('cloud')
        cloud = new LocalBlockStore(cloudRoot, 'cloud', true)
        final long now = System.currentTimeMillis()
        final Cid r1 = run('r1', 'A', now - 3_600_000L)
        run('r2', 'B', now - 3_000_000L)
        final Cid selection = cloud.putDagCbor(Fixtures.selection([[item: [address: items[r1], via: []]]]))
        StoreLog.append(cloud, StoreLogKind.SELECTION, selection, now - 2_000_000L)
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    /** What Index logged, for a test that attaches it. */
    ListAppender<ILoggingEvent> logged

    def cleanup() {
        index?.close()
        if( logged != null )
            ((Logger) LoggerFactory.getLogger(Index)).detachAppender(logged)
    }

    /** A succeeded run with one item, logged at `at`; returns its completion. */
    private Cid run(String name, String sample, long at) {
        final Cid content = cloud.putStreaming(new ByteArrayInputStream("bam-${sample}".bytes))
        final Cid item = cloud.putDagCbor(Fixtures.outputItem2([[sample: sample], Fixtures.leaf2("${sample}.bam", content, 5)]))
        final Cid m = cloud.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid coll = cloud.putDagCbor(Fixtures.outputCollection(m, 'aligned', [[item, ["aligned/${sample}.bam".toString()]]]))
        final Cid rc = cloud.putDagCbor(Fixtures.runCompletion(m, [coll], [finished_at: String.format('2026-10-27T10:%02d:00.000Z', runs++)]))
        StoreLog.append(cloud, StoreLogKind.RUN, rc, at)
        items[rc] = item
        collections[rc] = coll
        return rc
    }

    private static long dagCborBlocksIn(BlockStore store) {
        return store.listBlocks().withCloseable { java.util.stream.Stream<Cid> s -> s.filter { Cid c -> c.isDagCbor() }.count() }
    }

    /** IndexSnapshot.write from a scratch index of `member`, as IndexSeedTest does. */
    private void writeSnapshotOf(LocalBlockStore member, Path root) {
        final Index source = Index.open(tmp.resolve("producer-${UUID.randomUUID()}.sqlite"))
        try {
            source.catchUp(member, StoreLog.of(member), member.alias())
            IndexSnapshot.write(source, member.alias(), new LocalSnapshotStorage(root), 0L, null, tmp.resolve('t'))
        }
        finally {
            source.close()
        }
    }

    /** `store`, but every open() from the `nth` call on throws `failure`. */
    private static BlockStore failingAfter(BlockStore store, int nth, IOException failure) {
        return new IndexTest.Counting(store) {
            @Override InputStream open(Cid cid) {
                if( opens + 1 >= nth ) throw failure
                return super.open(cid)
            }
        }
    }

    def 'a member is indexed once the cache holds its watermark or a block scan'() {
        given:
        final LocalBlockStore emptyCloud = new LocalBlockStore(tmp.resolve('empty'), 'cloud', true)

        expect:
        !index.isIndexed('cloud')

        when:
        index.catchUp(emptyCloud, StoreLog.of(emptyCloud), 'cloud')

        then:
        index.isIndexed('cloud')
        index.watermark('cloud') == null
        index.metaValue('block_scan:cloud') == 'empty'
    }

    def 'trySeed seeds from a usable snapshot and otherwise says why, reading no block'() {
        given:
        final IndexTest.Counting counting = new IndexTest.Counting(cloud)

        expect:
        index.trySeed(counting, StoreLog.of(cloud), 'cloud', new LocalSnapshotStorage(cloudRoot), tmp.resolve('t')) == 'absent'
        counting.opens == 0 && index.countRows('run') == 0 && !index.isIndexed('cloud')

        when:
        writeSnapshotOf(cloud, cloudRoot)

        then:
        index.trySeed(counting, StoreLog.of(cloud), 'cloud', new LocalSnapshotStorage(cloudRoot), tmp.resolve('t')) == null
        counting.opens == 0
        index.isIndexed('cloud') && index.countRows('run') == 2
    }

    def 'trySeed leaves a member that already has a watermark alone'() {
        given:
        index.catchUp(cloud, StoreLog.of(cloud), 'cloud')

        expect:
        index.trySeed(cloud, StoreLog.of(cloud), 'cloud', new LocalSnapshotStorage(cloudRoot), tmp.resolve('t')) == null
    }

    def 'trySeed says unreadable when fetching the snapshot throws'() {
        given:
        final SnapshotStorage refusing = Stub(SnapshotStorage) { fetch(_) >> { throw new IllegalStateException('403') } }

        expect:
        index.trySeed(cloud, StoreLog.of(cloud), 'cloud', refusing, tmp.resolve('t')) == 'unreadable'
        !index.isIndexed('cloud')
    }

    def 'indexMember reports listing, every metadata block read, each record ingested, then the log'() {
        given:
        final List<List<Object>> seen = []

        when:
        index.indexMember(cloud, StoreLog.of(cloud), 'cloud', cloud, { String p, long d, long t -> seen << [p, d, t] } as IndexProgress)

        then:
        seen*.getAt(0).unique() == ['listing', 'reading', 'ingesting', 'log']
        seen.first() == ['listing', -1L, -1L]
        seen.findAll { it[0] == 'reading' }.first() == ['reading', 0L, dagCborBlocksIn(cloud)]
        seen.findAll { it[0] == 'reading' }.last() == ['reading', dagCborBlocksIn(cloud), dagCborBlocksIn(cloud)]
        seen.findAll { it[0] == 'ingesting' }.last() == ['ingesting', 3L, 3L]          // two runs and a Selection
        seen.last() == ['log', -1L, -1L]
        index.isIndexed('cloud') && index.watermark('cloud') == StoreLog.read(cloud)[0].name
        index.metaValue('block_scan:cloud') == 'done'
        index.countRows('run') == 2
    }

    def 'indexMember marks a member with no records empty'() {
        given:
        final LocalBlockStore emptyCloud = new LocalBlockStore(tmp.resolve('empty'), 'cloud', true)

        when:
        index.indexMember(emptyCloud, StoreLog.of(emptyCloud), 'cloud', emptyCloud, { String p, long d, long t -> } as IndexProgress)

        then:
        index.isIndexed('cloud')
        index.watermark('cloud') == null
        index.metaValue('block_scan:cloud') == 'empty'
    }

    def 'a read that fails mid-scan throws and marks nothing'() {
        given:
        final BlockStore failing = failingAfter(cloud, 3, new IOException('expired'))

        when:
        index.indexMember(failing, StoreLog.of(cloud), 'cloud', cloud, { String p, long d, long t -> } as IndexProgress)

        then:
        thrown(IOException)
        !index.isIndexed('cloud') && index.watermark('cloud') == null && index.metaValue('block_scan:cloud') == null
    }

    def 'a read that fails while ingesting throws and marks nothing'() {
        given: 'a run collection that reads in the reading phase and fails when its run is ingested'
        final Cid collection = collections.values().first()
        final BlockStore failing = new IndexTest.Counting(cloud) {
            int collectionOpens = 0
            @Override InputStream open(Cid cid) {
                if( cid == collection && ++collectionOpens > 1 ) throw new IOException('expired')
                return super.open(cid)
            }
        }
        final List<String> phases = []

        when:
        index.indexMember(failing, StoreLog.of(cloud), 'cloud', cloud, { String p, long d, long t -> phases << p } as IndexProgress)

        then:
        final IOException e = thrown()
        e.message == 'expired'
        phases.contains('ingesting')
        !index.isIndexed('cloud') && index.watermark('cloud') == null && index.metaValue('block_scan:cloud') == null
    }

    def 'a read that fails while ingesting is not also logged as a block that could not be decoded'() {
        given: 'a store whose reads all fail once the run collections have been read in the reading phase'
        Set<Cid> runCollections = collections.values() as Set<Cid>
        final BlockStore failing = new IndexTest.Counting(cloud) {
            int collectionOpens = 0
            @Override InputStream open(Cid cid) {
                if( cid in runCollections && ++collectionOpens > runCollections.size() ) throw new IOException('expired')
                return super.open(cid)
            }
        }
        logged = new ListAppender<ILoggingEvent>()
        logged.start()
        ((Logger) LoggerFactory.getLogger(Index)).addAppender(logged)

        when:
        index.indexMember(failing, StoreLog.of(cloud), 'cloud', cloud, { String p, long d, long t -> } as IndexProgress)

        then:
        thrown(IOException)
        !logged.list.any { it.formattedMessage.contains('could not be decoded') }
        !logged.list.any { it.level == Level.WARN }
    }

    def 'a block that vanishes between the listing and its read is skipped'() {
        given: 'a listing that names a metadata block the store no longer holds'
        final Cid gone = Cid.of(Cid.DAG_CBOR, Hashing.sha256('swept'.getBytes('UTF-8')))
        final BlockStore swept = new IndexTest.Counting(cloud) {
            @Override java.util.stream.Stream<Cid> listBlocks() { (delegate.listBlocks().toList() + [gone]).stream() }
        }
        final List<List<Object>> seen = []

        when:
        index.indexMember(swept, StoreLog.of(cloud), 'cloud', cloud, { String p, long d, long t -> seen << [p, d, t] } as IndexProgress)

        then:
        seen.findAll { it[0] == 'reading' }.last() == ['reading', dagCborBlocksIn(cloud) + 1, dagCborBlocksIn(cloud) + 1]
        index.metaValue('block_scan:cloud') == 'done'
        index.countRows('run') == 2
    }

    def 'a progress callback that throws stops the scan and marks nothing'() {
        when:
        index.indexMember(cloud, StoreLog.of(cloud), 'cloud', cloud,
            { String p, long d, long t -> if( p == 'reading' && d == 2 ) throw new CancellationException() } as IndexProgress)

        then:
        thrown(CancellationException)
        !index.isIndexed('cloud')
    }

    def 'the tolerant scan still indexes what it can read when a read fails'() {
        given:
        final BlockStore failing = failingAfter(cloud, 3, new IOException('expired'))

        when:
        index.catchUp(failing, StoreLog.of(cloud), 'cloud')

        then:
        noExceptionThrown()
        index.metaValue('block_scan:cloud') != null
    }

    def 'reset removes the local snapshots kept beside the cache file'() {
        given:
        Files.createDirectories(IndexPaths.localSnapshotsDir(index.file).resolve('cloud/index'))
        Files.write(IndexPaths.localSnapshotsDir(index.file).resolve('cloud/index/v7.sqlite'), [1, 2, 3] as byte[])

        when:
        index.reset()

        then:
        !Files.exists(IndexPaths.localSnapshotsDir(index.file))
    }

    def 'reset with no local snapshots succeeds'() {
        when:
        index.reset()

        then:
        noExceptionThrown()
        !Files.exists(IndexPaths.localSnapshotsDir(index.file))
    }
}
