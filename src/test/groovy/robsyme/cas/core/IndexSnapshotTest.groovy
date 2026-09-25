package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §15: a member's Index Snapshot holds that member's runs and what
 * they reach, copied by SQL from the cache index into one rollback-journal
 * file of 4096-byte pages, replaced atomically.
 */
class IndexSnapshotTest extends Specification {

    @TempDir
    Path tempDir

    Path labRoot
    Path sharedRoot
    LocalBlockStore lab
    LocalBlockStore shared
    Index index

    def setup() {
        labRoot = tempDir.resolve('lab')
        sharedRoot = tempDir.resolve('shared')
        lab = new LocalBlockStore(labRoot, 'lab', true)
        // A member being read-only in a real composition is a property of the
        // consuming session, not of the store fixture; this test writes runs
        // straight into `shared` as a fixture, as IndexTest does at line 519.
        shared = new LocalBlockStore(sharedRoot, 'shared', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    /** One run of one item per sample into a store; returns [completion, collection, items by sample]. */
    private List run(LocalBlockStore store, String runName, List<String> samples) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: runName, nf_run_hash: "hash-$runName"))
        final List<List> withPaths = []
        final Map<String, Cid> items = [:]
        for( String sample : samples ) {
            final Cid item = store.putDagCbor(Fixtures.outputItem(
                [[sample: sample], Fixtures.leaf("${sample}.bam".toString(), Fixtures.contentCid("bam-$sample"), 10L)]))
            items[sample] = item
            withPaths << [item, ["aligned/${sample}.bam".toString()]]
        }
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', withPaths))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, System.currentTimeMillis())
        return [completion, collection, items]
    }

    private void catchUp() {
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(shared, StoreLog.of(shared), 'shared')
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

    def 'the snapshot holds the member own runs and only what they reach'() {
        given:
        final List a = run(lab, 'a', ['A', 'B'])
        final List b = run(shared, 'b', ['B', 'C'])       // item B is the same block in both runs
        catchUp()

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 0L)
        final Path file = result.path

        then:
        result.written
        result.runs == 1
        file == labRoot.resolve('index/v2.sqlite')
        rows(file, 'SELECT completion_cid, member FROM run') == [[a[0].toString(), null]]
        rows(file, 'SELECT collection_cid FROM collection')*.get(0) == [a[1].toString()]
        rows(file, 'SELECT DISTINCT completion_cid FROM producer')*.get(0) == [a[0].toString()]
        rows(file, 'SELECT item_cid FROM item ORDER BY item_cid')*.get(0) == ((Map) a[2]).values()*.toString().sort()
        rows(file, "SELECT count(*) FROM item_attr WHERE path = 'sample'") == [[2]]
        !rows(file, 'SELECT completion_cid FROM run')*.get(0).contains(b[0].toString())
    }

    def 'the snapshot is one rollback-journal file of 4096-byte pages with the cache schema'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path
        final byte[] header = Files.newInputStream(file).withCloseable { it.readNBytes(100) }

        then:
        (((header[16] & 0xff) << 8) | (header[17] & 0xff)) == 4096
        header[18] == 1 && header[19] == 1
        ['-wal', '-shm', '-journal'].every { !Files.exists(file.resolveSibling(file.fileName.toString() + it)) }
        rows(file, 'SELECT version FROM schema_version') == [[Index.SCHEMA_VERSION]]
        rows(file, "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name") ==
            rows(index.file, "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        Files.list(file.parent).withCloseable { it.toList() }*.fileName*.toString() == ['v2.sqlite']
    }

    def 'meta carries the member watermark and the time it was written'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()

        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path
        final Map meta = rows(file, 'SELECT key, value FROM meta').collectEntries { [(it[0]): it[1]] }

        then:
        meta.keySet() == ['store_log_watermark', 'snapshot_written_at'] as Set
        meta.store_log_watermark == index.watermark('lab')
        meta.store_log_watermark ==~ /\d{13}-run-b[a-z2-7]{58}/
        meta.snapshot_written_at ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
    }

    def 'a member with no log has no watermark key'() {
        when:
        final Path file = IndexSnapshot.write(index, 'lab', labRoot, 0L).path

        then:
        rows(file, 'SELECT key FROM meta')*.get(0) == ['snapshot_written_at']
        rows(file, 'SELECT count(*) FROM run') == [[0]]
    }

    def 'an existing snapshot at or over the cap is left untouched'() {
        given:
        final Path file = IndexSnapshot.pathIn(labRoot)
        Files.createDirectories(file.parent)
        Files.write(file, ('x' * 100).bytes)

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 100L)

        then:
        !result.written
        result.skipped == 'over_cap'
        new String(Files.readAllBytes(file)) == 'x' * 100
    }

    def 'a new snapshot over the cap does not replace the old one'() {
        given:
        run(lab, 'a', ['A'])
        catchUp()
        final Path file = IndexSnapshot.pathIn(labRoot)
        Files.createDirectories(file.parent)
        Files.write(file, 'old'.bytes)

        when:
        final IndexSnapshot.Result result = IndexSnapshot.write(index, 'lab', labRoot, 1000L)

        then:
        result.skipped == 'over_cap'
        result.bytes > 1000L
        new String(Files.readAllBytes(file)) == 'old'
        Files.list(file.parent).withCloseable { it.toList() }.size() == 1
    }

    def 'concurrent writers leave a complete database, never a torn one (Review Focus 5)'() {
        given:
        ['A', 'B', 'C', 'D'].each { run(lab, "run-$it", [it]) }
        catchUp()
        final def pool = Executors.newFixedThreadPool(4)
        final CountDownLatch go = new CountDownLatch(1)
        final List<Throwable> failures = Collections.synchronizedList([])

        when:
        4.times {
            pool.submit {
                final Index own = Index.open(index.file)
                try {
                    go.await()
                    5.times { IndexSnapshot.write(own, 'lab', labRoot, 0L) }
                }
                catch( Throwable t ) {
                    failures << t
                }
                finally {
                    own.close()
                }
            }
        }
        go.countDown()
        pool.shutdown()
        pool.awaitTermination(60, TimeUnit.SECONDS)
        final Path file = IndexSnapshot.pathIn(labRoot)

        then:
        failures == []
        rows(file, 'PRAGMA integrity_check') == [['ok']]
        rows(file, 'SELECT count(*) FROM run') == [[4]]
        Files.list(file.parent).withCloseable { it.toList() }*.fileName*.toString() == ['v2.sqlite']
    }

    def 'the page is written beside the snapshot only when its bytes differ'() {
        given:
        final byte[] page = '<!doctype html><title>x</title>'.bytes

        expect:
        IndexSnapshot.writePage(labRoot, page)
        Files.readAllBytes(labRoot.resolve('index.html')) == page
        !IndexSnapshot.writePage(labRoot, page)
        IndexSnapshot.writePage(labRoot, '<!doctype html><title>y</title>'.bytes)
    }
}
