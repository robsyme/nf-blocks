package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 2, B2, under a heap too small for the storage figures. Runs only
 * under {@code ./gradlew memoryBoundTest} (48 MiB). A member with 250,000
 * file references needs well over 48 MiB to compute its figures (the
 * integrator measured about 1.3 KB of heap per reference on gen_year.py's
 * year), so writeStorage runs out of memory for real. The snapshot must still
 * be written, lean, and no Throwable may escape.
 */
class SnapshotHeapBoundTest extends Specification {
    @TempDir Path root

    static final int REFS = 250_000

    def 'storage figures that do not fit the heap give a lean snapshot, and no error escapes'() {
        given:
        assert Runtime.runtime.maxMemory() <= 64L << 20 : 'run this under ./gradlew memoryBoundTest'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Map run = f.run('a', [[s: f.raw('one')]])
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        addRefs(root.resolve('cache.sqlite'), ((List<Cid>) run.items)[0].toString(), REFS)

        when:
        def r = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve('out')), 0L, null, root.resolve('t'), null, false, 1000L, null)
        Path snap = IndexSnapshot.pathIn(root.resolve('out'))

        then:
        noExceptionThrown()
        r.written
        r.omitted == IndexSnapshot.OUT_OF_MEMORY
        value(snap, "SELECT value FROM meta WHERE key = 'storage_omitted'") == 'out_of_memory'
        value(snap, "SELECT value FROM meta WHERE key = 'snapshot_max_bytes'") == null
        value(snap, "SELECT value FROM meta WHERE key = 'sweep_grace_ms'") == '1000'
        value(snap, 'SELECT count(*) FROM run') == 1
        ['block', 'block_ref', 'root', 'store_stats', 'root_stats', 'item_size'].every { String t -> value(snap, "SELECT count(*) FROM ${t}") == 0 }

        and: 'the cache remembers, so the next write goes lean without running out again'
        index.metaValue(IndexSnapshot.OUT_OF_MEMORY_KEY + ':lab').startsWith("${REFS + 1} ")

        cleanup:
        index?.close()
    }

    private static Object value(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            return rs.next() ? rs.getObject(1) : null
        } finally { c.close() }
    }

    private static void addRefs(Path cache, String owner, int n) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${cache}")
        try {
            c.autoCommit = false
            final PreparedStatement ref = c.prepareStatement('INSERT INTO block_ref(owner_cid, cid, name) VALUES (?, ?, ?)')
            final PreparedStatement block = c.prepareStatement("INSERT INTO block(cid, size, codec) VALUES (?, ?, 'raw')")
            for( int i = 0; i < n; i++ ) {
                final String cid = Fixtures.contentCid("extra-$i").toString()
                ref.setString(1, owner); ref.setString(2, cid); ref.setString(3, "sample-${i}/reads.fastq.gz".toString()); ref.addBatch()
                block.setString(1, cid); block.setLong(2, 1000L + i); block.addBatch()
                if( i % 10_000 == 9_999 ) { ref.executeBatch(); block.executeBatch() }
            }
            ref.executeBatch(); block.executeBatch()
            c.commit()
        } finally { c.close() }
    }
}
