package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/**
 * P2 acceptance on the real roblab store: the snapshot reproduces the
 * figures worked out by hand from its blocks (constraints, "Real-store
 * acceptance"), and the cost of a cold rebuild after a schema bump.
 * Opt-in, since the store is local to one machine:
 *
 *   ./gradlew test --tests robsyme.cas.core.RoblabStorageTest -Droblab.store=/tmp/cas-exploration/store
 *
 * The store is copied first and never written. With -Droblab.out=<dir> the
 * snapshot is also copied to <dir>/index/v<SCHEMA_VERSION>.sqlite.
 */
@Requires({ System.getProperty('roblab.store') })
class RoblabStorageTest extends Specification {

    @TempDir Path tmp

    private static void copyTree(Path from, Path to) {
        Files.walk(from).withCloseable { stream ->
            stream.forEach { Path p ->
                final Path target = to.resolve(from.relativize(p).toString())
                if( Files.isDirectory(p) ) Files.createDirectories(target)
                else Files.copy(p, target)
            }
        }
    }

    private static List<Map> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final def md = rs.metaData
            final List<Map> out = []
            while( rs.next() )
                out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            return out
        }
        finally {
            c.close()
        }
    }

    private static Closure<Boolean> counting(BlockStore store, int[] calls) {
        return { Cid cid -> calls[0]++; store.has(cid) }
    }

    def 'the roblab snapshot reproduces the acceptance figures'() {
        given: 'a copy of the store, ingested from its blocks and Store Log'
        final Path source = Paths.get(System.getProperty('roblab.store'))
        final Path copy = tmp.resolve('store')
        copyTree(source, copy)
        final LocalBlockStore store = new LocalBlockStore(copy, 'roblab', true)
        Index index = Index.open(tmp.resolve('cache.sqlite'))
        index.catchUp(store, StoreLog.of(store), 'roblab')

        when: 'written as a session writes it, asking the store which releasing blocks are still there'
        final int[] looked = [0] as int[]
        final Closure<Boolean> present = counting(store, looked)
        IndexSnapshot.Result r = IndexSnapshot.write(index, 'roblab', new LocalSnapshotStorage(copy), 0L, null,
            tmp.resolve('t'), null, false, 14L * SweepPolicy.DAY, present, store.retentionStorage())
        Path snap = IndexSnapshot.pathIn(copy)
        Map s = rows(snap, 'SELECT * FROM store_stats')[0]
        Map<String, Map> runs = rows(snap, '''SELECT run.run_name, st.reachable_bytes, st.reachable_blocks, st.exclusive_bytes
                                                FROM root JOIN root_stats st ON st.root_id = root.id
                                                JOIN run ON run.completion_cid = root.cid WHERE root.kind = 'run' ''')
            .collectEntries { Map row -> [(row.run_name): row] }
        Path v3 = source.resolve('index/v3.sqlite')
        println "roblab store_stats: ${s}"
        println "roblab presence lookups: ${looked[0]}, absent: ${index.absentBlocks('roblab').size()}"
        println "roblab dedup: ${String.format('%.3f', StorageStatsTest.pageDedup(s))}"
        runs.keySet().sort().each { String n -> println "roblab run ${n}: ${runs[n]}" }
        println "roblab snapshot bytes: v${Index.SCHEMA_VERSION} ${Files.size(snap)}" + (Files.exists(v3) ? ", v3 ${Files.size(v3)}" : '')
        if( System.getProperty('roblab.out') ) {
            final Path out = Paths.get(System.getProperty('roblab.out')).resolve(IndexSnapshot.relativePath())
            Files.createDirectories(out.parent)
            Files.copy(snap, out, StandardCopyOption.REPLACE_EXISTING)
            println "roblab snapshot copied to ${out}"
        }

        // roblab is written to by its owner (a fifth run, crazy_miescher, arrived on 2026-10-25 at 16:50),
        // so whole-store totals keep moving and are not asserted. The historical acceptance figures,
        // measured before 2026-10-25 16:50, were stored 142,134,100 B in 1,285 blocks, referenced
        // 219,874,873 B, dedup 1.547 and shared 62,751,397 B (docs/design/reports/p2.md). What is
        // asserted is what adding runs cannot move: the acceptance runs' own figures, and invariants
        // that hold for any content. Roblab has no Handoff and no Selection item from another member,
        // so its dedup check exercises only R2's memberships; IndexSnapshotHandoffStatsTest and
        // IndexSnapshotStorageTest (R8) give the other references teeth.
        final Map blocks = rows(snap, '''SELECT count(*) AS n, coalesce(sum(size), 0) AS bytes FROM block
                                         WHERE codec = 'raw' ''')[0]
        println "roblab raw blocks: ${blocks}"

        then: 'complete, and with nothing releasing the store was asked nothing'
        r.written
        looked[0] == 0
        s.partial == 0

        and: 'stored is every raw block of the snapshot counted once (R1)'
        (s.stored_bytes as long) == (blocks.bytes as long)
        (s.stored_blocks as long) == (blocks.n as long)

        and: "the dedup the page shows from this row is at least 1x"
        StorageStatsTest.pageDedup(s) >= 1.0d

        and: 'reachable per acceptance run, which other runs cannot change'
        runs.distraught_murdock.subMap('reachable_bytes', 'reachable_blocks') == [reachable_bytes: 78_392_535, reachable_blocks: 894]
        runs.sad_yonath.subMap('reachable_bytes', 'reachable_blocks') == [reachable_bytes: 78_390_155, reachable_blocks: 894]
        runs.sharp_magritte.subMap('reachable_bytes', 'reachable_blocks') == [reachable_bytes: 53_400_488, reachable_blocks: 528]
        runs.sleepy_heyrovsky.reachable_bytes == 0

        and: 'frees on release (R4): a later run sharing their files can only lower these'
        (runs.distraught_murdock.exclusive_bytes as long) <= 63_743_945L
        (runs.sad_yonath.exclusive_bytes as long) <= 15_638_758L
        runs.sharp_magritte.exclusive_bytes == 0
        runs.sleepy_heyrovsky.exclusive_bytes == 0

        cleanup:
        index?.close()
    }

    /** The tables a rebuild after a schema bump fills, in DESIGN.md §12's order. */
    static final List<String> MAIN_TABLES = ['run', 'collection', 'item', 'collection_item', 'item_attr', 'producer', 'consumer',
                                             'log_entry', 'claim', 'claim_current', 'missing', 'block', 'block_ref',
                                             'transfer', 'transfer_root', 'project', 'run_input', 'handoff', 'meta']

    def 'a cold catch-up of the whole store, not seeded from any snapshot, and a snapshot write after it, timed'() {
        given: 'a copy of the store and an empty cache of this schema version'
        final Path copy = tmp.resolve('store')
        copyTree(Paths.get(System.getProperty('roblab.store')), copy)
        final LocalBlockStore store = new LocalBlockStore(copy, 'roblab', true)

        when: 'the catch-up a session does on a cache it had to recreate'
        final long t0 = System.nanoTime()
        Index index = Index.open(tmp.resolve('cache.sqlite'))
        index.catchUp(store, StoreLog.of(store), 'roblab')
        final long caughtUp = System.nanoTime()
        IndexSnapshot.Result r = IndexSnapshot.write(index, 'roblab', new LocalSnapshotStorage(copy), 0L, null,
            tmp.resolve('t'), null, false, 14L * SweepPolicy.DAY, counting(store, [0] as int[]), store.retentionStorage())
        final long written = System.nanoTime()
        final Path cacheFile = index.file
        final Path snap = IndexSnapshot.pathIn(copy)
        println "roblab v${Index.SCHEMA_VERSION} cold catch-up: ${(caughtUp - t0) / 1_000_000L} ms; snapshot write: ${(written - caughtUp) / 1_000_000L} ms"
        println "roblab v${Index.SCHEMA_VERSION} cache ${Files.size(cacheFile)} B, snapshot ${Files.size(snap)} B"
        for( String t : MAIN_TABLES )
            println "roblab rows ${t}: cache ${rows(cacheFile, "SELECT count(*) AS n FROM ${t}")[0].n}, snapshot ${rows(snap, "SELECT count(*) AS n FROM ${t}")[0].n}"

        then: 'every table there, the cache not seeded, and the snapshot written'
        index.metaValue('seeded_from:roblab') == null
        rows(cacheFile, 'SELECT version FROM schema_version') == [[version: Index.SCHEMA_VERSION]]
        r.written

        cleanup:
        index?.close()
    }
}
