package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 2, B2: a member whose storage figures would pass the cap, or
 * whose figures run out of memory, gets a lean v4 snapshot: its records, no
 * content edges and no storage rows, and meta storage_omitted saying why.
 * The size is estimated before the content is copied, from the member's
 * block_ref rows times a bytes-per-ref figure the cache learns from each
 * full build.
 */
class IndexSnapshotLeanTest extends Specification {
    @TempDir Path root

    /** The tables a lean snapshot leaves empty: the content edges and every storage table. */
    static final List<String> LEAN_EMPTY = ['block', 'block_ref', 'root', 'root_stats', 'root_shared', 'store_stats', 'asserter_stats',
                                            'pipeline_stats', 'collection_size', 'item_size', 'kind_stats', 'pinned_overlap']

    RetentionFixture f
    Map run
    Index index

    def setup() {
        f = new RetentionFixture(root.resolve('lab'))
        run = f.run('a', [[s: f.raw('one')], [t: f.raw('two')]])
        index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
    }

    def cleanup() {
        index?.close()
    }

    private static List<Map> rows(Path db, String sql) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def rs = c.createStatement().executeQuery(sql); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    private static Map<String, String> meta(Path db) {
        rows(db, 'SELECT key, value FROM meta').collectEntries { Map r -> [(r.key): r.value] } as Map<String, String>
    }

    /** n more file references under the run's first item, straight into the cache, as a large member's would be. */
    private void addRefs(int n) {
        final String owner = ((List<Cid>) run.items)[0].toString()
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${root.resolve('cache.sqlite')}")
        try {
            c.autoCommit = false
            final PreparedStatement ref = c.prepareStatement('INSERT INTO block_ref(owner_cid, cid, name) VALUES (?, ?, ?)')
            final PreparedStatement block = c.prepareStatement("INSERT INTO block(cid, size, codec) VALUES (?, ?, 'raw')")
            for( int i = 0; i < n; i++ ) {
                final String cid = Fixtures.contentCid("extra-$i").toString()
                ref.setString(1, owner); ref.setString(2, cid); ref.setString(3, "f${i}.txt".toString()); ref.addBatch()
                block.setString(1, cid); block.setLong(2, 1000L + i); block.addBatch()
            }
            ref.executeBatch(); block.executeBatch()
            c.commit()
        } finally { c.close() }
    }

    private void setCacheMeta(String key, String value) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${root.resolve('cache.sqlite')}")
        try {
            final PreparedStatement s = c.prepareStatement('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)')
            s.setString(1, key); s.setString(2, value); s.execute()
        } finally { c.close() }
    }

    private IndexSnapshot.Result write(String out, long maxBytes, Closure<Boolean> present = null, Path temp = root.resolve('t')) {
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve(out)), maxBytes, null, temp, null, false, 1000L, present)
    }

    private Path snap(String out) { IndexSnapshot.pathIn(root.resolve(out)) }

    private static void assertLean(Path snap, String reason, long cap) {
        assert meta(snap)[IndexSnapshot.STORAGE_OMITTED_KEY] == reason
        // The cap in force, and none at all when there is none: never '0'.
        assert meta(snap)[IndexSnapshot.MAX_BYTES_KEY] == (cap > 0 ? String.valueOf(cap) : null)
        assert meta(snap)[IndexSnapshot.SWEEP_GRACE_KEY] == '1000'
        for( String table : LEAN_EMPTY )
            assert rows(snap, "SELECT count(*) AS n FROM ${table}")[0].n == 0 : "${table} is not empty in a lean snapshot"
        assert rows(snap, 'SELECT count(*) AS n FROM run')[0].n == 1
        assert rows(snap, 'SELECT count(*) AS n FROM item')[0].n == 2
    }

    def 'a full snapshot says nothing is omitted'() {
        when:
        def r = write('out', 0L)

        then:
        r.written
        r.omitted == null
        !meta(snap('out')).containsKey(IndexSnapshot.STORAGE_OMITTED_KEY)
        rows(snap('out'), 'SELECT count(*) AS n FROM store_stats')[0].n == 1
    }

    def 'a member whose estimate passes the cap gets a lean snapshot, never a full one built and thrown away'() {
        given: '10,000 references: about 4.26 MB with the lean part, at the starting 400 B per reference'
        addRefs(10_000)
        final long cap = 4L << 20

        when:
        def r = write('out', cap)

        then:
        r.written
        r.omitted == IndexSnapshot.OVER_CAP
        !r.seededLean
        r.bytes < cap
        assertLean(snap('out'), 'over_cap', cap)

        and: 'no full build ran, so nothing was learned'
        index.metaValue(IndexSnapshot.BYTES_PER_REF_KEY + ':lab') == null
    }

    def 'a member over the old 860 B estimate but under its lean part plus 400 B per reference builds full, with nothing learned first'() {
        given: 'L + refs x 400 is about 4.26 MB, L + refs x 860 about 8.86 MB, and the full snapshot about 3.8 MB'
        addRefs(10_000)
        final long cap = 6L << 20

        when:
        def r = write('out', cap)

        then:
        r.written
        r.omitted == null
        r.bytes < cap
        rows(snap('out'), 'SELECT count(*) AS n FROM block_ref')[0].n == 10_002
    }

    def 'a member of a few thousand references teaches its figure too'() {
        given:
        addRefs(2_000)

        when:
        write('out', 0L)

        then:
        (index.metaValue(IndexSnapshot.BYTES_PER_REF_KEY + ':lab') as long) in 1L..<IndexSnapshot.DEFAULT_BYTES_PER_REF
    }

    def 'a member sent lean by its estimate builds full on the next write once its measured lean part shows room'() {
        given: 'run a with 10,002 references, and run b with 1,000 items that make the lean part large'
        addRefs(10_000)
        Map b = f.run('b', (1..1000).collect { int i -> [("f${i}".toString()): f.raw("b file ${i}")] })
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        final long leanWithB = write('probe', 1L).bytes
        final long cap = leanWithB + 11_002L * 400 - 1

        when: 'the estimate with run b passes the cap'
        def first = write('out', cap)

        then:
        first.written
        first.omitted == IndexSnapshot.OVER_CAP

        when: 'run b is forgotten, so the lean part this build measures is smaller'
        index.forget([(Cid) b.completion])
        def next = write('out', cap)

        then:
        next.written
        next.omitted == null
        next.bytes < cap
        !meta(snap('out')).containsKey(IndexSnapshot.STORAGE_OMITTED_KEY)
    }

    def 'a full build teaches the cache its bytes per reference, and a member that fits then gets its figures'() {
        given:
        addRefs(10_000)
        final long cap = 4L << 20

        when: 'a full build at any size'
        def full = write('full', 0L)
        final long learned = index.metaValue(IndexSnapshot.BYTES_PER_REF_KEY + ':lab') as long

        then:
        full.written
        full.omitted == null
        full.bytes < cap
        learned > 0 && learned < 860

        when: 'the same cap that gave a lean snapshot before'
        def r = write('out', cap)

        then:
        r.written
        r.omitted == null
        rows(snap('out'), 'SELECT count(*) AS n FROM block_ref')[0].n == 10_002
        !meta(snap('out')).containsKey(IndexSnapshot.STORAGE_OMITTED_KEY)
    }

    def 'a full build that passes the cap after all is written lean, and the figure is corrected'() {
        given: 'a figure far too low, so the estimate fits'
        addRefs(10_000)
        final long full = write('full', 0L).bytes
        setCacheMeta(IndexSnapshot.BYTES_PER_REF_KEY + ':lab', '1')

        when:
        def r = write('out', full - 1)

        then:
        r.written
        r.omitted == IndexSnapshot.OVER_CAP
        assertLean(snap('out'), 'over_cap', full - 1)
        (index.metaValue(IndexSnapshot.BYTES_PER_REF_KEY + ':lab') as long) > 1
    }

    def 'a lean snapshot still over the cap is over_cap, and later writes do not build again until the runs shrink or the cap changes'() {
        when:
        def first = write('out', 1L)

        then:
        !first.written
        first.skipped == IndexSnapshot.OVER_CAP
        first.omitted == IndexSnapshot.OVER_CAP
        !Files.exists(snap('out'))

        when: 'the same cap again: nothing is built, so the temp directory is never made'
        def again = write('out', 1L, null, root.resolve('never'))

        then:
        again.skipped == IndexSnapshot.OVER_CAP
        again.omitted == IndexSnapshot.OVER_CAP
        !Files.exists(root.resolve('never'))

        when: 'another run grows the member: still nothing built'
        Map grownRun = f.run('b', [[u: f.raw('three')]])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        def grown = write('out', 1L, null, root.resolve('never'))

        then:
        grown.skipped == IndexSnapshot.OVER_CAP
        !Files.exists(root.resolve('never'))

        when: 'forgets take the runs: the verdict no longer holds'
        index.forget([(Cid) run.completion, (Cid) grownRun.completion])
        def fewer = write('out', 1L, null, root.resolve('built'))

        then:
        fewer.skipped == IndexSnapshot.OVER_CAP
        Files.exists(root.resolve('built'))

        when: 'a cap the lean snapshot fits under'
        def fits = write('out', 64L << 20)

        then:
        fits.written
    }

    def 'a rebuild ignores the over-cap verdict'() {
        given:
        write('out', 1L)

        when:
        def r = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve('out')), 1L, null, root.resolve('built'), null, true, 1000L, null)

        then:
        r.skipped == IndexSnapshot.OVER_CAP
        Files.exists(root.resolve('built'))
    }

    def 'storage figures that run out of memory give a lean snapshot, and no error escapes'() {
        given: 'a released run, so the build asks the store about its blocks; the question runs out of memory'
        f.claim((Cid) run.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        // The run's two files are asked on the lookup pool, so both lookups may start before the Error stops the rest.
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger()
        Closure<Boolean> oom = { Cid c -> calls.incrementAndGet(); throw new OutOfMemoryError('Java heap space') } as Closure<Boolean>

        when:
        def r = write('out', 0L, oom)
        int first = calls.get()

        then:
        noExceptionThrown()
        r.written
        r.omitted == IndexSnapshot.OUT_OF_MEMORY
        assertLean(snap('out'), 'out_of_memory', 0L)
        rows(snap('out'), 'SELECT count(*) AS n FROM claim')[0].n == 1
        first >= 1

        when: 'the next write, at the same size and heap, goes lean without trying again'
        def again = write('again', 0L, oom)

        then:
        again.written
        again.omitted == IndexSnapshot.OUT_OF_MEMORY
        calls.get() == first
        assertLean(snap('again'), 'out_of_memory', 0L)
    }

    def 'out of memory with a content missing row leaves that row out with the edges'() {
        given: 'a released run whose item names a directory that has not arrived'
        final Cid dir = Fixtures.cidOf([kind: 'DirectoryManifest', schema: 1, entries: []])
        final Cid item = f.store.putDagCbor(Fixtures.outputItem2([[sample: 'dir'], Fixtures.leaf2('reads', dir, 0)]))
        final Cid manifest = f.store.putDagCbor(Fixtures.runManifest(run_name: 'b', nf_run_hash: 'hash-b'))
        final Cid coll = f.store.putDagCbor(Fixtures.outputCollection(manifest, 'out', [[item, ['out/reads']]]))
        final Cid completion = f.store.putDagCbor(Fixtures.runCompletion(manifest, [coll]))
        StoreLog.append(f.store, StoreLogKind.RUN, completion, ++f.clock)
        f.claim(completion, 'set', 'retain', 'lineage')
        // Run a released too, so the build has files to ask the store about.
        f.claim((Cid) run.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert rows(root.resolve('cache.sqlite'), "SELECT count(*) AS n FROM missing WHERE have_cid = '${item}'")[0].n == 1

        when:
        def r = write('out', 0L, { Cid c -> throw new OutOfMemoryError('Java heap space') } as Closure<Boolean>)

        then:
        r.omitted == IndexSnapshot.OUT_OF_MEMORY
        rows(snap('out'), "SELECT count(*) AS n FROM missing WHERE have_cid = '${item}'")[0].n == 0
        rows(snap('out'), 'SELECT count(*) AS n FROM block_ref')[0].n == 0
    }

    def 'a cache seeded from a lean snapshot keeps writing lean ones, for the same reason, until it is rebuilt'() {
        given:
        addRefs(10_000)
        write('out', 4L << 20)
        assert meta(snap('out'))[IndexSnapshot.STORAGE_OMITTED_KEY] == 'over_cap'
        Index seeded = Index.open(root.resolve('seeded.sqlite'))

        when:
        seeded.seedFrom(snap('out'), 'lab')
        def r = IndexSnapshot.write(seeded, 'lab', new LocalSnapshotStorage(root.resolve('again')), 0L, null, root.resolve('t'), null, false, 1000L, null)

        then: 'no content edges were seeded, so figures from them would be wrong'
        rows(root.resolve('seeded.sqlite'), 'SELECT count(*) AS n FROM block_ref')[0].n == 0
        r.written
        r.omitted == IndexSnapshot.SEEDED_LEAN
        r.seededLean
        assertLean(snap('again'), 'seeded_lean', 0L)

        when: 'with a cap, the cap in force is the one written'
        def capped = IndexSnapshot.write(seeded, 'lab', new LocalSnapshotStorage(root.resolve('capped')), 64L << 20, null, root.resolve('t'), null, false, 1000L, null)

        then: 'still seeded_lean, never the reason the seeding snapshot had'
        capped.written
        capped.omitted == IndexSnapshot.SEEDED_LEAN
        assertLean(snap('capped'), 'seeded_lean', 64L << 20)

        when: 'a reset, as reindex does, then a full ingest'
        seeded.reset()
        seeded.catchUp(f.store, StoreLog.of(f.store), 'lab')
        def full = IndexSnapshot.write(seeded, 'lab', new LocalSnapshotStorage(root.resolve('full')), 0L, null, root.resolve('t'), null, true, 1000L, null)

        then:
        full.written
        full.omitted == null

        cleanup:
        seeded?.close()
    }

    /** A cache seeded from a lean snapshot of the lab (fix round 3, item 3). */
    private Index leanSeeded() {
        addRefs(10_000)
        write('seed', 4L << 20)
        assert meta(snap('seed'))[IndexSnapshot.STORAGE_OMITTED_KEY] == 'over_cap'
        Index seeded = Index.open(root.resolve('seeded.sqlite'))
        seeded.seedFrom(snap('seed'), 'lab')
        return seeded
    }

    /** A SnapshotStorage that counts its fetches, over a local one, whose base may hide whether it has figures. */
    static class Counting implements SnapshotStorage {
        @Delegate(excludes = ['fetchVersion']) final LocalSnapshotStorage local
        int fetches
        boolean failing
        Counting(Path memberRoot) { local = new LocalSnapshotStorage(memberRoot) }
        Path fetchVersion(SnapshotBase base, Path tempDir) {
            fetches++
            if( failing ) throw new IOException('503 Slow Down')
            local.fetchVersion(base, tempDir)
        }
    }

    def 'baseOf says whether a snapshot has storage figures'() {
        given:
        write('full', 0L)
        addRefs(10_000)
        write('lean', 4L << 20)

        expect:
        new LocalSnapshotStorage(root.resolve('full')).base(root.resolve('t')).figures == Boolean.TRUE
        new LocalSnapshotStorage(root.resolve('lean')).base(root.resolve('t')).figures == Boolean.FALSE
    }

    def 'a lean-seeded cache does not replace a snapshot with storage figures, even on a rebuild, and builds nothing'() {
        given: 'a snapshot with figures there'
        write('out', 0L)
        byte[] before = Files.readAllBytes(snap('out'))
        Index seeded = leanSeeded()
        LocalSnapshotStorage storage = new LocalSnapshotStorage(root.resolve('out'))

        when:
        def r = IndexSnapshot.write(seeded, 'lab', storage, 0L, storage.base(root.resolve('t')), root.resolve('never'), null, rebuild, 1000L, null)

        then:
        !r.written
        r.skipped == IndexSnapshot.SEEDED_LEAN_BASE_FULL
        r.omitted == IndexSnapshot.SEEDED_LEAN
        r.seededLean
        IndexSnapshot.skipAdvice(r).contains('`nextflow plugin nf-blocks:reindex --seed false`')
        Files.readAllBytes(snap('out')) == before
        !Files.exists(root.resolve('never'))

        cleanup:
        seeded?.close()

        where:
        rebuild << [false, true]
    }

    def 'a lean-seeded cache still replaces a lean snapshot, and a first write'() {
        given:
        Index seeded = leanSeeded()
        LocalSnapshotStorage lean = new LocalSnapshotStorage(root.resolve('seed'))

        when:
        def r = IndexSnapshot.write(seeded, 'lab', lean, 0L, lean.base(root.resolve('t')), root.resolve('t'), null, false, 1000L, null)
        def first = IndexSnapshot.write(seeded, 'lab', new LocalSnapshotStorage(root.resolve('none')), 0L, null, root.resolve('t'), null, false, 1000L, null)

        then:
        r.written
        r.omitted == IndexSnapshot.SEEDED_LEAN
        meta(snap('seed'))[IndexSnapshot.STORAGE_OMITTED_KEY] == 'seeded_lean'
        first.written
        IndexSnapshot.skipAdvice(r) == ''

        cleanup:
        seeded?.close()
    }

    def 'a base that does not say whether it has figures is fetched and read, only for a lean-seeded cache'() {
        given:
        write('out', 0L)
        Index seeded = leanSeeded()
        Counting storage = new Counting(root.resolve('out'))
        SnapshotBase base = storage.base(root.resolve('t'))
        base.figures = null

        when: 'a cache that is not lean-seeded writes: no fetch'
        def plain = IndexSnapshot.write(index, 'lab', storage, 0L, base, root.resolve('t'), null, true, 1000L, null)

        then:
        plain.written
        storage.fetches == 0

        when: 'the lean-seeded one: one fetch, and the snapshot with figures stays'
        base = storage.base(root.resolve('t'))
        base.figures = null
        def r = IndexSnapshot.write(seeded, 'lab', storage, 0L, base, root.resolve('t'), null, false, 1000L, null)

        then:
        r.skipped == IndexSnapshot.SEEDED_LEAN_BASE_FULL
        storage.fetches == 1
        rows(snap('out'), 'SELECT count(*) AS n FROM store_stats')[0].n == 1
        seeded.metaValue(IndexSnapshot.BASE_FIGURES_KEY + ':lab') == "${base.tag} true".toString()

        when: 'the next write of the same base reads the answer from the cache'
        def next = IndexSnapshot.write(seeded, 'lab', storage, 0L, base, root.resolve('t'), null, false, 1000L, null)

        then:
        next.skipped == IndexSnapshot.SEEDED_LEAN_BASE_FULL
        storage.fetches == 1

        cleanup:
        seeded?.close()
    }

    def 'a base fetch that fails does not fail the write, and keeps the base: seeded_lean_base_unknown'() {
        given:
        write('out', 0L)
        byte[] before = Files.readAllBytes(snap('out'))
        Index seeded = leanSeeded()
        Counting storage = new Counting(root.resolve('out'))
        storage.failing = true
        SnapshotBase base = storage.base(root.resolve('t'))
        base.figures = null

        when:
        def r = IndexSnapshot.write(seeded, 'lab', storage, 0L, base, root.resolve('t'), null, false, 1000L, null)

        then:
        noExceptionThrown()
        storage.fetches == 1
        !r.written
        r.skipped == IndexSnapshot.SEEDED_LEAN_BASE_UNKNOWN
        r.seededLean
        IndexSnapshot.skipAdvice(r).contains('`nextflow plugin nf-blocks:reindex --seed false`')
        Files.readAllBytes(snap('out')) == before
        seeded.metaValue(IndexSnapshot.BASE_FIGURES_KEY + ':lab') == null

        cleanup:
        seeded?.close()
    }

    private static ListAppender<ILoggingEvent> captureSnapshotLog() {
        final ListAppender<ILoggingEvent> a = new ListAppender<ILoggingEvent>()
        a.start()
        ((Logger) LoggerFactory.getLogger(IndexSnapshot)).addAppender(a)
        return a
    }

    private static void release(ListAppender<ILoggingEvent> a) {
        ((Logger) LoggerFactory.getLogger(IndexSnapshot)).detachAppender(a)
    }

    def 'a heap that runs out is remembered, and both warnings advise more heap through NXF_OPTS (fix round 3, item 8)'() {
        given:
        f.claim((Cid) run.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Closure<Boolean> oom = { Cid c -> throw new OutOfMemoryError(message) } as Closure<Boolean>
        ListAppender<ILoggingEvent> logged = captureSnapshotLog()

        when:
        def r = write('out', 0L, oom)
        def again = write('again', 0L, { Cid c -> true } as Closure<Boolean>)

        then:
        r.omitted == IndexSnapshot.OUT_OF_MEMORY
        r.heapExhausted
        index.metaValue(IndexSnapshot.OUT_OF_MEMORY_KEY + ':lab') != null
        again.omitted == IndexSnapshot.OUT_OF_MEMORY
        again.heapExhausted
        logged.list.count { ILoggingEvent e -> e.level == Level.WARN && e.formattedMessage.contains('ran out of memory') &&
            e.formattedMessage.contains('NXF_OPTS=-Xmx4g') } == 1
        logged.list.count { ILoggingEvent e -> e.formattedMessage.contains('an earlier build ran out of heap') &&
            e.formattedMessage.contains('NXF_OPTS=-Xmx4g') } == 1

        cleanup:
        release(logged)

        where:
        message << ['Java heap space', 'GC overhead limit exceeded']
    }

    def 'an OutOfMemoryError that is not the heap gives a lean snapshot but is not remembered (fix round 3, item 8)'() {
        given:
        f.claim((Cid) run.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Closure<Boolean> oom = { Cid c -> throw new OutOfMemoryError(message) } as Closure<Boolean>
        ListAppender<ILoggingEvent> logged = captureSnapshotLog()

        when:
        def r = write('out', 0L, oom)

        then: 'lean this time, with nothing remembered and no advice of more heap'
        noExceptionThrown()
        r.omitted == IndexSnapshot.OUT_OF_MEMORY
        !r.heapExhausted
        index.metaValue(IndexSnapshot.OUT_OF_MEMORY_KEY + ':lab') == null
        logged.list.count { ILoggingEvent e -> e.level == Level.WARN && e.formattedMessage.contains('the next build tries them again') &&
            !e.formattedMessage.contains('NXF_OPTS') } == 1

        when: 'the next build tries the figures again, and asks about both files'
        java.util.concurrent.atomic.AtomicInteger asked = new java.util.concurrent.atomic.AtomicInteger()
        def again = write('again', 0L, { Cid c -> asked.incrementAndGet(); true } as Closure<Boolean>)

        then:
        again.omitted == null
        asked.get() == 2

        cleanup:
        release(logged)

        where:
        message << ['unable to create native thread: possibly out of memory or process/resource limits reached', 'Metaspace', null]
    }
}
