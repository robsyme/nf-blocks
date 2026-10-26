package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 2, B1: a sweep takes the files only a released run held, but the
 * run keeps its records and the cache its block and block_ref rows. The
 * snapshot build looks up the blocks only released or deleted roots reach,
 * and leaves a gone one out of every storage figure. The first test is the
 * integrator's reproduction (ReviewSweptTest).
 */
class IndexSnapshotSweptTest extends Specification {
    @TempDir Path root

    private static List<Map> rows(Path db, String sql) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def rs = c.createStatement().executeQuery(sql); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    private static void ageEverything(RetentionFixture f) {
        long aged = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c -> Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(aged)) }
    }

    /** Two sweeps, the second past the first's ledger, so the trashed blocks are deleted. */
    private static void sweep(RetentionFixture f) {
        def say = { String msg -> null } as Closure<Void>
        def sleeper = { long ms -> null } as Closure<Void>
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        RetentionStorage s = f.store.retentionStorage()
        for( String name : s.listLedgers() ) {
            final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
            final TrashLedger past = new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)
            s.writeLedger(past.name, past.toJson()); if( past.name != name ) s.deleteLedger(name)
        }
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        f.settleSweepLock()
    }

    /**
     * Lab with run a (shared) and run b (shared + only), b released and swept,
     * so `only` is gone. The fixture's Leaves all say 1 byte.
     */
    private Map sweptLab() {
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid shared = f.raw('shared')
        Cid only = f.raw('only b')
        Map a = f.run('a', [[s: shared]])
        Map b = f.run('b', [[s: shared, u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageEverything(f)
        sweep(f)
        assert !f.store.has(only)
        assert f.store.has(shared)
        [f: f, shared: shared, only: only, a: a, b: b]
    }

    /** A presence function over a store that counts its calls, and the most it saw running at once. */
    static class Counter {
        final AtomicInteger n = new AtomicInteger()
        final AtomicInteger running = new AtomicInteger()
        final AtomicInteger most = new AtomicInteger()
        final Closure<Boolean> present

        Counter(BlockStore store, long pauseMillis = 0L, Set<Cid> failing = [] as Set) {
            present = { Cid c ->
                n.incrementAndGet()
                final int now = running.incrementAndGet()
                most.accumulateAndGet(now) { int a, int b -> Math.max(a, b) }
                try {
                    if( pauseMillis ) Thread.sleep(pauseMillis)
                    if( failing.contains(c) ) throw new IOException('403')
                    return store.has(c)
                }
                finally {
                    running.decrementAndGet()
                }
            } as Closure<Boolean>
        }

        int getCalls() { n.get() }
        void setCalls(int v) { n.set(v) }
    }

    private static Counter counting(RetentionFixture f) { new Counter(f.store) }

    private IndexSnapshot.Result write(Index index, String out, Closure<Boolean> present) {
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve(out)), 0L, null, root.resolve('t'), null, false, 1000L, present,
            new LocalRetentionStorage(root.resolve('lab')))
    }

    private Path snap(String out) { IndexSnapshot.pathIn(root.resolve(out)) }

    private static List<String> absentRows(Path cache) {
        rows(cache, 'SELECT member, cid FROM block_absent ORDER BY cid').collect { Map r -> "${r.member} ${r.cid}".toString() }
    }

    def 'after a sweep reclaims a released run, the snapshot no longer counts its bytes'() {
        given:
        Map lab = sweptLab()
        RetentionFixture f = lab.f

        when: 'a fresh cache ingests the swept store and writes the snapshot'
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        def r = write(index, 'out', f.store.&has)
        Path snap = snap('out')

        then:
        r.written
        rows(snap, 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, releasing_bytes: 0]

        and: 'every other storage figure leaves the gone block out'
        rows(snap, 'SELECT stored_blocks, referenced_bytes, held_bytes, pinned_bytes, shared_bytes FROM store_stats')[0] ==
            [stored_blocks: 1, referenced_bytes: 2, held_bytes: 1, pinned_bytes: 0, shared_bytes: 1]
        rows(snap, """SELECT r.cid, r.state, s.reachable_bytes, s.reachable_blocks, s.exclusive_bytes FROM root r
                      JOIN root_stats s ON s.root_id = r.id ORDER BY r.state""") == [
            [cid: lab.a.completion.toString(), state: 'held', reachable_bytes: 1, reachable_blocks: 1, exclusive_bytes: 1],
            [cid: lab.b.completion.toString(), state: 'released', reachable_bytes: 1, reachable_blocks: 1, exclusive_bytes: 0],
        ]
        rows(snap, 'SELECT asserted_by, reachable_bytes, releasing_bytes FROM asserter_stats') ==
            [[asserted_by: 'test', reachable_bytes: 1, releasing_bytes: 0]]
        rows(snap, 'SELECT bytes, blocks, releasing_bytes FROM pipeline_stats').every { Map p -> p.releasing_bytes == 0 && p.blocks <= 1 }
        rows(snap, 'SELECT sum(files) AS files, sum(bytes) AS bytes FROM kind_stats')[0] == [files: 1, bytes: 1]

        and: 'item_size is what is stored here (follow-up B2); collection_size stays what the records name'
        rows(snap, 'SELECT max(bytes) AS bytes, max(blocks) AS blocks FROM item_size')[0] == [bytes: 1, blocks: 1]
        rows(snap, 'SELECT max(bytes) AS bytes FROM collection_size')[0] == [bytes: 2]

        and: 'the absence is in the cache, never in the snapshot'
        absentRows(root.resolve('cache.sqlite')) == ["lab ${lab.only}".toString()]
        rows(snap, "SELECT name FROM sqlite_master WHERE name = 'block_absent'") == []

        cleanup:
        index?.close()
    }

    def 'without a presence function nothing is looked up, as before'() {
        given:
        Map lab = sweptLab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(lab.f.store, StoreLog.of(lab.f.store), 'lab')

        when:
        write(index, 'out', null)

        then:
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 1]
        absentRows(root.resolve('cache.sqlite')) == []

        cleanup:
        index?.close()
    }

    def 'a block found absent is not looked up again; a present one is, at every build'() {
        given: 'run c released too, its block still in the store'
        Map lab = sweptLab()
        RetentionFixture f = lab.f
        Cid kept = f.raw('kept by c')
        Map c = f.run('c', [[k: kept]])
        f.claim(c.completion, 'set', 'retain', 'lineage')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Counter counter = counting(f)

        when:
        write(index, 'out', counter.present)

        then: 'one look each at the gone block, the releasing one, and shared, which held run a reaches under once-released run b'
        counter.calls == 3
        index.presentBlocks('lab') == [lab.shared.toString()] as Set
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 1]

        when:
        counter.calls = 0
        write(index, 'out', counter.present)

        then: 'only the releasing present one is asked again'
        counter.calls == 1
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 1]

        cleanup:
        index?.close()
    }

    def 'with nothing releasing, the build asks the store nothing'() {
        given:
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        f.run('a', [[s: f.raw('shared')]])
        f.run('b', [[s: f.raw('shared'), u: f.raw('only b')]])
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Counter counter = counting(f)

        when:
        def r = write(index, 'out', counter.present)

        then:
        r.written
        counter.calls == 0
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 0]

        cleanup:
        index?.close()
    }

    def 'a lookup that fails counts the block as present and records nothing'() {
        given:
        Map lab = sweptLab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(lab.f.store, StoreLog.of(lab.f.store), 'lab')

        when:
        write(index, 'out', { Cid c -> throw new IOException('403') } as Closure<Boolean>)

        then:
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 1]
        absentRows(root.resolve('cache.sqlite')) == []

        cleanup:
        index?.close()
    }

    def 'a cache seeded from the snapshot looks once, then remembers'() {
        given: 'the snapshot written by one cache'
        Map lab = sweptLab()
        RetentionFixture f = lab.f
        Index writer = Index.open(root.resolve('writer.sqlite'))
        writer.catchUp(f.store, StoreLog.of(f.store), 'lab')
        write(writer, 'lab-out', f.store.&has)
        writer.close()

        and: 'a second cache seeded from it'
        Index seeded = Index.open(root.resolve('seeded.sqlite'))
        assert seeded.seedFrom(snap('lab-out'), 'lab')
        Counter counter = counting(f)

        when:
        write(seeded, 'out', counter.present)

        then: 'the gone block, and shared once, since held run a reaches it under once-released run b'
        absentRows(root.resolve('seeded.sqlite')) == ["lab ${lab.only}".toString()]
        counter.calls == 2
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, releasing_bytes: 0]

        when:
        counter.calls = 0
        write(seeded, 'out', counter.present)

        then:
        counter.calls == 0
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, releasing_bytes: 0]

        cleanup:
        seeded?.close()
    }

    def 'a new run that writes the block again clears its absence'() {
        given:
        Map lab = sweptLab()
        RetentionFixture f = lab.f
        Path cache = root.resolve('cache.sqlite')
        Index index = Index.open(cache)
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        write(index, 'out', f.store.&has)
        assert absentRows(cache) == ["lab ${lab.only}".toString()]

        when: 'run c writes the same file again, in a new item, and is ingested'
        assert f.raw('only b') == lab.only
        Map c = f.run('c', [[again: lab.only]])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then:
        absentRows(cache) == []

        when: 'c is released too: the block counts as releasing again'
        f.claim(c.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        write(index, 'out', f.store.&has)

        then:
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 2, releasing_bytes: 1]

        cleanup:
        index?.close()
    }

    def 'an existing cache of this schema version gains block_absent when opened'() {
        given: 'a v4 cache written before the table existed'
        Path cache = root.resolve('old.sqlite')
        Index.open(cache).close()
        DriverManager.getConnection("jdbc:sqlite:${cache}").withCloseable { it.createStatement().execute('DROP TABLE block_absent') }

        when:
        Index index = Index.open(cache)
        index.recordAbsent('lab', ['bafkreiexample'])

        then:
        index.absentBlocks('lab') == ['bafkreiexample'] as Set
        index.absentBlocks('core') == [] as Set

        when: 'a reset empties it with the rest'
        index.reset()

        then:
        index.absentBlocks('lab').isEmpty()

        cleanup:
        index?.close()
    }

    def 'lookups run at most 16 at once, all asked, and one that fails still counts as present'() {
        given: 'a released run of 40 files: 20 swept (deleted by hand), 2 of the rest unreadable'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        List<Cid> files = (1..40).collect { int i -> f.raw("file ${i}".toString()) }
        Map gone = f.run('gone', [files.withIndex().collectEntries { Cid c, int i -> [("f${i}".toString()): c] }])
        f.claim(gone.completion, 'set', 'retain', 'lineage')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        files.take(20).each { Cid c -> Files.delete(((LocalBlockStore) f.store).blockPath(c)) }
        Set<Cid> unreadable = [files[0], files[30]] as Set
        Counter counter = new Counter(f.store, 30L, unreadable)

        when:
        write(index, 'out', counter.present)

        then:
        counter.calls == 40
        counter.most.get() > 1
        counter.most.get() <= 16
        index.absentBlocks('lab') == files.subList(1, 20).collect { it.toString() } as Set
        rows(snap('out'), 'SELECT stored_blocks, releasing_bytes FROM store_stats')[0] == [stored_blocks: 21, releasing_bytes: 21]

        cleanup:
        index?.close()
    }

    def 'a recorded absence a held root reaches is looked at again and dropped when present; one no root reaches is dropped unasked'() {
        given: 'the swept lab, plus recorded absences for a block run a holds and for one no root reaches'
        Map lab = sweptLab()
        RetentionFixture f = lab.f
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        write(index, 'out', f.store.&has)
        index.recordAbsent('lab', [lab.shared.toString(), 'bafkreinorootreachesthis'])
        Counter counter = counting(f)

        when:
        write(index, 'out', counter.present)

        then: 'only the swept block stays recorded; shared, which run a holds, was asked once and is there'
        index.absentBlocks('lab') == [lab.only.toString()] as Set
        counter.calls == 1
        rows(snap('out'), 'SELECT stored_bytes, held_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, held_bytes: 1, releasing_bytes: 0]

        when: 'run a is released too: shared gets one fresh look'
        f.claim(lab.a.completion, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        counter.calls = 0
        write(index, 'out', counter.present)

        then:
        counter.calls == 1
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, releasing_bytes: 1]

        cleanup:
        index?.close()
    }

    def 'a cache whose block_absent cannot be made opens with its rows, and the build still checks'() {
        given: 'a v4 cache with a run, without the table, and another connection holding the write lock'
        Map lab = sweptLab()
        Path cache = root.resolve('old.sqlite')
        Index first = Index.open(cache)
        first.catchUp(lab.f.store, StoreLog.of(lab.f.store), 'lab')
        first.close()
        DriverManager.getConnection("jdbc:sqlite:${cache}").withCloseable { it.createStatement().execute('DROP TABLE block_absent') }
        def locker = DriverManager.getConnection("jdbc:sqlite:${cache}")
        locker.createStatement().execute('BEGIN IMMEDIATE')

        when: 'it opens while the lock is held (SQLITE_BUSY past the busy timeout)'
        Index index = Index.open(cache)
        locker.createStatement().execute('ROLLBACK')
        locker.close()
        locker = null

        then: 'nothing was dropped, and the memo is off'
        index.isRunIndexed((Cid) lab.b.completion)
        index.absentBlocks('lab').isEmpty()

        when: 'a build asks the store, and remembers nothing'
        Counter counter = counting(lab.f)
        write(index, 'out', counter.present)
        write(index, 'out', counter.present)

        then: 'each build asks about the gone block and about shared'
        counter.calls == 4
        index.presentBlocks('lab').isEmpty()
        index.absentBlocks('lab').isEmpty()
        rows(snap('out'), 'SELECT stored_bytes, releasing_bytes FROM store_stats')[0] == [stored_bytes: 1, releasing_bytes: 0]

        cleanup:
        locker?.close()
        index?.close()
    }
}
