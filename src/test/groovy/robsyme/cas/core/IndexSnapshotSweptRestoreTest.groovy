package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 3, item 1: a Claim brings no bytes back. A swept run that is
 * restored (del retain), or an item of it pinned after the sweep, still has
 * its swept files counted in no storage figure. A recorded absence stands
 * whatever the roots' state; one a held or pinned root reaches is looked at
 * again at each build, and only a present answer drops it. The first three
 * features are the integrator's repro (ReviewSwept2Test).
 */
class IndexSnapshotSweptRestoreTest extends Specification {
    @TempDir Path root

    private static List<Map> rows(Path db, String sql) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def rs = c.createStatement().executeQuery(sql); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    private static void sweep(RetentionFixture f) {
        long aged = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c -> Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(aged)) }
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

    /** Run a (shared) and run b (shared + only), b released and swept, so `only` is gone. The fixture's Leaves all say 1 byte. */
    private Map lab() {
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid shared = f.raw('shared')
        Cid only = f.raw('only b')
        Map a = f.run('a', [[s: shared]])
        Map b = f.run('b', [[s: shared, u: only]])
        Cid release = f.claim(b.completion, 'set', 'retain', 'lineage')
        sweep(f)
        assert !f.store.has(only)
        [f: f, b: b, only: only, shared: shared, release: release]
    }

    private Map write(Index index, Closure<Boolean> present) {
        def r = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve('out')), 0L, null, root.resolve('t'), null, false, 1000L, present,
            new LocalRetentionStorage(root.resolve('lab')))
        assert r.written
        rows(IndexSnapshot.pathIn(root.resolve('out')), 'SELECT stored_bytes, held_bytes, pinned_bytes, releasing_bytes FROM store_stats')[0]
    }

    private Map write(RetentionFixture f, Index index) { write(index, { Cid c -> f.store.has(c) } as Closure<Boolean>) }

    def 'the integrator repro, with the presence function'() {
        given:
        Map l = lab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(l.f.store, StoreLog.of(l.f.store), 'lab')

        expect:
        write(l.f, index) == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]

        cleanup:
        index?.close()
    }

    def 'a swept run restored with del retain counts only what is left of it'() {
        given:
        Map l = lab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(l.f.store, StoreLog.of(l.f.store), 'lab')
        assert write(l.f, index).stored_bytes == 1
        RetentionFixture f = l.f
        f.claim(l.b.completion, 'del', 'retain', null, [l.release])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store)

        when:
        Map s = write(index, counter.present)

        then: 'only 1 byte is in the store, and the restored run holds only that'
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        rows(IndexSnapshot.pathIn(root.resolve('out')), """SELECT s.reachable_bytes FROM root r JOIN root_stats s ON s.root_id = r.id
                                                           WHERE r.cid = '${l.b.completion}'""")[0].reachable_bytes == 1

        and: 'the absence is kept and asked about once; shared is looked at again, since the restore is a retain Claim'
        index.absentBlocks('lab') == [l.only.toString()] as Set
        counter.calls == 2

        when: 'the next build asks again, since only a new put brings it back'
        counter.calls = 0
        s = write(index, counter.present)

        then:
        s.stored_bytes == 1
        counter.calls == 1
        index.absentBlocks('lab') == [l.only.toString()] as Set

        cleanup:
        index?.close()
    }

    def 'an item of a swept run pinned after the sweep pins only what is left of it'() {
        given:
        Map l = lab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(l.f.store, StoreLog.of(l.f.store), 'lab')
        assert write(l.f, index).stored_bytes == 1
        RetentionFixture f = l.f
        f.claim((Cid) l.b.items[0], 'add', 'pin', 'too late')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        when:
        Map s = write(f, index)

        then: 'only 1 byte is in the store, and the pin holds only that'
        s == [stored_bytes: 1, held_bytes: 0, pinned_bytes: 1, releasing_bytes: 0]
        index.absentBlocks('lab') == [l.only.toString()] as Set

        cleanup:
        index?.close()
    }

    def 'a swept block put back by hand, under a restored run, is found by the re-check and counts again'() {
        given:
        Map l = lab()
        RetentionFixture f = l.f
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert write(f, index).stored_bytes == 1
        f.claim(l.b.completion, 'del', 'retain', null, [l.release])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert write(f, index).stored_bytes == 1

        when: 'the same bytes are put again, with no run that names them'
        assert f.raw('only b') == l.only
        Map s = write(f, index)

        then:
        s == [stored_bytes: 2, held_bytes: 2, pinned_bytes: 0, releasing_bytes: 0]
        index.absentBlocks('lab').isEmpty()

        cleanup:
        index?.close()
    }

    def 're-checks run at most 16 at once, and one that fails counts as present this time and keeps its record'() {
        given: 'a released run of 40 files, 20 of them swept by hand and recorded'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        List<Cid> files = (1..40).collect { int i -> f.raw("file ${i}".toString()) }
        Map run = f.run('gone', [files.withIndex().collectEntries { Cid c, int i -> [("f${i}".toString()): c] }])
        Cid release = f.claim(run.completion, 'set', 'retain', 'lineage')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        files.take(20).each { Cid c -> Files.delete(((LocalBlockStore) f.store).blockPath(c)) }
        assert write(f, index).stored_bytes == 20
        Set<String> swept = files.take(20).collect { it.toString() } as Set
        assert index.absentBlocks('lab') == swept

        and: 'the run is restored, and two of the swept files cannot be looked up'
        f.claim(run.completion, 'del', 'retain', null, [release])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store, 30L, [files[0], files[7]] as Set)

        when:
        Map s = write(index, counter.present)

        then: 'the 20 recorded blocks are asked again, and the other 20 once, since the run was once released; a few at a time'
        counter.calls == 40
        counter.most.get() > 1
        counter.most.get() <= 16

        and: 'the two failed lookups count as present this time, and every record stays'
        s == [stored_bytes: 22, held_bytes: 22, pinned_bytes: 0, releasing_bytes: 0]
        index.absentBlocks('lab') == swept
        index.presentBlocks('lab') == files.drop(20).collect { it.toString() } as Set

        cleanup:
        index?.close()
    }

    def 'CREATE TABLE IF NOT EXISTS needs no write lock when the table exists: the memo stays on under a concurrent writer'() {
        given:
        Path cache = root.resolve('c.sqlite')
        Index.open(cache).close()
        def locker = DriverManager.getConnection("jdbc:sqlite:${cache}")
        locker.createStatement().execute('BEGIN IMMEDIATE')

        when:
        Index index = Index.open(cache)

        then:
        index.@absentMemo

        cleanup:
        locker?.createStatement()?.execute('ROLLBACK')
        locker?.close()
        index?.close()
    }

    /** A cache that never saw the sweep: a reindex, a seeded cache, another machine's. */
    private Index cold(RetentionFixture f, String name) {
        Index index = Index.open(root.resolve(name))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert index.absentBlocks('lab').isEmpty()
        return index
    }

    def 'a cold cache after a sweep and a restore asks once and counts only what is left; the next build asks only about what is gone'() {
        given: 'released, swept and restored, as the warm cache saw it'
        Map l = lab()
        RetentionFixture f = l.f
        f.claim(l.b.completion, 'del', 'retain', null, [l.release])

        when: 'a cache that never saw the sweep builds'
        Index index = cold(f, 'cold.sqlite')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store)
        Map s = write(index, counter.present)

        then: 'both blocks of the once-released run were asked about, once each'
        s == [stored_bytes: 1, held_bytes: 1, pinned_bytes: 0, releasing_bytes: 0]
        counter.calls == 2
        index.absentBlocks('lab') == [l.only.toString()] as Set
        index.presentBlocks('lab') == [l.shared.toString()] as Set

        when: 'the next build'
        counter.calls = 0
        s = write(index, counter.present)

        then: 'the present block is not asked again; the gone one is, as every recorded absence a held root reaches'
        s.stored_bytes == 1
        counter.calls == 1

        cleanup:
        index?.close()
    }

    def 'a cold cache with a pin on an item of a swept run asks once and pins only what is left'() {
        given:
        Map l = lab()
        RetentionFixture f = l.f
        f.claim((Cid) l.b.items[0], 'add', 'pin', 'too late')

        when:
        Index index = cold(f, 'cold.sqlite')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store)
        Map s = write(index, counter.present)

        then:
        s == [stored_bytes: 1, held_bytes: 0, pinned_bytes: 1, releasing_bytes: 0]
        counter.calls == 2
        index.absentBlocks('lab') == [l.only.toString()] as Set
        index.presentBlocks('lab').size() == 1

        cleanup:
        index?.close()
    }

    def 'a presence is dropped once the block is releasing-only again, and it is then checked as a candidate'() {
        given: 'a cold cache that recorded shared as present under the restored run'
        Map l = lab()
        RetentionFixture f = l.f
        Cid restore = f.claim(l.b.completion, 'del', 'retain', null, [l.release])
        Index index = cold(f, 'cold.sqlite')
        write(f, index)
        assert index.presentBlocks('lab').size() == 1

        when: 'run a is released and b released again: shared is releasing-only'
        f.claim(l.b.completion, 'set', 'retain', 'lineage', [restore])
        Cid aRun = Cid.parse((String) rows(root.resolve('cold.sqlite'), "SELECT completion_cid FROM run WHERE run_name = 'a'")[0].completion_cid)
        f.claim(aRun, 'set', 'retain', 'lineage')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store)
        Map s = write(index, counter.present)

        then: 'its presence is gone, and it was asked as a candidate'
        index.presentBlocks('lab').isEmpty()
        counter.calls == 1
        s == [stored_bytes: 1, held_bytes: 0, pinned_bytes: 0, releasing_bytes: 1]

        cleanup:
        index?.close()
    }

    def 'a store never released or deleted is asked nothing, cold or warm'() {
        given:
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        f.run('a', [[s: f.raw('one')]])
        Index index = cold(f, 'cold.sqlite')
        IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter(f.store)

        when:
        write(index, counter.present)
        write(index, counter.present)

        then:
        counter.calls == 0
        index.presentBlocks('lab').isEmpty()

        cleanup:
        index?.close()
    }

    def 'an ingest clears only its own member\'s absence'() {
        given: 'one cache, two members, both recording the same block absent'
        Map l = lab()
        RetentionFixture f = l.f
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        index.recordAbsent('lab', [l.only.toString()])
        index.recordAbsent('other', [l.only.toString()])

        when: 'lab writes the block again in a new run, and the cache ingests it for lab'
        f.run('c', [[u: f.raw('only b')]])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then: 'lab\'s row is gone, other\'s stays: the block may still be gone from that store'
        index.absentBlocks('lab').isEmpty()
        index.absentBlocks('other') == [l.only.toString()] as Set

        cleanup:
        index?.close()
    }

    def 'a presence recorded before an unobserved release, sweep and restore does not hide the swept block (the reviewer\'s repro)'() {
        given: 'a (shared) held, b (shared + only) released; the cache records shared present'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid shared = f.raw('shared')
        Cid only = f.raw('only b')
        Map a = f.run('a', [[s: shared]])
        Map b = f.run('b', [[s: shared, u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        write(f, index)
        assert index.presentBlocks('lab') == [shared.toString()] as Set

        when: 'a is released, swept and restored with no build by this cache in between'
        Cid rel = f.claim(a.completion, 'set', 'retain', 'lineage')
        sweep(f)
        assert !f.store.has(shared)
        f.claim(a.completion, 'del', 'retain', null, [rel])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map s = write(f, index)
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        fresh.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map control = write(f, fresh)
        fresh.close()

        then: 'nothing of a or b is left on disk, so nothing is stored, as a fresh cache says'
        s.stored_bytes == 0
        control.stored_bytes == 0
        index.absentBlocks('lab').contains(shared.toString())

        cleanup:
        index?.close()
    }

    def 'a presence recorded under a pin does not survive an unobserved unpin, sweep and re-pin'() {
        given: 'run a released, its item pinned; the cache records the pinned file present'
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid file = f.raw('pinned')
        Map a = f.run('a', [[p: file]])
        f.claim(a.completion, 'set', 'retain', 'lineage')
        Cid item = (Cid) a.items[0]
        Cid pin = f.claim(item, 'add', 'pin', 'keep')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert write(f, index).pinned_bytes == 1
        assert index.presentBlocks('lab') == [file.toString()] as Set

        when: 'unpinned, swept and pinned again with no build in between'
        f.claim(item, 'del', 'pin', null, [pin])
        sweep(f)
        assert !f.store.has(file)
        f.claim(item, 'add', 'pin', 'again')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map s = write(f, index)

        then:
        s.stored_bytes == 0
        s.pinned_bytes == 0
        index.absentBlocks('lab') == [file.toString()] as Set

        cleanup:
        index?.close()
    }

    def 'every presence of every member goes when any Claim is ingested, under any member, when one is newly logged, at a seed, and at a forget'() {
        given:
        Map l = lab()
        RetentionFixture f = l.f
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'other')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve('snap')), 0L, null, root.resolve('t'))
        Closure<Void> mark = { -> index.recordPresent('lab', ['x']); index.recordPresent('other', ['y']); null }
        Closure<Boolean> none = { -> index.presentBlocks('lab').isEmpty() && index.presentBlocks('other').isEmpty() }

        when: 'a pin Claim is ingested for lab'
        mark()
        index.ingestClaim(f.store, f.claim((Cid) l.b.items[0], 'add', 'pin', 'p'), 'lab')

        then: 'both members look again'
        none()

        when: 'a Claim of any other attribute is ingested: it may supersede a pin or a release'
        mark()
        index.ingestClaim(f.store, f.claim((Cid) l.b.items[0], 'set', 'note', 'n'), 'lab')

        then:
        none()

        when: 'a Claim is ingested under another member than the one whose log carries it (a retry, a scan)'
        mark()
        index.ingestClaim(f.store, f.claim((Cid) l.b.completion, 'set', 'name', 'z'), 'other')

        then: 'lab looks again too'
        none()

        when: 'a Claim other indexed first is then logged in lab'
        f.claim((Cid) l.b.completion, 'set', 'name', 'w')
        index.catchUp(f.store, StoreLog.of(f.store), 'other')
        mark()
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then:
        none()

        when: 'a seed'
        mark()
        index.seedFrom(IndexSnapshot.pathIn(root.resolve('snap')), 'lab')

        then:
        none()

        when: 'a forget'
        mark()
        index.forget([(Cid) l.release])

        then:
        none()

        cleanup:
        index?.close()
    }

    def 'an unpin by a Claim of another attribute, a sweep, and a restore by another such Claim: the swept file does not count (reviewer probe)'() {
        given:
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid file = f.raw('pinned')
        Map a = f.run('a', [[p: file]])
        Cid rel = f.claim(a.completion, 'set', 'retain', 'lineage')
        Cid item = (Cid) a.items[0]
        Cid pin = f.claim(item, 'add', 'pin', 'keep')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert write(f, index).pinned_bytes == 1
        assert index.presentBlocks('lab') == [file.toString()] as Set

        when: 'a name Claim supersedes the pin, a sweep takes the file, a name Claim supersedes the release'
        f.claim(item, 'set', 'name', 'x', [pin])
        sweep(f)
        assert !f.store.has(file)
        f.claim(a.completion, 'set', 'name', 'y', [rel])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map s = write(f, index)
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        fresh.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map control = write(f, fresh)
        fresh.close()

        then: 'as a fresh cache says: nothing'
        control == [stored_bytes: 0, held_bytes: 0, pinned_bytes: 0, releasing_bytes: 0]
        s == control

        cleanup:
        index?.close()
    }

    def 'an unpin by a Claim of another attribute, a sweep, and a new Selection of the item: the swept file does not count (reviewer probe)'() {
        given:
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid file = f.raw('pinned')
        Map a = f.run('a', [[p: file]])
        f.claim(a.completion, 'set', 'retain', 'lineage')
        Cid item = (Cid) a.items[0]
        Cid pin = f.claim(item, 'add', 'pin', 'keep')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        assert write(f, index).pinned_bytes == 1
        assert index.presentBlocks('lab') == [file.toString()] as Set

        when:
        f.claim(item, 'set', 'name', 'x', [pin])
        sweep(f)
        assert !f.store.has(file)
        f.selection(item, (Cid) a.collection)
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map s = write(f, index)
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        fresh.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map control = write(f, fresh)
        fresh.close()

        then:
        control.stored_bytes == 0
        s == control

        cleanup:
        index?.close()
    }

    def 'an unpin ingested by another process while a build looks up does not leave a stale presence (reviewer race probe)'() {
        given:
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        Cid file = f.raw('pinned')
        Map a = f.run('a', [[p: file]])
        f.claim(a.completion, 'set', 'retain', 'lineage')
        Cid item = (Cid) a.items[0]
        Cid pin = f.claim(item, 'add', 'pin', 'keep')
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        boolean raced = false

        when: 'during the first build, another process writes and ingests an unpin into the same cache file'
        write(index, { Cid c ->
            if( !raced ) {
                raced = true
                f.claim(item, 'set', 'name', 'x', [pin])
                Index other = Index.open(root.resolve('cache.sqlite'))
                other.catchUp(f.store, StoreLog.of(f.store), 'lab')
                other.close()
            }
            f.store.has(c)
        } as Closure<Boolean>)

        then: 'the build recorded nothing present, since the generation moved'
        raced
        index.presentBlocks('lab').isEmpty()

        when: 'a sweep takes the file, and a new Selection holds the item, with no Claim'
        sweep(f)
        assert !f.store.has(file)
        f.selection(item, (Cid) a.collection)
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map s = write(f, index)
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        fresh.catchUp(f.store, StoreLog.of(f.store), 'lab')
        Map control = write(f, fresh)
        fresh.close()

        then: 'as a fresh cache says'
        control.stored_bytes == 0
        s == control

        cleanup:
        index?.close()
    }

    def 'block_present_generation moves at every drop, and a presence is recorded only at the generation the build read'() {
        given:
        Map l = lab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(l.f.store, StoreLog.of(l.f.store), 'lab')
        String before = index.presentGeneration()

        when:
        index.ingestClaim(l.f.store, l.f.claim((Cid) l.b.completion, 'set', 'name', 'n'), 'lab')
        String after = index.presentGeneration()

        then:
        index.metaValue(Index.PRESENT_GENERATION_KEY) == after
        after != before
        !index.recordPresentIf('lab', ['x'], before)
        index.presentBlocks('lab').isEmpty()
        index.recordPresentIf('lab', ['x'], after)
        index.presentBlocks('lab') == ['x'] as Set

        cleanup:
        index?.close()
    }

    def 'a reset moves block_present_generation on, so a build that read it before the reset records nothing, from a fresh cache'() {
        given:
        Index index = Index.open(root.resolve('cache.sqlite'))
        String before = index.presentGeneration()

        when:
        index.reset()

        then:
        before == '0'
        !index.recordPresentIf('lab', ['x'], before)
        index.presentBlocks('lab').isEmpty()
        index.presentGeneration() == '1'

        cleanup:
        index?.close()
    }

    def 'a reset moves block_present_generation on, so a build that read it before the reset records nothing, even after as many drops again'() {
        given:
        Map l = lab()
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(l.f.store, StoreLog.of(l.f.store), 'lab')
        String before = index.presentGeneration()
        int drops = Integer.parseInt(before)

        when:
        index.reset()
        String afterReset = index.presentGeneration()
        // As many drops again as before the reset: without the bump, the counter would be back at `before`.
        for( int i = 0; i < drops; i++ )
            index.ingestClaim(l.f.store, l.f.claim((Cid) l.b.completion, 'set', 'name', "n${i}".toString()), 'lab')

        then:
        drops > 0
        !index.recordPresentIf('lab', ['x'], before)
        index.presentBlocks('lab').isEmpty()
        afterReset == String.valueOf(drops + 1)
        index.presentGeneration() == String.valueOf(2 * drops + 1)

        cleanup:
        index?.close()
    }

    def 'a reset reads a missing or non-number block_present_generation as 0 and moves it to 1, without throwing'() {
        given:
        Index index = Index.open(root.resolve('cache.sqlite'))
        index.setMetaValue(Index.PRESENT_GENERATION_KEY, value)
        String before = index.presentGeneration()

        when:
        index.reset()

        then:
        noExceptionThrown()
        index.presentGeneration() == '1'
        !index.recordPresentIf('lab', ['x'], before)
        index.presentBlocks('lab').isEmpty()

        cleanup:
        index?.close()

        where:
        value << [null, '', 'not a number', '99999999999999999999', '3.5']
    }
}
