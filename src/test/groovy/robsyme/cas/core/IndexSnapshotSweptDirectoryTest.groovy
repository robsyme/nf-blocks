package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Follow-up round, part B: a DirectoryManifest a sweep took. A warm cache
 * still holds its edges in block_ref, so the figures walked through it to
 * files other runs still hold (B1); and item_size counted the record's
 * files, swept or not (B2). The first feature follows the integrator's
 * scripts/warm-repro.sh on a fixture: release dm, sweep, restore dm,
 * snapshot, release sad, sweep; what each Release offers to free is what
 * the sweep then deletes.
 */
class IndexSnapshotSweptDirectoryTest extends Specification {
    @TempDir Path root

    private static List<Map> rows(Path db, String sql, List params = []) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def s = c.prepareStatement(sql)
            params.eachWithIndex { Object p, int i -> s.setObject(i + 1, p) }
            def rs = s.executeQuery(); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    /** The snapshot as a session writes it: presence checks, and the lab's sweep.lock. */
    private Path write(RetentionFixture f, Index index, String out = 'out') {
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        def r = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve(out)), 0L, null, root.resolve('t'), null, false, 1000L,
            { Cid c -> f.store.has(c) } as Closure<Boolean>, f.store.retentionStorage())
        assert r.written
        IndexSnapshot.pathIn(root.resolve(out))
    }

    /** What a Release of this run offers to free: its root's exclusive bytes (run.js `figures.exclusive`). */
    private static Map runRoot(Path snap, Cid completion) {
        rows(snap, '''SELECT s.exclusive_bytes, s.reachable_bytes, s.partial FROM root r JOIN root_stats s ON s.root_id = r.id
                      WHERE r.kind = 'run' AND r.cid = ?''', [completion.toString()])[0]
    }

    private static Map store(Path snap) { rows(snap, 'SELECT stored_bytes, held_bytes, releasing_bytes, partial FROM store_stats')[0] }

    private static Map itemSize(Path snap, Cid item) { rows(snap, 'SELECT bytes, blocks FROM item_size WHERE item_cid = ?', [item.toString()])[0] }

    /**
     * Ages every block, then sweeps twice (trash, then the ledgers past due:
     * delete), as the script's `sweep` does; the raw bytes deleted. With
     * `settle` the lock is then backdated, as a minute later; without, the
     * next builds fall inside its release window and record no presence.
     */
    private static long sweep(RetentionFixture f, boolean settle = true) {
        final Map<Cid, Long> sizes = [:]
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c ->
            sizes[c] = f.store.size(c)
            Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(System.currentTimeMillis() - 15 * SweepPolicy.DAY))
        }
        def say = { String msg -> null } as Closure<Void>
        def sleeper = { long ms -> null } as Closure<Void>
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        RetentionStorage s = f.store.retentionStorage()
        for( String name : s.listLedgers() ) {
            final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
            final TrashLedger past = new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)
            s.writeLedger(past.name, past.toJson()); if( past.name != name ) s.deleteLedger(name)
        }
        final SweepReport r = new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        if( settle )
            f.settleSweepLock()
        long raw = 0
        for( Cid c : r.deleted )
            if( !c.isDagCbor() ) raw += sizes[c]
        raw
    }

    /**
     * A Claim logged no earlier than now: a sweep logs its Forget entries at
     * the real time, and a Store Log entry older than the cache's watermark
     * would never be caught up.
     */
    private static Cid claim(RetentionFixture f, Cid subject, String verb, String value, List<Cid> supersedes = []) {
        f.clock = Math.max(f.clock, System.currentTimeMillis())
        f.claim(subject, verb, 'retain', value, supersedes)
    }

    /**
     * Run dm writes a directory (shared + dm only) and a log the held run
     * `keep` also wrote; run sad writes another directory (shared + sad only).
     * So `shared` is under dm's directory and under sad's.
     */
    private Map lab() {
        RetentionFixture f = new RetentionFixture(root.resolve('lab'))
        f.trueSizes = true
        Cid shared = f.raw('shared by both directories ' * 40)
        Cid dmOnly = f.raw('only in the directory of dm ' * 30)
        Cid sadOnly = f.raw('only in the directory of sad ' * 20)
        Cid log = f.raw('a log dm and keep both wrote ' * 10)
        Cid kept = f.raw('only keep wrote this ' * 5)
        Cid dirDm = f.dir(['a.txt': shared, 'b.txt': dmOnly])
        Cid dirSad = f.dir(['a.txt': shared, 'c.txt': sadOnly])
        Map keep = f.run('keep', [[log: log, k: kept]])
        Map dm = f.run('dm', [[out: dirDm, log: log]])
        Map sad = f.run('sad', [[out: dirSad]])
        // Sizes now, while every block is there.
        Map<String, Long> n = [shared: shared, dmOnly: dmOnly, sadOnly: sadOnly, log: log, kept: kept]
            .collectEntries { String k, Cid c -> [(k): f.store.size(c)] } as Map<String, Long>
        [f: f, n: n, shared: shared, dmOnly: dmOnly, sadOnly: sadOnly, log: log, kept: kept, dirDm: dirDm, dirSad: dirSad, keep: keep, dm: dm, sad: sad]
    }

    def 'release, sweep, restore, release another, sweep: what each Release offers to free is what the sweep deletes (lock settled: #settle)'() {
        given:
        Map l = lab()
        RetentionFixture f = l.f
        Index index = Index.open(root.resolve('cache.sqlite'))

        when: 'release dm, as offered by the first snapshot'
        long freesDm = runRoot(write(f, index), l.dm.completion).exclusive_bytes as long
        Cid release = claim(f, (Cid) l.dm.completion, 'set', 'lineage')
        write(f, index)
        long sweptDm = sweep(f, settle)

        then: 'its directory and the file only dm held went'
        !f.store.has(l.dirDm) && !f.store.has(l.dmOnly)
        f.store.has(l.shared)
        sweptDm == l.n.dmOnly
        freesDm == sweptDm

        when: 'a snapshot after the sweep, then dm restored and a snapshot, as the warm cache sees it'
        Path afterSweep = write(f, index)
        Map dmItem = itemSize(afterSweep, (Cid) l.dm.items[0])
        claim(f, (Cid) l.dm.completion, 'del', null, [release])
        Path restored = write(f, index)
        Set<String> presentAfterRestore = index.presentBlocks('lab')
        Map sadOffer = runRoot(restored, l.sad.completion)
        Map dmRoot = runRoot(restored, l.dm.completion)
        Map storeRow = store(restored)

        and: 'a cache that never saw any of it, as control'
        Index fresh = Index.open(root.resolve('fresh.sqlite'))
        Path cold = write(f, fresh, 'cold')
        Map coldSad = runRoot(cold, l.sad.completion)
        Map coldDm = runRoot(cold, l.dm.completion)
        fresh.close()

        then: "releasing sad frees the shared file too: dm's swept directory no longer leads to it, as a cold cache says"
        sadOffer.exclusive_bytes == l.n.shared + l.n.sadOnly
        sadOffer == coldSad

        and: 'a dead end: dm reaches only its log, and says at least'
        dmRoot.reachable_bytes == l.n.log
        dmRoot.partial == 1
        dmRoot == coldDm
        storeRow.partial == 1
        dmItem == [bytes: l.n.log, blocks: 1]

        when: 'sad released and swept'
        claim(f, (Cid) l.sad.completion, 'set', 'lineage')
        long sweptSad = sweep(f, settle)

        then: 'what the sweep deleted is what the Release offered'
        !f.store.has(l.shared) && !f.store.has(l.sadOnly) && !f.store.has(l.dirSad)
        sweptSad == (sadOffer.exclusive_bytes as long)

        and: 'the next snapshot of the warm cache agrees with a cold one'
        Path last = write(f, index)
        Index again = Index.open(root.resolve('again.sqlite'))
        Path coldLast = write(f, again, 'cold2')
        again.close()
        store(last) == store(coldLast)
        store(last).stored_bytes == l.n.log + l.n.kept

        and: 'settled, the restored build recorded presences; unsettled, it fell inside the release window, as warm-repro.sh runs, and recorded none'
        settle ? !presentAfterRestore.isEmpty() : presentAfterRestore.isEmpty()

        cleanup:
        index?.close()

        where:
        settle << [true, false]
    }

    def "a swept item's item_size counts only its files still stored here; collection_size still describes the record"() {
        given:
        Map l = lab()
        RetentionFixture f = l.f
        Index index = Index.open(root.resolve('cache.sqlite'))
        Path before = write(f, index)
        Map collectionBefore = rows(before, 'SELECT bytes, blocks FROM collection_size WHERE collection_cid = ?', [l.dm.collection.toString()])[0]
        Map itemBefore = itemSize(before, (Cid) l.dm.items[0])

        when:
        claim(f, (Cid) l.dm.completion, 'set', 'lineage')
        write(f, index)
        sweep(f)
        Path after = write(f, index)

        then: 'before: every file the record names; after: the log only (its directory went, so shared is not reached)'
        itemBefore == [bytes: l.n.log + l.n.shared + l.n.dmOnly, blocks: 3]
        itemSize(after, (Cid) l.dm.items[0]) == [bytes: l.n.log, blocks: 1]
        rows(after, 'SELECT bytes, blocks FROM collection_size WHERE collection_cid = ?', [l.dm.collection.toString()])[0] == collectionBefore

        cleanup:
        index?.close()
    }
}
