package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

/**
 * A Handoff logged in the member is a root of the storage figures, as Mark
 * makes it one for a sweep (DESIGN.md §22, Sweep): it holds its files as
 * content and its Selection whole, and a clean `delete` frees both. So the
 * figures match what a sweep keeps (integrator finding S3).
 *
 * One run `a` released by its asserter, with items holding `picked` (33 B)
 * and `other` (5 B). Selection `sel` of the picked item, logged here. A
 * Handoff of `sel` with a 9 B `samplesheet.csv`. Leaves say real sizes.
 */
class IndexSnapshotHandoffStatsTest extends Specification {

    @TempDir Path root

    RetentionFixture f
    Cid picked, other, sheet, sel, handoff
    Map run

    def setup() {
        f = new RetentionFixture(root.resolve('lab'))
        f.trueSizes = true
    }

    private void lab(boolean selectionDeleted, boolean handoffDeleted, boolean handoffPinned) {
        picked = f.raw('p' * 33)
        other = f.raw('o' * 5)
        run = f.run('a', [[reads: picked], [log: other]])
        f.claim((Cid) run.completion, 'set', 'retain', 'lineage')
        final Cid pickedItem = ((List<Cid>) run.items).find { Cid i -> itemHolds(i, picked) }
        sel = f.selection(pickedItem, (Cid) run.collection)
        sheet = f.raw('id,fq\na,b')
        handoff = f.handoff(sel, null, ['samplesheet.csv': sheet])
        if( selectionDeleted )
            f.claim(sel, 'delete', null, null)
        if( handoffDeleted )
            f.claim(handoff, 'delete', null, null)
        if( handoffPinned )
            f.claim(handoff, 'add', 'pin', 'cited')
        assert f.store.size(picked) == 33 && f.store.size(other) == 5 && f.store.size(sheet) == 9
    }

    private boolean itemHolds(Cid item, Cid file) {
        final Map block = (Map) DagCbor.decode(f.store.open(item).withCloseable { it.readAllBytes() })
        return Index.leavesOf(block.get('value')).any { Map leaf -> leaf.get('address') == file }
    }

    private Index cache(String name) {
        final Index index = Index.open(root.resolve(name))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        return index
    }

    private Path snapshot(Index index, String out, Closure<Boolean> present = null) {
        assert IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve(out)), 0L, null, root.resolve('t'), null, false,
            1000L, present, new LocalRetentionStorage(root.resolve('lab'))).written
        return IndexSnapshot.pathIn(root.resolve(out))
    }

    /** Two sweeps, the second past the first's ledger, so what the first trashed is deleted. */
    private void sweep() {
        final long aged = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c -> Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(aged)) }
        final Closure<Void> say = { String msg -> null } as Closure<Void>
        final Closure<Void> sleeper = { long ms -> null } as Closure<Void>
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        final RetentionStorage s = f.store.retentionStorage()
        for( String name : s.listLedgers() ) {
            final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
            final TrashLedger past = new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)
            s.writeLedger(past.name, past.toJson()); if( past.name != name ) s.deleteLedger(name)
        }
        new Sweep(f.store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
        f.settleSweepLock()
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

    private static Map store(Path db) {
        rows(db, 'SELECT stored_bytes, pinned_bytes, held_bytes, releasing_bytes FROM store_stats')[0]
    }

    /** Each root of the subject, by kind: [state, reachable, exclusive]. */
    private static Map<String, List> rootsOf(Path db, Cid subject) {
        rows(db, """SELECT r.kind, r.state, s.reachable_bytes, s.exclusive_bytes FROM root r JOIN root_stats s ON s.root_id = r.id
                    WHERE r.cid = '${subject}' ORDER BY r.id""".toString())
            .collectEntries { Map r -> [(r.kind): [r.state, r.reachable_bytes, r.exclusive_bytes]] }
    }

    @Unroll
    def 'a Handoff root holds its files and its Selection; selection deleted #selectionDeleted, handoff deleted #handoffDeleted, pinned #pinned'() {
        given:
        lab(selectionDeleted, handoffDeleted, pinned)
        final Index before = cache('before.sqlite')

        when:
        final Path snap = snapshot(before, 'before')

        then: 'the store partition: the samplesheet is stored, and what the Handoff keeps is not releasing'
        store(snap) == [stored_bytes: 47, pinned_bytes: storeBytes[0], held_bytes: storeBytes[1], releasing_bytes: storeBytes[2]]

        and: "the Handoff's root, beside its Selection's and the run's"
        rootsOf(snap, handoff) == handoffRoots
        rootsOf(snap, sel).selection[0] == (selectionDeleted ? 'deleted' : 'held')
        rootsOf(snap, (Cid) run.completion).run[0] == 'released'

        when: 'a sweep runs'
        final Map<String, Long> counted = rows(snap, "SELECT cid, size FROM block WHERE codec = 'raw'")
            .collectEntries { Map r -> [(r.cid): r.size] }
        sweep()
        final Map<String, Long> kept = counted.findAll { String cid, Long n -> f.store.has(Cid.parse(cid)) }

        then: 'it takes exactly what was releasing and keeps what was held or pinned'
        counted.keySet() == [picked, other, sheet]*.toString() as Set
        (counted.values().sum() - (kept.values().sum() ?: 0L)) == storeBytes[2]
        (kept.values().sum() ?: 0L) == storeBytes[0] + storeBytes[1]

        when: 'a fresh cache of the swept store writes the snapshot again'
        final Index after = cache('after.sqlite')
        final Path again = snapshot(after, 'after', f.store.&has)

        then: 'what was releasing is gone from every figure, and nothing releases'
        store(again) == [stored_bytes: storeBytes[0] + storeBytes[1], pinned_bytes: storeBytes[0], held_bytes: storeBytes[1], releasing_bytes: 0]

        cleanup:
        before?.close()
        after?.close()

        where: 'storeBytes is [pinned, held, releasing]; a root is [state, reachable, exclusive]'
        selectionDeleted | handoffDeleted | pinned || storeBytes   | handoffRoots
        false            | false          | false  || [0, 42, 5]   | [handoff: ['held', 42, 9]]
        true             | false          | false  || [0, 42, 5]   | [handoff: ['held', 42, 42]]
        false            | true           | false  || [0, 33, 14]  | [handoff: ['deleted', 42, 9]]
        true             | true           | false  || [0, 0, 47]   | [handoff: ['deleted', 42, 42]]
        true             | true           | true   || [42, 0, 5]   | [handoff: ['deleted', 42, 0], pin: ['pinned', 42, 42]]
    }

    def "referenced counts the Handoff's files, so it covers stored: a pinned Selection, a released run"() {
        given: 'the lab, with the Selection pinned'
        lab(false, false, false)
        f.claim(sel, 'add', 'pin', 'cited')
        final Index index = cache('cache.sqlite')

        when:
        final Map s = rows(snapshot(index, 'out'), 'SELECT * FROM store_stats')[0]

        then: 'picked is pinned, the samplesheet held by the Handoff, other releasing'
        // referenced: (out, picked item) 33 + (out, other item) 5 + the Handoff's one file Leaf 9 = 47.
        s == [stored_bytes: 47, stored_blocks: 3, referenced_bytes: 47, pinned_bytes: 33, held_bytes: 9, releasing_bytes: 5,
              shared_bytes: 0, partial: 0]

        and: "dedup as the page computes it (web/src/model.js storeStats) is not below 1x"
        StorageStatsTest.pageDedup(s) == 1.0d

        cleanup:
        index?.close()
    }

    def "the samplesheet counts as a stored csv and is no item"() {
        given:
        lab(false, false, false)
        final Index index = cache('cache.sqlite')

        when:
        final Path snap = snapshot(index, 'out')

        then:
        rows(snap, "SELECT files, bytes FROM kind_stats WHERE kind = 'csv'") == [[files: 1, bytes: 9]]
        rows(snap, "SELECT 1 FROM item_size WHERE item_cid = '${handoff}'".toString()) == []
        rows(snap, "SELECT asserted_by, since IS NOT NULL AS since FROM root WHERE kind = 'handoff'") == [[asserted_by: 'test', since: 1]]

        cleanup:
        index?.close()
    }
}
