package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * P3 plan D8: a member that received a partial copy of a run holds all of
 * the run's records (D7) but only some of its files. The cache that caught
 * up the source too knows every file's size, so without a presence check the
 * destination's figures count the files it never received (P2's R8). The
 * runs and roots a Transfer logged in the member names are presence-checked
 * as the ever-released ones are, through block_absent and block_present.
 */
class IndexSnapshotTransferPresenceTest extends Specification {

    @TempDir Path tmp

    private static List<Map> rows(Path db, String sql) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def rs = c.createStatement().executeQuery(sql); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    private static void copy(BlockStore from, BlockStore to, Cid cid) {
        final byte[] bytes = from.open(cid).withCloseable { it.bytes }
        to.put(cid, new ByteArrayInputStream(bytes), bytes.length)
    }

    /**
     * Member a holds a run of two items: `kept` (one file) and `left` (a file
     * and a directory of one file). Member b holds every record of the run
     * and the script, the kept item's file, a Selection of the kept item and
     * the run's entry, with, when {@code transferred}, a Transfer naming the
     * run and the Selection and its entry. Both caught up in one cache.
     */
    private Map layout(boolean transferred) {
        final LocalBlockStore a = new LocalBlockStore(tmp.resolve('a'), 'a', true)
        final LocalBlockStore b = new LocalBlockStore(tmp.resolve('b'), 'b', true)
        final RetentionFixture f = new RetentionFixture(a)
        f.trueSizes = true
        final Cid keptFile = f.raw('the copied file')
        final Cid leftFile = f.raw('a file left behind in a')
        final Cid inner = f.raw('a file in a directory left behind')
        final Cid dir = f.dir([inner: inner])
        final Map run = f.run('r', [[k: keptFile], [l: leftFile, d: dir]])
        // The same block the fixture wrote: a put of an existing block is a no-op that returns its cid.
        final Cid kept = a.putDagCbor(OutputItem.of([k: Leaf.of('k', keptFile, f.sizeOf(keptFile))]).toCbor())
        assert ((List<Cid>) run.items).contains(kept)
        final Cid left = (Cid) run.items.find { it != kept }

        for( Cid c : [run.completion, run.manifest, run.script, run.collection, kept, left, keptFile] )
            copy(a, b, (Cid) c)
        StoreLog.append(b, StoreLogKind.RUN, (Cid) run.completion, ++f.clock)
        final Cid selection = b.putDagCbor(new Selection('rob@lab', [Selection.item(kept, [(Cid) run.collection])], []).toCbor())
        StoreLog.append(b, StoreLogKind.SELECTION, selection, ++f.clock)
        if( transferred ) {
            final Transfer t = new Transfer('rob@lab', [selection], 'a', 'b', 'results', [], [(Cid) run.completion],
                7L, 7L, a.size(keptFile), '2026-10-25T10:00:00.000Z')
            StoreLog.append(b, StoreLogKind.TRANSFER, b.putDagCbor(t.toCbor()), ++f.clock)
        }
        assert !b.has(leftFile) && !b.has(dir) && !b.has(inner)

        final Index index = Index.open(tmp.resolve('cache.sqlite'))
        index.catchUp(a, StoreLog.of(a), 'a')
        index.catchUp(b, StoreLog.of(b), 'b')
        return [a: a, b: b, index: index, run: run, kept: kept, left: left,
                keptFile: keptFile, leftFile: leftFile, dir: dir, inner: inner, selection: selection]
    }

    private Map write(Map l, IndexSnapshotSweptTest.Counter counter) {
        final IndexSnapshot.Result r = IndexSnapshot.write((Index) l.index, 'b', new LocalSnapshotStorage(tmp.resolve('out')), 0L, null,
            tmp.resolve('t'), null, false, 1000L, counter.present, new LocalRetentionStorage(tmp.resolve('b')))
        assert r.written
        return rows(IndexSnapshot.pathIn(tmp.resolve('out')), 'SELECT stored_bytes, stored_blocks, held_bytes, releasing_bytes FROM store_stats')[0]
    }

    private long size(Map l, String file) { ((BlockStore) l.a).size((Cid) l[file]) }

    private Map itemSize(Map l, String item) {
        rows(IndexSnapshot.pathIn(tmp.resolve('out')), "SELECT bytes, blocks FROM item_size WHERE item_cid = '${l[item]}'")[0]
    }

    def 'a partial copy counts only the files the member received, and remembers what it asked'() {
        given:
        final Map l = layout(true)
        final Index index = (Index) l.index
        final IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter((BlockStore) l.b)

        when:
        final Map s = write(l, counter)

        then: 'only the kept file is stored and held'
        s == [stored_bytes: size(l, 'keptFile'), stored_blocks: 1, held_bytes: size(l, 'keptFile'), releasing_bytes: 0]
        itemSize(l, 'kept') == [bytes: size(l, 'keptFile'), blocks: 1]
        itemSize(l, 'left') == [bytes: 0, blocks: 0]

        and: 'the left item\'s file and directory are recorded absent; the walk stops at the directory, so its file is not'
        index.absentBlocks('b') == [l.leftFile.toString(), l.dir.toString()] as Set
        index.presentBlocks('b') == [l.keptFile.toString()] as Set
        and: 'the directory\'s file was asked in the same round as the directory, before the walk knew to stop there'
        counter.calls == 4

        when: 'a second build'
        counter.calls = 0
        final Map again = write(l, counter)

        then: 'the kept file is not asked again; the two recorded absences are, as every absence a held root reaches is'
        again == s
        counter.calls == 2
        index.absentBlocks('b') == [l.leftFile.toString(), l.dir.toString()] as Set
        index.presentBlocks('b') == [l.keptFile.toString()] as Set

        cleanup:
        index?.close()
    }

    def 'the same layout without the Transfer counts every file the cache knows of'() {
        given:
        final Map l = layout(false)
        final Index index = (Index) l.index
        final IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter((BlockStore) l.b)

        when:
        final Map s = write(l, counter)

        then: 'the files left in a are counted, and nothing is asked'
        final long all = size(l, 'keptFile') + size(l, 'leftFile') + size(l, 'inner')
        s == [stored_bytes: all, stored_blocks: 3, held_bytes: all, releasing_bytes: 0]
        counter.calls == 0
        index.absentBlocks('b').isEmpty()

        cleanup:
        index?.close()
    }

    def 'a Transfer logged only in another member exposes nothing here'() {
        given: 'the Transfer is in b, and the snapshot is a\'s'
        final Map l = layout(true)
        final Index index = (Index) l.index
        final IndexSnapshotSweptTest.Counter counter = new IndexSnapshotSweptTest.Counter((BlockStore) l.a)

        when:
        final IndexSnapshot.Result r = IndexSnapshot.write(index, 'a', new LocalSnapshotStorage(tmp.resolve('out-a')), 0L, null,
            tmp.resolve('t'), null, false, 1000L, counter.present, new LocalRetentionStorage(tmp.resolve('a')))

        then:
        r.written
        counter.calls == 0
        index.absentBlocks('a').isEmpty()
        index.presentBlocks('a').isEmpty()

        cleanup:
        index?.close()
    }
}
