package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** P3 plan 1.5: `transfer` entries fill `transfer` and `transfer_root`, in the cache and in a member's snapshot. */
class IndexTransferTest extends Specification {

    @TempDir Path tmp
    RetentionFixture f
    Index index

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private void catchUp(Index into = index, BlockStore store = f.store) {
        into.catchUp(store, StoreLog.of(store), store.alias())
    }

    /** A Transfer of one Selection root carrying the given runs, logged as `transfer` in {@code store}. */
    private Cid transfer(BlockStore store, Cid root, List<Cid> runs, String timestamp = '2026-10-24T10:00:00.000Z', boolean log = true) {
        final Transfer t = new Transfer('rob@lab', [root], 'roblab', store.alias(), 'results', ['projects', 'names'],
            runs, 15L, 13L, 1186000L, timestamp)
        final Cid cid = store.putDagCbor(t.toCbor())
        if( log )
            StoreLog.append(store, StoreLogKind.TRANSFER, cid, ++f.clock)
        return cid
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

    def 'a transfer entry fills transfer, transfer_root and log_entry'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid root = f.selection((Cid) run.items[0], (Cid) run.collection)
        final Cid t = transfer(f.store, root, [(Cid) run.completion])

        when:
        catchUp()

        then:
        rows(index.file, 'SELECT * FROM transfer') == [[t.toString(), 'roblab', 'lab', 'rob@lab', 'results', 'names,projects',
                                                       15, 13, 1186000, '2026-10-24T10:00:00.000Z']]
        rows(index.file, 'SELECT transfer_cid, root_cid, role FROM transfer_root ORDER BY role') ==
            [[t.toString(), root.toString(), 'root'], [t.toString(), run.completion.toString(), 'run']]
        rows(index.file, "SELECT kind, member FROM log_entry WHERE cid = '${t}'") == [['transfer', 'lab']]
        index.isTransferIndexed(t)
    }

    def 'no claims carried is an empty claims column'() {
        given:
        final Cid root = f.raw('a selection stand-in')
        final Cid t = f.store.putDagCbor(new Transfer('rob@lab', [root], 'a', 'lab', 'all', [], [], 1L, 1L, 10L,
            '2026-10-24T10:00:00.000Z').toCbor())
        StoreLog.append(f.store, StoreLogKind.TRANSFER, t, ++f.clock)

        when:
        catchUp()

        then:
        rows(index.file, 'SELECT claims, depth FROM transfer') == [['', 'all']]
        rows(index.file, 'SELECT role FROM transfer_root') == [['root']]
    }

    def 'catch-up is idempotent across the overlap window, and ingesting the same Transfer again leaves one set of rows'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid t = transfer(f.store, (Cid) run.completion, [(Cid) run.completion])

        when:
        catchUp()
        final List<List<Object>> transfers = rows(index.file, 'SELECT * FROM transfer')
        final List<List<Object>> roots = rows(index.file, 'SELECT * FROM transfer_root ORDER BY role')
        index.setMetaValue('store_log_watermark:lab', null)
        catchUp()
        catchUp()

        then:
        index.countRows('transfer') == 1
        index.countRows('transfer_root') == 2

        when: 'the ingest itself runs twice more for the same cid, past the already-indexed check'
        index.ingestTolerant(f.store, StoreLogKind.TRANSFER, t, 'lab', f.store)
        index.ingestTolerant(f.store, StoreLogKind.TRANSFER, t, 'lab', f.store)

        then:
        rows(index.file, 'SELECT * FROM transfer') == transfers
        rows(index.file, 'SELECT * FROM transfer_root ORDER BY role') == roots
    }

    def 'a Transfer block not yet arrived leaves missing(NULL, cid) and is ingested once it lands'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid t = transfer(f.store, (Cid) run.completion, [])
        final Path p = ((LocalBlockStore) f.store).blockPath(t)
        final byte[] bytes = Files.readAllBytes(p)
        Files.delete(p)

        when:
        catchUp()

        then:
        !index.isTransferIndexed(t)
        rows(index.file, 'SELECT have_cid, needed_cid FROM missing') == [[null, t.toString()]]

        when: 'it arrives after the watermark has passed its entry'
        f.store.put(t, new ByteArrayInputStream(bytes), bytes.length)
        catchUp()

        then:
        index.isTransferIndexed(t)
        index.countRows('missing') == 0
    }

    def 'a malformed Transfer block is skipped, not retried, and does not stop the catch-up'() {
        given:
        final Cid bad = f.store.putDagCbor([kind: 'Transfer', schema: 1L, asserted_by: 'x', roots: 'not a list'])
        StoreLog.append(f.store, StoreLogKind.TRANSFER, bad, ++f.clock)
        final Map run = f.run('after', [[x: f.raw('x')]])

        when:
        catchUp()
        catchUp()

        then:
        !index.isTransferIndexed(bad)
        index.isRunIndexed((Cid) run.completion)
        index.countRows('missing') == 0
    }

    def 'a rebuild and a first block scan both find a Transfer'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid t = transfer(f.store, (Cid) run.completion, [(Cid) run.completion])

        when:
        index.rebuild(f.store, 'lab')

        then:
        index.isTransferIndexed(t)
        rows(index.file, 'SELECT kind FROM log_entry WHERE cid = ' + "'${t}'") == [['transfer']]

        when: 'a Transfer block with no entry yet (a copy that stopped before logging it)'
        final Cid unlogged = transfer(f.store, (Cid) run.collection, [], '2026-10-24T11:00:00.000Z', false)
        final Index scanned = Index.open(tmp.resolve('scanned.sqlite'))
        catchUp(scanned)

        then:
        scanned.isTransferIndexed(t)
        scanned.isTransferIndexed(unlogged)

        cleanup:
        scanned?.close()
    }

    def 'forgetting a Transfer cid removes its rows, and a forget entry does too'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid one = transfer(f.store, (Cid) run.completion, [(Cid) run.completion])
        final Cid two = transfer(f.store, (Cid) run.collection, [(Cid) run.completion], '2026-10-24T11:00:00.000Z')
        catchUp()

        when: 'its block is gone, as after a sweep, and the cache forgets it'
        Files.delete(((LocalBlockStore) f.store).blockPath(one))
        index.forget([one])

        then:
        rows(index.file, 'SELECT cid FROM transfer') == [[two.toString()]]
        rows(index.file, 'SELECT DISTINCT transfer_cid FROM transfer_root') == [[two.toString()]]

        when: 'a sweep deletes the other and logs a forget for it'
        Files.delete(((LocalBlockStore) f.store).blockPath(two))
        final Cid forget = f.store.putDagCbor(new Forget('test', [two]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, forget, ++f.clock)
        catchUp()

        then:
        index.countRows('transfer') == 0
        index.countRows('transfer_root') == 0
    }

    def 'reset empties the transfer tables'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        transfer(f.store, (Cid) run.completion, [])
        catchUp()

        when:
        index.reset()

        then:
        index.countRows('transfer') == 0
        index.countRows('transfer_root') == 0
    }

    /** lab's Store Log, counting how often the whole log is listed. */
    private StoreLog counted(int[] listings) {
        final StoreLogStorage inner = ((LocalBlockStore) f.store).storeLogStorage()
        return new StoreLog([putEntry   : { String name -> inner.putEntry(name) },
                             listEntries: { -> listings[0]++; inner.listEntries() },
                             hasEntry   : { String name -> inner.hasEntry(name) }] as StoreLogStorage)
    }

    /** A Transfer of a run, then another run an hour later: a watermark at the newest entry is past the overlap before the transfer entry. */
    private Map transferThenAnHourLater() {
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid t = transfer(f.store, (Cid) run.completion, [(Cid) run.completion])
        f.clock += 3_600_000L
        final Map later = f.run('later', [[y: f.raw('y')]])
        return [run: run, transfer: t, later: later]
    }

    def 'a cache opened again reads only the log tail'() {
        given:
        transferThenAnHourLater()
        catchUp()
        index.close()
        final int[] listings = [0]

        when:
        index = Index.open(tmp.resolve('cache.sqlite'))
        index.catchUp(f.store, counted(listings), 'lab')

        then:
        listings[0] == 1
        index.countRows('transfer') == 1
    }

    def 'a cold cache seeded from a snapshot reads only the log tail'() {
        given:
        final Map l = transferThenAnHourLater()
        catchUp()
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        final int[] listings = [0]

        when:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, counted(listings), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.isTransferIndexed((Cid) l.transfer)
        listings[0] == 1

        cleanup:
        cold?.close()
    }

    def "a member's snapshot carries the transfer rows its Store Log names, and a seeded cache has them"() {
        given: 'a Transfer into lab and another into shared, both in one cache'
        final LocalBlockStore shared = new LocalBlockStore(tmp.resolve('shared'), 'shared', true)
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid labTransfer = transfer(f.store, (Cid) run.completion, [(Cid) run.completion])
        final Cid sharedTransfer = transfer(shared, Fixtures.cidOf([kind: 'Selection', n: 9]), [])
        catchUp()
        catchUp(index, shared)
        assert index.countRows('transfer') == 2

        when:
        final IndexSnapshot.Result written = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        final Path snapshot = tmp.resolve('lab').resolve(IndexSnapshot.relativePath())

        then:
        written.written
        rows(snapshot, 'SELECT cid FROM transfer') == [[labTransfer.toString()]]
        rows(snapshot, 'SELECT DISTINCT transfer_cid FROM transfer_root') == [[labTransfer.toString()]]
        rows(snapshot, "SELECT kind FROM log_entry WHERE cid = '${labTransfer}'") == [['transfer']]
        !rows(snapshot, 'SELECT cid FROM log_entry').contains([sharedTransfer.toString()])

        when: 'a cold cache seeds from it'
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        cold.isTransferIndexed(labTransfer)
        rows(cold.file, 'SELECT * FROM transfer') == rows(index.file, "SELECT * FROM transfer WHERE cid = '${labTransfer}'")
        rows(cold.file, 'SELECT * FROM transfer_root ORDER BY role') ==
            rows(index.file, "SELECT * FROM transfer_root WHERE transfer_cid = '${labTransfer}' ORDER BY role")

        cleanup:
        cold?.close()
    }

    def 'a seeded Transfer whose block the snapshot writer could not read is retried after the seed'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        final Cid t = transfer(f.store, (Cid) run.completion, [])
        final Path p = ((LocalBlockStore) f.store).blockPath(t)
        final byte[] bytes = Files.readAllBytes(p)
        Files.delete(p)
        catchUp()
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        f.store.put(t, new ByteArrayInputStream(bytes), bytes.length)

        when:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        cold.isTransferIndexed(t)

        cleanup:
        cold?.close()
    }
}
