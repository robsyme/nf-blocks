package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * P2 task 3: the v4 snapshot carries the member's content tables (block,
 * block_ref, content `missing`), its roots and the storage aggregates, all
 * per store. Two members hold copies of the same run with different pins, so
 * the same run frees a different number of bytes in each. Every expected
 * value is worked out by hand in the comments.
 *
 * The fixture is StorageStatsTest's: raw blocks A..F of 1, 2, 4, 8, 16 and
 * 32 B; items i1..i6 hold A..F, one each. run1 (ana) outputs c1 = i1 i2 i3,
 * run2 (ben) c2 = i3 i4, run3 (cy) c3 = i4 i5 i6, all in pipeline p.
 *
 *   labA: run1, run2, ana's Selection of i2, and ben's pin on run1.
 *   labB: run2 and run3 (copies), cy's release of run3, and ana's pin on i4.
 */
class IndexSnapshotStorageTest extends Specification {

    static final long GRACE = 14L * 86_400_000L

    @TempDir Path tmp

    LocalBlockStore labA
    LocalBlockStore labB
    Map<String, Cid> raw = [:]
    Map<String, Cid> item = [:]
    Map<String, Cid> run = [:]
    Map<String, String> finished = [:]

    def setup() {
        labA = new LocalBlockStore(tmp.resolve('labA'), 'labA', true)
        labB = new LocalBlockStore(tmp.resolve('labB'), 'labB', true)
    }

    // -- building the members ------------------------------------------------

    private static final Map<String, Integer> SIZES = [A: 1, B: 2, C: 4, D: 8, E: 16, F: 32]
    private static final Map<String, String> NAMES = [A: 'a.txt', B: 'b.bam', C: 'c.BAM', D: 'd', E: 'e.txt', F: 'f.html']
    private static final Map<String, String> ITEM_OF = [A: 'i1', B: 'i2', C: 'i3', D: 'i4', E: 'i5', F: 'i6']

    /** The raw block and the item that holds it, written into `store`. */
    private void putItem(LocalBlockStore store, String block) {
        final byte[] bytes = new byte[SIZES[block]]
        Arrays.fill(bytes, (byte) block.charAt(0))
        raw[block] = store.putStreaming(new ByteArrayInputStream(bytes))
        item[ITEM_OF[block]] = store.putDagCbor(Fixtures.outputItem2(
            [[sample: ITEM_OF[block]], Fixtures.leaf2(NAMES[block], raw[block], SIZES[block])]))
    }

    /** One run of the fixture into `store`, its blocks first, and its Store Log entry. */
    private Cid putRun(LocalBlockStore store, String name, String asserter, List<String> blocks, int minute, long loggedAt) {
        blocks.each { putItem(store, it) }
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-${name}".toString()))
        final Cid coll = store.putDagCbor(Fixtures.outputCollection(manifest, 'out',
            blocks.collect { String b -> [item[ITEM_OF[b]], ["out/${NAMES[b]}".toString()]] }))
        finished[name] = String.format('2026-10-20T10:%02d:00.000Z', minute)
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(manifest, [coll], [asserted_by: asserter, finished_at: finished[name]]))
        StoreLog.append(store, StoreLogKind.RUN, rc, loggedAt)
        run[name] = rc
        return rc
    }

    private static Cid putClaim(LocalBlockStore store, String asserter, Cid subject, String verb, String attribute, Object value,
                                String timestamp, long loggedAt, List<Cid> supersedes = []) {
        final Map block = Fixtures.claim(subject, verb, attribute, value, supersedes, timestamp)
        block.asserted_by = asserter
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, StoreLogKind.CLAIM, cid, loggedAt)
        return cid
    }

    long now = System.currentTimeMillis()
    Cid sel1
    Cid selD

    /** Both members as the class comment describes them. */
    private void twoStores() {
        putRun(labA, 'run1', 'ana', ['A', 'B', 'C'], 1, now - 9_000_000L)
        putRun(labA, 'run2', 'ben', ['C', 'D'], 2, now - 8_000_000L)
        sel1 = labA.putDagCbor(new Selection('ana', [Selection.item(item.i2, [])], []).toCbor())
        StoreLog.append(labA, StoreLogKind.SELECTION, sel1, now - 7_000_000L)
        putClaim(labA, 'ben', run.run1, Claim.ADD, Claim.PIN, 'keep', '2026-10-21T09:00:00.000Z', now - 6_000_000L)

        putRun(labB, 'run2', 'ben', ['C', 'D'], 2, now - 8_000_000L)
        putRun(labB, 'run3', 'cy', ['D', 'E', 'F'], 3, now - 5_000_000L)
        putClaim(labB, 'cy', run.run3, Claim.SET, Claim.RETAIN, Claim.LINEAGE, '2026-10-22T09:00:00.000Z', now - 4_000_000L)
        putClaim(labB, 'ana', item.i4, Claim.ADD, Claim.PIN, 'mine', '2026-10-22T10:00:00.000Z', now - 3_000_000L)
    }

    /** A cache that caught both members up, as one session over the composition would. */
    private Index cacheOfBoth() {
        final Index index = Index.open(tmp.resolve("cache-${UUID.randomUUID()}.sqlite"))
        index.catchUp(labA, StoreLog.of(labA), 'labA')
        index.catchUp(labB, StoreLog.of(labB), 'labB')
        return index
    }

    private Path snapshot(Index index, String member, Long grace = null) {
        final Path root = tmp.resolve("out-${member}-${UUID.randomUUID()}")
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(root)
        final IndexSnapshot.Result r = IndexSnapshot.write(index, member, storage, 0L, null, tmp.resolve('t'), null, false, grace)
        assert r.written
        return IndexSnapshot.pathIn(root)
    }

    // -- reading a snapshot --------------------------------------------------

    private static List<Map> rows(Path db, String sql) {
        Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
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

    /** Root rows with their stats, keyed by "kind cid asserter" so the hash cids stay out of the expectations. */
    private Map<String, Map> roots(Path db) {
        final Map<String, String> label = [:]
        run.each { String n, Cid c -> label[c.toString()] = n }
        item.each { String n, Cid c -> label[c.toString()] = n }
        if( sel1 ) label[sel1.toString()] = 'sel1'
        if( selD ) label[selD.toString()] = 'selD'
        return rows(db, '''SELECT r.kind, r.cid, r.asserted_by, r.state, r.since, s.reachable_bytes, s.reachable_blocks,
                                  s.exclusive_bytes, s.exclusive_blocks, s.pinned_bytes, s.partial
                           FROM root r JOIN root_stats s ON s.root_id = r.id''').collectEntries { Map r ->
            [("${r.kind} ${label[r.cid] ?: r.cid} ${r.asserted_by}".toString()):
                 [state: r.state, since: r.since, reachable: r.reachable_bytes, blocks: r.reachable_blocks,
                  exclusive: r.exclusive_bytes, exclusiveBlocks: r.exclusive_blocks, pinned: r.pinned_bytes, partial: r.partial]]
        }
    }

    private static Map storeStats(Path db) {
        final List<Map> all = rows(db, 'SELECT * FROM store_stats')
        assert all.size() == 1
        return all[0]
    }

    private static String loggedAt(long millis) {
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC)
            .format(java.time.Instant.ofEpochMilli(millis))
    }

    // -- the two-store fixture -----------------------------------------------

    def 'labA: store, asserter, pipeline and root stats'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then: 'the store'
        // stored: A B C D = 1+2+4+8 = 15 in 4 blocks. referenced: c1 1+2+4 + c2 4+8 = 19.
        // pinned: ben's pin on run1 reaches A B C = 7. held: run1 run2 sel1 reach A B C D, minus pinned: D = 8.
        // releasing: nothing is reached only by a released or deleted root, 0.
        // shared by two or more run roots: C (run1, run2) = 4.
        storeStats(a) == [stored_bytes: 15, stored_blocks: 4, referenced_bytes: 19, pinned_bytes: 7,
                          held_bytes: 8, releasing_bytes: 0, shared_bytes: 4, partial: 0]

        and: 'the roots'
        // Content roots: run1 {A B C}, run2 {C D}, sel1 {B}, ben's pin {A B C}, all held or pinned.
        // Exclusive: run2 alone reaches D = 8. A, B and C each have two or more content roots.
        // pinned_bytes: each closure's part of the pinned set {A B C}.
        // since: a held run's finished_at, a held Selection's log entry, a pin's Claim timestamp.
        roots(a) == [
            'run run1 ana'      : [state: 'held', since: finished.run1, reachable: 7, blocks: 3, exclusive: 0, exclusiveBlocks: 0, pinned: 7, partial: 0],
            'run run2 ben'      : [state: 'held', since: finished.run2, reachable: 12, blocks: 2, exclusive: 8, exclusiveBlocks: 1, pinned: 4, partial: 0],
            'selection sel1 ana': [state: 'held', since: loggedAt(now - 7_000_000L), reachable: 2, blocks: 1, exclusive: 0, exclusiveBlocks: 0, pinned: 2, partial: 0],
            'pin run1 ben'      : [state: 'pinned', since: '2026-10-21T09:00:00.000Z', reachable: 7, blocks: 3, exclusive: 0, exclusiveBlocks: 0, pinned: 7, partial: 0],
        ]

        and: 'the asserters'
        // ana: run1 and sel1 reach A B C = 7; ben's pin reaches each of them too, so exclusive 0.
        // ben: run2 and the pin reach A B C D = 15; only D is ben's alone = 8; the pin reaches A B C = 7.
        rows(a, 'SELECT * FROM asserter_stats ORDER BY asserted_by') == [
            [asserted_by: 'ana', reachable_bytes: 7, exclusive_bytes: 0, pinned_bytes: 0, releasing_bytes: 0],
            [asserted_by: 'ben', reachable_bytes: 15, exclusive_bytes: 8, pinned_bytes: 7, releasing_bytes: 0],
        ]

        and: 'the pipeline'
        // Two held runs, together A B C D = 15 in 4 blocks; nothing releasing.
        rows(a, 'SELECT * FROM pipeline_stats') == [[pipeline: 'p', runs: 2, bytes: 15, blocks: 4, releasing_bytes: 0]]

        and: 'the content tables hold labA closure only: the four raw blocks and the four items naming them'
        rows(a, 'SELECT cid FROM block ORDER BY cid')*.cid == ['A', 'B', 'C', 'D'].collect { raw[it].toString() }.sort()
        rows(a, 'SELECT owner_cid FROM block_ref ORDER BY owner_cid')*.owner_cid == ['i1', 'i2', 'i3', 'i4'].collect { item[it].toString() }.sort()

        cleanup:
        cache?.close()
    }

    def 'labB: store, asserter, pipeline and root stats'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when:
        Path b = snapshot(cache, 'labB')

        then: 'the store'
        // stored: C D E F = 4+8+16+32 = 60 in 4 blocks. referenced: c2 4+8 + c3 8+16+32 = 68.
        // pinned: ana's pin on i4 reaches D = 8. held: run2 reaches C D, minus pinned: C = 4.
        // releasing: run3 is released and alone reaches E F = 48. 8 + 4 + 48 = 60.
        // shared by two or more run roots: D (run2, run3) = 8.
        storeStats(b) == [stored_bytes: 60, stored_blocks: 4, referenced_bytes: 68, pinned_bytes: 8,
                          held_bytes: 4, releasing_bytes: 48, shared_bytes: 8, partial: 0]

        and: 'the roots'
        // Content roots: run2 {C D} and ana's pin {D}; run3 {D E F} is released.
        // run2 exclusive: C = 4, since labB has no run1; D is kept by ana's pin here.
        // run3 exclusive: E + F = 48; D is held by run2 and the pin, and nobody pins i6 in labB.
        // The pin: D is reached by run2 too, so 0.
        // since: the release Claim's timestamp for run3.
        roots(b) == [
            'run run2 ben': [state: 'held', since: finished.run2, reachable: 12, blocks: 2, exclusive: 4, exclusiveBlocks: 1, pinned: 8, partial: 0],
            'run run3 cy' : [state: 'released', since: '2026-10-22T09:00:00.000Z', reachable: 56, blocks: 3, exclusive: 48, exclusiveBlocks: 2, pinned: 8, partial: 0],
            'pin i4 ana'  : [state: 'pinned', since: '2026-10-22T10:00:00.000Z', reachable: 8, blocks: 1, exclusive: 0, exclusiveBlocks: 0, pinned: 8, partial: 0],
        ]

        and: 'the asserters'
        // ana: the pin reaches D = 8, which run2 (ben) reaches too, so exclusive 0; pinned 8.
        // ben: run2 reaches C D = 12; C is ben's alone = 4.
        // cy: run3 is released, so reachable 0; it reaches the releasing E F = 48.
        rows(b, 'SELECT * FROM asserter_stats ORDER BY asserted_by') == [
            [asserted_by: 'ana', reachable_bytes: 8, exclusive_bytes: 0, pinned_bytes: 8, releasing_bytes: 0],
            [asserted_by: 'ben', reachable_bytes: 12, exclusive_bytes: 4, pinned_bytes: 0, releasing_bytes: 0],
            [asserted_by: 'cy', reachable_bytes: 0, exclusive_bytes: 0, pinned_bytes: 0, releasing_bytes: 48],
        ]

        and: 'the pipeline'
        // Two runs. run2 holds C D = 12 in 2 blocks; run3 is released and unpinned, its releasing part is E F = 48.
        rows(b, 'SELECT * FROM pipeline_stats') == [[pipeline: 'p', runs: 2, bytes: 12, blocks: 2, releasing_bytes: 48]]

        cleanup:
        cache?.close()
    }

    def 'the same run frees different bytes in each store, because the pins differ'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')
        Path b = snapshot(cache, 'labB')

        then:
        // labA: D (8) is run2's alone. labB: D is kept by ana's pin, and C (4) has no run1 to share it.
        roots(a)['run run2 ben'].exclusive == 8
        roots(b)['run run2 ben'].exclusive == 4

        cleanup:
        cache?.close()
    }

    /** A fresh cache of labA alone, as a session after each step would have. */
    private Index cacheOfA() {
        final Index index = Index.open(tmp.resolve("cache-${UUID.randomUUID()}.sqlite"))
        index.catchUp(labA, StoreLog.of(labA), 'labA')
        return index
    }

    private Map<String, Map> asserters(Path db) {
        rows(db, 'SELECT * FROM asserter_stats ORDER BY asserted_by').collectEntries { Map r ->
            [(r.asserted_by): [reachable: r.reachable_bytes, exclusive: r.exclusive_bytes, pinned: r.pinned_bytes, releasing: r.releasing_bytes]]
        }
    }

    private List<Map> overlap(Path db, Cid subject) {
        rows(db, "SELECT asserted_by, bytes FROM pinned_overlap WHERE subject_cid = '${subject}' ORDER BY asserted_by")
    }

    def 'a pinned Selection is a pin root: pinned, then deleted and still pinned, then unpinned (F5)'() {
        given: 'labA: run1 (ana) A B C, run2 (ben) C D, and selD, ana\'s Selection of i4 (D), pinned by cy'
        putRun(labA, 'run1', 'ana', ['A', 'B', 'C'], 1, now - 9_000_000L)
        putRun(labA, 'run2', 'ben', ['C', 'D'], 2, now - 8_000_000L)
        selD = labA.putDagCbor(new Selection('ana', [Selection.item(item.i4, [])], []).toCbor())
        StoreLog.append(labA, StoreLogKind.SELECTION, selD, now - 7_000_000L)
        final Cid pin = putClaim(labA, 'cy', selD, Claim.ADD, Claim.PIN, 'figure 3', '2026-10-21T09:00:00.000Z', now - 6_000_000L)
        Index cache = cacheOfA()

        when:
        Path held = snapshot(cache, 'labA')

        then: 'held and pinned'
        // stored A B C D = 15, referenced c1 7 + c2 12 = 19. cy's pin on selD reaches D = 8: pinned.
        // held: A B C = 7. shared by two runs: C = 4.
        storeStats(held) == [stored_bytes: 15, stored_blocks: 4, referenced_bytes: 19, pinned_bytes: 8,
                             held_bytes: 7, releasing_bytes: 0, shared_bytes: 4, partial: 0]
        // run2: C is run1's too, D is selD's and the pin's, so exclusive 0. selD and the pin share D.
        roots(held) == [
            'run run1 ana'      : [state: 'held', since: finished.run1, reachable: 7, blocks: 3, exclusive: 3, exclusiveBlocks: 2, pinned: 0, partial: 0],
            'run run2 ben'      : [state: 'held', since: finished.run2, reachable: 12, blocks: 2, exclusive: 0, exclusiveBlocks: 0, pinned: 8, partial: 0],
            'selection selD ana': [state: 'held', since: loggedAt(now - 7_000_000L), reachable: 8, blocks: 1, exclusive: 0, exclusiveBlocks: 0, pinned: 8, partial: 0],
            'pin selD cy'       : [state: 'pinned', since: '2026-10-21T09:00:00.000Z', reachable: 8, blocks: 1, exclusive: 0, exclusiveBlocks: 0, pinned: 8, partial: 0],
        ]
        // cy reaches D through the pin; ana and ben reach it too, so nobody's alone.
        asserters(held).cy == [reachable: 8, exclusive: 0, pinned: 8, releasing: 0]
        overlap(held, selD) == [[asserted_by: 'cy', bytes: 8]]
        overlap(held, run.run2) == [[asserted_by: 'cy', bytes: 8]]
        overlap(held, run.run1) == []

        when: 'ana deletes selD; cy\'s pin still holds'
        putClaim(labA, 'ana', selD, Claim.DELETE, null, null, '2026-10-22T09:00:00.000Z', now - 5_000_000L)
        cache.close()
        cache = cacheOfA()
        Path hidden = snapshot(cache, 'labA')

        then: 'the pin keeps D, so releasing run2 still frees nothing'
        storeStats(hidden) == [stored_bytes: 15, stored_blocks: 4, referenced_bytes: 19, pinned_bytes: 8,
                               held_bytes: 7, releasing_bytes: 0, shared_bytes: 4, partial: 0]
        roots(hidden)['selection selD ana'].state == 'deleted'
        roots(hidden)['run run2 ben'].exclusive == 0
        roots(hidden)['pin selD cy'] == [state: 'pinned', since: '2026-10-21T09:00:00.000Z', reachable: 8, blocks: 1, exclusive: 0, exclusiveBlocks: 0, pinned: 8, partial: 0]
        asserters(hidden).cy.pinned == 8
        overlap(hidden, selD) == [[asserted_by: 'cy', bytes: 8]]

        when: 'cy\'s del pin supersedes the pin'
        putClaim(labA, 'cy', selD, Claim.DEL, Claim.PIN, null, '2026-10-23T09:00:00.000Z', now - 4_000_000L, [pin])
        cache.close()
        cache = cacheOfA()
        Path unpinned = snapshot(cache, 'labA')

        then: 'no pin root; run2 alone among content roots reaches D, so releasing it frees 8'
        storeStats(unpinned) == [stored_bytes: 15, stored_blocks: 4, referenced_bytes: 19, pinned_bytes: 0,
                                 held_bytes: 15, releasing_bytes: 0, shared_bytes: 4, partial: 0]
        roots(unpinned).keySet() == ['run run1 ana', 'run run2 ben', 'selection selD ana'] as Set
        roots(unpinned)['run run2 ben'].exclusive == 8
        asserters(unpinned).cy == null
        rows(unpinned, 'SELECT * FROM pinned_overlap') == []

        cleanup:
        cache?.close()
    }

    def 'a Selection here of an item only another member holds counts its blocks here (R8)'() {
        given: 'labA selects i6, which only labB run3 outputs'
        twoStores()
        final Cid sel2 = labA.putDagCbor(new Selection('ana', [Selection.item(item.i6, [])], []).toCbor())
        StoreLog.append(labA, StoreLogKind.SELECTION, sel2, now - 1_000_000L)
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        // F (32) is reachable from sel2 in labA, and stored there by this ruling: 15 + 32 = 47.
        rows(a, "SELECT s.reachable_bytes FROM root r JOIN root_stats s ON s.root_id = r.id WHERE r.cid = '${sel2}'") == [[reachable_bytes: 32]]
        storeStats(a).stored_bytes == 47
        // i6's references count once here, as its membership of c3 does in labB: 19 + 32 = 51, so dedup stays >= 1.
        storeStats(a).referenced_bytes == 51

        cleanup:
        cache?.close()
    }

    def 'the content copy searches the cache block_ref by owner, never scanning it'() {
        given: 'a snapshot database with a cache attached as src, both on the index schema'
        Index.open(tmp.resolve('cache.sqlite')).close()
        Connection c = DriverManager.getConnection("jdbc:sqlite:${tmp.resolve('snap.sqlite')}")
        Index.ddl().each { String sql -> c.createStatement().execute(sql) }
        c.createStatement().execute("ATTACH DATABASE '${tmp.resolve('cache.sqlite')}' AS src")

        when:
        List<String> plans = IndexSnapshot.COPY_CONTENT.collectMany { String sql ->
            final def rs = c.createStatement().executeQuery('EXPLAIN QUERY PLAN ' + sql)
            final List<String> details = []
            while( rs.next() ) details << rs.getString('detail')
            details
        }

        then:
        plans.any { it.startsWith('SEARCH r USING INDEX block_ref_owner') }
        !plans.any { it ==~ /SCAN (r|block_ref|src\.block_ref|b|block)\b.*/ }

        cleanup:
        c?.close()
    }

    // -- seeding -------------------------------------------------------------

    private static final List<String> STATS_TABLES = ['root', 'root_stats', 'root_shared', 'store_stats', 'asserter_stats',
                                                      'pipeline_stats', 'collection_size', 'item_size', 'kind_stats', 'pinned_overlap']

    private static List<String> textRows(Path db, String table) {
        return rows(db, "SELECT * FROM ${table}".toString()).collect { Map r -> r.values().collect { String.valueOf(it) }.join('|') }.sort()
    }

    def 'a cache seeded from labA snapshot has the content rows of a full ingest and writes the same stats'() {
        given:
        twoStores()
        Index writer = cacheOfBoth()
        Path a = snapshot(writer, 'labA')
        Index full = Index.open(tmp.resolve('full.sqlite'))
        full.catchUp(labA, StoreLog.of(labA), 'labA')
        Index seeded = Index.open(tmp.resolve('seeded.sqlite'))

        when:
        seeded.seedFrom(a, 'labA')
        Path again = snapshot(seeded, 'labA')

        then: 'block and block_ref as a full ingest of labA wrote them'
        ['block', 'block_ref'].every { String t -> textRows(seeded.file, t) == textRows(full.file, t) }
        textRows(seeded.file, 'block').size() == 4

        and: 'the snapshot written from the seeded cache has the same stats tables'
        STATS_TABLES.every { String t -> textRows(again, t) == textRows(a, t) }
        textRows(again, 'root').size() == 4

        cleanup:
        writer?.close(); full?.close(); seeded?.close()
    }

    // -- grace ---------------------------------------------------------------

    def 'sweep_grace_ms is in the meta when the writer knows it, and absent otherwise'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        expect:
        rows(snapshot(cache, 'labA', GRACE), "SELECT value FROM meta WHERE key = '${IndexSnapshot.SWEEP_GRACE_KEY}'") == [[value: '1209600000']]
        rows(snapshot(cache, 'labA'), "SELECT value FROM meta WHERE key = 'sweep_grace_ms'") == []

        cleanup:
        cache?.close()
    }

    // -- root states and partial -----------------------------------------------

    def 'root states follow ClaimState: a delete hides, a conflicted retain or deletion holds'() {
        given: 'four runs in one member'
        putRun(labA, 'gone', 'ana', ['A'], 1, now - 9_000_000L)
        putRun(labA, 'torn', 'ana', ['B'], 2, now - 8_000_000L)
        putRun(labA, 'split', 'ana', ['C'], 3, now - 7_000_000L)
        putRun(labA, 'back', 'ana', ['D'], 4, now - 6_000_000L)
        and: 'gone is deleted; torn has two retain Claims that do not supersede each other'
        putClaim(labA, 'ana', run.gone, Claim.DELETE, null, null, '2026-10-21T01:00:00.000Z', now - 5_000_000L)
        putClaim(labA, 'ana', run.torn, Claim.SET, Claim.RETAIN, Claim.LINEAGE, '2026-10-21T02:00:00.000Z', now - 5_000_000L)
        putClaim(labA, 'ben', run.torn, Claim.SET, Claim.RETAIN, 'all', '2026-10-21T03:00:00.000Z', now - 5_000_000L)
        and: 'split has two deletion Claims in conflict; back was released, then the release superseded by `set retain all`'
        putClaim(labA, 'ana', run.split, Claim.DELETE, null, null, '2026-10-21T04:00:00.000Z', now - 5_000_000L)
        putClaim(labA, 'ben', run.split, Claim.DEL, null, null, '2026-10-21T05:00:00.000Z', now - 5_000_000L)
        final Cid release = putClaim(labA, 'ana', run.back, Claim.SET, Claim.RETAIN, Claim.LINEAGE, '2026-10-21T06:00:00.000Z', now - 5_000_000L)
        putClaim(labA, 'ana', run.back, Claim.SET, Claim.RETAIN, 'all', '2026-10-21T07:00:00.000Z', now - 4_000_000L, [release])
        Index cache = cacheOfBoth()

        when:
        Map<String, Map> r = roots(snapshot(cache, 'labA'))

        then:
        // gone: deleted since its delete Claim; it holds nothing, so A is releasing and all its own.
        r['run gone ana'].subMap('state', 'since', 'exclusive') == [state: 'deleted', since: '2026-10-21T01:00:00.000Z', exclusive: 1]
        r['run torn ana'].subMap('state', 'since') == [state: 'held', since: finished.torn]
        r['run split ana'].subMap('state', 'since') == [state: 'held', since: finished.split]
        r['run back ana'].subMap('state', 'since') == [state: 'held', since: finished.back]

        and: 'A is releasing; the held runs hold B C D'
        storeStats(snapshot(cache, 'labA')).subMap('releasing_bytes', 'held_bytes') == [releasing_bytes: 1, held_bytes: 14]

        and: 'the pipeline leaves the deleted run out'
        rows(snapshot(cache, 'labA'), 'SELECT runs, bytes FROM pipeline_stats') == [[runs: 3, bytes: 14]]

        cleanup:
        cache?.close()
    }

    def 'a directory that has not arrived makes the store and the roots that reach it partial, and its missing row travels'() {
        given: 'run1 holds A; run2 holds an item whose Leaf addresses a DirectoryManifest that is not in the store'
        putRun(labA, 'run1', 'ana', ['A'], 1, now - 9_000_000L)
        final Cid dir = Fixtures.cidOf([kind: 'DirectoryManifest', schema: 1, entries: []])
        final Cid withDir = labA.putDagCbor(Fixtures.outputItem2([[sample: 'dir'], Fixtures.leaf2('reads', dir, 0)]))
        final Cid manifest = labA.putDagCbor(Fixtures.runManifest(run_name: 'run2', nf_run_hash: 'hash-run2'))
        final Cid coll = labA.putDagCbor(Fixtures.outputCollection(manifest, 'out', [[withDir, ['out/reads']]]))
        final Cid run2 = labA.putDagCbor(Fixtures.runCompletion(manifest, [coll], [asserted_by: 'ben']))
        StoreLog.append(labA, StoreLogKind.RUN, run2, now - 8_000_000L)
        run.run2 = run2
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        rows(a, 'SELECT have_cid, needed_cid FROM missing') == [[have_cid: withDir.toString(), needed_cid: dir.toString()]]
        storeStats(a).partial == 1
        roots(a)['run run2 ben'].partial == 1
        roots(a)['run run1 ana'].partial == 0

        cleanup:
        cache?.close()
    }

    def 'an OutputItem that has not arrived makes the store and the roots that hold its collection partial'() {
        given: 'run1 holds A; run2 outputs i2, which is not in the store; ana pins run2 and selects the missing item'
        putRun(labA, 'run1', 'ana', ['A'], 1, now - 9_000_000L)
        final Cid gone = Fixtures.cidOf(Fixtures.outputItem2([[sample: 'never'], Fixtures.leaf2('n.txt', Fixtures.contentCid('n'), 1)]))
        final Cid manifest = labA.putDagCbor(Fixtures.runManifest(run_name: 'run2', nf_run_hash: 'hash-run2'))
        final Cid coll = labA.putDagCbor(Fixtures.outputCollection(manifest, 'out', [[gone, ['out/n.txt']]]))
        final Cid run2 = labA.putDagCbor(Fixtures.runCompletion(manifest, [coll], [asserted_by: 'ben']))
        StoreLog.append(labA, StoreLogKind.RUN, run2, now - 8_000_000L)
        run.run2 = run2
        putClaim(labA, 'ana', run2, Claim.ADD, Claim.PIN, 'keep', '2026-10-21T09:00:00.000Z', now - 6_000_000L)
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        rows(a, 'SELECT have_cid, needed_cid FROM missing') == [[have_cid: coll.toString(), needed_cid: gone.toString()]]
        storeStats(a).partial == 1
        roots(a)['run run2 ben'].partial == 1
        roots(a)['pin run2 ana'].partial == 1
        roots(a)['run run1 ana'].partial == 0

        cleanup:
        cache?.close()
    }

    def 'a run whose OutputCollection has not arrived is partial, and a missing RunManifest alone is not'() {
        given: 'run2 names a collection that is not in the store; run3 has its collection but not its manifest'
        putRun(labA, 'run1', 'ana', ['A'], 1, now - 9_000_000L)
        final Cid manifest2 = labA.putDagCbor(Fixtures.runManifest(run_name: 'run2', nf_run_hash: 'hash-run2'))
        final Cid goneColl = Fixtures.cidOf(Fixtures.outputCollection(manifest2, 'out', []))
        final Cid run2 = labA.putDagCbor(Fixtures.runCompletion(manifest2, [goneColl], [asserted_by: 'ben']))
        StoreLog.append(labA, StoreLogKind.RUN, run2, now - 8_000_000L)
        run.run2 = run2
        final Cid manifest3 = Fixtures.cidOf(Fixtures.runManifest(run_name: 'run3', nf_run_hash: 'hash-run3'))
        final Cid coll3 = labA.putDagCbor(Fixtures.outputCollection(manifest3, 'out', [[item.i1, ['out/a.txt']]]))
        final Cid run3 = labA.putDagCbor(Fixtures.runCompletion(manifest3, [coll3], [asserted_by: 'cy']))
        StoreLog.append(labA, StoreLogKind.RUN, run3, now - 7_000_000L)
        run.run3 = run3
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        storeStats(a).partial == 1
        roots(a)['run run2 ben'].partial == 1
        roots(a)['run run3 cy'].partial == 0
        roots(a)['run run1 ana'].partial == 0

        cleanup:
        cache?.close()
    }
}
