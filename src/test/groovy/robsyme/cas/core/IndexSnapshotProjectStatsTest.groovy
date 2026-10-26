package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * P4 plan 1.4: `project_stats`, per store, from the current membership
 * Claims the cache knows across the composition. Every expected value is
 * worked out by hand in the comments.
 *
 * Raw blocks A..F of 1, 2, 4, 8, 16 and 32 B; items i1..i6 hold A..F, one each.
 *
 *   labA: run1 (ana) i1 i2 i3, run2 (ben) i3 i4, run3 (cy) i5, released by cy;
 *         sel1 (ana) of i2 and i4, pinned by ana.
 *   labB: run4 (dee) i6.
 *
 * Membership Claims (where each is logged):
 *   P: run1 (labA), sel1 (labA), run4 (labB), run3 (labA, then removed by a
 *      `del project` logged in labB).
 *   Q: run2 (labB), i3 (labA), run3 (labA).
 *
 * So labA's own Claims say run3 is still in P, and leave run2 out of Q: only
 * the composition view gets either right.
 */
class IndexSnapshotProjectStatsTest extends Specification {

    static final long GRACE = 14L * 86_400_000L

    @TempDir Path tmp

    LocalBlockStore labA
    LocalBlockStore labB
    Map<String, Cid> raw = [:]
    Map<String, Cid> item = [:]
    Map<String, Cid> run = [:]
    Cid sel1
    Cid sel1Pin
    Cid projectP
    Cid projectQ
    long now = System.currentTimeMillis()
    long clock = now - 10_000_000L

    def setup() {
        labA = new LocalBlockStore(tmp.resolve('labA'), 'labA', true)
        labB = new LocalBlockStore(tmp.resolve('labB'), 'labB', true)
    }

    // -- building the members ------------------------------------------------

    private static final Map<String, Integer> SIZES = [A: 1, B: 2, C: 4, D: 8, E: 16, F: 32]
    private static final Map<String, String> ITEM_OF = [A: 'i1', B: 'i2', C: 'i3', D: 'i4', E: 'i5', F: 'i6']

    private void putItem(LocalBlockStore store, String block) {
        final byte[] bytes = new byte[SIZES[block]]
        Arrays.fill(bytes, (byte) block.charAt(0))
        raw[block] = store.putStreaming(new ByteArrayInputStream(bytes))
        item[ITEM_OF[block]] = store.putDagCbor(Fixtures.outputItem2(
            [[sample: ITEM_OF[block]], Fixtures.leaf2("${block}.txt".toString(), raw[block], SIZES[block])]))
    }

    private Cid putRun(LocalBlockStore store, String name, String asserter, List<String> blocks, int minute) {
        blocks.each { putItem(store, it) }
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-${name}".toString()))
        final Cid coll = store.putDagCbor(Fixtures.outputCollection(manifest, 'out',
            blocks.collect { String b -> [item[ITEM_OF[b]], ["out/${b}.txt".toString()]] }))
        final String finished = String.format('2026-10-20T10:%02d:00.000Z', minute)
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(manifest, [coll], [asserted_by: asserter, finished_at: finished]))
        StoreLog.append(store, StoreLogKind.RUN, rc, ++clock)
        run[name] = rc
        return rc
    }

    private Cid putClaim(LocalBlockStore store, String asserter, Cid subject, String verb, String attribute, Object value,
                         List<Cid> supersedes = []) {
        final Map block = Fixtures.claim(subject, verb, attribute, value, supersedes, '2026-10-22T09:00:00.000Z')
        block.asserted_by = asserter
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, StoreLogKind.CLAIM, cid, ++clock)
        return cid
    }

    private Cid putProject(LocalBlockStore store, String name) {
        final Cid cid = store.putDagCbor(new Project('p4-test@example.org', name, '', '2026-10-25T09:00:00.000Z').toCbor())
        StoreLog.append(store, StoreLogKind.PROJECT, cid, ++clock)
        return cid
    }

    /** Both members and the membership Claims, as the class comment describes them. */
    private void twoStores() {
        putRun(labA, 'run1', 'ana', ['A', 'B', 'C'], 1)
        putRun(labA, 'run2', 'ben', ['C', 'D'], 2)
        putRun(labA, 'run3', 'cy', ['E'], 3)
        sel1 = labA.putDagCbor(new Selection('ana', [Selection.item(item.i2, []), Selection.item(item.i4, [])], []).toCbor())
        StoreLog.append(labA, StoreLogKind.SELECTION, sel1, ++clock)
        putClaim(labA, 'cy', run.run3, Claim.SET, Claim.RETAIN, Claim.LINEAGE)
        sel1Pin = putClaim(labA, 'ana', sel1, Claim.ADD, Claim.PIN, 'keep')
        putRun(labB, 'run4', 'dee', ['F'], 4)

        projectP = putProject(labA, 'P')
        projectQ = putProject(labA, 'Q')
        putClaim(labA, 'ana', run.run1, Claim.ADD, Claim.PROJECT, projectP)
        putClaim(labA, 'ana', sel1, Claim.ADD, Claim.PROJECT, projectP)
        putClaim(labB, 'dee', run.run4, Claim.ADD, Claim.PROJECT, projectP)
        final Cid added = putClaim(labA, 'cy', run.run3, Claim.ADD, Claim.PROJECT, projectP)
        putClaim(labB, 'cy', run.run3, Claim.DEL, Claim.PROJECT, null, [added])
        putClaim(labB, 'ben', run.run2, Claim.ADD, Claim.PROJECT, projectQ)
        putClaim(labA, 'ana', item.i3, Claim.ADD, Claim.PROJECT, projectQ)
        putClaim(labA, 'cy', run.run3, Claim.ADD, Claim.PROJECT, projectQ)
    }

    private Index cacheOfBoth(Path file = tmp.resolve("cache-${UUID.randomUUID()}.sqlite")) {
        final Index index = Index.open(file)
        index.catchUp(labA, StoreLog.of(labA), 'labA')
        index.catchUp(labB, StoreLog.of(labB), 'labB')
        return index
    }

    private IndexSnapshot.Result write(Index index, String member, String out, long maxBytes = 0L, Closure<Boolean> present = null) {
        IndexSnapshot.write(index, member, new LocalSnapshotStorage(tmp.resolve(out)), maxBytes, null, tmp.resolve('t'), null, false,
            GRACE, present)
    }

    private Path snap(String out) { IndexSnapshot.pathIn(tmp.resolve(out)) }

    private Path snapshot(Index index, String member) {
        final String out = "out-${member}-${UUID.randomUUID()}"
        assert write(index, member, out).written
        return snap(out)
    }

    // -- reading a snapshot --------------------------------------------------

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

    /** project_stats keyed by project name, so the hash cids stay out of the expectations. */
    private Map<String, Map> projectStats(Path db) {
        final Map<String, String> name = [(projectP.toString()): 'P', (projectQ.toString()): 'Q']
        rows(db, 'SELECT * FROM project_stats').collectEntries { Map r ->
            [(name[r.project_cid] ?: r.project_cid): [reachable: r.reachable_bytes, pinned: r.pinned_bytes, held: r.held_bytes,
                                                       releasing: r.releasing_bytes, roots: r.roots]]
        }
    }

    /** n more file references under `owner`, straight into the cache, as a large member's would be. */
    private static void addRefs(Path cache, String owner, int n) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${cache}")
        try {
            c.autoCommit = false
            final def ref = c.prepareStatement('INSERT INTO block_ref(owner_cid, cid, name) VALUES (?, ?, ?)')
            final def block = c.prepareStatement("INSERT INTO block(cid, size, codec) VALUES (?, ?, 'raw')")
            for( int i = 0; i < n; i++ ) {
                final String cid = Fixtures.contentCid("extra-$i").toString()
                ref.setString(1, owner); ref.setString(2, cid); ref.setString(3, "f${i}.txt".toString()); ref.addBatch()
                block.setString(1, cid); block.setLong(2, 1000L + i); block.addBatch()
            }
            ref.executeBatch(); block.executeBatch()
            c.commit()
        }
        finally {
            c.close()
        }
    }

    /**
     * labA:
     *   reach: A run1 (held); B run1, sel1 and ana's pin (pinned); C run1, run2 (held);
     *   D run2, sel1 and ana's pin (pinned); E run3 only, released (releasing).
     *   P: run1 {A B C} and sel1 {B D}; B, which both reach, once. run4 is not here,
     *      run3 was removed. Union A B C D = 15: pinned B D = 10, held A C = 5, roots 2.
     *   Q: run2 {C D}, i3 {C}, run3 {E}. Union C D E = 28: pinned D = 8, held C = 4,
     *      releasing E = 16, roots 3. C, D and B count in both P and Q.
     */
    static final Map<String, Map> LAB_A = [
        P: [reachable: 15, pinned: 10, held: 5, releasing: 0, roots: 2],
        Q: [reachable: 28, pinned: 8, held: 4, releasing: 16, roots: 3],
    ]

    // -- the tests -----------------------------------------------------------

    def 'the cache holds a membership value as DAG-JSON link text'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        expect:
        rows(cache.file, "SELECT DISTINCT value FROM claim_current WHERE attribute = 'project' AND value IS NOT NULL ORDER BY value")*.value ==
            [projectP, projectQ].collect { Cid p -> "{\"/\":\"${p}\"}".toString() }.sort()

        cleanup:
        cache?.close()
    }

    def 'labA: each project is the union of its members here, by the store segments, from the composition Claims'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        projectStats(a) == LAB_A

        and: 'the segments are the store partition restricted to each union'
        rows(a, 'SELECT stored_bytes, pinned_bytes, held_bytes, releasing_bytes FROM store_stats') ==
            [[stored_bytes: 31, pinned_bytes: 10, held_bytes: 5, releasing_bytes: 16]]

        cleanup:
        cache?.close()
    }

    def 'labB: a project counts only its members held here, and a project with none held here has no row'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when:
        Path b = snapshot(cache, 'labB')

        then:
        // P: run4 {F} = 32, held, 1 root; run1, sel1 are labA's. Q: run2's Claim is logged here, but run2 is not: no row.
        projectStats(b) == [P: [reachable: 32, pinned: 0, held: 32, releasing: 0, roots: 1]]

        cleanup:
        cache?.close()
    }

    def 'a pinned Selection member counts as pinned when deleted, and as held once a del pin supersedes the pin (F5)'() {
        given:
        twoStores()
        putClaim(labA, 'ana', sel1, Claim.DELETE, null, null)
        Index deleted = cacheOfBoth()

        when:
        Path a = snapshot(deleted, 'labA')

        then: 'P as before: ana\'s pin on the deleted sel1 still reaches B and D'
        projectStats(a).P == LAB_A.P

        when:
        putClaim(labA, 'ana', sel1, Claim.DEL, Claim.PIN, null, [sel1Pin])
        Index unpinned = cacheOfBoth()
        Path b = snapshot(unpinned, 'labA')

        then: 'P: sel1 is deleted and unpinned, so B (run1) and D (run2) are held. Union A B C D = 15, held 15, roots 2'
        projectStats(b).P == [reachable: 15, pinned: 0, held: 15, releasing: 0, roots: 2]
        // Q: run2 {C D}, i3 {C}, run3 {E}: D is held by run2 now, so pinned 0, held C D = 12, releasing E = 16.
        projectStats(b).Q == [reachable: 28, pinned: 0, held: 12, releasing: 16, roots: 3]

        cleanup:
        deleted?.close()
        unpinned?.close()
    }

    def 'a single-member project agrees with that root in root_stats'() {
        given:
        twoStores()
        final Cid r = putProject(labA, 'R')
        putClaim(labA, 'ben', run.run2, Claim.ADD, Claim.PROJECT, r)
        Index cache = cacheOfBoth()

        when:
        Path a = snapshot(cache, 'labA')

        then:
        // run2 {C D}: reachable 12, pinned D = 8, held C = 4.
        rows(a, "SELECT reachable_bytes, pinned_bytes, held_bytes, releasing_bytes, roots FROM project_stats WHERE project_cid = '${r}'") ==
            [[reachable_bytes: 12, pinned_bytes: 8, held_bytes: 4, releasing_bytes: 0, roots: 1]]
        rows(a, "SELECT s.reachable_bytes, s.pinned_bytes FROM root r JOIN root_stats s ON s.root_id = r.id WHERE r.cid = '${run.run2}' AND r.kind = 'run'") ==
            [[reachable_bytes: 12, pinned_bytes: 8]]

        cleanup:
        cache?.close()
    }

    def 'a block a sweep took leaves the project figures, as it leaves every storage figure'() {
        given:
        twoStores()
        Index cache = cacheOfBoth()

        when: 'E, which only the released run3 reaches, is gone from labA'
        assert write(cache, 'labA', 'swept', 0L, { Cid c -> c != raw.E } as Closure<Boolean>).written

        then:
        // Q: C D = 12, pinned D = 8, held C = 4, nothing releasing; run3 is still one of its 3 roots here.
        projectStats(snap('swept')) == [P: LAB_A.P, Q: [reachable: 12, pinned: 8, held: 4, releasing: 0, roots: 3]]
        rows(snap('swept'), 'SELECT stored_bytes, releasing_bytes FROM store_stats') == [[stored_bytes: 15, releasing_bytes: 0]]

        cleanup:
        cache?.close()
    }

    def 'a lean snapshot has no project_stats rows: over the cap after a full build, and out of memory'() {
        given: 'labA with 10,000 more file references under i1, so its figures dwarf its records'
        twoStores()
        Index cache = cacheOfBoth()
        addRefs(cache.file, item.i1.toString(), 10_000)
        final long full = write(cache, 'labA', 'full').bytes
        assert rows(snap('full'), 'SELECT count(*) AS n FROM project_stats')[0].n == 2

        when: 'a bytes-per-reference figure far too low, so the estimate fits and the full build passes the cap'
        cache.setMetaValue(IndexSnapshot.BYTES_PER_REF_KEY + ':labA', '1')
        def over = write(cache, 'labA', 'over', full - 1)

        then:
        over.written
        over.omitted == IndexSnapshot.OVER_CAP
        rows(snap('over'), 'SELECT count(*) AS n FROM asserter_stats')[0].n == 0
        rows(snap('over'), 'SELECT count(*) AS n FROM project_stats')[0].n == 0

        when: 'the presence lookup for the released run3 runs out of memory'
        Closure<Boolean> oom = { Cid c -> throw new OutOfMemoryError('Java heap space') } as Closure<Boolean>
        def ran = write(cache, 'labA', 'oom', 0L, oom)

        then:
        ran.written
        ran.omitted == IndexSnapshot.OUT_OF_MEMORY
        rows(snap('oom'), 'SELECT count(*) AS n FROM asserter_stats')[0].n == 0
        rows(snap('oom'), 'SELECT count(*) AS n FROM project_stats')[0].n == 0

        cleanup:
        cache?.close()
    }
}
