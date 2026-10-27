package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.LocalStoreIdStorage
import robsyme.cas.core.StoreIds
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The opt-in rule that leaves a member the cache does not hold and that has
 * no usable Index Snapshot out of catch-up (background store indexing,
 * spec §6.1), and building or pushing any member's snapshot (§6.4, §6.5).
 * The composition is `lab` (writable, local) and `cloud` (local, two logged
 * runs, no snapshot).
 */
class CasSessionUnindexedTest extends Specification {

    static final String ABSENT_LINE = "store member 'cloud' has no Index Snapshot, so the explorer does not list it. " +
        'Index it from the Storage page, or run nf-blocks:snapshot against it.'

    @TempDir
    Path tmp

    Path labRoot
    Path cloudRoot
    ByteArrayOutputStream err
    CasSession session
    Index index

    /** The runs lab holds as blocks with no Store Log entry, so its catch-up scans them and says nothing. */
    int labRuns = 1

    def setup() {
        CasSession.platformIdentity = { String e, String t -> throw new IOException('no platform in tests') } as PlatformIdentity
        CasSession.gitEmail = { -> 'someone@example.org' } as GitEmail
        CasSession.env = [:]
        labRoot = tmp.resolve('lab')
        cloudRoot = tmp.resolve('cloud')
        final LocalBlockStore lab = new LocalBlockStore(labRoot, 'lab', true)
        lab.putDagCbor(Fixtures.runCompletion(lab.putDagCbor(Fixtures.runManifest(run_name: 'lab-1', nf_run_hash: 'lab-1')), []))
        logRun(cloudRoot, 'cloud-1')
        logRun(cloudRoot, 'cloud-2')
        session = newSession('cache')
        index = session.openIndex()
    }

    def cleanup() {
        index?.close()
    }

    private CasSession newSession(String cache) {
        err = new ByteArrayOutputStream()
        final CasConfig cfg = CasConfig.from([cas: [stores : [lab: [location: labRoot.toString()], cloud: [location: cloudRoot.toString()]],
                                                    resolve: ['lab', 'cloud'],
                                                    tmpDir : tmp.resolve('scratch').toString(),
                                                    index  : [path: tmp.resolve("${cache}.sqlite").toString()]]], 'cas://lab')
        return CasSession.forCommand(cfg, new PrintStream(err, true, 'UTF-8'))
    }

    /** One run in the member at {@code root}, announced in its Store Log. */
    private static void logRun(Path root, String run) {
        final LocalBlockStore member = new LocalBlockStore(root, root.fileName.toString(), true)
        final Cid m = member.putDagCbor(Fixtures.runManifest(run_name: run, nf_run_hash: run))
        final Cid rc = member.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(member, StoreLogKind.RUN, rc, System.currentTimeMillis())
    }

    /** A snapshot of cloud written into {@code storage} over any there, built by a scan in a cache of its own, with {@code storeId} in its meta. */
    private void writeCloudSnapshot(LocalSnapshotStorage storage, String storeId) {
        Files.deleteIfExists(Path.of(storage.describe()))
        final Index scratch = Index.open(tmp.resolve("scratch-${UUID.randomUUID()}.sqlite"))
        try {
            final LocalBlockStore cloud = new LocalBlockStore(cloudRoot, 'cloud', false)
            if( storeId != null )
                scratch.reconcileStoreIds([cloud: storeId], { String a, String w -> true })
            scratch.catchUp(cloud, StoreLog.of(cloud), 'cloud')
            assert IndexSnapshot.write(scratch, 'cloud', storage, 0L, null, tmp, storeId).written
        }
        finally {
            scratch.close()
        }
    }

    private String lines() { err.toString('UTF-8') }

    def 'with the rule on, a member with no snapshot that the cache does not hold is left out, named once, never scanned'() {
        given:
        session.leaveUnindexedOut([:])

        when:
        2.times { session.catchUpIndex(index) }

        then:
        lines().readLines() == [ABSENT_LINE]
        session.unindexed() == [cloud: 'absent']
        !index.isIndexed('cloud')
        index.countRows('run') == labRuns
    }

    def 'with the rule on, a member whose snapshot seeds is caught up as before'() {
        given:
        writeCloudSnapshot(new LocalSnapshotStorage(cloudRoot), null)
        session.leaveUnindexedOut([:])

        when:
        final Set<String> failed = session.catchUpIndex(index)

        then:
        failed.isEmpty()
        index.isIndexed('cloud')
        index.metaValue('seeded_from:cloud') != null
        index.countRows('run') == labRuns + 2
        session.unindexed().isEmpty()
        lines().isEmpty()
    }

    def 'the writable member is never left out'() {
        given: 'lab with a logged run and no snapshot'
        logRun(labRoot, 'lab-2')
        session.leaveUnindexedOut([:])

        when:
        session.catchUpIndex(index)

        then:
        index.isIndexed('lab')
        !session.unindexed().containsKey('lab')
        index.countRows('run') == labRuns + 1
    }

    def 'a known member is skipped without a fetch, and a seed source override seeds from a local snapshot'() {
        given: 'cloud has a usable snapshot of its own, but the caller already decided it is not indexed'
        writeCloudSnapshot(new LocalSnapshotStorage(cloudRoot), null)
        session.leaveUnindexedOut([cloud: 'absent'])

        when:
        final Set<String> failed = session.catchUpIndex(index)

        then:
        !failed.contains('cloud')
        !index.isIndexed('cloud')
        session.unindexed() == [cloud: 'absent']
        lines().isEmpty()

        when: 'a fresh session seeds cloud from a snapshot kept elsewhere'
        Files.delete(IndexSnapshot.pathIn(cloudRoot))
        final Path localRoot = tmp.resolve('local-snapshots/cloud')
        writeCloudSnapshot(new LocalSnapshotStorage(localRoot), null)
        index.close()
        session = newSession('fresh')
        index = session.openIndex()
        session.leaveUnindexedOut([:])
        session.seedFrom('cloud', new LocalSnapshotStorage(localRoot))
        session.catchUpIndex(index)

        then:
        index.isIndexed('cloud')
        index.metaValue('seeded_from:cloud') != null
        index.countRows('run') == labRuns + 2
        session.unindexed().isEmpty()
        lines().isEmpty()
    }

    def 'without the rule, catch-up scans a member with no snapshot as before'() {
        when:
        session.catchUpIndex(index)

        then:
        index.isIndexed('cloud')
        index.countRows('run') == labRuns + 2
        lines().readLines() == ["store member 'cloud' has no usable Index Snapshot (absent); indexing it from every block " +
            'instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.']
    }

    def 'notListedLine names the reason unless the snapshot is absent'() {
        expect:
        CasSession.notListedLine('cloud', 'absent') == ABSENT_LINE
        CasSession.notListedLine('cloud', 'unreadable') == "store member 'cloud' has no usable Index Snapshot (unreadable), so the explorer " +
            'does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.'
    }

    def 'member names a composition member, or null'() {
        expect:
        session.member('cloud').alias() == 'cloud'
        session.member('ghost') == null
    }

    def 'ownSnapshotUnusable: absent, another incarnation, usable'() {
        given: 'cloud has a store-id, and the cache has recorded it'
        final String id = StoreIds.ensure(new LocalStoreIdStorage(cloudRoot))
        index.reconcileStoreIds([cloud: id], { String a, String w -> true })

        expect:
        session.ownSnapshotUnusable('cloud', index) == 'absent'

        when: 'a snapshot written for another incarnation'
        writeCloudSnapshot(new LocalSnapshotStorage(cloudRoot), 'f' * 32)

        then:
        session.ownSnapshotUnusable('cloud', index).startsWith('written for another incarnation')

        when: 'a snapshot written for this one'
        writeCloudSnapshot(new LocalSnapshotStorage(cloudRoot), id)

        then:
        session.ownSnapshotUnusable('cloud', index) == null
    }

    def 'ownSnapshotUnusable: a snapshot with no store id whose watermark is not in the Store Log'() {
        given:
        writeCloudSnapshot(new LocalSnapshotStorage(cloudRoot), null)
        cloudRoot.resolve('log').toFile().deleteDir()
        logRun(cloudRoot, 'cloud-3')

        expect:
        session.ownSnapshotUnusable('cloud', index).startsWith('its watermark ')
    }

    def 'buildMemberSnapshot builds any member from the cache and writes nothing to it'() {
        given:
        session.catchUpIndex(index)

        when:
        final IndexSnapshot.Built built = session.buildMemberSnapshot(index, 'cloud')

        then:
        built.runs == 2
        Files.isRegularFile(built.file)
        !Files.exists(IndexSnapshot.pathIn(cloudRoot))
    }

    def 'pushMemberSnapshot writes index/v7.sqlite and the store-id into a non-writable member, and no index.html'() {
        when:
        final String id = session.ensureStoreId('cloud')
        session.catchUpIndex(index)
        index.reconcileStoreIds([cloud: id], { String a, String w -> true })
        final IndexSnapshot.Result r = session.pushMemberSnapshot(index, 'cloud', null, id)

        then:
        r.written
        r.runs == 2
        Files.isRegularFile(cloudRoot.resolve('index/v7.sqlite'))
        Files.exists(cloudRoot.resolve('store-id'))
        session.ensureStoreId('cloud') == id
        !Files.exists(cloudRoot.resolve('index.html'))
        session.ownSnapshotUnusable('cloud', index) == null
    }
}
