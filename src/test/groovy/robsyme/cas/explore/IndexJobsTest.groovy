package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import org.slf4j.LoggerFactory
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.GitEmail
import robsyme.cas.PlatformIdentity
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.LocalStoreIdStorage
import robsyme.cas.core.StoreIds
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogEntry
import robsyme.cas.core.StoreLogKind
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3Failures
import robsyme.cas.s3.S3Head
import robsyme.cas.s3.S3Ops
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout

/**
 * Each member's index state in the explorer and the one job thread that
 * indexes, pushes and refreshes (background store indexing, spec §5, §6.3 to
 * §6.6). The composition is `lab` (writable, local, one run), `cloud` (local,
 * two logged runs, no snapshot, no store-id), `ro` (local, readOnly, no
 * snapshot) and `out` (configured, not in cas.resolve).
 */
@Timeout(60)
class IndexJobsTest extends Specification {

    @TempDir
    Path tmp

    Path labRoot
    Path cloudRoot
    Path localRoot
    CasSession session
    Index shared
    Object putLock
    IndexJobs jobs

    /** The runs lab holds as blocks with no Store Log entry. */
    int labRuns = 1

    /** Every IndexJobs and Index a test opened, stopped and closed in cleanup. */
    List<IndexJobs> opened = []
    List<Index> indexes = []

    Closure<S3Ops> savedS3

    /** IndexJobs logs a failed job's stack trace at DEBUG; the failures here are on purpose. */
    final Logger jobsLog = (Logger) LoggerFactory.getLogger(IndexJobs)
    Level savedLevel

    def setup() {
        savedLevel = jobsLog.level
        jobsLog.level = Level.INFO
        CasSession.platformIdentity = { String e, String t -> throw new IOException('no platform in tests') } as PlatformIdentity
        CasSession.gitEmail = { -> 'someone@example.org' } as GitEmail
        CasSession.env = [:]
        savedS3 = CasSession.s3OpsFactory
        labRoot = tmp.resolve('lab')
        cloudRoot = tmp.resolve('cloud')
        localRoot = IndexPaths.localSnapshotsDir(tmp.resolve('cache.sqlite'))
        final LocalBlockStore lab = new LocalBlockStore(labRoot, 'lab', true)
        lab.putDagCbor(Fixtures.runCompletion(lab.putDagCbor(Fixtures.runManifest(run_name: 'lab-1', nf_run_hash: 'lab-1')), []))
        logRun(new LocalBlockStore(cloudRoot, 'cloud', true), 'cloud-1')
        logRun(new LocalBlockStore(cloudRoot, 'cloud', true), 'cloud-2')
        Files.createDirectories(tmp.resolve('ro'))
        Files.createDirectories(tmp.resolve('out'))
        open()
    }

    def cleanup() {
        opened.each { it.stop(5_000) }
        indexes.each { it.close() }
        CasSession.s3OpsFactory = savedS3
        jobsLog.level = savedLevel
    }

    private CasConfig config(Map cloud = [location: cloudRoot.toString()]) {
        return CasConfig.from([cas: [stores : [lab  : [location: labRoot.toString()], cloud: cloud,
                                               ro   : [location: tmp.resolve('ro').toString(), readOnly: true],
                                               out  : [location: tmp.resolve('out').toString()]],
                                     resolve: ['lab', 'cloud', 'ro'],
                                     tmpDir : tmp.toString(),
                                     index  : [path: tmp.resolve('cache.sqlite').toString()]]], 'cas://lab')
    }

    /** A fresh explorer start over the same cache: a session with the rule on, its first catch-up, then classify. */
    private IndexJobs open(CasConfig cfg = config()) {
        session = CasSession.forCommand(cfg, new PrintStream(new ByteArrayOutputStream(), true, 'UTF-8'))
        session.leaveUnindexedOut([:])
        shared = session.openIndex()
        indexes << shared
        putLock = session.newPut(shared)
        session.catchUpIndex(shared)
        jobs = new IndexJobs(session, shared, putLock, localRoot, ExploreCommand.memberInfoOf(cfg))
        opened << jobs
        jobs.classify()
        return jobs
    }

    /** One run in {@code member}, announced in its Store Log. */
    private static void logRun(BlockStore member, String run) {
        final Cid m = member.putDagCbor(Fixtures.runManifest(run_name: run, nf_run_hash: run))
        final Cid rc = member.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(member, StoreLogKind.RUN, rc, System.currentTimeMillis())
    }

    /** The local snapshot's version tag: size, mtime and inode. */
    private String localTag(String alias) {
        return new LocalSnapshotStorage(localRoot.resolve(alias)).base(tmp).tag
    }

    /** Holds the job at its first `reading` report until {@code release} counts down. */
    private CountDownLatch holdAtReading(CountDownLatch release) {
        final CountDownLatch held = new CountDownLatch(1)
        jobs.onReport = { String p, long d, long t -> if( p == 'reading' ) { held.countDown(); release.await() } }
        return held
    }

    def 'classify: the writable member snapshot, a member with no snapshot none with its reason, one outside cas.resolve outside'() {
        expect:
        jobs.stateOf('lab').state == 'snapshot'
        jobs.stateOf('cloud') == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
        jobs.stateOf('out').state == 'outside'
        jobs.states().keySet() as List == ['lab', 'cloud', 'ro', 'out']
    }

    def 'index locally: each phase in order, then local; nothing is written to the member and the cache holds it'() {
        given:
        final List<String> phases = []
        jobs.onReport = { String p, long d, long t -> phases << p }

        when:
        final IndexJobs.Answer a = jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        a.status == 202 && a.body.index.state == 'indexing'
        a.body.member == 'cloud' && a.body.mode == 'local'
        phases.unique() == ['listing', 'reading', 'ingesting', 'log', 'snapshot']
        jobs.stateOf('cloud').state == 'local' && jobs.stateOf('cloud').local
        jobs.stateOf('cloud').at != null && jobs.stateOf('cloud').progress == null && jobs.stateOf('cloud').mode == 'local'
        Files.isRegularFile(localRoot.resolve('cloud/index/v7.sqlite'))
        jobs.localSnapshot('cloud') == localRoot.resolve('cloud/index/v7.sqlite')
        !jobs.servesOwn('cloud')
        !Files.exists(cloudRoot.resolve('index')) && !Files.exists(cloudRoot.resolve('store-id'))
        shared.isIndexed('cloud') && shared.countRows('run') == labRuns + 2
        !tmp.toFile().list().any { it.startsWith('nf-blocks-index-') }
    }

    def 'while a job runs its state is indexing with the phase, counts and start time'() {
        given:
        final CountDownLatch release = new CountDownLatch(1)
        final CountDownLatch held = holdAtReading(release)

        when:
        jobs.start('cloud', 'local')
        held.await()
        final Map<String, Object> during = jobs.stateOf('cloud')
        release.countDown()
        jobs.awaitIdle(30_000)

        then:
        during.state == 'indexing' && during.mode == 'local' && !during.local
        (during.progress as Map).phase == 'reading' && (during.progress as Map).done == 0L && (during.progress as Map).total > 0L
        (during.progress as Map).startedAt ==~ /\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z/
    }

    def 'index and push writes index/v7.sqlite and a store-id into the member, no index.html, and serves its own'() {
        given:
        final List<String> phases = []
        jobs.onReport = { String p, long d, long t -> phases << p }

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        phases.unique() == ['listing', 'reading', 'ingesting', 'log', 'snapshot', 'push']
        jobs.stateOf('cloud').state == 'snapshot' && jobs.localSnapshot('cloud') == null && jobs.servesOwn('cloud')
        Files.isRegularFile(cloudRoot.resolve('index/v7.sqlite')) && Files.exists(cloudRoot.resolve('store-id'))
        !Files.exists(cloudRoot.resolve('index.html')) && !Files.exists(localRoot.resolve('cloud'))
        shared.isIndexed('cloud')
        !tmp.toFile().list().any { it.startsWith('nf-blocks-index-') }
    }

    def 'push to store on a local member writes without scanning again'() {
        given:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        final List<String> phases = []
        jobs.onReport = { String p, long d, long t -> phases << p }

        when:
        final IndexJobs.Answer a = jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        a.status == 202 && a.body.index.state == 'indexing' && a.body.index.local
        phases.unique() in [['log', 'push'], ['push']]
        jobs.stateOf('cloud').state == 'snapshot' && jobs.stateOf('cloud').mode == 'push'
        Files.isRegularFile(cloudRoot.resolve('index/v7.sqlite')) && Files.exists(cloudRoot.resolve('store-id'))
        !Files.exists(localRoot.resolve('cloud'))
    }

    def 'a push the guard refuses fails with its reason and keeps the local index'() {
        given: 'cloud indexed here'
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        and: 'another writer scanned a third run that is not in the Store Log, and wrote a snapshot of cloud listing it'
        final LocalBlockStore cloud = new LocalBlockStore(cloudRoot, 'cloud', true)
        cloud.putDagCbor(Fixtures.runCompletion(cloud.putDagCbor(Fixtures.runManifest(run_name: 'cloud-3', nf_run_hash: 'cloud-3')), []))
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        other.catchUp(cloud, StoreLog.of(cloud), 'cloud')
        final LocalSnapshotStorage own = new LocalSnapshotStorage(cloudRoot)
        assert IndexSnapshot.write(other, 'cloud', own, 0L, null, tmp).written
        final String before = own.base(tmp).tag

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'failed'
        jobs.stateOf('cloud').reason == "the store's own snapshot lists more runs than this index, so it was kept"
        jobs.stateOf('cloud').mode == 'push' && jobs.stateOf('cloud').local
        jobs.localSnapshot('cloud') != null && !jobs.servesOwn('cloud')
        own.base(tmp).tag == before
        jobs.unindexedForCopy() == [ro: 'absent']
    }

    def 'a push on a member with no store-id and an empty Store Log keeps the cache and pushes its runs (#how)'() {
        given: 'cloud holds a run as a block only: no Store Log entry and no store-id'
        final Path quietRoot = tmp.resolve('quiet')
        final LocalBlockStore quiet = new LocalBlockStore(quietRoot, 'cloud', true)
        quiet.putDagCbor(Fixtures.runCompletion(quiet.putDagCbor(Fixtures.runManifest(run_name: 'quiet-1', nf_run_hash: 'quiet-1')), []))
        open(config([location: quietRoot.toString()]))
        and: 'ro indexed on this machine'
        jobs.start('ro', 'local')
        jobs.awaitIdle(30_000)
        assert jobs.localSnapshot('ro') != null
        and:
        if( how == 'Push to store' ) {
            jobs.start('cloud', 'local')
            jobs.awaitIdle(30_000)
            assert jobs.stateOf('cloud').state == 'local'
        }

        when: 'the push, then a catch-up as the next save would run'
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)
        synchronized( putLock ) {
            session.catchUpIndex(shared)
        }

        then:
        jobs.stateOf('cloud').state == 'snapshot'
        new LocalSnapshotStorage(quietRoot).base(tmp).runs == 1
        shared.storeIdOf('cloud') == StoreIds.read(quiet)
        shared.countRows('run') == labRuns + 1
        shared.isIndexed('ro') && jobs.localSnapshot('ro') != null

        where:
        how << ['Index and push', 'Push to store']
    }

    def 'refusals: #alias #mode is #status #error'() {
        expect:
        jobs.start(alias, mode).with { [status, body.error] } == [status, error]
        jobs.start(alias, mode).body.message

        where:
        alias   | mode    || status | error
        'nope'  | 'local' || 400    | 'unknown_member'
        'lab'   | 'local' || 400    | 'writable_member'
        'ro'    | 'push'  || 400    | 'not_writable'
        'cloud' | 'redo'  || 400    | 'bad_request'
        'cloud' | null    || 400    | 'bad_request'
        'out'   | 'local' || 400    | 'bad_request'
    }

    def 'one job at a time: a second start is 409 while the first runs'() {
        given:
        final CountDownLatch release = new CountDownLatch(1)
        final CountDownLatch held = holdAtReading(release)
        jobs.start('cloud', 'local')
        held.await()

        when:
        final IndexJobs.Answer second = jobs.start('ro', 'local')
        release.countDown()
        jobs.awaitIdle(30_000)

        then:
        second.status == 409
        second.body == [error: 'index_running', message: 'another store is being indexed; try again when it has finished']
        jobs.stateOf('ro').state == 'none'
        jobs.start('cloud', 'local').with { [status, body.error] } == [400, 'already_indexed']
    }

    def 'a write to the cache while a job scans is not held behind it'() {
        given:
        final CountDownLatch release = new CountDownLatch(1)
        final CountDownLatch held = holdAtReading(release)
        final LocalBlockStore lab = new LocalBlockStore(labRoot, 'lab', true)
        final Cid cid = lab.putDagCbor(Fixtures.runManifest(run_name: 'lab-2', nf_run_hash: 'lab-2'))
        final StoreLogEntry entry = StoreLog.parse(StoreLog.entryName(StoreLogKind.RUN, cid, System.currentTimeMillis()))
        jobs.start('cloud', 'local')
        held.await()

        when:
        final long t0 = System.nanoTime()
        synchronized( putLock ) {
            shared.recordLogEntry(entry, 'lab')
        }

        then:
        (System.nanoTime() - t0) < 1_000_000_000L
        !shared.isIndexed('cloud')

        cleanup:
        release.countDown()
        jobs.awaitIdle(30_000)
    }

    def 'stop() ends a running job and leaves the member as it was'() {
        given:
        final CountDownLatch held = holdAtReading(new CountDownLatch(1))
        jobs.start('cloud', 'local')
        held.await()

        when:
        final long t0 = System.currentTimeMillis()
        jobs.stop(5_000)

        then:
        System.currentTimeMillis() - t0 < 5_000
        jobs.stateOf('cloud') == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
        !shared.isIndexed('cloud') && !Files.exists(localRoot.resolve('cloud'))
        !tmp.toFile().list().any { it.startsWith('nf-blocks-index-') }
    }

    def 'a stop that lands once the cache holds the member never puts it back to none'() {
        given: 'a stop between the seed and placing the local snapshot, as an interrupted copy would throw'
        jobs.onSeeded = { -> jobs.stop(0); throw new CancellationException('stopped while placing') }

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        shared.isIndexed('cloud')
        jobs.stateOf('cloud').state == 'indexing'
        jobs.unindexedForCopy() == [cloud: IndexJobs.BEING_INDEXED, ro: 'absent']
    }

    def 'once the cache holds the member a stop waits for its local snapshot to be placed'() {
        given:
        jobs.onSeeded = { -> jobs.stop(0) }

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        shared.isIndexed('cloud')
        jobs.stateOf('cloud').state == 'local' && jobs.localSnapshot('cloud') != null
    }

    def 'a CancellationException that is not a stop fails the job'() {
        given:
        jobs.onReport = { String p, long d, long t -> if( p == 'reading' ) throw new CancellationException('not a stop') }

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'failed'
        (jobs.stateOf('cloud').reason as String).contains('not a stop')
    }

    def 'stop() before a submitted job has started leaves no job running'() {
        given: 'the job thread busy, so the job waits to start'
        final CountDownLatch busy = new CountDownLatch(1)
        jobs.thread.execute({ ->
            try { busy.await() } catch( InterruptedException e ) { }
        } as Runnable)
        jobs.start('cloud', 'local')

        when:
        jobs.stop(5_000)

        then:
        jobs.awaitIdle(1_000)
        jobs.stateOf('cloud') == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
    }

    def 'a local member whose snapshot a cache reset deleted reads none and can be indexed again'() {
        given:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        assert jobs.stateOf('cloud').state == 'local'

        when:
        synchronized( putLock ) {
            shared.reset()
        }

        then:
        jobs.stateOf('cloud').state == 'none' && !jobs.stateOf('cloud').local
        jobs.localSnapshot('cloud') == null && jobs.localRootsForCopy() == [:]
        jobs.unindexedForCopy().containsKey('cloud')

        when:
        final IndexJobs.Answer again = jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        again.status == 202
        jobs.stateOf('cloud').state == 'local' && jobs.localSnapshot('cloud') != null
        shared.isIndexed('cloud')
    }

    def 'a held member whose own snapshot cannot be looked at when the explorer starts is snapshot'() {
        given: 'cloud in a bucket with a snapshot of its own'
        final RefusingS3 bucket = new RefusingS3('member-bucket')
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        final CasConfig cfg = config([location: 's3://member-bucket/cloud'])
        logRun(new S3BlockStore(bucket, cfg.remoteOf('cloud').prefix, 'cloud', true, tmp), 'cloud-1')
        open(cfg)
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)
        assert jobs.stateOf('cloud').state == 'snapshot'

        when: 'the next start finds every read of the bucket refused'
        bucket.refuseAll = true
        final IndexJobs again = open(cfg)

        then:
        again.stateOf('cloud').state == 'snapshot'
        again.servesOwn('cloud') && again.localSnapshot('cloud') == null
    }

    def 'index and push replaces an own snapshot from an earlier incarnation that lists more runs'() {
        given: 'cloud has an own snapshot with no store_id whose watermark is not in its Store Log'
        final LocalBlockStore elsewhere = new LocalBlockStore(tmp.resolve('elsewhere'), 'cloud', true)
        (1..3).each { logRun(elsewhere, "old-${it}".toString()) }
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        other.catchUp(elsewhere, StoreLog.of(elsewhere), 'cloud')
        assert IndexSnapshot.write(other, 'cloud', new LocalSnapshotStorage(cloudRoot), 0L, null, tmp).written
        open()
        assert (jobs.stateOf('cloud').reason as String).contains('earlier incarnation')

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'snapshot'
        new LocalSnapshotStorage(cloudRoot).base(tmp).runs == 2
    }

    /** cloud in a bucket with two logged runs; the session is opened over it once, so its store-id can be made. */
    private Map s3Cloud() {
        final RefusingS3 bucket = new RefusingS3('member-bucket')
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        final CasConfig cfg = config([location: 's3://member-bucket/cloud'])
        final S3BlockStore cloud = new S3BlockStore(bucket, cfg.remoteOf('cloud').prefix, 'cloud', true, tmp)
        logRun(cloud, 'cloud-1')
        logRun(cloud, 'cloud-2')
        open(cfg)
        return [bucket: bucket, cfg: cfg, cloud: cloud]
    }

    def 'Index and push over an S3 member own snapshot from another incarnation replaces it'() {
        given: 'cloud has a store-id, and its own snapshot was written for another id'
        final Map s3 = s3Cloud()
        final String id = session.ensureStoreId('cloud')
        final String otherId = StoreIds.newId()
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        other.reconcileStoreIds([cloud: otherId], { String a, String w -> true })
        other.catchUp(s3.cloud as BlockStore, StoreLog.of(s3.cloud as BlockStore), 'cloud')
        assert IndexSnapshot.write(other, 'cloud', session.snapshotsOf('cloud'), 0L, null, tmp, otherId).written
        open(s3.cfg as CasConfig)
        assert (jobs.stateOf('cloud').reason as String).startsWith('written for another incarnation')

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'snapshot'
        session.snapshotsOf('cloud').base(tmp).storeId == id
    }

    def 'Index and push over an S3 member own snapshot from an earlier incarnation that lists more runs replaces it'() {
        given: 'its own snapshot has no store_id, three runs, and a watermark its Store Log lacks'
        final Map s3 = s3Cloud()
        final LocalBlockStore elsewhere = new LocalBlockStore(tmp.resolve('elsewhere'), 'cloud', true)
        (1..3).each { logRun(elsewhere, "old-${it}".toString()) }
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        other.catchUp(elsewhere, StoreLog.of(elsewhere), 'cloud')
        assert IndexSnapshot.write(other, 'cloud', session.snapshotsOf('cloud'), 0L, null, tmp).written
        open(s3.cfg as CasConfig)
        assert (jobs.stateOf('cloud').reason as String).contains('earlier incarnation')

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'snapshot'
        session.snapshotsOf('cloud').base(tmp).runs == 2
    }

    def 'Push to store over an S3 member own snapshot from another writer of this incarnation is still refused'() {
        given: 'cloud indexed here, then another writer pushes a snapshot of this incarnation with more runs'
        final Map s3 = s3Cloud()
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        final String id = session.ensureStoreId('cloud')
        (s3.cloud as S3BlockStore).putDagCbor(Fixtures.runCompletion((s3.cloud as S3BlockStore).putDagCbor(Fixtures.runManifest(run_name: 'cloud-3', nf_run_hash: 'cloud-3')), []))
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        other.reconcileStoreIds([cloud: id], { String a, String w -> true })
        other.catchUp(s3.cloud as BlockStore, StoreLog.of(s3.cloud as BlockStore), 'cloud')
        assert IndexSnapshot.write(other, 'cloud', session.snapshotsOf('cloud'), 0L, null, tmp, id).written
        final String before = session.snapshotsOf('cloud').base(tmp).tag

        when:
        jobs.start('cloud', 'push')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'failed'
        jobs.stateOf('cloud').reason == "the store's own snapshot lists more runs than this index, so it was kept"
        session.snapshotsOf('cloud').base(tmp).tag == before
    }

    def 'an S3 member whose reads are refused ends failed with the credentials wording'() {
        given: 'cloud in a bucket whose block reads are refused once a job has listed them'
        final RefusingS3 bucket = new RefusingS3('member-bucket')
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        final CasConfig cfg = config([location: 's3://member-bucket/cloud'])
        final S3BlockStore cloud = new S3BlockStore(bucket, cfg.remoteOf('cloud').prefix, 'cloud', true, tmp)
        logRun(cloud, 'cloud-1')
        logRun(cloud, 'cloud-2')
        open(cfg)
        bucket.armed = true

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        bucket.refused > 0
        jobs.stateOf('cloud').state == 'failed'
        jobs.stateOf('cloud').reason == S3Failures.REFUSED
        jobs.stateOf('cloud').mode == 'local' && !jobs.stateOf('cloud').local && jobs.stateOf('cloud').at != null
        !shared.isIndexed('cloud') && jobs.localSnapshot('cloud') == null
        jobs.unindexedForCopy() == [cloud: S3Failures.REFUSED, ro: 'absent']
        !tmp.toFile().list().any { it.startsWith('nf-blocks-index-') }
    }

    def 'a refresh whose catch-up is refused ends failed with the credentials wording'() {
        given: 'cloud in a bucket, indexed on this machine'
        final RefusingS3 bucket = new RefusingS3('member-bucket')
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        final CasConfig cfg = config([location: 's3://member-bucket/cloud'])
        logRun(new S3BlockStore(bucket, cfg.remoteOf('cloud').prefix, 'cloud', true, tmp), 'cloud-1')
        open(cfg)
        jobs.startQueued()
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        assert jobs.stateOf('cloud').state == 'local'
        and: 'every read of the bucket refused from now on'
        bucket.refuseAll = true

        when: 'a copy into cloud queues a refresh'
        jobs.copied('cloud')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'failed'
        jobs.stateOf('cloud').reason == S3Failures.REFUSED
        jobs.stateOf('cloud').local && jobs.localSnapshot('cloud') != null
    }

    def 'restart: a locally indexed member is local at once and a refresh rebuilds its snapshot'() {
        given:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        final String before = localTag('cloud')
        final long mtime = Files.getLastModifiedTime(localRoot.resolve('cloud/index/v7.sqlite')).toMillis()
        logRun(new LocalBlockStore(cloudRoot, 'cloud', true), 'cloud-3')

        when:
        final IndexJobs again = open()

        then:
        again.stateOf('cloud').state == 'local' && again.stateOf('cloud').local
        again.stateOf('cloud').at == Index.isoMillis(mtime)
        again.localSnapshot('cloud') != null

        when:
        again.startQueued()
        again.awaitIdle(30_000)

        then:
        again.stateOf('cloud').state == 'local'
        localTag('cloud') != before
        shared.countRows('run') == labRuns + 3
    }

    def 'a member the cache holds without a local snapshot is indexing until its snapshot is built'() {
        given: 'the cache caught up cloud with the rule off (ro left out, so it stays not indexed)'
        final CasSession plain = CasSession.forCommand(config(), new PrintStream(new ByteArrayOutputStream(), true, 'UTF-8'))
        plain.catchUpIndex(shared, true, ['ro'] as Set<String>)
        assert shared.isIndexed('cloud')

        when:
        final IndexJobs again = open()

        then:
        again.stateOf('cloud').state == 'indexing' && !again.stateOf('cloud').local
        again.localSnapshot('cloud') == null
        again.unindexedForCopy() == [cloud: IndexJobs.BEING_INDEXED, ro: 'absent']
        again.start('ro', 'local').status == 409

        when:
        again.startQueued()
        again.awaitIdle(30_000)

        then:
        again.stateOf('cloud').state == 'local' && again.stateOf('cloud').local
        again.localSnapshot('cloud') != null
        !Files.exists(cloudRoot.resolve('index'))
    }

    def 'an own snapshot from another incarnation leaves the member none; once indexed, the local one is served'() {
        given: 'cloud has a store-id and an index/v7.sqlite written for another id'
        final String id = StoreIds.ensure(new LocalStoreIdStorage(cloudRoot))
        final String otherId = StoreIds.newId()
        final Index other = Index.open(tmp.resolve('other.sqlite'))
        indexes << other
        final LocalBlockStore cloud = new LocalBlockStore(cloudRoot, 'cloud', false)
        other.reconcileStoreIds([cloud: otherId], { String a, String w -> true })
        other.catchUp(cloud, StoreLog.of(cloud), 'cloud')
        assert IndexSnapshot.write(other, 'cloud', new LocalSnapshotStorage(cloudRoot), 0L, null, tmp, otherId).written

        when:
        open()

        then:
        jobs.stateOf('cloud').state == 'none'
        (jobs.stateOf('cloud').reason as String).startsWith('written for another incarnation')
        !jobs.servesOwn('cloud') && jobs.localSnapshot('cloud') == null

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'local'
        jobs.localSnapshot('cloud') != null && !jobs.servesOwn('cloud')
        shared.storeIdOf('cloud') == id

        when:
        final IndexJobs again = open()

        then:
        again.stateOf('cloud').state == 'local'
        again.localSnapshot('cloud') != null && !again.servesOwn('cloud')
    }

    def 'copied() refreshes a local member; the copy view lists unindexed members and local roots'() {
        given:
        jobs.startQueued()

        expect:
        jobs.unindexedForCopy() == [cloud: 'absent', ro: 'absent']
        jobs.localRootsForCopy() == [:]

        when:
        jobs.start('cloud', 'local')
        jobs.awaitIdle(30_000)
        final String before = localTag('cloud')

        then:
        jobs.unindexedForCopy() == [ro: 'absent']
        jobs.localRootsForCopy() == [cloud: localRoot.resolve('cloud')]

        when:
        logRun(new LocalBlockStore(cloudRoot, 'cloud', true), 'cloud-3')
        jobs.copied('cloud')
        jobs.copied('ro')
        jobs.awaitIdle(30_000)

        then:
        jobs.stateOf('cloud').state == 'local'
        localTag('cloud') != before
        shared.countRows('run') == labRuns + 3
        jobs.stateOf('ro').state == 'none'
    }

    def 'a failure reason redacts local paths; a credentials failure is told in plain words'() {
        expect:
        !jobs.reasonFor(new IOException("cannot read ${tmp.resolve('x')}")).contains(tmp.toString())
        jobs.reasonFor(new RuntimeException('wrapped', S3Exception.builder().statusCode(403).build() as Throwable)) == S3Failures.REFUSED
    }

    /** A bucket whose block GETs answer 403 once it is armed and a block listing has happened, as when credentials expire mid-scan. */
    static class RefusingS3 extends MemoryS3Ops {
        volatile boolean armed
        volatile boolean listed
        /** Every list, HEAD and GET answers 403, as when credentials have expired. */
        volatile boolean refuseAll
        volatile int refused

        RefusingS3(String bucket) { super(bucket) }

        private void refuse() {
            refused++
            throw S3Exception.builder().statusCode(403).message('Access Denied').build()
        }

        @Override
        List list(String prefix, int maxKeys) {
            if( refuseAll )
                refuse()
            if( armed && prefix.contains('blocks/') )
                listed = true
            return super.list(prefix, maxKeys)
        }

        @Override
        S3Head head(String key) {
            if( refuseAll )
                refuse()
            return super.head(key)
        }

        @Override
        InputStream get(String key, String ifMatch, long start, long length) {
            if( refuseAll || (listed && key.contains('/blocks/')) )
                refuse()
            return super.get(key, ifMatch, start, length)
        }
    }
}
