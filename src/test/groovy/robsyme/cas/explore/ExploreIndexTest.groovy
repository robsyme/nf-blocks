package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.CopyService
import robsyme.cas.GitEmail
import robsyme.cas.PlatformIdentity
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.s3.S3Ops
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout

/**
 * The explorer server's side of background store indexing: each member's
 * index object in members.json, GET and POST /api/index, and a member indexed
 * on this machine served from its local snapshot (spec §5, §6.4, §7). The
 * composition is IndexJobsTest's: `lab` (writable), `cloud` (two logged runs,
 * no snapshot), `ro` (readOnly) and `out` (not in cas.resolve).
 */
@Timeout(60)
class ExploreIndexTest extends Specification {

    @TempDir
    Path tmp

    Path labRoot
    Path cloudRoot
    Path localRoot
    CasSession session
    Index shared
    IndexJobs jobs
    ExploreServer server
    String token = ExploreServer.newToken()
    CountDownLatch release = new CountDownLatch(1)
    Closure<S3Ops> savedS3

    def setup() {
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
        final CasConfig cfg = CasConfig.from([cas: [stores : [lab  : [location: labRoot.toString()], cloud: [location: cloudRoot.toString()],
                                                              ro   : [location: tmp.resolve('ro').toString(), readOnly: true],
                                                              out  : [location: tmp.resolve('out').toString()]],
                                                    resolve: ['lab', 'cloud', 'ro'],
                                                    tmpDir : tmp.toString(),
                                                    index  : [path: tmp.resolve('cache.sqlite').toString()]]], 'cas://lab')
        session = CasSession.forCommand(cfg, new PrintStream(new ByteArrayOutputStream(), true, 'UTF-8'))
        session.leaveUnindexedOut([:])
        shared = session.openIndex()
        final Object putLock = session.newPut(shared)
        session.catchUpIndex(shared)
        jobs = new IndexJobs(session, shared, putLock, localRoot, ExploreCommand.memberInfoOf(cfg))
        jobs.classify()
        jobs.startQueued()
    }

    def cleanup() {
        release.countDown()
        server?.stop()
        jobs.stop(5_000)
        shared.close()
        CasSession.s3OpsFactory = savedS3
    }

    private static void logRun(BlockStore member, String run) {
        final Cid m = member.putDagCbor(Fixtures.runManifest(run_name: run, nf_run_hash: run))
        final Cid rc = member.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(member, StoreLogKind.RUN, rc, System.currentTimeMillis())
    }

    /** A server over lab, cloud and ro; with {@code indexing} the jobs are attached. */
    private ExploreServer serve(boolean indexing = true) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : ['lab', 'cloud', 'ro'] ) {
            final MemberFiles own = new LocalMemberFiles(tmp.resolve(alias == 'lab' ? 'lab' : alias))
            members.put(alias, indexing ? new LocalSnapshotFiles(alias, own, jobs) : own)
        }
        final ExploreServer s = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, null, token)
            .memberInfo(ExploreCommand.memberInfoOf(session.config))
        if( indexing )
            s.indexing(jobs)
        server = s
        return s.start(0)
    }

    private Map<String, String> good() {
        return ['Content-Type': 'application/json', 'X-NF-Blocks-Token': token, Origin: "http://127.0.0.1:${server.port}".toString()]
    }

    private RawHttp.Response get(String path) {
        return RawHttp.send(server.port, 'GET', path, [:])
    }

    private RawHttp.Response post(String path, Object body, Map<String, String> headers = good()) {
        final byte[] bytes = body instanceof String ? ((String) body).getBytes('UTF-8') : JsonOutput.toJson(body).getBytes('UTF-8')
        return RawHttp.send(server.port, 'POST', path, headers, bytes)
    }

    private static Object json(RawHttp.Response r) {
        return new JsonSlurper().parse(r.body)
    }

    def 'members.json carries each member index object'() {
        given:
        serve()

        expect:
        json(get('/members.json')).members.find { it.alias == 'cloud' }.index == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
        json(get('/members.json')).members.find { it.alias == 'lab' }.index.state == 'snapshot'
    }

    def 'members.json looks again at members refused at start, so a reload after signing in sees them as they are'() {
        given:
        jobs = Spy(jobs)
        serve()

        when:
        get('/members.json')

        then:
        1 * jobs.recheckUnread()
    }

    def 'members.json has no index key on a server without indexing'() {
        given:
        serve(false)

        expect:
        json(get('/members.json')).members.every { !it.containsKey('index') }
    }

    def 'GET /api/index answers every member; a server without indexing answers 404'() {
        when:
        serve()
        final Map body = (Map) json(get('/api/index'))

        then:
        get('/api/index').status == 200
        body.members.keySet() as List == ['lab', 'cloud', 'ro', 'out']
        body.members.cloud.state == 'none'
        RawHttp.send(server.port, 'HEAD', '/api/index', [:]).status == 200

        when:
        server.stop()
        serve(false)

        then:
        get('/api/index').status == 404
    }

    def 'POST /api/index starts a job: 202, then 409 index_running while it runs'() {
        given:
        serve()
        final CountDownLatch held = new CountDownLatch(1)
        jobs.onReport = { String p, long d, long t -> if( p == 'reading' ) { held.countDown(); release.await() } }

        when:
        final RawHttp.Response first = post('/api/index', [member: 'cloud', mode: 'local'])

        then:
        first.status == 202
        json(first).member == 'cloud'
        json(first).mode == 'local'
        json(first).index.state == 'indexing'
        held.await(10, TimeUnit.SECONDS)

        when:
        final RawHttp.Response second = post('/api/index', [member: 'ro', mode: 'local'])

        then:
        second.status == 409
        json(second).error == 'index_running'
        json(get('/api/index')).members.cloud.state == 'indexing'

        when:
        release.countDown()

        then:
        jobs.awaitIdle(30_000)
        json(get('/api/index')).members.cloud.state == 'local'
    }

    def 'POST /api/index refusals'() {
        given:
        serve()

        expect:
        post('/api/index', [member: 'cloud', mode: 'local'], good() - ['X-NF-Blocks-Token': token]).status == 403
        post('/api/index', [member: 'cloud', mode: 'local'], good() + ['Content-Type': 'text/plain']).status == 415
        with(post('/api/index', [member: 'lab', mode: 'local'])) {
            status == 400
            json(it).error == 'writable_member'
        }
        with(post('/api/index', [member: 'ro', mode: 'push'])) {
            status == 400
            json(it).error == 'not_writable'
        }
        with(post('/api/index', [member: 7])) {
            status == 400
            json(it).error == 'bad_request'
        }
        with(post('/api/index', [member: 'cloud', mode: 3])) {
            status == 400
            json(it).error == 'bad_request'
        }
        with(post('/api/index', '[1]')) {
            status == 400
            json(it).error == 'bad_request'
        }
        with(post('/api/index', 'not json')) {
            status == 400
            json(it).error == 'bad_request'
        }
    }

    def 'POST /api/index without indexing answers 405'() {
        given:
        serve(false)

        expect:
        post('/api/index', [member: 'cloud', mode: 'local']).status == 405
    }

    def 'the snapshot URL of a member indexed on this machine is its local file; a member not indexed answers 404 even with a snapshot of its own'() {
        given:
        serve()
        Files.createDirectories(cloudRoot.resolve('index'))
        Files.write(cloudRoot.resolve('index/v7.sqlite'), 'a snapshot of another incarnation'.bytes)

        expect:
        get('/m/cloud/index/v7.sqlite').status == 404

        when:
        final RawHttp.Response started = post('/api/index', [member: 'cloud', mode: 'local'])

        then:
        started.status == 202
        jobs.awaitIdle(30_000)

        when:
        final RawHttp.Response served = get('/m/cloud/index/v7.sqlite')

        then:
        served.status == 200
        served.body == Files.readAllBytes(localRoot.resolve('cloud/index/v7.sqlite'))
        get('/m/lab/log/').status == 200
    }

    def 'a real copy into a member indexed on this machine queues its refresh'() {
        given:
        server = serve()
        post('/api/index', [member: 'cloud', mode: 'local'])
        assert jobs.awaitIdle(30_000)
        final String before = jobs.stateOf('cloud').at
        Thread.sleep(25)
        server.copies({ Map request, boolean dryRun -> new CopyService.Outcome(200, [copied: 1] as Map<String, Object>) } as ExploreServer.Copier,
            new PrintStream(new ByteArrayOutputStream(), true))

        when:
        final RawHttp.Response copied = post('/api/copy', [root: 'run:x', from: null, to: 'cloud', depth: 'results'])

        then: 'the answer carries the index of the destination, its refresh already running'
        copied.status == 200
        json(copied).copied == 1
        json(copied).index.state == 'indexing' && json(copied).index.local
        jobs.awaitIdle(30_000)
        jobs.stateOf('cloud').state == 'local'
        jobs.stateOf('cloud').at > before
    }

    def 'a real copy into a member with no local snapshot carries its index as it is'() {
        given:
        server = serve()
        server.copies({ Map request, boolean dryRun -> new CopyService.Outcome(200, [copied: 1] as Map<String, Object>) } as ExploreServer.Copier,
            new PrintStream(new ByteArrayOutputStream(), true))

        when:
        final RawHttp.Response copied = post('/api/copy', [root: 'run:x', from: null, to: 'ro', depth: 'results'])

        then:
        copied.status == 200
        json(copied).index == jobs.stateOf('ro')
        json(copied).index.state == 'none'
    }

    def 'a dry run into a member indexed on this machine queues nothing'() {
        given:
        server = serve()
        post('/api/index', [member: 'cloud', mode: 'local'])
        assert jobs.awaitIdle(30_000)
        final String before = jobs.stateOf('cloud').at
        Thread.sleep(25)
        server.copies({ Map request, boolean dryRun -> new CopyService.Outcome(200, [copied: 0] as Map<String, Object>) } as ExploreServer.Copier,
            new PrintStream(new ByteArrayOutputStream(), true))

        when:
        final RawHttp.Response dry = post('/api/copy?dry_run=true', [root: 'run:x', from: null, to: 'cloud', depth: 'results'])

        then:
        dry.status == 200
        !(json(dry) as Map).containsKey('index')
        jobs.awaitIdle(30_000)
        jobs.stateOf('cloud').at == before
    }

    def 'another POST path lists /api/index in its 405'() {
        given:
        serve()

        when:
        final RawHttp.Response r = post('/api/other', [:])

        then:
        r.status == 405
        r.text().contains('only /api/put, /api/copy, /api/handoff and /api/index take a POST')
    }
}
