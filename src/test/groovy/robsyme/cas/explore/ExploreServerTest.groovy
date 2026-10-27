package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §15: what explore serves, and everything it refuses. */
class ExploreServerTest extends Specification {

    static final String CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
    static final String ENTRY = "8232774159999-run-${CID}"

    @TempDir
    Path tempDir

    ExploreServer server
    byte[] snapshot = (0..<10240).collect { (byte) (it & 0xff) } as byte[]
    byte[] page = '<!doctype html><title>explorer</title>'.getBytes('UTF-8')

    def setup() {
        final Path lab = tempDir.resolve('lab')
        Files.createDirectories(lab.resolve('index'))
        Files.write(lab.resolve('index/v3.sqlite'), snapshot)
        Files.createDirectories(lab.resolve("blocks/${CID[-2..-1]}"))
        Files.write(lab.resolve("blocks/${CID[-2..-1]}/${CID}"), [0xa0] as byte[])
        Files.createDirectories(lab.resolve('log'))
        Files.createFile(lab.resolve("log/${ENTRY}"))
        Files.createFile(lab.resolve('log/.DS_Store'))
        Files.createDirectories(lab.resolve('coords/aligned'))
        Files.write(lab.resolve('coords/aligned/A.bam'), 'cas://x/A.bam'.bytes)
        Files.createDirectories(lab.resolve('nf/abc'))
        Files.write(lab.resolve('nf/abc/.data.json'), '{"path": "/Users/someone/work"}'.bytes)
        final Path other = tempDir.resolve('other')
        Files.createDirectories(other)
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(lab))
        members.put('other', new LocalMemberFiles(other))
        server = new ExploreServer(members, 'lab', page).start(0)
    }

    def cleanup() {
        server?.stop()
    }

    private RawHttp.Response get(String path, Map<String, String> headers = [:]) {
        RawHttp.send(server.port, 'GET', path, headers)
    }

    def 'it listens on loopback only and prints its own URL'() {
        expect:
        server.url == "http://127.0.0.1:${server.port}/"
    }

    def 'the page is served at / and /index.html'() {
        expect:
        ['/', '/index.html'].every { String p ->
            final def r = get(p)
            r.status == 200 && r.headers['content-type'].startsWith('text/html') && r.body == page
        }
    }

    def 'members.json lists every member, writable first'() {
        when:
        final def r = get('/members.json')

        then:
        r.status == 200
        new JsonSlurper().parse(r.body) == [members: [
            [alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null, copy_target: true, copy_reason: null],
            [alias: 'other', writable: false, base: 'm/other/', kind: 'local', location: null, copy_target: true, copy_reason: null],
        ], write: false, copy: false]
    }

    def 'members.json carries kind, an S3 location, and the copy judgement given to the server'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        members.put('org', new LocalMemberFiles(tempDir.resolve('other')))
        final ExploreServer judged = new ExploreServer(members, 'lab', page)
            .memberInfo([lab: new ExploreServer.MemberInfo('local', null, true, null),
                         org: new ExploreServer.MemberInfo('s3', 's3://lab-blocks/cas', false, 'readOnly is set for this store')])
            .start(0)

        when:
        final def r = RawHttp.send(judged.port, 'GET', '/members.json')

        then:
        new JsonSlurper().parse(r.body).members == [
            [alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null, copy_target: true, copy_reason: null],
            [alias: 'org', writable: false, base: 'm/org/', kind: 's3', location: 's3://lab-blocks/cas', copy_target: false, copy_reason: 'readOnly is set for this store'],
        ]
        !r.text().contains(tempDir.toString())

        cleanup:
        judged?.stop()
    }

    def 'members.json carries the identity when the server has a principal'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir))
        final ExploreServer signed = new ExploreServer(members, 'lab', page, null, null, null,
            { -> new robsyme.cas.Principal('ada@example.org', 'git') } as Supplier<robsyme.cas.Principal>).start(0)

        when:
        final def r = RawHttp.send(signed.port, 'GET', '/members.json', [:])

        then:
        r.status == 200
        new JsonSlurper().parse(r.body).identity == [principal: 'ada@example.org', source: 'git']

        cleanup:
        signed?.stop()
    }

    def 'the server answers / while the identity is still resolving, and members.json once it has'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir))
        CountDownLatch resolved = new CountDownLatch(1)
        ExploreServer signed = new ExploreServer(members, 'lab', page, null, null, null, { ->
            resolved.await()
            new robsyme.cas.Principal('ada@example.org', 'platform')
        } as Supplier<robsyme.cas.Principal>).start(0)
        def pool = Executors.newSingleThreadExecutor()

        when: 'the page asks for members.json while the identity is blocked'
        Future<RawHttp.Response> pending = pool.submit({ -> RawHttp.send(signed.port, 'GET', '/members.json', [:]) } as Callable<RawHttp.Response>)
        final def root = RawHttp.send(signed.port, 'GET', '/', [:])

        then: 'the page itself is served, and members.json waits'
        root.status == 200
        !pending.isDone()

        when:
        resolved.countDown()
        final RawHttp.Response r = pending.get(5, TimeUnit.SECONDS)

        then:
        r.status == 200
        new JsonSlurper().parse(r.body).identity == [principal: 'ada@example.org', source: 'platform']

        cleanup:
        resolved.countDown()
        pool.shutdownNow()
        signed?.stop()
    }

    def 'members.json has no identity key when the server was built without a principal'() {
        expect:
        !new JsonSlurper().parse(get('/members.json').body).containsKey('identity')
    }

    def 'the snapshot is served whole, and by single ranges'() {
        expect:
        get('/m/lab/index/v3.sqlite').with { status == 200 && body == snapshot && headers['accept-ranges'] == 'bytes' }
        get('/m/lab/index/v3.sqlite', [Range: 'bytes=4096-8191']).with {
            status == 206 && headers['content-range'] == 'bytes 4096-8191/10240' &&
                body == Arrays.copyOfRange(snapshot, 4096, 8192)
        }
        get('/m/lab/index/v3.sqlite', [Range: 'bytes=-10']).with { status == 206 && body.length == 10 }
        get('/m/lab/index/v3.sqlite', [Range: 'bytes=20000-']).with { status == 416 && headers['content-range'] == 'bytes */10240' }
        RawHttp.send(server.port, 'HEAD', '/m/lab/index/v3.sqlite').with { status == 200 && body.length == 0 }
    }

    def 'the snapshot and blocks carry an ETag, and a replaced snapshot gets a new one (final review finding 1)'() {
        given:
        final Path file = tempDir.resolve('lab/index/v3.sqlite')
        final String before = get('/m/lab/index/v3.sqlite', [Range: 'bytes=0-4095']).headers['etag']
        final String whole = get('/m/lab/index/v3.sqlite').headers['etag']
        final String head = RawHttp.send(server.port, 'HEAD', '/m/lab/index/v3.sqlite').headers['etag']

        when: 'replaced as IndexSnapshot writes it: same size, new bytes, an atomic move'
        final Path temp = file.resolveSibling('v3.sqlite.tmp')
        Files.write(temp, snapshot.collect { byte b -> (byte) (b ^ 1) } as byte[])
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        final RawHttp.Response after = get('/m/lab/index/v3.sqlite', [Range: 'bytes=0-4095'])

        then:
        before ==~ /^"[0-9a-z-]+"$/
        whole == before
        head == before
        after.status == 206
        after.headers['etag'] ==~ /^"[0-9a-z-]+"$/
        after.headers['etag'] != before
        get("/m/lab/blocks/${CID[-2..-1]}/${CID}").headers['etag'] ==~ /^"[0-9a-z-]+"$/
    }

    def 'an opened member file keeps the size, tag and bytes of the file it opened'() {
        given:
        final Path file = tempDir.resolve('lab/index/v3.sqlite')
        final MemberFiles files = new LocalMemberFiles(tempDir.resolve('lab'))
        final MemberFiles.Opened opened = files.open('index/v3.sqlite')

        when: 'the file is replaced by a bigger one after it was opened'
        final Path temp = file.resolveSibling('v3.sqlite.tmp')
        Files.write(temp, new byte[20000])
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        final MemberFiles.Opened reopened = files.open('index/v3.sqlite')

        then:
        opened.size == 10240L
        opened.read(4096, 100).withCloseable { it.readNBytes(100) } == Arrays.copyOfRange(snapshot, 4096, 4196)
        reopened.size == 20000L
        reopened.tag != opened.tag
        files.open('index/v2.sqlite') == null
        files.open('index') == null

        cleanup:
        opened?.close()
        reopened?.close()
    }

    def 'a block is served when its shard matches its cid'() {
        expect:
        get("/m/lab/blocks/${CID[-2..-1]}/${CID}").with { status == 200 && body == ([0xa0] as byte[]) }
        get("/m/lab/blocks/aa/${CID}").status == 404
    }

    def 'the log listing is JSON of Store Log names only'() {
        when:
        final def r = get('/m/lab/log/')

        then:
        r.status == 200
        r.headers['content-type'].startsWith('application/json')
        new JsonSlurper().parse(r.body) == [entries: [ENTRY]]
        new JsonSlurper().parse(get('/m/other/log/').body) == [entries: []]
    }

    def 'nothing else under a member is ever served (Review Focus 4): #path'() {
        expect:
        get(path).status == 404

        where:
        path << ['/m/lab/coords/aligned/A.bam', '/m/lab/nf/abc/.data.json', '/m/lab/../../etc/passwd',
                 '/m/lab/%2e%2e/%2e%2e/etc/passwd', '/m/lab/index/../nf/abc/.data.json', '/m/ghost/index/v3.sqlite',
                 '/m/lab/index/v3.sqlite-wal', '/m/lab/log/.DS_Store', '/m/lab/blocks/', '/etc/passwd']
    }

    def 'a foreign Host or Origin is refused (Review Focus 4): #headers'() {
        expect:
        get('/m/lab/index/v3.sqlite', headers).status == 403

        where:
        headers << [[Host: 'attacker.example:80'], [Host: 'evil.test'], [Host: '127.0.0.1:1'],
                    [Origin: 'http://attacker.example'], [Origin: 'null']]
    }

    def 'its own origin is accepted under either loopback name'() {
        expect:
        get('/members.json', [Host: "localhost:${server.port}".toString(), Origin: "http://localhost:${server.port}".toString()]).status == 200
        get('/members.json', [Origin: "http://127.0.0.1:${server.port}".toString()]).status == 200
    }

    def 'it is read-only'() {
        expect:
        RawHttp.send(server.port, 'POST', '/m/lab/index/v3.sqlite').with { status == 405 && headers['allow'] == 'GET, HEAD' }
    }

    def 'every response carries X-Content-Type-Options: nosniff (final review finding 7)'() {
        expect:
        get('/').headers['x-content-type-options'] == 'nosniff'
        get('/members.json').headers['x-content-type-options'] == 'nosniff'
        get('/m/lab/log/').headers['x-content-type-options'] == 'nosniff'
        get("/m/lab/blocks/${CID[-2..-1]}/${CID}").headers['x-content-type-options'] == 'nosniff'
        get('/m/lab/index/v3.sqlite').headers['x-content-type-options'] == 'nosniff'
    }

    def 'no CORS headers are sent'() {
        expect:
        !get('/m/lab/index/v3.sqlite', [Origin: "http://127.0.0.1:${server.port}".toString()]).headers.keySet().any { it.startsWith('access-control') }
    }

    def 'members.json greys out an S3 copy target that cannot be reached, and leaves the rest as judged'() {
        given:
        final MemberProbeTest.Fake cloud = new MemberProbeTest.Fake(answer: { ->
            throw software.amazon.awssdk.services.s3.model.S3Exception.builder().statusCode(403).message('Forbidden').build() })
        final MemberProbeTest.Fake bundle = new MemberProbeTest.Fake()
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        members.put('cloud', cloud)
        members.put('bundle', bundle)
        // Copying needs a copier and the token, as from nf-blocks:explore.
        final ExploreServer probed = new ExploreServer(members, 'lab', page, null, ExploreServer.newToken())
            .memberInfo([lab   : new ExploreServer.MemberInfo('local', null, true, null),
                         cloud : new ExploreServer.MemberInfo('s3', 's3://bucket/cas', true, null),
                         bundle: new ExploreServer.MemberInfo('s3', 's3://bundle/cas', false, 'readOnly is set for this store')])
            .memberProbe(new MemberProbe(members))
            .copies({ Map request, boolean dryRun -> null } as ExploreServer.Copier, null)
            .start(0)

        when:
        final List<Map> listed = (List<Map>) new JsonSlurper().parse(RawHttp.send(probed.port, 'GET', '/members.json').body).members

        then: 'the unreachable target is no target now, and says what to do'
        listed.find { it.alias == 'cloud' }.copy_target == false
        listed.find { it.alias == 'cloud' }.copy_reason == "${robsyme.cas.s3.S3Failures.REFUSED}. Refresh your AWS credentials, then reload this page".toString()

        and: 'a store that was never a target keeps its own reason and is not asked'
        listed.find { it.alias == 'bundle' } .copy_reason == 'readOnly is set for this store'
        bundle.asked.get() == 0

        and: 'the local writable member is not asked either'
        listed.find { it.alias == 'lab' }.copy_target == true

        cleanup:
        probed?.stop()
    }

    def 'members.json asks nothing when this server cannot copy'() {
        given:
        final MemberProbeTest.Fake cloud = new MemberProbeTest.Fake()
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        members.put('cloud', cloud)
        final ExploreServer quiet = new ExploreServer(members, 'lab', page, null, ExploreServer.newToken())
            .memberInfo([lab  : new ExploreServer.MemberInfo('local', null, true, null),
                         cloud: new ExploreServer.MemberInfo('s3', 's3://bucket/cas', true, null)])
            .memberProbe(new MemberProbe(members))
            .start(0)

        when:
        RawHttp.send(quiet.port, 'GET', '/members.json')

        then:
        cloud.asked.get() == 0

        cleanup:
        quiet?.stop()
    }
}
