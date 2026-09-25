package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path

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
        Files.write(lab.resolve('index/v2.sqlite'), snapshot)
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
            [alias: 'lab', writable: true, base: 'm/lab/'],
            [alias: 'other', writable: false, base: 'm/other/'],
        ]]
    }

    def 'the snapshot is served whole, and by single ranges'() {
        expect:
        get('/m/lab/index/v2.sqlite').with { status == 200 && body == snapshot && headers['accept-ranges'] == 'bytes' }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=4096-8191']).with {
            status == 206 && headers['content-range'] == 'bytes 4096-8191/10240' &&
                body == Arrays.copyOfRange(snapshot, 4096, 8192)
        }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=-10']).with { status == 206 && body.length == 10 }
        get('/m/lab/index/v2.sqlite', [Range: 'bytes=20000-']).with { status == 416 && headers['content-range'] == 'bytes */10240' }
        RawHttp.send(server.port, 'HEAD', '/m/lab/index/v2.sqlite').with { status == 200 && body.length == 0 }
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
                 '/m/lab/%2e%2e/%2e%2e/etc/passwd', '/m/lab/index/../nf/abc/.data.json', '/m/ghost/index/v2.sqlite',
                 '/m/lab/index/v2.sqlite-wal', '/m/lab/log/.DS_Store', '/m/lab/blocks/', '/etc/passwd']
    }

    def 'a foreign Host or Origin is refused (Review Focus 4): #headers'() {
        expect:
        get('/m/lab/index/v2.sqlite', headers).status == 403

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
        RawHttp.send(server.port, 'POST', '/m/lab/index/v2.sqlite').with { status == 405 && headers['allow'] == 'GET, HEAD' }
    }

    def 'no CORS headers are sent'() {
        expect:
        !get('/m/lab/index/v2.sqlite', [Origin: "http://127.0.0.1:${server.port}".toString()]).headers.keySet().any { it.startsWith('access-control') }
    }
}
