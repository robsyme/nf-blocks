package robsyme.cas.explore

import java.nio.file.Path

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.StoreLog
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 9.5: the write endpoint's safety, and that it is the builder. */
class ExploreWriteTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    ExploreServer server
    String token = ExploreServer.newToken()
    Cid item, collection

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        item = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A']]]))
        final Put put = new Put(store, store, index, 'ada', { -> System.currentTimeMillis() }, { -> index.catchUp(store, StoreLog.of(store), 'lab') })
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        server = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, put, token).start(0)
    }

    def cleanup() {
        server?.stop()
        index?.close()
    }

    private byte[] request() {
        return ('{"kind":"Selection","members":["cas://' + collection + '/' + item + '"],"derived_from":[]}').getBytes('UTF-8')
    }

    private RawHttp.Response post(String path, Map<String, String> headers, byte[] body = request()) {
        return RawHttp.send(server.port, 'POST', path, headers, body)
    }

    private Map<String, String> good(Map<String, String> extra = [:]) {
        return ['Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': token,
                Origin: "http://127.0.0.1:${server.port}".toString()] + extra
    }

    def 'a POST with the token writes through the builder and answers DAG-JSON'() {
        when:
        final def r = post('/api/put', good())
        final Map body = (Map) DagJson.decode(r.body)

        then:
        r.status == 200
        r.headers['content-type'] == 'application/vnd.ipld.dag-json'
        body.written == true
        store.has((Cid) body.address)
    }

    def 'application/json with a charset is accepted, and dry_run=true writes nothing'() {
        when:
        final def r = post('/api/put?dry_run=true', good('Content-Type': 'application/json; charset=utf-8'))

        then:
        r.status == 200
        ((Map) DagJson.decode(r.body)).exists == false
        StoreLog.read(store).isEmpty()
    }

    def 'refused before the builder runs: #why'() {
        when:
        final def r = post('/api/put', headers(server.port, token))

        then:
        r.status == status
        StoreLog.read(store).isEmpty()

        where:
        why                    | status | headers
        'no token'             | 403    | { int p, String t -> ['Content-Type': 'application/json'] }
        'a wrong token'        | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': 'a' * 26] }
        'a foreign Origin'     | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Origin: 'http://evil.example'] }
        'text/plain'           | 415    | { int p, String t -> ['Content-Type': 'text/plain', 'X-NF-Blocks-Token': t] }
        'no content type'      | 415    | { int p, String t -> ['X-NF-Blocks-Token': t] }
    }

    def 'a body over 2 MiB is 413, and a broken body is 400 invalid, never 500 (Review Focus 4)'() {
        expect:
        post('/api/put', good(), new byte[2 * 1024 * 1024 + 1]).status == 413
        post('/api/put', good(), ('[' * 10_000).getBytes('UTF-8')).with {
            status == 400 && ((Map) DagJson.decode(body)).error == 'invalid'
        }
        post('/api/put', good(), [0x7b, 0xff] as byte[]).with {
            status == 400 && ((Map) DagJson.decode(body)).error == 'invalid'
        }
    }

    def 'a builder refusal keeps its status and body'() {
        when:
        final def r = post('/api/put', good(), '{"kind":"Selection","members":[],"derived_from":[]}'.getBytes('UTF-8'))

        then:
        r.status == 400
        ((Map) DagJson.decode(r.body)).error == 'empty'
    }

    def 'POST anywhere else is 405, and GET /api/put is 404'() {
        expect:
        post('/members.json', good()).status == 405
        RawHttp.send(server.port, 'GET', '/api/put').status == 404
    }

    def 'members.json says whether this server writes; the launch URL carries the token'() {
        expect:
        new JsonSlurper().parse(RawHttp.send(server.port, 'GET', '/members.json').body) ==
            [members: [[alias: 'lab', writable: true, base: 'm/lab/']], write: true]
        server.launchUrl == "http://127.0.0.1:${server.port}/?token=${token}"
        token ==~ /[a-z2-7]{26}/
    }
}
