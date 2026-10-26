package robsyme.cas.explore

import java.nio.file.Path

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.HandoffWriter
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** `POST /api/handoff` (explorer v0.3 P4 plan 1.5): /api/put's transport checks, then the Handoff writer, plain JSON both ways. */
class ExploreHandoffTest extends Specification {

    static final String SIGNER = 'p4-test@example.org'

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    ExploreServer server
    String token = ExploreServer.newToken()
    Cid selection

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A']]]))
        selection = store.putDagCbor(new Selection(SIGNER, [Selection.item(item, [collection])], []).toCbor())
        server = serve(store)
    }

    def cleanup() {
        server?.stop()
        index?.close()
    }

    private ExploreServer serve(LocalBlockStore writable, boolean handoffs = true) {
        final Put put = new Put(store, writable, index, { -> SIGNER }, { -> System.currentTimeMillis() }, { -> index.catchUp(store, StoreLog.of(store), 'lab') })
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        final ExploreServer s = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, put, token)
        if( handoffs )
            s.handoffs(new HandoffWriter(store, writable, index, { -> SIGNER }, { -> System.currentTimeMillis() },
                { -> index.catchUp(store, StoreLog.of(store), 'lab') }, put))
        return s.start(0)
    }

    private byte[] request(Map overrides = [:]) {
        final Map r = [template : 'nf-core/differentialabundance', selection: selection.toString(), project: null,
                       files    : [[name: 'samplesheet.csv', text: 'sample,condition\nA,treated\n']],
                       params   : [input: [file: 'samplesheet.csv'], gtf: '/data/genes.gtf'],
                       timestamp: Index.isoMillis(System.currentTimeMillis())]
        r.putAll(overrides)
        return JsonOutput.toJson(r).getBytes('UTF-8')
    }

    private RawHttp.Response post(String path, Map<String, String> headers, byte[] body = request()) {
        return RawHttp.send(server.port, 'POST', path, headers, body)
    }

    private Map<String, String> good(Map<String, String> extra = [:]) {
        return ['Content-Type': 'application/json', 'X-NF-Blocks-Token': token,
                Origin: "http://127.0.0.1:${server.port}".toString()] + extra
    }

    private static Map parsed(RawHttp.Response r) {
        return (Map) new JsonSlurper().parse(r.body)
    }

    def 'a POST with the token writes the hand-off and answers plain JSON'() {
        when:
        final def r = post('/api/handoff', good('Content-Type': 'application/json; charset=utf-8'))
        final Map body = parsed(r)

        then:
        r.status == 200
        r.headers['content-type'] == 'application/json'
        body.written == true
        store.has(Cid.parse((String) body.address))
        body.params.gtf == '/data/genes.gtf'
        ((String) body.params.input).startsWith('cas://bafk')
        StoreLog.read(store)*.name == [body.entry]
        StoreLog.read(store)[0].kind == StoreLogKind.HANDOFF
    }

    def 'dry_run=true answers the same body with written false and entry null, and writes nothing'() {
        when:
        final def dry = post('/api/handoff?dry_run=true', good())
        final Map body = parsed(dry)

        then:
        dry.status == 200
        body.written == false
        body.containsKey('entry') && body.entry == null
        !store.has(Cid.parse((String) body.address))
        !store.has(Cid.parse((String) ((List<Map>) body.files)[0].address))
        StoreLog.read(store).isEmpty()
    }

    def 'refused before the writer runs: #why'() {
        when:
        final def r = post('/api/handoff', headers(server.port, token))

        then:
        r.status == status
        r.headers['content-type'] == 'text/plain; charset=utf-8'
        StoreLog.read(store).isEmpty()

        where:
        why                    | status | headers
        'no token'             | 403    | { int p, String t -> ['Content-Type': 'application/json'] }
        'a wrong token'        | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': 'a' * 26] }
        'a foreign Origin'     | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Origin: 'http://evil.example'] }
        'a foreign Host'       | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Host: 'evil.example'] }
        'DAG-JSON'             | 415    | { int p, String t -> ['Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': t] }
        'text/plain'           | 415    | { int p, String t -> ['Content-Type': 'text/plain', 'X-NF-Blocks-Token': t] }
    }

    def 'a body over 2 MiB is 413'() {
        expect:
        post('/api/handoff', good(), new byte[(int) Put.MAX_REQUEST_BYTES + 1]).status == 413
    }

    def 'a writer refusal is plain JSON {error, message} with its status'() {
        when:
        final def broken = post('/api/handoff', good(), '{"template":'.getBytes('UTF-8'))
        final def unknown = post('/api/handoff', good(), request(selection: Fixtures.cidOf([kind: 'Selection', n: 1]).toString()))

        then:
        broken.status == 400
        broken.headers['content-type'] == 'application/json'
        parsed(broken).error == 'bad_request'
        parsed(broken).keySet() == ['error', 'message'] as Set
        unknown.status == 400
        parsed(unknown).error == 'not_found'
        StoreLog.read(store).isEmpty()
    }

    def 'a read-only writable member is 409 not_writable'() {
        given:
        server.stop()
        server = serve(new LocalBlockStore(tempDir.resolve('lab'), 'lab', false))

        when:
        final def r = post('/api/handoff', good())

        then:
        r.status == 409
        parsed(r).error == 'not_writable'
    }

    def 'a server without a Handoff writer answers 405, and GET /api/handoff is 404'() {
        given:
        server.stop()
        server = serve(store, false)

        expect:
        post('/api/handoff', good()).status == 405
        RawHttp.send(server.port, 'GET', '/api/handoff').status == 404
    }
}
