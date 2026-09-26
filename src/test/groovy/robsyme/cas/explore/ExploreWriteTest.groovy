package robsyme.cas.explore

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import groovy.json.JsonSlurper
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.Samplesheet
import robsyme.cas.core.StoreLog
import spock.lang.Specification
import spock.lang.TempDir

/**
 * A {@link LocalBlockStore} (so {@code StoreLog.of} still recognises it) whose
 * {@code put} pauses until released, so a test can hold a write in flight.
 */
class SlowWritable extends LocalBlockStore {
    private final CountDownLatch started
    private final CountDownLatch proceed

    SlowWritable(Path root, String alias, CountDownLatch started, CountDownLatch proceed) {
        super(root, alias, true)
        this.started = started; this.proceed = proceed
    }

    @Override
    void put(Cid cid, InputStream input, long expectedSize) {
        started.countDown()
        proceed.await(10, TimeUnit.SECONDS)
        super.put(cid, input, expectedSize)
    }
}

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

    def 'a lone surrogate or an out-of-range integer in the request is 400 invalid, never 500 (final review finding 1)'() {
        given:
        final byte[] loneSurrogate = ('{"kind":"Claim","subject":{"/":"' + item + '"},"verb":"set","attribute":"x","value":"\\ud800",' +
            '"supersedes":[],"timestamp":"2026-09-25T10:00:00.000Z"}').getBytes('UTF-8')
        final byte[] hugeInteger = ('{"kind":"Claim","subject":{"/":"' + item + '"},"verb":"set","attribute":"x","value":184467440737095516160000,' +
            '"supersedes":[],"timestamp":"2026-09-25T10:00:00.000Z"}').getBytes('UTF-8')

        expect:
        post('/api/put', good(), loneSurrogate).with {
            status == 400 && ((Map) DagJson.decode(body)).error == 'invalid'
        }
        post('/api/put', good(), hugeInteger).with {
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

    def 'stop() lets a write already in flight finish rather than interrupting it (final review finding 5)'() {
        given:
        final CountDownLatch started = new CountDownLatch(1)
        final CountDownLatch proceed = new CountDownLatch(1)
        final BlockStore slowWritable = new SlowWritable(tempDir.resolve('lab'), 'lab', started, proceed)
        final Put slowPut = new Put(store, slowWritable, index, 'ada', { -> System.currentTimeMillis() },
            { -> index.catchUp(store, StoreLog.of(store), 'lab') })
        final LinkedHashMap<String, MemberFiles> slowMembers = new LinkedHashMap<>()
        slowMembers.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        final ExploreServer slow = new ExploreServer(slowMembers, 'lab', '<!doctype html>'.bytes, slowPut, token).start(0)
        final Map<String, String> slowHeaders = ['Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': token,
            Origin: "http://127.0.0.1:${slow.port}".toString()]
        RawHttp.Response response = null
        Throwable failure = null

        when:
        final Thread writer = Thread.start {
            try {
                response = RawHttp.send(slow.port, 'POST', '/api/put', slowHeaders, request())
            }
            catch( Throwable t ) {
                failure = t
            }
        }
        started.await(5, TimeUnit.SECONDS)
        // stop() begins (and, fixed, waits for the exchange) before the latch
        // that lets the write itself proceed is released, so a fix that
        // interrupts the write on the way in would abort it here.
        final Thread stopper = Thread.start { slow.stop() }
        Thread.sleep(200)
        proceed.countDown()
        writer.join(10_000)
        stopper.join(10_000)

        then:
        failure == null
        response != null
        response.status == 200
        final Map body = (Map) DagJson.decode(response.body)
        body.written == true
        store.has((Cid) body.address)
    }

    def 'GET /api/samplesheet/<selection>.csv and .json answer the export'() {
        given:
        final Map written = (Map) DagJson.decode(post('/api/put', good()).body)
        final Cid selection = (Cid) written.address
        server.stop()
        final ExploreServer.Exporter exporter = { Cid s, String format ->
            index.catchUp(store, StoreLog.of(store), 'lab')
            final Samplesheet sheet = Samplesheet.of(store, index.selectionItems(s))
            return (format == 'csv' ? sheet.csv() : sheet.json()).getBytes('UTF-8')
        } as ExploreServer.Exporter
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        server = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, null, null, exporter).start(0)

        when:
        final def csv = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.csv")
        final def json = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.json")
        final def absent = RawHttp.send(server.port, 'GET', "/api/samplesheet/${Fixtures.cidOf([kind: 'Selection', n: 99])}.csv")

        then:
        csv.status == 200
        csv.headers['content-type'] == 'text/csv; charset=utf-8'
        csv.headers['content-disposition'] == "attachment; filename=\"selection-${selection.toString().take(16)}.csv\""
        csv.text().readLines()[0] == 'sample'
        json.status == 200
        new JsonSlurper().parseText(json.text()) == [[sample: 'A']]
        absent.status == 404
    }

    def 'GET /api/samplesheet carries non-ASCII text as UTF-8 (Review Focus 3)'() {
        given:
        final String text = 'café – naïve ✓ 𝄞'
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid textItem = store.putDagCbor(Fixtures.outputItem([[sample: 'A', note: text]]))
        final Cid textCollection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[textItem, ['aligned/A']]]))
        final byte[] selectionRequest = ('{"kind":"Selection","members":["cas://' + textCollection + '/' + textItem + '"],"derived_from":[]}').getBytes('UTF-8')
        final Map written = (Map) DagJson.decode(post('/api/put', good(), selectionRequest).body)
        final Cid selection = (Cid) written.address
        server.stop()
        final ExploreServer.Exporter exporter = { Cid s, String format ->
            index.catchUp(store, StoreLog.of(store), 'lab')
            final Samplesheet sheet = Samplesheet.of(store, index.selectionItems(s))
            return (format == 'csv' ? sheet.csv() : sheet.json()).getBytes('UTF-8')
        } as ExploreServer.Exporter
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tempDir.resolve('lab')))
        server = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, null, null, exporter).start(0)

        when:
        final def csv = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.csv")
        final def json = RawHttp.send(server.port, 'GET', "/api/samplesheet/${selection}.json")

        then:
        csv.status == 200
        csv.headers['content-type'] == 'text/csv; charset=utf-8'
        csv.text().contains(text)
        json.status == 200
        ((List<Map>) new JsonSlurper().parseText(json.text())).find { it.sample == 'A' }.note == text
    }
}
