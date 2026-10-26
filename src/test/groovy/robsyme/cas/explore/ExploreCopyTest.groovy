package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import robsyme.cas.CopyService
import robsyme.cas.Principal
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogEntry
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.Transfer
import spock.lang.Specification
import spock.lang.TempDir

/**
 * `POST /api/copy` (P3 plan 1.7) over two real local members: the transport
 * checks it shares with /api/put, CopyService's statuses and bodies as JSON,
 * and one copy at a time.
 *
 * `lab` holds one run (counts: c1 {s1, counts1}; c2 {s2, counts2}) and a
 * Selection `sel` = {c1 via counts}, named. `org` is an empty local member.
 */
class ExploreCopyTest extends Specification {

    @TempDir
    Path tmp

    LocalBlockStore lab
    long now = 1_760_000_000_000L
    Cid counts1, c1, c2, manifest, countsC, completion, sel
    Index index
    ExploreServer server
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    String token = ExploreServer.newToken()

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        counts1 = raw('counts-s1')
        c1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], counts: Fixtures.leaf2('c.tsv', counts1, 9)]))
        c2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], counts: Fixtures.leaf2('c2.tsv', raw('counts-s2'), 9)]))
        manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'one', pipeline: 'p1', nf_run_hash: 'unknown'))
        countsC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'counts', [[c1, ['c.tsv']], [c2, ['c2.tsv']]]))
        completion = logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [countsC]))
        sel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c1, [countsC])], []).toCbor())
        logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop', []))
        index = Index.open(tmp.resolve('explore-cache/index.sqlite'))
        server = serve({ ExploreServer s -> s.copies(config(), new PrintStream(err, true)) })
    }

    def cleanup() {
        server?.stop()
        index?.close()
    }

    private Cid raw(String text) {
        return lab.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8')))
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = lab.putDagCbor(block)
        StoreLog.append(lab, kind, cid, now += 1000)
        return cid
    }

    private Map config() {
        return [lineage: [store: [location: 'cas://lab']],
                cas    : [stores : [lab: [location: tmp.resolve('lab').toString()], org: [location: tmp.resolve('org').toString()]],
                          resolve: ['lab'], asserted_by: 'copier',
                          index  : [path: tmp.resolve('cache/index.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
    }

    /** A server over lab and org, with a Put on lab, configured by {@code copies}, signing as {@code principal}'s answer. */
    private ExploreServer serve(Closure copies, Supplier<Principal> principal = null) {
        final Put put = new Put(lab, lab, index, { -> 'ada' }, { -> System.currentTimeMillis() }, { -> index.catchUp(lab, StoreLog.of(lab), 'lab') })
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tmp.resolve('lab')))
        members.put('org', new LocalMemberFiles(tmp.resolve('org')))
        final ExploreServer s = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, put, token, null, principal)
        copies.call(s)
        return s.start(0)
    }

    private Map<String, String> good(ExploreServer s = server, Map<String, String> extra = [:]) {
        return ['Content-Type': 'application/json', 'X-NF-Blocks-Token': token, Origin: "http://127.0.0.1:${s.port}".toString()] + extra
    }

    private static byte[] ask(Map request) {
        return JsonOutput.toJson(request).getBytes('UTF-8')
    }

    private byte[] selectionCopy(Map extra = [:]) {
        return ask([root: "selection:${sel}".toString(), from: null, to: 'org', depth: 'results', claims: ['names'], accept_audit: false] + extra)
    }

    private RawHttp.Response post(String path, Map<String, String> headers = good(), byte[] body = selectionCopy(), ExploreServer s = server) {
        return RawHttp.send(s.port, 'POST', path, headers, body)
    }

    private static Map json(RawHttp.Response r) {
        return (Map) new JsonSlurper().parse(r.body)
    }

    /** A plan writes nothing to the destination, not even its directory. */
    private boolean orgUntouched() {
        return !Files.exists(tmp.resolve('org'))
    }

    def 'a dry run answers the 1.3 plan as JSON and writes nothing to the destination'() {
        when:
        final def r = post('/api/copy?dry_run=true')
        final Map body = json(r)

        then:
        r.status == 200
        r.headers['content-type'] == 'application/json'
        r.headers['x-content-type-options'] == 'nosniff'
        body.root == [kind: 'selection', cid: sel.toString()]
        body.from == 'lab' && body.to == 'org'
        body.depth == 'results'
        body.claims == ['names']
        body.dry_run == true
        ((Map) body.plan).transfer == ((Map) body.plan).blocks
        ((Map) body.plan).audit == [checked: 1, findings: []]
        ((Map) body.depths).keySet() == ['results', 'lineage', 'all'] as Set
        body.command == "nextflow plugin nf-blocks:copy selection:${sel} --from lab --to org --depth results --claims names --dry-run".toString()
        !body.containsKey('copied')
        orgUntouched()
    }

    def 'a real copy answers with what it copied and its Transfer, and the destination log the server serves has the entry'() {
        when:
        final def r = post('/api/copy')
        final Map body = json(r)
        final LocalBlockStore org = new LocalBlockStore(tmp.resolve('org'), 'org', false)
        final List<StoreLogEntry> log = StoreLog.read(org)
        final List<String> served = (List<String>) json(RawHttp.send(server.port, 'GET', '/m/org/log/')).entries

        then:
        r.status == 200
        r.headers['content-type'] == 'application/json'
        body.dry_run == false
        body.copied == ((Map) body.plan).transfer
        body.copied > 0
        body.failures == []
        body.snapshot == 'written'
        Cid.isCid((String) body.transfer_entry)

        and: 'the Transfer entry is the newest in org, and the explorer lists it'
        log[0].kind == StoreLogKind.TRANSFER
        log[0].cid.toString() == body.transfer_entry
        served.any { it.endsWith("-transfer-${body.transfer_entry}") }
        [sel, c1, counts1, countsC, completion, manifest].every { org.has(it) }

        when: 'the same copy again changes nothing'
        final def again = post('/api/copy')
        final Map second = json(again)

        then: 'and logs no second Transfer'
        again.status == 200
        second.copied == 0
        second.transfer_entry == null
        StoreLog.read(org).count { it.kind == StoreLogKind.TRANSFER } == 1
    }

    def "a copy through the endpoint signs its Transfer with the server's principal, resolving nothing itself"() {
        given: 'the config would sign as copier; the server signs as the identity members.json shows'
        AtomicInteger asked = new AtomicInteger()
        Principal mine = new Principal('ada@example.org', 'platform')
        ExploreServer signed = serve({ ExploreServer s -> s.copies(config(), new PrintStream(err, true)) },
            { -> asked.incrementAndGet(); mine } as Supplier<Principal>)

        when:
        final def dry = post('/api/copy?dry_run=true', good(signed), selectionCopy(), signed)
        final int afterDryRun = asked.get()
        final def r = post('/api/copy', good(signed), selectionCopy(), signed)
        final Map body = json(r)
        final LocalBlockStore org = new LocalBlockStore(tmp.resolve('org'), 'org', false)
        final Transfer t = Transfer.fromCbor((Map) robsyme.cas.core.DagCbor.decode(org.open(Cid.parse((String) body.transfer_entry)).bytes))

        then:
        dry.status == 200
        afterDryRun == 0
        r.status == 200
        t.assertedBy == 'ada@example.org'
        json(RawHttp.send(signed.port, 'GET', '/members.json')).identity == [principal: 'ada@example.org', source: 'platform']
        !err.toString().contains('signing as')

        cleanup:
        signed?.stop()
    }

    def 'dry runs share two places: a third waits for one to finish, then answers'() {
        given: 'a copier whose dry runs wait until released'
        CountDownLatch release = new CountDownLatch(1)
        AtomicInteger inside = new AtomicInteger()
        AtomicInteger most = new AtomicInteger()
        AtomicInteger entered = new AtomicInteger()
        ExploreServer held = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                most.accumulateAndGet(inside.incrementAndGet(), { int a, int b -> Math.max(a, b) })
                entered.incrementAndGet()
                release.await(10, TimeUnit.SECONDS)
                inside.decrementAndGet()
                return new CopyService.Outcome(200, [dry_run: dryRun, plan: [:]] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
        List<RawHttp.Response> answers = Collections.synchronizedList([])

        when:
        final List<Thread> threads = (1..3).collect { Thread.start { answers << post('/api/copy?dry_run=true', good(held), selectionCopy(), held) } }
        final long deadline = System.currentTimeMillis() + 5000
        while( entered.get() < 2 && System.currentTimeMillis() < deadline )
            Thread.sleep(20)
        // Long enough for a third to have entered, had nothing stopped it.
        Thread.sleep(500)
        final int enteredBeforeRelease = entered.get()
        release.countDown()
        threads.each { it.join(10_000) }

        then:
        enteredBeforeRelease == 2
        answers.size() == 3
        answers.every { it.status == 200 }
        entered.get() == 3
        most.get() == 2

        cleanup:
        release.countDown()
        held?.stop()
    }

    /** A server whose dry runs count themselves in and wait for {@code release}. */
    private ExploreServer holdingDryRuns(CountDownLatch release, AtomicInteger entered) {
        return serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                entered.incrementAndGet()
                release.await(10, TimeUnit.SECONDS)
                return new CopyService.Outcome(200, [dry_run: dryRun, plan: [:], root: request.root] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
    }

    private static void waitFor(Closure<Boolean> condition) {
        final long deadline = System.currentTimeMillis() + 5000
        while( !condition.call() && System.currentTimeMillis() < deadline )
            Thread.sleep(20)
    }

    def 'D10: at most one dry run waits; a newer one takes its place and the older answers 409 superseded'() {
        given:
        CountDownLatch release = new CountDownLatch(1)
        AtomicInteger entered = new AtomicInteger()
        ExploreServer held = holdingDryRuns(release, entered)
        Map<String, RawHttp.Response> answers = Collections.synchronizedMap([:])
        Closure ask = { String name -> Thread.start { answers[name] = post('/api/copy?dry_run=true', good(held), selectionCopy(root: name), held) } }

        when: 'two run, a third waits, then a fourth arrives'
        final List<Thread> threads = [ask('one'), ask('two')]
        waitFor { entered.get() == 2 }
        threads << ask('three')
        Thread.sleep(300)
        threads << ask('four')
        waitFor { answers.containsKey('three') }
        final boolean answeredBeforeRelease = answers.containsKey('three')
        release.countDown()
        threads*.join(10_000)

        then: 'the third answered at once, without running'
        answeredBeforeRelease
        answers.three.status == 409
        answers.three.headers['content-type'] == 'application/json'
        json(answers.three).error == 'superseded'
        json(answers.three).message

        and: 'the others ran'
        ['one', 'two', 'four'].every { answers[it].status == 200 && json(answers[it]).root == it }
        entered.get() == 3

        cleanup:
        release.countDown()
        held?.stop()
    }

    def 'D10: a dry run waiting when the server stops answers 503 at once'() {
        given:
        CountDownLatch release = new CountDownLatch(1)
        AtomicInteger entered = new AtomicInteger()
        ExploreServer held = holdingDryRuns(release, entered)
        held.graceSeconds = 5
        Map<String, RawHttp.Response> answers = Collections.synchronizedMap([:])
        Closure ask = { String name -> Thread.start { answers[name] = post('/api/copy?dry_run=true', good(held), selectionCopy(root: name), held) } }

        when:
        final List<Thread> threads = [ask('one'), ask('two')]
        waitFor { entered.get() == 2 }
        threads << ask('three')
        Thread.sleep(300)
        final Thread stopper = Thread.start { held.stop() }
        waitFor { answers.containsKey('three') }
        final boolean answeredWhileStopping = answers.containsKey('three')
        release.countDown()
        threads*.join(10_000)
        stopper.join(10_000)

        then:
        answeredWhileStopping
        answers.three.status == 503
        json(answers.three).error == 'stopping'
        entered.get() == 2

        cleanup:
        release.countDown()
    }

    def 'refused in plain text before CopyService runs: #why'() {
        when:
        final def r = post('/api/copy', headers.call(server.port, token) as Map<String, String>)

        then:
        r.status == status
        r.headers['content-type'] == 'text/plain; charset=utf-8'
        orgUntouched()

        where:
        why                    | status | headers
        'no token'             | 403    | { int p, String t -> ['Content-Type': 'application/json'] }
        'a wrong token'        | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': 'a' * 26] }
        'a foreign Origin'     | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Origin: 'http://evil.example'] }
        'a foreign Host'       | 403    | { int p, String t -> ['Content-Type': 'application/json', 'X-NF-Blocks-Token': t, Host: "evil.example:${p}".toString()] }
        'text/plain'           | 415    | { int p, String t -> ['Content-Type': 'text/plain', 'X-NF-Blocks-Token': t] }
        'DAG-JSON'             | 415    | { int p, String t -> ['Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': t] }
        'no content type'      | 415    | { int p, String t -> ['X-NF-Blocks-Token': t] }
    }

    def 'application/json with a charset is accepted, and a body over 2 MiB is 413'() {
        expect:
        post('/api/copy?dry_run=true', good(server, ['Content-Type': 'application/json; charset=utf-8'])).status == 200
        post('/api/copy', good(), new byte[(int) Put.MAX_REQUEST_BYTES + 1]).status == 413
        orgUntouched()
    }

    def 'a bad request is 400 with {error, message}: #why'() {
        when: 'a Map is merged into a good request; a String is sent as it is'
        final byte[] body = request instanceof Map ? selectionCopy((Map) request) : ((String) request).getBytes('UTF-8')
        final def r = post(path, good(), body)
        final Map answer = json(r)

        then:
        r.status == 400
        r.headers['content-type'] == 'application/json'
        answer.keySet() == ['error', 'message'] as Set
        answer.error == code
        orgUntouched()

        where:
        why                  | path                     | request                    | code
        'an unknown member'  | '/api/copy'              | [to: 'nowhere']            | 'unknown_member'
        'the same member'    | '/api/copy?dry_run=true' | [from: 'lab', to: 'lab']   | 'same_member'
        'a bad depth'        | '/api/copy?dry_run=true' | [depth: 'deep']            | 'bad_depth'
        'malformed JSON'     | '/api/copy'              | '{"root": "selection:'     | 'bad_request'
        'an empty body'      | '/api/copy'              | ''                         | 'bad_request'
        'a JSON array'       | '/api/copy'              | '["selection:x"]'          | 'bad_request'
        'deep nesting'       | '/api/copy'              | '[' * 100_000              | 'bad_request'
    }

    def 'audit findings on a real copy are 409 with the plan as the body, and nothing is written'() {
        given: 'a second run whose RunManifest keeps an absolute path in params'
        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', params: [outdir: '/home/someone/results']))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's3'], out: Fixtures.leaf2('x', counts1, 9)]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(leaky, 'out', [[item, ['x']]]))
        logged(StoreLogKind.RUN, Fixtures.runCompletion(leaky, [coll]))
        final Cid two = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [coll])], []).toCbor())

        when:
        final def r = post('/api/copy', good(), selectionCopy(root: "selection:${two}".toString()))
        final Map body = json(r)

        then:
        r.status == 409
        r.headers['content-type'] == 'application/json'
        body.dry_run == false
        body.root == [kind: 'selection', cid: two.toString()]
        ((List<Map>) ((Map) ((Map) body.plan).audit).findings).collect { [it.cid, it.kind, it.where] } ==
            [[leaky.toString(), 'RunManifest', 'params.outdir']]
        !body.containsKey('copied')
        orgUntouched()

        and: 'the body says why'
        body.refused == 'audit'

        when: 'accepted'
        final def accepted = post('/api/copy', good(), selectionCopy(root: "selection:${two}".toString(), accept_audit: true))

        then:
        accepted.status == 200
        json(accepted).transfer_entry != null
        json(accepted).refused == null
    }

    def 'D9: a copy of a Selection the destination deleted is 409 refused not_kept with the plan, and nothing is written'() {
        given:
        assert post('/api/copy').status == 200
        final LocalBlockStore org = new LocalBlockStore(tmp.resolve('org'), 'org', true)
        final Cid deletion = org.putDagCbor(Fixtures.claim(sel, 'delete', null, null, []))
        StoreLog.append(org, StoreLogKind.CLAIM, deletion, now += 1000)
        final List<StoreLogEntry> before = StoreLog.read(org)

        when:
        final def dry = post('/api/copy?dry_run=true')
        final def r = post('/api/copy')
        final Map body = json(r)

        then:
        dry.status == 200
        ((Map) json(dry).plan).kept == false
        r.status == 409
        r.headers['content-type'] == 'application/json'
        body.refused == 'not_kept'
        ((Map) body.plan).kept == false
        ((List<Map>) ((Map) body.plan).destination_state).find { it.cid == sel.toString() } ==
            [cid: sel.toString(), role: 'root', name: 'wigshop', state: 'deleted']
        StoreLog.read(org) == before
    }

    def 'one real copy at a time: a second is 409 copy_running, while a dry run and a write still go through'() {
        given: 'a copier that holds the copy lock until released'
        CountDownLatch inside = new CountDownLatch(1)
        CountDownLatch release = new CountDownLatch(1)
        List<Boolean> calls = Collections.synchronizedList([])
        ExploreServer held = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                calls << dryRun
                if( !dryRun ) {
                    inside.countDown()
                    release.await(10, TimeUnit.SECONDS)
                }
                return new CopyService.Outcome(200, [dry_run: dryRun, plan: [:]] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
        RawHttp.Response first = null

        when:
        final Thread copying = Thread.start { first = post('/api/copy', good(held), selectionCopy(), held) }
        assert inside.await(5, TimeUnit.SECONDS)
        final def second = post('/api/copy', good(held), selectionCopy(), held)
        final def dry = post('/api/copy?dry_run=true', good(held), selectionCopy(), held)
        final def write = post('/api/put', good(held),
            ('{"kind":"Selection","members":["cas://' + countsC + '/' + c2 + '"],"derived_from":[]}').getBytes('UTF-8'), held)
        release.countDown()
        copying.join(10_000)
        final def third = post('/api/copy', good(held), selectionCopy(), held)

        then:
        second.status == 409
        second.headers['content-type'] == 'application/json'
        json(second).error == 'copy_running'
        json(second).message
        dry.status == 200
        write.status == 200
        ((Map) DagJson.decode(write.body)).written == true
        first.status == 200
        third.status == 200
        calls == [false, true, false]

        cleanup:
        release.countDown()
        held?.stop()
    }

    def 'an exception from a copier is 500 copy_failed as <class>: <message>'() {
        given:
        ExploreServer failing = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun -> throw new IOException('the bucket went away') } as ExploreServer.Copier, new PrintStream(err, true))
        })

        when:
        final def r = post('/api/copy', good(failing), selectionCopy(), failing)

        then:
        r.status == 500
        r.headers['content-type'] == 'application/json'
        json(r) == [error: 'copy_failed', message: 'IOException: the bucket went away']

        when: 'the lock was released'
        final def again = post('/api/copy', good(failing), selectionCopy(), failing)

        then:
        again.status == 500
        json(again).error == 'copy_failed'

        cleanup:
        failing?.stop()
    }

    def 'an exception thrown out of CopyService is 500 copy_failed as <class>: <message>, with the alias and no absolute path'() {
        given: 'org/log is a file, so reading the destination log throws out of the plan'
        Files.createDirectories(tmp.resolve('org'))
        Files.write(tmp.resolve('org').resolve('log'), 'in the way'.getBytes('UTF-8'))
        ExploreServer real = serve({ ExploreServer s -> s.copies(config(), new PrintStream(err, true)) })

        when:
        final def r = post('/api/copy', good(real), selectionCopy(), real)
        final Map body = json(r)

        then:
        r.status == 500
        body.error == 'copy_failed'
        ((String) body.message) =~ /^\w+Exception: /
        ((String) body.message).contains('org:')
        !((String) body.message).contains(tmp.toString())
        !(((String) body.message) =~ /(^|[^\w:\/.])\/[\w.-]+/)

        cleanup:
        real?.stop()
    }

    def 'stop() waits past its ordinary grace for a copy in flight, and says so on stderr'() {
        given:
        CountDownLatch inside = new CountDownLatch(1)
        CountDownLatch release = new CountDownLatch(1)
        ExploreServer held = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                inside.countDown()
                release.await(20, TimeUnit.SECONDS)
                return new CopyService.Outcome(200, [dry_run: false, plan: [:], copied: 1] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
        held.graceSeconds = 1
        RawHttp.Response response = null

        when:
        final Thread copying = Thread.start { response = post('/api/copy', good(held), selectionCopy(), held) }
        assert inside.await(5, TimeUnit.SECONDS)
        final Thread stopper = Thread.start { held.stop() }
        // Past the one-second grace an ordinary exchange would get.
        stopper.join(2500)
        final boolean stoppedEarly = !stopper.alive
        release.countDown()
        copying.join(10_000)
        stopper.join(10_000)

        then:
        !stoppedEarly
        !stopper.alive
        response.status == 200
        json(response).copied == 1
        err.toString().contains('nf-blocks:explore: a copy is running; waiting up to 1800 s for it to finish before stopping')
        err.toString().contains("kill -9 ${ProcessHandle.current().pid()}")
        !err.toString().contains('cut off')

        cleanup:
        release.countDown()
    }

    def 'a copy that takes the lock while stop() is under way still gets the copy grace'() {
        given: 'a copier that works for longer than the ordinary grace, uninterrupted'
        CountDownLatch inside = new CountDownLatch(1)
        boolean finished = false
        ExploreServer held = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                inside.countDown()
                Thread.sleep(2500)
                finished = true
                return new CopyService.Outcome(200, [dry_run: false, plan: [:], copied: 1] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
        held.graceSeconds = 1
        held.copyGraceSeconds = 20
        byte[] body = selectionCopy()

        when: 'the request is in flight, its body not yet sent, when stop() starts'
        Socket socket = new Socket('127.0.0.1', held.port)
        final Map<String, String> headers = good(held) + [Host: "127.0.0.1:${held.port}".toString(), 'Content-Length': String.valueOf(body.length)]
        socket.outputStream.write(("POST /api/copy HTTP/1.1\r\n" + headers.collect { k, v -> "${k}: ${v}\r\n" }.join('') +
            'Connection: close\r\n\r\n').getBytes('ISO-8859-1'))
        socket.outputStream.flush()
        Thread.sleep(300)
        final Thread stopper = Thread.start { held.stop() }
        Thread.sleep(300)
        socket.outputStream.write(body)
        socket.outputStream.flush()
        assert inside.await(5, TimeUnit.SECONDS)
        stopper.join(15_000)

        then: 'stop() waited for it past the ordinary grace, and said how to stop now'
        !stopper.alive
        finished
        err.toString().contains('nf-blocks:explore: a copy is running; waiting up to 20 s for it to finish before stopping')
        err.toString().contains("kill -9 ${ProcessHandle.current().pid()}")
        !err.toString().contains('cut off')

        cleanup:
        socket?.close()
    }

    def 'a copy that outlives the copy grace is cut off with a line on stderr'() {
        given:
        CountDownLatch inside = new CountDownLatch(1)
        ExploreServer held = serve({ ExploreServer s ->
            s.copies({ Map request, boolean dryRun ->
                inside.countDown()
                new CountDownLatch(1).await(20, TimeUnit.SECONDS)
                return new CopyService.Outcome(200, [:] as Map<String, Object>)
            } as ExploreServer.Copier, new PrintStream(err, true))
        })
        held.graceSeconds = 1
        held.copyGraceSeconds = 1

        when:
        final Thread copying = Thread.start {
            try {
                post('/api/copy', good(held), selectionCopy(), held)
            }
            catch( Throwable ignored ) {
                // The connection is closed under it.
            }
        }
        assert inside.await(5, TimeUnit.SECONDS)
        held.stop()
        copying.join(10_000)

        then:
        err.toString().contains('waiting up to 1 s')
        err.toString().contains('nf-blocks:explore: the copy in flight was cut off; the destination has no Transfer for it')
    }

    def 'members.json says the server copies; without a copier, or without a token, it does not'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('lab', new LocalMemberFiles(tmp.resolve('lab')))
        final ExploreServer tokenless = new ExploreServer(members, 'lab', '<!doctype html>'.bytes)
            .copies(config(), new PrintStream(err, true)).start(0)
        final ExploreServer copierless = new ExploreServer(members, 'lab', '<!doctype html>'.bytes, null, token).start(0)

        expect:
        json(RawHttp.send(server.port, 'GET', '/members.json')).copy == true
        json(RawHttp.send(tokenless.port, 'GET', '/members.json')).copy == false
        json(RawHttp.send(copierless.port, 'GET', '/members.json')).copy == false
        post('/api/copy', good(tokenless), selectionCopy(), tokenless).status == 405
        post('/api/copy', good(copierless), selectionCopy(), copierless).status == 405

        cleanup:
        tokenless?.stop()
        copierless?.stop()
    }
}
