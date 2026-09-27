package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Claim
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.PutError
import robsyme.cas.core.StoreLog
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 2 and 9.2: `nf-blocks:put <file|->`, with --dry-run, calling the one builder. */
class CasCommandsPutTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    Cid item, collection

    def setup() {
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
    }

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
                asserted_by: 'ada',
            ],
        ]
    }

    private String request() { '{"kind":"Selection","members":["cas://' + collection + '/' + item + '"],"derived_from":[]}' }

    private int run(List<String> args, String stdin = '', CasCommands commands = new CasCommands()) {
        return commands.run('put', args, config(), new PrintStream(out, true), new PrintStream(err, true),
            new ByteArrayInputStream(stdin.getBytes('UTF-8')))
    }

    /** Each line of stdout, decoded; --name prints one body per request. */
    private List<Map> bodies() {
        return out.toString().readLines().findAll { String l -> l.trim() }.collect { String l -> (Map) DagJson.decode(l) }
    }

    private int logSize() { StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).size() }

    def 'put writes the block and prints the response'() {
        given:
        final Path file = tempDir.resolve('selection.json')
        file.text = request()

        when:
        final int status = run([file.toString()])
        final Map body = (Map) DagJson.decode(out.toString().trim())

        then:
        status == 0
        body.written == true
        body.address instanceof Cid
        new LocalBlockStore(tempDir.resolve('store'), 'lab', false).has((Cid) body.address)
        StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).size() == 1
    }

    def '--dry-run arrives as the pair --dry-run true and writes nothing'() {
        when:
        final int status = run(['-', '--dry-run', 'true'], request())
        final Map body = (Map) DagJson.decode(out.toString().trim())

        then:
        status == 0
        body.exists == false
        body.names == []
        StoreLog.read(new LocalBlockStore(tempDir.resolve('store'), 'lab', false)).isEmpty()
    }

    def 'a refusal prints the error body and exits 1'() {
        when:
        final int status = run(['-'], '{"kind":"Selection","members":[],"derived_from":[]}')

        then:
        status == 1
        DagJson.decode(out.toString().trim()) == [error: 'empty', message: 'an empty Selection is refused (block explorer spec section 7.2)', at: '/members']
    }

    def 'usage errors exit 2'() {
        expect:
        run(args) == 2

        where:
        args << [[], ['a.json', 'b.json'], ['a.json', '--dry-run', 'maybe'], ['a.json', '--port', '1']]
    }

    def 'a file that does not exist is a failure the verb reports'() {
        expect:
        run([tempDir.resolve('nope.json').toString()]) == 1
        err.toString().contains('nope.json')
    }

    def '--name writes the Selection, then a set name Claim about it through the same builder'() {
        when:
        final int status = run(['-', '--name', 'greetings'], request())
        final List<Map> printed = bodies()
        final Map claim = (Map) printed[1].block

        then:
        status == 0
        printed.size() == 2
        printed[0].written == true
        printed[1].written == true
        claim.kind == 'Claim'
        claim.subject == printed[0].address
        claim.verb == Claim.SET
        claim.attribute == Claim.NAME
        claim.value == 'greetings'
        claim.supersedes == []
        claim.asserted_by == 'ada'
        claim.timestamp ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
        logSize() == 2
    }

    def 'a new --name on a Selection already written supersedes its current name Claim'() {
        given:
        run(['-', '--name', 'first'], request())
        final Cid first = (Cid) bodies()[1].address
        out.reset()

        when:
        final int status = run(['-', '--name', 'second'], request())
        final List<Map> printed = bodies()

        then:
        status == 0
        printed[0].written == false
        printed[1].block.value == 'second'
        printed[1].block.supersedes == [first]
        logSize() == 3
    }

    def 'the one current name already equal writes no Claim'() {
        given:
        run(['-', '--name', 'greetings'], request())
        out.reset()

        when:
        final int status = run(['-', '--name', ' greetings '], request())

        then:
        status == 0
        bodies().size() == 1
        bodies()[0].written == false
        err.toString().contains("already named 'greetings'")
        logSize() == 2
    }

    def '--dry-run --name prints the dry run and the Claim it would send, and writes nothing'() {
        when:
        final int status = run(['-', '--dry-run', 'true', '--name', 'greetings'], request())
        final List<Map> printed = bodies()

        then:
        status == 0
        printed.size() == 2
        printed[0].exists == false
        printed[1].subMap(['kind', 'subject', 'verb', 'attribute', 'value', 'supersedes']) ==
            [kind: 'Claim', subject: printed[0].address, verb: 'set', attribute: 'name', value: 'greetings', supersedes: []]
        printed[1].timestamp ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/
        logSize() == 0
    }

    def 'a Claim the builder refuses is "saved, naming failed", exit 1, the Selection kept'() {
        when:
        // The verb's clock an hour behind the builder's: the builder refuses the Claim with clock_skew.
        final int status = run(['-', '--name', 'greetings'], request(), new CasCommands(clock: ({ -> System.currentTimeMillis() - 3_600_000L })))
        final List<Map> printed = bodies()

        then:
        status == 1
        printed[0].written == true
        printed[1].error == PutError.CLOCK_SKEW
        err.toString().contains("saved ${printed[0].address}, naming failed")
        logSize() == 1
    }

    def '--name is for a Selection and a non-blank name; a usage error writes nothing'() {
        given:
        final String claimRequest = '{"kind":"Claim","subject":{"/":"' + item + '"},"verb":"delete","attribute":null,"value":null,"supersedes":[],"timestamp":"2026-09-25T10:00:00.000Z"}'

        expect:
        run(['-', '--name', 'x'], claimRequest) == 2
        err.toString().contains('--name names a Selection')
        run(['-', '--name', '   '], request()) == 2
        run(['-', '--name', 'n' * 257], request()) == 2
        logSize() == 0
    }
}
