package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
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

    private int run(List<String> args, String stdin = '') {
        return new CasCommands().run('put', args, config(), new PrintStream(out, true), new PrintStream(err, true),
            new ByteArrayInputStream(stdin.getBytes('UTF-8')))
    }

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
}
