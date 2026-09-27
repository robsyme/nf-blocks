package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.DagJson
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Samplesheet
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** Decision 7 of the milestone 3 plan, ticket 05: nf-blocks:items, read-only, four formats, several runs. */
class ItemsCommandTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    LocalBlockStore store
    long clock = 1_758_000_000_000L
    Cid itemA, itemB, itemC, bare, itemD
    Cid run1, coll1, run2, coll2

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        itemA = item([[sample: 'A', lane: 2L, note: 'tumour batch'], Fixtures.leaf('A.txt', Fixtures.contentCid('A'), 1L)])
        itemB = item([[sample: 'B', lane: '2', formula: 'x=y'], Fixtures.leaf('B.txt', Fixtures.contentCid('B'), 1L)])
        itemC = item([[sample: 'Zoë', lane: 1L], Fixtures.leaf('C.txt', Fixtures.contentCid('C'), 1L)])
        // Review Focus 1: a bare file published alone (no Meta Map), and one whose Meta Map holds only a list.
        bare = item(Fixtures.leaf('bare.txt', Fixtures.contentCid('bare'), 1L))
        itemD = item([[ids: [1L, 2L]], Fixtures.leaf('D.txt', Fixtures.contentCid('D'), 1L)])
        final List<Cid> first = logRun('hash1', 'r1', '2026-09-03T10:05:00.000Z', [itemA, itemB, itemC, bare])
        run1 = first[0]
        coll1 = first[1]
        final List<Cid> second = logRun('hash2', 'r2', '2026-09-04T10:05:00.000Z', [itemA, itemD])
        run2 = second[0]
        coll2 = second[1]
    }

    private Cid item(Object value) { store.putDagCbor(Fixtures.outputItem(value)) }

    /** One run of pipeline `p` with one output, `greetings`, logged so catch-up finds it. [completion, collection]. */
    private List<Cid> logRun(String hash, String runName, String finishedAt, List<Cid> items) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(nf_run_hash: hash, run_name: runName))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'greetings',
            items.collect { Cid i -> [i, ["greetings/${i}".toString()]] }))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [finished_at: finishedAt]))
        StoreLog.append(store, StoreLogKind.RUN, completion, clock += 1000)
        return [completion, collection]
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

    private int run(List<String> args) {
        return new CasCommands().run('items', args, config(), new PrintStream(out, true, 'UTF-8'), new PrintStream(err, true, 'UTF-8'))
    }

    private String stdout() { out.toString('UTF-8') }

    private static String occ(Cid collection, Cid item) { "cas://${collection}/${item}".toString() }

    /** The CSV as rows by header; these fixtures hold no comma or quote, so a plain split reads them. */
    private static List<Map<String, String>> table(String csv) {
        assert !csv.contains('"')
        final List<String> lines = csv.readLines()
        final List<String> header = lines[0].split(',', -1) as List<String>
        return lines.drop(1).collect { String line ->
            [header, line.split(',', -1) as List<String>].transpose().collectEntries() as Map<String, String>
        }
    }

    def 'csv is the samplesheet of the matched items with a leading occurrence column'() {
        when:
        final int status = run(['greetings', 'sample=A', '--run', 'lid://hash1'])
        final List<String> lines = stdout().readLines()

        then:
        status == 0
        // Meta Map columns in canonical DAG-CBOR key order: lane, note (4), sample (6).
        lines == ['occurrence,lane,note,sample,1', "${occ(coll1, itemA)},2,tumour batch,A,cas://${Fixtures.contentCid('A')}/A.txt".toString()]
        lines.collect { String l -> l.substring(l.indexOf(',') + 1) } == Samplesheet.of(store, [itemA]).csv().readLines()
    }

    def 'a condition matches as text across types, and conditions AND'() {
        expect:
        run(['greetings', 'lane=2', '--run', 'lid://hash1', '--format', 'occurrences']) == 0
        stdout().readLines() == [itemA, itemB].sort().collect { Cid i -> occ(coll1, i) }

        when:
        out.reset()
        run(['greetings', 'lane=2', 'sample=B', '--run', 'lid://hash1', '--format', 'occurrences'])

        then:
        stdout().readLines() == [occ(coll1, itemB)]
    }

    def 'a condition splits at the first =, and a value may hold spaces or non-ASCII (Review Focus 2)'() {
        expect:
        run(['greetings', condition, '--run', 'lid://hash1', '--format', 'occurrences']) == 0
        stdout().readLines() == [occ(coll1, expected.call(this) as Cid)]

        where:
        condition           | expected
        'formula=x=y'       | ({ ItemsCommandTest t -> t.itemB })
        'note=tumour batch' | ({ ItemsCommandTest t -> t.itemA })
        'sample=Zoë'        | ({ ItemsCommandTest t -> t.itemC })
    }

    def 'an item with no Meta Map still has its row: an occurrence and its file column (Review Focus 1)'() {
        when:
        final int status = run(['greetings', '--run', 'lid://hash1'])
        final List<Map<String, String>> rows = table(stdout())
        final Map<String, String> row = rows.find { it.occurrence == occ(coll1, bare) }

        then:
        status == 0
        rows.size() == 4
        row.file == "cas://${Fixtures.contentCid('bare')}/bare.txt".toString()
        row.sample == ''
        row['1'] == ''
        rows.find { it.occurrence == occ(coll1, itemA) }.file == ''
    }

    def 'json is the lossless samplesheet with an occurrence key; a list-only Meta Map keeps its list (Review Focus 1)'() {
        when:
        final int status = run(['greetings', '--run', 'lid://hash2', '--format', 'json'])
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(stdout())

        then:
        status == 0
        rows*.occurrence == [itemA, itemD].sort().collect { Cid i -> occ(coll2, i) }
        rows.find { it.occurrence == occ(coll2, itemD) } == [occurrence: occ(coll2, itemD), ids: [1, 2], '1': "cas://${Fixtures.contentCid('D')}/D.txt".toString()]
    }

    def 'several runs: the union, sorted by item then collection; latest takes --pipeline; a run given twice counts once'() {
        expect:
        run(['greetings', 'sample=A', '--run', 'lid://hash1,latest', '--pipeline', 'p', '--format', 'occurrences']) == 0
        stdout().readLines() == [coll1, coll2].sort().collect { Cid c -> occ(c, itemA) }

        when:
        out.reset()
        run(['greetings', 'sample=A', '--run', "lid://hash1,cas://${run1}".toString(), '--format', 'occurrences'])

        then:
        stdout().readLines() == [occ(coll1, itemA)]
    }

    def 'selection is a complete put request that put accepts, one member per item with every via'() {
        when:
        final int status = run(['greetings', 'sample=A', '--run', 'lid://hash1,lid://hash2', '--format', 'selection'])
        final String request = stdout()

        then:
        status == 0
        DagJson.decode(request.trim()) == [derived_from: [], kind: 'Selection', members: [coll1, coll2].sort().collect { Cid c -> occ(c, itemA) }]

        when:
        out.reset()
        final int put = new CasCommands().run('put', ['-'], config(), new PrintStream(out, true, 'UTF-8'), new PrintStream(err, true, 'UTF-8'),
            new ByteArrayInputStream(request.getBytes('UTF-8')))
        final Map written = (Map) DagJson.decode(stdout().trim())

        then:
        put == 0
        written.written == true
        written.block.members == [[item: [address: itemA, via: [coll1, coll2].sort()]]]
    }

    def 'items writes nothing: no Store Log entry, no block, no Index Snapshot'() {
        given:
        final int entries = StoreLog.read(store).size()
        final long blocks = Files.walk(tempDir.resolve('store')).count()

        when:
        final int status = run(['greetings', '--run', 'lid://hash1,lid://hash2', '--format', 'selection'])

        then:
        status == 0
        StoreLog.read(store).size() == entries
        Files.walk(tempDir.resolve('store')).count() == blocks
        !Files.exists(tempDir.resolve('store/index'))
    }

    def 'no match: a header, an empty list or nothing, exit 0; for selection nothing and exit 1'() {
        expect:
        run(['greetings', 'sample=nobody', '--run', 'lid://hash1', '--format', format]) == status
        stdout() == printed
        err.toString('UTF-8').contains("no item of output 'greetings' matches")

        where:
        format        | status | printed
        'csv'         | 0      | 'occurrence\n'
        'json'        | 0      | '[]\n'
        'occurrences' | 0      | ''
        'selection'   | 1      | ''
    }

    def 'a run without the output, or a reference the index cannot resolve, is a failure the verb reports'() {
        expect:
        run(args) == 1
        err.toString('UTF-8').contains(named)

        where:
        args                                  | named
        ['nothing', '--run', 'lid://hash1']   | "has no output 'nothing'; its outputs are greetings"
        ['greetings', '--run', 'lid://nope']  | "no run with nextflow run hash 'nope'"
    }

    def 'usage errors exit 2 and name the argument (Review Focus 2)'() {
        expect:
        run(args) == 2
        err.toString('UTF-8').contains(named)

        where:
        args                                                              | named
        []                                                                | 'output name'
        ['greetings']                                                     | '--run'
        ['greetings', 'sample', '--run', 'lid://hash1']                   | "'sample'"
        ['greetings', '=x', '--run', 'lid://hash1']                       | "'=x'"
        ['greetings', 'sample=A', 'sample=B', '--run', 'lid://hash1']     | "'sample'"
        ['sample=A', '--run', 'lid://hash1']                              | "'sample=A'"
        ['greetings', '--run', 'lid://hash1', '--format', 'table']        | "'table'"
        ['greetings', '--run', 'latest']                                  | 'pipeline'
        ['greetings', '--run', 'lid://hash1,,lid://hash2']                | 'empty run reference'
        ['greetings', '--run', 'bogus']                                   | "'bogus'"
        ['greetings', '--run', 'lid://hash1', '--pipeline', 'p']          | '--pipeline goes with --run latest'
        ['greetings', '--run', 'lid://hash1', '--port', '1']              | '--port'
    }
}
