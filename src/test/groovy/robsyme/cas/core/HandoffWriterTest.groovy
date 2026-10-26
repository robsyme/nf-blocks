package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

/** Explorer v0.3 P4 plan 1.5 and ruling R5: the Handoff writer behind POST /api/handoff. */
class HandoffWriterTest extends Specification {

    static final long NOW = 1_758_000_000_000L                // 2025-09-16T05:20:00.000Z
    static final String TS = '2025-09-16T05:20:00.000Z'
    static final String SIGNER = 'p4-test@example.org'
    static final String SHEET = 'sample,condition\nA,treated\nB,control\n'
    static final String CONTRASTS = 'id,variable,reference,target\ntreated_vs_control,condition,control,treated\n'
    static final Cid MATRIX = Fixtures.contentCid('matrix')
    static final String GITHUB_TOKEN = 'ghp_' + 'a' * 36

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    HandoffWriter writer
    long now = NOW
    Cid item, collection, selection, project

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        writer = newWriter(store)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        item = store.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A']]]))
        selection = store.putDagCbor(new Selection(SIGNER, [Selection.item(item, [collection])], []).toCbor())
        project = store.putDagCbor(new Project(SIGNER, 'Treatment study', '', TS).toCbor())
    }

    def cleanup() {
        index?.close()
    }

    private HandoffWriter newWriter(BlockStore writable) {
        return new HandoffWriter(store, writable, index, { -> SIGNER }, { now }, { index.catchUp(store, StoreLog.of(store), 'lab') })
    }

    private Map request(Map overrides = [:]) {
        final Map r = [template : 'nf-core/differentialabundance',
                       selection: selection.toString(),
                       project  : project.toString(),
                       files    : [[name: 'samplesheet.csv', text: SHEET], [name: 'contrasts.csv', text: CONTRASTS]],
                       params   : [input    : [file: 'samplesheet.csv'],
                                   contrasts: [file: 'contrasts.csv'],
                                   matrix   : "cas://${MATRIX}/salmon.merged.gene_counts.tsv".toString(),
                                   gtf      : '/data/ref/genes.gtf',
                                   fasta    : 's3://bucket/ref/genome.fa',
                                   remote   : GITHUB_TOKEN,
                                   outdir   : 'results'],
                       timestamp: TS]
        r.putAll(overrides)
        return r
    }

    private static byte[] json(Map request) {
        return JsonOutput.toJson(request).getBytes('UTF-8')
    }

    private static Cid raw(String text) {
        return Cid.of(Cid.RAW, Hashing.sha256(text.getBytes('UTF-8')))
    }

    private HandoffWriter.Refusal refused(Object request, HandoffWriter w = writer) {
        try {
            if( request instanceof byte[] )
                w.handoff((byte[]) request, false)
            else
                w.handoff(json((Map) request), false)
        }
        catch( HandoffWriter.Refusal e ) {
            return e
        }
        throw new AssertionError('expected a refusal')
    }

    private List<Path> blockFiles() {
        return Files.walk(tempDir.resolve('store/blocks')).withCloseable { it.filter { Files.isRegularFile(it) }.toList() }
    }

    def 'a hand-off writes its files as raw blocks, the Handoff, then a handoff entry, and is indexed'() {
        when:
        final HandoffWriter.Result r = writer.handoff(json(request()), false)
        final Cid sheet = raw(SHEET)
        final Cid contrasts = raw(CONTRASTS)
        final Map expected = new Handoff(SIGNER, 'nf-core/differentialabundance', selection, project,
            [Leaf.of('samplesheet.csv', sheet, (long) SHEET.length()), Leaf.of('contrasts.csv', contrasts, (long) CONTRASTS.length())],
            [input    : "cas://${sheet}/samplesheet.csv".toString(),
             contrasts: "cas://${contrasts}/contrasts.csv".toString(),
             matrix   : "cas://${MATRIX}/salmon.merged.gene_counts.tsv".toString(),
             gtf      : Records.REDACTED_LOCATION,
             fasta    : Records.REDACTED_LOCATION,
             remote   : Records.REDACTED_SECRET,
             outdir   : 'results'],
            TS).toCbor()

        then: 'the block is the Handoff of plan 1.5, its params scrubbed'
        r.written
        r.address == DagCbor.cidOf(DagCbor.encode(expected))
        DagCbor.decode(store.open(r.address).readAllBytes()) == expected
        expected.files*.name == ['contrasts.csv', 'samplesheet.csv']

        and: 'each file is a raw block of its UTF-8 bytes'
        store.open(sheet).readAllBytes() == SHEET.getBytes('UTF-8')
        store.open(contrasts).readAllBytes() == CONTRASTS.getBytes('UTF-8')

        and: 'one handoff Store Log entry, which the index has ingested'
        StoreLog.read(store)*.name == [r.entry]
        r.entry == StoreLog.entryName(StoreLogKind.HANDOFF, r.address, NOW)
        index.firstLogEntry(r.address, 'lab')?.name == r.entry
    }

    def 'the response carries the files and the params with file references resolved, unscrubbed'() {
        when:
        final Map body = (Map) new JsonSlurper().parse(writer.handoff(json(request()), false).body())
        final Cid sheet = raw(SHEET)
        final Cid contrasts = raw(CONTRASTS)

        then:
        body.keySet() as List == ['address', 'files', 'params', 'entry', 'written']
        Cid.parse((String) body.address).codec == Cid.DAG_CBOR
        body.files == [[name: 'contrasts.csv', address: contrasts.toString(), size: CONTRASTS.length()],
                       [name: 'samplesheet.csv', address: sheet.toString(), size: SHEET.length()]]
        body.params == [input    : "cas://${sheet}/samplesheet.csv".toString(),
                        contrasts: "cas://${contrasts}/contrasts.csv".toString(),
                        matrix   : "cas://${MATRIX}/salmon.merged.gene_counts.tsv".toString(),
                        gtf      : '/data/ref/genes.gtf',
                        fasta    : 's3://bucket/ref/genome.fa',
                        remote   : GITHUB_TOKEN,
                        outdir   : 'results']
        body.written == true
        body.entry instanceof String
    }

    def 'text is stored as its UTF-8 bytes, unchanged: CRLF, no trailing newline, non-ASCII'() {
        given:
        final String text = 'sample,note\r\nA,café ✓ 𝄞'

        when:
        final HandoffWriter.Result r = writer.handoff(json(request(files: [[name: 'a.csv', text: text]], params: [:])), false)

        then:
        r.files[0].address == raw(text)
        r.files[0].size == (long) text.getBytes('UTF-8').length
        store.open(raw(text)).readAllBytes() == text.getBytes('UTF-8')
    }

    def 'a dry run answers the same address, files and params and writes nothing, not even the raw blocks'() {
        given:
        final int before = blockFiles().size()

        when:
        final HandoffWriter.Result dry = writer.handoff(json(request()), true)
        final Map body = (Map) new JsonSlurper().parse(dry.body())

        then:
        !dry.written
        body.entry == null
        body.written == false
        blockFiles().size() == before
        !store.has(raw(SHEET))
        StoreLog.read(store).isEmpty()

        when:
        final HandoffWriter.Result real = writer.handoff(json(request()), false)

        then:
        real.address == dry.address
        real.files == dry.files
        real.params == dry.params
    }

    def 'a retried hand-off writes nothing more and answers the existing entry, as Put does'() {
        given:
        final HandoffWriter.Result first = writer.handoff(json(request()), false)
        now += 60_000

        when:
        final HandoffWriter.Result again = writer.handoff(json(request()), false)

        then:
        !again.written
        again.address == first.address
        again.entry == first.entry
        StoreLog.read(store).size() == 1
    }

    def 'raw blocks the writable member already holds are not written again, and the entry comes last'() {
        given: 'the samplesheet already held, and a member that records each put and the Store Log at that moment'
        final byte[] sheetBytes = SHEET.getBytes('UTF-8')
        store.put(raw(SHEET), new ByteArrayInputStream(sheetBytes), (long) sheetBytes.length)
        final List<String> puts = []
        final LocalBlockStore recording = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override
            void put(Cid cid, InputStream input, long expectedSize) {
                puts << "${cid} entries=${StoreLog.read(this).size()}".toString()
                super.put(cid, input, expectedSize)
            }
        }
        final HandoffWriter w = newWriter(recording)

        when:
        final HandoffWriter.Result r = w.handoff(json(request()), false)

        then:
        puts == ["${raw(CONTRASTS)} entries=0".toString(), "${r.address} entries=0".toString()]
        StoreLog.read(store)*.name == [r.entry]
    }

    def 'selection and project may be DAG-JSON links, and project may be null or left out'() {
        when:
        final Cid linked = writer.handoff(json(request(selection: ['/': selection.toString()], project: ['/': project.toString()])), true).address
        final Cid plain = writer.handoff(json(request()), true).address
        final HandoffWriter.Result none = writer.handoff(json(request(project: null)), false)
        final Map omitted = request()
        omitted.remove('project')

        then:
        linked == plain
        DagCbor.decode(store.open(none.address).readAllBytes()).project == null
        writer.handoff(json(omitted), true).address == none.address
    }

    def 'asserted_by is the server principal, asked only once the request is well formed'() {
        given:
        int asked = 0
        final HandoffWriter counted = new HandoffWriter(store, store, index, { -> asked++; SIGNER }, { now }, { })

        when:
        refused(request(template: ''), counted)

        then:
        asked == 0

        when:
        final HandoffWriter.Result r = counted.handoff(json(request()), false)

        then:
        asked == 1
        DagCbor.decode(store.open(r.address).readAllBytes()).asserted_by == SIGNER
    }

    def '#why is refused as #code, naming #field, and nothing is written'() {
        given:
        final int before = blockFiles().size()

        when:
        final HandoffWriter.Refusal e = refused(request(overrides))

        then:
        e.code == code
        e.status == 400
        e.message.contains(field)
        new JsonSlurper().parse(e.body()) == [error: code, message: e.message]
        blockFiles().size() == before
        StoreLog.read(store).isEmpty()

        where:
        why                              | overrides                                                                         | code          | field
        'no template'                    | [template: '']                                                                    | 'bad_request' | 'template'
        'a template not a string'        | [template: 7]                                                                     | 'bad_request' | 'template'
        'a template over 256 characters' | [template: 'x' * 257]                                                             | 'bad_request' | 'template'
        'a template with a space'        | [template: 'nf-core/differential abundance']                                     | 'bad_request' | 'template'
        'a template with a newline'      | [template: 'nf-core/rnaseq\nrm -rf']                                             | 'bad_request' | 'template'
        'a template with a colon'        | [template: 'https://github.com/nf-core/rnaseq']                                   | 'bad_request' | 'template'
        'a template not ASCII'           | [template: 'nf-core/r\u00e9seq']                                                  | 'bad_request' | 'template'
        'no selection'                   | [selection: null]                                                                 | 'bad_request' | 'selection'
        'a selection that is no cid'     | [selection: 'bafynot']                                                            | 'bad_request' | 'selection'
        'a project that is no cid'       | [project: 12]                                                                     | 'bad_request' | 'project'
        'no files'                       | [files: []]                                                                       | 'bad_request' | 'files'
        'files not a list'               | [files: 'a.csv']                                                                  | 'bad_request' | 'files'
        'a file name with a slash'       | [files: [[name: 'a/b.csv', text: 'x']], params: [:]]                              | 'bad_request' | 'files[0].name'
        'a file name over 128'           | [files: [[name: 'a' * 129, text: 'x']], params: [:]]                              | 'bad_request' | 'files[0].name'
        'a file named ..'                | [files: [[name: '..', text: 'x']], params: [:]]                                   | 'bad_request' | 'files[0].name'
        'a file name twice'              | [files: [[name: 'a.csv', text: 'x'], [name: 'a.csv', text: 'y']], params: [:]]    | 'bad_request' | 'files[1].name'
        'a file text not a string'       | [files: [[name: 'a.csv', text: 5]], params: [:]]                                  | 'bad_request' | 'files[0].text'
        'a file with another key'        | [files: [[name: 'a.csv', text: 'x', mode: 'w']], params: [:]]                     | 'bad_request' | 'files[0]'
        'params not a map'               | [params: ['input']]                                                               | 'bad_request' | 'params'
        'a param that is a number'       | [params: [n: 3]]                                                                  | 'bad_request' | 'params.n'
        'a param naming no file'         | [params: [input: [file: 'missing.csv']]]                                          | 'bad_request' | 'params.input'
        'a param map with another key'   | [params: [input: [file: 'samplesheet.csv', x: 1]]]                                | 'bad_request' | 'params.input'
        'no timestamp'                   | [timestamp: null]                                                                 | 'bad_request' | 'timestamp'
        'a timestamp without millis'     | [timestamp: '2025-09-16T05:20:00Z']                                               | 'bad_request' | 'timestamp'
        'a timestamp not an instant'     | [timestamp: '2025-02-30T05:20:00.000Z']                                           | 'bad_request' | 'timestamp'
        'an unknown field'               | [kind: 'Handoff']                                                                 | 'bad_request' | 'kind'
        'a timestamp 11 minutes off'     | [timestamp: '2025-09-16T05:31:00.000Z']                                           | 'clock_skew'  | 'timestamp'
        'a selection not held'           | [selection: Fixtures.cidOf([kind: 'Selection', n: 1]).toString()]                | 'not_found'   | 'selection'
        'a project not held'             | [project: Fixtures.cidOf([kind: 'Project', n: 1]).toString()]                    | 'not_found'   | 'project'
    }

    def 'a template is 1 to 256 characters from A-Z a-z 0-9 . _ / -'() {
        expect:
        ['nf-core/differentialabundance', 'robsyme/my_pipeline.v2', 'x' * 256, 'a'].every { String t ->
            writer.handoff(json(request(template: t)), true).address != null
        }
    }

    def 'a selection or project of another kind is wrong_kind'() {
        expect:
        refused(request(selection: project.toString())).with { code == 'wrong_kind' && message.contains('selection') }
        refused(request(project: selection.toString())).with { code == 'wrong_kind' && message.contains('project') }
        refused(request(selection: item.toString())).code == 'wrong_kind'
    }

    def 'a raw selection the composition holds is wrong_kind'() {
        given:
        final byte[] bytes = 'raw'.getBytes('UTF-8')
        store.put(raw('raw'), new ByteArrayInputStream(bytes), (long) bytes.length)

        expect:
        refused(request(selection: raw('raw').toString())).code == 'wrong_kind'
    }

    def 'a body that is not JSON, not a JSON object, not UTF-8, or holds a lone surrogate is bad_request'() {
        expect:
        refused('{"template":'.getBytes('UTF-8')).code == 'bad_request'
        refused('[1]'.getBytes('UTF-8')).code == 'bad_request'
        refused(('[' * 10_000).getBytes('UTF-8')).code == 'bad_request'
        refused([0x7b, 0xff, 0x7d] as byte[]).code == 'bad_request'
        refused(json(request()).with { new String(it, 'UTF-8') }.replace('A,treated', 'A,\\ud800').getBytes('UTF-8')).with {
            code == 'bad_request' && message.contains('files[')
        }
    }

    def 'a request over Put.MAX_REQUEST_BYTES is too_large'() {
        expect:
        refused(new byte[(int) Put.MAX_REQUEST_BYTES + 1]).code == 'too_large'
    }

    def 'a writable member that is read-only is not_writable, a 409, and a dry run there still answers'() {
        given:
        final HandoffWriter readOnly = newWriter(new LocalBlockStore(tempDir.resolve('store'), 'lab', false))

        when:
        final HandoffWriter.Refusal e = refused(request(), readOnly)

        then:
        e.code == 'not_writable'
        e.status == 409
        StoreLog.read(store).isEmpty()
        readOnly.handoff(json(request()), true).address != null
    }

    def 'a hand-off the store refuses (#why) never asks who signs, as Put does'() {
        given:
        int asked = 0
        final BlockStore writable = readOnly ? new LocalBlockStore(tempDir.resolve('store'), 'lab', false) : store
        final HandoffWriter counted = new HandoffWriter(store, writable, index, { -> asked++; SIGNER }, { now },
            { index.catchUp(store, StoreLog.of(store), 'lab') })

        when:
        final Map o = overrides.collectEntries { k, v -> [(k): v == 'PROJECT' ? project.toString() : v] }
        final HandoffWriter.Refusal e = refused(request(o), counted)

        then:
        e.code == code
        asked == 0

        where:
        why                         | readOnly | overrides                                                  || code
        'a read-only member'        | true     | [:]                                                        || 'not_writable'
        'a Selection not held'      | false    | [selection: Fixtures.cidOf([kind: 'Selection', n: 99]).toString()] || 'not_found'
        'a selection that is a Project' | false | [selection: 'PROJECT']                                    || 'wrong_kind'
        'a Project not held'        | false    | [project: Fixtures.cidOf([kind: 'Project', n: 99]).toString()] || 'not_found'
    }

    def 'a dry run signs, so its address is the one a write gives'() {
        given:
        int asked = 0
        final HandoffWriter counted = new HandoffWriter(store, store, index, { -> asked++; SIGNER }, { now },
            { index.catchUp(store, StoreLog.of(store), 'lab') })

        expect:
        counted.handoff(json(request()), true).address == counted.handoff(json(request()), false).address
        asked == 2
    }

}
