package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Explorer v0.3 P4 plan 1.1 and 1.2: Put builds a Project, and validates the Claims on and about projects. */
class PutProjectTest extends Specification {

    static final long NOW = 1_758_000_000_000L                // 2025-09-16T05:20:00.000Z
    static final String TS = '2025-09-16T05:20:00.000Z'
    static final String SIGNER = 'p4-test@example.org'

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    Put put
    long now = NOW
    Cid manifest, itemA, collA, run, selection, project, leaf

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        put = new Put(store, store, index, { -> SIGNER }, { now }, { index.catchUp(store, StoreLog.of(store), 'lab') })
        manifest = store.putDagCbor(Fixtures.runManifest())
        itemA = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        collA = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[itemA, ['aligned/A.bam']]]))
        run = store.putDagCbor(Fixtures.runCompletion(manifest, [collA]))
        selection = store.putDagCbor(new Selection(SIGNER, [Selection.item(itemA, [collA])], []).toCbor())
        project = store.putDagCbor(new Project(SIGNER, 'tumour atlas', '', TS).toCbor())
        leaf = Fixtures.contentCid('leaf content')
        final byte[] leafBytes = 'leaf content'.getBytes('UTF-8')
        store.put(leaf, new ByteArrayInputStream(leafBytes), (long) leafBytes.length)
    }

    def cleanup() {
        index?.close()
    }

    private PutResult send(Map request, boolean dry = false) { put.put((Object) request, dry) }

    private PutResult send(String json, boolean dry = false) { put.put(json.getBytes('UTF-8'), dry) }

    private PutError refused(Object request) {
        try {
            if( request instanceof String )
                send((String) request)
            else
                send((Map) request)
            return null
        }
        catch( PutError e ) {
            return e
        }
    }

    private static Map projectRequest(Map overrides = [:]) {
        final Map request = [kind: 'Project', name: 'tumour atlas', description: 'samples from the 2025 cohort', created_at: TS]
        request.putAll(overrides)
        return request
    }

    private static Map claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = [], String ts = TS) {
        return [kind: 'Claim', subject: subject, verb: verb, attribute: attribute, value: value, supersedes: supersedes, timestamp: ts]
    }

    private static Map ref(String id, String pipeline = 'nf-core/rnaseq') { [pipeline: pipeline, id: id] }

    private static Map occurrence(String id, Cid run) { [pipeline: 'nf-core/rnaseq', id: id, run: run] }

    /** Runs each [request, code, at] case; returns the ones that went otherwise, and checks nothing was written. */
    private List<String> mismatches(List<List> cases) {
        final int blocksBefore = store.listBlocks().withCloseable { it.count() } as int
        final List<String> wrong = cases.findResults { List c ->
            final PutError e = refused(c[0])
            (e?.code == c[1] && e?.at == c[2]) ? null :
                "${String.valueOf(c[0]).take(160)}: got ${e?.code} at ${e?.at} (${e?.message}), want ${c[1]} at ${c[2]}".toString()
        }
        assert (store.listBlocks().withCloseable { it.count() } as int) == blocksBefore
        return wrong
    }

    // ------------------------------------------------------------------ Project

    def 'a Project is built from name, description and created_at; the server fills schema and asserted_by; logged as project'() {
        when:
        final PutResult r = send(projectRequest())
        final Map expected = new Project(SIGNER, 'tumour atlas', 'samples from the 2025 cohort', TS).toCbor()

        then:
        r.written
        r.block == expected
        r.block.schema == Records.SCHEMA
        r.block.asserted_by == SIGNER
        r.address == DagCbor.cidOf(DagCbor.encode(expected))
        store.has(r.address)
        r.entry == StoreLog.entryName(StoreLogKind.PROJECT, r.address, NOW)
        StoreLog.read(store)*.name == [r.entry]
    }

    def 'an empty description is a Project; a retried Project writes nothing new, even eleven minutes later'() {
        given:
        final PutResult first = send(projectRequest(name: 'retried', description: ''))
        now += 11 * 60_000

        when:
        final PutResult again = send(projectRequest(name: 'retried', description: ''))

        then:
        first.written
        !again.written
        again.address == first.address
        again.entry == first.entry
        StoreLog.read(store).size() == 1
    }

    def 'a dry run of a Project writes nothing'() {
        when:
        final PutResult dry = send(projectRequest(), true)

        then:
        dry.dryRun
        !dry.exists
        !store.has(dry.address)
        StoreLog.read(store).isEmpty()
    }

    def 'a Project request with a bad shape or a skewed clock is refused, each with its code and where'() {
        expect:
        mismatches([
            [projectRequest(extra: 1), 'invalid', '/extra'],
            [projectRequest(schema: 1), 'invalid', '/schema'],
            [projectRequest(asserted_by: 'mallory'), 'invalid', '/asserted_by'],
            [projectRequest(name: ''), 'invalid', '/name'],
            [projectRequest(name: '   '), 'invalid', '/name'],
            [projectRequest(name: 'x' * 257), 'invalid', '/name'],
            [projectRequest(name: 3L), 'invalid', '/name'],
            [projectRequest().findAll { it.key != 'name' }, 'invalid', '/name'],
            [projectRequest().findAll { it.key != 'description' }, 'invalid', '/description'],
            [projectRequest(description: null), 'invalid', '/description'],
            [projectRequest(description: 'x' * 4097), 'invalid', '/description'],
            [projectRequest().findAll { it.key != 'created_at' }, 'invalid', '/created_at'],
            [projectRequest(created_at: '2025-09-16T05:20:00Z'), 'invalid', '/created_at'],
            [projectRequest(created_at: '2025-02-30T05:20:00.000Z'), 'invalid', '/created_at'],
            [projectRequest(created_at: '2025-09-16T05:30:00.001Z'), 'clock_skew', '/created_at'],
            [projectRequest(created_at: '2025-09-16T05:09:59.999Z'), 'clock_skew', '/created_at'],
        ]) == []
    }

    def 'name and description at their limits are a Project'() {
        expect:
        send(projectRequest(name: 'x' * 256, description: 'd' * 4096)).written
    }

    def 'the refusals that name the buildable kinds name all three'() {
        when:
        final PutError other = refused('{"kind":"RunCompletion"}')
        final PutError none = refused('{"name":"x"}')
        final PutError list = refused('[1]')

        then:
        other.code == 'wrong_kind'
        other.message == 'a client may build a Selection, a Claim or a Project, not RunCompletion'
        none.code == 'invalid'
        none.message == "the request needs a kind, 'Selection', 'Claim' or 'Project'"
        list.message == "the request is a DAG-JSON map with a kind, 'Selection', 'Claim' or 'Project'"
    }

    // ---------------------------------------------------------- Claims, written

    def 'add project makes #what a member of a Project the composition holds'() {
        when:
        final PutResult r = send(claim(subject(what), 'add', 'project', project))

        then:
        r.written
        r.block.value == project

        where:
        what << ['run', 'selection', 'item']
    }

    private Cid subject(String what) {
        return [run: run, selection: selection, item: itemA, project: project][what]
    }

    def 'a Project takes each Claim of plan 1.2'() {
        given:
        final List<List> cases = [
            ['add', 'sample', ref('A')],
            ['add', 'sample', ref('x' * 256, 'p' * 256)],
            ['add', 'annotation', [sample: ref('A'), name: 'condition', value: 'treated']],
            ['add', 'annotation', [sample: ref('A'), name: '_x.y_2', value: '']],
            ['add', 'annotation', [sample: ref('A'), name: 'c' * 64, value: 'v' * 256]],
            ['add', 'same_sample', [a: occurrence('A', run), b: occurrence('A_rep', run), same: true]],
            ['add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: false]],
            ['set', 'policy.default_store', 'shared'],
            ['set', 'policy.retention', 'pin_selections'],
            ['set', 'policy.retention', 'none'],
            ['add', 'policy.people', 'ada@example.org'],
            ['set', 'name', 'tumour atlas, 2025'],
            ['delete', null, null],
        ]

        expect:
        cases.findResults { List c ->
            try {
                send(claim(project, (String) c[0], (String) c[1], c[2])).written ? null : "${c}: not written".toString()
            }
            catch( PutError e ) {
                "${c}: ${e.code} at ${e.at} (${e.message})".toString()
            }
        } == []
    }

    def 'an add on a Project supersedes earlier Claims of its own attribute (the edit), and del removes one'() {
        given:
        final Cid sample = send(claim(project, 'add', 'sample', ref('A'))).address
        final Cid annotation = send(claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'treated'])).address
        final Cid same = send(claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: true])).address
        final Cid person = send(claim(project, 'add', 'policy.people', 'ada@example.org')).address
        final Cid retention = send(claim(project, 'set', 'policy.retention', 'pin_selections')).address
        final Cid defaultStore = send(claim(project, 'set', 'policy.default_store', 'lab')).address
        final Cid deleted = send(claim(project, 'delete', null, null)).address

        when:
        final Cid edited = send(claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'control'], [annotation])).address

        then:
        [
            claim(project, 'del', 'annotation', null, [edited]),
            claim(project, 'del', 'sample', null, [sample]),
            claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: false], [same]),
            claim(project, 'del', 'policy.people', null, [person]),
            claim(project, 'set', 'policy.retention', 'none', [retention]),
            claim(project, 'del', 'policy.default_store', null, [defaultStore]),
            claim(project, 'del', null, null, [deleted]),
        ].every { Map c -> send(c).written }
    }

    def 'del project removes a membership'() {
        given:
        final Cid membership = send(claim(itemA, 'add', 'project', project)).address

        expect:
        send(claim(itemA, 'del', 'project', null, [membership])).written
    }

    // --------------------------------------------------------- Claims, refused

    def 'Claim value shapes for projects are refused as invalid at /value, before the store is read'() {
        expect:
        mismatches([
            // add project
            [claim(itemA, 'add', 'project', project.toString()), 'invalid', '/value'],
            [claim(itemA, 'add', 'project', null), 'invalid', '/value'],
            // SampleRef
            [claim(project, 'add', 'sample', 'A'), 'invalid', '/value'],
            [claim(project, 'add', 'sample', [pipeline: 'nf-core/rnaseq']), 'invalid', '/value'],
            [claim(project, 'add', 'sample', [id: 'A']), 'invalid', '/value'],
            [claim(project, 'add', 'sample', ref('A') + [run: run]), 'invalid', '/value'],
            [claim(project, 'add', 'sample', ref('')), 'invalid', '/value'],
            [claim(project, 'add', 'sample', ref('A', '')), 'invalid', '/value'],
            [claim(project, 'add', 'sample', ref('x' * 257)), 'invalid', '/value'],
            [claim(project, 'add', 'sample', [pipeline: 'nf-core/rnaseq', id: 7L]), 'invalid', '/value'],
            // annotation
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [name: 'condition', value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'treated', note: 'x']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref(''), name: 'condition', value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: '1condition', value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'con dition', value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: '', value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'c' * 65, value: 'treated']), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'v' * 257]), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 1L]), 'invalid', '/value'],
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: null]), 'invalid', '/value'],
            // same_sample
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run)]), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: 'true']), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), same: true]), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: true, why: 'x']), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: ref('A'), b: occurrence('B', run), same: true]), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run) + [extra: 1L], same: true]), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: [pipeline: 'p', id: 'B', run: run.toString()], same: true]), 'invalid', '/value'],
            [claim(project, 'add', 'same_sample', [a: occurrence('', run), b: occurrence('B', run), same: true]), 'invalid', '/value'],
            // policies
            [claim(project, 'set', 'policy.default_store', ''), 'invalid', '/value'],
            [claim(project, 'set', 'policy.default_store', 's' * 257), 'invalid', '/value'],
            [claim(project, 'set', 'policy.default_store', 1L), 'invalid', '/value'],
            [claim(project, 'set', 'policy.retention', 'all'), 'invalid', '/value'],
            [claim(project, 'set', 'policy.retention', true), 'invalid', '/value'],
            [claim(project, 'add', 'policy.people', ''), 'invalid', '/value'],
            [claim(project, 'add', 'policy.people', 'p' * 257), 'invalid', '/value'],
            [claim(project, 'add', 'policy.people', [principal: 'ada']), 'invalid', '/value'],
            // del still has no value
            [claim(project, 'del', 'sample', ref('A'), [run]), 'invalid', '/value'],
        ]) == []
    }

    def 'verb and attribute pairs that are not P4 Claims are refused as invalid at /attribute'() {
        expect:
        mismatches([
            [claim(project, 'add', 'policy.default_store', 'lab'), 'invalid', '/attribute'],
            [claim(project, 'add', 'policy.retention', 'none'), 'invalid', '/attribute'],
            [claim(project, 'add', 'name', 'x'), 'invalid', '/attribute'],
            [claim(project, 'set', 'sample', ref('A')), 'invalid', '/attribute'],
            [claim(project, 'set', 'annotation', [sample: ref('A'), name: 'c', value: 'v']), 'invalid', '/attribute'],
            [claim(project, 'set', 'policy.people', 'ada'), 'invalid', '/attribute'],
            [claim(itemA, 'set', 'project', project), 'invalid', '/attribute'],
            [claim(project, 'del', 'sample', null, []), 'invalid', '/supersedes'],
        ]) == []
    }

    def 'add project checks its subject and its value against the composition'() {
        given:
        final Cid absent = Fixtures.cidOf(new Project(SIGNER, 'elsewhere', '', TS).toCbor())

        expect:
        mismatches([
            [claim(Fixtures.cidOf(Fixtures.outputItem([[sample: 'Z']])), 'add', 'project', project), 'not_found', '/subject'],
            [claim(manifest, 'add', 'project', project), 'wrong_kind', '/subject'],
            [claim(collA, 'add', 'project', project), 'wrong_kind', '/subject'],
            [claim(leaf, 'add', 'project', project), 'wrong_kind', '/subject'],
            [claim(itemA, 'add', 'project', absent), 'not_found', '/value'],
            [claim(itemA, 'add', 'project', run), 'wrong_kind', '/value'],
            [claim(itemA, 'add', 'project', selection), 'wrong_kind', '/value'],
            [claim(itemA, 'add', 'project', leaf), 'wrong_kind', '/value'],
        ]) == []
    }

    def 'project-scoped attributes need a Project subject, and a Project takes only its own attributes'() {
        expect:
        mismatches([
            [claim(itemA, 'add', 'sample', ref('A')), 'wrong_kind', '/subject'],
            [claim(run, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'treated']), 'wrong_kind', '/subject'],
            [claim(selection, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: true]), 'wrong_kind', '/subject'],
            [claim(selection, 'set', 'policy.default_store', 'lab'), 'wrong_kind', '/subject'],
            [claim(run, 'set', 'policy.retention', 'none'), 'wrong_kind', '/subject'],
            [claim(itemA, 'add', 'policy.people', 'ada'), 'wrong_kind', '/subject'],
            [claim(leaf, 'add', 'sample', ref('A')), 'wrong_kind', '/subject'],
            [claim(itemA, 'set', 'policy.colour', 'blue'), 'wrong_kind', '/subject'],
            [claim(Fixtures.cidOf(new Project(SIGNER, 'elsewhere', '', TS).toCbor()), 'add', 'sample', ref('A')), 'not_found', '/subject'],
            [claim(project, 'add', 'project', project), 'invalid', '/attribute'],
            [claim(project, 'add', 'pin', 'paper'), 'invalid', '/attribute'],
            [claim(project, 'set', 'retain', 'lineage'), 'invalid', '/attribute'],
            [claim(project, 'set', 'colour', 'blue'), 'invalid', '/attribute'],
            [claim(project, 'set', 'policy.colour', 'blue'), 'invalid', '/attribute'],
        ]) == []
    }

    def 'each Occurrence names a RunCompletion the composition holds'() {
        given:
        final Cid absent = Fixtures.cidOf(Fixtures.runCompletion(manifest, []))

        expect:
        mismatches([
            [claim(project, 'add', 'same_sample', [a: occurrence('A', absent), b: occurrence('B', run), same: true]), 'not_found', '/value/a/run'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', itemA), same: true]), 'wrong_kind', '/value/b/run'],
            [claim(project, 'add', 'same_sample', [a: occurrence('A', leaf), b: occurrence('B', run), same: true]), 'wrong_kind', '/value/a/run'],
        ]) == []
    }

    def 'a P4 Claim supersedes only Claims of its own subject and attribute'() {
        given:
        final Cid sample = send(claim(project, 'add', 'sample', ref('A'))).address
        final Cid named = send(claim(project, 'set', 'name', 'renamed')).address
        final Cid retention = send(claim(project, 'set', 'policy.retention', 'none')).address
        final Cid membership = send(claim(itemA, 'add', 'project', project)).address
        final Cid itemName = send(claim(itemA, 'set', 'name', 'a')).address
        final Cid pin = send(claim(itemA, 'add', 'pin', 'paper')).address
        final Cid otherProject = send(projectRequest(name: 'other')).address
        final Cid otherSample = send(claim(otherProject, 'add', 'sample', ref('A'))).address

        expect:
        mismatches([
            [claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'x'], [sample]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'add', 'sample', ref('B'), [otherSample]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'del', 'sample', null, [named]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'del', 'same_sample', null, [sample]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'set', 'policy.default_store', 'lab', [retention]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'del', 'policy.people', null, [retention]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', 'project', null, [itemName]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', 'project', null, [pin]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'add', 'project', project, [itemName]), 'wrong_kind', '/supersedes/0'],
            [claim(selection, 'del', 'project', null, [membership]), 'wrong_kind', '/supersedes/0'],
        ]) == []
    }

    def 'a P4 Claim is superseded only by a Claim of its own attribute, so a rename or a bare del cannot remove it'() {
        given:
        final Cid membership = send(claim(itemA, 'add', 'project', project)).address
        final Cid runMembership = send(claim(run, 'add', 'project', project)).address
        final Cid sample = send(claim(project, 'add', 'sample', ref('A'))).address
        final Cid annotation = send(claim(project, 'add', 'annotation', [sample: ref('A'), name: 'condition', value: 'treated'])).address
        final Cid same = send(claim(project, 'add', 'same_sample', [a: occurrence('A', run), b: occurrence('B', run), same: true])).address
        final Cid retention = send(claim(project, 'set', 'policy.retention', 'none')).address
        final Cid person = send(claim(project, 'add', 'policy.people', 'ada@example.org')).address

        expect:
        mismatches([
            [claim(itemA, 'set', 'name', 'a', [membership]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', null, null, [membership]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'delete', null, null, [membership]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', 'pin', null, [membership]), 'wrong_kind', '/supersedes/0'],
            [claim(run, 'set', 'retain', 'lineage', [runMembership]), 'wrong_kind', '/supersedes/0'],
            [claim(run, 'del', null, null, [runMembership]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'set', 'name', 'renamed', [sample]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'del', null, null, [annotation]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'delete', null, null, [same]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'del', null, null, [retention]), 'wrong_kind', '/supersedes/0'],
            [claim(project, 'set', 'name', 'renamed', [person]), 'wrong_kind', '/supersedes/0'],
        ]) == []
    }

    def 'a bare del still undoes a delete, on a Project and on a run in one'() {
        given:
        send(claim(run, 'add', 'project', project))
        final Cid deletedProject = send(claim(project, 'delete', null, null)).address
        final Cid deletedRun = send(claim(run, 'delete', null, null)).address
        final Cid named = send(claim(project, 'set', 'name', 'atlas')).address

        expect:
        send(claim(project, 'del', null, null, [deletedProject])).written
        send(claim(run, 'del', null, null, [deletedRun])).written
        send(claim(project, 'set', 'name', 'atlas two', [named])).written
    }

    def 'a P4 Claim with a skewed timestamp is clock_skew'() {
        expect:
        mismatches([
            [claim(project, 'add', 'sample', ref('A'), [], '2025-09-16T05:30:00.001Z'), 'clock_skew', '/timestamp'],
        ]) == []
    }
}
