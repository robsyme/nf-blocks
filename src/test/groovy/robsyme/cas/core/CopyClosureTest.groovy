package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonOutput
import spock.lang.Specification
import spock.lang.TempDir

/**
 * P3 plan 1.4 with D7: which blocks and Claims `nf-blocks:copy` carries at
 * each depth. Two members: `lab` (the source) and `other`, both caught up into
 * one index, as the copy composition's cache is.
 *
 * Run one (nf records held, so the task rule applies):
 *   bam:    i1 {meta.id s1, bam1}, i2 {s2, bam2}
 *   counts: c1 {s1, counts1, dir1 -> f1, sub/dir2 -> f2, an unaddressed reads Leaf}, c2 {s2, counts2}
 *   qc:     q1 {s1, qc1, an unaddressed Leaf}
 *   nf: counts1 comes from COUNT, which read ALIGN's a.bam (published as bam1); qc1 from QC, which read only inputs.
 * Run two (nf_run_hash unknown, so the join-key rule applies), with an InputSet (run metadata) and an index file:
 *   out:    r1 {s1, x1}, r2 {s9, x2}, r3 {s1, x3}
 * Selections: root = {c1 via counts, nested}; nested = {r1 via out}.
 * f2 is held only by `other`, so it is missing at the source.
 */
class CopyClosureTest extends Specification {

    static final String COUNT = '3' * 32
    static final String ALIGN = '1' * 32
    static final String QC = '4' * 32

    @TempDir Path tmp

    LocalBlockStore lab
    LocalBlockStore other
    Index index
    long clock = 1_758_000_000_000L

    // run one
    Cid bam1, bam2, counts1, counts2, qc1, f1, f2, script1, dir1, dir2
    Cid i1, i2, c1, c2, q1, bamC, countsC, qcC, manifest1, completion1
    // run two
    Cid x1, x2, x3, idx, r1, r2, r3, outC, manifest2, completion2, inputSet
    // Selections
    Cid nested, rootSel
    // Claims
    Cid nameOld, nameNew, pinC1, retainRun1, deleteRun2, projectR1, noteX1, labelX3, nameX2, pinBam2
    Cid elsewhereName, elsewhereDelete, elsewhereDel

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        other = new LocalBlockStore(tmp.resolve('other'), 'other', true)
        index = Index.open(tmp.resolve('cache/index.sqlite'))
        build()
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(other, StoreLog.of(other), 'other')
    }

    def cleanup() {
        index?.close()
    }

    private static Cid raw(BlockStore store, String text) {
        return store.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8')))
    }

    private Cid logged(BlockStore store, StoreLogKind kind, Map block) {
        final Cid cid = store.putDagCbor(block)
        StoreLog.append(store, kind, cid, clock += 1000)
        return cid
    }

    private Cid claim(BlockStore store, Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = []) {
        return logged(store, StoreLogKind.CLAIM, Fixtures.claim(subject, verb, attribute, value, supersedes))
    }

    private static Map leaf(String name, Cid address, String content) {
        return Fixtures.leaf2(name, address, content.getBytes('UTF-8').length)
    }

    private static Map unaddressed(String name) {
        return [kind: 'Leaf', name: name, address: null, size: null, reason: 'never_published']
    }

    private static Map entry(String name, String mode, long size, Cid address) {
        return [name: name, mode: mode, size: size, address: address, target: null]
    }

    private void nfRecord(String key, Map body) {
        final Path dir = tmp.resolve('lab/nf').resolve(key)
        Files.createDirectories(dir)
        dir.resolve('.data.json').text = JsonOutput.toJson(body)
    }

    private void build() {
        bam1 = raw(lab, 'bam-s1'); bam2 = raw(lab, 'bam-s2')
        counts1 = raw(lab, 'counts-s1'); counts2 = raw(lab, 'counts-s2')
        qc1 = raw(lab, 'qc-s1'); f1 = raw(lab, 'f1'); f2 = raw(other, 'f2')
        script1 = raw(lab, 'workflow { }')
        dir2 = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [entry('f2', 'regular', 2, f2)]])
        dir1 = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [entry('f1', 'regular', 2, f1), entry('sub', 'directory', 0, dir2)]])
        i1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], bam: leaf('a.bam', bam1, 'bam-s1')]))
        i2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], bam: leaf('b.bam', bam2, 'bam-s2')]))
        c1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], counts: leaf('c.tsv', counts1, 'counts-s1'),
                                                   dir: Fixtures.leaf2('d', dir1, 0), reads: unaddressed('r.fq')]))
        c2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], counts: leaf('c2.tsv', counts2, 'counts-s2')]))
        q1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], qc: leaf('q.txt', qc1, 'qc-s1'), raw: unaddressed('q.fq')]))
        manifest1 = lab.putDagCbor(Fixtures.runManifest(run_name: 'one', nf_run_hash: 'a1b2c3d4e5f6', script: script1, pipeline: 'p1'))
        bamC = lab.putDagCbor(Fixtures.outputCollection(manifest1, 'bam', [[i1, ['a.bam']], [i2, ['b.bam']]]))
        countsC = lab.putDagCbor(Fixtures.outputCollection(manifest1, 'counts', [[c1, ['c.tsv', 'd', null]], [c2, ['c2.tsv']]]))
        qcC = lab.putDagCbor(Fixtures.outputCollection(manifest1, 'qc', [[q1, ['q.txt', null]]]))
        completion1 = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(manifest1, [bamC, countsC, qcC]))

        nfRecord('a1b2c3d4e5f6', [kind: 'WorkflowRun', spec: [name: 'one']])
        nfRecord('a1b2c3d4e5f6/counts/c.tsv', [kind: 'FileOutput', spec: [path: "cas://${counts1}/c.tsv".toString(), source: "lid://${COUNT}/c.tsv".toString()]])
        nfRecord('a1b2c3d4e5f6/bam/a.bam', [kind: 'FileOutput', spec: [path: "cas://${bam1}/a.bam".toString(), source: "lid://${ALIGN}/a.bam".toString()]])
        nfRecord('a1b2c3d4e5f6/qc/q.txt', [kind: 'FileOutput', spec: [path: "cas://${qc1}/q.txt".toString(), source: "lid://${QC}/q.txt".toString()]])
        nfRecord(COUNT, [kind: 'TaskRun', spec: [input: [[type: 'Path', name: 'bam', value: "lid://${ALIGN}/a.bam".toString()]]]])
        nfRecord(ALIGN, [kind: 'TaskRun', spec: [input: [[type: 'Path', name: 'reads', value: 'lid://' + ('9' * 32) + '/r.fq']]]])
        nfRecord(QC, [kind: 'TaskRun', spec: [input: [[type: 'Path', name: 'reads', value: 'lid://' + ('9' * 32) + '/r.fq']]]])

        x1 = raw(lab, 'x-one'); x2 = raw(lab, 'x-two'); x3 = raw(lab, 'x-three'); idx = raw(lab, 'index,csv')
        manifest2 = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', nf_run_hash: 'unknown', pipeline: 'p2'))
        r1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], out: leaf('x1', x1, 'x-one')]))
        r2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's9'], out: leaf('x2', x2, 'x-two')]))
        r3 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], out: leaf('x3', x3, 'x-three')]))
        final Map out = Fixtures.outputCollection(manifest2, 'out', [[r1, ['x1']], [r2, ['x2']], [r3, ['x3']]])
        out.index = [leaf: leaf('index.csv', idx, 'index,csv'), path: 'index.csv']
        outC = lab.putDagCbor(out)
        inputSet = lab.putDagCbor([kind: 'InputSet', schema: 1, inputs: []])
        completion2 = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(manifest2, [outC], [input_set: inputSet]))

        nested = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(r1, [outC])], []).toCbor())
        rootSel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c1, [countsC]), Selection.selection(nested)], []).toCbor())

        nameOld = claim(lab, rootSel, 'set', 'name', 'wig')
        nameNew = claim(lab, rootSel, 'set', 'name', 'wigshop', [nameOld])
        pinC1 = claim(lab, c1, 'add', 'pin', 'keep')
        retainRun1 = claim(lab, completion1, 'set', 'retain', 'lineage')
        deleteRun2 = claim(lab, completion2, 'delete', null, null)
        projectR1 = claim(lab, r1, 'set', 'project', 'p1')
        noteX1 = claim(lab, x1, 'set', 'note', 'hi')
        labelX3 = claim(lab, x3, 'set', 'label', 'up')
        nameX2 = claim(lab, x2, 'set', 'name', 'x2')
        pinBam2 = claim(lab, bam2, 'add', 'pin', 'hold')

        // Logged only in the other member: none of these count for a copy from lab.
        elsewhereName = claim(other, rootSel, 'set', 'name', 'elsewhere')
        elsewhereDelete = claim(other, r1, 'delete', null, null)
        elsewhereDel = claim(other, rootSel, 'del', 'name', null, [nameNew])
    }

    private CopyClosure.Result closure(Map args = [:]) {
        final CopyClosure.Root root = (CopyClosure.Root) (args.root ?: new CopyClosure.Root('selection', rootSel))
        final Set<String> categories = (Set<String>) (args.containsKey('claims') ? args.claims : ['names', 'projects', 'annotations'] as Set)
        final LineageRecords nf = args.containsKey('nf') ? (LineageRecords) args.nf : new LocalLineageRecords(tmp.resolve('lab'))
        return CopyClosure.compute(lab, index, 'lab', root, (String) (args.depth ?: 'results'), categories, nf)
    }

    private static Map<String, Set<Cid>> groups(CopyClosure.Result r) {
        final Map<String, Set<Cid>> out = new LinkedHashMap<String, Set<Cid>>()
        for( CopyClosure.Entry e : r.entries )
            out.computeIfAbsent(e.group) { new LinkedHashSet<Cid>() }.add(e.cid)
        return out
    }

    private static CopyClosure.Run run(CopyClosure.Result r, Cid completion) {
        return r.runs.find { it.completion == completion }
    }

    private static int encodedLength(BlockStore store, Cid cid) {
        return store.open(cid).withCloseable { it.bytes.length }
    }

    def 'results: the root tree, its items and files, the via collections, and every run record'() {
        when:
        final CopyClosure.Result r = closure()
        final Map<String, Set<Cid>> g = groups(r)

        then:
        r.depth == 'results'
        g.keySet() as List == ['root', 'items', 'files', 'collections', 'run_records', 'claims']
        g.root == [rootSel, nested] as Set
        g.items == [c1, r1] as Set
        g.files == [counts1, dir1, f1, dir2, f2, x1] as Set
        g.collections == [countsC, outC] as Set
        g.run_records == [completion1, manifest1, script1, bamC, qcC, i1, i2, c2, q1,
                          completion2, manifest2, inputSet, r2, r3] as Set
        g.claims == [nameNew, nameOld, projectR1, noteX1] as Set
        r.entries*.cid.toSet().size() == r.entries.size()
        r.entries.every { it.depth == 'results' }

        and: 'groups come in 1.3 order'
        r.entries*.group.unique() == ['root', 'items', 'files', 'collections', 'run_records', 'claims']

        and:
        r.unaddressed == 1
        r.missingAtSource == [f2]
        r.runs*.completion == [completion1, completion2].sort { it.toString() }
        run(r, completion1) == new CopyClosure.Run(completion1, manifest1, script1, 'one', 'p1', null)
        run(r, completion2) == new CopyClosure.Run(completion2, manifest2, null, 'two', 'p2', null)
    }

    def 'kinds and sizes come from the block table, the Leaf, the encoded record and the store'() {
        when:
        final Map<Cid, CopyClosure.Entry> byCid = closure(depth: 'all').entries.collectEntries { [(it.cid): it] }

        then:
        byCid[counts1].kind == 'raw'
        byCid[counts1].size == 'counts-s1'.length()
        byCid[script1].kind == 'raw'
        byCid[script1].size == 'workflow { }'.length()
        byCid[f2].size == 2L
        byCid[dir1].kind == 'DirectoryManifest'
        byCid[dir1].size == encodedLength(lab, dir1)
        byCid[manifest1].kind == 'RunManifest'
        byCid[manifest1].size == encodedLength(lab, manifest1)
        byCid[rootSel].kind == 'Selection'
        byCid[i1].kind == 'OutputItem'
        byCid[outC].kind == 'OutputCollection'
        byCid[completion2].kind == 'RunCompletion'
        byCid[inputSet].kind == 'InputSet'
        byCid[nameNew].kind == 'Claim'
        byCid[nameNew].size == encodedLength(lab, nameNew)
    }

    def 'lineage: the task rule for a run with nf records, the join key for one without'() {
        when:
        final CopyClosure.Result r = closure(depth: 'lineage')
        final Map<String, Set<Cid>> g = groups(r)

        then: 'task rule on run one adds bam1 (i1 is upstream of c1); q1 shares s1 but is not upstream'
        g.lineage == [bam1, x3] as Set
        run(r, completion1).lineage == 'tasks'
        run(r, completion2).lineage == 'join_key'
        g.claims == [nameNew, nameOld, projectR1, noteX1, labelX3] as Set
        r.claims*.cid as Set == [nameNew, nameOld, projectR1, noteX1, labelX3, pinC1] as Set
        r.entries.find { it.cid == x3 }.depth == 'lineage'
        r.entries.find { it.cid == labelX3 }.depth == 'lineage'
        r.claims.find { it.cid == labelX3 }.depth == 'lineage'

        and: 'item records stay in run_records'
        g.run_records.containsAll([i1, r3])
        r.unaddressed == 1
        !g.containsKey('run_outputs')
    }

    def 'lineage falls back to the join key when the source holds no nf records'() {
        when:
        final CopyClosure.Result r = closure(depth: 'lineage', nf: null)

        then:
        r.runs*.lineage == ['join_key', 'join_key']
        groups(r).lineage == [bam1, qc1, x3] as Set
        r.unaddressed == 2

        when: 'an empty nf/ tree reads the same'
        final CopyClosure.Result empty = closure(depth: 'lineage', nf: new LocalLineageRecords(tmp.resolve('nowhere')))

        then:
        empty.runs*.lineage == ['join_key', 'join_key']
        groups(empty).lineage == [bam1, qc1, x3] as Set
    }

    def 'all: everything the runs stored, and the Claims on it'() {
        when:
        final CopyClosure.Result r = closure(depth: 'all')
        final Map<String, Set<Cid>> g = groups(r)

        then:
        g.keySet() as List == ['root', 'items', 'files', 'collections', 'run_records', 'lineage', 'run_outputs', 'claims']
        g.lineage == [bam1, x3] as Set
        g.run_outputs == [bam2, counts2, qc1, x2, idx] as Set
        r.entries.find { it.cid == inputSet }.group == 'run_records'
        r.entries.find { it.cid == inputSet }.depth == 'results'
        g.claims == [nameNew, nameOld, projectR1, noteX1, labelX3, nameX2] as Set
        r.claims.findAll { it.carried }*.cid as Set == g.claims
        r.claims.findAll { !it.carried }*.cid as Set == [pinC1, pinBam2] as Set
        r.entries.find { it.cid == bam2 }.depth == 'all'
        r.entries.find { it.cid == f1 }.depth == 'results'
        r.unaddressed == 2
        r.missingAtSource == [f2]
        run(r, completion1).lineage == 'tasks'
        run(r, completion2).lineage == 'join_key'
    }

    def 'one walk: at() views the same closure at each depth, and depths totals agree'() {
        given:
        final CopyClosure.Result all = closure(depth: 'all')

        expect:
        ['results', 'lineage', 'all'].every { String d ->
            final CopyClosure.Result direct = closure(depth: d)
            final CopyClosure.Result view = all.at(d)
            view.entries == direct.entries && view.claims == direct.claims && view.runs == direct.runs &&
                view.skippedClaims == direct.skippedClaims && view.unaddressed == direct.unaddressed &&
                view.missingAtSource == direct.missingAtSource
        }
        all.depths.keySet() as List == ['results', 'lineage', 'all']
        ['results', 'lineage', 'all'].every { String d ->
            final CopyClosure.Result v = all.at(d)
            all.depths[d].blocks == v.entries.size() &&
                all.depths[d].bytes == (long) v.entries.sum { CopyClosure.Entry e -> e.size ?: 0L } &&
                all.depths[d].unsized == v.entries.count { it.size == null }
        }
        all.depths.results.blocks < all.depths.lineage.blocks
        all.depths.lineage.blocks < all.depths.all.blocks
    }

    def 'claims: current per the source log only, listed with what they supersede, carried by category'() {
        when:
        final CopyClosure.Result r = closure()
        final Map<Cid, CopyClosure.ClaimEntry> byCid = r.claims.collectEntries { [(it.cid): it] }

        then: 'nameNew stays current: the del logged in other does not count'
        byCid.keySet() == [nameNew, nameOld, projectR1, noteX1, pinC1] as Set
        byCid.values().findAll { it.carried }*.cid as Set == [nameNew, nameOld, projectR1, noteX1] as Set
        byCid[pinC1].category == 'pins'
        !byCid[pinC1].carried
        byCid[pinC1].depth == 'results'
        byCid[nameNew].category == 'names'
        byCid[nameNew].subject == rootSel
        byCid[nameNew].verb == 'set'
        byCid[nameNew].attribute == 'name'
        byCid[nameNew].value == 'wigshop'
        byCid[nameNew].assertedBy == 'ada'
        byCid[nameNew].timestamp == '2026-09-25T10:00:00.000Z'
        byCid[nameOld].category == 'names'
        byCid[nameNew].supersedes == [nameOld]
        byCid[nameNew].current
        byCid[nameOld].supersedes == []
        !byCid[nameOld].current
        byCid[nameOld].carried
        byCid[projectR1].current
        byCid[pinC1].current
        byCid[projectR1].category == 'projects'
        byCid[noteX1].category == 'annotations'

        and: 'the current Claims that never travel, by reason'
        r.skippedClaims == [retain: 1, deletion: 1]
        closure(depth: 'all').skippedClaims == [retain: 1, deletion: 1]

        and: 'only carried Claims count in the totals'
        r.depths.results.blocks == r.entries.size()
        !r.entries*.cid.contains(pinC1)
    }

    def 'claims: pins only when asked, and none means none'() {
        when:
        final CopyClosure.Result pins = closure(depth: 'all', claims: ['pins'] as Set)

        then:
        pins.claims.findAll { it.carried }*.cid as Set == [pinC1, pinBam2] as Set
        pins.claims.findAll { it.carried }.every { it.category == 'pins' }
        groups(pins).claims == [pinC1, pinBam2] as Set
        pins.skippedClaims == [retain: 1, deletion: 1]
        pins.at('results').claims.findAll { it.carried }*.cid == [pinC1]

        when:
        final CopyClosure.Result none = closure(claims: [] as Set)

        then:
        none.claims*.cid as Set == [nameNew, nameOld, projectR1, noteX1, pinC1] as Set
        none.claims.every { !it.carried }
        !groups(none).containsKey('claims')
        none.skippedClaims == [retain: 1, deletion: 1]
    }

    def 'a run root: the whole run is results, and lineage has nothing to add'() {
        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('run', completion2), depth: 'all')
        final Map<String, Set<Cid>> g = groups(r.at('results'))

        then:
        g.root == [completion2] as Set
        g.items == [r1, r2, r3] as Set
        g.files == [x1, x2, x3, idx] as Set
        g.collections == [outC] as Set
        g.run_records == [manifest2, inputSet] as Set
        g.claims == [projectR1, noteX1, labelX3, nameX2] as Set
        r.at('results').claims*.cid as Set == [projectR1, noteX1, labelX3, nameX2] as Set
        r.at('results').skippedClaims == [retain: 0, deletion: 1]
        r.at('lineage').entries == r.at('results').entries
        r.at('lineage').runs*.lineage == ['join_key']
        !groups(r).containsKey('inputs')
        r.runs*.completion == [completion2]
        r.missingAtSource == []
    }

    def 'a closure block the source lacks is missing at the source, not dropped'() {
        given: 'run two is logged in other too, and other alone holds a block of it'
        final Cid onlyThere = raw(other, 'only-in-other')
        final Cid item = other.putDagCbor(Fixtures.outputItem2([meta: [id: 'zz'], f: leaf('o', onlyThere, 'only-in-other')]))
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [])], []).toCbor())
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('selection', sel))

        then: 'an unreadable item is listed and missing; its files are unknown'
        groups(r).items == [item] as Set
        r.missingAtSource == [item]
        r.entries.find { it.cid == item }.size == null
        r.depths.results.unsized == 1
        r.runs == []
    }

    def 'a via collection whose RunManifest has no indexed run still brings that manifest and its script'() {
        given: 'a collection of a run that never completed: no RunCompletion names its manifest'
        final Cid script = raw(lab, 'workflow { orphan() }')
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'orphan', nf_run_hash: 'orphanhash', script: script, pipeline: 'p5'))
        final Cid zf = raw(lab, 'z-file')
        final Cid z = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's5'], z: leaf('z', zf, 'z-file')]))
        final Cid zC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'z', [[z, ['z']]]))
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(z, [zC])], []).toCbor())
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('selection', sel), depth: 'all')
        final Map<String, Set<Cid>> g = groups(r)

        then: 'the destination Mark follows the collection to its manifest (META), so both travel at results'
        g.collections == [zC] as Set
        g.run_records == [manifest, script] as Set
        r.entries.find { it.cid == manifest }.depth == 'results'
        r.entries.find { it.cid == manifest }.kind == 'RunManifest'
        r.entries.find { it.cid == script }.depth == 'results'
        r.runs == []
        r.missingAtSource == []
    }

    def 'a via collection whose run the index knows, but whose RunCompletion the source lacks, still brings its RunManifest'() {
        given: 'the index has run one; then lab loses its RunCompletion'
        Files.delete(lab.blockPath(completion1))

        when:
        final CopyClosure.Result r = closure()
        final Map<String, Set<Cid>> g = groups(r)

        then: 'the manifest and script travel with the collection, as Mark at the destination follows it'
        g.run_records.containsAll([manifest1, script1])
        r.entries.find { it.cid == manifest1 }.kind == 'RunManifest'
        r.missingAtSource.contains(completion1)
        !r.missingAtSource.contains(manifest1)
    }

    def 'a supersedes chain whose current Claim the source lacks is listed, and none of it is carried'() {
        given: 'a renamed item, then lab loses the current name Claim (the index still has it)'
        final Cid one = claim(lab, c1, 'set', 'name', 'first')
        final Cid two = claim(lab, c1, 'set', 'name', 'second', [one])
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        Files.delete(lab.blockPath(two))

        when:
        final CopyClosure.Result r = closure(claims: ['names'] as Set)
        final Map<Cid, CopyClosure.ClaimEntry> byCid = r.claims.collectEntries { [(it.cid): it] }

        then:
        byCid[two].current && !byCid[two].carried
        !byCid[one].current && !byCid[one].carried
        !groups(r).claims.contains(one)
        !groups(r).claims.contains(two)

        and: 'a chain whose head the source holds is carried whole, as before'
        byCid[nameNew].carried && byCid[nameOld].carried
    }

    def 'an nf_run_hash that is not hex is never read as a path: the join key applies'() {
        given: 'a run whose manifest names an absolute path, one that holds nf records, as its nf_run_hash'
        final String outside = tmp.resolve('lab/nf/a1b2c3d4e5f6').toString()
        final Cid m = lab.putDagCbor(Fixtures.runManifest(run_name: 'odd', nf_run_hash: outside, pipeline: 'p6'))
        final Cid of = raw(lab, 'odd-file')
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's6'], o: leaf('o', of, 'odd-file')]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(m, 'o', [[item, ['o']]]))
        final Cid odd = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(m, [coll]))
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('run', odd), depth: 'lineage')

        then:
        run(r, odd).lineage == 'join_key'

        and: 'the local nf/ reader takes no absolute key either'
        new LocalLineageRecords(tmp.resolve('lab')).keysUnder(outside + '/') == []
        new LocalLineageRecords(tmp.resolve('lab')).read(outside) == null
    }

    def 'an item only the index lists for a run: the join key brings its record as well as its files'() {
        given: 'run four is logged in other, which alone holds its second collection; lab holds the rest'
        final Map manifestBlock = Fixtures.runManifest(run_name: 'four', nf_run_hash: 'unknown', pipeline: 'p4')
        final Cid manifest = lab.putDagCbor(manifestBlock)
        other.putDagCbor(manifestBlock)
        final Cid af = raw(lab, 'a-one'); final Cid bf = raw(lab, 'a-two'); final Cid yf = raw(lab, 'y-file')
        final Map a1Block = Fixtures.outputItem2([meta: [id: 's7'], a: leaf('a1', af, 'a-one')])
        final Map a2Block = Fixtures.outputItem2([meta: [id: 's8'], a: leaf('a2', bf, 'a-two')])
        final Map yBlock = Fixtures.outputItem2([meta: [id: 's7'], y: leaf('y', yf, 'y-file')])
        final Cid a1 = lab.putDagCbor(a1Block); final Cid a2 = lab.putDagCbor(a2Block); final Cid y = lab.putDagCbor(yBlock)
        [a1Block, a2Block, yBlock].each { other.putDagCbor(it) }
        final Map aBlock = Fixtures.outputCollection(manifest, 'a', [[a1, ['a1']], [a2, ['a2']]])
        final Cid aC = lab.putDagCbor(aBlock); other.putDagCbor(aBlock)
        final Cid bC = other.putDagCbor(Fixtures.outputCollection(manifest, 'b', [[y, ['y']]]))
        final Map completionBlock = Fixtures.runCompletion(manifest, [aC, bC])
        lab.putDagCbor(completionBlock)
        final Cid completion = logged(other, StoreLogKind.RUN, completionBlock)
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(a1, [aC])], []).toCbor())
        index.catchUp(other, StoreLog.of(other), 'other')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('selection', sel), depth: 'lineage')
        final Map<String, Set<Cid>> g = groups(r)

        then: 'y shares s7 with a1 and no collection the source holds lists it'
        run(r, completion).lineage == 'join_key'
        g.lineage == [yf] as Set
        g.run_records == [completion, manifest, bC, a2, y] as Set
        r.entries.find { it.cid == y }.depth == 'lineage'
        r.entries.find { it.cid == y }.kind == 'OutputItem'
        r.missingAtSource == [bC]

        and: 'results asks for neither'
        !r.at('results').entries*.cid.contains(y)
        !r.at('results').entries*.cid.contains(yf)
    }

    def 'a block the source lacks is asked for once, however often the walk reaches it'() {
        given: 'a via collection only other holds, which the selected item also names as a directory'
        final Cid gone = other.putDagCbor(Fixtures.outputCollection(manifest1, 'gone', [[c1, ['c.tsv']]]))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's6'], d: Fixtures.leaf2('d', gone, 0)]))
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [gone])], []).toCbor())
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        final List<Cid> opened = Collections.synchronizedList(new ArrayList<Cid>())
        final BlockStore counting = new LocalBlockStore(tmp.resolve('lab'), 'lab', true) {
            @Override
            InputStream open(Cid cid) {
                opened.add(cid)
                return super.open(cid)
            }
        }

        when:
        final CopyClosure.Result r = CopyClosure.compute(counting, index, 'lab', new CopyClosure.Root('selection', sel), 'all',
            [] as Set, null)

        then: 'placed in files (the better group), missing, and read once'
        r.entries.find { it.cid == gone }.group == 'files'
        r.missingAtSource == [gone]
        opened.count { it == gone } == 1
    }

    def 'a block placed first in a later group moves to the better one, and what is under it follows'() {
        given: 'run six names a directory as its script, and its one item publishes the same directory'
        final Cid h = raw(lab, 'h-file'); final Cid gf = raw(lab, 'g-file')
        final Cid sub = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [entry('h', 'regular', 6, h)]])
        final Cid dir = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [entry('g', 'regular', 6, gf), entry('sub', 'directory', 0, sub)]])
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'six', nf_run_hash: 'unknown', script: dir, pipeline: 'p6'))
        final Cid w = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's6'], d: Fixtures.leaf2('d', dir, 0)]))
        final Cid w2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's60'], d: Fixtures.leaf2('d', dir, 0), h: leaf('h', h, 'h-file')]))
        final Cid wC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'w', [[w, ['d']], [w2, ['d', 'h']]]))
        final Cid completion = logged(lab, StoreLogKind.RUN, Fixtures.runCompletion(manifest, [wC]))
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(w, [wC])], []).toCbor())
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('selection', sel), depth: 'all')
        final Map<Cid, CopyClosure.Entry> byCid = r.entries.collectEntries { [(it.cid): it] }

        then: 'the script placement (run_records) gives way to files, and the run output w2 reaches it again for nothing'
        [dir, gf, sub, h].every { byCid[it].group == 'files' && byCid[it].depth == 'results' }
        byCid[dir].kind == 'DirectoryManifest'
        byCid[sub].kind == 'DirectoryManifest'
        byCid[w2].group == 'run_records'
        groups(r).run_records == [completion, manifest, w2] as Set
        !groups(r).containsKey('run_outputs')
    }

    def 'a two-level supersedes chain is listed whole, under the current Claim'() {
        given:
        final Cid one = claim(lab, c1, 'set', 'name', 'first')
        final Cid two = claim(lab, c1, 'set', 'name', 'second', [one])
        final Cid three = claim(lab, c1, 'set', 'name', 'third', [two])
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final Map<Cid, CopyClosure.ClaimEntry> byCid = closure(claims: ['names'] as Set).claims.collectEntries { [(it.cid): it] }

        then:
        [one, two, three].every { byCid[it].category == 'names' && byCid[it].carried && byCid[it].depth == 'results' }
        byCid[three].current
        !byCid[two].current
        !byCid[one].current
        byCid[three].supersedes == [two]
        byCid[two].supersedes == [one]
        byCid[one].supersedes == []
    }

    def 'an item picked by a query (no via) is reached through every output collection that lists it'() {
        given:
        final Cid sel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c2, [])], []).toCbor())
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(root: new CopyClosure.Root('selection', sel))
        final Map<String, Set<Cid>> g = groups(r)

        then:
        g.items == [c2] as Set
        g.files == [counts2] as Set
        g.collections == [countsC] as Set
        r.runs*.completion == [completion1]
        g.run_records == [completion1, manifest1, script1, bamC, qcC, i1, i2, c1, q1] as Set
    }

    // ---- projects (explorer v0.3 P4 plan 1.8)

    private Cid project(BlockStore store, String name) {
        return logged(store, StoreLogKind.PROJECT, new Project('ada', name, '', '2026-09-25T10:00:00.000Z').toCbor())
    }

    private static Map<Cid, CopyClosure.ClaimEntry> byCid(CopyClosure.Result r) {
        return r.claims.collectEntries { [(it.cid): it] }
    }

    def 'categories: project and policy.* are projects; sample, annotation and same_sample are annotations'() {
        given: 'a Project that c1 is a member of, and the Claims a project page writes on it'
        final Cid proj = project(lab, 'RNA')
        final Cid member = claim(lab, c1, 'add', 'project', proj)
        final Cid store = claim(lab, proj, 'set', 'policy.default_store', 'lab')
        final Cid retention = claim(lab, proj, 'set', 'policy.retention', 'pin_selections')
        final Cid people = claim(lab, proj, 'add', 'policy.people', 'ada@example.org')
        final Cid future = claim(lab, proj, 'set', 'policy.future', 'x')
        final Cid sample = claim(lab, proj, 'add', 'sample', [pipeline: 'p1', id: 's1'])
        final Cid note = claim(lab, proj, 'add', 'annotation', [sample: [pipeline: 'p1', id: 's1'], name: 'condition', value: 'control'])
        final Cid same = claim(lab, proj, 'add', 'same_sample',
            [a: [pipeline: 'p1', id: 's1', run: completion1], b: [pipeline: 'p2', id: 's1', run: completion2], same: true])
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final Map<Cid, CopyClosure.ClaimEntry> c = byCid(closure())

        then:
        c[member].category == 'projects'
        [store, retention, people, future].every { c[it].category == 'projects' && c[it].carried }
        [sample, note, same].every { c[it].category == 'annotations' && c[it].carried }
        c[nameNew].category == 'names'
        c[noteX1].category == 'annotations'
    }

    def 'projects: a carried membership brings its Project, sized, and the Project own Claims by category'() {
        given:
        final Cid proj = project(lab, 'RNA')
        final Cid member = claim(lab, c1, 'add', 'project', proj)
        final Cid rename0 = claim(lab, proj, 'set', 'name', 'RNA v1')
        final Cid rename = claim(lab, proj, 'set', 'name', 'RNA v2', [rename0])
        final Cid store = claim(lab, proj, 'set', 'policy.default_store', 'lab')
        final Cid note = claim(lab, proj, 'add', 'annotation', [sample: [pipeline: 'p1', id: 's1'], name: 'condition', value: 'control'])
        final Cid pin = claim(lab, proj, 'add', 'pin', 'keep')
        final Cid retain = claim(lab, proj, 'set', 'retain', 'lineage')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()
        final Map<Cid, CopyClosure.ClaimEntry> c = byCid(r)
        final CopyClosure.Entry e = r.entries.find { it.cid == proj }

        then: 'the Project is a block of the claims group, at its membership depth, sized by its encoding'
        e.group == 'claims'
        e.kind == 'Project'
        e.depth == 'results'
        e.size == (long) encodedLength(lab, proj)
        !r.missingAtSource.contains(proj)

        and: 'its Claims, with their history, carried by category; retain counted, never listed'
        [member, store].every { c[it].category == 'projects' && c[it].carried }
        [rename, rename0].every { c[it].category == 'names' && c[it].carried && c[it].subject == proj && c[it].depth == 'results' }
        c[rename].current && !c[rename0].current
        c[note].category == 'annotations' && c[note].carried
        c[pin].category == 'pins' && !c[pin].carried
        !c.containsKey(retain)
        r.skippedClaims == [retain: 2, deletion: 1]
        groups(r).claims.containsAll([proj, member, rename, rename0, store, note])
        !groups(r).claims.contains(pin)

        and: 'every view agrees: the membership is results, so is the Project'
        closure(depth: 'all').entries.find { it.cid == proj }.depth == 'results'
    }

    def 'projects: without the projects category, no Project and none of its Claims'() {
        given:
        final Cid proj = project(lab, 'RNA')
        final Cid member = claim(lab, c1, 'add', 'project', proj)
        final Cid rename = claim(lab, proj, 'set', 'name', 'RNA v2')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(claims: ['names', 'annotations'] as Set)

        then:
        !r.entries*.cid.contains(proj)
        byCid(r)[member].category == 'projects'
        !byCid(r)[member].carried
        !byCid(r).containsKey(rename)

        when: 'projects alone: the Project travels, its name Claim is listed and not carried'
        final CopyClosure.Result p = closure(claims: ['projects'] as Set)

        then:
        p.entries.find { it.cid == proj }.group == 'claims'
        byCid(p)[member].carried
        byCid(p)[rename].category == 'names'
        !byCid(p)[rename].carried
        !groups(p).claims.contains(rename)
    }

    def 'projects: the shallowest membership gives the Project its depth'() {
        given: 'x3 is a lineage file (an odd subject, but the closure does not judge subjects)'
        final Cid proj = project(lab, 'RNA')
        final Cid member = claim(lab, x3, 'add', 'project', proj)
        final Cid rename = claim(lab, proj, 'set', 'name', 'RNA v2')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure(depth: 'all')

        then:
        r.entries.find { it.cid == proj }.depth == 'lineage'
        byCid(r)[rename].depth == 'lineage'
        !r.at('results').entries*.cid.contains(proj)
        !r.at('results').claims*.cid.contains(rename)

        when: 'a second membership on a results item'
        claim(lab, c1, 'add', 'project', proj)
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        final CopyClosure.Result again = closure(depth: 'all')

        then:
        again.entries.find { it.cid == proj }.depth == 'results'
        byCid(again)[rename].depth == 'results'
    }

    def 'projects: a Project the source lacks is missing at the source, and its membership still travels'() {
        given: 'the Project is only in other; its rename was logged in lab'
        final Cid proj = project(other, 'RNA')
        final Cid member = claim(lab, c1, 'add', 'project', proj)
        final Cid rename = claim(lab, proj, 'set', 'name', 'RNA v2')
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(other, StoreLog.of(other), 'other')

        when:
        final CopyClosure.Result r = closure()

        then: 'no error'
        r.missingAtSource.contains(proj)
        r.entries.find { it.cid == proj }.group == 'claims'
        r.entries.find { it.cid == proj }.kind == 'Project'
        byCid(r)[member].carried
        byCid(r)[rename].carried
    }

    def 'projects: only a current add whose value links a Project brings one'() {
        given:
        final Cid gone = project(lab, 'gone')
        final Cid added = claim(lab, c1, 'add', 'project', gone)
        claim(lab, c1, 'del', 'project', gone, [added])
        final Cid notAProject = claim(lab, c1, 'add', 'project', i1)
        final Cid rawValue = claim(lab, c1, 'add', 'project', bam2)
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()

        then: 'a removed membership, a link to an OutputItem and one to a raw block add no block'
        !r.entries*.cid.contains(gone)
        r.entries.find { it.cid == i1 }.group == 'run_records'
        r.entries.find { it.cid == bam2 } == null
        byCid(r)[notAProject].carried
        byCid(r)[rawValue].carried
    }

    // ---- hand-offs (explorer v0.3 P4 plan 1.5; the Handoff is the Selection's citation)

    private Cid handoff(BlockStore store, Cid selection, Cid project, List<Leaf> files, boolean log = true) {
        final Map block = new Handoff('ada', 'nf-core/differentialabundance', selection, project, files,
            [input: "cas://${files[0].address}/${files[0].name}".toString()], '2026-10-25T10:00:00.000Z').toCbor()
        return log ? logged(store, StoreLogKind.HANDOFF, block) : store.putDagCbor(block)
    }

    private static Leaf file(String name, Cid address, String content) {
        return Leaf.of(name, address, (long) content.getBytes('UTF-8').length)
    }

    def 'handoffs: a Handoff citing the root or a nested Selection travels with its files, at every depth'() {
        given: 'a hand-off of the root Selection with two files, one of the nested Selection, and one of a Selection the copy leaves out'
        final Cid sheet = raw(lab, 'sample,condition\ns1,control\n')
        final Cid contrasts = raw(lab, 'id,variable\nc,condition\n')
        final Cid h1 = handoff(lab, rootSel, null, [file('samplesheet.csv', sheet, 'sample,condition\ns1,control\n'),
                                                    file('contrasts.csv', contrasts, 'id,variable\nc,condition\n')])
        final Cid nestedSheet = raw(lab, 'sample\ns1\n')
        final Cid h2 = handoff(lab, nested, null, [file('samplesheet.csv', nestedSheet, 'sample\ns1\n')])
        final Cid elsewhereSel = logged(lab, StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c2, [countsC])], []).toCbor())
        final Cid otherSheet = raw(lab, 'sample\ns2\n')
        final Cid h3 = handoff(lab, elsewhereSel, null, [file('samplesheet.csv', otherSheet, 'sample\ns2\n')])
        final Cid hName = claim(lab, h1, 'set', 'name', 'DE run 1')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()
        final Map<Cid, CopyClosure.Entry> e = r.entries.collectEntries { [(it.cid): it] }

        then: 'the handoffs group holds both Handoffs and their files, at results, sized'
        groups(r).handoffs == [h1, sheet, contrasts, h2, nestedSheet] as Set
        e[h1].kind == 'Handoff' && e[h2].kind == 'Handoff'
        e[h1].size == (long) encodedLength(lab, h1)
        [sheet, contrasts, nestedSheet].every { e[it].kind == 'raw' }
        e[sheet].size == 'sample,condition\ns1,control\n'.length()
        e[contrasts].size == 'id,variable\nc,condition\n'.length()
        [h1, sheet, contrasts, h2, nestedSheet].every { e[it].depth == 'results' }
        ![h1, sheet, contrasts, h2, nestedSheet].any { r.missingAtSource.contains(it) }

        and: 'the group sits before claims, and a Claim on the Handoff is carried by its category'
        CopyClosure.GROUPS.indexOf('handoffs') == CopyClosure.GROUPS.indexOf('claims') - 1
        byCid(r)[hName].carried && byCid(r)[hName].subject == h1 && byCid(r)[hName].category == 'names'

        and: 'a hand-off of a Selection outside the copy stays behind'
        !e.containsKey(h3) && !e.containsKey(otherSheet)

        and: 'every view has them'
        closure(depth: 'all').entries.findAll { it.group == 'handoffs' }*.depth.toSet() == ['results'] as Set
        closure(claims: [] as Set).entries*.cid.containsAll([h1, sheet, contrasts, h2, nestedSheet])
    }

    def 'handoffs: only those the source log announced; a Handoff project link is not followed'() {
        given: 'a hand-off of the root logged in lab, into a project; another logged only in other'
        final Cid proj = project(lab, 'RNA')
        final Cid sheet = raw(lab, 'sample\ns1\n')
        final Cid h1 = handoff(lab, rootSel, proj, [file('samplesheet.csv', sheet, 'sample\ns1\n')])
        final Cid otherSheet = raw(other, 'sample\ns9\n')
        final Cid h9 = handoff(other, rootSel, null, [file('samplesheet.csv', otherSheet, 'sample\ns9\n')])
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        index.catchUp(other, StoreLog.of(other), 'other')

        when:
        final CopyClosure.Result r = closure()

        then:
        groups(r).handoffs == [h1, sheet] as Set
        !r.entries*.cid.contains(proj)
        !r.entries*.cid.contains(h9) && !r.entries*.cid.contains(otherSheet)
    }

    def 'handoffs: one deleted at the source stays behind; an undeleted or pinned one travels'() {
        given:
        final Cid s1 = raw(lab, 'one\n')
        final Cid s2 = raw(lab, 'two\n')
        final Cid s3 = raw(lab, 'three\n')
        final Cid deleted = handoff(lab, rootSel, null, [file('samplesheet.csv', s1, 'one\n')])
        claim(lab, deleted, 'delete', null, null)
        final Cid undeleted = handoff(lab, rootSel, null, [file('samplesheet.csv', s2, 'two\n')])
        final Cid gone = claim(lab, undeleted, 'delete', null, null)
        claim(lab, undeleted, 'del', null, null, [gone])
        final Cid pinned = handoff(lab, rootSel, null, [file('samplesheet.csv', s3, 'three\n')])
        claim(lab, pinned, 'delete', null, null)
        claim(lab, pinned, 'add', 'pin', 'keep')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()

        then: 'deletions never travel, so a deleted hand-off would arrive undeleted: it stays behind'
        groups(r).handoffs == [undeleted, s2, pinned, s3] as Set
        !r.entries*.cid.contains(deleted) && !r.entries*.cid.contains(s1)
    }

    def 'handoffs: a file the source lacks is missing at the source, and no error'() {
        given:
        final Cid sheet = raw(lab, 'sample\ns1\n')
        final Cid absentFile = raw(other, 'only in other\n')
        final Cid h1 = handoff(lab, rootSel, null, [file('samplesheet.csv', sheet, 'sample\ns1\n'),
                                                    file('contrasts.csv', absentFile, 'only in other\n')])
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()

        then:
        groups(r).handoffs == [h1, sheet, absentFile] as Set
        r.missingAtSource.contains(absentFile)
        !r.missingAtSource.contains(sheet) && !r.missingAtSource.contains(h1)
        r.entries.find { it.cid == absentFile }.size == 'only in other\n'.length()
    }

    def 'handoffs: a Handoff the source logs but lacks is missing at the source'() {
        given: 'the block is only in other, but lab logged it (the index read it from other)'
        final Cid sheet = raw(other, 'sample\ns1\n')
        final Cid h1 = handoff(other, rootSel, null, [file('samplesheet.csv', sheet, 'sample\ns1\n')], false)
        StoreLog.append(lab, StoreLogKind.HANDOFF, h1, clock += 1000)
        StoreLog.append(other, StoreLogKind.HANDOFF, h1, clock += 1000)
        index.catchUp(other, StoreLog.of(other), 'other')
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        when:
        final CopyClosure.Result r = closure()

        then:
        r.missingAtSource.contains(h1)
        r.entries.find { it.cid == h1 }.group == 'handoffs'
        r.entries.find { it.cid == h1 }.kind == 'Handoff'
    }

    def 'handoffs: a run root carries none'() {
        given:
        final Cid sheet = raw(lab, 'sample\ns1\n')
        handoff(lab, rootSel, null, [file('samplesheet.csv', sheet, 'sample\ns1\n')])
        index.catchUp(lab, StoreLog.of(lab), 'lab')

        expect:
        closure(root: new CopyClosure.Root('run', completion1), depth: 'all').entries.every { it.group != 'handoffs' }
    }
}
