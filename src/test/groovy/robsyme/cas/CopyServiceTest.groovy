package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import groovy.json.JsonSlurper
import robsyme.cas.cli.CasCommands
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Handoff
import robsyme.cas.core.Index
import robsyme.cas.core.Leaf
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalRetentionStorage
import robsyme.cas.core.Project
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogEntry
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.SweepLock
import robsyme.cas.core.Transfer
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3Ops
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/**
 * `nf-blocks:copy` through {@link CopyService} (P3 plan 1.1 to 1.6): the plan
 * body, the copy and its write order, and the destination it leaves.
 *
 * `lab` holds one run:
 *   counts: c1 {meta.id s1, counts1, dir1 -> f1}, c2 {s2, counts2}
 *   bam:    i1 {s1, bam1}
 * and a Selection `sel` = {c1 via counts}, named; c1 is pinned and the run released.
 * `org` is an empty local member, `ro` a read-only one.
 */
class CopyServiceTest extends Specification {

    @TempDir Path tmp

    LocalBlockStore lab
    long now = 1_760_000_000_000L
    Closure<Long> clock = { -> now += 1000 } as Closure<Long>

    Cid counts1, counts2, bam1, f1, script, dir1, c1, c2, i1, manifest, countsC, bamC, completion, sel
    Cid nameSel, pinC1, retainRun

    Map<String, MemoryS3Ops> buckets = [:]
    Closure<S3Ops> savedS3

    def setup() {
        savedS3 = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map config, String bucket -> buckets.computeIfAbsent(bucket) { new MemoryS3Ops(bucket) } } as Closure<S3Ops>
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        counts1 = raw('counts-s1'); counts2 = raw('counts-s2'); bam1 = raw('bam-s1'); f1 = raw('f1'); script = raw('workflow { }')
        dir1 = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [[name: 'f1', mode: 'regular', size: 2, address: f1, target: null]]])
        c1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], counts: Fixtures.leaf2('c.tsv', counts1, 9), dir: Fixtures.leaf2('d', dir1, 0)]))
        c2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], counts: Fixtures.leaf2('c2.tsv', counts2, 9)]))
        i1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], bam: Fixtures.leaf2('a.bam', bam1, 6)]))
        manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'one', pipeline: 'p1', nf_run_hash: 'unknown', script: script))
        countsC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'counts', [[c1, ['c.tsv', 'd']], [c2, ['c2.tsv']]]))
        bamC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'bam', [[i1, ['a.bam']]]))
        completion = logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [bamC, countsC]))
        sel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c1, [countsC])], []).toCbor())
        nameSel = logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop', []))
        pinC1 = logged(StoreLogKind.CLAIM, Fixtures.claim(c1, 'add', 'pin', 'keep', []))
        retainRun = logged(StoreLogKind.CLAIM, Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
    }

    def cleanup() {
        CasSession.s3OpsFactory = savedS3
    }

    private Cid raw(String text) {
        return lab.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8')))
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = lab.putDagCbor(block)
        StoreLog.append(lab, kind, cid, now += 1000)
        return cid
    }

    private Map config(Map extraStores = [:]) {
        final Map stores = [lab: [location: tmp.resolve('lab').toString()],
                            org: [location: tmp.resolve('org').toString()],
                            ro : [location: tmp.resolve('ro').toString(), readOnly: true]]
        stores.putAll(extraStores)
        return [lineage: [store: [location: 'cas://lab']],
                cas    : [stores: stores, resolve: ['lab'], asserted_by: 'copier',
                          index: [path: tmp.resolve('cache/index.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
    }

    private CopyService.Outcome copy(Map request, boolean dryRun = false, Map cfg = config()) {
        final Map ask = [root: "selection:${sel}".toString(), to: 'org'] + request
        return new CopyService(clock).run(cfg, ask, dryRun, new PrintStream(new ByteArrayOutputStream(), true))
    }

    private LocalBlockStore org() { new LocalBlockStore(tmp.resolve('org'), 'org', false) }

    private static Map group(Map body, String name) {
        return (Map) ((List) ((Map) body.plan).groups).find { ((Map) it).group == name }
    }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final ResultSet rs = c.createStatement().executeQuery(sql)
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..rs.metaData.columnCount).collect { rs.getObject(it) }
            return out
        }
        finally {
            c.close()
        }
    }

    def 'a dry run plans the results closure per group, against the destination, and writes no block or entry'() {
        when:
        final CopyService.Outcome o = copy([:], true)
        final Map plan = (Map) o.body.plan

        then:
        o.status == 200
        o.body.root == [kind: 'selection', cid: sel.toString()]
        o.body.from == 'lab' && o.body.to == 'org'
        o.body.depth == 'results'
        o.body.claims == ['names', 'projects', 'annotations']
        o.body.dry_run == true
        o.body.command == "nextflow plugin nf-blocks:copy selection:${sel} --from lab --to org --depth results --claims names,projects,annotations --dry-run".toString()
        !o.body.containsKey('copied')

        and: 'root 1, items c1, files counts1 dir1 f1, collections countsC, run records (D7) and the name Claim'
        plan.blocks == 13
        plan.present == 0
        plan.transfer == 13
        plan.transfer_bytes == plan.bytes
        plan.missing_at_source == 0
        plan.unsized == 0
        ((List) plan.groups)*.group == ['root', 'items', 'files', 'collections', 'run_records', 'lineage', 'run_outputs', 'inputs', 'handoffs', 'claims']
        group(o.body, 'root').blocks == 1
        group(o.body, 'items').blocks == 1
        group(o.body, 'files').blocks == 3
        group(o.body, 'collections').blocks == 1
        group(o.body, 'run_records').blocks == 6
        group(o.body, 'lineage').blocks == 0
        group(o.body, 'claims').blocks == 1
        group(o.body, 'files').bytes == 9 + 2 + encoded(dir1)
        group(o.body, 'files').keySet() == ['group', 'blocks', 'bytes', 'present', 'present_bytes', 'transfer', 'transfer_bytes'] as Set

        and:
        plan.runs == [[completion: completion.toString(), manifest: manifest.toString(), script: script.toString(),
                       run_name: 'one', pipeline: 'p1', present: false, lineage: null]]
        ((List<Map>) plan.claims).collectEntries { [(it.cid): [it.category, it.carried, it.current, it.present]] } ==
            [(nameSel.toString()): ['names', true, true, false], (pinC1.toString()): ['pins', false, true, false]]
        ((List<Map>) plan.claims).find { it.cid == nameSel.toString() } ==
            [cid: nameSel.toString(), category: 'names', subject: sel.toString(), verb: 'set', attribute: 'name', value: 'wigshop',
             asserted_by: 'ada', timestamp: '2026-09-25T10:00:00.000Z', present: false, supersedes: [], current: true, carried: true]
        plan.skipped_claims == [retain: 1, deletion: 0]
        plan.audit == [checked: 1, findings: []]

        and: 'every depth from the same walk'
        ((Map) o.body.depths).keySet() as List == ['results', 'lineage', 'all']
        ((Map) o.body.depths).results == [blocks: 13, bytes: plan.bytes, transfer: 13, transfer_bytes: plan.bytes]
        // all adds the run's other content, counts2 and bam1; the pin is listed, not carried
        ((Map) ((Map) o.body.depths).all).blocks == 15
        ((Map) ((Map) o.body.depths).all).transfer == 15

        and: 'the destination gained nothing at all, not even its directory or a store-id'
        !Files.exists(tmp.resolve('org'))
    }

    private int encoded(Cid cid) { lab.open(cid).withCloseable { it.bytes.length } }

    def 'a real copy writes every block, then the entries the destination lacks, then the Transfer and its entry last'() {
        when:
        final CopyService.Outcome o = copy([:])

        then:
        o.status == 200
        o.body.dry_run == false
        o.body.copied == 13
        o.body.copied_bytes == ((Map) o.body.plan).bytes
        o.body.failures == []
        o.body.snapshot == 'written'
        o.body.command == "nextflow plugin nf-blocks:copy selection:${sel} --from lab --to org --depth results --claims names,projects,annotations".toString()

        and: 'every planned block is at the destination, verbatim'
        [sel, c1, counts1, dir1, f1, countsC, completion, manifest, script, bamC, i1, c2, nameSel].every { org().has(it) }
        !org().has(counts2) && !org().has(bam1) && !org().has(pinC1) && !org().has(retainRun)

        and: 'the run, the Selection and the carried Claim are logged, and the Transfer entry is the newest'
        final List<StoreLogEntry> log = StoreLog.read(org())
        log*.kind.toSet() == [StoreLogKind.RUN, StoreLogKind.SELECTION, StoreLogKind.CLAIM, StoreLogKind.TRANSFER] as Set
        log.findAll { it.kind == StoreLogKind.CLAIM }*.cid == [nameSel]
        log[0].kind == StoreLogKind.TRANSFER
        log[0].writtenAtMillis > log[1].writtenAtMillis
        log[0].cid.toString() == o.body.transfer_entry

        and: 'the Transfer names the copier, the root, the runs and the counts'
        final Transfer t = Transfer.fromCbor((Map) DagCbor.decode(org().open(log[0].cid).bytes))
        t.assertedBy == 'copier'
        t.roots == [sel]
        t.from == 'lab' && t.to == 'org'
        t.depth == 'results'
        t.claims == ['annotations', 'names', 'projects']
        t.runs == [completion]
        t.blocks == 13 && t.copied == 13 && t.bytes == ((Map) o.body.plan).bytes
        t.timestamp == Index.isoMillis(log[0].writtenAtMillis)

        and: 'the snapshot of org was written'
        Files.exists(tmp.resolve('org/index/v6.sqlite'))
    }

    def 'a carried membership brings its Project, logged as project with its Claims, (P4 1.8)'() {
        given: 'a Project in lab, the Selection a member of it, and the project renamed'
        final Cid project = logged(StoreLogKind.PROJECT, new Project('ada', 'Drought', '', '2026-10-25T10:00:00.000Z').toCbor())
        final Cid member = logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'add', 'project', project, []))
        final Cid renamed = logged(StoreLogKind.CLAIM, Fixtures.claim(project, 'set', 'name', 'Drought 2026', []))

        when:
        final CopyService.Outcome o = copy([:])

        then:
        o.status == 200
        [project, member, renamed].every { org().has(it) }

        and: 'the Project has a project entry, written with the Claims, before the Transfer'
        final List<StoreLogEntry> log = StoreLog.read(org())
        log.findAll { it.kind == StoreLogKind.PROJECT }*.cid == [project]
        log[0].kind == StoreLogKind.TRANSFER
        log.findIndexOf { it.kind == StoreLogKind.PROJECT } > log.findIndexOf { it.cid == member }   // newest first: logged before the Claims
    }

    def 'a hand-off of the copied Selection travels: the Handoff, its files and a handoff entry, so its --input resolves at the destination'() {
        given: 'a hand-off of sel in lab, with a samplesheet and a contrasts file'
        final String sheetText = 'sample,condition\ns1,control\n'
        final String contrastsText = 'id,variable,reference,target\nc,condition,control,treated\n'
        final Cid sheet = raw(sheetText)
        final Cid contrasts = raw(contrastsText)
        final String input = "cas://${sheet}/samplesheet.csv".toString()
        final Cid handoff = logged(StoreLogKind.HANDOFF, new Handoff('ada', 'nf-core/differentialabundance', sel, null,
            [Leaf.of('samplesheet.csv', sheet, (long) sheetText.length()), Leaf.of('contrasts.csv', contrasts, (long) contrastsText.length())],
            [input: input, contrasts: "cas://${contrasts}/contrasts.csv".toString()], '2026-10-25T10:00:00.000Z').toCbor())
        final long handoffBytes = sheetText.length() + contrastsText.length() + encoded(handoff)

        when: 'a dry run'
        final CopyService.Outcome dry = copy([:], true)

        then: 'the plan lists the Handoff and its two files, sized, in the handoffs group, and counts them in every depth'
        group(dry.body, 'handoffs') == [group: 'handoffs', blocks: 3, bytes: handoffBytes, present: 0, present_bytes: 0,
                                        transfer: 3, transfer_bytes: handoffBytes]
        ((Map) dry.body.plan).blocks == 16
        ((Map) ((Map) dry.body.depths).results).blocks == 16
        ((Map) ((Map) dry.body.depths).all).blocks == 18
        !Files.exists(tmp.resolve('org'))

        when: 'the copy'
        final CopyService.Outcome o = copy([:])

        then: 'the Handoff and both files are at org, verbatim'
        o.status == 200
        o.body.copied == 16
        [handoff, sheet, contrasts].every { org().has(it) }
        org().open(sheet).withCloseable { new String(it.bytes, 'UTF-8') } == sheetText
        org().open(contrasts).withCloseable { new String(it.bytes, 'UTF-8') } == contrastsText

        and: 'a handoff entry, written with the Selections and Claims, before the Transfer'
        final List<StoreLogEntry> log = StoreLog.read(org())
        log.findAll { it.kind == StoreLogKind.HANDOFF }*.cid == [handoff]
        log[0].kind == StoreLogKind.TRANSFER
        log.findIndexOf { it.kind == StoreLogKind.HANDOFF } > 0

        and: 'the Transfer is as it was: one root, the run, the counts'
        final Transfer t = Transfer.fromCbor((Map) DagCbor.decode(org().open(log[0].cid).bytes))
        t.roots == [sel]
        t.runs == [completion]
        t.blocks == 16 && t.copied == 16

        when: 'a fresh index over org alone'
        final Index fresh = Index.open(tmp.resolve('fresh.sqlite'))
        fresh.catchUp(org(), StoreLog.of(org()), 'org')
        fresh.close()

        then: 'it has the handoff row, and the samplesheet the --input names is a block org holds'
        rows(tmp.resolve('fresh.sqlite'), 'SELECT cid, template, selection_cid, project_cid FROM handoff') ==
            [[handoff.toString(), 'nf-core/differentialabundance', sel.toString(), null]]
        Handoff.fromCbor((Map) DagCbor.decode(org().open(handoff).bytes)).params.input == input
        new LocalBlockStore(tmp.resolve('org'), 'org', true).open(Cid.parse(input.substring('cas://'.length(), input.lastIndexOf('/'))))
            .withCloseable { new String(it.bytes, 'UTF-8') } == sheetText
    }

    def 'a hand-off deleted at the source does not travel'() {
        given:
        final Cid sheet = raw('sample\ns1\n')
        final Cid handoff = logged(StoreLogKind.HANDOFF, new Handoff('ada', 'rnaseq', sel, null,
            [Leaf.of('samplesheet.csv', sheet, 10L)], [:], '2026-10-25T10:00:00.000Z').toCbor())
        logged(StoreLogKind.CLAIM, Fixtures.claim(handoff, 'delete', null, null, []))

        when:
        final CopyService.Outcome o = copy([:])

        then:
        o.status == 200
        group(o.body, 'handoffs').blocks == 0
        !org().has(handoff) && !org().has(sheet)
        !StoreLog.read(org()).any { it.kind == StoreLogKind.HANDOFF }
    }

    def 'a re-copy that would change nothing writes no block, no entry and no Transfer'() {
        given:
        copy([:])
        final List<StoreLogEntry> before = StoreLog.read(org())

        when:
        final CopyService.Outcome dry = copy([from: 'lab'], true)
        final CopyService.Outcome again = copy([from: 'lab'])
        final List<StoreLogEntry> after = StoreLog.read(org())

        then:
        ((Map) dry.body.plan).present == 13
        ((Map) dry.body.plan).transfer == 0
        ((Map) dry.body.plan).present_bytes == ((Map) dry.body.plan).bytes
        ((List<Map>) ((Map) dry.body.plan).runs)[0].present == true

        and:
        again.status == 200
        again.body.copied == 0
        again.body.copied_bytes == 0
        again.body.transfer_entry == null
        again.body.failures == []
        again.body.snapshot == 'skipped'
        after == before
    }

    def 'a re-copy that brings only a new carried Claim writes it, its entry and a Transfer'() {
        given:
        copy([:])
        final List<StoreLogEntry> before = StoreLog.read(org())
        final Cid renamed = logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop2', [nameSel]))

        when:
        final CopyService.Outcome again = copy([:])
        final List<StoreLogEntry> after = StoreLog.read(org())

        then:
        again.status == 200
        again.body.copied == 1
        org().has(renamed)
        after.size() == before.size() + 2
        after[1].kind == StoreLogKind.CLAIM && after[1].cid == renamed
        after[0].kind == StoreLogKind.TRANSFER
        after[0].cid.toString() == again.body.transfer_entry
        again.body.snapshot == 'written'
    }

    def 'the Transfer entry is the newest in the destination log even when an entry there is stamped ahead of this clock'() {
        given:
        final LocalBlockStore dest = new LocalBlockStore(tmp.resolve('org'), 'org', true)
        final long ahead = now + 100_000_000L
        StoreLog.append(dest, StoreLogKind.CLAIM, Fixtures.contentCid('elsewhere'), ahead)

        when:
        final CopyService.Outcome o = copy([:])
        final List<StoreLogEntry> log = StoreLog.read(org())

        then:
        o.status == 200
        log[0].kind == StoreLogKind.TRANSFER
        log.last().writtenAtMillis == ahead
        log[0].writtenAtMillis > log[1].writtenAtMillis
        log[1].writtenAtMillis > ahead
    }

    def 'a dry run into a member that does not exist yet writes nothing there or in the source'() {
        given:
        final Map cfg = config([fresh: [location: tmp.resolve('fresh').toString()]])
        final List<String> labBefore = listing(tmp.resolve('lab'))

        when:
        final CopyService.Outcome o = copy([to: 'fresh'], true, cfg)

        then:
        o.status == 200
        ((Map) o.body.plan).transfer == 13
        !Files.exists(tmp.resolve('fresh'))
        listing(tmp.resolve('lab')) == labBefore
    }

    def 'a dry run into an S3 member writes no object'() {
        given:
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])

        when:
        final CopyService.Outcome o = copy([to: 'bucket'], true, cfg)
        final MemoryS3Ops ops = buckets['org-bucket']

        then:
        o.status == 200
        ((Map) o.body.plan).transfer == 13
        ops.objects.isEmpty()
        ops.calls.findAll { !(it.startsWith('HEAD ') || it.startsWith('GET ') || it.startsWith('LIST ')) } == []
    }

    private static List<String> listing(Path dir) {
        return Files.walk(dir).withCloseable { it.collect { Path p -> "${dir.relativize(p)} ${Files.isRegularFile(p) ? Files.size(p) : 'dir'}".toString() }.sort() }
    }

    def 'a readOnly that is not true or false on the target is a bad_config 400'() {
        given:
        final Map cfg = config()
        ((Map) ((Map) cfg.cas).stores).org = [location: tmp.resolve('org').toString(), readOnly: 'yes']

        when:
        final CopyService.Outcome o = copy([:], true, cfg)

        then:
        o.status == 400
        o.body.error == 'bad_config'
        o.body.message.toString().contains('cas.stores.org.readOnly')
    }

    def 'a readOnly that is not true or false on any configured member is a bad_config 400, not a thrown error'() {
        given: 'the source, or a member the copy does not touch'
        final Map cfg = config((alias): [location: tmp.resolve(alias).toString(), readOnly: 'no'])

        when:
        final CopyService.Outcome o = copy([:], dryRun, cfg)

        then:
        o.status == 400
        o.body.error == 'bad_config'
        o.body.message.toString().contains("cas.stores.${alias}.readOnly")

        where:
        alias   | dryRun
        'lab'   | true
        'other' | true
        'other' | false
    }

    def 'run:latest is refused with a message about copy'() {
        when:
        final CopyService.Outcome o = copy([root: 'run:latest'], true)

        then:
        o.status == 400
        o.body.error == 'bad_root'
        o.body.message == "copy takes a run by address (run:<cid>, run:lid://<hash> or run:cas://<cid>); 'latest' needs a pipeline, which copy has no flag for"
    }

    def 'without --from, the source is the first member after the destination that holds the root'() {
        given:
        copy([:])

        when: 'org now holds the root too, and comes first in cas.resolve'
        final Map cfg = config()
        ((Map) cfg.cas).resolve = ['org', 'lab']
        final CopyService.Outcome o = new CopyService(clock).run(cfg, [root: "selection:${sel}".toString(), to: 'org'], true,
            new PrintStream(new ByteArrayOutputStream(), true))

        then:
        o.status == 200
        o.body.from == 'lab'
    }

    def 'a fresh index over the destination alone has the run, the Selection and the Transfer rows'() {
        given:
        final CopyService.Outcome o = copy([:])
        final Index fresh = Index.open(tmp.resolve('fresh.sqlite'))

        when:
        fresh.catchUp(org(), StoreLog.of(org()), 'org')
        fresh.close()
        final Path db = tmp.resolve('fresh.sqlite')

        then:
        rows(db, 'SELECT completion_cid, manifest_cid, member FROM run') == [[completion.toString(), manifest.toString(), 'org']]
        rows(db, "SELECT collection_cid FROM collection WHERE kind = 'selection'") == [[sel.toString()]]
        rows(db, 'SELECT cid, from_member, to_member, asserted_by, depth, blocks, copied FROM transfer') ==
            [[o.body.transfer_entry, 'lab', 'org', 'copier', 'results', 13, 13]]
        rows(db, 'SELECT root_cid, role FROM transfer_root ORDER BY role') == [[sel.toString(), 'root'], [completion.toString(), 'run']]
    }

    def 'a first copy into a member an explore cache scanned while empty rebuilds no cache, and lab\'s rows stay'() {
        given: 'an explore-style session over [lab, org], sharing cas.index.path, caught up while org holds nothing'
        final Map cfg = config()
        final Map explore = new LinkedHashMap(cfg)
        explore.cas = new LinkedHashMap((Map) cfg.cas)
        ((Map) explore.cas).resolve = ['lab', 'org']
        final Path db = tmp.resolve('cache/index.sqlite')
        final ByteArrayOutputStream before = new ByteArrayOutputStream()
        catchUpWith(explore, before)
        assert rows(db, "SELECT value FROM meta WHERE key = 'block_scan:org'").size() == 1
        assert rows(db, "SELECT value FROM meta WHERE key = 'store_id:org'").isEmpty()
        final List<List<Object>> labRuns = rows(db, "SELECT completion_cid FROM run WHERE member = 'lab'")
        final List<List<Object>> labEntries = rows(db, "SELECT cid, kind FROM log_entry WHERE member = 'lab' ORDER BY cid")
        assert labRuns == [[completion.toString()]] && labEntries.size() == 5

        when: 'a real copy creates org and its store-id, then the explore session catches up again'
        final ByteArrayOutputStream err = new ByteArrayOutputStream()
        final CopyService.Outcome o = new CopyService(clock).run(cfg, [root: "selection:${sel}".toString(), to: 'org'], false,
            new PrintStream(err, true))
        final ByteArrayOutputStream after = new ByteArrayOutputStream()
        catchUpWith(explore, after)

        then:
        o.status == 200 && o.body.copied == 13
        Files.exists(tmp.resolve('org/store-id'))
        !(err.toString('UTF-8') + after.toString('UTF-8')).contains('rebuilding the cache index')
        rows(db, "SELECT value FROM meta WHERE key = 'store_id:org'") == [[Files.readString(tmp.resolve('org/store-id')).trim()]]
        rows(db, "SELECT completion_cid FROM run WHERE member = 'lab'") == labRuns
        rows(db, "SELECT cid, kind FROM log_entry WHERE member = 'lab' ORDER BY cid") == labEntries
    }

    private static void catchUpWith(Map cfg, ByteArrayOutputStream err) {
        final CasSession cas = CasSession.forCommand(CasConfig.from(cfg, 'cas://lab'), new PrintStream(err, true))
        final Index index = cas.openIndex()
        try {
            assert cas.catchUpIndex(index).isEmpty()
        }
        finally {
            index.close()
        }
    }

    def 'a sweep dry run of a composition of the destination alone finds no missing metadata (D7)'() {
        given:
        copy([:])
        final Map alone = [lineage: [store: [location: 'cas://org']],
                           cas    : [stores: [org: [location: tmp.resolve('org').toString()]], asserted_by: 'copier',
                                     index: [path: tmp.resolve('alone.sqlite').toString()]]]
        final ByteArrayOutputStream out = new ByteArrayOutputStream()

        when:
        final int code = new CasCommands().run('sweep', ['--format', 'json'], alone, new PrintStream(out, true),
            new PrintStream(new ByteArrayOutputStream(), true))
        final Map report = (Map) new JsonSlurper().parseText(out.toString())

        then:
        code == 0
        report.missing_metadata == []
        report.stopped == null
    }

    def 'a copy into an S3 member writes its blocks, entries and Transfer to the bucket'() {
        given:
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])

        when:
        final CopyService.Outcome o = copy([to: 'bucket'], false, cfg)
        final S3BlockStore dest = new S3BlockStore(buckets['org-bucket'], 'cas/', 'bucket', false, tmp.resolve('t'))
        final List<StoreLogEntry> log = StoreLog.read(dest)

        then:
        o.status == 200
        o.body.copied == 13
        [sel, c1, counts1, dir1, f1, completion, manifest, script, nameSel].every { dest.has(it) }
        log[0].kind == StoreLogKind.TRANSFER
        log[0].cid.toString() == o.body.transfer_entry
        dest.has(log[0].cid)
        log*.kind.count { it == StoreLogKind.RUN } == 1
        o.body.snapshot == 'written'
        buckets['org-bucket'].objects.containsKey('cas/index/v6.sqlite')
    }

    def 'a read-only member is not a target'() {
        when:
        final CopyService.Outcome o = copy([to: 'ro'], dryRun)

        then:
        o.status == 400
        o.body == [error: 'not_a_target', message: "'ro' is not a copy target: ${CopyTargets.READ_ONLY}".toString()]
        !Files.exists(tmp.resolve('ro'))

        where:
        dryRun << [true, false]
    }

    def 'an audit finding is listed by a dry run and stops a real copy unless accepted'() {
        given: 'a second run whose RunManifest keeps an absolute path'
        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', params: [outdir: '/home/someone/results']))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's3'], out: Fixtures.leaf2('x', counts2, 9)]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(leaky, 'out', [[item, ['x']]]))
        logged(StoreLogKind.RUN, Fixtures.runCompletion(leaky, [coll]))
        final Cid two = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [coll])], []).toCbor())

        when:
        final CopyService.Outcome dry = copy([root: two.toString()], true)
        final CopyService.Outcome refused = copy([root: two.toString()])

        then:
        dry.status == 200
        ((Map) ((Map) dry.body.plan).audit).checked == 1
        ((List<Map>) ((Map) ((Map) dry.body.plan).audit).findings).collect { [it.cid, it.kind, it.where] } ==
            [[leaky.toString(), 'RunManifest', 'params.outdir']]
        refused.status == 409
        refused.body.dry_run == false
        !refused.body.containsKey('copied')
        ((List) ((Map) ((Map) refused.body.plan).audit).findings).size() == 1
        !Files.exists(tmp.resolve('org'))

        when:
        final CopyService.Outcome accepted = copy([root: two.toString(), accept_audit: true])

        then:
        accepted.status == 200
        accepted.body.command.toString().endsWith('--accept-audit true')
        org().has(leaky)
    }

    /** A Claim written into org and logged there, as org's own users would. */
    private Cid claimAtOrg(Map block) {
        final LocalBlockStore dest = new LocalBlockStore(tmp.resolve('org'), 'org', true)
        final Cid cid = dest.putDagCbor(block)
        StoreLog.append(dest, StoreLogKind.CLAIM, cid, now += 1000)
        return cid
    }

    private static List<Map> destinationState(CopyService.Outcome o) {
        return (List<Map>) ((Map) o.body.plan).destination_state
    }

    /** Every file under org, with its size, for "nothing was written". */
    private List<String> orgListing() { listing(tmp.resolve('org')) }

    def 'D9: a run the destination logs and has released makes kept false, and a real copy is refused before any write'() {
        given: 'org holds the copy, then releases the run itself; lab gains a new name the next copy would carry'
        copy([:])
        claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        final Cid renamed = logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop2', [nameSel]))
        final List<String> before = orgListing()

        when:
        final CopyService.Outcome dry = copy([:], true)

        then:
        dry.status == 200
        dry.body.refused == null
        ((Map) dry.body.plan).kept == false
        destinationState(dry) == [
            [cid: sel.toString(), role: 'root', name: 'wigshop2', state: 'held'],
            [cid: completion.toString(), role: 'run', name: 'one', state: 'released'],
        ]

        when:
        final CopyService.Outcome refused = copy([:])

        then:
        refused.status == 409
        refused.body.refused == 'not_kept'
        refused.body.dry_run == false
        ((Map) refused.body.plan).kept == false
        !refused.body.containsKey('copied')
        !org().has(renamed)
        orgListing() == before
    }

    def 'D9: a Selection deleted at the destination makes kept false, and a real copy is refused'() {
        given:
        copy([:])
        claimAtOrg(Fixtures.claim(sel, 'delete', null, null, []))
        final List<String> before = orgListing()

        when:
        final CopyService.Outcome dry = copy([:], true)
        final CopyService.Outcome refused = copy([:])

        then:
        ((Map) dry.body.plan).kept == false
        destinationState(dry).find { it.role == 'root' } == [cid: sel.toString(), role: 'root', name: 'wigshop', state: 'deleted']
        destinationState(dry).find { it.role == 'run' }.state == 'held'
        refused.status == 409
        refused.body.refused == 'not_kept'
        orgListing() == before
    }

    def 'D9: a pinned run, a held run and a pinned run that is also deleted are kept'() {
        given:
        copy([:])
        if( pin )
            claimAtOrg(Fixtures.claim(completion, 'add', 'pin', 'keep', []))
        if( release )
            claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        if( delete )
            claimAtOrg(Fixtures.claim(completion, 'delete', null, null, []))

        when:
        final CopyService.Outcome dry = copy([root: "run:${completion}".toString()], true)
        final CopyService.Outcome real = copy([root: "run:${completion}".toString()])

        then:
        ((Map) dry.body.plan).kept == true
        destinationState(dry) == [
            [cid: completion.toString(), role: 'root', name: 'one', state: state],
        ]
        real.status == 200
        real.body.refused == null

        where:
        pin   | release | delete || state
        false | false   | false  || 'held'
        true  | false   | false  || 'pinned'
        true  | true    | false  || 'pinned'
        true  | false   | true   || 'pinned'
    }

    def 'D9: the Claims the copy carries count, so a carried del pin under a release at the destination makes kept false'() {
        given: 'org pins and releases the run; lab holds a del pin superseding org\'s pin'
        copy([:])
        final Cid orgPin = claimAtOrg(Fixtures.claim(completion, 'add', 'pin', 'keep', []))
        claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        lab.putDagCbor(Fixtures.claim(completion, 'add', 'pin', 'keep', []))
        final Cid unpin = logged(StoreLogKind.CLAIM, Fixtures.claim(completion, 'del', 'pin', null, [orgPin]))
        final List<String> before = orgListing()

        when: 'pins are carried'
        final CopyService.Outcome dry = copy([claims: ['pins']], true)
        final CopyService.Outcome refused = copy([claims: ['pins']])

        then: 'the plan describes org after the copy logs the del pin'
        ((Map) dry.body.plan).kept == false
        destinationState(dry).find { it.role == 'run' }.state == 'released'
        refused.status == 409
        refused.body.refused == 'not_kept'
        !org().has(unpin)
        orgListing() == before

        when: 'pins are not carried'
        final CopyService.Outcome kept = copy([claims: ['names']], true)

        then: 'org\'s pin stands'
        ((Map) kept.body.plan).kept == true
        destinationState(kept).find { it.role == 'run' }.state == 'pinned'
    }

    def 'D9: a conflicted retain group at the destination reads as held'() {
        given:
        copy([:])
        claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'all', [], '2026-09-26T10:00:00.000Z'))

        expect:
        destinationState(copy([:], true)).find { it.role == 'run' }.state == 'held'
        ((Map) copy([:], true).body.plan).kept == true
    }

    def 'D9: a destination that does not hold the root reads absent, kept true, and the plan stays write-free'() {
        when:
        final CopyService.Outcome dry = copy([:], true)

        then:
        ((Map) dry.body.plan).kept == true
        destinationState(dry) == [
            [cid: sel.toString(), role: 'root', name: 'wigshop', state: 'absent'],
            [cid: completion.toString(), role: 'run', name: 'one', state: 'absent'],
        ]
        !Files.exists(tmp.resolve('org'))
    }

    def 'D9: a release the destination logs before it logs the run itself still makes kept false'() {
        given: 'org holds a release of the run, but no run- entry for it'
        claimAtOrg(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))

        expect:
        destinationState(copy([:], true)).find { it.role == 'run' }.state == 'released'
        copy([:]).status == 409
    }

    def 'D9: an S3 destination is read with HEAD, GET and LIST only, and its release refuses the copy'() {
        given:
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])
        copy([to: 'bucket'], false, cfg)
        final S3BlockStore dest = new S3BlockStore(buckets['org-bucket'], 'cas/', 'bucket', true, tmp.resolve('t'))
        final Cid release = dest.putDagCbor(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        StoreLog.append(dest, StoreLogKind.CLAIM, release, now += 1000)
        final MemoryS3Ops ops = buckets['org-bucket']
        ops.calls.clear()

        when:
        final CopyService.Outcome dry = copy([to: 'bucket'], true, cfg)

        then:
        destinationState(dry).find { it.role == 'run' }.state == 'released'
        ((Map) dry.body.plan).kept == false
        ops.calls.findAll { !(it.startsWith('HEAD ') || it.startsWith('GET ') || it.startsWith('LIST ')) } == []
        ops.calls.any { it.startsWith('LIST cas/log/') }

        when:
        ops.calls.clear()
        final CopyService.Outcome refused = copy([to: 'bucket'], false, cfg)

        then:
        refused.status == 409
        refused.body.refused == 'not_kept'
        ops.calls.findAll { !(it.startsWith('HEAD ') || it.startsWith('GET ') || it.startsWith('LIST ')) } == []
    }

    def 'audit findings on a real copy say refused audit; not_kept wins when both apply'() {
        given: 'a second run whose RunManifest keeps an absolute path'
        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', params: [outdir: '/home/someone/results']))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's3'], out: Fixtures.leaf2('x', counts2, 9)]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(leaky, 'out', [[item, ['x']]]))
        final Cid two = logged(StoreLogKind.RUN, Fixtures.runCompletion(leaky, [coll]))
        final Cid twoSel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [coll])], []).toCbor())

        when:
        final CopyService.Outcome audit = copy([root: twoSel.toString()])
        claimAtOrg(Fixtures.claim(two, 'set', 'retain', 'lineage', []))
        final CopyService.Outcome both = copy([root: twoSel.toString()])

        then:
        audit.status == 409
        audit.body.refused == 'audit'
        both.status == 409
        both.body.refused == 'not_kept'
        ((List) ((Map) ((Map) both.body.plan).audit).findings).size() == 1
    }

    def 'a real copy registers as a Live Writer, waits out a sweep, then asks has again of what the plan counted present'() {
        given: 'org holds the copy, and lab gains a name the next copy carries'
        copy([:])
        final Cid renamed = logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop2', [nameSel]))
        Path orgRoot = tmp.resolve('org')

        and: 'a sweep holds org/sweep.lock, and takes counts1 (trashed, past its deadline) before it lets go'
        SweepLock sweep = new SweepLock(new LocalRetentionStorage(orgRoot), 'sweep-9', { -> System.currentTimeMillis() } as Closure<Long>)
        sweep.take()
        int slept = 0
        long registered = 0
        CasSession.liveSleeper = { long ms ->
            slept++
            registered = Files.list(orgRoot.resolve('live')).withCloseable { it.count() }
            Files.delete(org().blockPath(counts1))
            sweep.release()
        } as Closure<Void>

        when:
        final CopyService.Outcome o = copy([:])

        then: 'the copy waited, registered, and wrote counts1 again as well as the new name'
        slept == 1
        registered == 1
        o.status == 200
        org().has(counts1)
        org().has(renamed)
        o.body.copied == 2
        ((Map) o.body.plan).present == 12

        and: 'it deregistered when it finished'
        Files.list(orgRoot.resolve('live')).withCloseable { it.count() } == 0

        cleanup:
        CasSession.liveSleeper = { long ms -> Thread.sleep(ms) } as Closure<Void>
    }

    def 'a run whose RunCompletion the source lacks: its RunManifest travels and is audited, and the Transfer does not name the run'() {
        given: 'the cache knows the run, then lab loses its RunCompletion'
        copy([:], true)
        Files.delete(lab.blockPath(completion))

        when:
        final CopyService.Outcome o = copy([:])
        final List<StoreLogEntry> log = StoreLog.read(org())
        final Transfer t = Transfer.fromCbor((Map) DagCbor.decode(org().open(log[0].cid).bytes))

        then:
        o.status == 200
        ((Map) o.body.plan).missing_at_source == 1
        ((Map) ((Map) o.body.plan).audit).checked == 1
        org().has(manifest) && org().has(script)
        !org().has(completion)
        t.runs == []
        log.every { it.kind != StoreLogKind.RUN }
    }

    def 'the audit is the same at every depth, and checks each RunManifest once'() {
        given: 'a Selection of two items of a run whose RunManifest keeps an absolute path'
        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', params: [outdir: '/home/someone/results']))
        final Cid a = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's3'], out: Fixtures.leaf2('x', counts2, 9)]))
        final Cid b = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's4'], out: Fixtures.leaf2('y', bam1, 6)]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(leaky, 'out', [[a, ['x']], [b, ['y']]]))
        logged(StoreLogKind.RUN, Fixtures.runCompletion(leaky, [coll]))
        final Cid two = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(a, [coll]), Selection.item(b, [coll])], []).toCbor())

        when:
        final List<Map> audits = ['results', 'lineage', 'all'].collect { String d -> (Map) ((Map) copy([root: two.toString(), depth: d], true).body.plan).audit }

        then:
        audits.every { it == audits[0] }
        audits[0].checked == 1
        ((List<Map>) audits[0].findings).collect { [it.cid, it.where] } == [[leaky.toString(), 'params.outdir']]
    }

    def 'a RunManifest missing at the source is counted missing, not audited, and is no error'() {
        given: 'the cache knows the run, then lab loses its RunManifest'
        copy([:], true)
        Files.delete(lab.blockPath(manifest))

        when:
        final CopyService.Outcome dry = copy([:], true)

        then:
        dry.status == 200
        ((Map) dry.body.plan).missing_at_source == 1
        ((Map) dry.body.plan).audit == [checked: 0, findings: []]
    }

    def 'a RunManifest reached only through a via collection, with no indexed run, is audited too'() {
        given: 'a collection of a run that never completed, whose manifest keeps an absolute path'
        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'orphan', nf_run_hash: 'orphanhash', params: [outdir: '/home/someone/x']))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's9'], out: Fixtures.leaf2('x', counts2, 9)]))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(leaky, 'out', [[item, ['x']]]))
        final Cid orphanSel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [coll])], []).toCbor())

        when:
        final CopyService.Outcome dry = copy([root: orphanSel.toString()], true)
        final CopyService.Outcome refused = copy([root: orphanSel.toString()])

        then:
        dry.status == 200
        ((List) ((Map) dry.body.plan).runs).isEmpty()
        ((Map) ((Map) dry.body.plan).audit).checked == 1
        ((List<Map>) ((Map) ((Map) dry.body.plan).audit).findings).collect { [it.cid, it.kind, it.where] } ==
            [[leaky.toString(), 'RunManifest', 'params.outdir']]
        refused.status == 409
        !Files.exists(tmp.resolve('org'))
    }

    def 'a block that fails its put stops the copy before any entry or Transfer'() {
        given: 'the script in lab no longer hashes to its address (same length, other bytes)'
        lab.blockPath(script).toFile().setWritable(true)
        Files.write(lab.blockPath(script), 'workflow { x'.getBytes('UTF-8'))

        when:
        final CopyService.Outcome o = copy([:])

        then:
        o.status == 500
        ((List<Map>) o.body.failures)*.cid == [script.toString()]
        o.body.transfer_entry == null
        o.body.snapshot == null

        and: 'the content written before it stays, unannounced'
        o.body.copied > 0
        org().has(counts1) && org().has(f1)
        !org().has(completion)
        StoreLog.read(org()).isEmpty()
    }

    @Requires({ System.getProperty('user.name') != 'root' })
    def 'a local member whose put fails reports its class and an alias, never its absolute path'() {
        given: 'org/blocks is read-only, so creating a shard directory in it fails naming the path'
        Files.createDirectories(tmp.resolve('org').resolve('blocks'))
        tmp.resolve('org').resolve('blocks').toFile().setWritable(false)

        when:
        final CopyService.Outcome o = copy([:])
        final String error = ((List<Map>) o.body.failures)[0].error

        then:
        o.status == 500
        error =~ /^\w+Exception: /
        error.contains('org:/blocks/')
        !error.contains(tmp.toString())
        !(error =~ /(^|[^\w:\/.])\/[\w.-]+/)

        cleanup:
        tmp.resolve('org').resolve('blocks').toFile().setWritable(true)
    }

    def 'an S3 member whose put fails reports its class and an alias, never its bucket path or a local one'() {
        given:
        buckets['org-bucket'] = new MemoryS3Ops('org-bucket') {
            @Override
            robsyme.cas.s3.S3Written put(String key, robsyme.cas.s3.S3Body body, robsyme.cas.s3.S3PutOptions o) {
                if( key.startsWith('cas/blocks/') )
                    throw new IOException("PUT s3://org-bucket/${key} failed; spool ${tmp.resolve('t')}/s3-spool and ${tmp.resolve('cache/index.sqlite')}".toString())
                return super.put(key, body, o)
            }
        }
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])

        when:
        final CopyService.Outcome o = copy([to: 'bucket'], false, cfg)
        final String error = ((List<Map>) o.body.failures)[0].error

        then:
        o.status == 500
        error.startsWith('IOException: PUT bucket:/blocks/')
        error.contains('spool [local path]')
        !error.contains('s3://')
        !error.contains(tmp.toString())
    }

    def 'a snapshot failure is failed: <class>: <message> with the alias and no local path, in the body and on err'() {
        given:
        buckets['org-bucket'] = new MemoryS3Ops('org-bucket') {
            @Override
            robsyme.cas.s3.S3Written put(String key, robsyme.cas.s3.S3Body body, robsyme.cas.s3.S3PutOptions o) {
                if( key.startsWith('cas/index/') )
                    throw new IOException("PUT s3://org-bucket/${key} from ${tmp.resolve('cache/index.sqlite')} failed".toString())
                return super.put(key, body, o)
            }
        }
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])
        final ByteArrayOutputStream err = new ByteArrayOutputStream()

        when:
        final CopyService.Outcome o = new CopyService(clock).run(cfg, [root: "selection:${sel}".toString(), to: 'bucket'], false, new PrintStream(err, true))

        then:
        o.status == 200
        ((String) o.body.snapshot).startsWith('failed: ')
        ((String) o.body.snapshot).contains('IOException: PUT bucket:/index/')
        !((String) o.body.snapshot).contains(tmp.toString())
        !((String) o.body.snapshot).contains('s3://')
        !err.toString().contains(tmp.toString())
    }

    def 'a re-copy into an S3 member that changes nothing registers no Live Writer: no PUT and no DELETE'() {
        given:
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])
        copy([to: 'bucket'], false, cfg)
        final MemoryS3Ops ops = buckets['org-bucket']
        ops.calls.clear()

        when:
        final CopyService.Outcome again = copy([to: 'bucket'], false, cfg)

        then:
        again.status == 200
        again.body.copied == 0
        again.body.transfer_entry == null
        ops.calls.findAll { it.startsWith('PUT ') || it.startsWith('DELETE ') } == []
    }

    def 'a re-copy that transfers nothing but lacks one ordinary log entry still registers a Live Writer, and writes the entry'() {
        given:
        final Map cfg = config([bucket: [location: 's3://org-bucket/cas']])
        copy([to: 'bucket'], false, cfg)
        final MemoryS3Ops ops = buckets['org-bucket']
        final String entry = ops.objects.keySet().find { it.startsWith('cas/log/') && it.contains(sel.toString()) }
        ops.objects.remove(entry)
        ops.calls.clear()

        when:
        final CopyService.Outcome again = copy([to: 'bucket'], false, cfg)

        then:
        entry != null
        again.status == 200
        again.body.copied == 0
        again.body.transfer_entry != null
        ops.calls.any { it.startsWith('PUT cas/live/') }
        ops.calls.any { it.startsWith('DELETE cas/live/') }
        ops.objects.keySet().any { it.startsWith('cas/log/') && it.contains(sel.toString()) }
    }

    def 'roots: run:<cid>, a bare RunManifest, a bare Selection; and what is refused'() {
        expect:
        copy([root: "run:${completion}".toString()], true).body.root == [kind: 'run', cid: completion.toString()]
        copy([root: manifest.toString()], true).body.root == [kind: 'run', cid: completion.toString()]
        copy([root: sel.toString()], true).body.root == [kind: 'selection', cid: sel.toString()]

        and:
        copy([root: counts1.toString()], true).body.error == 'bad_root'
        copy([root: "selection:${completion}".toString()], true).body.error == 'bad_root'
        copy([root: 'selection:nope'], true).body.error == 'bad_root'
        copy([root: 'what'], true).body.error == 'bad_root'
        copy([root: "run:${counts1}".toString()], true).body.error == 'bad_root'
        copy([root: "selection:${Fixtures.contentCid('nowhere')}".toString()], true).body.error == 'root_not_found'
        copy([to: 'nowhere'], true).body.error == 'unknown_member'
        copy([from: 'nowhere'], true).body.error == 'unknown_member'
        copy([from: 'org'], true).body.error == 'same_member'
        copy([from: 'ro'], true).body.error == 'root_not_found'
        copy([depth: 'deep'], true).body.error == 'bad_depth'
        copy([claims: ['none', 'names']], true).body.error == 'bad_claims'
        copy([claims: ['retain']], true).body.error == 'bad_claims'
        copy([claims: []], true).body.error == 'bad_claims'
    }

    def 'run root at depth all: every output of the run, and pins carried when asked'() {
        when:
        final CopyService.Outcome o = copy([root: "run:${completion}".toString(), depth: 'all', claims: ['pins', 'names']], true)
        final Map plan = (Map) o.body.plan

        then:
        o.body.claims == ['names', 'pins']
        o.body.command.toString().contains('--depth all --claims names,pins')
        group(o.body, 'root').blocks == 1
        group(o.body, 'items').blocks == 3
        group(o.body, 'collections').blocks == 2
        ((List<Map>) plan.claims).find { it.cid == pinC1.toString() }.carried == true
        ((List<Map>) plan.runs)[0].lineage == 'join_key'
    }

    def 'claims none carries no Claim'() {
        when:
        final CopyService.Outcome o = copy([claims: ['none']], true)

        then:
        o.body.claims == []
        o.body.command.toString().contains('--claims none')
        group(o.body, 'claims').blocks == 0
        ((List<Map>) ((Map) o.body.plan).claims).every { !it.carried }
    }
}
