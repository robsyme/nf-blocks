package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions

import spock.lang.Specification
import spock.lang.TempDir

class MarkTest extends Specification {

    @TempDir Path root
    RetentionFixture f

    def setup() { f = new RetentionFixture(root) }

    private Mark mark() { Mark.of(f.store, f.logs(), 4) }

    def 'straight after a run everything it wrote is live'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')], [d: f.dir([one: f.raw('one')])]])

        when:
        final Mark m = mark()
        final Set<Cid> all = f.store.listBlocks().withCloseable { it.toList() } as Set

        then:
        m.live.containsAll(all)
        m.roots.runs == 1
        m.roots.contentRoots == 1
        m.missingMetadata == []
    }

    def 'a released run keeps its metadata and loses its content, directories included'() {
        given:
        final Cid x = f.raw('x'); final Cid one = f.raw('one'); final Cid d = f.dir([one: one])
        final Map r = f.run('a', [[x: x], [d: d]])
        f.claim(r.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        !m.isLive(x) && !m.isLive(d) && !m.isLive(one)
        [r.completion, r.manifest, r.script, r.collection, *r.items].every { m.isLive((Cid) it) }
        m.roots.released == 1
    }

    def 'content shared with a kept run stays live'() {
        given:
        final Cid shared = f.raw('shared')
        final Map a = f.run('a', [[s: shared]])
        final Map b = f.run('b', [[s: shared, u: f.raw('only b')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        expect:
        mark().isLive(shared)
    }

    def 'a file shared inside two directories stays live when one run is released'() {
        given:
        final Cid two = f.raw('two')
        final Cid oneA = f.raw('one a'); final Cid oneB = f.raw('one b')
        final Map a = f.run('a', [[d: f.dir([one: oneA, two: two])]])
        final Map b = f.run('b', [[d: f.dir([one: oneB, two: two])]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        m.isLive(two) && m.isLive(oneA)
        !m.isLive(oneB)
    }

    def 'a pinned item in a released run keeps its content, the rest goes'() {
        given:
        final Cid keep = f.raw('keep'); final Cid lose = f.raw('lose')
        final Map r = f.run('a', [[k: keep], [l: lose]])
        final Cid pinnedItem = r.items.find { Cid i -> leavesOf(i).contains(keep) }
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Cid pin = f.claim(pinnedItem, 'add', 'pin', 'figure 3')

        when:
        final Mark m = mark()

        then:
        m.isLive(keep) && m.isLive(pin)
        !m.isLive(lose)
    }

    def 'a deleted run gives up metadata and content, and its delete Claim goes with it'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Cid del = f.claim(r.completion, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        !m.isLive(r.completion) && !m.isLive(r.manifest) && !m.isLive(del)
        m.roots.hidden == 1
    }

    def 'a pin beats delete: hidden but pinned keeps the whole closure'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, 'delete', null, null)
        f.claim(r.completion, 'add', 'pin', 'keep it')

        when:
        final Mark m = mark()

        then:
        m.isLive(r.completion) && m.isLive(r.manifest) && m.isLive(x)
        m.roots.hiddenButPinned == 1
    }

    def 'a conflicted retain or deletion keeps everything'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, verb, attribute, value)
        f.claim(r.completion, verb2, attribute, value2)

        expect:
        mark().isLive(x)

        where:
        verb     | attribute | value     | verb2    | value2
        'set'    | 'retain'  | 'lineage' | 'set'    | 'lineage'
        'delete' | null      | null      | 'delete' | null
    }

    def 'del retain superseding the release makes the content live again'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        final Cid release = f.claim(r.completion, 'set', 'retain', 'lineage')
        f.claim(r.completion, 'del', 'retain', null, [release])

        expect:
        mark().isLive(x)
    }

    def 'a Selection roots its items content and keeps its via collection as metadata only'() {
        given:
        final Cid picked = f.raw('picked'); final Cid other = f.raw('other')
        final Map r = f.run('a', [[p: picked], [o: other]])
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        f.selection(item, r.collection)
        f.claim(r.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        m.isLive(picked) && m.isLive(r.collection) && m.isLive(r.manifest)
        !m.isLive(other)
        m.roots.selections == 1
    }

    def 'a missing item refuses; a missing directory manifest is only reported'() {
        given:
        final Cid gone = f.dir([one: f.raw('one')])
        final Map r = f.run('a', [[g: gone]])
        f.store.blockPath(gone).toFile().delete()

        when:
        Mark m = mark()

        then:
        m.missingContent == [gone]
        m.missingMetadata == []

        when:
        f.store.blockPath((Cid) r.items[0]).toFile().delete()
        m = mark()

        then:
        m.missingMetadata.size() == 1
        m.missingMetadata[0].startsWith(r.items[0].toString())
    }

    def 'a log entry whose block is in no member is dangling, not a root'() {
        given:
        final Cid ghost = Cid.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')
        StoreLog.append(f.store, StoreLogKind.RUN, ghost, ++f.clock)

        when:
        final Mark m = mark()

        then:
        m.danglingEntries.size() == 1
        m.danglingEntries[0].endsWith("-run-${ghost}")
        m.missingMetadata == []
    }

    def 'extend adds what a new pin roots and never removes'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Mark m = mark()
        final List<StoreLogEntry> before = StoreLog.read(f.store)
        f.claim((Cid) r.items[0], 'add', 'pin', 'late')
        final List<StoreLogEntry> added = StoreLog.read(f.store).findAll { !(it in before) }

        expect:
        !m.isLive(x)

        when:
        m.extend([new MemberLog('lab', true, added)])

        then:
        m.isLive(x)
    }

    def 'a read-only member roots too, and its blocks are read through the composition'() {
        given: 'a second member holding a run whose content is in the writable member'
        final Cid x = f.raw('x')
        final RetentionFixture other = new RetentionFixture(root.resolve('other'))
        other.store.put(x, new ByteArrayInputStream('x'.bytes), 1L)
        other.run('b', [[x: x]])
        final CompositeStore both = new CompositeStore([f.store, new LocalBlockStore(root.resolve('other'), 'shared', false)])

        when:
        final Mark m = Mark.of(both, [f.logs()[0], new MemberLog('shared', false, StoreLog.read(other.store))], 4)

        then:
        m.isLive(x)
    }

    // ---- fix round 1 ----

    def 'the same delete Claim entry logged twice within one extend call still hides the run'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        f.claim(r.completion, 'delete', null, null)
        final MemberLog log = f.logs()[0]

        when:
        final Mark m = Mark.of(f.store, [log, log], 4)

        then:
        m.roots.hidden == 1
        !m.isLive((Cid) r.completion)
    }

    def 'a delete Claim held in the writable member on a run held only in a read-only member stays live'() {
        given:
        final Cid x = f.raw('x')
        final RetentionFixture other = new RetentionFixture(root.resolve('other'))
        other.store.put(x, new ByteArrayInputStream('x'.bytes), 1L)
        final Map r = other.run('b', [[x: x]])
        final CompositeStore both = new CompositeStore([f.store, new LocalBlockStore(root.resolve('other'), 'shared', false)])
        final Cid del = f.claim((Cid) r.completion, 'delete', null, null)

        when:
        final Mark m = Mark.of(both, [f.logs()[0], new MemberLog('shared', false, StoreLog.read(other.store))], 4)

        then:
        m.isLive(del)
        !m.isLive((Cid) r.completion)
    }

    def 'a Claim block that will not parse is kept live, unexpanded, and listed as unreadable'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Map bad = Records.head(Records.CLAIM)
        bad.put('asserted_by', 'test')
        bad.put('subject', r.completion)
        bad.put('verb', 'nope')
        bad.put('attribute', null)
        bad.put('value', null)
        bad.put('supersedes', [])
        bad.put('timestamp', Index.isoMillis(++f.clock))
        final Cid badClaim = f.store.putDagCbor(bad)
        StoreLog.append(f.store, StoreLogKind.CLAIM, badClaim, ++f.clock)

        when:
        final Mark m = mark()

        then:
        m.isLive(badClaim)
        m.unreadableClaims == [badClaim]
    }

    def 'a corrupt dag-cbor block reachable from a run makes Mark.of throw, not read as missing'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Path itemPath = f.store.blockPath((Cid) r.items[0])
        Files.setPosixFilePermissions(itemPath, PosixFilePermissions.fromString('rw-r--r--'))
        Files.write(itemPath, [0x81] as byte[])

        when:
        mark()

        then:
        thrown(IllegalArgumentException)
    }

    def 'a root block that decodes to a non-map is live and unexpanded, not dangling or missing'() {
        given:
        final Cid weird = f.store.putDagCbor(['just', 'a', 'list'])
        StoreLog.append(f.store, StoreLogKind.RUN, weird, ++f.clock)

        when:
        final Mark m = mark()

        then:
        m.isLive(weird)
        m.danglingEntries == []
        m.missingMetadata == []
    }

    def 'a pinned collection in a released run keeps its items content and its RunManifest'() {
        given:
        final Cid keep = f.raw('keep')
        final Map r = f.run('a', [[k: keep]])
        f.claim(r.completion, 'set', 'retain', 'lineage')
        f.claim((Cid) r.collection, 'add', 'pin', 'figure 1')

        when:
        final Mark m = mark()

        then:
        m.isLive(keep)
        m.isLive((Cid) r.manifest)
        m.roots.pinnedSubjects == 1
    }

    def 'a nested Selection roots its inner Selections items content'() {
        given:
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim(r.completion, 'delete', null, null)
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        final Cid inner = f.selectionBlock(item, (Cid) r.collection)
        final Cid outer = f.nestedSelection(inner)

        when:
        final Mark m = mark()

        then:
        !m.isLive((Cid) r.completion)
        m.isLive(inner)
        m.isLive(picked)
        m.roots.selections == 1
    }

    def 'a Selection with a clean delete is not a root, and a pin overrides the delete'() {
        given:
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim(r.completion, 'delete', null, null)
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        final Cid sel = f.selection(item, (Cid) r.collection)
        f.claim(sel, 'delete', null, null)

        when:
        final Mark m1 = mark()

        then:
        !m1.isLive(sel)
        !m1.isLive(picked)

        when:
        f.claim(sel, 'add', 'pin', 'keep')
        final Mark m2 = mark()

        then:
        m2.isLive(sel)
        m2.isLive(picked)
    }

    def 'a pinned Selection with a clean delete keeps its whole closure; a del pin superseding the pin frees it'() {
        given: 'the run that wrote the picked file is released, so only the Selection can keep it'
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        final Cid sel = f.selection(item, (Cid) r.collection)
        final Cid pin = f.claim(sel, 'add', 'pin', 'figure 3')
        f.claim(sel, 'delete', null, null)

        when:
        final Mark m1 = mark()

        then:
        m1.isLive(sel) && m1.isLive(item) && m1.isLive(picked)
        m1.roots.deletedSelections == 0
        m1.roots.selections == 1

        when:
        f.claim(sel, 'del', 'pin', null, [pin])
        final Mark m2 = mark()

        then:
        !m2.isLive(sel) && !m2.isLive(picked)
        m2.isLive(item)
        m2.roots.deletedSelections == 1
    }

    def 'extend with a later del retain superseding the release makes the run content live again'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        final Cid release = f.claim(r.completion, 'set', 'retain', 'lineage')
        final Mark m = mark()
        final List<StoreLogEntry> before = StoreLog.read(f.store)

        expect:
        !m.isLive(x)

        when:
        f.claim(r.completion, 'del', 'retain', null, [release])
        final List<StoreLogEntry> added = StoreLog.read(f.store).findAll { !(it in before) }
        m.extend([new MemberLog('lab', true, added)])

        then:
        m.isLive(x)
    }

    def 'a level wider than one window is still fully processed across multiple windows'() {
        given: 'threads=1 makes the window 8 wide, and this run has 10 items in one collection level'
        final List<Cid> contents = (1..10).collect { int i -> f.raw("content ${i}") }
        final Map r = f.run('a', contents.collect { Cid c -> [f: c] })

        when:
        final Mark m = Mark.of(f.store, f.logs(), 1)

        then:
        contents.every { Cid c -> m.isLive(c) }
        ((List<Cid>) r.items).every { Cid i -> m.isLive(i) }
    }

    def 'a Claim whose supersedes is not a list is unreadable, kept live and listed, and the mark goes on'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Map bad = new Claim('test', (Cid) r.completion, 'delete', null, null, [], Index.isoMillis(++f.clock)).toCbor()
        bad.put('supersedes', 'not a list')
        final Cid badClaim = f.store.putDagCbor(bad)
        StoreLog.append(f.store, StoreLogKind.CLAIM, badClaim, ++f.clock)

        when:
        final Mark m = mark()

        then:
        m.isLive(badClaim)
        m.unreadableClaims == [badClaim]
        m.isLive((Cid) r.completion)
        m.roots.contentRoots == 1
    }

    def "a RunCompletion's input_set link is metadata: kept for a run, and for a released one"() {
        given:
        final Cid inputSet = f.store.putDagCbor([note: 'an input set'])
        final Map r = f.run('a', [[x: f.raw('x')]], [inputSet: inputSet])
        final Cid otherInputs = f.store.putDagCbor([note: 'another input set'])
        final Map b = f.run('b', [[y: f.raw('y')]], [inputSet: otherInputs])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        m.isLive(inputSet)
        m.isLive(otherInputs)
        m.missingMetadata == []
    }


    def 'a forget entry roots its block; an entry whose block is gone is dangling'() {
        given:
        final Cid kept = f.store.putDagCbor(new Forget('test', [Fixtures.cidOf([kind: 'X'])]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, kept, ++f.clock)
        final Cid lost = Fixtures.cidOf(new Forget('test', [Fixtures.cidOf([kind: 'Y'])]).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, lost, ++f.clock)

        when:
        final Mark m = Mark.of(f.store, f.logs(), 4)

        then:
        m.isLive(kept)
        !m.isLive(lost)
        m.danglingEntries.any { it.contains(lost.toString()) }
    }

    def 'a transfer entry roots its own block and not what it names; one whose block is gone is dangling'() {
        given: 'a run held only by a Transfer that names it as root and run (its own run entry is not in the log)'
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        final LocalBlockStore store = (LocalBlockStore) f.store
        Files.list(root.resolve('log')).withCloseable { it.toList() }.each { Files.delete(it) }
        final Transfer t = new Transfer('rob@lab', [(Cid) r.completion], 'roblab', 'lab', 'all', [], [(Cid) r.completion],
            5L, 5L, 100L, '2026-10-24T10:00:00.000Z')
        final Cid kept = store.putDagCbor(t.toCbor())
        StoreLog.append(store, StoreLogKind.TRANSFER, kept, ++f.clock)
        final Cid lost = Fixtures.cidOf(new Transfer('rob@lab', [(Cid) r.collection], 'roblab', 'lab', 'all', [], [],
            1L, 1L, 1L, '2026-10-24T11:00:00.000Z').toCbor())
        StoreLog.append(store, StoreLogKind.TRANSFER, lost, ++f.clock)

        when:
        final Mark m = Mark.of(f.store, f.logs(), 4)

        then:
        m.isLive(kept)
        m.live == [kept] as Set
        m.roots.runs == 0
        m.missingMetadata == []
        m.danglingEntries.size() == 1
        m.danglingEntries[0].contains(lost.toString())
        m.danglingEntries[0].contains('-transfer-')
    }

    // ---- P4: Project and Handoff roots (R5, R6), InputSet (R1) ----

    def 'a project entry roots its Project block alone; a clean delete frees it and a del superseding it restores it'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        f.claim((Cid) r.completion, 'delete', null, null)
        final Cid p = f.project('rnaseq study')

        when:
        final Mark m1 = mark()

        then:
        m1.live == [p] as Set
        m1.roots.projects == 1
        m1.roots.deletedProjects == 0

        when:
        final Cid del = f.claim(p, 'delete', null, null)
        final Mark m2 = mark()

        then:
        !m2.isLive(p)
        !m2.isLive(del)
        m2.roots.projects == 0
        m2.roots.deletedProjects == 1

        when:
        final Cid undo = f.claim(p, 'del', null, null, [del])
        final Mark m3 = mark()

        then:
        m3.isLive(p)
        m3.isLive(del) && m3.isLive(undo)
        m3.roots.projects == 1
    }

    def 'a conflicted deletion keeps a Project, and a pin beats its delete'() {
        given:
        final Cid p = f.project('study')
        f.claim(p, 'delete', null, null)
        f.claim(p, second, null, null)

        expect:
        mark().isLive(p)

        where:
        second << ['delete', 'del']
    }

    def 'a pinned Project with a clean delete is kept'() {
        given:
        final Cid p = f.project('study')
        f.claim(p, 'delete', null, null)
        f.claim(p, 'add', 'pin', 'keep')

        expect:
        mark().isLive(p)
    }

    def 'a handoff entry roots its files as content and its Selection whole, never its Project'() {
        given: 'a deleted run whose one picked item a Selection holds; the Project is deleted'
        final Cid picked = f.raw('picked'); final Cid other = f.raw('other')
        final Map r = f.run('a', [[p: picked], [o: other]])
        f.claim((Cid) r.completion, 'delete', null, null)
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        final Cid sel = f.selectionBlock(item, (Cid) r.collection)
        final Cid project = f.project('study')
        f.claim(project, 'delete', null, null)
        final Cid sheet = f.raw('sample,fastq_1\nA,cas://x/a.fq.gz\n')
        final Cid contrasts = f.dir([one: f.raw('contrast one')])
        final Cid h = f.handoff(sel, project, ['samplesheet.csv': sheet, contrasts: contrasts])

        when:
        final Mark m = mark()

        then:
        m.isLive(h)
        m.isLive(sheet) && m.isLive(contrasts)
        m.isLive(sel) && m.isLive(item) && m.isLive(picked)
        m.isLive((Cid) r.collection) && m.isLive((Cid) r.manifest)
        !m.isLive(other)
        !m.isLive(project)
        !m.isLive((Cid) r.completion)
        m.roots.handoffs == 1
        m.missingMetadata == [] && m.missingContent == []
    }

    def 'a Handoff with a clean delete is not a root; a del superseding it or a pin keeps it'() {
        given:
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim((Cid) r.completion, 'delete', null, null)
        final Cid sel = f.selectionBlock((Cid) r.items[0], (Cid) r.collection)
        final Cid sheet = f.raw('sheet')
        final Cid h = f.handoff(sel, f.project('study'), ['samplesheet.csv': sheet])
        final Cid del = f.claim(h, 'delete', null, null)

        when:
        final Mark m1 = mark()

        then:
        !m1.isLive(h) && !m1.isLive(sheet) && !m1.isLive(sel) && !m1.isLive(picked)
        m1.roots.deletedHandoffs == 1
        m1.roots.handoffs == 0

        when:
        f.claim(h, 'del', null, null, [del])
        final Mark m2 = mark()

        then:
        m2.isLive(h) && m2.isLive(sheet) && m2.isLive(picked)
        m2.roots.handoffs == 1
    }

    def 'a pinned Handoff with a clean delete keeps its files and its Selection'() {
        given:
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim((Cid) r.completion, 'delete', null, null)
        final Cid sel = f.selectionBlock((Cid) r.items[0], (Cid) r.collection)
        final Cid sheet = f.raw('sheet')
        final Cid h = f.handoff(sel, null, ['samplesheet.csv': sheet])
        f.claim(h, 'delete', null, null)
        f.claim(h, 'add', 'pin', 'cited in the paper')

        when:
        final Mark m = mark()

        then:
        m.isLive(h) && m.isLive(sheet) && m.isLive(sel) && m.isLive(picked)
    }

    def 'a missing hand-off file is missing content, as an item leaf is; a missing handed-off Selection is missing metadata'() {
        given: 'a raw file is live without a read, as an item leaf is; a Directory Manifest file is read'
        final Map r = f.run('a', [[p: f.raw('picked')]])
        final Cid sel = f.selectionBlock((Cid) r.items[0], (Cid) r.collection)
        final Cid sheet = f.raw('sheet')
        final Cid contrasts = f.dir([one: f.raw('contrast one')])
        final Cid h = f.handoff(sel, null, ['samplesheet.csv': sheet, contrasts: contrasts])
        f.store.blockPath(contrasts).toFile().delete()

        when:
        Mark m = mark()

        then:
        m.missingContent == [contrasts]
        m.missingMetadata == []

        when:
        f.store.blockPath(sel).toFile().delete()
        m = mark()

        then:
        m.missingMetadata.size() == 1
        m.missingMetadata[0] == "${sel} (needed by ${h})".toString()
    }

    def "a run's InputSet is kept as itself only: the inputs it names are not kept by it"() {
        given: 'a raw block the store holds, read by run a, and another read by a and written by b'
        final Cid input = f.raw('reads.fastq.gz')
        final Cid shared = f.raw('reference.fa')
        f.run('b', [[ref: shared]])
        final Cid inputs = f.inputSet([input, shared])
        final Map r = f.run('a', [[x: f.raw('x')]], [inputSet: inputs])
        if( verb )
            f.claim((Cid) r.completion, verb, attribute, value)

        when:
        final Mark m = mark()

        then:
        m.isLive(inputs) == inputSetLive
        !m.isLive(input)
        m.isLive(shared)
        m.missingContent == [] && m.missingMetadata == []

        where:
        verb     | attribute | value     || inputSetLive
        null     | null      | null      || true
        'set'    | 'retain'  | 'lineage' || true
        'delete' | null      | null      || false
    }

    def "an InputSet's absent input is not missing content: its addresses are never followed"() {
        given:
        final Cid absent = Fixtures.cidOf([kind: 'never stored'])
        final Cid raw = Cid.of(Cid.RAW, Hashing.sha256('never stored either'.getBytes('UTF-8')))
        final Map r = f.run('a', [[x: f.raw('x')]], [inputSet: f.inputSet([absent, raw])])

        when:
        final Mark m = mark()

        then:
        m.missingContent == []
        m.missingMetadata == []
    }

    def 'a run keeps its ItemLineage as itself and follows nothing under it'() {
        given: 'an InputSet reading an upstream item no run here holds, and an ItemLineage over it'
        final Cid upstreamItem = f.store.putDagCbor(OutputItem.of([x: Leaf.of('x', f.raw('upstream'), 1L)]).toCbor())
        final Cid inputs = f.store.putDagCbor(new InputSet('test', [
            new InputSet.Input(InputSet.TASK, 'in', f.raw('staged'), 1L, null, null)
                .withFrom([new InputSet.Origin(upstreamItem, null, null)])]).toCbor())
        final Map made = f.run('a', [[x: f.raw('x')]], [inputSet: inputs])
        final Cid lineage = f.store.putDagCbor(new ItemLineage('test', inputs,
            [ItemLineage.Derived.of((Cid) made.items[0], [0], [])]).toCbor())
        final Map r = f.run('b', [[y: f.raw('y')]], [inputSet: inputs, itemLineage: lineage])

        when:
        final Mark m = mark()

        then:
        m.isLive(lineage)
        !m.isLive(upstreamItem)
        m.missingContent == [] && m.missingMetadata == []
    }

    private Set<Cid> leavesOf(Cid item) {
        return OutputItem.fromCbor((Map) DagCbor.decode(f.store.open(item).readAllBytes())).leaves()*.address as Set
    }
}
