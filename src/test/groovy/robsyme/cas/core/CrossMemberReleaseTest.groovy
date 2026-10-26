package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * P3 decision D2 (plan section 1.8): a sweep of member A releases or deletes
 * a run or Selection A's Store Log lists only when both A's own Claims
 * (those A's Store Log lists) and the whole composition's Claims say so; one
 * logged only in other members is judged by the composition alone. A pin in
 * any member keeps. Two local members: A ('lab') is swept, B ('shared') is
 * read-only in the composition and logs its own Claims.
 */
class CrossMemberReleaseTest extends Specification {

    @TempDir Path root
    RetentionFixture a
    RetentionFixture b
    CompositeStore both
    final Closure<Void> say = { String s -> null } as Closure<Void>
    final Closure<Void> sleeper = { long ms -> null } as Closure<Void>

    def setup() {
        a = new RetentionFixture(root.resolve('a'))
        b = new RetentionFixture(root.resolve('b'))
        both = new CompositeStore([a.store, new LocalBlockStore(root.resolve('b'), 'shared', false)])
    }

    private List<MemberLog> logs() {
        return [new MemberLog('lab', true, StoreLog.read(a.store)), new MemberLog('shared', false, StoreLog.read(b.store))]
    }

    private Mark mark() { Mark.of(both, logs(), 4) }

    /** Puts a block A holds into B too, as a copy would, and logs it in B. */
    private void logInB(Cid cid, StoreLogKind kind) {
        final byte[] bytes = a.store.open(cid).withCloseable { it.readAllBytes() }
        b.store.put(cid, new ByteArrayInputStream(bytes), bytes.length)
        StoreLog.append(b.store, kind, cid, ++b.clock)
    }

    /** A Claim written to and logged in B only. */
    private Cid claimInB(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = []) {
        return b.claim(subject, verb, attribute, value, supersedes)
    }

    private void ageA() {
        final long then = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        a.store.listBlocks().withCloseable { it.toList() }.each { Cid c ->
            Files.setLastModifiedTime(a.store.blockPath(c), FileTime.fromMillis(then))
        }
    }

    private SweepReport sweepA() {
        ageA()
        return new Sweep(both, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)
    }

    private Set<Cid> trashedInA() {
        final RetentionStorage s = a.store.retentionStorage()
        return s.listLedgers().collectMany { String n -> TrashLedger.parse(n, s.readLedger(n)).blocks.keySet() } as Set<Cid>
    }

    private static List<Map> rows(Path db, String sql, List params = []) {
        def c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            def s = c.prepareStatement(sql)
            params.eachWithIndex { Object p, int i -> s.setObject(i + 1, p) }
            def rs = s.executeQuery(); def md = rs.metaData; def out = []
            while( rs.next() ) out << (1..md.columnCount).collectEntries { int i -> [(md.getColumnLabel(i)): rs.getObject(i)] }
            out
        } finally { c.close() }
    }

    def 'a run held in A and released only in B is kept by a sweep of A, and A\'s figures call it held'() {
        given:
        a.trueSizes = true
        final Cid x = a.raw('only this run wrote x ' * 20)
        final Map r = a.run('r', [[x: x]])
        claimInB((Cid) r.completion, 'set', 'retain', 'lineage')

        when: "A's snapshot, built by a session whose cache holds both members"
        final Index index = Index.open(root.resolve('cache.sqlite'))
        index.catchUp(a.store, StoreLog.of(a.store), 'lab')
        index.catchUp(both.members[1], StoreLog.of(both.members[1]), 'shared')
        def w = IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(root.resolve('out')), 0L, null, root.resolve('t'), null, false, 1000L,
            { Cid c -> a.store.has(c) } as Closure<Boolean>, a.store.retentionStorage())
        index.close()
        final Path snap = IndexSnapshot.pathIn(root.resolve('out'))

        then:
        w.written
        rows(snap, "SELECT state FROM root WHERE kind = 'run' AND cid = ?", [r.completion.toString()])*.state == ['held']
        rows(snap, 'SELECT releasing_bytes FROM store_stats')[0].releasing_bytes == 0

        when:
        final SweepReport s = sweepA()

        then:
        s.applied
        s.trashed == 0
        !trashedInA().contains(x)
    }

    def "B's pin on a run released in A keeps its content"() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        claimInB((Cid) r.completion, 'add', 'pin', 'keep this')

        expect:
        mark().isLive(x)
        sweepA().trashed == 0
    }

    def "a del retain logged in B that supersedes A's release keeps the content"() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        final Cid release = a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        logInB(release, StoreLogKind.CLAIM)
        claimInB((Cid) r.completion, 'del', 'retain', null, [release])

        expect:
        mark().isLive(x)
        sweepA().trashed == 0
    }

    def 'a run released in A, with no Claims in B, is still swept'() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        b.run('other', [[y: b.raw('y')]])

        when:
        final Mark m = mark()
        final SweepReport s = sweepA()

        then:
        !m.isLive(x)
        m.roots.released == 1
        s.trashed == 1
        trashedInA() == [x] as Set
    }

    def 'a delete logged only in B leaves the run rooted in A'() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        claimInB((Cid) r.completion, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        m.roots.hidden == 0
        m.roots.contentRoots == 1
        m.isLive((Cid) r.completion)
        m.isLive(x)
        sweepA().trashed == 0
    }

    def "a run logged only in B and released only in B lets A's sweep trash A's copy of its content"() {
        given:
        final Cid y = b.raw('y')
        a.store.put(y, new ByteArrayInputStream('y'.bytes), 1L)
        final Map r = b.run('theirs', [[y: y]])
        claimInB((Cid) r.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then: 'judged by the composition alone, as before P3'
        m.roots.released == 1
        m.roots.contentRoots == 0
        !m.isLive(y)
        sweepA().trashed == 1
        trashedInA() == [y] as Set
    }

    def "a run logged only in B and deleted only in B is hidden for A's sweep"() {
        given:
        final Cid y = b.raw('y')
        a.store.put(y, new ByteArrayInputStream('y'.bytes), 1L)
        final Map r = b.run('theirs', [[y: y]])
        claimInB((Cid) r.completion, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        m.roots.hidden == 1
        !m.isLive((Cid) r.completion)
        !m.isLive(y)
        sweepA().trashed == 1
        trashedInA() == [y] as Set
    }

    def "extend re-roots a run first seen in B's log when A's log lists it too"() {
        given: 'a run logged only in B and released there, so its content is dead for A'
        final Cid y = b.raw('y')
        a.store.put(y, new ByteArrayInputStream('y'.bytes), 1L)
        final Map r = b.run('theirs', [[y: y]])
        claimInB((Cid) r.completion, 'set', 'retain', 'lineage')
        final Mark m = mark()

        expect:
        !m.isLive(y)

        when: "a copy logs the run in A, without B's release"
        StoreLog.append(a.store, StoreLogKind.RUN, (Cid) r.completion, ++a.clock)
        m.extend([new MemberLog('lab', true, StoreLog.read(a.store)), new MemberLog('shared', false, [])])

        then: "A's own Claims do not release it, so it holds its content"
        m.isLive(y)
    }

    def 'a delete logged in both members hides the run, as today'() {
        given:
        final Map r = a.run('r', [[x: a.raw('x')]])
        final Cid del = a.claim((Cid) r.completion, 'delete', null, null)
        logInB(del, StoreLogKind.CLAIM)

        when:
        final Mark m = mark()

        then:
        m.roots.hidden == 1
        !m.isLive((Cid) r.completion)
    }

    def 'a Selection in A deleted only in B stays a root'() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        final Cid sel = a.selection((Cid) r.items[0], (Cid) r.collection)
        claimInB(sel, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        m.roots.deletedSelections == 0
        m.isLive(sel)
        m.isLive(x)
    }

    def "a Selection in A deleted in both members and pinned in B's log stays a root, though a del pin in A supersedes the pin"() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        final Cid sel = a.selection((Cid) r.items[0], (Cid) r.collection)
        logInB(a.claim(sel, 'delete', null, null), StoreLogKind.CLAIM)
        final Cid pin = claimInB(sel, 'add', 'pin', 'theirs')
        a.claim(sel, 'del', 'pin', null, [pin])

        when:
        final Mark m = mark()

        then: 'the composition has no current pin and A logs no add pin, but B still pins'
        m.roots.deletedSelections == 0
        m.isLive(sel)
        m.isLive(x)
    }

    def 'a Project or Handoff in A deleted only in B stays a root; one logged only in B goes by the composition'() {
        given:
        final Cid project = a.project('study')
        claimInB(project, 'delete', null, null)
        final Map r = a.run('r', [[x: a.raw('x')]])
        final Cid sel = a.selectionBlock((Cid) r.items[0], (Cid) r.collection)
        final Cid sheet = a.raw('sheet')
        final Cid h = a.handoff(sel, project, ['samplesheet.csv': sheet])
        claimInB(h, 'delete', null, null)
        final Cid elsewhere = b.project('only in B')
        b.claim(elsewhere, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        m.isLive(project) && m.isLive(h) && m.isLive(sheet)
        m.roots.deletedProjects == 1
        m.roots.deletedHandoffs == 0
        !m.isLive(elsewhere)
    }

    def "A's pin superseded by a del pin in B still keeps: a pin in A's own log pins"() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        final Cid pin = a.claim((Cid) r.completion, 'add', 'pin', 'mine')
        logInB(pin, StoreLogKind.CLAIM)
        claimInB((Cid) r.completion, 'del', 'pin', null, [pin])

        expect:
        mark().isLive(x)
    }

    def "B's pin superseded by a del pin logged only in A still keeps: a pin in any member's own log pins"() {
        given:
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        final Cid pin = claimInB((Cid) r.completion, 'add', 'pin', 'theirs')
        a.claim((Cid) r.completion, 'del', 'pin', null, [pin])

        expect: 'the composition has no current pin, A logs no add pin, and B still pins'
        mark().isLive(x)
    }

    def 'extend re-roots a subject when a Claim already read from B appears in A\'s log'() {
        given: "A releases; B releases twice over A's, so the composition reads one clean release"
        final Cid x = a.raw('x')
        final Map r = a.run('r', [[x: x]])
        final Cid r1 = a.claim((Cid) r.completion, 'set', 'retain', 'lineage')
        logInB(r1, StoreLogKind.CLAIM)
        final Cid r2 = claimInB((Cid) r.completion, 'set', 'retain', 'lineage', [r1])
        final Cid r3 = claimInB((Cid) r.completion, 'set', 'retain', 'lineage', [r2])
        final Mark m = mark()
        final List<StoreLogEntry> before = StoreLog.read(a.store)

        expect: 'both views say released'
        !m.isLive(x)

        when: "A's log now lists r3 but not r2, so A's own retain group is r1 and r3, a conflict"
        StoreLog.append(a.store, StoreLogKind.CLAIM, r3, ++a.clock)
        m.extend([new MemberLog('lab', true, StoreLog.read(a.store).findAll { !(it in before) }), new MemberLog('shared', false, [])])

        then:
        m.isLive(x)
    }
}
