package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.Index
import robsyme.cas.core.LocalRetentionStorage
import robsyme.cas.core.RetentionFixture
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.SweepLock
import robsyme.cas.core.SweepPolicy
import robsyme.cas.core.TrashLedger
import spock.lang.TempDir
import spock.lang.Specification

/**
 * The `sweep`, `prune` and `untrash` verbs (milestone 6 task 9), driven the
 * way `CasCommandsTest` drives `snapshot` and `put`: through
 * `CasCommands.run` over a local store built with `RetentionFixture`.
 */
class RetentionCommandsTest extends Specification {

    @TempDir
    Path tempDir

    Path store
    Path indexPath
    RetentionFixture f
    Map theRun
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    def setup() {
        store = tempDir.resolve('store')
        indexPath = tempDir.resolve('cache/index.sqlite')
        f = new RetentionFixture(store)
        theRun = f.run('a', [[x: f.raw('x')]])
    }

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: store.toString()]],
                index: [path: indexPath.toString()],
            ],
        ]
    }

    private int run(String cmd, List<String> args = []) {
        return new CasCommands().run(cmd, args, config(), new PrintStream(out, true), new PrintStream(err, true))
    }

    /** Backdates every listed block of the writable member by 15 days -- past the default 14-day age floor. */
    private void ageAll() {
        final long then = System.currentTimeMillis() - 15 * SweepPolicy.DAY
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c ->
            Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(then))
        }
    }

    /** Rewrites every ledger with a deadline already past, under its new name (Task 7's helper). */
    private void expireLedgers() {
        final RetentionStorage s = f.store.retentionStorage()
        for( String name : s.listLedgers() ) {
            final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
            final TrashLedger past = new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)
            s.writeLedger(past.name, past.toJson())
            if( past.name != name )
                s.deleteLedger(name)
        }
    }

    private Index index() {
        final Index i = Index.open(indexPath)
        i.catchUp(f.store, StoreLog.of(f.store), 'lab')
        return i
    }

    private Set<Cid> ledgerBlocks() {
        final RetentionStorage s = f.store.retentionStorage()
        final Set<Cid> out = [] as Set
        for( String name : s.listLedgers() )
            out.addAll(TrashLedger.parse(name, s.readLedger(name)).blocks.keySet())
        return out
    }

    // --------------------------------------------------------------- sweep

    def 'sweep without --apply is a dry run: exit 0, a report, nothing written'() {
        when:
        final int code = run('sweep', [])

        then:
        code == 0
        out.toString().startsWith('dry run over lab')
        !Files.exists(store.resolve('sweep.lock'))
        !Files.exists(store.resolve('trash'))
    }

    def 'sweep --format json prints the report as one JSON object'() {
        when:
        run('sweep', ['--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        json.applied == false
        json.dead == 0
        ((Map) json.roots).runs == 1
    }

    /** A Store Log entry of a kind this build does not know, as a newer build would write it. */
    private void unknownEntry(String kind) {
        Files.createFile(store.resolve('log').resolve("${StoreLog.reverseTimestamp(++f.clock)}-${kind}-${theRun.completion}".toString()))
    }

    def 'a dry run beside an unknown kind exits 0 and names it, in text and in json'() {
        given:
        unknownEntry('frob')

        when:
        final int code = run('sweep', [])
        final String text = out.toString()
        out.reset()
        final int jsonCode = run('sweep', ['--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        code == 0 && jsonCode == 0
        text.contains('unknown    the Store Log of member lab holds entries of kind(s) frob that this build does not know')
        json.unknown_kinds == [lab: ['frob']]
        json.stopped == null
    }

    def 'sweep --apply beside an unknown kind exits 1, says to upgrade, and trashes nothing'() {
        given:
        f.claim(theRun.completion, 'set', 'retain', 'lineage')
        ageAll()
        unknownEntry('frob')

        when:
        final int code = run('sweep', ['--apply', 'true'])

        then:
        code == 1
        out.toString().contains('stopped    the Store Log of member lab holds entries of kind(s) frob that this build does not know, ' +
            'so a sweep could delete blocks they keep; upgrade nf-blocks')
        !Files.exists(store.resolve('trash')) || Files.list(store.resolve('trash')).count() == 0
    }

    def 'sweep --apply after a release writes a ledger and exits 0'() {
        given:
        f.claim(theRun.completion, 'set', 'retain', 'lineage')
        ageAll()

        expect:
        run('sweep', ['--apply', 'true']) == 0
        Files.list(store.resolve('trash')).count() == 1
    }

    def 'sweep --apply beside a fresh registration exits 1 naming it, and the report says applied: false'() {
        given:
        Files.createDirectories(store.resolve('live'))
        Files.write(store.resolve('live/s1'), '{"run_name":"busy_bee"}'.bytes)

        when:
        final int code = run('sweep', ['--apply', 'true', '--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        code == 1
        json.applied == false
        err.toString().contains('busy_bee') || out.toString().contains('busy_bee')
    }

    def 'an applied sweep that deletes a hidden run forgets it in the index and rewrites the snapshot'() {
        given: 'a deleted run, aged, swept once (ledgered), its ledger expired'
        f.claim(theRun.completion, 'delete', null, null)
        ageAll()
        run('sweep', ['--apply', 'true'])
        expireLedgers()

        when:
        run('sweep', ['--apply', 'true'])

        then:
        Index.open(indexPath).withCloseable { Index i -> i.countRows('run') } == 0
        out.toString().contains('wrote')
    }

    def 'an applied sweep that deletes blocks, with --format json, prints exactly one JSON object with a snapshot field'() {
        given: 'a deleted run, aged, swept once (ledgered), its ledger expired'
        f.claim(theRun.completion, 'delete', null, null)
        ageAll()
        run('sweep', ['--apply', 'true'])
        expireLedgers()
        out.reset()
        err.reset()

        when:
        run('sweep', ['--apply', 'true', '--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        json.applied == true
        ((List) json.deleted).size() > 0
        json.snapshot instanceof String
        ((String) json.snapshot).contains('wrote')
    }

    // --------------------------------------------------------------- prune

    def 'prune dry run lists decisions; --apply writes set retain Claims'() {
        given: 'runs s1 (older) and s2 (newer) of pipeline p'
        final Cid s1 = f.run('s1', [[a: f.raw('s1-a')]],
            [pipeline: 'p', status: 'succeeded', finishedAt: '2026-09-20T00:00:00.000Z']).completion as Cid
        final Cid s2 = f.run('s2', [[a: f.raw('s2-a')]],
            [pipeline: 'p', status: 'succeeded', finishedAt: '2026-09-25T00:00:00.000Z']).completion as Cid

        when:
        int code = run('prune', ['--keep-last', '1'])

        then:
        code == 0
        out.toString().readLines().find { it.startsWith('release') }.contains('s1')
        StoreLog.read(f.store).count { it.kind == StoreLogKind.CLAIM } == 0

        when:
        code = run('prune', ['--keep-last', '1', '--apply', 'true'])

        then:
        code == 0
        index().withCloseable { Index i -> i.claimState(s1).released }
        !index().withCloseable { Index i -> i.claimState(s2).released }
    }

    def 'sweep --budget 0 is a usage error, not a sweep without a cap'() {
        given:
        f.claim(theRun.completion, 'set', 'retain', 'lineage')
        ageAll()

        when:
        final int code = run('sweep', ['--apply', 'true', '--budget', budget])

        then:
        code == 2
        err.toString().contains('--budget')
        err.toString().contains('leave --budget out')
        !Files.exists(store.resolve('trash'))

        where:
        budget << ['0', '0 B']
    }

    def 'prune without a policy is a usage error'() {
        expect:
        run('prune', []) == 2
    }

    // ------------------------------------------------------------ untrash

    def 'untrash takes an address out of its ledger and says it will come back'() {
        given: 'a sweep has ledgered block x'
        f.claim(theRun.completion, 'set', 'retain', 'lineage')
        ageAll()
        run('sweep', ['--apply', 'true'])
        final Cid x = f.raw('x')

        when:
        final int code = run('untrash', [x.toString()])

        then:
        code == 0
        ledgerBlocks() == [] as Set
        err.toString().contains('trashed again by the next sweep')
    }

    def 'untrash refuses while another sweep holds the lock'() {
        given:
        new SweepLock(new LocalRetentionStorage(store), 'other', { -> System.currentTimeMillis() }).take()

        expect:
        run('untrash', ['--sweep', 'whatever']) == 1
    }

    private List<String> runsIn(Path sqlite) {
        return java.sql.DriverManager.getConnection("jdbc:sqlite:${sqlite}").withCloseable { c ->
            final java.sql.ResultSet rs = c.createStatement().executeQuery('SELECT completion_cid FROM run ORDER BY 1')
            final List<String> out = []
            while( rs.next() ) out << rs.getString(1)
            out
        }
    }

    def 'a sweep that deletes a run rewrites an existing snapshot, and the cache forgets the run through its own catch-up'() {
        given: 'runs a (deleted) and kept, a snapshot of both, a sweep that trashed a, its ledger expired'
        final Map kept = f.run('kept', [[k: f.raw('k')]])
        run('snapshot')
        f.claim(theRun.completion, 'delete', null, null)
        ageAll()
        run('sweep', ['--apply', 'true'])
        expireLedgers()
        out.reset()

        when:
        final int status = run('sweep', ['--apply', 'true', '--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        status == 0
        ((String) json.snapshot).startsWith('wrote')
        runsIn(store.resolve('index/v7.sqlite')) == [kept.completion.toString()]
        runsIn(indexPath) == [kept.completion.toString()]
        StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET }
    }

    def 'a sweep whose forget entry could not be written still rewrites the snapshot without the runs it deleted'() {
        given: 'runs a and kept in a snapshot; a\'s blocks deleted by a sweep that could not log a forget entry'
        final Map kept = f.run('kept', [[k: f.raw('k')]])
        run('snapshot')
        final List<Cid> gone = [(Cid) theRun.completion, (Cid) theRun.collection] + (List<Cid>) theRun.items
        gone.each { Cid c -> Files.delete(f.store.blockPath(c)) }
        final robsyme.cas.core.SweepReport r = new robsyme.cas.core.SweepReport(deleted: gone, unannounced: gone)
        final robsyme.cas.CasSession cas = new robsyme.cas.CasSession(robsyme.cas.CasConfig.fromSession(config()))

        when:
        final String line = RetentionCommands.forgetAndSnapshot(cas, r)

        then:
        line.startsWith('wrote')
        runsIn(store.resolve('index/v7.sqlite')) == [kept.completion.toString()]
        runsIn(indexPath) == [kept.completion.toString()]
    }
}
