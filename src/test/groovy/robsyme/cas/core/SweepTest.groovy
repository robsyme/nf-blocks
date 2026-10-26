package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime

import spock.lang.Specification
import spock.lang.TempDir

class SweepTest extends Specification {

    @TempDir Path root
    RetentionFixture f
    final List<String> said = []
    final List<Long> slept = []
    Closure<Void> say = { String s -> said << s; null } as Closure<Void>
    Closure<Void> sleeper = { long ms -> slept << ms; null } as Closure<Void>

    def setup() { f = new RetentionFixture(root) }

    private Sweep sweep() { new Sweep(f.store, SweepPolicy.defaults()) }

    /** Backdates every listed block of the writable member by `millis`. */
    private void ageAll(long millis) {
        final long then = System.currentTimeMillis() - millis
        f.store.listBlocks().withCloseable { it.toList() }.each { Cid c ->
            Files.setLastModifiedTime(f.store.blockPath(c), FileTime.fromMillis(then))
        }
    }

    /** Rewrites every ledger with a deadline already past, under its new name. */
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

    private static Set<String> listing(Path dir) {
        return Files.walk(dir).withCloseable { it.toList() }.collect { Path p ->
            "${dir.relativize(p)} ${Files.isRegularFile(p) ? Files.size(p) : -1} ${Files.getLastModifiedTime(p).toMillis()}".toString()
        } as Set
    }

    def 'a dry run straight after a run reports an empty dead set and writes nothing'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        ageAll(15 * SweepPolicy.DAY)
        final Set<String> before = listing(root)

        when:
        final SweepReport r = new Sweep(f.store, SweepPolicy.defaults()).dryRun()

        then:
        r.dead == 0
        !r.applied
        listing(root) == before
        r.toText().startsWith('dry run over lab (')
    }

    def 'a dry run over released content writes nothing either'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only b')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        f.store.retentionStorage().putLive('old', '{}'.bytes)
        Files.setLastModifiedTime(root.resolve('live/old'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        Files.write(root.resolve('blocks/.tmp-old'), 'x'.bytes)
        Files.setLastModifiedTime(root.resolve('blocks/.tmp-old'), FileTime.fromMillis(System.currentTimeMillis() - 15 * SweepPolicy.DAY))
        ageAll(15 * SweepPolicy.DAY)
        final Set<String> before = listing(root)

        when:
        final SweepReport r = sweep().dryRun()

        then:
        r.dead == 1
        r.trashed == 1
        r.scratch == 1
        r.staleRegistrations == 1
        listing(root) == before
        r.toText().contains('would be trashed')
    }

    def 'apply after a release ledgers exactly the unshared content; blocks stay readable'() {
        given:
        final Cid shared = f.raw('shared')
        final Cid only = f.raw('only b')
        f.run('a', [[s: shared]])
        final Map b = f.run('b', [[s: shared, u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final List<String> ledgers = f.store.retentionStorage().listLedgers()

        then:
        r.applied
        r.trashed == 1
        ledgers.size() == 1
        r.ledger == ledgers[0]
        TrashLedger.parse(ledgers[0], f.store.retentionStorage().readLedger(ledgers[0])).blocks.keySet() == [only] as Set
        f.store.has(only)
        SweepLock.holder(f.store.retentionStorage()) == null
        r.toText().contains("ledger     trash/${ledgers[0]}")
    }

    def 'restored content is dropped from the ledger, not deleted'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        final Cid release = f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        f.claim(b.completion, 'del', 'retain', null, [release])
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.rescued == 1
        r.deleted == []
        f.store.has(only)
        f.store.retentionStorage().listLedgers() == []
    }

    def 'a later sweep past the deadline deletes what is still dead, and its log entries'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        final Map gone = f.run('gone', [[g: f.raw('gone')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !f.store.has(only)
        !f.store.has((Cid) gone.completion)
        f.store.has((Cid) b.completion)
        r.deleted.contains(gone.completion)
        !StoreLog.read(f.store).any { it.cid == gone.completion }
        f.store.retentionStorage().listLedgers() == []
        r.applied
    }

    def 'a ledger whose deadline has not passed deletes nothing'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.waiting == 1
        r.deleted == []
        r.trashed == 0
        f.store.has(only)
        f.store.retentionStorage().listLedgers().size() == 1
    }

    def 'nothing younger than the age floor is trashed'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.young == 1
        r.trashed == 0
        f.store.retentionStorage().listLedgers() == []
    }

    def 'a fresh registration refuses the sweep; a stale one is ignored and deleted'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        f.store.retentionStorage().putLive('running', '{"run_name":"busy_bee"}'.bytes)

        when:
        SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !r.applied
        r.stopped.contains('busy_bee')
        SweepLock.holder(f.store.retentionStorage()) == null

        when:
        Files.setLastModifiedTime(root.resolve('live/running'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        f.store.retentionStorage().listLive() == []
    }

    def 'a lock held by another sweep refuses the sweep without wait'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        new SweepLock(f.store.retentionStorage(), 'other-sweep', { -> System.currentTimeMillis() } as Closure<Long>).take()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !r.applied
        r.stopped.contains('other-sweep')
        SweepLock.holder(f.store.retentionStorage()).sweepId == 'other-sweep'
    }

    def 'with wait, the sweep polls until the registration is gone, holding no lock meanwhile'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        f.store.retentionStorage().putLive('running', '{}'.bytes)
        int polls = 0
        final Closure<Void> poll = { long ms ->
            assert ms == LiveWriter.POLL_MILLIS
            assert SweepLock.holder(f.store.retentionStorage()) == null
            if( ++polls == 2 ) f.store.retentionStorage().deleteLive('running')
        } as Closure<Void>

        when:
        final SweepReport r = sweep().apply(true, 0L, { -> false }, say, poll)

        then:
        r.applied
        polls == 2
        said.size() == 2
    }

    def 'a registration that appears mid-sweep stops it before the next batch'() {
        given: 'a store with due blocks; the stop check writes a registration on its first call'
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final Closure<Boolean> writerArrives = { -> f.store.retentionStorage().putLive('late', '{}'.bytes); false } as Closure<Boolean>

        when:
        final SweepReport r = sweep().apply(false, 0L, writerArrives, say, sleeper)

        then:
        r.stopped.contains('live')
        !r.applied
        r.deleted == []
        f.store.retentionStorage().listLedgers().size() == 1
    }

    def 'a stop request before the first batch deletes nothing'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> true }, say, sleeper)

        then:
        r.stopped == 'interrupted'
        r.deleted == []
        f.store.has(only)
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'a pin written during the sweep rescues its content before the delete batch'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        boolean pinned = false
        final Closure<Boolean> pinArrives = { -> if( !pinned ) { f.claim((Cid) b.items[0], 'add', 'pin', 'late'); pinned = true }; false } as Closure<Boolean>

        when:
        final SweepReport r = sweep().apply(false, 0L, pinArrives, say, sleeper)

        then:
        f.store.has(only)
        r.deleted == []
        r.rescued == 1
    }

    def 'a pinned and deleted Selection keeps its file through a sweep; after del pin the next sweep trashes it'() {
        given:
        final Cid picked = f.raw('picked')
        final Map r = f.run('a', [[p: picked]])
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Cid sel = f.selection((Cid) r.items[0], (Cid) r.collection)
        final Cid pin = f.claim(sel, 'add', 'pin', 'figure 3')
        f.claim(sel, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport kept = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        kept.applied
        kept.trashed == 0
        f.store.retentionStorage().listLedgers() == []

        when:
        f.claim(sel, 'del', 'pin', null, [pin])
        ageAll(15 * SweepPolicy.DAY)
        final SweepReport freed = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final List<String> ledgers = f.store.retentionStorage().listLedgers()

        then:
        freed.applied
        ledgers.size() == 1
        TrashLedger.parse(ledgers[0], f.store.retentionStorage().readLedger(ledgers[0])).blocks.keySet().containsAll([picked, sel])
    }

    def 'a missing metadata block refuses --apply and a dry run reports it'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        f.store.blockPath((Cid) r.collection).toFile().delete()

        when:
        final SweepReport dry = sweep().dryRun()
        final SweepReport applied = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        dry.missingMetadata.size() == 1
        !applied.applied
        applied.stopped.contains('cannot read')
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'old scratch is cleared; young scratch stays'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        Files.write(root.resolve('blocks/.tmp-old'), 'x'.bytes)
        Files.setLastModifiedTime(root.resolve('blocks/.tmp-old'), FileTime.fromMillis(System.currentTimeMillis() - 15 * SweepPolicy.DAY))
        Files.write(root.resolve('blocks/.tmp-new'), 'x'.bytes)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.scratch == 1
        !Files.exists(root.resolve('blocks/.tmp-old'))
        Files.exists(root.resolve('blocks/.tmp-new'))
    }

    def 'a read-only member is never swept'() {
        given: 'a composition whose read-only member holds a dead block'
        f.run('a', [[x: f.raw('x')]])
        final Cid deadInRo = new LocalBlockStore(root.resolve('ro'), 'ro', true).putStreaming(new ByteArrayInputStream('dead in ro'.bytes))
        final LocalBlockStore ro = new LocalBlockStore(root.resolve('ro'), 'ro', false)
        Files.setLastModifiedTime(ro.blockPath(deadInRo), FileTime.fromMillis(System.currentTimeMillis() - 15 * SweepPolicy.DAY))
        ageAll(15 * SweepPolicy.DAY)
        final CompositeStore both = new CompositeStore([f.store, ro])

        when:
        final SweepReport r = new Sweep(both, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.dead == 0
        Files.exists(ro.blockPath(deadInRo))
        !Files.exists(root.resolve('ro/trash'))
        !Files.exists(root.resolve('ro/sweep.lock'))
    }

    def 'a store that is not writable cannot be swept'() {
        when:
        new Sweep(new LocalBlockStore(root, 'lab', false), SweepPolicy.defaults())

        then:
        thrown(IllegalArgumentException)
    }

    def 'a ledger that does not parse is left alone and warned about'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        f.store.retentionStorage().writeLedger('garbage', 'nope'.bytes)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.warnings.any { it.contains('garbage') }
        f.store.retentionStorage().listLedgers() == ['garbage']
    }
    /** Local retention storage whose heartbeats (replacing a held lock) fail as a transient S3 error would. */
    static class FailingHeartbeats implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['replaceLock']) final RetentionStorage inner
        FailingHeartbeats(RetentionStorage inner) { this.inner = inner }
        String replaceLock(String version, byte[] body) {
            if( new String(body, 'UTF-8').contains('"held"') )
                throw new IOException('transient failure')
            return inner.replaceLock(version, body)
        }
    }

    def 'a heartbeat that throws counts as a lost lock: nothing is deleted and the lock is released'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        Files.delete(root.resolve('sweep.lock'))
        final RetentionStorage failing = new FailingHeartbeats(f.store.retentionStorage())
        final LocalBlockStore flaky = new LocalBlockStore(root, 'lab', true) {
            @Override RetentionStorage retentionStorage() { failing }
        }

        when:
        final SweepReport r = new Sweep(flaky, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.stopped.contains('lock')
        !r.applied
        r.deleted == []
        f.store.has(only)
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'the scheduled heartbeat marks the lock lost when it throws or is taken over'() {
        given:
        final RetentionStorage failing = new FailingHeartbeats(f.store.retentionStorage())
        final SweepLock lock = new SweepLock(failing, 's', { -> System.currentTimeMillis() } as Closure<Long>)
        lock.take()
        final java.util.concurrent.atomic.AtomicBoolean lost = new java.util.concurrent.atomic.AtomicBoolean(false)

        when:
        Sweep.beat(lock, lost)

        then:
        noExceptionThrown()
        lost.get()

        when: 'a lock never held cannot heartbeat either'
        final java.util.concurrent.atomic.AtomicBoolean lost2 = new java.util.concurrent.atomic.AtomicBoolean(false)
        Sweep.beat(new SweepLock(f.store.retentionStorage(), 't', { -> 0L } as Closure<Long>), lost2)

        then:
        lost2.get()
    }

    def 'the report reads as the brief shows, with sizes to one decimal and thousands separators'() {
        given:
        final SweepReport r = new SweepReport(alias: 'lab', location: '/data/lab', ageFloorMillis: 14 * SweepPolicy.DAY,
            roots: new Mark.Roots(3, 2, 1, 0, 0, 1, 0, 2, 4), live: 1190, blocks: 1204, bytes: 5_200_000_000L,
            dead: 14, deadBytes: 1_100_000_000L, young: 2, trashed: 9, trashedBytes: 900_000_000L, due: 2, dueBytes: 200_000_000L,
            waiting: 1, rescued: 0, requests: 1230, blocksRead: 1204, listingPages: 2, ledgersRead: 2)

        expect:
        r.toText().readLines() == [
            'dry run over lab (/data/lab), nothing written',
            'roots      3 runs: 2 with content, 1 released, 0 hidden (0 hidden but pinned); 1 Selection; 2 pinned subjects; 4 Claims live',
            'blocks     1,204 in lab (5.2 GB); 1,190 live',
            'dead       14 (1.1 GB): 2 younger than the age floor (14d), 9 would be trashed (900.0 MB), 3 in Trash',
            'trash      2 past their deadline would be deleted (200.0 MB), 1 waiting, 0 rescued',
            'scratch    0 upload leftovers older than the age floor',
            'live       none registered',
            'requests   about 1,230 (1,204 block reads, 2 listing pages, 2 ledgers, 2 for the lock and registrations)',
        ]
        SweepReport.size(999) == '999 B'
        SweepReport.size(1_500) == '1.5 KB'
        SweepReport.size(999_990) == '1.0 MB'
        r.toJson().blocks == 1204
        r.toJson().roots.content_roots == 2
    }

    def 'an applied report says what it did, in the past tense, with its ledger and why it stopped'() {
        given:
        final SweepReport r = new SweepReport(sweepId: 's-1', applied: false, alias: 'lab', location: '/data/lab',
            ageFloorMillis: 600_000L, deleted: [f.raw('a')], deletedBytes: 1L, ledger: '1791000000000-s-1', stopped: 'interrupted',
            fresh: [new LiveRegistry.Registration('9c1e', 20_000L, 'happy_turing', 'p')])

        when:
        final List<String> lines = r.toText().readLines()

        then:
        lines[0] == 'sweep s-1 over lab (/data/lab), not applied'
        lines.contains('trash      1 past their deadline deleted (1 B), 0 waiting, 0 rescued')
        lines.contains('live       1 run registered: happy_turing (session 9c1e), heartbeat 20 s ago')
        lines.contains('ledger     trash/1791000000000-s-1')
        lines.last() == 'stopped    interrupted'
        lines.any { it.contains('age floor (10m)') }
        r.toJson().fresh == [[session: '9c1e', run_name: 'happy_turing', pipeline: 'p', age_seconds: 20L]]
    }

    // ------------------------------------------------ final review fixes (items 1, 2)

    /** Writes a `verb` Claim on `subject` whose address sorts before `before`'s, by moving the fixture clock. */
    private Cid claimSortingBefore(Cid subject, String verb, Cid before) {
        while( true ) {
            final Claim c = new Claim('test', subject, verb, null, null, [], Index.isoMillis(f.clock + 1))
            if( DagCbor.cidOf(DagCbor.encode(c.toCbor())).toString() < before.toString() )
                return f.claim(subject, verb, null, null)
            f.clock++
        }
    }

    /** Local retention storage with some behaviour replaced, served by a LocalBlockStore over the same root. */
    private LocalBlockStore storeWith(RetentionStorage storage) {
        return new LocalBlockStore(root, 'lab', true) {
            @Override RetentionStorage retentionStorage() { storage }
        }
    }

    /** A hidden run, trashed by one sweep, its ledger now past its deadline; returns the run and its delete Claim. */
    private Map hiddenRunDue() {
        final Map gone = f.run('gone', [[g: f.raw('gone')]])
        final Cid del = claimSortingBefore((Cid) gone.completion, 'delete', (Cid) gone.completion)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        return gone + [claim: del]
    }

    def 'a stop between batches never leaves a deleted run without its delete Claim'() {
        given: 'batches of one, and a stop as soon as either the Claim or its subject has gone'
        final Map gone = hiddenRunDue()
        final Cid del = (Cid) gone.claim
        final Cid completion = (Cid) gone.completion
        final Sweep s = sweep()
        s.batchSize = 1
        final Closure<Boolean> stop = { -> !f.store.has(del) || !f.store.has(completion) } as Closure<Boolean>

        when:
        final SweepReport r = s.apply(false, 0L, stop, say, sleeper)
        final SweepReport dry = sweep().dryRun()

        then: 'the subject went first; its Claim stays ledgered, so the run is not resurrected'
        r.stopped == 'interrupted'
        !f.store.has(completion)
        f.store.has(del)
        dry.missingMetadata == []
        dry.roots.contentRoots == 0

        when: 'the next sweep finishes the job'
        final SweepReport rest = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        rest.applied
        !f.store.has(del)
        f.store.retentionStorage().listLedgers() == []
    }

    /** Local retention storage whose deleteBlocks leaves the given blocks in place and reports them failed. */
    static class PartialDeletes implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['deleteBlocks']) final RetentionStorage inner
        final Set<Cid> refuse
        PartialDeletes(RetentionStorage inner, Set<Cid> refuse) { this.inner = inner; this.refuse = refuse }
        List<Cid> deleteBlocks(Collection<Cid> cids) {
            inner.deleteBlocks(cids.findAll { !refuse.contains(it) })
            return cids.findAll { refuse.contains(it) }.toList()
        }
    }

    def 'a partial DeleteObjects failure keeps every Claim for the next sweep'() {
        given:
        final Map gone = hiddenRunDue()
        final Cid del = (Cid) gone.claim
        final Cid completion = (Cid) gone.completion
        final LocalBlockStore flaky = storeWith(new PartialDeletes(f.store.retentionStorage(), [completion] as Set))

        when:
        final SweepReport r = new Sweep(flaky, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        f.store.has(completion)
        f.store.has(del)
        r.warnings.any { it.contains('could not be deleted') }
        sweep().dryRun().roots.contentRoots == 0

        when:
        final SweepReport rest = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        rest.applied
        !f.store.has(completion)
        !f.store.has(del)
    }

    def 'a due Claim whose subject is still waiting in Trash is held back, not deleted'() {
        given: 'the run completion moved to a ledger whose deadline has not passed'
        final Map gone = hiddenRunDue()
        final Cid del = (Cid) gone.claim
        final Cid completion = (Cid) gone.completion
        final RetentionStorage s = f.store.retentionStorage()
        final String name = s.listLedgers()[0]
        final TrashLedger l = TrashLedger.parse(name, s.readLedger(name))
        s.writeLedger(name, l.without([completion] as Set).toJson())
        final TrashLedger later = new TrashLedger('later', System.currentTimeMillis() + SweepPolicy.DAY, l.trashedAt, [(completion): l.blocks[completion]])
        s.writeLedger(later.name, later.toJson())

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final SweepReport dry = sweep().dryRun()

        then:
        r.applied
        f.store.has(completion)
        f.store.has(del)
        s.listLedgers().any { TrashLedger.parse(it, s.readLedger(it)).blocks.containsKey(del) }
        dry.roots.hidden == 1
        dry.roots.contentRoots == 0
        dry.missingMetadata == []
    }

    /** Local retention storage whose ledger writes fail. */
    static class FailingLedgers implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['writeLedger']) final RetentionStorage inner
        FailingLedgers(RetentionStorage inner) { this.inner = inner }
        void writeLedger(String name, byte[] body) { throw new IOException('ledger write refused') }
    }

    def 'the lock is released when the applied pass throws'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)

        when:
        new Sweep(storeWith(new FailingLedgers(f.store.retentionStorage())), SweepPolicy.defaults())
            .apply(false, 0L, { -> false }, say, sleeper)

        then:
        final IOException e = thrown()
        e.message == 'ledger write refused'
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    /** Local retention storage counting its block listings. */
    static class CountingListings implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['listBlockStats']) final RetentionStorage inner
        int listings = 0
        CountingListings(RetentionStorage inner) { this.inner = inner }
        List<BlockStat> listBlockStats() { listings++; inner.listBlockStats() }
    }

    def 'a stop request during the mark ends the sweep there and releases the lock'() {
        given:
        f.run('a', [[x: f.raw('x')], [y: f.raw('y')]])
        final CountingListings counting = new CountingListings(f.store.retentionStorage())

        when:
        final SweepReport r = new Sweep(storeWith(counting), SweepPolicy.defaults()).apply(false, 0L, { -> true }, say, sleeper)

        then:
        r.stopped == 'interrupted'
        !r.applied
        counting.listings == 0
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'abandon releases the lock of a sweep still running, which then stops before deleting'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final Sweep s = sweep()
        SweepLock.Holder afterAbandon = new SweepLock.Holder('not called', null, 0L)
        boolean abandoned = false
        final Closure<Boolean> hook = { ->
            if( !abandoned ) {
                abandoned = true
                s.abandon()
                afterAbandon = SweepLock.holder(f.store.retentionStorage())
            }
            false
        } as Closure<Boolean>

        when:
        final SweepReport r = s.apply(false, 0L, hook, say, sleeper)

        then:
        abandoned
        afterAbandon == null
        r.stopped.contains('lock')
        r.deleted == []
        f.store.has(only)
    }

    def 'each delete batch of metadata is announced by one forget entry'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.forgets.size() >= 1
        final List<StoreLogEntry> entries = StoreLog.read(f.store).findAll { it.kind == StoreLogKind.FORGET }
        entries*.cid as Set == r.forgets as Set
        final Set<Cid> listed = r.forgets.collectMany { Cid c ->
            Forget.fromCbor((Map) DagCbor.decode(f.store.open(c).withCloseable { it.readAllBytes() })).deleted } as Set
        listed.contains(gone.completion)
        listed.every { Cid c -> c.isDagCbor() }
        r.unannounced == []
    }

    def 'a dry run never asks who signs; an applied sweep that writes Forget blocks asks once'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        final Map also = f.run('also', [[h: f.raw('h')]])
        f.claim(also.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        int asked = 0
        final java.util.function.Supplier<String> who = { -> asked++; 'ana@example.org' } as java.util.function.Supplier<String>
        new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        asked = 0

        when:
        final SweepReport dry = new Sweep(f.store, SweepPolicy.defaults(), who).dryRun()

        then:
        dry.deleted == []
        asked == 0

        when: 'one block per batch, so several Forget blocks'
        final Sweep applied = new Sweep(f.store, SweepPolicy.defaults(), who)
        applied.batchSize = 1
        final SweepReport r = applied.apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.forgets.size() > 1
        asked == 1
        r.forgets.every { Cid c -> Forget.fromCbor((Map) DagCbor.decode(f.store.open(c).withCloseable { it.readAllBytes() })).assertedBy == 'ana@example.org' }
    }

    def 'a blank or null answer signs the Forget as nf-blocks'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        final java.util.function.Supplier<String> who = { -> answer } as java.util.function.Supplier<String>
        new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.forgets.size() >= 1
        r.forgets.every { Cid c -> Forget.fromCbor((Map) DagCbor.decode(f.store.open(c).withCloseable { it.readAllBytes() })).assertedBy == 'nf-blocks' }

        where:
        answer << [null, '', '  ']
    }

    def 'a sweep that deletes only raw content writes no forget entry'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.forgets == []
        !StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET }
    }

    def 'a later sweep keeps the Forget blocks its entries name'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final SweepReport first = sweep().apply(false, 0L, { -> false }, say, sleeper)
        ageAll(15 * SweepPolicy.DAY)

        when:
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        first.forgets.every { Cid c -> f.store.has(c) }
    }

    def 'a forget entry that cannot be written leaves its cids unannounced and warns'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        // The brief's alternative to a read-only log/: only the forget append fails, so the
        // rest of the sweep (deleting log entries, rewriting ledgers) still works.
        final StoreLogStorage logs = new LocalStoreLogStorage(root)
        final StoreLogStorage refusing = new StoreLogStorage() {
            void putEntry(String name) {
                if( name.contains('-forget-') )
                    throw new IOException('denied')
                logs.putEntry(name)
            }
            List<String> listEntries() { logs.listEntries() }
            boolean hasEntry(String name) { logs.hasEntry(name) }
        }
        final BlockStore store = new LocalBlockStore(root, 'lab', true) {
            @Override
            StoreLogStorage storeLogStorage() { refusing }
        }

        when:
        final SweepReport r = new Sweep(store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.unannounced.contains(gone.completion)
        r.warnings.any { it.contains('reindex') }
        r.forgets == []
        !StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET }
    }

    /** Local retention storage whose deleteBlocks deletes all but the given blocks, then throws. */
    static class DeletesThenThrows implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['deleteBlocks']) final RetentionStorage inner
        final Set<Cid> keep
        DeletesThenThrows(RetentionStorage inner, Set<Cid> keep) { this.inner = inner; this.keep = keep }
        List<Cid> deleteBlocks(Collection<Cid> cids) {
            inner.deleteBlocks(cids.findAll { !keep.contains(it) })
            throw new IOException('connection reset mid-batch')
        }
    }

    def 'a batch whose delete throws is still announced, and a cache keeps the rows of what survived'() {
        given: 'a cache caught up on a hidden run that is due, and a delete that removes all but its RunCompletion, then fails'
        Map gone = hiddenRunDue()
        Cid completion = (Cid) gone.completion
        Cid collection = (Cid) gone.collection
        Index index = Index.open(root.resolve('../cache-' + root.fileName + '.sqlite'))
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        LocalBlockStore flaky = storeWith(new DeletesThenThrows(f.store.retentionStorage(), [completion] as Set))

        when:
        SweepReport r = new Sweep(flaky, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then: 'the sweep stopped, and one forget entry lists the whole batch'
        r.stopped.contains('connection reset mid-batch')
        r.forgets.size() == 1
        r.unannounced == []
        List<Cid> listed = Forget.fromCbor((Map) DagCbor.decode(f.store.open(r.forgets[0]).withCloseable { it.readAllBytes() })).deleted
        listed.contains(completion)
        listed.contains(collection)
        StoreLog.read(f.store).any { it.kind == StoreLogKind.FORGET && it.cid == r.forgets[0] }

        when: 'the cache catches up'
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then: 'the RunCompletion that survived keeps its row; the deleted collection is forgotten'
        f.store.has(completion)
        !f.store.has(collection)
        index.isRunIndexed(completion)
        index.countRows('collection') == 0

        cleanup:
        index?.close()
    }

    // ------------------------------------------------ P4: unknown kinds (R8), Project, Handoff, InputSet (R1)

    /** Writes a well-formed Store Log entry of a kind this build does not know into the member at `dir`. */
    private static String unknownEntry(Path dir, String kind, long millis = 1_790_000_000_000L) {
        final String name = "${StoreLog.reverseTimestamp(millis)}-${kind}-${Fixtures.cidOf([kind: kind, at: millis])}".toString()
        Files.createDirectories(dir.resolve('log'))
        Files.createFile(dir.resolve('log').resolve(name))
        return name
    }

    private static final String REFUSAL = 'that this build does not know, so a sweep could delete blocks they keep; upgrade nf-blocks'

    def 'a dry run works beside unknown kinds and lists them, per member'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        unknownEntry(root, 'zap')
        unknownEntry(root, 'frob')
        unknownEntry(root, 'frob', 1_790_000_000_001L)
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport r = sweep().dryRun()

        then:
        r.unknownKinds == [lab: ['frob', 'zap']]
        r.roots.contentRoots == 1
        r.toJson().unknown_kinds == [lab: ['frob', 'zap']]
        r.toText().readLines().contains(
            'unknown    the Store Log of member lab holds entries of kind(s) frob, zap ' + REFUSAL + '; --apply would refuse')
        r.stopped == null
    }

    def 'with only known kinds the report lists none'() {
        given:
        f.run('a', [[x: f.raw('x')]])

        when:
        final SweepReport r = sweep().dryRun()

        then:
        r.unknownKinds == [:]
        r.toJson().unknown_kinds == [:]
        !r.toText().contains('unknown')
    }

    def 'an applied sweep refuses before deleting or trashing anything when the swept member logs an unknown kind'() {
        given: 'a hidden run due for deletion, released content to trash, and an entry of an unknown kind'
        final Map gone = hiddenRunDue()
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        unknownEntry(root, 'frob')
        final List<String> ledgersBefore = f.store.retentionStorage().listLedgers()
        final Set<String> before = listing(root.resolve('blocks'))

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !r.applied
        r.stopped == 'the Store Log of member lab holds entries of kind(s) frob ' + REFUSAL
        r.deleted == []
        r.ledger == null
        f.store.has((Cid) gone.completion) && f.store.has(only)
        listing(root.resolve('blocks')) == before
        f.store.retentionStorage().listLedgers() == ledgersBefore
        SweepLock.holder(f.store.retentionStorage()) == null
        r.unknownKinds == [lab: ['frob']]
    }

    def 'a hyphenated future kind, or a known kind whose cid this build cannot parse, refuses an applied sweep'() {
        given:
        final Map gone = hiddenRunDue()
        ageAll(15 * SweepPolicy.DAY)
        unknownEntry(root, 'input-set')
        Files.createFile(root.resolve('log').resolve("${StoreLog.reverseTimestamp(1_790_000_000_002L)}-run-zb2rhfuturecidform".toString()))

        when:
        final SweepReport dry = sweep().dryRun()
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        dry.unknownKinds == [lab: ['input-set', 'run']]
        !r.applied
        r.stopped == 'the Store Log of member lab holds entries of kind(s) input-set, run ' + REFUSAL
        r.deleted == []
        f.store.has((Cid) gone.completion)
    }

    def 'an unknown kind in a read-only member refuses the sweep too'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        final LocalBlockStore ro = new LocalBlockStore(root.resolve('ro'), 'ro', false)
        unknownEntry(root.resolve('ro'), 'zap')
        final CompositeStore both = new CompositeStore([f.store, ro])

        when:
        final SweepReport dry = new Sweep(both, SweepPolicy.defaults()).dryRun()
        final SweepReport r = new Sweep(both, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        dry.unknownKinds == [ro: ['zap']]
        dry.trashed == 1
        !r.applied
        r.stopped == 'the Store Log of member ro holds entries of kind(s) zap ' + REFUSAL
        f.store.retentionStorage().listLedgers() == []
    }

    def 'an entry of an unknown kind logged during the sweep stops it before the delete batch'() {
        given:
        final Map gone = hiddenRunDue()
        boolean wrote = false
        final Closure<Boolean> newerBuildWrites = { -> if( !wrote ) { unknownEntry(root, 'frob'); wrote = true }; false } as Closure<Boolean>

        when:
        final SweepReport r = sweep().apply(false, 0L, newerBuildWrites, say, sleeper)

        then:
        wrote
        !r.applied
        r.stopped == 'the Store Log of member lab holds entries of kind(s) frob ' + REFUSAL
        r.deleted == []
        f.store.has((Cid) gone.completion)
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'a deleted Project and a deleted Handoff are trashed; a live Handoff keeps its files and Selection'() {
        given:
        final Map r = f.run('a', [[p: f.raw('picked')]])
        f.claim((Cid) r.completion, 'delete', null, null)
        final Cid sel = f.selectionBlock((Cid) r.items[0], (Cid) r.collection)
        final Cid project = f.project('study')
        final Cid sheet = f.raw('kept sheet')
        final Cid kept = f.handoff(sel, project, ['samplesheet.csv': sheet])
        final Cid goneSheet = f.raw('gone sheet')
        final Cid gone = f.handoff(sel, project, ['samplesheet.csv': goneSheet])
        f.claim(gone, 'delete', null, null)
        f.claim(project, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport s = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final Set<Cid> trashed = TrashLedger.parse(s.ledger, f.store.retentionStorage().readLedger(s.ledger)).blocks.keySet()

        then:
        s.applied
        trashed.containsAll([project, gone, goneSheet])
        !trashed.any { it in [kept, sheet, sel, (Cid) r.items[0]] }
        !trashed.contains((Cid) r.collection)
        trashed.contains((Cid) r.completion)
    }

    def "a run's InputSet keeps itself, never the inputs it names; a sweep trashes an input nothing else reaches"() {
        given:
        final Cid input = f.raw('reads.fastq.gz')
        final Cid shared = f.raw('reference.fa')
        f.run('b', [[ref: shared]])
        final Cid inputs = f.inputSet([input, shared])
        final Map r = f.run('a', [[x: f.raw('x')]], [inputSet: inputs])
        if( verb )
            f.claim((Cid) r.completion, verb, attribute, value)
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport s = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final Set<Cid> trashed = TrashLedger.parse(s.ledger, f.store.retentionStorage().readLedger(s.ledger)).blocks.keySet()

        then:
        s.applied
        trashed.contains(input)
        !trashed.contains(shared)
        trashed.contains(inputs) == !inputSetKept

        where:
        verb     | attribute | value     || inputSetKept
        null     | null      | null      || true
        'set'    | 'retain'  | 'lineage' || true
        'delete' | null      | null      || false
    }

    /** Local retention storage that records, in order, when the lock is taken and each delete batch. */
    static class RecordingStorage implements RetentionStorage {
        @groovy.lang.Delegate(excludes = ['createLock', 'deleteBlocks']) final RetentionStorage inner
        final List<String> events
        RecordingStorage(RetentionStorage inner, List<String> events) { this.inner = inner; this.events = events }
        String createLock(byte[] body) { events << 'lock'; inner.createLock(body) }
        List<Cid> deleteBlocks(Collection<Cid> cids) { events << 'delete'; inner.deleteBlocks(cids) }
    }

    def 'an applied sweep with metadata past its deadline asks who signs before it takes the lock and before the first delete'() {
        given: 'a hidden run trashed by an earlier sweep, its ledger now past its deadline'
        hiddenRunDue()
        final List<String> events = []
        boolean lockHeldWhenAsked = true
        final java.util.function.Supplier<String> who = { ->
            events << 'signer'
            lockHeldWhenAsked = SweepLock.holder(f.store.retentionStorage()) != null
            'ana@example.org'
        } as java.util.function.Supplier<String>

        when:
        final SweepReport r = new Sweep(storeWith(new RecordingStorage(f.store.retentionStorage(), events)), SweepPolicy.defaults(), who)
            .apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        r.forgets.size() >= 1
        events.count('signer') == 1
        events.first() == 'signer'
        events.contains('lock') && events.contains('delete')
        !lockHeldWhenAsked
    }

    def 'a signer that throws does not stop the sweep: what it deleted stays unannounced, with a warning'() {
        given: 'a hidden run past its deadline, and a supplier that always throws'
        final Map gone = hiddenRunDue()
        int asked = 0
        final java.util.function.Supplier<String> who = { -> asked++; throw new IllegalStateException('identity broke') } as java.util.function.Supplier<String>

        when:
        final SweepReport r = new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)

        then: 'the sweep ran to the end and released the lock, as it did when only the first Forget asked'
        r.applied
        r.stopped == null
        r.deleted.contains(gone.completion)
        !f.store.has((Cid) gone.completion)
        r.forgets == []
        r.unannounced.contains(gone.completion)
        r.warnings.any { it.contains('identity broke') && it.contains('reindex') }
        SweepLock.holder(f.store.retentionStorage()) == null
        asked >= 2      // once before the lock, again at the first Forget
    }

    def 'an applied sweep with nothing past its deadline, or only raw content due, never asks who signs'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        int asked = 0
        final java.util.function.Supplier<String> who = { -> asked++; 'ana@example.org' } as java.util.function.Supplier<String>

        when: 'the first sweep only trashes, and the second finds its ledger still waiting'
        final SweepReport trashing = new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)
        final SweepReport waiting = new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)

        then:
        trashing.applied && trashing.trashed == 1
        waiting.applied && waiting.waiting == 1 && waiting.deleted == []
        asked == 0

        when: 'the ledger, which holds only raw content, passes its deadline'
        expireLedgers()
        final SweepReport raw = new Sweep(f.store, SweepPolicy.defaults(), who).apply(false, 0L, { -> false }, say, sleeper)

        then:
        raw.applied
        raw.deleted.size() == 1
        raw.forgets == []
        asked == 0
    }
}
