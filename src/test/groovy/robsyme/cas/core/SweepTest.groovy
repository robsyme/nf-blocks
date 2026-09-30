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
}
