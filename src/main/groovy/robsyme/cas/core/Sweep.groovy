package robsyme.cas.core

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The sweep of a store's writable member (ticket 20 answers 1 to 6). A dry run
 * marks, lists and plans, and writes nothing. An applied sweep takes the lock,
 * refuses beside a fresh registration, deletes the blocks of past-deadline
 * ledgers that are still dead (re-checking before every batch), rewrites the
 * ledgers, then trashes what is newly dead into one new ledger. Deletion is
 * irreversible, so every doubt resolves to keeping: a failed re-check stops
 * the sweep before its next step.
 */
@Slf4j
@CompileStatic
class Sweep {

    static final int BATCH = 1000
    static final int THREADS = 32

    private final BlockStore store
    private final BlockStore writable
    private final RetentionStorage storage
    private final SweepPolicy policy

    Sweep(BlockStore store, SweepPolicy policy) {
        policy.validate()
        this.store = store
        this.writable = store instanceof CompositeStore ? ((CompositeStore) store).members[0] : store
        if( !(writable instanceof RetainedStore) || !writable.isWritable() )
            throw new IllegalArgumentException("store member '${writable.alias()}' is not a writable member this build can sweep")
        this.storage = ((RetainedStore) writable).retentionStorage()
        this.policy = policy
    }

    private List<BlockStore> members() {
        return store instanceof CompositeStore ? ((CompositeStore) store).members : [store]
    }

    private List<MemberLog> logs() {
        return members().collect { BlockStore m -> new MemberLog(m.alias(), m.is(writable), StoreLog.of(m).read()) }
    }

    private SweepReport newReport() {
        final SweepReport r = new SweepReport()
        r.alias = writable.alias()
        r.location = storage.describe()
        r.ageFloorMillis = policy.ageFloorMillis
        r.warnings.addAll(policy.warnings())
        return r
    }

    SweepReport dryRun() {
        final SweepReport r = newReport()
        final LiveRegistry registry = new LiveRegistry(storage)
        r.fresh = registry.fresh()
        final Mark mark = Mark.of(store, logs(), THREADS)
        final List<BlockStat> stats = storage.listBlockStats()
        final List<TrashLedger> ledgers = ledgers(r)
        final SweepPlan plan = SweepPlan.of(mark, stats, ledgers, storage.nowMillis(), policy, 0L)
        fill(r, mark, stats, plan, ledgers.size())
        r.trashed = plan.trash.size()
        r.trashedBytes = sizeOf(plan.trash)
        r.scratch = oldScratch().size()
        r.staleRegistrations = registry.stale().size()
        return r
    }

    SweepReport apply(boolean wait, long budgetBytes, Closure<Boolean> stopRequested, Closure<Void> say, Closure<Void> sleeper) {
        final String id = SweepLock.newSweepId(System.currentTimeMillis())
        final SweepReport r = newReport()
        r.sweepId = id
        final SweepLock lock = new SweepLock(storage, id, { -> System.currentTimeMillis() } as Closure<Long>)
        final LiveRegistry registry = new LiveRegistry(storage)
        // Take the lock, then list live/ (ticket 20 answer 5). With --wait, wait holding nothing,
        // so runs that register while we wait are not stuck behind us.
        while( true ) {
            final SweepLock.Holder h = lock.take()
            if( h == null ) {
                List<LiveRegistry.Registration> fresh = null
                try {
                    fresh = registry.fresh()
                }
                catch( Exception e ) {
                    releaseQuietly(lock)
                    throw e
                }
                if( fresh.isEmpty() )
                    break
                releaseQuietly(lock)
                r.fresh = fresh
                if( !wait ) {
                    r.stopped = "a pipeline is running against ${storage.describe()}: ${describe(fresh)}; a sweep never runs beside a Live Writer (retry, or pass --wait)".toString()
                    return r
                }
                say.call("waiting for ${describe(fresh)} to finish; checking every 30 s".toString())
            }
            else {
                if( !wait ) {
                    r.stopped = "sweep ${h.sweepId} holds sweep.lock in ${storage.describe()} (heartbeat ${h.ageMillis.intdiv(1000L)} s ago)".toString()
                    return r
                }
                say.call("waiting for sweep ${h.sweepId} to release the lock; checking every 30 s".toString())
            }
            sleeper.call(LiveWriter.POLL_MILLIS)
        }
        r.fresh = []
        final ScheduledExecutorService beats = Executors.newSingleThreadScheduledExecutor({ Runnable task ->
            final Thread t = new Thread(task, 'nf-blocks-sweep-heartbeat'); t.daemon = true; t } as ThreadFactory)
        final AtomicBoolean lost = new AtomicBoolean(false)
        beats.scheduleAtFixedRate({ -> beat(lock, lost) } as Runnable,
            SweepLock.HEARTBEAT_MILLIS, SweepLock.HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        try {
            return applyLocked(new Pass(r, lock, lost, registry, stopRequested), budgetBytes)
        }
        finally {
            beats.shutdownNow()
            try {
                beats.awaitTermination(5, TimeUnit.SECONDS)
            }
            catch( InterruptedException e ) {
                Thread.currentThread().interrupt()
            }
            releaseQuietly(lock)
        }
    }

    /**
     * The scheduled heartbeat. Only ever touches the lock: a heartbeat that is
     * refused (taken over) or throws (a transient store error) counts as a lost
     * lock, which the sweeping thread sees at its next re-check. Throwing out of
     * a scheduled task would silently cancel it, so nothing escapes.
     */
    static void beat(SweepLock lock, AtomicBoolean lost) {
        try {
            if( !lock.heartbeat() )
                lost.set(true)
        }
        catch( Throwable t ) {
            log.warn("nf-blocks sweep: the sweep lock heartbeat failed, stopping before the next step: ${t.message}")
            lost.set(true)
        }
    }

    private static void releaseQuietly(SweepLock lock) {
        try {
            lock.release()
        }
        catch( Exception e ) {
            log.warn("nf-blocks sweep: could not release the sweep lock; it goes stale in 10 minutes: ${e.message}")
        }
    }

    /** One applied pass: its report, lock, and the Store Log entries its mark has already seen. */
    @CompileStatic
    private static class Pass {
        final SweepReport r
        final SweepLock lock
        final AtomicBoolean lost
        final LiveRegistry registry
        final Closure<Boolean> stopRequested
        Mark mark
        List<MemberLog> seen
        final List<Set<String>> seenNames = new ArrayList<Set<String>>()
        boolean lockLost

        Pass(SweepReport r, SweepLock lock, AtomicBoolean lost, LiveRegistry registry, Closure<Boolean> stopRequested) {
            this.r = r
            this.lock = lock
            this.lost = lost
            this.registry = registry
            this.stopRequested = stopRequested
        }
    }

    private SweepReport applyLocked(Pass p, long budgetBytes) {
        final SweepReport r = p.r
        p.seen = logs()
        for( MemberLog l : p.seen )
            p.seenNames.add(new HashSet<String>(l.entries*.name))
        final Mark mark = Mark.of(store, p.seen, THREADS)
        p.mark = mark
        final List<BlockStat> stats = storage.listBlockStats()
        final List<TrashLedger> ledgers = ledgers(r)
        final long now = storage.nowMillis()
        final SweepPlan plan = SweepPlan.of(mark, stats, ledgers, now, policy, budgetBytes)
        fill(r, mark, stats, plan, ledgers.size())
        r.trashed = 0
        r.trashedBytes = 0L
        if( mark.missingMetadata ) {
            r.stopped = "the mark cannot read ${mark.missingMetadata.size()} metadata block(s) it needs, so it cannot tell what lies under them: ${mark.missingMetadata.take(20).join('; ')}".toString()
            return r
        }

        // Delete what is due, a batch at a time, re-checking before each (plan decision 10).
        final List<Cid> deleted = new ArrayList<Cid>()
        final List<BlockStat> due = new ArrayList<BlockStat>(plan.due)
        for( int from = 0; from < due.size(); from += BATCH ) {
            r.stopped = recheck(p)
            if( r.stopped != null )
                break
            final List<BlockStat> batch = new ArrayList<BlockStat>()
            for( BlockStat s : due.subList(from, Math.min(due.size(), from + BATCH)) )
                if( !mark.isLive(s.cid) )
                    batch.add(s)
            if( batch.isEmpty() )
                continue
            List<Cid> failed = null
            try {
                failed = storage.deleteBlocks(batch*.cid)
            }
            catch( Exception e ) {
                // Which of the batch went is unknown: record none. A later sweep drops what is
                // gone from its ledger and clears its log entries as dangling.
                r.stopped = "deleting a batch of ${batch.size()} block(s) failed: ${e.message}".toString()
                break
            }
            final Set<Cid> failedSet = new HashSet<Cid>(failed)
            for( BlockStat s : batch )
                if( !failedSet.contains(s.cid) ) {
                    deleted.add(s.cid)
                    r.deletedBytes += s.size
                }
            if( failed )
                r.warnings.add("${failed.size()} block(s) could not be deleted and stay in Trash: ${failed.take(20).join(', ')}".toString())
        }
        r.deleted = deleted
        deleteLogEntries(deleted)
        // Rewrite every ledger without what was deleted, is live now, or is gone (plan decision 10),
        // unless the lock was lost: then another sweep owns the ledgers, and leaving them as they are
        // only keeps blocks (a later sweep drops what is gone).
        r.rescued = 0      // what this sweep actually dropped, not the plan's count
        if( !p.lockLost )
            rewriteLedgers(r, ledgers, deleted, stats, mark)
        if( r.stopped == null )
            r.stopped = recheck(p)
        if( r.stopped == null ) {
            final Map<Cid, Long> trash = new LinkedHashMap<Cid, Long>()
            for( BlockStat s : plan.trash )
                if( !mark.isLive(s.cid) )
                    trash.put(s.cid, s.size)
            if( !trash.isEmpty() ) {
                final TrashLedger ledger = new TrashLedger(r.sweepId, storage.nowMillis() + policy.graceMillis, Index.isoMillis(System.currentTimeMillis()), trash)
                storage.writeLedger(ledger.name, ledger.toJson())
                r.ledger = ledger.name
            }
            r.trashed = trash.size()
            long bytes = 0L
            for( Long v : trash.values() )
                bytes += v
            r.trashedBytes = bytes
            for( Stamped s : oldScratch() ) {
                storage.deleteScratch(s)
                r.scratch++
            }
            r.staleRegistrations = p.registry.stale().size()
            p.registry.deleteStale()
            deleteDangling(mark)
            r.applied = true
        }
        return r
    }

    /**
     * Null when the sweep may go on; else why it stops. In order: a stop request,
     * the lock (lost to the heartbeat thread, or refused or failing now), a fresh
     * registration, then the Store Log entries written since the mark, which
     * extend it. Any failure here stops the sweep: we cannot show it is safe.
     */
    private String recheck(Pass p) {
        try {
            if( p.stopRequested.call() )
                return 'interrupted'
            boolean held
            try {
                held = !p.lost.get() && p.lock.heartbeat()
            }
            catch( Exception e ) {
                log.warn("nf-blocks sweep: the sweep lock heartbeat failed: ${e.message}")
                held = false
            }
            if( !held ) {
                p.lockLost = true
                p.lost.set(true)
                return 'the sweep lock was taken over, or could not be renewed'
            }
            final List<LiveRegistry.Registration> fresh = p.registry.fresh()
            if( fresh ) {
                p.r.fresh = fresh
                return "a pipeline started: ${describe(fresh)} is live".toString()
            }
            p.mark.extend(newEntries(p))
            if( p.mark.missingMetadata )
                return "the mark cannot read ${p.mark.missingMetadata.size()} metadata block(s) written since it began: ${p.mark.missingMetadata.take(20).join('; ')}".toString()
            return null
        }
        catch( Exception e ) {
            return "the re-check before the next step failed, so nothing more is deleted: ${e.message}".toString()
        }
    }

    /** Per member, the Store Log entries not seen before; they are added to what is seen, so the next re-check reads only newer ones. */
    private List<MemberLog> newEntries(Pass p) {
        final List<MemberLog> fresh = logs()
        final List<MemberLog> out = new ArrayList<MemberLog>()
        for( int i = 0; i < fresh.size(); i++ ) {
            final MemberLog now = fresh[i]
            final Set<String> names = p.seenNames[i]
            final List<StoreLogEntry> added = new ArrayList<StoreLogEntry>()
            for( StoreLogEntry e : now.entries )
                if( names.add(e.name) )
                    added.add(e)
            out.add(new MemberLog(now.alias, now.writable, added))
        }
        return out
    }

    private void rewriteLedgers(SweepReport r, List<TrashLedger> ledgers, List<Cid> deleted, List<BlockStat> stats, Mark mark) {
        final Set<Cid> drop = new HashSet<Cid>(deleted)
        final Set<Cid> listed = new HashSet<Cid>()
        for( BlockStat s : stats )
            listed.add(s.cid)
        for( TrashLedger l : ledgers ) {
            final Set<Cid> leaving = new LinkedHashSet<Cid>()
            for( Cid c : l.blocks.keySet() ) {
                if( drop.contains(c) )
                    leaving.add(c)
                else if( mark.isLive(c) || !listed.contains(c) ) {
                    leaving.add(c)
                    r.rescued++
                }
            }
            if( leaving.isEmpty() )
                continue
            final TrashLedger rest = l.without(leaving)
            if( rest.blocks.isEmpty() )
                storage.deleteLedger(l.name)
            else
                storage.writeLedger(l.name, rest.toJson())
        }
    }

    /** Every ledger, parsed. One that does not parse is left alone and warned about: it holds nothing we would delete. */
    private List<TrashLedger> ledgers(SweepReport r) {
        final List<TrashLedger> out = new ArrayList<TrashLedger>()
        for( String name : storage.listLedgers() ) {
            final byte[] body = storage.readLedger(name)
            if( body == null )
                continue
            try {
                out.add(TrashLedger.parse(name, body))
            }
            catch( IllegalArgumentException e ) {
                r.warnings.add("skipping trash/${name}: ${e.message}".toString())
            }
        }
        return out
    }

    private List<Stamped> oldScratch() {
        final long now = storage.nowMillis()
        return storage.listScratch().findAll { Stamped s -> now - s.lastModifiedMillis >= policy.ageFloorMillis }
    }

    /** Decision 7: a deleted block's Store Log entries go with it. */
    private void deleteLogEntries(List<Cid> deleted) {
        if( deleted.isEmpty() )
            return
        final Set<Cid> gone = new HashSet<Cid>(deleted)
        for( StoreLogEntry e : StoreLog.of(writable).read() )
            if( gone.contains(e.cid) )
                storage.deleteLogEntry(e.name)
    }

    /**
     * Entries of the writable member's Store Log whose root block is absent.
     * Only those older than the age floor go: a writer that logged a root a
     * moment before its block lands must not lose the root.
     */
    private void deleteDangling(Mark mark) {
        final long now = storage.nowMillis()
        for( String name : mark.danglingEntries ) {
            final StoreLogEntry e = StoreLog.parse(name)
            if( e != null && now - e.writtenAtMillis >= policy.ageFloorMillis )
                storage.deleteLogEntry(name)
        }
    }

    private static long sizeOf(List<BlockStat> stats) {
        long n = 0L
        for( BlockStat s : stats )
            n += s.size
        return n
    }

    private static void fill(SweepReport r, Mark m, List<BlockStat> stats, SweepPlan p, int ledgerCount) {
        r.roots = m.roots
        r.live = m.live.size()
        r.blocks = stats.size()
        r.bytes = sizeOf(stats)
        r.dead = p.dead.size()
        r.deadBytes = sizeOf(p.dead)
        r.young = p.young.size()
        r.trashed = p.trash.size()
        r.trashedBytes = sizeOf(p.trash)
        r.overBudget = p.overBudget.size()
        r.due = p.due.size()
        r.dueBytes = sizeOf(p.due)
        r.waiting = p.waiting.size()
        r.rescued = p.rescued.size()
        r.missingMetadata = new ArrayList<String>(m.missingMetadata)
        r.missingContent = new ArrayList<Cid>(m.missingContent)
        r.dangling = m.danglingEntries.size()
        r.unreadableClaims = m.unreadableClaims.size()
        r.blocksRead = m.blocksRead
        r.listingPages = (int) Math.max(1L, (stats.size() + 999L).intdiv(1000L))
        r.ledgersRead = ledgerCount
        r.requests = r.blocksRead + r.listingPages + ledgerCount + 2
    }

    static String describe(List<LiveRegistry.Registration> fresh) {
        return fresh.collect { LiveRegistry.Registration r ->
            final String name = r.runName ? "run ${r.runName}" : 'an unnamed run'
            final String pipeline = r.pipeline ? " of pipeline ${r.pipeline}" : ''
            "${name}${pipeline} (session ${r.session}, heartbeat ${r.ageMillis.intdiv(1000L)} s ago)".toString()
        }.join('; ')
    }
}
