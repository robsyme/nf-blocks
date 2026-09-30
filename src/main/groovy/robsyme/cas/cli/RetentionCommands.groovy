package robsyme.cas.cli

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.SweepSettings
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.Prune
import robsyme.cas.core.PruneDecision
import robsyme.cas.core.Put
import robsyme.cas.core.PutError
import robsyme.cas.core.PutResult
import robsyme.cas.core.RetainedStore
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.Sweep
import robsyme.cas.core.SweepLock
import robsyme.cas.core.SweepPolicy
import robsyme.cas.core.SweepReport
import robsyme.cas.core.TrashLedger

/**
 * `nextflow plugin nf-blocks:sweep|prune|untrash` (ticket 20 and 21, plan
 * milestone 6 task 9). Static methods in the style of {@link ItemsCommand}:
 * each builds its own {@link CasSession} and reports a failure rather than
 * throwing, except for a usage error ({@link UsageException}, exit 2), which
 * {@code CasCommands.run} turns into an exit status.
 */
@CompileStatic
class RetentionCommands {

    // --------------------------------------------------------------- sweep

    static int sweep(Options o, Map config, PrintStream out, PrintStream err) {
        if( o.positionals )
            throw new UsageException("sweep takes no arguments, got ${o.positionals}")
        final boolean apply = bool(o, 'apply')
        final boolean wait = bool(o, 'wait')
        final String format = o.flag('format') ?: 'text'
        if( !(format in ['text', 'json']) )
            throw new UsageException("--format is text or json, got '${format}'")
        if( wait && !apply )
            throw new UsageException('--wait applies to --apply only; a dry run never waits')
        final long budget = o.flag('budget') ? bytes(o.flag('budget')) : 0L
        final SweepPolicy policy = policy(config, err)
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        cas.checkClock()
        final Sweep sweeper = new Sweep(cas.store, policy)
        if( !apply ) {
            final SweepReport r = sweeper.dryRun()
            out.println(format == 'json' ? JsonOutput.toJson(r.toJson()) : r.toText())
            return 0
        }
        final AtomicBoolean stop = new AtomicBoolean(false)
        final CountDownLatch done = new CountDownLatch(1)
        final Thread hook = new Thread({ ->
            stop.set(true)
            try { done.await(30, TimeUnit.SECONDS) } catch( InterruptedException e ) { Thread.currentThread().interrupt() }
        } as Runnable, 'nf-blocks-sweep-stop')
        Runtime.runtime.addShutdownHook(hook)
        SweepReport r = null
        try {
            r = sweeper.apply(wait, budget, { -> stop.get() } as Closure<Boolean>,
                { String m -> err.println("nf-blocks:sweep: ${m}") } as Closure<Void>,
                { long ms -> Thread.sleep(ms) } as Closure<Void>)
        }
        finally {
            done.countDown()
            try { Runtime.runtime.removeShutdownHook(hook) } catch( IllegalStateException e ) { /* already shutting down */ }
        }
        out.println(format == 'json' ? JsonOutput.toJson(r.toJson()) : r.toText())
        if( r.deleted )
            forgetAndSnapshot(cas, r, out)
        return r.stopped == null ? 0 : 1
    }

    /** Plan decision 7. Derived: a failure warns and the sweep's result stands. */
    private static void forgetAndSnapshot(CasSession cas, SweepReport r, PrintStream out) {
        final SnapshotBase base = cas.snapshotBase()
        final Index index = cas.openIndex()
        try {
            index.forget(r.deleted)
            final Set<String> failed = cas.catchUpIndex(index)
            final IndexSnapshot.Result s = cas.snapshotWritable(index, 0L, base, failed)
            out.println(s.skipped ? "snapshot   not rewritten: ${s.skipped}" :
                "snapshot   wrote ${cas.snapshotsOf(cas.config.writableAlias).describe()} (${s.runs} runs)")
        }
        catch( Exception e ) {
            out.println("snapshot   not rewritten: ${e.message}; run nf-blocks:snapshot")
        }
        finally {
            index.close()
        }
    }

    // --------------------------------------------------------------- prune

    static int prune(Options o, Map config, PrintStream out, PrintStream err, Closure<Long> clock) {
        if( o.positionals )
            throw new UsageException("prune takes no arguments, got ${o.positionals}")
        final boolean apply = bool(o, 'apply')
        final Integer keepLast = o.flag('keep-last') == null ? null : o.intFlag('keep-last', 0)
        final String keepNewerText = o.flag('keep-newer')
        Long keepNewerMillis = null
        if( keepNewerText != null ) {
            try {
                keepNewerMillis = Duration.of(keepNewerText).toMillis()
            }
            catch( IllegalArgumentException e ) {
                throw new UsageException("--keep-newer is a duration such as '30d'; got '${keepNewerText}'")
            }
        }
        if( keepLast == null && keepNewerMillis == null )
            throw new UsageException('prune needs --keep-last <n> or --keep-newer <period>, or both')
        final String pipeline = o.flag('pipeline')

        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            cas.catchUpIndex(index)
            final long now = clock.call()
            final List<PruneDecision> decisions = new Prune(index).plan(pipeline, keepLast, keepNewerMillis, now)
            printPlan(decisions, out)
            if( !apply )
                return 0
            cas.checkClock()
            final List<PruneDecision> releases = decisions.findAll { PruneDecision d -> d.action == 'release' }
            final Put builder = cas.newPut(index)
            int wrote = 0
            for( PruneDecision d : releases ) {
                try {
                    final PutResult saved = builder.put((Object) Prune.request(d, Index.isoMillis(clock.call())), false)
                    out.println(new String(saved.body(), 'UTF-8'))
                    wrote++
                }
                catch( PutError e ) {
                    out.println(new String(e.body(), 'UTF-8'))
                    err.println("nf-blocks:prune: wrote ${wrote} of ${releases.size()} Claims; stopped at ${d.run.runName}: ${e.message}")
                    return 1
                }
            }
            return 0
        }
        finally {
            index.close()
        }
    }

    private static void printPlan(List<PruneDecision> decisions, PrintStream out) {
        for( PruneDecision d : decisions )
            out.println("${d.action}  ${d.pipeline}  ${d.run.runName}  ${d.run.finishedAt}  ${d.run.completion}  ${d.reason}")
        final int release = (int) decisions.count { PruneDecision d -> d.action == 'release' }
        final int keep = (int) decisions.count { PruneDecision d -> d.action == 'keep' }
        final int skip = (int) decisions.count { PruneDecision d -> d.action == 'skip' }
        out.println("${decisions.size()} run(s): ${release} to release, ${keep} to keep, ${skip} skipped")
    }

    // ------------------------------------------------------------ untrash

    static int untrash(Options o, Map config, PrintStream out, PrintStream err) {
        final String sweepId = o.flag('sweep')
        if( sweepId != null && o.positionals )
            throw new UsageException('untrash takes cids or --sweep <id>, not both')
        if( sweepId == null && !o.positionals )
            throw new UsageException('untrash needs one or more cids, or --sweep <id>')
        final List<Cid> named = []
        for( String p : o.positionals ) {
            if( !Cid.isCid(p) )
                throw new UsageException("'${p}' is not a cid")
            named.add(Cid.parse(p))
        }
        final Set<Cid> namedSet = new LinkedHashSet<Cid>(named)

        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final BlockStore writable = cas.members()[0]
        if( !(writable instanceof RetainedStore) )
            throw new IllegalStateException("store member '${writable.alias()}' has no retention objects to untrash")
        final RetentionStorage storage = ((RetainedStore) writable).retentionStorage()

        final Closure<Long> localClock = { -> System.currentTimeMillis() } as Closure<Long>
        final SweepLock lock = new SweepLock(storage, SweepLock.newSweepId(System.currentTimeMillis()), localClock)
        final SweepLock.Holder holder = lock.take()
        if( holder != null ) {
            err.println("nf-blocks:untrash: sweep ${holder.sweepId} holds sweep.lock in ${storage.describe()} (heartbeat ${holder.ageMillis.intdiv(1000L)} s ago)")
            return 1
        }
        try {
            final Map<String, Integer> takenByLedger = new LinkedHashMap<String, Integer>()
            final Set<Cid> found = new LinkedHashSet<Cid>()
            for( String name : storage.listLedgers() ) {
                final byte[] body = storage.readLedger(name)
                if( body == null )
                    continue
                TrashLedger ledger = null
                try {
                    ledger = TrashLedger.parse(name, body)
                }
                catch( IllegalArgumentException e ) {
                    err.println("nf-blocks:untrash: skipping trash/${name}: ${e.message}")
                    continue
                }
                if( sweepId != null ) {
                    if( ledger.sweepId != sweepId )
                        continue
                    takenByLedger[name] = ledger.blocks.size()
                    storage.deleteLedger(name)
                }
                else {
                    final Set<Cid> present = new LinkedHashSet<Cid>(ledger.blocks.keySet())
                    present.retainAll(namedSet)
                    if( present.isEmpty() )
                        continue
                    found.addAll(present)
                    takenByLedger[name] = present.size()
                    final TrashLedger rest = ledger.without(present)
                    if( rest.blocks.isEmpty() )
                        storage.deleteLedger(name)
                    else
                        storage.writeLedger(name, rest.toJson())
                }
            }
            if( sweepId != null && takenByLedger.isEmpty() ) {
                err.println("nf-blocks:untrash: no Trash ledger for sweep '${sweepId}'")
                return 1
            }
            if( sweepId == null && found.isEmpty() ) {
                err.println('nf-blocks:untrash: none of the given addresses are in any Trash ledger')
                return 1
            }
            for( Map.Entry<String, Integer> e : takenByLedger.entrySet() )
                out.println("untrash    took ${e.value} block(s) out of trash/${e.key}")
            if( sweepId == null )
                for( Cid c : named )
                    if( !found.contains(c) )
                        out.println("untrash    ${c} is not in any Trash ledger")
            err.println('a block nothing reaches is trashed again by the next sweep; to keep it, pin it (put add pin) or restore its run (put del retain)')
            return 0
        }
        finally {
            lock.release()
        }
    }

    // -------------------------------------------------------------- helpers

    /** A bare flag (`--apply`, `--wait`): its value, if given at all, is `true` or `false`, as `put --dry-run`. */
    private static boolean bool(Options o, String name) {
        final String v = o.flag(name)
        if( !(v in [null, 'true', 'false']) )
            throw new UsageException("--${name} is a flag, got '${v}'")
        return v == 'true'
    }

    private static long bytes(String text) {
        try {
            return new MemoryUnit(text).toBytes()
        }
        catch( Exception e ) {
            throw new UsageException("--budget is a size such as '10 GB' or '500MB'; got '${text}': ${e.message}")
        }
    }

    /** cas.sweep.ageFloor and cas.sweep.grace, with warnings on stderr. A bad value is an IllegalArgumentException: exit 1. */
    private static SweepPolicy policy(Map config, PrintStream err) {
        final SweepPolicy p = SweepSettings.of(config)
        for( String w : p.warnings() )
            err.println("nf-blocks:sweep: ${w}")
        return p
    }
}
