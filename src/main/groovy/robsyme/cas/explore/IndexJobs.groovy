package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import robsyme.cas.CasSession
import robsyme.cas.CopyErrors
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Index
import robsyme.cas.core.IndexProgress
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.StoreIds
import robsyme.cas.core.StoreLog
import robsyme.cas.s3.S3Failures

/**
 * Each member's index state in the explorer, and the one job thread that
 * indexes a member, pushes its snapshot and refreshes a local snapshot
 * (background store indexing, spec §5, §6.3 to §6.6). A scan writes a scratch
 * Index file; explore's cache gets the member's rows in one seed at the end,
 * under the explorer's Put, so a save is never held behind a scan and a
 * stopped or failed scan leaves the member as it was.
 */
@Slf4j
@CompileStatic
class IndexJobs {

    static final String SNAPSHOT = 'snapshot'
    static final String LOCAL = 'local'
    static final String NONE = 'none'
    static final String INDEXING = 'indexing'
    static final String FAILED = 'failed'
    static final String OUTSIDE = 'outside'

    static final String MODE_LOCAL = 'local'
    static final String MODE_PUSH = 'push'

    static final String RUNNING = 'another store is being indexed; try again when it has finished'
    /** Why a copy leaves out a member whose first local snapshot is still being built. */
    static final String BEING_INDEXED = 'it is being indexed'
    static final String NOT_READ = 'could not be read at start'

    /** What a refused push says, by the guard that kept the member's own snapshot. */
    private static final Map<String, String> GUARD_REASONS = [
        (IndexSnapshot.FEWER_RUNS)        : "the store's own snapshot lists more runs than this index, so it was kept",
        (IndexSnapshot.OLDER_WATERMARK)   : "the store's own snapshot is newer than this index, so it was kept",
        (IndexSnapshot.REPLACED_MEANWHILE): "another writer replaced the store's snapshot meanwhile, so it was kept",
        (IndexSnapshot.STALE_STORE)       : 'the store was replaced while it was being indexed',
    ].asImmutable()

    /** What {@link #start} answers: an HTTP status and its JSON body. */
    @CompileStatic
    static final class Answer {
        final int status
        final Map<String, Object> body

        Answer(int status, Map<String, Object> body) {
            this.status = status
            this.body = body
        }
    }

    /** One member's state, never changed: a new one replaces it whole. */
    @CompileStatic
    private static final class State {
        final String state
        final Long at
        final String reason
        final String mode
        final boolean local
        /** Null unless indexing. */
        final String phase
        final long done
        final long total
        final Long startedAt

        State(String state, Long at, String reason, String mode, boolean local) {
            this(state, at, reason, mode, local, null, -1L, -1L, null)
        }

        State(String state, Long at, String reason, String mode, boolean local, String phase, long done, long total, Long startedAt) {
            this.state = state
            this.at = at
            this.reason = reason
            this.mode = mode
            this.local = local
            this.phase = phase
            this.done = done
            this.total = total
            this.startedAt = startedAt
        }

        /** True when this state serves the member's local snapshot. */
        boolean servesLocal() {
            return state == LOCAL || (local && (state == INDEXING || state == FAILED))
        }

        Map<String, Object> toMap() {
            final Map<String, Object> progress = phase == null ? null
                : [phase: phase, done: done, total: total, startedAt: Index.isoMillis(startedAt)] as Map<String, Object>
            return [state: state, at: at == null ? null : Index.isoMillis(at), progress: progress, reason: reason, mode: mode,
                    local: local] as Map<String, Object>
        }
    }

    /** The job on the thread: its member, its mode, the state it started from, and whether a local snapshot is served. */
    @CompileStatic
    private static final class Run {
        final String alias
        final String mode
        final State before
        final long startedAt
        volatile boolean local
        /** True once explore's cache holds the member this job scanned. */
        volatile boolean seeded

        Run(String alias, String mode, State before, long startedAt) {
            this.alias = alias
            this.mode = mode
            this.before = before
            this.startedAt = startedAt
            this.local = before.servesLocal()
        }
    }

    private final CasSession session
    private final Index shared
    private final Object putLock
    private final Path localRoot
    private final Map<String, ExploreServer.MemberInfo> info

    @PackageScope Closure<Long> clock = { -> System.currentTimeMillis() }
    /** Called on the job thread with each (phase, done, total), after the state has it. */
    @PackageScope Closure onReport
    /** Called on the job thread once explore's cache holds the scanned member, before its local snapshot is placed. */
    @PackageScope Closure onSeeded

    private final Map<String, State> states = new ConcurrentHashMap<String, State>()

    /** Guards {@link #queued}, {@link #running} and {@link #started}; notified when a job ends. */
    private final Object lock = new Object()
    /** Members whose local snapshot waits for a refresh, in order. */
    private final LinkedHashSet<String> queued = new LinkedHashSet<String>()
    /** The job on the thread, or submitted to it, or null. */
    private Run running
    private boolean started
    /** Set once by {@link #stop}: a job that ends after it was stopped, however it ends. */
    private volatile boolean stopping

    @PackageScope final ExecutorService thread = Executors.newSingleThreadExecutor({ Runnable r ->
        final Thread t = new Thread(r, 'nf-blocks-index')
        t.daemon = true
        return t
    } as ThreadFactory)

    IndexJobs(CasSession session, Index shared, Object putLock, Path localRoot, Map<String, ExploreServer.MemberInfo> info) {
        this.session = session
        this.shared = shared
        this.putLock = putLock
        this.localRoot = localRoot
        this.info = info ?: Collections.<String, ExploreServer.MemberInfo> emptyMap()
    }

    /** The member's index object for members.json and /api/index, every key present; null for an unknown alias. */
    Map<String, Object> stateOf(String alias) {
        return current(alias)?.toMap()
    }

    /**
     * The member's state, read as none when it serves a local snapshot whose
     * file is gone: a cache reset deletes them all, and the cache no longer
     * holds the member. A running job's state stands until the job ends.
     */
    private State current(String alias) {
        final State s = states.get(alias)
        if( s == null || s.state == INDEXING || !s.servesLocal() || Files.isRegularFile(localFile(alias)) )
            return s
        return new State(NONE, null, session.unindexed().get(alias) ?: 'absent', null, false)
    }

    /** Every configured member's index object, in config order. */
    Map<String, Map<String, Object>> states() {
        final Map<String, Map<String, Object>> all = new LinkedHashMap<String, Map<String, Object>>()
        for( String alias : session.config.configuredAliases )
            all.put(alias, stateOf(alias))
        return all
    }

    /**
     * Each member's state at startup, after the session's first catch-up with
     * {@link CasSession#leaveUnindexedOut} on. A member the cache holds with no
     * usable snapshot of its own gets a refresh queued, which
     * {@link #startQueued} begins.
     */
    void classify() {
        final Map<String, String> unindexed = session.unindexed()
        for( String alias : session.config.configuredAliases ) {
            if( alias == session.config.writableAlias ) {
                states.put(alias, new State(SNAPSHOT, null, null, null, false))
            }
            else if( !session.config.members.contains(alias) ) {
                states.put(alias, new State(OUTSIDE, null, null, null, false))
            }
            else if( unindexed.containsKey(alias) ) {
                // The cache does not hold it, so a local snapshot left from an earlier cache is not served.
                deleteLocal(alias)
                states.put(alias, new State(NONE, null, unindexed.get(alias), null, false))
            }
            else if( shared.isIndexed(alias) ) {
                classifyHeld(alias)
            }
            else {
                states.put(alias, new State(NONE, null, NOT_READ, null, false))
            }
        }
    }

    private void classifyHeld(String alias) {
        final String unusable = session.ownSnapshotUnusable(alias, shared)
        if( unusable == null ) {
            deleteLocal(alias)
            states.put(alias, new State(SNAPSHOT, null, null, null, false))
            return
        }
        // A snapshot that could not be looked at (a refused HEAD) is served as before indexing existed, and fails there if it must.
        if( unusable == CasSession.UNREADABLE ) {
            states.put(alias, new State(SNAPSHOT, null, null, null, false))
            return
        }
        final Path file = localFile(alias)
        if( Files.isRegularFile(file) )
            states.put(alias, new State(LOCAL, Files.getLastModifiedTime(file).toMillis(), null, null, true))
        else
            states.put(alias, new State(INDEXING, null, null, MODE_LOCAL, false, IndexProgress.LOG, -1L, -1L, clock.call()))
        synchronized( lock ) {
            queued.add(alias)
        }
    }

    /** Begins the refreshes {@link #classify} and {@link #copied} queued, one after another. */
    void startQueued() {
        synchronized( lock ) {
            started = true
            next()
        }
    }

    /** POST /api/index: starts indexing {@code alias} in {@code mode}, or says why not. */
    Answer start(String alias, String mode) {
        if( !session.config.configuredAliases.contains(alias) )
            return refused(400, 'unknown_member', 'there is no configured store member by that name')
        if( alias == session.config.writableAlias )
            return refused(400, 'writable_member', 'the writable member is indexed when the explorer starts')
        if( mode != MODE_LOCAL && mode != MODE_PUSH )
            return refused(400, 'bad_request', 'mode must be local or push')
        if( states.get(alias)?.state == OUTSIDE )
            return refused(400, 'bad_request', "'${alias}' is not in cas.resolve, so the explorer does not index it".toString())
        final ExploreServer.MemberInfo member = info.get(alias)
        if( mode == MODE_PUSH && member != null && !member.copyTarget )
            return refused(400, 'not_writable', member.copyReason ?: 'this store cannot take a write')
        // Asked outside the job lock: a caller holding Put may wait on that lock (copied).
        final boolean held
        synchronized( putLock ) {
            held = shared.isIndexed(alias)
        }
        synchronized( lock ) {
            if( running != null || !queued.isEmpty() || stopping )
                return refused(409, 'index_running', RUNNING)
            final State before = current(alias)
            if( before.state == SNAPSHOT || (mode == MODE_LOCAL && before.state == LOCAL) )
                return refused(400, 'already_indexed', "'${alias}' is already indexed".toString())
            final State begun
            if( !held )
                begun = submit(alias, mode, before, IndexProgress.LISTING) { Run run, IndexProgress progress -> index(run, progress) }
            else if( mode == MODE_PUSH )
                begun = submit(alias, mode, before, IndexProgress.LOG) { Run run, IndexProgress progress -> pushHeld(run, progress) }
            else
                begun = submit(alias, mode, before, IndexProgress.LOG) { Run run, IndexProgress progress -> refresh(run, progress) }
            return new Answer(202, [member: alias, mode: mode, index: begun.toMap()] as Map<String, Object>)
        }
    }

    private static Answer refused(int status, String error, String message) {
        return new Answer(status, [error: error, message: message] as Map<String, Object>)
    }

    /** The member's local snapshot file when it exists and its state serves it; else null. */
    Path localSnapshot(String alias) {
        final State s = current(alias)
        if( s == null || !s.servesLocal() )
            return null
        final Path file = localFile(alias)
        return Files.isRegularFile(file) ? file : null
    }

    /** True when the member serves its own snapshot, as before indexing existed. */
    boolean servesOwn(String alias) {
        final String state = states.get(alias)?.state
        return state == SNAPSHOT || state == OUTSIDE
    }

    /** After a real copy into {@code alias}: its local snapshot, when it serves one, is rebuilt. */
    void copied(String alias) {
        if( localSnapshot(alias) == null )
            return
        synchronized( lock ) {
            queued.add(alias)
            next()
        }
    }

    /** The members a copy must leave out, alias to reason: not indexed, failed with nothing served, or a first index running. */
    Map<String, String> unindexedForCopy() {
        final Map<String, String> out = new LinkedHashMap<String, String>()
        for( String alias : session.config.configuredAliases ) {
            final State s = current(alias)
            if( s == null || s.servesLocal() )
                continue
            if( s.state == NONE || s.state == FAILED )
                out.put(alias, s.reason)
            else if( s.state == INDEXING )
                out.put(alias, BEING_INDEXED)
        }
        return out
    }

    /** The members a copy seeds from their local snapshot, alias to the directory holding `index/v7.sqlite`. */
    Map<String, Path> localRootsForCopy() {
        final Map<String, Path> out = new LinkedHashMap<String, Path>()
        for( String alias : session.config.configuredAliases ) {
            if( localSnapshot(alias) != null )
                out.put(alias, localRoot.resolve(alias))
        }
        return out
    }

    /** Clears the queue, interrupts the running job and waits up to {@code waitMillis} for it to end. */
    void stop(long waitMillis) {
        synchronized( lock ) {
            queued.clear()
            stopping = true
        }
        final List<Runnable> unstarted = thread.shutdownNow()
        synchronized( lock ) {
            // A job submitted but never started has no thread left to end it.
            if( !unstarted.isEmpty() && running != null ) {
                states.put(running.alias, running.before)
                running = null
                lock.notifyAll()
            }
        }
        try {
            thread.awaitTermination(waitMillis, TimeUnit.MILLISECONDS)
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
        }
    }

    /** Waits until no job runs and none started is queued; false when {@code millis} pass first. */
    @PackageScope boolean awaitIdle(long millis) {
        final long deadline = System.currentTimeMillis() + millis
        synchronized( lock ) {
            while( running != null || (started && !stopping && !queued.isEmpty()) ) {
                final long left = deadline - System.currentTimeMillis()
                if( left <= 0 )
                    return false
                lock.wait(left)
            }
            return true
        }
    }

    /** A failure's reason for the page: the credentials wording, or the exception with local paths redacted. */
    String reasonFor(Throwable e) {
        return S3Failures.credentials(e) ?: CopyErrors.describe(e, CopyErrors.rootsOf(session.config.rawConfig))
    }

    // ------------------------------------------------------------------ jobs

    /** Under {@link #lock}: the member's state becomes indexing and {@code body} runs on the job thread. */
    private State submit(String alias, String mode, State before, String phase, Closure<Void> body) {
        final Run run = new Run(alias, mode, before, clock.call())
        final State begun = new State(INDEXING, before.at, null, mode, run.local, phase, -1L, -1L, run.startedAt)
        running = run
        states.put(alias, begun)
        final IndexProgress progress = { String p, long done, long total -> report(run, p, done, total) } as IndexProgress
        thread.execute({ -> runJob(run, progress, body) } as Runnable)
        return begun
    }

    /** Under {@link #lock}: the next queued refresh, once started and when no job runs. */
    private void next() {
        while( started && !stopping && running == null && !queued.isEmpty() ) {
            final String alias = queued.iterator().next()
            queued.remove(alias)
            final State before = current(alias)
            // Since it was queued the member may have been pushed, or have failed with nothing to refresh.
            if( before == null || !(before.servesLocal() || before.state == INDEXING) )
                continue
            submit(alias, MODE_LOCAL, before, IndexProgress.LOG) { Run run, IndexProgress progress -> refresh(run, progress) }
        }
    }

    private void runJob(Run run, IndexProgress progress, Closure<Void> body) {
        try {
            body.call(run, progress)
        }
        catch( Throwable e ) {
            if( stopping ) {
                states.put(run.alias, run.seeded ? stoppedAfterSeed(run) : run.before)
            }
            else {
                final String reason = reasonFor(e)
                log.warn("indexing store member '${run.alias}' failed: ${reason}")
                log.debug("indexing store member '${run.alias}' failed", e)
                states.put(run.alias, new State(FAILED, clock.call(), reason, run.mode, run.local && Files.isRegularFile(localFile(run.alias))))
            }
        }
        finally {
            synchronized( lock ) {
                running = null
                next()
                lock.notifyAll()
            }
        }
    }

    /**
     * A stop after explore's cache took the member never puts back the state
     * it started from: local when its snapshot was placed, else indexing with
     * a refresh queued, as {@link #classify} would find it.
     */
    private State stoppedAfterSeed(Run run) {
        final Path file = localFile(run.alias)
        if( Files.isRegularFile(file) )
            return new State(LOCAL, Files.getLastModifiedTime(file).toMillis(), null, run.mode, true)
        synchronized( lock ) {
            queued.add(run.alias)
        }
        return new State(INDEXING, null, null, MODE_LOCAL, false, IndexProgress.LOG, -1L, -1L, clock.call())
    }

    /** The progress callback: the state takes the step, then {@link #onReport} sees it; a stopped job throws. */
    private void report(Run run, String phase, long done, long total) {
        checkStopped()
        states.put(run.alias, new State(INDEXING, run.before.at, null, run.mode, run.local, phase, done, total, run.startedAt))
        final Closure observer = onReport
        if( observer != null )
            observer.call(phase, done, total)
    }

    private void checkStopped() {
        if( stopping || Thread.currentThread().isInterrupted() )
            throw new CancellationException('the index job was stopped')
    }

    /**
     * Index (and push): every block of the member into a scratch Index, its
     * snapshot built from that, then explore's cache seeded from the snapshot
     * in one transaction and the snapshot kept as the member's local one.
     */
    private void index(Run run, IndexProgress progress) {
        final String alias = run.alias
        final boolean push = run.mode == MODE_PUSH
        final Path tmp = session.config.tmpDir
        final BlockStore member = session.member(alias)
        final StoreLog storeLog = StoreLog.of(member)
        // Taken before the scan, so the push's guard covers whatever is written meanwhile.
        final SnapshotBase base = push ? pushBase(alias) : null
        final String storeId = push ? session.ensureStoreId(alias) : StoreIds.read(member)
        if( push )
            adoptStoreId(alias, storeId)
        Files.createDirectories(tmp)
        final Path file = tmp.resolve("nf-blocks-index-${alias}-${UUID.randomUUID()}.sqlite".toString())
        Index scratch = null
        try {
            scratch = Index.open(file)
            if( storeId != null )
                scratch.reconcileStoreIds([(alias): storeId], { String a, String w -> Boolean.TRUE } as Closure<Boolean>)
            scratch.indexMember(member, storeLog, alias, session.store, progress)
            progress.report(IndexProgress.SNAPSHOT, -1L, -1L)
            final IndexSnapshot.Built built = session.buildMemberSnapshot(scratch, alias)
            try {
                checkStopped()
                synchronized( putLock ) {
                    shared.seedFrom(built.file, alias, storeLog)
                }
                run.seeded = true
                final Closure seeded = onSeeded
                if( seeded != null )
                    seeded.call()
                placeWhole(alias, built)
                run.local = true
            }
            finally {
                Files.deleteIfExists(built.file)
            }
            if( !push ) {
                indexedLocally(run)
                return
            }
            progress.report(IndexProgress.PUSH, -1L, -1L)
            pushed(run, session.pushMemberSnapshot(scratch, alias, base, storeId))
        }
        finally {
            scratch?.close()
            for( String suffix : ['', '-journal', '-wal', '-shm'] )
                Files.deleteIfExists(file.resolveSibling(file.fileName.toString() + suffix))
        }
    }

    /** Push to store on a member the cache holds: top up, then write its snapshot from explore's cache, with no scan. */
    private void pushHeld(Run run, IndexProgress progress) {
        final String alias = run.alias
        final SnapshotBase base = pushBase(alias)
        final String storeId = session.ensureStoreId(alias)
        adoptStoreId(alias, storeId)
        progress.report(IndexProgress.LOG, -1L, -1L)
        catchUp(alias)
        progress.report(IndexProgress.PUSH, -1L, -1L)
        final Index own = session.openIndex()
        try {
            pushed(run, session.pushMemberSnapshot(own, alias, base, storeId))
        }
        finally {
            own.close()
        }
    }

    /** Refresh: top up explore's cache from the member's Store Log, then rebuild its local snapshot. */
    private void refresh(Run run, IndexProgress progress) {
        final String alias = run.alias
        progress.report(IndexProgress.LOG, -1L, -1L)
        catchUp(alias)
        progress.report(IndexProgress.SNAPSHOT, -1L, -1L)
        final Index own = session.openIndex()
        try {
            final IndexSnapshot.Built built = session.buildMemberSnapshot(own, alias)
            try {
                checkStopped()
                place(alias, built)
                run.local = true
            }
            finally {
                Files.deleteIfExists(built.file)
            }
        }
        finally {
            own.close()
        }
        indexedLocally(run)
    }

    /** Explore's catch-up, under Put as every writer of the cache; the member's own failure fails the job, with its cause. */
    private void catchUp(String alias) {
        final Exception cause
        synchronized( putLock ) {
            cause = session.catchUpIndex(shared).contains(alias) ? session.lastFailure(alias) : null
        }
        if( cause != null )
            throw new IOException("could not catch up the index from store member '${alias}': ${cause.message}".toString(), cause)
    }

    /**
     * A push's store-id in explore's cache before anything catches up: else a
     * member with no Store Log entries, whose rows here no watermark dates,
     * would read as replaced and the whole cache would be reset.
     */
    private void adoptStoreId(String alias, String storeId) {
        synchronized( putLock ) {
            shared.adoptStoreId(alias, storeId)
        }
    }

    /**
     * What a push guards the member's own snapshot as: none when that snapshot
     * is from another incarnation, which the explorer would not use and the
     * push is there to replace.
     */
    private SnapshotBase pushBase(String alias) {
        if( Index.isIncarnationReason(session.ownSnapshotUnusable(alias, shared)) )
            return null
        return session.snapshotsOf(alias).base(session.config.tmpDir)
    }

    private void indexedLocally(Run run) {
        states.put(run.alias, new State(LOCAL, clock.call(), null, run.mode, true))
    }

    /** A push's outcome: written, the member serves its own and the local one goes; refused, a failure with the guard's reason. */
    private void pushed(Run run, IndexSnapshot.Result result) {
        if( result.written ) {
            deleteLocal(run.alias)
            run.local = false
            states.put(run.alias, new State(SNAPSHOT, clock.call(), null, run.mode, false))
            return
        }
        final String reason = GUARD_REASONS.get(result.skipped) ?: result.skipped
        log.warn("the Index Snapshot of store member '${run.alias}' was not written: ${reason}")
        states.put(run.alias, new State(FAILED, clock.call(), reason, run.mode, run.local && Files.isRegularFile(localFile(run.alias))))
    }

    // ----------------------------------------------------------- local files

    private Path localFile(String alias) {
        return IndexSnapshot.pathIn(localRoot.resolve(alias))
    }

    /**
     * {@link #place}, with an interrupt that came before it held back until it
     * is done: once the cache holds the member, its local snapshot is placed
     * even when the job is being stopped.
     */
    private void placeWhole(String alias, IndexSnapshot.Built built) {
        final boolean interrupted = Thread.interrupted()
        try {
            place(alias, built)
        }
        finally {
            if( interrupted )
                Thread.currentThread().interrupt()
        }
    }

    /** Puts {@code built} in place as the member's local snapshot, atomically. */
    private void place(String alias, IndexSnapshot.Built built) {
        final SnapshotBase written = new SnapshotBase(null, built.bytes, built.runs, built.storeId, built.watermark, built.omitted == null)
        new LocalSnapshotStorage(localRoot.resolve(alias)).replace(built.file, written, null)
    }

    /** Removes the member's local snapshot directory; a leftover is never served, so a failure is only logged. */
    private void deleteLocal(String alias) {
        final Path dir = localRoot.resolve(alias)
        if( !Files.exists(dir) )
            return
        try {
            final Stream<Path> paths = Files.walk(dir)
            try {
                for( Path each : paths.sorted(Comparator.reverseOrder()).toList() )
                    Files.delete(each)
            }
            finally {
                paths.close()
            }
        }
        catch( IOException | UncheckedIOException e ) {
            log.debug("could not delete the local Index Snapshot of '${alias}' in ${dir}: ${e.message}")
        }
    }
}
