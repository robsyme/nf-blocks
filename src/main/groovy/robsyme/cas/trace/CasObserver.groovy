package robsyme.cas.trace

import java.nio.file.Path
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.config.Manifest
import nextflow.exception.AbortRunException
import nextflow.script.WorkflowMetadata
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.TaskEvent
import nextflow.trace.event.WorkflowOutputEvent
import nextflow.util.ConfigHelper
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.nio.CasPath
import robsyme.cas.core.Anomalies
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Coordinates
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunCompletion
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.RunManifest

/**
 * Turns a run's publishes into a queryable run (DESIGN.md §11): it writes the
 * RunManifest, captures each workflow output, and at {@code onFlowComplete} --
 * the one point where the publish pool is drained (ticket 07) -- joins them
 * into OutputItems, OutputCollections and a single RunCompletion, appends the
 * Run Log entry and updates the index.
 *
 * Rule 3 (DESIGN.md §0): a provenance-block write that fails aborts the run;
 * the index and the run log are derived, so their failures log and continue.
 */
@Slf4j
@CompileStatic
class CasObserver implements TraceObserverV2 {

    private Session session
    private CasSession cas

    /** output name -> the captured (already target-normalised) value, for the join. */
    private final Map<String, Object> capturedOutputs = new LinkedHashMap<String, Object>()

    /** join key -> the labels seen on its publish event, kept for the index layer. */
    private final ConcurrentHashMap<String, List<String>> labels = new ConcurrentHashMap<>()

    /** The missing-fromStore hint is logged once per run, however often onFlowError fires. */
    private final AtomicBoolean hinted = new AtomicBoolean(false)

    // --------------------------------------------------------------- lifecycle

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.cas = CasSession.of(session)
        validateOutputDir()
    }

    /**
     * The writable member is the alias in {@code lineage.store.location};
     * {@code outputDir} must name the same alias or provenance would split
     * across two stores (DESIGN.md §2). An unset {@code outputDir} was given
     * the alias by {@link CasObserverFactory#defaultOutputDir}, which sets
     * {@code session.outputDir} and leaves the config as written, so that is
     * what is judged when the config names none.
     */
    private void validateOutputDir() {
        final String configured = session.config?.get('outputDir') as String
        final String outputDir = configured ?: defaultedOutputDir()
        final String outputAlias = CasConfig.aliasOf(outputDir)
        if( outputAlias != cas.config.writableAlias )
            throw new AbortRunException("outputDir must publish through the lineage store 'cas://${cas.config.writableAlias}', but is '${configured ?: 'unset'}'")
    }

    /** {@code session.outputDir} when the factory defaulted it to a cas path, else null. */
    private String defaultedOutputDir() {
        final Path current = session.outputDir
        return current instanceof CasPath ? current.toString() : null
    }

    @Override
    void onFlowBegin() {
        // The Nextflow run key (LinObserver.executionHash) is set when
        // CasLinStore.save(<hash>, WorkflowRun) runs during LinObserver's own
        // create. If it has not arrived, the manifest is deferred to the join.
        if( cas.getNextflowRunKey() == null ) {
            log.debug('nextflow run key not yet known at onFlowBegin; deferring the run manifest to the join')
            return
        }
        writeRunManifest()
    }

    @Override
    void onFilePublish(FilePublishEvent event) {
        final Path target = event.target
        if( target == null || !isCasTarget(target) )
            return
        final String key = Coordinates.key(target)
        if( event.labels )
            labels.put(key, event.labels)
        // Our upload() already hashed and recorded this; if neither the publish
        // nor its durable pointer file names the coordinate, provenance is lost.
        if( cas.publishFor(key) != null )
            return
        if( cas.coordinates.read(relPathOf(key)).isPresent() )
            return
        throw new AbortRunException("Published output '${target}' has no recorded address: neither a publish this run nor a pointer file names it")
    }

    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {
        capturedOutputs.put(event.name, event.value)
    }

    @Override
    void onTaskCached(TaskEvent event) {
        // Skeleton: record only. Address reuse by task hash is a later task.
        log.debug("cached task ${event?.handler?.task?.hash}")
    }

    /**
     * On a run that is already failing ({@code session.error} set by
     * Session.abort, Session.groovy:819, before it notifies completion at 830),
     * a failure here is logged rather than thrown: an AbortRunException would
     * stop Session.notifyEvent (Session.groovy:1125-1128) before notifyError
     * (831) reaches any observer, losing the fromStore hint and the user's own
     * onError (ticket 07 Q2). A run with no error keeps rule 3's abort.
     */
    @Override
    void onFlowComplete() {
        final Throwable failing = session?.error
        if( failing == null ) {
            completeRun()
            return
        }
        try {
            completeRun()
        }
        catch( Exception e ) {
            log.warn("the run is already failing (${failing.message ?: failing.class.name}), and nf-blocks could not record it either: ${e.message}", e)
        }
    }

    private void completeRun() {
        // Fires twice on a failed run (no barrier on that path); the latch keeps
        // exactly one RunCompletion.
        if( !cas.claimCompletion() ) {
            // The other notification is writing the RunCompletion. A failed
            // run's second notification comes from Session.destroy on main,
            // which reaches System.exit next, so wait until the write is done.
            if( !cas.awaitCompletionWritten(COMPLETION_WAIT_MILLIS) )
                log.warn("the run's RunCompletion was still being written after ${COMPLETION_WAIT_MILLIS} ms; not waiting longer")
            return
        }
        try {
            runUninterrupted { writeCompletion() }
        }
        finally {
            cas.completionWritten()
        }
    }

    /**
     * Session.abort calls this after onFlowComplete with {@code session.error}
     * already set (Session.groovy:830-831), before the launcher prints the
     * error. The hint goes to {@link ConsoleLog}, a logger Nextflow's console
     * filter admits, so it is printed on the terminal just above that error
     * (and in {@code .nextflow.log}). The event carries no handler there, so
     * the error is read from the session.
     */
    @Override
    void onFlowError(TaskEvent event) {
        final String hint = FromStoreHint.of(session?.error)
        if( hint != null && hinted.compareAndSet(false, true) )
            ConsoleLog.LOG.warn(hint)
    }

    /** How long the losing notification waits for the winner's write. */
    static final long COMPLETION_WAIT_MILLIS = 60_000L

    /**
     * Nextflow shuts its executors down, interrupting their threads, while a
     * failed run is still notifying observers on one of them. An interrupt that
     * lands mid-write would lose the RunCompletion: the latch is already
     * claimed, so the second notification writes nothing. The provenance is
     * therefore written on a thread of our own; this one waits for it without
     * honouring the interrupt, then hands the interrupt back.
     */
    private static void runUninterrupted(Closure body) {
        final Throwable[] failure = new Throwable[1]
        final Thread worker = new Thread({
            try {
                body.call()
            }
            catch( Throwable t ) {
                failure[0] = t
            }
        } as Runnable, 'nf-blocks-completion')
        worker.start()
        boolean interrupted = false
        while( true ) {
            try {
                worker.join()
                break
            }
            catch( InterruptedException e ) {
                interrupted = true
            }
        }
        if( interrupted || Thread.currentThread().isInterrupted() )
            Thread.currentThread().interrupt()
        final Throwable thrown = failure[0]
        if( thrown instanceof RuntimeException )
            throw (RuntimeException) thrown
        if( thrown instanceof Error )
            throw (Error) thrown
        if( thrown != null )
            throw new AbortRunException("Unable to write the run's provenance: ${thrown.message}", thrown)
    }

    private void writeCompletion() {
        final Cid manifest = ensureManifest()
        final Join.Result joined = Join.join(capturedOutputs, cas)

        final List<Cid> collections = new ArrayList<Cid>()
        final Map<String, Cid> byName = new TreeMap<String, Cid>()
        for( Join.JoinedOutput output : joined.outputs ) {
            for( OutputItem item : output.items )
                putBlock(item.toCbor(), 'OutputItem')
            byName.put(output.name, putBlock(output.collection.toCbor(), 'OutputCollection'))
        }
        // collections sorted by output name (DESIGN.md §6).
        collections.addAll(byName.values())

        final boolean success = session.isSuccess()
        final WorkflowMetadata meta = session.workflowMetadata
        final Cid completion = putBlock(new RunCompletion([
            assertedBy        : cas.assertedBy,
            run               : manifest,
            collections       : collections,
            inputSet          : null,
            status            : success ? RunCompletion.SUCCEEDED : RunCompletion.FAILED,
            exitStatus        : meta?.exitStatus,
            possiblyIncomplete: !success,
            startedAt         : iso(meta?.start),
            finishedAt        : iso(meta?.complete),
            anomalies         : joined.anomalies ?: Anomalies.NONE,
            error             : success ? null : (meta?.errorMessage ?: null),
        ]).toCbor(), 'RunCompletion')

        appendStoreLog(completion)
        indexRun(completion)
    }

    // --------------------------------------------------------- run manifest

    private Cid ensureManifest() {
        final Cid existing = cas.getRunManifest()
        return existing != null ? existing : writeRunManifest()
    }

    /**
     * The run's config as text for the RunManifest (DESIGN.md §6): Nextflow's
     * own resolved config, the text Platform receives as `configText`, with
     * closures rendered as source and Nextflow's SECRET_KEYS masked. `CmdRun`
     * computes it whenever `lineage.enabled` is set; the fallback renders
     * `session.config` the same canonical way, closures as their object text,
     * masking nothing. Either way RunManifest runs it through
     * Records.scrubConfigText, which redacts secret-named keys' values, paths,
     * non-portable URIs and the user name.
     */
    private String configText() {
        if( session.resolvedConfig != null )
            return session.resolvedConfig
        return ConfigHelper.toCanonicalString((session.config ?: [:]) as Map)
    }

    private Cid writeRunManifest() {
        final WorkflowMetadata meta = session.workflowMetadata
        final Manifest manifest = meta?.manifest
        final Cid cid = putBlock(new RunManifest([
            assertedBy     : cas.assertedBy,
            pipeline       : pipelineIdentity(meta, manifest),
            repository     : meta?.repository,
            revision       : meta?.revision,
            commitId       : meta?.commitId,
            runName        : runName(meta),
            nfRunHash      : cas.getNextflowRunKey() ?: 'unknown',
            sessionId      : session.uniqueId?.toString() ?: 'unknown',
            resumed        : (meta?.resume ?: session.resumeMode),
            nextflowVersion: meta?.nextflow?.version?.toString() ?: 'unknown',
            params         : (session.params ?: [:]) as Map,
            config         : configText(),
            script         : null,
            startedAt      : iso(meta?.start),
        ]).toCbor(), 'RunManifest')
        cas.setRunManifest(cid)
        return cid
    }

    private String pipelineIdentity(WorkflowMetadata meta, Manifest manifest) {
        final String override = navigate('cas.pipeline')
        return override ?: (manifest?.name ?: (meta?.projectName ?: 'unknown'))
    }

    private String runName(WorkflowMetadata meta) {
        return session.runName ?: (meta?.runName ?: 'run')
    }

    // ------------------------------------------------------------- derived

    /**
     * Updates the derived index for this run. A failure here logs and marks the
     * index stale; it never aborts (DESIGN.md §11, Rule 3).
     */
    private void indexRun(Cid completion) {
        Index index = null
        try {
            index = openIndex()
            index.ingestRun(cas.store, completion, cas.config.writableAlias)
            // Also fold in any read-only members' run logs, so this user's index
            // reflects the whole composition and not only what this run wrote.
            cas.catchUpIndex(index)
            writeSnapshot(index)
        }
        catch( Exception e ) {
            log.warn("the index could not be updated for run ${completion}; it is derived and can be rebuilt: ${e.message}", e)
            try {
                index?.markStale()
            }
            catch( Exception ignored ) {
            }
        }
        finally {
            index?.close()
        }
    }

    /** Overridable seam so a test can inject an index failure (DESIGN.md §11). */
    protected Index openIndex() {
        return cas.openIndex()
    }

    /** The member's Index Snapshot, under the cap (DESIGN.md §15). Derived: a failure only warns. */
    private void writeSnapshot(Index index) {
        try {
            final IndexSnapshot.Result result = cas.snapshotWritable(index, cas.config.snapshotMaxBytes)
            if( result.skipped )
                log.info("the Index Snapshot of store '${cas.config.writableAlias}' is over cas.snapshot.maxBytes " +
                    "(${cas.config.snapshotMaxBytes} bytes) and was not rewritten; `nextflow plugin nf-blocks:snapshot` rewrites it at any size")
        }
        catch( Exception e ) {
            log.warn("the Index Snapshot of store '${cas.config.writableAlias}' could not be written; it is derived: ${e.message}", e)
        }
    }

    private void appendStoreLog(Cid completion) {
        try {
            StoreLog.append(cas.store, StoreLogKind.RUN, completion, nowMillis())
        }
        catch( Exception e ) {
            log.warn("the store log entry for ${completion} could not be written; it is derived: ${e.message}", e)
        }
    }

    /** When the entry is written; overridable so a test can fix the clock. */
    protected long nowMillis() {
        return System.currentTimeMillis()
    }

    // ------------------------------------------------------------- plumbing

    /** Writes a provenance block; a failure here aborts the run (Rule 3). */
    private Cid putBlock(Object cbor, String kind) {
        try {
            return cas.store.putDagCbor(cbor)
        }
        catch( Exception e ) {
            throw new AbortRunException("Unable to write the ${kind} provenance block: ${e.message}", e)
        }
    }

    private String navigate(String dottedKey) {
        Object node = session.config
        for( String segment : dottedKey.split('\\.') ) {
            if( !(node instanceof Map) )
                return null
            node = ((Map) node).get(segment)
        }
        return node == null ? null : node.toString()
    }

    private static boolean isCasTarget(Path target) {
        return target.getFileSystem()?.provider()?.getScheme() == Coordinates.SCHEME
    }

    private static String relPathOf(String key) {
        final String prefix = Coordinates.SCHEME + '://'
        final String rest = key.substring(prefix.length())
        final int slash = rest.indexOf('/')
        return slash < 0 ? '' : rest.substring(slash + 1)
    }

    private static String iso(OffsetDateTime when) {
        return (when ?: OffsetDateTime.now()).toInstant().truncatedTo(ChronoUnit.MILLIS).toString()
    }
}
