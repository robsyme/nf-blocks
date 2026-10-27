package robsyme.cas.trace

import java.nio.file.Path
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.config.Manifest
import nextflow.exception.AbortRunException
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
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
import robsyme.cas.core.InputSet
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.ItemLineage
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunCompletion
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.RunManifest
import robsyme.cas.core.SnapshotBase

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

    /**
     * Guards {@link #capturedOutputs} and {@link #capturedIndexes}. Nextflow
     * calls onWorkflowOutput from its dataflow actor threads, several within
     * milliseconds, and a plain map loses puts (the Gate's intermittent
     * checks 14 and 16). A lock rather than ConcurrentHashMap, because an
     * output's value may be null.
     */
    private final Object captureLock = new Object()

    /** output name -> the captured (already target-normalised) value, for the join. Guarded by captureLock. */
    private final Map<String, Object> capturedOutputs = new LinkedHashMap<String, Object>()

    /** output name -> the Output Index File Nextflow named for it (ticket 26). Guarded by captureLock. */
    private final Map<String, Path> capturedIndexes = new LinkedHashMap<String, Path>()

    /** join key -> the labels seen on its publish event, kept for the index layer. */
    private final ConcurrentHashMap<String, List<String>> labels = new ConcurrentHashMap<>()

    /** Every cas:// coordinate a publish event named this run, for the unjoined count (ticket 19). */
    private final Set<String> publishedKeys = ConcurrentHashMap.newKeySet()

    /** The missing-fromStore hint is logged once per run, however often onFlowError fires. */
    private final AtomicBoolean hinted = new AtomicBoolean(false)

    /**
     * The RunCompletion's cid, set the moment putBlock returns it (on the
     * nf-blocks-completion thread, read after the join). From then on the run
     * is recorded, and completeRun lets nothing that fails later escape
     * (fix round 3, G3).
     */
    private volatile Cid recordedAs

    /** Process names already warned about an unchained afterScript (Task 9 fix round 1), so onProcessCreate warns once each. */
    private final Set<String> nodeHashWarned = ConcurrentHashMap.newKeySet()

    /** Processes already warned about publishDir, so each is warned once. */
    private final Set<String> publishDirWarned = ConcurrentHashMap.newKeySet()

    /**
     * The files this run's tasks read, for its InputSet (explorer v0.3 P4
     * plan 1.3). Made on first use, by when the run's params are final.
     */
    private InputRecorder inputs

    /** Which run inputs and tasks gave each output item, for the run's ItemLineage (cross-run lineage spec §5.2). */
    private final ItemDerivation derivation = new ItemDerivation()

    /** Observers of this JVM whose RunCompletion is not yet written, for {@link #finishPending}. */
    private static final Set<CasObserver> PENDING = ConcurrentHashMap.newKeySet()

    // --------------------------------------------------------------- lifecycle

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.cas = CasSession.of(session)
        PENDING.add(this)
        validateOutputDir()
        // Ticket 03 decision 2: one HEAD on the writable S3 member, before any entry is stamped.
        cas.checkClock()
        // DESIGN.md §20 decision 2: the member's id exists before anything else this run writes there.
        cas.writableStoreId()
        // Ticket 20 answers 1 and 5: register as a Live Writer before any fromStore read or publish.
        final WorkflowMetadata meta = session.workflowMetadata
        cas.startLiveWriter(session.uniqueId.toString(), [
            run_name  : runName(meta),
            pipeline  : pipelineIdentity(meta, meta?.manifest),
            started_at: iso(meta?.start),
        ])
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
        // Resolve the identity now, before any task runs, so the "signing as" line or the
        // anonymous warning (and any slow Platform read) comes at the start of the run, not
        // at the join when the manifest below is deferred.
        cas.principal
        // The Nextflow run key (LinObserver.executionHash) is set when
        // CasLinStore.save(<hash>, WorkflowRun) runs during LinObserver's own
        // create. If it has not arrived, the manifest is deferred to the join.
        // params.input is queued now, so its stat and hash overlap with the run.
        recordSamplesheet()
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
        publishedKeys.add(key)
        if( event.labels )
            labels.put(key, event.labels)
        // An Output Index File has no source (Nextflow writes it straight to the target); notePublish ignores a null one.
        derivation.notePublish(key, event.source)
        // A file written by appends (a CSV Output Index File) is hashed now, once.
        finalizeSpools { cas.finalizePending(key) }
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
        synchronized( captureLock ) {
            capturedOutputs.put(event.name, event.value)
            if( event.index != null )
                capturedIndexes.put(event.name, event.index)
        }
    }

    /** A copy of the outputs captured so far, taken under the lock. */
    @groovy.transform.PackageScope
    Map<String, Object> capturedOutputs() {
        synchronized( captureLock ) {
            return new LinkedHashMap<String, Object>(capturedOutputs)
        }
    }

    /** A copy of the Output Index Files captured so far, taken under the lock. */
    @groovy.transform.PackageScope
    Map<String, Path> capturedIndexes() {
        synchronized( captureLock ) {
            return new LinkedHashMap<String, Path>(capturedIndexes)
        }
    }

    @Override
    void onTaskComplete(TaskEvent event) {
        recordInputs(event)
    }

    @Override
    void onTaskCached(TaskEvent event) {
        // Address reuse by task hash is a later task; a cached task's inputs are still inputs of this run.
        log.debug("cached task ${event?.handler?.task?.hash}")
        recordInputs(event)
    }

    /**
     * Runs on Nextflow's task-completion thread, so it only classifies the
     * task's staged files and queues their stat and hash (InputRecorder). The
     * InputSet is a record of what the run read, so a failure here is logged
     * and the run goes on. The task is noted for item lineage either way, with
     * whatever was staged, so its items still name it.
     */
    private void recordInputs(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        List<InputRecorder.Staged> staged = Collections.<InputRecorder.Staged> emptyList()
        try {
            staged = inputRecorder().recordTask(task)
        }
        catch( Exception e ) {
            log.warn("nf-blocks could not record the inputs of task ${taskName(task)}: ${e.message}", e)
        }
        try {
            derivation.noteTask(task, staged)
        }
        catch( Exception e ) {
            log.warn("nf-blocks could not note task ${taskName(task)} for item lineage: ${e.message}", e)
        }
    }

    /** The task's name for a log line; TaskRun.getName reads the task's config, which may not be there. */
    private static String taskName(TaskRun task) {
        try {
            return task?.name
        }
        catch( Exception ignored ) {
            return task?.hash?.toString()
        }
    }

    private void recordSamplesheet() {
        try {
            inputRecorder().recordSamplesheet()
        }
        catch( Exception e ) {
            log.warn("nf-blocks could not record params.input: ${e.message}", e)
        }
    }

    private synchronized InputRecorder inputRecorder() {
        if( inputs == null )
            inputs = newInputRecorder(session?.workDir, session?.baseDir, (session?.params ?: [:]) as Map, cas.config.inputsHash)
        return inputs
    }

    /** Overridable seam so a test can hand the observer a recorder that is slow or fails. */
    protected InputRecorder newInputRecorder(Path workDir, Path projectDir, Map params, boolean hash) {
        return new InputRecorder(workDir, projectDir, params, hash)
    }

    /** The run's InputSet with its block, and the Input each kept staged file became, for the item lineage. */
    @CompileStatic
    private static final class RunInputs {
        final InputSet set
        final Map<String, Object> block
        final Map<InputRecorder.Staged, InputSet.Input> byStaged

        RunInputs(InputSet set, Map<InputRecorder.Staged, InputSet.Input> byStaged) {
            this.set = set
            this.block = set.toCbor()
            this.byStaged = byStaged
        }
    }

    /**
     * Waits for the queued stats and hashes, up to {@code cas.inputs.waitSeconds} on a
     * successful run and at most {@link #FAILED_RUN_INPUT_WAIT_MILLIS} of it on
     * a failed one (inside the time the losing notification and the plugin
     * stop wait for the completion). An input whose stat or hash is still
     * running then is listed with reason "unfinished", and the console says
     * how many. The run's status and its RunCompletion are as they would
     * have been.
     *
     * Null when the InputSet could not be made: it records what the run read,
     * so a failure making it warns and the RunCompletion links none.
     */
    private RunInputs collectInputs(boolean success) {
        InputRecorder recorder = null
        try {
            recorder = inputRecorder()
            recordSamplesheet()
            final long waitMillis = inputWaitMillis(success)
            // Every fromStore read is over by the join, so the Origins are complete when collect attaches them.
            recorder.setOrigins(cas.origins)
            final List<InputSet.Input> read = recorder.collect(waitMillis)
            final int left = recorder.leftUnfinished
            if( left > 0 )
                ConsoleLog.LOG.warn("nf-blocks: ${left} input(s) were still being read (a stat or a hash) after ${waitMillis.intdiv(1000L)} s, so the run records them without an address, as unfinished; " +
                    "cas.inputs.waitSeconds (${cas.config.inputsWaitSeconds}) sets how long a run waits${success ? '' : ', at most 30 s for a failed run'}")
            return new RunInputs(new InputSet(cas.assertedBy, read), recorder.collectedByStaged)
        }
        catch( Throwable t ) {
            ConsoleLog.LOG.warn("nf-blocks: the run's InputSet could not be made, so its RunCompletion records no inputs: ${t.message ?: t.class.name}")
            log.warn("the run's InputSet could not be made: ${t.message ?: t.class.name}", t)
            return null
        }
        finally {
            try {
                recorder?.close()
            }
            catch( Throwable t ) {
                log.debug("the input hashing pool could not be stopped: ${t.message}")
            }
        }
    }

    /**
     * The run's ItemLineage block, or null when it could not be made: like the
     * InputSet it names, it records where the outputs came from, so a failure
     * making it warns and the RunCompletion links none. Writing the block is a
     * provenance write, and its failure aborts the run (rule 3).
     */
    private Cid writeItemLineage(List<Join.JoinedOutput> outputs, RunInputs inputs, Cid inputSet) {
        final Map<String, Object> block
        try {
            block = new ItemLineage(cas.assertedBy, inputSet, deriveItems(outputs, inputs.set, inputs.byStaged)).toCbor()
        }
        catch( Throwable t ) {
            ConsoleLog.LOG.warn("nf-blocks: the run's item lineage could not be made, so its RunCompletion records none: ${t.message ?: t.class.name}")
            log.warn("the run's item lineage could not be made: ${t.message ?: t.class.name}", t)
            return null
        }
        return putBlock(block, 'ItemLineage')
    }

    private long inputWaitMillis(boolean success) {
        final long configured = cas.config.inputsWaitSeconds * 1000L
        return success ? configured : Math.min(FAILED_RUN_INPUT_WAIT_MILLIS, configured)
    }

    /**
     * The publishDir warning comes first and does not depend on node hashing:
     * a process may declare publishDir with node hashing off entirely.
     */
    @Override
    void onProcessCreate(TaskProcessor process) {
        warnPublishDir(process)
        warnNodeHash(process)
    }

    /** Ticket 19 answer 2: lineage comes from workflow outputs only, so a publishDir process is told once. */
    private void warnPublishDir(TaskProcessor process) {
        final String name = process?.name
        if( name == null )
            return
        final Object declared = process.config?.get('publishDir')
        if( !declared || !publishDirWarned.add(name) )
            return
        ConsoleLog.LOG.warn("nf-blocks: process '${name}' uses publishDir; the files it publishes are stored but no run records them. " +
            "Declare them as workflow outputs (output { }) to keep their lineage")
    }

    /**
     * NodeHash.install (Task 9) chains its script into config.process and each
     * selector, but ProcessConfigBuilder.applyConfigDefaults only applies a
     * process-scope default when the process definition sets no afterScript of
     * its own ({@code !config.containsKey(key)},
     * ProcessConfigBuilder.groovy:231 at v26.04.6): a process whose body sets
     * `afterScript` directly gets none of ours chained ahead of it, so it is
     * not hashed on the node though `cas.nodeHash`/`fusion.enabled` says it
     * should be. Warned once per process name through {@link ConsoleLog}
     * rather than turned into a run failure: the head node still addresses
     * that process's outputs.
     */
    private void warnNodeHash(TaskProcessor process) {
        final Map config = session?.config
        if( config == null || !CasConfig.nodeHashEnabled(config) )
            return
        final String name = process?.name
        if( name == null || !nodeHashWarned.add(name) )
            return
        final Object afterScript = process.config?.get('afterScript')
        if( afterScript instanceof CharSequence && afterScript.toString().startsWith(NodeHash.script()) )
            return
        ConsoleLog.LOG.warn("nf-blocks: process '${name}' sets its own afterScript, so node hashing is not chained ahead of it; its outputs are addressed on the head node")
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
            // Either way this observer has nothing left for the plugin stop to do.
            PENDING.remove(this)
            if( !cas.awaitCompletionWritten(COMPLETION_WAIT_MILLIS) )
                log.warn("the run's RunCompletion was still being written after ${COMPLETION_WAIT_MILLIS} ms; not waiting longer")
            return
        }
        try {
            runUninterrupted { writeCompletion() }
        }
        catch( Throwable t ) {
            // Once the RunCompletion is put the run is recorded: whatever fails
            // after it (a derived write, a log line, even an OutOfMemoryError in
            // a catch) only warns (fix round 3, G3).
            final Cid recorded = recordedAs
            if( recorded != null ) {
                warnRecorded(recorded, t)
                return
            }
            // Session.notifyEvent logs anything but an AbortRunException at debug
            // (Session.groovy:1125-1128), so the user would never see the run go
            // unrecorded. Say so on the terminal, then fail as before (§0 rule 3).
            ConsoleLog.LOG.error("nf-blocks: run '${runName(session?.workflowMetadata)}' was not recorded in the store; " +
                "its RunCompletion could not be written: ${t.message ?: t.class.name}")
            throw t
        }
        finally {
            try {
                cas.completionWritten()
                PENDING.remove(this)
                // The winning notification only: the loser returned above and leaves this to us.
                cas.stopLiveWriter()
            }
            catch( Throwable t ) {
                final Cid recorded = recordedAs
                if( recorded == null )
                    throw t
                warnRecorded(recorded, t)
            }
        }
    }

    /**
     * Says, on the terminal, that the run is recorded though a step after its
     * RunCompletion failed. It never throws: a logger that fails (an
     * OutOfMemoryError in an appender) falls back to System.err, and a
     * failure there is dropped, since the run is in the store either way.
     */
    private static void warnRecorded(Cid completion, Throwable failure) {
        try {
            ConsoleLog.LOG.warn("nf-blocks: the run was recorded as ${completion}; a later step failed: " +
                "${failure.message ?: failure.class.name}${IndexSnapshot.heapAdvice(failure)}")
        }
        catch( Throwable ignored ) {
            try {
                System.err.println('nf-blocks: the run was recorded as ' + completion + '; a later step failed: ' + failure)
            }
            catch( Throwable dropped ) {
            }
            return
        }
        try {
            // The stack trace goes to .nextflow.log, as the other derived-write catches log theirs.
            log.warn("the run was recorded as ${completion}; a later step failed: ${failure.message ?: failure.class.name}", failure)
        }
        catch( Throwable ignored ) {
        }
    }

    /**
     * Called from {@code CasPlugin.stop()}, which Nextflow runs on main from
     * {@code ScriptRunner.shutdown} (Session.destroy, then Plugins.stop, then
     * System.exit). From 26.08.0-edge (nextflow 14d5f26c4, #7349)
     * {@code Session.shutdown0} runs once, so an aborted run is notified only
     * on the thread that called {@code Session.abort}, and main no longer
     * waits for it. Here main either waits for that thread's write, or, when
     * the notification has not reached this observer yet, writes the failed
     * RunCompletion itself; the latch keeps it to one either way.
     */
    static void finishPending() {
        for( CasObserver observer : new ArrayList<CasObserver>(PENDING) ) {
            try {
                observer.finishOnStop()
            }
            catch( Throwable t ) {
                // completeRun has already said so on the terminal.
                log.warn("nf-blocks could not record the run when the plugin stopped: ${t.message}", t)
            }
            finally {
                PENDING.remove(observer)
            }
        }
    }

    /** Test seam: forgets every pending observer, so one test's observers never reach another's stop. */
    @PackageScope
    static void resetPending() {
        PENDING.clear()
    }

    private void finishOnStop() {
        if( cas == null || session == null )
            return
        if( cas.completionClaimed() ) {
            if( !cas.awaitCompletionWritten(COMPLETION_WAIT_MILLIS) )
                log.warn("the run's RunCompletion was still being written after ${COMPLETION_WAIT_MILLIS} ms when the plugin stopped; not waiting longer")
            return
        }
        // Only an aborted run can reach the stop unnotified: a run that ends
        // normally is notified on main, from Session.destroy, before Plugins.stop.
        if( !session.isAborted() )
            return
        log.debug('the aborted run was not yet notified of its completion when the plugin stopped; writing its RunCompletion now')
        onFlowComplete()
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

    /** The most a failed run's completion waits for input hashing still queued (cas.inputs.waitSeconds may lower it). */
    static final long FAILED_RUN_INPUT_WAIT_MILLIS = 30_000L

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
        // A write through the provider with no publish event is still recorded.
        // From here on every write through the provider is stored when it closes.
        final List<String> unclosed = (List<String>) finalizeSpools { cas.finalizeAllPending() }
        if( unclosed )
            ConsoleLog.LOG.warn("nf-blocks: ${unclosed.size()} file(s) written through cas:// were still open when the run completed, so this run does not record them: " +
                "${unclosed.take(3).join(', ')}${unclosed.size() > 3 ? ', ...' : ''}")
        final Join.Result joined = Join.join(capturedOutputs(), capturedIndexes(), cas)

        // Every coordinate this run stored (an upload, a stream, a collectFile storeDir)
        // or a publish event named (a resumed publish known only by its pointer).
        final Set<String> stored = new TreeSet<String>(publishedKeys)
        stored.addAll(cas.publishes.keySet())
        stored.removeAll(joined.joinedKeys)
        final List<String> unjoined = new ArrayList<String>(stored)
        final Anomalies anomalies = (joined.anomalies ?: Anomalies.NONE).plus(Anomalies.unjoined(unjoined.size()))
        if( !unjoined.isEmpty() )
            ConsoleLog.LOG.warn("nf-blocks: ${unjoined.size()} file(s) stored this run are in no workflow output, so no run records them: " +
                "${unjoined.take(3).join(', ')}${unjoined.size() > 3 ? ', ...' : ''}. Declare them as workflow outputs (output { }) to keep their lineage")

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
        final RunInputs inputs = collectInputs(success)
        final Cid inputSet = inputs == null ? null : putBlock(inputs.block, 'InputSet')
        // Positions index the InputSet, so a lineage is written only beside one.
        final Cid itemLineage = inputSet == null ? null : writeItemLineage(joined.outputs, inputs, inputSet)
        final WorkflowMetadata meta = session.workflowMetadata
        final Cid completion = putBlock(new RunCompletion([
            assertedBy        : cas.assertedBy,
            run               : manifest,
            collections       : collections,
            inputSet          : inputSet,
            status            : success ? RunCompletion.SUCCEEDED : RunCompletion.FAILED,
            exitStatus        : meta?.exitStatus,
            possiblyIncomplete: !success,
            startedAt         : iso(meta?.start),
            finishedAt        : iso(meta?.complete),
            anomalies         : anomalies,
            error             : success ? null : errorText(meta),
            providers         : joined.providers,
            itemLineage       : itemLineage,
        ]).toCbor(), 'RunCompletion')
        // Recorded: nothing below may report the run as unrecorded, or fail it (fix round 3, G3).
        recordedAs = completion

        appendStoreLog(completion)
        log.info(cas.addresser.summary())
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

    /**
     * A failed run's error: WorkflowMetadata's, else the session's own. On an
     * aborted run written from the plugin stop, WorkflowMetadata's onComplete
     * attributes may not be set yet (it runs on the aborting thread). The
     * RunCompletion scrubs whichever it gets (Records.scrubText).
     */
    private String errorText(WorkflowMetadata meta) {
        final String fromMeta = meta?.errorMessage
        if( fromMeta )
            return fromMeta
        final Throwable error = session.error
        return error == null ? null : (error.message ?: error.class.name)
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
            // Before the catch-up, so the snapshot guard covers it (silent decision 16).
            final SnapshotBase base = cas.snapshotBase()
            index = openIndex()
            index.ingestRun(cas.store, completion, cas.config.writableAlias)
            // Also fold in any read-only members' run logs, so this user's index
            // reflects the whole composition and not only what this run wrote.
            final Set<String> failed = cas.catchUpIndex(index)
            writeSnapshot(index, base, failed)
        }
        catch( Throwable e ) {
            // The run is recorded by now, so even an OutOfMemoryError only warns (fix round 2, B2).
            log.warn("the run was recorded; the index could not be updated for run ${completion}: ${e.message ?: e.class.name}${IndexSnapshot.heapAdvice(e)}. " +
                "It is derived and can be rebuilt", e)
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

    /** Overridable seam so a test can make the walk fail, or act once the InputSet is written. */
    protected List<ItemLineage.Derived> deriveItems(List<Join.JoinedOutput> outputs, InputSet set,
                                                    Map<InputRecorder.Staged, InputSet.Input> byStaged) {
        return derivation.derive(outputs, set, byStaged)
    }

    /** Overridable seam so a test can inject an index failure (DESIGN.md §11). */
    protected Index openIndex() {
        return cas.openIndex()
    }

    /** Overridable seam so a test can make the snapshot write fail as it likes, an OutOfMemoryError included. */
    protected IndexSnapshot.Result writeIndexSnapshot(Index index, SnapshotBase base, Set<String> failed) {
        return cas.snapshotWritable(index, cas.config.snapshotMaxBytes, base, failed)
    }

    /**
     * The member's Index Snapshot, under the cap and the guard (DESIGN.md §15).
     * Derived: any failure only warns, an OutOfMemoryError included, since by
     * now the run is recorded (fix round 2, B2).
     */
    private void writeSnapshot(Index index, SnapshotBase base, Set<String> failed) {
        final String alias = cas.config.writableAlias
        final long cap = cas.config.snapshotMaxBytes
        try {
            final IndexSnapshot.Result r = writeIndexSnapshot(index, base, failed)
            // A lean-seeded cache cannot compute the figures at any size or cap; only a reindex that does not seed can.
            if( r.skipped == IndexSnapshot.OVER_CAP && r.seededLean )
                log.warn("the Index Snapshot of store '${alias}' is over cas.snapshot.maxBytes (${cap} bytes)" +
                    "${r.omitted != null ? ' even without its storage figures' : ''} and was not rewritten. This cache was seeded from a snapshot " +
                    "without storage figures; `nextflow plugin nf-blocks:reindex --seed false` rebuilds it with them")
            else if( r.skipped == IndexSnapshot.OVER_CAP && r.omitted != null )
                log.warn("the Index Snapshot of store '${alias}' is over cas.snapshot.maxBytes (${cap} bytes) even without its storage figures " +
                    "and was not rewritten; `nextflow plugin nf-blocks:snapshot` rewrites it at any size")
            else if( r.skipped == IndexSnapshot.OVER_CAP )
                log.warn("the Index Snapshot of store '${alias}' is over cas.snapshot.maxBytes (${cap} bytes) and was not rewritten; " +
                    "`nextflow plugin nf-blocks:snapshot` rewrites it at any size")
            else if( r.skipped == IndexSnapshot.SEEDED_LEAN_BASE_FULL || r.skipped == IndexSnapshot.SEEDED_LEAN_BASE_UNKNOWN )
                log.warn("the run was recorded; the Index Snapshot of store '${alias}' was not rewritten (${r.skipped})" +
                    "${IndexSnapshot.skipAdvice(r)}")
            else if( r.skipped )
                log.info("the Index Snapshot of store '${alias}' was not rewritten (${r.skipped}); it is derived, and the next writer rewrites it")
            else if( r.seededLean )
                log.info("the Index Snapshot of store '${alias}' was written without storage figures (${r.omitted}): this cache was seeded " +
                    "from a snapshot without them; `nextflow plugin nf-blocks:reindex --seed false` rebuilds it with them")
            else if( r.omitted == IndexSnapshot.OVER_CAP )
                log.info("the Index Snapshot of store '${alias}' was written without storage figures: with them it would pass cas.snapshot.maxBytes " +
                    "(${cap} bytes); `nextflow plugin nf-blocks:snapshot` writes them at any size")
            else if( r.omitted == IndexSnapshot.OUT_OF_MEMORY && r.heapExhausted )
                log.info("the Index Snapshot of store '${alias}' was written without storage figures, which ran out of heap; " +
                    "`nextflow plugin nf-blocks:snapshot` writes them once you ${IndexSnapshot.MORE_HEAP}")
            else if( r.omitted == IndexSnapshot.OUT_OF_MEMORY )
                log.info("the Index Snapshot of store '${alias}' was written without storage figures, which ran out of memory outside the heap; " +
                    "the next build tries them again")
        }
        catch( Throwable e ) {
            log.warn("the run was recorded; its Index Snapshot was not rewritten: ${e.message ?: e.class.name}${IndexSnapshot.heapAdvice(e)}. " +
                "The snapshot of store '${alias}' is derived, and the next writer rewrites it", e)
        }
    }

    private void appendStoreLog(Cid completion) {
        try {
            StoreLog.append(cas.store, StoreLogKind.RUN, completion, nowMillis())
            cas.noteLogged(completion)
        }
        catch( Throwable e ) {
            // The RunCompletion is written by now, so even an OutOfMemoryError only warns (fix round 2).
            log.warn("the run was recorded; its Store Log entry could not be written: ${e.message ?: e.class.name}${IndexSnapshot.heapAdvice(e)}. " +
                "The entry is derived, and the RunCompletion ${completion} is in the store", e)
        }
    }

    /** When the entry is written; overridable so a test can fix the clock. */
    protected long nowMillis() {
        return System.currentTimeMillis()
    }

    // ------------------------------------------------------------- plumbing

    /** Hashing a spool is a provenance write: a failure aborts the run (Rule 3). */
    private static Object finalizeSpools(Closure body) {
        try {
            return body.call()
        }
        catch( AbortRunException e ) {
            throw e
        }
        catch( Exception e ) {
            throw new AbortRunException("Unable to record a file written through cas://: ${e.message}", e)
        }
    }

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
