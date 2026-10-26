package robsyme.cas.trace

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.file.FileHolder
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfigV2
import nextflow.script.params.BaseParam
import nextflow.script.params.InParam
import nextflow.script.params.v2.ProcessInput
import nextflow.script.params.v2.ProcessTupleInput
import robsyme.cas.core.Cid
import robsyme.cas.core.Hashing
import robsyme.cas.core.InputSet
import robsyme.cas.core.Leaks
import robsyme.cas.core.Records
import robsyme.cas.nio.CasPath

/**
 * The files one run read, for its InputSet (explorer v0.3 P4 plan 1.3,
 * rulings R3 and R4). The observer hands it each completed or cached task;
 * it decides which staged files are inputs, gives each its role and sample,
 * and queues the stat and the hash on a small pool of its own, so Nextflow's
 * task-completion thread never touches a file system. {@link #collect} waits
 * for the pool when the run completes, up to a bound.
 *
 * An input is a file a task staged whose source is outside the run's work
 * directory, or a foreign file Nextflow copied into that directory's
 * {@code stage-*} area, plus {@code params.input} when it names a file. Only
 * a regular file this host reads as a local file is hashed: its real path is
 * on the default file system and not under Fusion's mount. A remote file
 * (s3, gs, az, http(s), a Fusion path or a link into one) is never opened,
 * not even params.input: it is listed with reason "remote" and the size a
 * stat gives. A local path that is no regular file (a directory, a FIFO, a
 * device) is never opened either. Each distinct file (same path) is stat'ed
 * and hashed once per run.
 */
@Slf4j
@CompileStatic
class InputRecorder {

    /** Hashing threads; each borrows one buffer from the shared HashBufferPool while it reads. */
    static final int THREADS = 4

    private static final Pattern SCHEME = ~/^[A-Za-z][A-Za-z0-9+.-]*:\/\//

    /** A file-name stem long enough to be a bearer token; {@link #isTokenStem} says whether it looks like one. */
    private static final Pattern TOKEN_STEM = ~/[A-Za-z0-9]{24,}/

    /**
     * How often a token's letters and digits change places at least, in a
     * remote URL path, where tokens are handed out. A random 32-character
     * [a-z0-9] token switches fewer than 2 times with probability 6.8e-5.
     */
    private static final int TOKEN_SWITCHES = 2

    /**
     * The same for a local file name, which is far more often a real name
     * than a downloaded token: a separator-free FASTQ name with a number
     * inside ({@code run20261025combinedreads}) switches twice and must stay
     * readable (orchestrator ruling). A random token switches fewer than 4
     * times with probability 1.3e-3. Known token shapes (ghp_ and the like)
     * are caught anywhere by {@link Leaks#redactCredentials}.
     */
    private static final int LOCAL_TOKEN_SWITCHES = 4

    /** A Box shared link's path: the segment directly under {@code /s/} or {@code /shared/static/} is its token. */
    private static final Pattern BOX_SHARED = ~/\/(s|shared\/static)\/[^\/]+\/*/

    private static final Pattern HEX = ~/[0-9A-Fa-f]+/

    private static final Pattern PORT = ~/[0-9]*/

    private static final String STAGE_PREFIX = 'stage-'

    /** Fusion's mount of remote storage on the default file system: a read there is a download. */
    private static final String FUSION_ROOT = '/fusion'

    /** Where a staged file's local path lies relative to the run's work directory. */
    enum Placement { OUTSIDE, STAGED, RUN }

    /** One file a task staged: its source (the value the task was given), where Nextflow put it, and the sample of the tuple that held it. */
    @Canonical
    @CompileStatic
    static final class Staged {
        Object source
        Path store
        String sample
    }

    /** What reading (or not reading) one source file found. */
    @Canonical
    @CompileStatic
    static final class Addressed {
        Cid address
        Long size
        String reason
    }

    /** One source file's work on the pool: a stat, then a hash when the file is readable here. */
    @CompileStatic
    private static final class Job {
        /** The size once the stat answered, so an input cut off at the deadline still has it. */
        volatile Long size
        volatile Future<Addressed> result
    }

    @CompileStatic
    private static final class Entry {
        final String role
        final String name
        final String sample
        /** The path a task was given, for the storeDir check at collect; null for params.input. */
        final Path origin
        final Job job

        Entry(String role, String name, String sample, Path origin, Job job) {
            this.role = role
            this.name = name
            this.sample = sample
            this.origin = origin
            this.job = job
        }
    }

    private final List<Path> workDirs
    /** The pipeline's own directory: a file under it is an asset of the pipeline, not data. */
    private final List<Path> projectDirs
    private final Map params
    private final boolean hash
    private final String samplesheetName
    private final Map<String, String> fileParams

    /** Source key (the path) to its one job. */
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>()

    /** One entry per (role, name, sample, source). */
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>()

    /**
     * The storeDir of every task recorded (as given and with symlinks followed). A file
     * under one is a task's output that Nextflow keeps outside the work directory.
     */
    private final Set<Path> storeDirs = ConcurrentHashMap.newKeySet()

    /** Each local storeDir's symlink resolution, queued on the pool; {@link #collect} waits for them within its bound. */
    private final Queue<Future<?>> storeDirJobs = new ConcurrentLinkedQueue<Future<?>>()

    /** How many inputs {@link #collect} gave up on at its deadline. */
    private volatile int leftUnfinished

    private final AtomicBoolean samplesheetSeen = new AtomicBoolean(false)
    private final AtomicLong hashedBytes = new AtomicLong()
    private final AtomicInteger hashedFiles = new AtomicInteger()
    private final Object poolLock = new Object()
    private ExecutorService pool

    /**
     * @param workDir the run's work directory, or null when unknown
     * @param params the run's params, for params.input and the file-valued params
     * @param hash {@code cas.inputs.hash}: when false nothing is read, and a local input is listed unaddressed
     */
    InputRecorder(Path workDir, Map params, boolean hash) {
        this(workDir, null, params, hash)
    }

    /**
     * @param projectDir the pipeline's directory (Nextflow's projectDir), or null when unknown. A task
     *        file under it that would be role task is role asset with no sample: nf-core stages its
     *        multiqc_config.yml in a tuple with meta id 'multiqc_report', which would otherwise make
     *        every two runs of the pipeline look as if they shared a sample's input.
     */
    InputRecorder(Path workDir, Path projectDir, Map params, boolean hash) {
        this.workDirs = workDirsOf(workDir)
        this.projectDirs = workDirsOf(projectDir)
        this.params = params ?: Collections.emptyMap()
        this.hash = hash
        this.fileParams = fileParams(this.params)
        final String input = locationText(this.params.get('input'))
        this.samplesheetName = input != null && looksLikeFile(input) ? baseName(input) : null
    }

    // ------------------------------------------------------------ recording

    /**
     * Records every input file of one completed or cached task. Touches no
     * file system: the stats and hashes are queued. The task's storeDir is
     * noted first, so a later task reading what this one stored there does
     * not count it.
     */
    void recordTask(TaskRun task) {
        if( task == null )
            return
        noteStoreDir(storeDirOf(task))
        recordStaged(staged(groupsOf(task), task.inputFiles))
    }

    /**
     * A storeDir of the run's processes: files under it are the run's own
     * outputs, never inputs. Noted as given at once; a local one's symlinks
     * are resolved on the pool, since a storeDir on a hung mount would hold
     * the task thread.
     */
    void noteStoreDir(Path storeDir) {
        if( storeDir == null )
            return
        final Path given = storeDir.toAbsolutePath().normalize()
        if( !storeDirs.add(given) || !isLocal(given) )
            return
        storeDirJobs.add(executor().submit({
            try {
                final Path real = realPathOf(given)
                if( real != null && real != given )
                    storeDirs.add(real)
            }
            catch( Exception e ) {
                log.debug("storeDir '${given}' could not be resolved: ${e.message}")
            }
        } as Runnable))
    }

    /** The task's resolved storeDir, or null when it has none or it cannot be evaluated. */
    private static Path storeDirOf(TaskRun task) {
        try {
            return task.config?.getStoreDir()
        }
        catch( Exception e ) {
            log.debug("the storeDir of task ${task.hash} could not be read: ${e.message}")
            return null
        }
    }

    void recordStaged(List<Staged> staged) {
        for( Staged s : staged )
            record(s)
    }

    /**
     * Records params.input once, when it names a file, with role samplesheet.
     * A local one is hashed; a remote one is never read, as any remote input.
     */
    void recordSamplesheet() {
        if( !samplesheetSeen.compareAndSet(false, true) )
            return
        final Object value = params.get('input')
        final String text = locationText(value)
        if( text == null || !looksLikeFile(text) )
            return
        final String fallbackName = baseName(text)
        Path path
        try {
            path = value instanceof Path ? (Path) value : resolveLocation(text)
        }
        catch( Exception e ) {
            log.debug("params.input '${fallbackName}' could not be resolved: ${e.message}")
            addDone(InputSet.SAMPLESHEET, fallbackName, null, null, 'input:' + text, new Addressed(null, null, InputSet.UNREADABLE))
            return
        }
        final String name = fileNameOf(path) ?: fallbackName
        if( path instanceof CasPath ) {
            addKnown(InputSet.SAMPLESHEET, name, null, null, (CasPath) path, null)
            return
        }
        addFile(InputSet.SAMPLESHEET, name, null, null, path, path)
    }

    private void record(Staged s) {
        if( !(s.source instanceof Path) )
            return
        final Path source = (Path) s.source
        final String name = fileNameOf(source)
        String role = roleOf(name, samplesheetName, fileParams)
        String sample = s.sample
        if( source instanceof CasPath ) {
            addKnown(role, name, sample, source, (CasPath) source, s.store)
            return
        }
        final Path store = s.store ?: source
        if( placement(store, workDirs) == Placement.RUN || placement(source, workDirs) == Placement.RUN )
            return
        if( storedByRun(source) )
            return
        if( role == InputSet.TASK && placement(source, projectDirs) != Placement.OUTSIDE ) {
            role = InputSet.ASSET
            sample = null
        }
        addFile(role, name, sample, source, source, store)
    }

    /**
     * Reads the first of the store path and the source that this host reads
     * as a local file. The store path is a local input itself, or the copy of
     * a foreign file (http, s3) that Nextflow downloaded into a local
     * {@code work/stage-*} to run the task: hashing it reads a file Nextflow
     * already fetched, and downloads nothing. The source is a local file
     * Nextflow staged to a remote work directory. Neither on this host by its
     * text: the file is remote, and only stat'ed.
     */
    private void addFile(String role, String name, String sample, Path origin, Path source, Path store) {
        final List<Path> here = new ArrayList<Path>(2)
        for( Path p : [store, source] ) {
            if( mayBeHere(p) && !here.contains(p) )
                here.add(p)
        }
        if( here.isEmpty() )
            addRemote(role, name, sample, origin, store)
        else
            addLocal(role, name, sample, origin, here)
    }

    /** Whether a path lies under the storeDir of a task recorded so far. */
    private boolean storedByRun(Path path) {
        return path != null && !storeDirs.isEmpty() && within(path, new ArrayList<Path>(storeDirs))
    }

    /**
     * Paths on this host by their text, in the order to try them; all of it
     * on the pool. The first whose real path (links followed) is not under
     * Fusion's mount is read ({@link #read}). When every one leads into
     * /fusion the file is remote, and the real path is only stat'ed. A path
     * with no real path is judged by its stat alone ({@link #unresolved}).
     */
    private void addLocal(String role, String name, String sample, Path origin, List<Path> here) {
        add(role, name, sample, origin, 'local:' + here[0], { Job job ->
            Path fused = null
            for( Path candidate : here ) {
                final Path real = realPathOrNull(candidate)
                if( real == null )
                    return unresolved(candidate, job)
                if( !onFusion(real) )
                    return read(real, job)
                fused = fused ?: real
            }
            return statRemote(fused, name, job)
        })
    }

    /**
     * A local path's real path, or null when it has none: realpath fails, or
     * gives something that is not an absolute path (Linux names a pipe or a
     * socket {@code pipe:[n]}, {@code socket:[n]}).
     */
    private Path realPathOrNull(Path path) {
        try {
            final Path real = realPathOf(path)
            return real != null && real.isAbsolute() ? real : null
        }
        catch( Exception e ) {
            log.debug("input '${path}' has no real path: ${e.message}")
            return null
        }
    }

    /**
     * A local path with no real path, by one stat that follows its links. A
     * missing file, a dangling link or a parent this user cannot search fails
     * the stat too, and is unreadable. On Linux, {@code /dev/fd/N} from
     * {@code --input <(...)} and a piped {@code /dev/stdin} are links to a
     * pipe or socket that realpath cannot follow and stat can: no regular
     * file, so unaddressed with no size, and never opened. A regular file
     * reached this way is not read either, since without its real path it
     * cannot be told from a link into Fusion's mount: unreadable, with its
     * size.
     */
    private Addressed unresolved(Path path, Job job) {
        final BasicFileAttributes attrs = statOf(path)
        if( !attrs.isRegularFile() )
            return new Addressed(null, null, InputSet.UNADDRESSED)
        job.size = attrs.size()
        return new Addressed(null, job.size, InputSet.UNREADABLE)
    }

    /**
     * A local file, by its real path. Only a regular file is opened, and only
     * to hash it. Anything else (a directory, a FIFO such as {@code --input
     * <(...)} gives on macOS, a device) is unaddressed with no size: hashing a
     * directory would mean a DirectoryManifest, and opening a FIFO would wait
     * for a writer or take bytes the pipeline itself is to read.
     */
    private Addressed read(Path file, Job job) {
        final BasicFileAttributes attrs = statOf(file)
        if( !attrs.isRegularFile() )
            return new Addressed(null, null, InputSet.UNADDRESSED)
        final long size = attrs.size()
        job.size = size
        if( !hash )
            return new Addressed(null, size, InputSet.UNADDRESSED)
        return new Addressed(hashed(file, size), size, null)
    }

    /** A file only remote storage holds: never opened; its size from a stat on the pool, or none. */
    private void addRemote(String role, String name, String sample, Path origin, Path file) {
        add(role, name, sample, origin, 'remote:' + file.toUri(), { Job job -> statRemote(file, name, job) })
    }

    private Addressed statRemote(Path file, String name, Job job) {
        Long size = null
        try {
            final BasicFileAttributes attrs = statOf(file)
            size = attrs.isDirectory() ? null : attrs.size()
        }
        catch( Exception e ) {
            log.debug("remote input '${name}' could not be stat'ed: ${e.message}")
        }
        job.size = size
        return new Addressed(null, size, InputSet.REMOTE)
    }

    /**
     * Content already addressed. A Store URI naming one raw block is its own
     * address; anything else (a coordinate, a leaf of an item) is resolved
     * through the provider's attributes, whose file key is the address.
     */
    private void addKnown(String role, String name, String sample, Path origin, CasPath path, Path copy) {
        add(role, name, sample, origin, 'cas:' + path, { Job job ->
            final Long copySize = copy != null && mayBeHere(copy) ? sizeOrNull(copy) : null
            if( path.isStoreUri() && path.segments.size() <= 1 && path.cid().isRaw() )
                return new Addressed(path.cid(), copySize != null ? copySize : sizeOrNull(path), null)
            final BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes)
            final Object key = attrs.fileKey()
            if( key == null || !Cid.isCid(key.toString()) )
                return new Addressed(null, null, InputSet.UNREADABLE)
            return new Addressed(Cid.parse(key.toString()), attrs.isDirectory() ? null : attrs.size(), null)
        })
    }

    private void addDone(String role, String name, String sample, Path origin, String sourceKey, Addressed result) {
        final Job done = new Job()
        done.size = result.size
        done.result = CompletableFuture.completedFuture(result)
        put(role, name, sample, origin, sourceKey, jobs.computeIfAbsent(sourceKey) { String k -> done })
    }

    /** Queues {@code work} once per source key; every entry naming that source shares the job. */
    private void add(String role, String name, String sample, Path origin, String sourceKey, Closure<Addressed> work) {
        if( entries.containsKey(entryKey(role, name, sample, sourceKey)) )
            return
        put(role, name, sample, origin, sourceKey, jobs.computeIfAbsent(sourceKey) { String k -> submit(name, work) })
    }

    private void put(String role, String name, String sample, Path origin, String sourceKey, Job job) {
        entries.putIfAbsent(entryKey(role, name, sample, sourceKey), new Entry(role, name, sample, origin, job))
    }

    private static String entryKey(String role, String name, String sample, String sourceKey) {
        return [role, name ?: '', sample ?: '', sourceKey].join('\u0000')
    }

    /** A job on the pool; a failure is the input's reason "unreadable", with the size if its stat answered. */
    private Job submit(String name, Closure<Addressed> work) {
        final Job job = new Job()
        job.result = executor().submit({
            try {
                return work.call(job)
            }
            catch( Exception e ) {
                log.debug("input '${name}' could not be read: ${e.message}")
                return new Addressed(null, job.size, InputSet.UNREADABLE)
            }
        } as Callable<Addressed>)
        return job
    }

    private ExecutorService executor() {
        synchronized( poolLock ) {
            if( pool == null ) {
                final AtomicInteger n = new AtomicInteger()
                pool = Executors.newFixedThreadPool(THREADS, { Runnable r ->
                    final Thread t = new Thread(r, "nf-blocks-inputs-${n.incrementAndGet()}")
                    t.daemon = true
                    return t
                } as ThreadFactory)
            }
            return pool
        }
    }

    private Cid hashed(Path file, long size) {
        final Cid cid = hashFile(file)
        hashedFiles.incrementAndGet()
        hashedBytes.addAndGet(size)
        return cid
    }

    // ------------------------------------------------------------ collecting

    /**
     * Every input recorded, once its stat and hash are done. With
     * {@code waitMillis} negative this waits as long as they take; otherwise
     * an input whose stat or hash is still running at the deadline is listed
     * with reason "unfinished" (with its size when the stat answered), and
     * {@link #getLeftUnfinished} says how many were. The storeDirs' symlink
     * resolution shares the same deadline. A file under a task's storeDir is
     * left out, whichever task was recorded first.
     */
    List<InputSet.Input> collect(long waitMillis) {
        final long started = System.nanoTime()
        final long deadline = waitMillis < 0 ? Long.MAX_VALUE : started + TimeUnit.MILLISECONDS.toNanos(waitMillis)
        for( Future<?> resolving : storeDirJobs ) {
            try {
                if( waitMillis < 0 )
                    resolving.get()
                else
                    resolving.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
            }
            catch( InterruptedException t ) {
                Thread.currentThread().interrupt()
                break
            }
            catch( Exception t ) {
                // Past the deadline the storeDir counts as given only.
            }
        }
        final List<InputSet.Input> out = new ArrayList<InputSet.Input>(entries.size())
        int abandoned = 0
        for( Entry e : entries.values() ) {
            if( storedByRun(e.origin) )
                continue
            final Future<Addressed> result = e.job.result
            Addressed a
            try {
                a = waitMillis < 0 ? result.get() : result.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
            }
            catch( TimeoutException | CancellationException t ) {
                result.cancel(true)
                abandoned++
                a = new Addressed(null, e.job.size, InputSet.UNFINISHED)
            }
            catch( ExecutionException t ) {
                a = new Addressed(null, e.job.size, InputSet.UNREADABLE)
            }
            catch( InterruptedException t ) {
                Thread.currentThread().interrupt()
                abandoned++
                a = new Addressed(null, e.job.size, InputSet.UNFINISHED)
            }
            out.add(new InputSet.Input(e.role, e.name, a.address, a.size, e.sample, a.reason))
        }
        // A remote params.input is never read, but a task may stage it (Nextflow downloads it into
        // stage-*), and that local copy is hashed: then it stands for the samplesheet, and the
        // unaddressed params.input entry of the same name says nothing more.
        final Set<String> addressedSheets = new HashSet<String>()
        for( InputSet.Input i : out )
            if( i.role == InputSet.SAMPLESHEET && i.address != null )
                addressedSheets.add(i.name)
        out.removeAll { InputSet.Input i -> i.role == InputSet.SAMPLESHEET && i.address == null && i.reason == InputSet.REMOTE && addressedSheets.contains(i.name) }
        leftUnfinished = abandoned
        final long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
        log.info("nf-blocks: ${out.size()} input(s) recorded; ${hashedFiles.get()} file(s), ${hashedBytes.get()} bytes hashed; " +
            "waited ${waited} ms for inputs at completion${abandoned ? ", ${abandoned} left unfinished" : ''}")
        return out
    }

    /** How many inputs the last {@link #collect} gave up on at its deadline (reason "unfinished"). */
    int getLeftUnfinished() {
        return leftUnfinished
    }

    /**
     * Stops the pool without waiting for it. A stat or hash still running (a
     * read blocked on a hung mount ignores the interrupt) keeps its daemon
     * thread until it returns or the JVM exits; nothing waits for it.
     */
    void close() {
        synchronized( poolLock ) {
            pool?.shutdownNow()
        }
    }

    // ------------------------------------------------------------ seams

    /** Hashes one file's whole content as a raw block, as stored files are (DESIGN.md §3). */
    protected Cid hashFile(Path path) {
        return Hashing.hashRaw(path)
    }

    /** One stat of a file, local or remote, on the pool; never opens its content. */
    protected BasicFileAttributes statOf(Path path) {
        return Files.readAttributes(path, BasicFileAttributes)
    }

    /** A local storeDir or input with its symlinks followed, on the pool; fails for a missing file or a dangling link. */
    protected Path realPathOf(Path path) {
        return path.toRealPath()
    }

    /** params.input as a path, through Nextflow's file helper (local, s3://, http(s)://, cas://). */
    protected Path resolveLocation(String text) {
        return FileHelper.asPath(text)
    }

    // ------------------------------------------------------------ rules

    /**
     * Each top-level param whose value looks like a path or URI, by name, to
     * that value's base name; params.input is the samplesheet, so it is not
     * one. Sorted by param name, which is the order roles are given in.
     */
    static Map<String, String> fileParams(Map params) {
        final Map<String, String> out = new TreeMap<String, String>()
        for( Map.Entry e : (params ?: Collections.emptyMap()).entrySet() ) {
            final String name = e.key?.toString()
            if( !name || name == 'input' )
                continue
            final String text = locationText(e.value)
            if( text == null || !looksLikeFile(text) )
                continue
            final String base = baseName(text)
            if( base )
                out.put(name, base)
        }
        return out
    }

    /** Role precedence (ruling R3): samplesheet, then param:<name> in name order, then task. */
    static String roleOf(String name, String samplesheetName, Map<String, String> fileParams) {
        if( name == null )
            return InputSet.TASK
        if( samplesheetName != null && name == samplesheetName )
            return InputSet.SAMPLESHEET
        for( Map.Entry<String, String> e : new TreeMap<String, String>(fileParams ?: Collections.<String, String> emptyMap()).entrySet() ) {
            if( e.value == name )
                return InputSet.PARAM_PREFIX + e.key
        }
        return InputSet.TASK
    }

    /**
     * The sample of one task input (ruling R3). A record (one value that is a
     * map) is its {@code meta.id}, else its {@code id}. A tuple, or a record
     * Nextflow destructured into components, is the {@code id} of its
     * component named {@code meta}, else its {@code id} component. A map
     * under any other name gives none: nf-core passes a reference as
     * {@code tuple val(meta2), path(fasta)} with {@code meta2.id} naming the
     * reference, which would otherwise make it a sample's input.
     */
    static String sampleOf(Map<String, Object> group) {
        if( group == null || group.isEmpty() )
            return null
        if( group.size() == 1 ) {
            final Object only = group.values().iterator().next()
            return only instanceof Map ? recordId((Map) only) : null
        }
        final String fromMeta = idOf(group.get('meta'))
        return fromMeta != null ? fromMeta : idText(group.get('id'))
    }

    private static String recordId(Map record) {
        final String fromMeta = idOf(record.get('meta'))
        return fromMeta != null ? fromMeta : idText(record.get('id'))
    }

    private static String idOf(Object value) {
        return value instanceof Map ? idText(((Map) value).get('id')) : null
    }

    private static String idText(Object value) {
        if( value instanceof CharSequence || value instanceof Number || value instanceof Boolean ) {
            final String text = value.toString()
            return text ? text : null
        }
        return null
    }

    /**
     * Each staged file of a task, once for every sample whose input holds it
     * (matched by the source path Nextflow staged), or once with no sample. A
     * value Nextflow turned into a file is not a file the run read.
     */
    static List<Staged> staged(List<Map<String, Object>> groups, Collection<FileHolder> holders) {
        final List<String> samples = new ArrayList<String>()
        final List<Set<Object>> held = new ArrayList<Set<Object>>()
        for( Map<String, Object> group : groups ?: Collections.<Map<String, Object>> emptyList() ) {
            samples.add(sampleOf(group))
            final Set<Object> paths = new HashSet<Object>()
            collectPaths(group.values(), paths, 0)
            held.add(paths)
        }
        final List<Staged> out = new ArrayList<Staged>()
        for( FileHolder holder : holders ?: Collections.<FileHolder> emptyList() ) {
            final Object source = holder.sourceObj
            if( !(source instanceof Path) )
                continue
            final Set<String> ids = new LinkedHashSet<String>()
            for( int i = 0; i < samples.size(); i++ ) {
                if( samples[i] != null && held[i].contains(source) )
                    ids.add(samples[i])
            }
            if( ids.isEmpty() )
                out.add(new Staged(source, holder.storePath, null))
            else
                for( String id : ids )
                    out.add(new Staged(source, holder.storePath, id))
        }
        return out
    }

    private static void collectPaths(Object value, Set<Object> into, int depth) {
        if( value == null || depth > 8 )
            return
        if( value instanceof Path )
            into.add(value)
        else if( value instanceof FileHolder ) {
            final Object source = ((FileHolder) value).sourceObj
            if( source != null )
                into.add(source)
        }
        else if( value instanceof Map )
            collectPaths(((Map) value).values(), into, depth + 1)
        else if( value instanceof Collection )
            for( Object element : (Collection) value )
                collectPaths(element, into, depth + 1)
    }

    /**
     * A task's inputs, one map of component name to value per declared input.
     * A typed process (ProcessConfigV2) declares records, tuples and plain
     * inputs; a classic one groups a tuple's elements by their shared
     * declaration index. Values are as the task received them, before staging.
     */
    static List<Map<String, Object>> groupsOf(TaskRun task) {
        final List<Map<String, Object>> out = new ArrayList<Map<String, Object>>()
        final Map<InParam, Object> inputs = task?.inputs
        if( !inputs )
            return out
        final Object config = task.processor?.getConfig()
        if( config instanceof ProcessConfigV2 ) {
            for( ProcessInput param : ((ProcessConfigV2) config).getInputs().getParams() ) {
                final Map<String, Object> group = new LinkedHashMap<String, Object>()
                if( param instanceof ProcessTupleInput )
                    for( ProcessInput component : ((ProcessTupleInput) param).getComponents() )
                        put(group, component.getName(), inputs.get(component))
                else
                    put(group, param.getName(), inputs.get(param))
                out.add(group)
            }
            return out
        }
        final Map<Integer, Map<String, Object>> byIndex = new TreeMap<Integer, Map<String, Object>>()
        int unindexed = Integer.MAX_VALUE.intdiv(2)
        for( Map.Entry<InParam, Object> e : inputs.entrySet() ) {
            final int index = e.key instanceof BaseParam ? (int) ((BaseParam) e.key).index : unindexed++
            final Map<String, Object> group = byIndex.computeIfAbsent(index) { Integer i -> new LinkedHashMap<String, Object>() }
            put(group, nameOf(e.key), e.value)
        }
        out.addAll(byIndex.values())
        return out
    }

    private static void put(Map<String, Object> group, String name, Object value) {
        String key = name ?: "#${group.size()}".toString()
        if( group.containsKey(key) )
            key = "${key}#${group.size()}".toString()
        group.put(key, value)
    }

    private static String nameOf(InParam param) {
        try {
            return param.name
        }
        catch( Exception ignored ) {
            return null
        }
    }

    /** The work directory as given and, on the local file system, as Nextflow's FileHolder resolves it (symlinks followed). */
    static List<Path> workDirsOf(Path workDir) {
        if( workDir == null )
            return Collections.emptyList()
        final List<Path> out = new ArrayList<Path>()
        final Path given = workDir.toAbsolutePath().normalize()
        out.add(given)
        if( isLocal(given) ) {
            try {
                final Path real = given.toRealPath()
                if( real != given )
                    out.add(real)
            }
            catch( Exception ignored ) {
            }
        }
        return out
    }

    static Placement placement(Path path, List<Path> workDirs) {
        if( path == null || !workDirs )
            return Placement.OUTSIDE
        for( Path dir : workDirs ) {
            if( dir.fileSystem.provider() != path.fileSystem.provider() )
                continue
            try {
                final Path p = path.toAbsolutePath().normalize()
                if( p == dir || !p.startsWith(dir) )
                    continue
                final Path first = dir.relativize(p).getName(0)
                return first.toString().startsWith(STAGE_PREFIX) ? Placement.STAGED : Placement.RUN
            }
            catch( Exception ignored ) {
            }
        }
        return Placement.OUTSIDE
    }

    /** Whether a path is one of the directories or lies under one (same file system only). */
    static boolean within(Path path, List<Path> dirs) {
        for( Path dir : dirs ) {
            if( dir.fileSystem.provider() != path.fileSystem.provider() )
                continue
            try {
                if( path.toAbsolutePath().normalize().startsWith(dir) )
                    return true
            }
            catch( Exception ignored ) {
            }
        }
        return false
    }

    // ------------------------------------------------------------ text

    private static boolean isLocal(Path path) {
        return path.fileSystem == FileSystems.default
    }

    /**
     * Whether a path may name a file this host reads as a local file: it is
     * on the default file system and its text is not under Fusion's mount.
     * Judged by text only, so the task thread touches no file system; a link
     * into /fusion passes here and is caught on the pool by its real path.
     */
    private static boolean mayBeHere(Path path) {
        return path != null && isLocal(path) && !onFusion(path)
    }

    /**
     * Whether a local path's text, made absolute and normalized, is Fusion's
     * mount of remote storage or under it ({@code /fusion}), where every read
     * is a download.
     */
    static boolean onFusion(Path path) {
        final String text = path.toAbsolutePath().normalize().toString()
        return text == FUSION_ROOT || text.startsWith(FUSION_ROOT + '/')
    }

    private Long sizeOrNull(Path path) {
        try {
            return statOf(path).size()
        }
        catch( Exception ignored ) {
            return null
        }
    }

    /** A param value as location text: a string, or a path's URI; anything else is no location. */
    private static String locationText(Object value) {
        if( value instanceof Path )
            return ((Path) value).toUri().toString()
        if( value instanceof CharSequence ) {
            final String text = value.toString().trim()
            return text ?: null
        }
        return null
    }

    /** A URI, an absolute or relative path with a separator, or the name of a file that exists here. */
    static boolean looksLikeFile(String text) {
        if( !text || text.contains('\n') )
            return false
        if( SCHEME.matcher(text).find() || text.contains('/') )
            return true
        try {
            return Files.isRegularFile(Path.of(text))
        }
        catch( Exception ignored ) {
            return false
        }
    }

    /**
     * A location's base name as an InputSet holds it: the last segment,
     * without a trailing slash, scrubbed ({@link #scrubbed}). A URI loses its
     * query, fragment and userinfo first ({@link #withoutSlashedUserinfo},
     * then {@link Leaks#withoutCredentials}, shared with the RunManifest
     * scrub), so a URI with no path gives its host. On a Box shared link the
     * whole stem is a token ({@link #isBoxSharedLink}).
     */
    static String baseName(String text) {
        if( text == null )
            return null
        if( SCHEME.matcher(text).find() ) {
            final String plain = Leaks.withoutCredentials(withoutSlashedUserinfo(text))
            return scrubbed(lastSegment(plain), true, isBoxSharedLink(plain))
        }
        return scrubbed(lastSegment(text), false, false)
    }

    private static String lastSegment(String text) {
        String t = text
        while( t.length() > 1 && t.endsWith('/') )
            t = t.substring(0, t.length() - 1)
        return t.substring(t.lastIndexOf('/') + 1)
    }

    /**
     * A base name with nothing in it that grants access. A remote name is cut
     * at its first ';', where a URI's path parameters start
     * ({@code x.fq;jsessionid=...}); a local name keeps its ';'. Then the stem
     * (the name up to its first '.') becomes the secret marker, keeping the
     * extension, when {@code token} says the location names its file by a
     * capability token, or when the stem is shaped like one
     * ({@link #isTokenStem}). Last, any credential shape the RunManifest scrub
     * knows ({@link Leaks#redactCredentials}, a GitHub token say) becomes the
     * marker. Null when nothing is left.
     */
    private static String scrubbed(String base, boolean remote, boolean token) {
        String name = base
        final int semicolon = remote ? name.indexOf(';') : -1
        if( semicolon >= 0 )
            name = name.substring(0, semicolon)
        if( !name )
            return null
        final int dot = name.indexOf('.')
        final String stem = dot < 0 ? name : name.substring(0, dot)
        if( token || isTokenStem(stem, remote ? TOKEN_SWITCHES : LOCAL_TOKEN_SWITCHES) )
            name = Records.REDACTED_SECRET + name.substring(stem.length())
        return Leaks.redactCredentials(name)
    }

    /**
     * Whether a URI with no userinfo, query or fragment is a Box shared link
     * ({@code https://app.box.com/s/<token>},
     * {@code https://<company>.box.com/shared/static/<token>.csv}): its host is
     * box.com or under it, and its last segment lies directly under
     * {@code /s/} or {@code /shared/static/}. That segment's stem is the
     * link's token whatever its shape. A file inside a shared folder
     * ({@code /s/<token>/file/<id>}) keeps its own name.
     */
    private static boolean isBoxSharedLink(String plain) {
        final Matcher scheme = SCHEME.matcher(plain)
        if( !scheme.lookingAt() )
            return false
        final String rest = plain.substring(scheme.end())
        final int slash = rest.indexOf('/')
        if( slash < 0 )
            return false
        String host = rest.substring(0, slash).toLowerCase(Locale.ROOT)
        final int colon = host.lastIndexOf(':')
        if( colon >= 0 )
            host = host.substring(0, colon)
        return (host == 'box.com' || host.endsWith('.box.com')) && BOX_SHARED.matcher(rest.substring(slash)).matches()
    }

    /**
     * Whether a file-name stem looks like a bearer token: 24 or more letters
     * and digits, switching between the two at least {@code switches} times
     * (a Box shared-link token, {@code abcd1234efgh...}). A word with a number
     * on it ({@code param3318408433160109677}) switches once. In a remote URL
     * a separator-free name with a number inside
     * ({@code run20261025combinedreads}) switches twice and is redacted too:
     * its name is lost, and nothing leaks; on local disk it is kept. A hex
     * digest or a CID has the shape too, but names content rather than
     * granting access, so it is kept.
     */
    private static boolean isTokenStem(String stem, int minSwitches) {
        if( !TOKEN_STEM.matcher(stem).matches() || HEX.matcher(stem).matches() || Cid.isCid(stem) )
            return false
        int switches = 0
        for( int i = 1; i < stem.length(); i++ ) {
            if( Character.isDigit(stem.charAt(i)) != Character.isDigit(stem.charAt(i - 1)) )
                switches++
        }
        return switches >= minSwitches
    }

    /**
     * A URI whose userinfo holds an unescaped '/', with that userinfo dropped
     * whole and the query and fragment cut; any other URI as it is, for
     * {@link Leaks#withoutCredentials} to drop an ordinary userinfo.
     *
     * Such a URI is ambiguous. RFC 3986 ends the authority at the first '/',
     * so {@code https://user:p@a/ss@host} is host {@code a} with path
     * {@code /ss@host}; it is also user {@code user}, password {@code p@a/ss}
     * and host {@code host}. The rule reads every such shape the second way,
     * so no part of a password can become a name. When the first segment
     * (the text after the scheme up to the first '/') holds a userinfo, an
     * '@' or a ':' that starts no port number, the userinfo runs to the last
     * '@' of the first later segment that holds an '@'. After that segment the
     * path is the URI's own, so {@code user:pa/ss@host/dir/a@b.fq} names
     * {@code a@b.fq}, and {@code user:pa/s@s@host/x.fq} names {@code x.fq}.
     * The cost falls on a plain userinfo followed by an '@' in the path:
     * {@code https://user:pass@host/data/x@1.fq} is read as a password
     * {@code pass@host/data/x} and named {@code 1.fq}. A first segment with no
     * userinfo ({@code host:8443/a@b.fq}, {@code [::1]/a@b.fq}, {@code bucket/a@b.fq})
     * ends the authority as RFC 3986 says, and a URI with no '@' after its
     * first '/' is left to {@link Leaks#withoutCredentials}.
     */
    private static String withoutSlashedUserinfo(String uri) {
        final Matcher scheme = SCHEME.matcher(uri)
        if( !scheme.lookingAt() )
            return uri
        String rest = uri.substring(scheme.end())
        final int query = firstOf(rest, '?', '#')
        if( query >= 0 )
            rest = rest.substring(0, query)
        final int slash = rest.indexOf('/')
        if( slash < 0 || !holdsUserinfo(rest.substring(0, slash)) )
            return uri
        final int at = rest.indexOf('@', slash)
        if( at < 0 )
            return uri
        final int segmentEnd = rest.indexOf('/', at)
        final int last = rest.lastIndexOf('@', segmentEnd < 0 ? rest.length() : segmentEnd)
        return uri.substring(0, scheme.end()) + rest.substring(last + 1)
    }

    /** Whether a URI's first segment holds a userinfo: an '@', or a ':' that starts no port number. */
    private static boolean holdsUserinfo(String segment) {
        if( segment.contains('@') )
            return true
        final int colon = segment.indexOf(':')
        return colon >= 0 && !segment.startsWith('[') && !PORT.matcher(segment.substring(colon + 1)).matches()
    }

    /**
     * A path's base name, scrubbed ({@link #scrubbed}). A remote provider's
     * file name may keep the URI's query (a presigned signature, a SAS token),
     * so for a path off the default file system it is cut at the first '?' or
     * '#' as well. A path with no file name, one whose URI has a userinfo
     * holding a '/' ({@link #withoutSlashedUserinfo}), and a Box shared link
     * give their URI's base name instead ({@link #baseName}). A remote file
     * whose own name holds '?', '#' or ';' loses what follows; a local name is
     * kept whole, short of a credential shape.
     */
    private static String fileNameOf(Path path) {
        if( path == null )
            return null
        final Path name = path.fileName
        if( isLocal(path) || path instanceof CasPath )
            return name == null ? null : scrubbed(name.toString(), false, false)
        String uri = null
        try {
            uri = path.toUri().toString()
        }
        catch( Exception ignored ) {
        }
        final boolean byUri = uri != null &&
            (withoutSlashedUserinfo(uri) != uri || isBoxSharedLink(Leaks.withoutCredentials(uri)))
        if( name != null && !byUri ) {
            String text = name.toString()
            final int query = firstOf(text, '?', '#')
            if( query >= 0 )
                text = text.substring(0, query)
            final String base = scrubbed(lastSegment(text), true, false)
            if( base )
                return base
        }
        return uri == null ? null : baseName(uri)
    }

    private static int firstOf(String text, String a, String b) {
        final int i = text.indexOf(a)
        final int j = text.indexOf(b)
        return i < 0 ? j : (j < 0 ? i : Math.min(i, j))
    }
}
