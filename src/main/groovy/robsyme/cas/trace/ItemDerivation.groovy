package robsyme.cas.trace

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun
import robsyme.cas.core.Cid
import robsyme.cas.core.InputSet
import robsyme.cas.core.ItemLineage
import robsyme.cas.core.Records

/**
 * Which run inputs and which publishing tasks gave each output item (cross-run
 * lineage spec §5.2). The observer notes each publish's work-dir source and
 * each completed or cached task's work dir and staged files as they happen;
 * {@link #derive} walks them at completion, in memory, touching no file system.
 *
 * From each of an item's published files the walk finds the task whose work
 * directory holds it, then that task's staged files: a run input is a hit, a
 * file under another task's work directory is followed the same way. A
 * published file that is itself one of the task's staged links maps to that
 * staged file alone. Each task is visited once per item and per such link, so
 * a cycle ends. Only the task that published a file is named, never one the
 * walk passes through. Safe to note from several threads at once.
 */
@Slf4j
@CompileStatic
class ItemDerivation {

    /** What one task leaves for the walk: paths and its reference, never the TaskRun, which would pin its values. */
    @PackageScope
    @CompileStatic
    static final class Noted {
        /** The task as an item names it, its tag scrubbed. */
        final ItemLineage.TaskRef ref
        /** Absolute and normalized. */
        final Path workDir
        final List<InputRecorder.Staged> staged

        /** Each staged file by where Nextflow linked it in the work dir, built on the walk's first look. */
        private Map<Path, List<InputRecorder.Staged>> byLink

        Noted(ItemLineage.TaskRef ref, Path workDir, List<InputRecorder.Staged> staged) {
            this.ref = ref
            this.workDir = workDir
            this.staged = staged
        }

        /** The staged files linked at {@code file}, empty when it is not a staged link; one pass over the staged list per task. */
        List<InputRecorder.Staged> linkedAt(Path file) {
            if( byLink == null ) {
                final Map<Path, List<InputRecorder.Staged>> links = new HashMap<Path, List<InputRecorder.Staged>>()
                for( InputRecorder.Staged s : staged ) {
                    if( s.stageName != null )
                        links.computeIfAbsent(workDir.resolve(s.stageName).normalize(), { Path p -> new ArrayList<InputRecorder.Staged>() }).add(s)
                }
                byLink = links
            }
            return byLink.get(file) ?: Collections.<InputRecorder.Staged> emptyList()
        }
    }

    /** A file in a task's work dir, still to be followed back to run inputs. */
    @CompileStatic
    private static final class Step {
        final Noted task
        final Path file

        Step(Noted task, Path file) {
            this.task = task
            this.file = file
        }
    }

    /** One item's walk: what it found and where it has been. */
    @CompileStatic
    private static final class Walk {
        final Set<Integer> positions = new HashSet<Integer>()
        final Set<ItemLineage.TaskRef> tasks = new HashSet<ItemLineage.TaskRef>()
        final Set<String> visited = new HashSet<String>()
    }

    /** Publish join key to the work-dir file published, absolute and normalized. */
    private final Map<String, Path> published = new ConcurrentHashMap<String, Path>()

    /** Task hash to what the task left. */
    private final Map<String, Noted> tasks = new ConcurrentHashMap<String, Noted>()

    /** Remembers the file published under {@code joinKey}; the last call for a key wins. */
    void notePublish(String joinKey, Path source) {
        final Path at = normalized(source)
        if( joinKey != null && at != null )
            published.put(joinKey, at)
    }

    void noteTask(TaskRun task, List<InputRecorder.Staged> staged) {
        if( task?.hash == null || task.workDir == null )
            return
        noteTask(task.hash.toString(), task.workDir, task.processor?.name, tagOf(task), staged)
    }

    /**
     * Remembers a task by hash. A null hash or work dir is ignored, and so is a task an ItemLineage
     * could not name (a hash that is not 32 lowercase hex, a blank process), so that one task cannot
     * cost every item its lineage.
     */
    void noteTask(String hash, Path workDir, String process, String tag, List<InputRecorder.Staged> staged) {
        final Path dir = normalized(workDir)
        if( hash == null || dir == null )
            return
        final ItemLineage.TaskRef ref
        try {
            ref = new ItemLineage.TaskRef(hash, process, Records.scrubText(tag))
        }
        catch( IllegalArgumentException e ) {
            log.warn("task ${hash} is left out of item lineage: ${e.message}")
            return
        }
        final List<InputRecorder.Staged> copy = Collections.unmodifiableList(
            new ArrayList<InputRecorder.Staged>(staged ?: Collections.<InputRecorder.Staged> emptyList()))
        tasks.put(hash, new Noted(ref, dir, copy))
    }

    /** The task noted under {@code hash}, or null. */
    @PackageScope
    Noted noted(String hash) {
        return tasks.get(hash)
    }

    /**
     * One entry per distinct item of {@code outputs} that has an input position
     * in {@code set} or a publishing task, merged across outputs.
     *
     * @param runInputs each staged file the InputRecorder kept, to its Input in {@code set}
     */
    List<ItemLineage.Derived> derive(List<Join.JoinedOutput> outputs, InputSet set,
                                     Map<InputRecorder.Staged, InputSet.Input> runInputs) {
        final Map<Path, Noted> byWorkDir = new HashMap<Path, Noted>()
        for( Noted t : tasks.values() )
            byWorkDir.put(t.workDir, t)
        final Map<Cid, Walk> walks = new LinkedHashMap<Cid, Walk>()
        for( Join.JoinedOutput output : outputs ) {
            for( int i = 0; i < output.itemCids.size(); i++ ) {
                final Walk walk = walks.computeIfAbsent(output.itemCids.get(i), { Cid c -> new Walk() })
                for( String key : output.itemKeys.get(i) ) {
                    final Path source = published.get(key)
                    final Noted task = source == null ? null : holding(source, byWorkDir)
                    if( task == null )
                        continue
                    walk.tasks.add(task.ref)
                    follow(task, source, walk, byWorkDir, set, runInputs)
                }
            }
        }
        final List<ItemLineage.Derived> out = new ArrayList<ItemLineage.Derived>()
        walks.each { Cid item, Walk walk ->
            if( walk.positions || walk.tasks )
                out.add(ItemLineage.Derived.of(item, walk.positions, walk.tasks))
        }
        return out
    }

    /** From {@code source} in {@code start}'s work dir back to run inputs, through every upstream task. */
    private static void follow(Noted start, Path source, Walk walk, Map<Path, Noted> byWorkDir, InputSet set,
                               Map<InputRecorder.Staged, InputSet.Input> runInputs) {
        final Deque<Step> pending = new ArrayDeque<Step>()
        pending.push(new Step(start, source))
        while( !pending.isEmpty() ) {
            final Step step = pending.pop()
            final Noted task = step.task
            final Path file = step.file
            final List<InputRecorder.Staged> picked = task.linkedAt(file)
            if( !walk.visited.add(task.ref.hash + '\u0000' + (picked ? file.toString() : '*')) )
                continue
            for( InputRecorder.Staged s : picked ?: task.staged ) {
                final InputSet.Input input = runInputs.get(s)
                if( input != null ) {
                    final int at = set.positionOf(input)
                    if( at >= 0 )
                        walk.positions.add(at)
                    continue
                }
                final Path upstreamFile = s.source instanceof Path ? normalized((Path) s.source) : null
                final Noted upstream = upstreamFile == null ? null : holding(upstreamFile, byWorkDir)
                if( upstream != null )
                    pending.push(new Step(upstream, upstreamFile))
            }
        }
    }

    /** The task whose work dir holds {@code file}, nearest first, or null. */
    private static Noted holding(Path file, Map<Path, Noted> byWorkDir) {
        for( Path dir = file.parent; dir != null; dir = dir.parent ) {
            final Noted task = byWorkDir.get(dir)
            if( task != null )
                return task
        }
        return null
    }

    /** {@code path} absolute and normalized, or null when it is null or has no absolute form (a relative cas path). */
    private static Path normalized(Path path) {
        if( path == null )
            return null
        try {
            return path.toAbsolutePath().normalize()
        }
        catch( Exception e ) {
            log.debug("'${path}' has no absolute form: ${e.message}")
            return null
        }
    }

    /** The task's trimmed tag, or null when it has none or it cannot be evaluated. */
    private static String tagOf(TaskRun task) {
        try {
            if( task.config == null || !task.config.containsKey('tag') )
                return null
            final Object tag = task.config.get('tag')
            return tag == null ? null : String.valueOf(tag).trim()
        }
        catch( Exception e ) {
            log.debug("the tag of task ${task.hash} could not be read: ${e.message}")
            return null
        }
    }
}
