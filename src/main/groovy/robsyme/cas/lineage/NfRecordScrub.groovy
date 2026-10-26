package robsyme.cas.lineage

import java.nio.file.Path
import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.extension.FilesEx
import nextflow.lineage.model.v1beta1.DataPath
import nextflow.lineage.model.v1beta1.Parameter
import nextflow.lineage.model.v1beta1.TaskOutput
import nextflow.lineage.model.v1beta1.TaskRun
import nextflow.lineage.model.v1beta1.Workflow
import nextflow.lineage.model.v1beta1.WorkflowOutput
import nextflow.lineage.model.v1beta1.WorkflowRun
import nextflow.lineage.serde.LinSerializable
import nextflow.script.WorkflowMetadata
import robsyme.cas.core.Leaks
import robsyme.cas.core.Records

/**
 * The location scrub for Nextflow's own lineage records, applied by
 * {@link CasLinStore#save} before {@code DefaultLinStore} writes them under a
 * member's nf/ (DESIGN.md §10). A member's nf/ is shared with everyone who can
 * read the member, so the records lose the launching machine's paths, home
 * directory and user name, judged by the same rules as a RunManifest
 * ({@link Records#scrub}, which uses {@code Leaks}).
 *
 * What Nextflow reads back to resolve lineage is kept: record keys, `lid://`
 * and `cas://` references, checksums, and every {@code FileOutput}, whose
 * {@code path} is the file a `lid://` read opens and `nextflow lineage check`
 * hashes. Redacting a work-dir FileOutput.path makes both fail with "Target
 * path [redacted-location] does not exist" (measured on 26.04.6), so a
 * work-dir path stays in nf/; only a published path becomes `cas://<cid>/…`.
 *
 * Every replacement value is computed before any field is assigned, so a
 * scrub that throws leaves the record as Nextflow handed it over (CasLinStore
 * then saves it unscrubbed). Fields are replaced on that record, so a field a
 * newer Nextflow adds survives, unscrubbed; the values inside are copied,
 * never changed, because a TaskRun's `val` inputs are the run's own objects.
 */
@CompileStatic
class NfRecordScrub {

    private static final String LOCATION = Records.REDACTED_LOCATION
    private static final String USER = Records.REDACTED_USER
    private static final String SECRET = Records.REDACTED_SECRET

    /** A marker a scrubbed string may hold, in place of the text it redacted. */
    private static final Pattern MARKER = Pattern.compile(
        Pattern.quote(LOCATION) + '|' + Pattern.quote(USER) + '|' + Pattern.quote(SECRET))

    /**
     * Text is replaced in a script only where it is not part of a longer path or word, nor the
     * tail of an expansion (`${PWD}/tmp`, `$(pwd)/tmp`, `$HOME`). Text after `}`, `)` or `$`
     * continues an expansion, so a host directory there stays too: `${X}/home/rob` keeps `rob`.
     */
    private static final String BEFORE = '(?<![A-Za-z0-9_.\\-/})$])'
    private static final String AFTER = '(?![A-Za-z0-9_\\-])'

    /** Redacted text shorter than this is not looked for in a script. */
    private static final int MIN_LITERAL = 3

    /**
     * A secret that a name carried shorter than this is not looked for in a script: it is as
     * likely to be ordinary code. That is a value under a secret-named key
     * (`key_column = 'sample'`) or one the flag, assignment, JSON-key, -D or bare-name rules found
     * (`--key sample`, `--auth basic`). A credential found by its own shape (a token, a key id, a
     * Bearer or Basic credential, a PEM key) is looked for from {@link #MIN_LITERAL}.
     */
    private static final int MIN_SECRET_LITERAL = 8

    /** The words inside the markers, which a script holds once a marker is written into it. */
    private static final Set<String> MARKER_WORDS = Collections.unmodifiableSet(
        [LOCATION, USER, SECRET].collect { String m -> m.substring(1, m.length() - 1) } as Set<String>)

    /** WorkflowRun.metadata fields that identify the pipeline, kept as the RunManifest keeps them. */
    private static final Set<String> PIPELINE_IDENTITY = ['repository', 'commitId', 'revision'] as Set

    /** The run's project repository URL; a path under it is a public source link, not a host path. */
    private final String repository

    /** The run's own host directories, longest first. */
    final List<String> locations

    private final List<Pattern> hostLocations

    /** What the scrub redacts in the run's params, and its marker: a script that spells it loses it too. */
    private final Map<String, String> paramsRedacted

    NfRecordScrub(String repository, Collection<String> hostLocations) {
        this(repository, hostLocations, null)
    }

    NfRecordScrub(String repository, Collection<String> hostLocations, Map params) {
        this.repository = repository ?: null
        final List<String> sorted = new ArrayList<String>(new LinkedHashSet<String>(
            hostLocations.findAll { String it -> it != null && it.length() > 1 && it != '/' }))
        sorted.sort { String a, String b -> b.length() <=> a.length() }
        this.locations = Collections.unmodifiableList(sorted)
        this.hostLocations = sorted.collect { String loc -> Pattern.compile(BEFORE + Pattern.quote(loc) + AFTER) }
        final Map<String, String> redacted = new LinkedHashMap<String, String>()
        if( params != null )
            data(params, true, redacted)
        this.paramsRedacted = Collections.unmodifiableMap(redacted)
    }

    /**
     * The scrub for a running session: its repository, its launch, project,
     * work and home directories as plain paths and as `file://` or `s3://` URIs,
     * and its params.
     */
    static NfRecordScrub forSession(Session session) {
        final WorkflowMetadata meta = session?.workflowMetadata
        final List<String> locations = new ArrayList<String>()
        if( meta != null ) {
            for( Path dir : [meta.launchDir, meta.projectDir, meta.workDir, meta.homeDir] )
                addLocation(locations, dir)
        }
        addLocation(locations, session?.workDir)
        return new NfRecordScrub(meta?.repository, locations, (Map) session?.params)
    }

    private static void addLocation(List<String> locations, Path dir) {
        if( dir == null )
            return
        final String uri = FilesEx.toUriString(dir)
        locations.add(uri)
        locations.add(dir.toString())
        if( uri.startsWith('/') )
            locations.add('file://' + uri)
    }

    /** The record with its location-bearing fields replaced; a FileOutput, or any other kind, unchanged. */
    LinSerializable scrub(LinSerializable value) {
        if( value instanceof WorkflowRun )
            return workflowRun((WorkflowRun) value)
        if( value instanceof TaskRun )
            return taskRun((TaskRun) value)
        if( value instanceof TaskOutput ) {
            final TaskOutput output = (TaskOutput) value
            final List<Parameter> outputs = parameters(output.output, false, null)
            output.output = outputs
            return output
        }
        if( value instanceof WorkflowOutput ) {
            final WorkflowOutput output = (WorkflowOutput) value
            final List<Parameter> outputs = parameters(output.output, false, null)
            output.output = outputs
            return output
        }
        return value
    }

    private WorkflowRun workflowRun(WorkflowRun run) {
        final Workflow workflow = run.workflow
        final List<DataPath> scriptFiles = workflow != null ? dataPaths(workflow.scriptFiles, null) : null
        // params and config are scrubbed as the RunManifest scrubs them, secret-named keys included.
        final List<Parameter> params = parameters(run.params, true, null)
        final Map config = (Map) data(run.config, true, null)
        final Map metadata = metadata(run.metadata)
        if( workflow != null )
            workflow.scriptFiles = scriptFiles
        run.params = params
        run.config = config
        run.metadata = metadata
        return run
    }

    private Map metadata(Map metadata) {
        if( metadata == null )
            return null
        final Map out = (Map) data(metadata, false, null)
        for( String field : PIPELINE_IDENTITY ) {
            if( metadata.containsKey(field) )
                out.put(field, metadata.get(field))
        }
        // A command line is judged token by token: one that starts with the
        // launcher's path keeps the rest of its shape.
        final Object commandLine = metadata.get('commandLine')
        if( commandLine instanceof CharSequence )
            out.put('commandLine', Records.scrubText(commandLine.toString()))
        // The user name goes whether or not it is this JVM's (a head job's user
        // is often not the person who launched it).
        if( metadata.get('userName') )
            out.put('userName', USER)
        final Object containers = metadata.get('container')
        if( containers instanceof Map ) {
            final Map<Object, Object> images = new LinkedHashMap<Object, Object>()
            for( Map.Entry entry : ((Map<?, ?>) containers).entrySet() )
                images.put(entry.key, entry.value instanceof CharSequence ? container(entry.value.toString()) : entry.value)
            out.put('container', images)
        }
        else if( containers instanceof CharSequence ) {
            out.put('container', container(containers.toString()))
        }
        return out
    }

    private TaskRun taskRun(TaskRun task) {
        // What the scrub redacts in the task's own values, so the script loses it as well.
        final Map<String, String> redacted = new LinkedHashMap<String, String>()
        final List<Parameter> input = parameters(task.input, false, redacted)
        final String conda = (String) data(task.conda, false, redacted)
        final String spack = (String) data(task.spack, false, redacted)
        // Nextflow records each params.x the script reads as globalVars['params.x'], so a
        // secret-named key goes here as it does in the run's params (task.ext.token too).
        final Map globalVars = (Map) data(task.globalVars, true, redacted)
        final List<DataPath> binEntries = dataPaths(task.binEntries, redacted)
        final String container = container(task.container)
        final String script = code(task.script, redacted)
        task.script = script
        task.input = input
        task.container = container
        task.conda = conda
        task.spack = spack
        task.globalVars = globalVars
        task.binEntries = binEntries
        return task
    }

    /** Script text scrubbed as {@link #code(String, Map)} does, with only the run's own redactions. */
    String code(String text) {
        return code(text, null)
    }

    /**
     * Script text with three things replaced where they stand as a whole path
     * or word: what the scrub redacted in the run's params, what it redacted in
     * this task's own values ({@code redacted}), and the run's host
     * directories. Code is not scrubbed token by token: {@link Records#scrubText}
     * would read the `/.* /` in `sed 's/.* //'` as a path and the record would
     * misreport what ran. Other absolute paths a script spells stay.
     */
    String code(String text, Map<String, String> redacted) {
        if( text == null )
            return null
        final Map<String, String> literals = new HashMap<String, String>(paramsRedacted)
        if( redacted != null )
            literals.putAll(redacted)
        final List<String> longestFirst = new ArrayList<String>(literals.keySet())
        longestFirst.sort { String a, String b -> b.length() <=> a.length() }
        String out = text
        for( String literal : longestFirst ) {
            if( out.contains(literal) )
                out = Pattern.compile(BEFORE + Pattern.quote(literal) + AFTER).matcher(out).replaceAll(Matcher.quoteReplacement(literals.get(literal)))
        }
        for( Pattern location : hostLocations )
            out = location.matcher(out).replaceAll(Matcher.quoteReplacement(LOCATION))
        return out
    }

    /** A registry image name stays; a local image file (`/x/bwa.sif`, `file:///x/bwa.sif`) goes. */
    private static String container(String image) {
        if( image == null )
            return null
        return image.startsWith('/') || image.startsWith('file:') ? LOCATION : image
    }

    private List<Parameter> parameters(List<Parameter> params, boolean secrets, Map<String, String> redacted) {
        if( params == null )
            return null
        // The names are a map's keys, as a RunManifest's top-level params are, and are named the same way.
        final List<String> named = new ArrayList<String>()
        for( Parameter p : params ) {
            if( p != null && p.name != null && !underRepository(p.name) )
                named.add(p.name)
        }
        final Map<Object, Object> names = Leaks.keyNames(named)
        final List<Parameter> out = new ArrayList<Parameter>(params.size())
        for( Parameter p : params ) {
            if( p == null ) {
                out.add(null)
                continue
            }
            final String name = names.containsKey(p.name) ? (String) names.get(p.name) : p.name
            if( name != p.name )
                string(p.name, redacted)
            final boolean secretName = secrets && p.name != null && (Records.isSecretKey(p.name) || Records.isSecretKey(name))
            final Object value = secretName ? secret(p.value, redacted) : data(p.value, secrets, redacted)
            out.add(new Parameter(p.type, name, value))
        }
        return out
    }

    private List<DataPath> dataPaths(List<DataPath> paths, Map<String, String> redacted) {
        if( paths == null )
            return null
        final List<DataPath> out = new ArrayList<DataPath>(paths.size())
        for( DataPath p : paths )
            out.add(p == null ? null : new DataPath(string(p.path, redacted), p.checksum))
        return out
    }

    /**
     * A copy of the value with every string judged by {@link Records#scrub}:
     * strings, Paths, Files, URIs and URLs by their text; numbers, booleans,
     * dates and anything else kept as they are. With {@code secrets}, a value
     * under a secret-named key goes as well, as in a RunManifest's params and a
     * task's globalVars. A task's inputs skip that rule: a `val(key)` input is
     * usually a sample key. What is redacted goes into {@code redacted} (may be null).
     */
    private Object data(Object value, boolean secrets, Map<String, String> redacted) {
        if( value == null )
            return null
        if( value instanceof CharSequence || value instanceof File || value instanceof URI || value instanceof URL )
            return string(value.toString(), redacted)
        if( value instanceof Path )
            return string(FilesEx.toUriString((Path) value), redacted)
        if( value instanceof DataPath )
            return new DataPath(string(((DataPath) value).path, redacted), ((DataPath) value).checksum)
        if( value instanceof Map ) {
            // Keys are named as the RunManifest scrub names them: a key the scrub would change
            // becomes the marker for what it held, numbered past any key the map keeps. A key
            // under the run's repository stays, as such a value does, and takes no number.
            final Map<Object, Object> names = Leaks.keyNames(((Map<?, ?>) value).keySet().findAll { !underRepository(String.valueOf(it)) })
            final Map<Object, Object> out = new LinkedHashMap<Object, Object>()
            for( Map.Entry entry : ((Map<?, ?>) value).entrySet() ) {
                final String text = String.valueOf(entry.key)
                final Object key = names.containsKey(entry.key) ? names.get(entry.key) : entry.key
                // What the key held is looked for in the script, as a value's pieces are.
                if( key != entry.key )
                    string(text, redacted)
                final boolean secretKey = secrets && (Records.isSecretKey(text) || Records.isSecretKey(String.valueOf(key)))
                out.put(key, secretKey ? secret(entry.value, redacted) : data(entry.value, secrets, redacted))
            }
            return out
        }
        if( value instanceof Collection || value instanceof Object[] ) {
            final Collection items = value instanceof Collection ? (Collection) value : Arrays.asList((Object[]) value)
            final List<Object> out = new ArrayList<Object>(items.size())
            for( Object item : items )
                out.add(data(item, secrets, redacted))
            return out
        }
        return value
    }

    /** Records.REDACTED_SECRET for a value under a secret-named key, as {@link Records#scrub} writes it. */
    private Object secret(Object value, Map<String, String> redacted) {
        if( value instanceof Number || value instanceof Path || (value instanceof CharSequence && value.toString().length() > 0) ) {
            // A number (`max_tokens = 1000`) or a short string is not looked for in a script, where it is as likely code.
            if( !(value instanceof Number) && value.toString().length() >= MIN_SECRET_LITERAL )
                remember(value.toString(), SECRET, redacted)
            return SECRET
        }
        if( value instanceof Collection || value instanceof Object[] ) {
            final Collection items = value instanceof Collection ? (Collection) value : Arrays.asList((Object[]) value)
            final List<Object> out = new ArrayList<Object>(items.size())
            for( Object item : items )
                out.add(secret(item, redacted))
            return out
        }
        return data(value, true, redacted)
    }

    /**
     * One string: a path under the run's own repository stays; a string equal to what the scrub
     * redacted in the run's params becomes its marker, so `val(params.db_password)` goes while a
     * `val(key)` input's name stays ordinary; anything else is judged by Records.scrub.
     */
    private String string(String text, Map<String, String> redacted) {
        if( text == null )
            return null
        if( underRepository(text) )
            return text
        // Null while the constructor collects the params' own redactions.
        if( paramsRedacted != null && paramsRedacted.containsKey(text) )
            return paramsRedacted.get(text)
        final String scrubbed = (String) Records.scrub(text)
        if( redacted != null && scrubbed != text )
            collect(text, scrubbed, redacted)
        return scrubbed
    }

    /** True for the run's repository URL or a path under it: a public source link, kept. */
    private boolean underRepository(String text) {
        return repository != null && (text == repository || text.startsWith(repository + '/'))
    }

    /**
     * The pieces of {@code original} that {@code scrubbed} replaced by a marker,
     * found by matching the original against the scrubbed text with each marker
     * as a wildcard: `--ref /data/g.fa` scrubbed to `--ref [redacted-location]`
     * yields `/data/g.fa`. A `file://` piece also yields its bare path, the form
     * a script interpolates.
     */
    private static void collect(String original, String scrubbed, Map<String, String> redacted) {
        final Matcher marker = MARKER.matcher(scrubbed)
        final StringBuilder regex = new StringBuilder('^')
        final List<String> kinds = new ArrayList<String>()
        final List<Boolean> afterAuthWord = new ArrayList<Boolean>()
        int last = 0
        while( marker.find() ) {
            regex.append(Pattern.quote(scrubbed.substring(last, marker.start()))).append('(.+?)')
            kinds.add(marker.group())
            afterAuthWord.add(AUTH_WORD.matcher(scrubbed.substring(0, marker.start())).find())
            last = marker.end()
        }
        if( kinds.isEmpty() )
            return
        regex.append(Pattern.quote(scrubbed.substring(last))).append('$')
        final Matcher m = Pattern.compile(regex.toString(), Pattern.DOTALL).matcher(original)
        if( !m.matches() )
            return
        for( int i = 0; i < kinds.size(); i++ ) {
            final String piece = m.group(i + 1)
            // A secret a name carried (`--key sample`, `password=x`) is as likely to be code when short;
            // one found by its own shape (a token, a key id, a Basic credential) is not.
            if( kinds[i] == SECRET && piece.length() < MIN_SECRET_LITERAL && !afterAuthWord[i] && Leaks.credentialKind(piece) == null )
                continue
            remember(piece, kinds[i], redacted)
            if( piece.startsWith('file://') )
                remember(piece.substring('file://'.length()), kinds[i], redacted)
        }
    }

    /** `Bearer` or `Basic` and a space at the end of text: the marker after it replaced a token found by its shape. */
    private static final Pattern AUTH_WORD = Pattern.compile('(?i)\\b(?:bearer|basic)[ \\t]+\\z')

    /**
     * Records a redacted piece for the script, unless it is too short to be told from code
     * ({@link #MIN_LITERAL}), is a marker or holds one, or is a marker's inner word, which would
     * match inside a marker already written.
     */
    private static void remember(String piece, String marker, Map<String, String> redacted) {
        if( redacted == null || piece.length() < MIN_LITERAL )
            return
        if( piece == marker || MARKER.matcher(piece).find() || MARKER_WORDS.contains(piece) )
            return
        redacted.put(piece, marker)
    }
}
