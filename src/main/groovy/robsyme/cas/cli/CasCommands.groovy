package robsyme.cas.cli

import java.nio.file.Path
import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Const
import nextflow.SysEnv
import nextflow.cli.Launcher
import nextflow.config.ConfigBuilder
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.Claim
import robsyme.cas.core.DagJson
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.Put
import robsyme.cas.core.PutError
import robsyme.cas.core.PutResult
import robsyme.cas.core.Records
import robsyme.cas.core.SnapshotBase
import robsyme.cas.explore.ExploreCommand

/**
 * `nextflow plugin nf-blocks:<verb>` (DESIGN.md §15). Builds the config the
 * way PluginAbstractExec does, but returns a real exit status: that trait
 * swallows every exception and returns 0.
 */
@Slf4j
@CompileStatic
class CasCommands {

    static final List<String> VERBS = ['explore', 'items', 'prune', 'put', 'snapshot', 'sweep', 'untrash']

    /** The verb's clock, which stamps the name Claim of put --name; a test seam. */
    Closure<Long> clock = { -> System.currentTimeMillis() } as Closure<Long>

    int exec(Launcher launcher, String pluginId, String cmd, List<String> args) {
        final Map config
        try {
            config = new ConfigBuilder()
                .setOptions(launcher.options)
                .setBaseDir(Paths.get('.'))
                .build()
        }
        catch( Exception e ) {
            System.err.println("nf-blocks:${cmd}: could not read the Nextflow config: ${e.message}")
            return 1
        }
        return run(cmd, args, config, System.out, System.err)
    }

    /**
     * Nextflow >= 26.08.0-edge (nextflow 1dc8cf68f): no Launcher, so no
     * {@code -c}; the config is read from the default files alone
     * ({@link LaunchConfig}).
     */
    int exec(String pluginId, String cmd, List<String> args) {
        return exec(pluginId, cmd, args, Const.APP_HOME_DIR, Paths.get('.'), SysEnv.get(), System.out, System.err)
    }

    int exec(String pluginId, String cmd, List<String> args, Path homeDir, Path launchDir, Map<String, String> env,
             PrintStream out, PrintStream err) {
        // A usage error needs no config, so a broken config file cannot turn it into exit 1.
        if( !(cmd in VERBS) ) {
            err.println(usage(cmd))
            return 2
        }
        final Path base = launchDir.toAbsolutePath().normalize()
        final Map config
        try {
            final List<Path> files = LaunchConfig.files(homeDir.toAbsolutePath().normalize(), base, env)
            // -c never reaches the plugin on this path, so a verb could act on another
            // store than the one meant: say what was read (review round 1).
            err.println(configNotice(cmd, files))
            config = LaunchConfig.read(files, base, env)
        }
        catch( Exception e ) {
            log.debug("nf-blocks:${cmd}: could not read the Nextflow config", e)
            err.println("nf-blocks:${cmd}: could not read the Nextflow config: ${e.message}")
            return 1
        }
        return run(cmd, args, config, out, err)
    }

    static String configNotice(String cmd, List<Path> files) {
        final String read = files
            ? "nf-blocks:${cmd}: read config ${files.join(', ')}"
            : "nf-blocks:${cmd}: read no Nextflow config file (none at \$NXF_HOME/config or ./nextflow.config)"
        return read + "\nnf-blocks:${cmd}: on Nextflow 26.08.0-edge and later, -c does not reach plugin verbs; " +
            'name the config with NXF_CONFIG_FILE=<file> or ./nextflow.config'
    }

    int run(String cmd, List<String> args, Map config, PrintStream out, PrintStream err) {
        return run(cmd, args, config, out, err, System.in)
    }

    int run(String cmd, List<String> args, Map config, PrintStream out, PrintStream err, InputStream stdin) {
        if( !(cmd in VERBS) ) {
            err.println(usage(cmd))
            return 2
        }
        try {
            switch( cmd ) {
                case 'snapshot':
                    return snapshot(Options.parse(args, [] as Set), config, out)
                case 'explore':
                    return ExploreCommand.run(args, config, out, err)
                case 'items':
                    return ItemsCommand.run(args, config, out, err)
                case 'put':
                    return put(Options.parse(args, ['dry-run', 'name'] as Set), config, out, err, stdin, clock)
                case 'sweep':
                    return RetentionCommands.sweep(Options.parse(args, ['apply', 'wait', 'budget', 'format'] as Set), config, out, err)
                case 'prune':
                    return RetentionCommands.prune(Options.parse(args, ['keep-last', 'keep-newer', 'pipeline', 'apply'] as Set), config, out, err, clock)
                case 'untrash':
                    return RetentionCommands.untrash(Options.parse(args, ['sweep'] as Set), config, out, err)
            }
            return 2
        }
        catch( UsageException e ) {
            err.println("nf-blocks:${cmd}: ${e.message}")
            return 2
        }
        catch( Exception e ) {
            log.debug("nf-blocks:${cmd} failed", e)
            err.println("nf-blocks:${cmd}: ${e.message}")
            return 1
        }
    }

    private static String usage(String cmd) {
        final String head = cmd ? "unknown command 'nf-blocks:${cmd}'" : 'no command given'
        return "${head}; usage: nextflow plugin nf-blocks:<command>\ncommands:\n" +
            '  explore [--port <n>]   serve the explorer and this composition\'s members on loopback\n' +
            '  items <output> [<path>=<value> ...] --run <ref>[,<ref>...] [--pipeline <id>] [--format csv|json|occurrences|selection]\n' +
            '                         one output\'s items across runs, as a samplesheet, occurrences or a put request; read-only\n' +
            '  put <file|/dev/stdin> [--dry-run] [--name <name>]  build and write one Selection or Claim from DAG-JSON;\n' +
            '                         a member may be an Item Occurrence, cas://<collection>/<item>; --name then names the Selection:\n' +
            '                         nextflow -q plugin nf-blocks:items ... --format selection | nextflow -q plugin nf-blocks:put /dev/stdin --name <name>\n' +
            '  snapshot               rewrite the writable member\'s Index Snapshot at any size\n' +
            '  sweep [--apply true] [--wait true] [--budget <size>] [--format text|json]\n' +
            '                         dry run unless --apply: roots, live, dead and Trash; --apply trashes the newly dead and deletes what is past its grace\n' +
            '  prune (--keep-last <n> | --keep-newer <period>) [--pipeline <id>] [--apply true]\n' +
            '                         dry run unless --apply: release the content of older runs, keeping their lineage (set retain "lineage")\n' +
            '  untrash (<cid>... | --sweep <id>)\n' +
            '                         take blocks out of the Trash ledger; a block nothing reaches is trashed again by the next sweep'
    }

    /**
     * Builds and writes one client-constructible block (spec section 9.2); the body goes to stdout either way.
     * With --name, the Selection is then named (decision 8 of the milestone 3 plan).
     */
    private static int put(Options options, Map config, PrintStream out, PrintStream err, InputStream stdin, Closure<Long> clock) {
        if( options.positionals.size() != 1 )
            throw new UsageException("put takes one file (or /dev/stdin), got ${options.positionals ?: 'none'}")
        final String dry = options.flag('dry-run')
        if( !(dry in [null, 'true', 'false']) )
            throw new UsageException("--dry-run is a flag, got '${dry}'")
        final String name = nameOption(options.flag('name'))
        final String source = options.positionals[0]
        final byte[] body = readCapped(source in STDIN_NAMES ? stdin : new FileInputStream(source))
        if( name != null ) {
            final String kind = requestKind(body)
            if( kind != null && kind != Records.SELECTION )
                throw new UsageException("--name names a Selection; this request is a ${kind}")
        }
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        // Ticket 03 decision 2: a ClockSkewException reaches run(), which prints it and exits 1.
        cas.checkClock()
        final Index index = cas.openIndex()
        try {
            final Put builder = cas.newPut(index)
            PutResult saved = null
            try {
                saved = builder.put(body, dry == 'true')
            }
            catch( PutError e ) {
                out.println(new String(e.body(), 'UTF-8'))
                return 1
            }
            out.println(new String(saved.body(), 'UTF-8'))
            return name == null ? 0 : nameSelection(builder, saved, body, name, dry == 'true', out, err, clock)
        }
        finally {
            index.close()
        }
    }

    /** --name's value, trimmed as the page trims a typed name; null when --name is absent. */
    private static String nameOption(String value) {
        if( value == null )
            return null
        final String name = value.trim()
        if( !name )
            throw new UsageException('--name needs a non-blank name')
        if( name.length() > Put.MAX_NAME_CHARS )
            throw new UsageException("--name is at most ${Put.MAX_NAME_CHARS} characters, got ${name.length()}")
        return name
    }

    /** The request's kind, or null when it is not a DAG-JSON map with a string kind (the builder then refuses it). */
    private static String requestKind(byte[] body) {
        Object request = null
        try {
            request = DagJson.decode(body)
        }
        catch( Exception e ) {
            return null
        }
        final Object kind = request instanceof Map ? ((Map) request).get('kind') : null
        return kind instanceof String ? (String) kind : null
    }

    /**
     * The set name Claim after a Selection (ticket 05 Q5), through the same
     * builder, superseding the current name Claims the Selection's dry run
     * reports; nothing when its one current name already equals {@code name}.
     * The request is the page's (web/src/write.js, rename).
     */
    private static int nameSelection(Put builder, PutResult saved, byte[] body, String name, boolean dryRun,
                                     PrintStream out, PrintStream err, Closure<Long> clock) {
        try {
            // A dry run already holds the current names; after a write, ask the builder for them.
            final PutResult state = dryRun ? saved : builder.put(body, true)
            if( state.names.size() == 1 && state.names[0] == name && state.nameClaims.size() == 1 ) {
                err.println("nf-blocks:put: ${saved.address} is already named '${name}'; no name Claim ${dryRun ? 'would be' : 'is'} written")
                return 0
            }
            final Map<String, Object> claim = new LinkedHashMap<String, Object>()
            claim.put('kind', Records.CLAIM)
            claim.put('subject', saved.address)
            claim.put('verb', Claim.SET)
            claim.put('attribute', Claim.NAME)
            claim.put('value', name)
            claim.put('supersedes', new ArrayList<Cid>(state.nameClaims))
            claim.put('timestamp', Index.isoMillis(clock.call()))
            if( dryRun ) {
                out.println(DagJson.encodeToString(claim))
                return 0
            }
            out.println(new String(builder.put((Object) claim, false).body(), 'UTF-8'))
            return 0
        }
        catch( PutError e ) {
            out.println(new String(e.body(), 'UTF-8'))
            err.println("nf-blocks:put: saved ${saved.address}, naming failed: ${e.message}")
            return 1
        }
    }

    /** The names put reads from the verb's own stdin stream rather than opening as a file. */
    static final Set<String> STDIN_NAMES = ['-', '/dev/stdin'] as Set<String>

    /**
     * At most one byte past the builder's request cap, so the builder refuses it with too_large.
     * A plain read loop: the input may be a pipe or FIFO, which cannot be sized or seeked
     * (Files.newInputStream's readNBytes asks its channel for size() and fails with "Illegal seek").
     */
    private static byte[] readCapped(InputStream input) {
        try {
            final int cap = (int) Put.MAX_REQUEST_BYTES + 1
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream()
            final byte[] buffer = new byte[8192]
            int n
            while( bytes.size() < cap && (n = input.read(buffer, 0, Math.min(buffer.length, cap - bytes.size()))) != -1 )
                bytes.write(buffer, 0, n)
            return bytes.toByteArray()
        }
        finally {
            input.close()
        }
    }

    /** Catches the index up from every member's Store Log, then rewrites the writable member's snapshot. */
    private static int snapshot(Options options, Map config, PrintStream out) {
        if( options.positionals )
            throw new UsageException("snapshot takes no arguments, got ${options.positionals}")
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final SnapshotBase base = cas.snapshotBase()
        final Index index = cas.openIndex()
        try {
            final Set<String> failed = cas.catchUpIndex(index)
            final IndexSnapshot.Result result = cas.snapshotWritable(index, 0L, base, failed)
            // Exit 0 either way: the snapshot is derived, and the old one stands.
            if( result.skipped )
                out.println("nf-blocks:snapshot: not rewritten: ${result.skipped}")
            else
                out.println("wrote ${cas.snapshotsOf(cas.config.writableAlias).describe()} (${result.bytes} bytes, ${result.runs} runs, watermark ${result.watermark ?: 'none'})")
            return 0
        }
        finally {
            index.close()
        }
    }
}
