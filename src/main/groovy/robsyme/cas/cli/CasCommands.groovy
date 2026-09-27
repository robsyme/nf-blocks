package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cli.Launcher
import nextflow.config.ConfigBuilder
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.Put
import robsyme.cas.core.PutError
import robsyme.cas.explore.ExploreCommand

/**
 * `nextflow plugin nf-blocks:<verb>` (DESIGN.md §15). Builds the config the
 * way PluginAbstractExec does, but returns a real exit status: that trait
 * swallows every exception and returns 0.
 */
@Slf4j
@CompileStatic
class CasCommands {

    static final List<String> VERBS = ['explore', 'items', 'put', 'snapshot']

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
                    return put(Options.parse(args, ['dry-run'] as Set), config, out, stdin)
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
            '  put <file|-> [--dry-run]  build and write one Selection or Claim from DAG-JSON\n' +
            '  snapshot               rewrite the writable member\'s Index Snapshot at any size'
    }

    /** Builds and writes one client-constructible block (spec section 9.2); the body goes to stdout either way. */
    private static int put(Options options, Map config, PrintStream out, InputStream stdin) {
        if( options.positionals.size() != 1 )
            throw new UsageException("put takes one file (or - for stdin), got ${options.positionals ?: 'none'}")
        final String dry = options.flag('dry-run')
        if( !(dry in [null, 'true', 'false']) )
            throw new UsageException("--dry-run is a flag, got '${dry}'")
        final String source = options.positionals[0]
        final byte[] body = source == '-' ? readCapped(stdin) : readCapped(Files.newInputStream(Paths.get(source)))
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        final Index index = cas.openIndex()
        try {
            out.println(new String(cas.newPut(index).put(body, dry == 'true').body(), 'UTF-8'))
            return 0
        }
        catch( PutError e ) {
            out.println(new String(e.body(), 'UTF-8'))
            return 1
        }
        finally {
            index.close()
        }
    }

    /** At most one byte past the builder's request cap, so the builder refuses it with too_large. */
    private static byte[] readCapped(InputStream input) {
        try {
            return input.readNBytes((int) Put.MAX_REQUEST_BYTES + 1)
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
        final Index index = cas.openIndex()
        try {
            cas.catchUpIndex(index)
            final IndexSnapshot.Result result = cas.snapshotWritable(index, 0L)
            out.println("wrote ${result.path} (${result.bytes} bytes, ${result.runs} runs, watermark ${result.watermark ?: 'none'})")
            return 0
        }
        finally {
            index.close()
        }
    }
}
