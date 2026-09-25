package robsyme.cas.cli

import java.nio.file.Paths

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cli.Launcher
import nextflow.config.ConfigBuilder
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot

/**
 * `nextflow plugin nf-blocks:<verb>` (DESIGN.md §15). Builds the config the
 * way PluginAbstractExec does, but returns a real exit status: that trait
 * swallows every exception and returns 0.
 */
@Slf4j
@CompileStatic
class CasCommands {

    static final List<String> VERBS = ['explore', 'snapshot']

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
        if( !(cmd in VERBS) ) {
            err.println(usage(cmd))
            return 2
        }
        try {
            switch( cmd ) {
                case 'snapshot':
                    return snapshot(Options.parse(args, [] as Set), config, out)
                case 'explore':
                    return explore(args, config, out, err)
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
            '  snapshot               rewrite the writable member\'s Index Snapshot at any size'
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

    /** Replaced by ExploreCommand in Task 7. */
    private static int explore(List<String> args, Map config, PrintStream out, PrintStream err) {
        err.println('nf-blocks:explore is not built yet')
        return 1
    }
}
