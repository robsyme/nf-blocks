package robsyme.cas.explore

import java.util.concurrent.CountDownLatch

import groovy.transform.CompileStatic
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.cli.Options
import robsyme.cas.cli.UsageException
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot

/**
 * `nextflow plugin nf-blocks:explore [--port <n>]` (DESIGN.md §15). Rewrites
 * the writable member's snapshot, serves until the JVM is interrupted, and
 * rewrites the snapshot again on the way out.
 */
@CompileStatic
class ExploreCommand {

    static final Set<String> FLAGS = ['port'] as Set

    @CompileStatic
    static class Started {
        final ExploreServer server
        final CasSession cas
        Started(ExploreServer server, CasSession cas) { this.server = server; this.cas = cas }
    }

    /** Blocks until interrupted. CmdPlugin calls System.exit when this returns. */
    static int run(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Started started = start(args, config, out, err)
        final CountDownLatch stopped = new CountDownLatch(1)
        Runtime.runtime.addShutdownHook(new Thread({
            try {
                started.server.stop()
                refresh(started.cas, err)
            }
            finally {
                stopped.countDown()
            }
        } as Runnable, 'nf-blocks-explore-exit'))
        stopped.await()
        return 0
    }

    static Started start(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Options options = Options.parse(args, FLAGS)
        if( options.positionals )
            throw new UsageException("explore takes no arguments, got ${options.positionals}")
        final CasConfig cas = CasConfig.fromSession(config)
        final CasSession session = new CasSession(cas)
        refresh(session, err)
        final ExploreServer server = new ExploreServer(membersOf(cas), cas.writableAlias, IndexSnapshot.bundledPage())
            .start(options.intFlag('port', 0))
        out.println("nf-blocks explorer: ${server.url}")
        out.flush()
        return new Started(server, session)
    }

    /** Catches the index up and rewrites the writable member's snapshot at any size. Derived: a failure warns. */
    static void refresh(CasSession cas, PrintStream err) {
        try {
            final Index index = cas.openIndex()
            try {
                cas.catchUpIndex(index)
                cas.snapshotWritable(index, 0L)
            }
            finally {
                index.close()
            }
        }
        catch( Exception e ) {
            err.println("nf-blocks:explore: could not rewrite the Index Snapshot (${e.message}); serving the one on disk")
        }
    }

    /** Every configured member the explorer serves, writable first. */
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.configuredAliases )
            members.put(alias, config.isRemote(alias)
                ? (MemberFiles) S3MemberFiles.open(config.remoteLocationOf(alias))
                : new LocalMemberFiles(config.locationOf(alias)))
        return members
    }
}
