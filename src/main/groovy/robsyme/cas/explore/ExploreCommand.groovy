package robsyme.cas.explore

import java.util.concurrent.CountDownLatch

import groovy.transform.CompileStatic
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.cli.Options
import robsyme.cas.cli.UsageException
import robsyme.cas.core.Cid
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.Put
import robsyme.cas.core.Samplesheet
import software.amazon.awssdk.services.s3.S3Client

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
        final Index index
        final Put put
        Started(ExploreServer server, CasSession cas, Index index, Put put) {
            this.server = server; this.cas = cas; this.index = index; this.put = put
        }
    }

    /** Blocks until interrupted. CmdPlugin calls System.exit when this returns. */
    static int run(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Started started = start(args, config, out, err)
        final CountDownLatch stopped = new CountDownLatch(1)
        Runtime.runtime.addShutdownHook(new Thread({
            try {
                started.server.stop()
                // Waits for an in-flight write or export, which also holds this lock (deferred minor, Task 10 review).
                synchronized( started.put ) {
                    started.index.close()
                }
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
        final Index index = session.openIndex()
        final String token = ExploreServer.newToken()
        final Put put = session.newPut(index)
        final ExploreServer.Exporter exporter = { Cid selection, String format ->
            synchronized( put ) {
                session.catchUpIndex(index)
                // A Selection copied into the composition without its Store Log
                // entry is indexed on the spot here too, the same way
                // CasExtension.resolveSelection does (final review finding 4).
                index.ensureSelectionIndexed(session.store, selection, session.config.writableAlias)
                final Samplesheet sheet = Samplesheet.of(session.store, index.selectionItems(selection))
                return (format == 'csv' ? sheet.csv() : sheet.json()).getBytes('UTF-8')
            }
        } as ExploreServer.Exporter
        final ExploreServer server = new ExploreServer(membersOf(cas), cas.writableAlias, IndexSnapshot.bundledPage(), put, token, exporter)
            .start(options.intFlag('port', 0))
        out.println("nf-blocks explorer: ${server.launchUrl}")
        out.flush()
        return new Started(server, session, index, put)
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

    /**
     * Every configured member the explorer serves, writable first. The S3
     * client for a remote alias comes from {@code s3ClientFactory}, called
     * once per remote alias; production leaves it at
     * {@link S3MemberFiles#defaultClient}, which resolves credentials and a
     * region through the default chains (possibly reaching IMDS). A test can
     * replace it with a factory that never touches the network.
     */
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config, Closure<S3Client> s3ClientFactory = { -> S3MemberFiles.defaultClient() }) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.configuredAliases ) {
            if( config.isRemote(alias) ) {
                final List<String> bucketAndPrefix = S3MemberFiles.bucketAndPrefix(config.remoteLocationOf(alias))
                members.put(alias, new S3MemberFiles(s3ClientFactory.call(), bucketAndPrefix[0], bucketAndPrefix[1]))
            }
            else {
                members.put(alias, new LocalMemberFiles(config.locationOf(alias)))
            }
        }
        return members
    }
}
