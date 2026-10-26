package robsyme.cas.explore

import java.util.concurrent.CountDownLatch
import java.util.function.Supplier

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.Principal
import robsyme.cas.CopyTargets
import robsyme.cas.cli.Options
import robsyme.cas.cli.UsageException
import robsyme.cas.core.Cid
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.HandoffWriter
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.Put
import robsyme.cas.core.Samplesheet
import robsyme.cas.core.SnapshotBase
import robsyme.cas.s3.S3CoordinateTree
import robsyme.cas.s3.S3Ops

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
        /** The nf-blocks-identity thread, so a test can join it before its identity seams are put back. */
        @PackageScope final Thread identity
        Started(ExploreServer server, CasSession cas, Index index, Put put, Thread identity) {
            this.server = server; this.cas = cas; this.index = index; this.put = put; this.identity = identity
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
        // A command session: stdout is the URL line alone, and every notice goes to stderr.
        final CasSession session = CasSession.forCommand(cas, err)
        // Ticket 03 decision 2; a ClockSkewException reaches CasCommands.run, which prints it and exits 1.
        session.checkClock()
        warnShadowed(session, err)
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
        final LinkedHashMap<String, MemberFiles> members = membersOf(cas, { String bucket -> CasSession.s3OpsFactory.call(config, bucket) } as Closure<S3Ops>)
        // members.json and a write wait on the session's one resolution; nothing else does.
        final ExploreServer server = new ExploreServer(members, cas.writableAlias, IndexSnapshot.bundledPage(), put, token, exporter,
            { -> session.principal } as Supplier<Principal>)
            .memberInfo(memberInfoOf(cas))
            // POST /api/copy: each copy builds its own composition from the loaded config (CopyService).
            .copies(config, err)
            // POST /api/handoff: the same writable member, index and signer as Put, under Put's monitor.
            .handoffs(new HandoffWriter(session.store, session.members()[0], index, { -> session.assertedBy } as Supplier<String>,
                { -> System.currentTimeMillis() }, { -> session.catchUpIndex(index) }, put))
            .start(options.intFlag('port', 0))
        out.println("nf-blocks explorer: ${server.launchUrl}")
        out.flush()
        // Listening and the URL out first, then resolve off the main thread, so the signing line or the
        // warning reaches stderr after the URL. Platform (5 s) and git (2 s) bound the wait.
        final Thread resolving = new Thread({ -> session.principal } as Runnable, 'nf-blocks-identity')
        resolving.daemon = true
        resolving.start()
        return new Started(server, session, index, put, resolving)
    }

    /** Catches the index up and rewrites the writable member's snapshot at any size. Derived: a failure warns. */
    static void refresh(CasSession cas, PrintStream err) {
        try {
            final SnapshotBase base = cas.snapshotBase()
            final Index index = cas.openIndex()
            try {
                final Set<String> failed = cas.catchUpIndex(index)
                final IndexSnapshot.Result r = cas.snapshotWritable(index, 0L, base, failed)
                if( r.skipped )
                    err.println("nf-blocks:explore: the Index Snapshot was not rewritten (${r.skipped})${IndexSnapshot.skipAdvice(r)}")
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
     * Silent decision 20: a coordinate under a pointer object of the writable S3
     * member reads as absent, so the explorer names them once at start.
     */
    private static void warnShadowed(CasSession session, PrintStream err) {
        final String alias = session.config.writableAlias
        final CoordinateTree coords = session.coordinatesOf(alias)
        if( !(coords instanceof S3CoordinateTree) )
            return
        final List<String> shadowed
        try {
            shadowed = ((S3CoordinateTree) coords).shadowedPointers(21)
        }
        catch( Exception e ) {
            // Advice only (rule 3): a listing that fails must not stop the explorer.
            err.println("nf-blocks:explore: could not look for shadowed coordinates in '${alias}' (${e.message}); continuing")
            return
        }
        if( shadowed )
            err.println("nf-blocks:explore: ${shadowed.size() > 20 ? 'more than 20' : shadowed.size()} coordinate(s) in '${alias}' are shadowed by a pointer above them, and read as absent: ${shadowed.take(20).join(', ')}")
    }

    /** What members.json says per member: its kind, an S3 member's location, and whether a copy may write it. */
    static LinkedHashMap<String, ExploreServer.MemberInfo> memberInfoOf(CasConfig config) {
        final LinkedHashMap<String, ExploreServer.MemberInfo> info = new LinkedHashMap<>()
        for( String alias : config.configuredAliases ) {
            final CopyTargets.Judgement judgement = CopyTargets.judge(config, alias)
            final boolean remote = config.isRemote(alias)
            info.put(alias, new ExploreServer.MemberInfo(remote ? 's3' : 'local', remote ? config.locationText(alias) : null,
                judgement.target, judgement.reason))
        }
        return info
    }

    /**
     * Every configured member the explorer serves, writable first. The S3Ops
     * for a remote alias comes from {@code s3Ops}, called once per remote alias
     * with its bucket; production passes {@code S3Access.open(config, bucket)}.
     */
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config, Closure<S3Ops> s3Ops) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.configuredAliases ) {
            if( config.isRemote(alias) ) {
                final List<String> bucketAndPrefix = S3MemberFiles.bucketAndPrefix(config.remoteLocationOf(alias))
                members.put(alias, new S3MemberFiles(s3Ops.call(bucketAndPrefix[0]), bucketAndPrefix[1]))
            }
            else {
                members.put(alias, new LocalMemberFiles(config.locationOf(alias)))
            }
        }
        return members
    }
}
