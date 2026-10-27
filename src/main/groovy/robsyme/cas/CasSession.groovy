package robsyme.cas

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import robsyme.cas.core.Anomalies
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.ClockSkew
import robsyme.cas.core.CompositeStore
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.LocalCoordinateTree
import robsyme.cas.core.IdentifiedStore
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.InputSet
import robsyme.cas.core.LiveWriter
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.Providers
import robsyme.cas.core.Put
import robsyme.cas.core.RetainedStore
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.SnapshotStorage
import robsyme.cas.core.StoreIds
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreRef
import robsyme.cas.nio.PublishAddresser
import robsyme.cas.s3.S3Access
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3CoordinateTree
import robsyme.cas.s3.S3Ops
import robsyme.cas.s3.S3PreconditionFailed
import robsyme.cas.s3.S3SnapshotStorage
import robsyme.cas.trace.ConsoleLog

/**
 * The one per-run shared object (DESIGN.md section 9). Everything the provider,
 * the lineage store, the observer and the channel factory need to agree on
 * during a run lives here, keyed by the Nextflow {@link Session}.
 *
 * Nothing in this class survives the JVM: anything a later run or a later JVM
 * needs is in the store (blocks, coords/, log/, nf/), never here. The provider
 * is a JVM singleton but the state is per-session, which is why it is keyed by
 * {@link Session} rather than memoised on the provider (see the Task 1 review).
 */
@Slf4j
@CompileStatic
class CasSession {

    /** One published path, recorded by the provider's {@code upload()} keyed by the join key. */
    @groovy.transform.Canonical
    static class Publish {
        /** The Store URI the coordinate now resolves to. */
        StoreRef ref
        /** Byte size of the published content. */
        long size
        /**
         * The Address Provider that produced the address, e.g. {@code head-node};
         * a directory's manifest is always {@code head-node}. The files inside a
         * directory are counted by the addresser, not recorded here (final review I5).
         */
        String provider
    }

    private static final ConcurrentHashMap<Session, CasSession> REGISTRY = new ConcurrentHashMap<>()

    /**
     * The {@link CasSession} for the given session, created once per session.
     * Building the store touches the filesystem, so it is done under the map's
     * per-key compute lock rather than racing across publish threads.
     */
    static CasSession of(Session session) {
        if( session == null )
            throw new IllegalStateException("No Nextflow session is bound; cannot resolve the cas store")
        return REGISTRY.computeIfAbsent(session, { Session s -> new CasSession(CasConfig.fromSession(s.config)) })
    }

    /** The current session's {@link CasSession}, via {@link Global#session}. */
    static CasSession current() {
        return of(Global.session as Session)
    }

    /** Test seam: bind a prebuilt instance for a session without a real store config. */
    static CasSession bind(Session session, CasSession instance) {
        REGISTRY.put(session, instance)
        return instance
    }

    static void unbind(Session session) {
        REGISTRY.remove(session)
    }

    final CasConfig config
    final BlockStore store
    final CoordinateTree coordinates

    /**
     * Where this session's notices go, once each: the identity warnings, the
     * "signing as" line, the storage-class warning, the clock check's, and the
     * index's missing-snapshot and rebuild lines ({@link #openIndex}). A run's
     * reach the Nextflow console through {@link ConsoleLog}. A command's
     * ({@link #forCommand}) go to the command's stderr, so its stdout carries
     * only its output.
     */
    @CompileStatic
    static final class Notices implements Index.Notice {

        /** Lines a run session has printed in this JVM. */
        private static final Set<String> CONSOLE_SEEN = ConcurrentHashMap.newKeySet()

        static final Notices CONSOLE = new Notices(Identity.RUN_WHAT, CONSOLE_SEEN, null)

        /** What the identity warnings say will read 'anonymous'. */
        final String what
        private final Set<String> seen
        private final PrintStream err

        private Notices(String what, Set<String> seen, PrintStream err) {
            this.what = what
            this.seen = seen
            this.err = err
        }

        /** A command's channel: its stderr, with its own record of what it printed. */
        static Notices to(PrintStream err) {
            return new Notices(Identity.COMMAND_WHAT, ConcurrentHashMap.<String>newKeySet(), err)
        }

        @Override
        void warn(String message) {
            if( !seen.add(message) )
                return
            if( err != null )
                err.println(message)
            else
                ConsoleLog.LOG.warn(message)
        }

        @Override
        void info(String message) {
            if( !seen.add(message) )
                return
            if( err != null )
                err.println(message)
            else
                ConsoleLog.LOG.info(message)
        }
    }

    private final Notices notices

    /** Test seams for the principal's sources. */
    static PlatformIdentity platformIdentity = new HttpPlatformIdentity()
    static GitEmail gitEmail = new ProcessGitEmail()
    static Map<String,String> env = System.getenv()

    /** Set once by {@link #getPrincipal}; guarded by {@code resolving}. */
    private volatile Principal resolved
    private final Object resolving = new Object()

    /**
     * Who this session signs as, and which source said so. Resolved on first
     * use and kept: only what signs (a run's blocks, put, an applied sweep or
     * prune, a write from explore) pays for Platform or git, and threads that
     * ask at once share one resolution. Resolving prints the "signing as" line,
     * or the warning, to this session's notices.
     */
    Principal getPrincipal() {
        final Principal known = resolved
        if( known != null )
            return known
        synchronized( resolving ) {
            if( resolved == null )
                resolved = resolvePrincipal()
            return resolved
        }
    }

    /** The name this session signs with: {@link #getPrincipal}'s. */
    String getAssertedBy() {
        return getPrincipal().name
    }

    private Principal resolvePrincipal() {
        final Principal p = Identity.resolve(config.assertedBy, config.rawConfig, env, gitEmail, platformIdentity, notices.what,
            { String m -> notices.warn(m); null } as Closure<Void>)
        // An anonymous principal has already printed its warning.
        if( !p.isAnonymous() )
            notices.info("nf-blocks: signing as ${p.name} (from ${Identity.sourceText(p)})".toString())
        return p
    }

    /** A session for a {@code nextflow plugin nf-blocks:<verb>} command; its notices go to {@code err}. */
    static CasSession forCommand(CasConfig config, PrintStream err) {
        return new CasSession(config, Notices.to(err))
    }

    /** Test seam: forget which lines run sessions have printed. */
    static void forgetNotices() { Notices.CONSOLE_SEEN.clear() }

    /** join key -> the address, size and provider recorded when the file was published. */
    final ConcurrentHashMap<String, Publish> publishes = new ConcurrentHashMap<>()

    /**
     * join key -> what a published directory could not address (DESIGN §6).
     * The provider's {@code upload()} records it here so the run's join
     * ({@code onFlowComplete}) can fold directory anomalies into the
     * RunCompletion without re-reading the manifest.
     */
    final ConcurrentHashMap<String, Anomalies> uploadAnomalies = new ConcurrentHashMap<>()

    /**
     * path -> the fromStore items it was read from, with the collection and
     * Selection each came through. Beside the emitted path, never in it, so a
     * task hash does not change; the run's input recorder reads it at the join.
     */
    final ConcurrentHashMap<Path, Set<InputSet.Origin>> origins = new ConcurrentHashMap<>()

    /** Adds {@code found} to the Origins of {@code path}; two items sharing a raw leaf share a path. */
    void addOrigins(Path path, Collection<InputSet.Origin> found) {
        origins.computeIfAbsent(path) { Path k -> ConcurrentHashMap.<InputSet.Origin>newKeySet() }.addAll(found)
    }

    /** Nextflow's own WorkflowRun key (LinObserver.executionHash), seen on the first save. */
    private final AtomicReference<String> nfRunKey = new AtomicReference<>()

    /** Our RunManifest address, written once at onFlowBegin. */
    private final AtomicReference<Cid> runManifest = new AtomicReference<>()

    /** Fires once: the join at onFlowComplete, which runs twice on a failed run. */
    private final AtomicBoolean completed = new AtomicBoolean(false)
    private final CountDownLatch completionDone = new CountDownLatch(1)

    /**
     * Store Log entries this run itself has written to the writable member,
     * by cid, so {@code catchUpIndex} can tell {@link Index#catchUp} not to
     * treat them as evidence a snapshot should already exist (ticket 18).
     */
    private final Set<String> loggedThisRun = ConcurrentHashMap.newKeySet()

    /** Notes a Store Log entry this run itself wrote (ticket 18). */
    void noteLogged(Cid entry) { loggedThisRun.add(entry.toString()) }

    /** A test seam: the S3Ops for a bucket, from the loaded config map. */
    static Closure<S3Ops> s3OpsFactory = { Map config, String bucket -> S3Access.open(config, bucket) } as Closure<S3Ops>

    /** alias -> the member's coordinate tree and snapshot storage, built beside its block store. */
    private final Map<String, CoordinateTree> trees = new LinkedHashMap<>()
    private final Map<String, SnapshotStorage> snapshots = new LinkedHashMap<>()

    /** A run's session: its notices reach the console. */
    CasSession(CasConfig config) {
        this(config, Notices.CONSOLE)
    }

    private CasSession(CasConfig config, Notices notices) {
        this.config = config
        this.notices = notices
        final List<BlockStore> members = new ArrayList<>()
        for( String alias : config.members ) {
            final boolean writable = alias == config.writableAlias
            if( config.isRemote(alias) ) {
                final S3Location at = config.remoteOf(alias)
                final S3Ops ops = s3OpsFactory.call(config.rawConfig, at.bucket)
                members.add(new S3BlockStore(ops, at.prefix, alias, writable, config.tmpDir))
                trees.put(alias, new S3CoordinateTree(ops, at.prefix))
                snapshots.put(alias, new S3SnapshotStorage(ops, at.prefix))
            }
            else {
                final Path root = config.locationOf(alias)
                members.add(new LocalBlockStore(root, alias, writable))
                trees.put(alias, new LocalCoordinateTree(root.resolve('coords')))
                snapshots.put(alias, new LocalSnapshotStorage(root))
            }
        }
        this.store = new CompositeStore(members)
        this.coordinates = trees.get(config.writableAlias)
        if( config.storageClassWarning )
            notices.warn(config.storageClassWarning)
    }

    /**
     * Test seam: an instance over an already-built store and coordinate tree.
     * The writable alias gets {@code coordinates}, and a snapshot storage over
     * the store's root when its writable member is local; every other local
     * member gets its own tree and storage from the config.
     */
    CasSession(CasConfig config, BlockStore store, CoordinateTree coordinates) {
        this.config = config
        this.notices = Notices.CONSOLE
        this.store = store
        this.coordinates = coordinates
        for( String alias : config.members ) {
            final Path root = config.locationOf(alias)
            if( alias == config.writableAlias || root == null )
                continue
            trees.put(alias, new LocalCoordinateTree(root.resolve('coords')))
            snapshots.put(alias, new LocalSnapshotStorage(root))
        }
        trees.put(config.writableAlias, coordinates)
        final BlockStore writable = members()[0]
        if( writable instanceof LocalBlockStore )
            snapshots.put(config.writableAlias, new LocalSnapshotStorage(((LocalBlockStore) writable).root))
        else if( config.writableLocation != null )
            snapshots.put(config.writableAlias, new LocalSnapshotStorage(config.writableLocation))
    }

    private volatile PublishAddresser addresser

    /** The run's Address Provider seam (DESIGN.md §8), built on first publish. */
    PublishAddresser getAddresser() {
        if( addresser == null ) synchronized( this ) {
            if( addresser == null ) {
                final Session s = Global.session as Session
                addresser = new PublishAddresser(store, members()[0], CasConfig.nodeHashEnabled(s?.config ?: config.rawConfig), s?.workDir)
            }
        }
        return addresser
    }

    /**
     * Brings the index up to date from every store member's Store Log, so a read
     * across a composition (a {@code fromStore} through {@code [out, lab]}) sees
     * the runs recorded in a read-only member and not only the writable one
     * (DESIGN.md §12). A cold member is seeded from its Index Snapshot first
     * (ticket 04). Each member advances its own watermark, so this is cheap to
     * call before a query and idempotent. Derived, so a member whose log cannot
     * be read is logged and skipped rather than failing the caller.
     *
     * @return the aliases whose catch-up threw
     */
    Set<String> catchUpIndex(Index index) {
        return catchUpIndex(index, true)
    }

    /** As {@link #catchUpIndex(Index)}; with {@code seed} false no member's Index Snapshot seeds the cache, so every block is scanned (reindex --seed false). */
    Set<String> catchUpIndex(Index index, boolean seed) {
        return catchUpIndex(index, seed, Collections.<String> emptySet())
    }

    /**
     * As {@link #catchUpIndex(Index, boolean)}, leaving the members in
     * {@code leftOut} alone: neither their logs nor their store-ids are read,
     * and the writable member's store-id is not created when it is left out.
     * A copy plans this way, so planning never writes to the destination.
     *
     * With {@link #leaveUnindexedOut} called, a non-writable member this cache
     * does not hold is seeded from its snapshot or left out, never scanned
     * (background store indexing, spec §6.1); a member left out is named once
     * through this session's notices and is not in the returned set.
     */
    Set<String> catchUpIndex(Index index, boolean seed, Set<String> leftOut) {
        final Set<String> failed = new LinkedHashSet<String>()
        try {
            index.reconcileStoreIds(memberStoreIds(leftOut), { String a, String w -> watermarkLogged(a, w) } as Closure<Boolean>)
        }
        catch( Exception e ) {
            log.warn("could not compare this cache with the store's ids; it is derived: ${e.message}", e)
        }
        for( BlockStore member : members() ) {
            final String alias = member.alias()
            if( leftOut.contains(alias) )
                continue
            try {
                if( unindexedLeftOut && alias != config.writableAlias && !index.isIndexed(alias) ) {
                    if( !seededOrLeftOut(index, member) )
                        continue
                    index.catchUp(member, StoreLog.of(member), alias, store)
                    continue
                }
                final Set<String> quietFor = alias == config.writableAlias ? loggedThisRun : Collections.<String> emptySet()
                index.catchUp(member, StoreLog.of(member), alias, seed ? seedSourceOf(alias) : null, config.tmpDir, quietFor, store)
            }
            catch( Exception e ) {
                failed.add(alias)
                log.warn("could not catch up the index from store member '${alias}'; it is derived: ${e.message}", e)
            }
        }
        return failed
    }

    /** Whether {@link #catchUpIndex} leaves out the members this cache does not hold and cannot seed; set once by {@link #leaveUnindexedOut}. */
    private volatile boolean unindexedLeftOut

    /** alias -> why the rule left that member out: the reason its snapshot could not seed, or what the caller already knew. */
    private final ConcurrentHashMap<String, String> unindexed = new ConcurrentHashMap<>()

    /** alias -> where the rule seeds that member from instead of its own snapshot ({@link #seedFrom}). */
    private final ConcurrentHashMap<String, SnapshotStorage> seedSources = new ConcurrentHashMap<>()

    /**
     * Turns on the rule in {@link #catchUpIndex(Index, boolean, Set)} for this
     * session: explore's, never a command's or a run's. {@code known} (alias
     * to reason) is what the caller has already decided is not indexed; those
     * members are skipped without fetching their snapshots.
     */
    void leaveUnindexedOut(Map<String, String> known) {
        unindexed.putAll(known)
        unindexedLeftOut = true
    }

    /** The members the rule has left out, alias to reason. */
    Map<String, String> unindexed() {
        return new LinkedHashMap<String, String>(unindexed)
    }

    /** The rule seeds {@code alias} from {@code storage}, a snapshot kept elsewhere, instead of the member's own. */
    void seedFrom(String alias, SnapshotStorage storage) {
        seedSources.put(alias, storage)
    }

    private SnapshotStorage seedSourceOf(String alias) {
        return seedSources.get(alias) ?: snapshotsOf(alias)
    }

    /** True once {@code member} is seeded; false when it is left out, which is recorded and named the first time. */
    private boolean seededOrLeftOut(Index index, BlockStore member) {
        final String alias = member.alias()
        if( unindexed.containsKey(alias) )
            return false
        final String reason = index.trySeed(member, StoreLog.of(member), alias, seedSourceOf(alias), config.tmpDir)
        if( reason == null )
            return true
        unindexed.put(alias, reason)
        notices.warn(notListedLine(alias, reason))
        return false
    }

    /** The line a member the explorer leaves out gets (spec §5.1), with the reason when its snapshot is there but unusable. */
    static String notListedLine(String alias, String reason) {
        final String has = reason == 'absent' ? 'has no Index Snapshot' : "has no usable Index Snapshot (${reason})"
        return "store member '${alias}' ${has}, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.".toString()
    }

    private volatile String writableId

    /**
     * The writable member's store-id, created when absent (DESIGN.md §20
     * decision 2, plan decision 5). Null when the member keeps none or it could
     * not be written; derived, so a failure warns once and the caller goes on.
     */
    String writableStoreId() {
        if( writableId != null )
            return writableId
        final BlockStore writable = members()[0]
        if( !(writable instanceof IdentifiedStore) || !writable.isWritable() )
            return null
        try {
            writableId = StoreIds.ensure(((IdentifiedStore) writable).storeIdStorage())
        }
        catch( Exception e ) {
            warnOnce("could not create the store-id of '${writable.alias()}'; caches cannot tell if it is replaced: ${e.message}".toString())
        }
        return writableId
    }

    /**
     * Every member's id now (null when it has none); a member whose id could not
     * be read is left out (plan decision 3), and so is the writable member when
     * its id could not be created, which would otherwise read as "the id vanished".
     */
    private Map<String, String> memberStoreIds(Set<String> leftOut) {
        final Map<String, String> ids = new LinkedHashMap<String, String>()
        for( BlockStore member : members() ) {
            if( !(member instanceof IdentifiedStore) || leftOut.contains(member.alias()) )
                continue
            if( member.alias() == config.writableAlias && member.isWritable() ) {
                final String id = writableStoreId()
                if( id != null )
                    ids.put(member.alias(), id)
                continue
            }
            try {
                ids.put(member.alias(), StoreIds.read(member))
            }
            catch( Exception e ) {
                log.debug("could not read the store-id of '${member.alias()}': ${e.message}")
            }
        }
        return ids
    }

    private Boolean watermarkLogged(String alias, String watermark) {
        final BlockStore member = member(alias)
        if( member == null )
            return null
        try {
            return StoreLog.of(member).has(watermark)
        }
        catch( Exception e ) {
            log.debug("could not look for ${watermark} in the Store Log of '${alias}': ${e.message}")
            return null
        }
    }

    /** The store's members, writable first; a single-store session has one. */
    List<BlockStore> members() {
        return store instanceof CompositeStore ? ((CompositeStore) store).members : [store]
    }

    /** The composition member {@code alias}, or null. */
    BlockStore member(String alias) {
        return members().find { BlockStore m -> m.alias() == alias }
    }

    /**
     * The coordinate tree of a member by alias (DESIGN.md §7), local or S3. A
     * store configured but left out of cas.resolve still has coordinates a
     * `cas://<alias>/...` path can name, so its tree is built on first use.
     */
    CoordinateTree coordinatesOf(String alias) {
        synchronized( trees ) {
            CoordinateTree t = trees.get(alias)
            if( t == null && config.isRemote(alias) ) {
                final S3Location at = config.remoteOf(alias)
                t = new S3CoordinateTree(s3OpsFactory.call(config.rawConfig, at.bucket), at.prefix)
            }
            else if( t == null && config.locationOf(alias) != null )
                t = new LocalCoordinateTree(config.locationOf(alias).resolve('coords'))
            if( t == null )
                throw new IllegalArgumentException("Unknown store alias '${alias}' -- configured stores: ${config.members.join(', ')}")
            trees.put(alias, t)
            return t
        }
    }

    /** Where a member keeps its Index Snapshot and page (DESIGN.md §15); null for an unknown alias. */
    SnapshotStorage snapshotsOf(String alias) {
        return snapshots.get(alias)
    }

    /**
     * The writable member's snapshot as it stands, taken before the catch-up so
     * the guard covers it (silent decision 16). Null when there is none, or when
     * it cannot be looked at: a snapshot replaced between the HEAD and the GET
     * that counts an uncounted one (S3PreconditionFailed), or any other failure.
     * Either way the write that follows is guarded as a first write.
     */
    SnapshotBase snapshotBase() {
        final SnapshotStorage storage = snapshotsOf(config.writableAlias)
        if( storage == null )
            return null
        try {
            return storage.base(config.tmpDir)
        }
        catch( S3PreconditionFailed e ) {
            log.warn("the Index Snapshot of '${config.writableAlias}' was replaced while it was being read (${e.message}); guarding the rewrite as if there were none")
            return null
        }
        catch( Exception e ) {
            log.warn("could not look at the Index Snapshot of '${config.writableAlias}'; it is derived: ${e.message}")
            return null
        }
    }

    /**
     * This composition's per-user cache index (DESIGN.md §12), named by every
     * member's location text, with its terminal lines sent to this session's
     * notices. The caller closes it.
     */
    Index openIndex() {
        final Index index = Index.open(IndexPaths.cachePath(config.locationTexts(), config.indexOverride))
        index.notice = notices
        return index
    }

    /**
     * The one builder over this composition, writing to the writable member
     * (block explorer spec section 9). The caller owns {@code index}; the
     * builder uses it under its own lock.
     */
    Put newPut(Index index) {
        return new Put(store, members()[0], index, { -> getAssertedBy() } as Supplier<String>, { -> System.currentTimeMillis() }, { -> catchUpIndex(index) })
    }

    /**
     * Rewrites the writable member's Index Snapshot from {@code index}, and the
     * page beside it when this build carries one (DESIGN.md §15 and ticket 04
     * decision 3). {@code base} is {@link #snapshotBase}, taken before the
     * catch-up; {@code failed} is what {@link #catchUpIndex} returned, and a
     * failed catch-up of the writable member keeps the old snapshot
     * ({@code catch_up_failed}). {@code maxBytes <= 0} writes at any size.
     */
    IndexSnapshot.Result snapshotWritable(Index index, long maxBytes, SnapshotBase base, Set<String> failed) {
        return snapshotWritable(index, maxBytes, base, failed, false)
    }

    /**
     * {@code cas.sweep.grace} (14 days by default) for the snapshot meta, so
     * the page can date the earliest deletion of released content (P2 ruling
     * R7). Null, with one warning, when the setting does not parse: the
     * snapshot is derived, and a sweep refuses the setting itself.
     */
    private Long sweepGraceMillis() {
        try {
            return SweepSettings.graceMillis(config.rawConfig)
        }
        catch( IllegalArgumentException e ) {
            warnOnce("${e.message}; the Index Snapshot is written without a sweep grace, so the explorer gives no deletion date for released content".toString())
            return null
        }
    }

    /**
     * As the 4-arg {@code snapshotWritable}; with {@code rebuild} the
     * snapshot replaces base even when base has a newer watermark or more
     * runs (`reindex`, and a sweep whose deletes no forget entry announces).
     */
    IndexSnapshot.Result snapshotWritable(Index index, long maxBytes, SnapshotBase base, Set<String> failed, boolean rebuild) {
        if( failed?.contains(config.writableAlias) )
            return new IndexSnapshot.Result(false, null, 0L, -1, null, IndexSnapshot.CATCH_UP_FAILED)
        final SnapshotStorage storage = snapshotsOf(config.writableAlias)
        final BlockStore writable = members()[0]
        final IndexSnapshot.Result result = IndexSnapshot.write(index, config.writableAlias, storage, maxBytes, base, config.tmpDir, writableStoreId(), rebuild,
            sweepGraceMillis(), presence(writable), sweepLockOf(writable))
        if( result.written ) {
            final byte[] page = IndexSnapshot.bundledPage()
            if( page != null )
                writePage(storage, page)
            else
                warnOnce('this build of nf-blocks carries no explorer page; the snapshot was written without index.html')
        }
        return result
    }

    /**
     * A sweep takes what only released or deleted runs reached, and a copy brings a run's records without all its
     * files (P3 plan D8): a snapshot build asks the member which blocks of those, and of transferred roots and runs, it lacks.
     */
    private static Closure<Boolean> presence(BlockStore member) {
        return { Cid cid -> member.has(cid) }
    }

    /** The member's sweep.lock: the build records no presence while a sweep may be deleting (null: none is recorded). */
    private static RetentionStorage sweepLockOf(BlockStore member) {
        return member instanceof RetainedStore ? ((RetainedStore) member).retentionStorage() : null
    }

    /**
     * Why the rule would not seed {@code alias} from the member's own snapshot,
     * in {@link Index#trySeed}'s words, or null when it would: the snapshot is
     * there, and its store id is the one this cache recorded for the member, or
     * it has none and its watermark is null or in the member's Store Log. Reads
     * the snapshot's HEAD (and a GET only for an uncounted S3 snapshot), not the
     * snapshot itself, so its schema_version is not checked.
     */
    String ownSnapshotUnusable(String alias, Index index) {
        final SnapshotStorage storage = snapshotsOf(alias)
        final BlockStore member = member(alias)
        if( storage == null || member == null )
            return 'absent'
        try {
            final SnapshotBase base = storage.base(config.tmpDir)
            if( base == null )
                return 'absent'
            final String memberId = index.storeIdOf(alias)
            if( base.storeId != null && memberId != null && base.storeId != memberId )
                return "written for another incarnation of the member (store_id ${base.storeId}, the member's is ${memberId})".toString()
            if( base.storeId == null && base.watermark != null && !StoreLog.of(member).has(base.watermark) )
                return "its watermark ${base.watermark} is not in the member's Store Log, so it may be from an earlier incarnation".toString()
            return null
        }
        catch( Exception e ) {
            log.debug("could not look at the Index Snapshot of '${alias}': ${e.message}", e)
            return 'unreadable'
        }
    }

    /** The store-id of {@code alias}, created when absent (a push writes one, spec §6.5); null when the member keeps none. */
    String ensureStoreId(String alias) {
        final BlockStore member = member(alias)
        return member instanceof IdentifiedStore ? StoreIds.ensure(((IdentifiedStore) member).storeIdStorage()) : null
    }

    /**
     * The snapshot of {@code alias} built from {@code index} as a local file in
     * the session's tmpDir, as {@code nf-blocks:snapshot} would build it against
     * that member: its presence check and its sweep.lock (spec §6.4). The caller
     * deletes {@code file}.
     */
    IndexSnapshot.Built buildMemberSnapshot(Index index, String alias) {
        final BlockStore member = member(alias)
        return IndexSnapshot.build(index, alias, config.tmpDir, sweepGraceMillis(), presence(member), 0L, sweepLockOf(member))
    }

    /**
     * Writes the snapshot of {@code alias} into the member with
     * {@link #snapshotWritable}'s guards against {@code base}, at any size and
     * without the page (spec §6.5). {@code storeId} is {@link #ensureStoreId}'s.
     */
    IndexSnapshot.Result pushMemberSnapshot(Index index, String alias, SnapshotBase base, String storeId) {
        final BlockStore member = member(alias)
        return IndexSnapshot.write(index, alias, snapshotsOf(alias), 0L, base, config.tmpDir, storeId, false, sweepGraceMillis(),
            presence(member), sweepLockOf(member))
    }

    /** The explorer page beside a written snapshot. Derived (rule 3): a failure warns, and the snapshot stands. */
    private void writePage(SnapshotStorage storage, byte[] page) {
        try {
            storage.writePage(page)
        }
        catch( Exception e ) {
            log.warn("the explorer page of store '${config.writableAlias}' could not be written beside its Index Snapshot; it is derived: ${e.message}", e)
        }
    }

    /**
     * The writable S3 member's clock check (ticket 03 decision 2): one HEAD, so
     * the S3Ops has seen a response and its Date header, then the skew judged.
     * Over 5 minutes throws {@link ClockSkewException}, over 1 minute warns. A
     * local writable member makes no request.
     */
    void checkClock() {
        final BlockStore writable = members()[0]
        if( !(writable instanceof S3BlockStore) )
            return
        final S3BlockStore s3 = (S3BlockStore) writable
        try {
            s3.ops.head(s3.prefix + IndexSnapshot.relativePath())
        }
        catch( Exception e ) {
            // A 403 on a missing key without s3:ListBucket, or the network: the check is advice, and
            // a real outage is reported by the first write (silent decision 19).
            notices.warn("could not check this machine's clock against S3 (${e.message}); continuing".toString())
            return
        }
        final Long server = s3.ops.firstServerDateMillis()
        if( server == null )
            return
        final long now = System.currentTimeMillis()
        switch( ClockSkew.judge(now, server) ) {
            case ClockSkew.Verdict.ABORT:
                throw new ClockSkewException(ClockSkew.describe(now, server))
            case ClockSkew.Verdict.WARN:
                notices.warn(ClockSkew.describe(now, server))
                break
            default:
                break
        }
    }

    /** How a waiting run sleeps between looks at sweep.lock; a test seam. */
    static Closure<Void> liveSleeper = { long ms -> Thread.sleep(ms) } as Closure<Void>

    private LiveWriter liveWriter
    private ScheduledExecutorService liveBeats
    private Thread liveHook

    /**
     * Ticket 20 answers 1 and 5: register as a Live Writer, then wait while a
     * sweep holds the lock. The writer is built under this session's monitor
     * and started outside it, so the (unbounded) wait on sweep.lock never
     * holds up another thread that needs the session.
     */
    void startLiveWriter(String session, Map<String, String> info) {
        LiveWriter writer = null
        synchronized( this ) {
            final BlockStore writable = members()[0]
            if( liveWriter != null || !(writable instanceof RetainedStore) || !writable.isWritable() )
                return
            liveBeats = Executors.newSingleThreadScheduledExecutor({ Runnable r ->
                final Thread t = new Thread(r, 'nf-blocks-live-heartbeat'); t.daemon = true; t } as ThreadFactory)
            final LiveWriter built = new LiveWriter(((RetainedStore) writable).retentionStorage(), session, info,
                { String m -> ConsoleLog.LOG.warn(m) } as Closure<Void>, liveSleeper, liveBeats)
            writer = built
            liveWriter = built
            liveHook = new Thread({ -> built.close() } as Runnable, 'nf-blocks-live-deregister')
            Runtime.runtime.addShutdownHook(liveHook)
        }
        writer.start()
    }

    synchronized void stopLiveWriter() {
        liveWriter?.close()
        liveWriter = null
        liveBeats?.shutdownNow()
        liveBeats = null
        if( liveHook != null ) {
            try { Runtime.runtime.removeShutdownHook(liveHook) } catch( IllegalStateException e ) { /* already shutting down */ }
            liveHook = null
        }
    }

    /** A notice for the person at the terminal, once: see {@link Notices}. */
    private void warnOnce(String message) {
        notices.warn(message)
    }

    void recordPublish(String joinKey, Publish publish) {
        publishes.put(joinKey, publish)
    }

    Publish publishFor(String joinKey) {
        return publishes.get(joinKey)
    }

    // ------------------------------------------------------------------ spools

    /**
     * A coordinate's bytes written through {@code newOutputStream}, on their
     * way to one block (DESIGN.md §8). A stream opened without APPEND is stored
     * when it closes. Nextflow builds a CSV Output Index File from a delete and
     * a series of appends, so an APPEND close is not the end of the file: that
     * spool is {@code deferred}, held in {@link #spools} and hashed once, when
     * something needs the coordinate or the join runs.
     */
    @CompileStatic
    private static class Spool {
        final CoordinateTree tree
        final String rel
        final String name
        final Path file
        final boolean deferred
        /** Streams opened on the spool and not yet closed; guarded by the key's lock. */
        int writers
        Spool(CoordinateTree tree, String rel, String name, Path file, boolean deferred) {
            this.tree = tree; this.rel = rel; this.name = name; this.file = file; this.deferred = deferred
        }
    }

    /** join key -> its deferred (APPEND) spool, while one is pending. */
    private final ConcurrentHashMap<String, Spool> spools = new ConcurrentHashMap<>()

    /** join key -> the object its spool operations synchronise on; never removed, so two threads always share one. */
    private final ConcurrentHashMap<String, Object> spoolLocks = new ConcurrentHashMap<>()

    /**
     * Set when the join's sweep ({@link #finalizeAllPending}) starts: from then
     * on an APPEND close is stored at once too, since nothing sweeps again (an
     * observer after nf-blocks, such as nf-prov, still writes into outputDir).
     */
    private volatile boolean sealed = false

    private Object spoolLock(String key) {
        return spoolLocks.computeIfAbsent(key, { String k -> new Object() })
    }

    /**
     * A stream into a spool for the coordinate. Without {@code append} the
     * stream has a spool of its own, stored as one block, one Pointer File
     * write and one {@code recordPublish} when it closes, and a pending
     * spool for the key is dropped first. With it, a pending spool is
     * appended to, else a new one is seeded from {@code current} (the
     * coordinate's content, or null when it resolves to no file), and the
     * close stores nothing until the join has run. The bytes are copied by
     * streams, never held (DESIGN.md §0 rule 2).
     */
    OutputStream openSpool(String key, CoordinateTree tree, String rel, String name, boolean append, Closure<InputStream> current) throws IOException {
        final Object lock = spoolLock(key)
        Spool spool = null
        synchronized( lock ) {
            Spool pending = spools.get(key)
            // Assumes no stream on the old spool is still open: Nextflow writes one path sequentially.
            if( pending != null && !append ) {
                spools.remove(key)
                Files.deleteIfExists(pending.file)
                pending = null
            }
            if( pending == null ) {
                pending = new Spool(tree, rel, name, newSpoolFile(), append)
                if( append ) {
                    final InputStream seed = current.call()
                    if( seed != null ) {
                        try { Files.copy(seed, pending.file, StandardCopyOption.REPLACE_EXISTING) }
                        catch( IOException e ) { Files.deleteIfExists(pending.file); throw e }
                        finally { seed.close() }
                    }
                    spools.put(key, pending)
                }
            }
            pending.writers++
            spool = pending
        }
        final OutputStream out = Files.newOutputStream(spool.file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
        final CasSession self = this
        final Spool target = spool
        return new FilterOutputStream(out) {
            private boolean closed = false
            @Override void write(byte[] b, int off, int len) throws IOException { out.write(b, off, len) }
            @Override
            void close() throws IOException {
                if( closed ) return
                closed = true
                boolean flushed = false
                try { super.close(); flushed = true }
                finally { self.spoolClosed(key, target, flushed) }
            }
        }
    }

    /**
     * A stream on {@code spool} closed. A spool of its own (no APPEND) is
     * stored now; a deferred one is stored now only once the join has run and
     * no other stream on it is open, and only while it is still the key's
     * pending spool (a delete or a later write may have dropped it).
     */
    @PackageScope void spoolClosed(String key, Spool spool, boolean flushed) throws IOException {
        synchronized( spoolLock(key) ) {
            spool.writers--
            if( !spool.deferred ) {
                try {
                    if( flushed )
                        storeSpool(key, spool)
                }
                finally { Files.deleteIfExists(spool.file) }
                return
            }
            if( flushed && sealed && spools.get(key).is(spool) )
                finalizePending(key)
        }
    }

    /** One block, one Pointer File write and one {@code recordPublish} of the spool's bytes. */
    private void storeSpool(String key, Spool spool) throws IOException {
        final long size = Files.size(spool.file)
        Cid cid = null
        final InputStream input = Files.newInputStream(spool.file)
        try { cid = store.putStreaming(input) }
        finally { input.close() }
        final StoreRef ref = new StoreRef(cid, spool.name)
        spool.tree.write(spool.rel, ref)
        recordPublish(key, new Publish(ref, size, Providers.HEAD_NODE))
        log.debug "cas: published a written file as block ${cid} at ${key} (${size} bytes)"
    }

    private Path newSpoolFile() throws IOException {
        final Path dir = config.tmpDir ?: Path.of(System.getProperty('java.io.tmpdir'))
        Files.createDirectories(dir)
        final Path file = Files.createTempFile(dir, 'cas-spool-', '.tmp')
        if( file.fileSystem == FileSystems.default )
            file.toFile().deleteOnExit()
        return file
    }

    /** True while any coordinate has a deferred spool not yet finalised; lets a read skip the lookup. */
    boolean hasPendingSpools() {
        return !spools.isEmpty()
    }

    /**
     * Hashes the coordinate's pending spool into one block, writes its Pointer
     * File and records the publish, then deletes the spool; a no-op when none
     * is pending, or while a stream on it is still open. A failure leaves the
     * spool pending, so a later call can still record it.
     */
    void finalizePending(String key) throws IOException {
        if( !spools.containsKey(key) )
            return
        synchronized( spoolLock(key) ) {
            final Spool spool = spools.get(key)
            if( spool == null || spool.writers > 0 )
                return
            storeSpool(key, spool)
            spools.remove(key)
            Files.deleteIfExists(spool.file)
        }
    }

    /** {@link #finalizePending} for every coordinate under {@code dirKey}, so a listing of it sees them. */
    void finalizePendingUnder(String dirKey) throws IOException {
        if( spools.isEmpty() )
            return
        final String prefix = dirKey + '/'
        for( String key : new ArrayList<String>(spools.keySet()) )
            if( key.startsWith(prefix) )
                finalizePending(key)
    }

    /**
     * Every pending spool finalised, for the join (a write with no publish
     * event is still recorded), and the session sealed: any close after this
     * is stored at once. A spool whose stream is still open holds bytes no one
     * finished writing, most likely a publish thread racing a failed run's
     * completion: it stays pending, so its close stores it, and its key is
     * returned so the caller can warn that the run does not record it. A hash
     * or write failure of a closed spool throws.
     *
     * @return the keys of the spools left out because a stream on them was open, sorted
     */
    List<String> finalizeAllPending() throws IOException {
        sealed = true
        final List<String> unclosed = new ArrayList<String>()
        for( String key : new ArrayList<String>(spools.keySet()) ) {
            finalizePending(key)
            if( spools.containsKey(key) )
                unclosed.add(key)
        }
        return unclosed.sort()
    }

    /** Drops the coordinate's pending spool, as a delete does; true when there was one. */
    boolean dropPending(String key) throws IOException {
        if( spools.isEmpty() )
            return false
        synchronized( spoolLock(key) ) {
            final Spool spool = spools.remove(key)
            if( spool == null )
                return false
            Files.deleteIfExists(spool.file)
            return true
        }
    }

    void recordUploadAnomalies(String joinKey, Anomalies anomalies) {
        uploadAnomalies.put(joinKey, anomalies)
    }

    Anomalies uploadAnomaliesFor(String joinKey) {
        return uploadAnomalies.get(joinKey)
    }

    void setNextflowRunKey(String key) {
        nfRunKey.compareAndSet(null, key)
    }

    String getNextflowRunKey() {
        return nfRunKey.get()
    }

    void setRunManifest(Cid cid) {
        runManifest.set(cid)
    }

    Cid getRunManifest() {
        return runManifest.get()
    }

    /** True exactly once, for the first caller; the join is written only then. */
    boolean claimCompletion() {
        return completed.compareAndSet(false, true)
    }

    /** True once a notification has claimed the completion, whether or not it has finished writing it. */
    boolean completionClaimed() {
        return completed.get()
    }

    /** Called by the notification that claimed the completion, once it has written it (or failed to). */
    void completionWritten() {
        completionDone.countDown()
    }

    /**
     * Waits for the claiming notification to finish writing the completion.
     * False on timeout, or when interrupted (the interrupt is restored).
     */
    boolean awaitCompletionWritten(long timeoutMillis) {
        try {
            return completionDone.await(timeoutMillis, TimeUnit.MILLISECONDS)
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
            return false
        }
    }
}
