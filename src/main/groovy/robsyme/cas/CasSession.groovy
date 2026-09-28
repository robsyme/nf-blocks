package robsyme.cas

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import groovy.transform.CompileStatic
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
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.Put
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.SnapshotStorage
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
    final String assertedBy

    /** join key -> the address, size and provider recorded when the file was published. */
    final ConcurrentHashMap<String, Publish> publishes = new ConcurrentHashMap<>()

    /**
     * join key -> what a published directory could not address (DESIGN §6).
     * The provider's {@code upload()} records it here so the run's join
     * ({@code onFlowComplete}) can fold directory anomalies into the
     * RunCompletion without re-reading the manifest.
     */
    final ConcurrentHashMap<String, Anomalies> uploadAnomalies = new ConcurrentHashMap<>()

    /** Nextflow's own WorkflowRun key (LinObserver.executionHash), seen on the first save. */
    private final AtomicReference<String> nfRunKey = new AtomicReference<>()

    /** Our RunManifest address, written once at onFlowBegin. */
    private final AtomicReference<Cid> runManifest = new AtomicReference<>()

    /** Fires once: the join at onFlowComplete, which runs twice on a failed run. */
    private final AtomicBoolean completed = new AtomicBoolean(false)
    private final CountDownLatch completionDone = new CountDownLatch(1)

    /** A test seam: the S3Ops for a bucket, from the loaded config map. */
    static Closure<S3Ops> s3OpsFactory = { Map config, String bucket -> S3Access.open(config, bucket) } as Closure<S3Ops>

    /** alias -> the member's coordinate tree and snapshot storage, built beside its block store. */
    private final Map<String, CoordinateTree> trees = new LinkedHashMap<>()
    private final Map<String, SnapshotStorage> snapshots = new LinkedHashMap<>()

    CasSession(CasConfig config) {
        this.config = config
        this.assertedBy = config.assertedBy
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
            warnOnce(config.storageClassWarning)
    }

    /**
     * Test seam: an instance over an already-built store and coordinate tree.
     * The writable alias gets {@code coordinates}, and a snapshot storage over
     * the store's root when its writable member is local; every other local
     * member gets its own tree and storage from the config.
     */
    CasSession(CasConfig config, BlockStore store, CoordinateTree coordinates) {
        this.config = config
        this.assertedBy = config.assertedBy
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
        final Set<String> failed = new LinkedHashSet<String>()
        for( BlockStore member : members() ) {
            try {
                index.catchUp(member, StoreLog.of(member), member.alias(), snapshotsOf(member.alias()), config.tmpDir)
            }
            catch( Exception e ) {
                failed.add(member.alias())
                log.warn("could not catch up the index from store member '${member.alias()}'; it is derived: ${e.message}", e)
            }
        }
        return failed
    }

    /** The store's members, writable first; a single-store session has one. */
    List<BlockStore> members() {
        return store instanceof CompositeStore ? ((CompositeStore) store).members : [store]
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

    /** This composition's per-user cache index (DESIGN.md §12), named by every member's location text. The caller closes it. */
    Index openIndex() {
        return Index.open(IndexPaths.cachePath(config.locationTexts(), config.indexOverride))
    }

    /**
     * The one builder over this composition, writing to the writable member
     * (block explorer spec section 9). The caller owns {@code index}; the
     * builder uses it under its own lock.
     */
    Put newPut(Index index) {
        return new Put(store, members()[0], index, assertedBy, { -> System.currentTimeMillis() }, { -> catchUpIndex(index) })
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
        if( failed?.contains(config.writableAlias) )
            return new IndexSnapshot.Result(false, null, 0L, -1, null, IndexSnapshot.CATCH_UP_FAILED)
        final SnapshotStorage storage = snapshotsOf(config.writableAlias)
        final IndexSnapshot.Result result = IndexSnapshot.write(index, config.writableAlias, storage, maxBytes, base, config.tmpDir)
        if( result.written ) {
            final byte[] page = IndexSnapshot.bundledPage()
            if( page != null )
                writePage(storage, page)
            else
                warnOnce('this build of nf-blocks carries no explorer page; the snapshot was written without index.html')
        }
        return result
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
            ConsoleLog.LOG.warn("could not check this machine's clock against S3 (${e.message}); continuing")
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
                ConsoleLog.LOG.warn(ClockSkew.describe(now, server))
                break
            default:
                break
        }
    }

    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    private static void warnOnce(String message) {
        if( WARNED.add(message) )
            log.warn(message)
    }

    void recordPublish(String joinKey, Publish publish) {
        publishes.put(joinKey, publish)
    }

    Publish publishFor(String joinKey) {
        return publishes.get(joinKey)
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
