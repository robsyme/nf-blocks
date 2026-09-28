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
import robsyme.cas.core.CompositeStore
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Put
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreRef

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
        /** The Address Provider that produced the address, e.g. {@code head-node}. */
        String provider
        /**
         * For a published directory, the providers of the files inside it
         * (provider name to addresses), so RunCompletion.providers covers every
         * address the run published (ticket 16 decision 1). Empty for a file.
         */
        Map<String, List<Cid>> contents = [:]
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

    CasSession(CasConfig config) {
        this.config = config
        this.assertedBy = config.assertedBy
        this.store = buildStore(config)
        this.coordinates = new CoordinateTree(config.writableLocation.resolve('coords'))
    }

    /** Test seam: an instance over an already-built store and coordinate tree. */
    CasSession(CasConfig config, BlockStore store, CoordinateTree coordinates) {
        this.config = config
        this.assertedBy = config.assertedBy
        this.store = store
        this.coordinates = coordinates
    }

    private static BlockStore buildStore(CasConfig config) {
        final List<BlockStore> members = new ArrayList<>()
        for( String alias : config.members ) {
            final Path location = config.locationOf(alias)
            members.add(new LocalBlockStore(location, alias, alias == config.writableAlias))
        }
        return new CompositeStore(members)
    }

    /**
     * Brings the index up to date from every store member's Store Log, so a read
     * across a composition (a {@code fromStore} through {@code [out, lab]}) sees
     * the runs recorded in a read-only member and not only the writable one
     * (DESIGN.md §12). Each member advances its own watermark, so this is cheap
     * to call before a query and idempotent. Derived, so a member whose log
     * cannot be read is logged and skipped rather than failing the caller.
     */
    void catchUpIndex(Index index) {
        for( BlockStore member : members() ) {
            try {
                index.catchUp(member, StoreLog.of(member), member.alias())
            }
            catch( Exception e ) {
                log.warn("could not catch up the index from store member '${member.alias()}'; it is derived: ${e.message}", e)
            }
        }
    }

    /** The store's members, writable first; a single-store session has one. */
    List<BlockStore> members() {
        return store instanceof CompositeStore ? ((CompositeStore) store).members : [store]
    }

    /** This composition's per-user cache index (DESIGN.md §12). The caller closes it. */
    Index openIndex() {
        return Index.open(IndexPaths.cachePath(config.localLocations(), config.indexOverride))
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
     * page beside it when this build carries one (DESIGN.md §15).
     * {@code maxBytes <= 0} writes at any size.
     */
    IndexSnapshot.Result snapshotWritable(Index index, long maxBytes) {
        final IndexSnapshot.Result result = IndexSnapshot.write(index, config.writableAlias, config.writableLocation, maxBytes)
        if( result.written ) {
            final byte[] page = IndexSnapshot.bundledPage()
            if( page != null )
                IndexSnapshot.writePage(config.writableLocation, page)
            else
                warnOnce('this build of nf-blocks carries no explorer page; the snapshot was written without index.html')
        }
        return result
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
