package robsyme.cas

import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Claim
import robsyme.cas.core.ClaimState
import robsyme.cas.core.CopyClosure
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LineageRecords
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalLineageRecords
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.NoSuchBlockException
import robsyme.cas.core.Records
import robsyme.cas.core.RunManifestAudit
import robsyme.cas.core.RunRef
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogEntry
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.Transfer
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3LineageRecords

/**
 * One `nf-blocks:copy` (P3 plan 1.1 to 1.6): a plan, and unless it is a dry
 * run, the copy itself. Shared by the verb (`cli/CopyCommand`) and the explore
 * server's `POST /api/copy`, so it knows nothing of either: it takes the
 * request as the endpoint's JSON keys and answers with a status and the 1.3
 * body. 200 is a plan or a finished copy, 400 a request it refuses
 * (`{error, message}`), 409 a real copy refused before any write (the body is
 * the plan, its `refused` saying why: `not_kept` when a root or run is
 * released or deleted at the destination, D9; `audit` for audit findings
 * without `accept_audit`), 500 a copy that failed part way (the body lists
 * the failures). Anything else, a config that does not parse or a store that
 * cannot be read, is thrown.
 *
 * The copy composition is the configured one with the destination as its
 * writable member and the source added to `cas.resolve` when missing, so its
 * index sees both and the destination is opened writable for this copy only
 * (plan D1).
 */
@Slf4j
@CompileStatic
class CopyService {

    static final int OK = 200
    static final int BAD_REQUEST = 400
    /** A real copy refused before any write; the body's `refused` says why. */
    static final int REFUSED = 409
    static final int FAILED = 500

    static final String BAD_ROOT = 'bad_root'
    static final String UNKNOWN_MEMBER = 'unknown_member'
    static final String SAME_MEMBER = 'same_member'
    static final String NOT_A_TARGET = 'not_a_target'
    static final String ROOT_NOT_FOUND = 'root_not_found'
    static final String BAD_DEPTH = 'bad_depth'
    static final String BAD_CLAIMS = 'bad_claims'
    static final String BAD_REQUEST_CODE = 'bad_request'
    /** A member's `cas.stores.<alias>.readOnly` is not true or false. */
    static final String BAD_CONFIG = 'bad_config'
    /** The root resolves only through the cache, and the source is a member the explorer's cache does not hold. */
    static final String SOURCE_NOT_INDEXED = 'not_indexed'
    /** The 400s that say the request was malformed, rather than naming something that cannot be copied: exit 2 at the CLI. */
    static final Set<String> USAGE_ERRORS = Collections.unmodifiableSet(
        [BAD_ROOT, UNKNOWN_MEMBER, SAME_MEMBER, BAD_DEPTH, BAD_CLAIMS, BAD_REQUEST_CODE] as Set<String>)

    static final List<String> DEFAULT_CLAIMS = Collections.unmodifiableList(
        [CopyClosure.NAMES, CopyClosure.PROJECTS, CopyClosure.ANNOTATIONS])
    static final String NO_CLAIMS = 'none'

    /** `refused` on a 409: a root or run released or deleted at the destination (D9). */
    static final String NOT_KEPT = 'not_kept'
    /** `refused` on a 409: audit findings without `accept_audit` (1.6). */
    static final String AUDIT = 'audit'

    /** `destination_state[].state` (D9). */
    static final String ABSENT = 'absent'
    static final String HELD = 'held'
    static final String PINNED = 'pinned'
    static final String RELEASED = 'released'
    static final String DELETED = 'deleted'

    static final String SELECTION_PREFIX = 'selection:'
    static final String RUN_PREFIX = 'run:'

    private static final int THREADS = 16

    @CompileStatic
    static final class Outcome {
        final int status
        final Map<String, Object> body

        Outcome(int status, Map<String, Object> body) {
            this.status = status
            this.body = body
        }
    }

    /**
     * What the explorer knows about which members are indexed (background
     * store indexing, spec §6.3), for a copy it runs: {@code unindexed} is
     * alias to reason for the members it lists as not indexed, and
     * {@code localRoots} maps an alias to the directory holding
     * {@code index/v7.sqlite} built on this machine. The CLI passes none.
     */
    @CompileStatic
    static final class Indexing {
        final Map<String, String> unindexed
        final Map<String, Path> localRoots

        Indexing(Map<String, String> unindexed, Map<String, Path> localRoots) {
            this.unindexed = unindexed
            this.localRoots = localRoots
        }
    }

    /** A request refused with a 400. */
    @CompileStatic
    private static final class Refused extends RuntimeException {
        final String code

        Refused(String code, String message) {
            super(message)
            this.code = code
        }
    }

    /** The request, checked. */
    @CompileStatic
    private static final class Ask {
        String root
        String from
        String to
        String depth
        /** The carried categories, in {@link CopyClosure#CATEGORIES} order; empty for `none`. */
        List<String> claims
        boolean acceptAudit
    }

    /** Stamps the log entries and the Transfer; a test seam. */
    private final Closure<Long> clock
    /** Who signs the Transfer; null to resolve it from the config as a command does. */
    private final Supplier<Principal> signer

    CopyService() {
        this({ -> System.currentTimeMillis() } as Closure<Long>)
    }

    CopyService(Closure<Long> clock) {
        this(clock, null)
    }

    CopyService(Closure<Long> clock, Supplier<Principal> signer) {
        this.clock = clock
        this.signer = signer
    }

    /**
     * @param config the raw Nextflow config map
     * @param request root, from (null for the default), to, depth, claims (a list), accept_audit
     * @param err where the session's notices and this copy's warnings go
     */
    static Outcome execute(Map config, Map request, boolean dryRun, PrintStream err) {
        return new CopyService().run(config, request, dryRun, err)
    }

    /**
     * As the 4-argument execute, signing with {@code signer}'s principal: the
     * explore server's own, so a copy there resolves no identity and prints
     * no "signing as" line. Asked only when a copy writes its Transfer; a
     * null supplier, or a null answer, resolves from the config instead.
     */
    static Outcome execute(Map config, Map request, boolean dryRun, PrintStream err, Supplier<Principal> signer) {
        return execute(config, request, dryRun, err, signer, null)
    }

    /**
     * As the 5-argument execute, under the explorer's view of which members
     * are indexed: the copy leaves {@code indexing.unindexed} out of its
     * catch-up, seeds a member from its local snapshot, and writes no
     * snapshot to a destination the explorer has not indexed. Null changes nothing.
     */
    static Outcome execute(Map config, Map request, boolean dryRun, PrintStream err, Supplier<Principal> signer, Indexing indexing) {
        return new CopyService({ -> System.currentTimeMillis() } as Closure<Long>, signer).run(config, request, dryRun, err, indexing)
    }

    Outcome run(Map config, Map request, boolean dryRun, PrintStream err) {
        return run(config, request, dryRun, err, null)
    }

    Outcome run(Map config, Map request, boolean dryRun, PrintStream err, Indexing indexing) {
        try {
            return plan(config, ask(request), dryRun, err, indexing)
        }
        catch( Refused r ) {
            final Map<String, Object> body = new LinkedHashMap<String, Object>()
            body.put('error', r.code)
            body.put('message', r.message)
            return new Outcome(BAD_REQUEST, body)
        }
        catch( ClockSkewException e ) {
            throw e
        }
        catch( Exception e ) {
            // await wrapped an InterruptedException; the caller still needs to see the flag.
            for( Throwable c = e; c != null; c = c.cause )
                if( c instanceof InterruptedException )
                    Thread.currentThread().interrupt()
            // Whatever else escapes is told without the operator's paths (CopyErrors): the verb prints
            // this message, and the explore server answers it as a 500 copy_failed.
            throw new Failed(CopyErrors.describe(e, CopyErrors.rootsOf(config)), e)
        }
    }

    /** An exception out of a copy, its message already redacted; the cause stays for the log. */
    static final class Failed extends RuntimeException {
        Failed(String message, Throwable cause) {
            super(message, cause)
        }
    }

    // ---- the request

    private static Ask ask(Map request) {
        final Ask a = new Ask()
        a.root = text(request, 'root')
        if( !a.root )
            throw new Refused(BAD_ROOT, "a copy needs a root: selection:<cid>, run:<ref> or a bare cid")
        a.to = text(request, 'to')
        if( !a.to )
            throw new Refused(BAD_REQUEST_CODE, 'a copy needs a destination member (--to <alias>)')
        a.from = text(request, 'from') ?: null
        a.depth = text(request, 'depth') ?: CopyClosure.RESULTS
        if( !CopyClosure.DEPTHS.contains(a.depth) )
            throw new Refused(BAD_DEPTH, "--depth is one of ${CopyClosure.DEPTHS.join(', ')}, got '${a.depth}'")
        a.claims = claims(request.get('claims'))
        final Object accept = request.get('accept_audit')
        if( accept != null && !(accept instanceof Boolean) )
            throw new Refused(BAD_REQUEST_CODE, "accept_audit is true or false, got '${accept}'")
        a.acceptAudit = accept == Boolean.TRUE
        return a
    }

    private static String text(Map request, String key) {
        final Object value = request.get(key)
        if( value != null && !(value instanceof CharSequence) )
            throw new Refused(BAD_REQUEST_CODE, "${key} must be a string, got ${value.getClass().simpleName}")
        return value?.toString()?.trim()
    }

    private static List<String> claims(Object value) {
        if( value == null )
            return DEFAULT_CLAIMS
        if( !(value instanceof List) )
            throw new Refused(BAD_CLAIMS, "claims is a list of ${CopyClosure.CATEGORIES.join(', ')}, or ['${NO_CLAIMS}'], got '${value}'")
        final Set<String> asked = new LinkedHashSet<String>()
        for( Object each : (List) value )
            asked.add(String.valueOf(each).trim())
        if( asked.contains(NO_CLAIMS) ) {
            if( asked.size() > 1 )
                throw new Refused(BAD_CLAIMS, "--claims ${NO_CLAIMS} stands alone, got ${asked.join(',')}")
            return Collections.<String> emptyList()
        }
        if( asked.isEmpty() )
            throw new Refused(BAD_CLAIMS, "--claims names at least one of ${CopyClosure.CATEGORIES.join(', ')}, or ${NO_CLAIMS}")
        for( String c : asked )
            if( !CopyClosure.CATEGORIES.contains(c) )
                throw new Refused(BAD_CLAIMS, "--claims takes ${CopyClosure.CATEGORIES.join(', ')} or ${NO_CLAIMS}, got '${c}'" +
                    (c in [CopyClosure.RETAIN, CopyClosure.DELETION] ? '; retain and deletion claims are about the source and never travel' : ''))
        return CopyClosure.CATEGORIES.findAll { String c -> asked.contains(c) } as List<String>
    }

    // ---- the composition

    /** The aliases under `cas.stores`, in config order. */
    private static List<String> configured(Map config) {
        final Object scope = config?.get(CasConfig.SCHEME)
        final Object stores = scope instanceof Map ? ((Map) scope).get('stores') : null
        return stores instanceof Map ? ((Map) stores).keySet().collect { Object k -> k.toString() } as List<String> : Collections.<String> emptyList()
    }

    private static boolean readOnlyIn(Map config, String alias) {
        final Object store = ((Map) ((Map) config.get(CasConfig.SCHEME)).get('stores')).get(alias)
        final Object value = store instanceof Map ? ((Map) store).get('readOnly') : null
        if( value != null && !(value instanceof Boolean) )
            throw new Refused(BAD_CONFIG, "cas.stores.${alias}.readOnly must be true or false, got '${value}'")
        return value == Boolean.TRUE
    }

    /** A copy of {@code config} whose `cas.resolve` is the configured list (or every alias) plus {@code from} when missing. */
    private static Map copyComposition(Map config, String from) {
        final Map scope = new LinkedHashMap((Map) config.get(CasConfig.SCHEME))
        final Object resolve = scope.get('resolve')
        final List<String> members = resolve instanceof List
            ? ((List) resolve).collect { Object a -> a.toString() } as List<String>
            : configured(config)
        if( from != null && !members.contains(from) )
            members.add(from)
        scope.put('resolve', members)
        final Map copy = new LinkedHashMap(config)
        copy.put(CasConfig.SCHEME, scope)
        return copy
    }

    private Outcome plan(Map config, Ask ask, boolean dryRun, PrintStream err, Indexing indexing) {
        final List<String> aliases = configured(config)
        if( !aliases.contains(ask.to) )
            throw new Refused(UNKNOWN_MEMBER, "'${ask.to}' is not a configured store; configured: ${aliases.join(', ') ?: 'none'}")
        if( ask.from != null && !aliases.contains(ask.from) )
            throw new Refused(UNKNOWN_MEMBER, "'${ask.from}' is not a configured store; configured: ${aliases.join(', ')}")
        if( ask.from == ask.to )
            throw new Refused(SAME_MEMBER, "--from and --to are both '${ask.to}'; a copy goes between two members")
        // CasConfig throws on a readOnly that is not a boolean, on any member: a 400 here instead.
        for( String alias : aliases )
            readOnlyIn(config, alias)
        // CasConfig refuses a read-only writable member, so this one is judged before the composition is built.
        if( readOnlyIn(config, ask.to) )
            throw new Refused(NOT_A_TARGET, "'${ask.to}' is not a copy target: ${CopyTargets.READ_ONLY}")
        final CasConfig cfg = CasConfig.from(copyComposition(config, ask.from), "${CasConfig.SCHEME}://${ask.to}".toString())
        final CopyTargets.Judgement judgement = CopyTargets.judge(cfg, ask.to)
        if( !judgement.target )
            throw new Refused(NOT_A_TARGET, "'${ask.to}' is not a copy target: ${judgement.reason}")

        final CasSession cas = CasSession.forCommand(cfg, err)
        if( indexing != null ) {
            cas.leaveUnindexedOut(indexing.unindexed)
            for( Map.Entry<String, Path> local : indexing.localRoots.entrySet() )
                cas.seedFrom(local.key, new LocalSnapshotStorage(local.value))
        }
        final Map<String, String> roots = CopyErrors.rootsOf(config)
        final BlockStore dest = cas.members()[0]
        // Taken before any catch-up, so the snapshot guard covers it (CasSession.snapshotBase). A read.
        final SnapshotBase base = dryRun ? null : cas.snapshotBase()
        // A destination the explorer has not indexed gets no snapshot from this copy (spec §6.3): one it lists
        // as not indexed or indexed only on this machine, or one with no snapshot of its own, whose snapshot
        // would need a scan of it.
        final boolean notIndexed = indexing != null && (indexing.unindexed.containsKey(ask.to) || indexing.localRoots.containsKey(ask.to)
            || (!dryRun && base == null))
        final Index index = cas.openIndex()
        try {
            // The plan asks the destination `has`, and reads its Store Log and the Claims that log
            // lists (D9); its store-id is not created and it is not caught up into the index,
            // so a plan (a dry run, or a refused copy) writes nothing there.
            final Set<String> failed = cas.catchUpIndex(index, true, Collections.singleton(ask.to))
            if( failed )
                err.println("nf-blocks:copy: could not catch up the index from ${failed.join(', ')}; the plan may miss what they announced")
            final CopyClosure.Root root = rootFrom(ask, cas, index)
            final BlockStore source = source(cas, ask.from, root.cid)
            final CopyClosure.Result all = CopyClosure.compute(source, index, source.alias(), root, CopyClosure.ALL,
                new LinkedHashSet<String>(ask.claims), lineageOf(source))
            final CopyClosure.Result view = all.at(ask.depth)
            final ExecutorService pool = Executors.newFixedThreadPool(THREADS)
            try {
                final Set<Cid> asked = new LinkedHashSet<Cid>(all.entries.collect { CopyClosure.Entry e -> e.cid })
                // Claims not carried are not entries, but the page still shows whether the destination has them.
                asked.addAll(all.claims.collect { CopyClosure.ClaimEntry c -> c.cid })
                final Set<Cid> present = presentAt(dest, asked, pool)
                final Set<Cid> missing = new HashSet<Cid>(all.missingAtSource)
                final Map<String, Object> audit = audit(source, view)
                final List<Map<String, Object>> destination = destinationState(dest, index, root, view, present, missing, pool)
                final Map<String, Object> body = body(ask, root, source.alias(), view, present, missing, audit, destination, dryRun)
                if( dryRun )
                    return new Outcome(OK, body)
                // D9 before the audit: a copy the destination would not keep is refused whatever the audit says.
                if( !(Boolean) ((Map) body.get('plan')).get('kept') ) {
                    body.put('refused', NOT_KEPT)
                    return new Outcome(REFUSED, body)
                }
                if( ((List) audit.get('findings')) && !ask.acceptAudit ) {
                    body.put('refused', AUDIT)
                    return new Outcome(REFUSED, body)
                }
                // Nothing to transfer and no entry to add: the copy writes nothing, so it needs no Live Writer
                // (a PUT and a DELETE on S3), no clock check and no second `has`. announce answers null.
                if( !anythingToWrite(dest, view, present, missing) )
                    return copy(cas, index, base, source, ask, root, view, present, missing, body, pool, err, roots, notIndexed)
                // A clock far from S3's would misdate the log entries this writes (ticket 03 decision 2).
                cas.checkClock()
                // DESIGN.md §19 rule 1: no sweep beside a Live Writer. Registering waits out a sweep
                // already running, which may have taken blocks the plan counted present: ask again.
                // Inside the try, so an interrupt while it waits still deregisters (stopLiveWriter is null-safe).
                try {
                    cas.startLiveWriter(UUID.randomUUID().toString(), [
                        run_name  : 'nf-blocks:copy',
                        pipeline  : "copy ${root.kind}:${root.cid} from ${source.alias()}".toString(),
                        started_at: Index.isoMillis(System.currentTimeMillis()),
                    ])
                    final Set<Cid> still = presentAt(dest, present, pool)
                    if( still.size() == present.size() )
                        return copy(cas, index, base, source, ask, root, view, present, missing, body, pool, err, roots, notIndexed)
                    final Map<String, Object> again = CopyService.body(ask, root, source.alias(), view, still, missing, audit, destination, dryRun)
                    return copy(cas, index, base, source, ask, root, view, still, missing, again, pool, err, roots, notIndexed)
                }
                finally {
                    cas.stopLiveWriter()
                }
            }
            finally {
                pool.shutdownNow()
            }
        }
        finally {
            index.close()
        }
    }

    /**
     * {@link #root}, saying why when the root was not found because the
     * source is a member the explorer's catch-up left out: a run named by
     * lid:// or by its RunManifest resolves only through the cache. The CLI
     * leaves no member out, so its message is unchanged.
     */
    private static CopyClosure.Root rootFrom(Ask ask, CasSession cas, Index index) {
        try {
            return root(ask.root, cas, index)
        }
        catch( Refused r ) {
            final String reason = ask.from != null && r.code == ROOT_NOT_FOUND ? cas.unindexed().get(ask.from) : null
            if( reason == null )
                throw r
            final String what = reason == 'absent' ? "'${ask.from}' has no Index Snapshot" : "'${ask.from}' is not indexed (${reason})"
            throw new Refused(SOURCE_NOT_INDEXED, "${what}, so this copy cannot find ${ask.root} in it; " +
                "index it from the explorer's Storage page, or run nf-blocks:snapshot against it")
        }
    }

    /** `selection:<cid>`, `run:<ref>`, or a bare cid whose kind decides. */
    private static CopyClosure.Root root(String text, CasSession cas, Index index) {
        if( text.startsWith(SELECTION_PREFIX) ) {
            final Cid cid = cidOf(text.substring(SELECTION_PREFIX.length()), text)
            final String kind = kindIn(cas.store, cid, text)
            if( kind != Records.SELECTION )
                throw new Refused(BAD_ROOT, "'${text}' names ${kind ? "a ${kind}" : 'a raw block'}, not a Selection")
            return new CopyClosure.Root(CopyClosure.SELECTION_ROOT, cid)
        }
        if( text.startsWith(RUN_PREFIX) ) {
            String ref = text.substring(RUN_PREFIX.length())
            if( ref == RunRef.LATEST )
                throw new Refused(BAD_ROOT, "copy takes a run by address (run:<cid>, run:lid://<hash> or run:cas://<cid>); " +
                    "'latest' needs a pipeline, which copy has no flag for")
            if( Cid.isCid(ref) )
                ref = RunRef.CAS_PREFIX + ref
            try {
                return new CopyClosure.Root(CopyClosure.RUN_ROOT, RunRef.resolve(index, cas.store, ref, null))
            }
            catch( IllegalArgumentException e ) {
                throw new Refused(BAD_ROOT, e.message)
            }
            catch( IllegalStateException e ) {
                throw new Refused(ROOT_NOT_FOUND, e.message)
            }
        }
        if( !Cid.isCid(text) )
            throw new Refused(BAD_ROOT, "a copy root is selection:<cid>, run:<ref> or a bare cid, got '${text}'")
        final Cid cid = Cid.parse(text)
        final String kind = kindIn(cas.store, cid, text)
        switch( kind ) {
            case Records.SELECTION:
                return new CopyClosure.Root(CopyClosure.SELECTION_ROOT, cid)
            case Records.RUN_COMPLETION:
                return new CopyClosure.Root(CopyClosure.RUN_ROOT, cid)
            case Records.RUN_MANIFEST:
                final Cid completion = index.runByManifest(cid).orElse(null)
                if( completion == null )
                    throw new Refused(ROOT_NOT_FOUND, "'${text}' is a RunManifest with no RunCompletion; the run did not finish")
                return new CopyClosure.Root(CopyClosure.RUN_ROOT, completion)
            default:
                throw new Refused(BAD_ROOT, "'${text}' is ${kind ? "a ${kind}" : 'a raw block'}; a copy root is a Selection, a RunCompletion or a RunManifest")
        }
    }

    private static Cid cidOf(String text, String root) {
        if( !Cid.isCid(text) )
            throw new Refused(BAD_ROOT, "'${root}' does not end in a content address")
        return Cid.parse(text)
    }

    /** The block's kind, read from any member of the composition; null for a raw block. */
    private static String kindIn(BlockStore store, Cid cid, String root) {
        if( !store.has(cid) )
            throw new Refused(ROOT_NOT_FOUND, "no member of this composition holds ${root}")
        if( !cid.isDagCbor() )
            return null
        final InputStream input = store.open(cid)
        try {
            final Object decoded = DagCbor.decode(input.readAllBytes())
            return decoded instanceof Map ? Records.kindOf((Map) decoded) : null
        }
        finally {
            input.close()
        }
    }

    /** The named member, which must hold the root; else the first member after the destination, in `cas.resolve` order, that does. */
    private static BlockStore source(CasSession cas, String from, Cid root) {
        final List<BlockStore> members = cas.members()
        if( from != null ) {
            final BlockStore named = members.find { BlockStore m -> m.alias() == from }
            if( !named.has(root) )
                throw new Refused(ROOT_NOT_FOUND, "'${from}' does not hold ${root}")
            return named
        }
        for( int i = 1; i < members.size(); i++ )
            if( members[i].has(root) )
                return members[i]
        throw new Refused(ROOT_NOT_FOUND, "no member other than '${members[0].alias()}' holds ${root}; nothing to copy it from")
    }

    /** The source's `nf/` records, or null when it is neither a local nor an S3 member. */
    private static LineageRecords lineageOf(BlockStore source) {
        if( source instanceof LocalBlockStore )
            return new LocalLineageRecords(((LocalBlockStore) source).root)
        if( source instanceof S3BlockStore )
            return new S3LineageRecords(((S3BlockStore) source).ops, ((S3BlockStore) source).prefix)
        return null
    }

    // ---- the plan

    /** The destination's own `has` for each cid, 16 at a time. */
    private static Set<Cid> presentAt(BlockStore dest, Collection<Cid> cids, ExecutorService pool) {
        final Map<Cid, Future<Boolean>> pending = new LinkedHashMap<Cid, Future<Boolean>>()
        for( Cid c : cids ) {
            final Cid cid = c
            pending.put(cid, pool.submit({ -> dest.has(cid) } as Callable<Boolean>))
        }
        final Set<Cid> present = new HashSet<Cid>()
        for( Map.Entry<Cid, Future<Boolean>> p : pending.entrySet() )
            if( await(p.value) )
                present.add(p.key)
        return present
    }

    /**
     * Every RunManifest in the depth's view, read from the source (1.6): those of the runs, and those
     * a via collection reaches that no indexed RunCompletion names. Each is checked once.
     */
    private static Map<String, Object> audit(BlockStore source, CopyClosure.Result view) {
        final Set<Cid> manifests = new LinkedHashSet<Cid>()
        for( CopyClosure.Entry e : view.entries )
            if( e.kind == Records.RUN_MANIFEST )
                manifests.add(e.cid)
        for( CopyClosure.Run run : view.runs )
            if( run.manifest != null )
                manifests.add(run.manifest)
        int checked = 0
        final List<Map<String, Object>> findings = new ArrayList<Map<String, Object>>()
        for( Cid manifest : manifests ) {
            if( !manifest.isDagCbor() )
                continue
            final Object block = manifestAt(source, manifest)
            if( !(block instanceof Map) )
                continue
            checked++
            for( RunManifestAudit.Finding f : RunManifestAudit.audit(manifest, (Map) block) )
                findings.add(f.toMap())
        }
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        out.put('checked', checked)
        out.put('findings', findings)
        return out
    }

    /** The decoded block, or null when the store lacks it (at the source it is then missing_at_source, and not audited). */
    private static Object manifestAt(BlockStore source, Cid cid) {
        final InputStream input
        try {
            input = source.open(cid)
        }
        catch( NoSuchBlockException e ) {
            return null
        }
        try {
            return DagCbor.decode(input.readAllBytes())
        }
        finally {
            input.close()
        }
    }

    /**
     * D9: each root (the one asked for, then nested Selections) and each run
     * (a `run:` root once, as the root), as the destination's own Claims and
     * the Claims this copy carries leave it. Read write-free: the
     * destination's Store Log listing (nothing when it does not exist yet),
     * and the Claim blocks that log lists, each from the index when it knows
     * the Claim (a Claim is its content) and else from the destination.
     * "absent" is a subject the destination neither logs nor has a Claim on.
     * A pin wins, as it does in Mark; a conflicted group holds.
     */
    private static List<Map<String, Object>> destinationState(BlockStore dest, Index index, CopyClosure.Root root,
                                                              CopyClosure.Result view, Set<Cid> present, Set<Cid> missing,
                                                              ExecutorService pool) {
        final boolean runRoot = root.kind == CopyClosure.RUN_ROOT
        final List<Object[]> subjects = new ArrayList<Object[]>()
        subjects.add([root.cid, 'root', runRoot ? runName(view, root.cid) : currentName(view, root.cid), runRoot] as Object[])
        for( CopyClosure.Entry e : view.entries )
            if( e.kind == Records.SELECTION && e.cid != root.cid )
                subjects.add([e.cid, 'root', currentName(view, e.cid), false] as Object[])
        // A `run:` root is listed once, as the root.
        for( CopyClosure.Run r : view.runs )
            if( r.completion != root.cid )
                subjects.add([r.completion, 'run', r.runName, true] as Object[])
        final Set<Cid> asked = new HashSet<Cid>()
        for( Object[] s : subjects )
            asked.add((Cid) s[0])

        final Set<Cid> loggedRuns = new HashSet<Cid>()
        final Set<Cid> loggedSelections = new HashSet<Cid>()
        final Set<Cid> claimCids = new LinkedHashSet<Cid>()
        for( StoreLogEntry e : StoreLog.of(dest).read() ) {
            if( e.kind == StoreLogKind.RUN )
                loggedRuns.add(e.cid)
            else if( e.kind == StoreLogKind.SELECTION )
                loggedSelections.add(e.cid)
            else if( e.kind == StoreLogKind.CLAIM )
                claimCids.add(e.cid)
        }
        final Map<Cid, List<ClaimState.Row>> rows = new HashMap<Cid, List<ClaimState.Row>>()
        final Map<Cid, Future<Claim>> reading = new LinkedHashMap<Cid, Future<Claim>>()
        for( Cid c : claimCids ) {
            final Claim known = index.claimOf(c)
            if( known != null ) {
                addRow(rows, asked, c, known)
                continue
            }
            final Cid cid = c
            reading.put(cid, pool.submit({ -> claimAt(dest, cid) } as Callable<Claim>))
        }
        for( Map.Entry<Cid, Future<Claim>> r : reading.entrySet() ) {
            final Claim claim = await(r.value)
            if( claim != null )
                addRow(rows, asked, r.key, claim)
        }

        // The Claims this copy carries, as announce will log them there (a block the source lacks and the
        // destination does not hold gets no entry), so the state is the destination's after the copy:
        // a carried `del pin` can unpin what the destination released.
        final Map<Cid, List<ClaimState.Row>> after = new HashMap<Cid, List<ClaimState.Row>>()
        for( Map.Entry<Cid, List<ClaimState.Row>> e : rows.entrySet() )
            after.put(e.key, new ArrayList<ClaimState.Row>(e.value))
        for( CopyClosure.ClaimEntry c : view.claims ) {
            if( !c.carried || c.subject == null || !asked.contains(c.subject) || claimCids.contains(c.cid) )
                continue
            if( missing.contains(c.cid) && !present.contains(c.cid) )
                continue
            after.computeIfAbsent(c.subject) { Cid s -> new ArrayList<ClaimState.Row>() }
                .add(new ClaimState.Row(c.cid.toString(), c.verb, c.attribute, c.value,
                    (c.supersedes ?: Collections.<Cid> emptyList()).collect { Cid x -> x.toString() }))
        }

        final List<Map<String, Object>> out = new ArrayList<Map<String, Object>>()
        for( Object[] s : subjects ) {
            final Cid cid = (Cid) s[0]
            final boolean run = (Boolean) s[3]
            // "absent" is judged on the destination's own Claims alone (interface 1.3).
            final List<ClaimState.Row> own = rows.get(cid)
            final ClaimState st = ClaimState.of(after.get(cid) ?: Collections.<ClaimState.Row> emptyList())
            final String state = st.pinned ? PINNED
                : st.deletion == ClaimState.DELETED ? DELETED
                : run && st.released ? RELEASED
                : own == null && !(run ? loggedRuns : loggedSelections).contains(cid) ? ABSENT
                : HELD
            final Map<String, Object> m = new LinkedHashMap<String, Object>()
            m.put('cid', cid.toString())
            m.put('role', s[1])
            m.put('name', s[2])
            m.put('state', state)
            out.add(m)
        }
        return out
    }

    private static void addRow(Map<Cid, List<ClaimState.Row>> rows, Set<Cid> asked, Cid cid, Claim claim) {
        if( !asked.contains(claim.subject) )
            return
        rows.computeIfAbsent(claim.subject) { Cid s -> new ArrayList<ClaimState.Row>() }
            .add(new ClaimState.Row(cid.toString(), claim.verb, claim.attribute, claim.value,
                claim.supersedes.collect { Cid c -> c.toString() }))
    }

    /** A Claim the destination holds; null when it lacks the block or the block is not a Claim this build reads. */
    private static Claim claimAt(BlockStore dest, Cid cid) {
        if( !cid.isDagCbor() )
            return null
        final Object block = manifestAt(dest, cid)
        if( !(block instanceof Map) )
            return null
        try {
            return Claim.fromCbor((Map) block)
        }
        catch( RuntimeException e ) {
            return null
        }
    }

    /** The subject's current name among the source's Claims the plan lists; null when it has none. */
    private static String currentName(CopyClosure.Result view, Cid subject) {
        final CopyClosure.ClaimEntry named = view.claims.find { CopyClosure.ClaimEntry c ->
            c.current && c.subject == subject && c.attribute == Claim.NAME && c.verb == Claim.SET }
        return named == null ? null : String.valueOf(named.value)
    }

    private static String runName(CopyClosure.Result view, Cid completion) {
        return view.runs.find { CopyClosure.Run r -> r.completion == completion }?.runName
    }

    /** Counts of a set of entries against the destination. */
    @CompileStatic
    private static final class Tally {
        int blocks, present, transfer
        long bytes, presentBytes, transferBytes

        void add(CopyClosure.Entry e, Set<Cid> present, Set<Cid> missing) {
            final long size = e.size ?: 0L
            blocks++
            bytes += size
            if( present.contains(e.cid) ) {
                this.present++
                presentBytes += size
            }
            else if( !missing.contains(e.cid) ) {
                transfer++
                transferBytes += size
            }
        }
    }

    private static Tally tally(List<CopyClosure.Entry> entries, Set<Cid> present, Set<Cid> missing) {
        final Tally t = new Tally()
        for( CopyClosure.Entry e : entries )
            t.add(e, present, missing)
        return t
    }

    /** The 1.3 body, without the fields a real copy adds. */
    private static Map<String, Object> body(Ask ask, CopyClosure.Root root, String from, CopyClosure.Result view,
                                            Set<Cid> present, Set<Cid> missing, Map<String, Object> audit,
                                            List<Map<String, Object>> destination, boolean dryRun) {
        final Map<String, Object> body = new LinkedHashMap<String, Object>()
        final Map<String, Object> rootMap = new LinkedHashMap<String, Object>()
        rootMap.put('kind', root.kind)
        rootMap.put('cid', root.cid.toString())
        body.put('root', rootMap)
        body.put('from', from)
        body.put('to', ask.to)
        body.put('depth', ask.depth)
        body.put('claims', new ArrayList<String>(ask.claims))
        body.put('dry_run', dryRun)

        final List<CopyClosure.Entry> entries = view.entries
        final Tally total = tally(entries, present, missing)
        final Map<String, Object> plan = new LinkedHashMap<String, Object>()
        plan.put('blocks', total.blocks)
        plan.put('bytes', total.bytes)
        plan.put('present', total.present)
        plan.put('present_bytes', total.presentBytes)
        plan.put('transfer', total.transfer)
        plan.put('transfer_bytes', total.transferBytes)
        plan.put('unsized', view.totals.unsized)
        plan.put('missing_at_source', view.missingAtSource.size())
        plan.put('unaddressed', view.unaddressed)

        final List<Map<String, Object>> groups = new ArrayList<Map<String, Object>>()
        for( String group : CopyClosure.GROUPS ) {
            final Tally t = tally(entries.findAll { CopyClosure.Entry e -> e.group == group }, present, missing)
            final Map<String, Object> g = new LinkedHashMap<String, Object>()
            g.put('group', group)
            g.put('blocks', t.blocks)
            g.put('bytes', t.bytes)
            g.put('present', t.present)
            g.put('present_bytes', t.presentBytes)
            g.put('transfer', t.transfer)
            g.put('transfer_bytes', t.transferBytes)
            groups.add(g)
        }
        plan.put('groups', groups)

        final List<Map<String, Object>> runs = new ArrayList<Map<String, Object>>()
        for( CopyClosure.Run r : view.runs ) {
            final Map<String, Object> m = new LinkedHashMap<String, Object>()
            m.put('completion', r.completion.toString())
            m.put('manifest', r.manifest?.toString())
            m.put('script', r.script?.toString())
            m.put('run_name', r.runName)
            m.put('pipeline', r.pipeline)
            m.put('present', present.contains(r.completion))
            m.put('lineage', r.lineage)
            runs.add(m)
        }
        plan.put('runs', runs)

        final List<Map<String, Object>> claims = new ArrayList<Map<String, Object>>()
        for( CopyClosure.ClaimEntry c : view.claims ) {
            final Map<String, Object> m = new LinkedHashMap<String, Object>()
            m.put('cid', c.cid.toString())
            m.put('category', c.category)
            m.put('subject', c.subject?.toString())
            m.put('verb', c.verb)
            m.put('attribute', c.attribute)
            m.put('value', jsonable(c.value))
            m.put('asserted_by', c.assertedBy)
            m.put('timestamp', c.timestamp)
            m.put('present', present.contains(c.cid))
            m.put('supersedes', c.supersedes.collect { Cid s -> s.toString() })
            m.put('current', c.current)
            m.put('carried', c.carried)
            claims.add(m)
        }
        plan.put('claims', claims)
        plan.put('skipped_claims', new LinkedHashMap<String, Object>(view.skippedClaims))
        plan.put('audit', audit)
        plan.put('destination_state', destination)
        plan.put('kept', !destination.any { Map<String, Object> d -> d.get('state') in [RELEASED, DELETED] })
        body.put('plan', plan)
        body.put('refused', null)

        final Map<String, Object> depths = new LinkedHashMap<String, Object>()
        for( String d : CopyClosure.DEPTHS ) {
            final Tally t = tally(view.at(d).entries, present, missing)
            final Map<String, Object> m = new LinkedHashMap<String, Object>()
            m.put('blocks', t.blocks)
            m.put('bytes', t.bytes)
            m.put('transfer', t.transfer)
            m.put('transfer_bytes', t.transferBytes)
            depths.put(d, m)
        }
        body.put('depths', depths)
        body.put('command', command(ask, root, from, dryRun))
        return body
    }

    /** The equivalent CLI line. */
    private static String command(Ask ask, CopyClosure.Root root, String from, boolean dryRun) {
        final StringBuilder line = new StringBuilder('nextflow plugin nf-blocks:copy ')
        line << root.kind << ':' << root.cid.toString()
        line << ' --from ' << from << ' --to ' << ask.to << ' --depth ' << ask.depth
        line << ' --claims ' << (ask.claims ? ask.claims.join(',') : NO_CLAIMS)
        if( ask.acceptAudit )
            line << ' --accept-audit true'
        if( dryRun )
            line << ' --dry-run'
        return line.toString()
    }

    /** Cids as strings, the rest as JSON takes it. */
    private static Object jsonable(Object value) {
        if( value instanceof Cid )
            return value.toString()
        if( value instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map<?, ?>) value).entrySet() )
                out.put(String.valueOf(e.key), jsonable(e.value))
            return out
        }
        if( value instanceof Collection )
            return ((Collection) value).collect { Object v -> jsonable(v) }
        if( value instanceof byte[] )
            return Base64.encoder.encodeToString((byte[]) value)
        return value
    }

    // ---- the copy

    /**
     * Write order (1.5 and the T6 brief): content, DirectoryManifests, items,
     * collections, then the script and RunManifest, the RunCompletion, and
     * Selections, Projects, Handoffs and Claims last, so a parent tends to
     * land after what it names. Within a step the blocks go 16 at a time.
     */
    private static int step(CopyClosure.Entry e) {
        switch( e.kind ) {
            case Records.DIRECTORY_MANIFEST: return 1
            case Records.OUTPUT_ITEM: return 2
            case Records.OUTPUT_COLLECTION: return 3
            case Records.RUN_MANIFEST: return 4
            case Records.RUN_COMPLETION: return 5
            case Records.SELECTION:
            case Records.PROJECT:
            case Records.HANDOFF:
            case Records.CLAIM: return 6
            // The script (raw, in run_records) goes with its RunManifest.
            default: return e.group == 'run_records' ? 4 : 0
        }
    }
    private static final int STEPS = 7

    private Outcome copy(CasSession cas, Index index, SnapshotBase base, BlockStore source, Ask ask, CopyClosure.Root root,
                         CopyClosure.Result view, Set<Cid> present, Set<Cid> missing, Map<String, Object> body,
                         ExecutorService pool, PrintStream err, Map<String, String> roots, boolean notIndexed) {
        final BlockStore dest = cas.members()[0]
        final List<CopyClosure.Entry> entries = view.entries
        final List<List<CopyClosure.Entry>> steps = new ArrayList<List<CopyClosure.Entry>>()
        for( int i = 0; i < STEPS; i++ )
            steps.add(new ArrayList<CopyClosure.Entry>())
        // Reversed, so a nested Selection or DirectoryManifest is submitted before the one naming it;
        // within a step the puts run 16 at a time, in no order, and only the steps are ordered.
        for( int i = entries.size() - 1; i >= 0; i-- ) {
            final CopyClosure.Entry e = entries[i]
            if( !present.contains(e.cid) && !missing.contains(e.cid) )
                steps[step(e)].add(e)
        }

        final AtomicInteger copied = new AtomicInteger()
        final AtomicLong bytes = new AtomicLong()
        final AtomicBoolean stop = new AtomicBoolean(false)
        final List<Map<String, Object>> failures = Collections.synchronizedList(new ArrayList<Map<String, Object>>())
        for( List<CopyClosure.Entry> batch : steps ) {
            final List<Future<?>> pending = new ArrayList<Future<?>>()
            for( CopyClosure.Entry e : batch ) {
                final CopyClosure.Entry entry = e
                pending.add(pool.submit({ ->
                    if( stop.get() )
                        return
                    try {
                        final Counting input = new Counting(source.open(entry.cid))
                        try {
                            dest.put(entry.cid, input, entry.size != null ? entry.size : -1L)
                        }
                        finally {
                            input.close()
                        }
                        copied.incrementAndGet()
                        bytes.addAndGet(input.count)
                    }
                    catch( Exception x ) {
                        stop.set(true)
                        final Map<String, Object> f = new LinkedHashMap<String, Object>()
                        f.put('cid', entry.cid.toString())
                        f.put('error', CopyErrors.describe(x, roots))
                        failures.add(f)
                    }
                } as Runnable))
            }
            for( Future<?> f : pending )
                await(f)
            if( stop.get() )
                break
        }
        body.put('copied', copied.get())
        body.put('copied_bytes', bytes.get())
        if( failures ) {
            body.put('transfer_entry', null)
            body.put('failures', new ArrayList<Map<String, Object>>(failures))
            body.put('snapshot', null)
            return new Outcome(FAILED, body)
        }

        final Principal given = signer?.get()
        final String assertedBy = given != null ? given.name : cas.assertedBy
        final Cid transfer = announce(dest, root, view, present, missing, ask, source.alias(), assertedBy, copied.get(), bytes.get())
        body.put('transfer_entry', transfer?.toString())
        body.put('failures', new ArrayList<Map<String, Object>>())
        if( notIndexed ) {
            err.println("nf-blocks:copy: '${ask.to}' has no Index Snapshot, so this copy did not write one; index it from the explorer's Storage page")
            body.put('snapshot', SNAPSHOT_NOT_INDEXED)
        }
        else if( transfer == null ) {
            // Nothing was written, so the destination's snapshot already says all there is.
            err.println("nf-blocks:copy: '${ask.to}' already holds every block and entry of this copy; no Transfer was written and its Index Snapshot stands")
            body.put('snapshot', SNAPSHOT_SKIPPED)
        }
        else
            body.put('snapshot', snapshot(cas, index, base, err, roots))
        return new Outcome(OK, body)
    }

    /**
     * The ordinary entries the destination log lacks, then the Transfer block
     * and its entry, last (1.5). Each entry is stamped after the one before,
     * starting after the newest entry already in the log, so the Transfer's
     * sorts newest. Null, with nothing written, when the copy wrote no block
     * and the log lacks no entry: a Transfer would record nothing.
     */
    private Cid announce(BlockStore dest, CopyClosure.Root root, CopyClosure.Result view, Set<Cid> present, Set<Cid> missing,
                         Ask ask, String from, String assertedBy, long copied, long bytes) {
        final StoreLog log = StoreLog.of(dest)
        final Set<String> logged = new HashSet<String>()
        long last = 0L
        for( StoreLogEntry e : log.read() ) {
            logged.add(e.kind.token + '-' + e.cid)
            last = Math.max(last, e.writtenAtMillis)
        }
        final List<Object[]> lacking = lacking(view, present, missing, logged)
        if( copied == 0 && lacking.isEmpty() )
            return null
        for( Object[] w : lacking ) {
            last = stamp(last)
            log.append((StoreLogKind) w[0], (Cid) w[1], last)
        }
        final long at = stamp(last)
        // D8: the runs whose records the copy carried; one whose RunCompletion the source lacked is not one.
        final List<Cid> runs = view.runs.findAll { CopyClosure.Run r -> !missing.contains(r.completion) }
            .collect { CopyClosure.Run r -> r.completion }
        final Transfer t = new Transfer(assertedBy, [root.cid], from, ask.to, ask.depth, ask.claims,
            runs, view.entries.size(), copied, bytes, Index.isoMillis(at))
        final Cid cid = dest.putDagCbor(t.toCbor())
        log.append(StoreLogKind.TRANSFER, cid, at)
        return cid
    }

    /**
     * False when a copy would write nothing: every entry the source has is
     * at the destination, and its log lacks none of the ordinary entries.
     * Then announce would answer null, so no Live Writer is needed.
     */
    private static boolean anythingToWrite(BlockStore dest, CopyClosure.Result view, Set<Cid> present, Set<Cid> missing) {
        for( CopyClosure.Entry e : view.entries )
            if( !present.contains(e.cid) && !missing.contains(e.cid) )
                return true
        final Set<String> logged = new HashSet<String>()
        for( StoreLogEntry e : StoreLog.of(dest).read() )
            logged.add(e.kind.token + '-' + e.cid)
        return !lacking(view, present, missing, logged).isEmpty()
    }

    /** The ordinary entries (run, Selection, Project, Handoff, carried Claim) the copy puts at the destination that {@code logged} lacks; adds them to it. */
    private static List<Object[]> lacking(CopyClosure.Result view, Set<Cid> present, Set<Cid> missing, Set<String> logged) {
        final List<Object[]> wanted = new ArrayList<Object[]>()
        for( CopyClosure.Run r : view.runs )
            wanted.add([StoreLogKind.RUN, r.completion] as Object[])
        for( CopyClosure.Entry e : view.entries )
            if( e.kind == Records.SELECTION )
                wanted.add([StoreLogKind.SELECTION, e.cid] as Object[])
            else if( e.kind == Records.PROJECT )
                wanted.add([StoreLogKind.PROJECT, e.cid] as Object[])
            else if( e.kind == Records.HANDOFF )
                wanted.add([StoreLogKind.HANDOFF, e.cid] as Object[])
        for( CopyClosure.ClaimEntry c : view.claims )
            if( c.carried )
                wanted.add([StoreLogKind.CLAIM, c.cid] as Object[])
        final List<Object[]> lacking = new ArrayList<Object[]>()
        for( Object[] w : wanted ) {
            final Cid cid = (Cid) w[1]
            // A block the source lacked was not copied; an entry for it would announce nothing.
            if( missing.contains(cid) && !present.contains(cid) )
                continue
            if( logged.add(((StoreLogKind) w[0]).token + '-' + cid) )
                lacking.add(w)
        }
        return lacking
    }

    private long stamp(long last) {
        return Math.max(clock.call(), last + 1)
    }

    static final String SNAPSHOT_WRITTEN = 'written'
    static final String SNAPSHOT_SKIPPED = 'skipped'
    /** The destination is a member the explorer has not indexed, so the copy writes it no snapshot. */
    static final String SNAPSHOT_NOT_INDEXED = 'not_indexed'

    /**
     * "written", "skipped" (the reason goes to err) or "failed: <message>"
     * (1.3). Derived (DESIGN.md §0 rule 3): the copy has succeeded whatever this says.
     */
    private static String snapshot(CasSession cas, Index index, SnapshotBase base, PrintStream err, Map<String, String> roots) {
        try {
            final Set<String> failed = cas.catchUpIndex(index)
            final IndexSnapshot.Result r = cas.snapshotWritable(index, 0L, base, failed)
            if( !r.skipped )
                return SNAPSHOT_WRITTEN
            err.println("nf-blocks:copy: the Index Snapshot of '${cas.config.writableAlias}' was not rewritten: ${r.skipped}${IndexSnapshot.skipAdvice(r)}")
            return SNAPSHOT_SKIPPED
        }
        catch( Exception e ) {
            log.debug('nf-blocks:copy: snapshot failed', e)
            final String why = CopyErrors.describe(e, roots)
            err.println("nf-blocks:copy: the copy is done, but the Index Snapshot of '${cas.config.writableAlias}' was not rewritten: ${why}; run nf-blocks:snapshot")
            return "failed: ${why}".toString()
        }
    }

    /** Counts what the put reads, so copied_bytes is what crossed. */
    @CompileStatic
    private static final class Counting extends FilterInputStream {
        long count

        Counting(InputStream input) { super(input) }

        @Override
        int read() {
            final int b = super.read()
            if( b >= 0 ) count++
            return b
        }

        @Override
        int read(byte[] buffer, int offset, int length) {
            final int n = super.read(buffer, offset, length)
            if( n > 0 ) count += n
            return n
        }
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.get()
        }
        catch( ExecutionException e ) {
            final Throwable cause = e.getCause()
            if( cause instanceof RuntimeException )
                throw (RuntimeException) cause
            if( cause instanceof Error )
                throw (Error) cause
            throw new UncheckedIOException(cause instanceof IOException ? (IOException) cause : new IOException(cause))
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
            throw new RuntimeException(e)
        }
    }
}
