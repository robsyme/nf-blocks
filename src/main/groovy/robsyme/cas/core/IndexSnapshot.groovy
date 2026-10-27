package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.SecureRandom
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Savepoint
import java.sql.Statement
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicReference

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j

/**
 * A member's Index Snapshot (DESIGN.md §15, block explorer spec section 4):
 * the member's own rows, copied out of the per-user cache index by SQL into
 * `<member>/index/v<N>.sqlite`, so a static page can read them with HTTP range
 * requests. Derived and rebuildable; never a source of truth.
 */
@Slf4j
@CompileStatic
class IndexSnapshot {

    static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024
    static final int PAGE_SIZE = 4096
    static final String WATERMARK_KEY = 'store_log_watermark'
    static final String STORE_ID_KEY = 'store_id'
    static final String WRITTEN_AT_KEY = 'snapshot_written_at'
    /** `cas.sweep.grace` in milliseconds, when the writer knows it (P2 ruling R7). */
    static final String SWEEP_GRACE_KEY = 'sweep_grace_ms'
    /**
     * Why a lean snapshot has no storage figures: OVER_CAP, OUT_OF_MEMORY or SEEDED_LEAN
     * (fix round 2, B2). A lean snapshot keeps the member's records and
     * leaves the content edges (block, block_ref) and every storage table
     * empty; the page falls back to its own walks.
     */
    static final String STORAGE_OMITTED_KEY = 'storage_omitted'
    /** The cas.snapshot.maxBytes in force, in a lean snapshot's meta, as decimal text; absent when no cap is in force. */
    static final String MAX_BYTES_KEY = 'snapshot_max_bytes'
    static final String PAGE_NAME = 'index.html'
    static final String PAGE_RESOURCE = '/robsyme/cas/explorer/index.html'

    /** Why a write kept the old snapshot (Result.skipped). */
    static final String OVER_CAP = 'over_cap'
    static final String FEWER_RUNS = 'fewer_runs'
    static final String REPLACED_MEANWHILE = 'replaced_meanwhile'
    static final String CATCH_UP_FAILED = 'catch_up_failed'
    static final String STALE_STORE = 'stale_store'
    static final String OLDER_WATERMARK = 'older_watermark'
    /**
     * The cache was seeded from a lean snapshot, so it can only write lean
     * ones, and the snapshot there has storage figures: a lean one does not
     * replace it (fix round 3, item 3). The run is still recorded.
     */
    static final String SEEDED_LEAN_BASE_FULL = 'seeded_lean_base_full'
    /**
     * As SEEDED_LEAN_BASE_FULL, when the snapshot there does not say whether
     * it has storage figures and could not be fetched to tell: a transient
     * error never replaces a full snapshot with a lean one.
     */
    static final String SEEDED_LEAN_BASE_UNKNOWN = 'seeded_lean_base_unknown'
    /** What every message about running out of heap advises (fix round 3, item 8). */
    static final String MORE_HEAP = 'give the JVM more heap, for example NXF_OPTS=-Xmx4g'
    /** What a lean-seeded cache needs, in every message that meets one. */
    static final String REINDEX_NO_SEED = '`nextflow plugin nf-blocks:reindex --seed false`'
    /** Why a written snapshot is lean (Result.omitted): computing its storage figures ran out of memory. Also OVER_CAP. */
    static final String OUT_OF_MEMORY = 'out_of_memory'
    /** Why a written snapshot is lean: the cache was seeded from a lean snapshot, so it has no content edges to count. */
    static final String SEEDED_LEAN = 'seeded_lean'

    /**
     * The size estimate made before the content is copied: L + refs x m. L is
     * the lean part, measured in the same build (its pages so far), so it is
     * never stale. m is the marginal cost per block_ref row of the content
     * and storage tables: 389 B on roblab (v4 1,744,896 B less v3 954,368 B
     * over 2,030 references) and about 385 B on gen_year.py's year, so it
     * starts at 400 B. Each full build of at least MIN_LEARN_REFS references
     * replaces it with (full size - L) / refs for that member; fewer than
     * that, and page granularity drowns the figure.
     */
    static final long DEFAULT_BYTES_PER_REF = 400L
    static final long MIN_LEARN_REFS = 1_000L

    /** Cache meta keys the build keeps per member, each suffixed with ':' and the member's alias. */
    static final String BYTES_PER_REF_KEY = 'snapshot_bytes_per_ref'
    /** "cap bytes runs omitted" of a lean snapshot that was still over the cap: later writes skip the build while it holds. */
    static final String OVER_CAP_VERDICT_KEY = 'snapshot_over_cap'
    /** "refs heap" of a build whose storage figures ran out of memory: at as many references and no more heap, go lean at once. */
    static final String OUT_OF_MEMORY_KEY = 'snapshot_out_of_memory'
    /** Set when this cache was seeded from a lean snapshot (Index.seedFrom; the value is that snapshot's storage_omitted, for the record). */
    static final String SEEDED_LEAN_KEY = 'snapshot_seeded_lean'
    /**
     * "<tag> true|false": whether the base with that tag (an S3 ETag) has
     * storage figures, read once from a base that does not say (an S3 object
     * written before x-amz-meta-storage), so it is not fetched at every write.
     */
    static final String BASE_FIGURES_KEY = 'snapshot_base_figures'

    /** What a lean snapshot leaves empty: the content edges and every table writeStorage fills. */
    private static final List<String> STORAGE_TABLES = ['root', 'root_stats', 'root_shared', 'store_stats', 'asserter_stats',
                                                        'pipeline_stats', 'collection_size', 'item_size', 'kind_stats', 'pinned_overlap',
                                                        'project_stats']

    /**
     * The current membership Claims (P4 plan 1.2) of the whole composition,
     * from the cache's claim_current, not the member's own Claims: a
     * project's storage here counts whichever member logged the membership.
     * A `del project` that superseded an `add` leaves no `add` row current.
     */
    private static final String MEMBERSHIPS = '''
        SELECT DISTINCT cc.subject_cid, cc.value FROM src.claim_current cc JOIN src.claim k ON k.claim_cid = cc.claim_cid
        WHERE cc.attribute = 'project' AND k.verb = 'add' AND cc.value IS NOT NULL'''

    private static final SecureRandom RANDOM = new SecureRandom()
    private static final DateTimeFormatter MILLIS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    /**
     * The member's runs: those its Store Log announced, and those its blocks
     * held when the index first scanned it (a store written before the Store
     * Log, DESIGN.md §12). Alias written as NULL, since an alias is a local label.
     */
    private static final String COPY_RUNS = '''
        INSERT INTO main.run
        SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
               run_name, asserted_by, status, possibly_incomplete, finished_at, NULL
        FROM src.run
        WHERE member = ? OR completion_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'run')'''

    /** The member's own Store Log rows. */
    private static final String COPY_LOG_ENTRIES =
        'INSERT INTO main.log_entry SELECT cid, kind, NULL, written_at FROM src.log_entry WHERE member = ?'

    /** The Selections the member logged, after the run closure so item and item_attr stay the runs' (spec section 4). */
    private static final List<String> COPY_SELECTIONS = [
        "INSERT INTO main.collection SELECT * FROM src.collection WHERE kind = 'selection' AND collection_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'selection')",
        "INSERT INTO main.collection_item SELECT * FROM src.collection_item WHERE collection_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
        "INSERT INTO main.selection_child SELECT * FROM src.selection_child WHERE parent_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
        "INSERT INTO main.selection_derived SELECT * FROM src.selection_derived WHERE selection_cid IN (SELECT collection_cid FROM main.collection WHERE kind = 'selection')",
    ]

    /** The Claims the member's Store Log announced; current state is recomputed from these alone (spec section 4). */
    private static final List<String> COPY_CLAIMS = [
        "INSERT INTO main.claim SELECT * FROM src.claim WHERE claim_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'claim')",
        'INSERT INTO main.claim_supersedes SELECT * FROM src.claim_supersedes WHERE claim_cid IN (SELECT claim_cid FROM main.claim)',
    ]

    /** The Transfers the member's Store Log announced (P3 plan 1.5), with their roots and runs. */
    private static final List<String> COPY_TRANSFERS = [
        "INSERT INTO main.transfer SELECT * FROM src.transfer WHERE cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'transfer')",
        'INSERT INTO main.transfer_root SELECT * FROM src.transfer_root WHERE transfer_cid IN (SELECT cid FROM main.transfer)',
    ]

    /**
     * The Projects and Handoffs the member's Store Log announced, and the
     * inputs of the runs copied (P4 plan 1.3-1.5), with their input origins and
     * item lineage. Their consumer rows are in COPY_CLOSURE.
     */
    private static final List<String> COPY_P4 = [
        "INSERT INTO main.project SELECT * FROM src.project WHERE project_cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'project')",
        "INSERT INTO main.handoff SELECT * FROM src.handoff WHERE cid IN (SELECT cid FROM src.log_entry WHERE member = ? AND kind = 'handoff')",
        'INSERT INTO main.run_input SELECT * FROM src.run_input WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.input_origin SELECT * FROM src.input_origin WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.item_input SELECT * FROM src.item_input WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.item_task SELECT * FROM src.item_task WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
    ]

    /**
     * The copied runs' InputSet wait records (Index.ingestInputSet), so a
     * cache seeded from the snapshot still waits for an InputSet that had
     * not arrived. In every snapshot: meta is in every cache.
     */
    private static final String COPY_INPUTSET_WAITS = """INSERT INTO main.meta(key, value)
        SELECT key, value FROM src.meta WHERE key GLOB '${Index.INPUTSET_WAIT}*'
          AND substr(key, ${Index.INPUTSET_WAIT.length() + 1}) IN (SELECT completion_cid FROM main.run)""".toString()

    /** The copied runs' ItemLineage wait records, as COPY_INPUTSET_WAITS does for InputSets. */
    private static final String COPY_ITEMLINEAGE_WAITS = """INSERT INTO main.meta(key, value)
        SELECT key, value FROM src.meta WHERE key GLOB '${Index.ITEMLINEAGE_WAIT}*'
          AND substr(key, ${Index.ITEMLINEAGE_WAIT.length() + 1}) IN (SELECT completion_cid FROM main.run)""".toString()

    /** Everything those runs reach, in dependency order. */
    private static final List<String> COPY_CLOSURE = [
        '''INSERT INTO main.collection SELECT * FROM src.collection
           WHERE kind = 'output' AND completion_cid IN (SELECT completion_cid FROM main.run)''',
        'INSERT INTO main.collection_item SELECT * FROM src.collection_item WHERE collection_cid IN (SELECT collection_cid FROM main.collection)',
        'INSERT INTO main.item SELECT DISTINCT item_cid FROM main.collection_item',
        'INSERT INTO main.producer SELECT * FROM src.producer WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        'INSERT INTO main.item_attr SELECT * FROM src.item_attr WHERE item_cid IN (SELECT item_cid FROM main.item)',
        'INSERT INTO main.consumer SELECT * FROM src.consumer WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
        '''INSERT INTO main.missing SELECT * FROM src.missing
           WHERE have_cid IN (SELECT completion_cid FROM main.run UNION SELECT collection_cid FROM main.collection)''',
    ]

    /**
     * The content the member's items reach (schema 4): the block_ref rows of
     * every item of its runs and Selections, of each Handoff it logged (its
     * files, P4 R5), and of every DirectoryManifest reachable from one, the
     * block rows they name, and the
     * `missing(owner, manifest)` rows of a directory that had not arrived, so
     * a cache seeded from here retries it as a full ingest would. A Selection
     * item held by another member counts here (P2 ruling R8). After
     * COPY_SELECTIONS, which fills the last of main.collection_item, and
     * COPY_P4, which fills main.handoff. The
     * CROSS JOIN keeps owner as the outer loop, so the cache's block_ref,
     * which holds every member's rows, is searched by owner and never scanned.
     */
    @PackageScope static final List<String> COPY_CONTENT = [
        '''INSERT INTO main.block_ref
           WITH RECURSIVE owner(cid) AS (
               SELECT item_cid FROM main.collection_item
               UNION
               SELECT cid FROM main.handoff
               UNION
               SELECT r.cid FROM src.block_ref r JOIN owner o ON r.owner_cid = o.cid
               JOIN src.block b ON b.cid = r.cid AND b.codec = 'dag-cbor')
           SELECT r.owner_cid, r.cid, r.name FROM owner o CROSS JOIN src.block_ref r ON r.owner_cid = o.cid''',
        'INSERT INTO main.block SELECT * FROM src.block WHERE cid IN (SELECT cid FROM main.block_ref)',
        'INSERT INTO main.missing SELECT * FROM src.missing WHERE have_cid IN (SELECT owner_cid FROM main.block_ref)',
    ]

    /** What one write did. */
    @CompileStatic
    static final class Result {
        final boolean written
        final Path path
        final long bytes
        final int runs
        final String watermark
        /**
         * Null when written; otherwise the guard that kept the old snapshot (OVER_CAP, STALE_STORE, SEEDED_LEAN_BASE_FULL,
         * SEEDED_LEAN_BASE_UNKNOWN, OLDER_WATERMARK, FEWER_RUNS, REPLACED_MEANWHILE, CATCH_UP_FAILED).
         */
        final String skipped
        /** Null for a snapshot with its storage figures; OVER_CAP, OUT_OF_MEMORY or SEEDED_LEAN for a lean one. */
        final String omitted
        /** True when it is lean because the cache was seeded from a lean snapshot: only `reindex --seed false` brings the figures back. */
        final boolean seededLean
        /**
         * True when it is lean (OUT_OF_MEMORY) because the Java heap ran out,
         * in this build or an earlier one the cache remembers, so more heap
         * brings the figures back. False for another OutOfMemoryError.
         */
        final boolean heapExhausted

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped) {
            this(written, path, bytes, runs, watermark, skipped, null)
        }

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped, String omitted) {
            this(written, path, bytes, runs, watermark, skipped, omitted, false)
        }

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped, String omitted, boolean seededLean) {
            this(written, path, bytes, runs, watermark, skipped, omitted, seededLean, false)
        }

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped, String omitted, boolean seededLean,
               boolean heapExhausted) {
            this.heapExhausted = heapExhausted
            this.written = written
            this.path = path
            this.bytes = bytes
            this.runs = runs
            this.watermark = watermark
            this.skipped = skipped
            this.omitted = omitted
            this.seededLean = seededLean
        }

        @Override
        String toString() { "IndexSnapshot.Result[written=$written, path=$path, bytes=$bytes, runs=$runs, skipped=$skipped, omitted=$omitted, seededLean=$seededLean, heapExhausted=$heapExhausted]" }
    }

    static String relativePath() { "index/v${Index.SCHEMA_VERSION}.sqlite" }

    static Path pathIn(Path memberRoot) { memberRoot.resolve(relativePath()) }

    @CompileStatic
    static final class Built {
        final Path file; final long bytes; final int runs; final String watermark; final String storeId
        /** Null, or why the snapshot is lean (OVER_CAP, OUT_OF_MEMORY, SEEDED_LEAN). */
        final String omitted
        /** Lean because the cache was seeded from a lean snapshot. */
        final boolean seededLean
        /** Lean because the Java heap ran out, now or in a build the cache remembers. */
        final boolean heapExhausted
        Built(Path file, long bytes, int runs, String watermark, String storeId, String omitted, boolean seededLean) {
            this(file, bytes, runs, watermark, storeId, omitted, seededLean, false)
        }
        Built(Path file, long bytes, int runs, String watermark, String storeId, String omitted, boolean seededLean, boolean heapExhausted) {
            this.file = file; this.bytes = bytes; this.runs = runs; this.watermark = watermark; this.storeId = storeId; this.omitted = omitted
            this.seededLean = seededLean
            this.heapExhausted = heapExhausted
        }
    }

    /** What one build did, for the meta the cache learns from it. */
    @CompileStatic
    private static final class Shape {
        int runs
        String omitted
        /** True when writeStorage itself ran out of heap (heapExhausted), not a note from an earlier build; remembered in the cache. */
        boolean ranOut
        /** True when writeStorage itself ran out of memory in this build, heap or not. */
        boolean oomNow
        /** True when lean because the cache was seeded from a lean snapshot. */
        boolean seeded
        /** block_ref rows of the member's closure, or -1 when they were never counted. */
        long refs = -1L
        /** The lean part's size, measured before the content was copied. */
        long leanBytes
        /** The vacuumed size with storage figures, or -1 when none was made. */
        long fullBytes = -1L
    }

    /** What a build knows before it starts: the cap and the cache's notes on the member. */
    @CompileStatic
    private static final class Plan {
        final long maxBytes
        final long bytesPerRef
        /** True when the cache was seeded from a lean snapshot of the member (its content edges are missing). */
        final boolean seededLean
        /** References at which an earlier build ran out of memory, and the heap it had; -1 for none. */
        final long oomRefs
        final long oomHeap

        Plan(Index index, String member, long maxBytes) {
            this.maxBytes = maxBytes
            this.bytesPerRef = positive(index.metaValue(BYTES_PER_REF_KEY + ':' + member), DEFAULT_BYTES_PER_REF)
            this.seededLean = index.metaValue(SEEDED_LEAN_KEY + ':' + member) != null
            final String[] oom = (index.metaValue(OUT_OF_MEMORY_KEY + ':' + member) ?: '').split(' ')
            this.oomRefs = oom.length == 2 ? positive(oom[0], -1L) : -1L
            this.oomHeap = oom.length == 2 ? positive(oom[1], -1L) : -1L
        }

        /**
         * Why to go lean once the lean part is built and the references
         * counted, or null to build the figures. The cap comes first: under
         * it, more heap would not bring the figures back.
         */
        String leanBefore(long leanBytes, long refs) {
            if( maxBytes > 0 && leanBytes + refs * bytesPerRef > maxBytes )
                return OVER_CAP
            if( oomRefs >= 0 && refs >= oomRefs && Runtime.runtime.maxMemory() <= oomHeap )
                return OUT_OF_MEMORY
            return null
        }

        private static long positive(String text, long otherwise) {
            try {
                final long n = text == null ? -1L : Long.parseLong(text.trim())
                return n > 0 ? n : otherwise
            }
            catch( NumberFormatException e ) {
                return otherwise
            }
        }
    }

    /** As the 4-arg build, without a sweep grace in the meta. */
    static Built build(Index index, String member, Path tempDir) {
        return build(index, member, tempDir, null)
    }

    /** As the 5-arg build, with no presence check. */
    static Built build(Index index, String member, Path tempDir, Long sweepGraceMillis) {
        return build(index, member, tempDir, sweepGraceMillis, null)
    }

    /** As the 6-arg build, with no cap: the build is lean only when its storage figures run out of memory. */
    static Built build(Index index, String member, Path tempDir, Long sweepGraceMillis, Closure<Boolean> present) {
        return build(index, member, tempDir, sweepGraceMillis, present, 0L)
    }

    /**
     * The member's snapshot as a local file in tempDir; the caller deletes it.
     * A non-null sweepGraceMillis goes in the meta. {@code present} answers
     * whether a block (a {@link Cid}) is in the member's store: blocks only
     * released or deleted roots reach are looked up with it, and one that is
     * gone is left out of the storage figures (fix round 2, B1). Null checks
     * nothing.
     *
     * The snapshot is lean (fix round 2, B2: records only, meta
     * storage_omitted) when the estimate with storage figures passes
     * {@code maxBytes} (> 0), when the full build passes it after all, when
     * computing the figures runs out of memory or did before at as many
     * references and no more heap, or when the cache was seeded from a lean
     * snapshot. Each full build of enough references teaches the cache the
     * member's bytes per reference.
     *
     * This overload has no sweep lock to read, so it records no presence
     * (see the 7-arg build).
     */
    static Built build(Index index, String member, Path tempDir, Long sweepGraceMillis, Closure<Boolean> present, long maxBytes) {
        return build(index, member, tempDir, sweepGraceMillis, present, maxBytes, null)
    }

    /**
     * As the 6-arg build. {@code sweepLock} is the member's retention storage,
     * whose sweep.lock the build reads before it records a block present; null
     * counts as a lock that may be held, so nothing is recorded present.
     */
    static Built build(Index index, String member, Path tempDir, Long sweepGraceMillis, Closure<Boolean> present, long maxBytes,
                       RetentionStorage sweepLock) {
        Files.createDirectories(tempDir)
        final String token = token()
        final Path buildFile = tempDir.resolve(".tmp-${token}.build")
        final Path vacuumed = tempDir.resolve("nf-blocks-snapshot-${token}.sqlite")
        try {
            final String watermark = index.watermark(member)
            final String storeId = index.storeIdOf(member)
            final Presence presence = present == null ? null : new Presence(index, member, present, sweepLock)
            final Shape shape = buildAndVacuum(buildFile, vacuumed, index.file, member, watermark, storeId, sweepGraceMillis, presence,
                new Plan(index, member, maxBytes))
            learn(index, member, shape)
            final boolean heap = shape.omitted == OUT_OF_MEMORY && (shape.ranOut || !shape.oomNow)
            return new Built(vacuumed, Files.size(vacuumed), shape.runs, watermark, storeId, shape.omitted, shape.seeded, heap)
        }
        catch( Throwable e ) {
            Files.deleteIfExists(vacuumed)
            throw e
        }
        finally {
            for( String suffix : ['', '-journal', '-wal', '-shm'] )
                Files.deleteIfExists(buildFile.resolveSibling(buildFile.fileName.toString() + suffix))
        }
    }

    /** As the 7-argument write, without a store-id check. */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir) {
        return write(index, member, storage, maxBytes, base, tempDir, null)
    }

    /** As the 8-argument write, guarding against base as usual (not a rebuild). */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir, String memberStoreId) {
        return write(index, member, storage, maxBytes, base, tempDir, memberStoreId, false)
    }

    /**
     * Writes the member's snapshot into storage unless a guard keeps the old
     * one (DESIGN.md §15, ticket 04 decision 3, §20 decision 10), in this
     * order: the cap on base (maxBytes > 0); a cache that does not hold
     * memberStoreId (when given) for the member; the cap on the build; a base
     * whose watermark is newer than the build's; fewer run rows than base once
     * the runs forget entries removed since base are added back; another
     * writer's version in place of base.
     *
     * The watermark and run-count guards are skipped for a base written for
     * another incarnation of the member (its store id is not memberStoreId),
     * and when {@code rebuild} is set: an operator's `reindex`, or a sweep
     * whose deletes no forget entry announces, whose snapshot has fewer runs
     * than base because those runs are really gone.
     *
     * With a cap, the build may be lean (see the 6-arg build). A lean build
     * still over the cap is remembered in the cache with the cap and the run
     * count, and later writes at the same cap return OVER_CAP without
     * building until the member has fewer runs (fix round 2, B2). A rebuild
     * always builds.
     */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir, String memberStoreId, boolean rebuild) {
        return write(index, member, storage, maxBytes, base, tempDir, memberStoreId, rebuild, null)
    }

    /** As the 8-argument write, with {@code sweepGraceMillis} (`cas.sweep.grace`) written to the meta when not null. */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir,
                        String memberStoreId, boolean rebuild, Long sweepGraceMillis) {
        return write(index, member, storage, maxBytes, base, tempDir, memberStoreId, rebuild, sweepGraceMillis, null)
    }

    /** As the 9-argument write, with {@code present} checking blocks a sweep may have taken (see the 5-arg build); no presence is recorded. */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir,
                        String memberStoreId, boolean rebuild, Long sweepGraceMillis, Closure<Boolean> present) {
        return write(index, member, storage, maxBytes, base, tempDir, memberStoreId, rebuild, sweepGraceMillis, present, null)
    }

    /** As the 10-argument write, reading {@code sweepLock} before recording a presence (see the 7-arg build). */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir,
                        String memberStoreId, boolean rebuild, Long sweepGraceMillis, Closure<Boolean> present, RetentionStorage sweepLock) {
        if( maxBytes > 0 && base != null && base.bytes >= maxBytes )
            // No build, but a lean-seeded cache still needs saying: only `reindex --seed false` brings its figures back.
            return new Result(false, null, base.bytes, -1, null, OVER_CAP, null, index.metaValue(SEEDED_LEAN_KEY + ':' + member) != null)
        if( memberStoreId != null && index.storeIdOf(member) != memberStoreId )
            return new Result(false, null, 0L, -1, index.watermark(member), STALE_STORE)
        final boolean otherIncarnation = memberStoreId != null && base?.storeId != null && base.storeId != memberStoreId
        final boolean judgeBase = base != null && !rebuild && !otherIncarnation
        // A lean-seeded cache only writes lean snapshots: it must not replace one with figures, even on a rebuild.
        // A base of another incarnation describes a store that is gone, so its figures are not worth keeping.
        if( base != null && !otherIncarnation && index.metaValue(SEEDED_LEAN_KEY + ':' + member) != null ) {
            final Boolean figures = baseHasFigures(index, member, storage, base, tempDir)
            if( figures == null || figures )
                return new Result(false, null, base.bytes, base.runs, index.watermark(member),
                    figures == null ? SEEDED_LEAN_BASE_UNKNOWN : SEEDED_LEAN_BASE_FULL, SEEDED_LEAN, true)
        }
        final String verdictKey = OVER_CAP_VERDICT_KEY + ':' + member
        if( maxBytes > 0 && !rebuild ) {
            final String[] verdict = overCapVerdict(index.metaValue(verdictKey))
            // Records only grow a lean snapshot, so one over this cap stays over until the member loses runs.
            if( verdict != null && verdict[0] == String.valueOf(maxBytes) && index.memberRunCount(member) >= (verdict[2] as int) )
                return new Result(false, null, verdict[1] as long, verdict[2] as int, index.watermark(member), OVER_CAP, verdict[3], verdict[3] == SEEDED_LEAN)
        }
        final Built built = build(index, member, tempDir, sweepGraceMillis, present, maxBytes, sweepLock)
        try {
            if( maxBytes > 0 && built.bytes > maxBytes ) {
                // Over the cap even without storage figures: remember it, so later runs skip the build.
                index.setMetaValue(verdictKey, "${maxBytes} ${built.bytes} ${built.runs} ${built.omitted}".toString())
                return new Result(false, null, built.bytes, built.runs, built.watermark, OVER_CAP, built.omitted, built.seededLean)
            }
            index.setMetaValue(verdictKey, null)
            if( judgeBase && newer(base.watermark, built.watermark) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, OLDER_WATERMARK, built.omitted)
            if( judgeBase && base.runs > built.runs + index.forgottenRunsSince(member, base.watermark) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, FEWER_RUNS, built.omitted)
            final SnapshotBase written = new SnapshotBase(null, built.bytes, built.runs, built.storeId, built.watermark, built.omitted == null)
            if( !storage.replace(built.file, written, base) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, REPLACED_MEANWHILE, built.omitted)
            return new Result(true, null, built.bytes, built.runs, built.watermark, null, built.omitted, built.seededLean, built.heapExhausted)
        }
        finally {
            Files.deleteIfExists(built.file)
        }
    }

    /**
     * True for an OutOfMemoryError of the Java heap ("Java heap space", "GC
     * overhead limit exceeded"), which more heap cures. Others (no native
     * thread, metaspace, direct buffers) are not remembered by the build.
     */
    static boolean heapExhausted(Throwable e) {
        if( !(e instanceof OutOfMemoryError) )
            return false
        final String m = e.message ?: ''
        return m.contains('Java heap space') || m.contains('GC overhead')
    }

    /** " (give the JVM more heap, ...)" after a heap OutOfMemoryError's message, else ''. */
    static String heapAdvice(Throwable e) {
        return heapExhausted(e) ? " (${MORE_HEAP})".toString() : ''
    }

    /**
     * What a CLI line adds to a skipped code to say what to do, or '' when
     * the code says enough. Only SEEDED_LEAN_BASE_FULL and SEEDED_LEAN_BASE_UNKNOWN need it.
     */
    static String skipAdvice(Result r) {
        if( r?.skipped == SEEDED_LEAN_BASE_FULL )
            return ": the snapshot there has storage figures, and this cache, seeded from a snapshot without them, cannot compute them; " +
                "${REINDEX_NO_SEED} rebuilds the cache with them".toString()
        if( r?.skipped == SEEDED_LEAN_BASE_UNKNOWN )
            return ": the snapshot there could not be read to tell whether it has storage figures, and this cache, seeded from a snapshot " +
                "without them, cannot compute them; ${REINDEX_NO_SEED} rebuilds the cache with them".toString()
        return ''
    }

    /**
     * True when base has storage figures. A base that does not say (an S3
     * object written before x-amz-meta-storage) is fetched at its own tag and
     * read once, and the answer is kept in the cache meta under that tag. One
     * gone, or whose file says neither, counts as lean. Null when the fetch
     * failed: the caller keeps the base rather than risk a full one.
     */
    private static Boolean baseHasFigures(Index index, String member, SnapshotStorage storage, SnapshotBase base, Path tempDir) {
        if( base.figures != null )
            return base.figures
        final String key = BASE_FIGURES_KEY + ':' + member
        if( base.tag != null ) {
            final String[] kept = (index.metaValue(key) ?: '').split(' ')
            if( kept.length == 2 && kept[0] == base.tag )
                return kept[1] == 'true'
        }
        Path copy = null
        try {
            copy = storage.fetchVersion(base, tempDir)
            if( copy == null )
                return false
            final Boolean figures = baseOf(copy, base.tag, base.bytes).figures
            if( figures == null )
                return false
            if( base.tag != null && !base.tag.contains(' ') )
                index.setMetaValue(key, "${base.tag} ${figures}".toString())
            return figures
        }
        catch( Exception e ) {
            log.debug("could not read the Index Snapshot of '${member}' to tell whether it has storage figures", e)
            return null
        }
        finally {
            if( copy != null )
                Files.deleteIfExists(copy)
        }
    }

    /** The old entry point: a local member, its own base, temp files beside it. */
    static Result write(Index index, String member, Path memberRoot, long maxBytes) {
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(memberRoot)
        final Path temp = pathIn(memberRoot).parent
        final Result r = write(index, member, storage, maxBytes, storage.base(temp), temp)
        return new Result(r.written, pathIn(memberRoot), r.bytes, r.runs, r.watermark, r.skipped, r.omitted, r.seededLean, r.heapExhausted)
    }

    /** [cap, bytes, runs, omitted] of an over-cap verdict, or null for none or one that does not parse. */
    private static String[] overCapVerdict(String text) {
        final String[] parts = (text ?: '').split(' ')
        if( parts.length != 4 || !(parts[3] in [OVER_CAP, OUT_OF_MEMORY, SEEDED_LEAN]) )
            return null
        for( int i = 0; i < 3; i++ )
            if( !(parts[i] ==~ /\d{1,18}/) )
                return null
        return parts
    }

    /**
     * Keeps what this build taught the cache about the member: its bytes per
     * reference after a full build of enough references, and whether its
     * storage figures ran out of memory (at how many references, with how
     * much heap). A build with figures clears the latter.
     */
    private static void learn(Index index, String member, Shape shape) {
        if( shape.fullBytes > 0 && shape.refs >= MIN_LEARN_REFS ) {
            final long perRef = (long) Math.ceil((shape.fullBytes - shape.leanBytes) / (double) shape.refs)
            index.setMetaValue(BYTES_PER_REF_KEY + ':' + member, String.valueOf(Math.max(1L, perRef)))
        }
        if( shape.ranOut )
            index.setMetaValue(OUT_OF_MEMORY_KEY + ':' + member, "${shape.refs} ${Runtime.runtime.maxMemory()}".toString())
        else if( shape.fullBytes > 0 )
            index.setMetaValue(OUT_OF_MEMORY_KEY + ':' + member, null)
    }

    /** True when Store Log entry name a is newer than b (smaller reverse timestamp); a non-null name is newer than none. */
    static boolean newer(String a, String b) {
        if( a == null || a.length() < 13 ) return false
        if( b == null || b.length() < 13 ) return true
        return a.substring(0, 13) < b.substring(0, 13)
    }

    /**
     * What a snapshot file says of itself: run rows (-1 when uncountable),
     * store_id and watermark when its meta has them, and whether it has
     * storage figures: a store_stats row, or a meta with no storage_omitted
     * (null when it has neither table).
     */
    static SnapshotBase baseOf(Path sqlite, String tag, long bytes) {
        final int runs = countRuns(sqlite)
        String storeId = null, watermark = null, omitted = null
        boolean hasMeta = false
        long stats = -1L
        try {
            DriverManager.getConnection("jdbc:sqlite:file:${sqlite.toAbsolutePath()}?mode=ro".toString()).withCloseable { Connection c ->
                final ResultSet rs = c.createStatement().executeQuery('SELECT key, value FROM meta')
                hasMeta = true
                while( rs.next() ) {
                    if( rs.getString(1) == STORE_ID_KEY ) storeId = rs.getString(2)
                    if( rs.getString(1) == WATERMARK_KEY ) watermark = rs.getString(2)
                    if( rs.getString(1) == STORAGE_OMITTED_KEY ) omitted = rs.getString(2)
                }
            }
        }
        catch( SQLException e ) {
            // No meta table: a snapshot from before §15's meta, or a hand-made one.
        }
        try {
            DriverManager.getConnection("jdbc:sqlite:file:${sqlite.toAbsolutePath()}?mode=ro".toString()).withCloseable { Connection c ->
                stats = countLong(c, 'SELECT count(*) FROM store_stats')
            }
        }
        catch( SQLException e ) {
            // No store_stats table: a snapshot from before schema 4.
        }
        final Boolean figures = stats > 0 ? Boolean.TRUE : hasMeta ? Boolean.valueOf(omitted == null) : null
        return new SnapshotBase(tag, bytes, runs, storeId, watermark, figures)
    }

    static int countRuns(Path sqlite) {
        try {
            return DriverManager.getConnection("jdbc:sqlite:file:${sqlite.toAbsolutePath()}?mode=ro".toString()).withCloseable { c ->
                count(c, 'SELECT count(*) FROM run')
            }
        }
        catch( SQLException e ) {
            return -1
        }
    }

    private static Shape buildAndVacuum(Path build, Path out, Path cacheFile, String member, String watermark, String storeId, Long sweepGraceMillis,
                                        Presence presence, Plan plan) {
        final Shape shape = new Shape()
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${build.toAbsolutePath()}".toString())
        try {
            exec(c, "PRAGMA page_size=${PAGE_SIZE}".toString())
            exec(c, 'PRAGMA synchronous=OFF')
            for( String sql : Index.ddl() )
                exec(c, sql)
            update(c, 'INSERT INTO schema_version(version) VALUES (?)', [(Object) Index.SCHEMA_VERSION])
            // ATTACH is refused inside a transaction, so it comes first.
            update(c, 'ATTACH DATABASE ? AS src', [(Object) cacheFile.toAbsolutePath().toString()])
            c.setAutoCommit(false)
            update(c, COPY_RUNS, [(Object) member, member])
            for( String sql : COPY_CLOSURE )
                exec(c, sql)
            update(c, COPY_LOG_ENTRIES, [(Object) member])
            update(c, COPY_SELECTIONS[0], [(Object) member])
            for( int i = 1; i < COPY_SELECTIONS.size(); i++ )
                exec(c, COPY_SELECTIONS[i])
            update(c, COPY_CLAIMS[0], [(Object) member])
            exec(c, COPY_CLAIMS[1])
            update(c, COPY_TRANSFERS[0], [(Object) member])
            exec(c, COPY_TRANSFERS[1])
            update(c, COPY_P4[0], [(Object) member])
            update(c, COPY_P4[1], [(Object) member])
            for( int i = 2; i < COPY_P4.size(); i++ )
                exec(c, COPY_P4[i])
            exec(c, COPY_INPUTSET_WAITS)
            exec(c, COPY_ITEMLINEAGE_WAITS)
            if( watermark != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WATERMARK_KEY, watermark])
            if( storeId != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) STORE_ID_KEY, storeId])
            if( sweepGraceMillis != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) SWEEP_GRACE_KEY, String.valueOf(sweepGraceMillis)])
            update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WRITTEN_AT_KEY, MILLIS.format(Instant.now())])
            // Everything a lean snapshot holds is in; the content and the figures are what can pass the cap.
            shape.leanBytes = pageCount(c) * PAGE_SIZE
            // A cache seeded from a lean snapshot has no content edges for the seeded runs:
            // seeded_lean, whatever reason that snapshot gave, until a reset and a full ingest.
            shape.seeded = plan.seededLean
            shape.omitted = plan.seededLean ? SEEDED_LEAN : null
            if( shape.omitted == null ) {
                final Savepoint beforeContent = c.setSavepoint()
                exec(c, COPY_CONTENT[0])
                shape.refs = countLong(c, 'SELECT count(*) FROM block_ref')
                shape.omitted = plan.leanBefore(shape.leanBytes, shape.refs)
                if( shape.omitted != null ) {
                    c.rollback(beforeContent)
                    log.info("the Index Snapshot of '${member}' is written without storage figures (${shape.omitted}): " +
                        "${shape.refs} references, about ${shape.leanBytes + shape.refs * plan.bytesPerRef} bytes with them" +
                        (shape.omitted == OUT_OF_MEMORY ? "; an earlier build ran out of heap at ${plan.oomRefs} references and " +
                            "${plan.oomHeap >> 20} MiB, so this one does not try; ${MORE_HEAP}" : ''))
                }
                else {
                    c.releaseSavepoint(beforeContent)
                    for( int i = 1; i < COPY_CONTENT.size(); i++ )
                        exec(c, COPY_CONTENT[i])
                }
            }
            if( shape.omitted != null )
                leanMeta(c, shape.omitted, plan.maxBytes)
            final List<StorageStats.Membership> memberships = shape.omitted == null ? memberships(c)
                : Collections.<StorageStats.Membership> emptyList()
            c.commit()
            c.setAutoCommit(true)
            exec(c, 'DETACH DATABASE src')
            // Running after the detach guarantees the unqualified table names
            // in ClaimCurrent can only mean the snapshot's own tables.
            c.setAutoCommit(false)
            final List<String> subjects = new ArrayList<String>()
            final ResultSet rs = c.createStatement().executeQuery('SELECT DISTINCT subject_cid FROM claim')
            while( rs.next() )
                subjects.add(rs.getString(1))
            for( String subject : subjects )
                ClaimCurrent.rewrite(c, subject)
            c.commit()
            if( shape.omitted == null ) {
                try {
                    writeStorage(c, presence, memberships)
                    c.commit()
                }
                catch( OutOfMemoryError e ) {
                    // Its maps are garbage once it unwinds; the records alone still make a snapshot.
                    c.rollback()
                    // Only the heap is remembered: more heap cures it, and the same heap would run out again.
                    // A native-thread or other OutOfMemoryError may pass, so the next build tries again.
                    shape.omitted = OUT_OF_MEMORY
                    shape.oomNow = true
                    shape.ranOut = heapExhausted(e)
                    log.warn("computing the storage figures of '${member}' ran out of memory (${e.message ?: 'OutOfMemoryError'}; " +
                        "${shape.refs} references, ${Runtime.runtime.maxMemory() >> 20} MiB heap); its Index Snapshot is written without them" +
                        (shape.ranOut ? "; later builds at as many references and no more heap skip them too; ${MORE_HEAP}"
                            : '; the next build tries them again'))
                    try {
                        dropStorage(c)
                        leanMeta(c, shape.omitted, plan.maxBytes)
                        c.commit()
                    }
                    catch( Throwable t ) {
                        // Keep the cause: the lean fallback failed because the figures ran out of memory.
                        t.addSuppressed(e)
                        throw t
                    }
                }
            }
            c.setAutoCommit(true)
            shape.runs = count(c, 'SELECT count(*) FROM run')
            vacuumInto(c, out)
            if( shape.omitted == null ) {
                shape.fullBytes = Files.size(out)
                if( plan.maxBytes > 0 && shape.fullBytes > plan.maxBytes ) {
                    // The estimate fell short: the same records, lean.
                    log.info("the Index Snapshot of '${member}' passed cas.snapshot.maxBytes (${shape.fullBytes} > ${plan.maxBytes} bytes) " +
                        "with its storage figures; it is written without them")
                    shape.omitted = OVER_CAP
                    c.setAutoCommit(false)
                    dropStorage(c)
                    leanMeta(c, shape.omitted, plan.maxBytes)
                    c.commit()
                    c.setAutoCommit(true)
                    Files.delete(out)
                    vacuumInto(c, out)
                }
            }
            return shape
        }
        finally {
            c.close()
        }
    }

    private static void vacuumInto(Connection c, Path out) {
        exec(c, "PRAGMA page_size=${PAGE_SIZE}".toString())
        update(c, 'VACUUM INTO ?', [(Object) out.toAbsolutePath().toString()])
    }

    /**
     * Empties what a lean snapshot leaves out: every storage table, the
     * content edges, and the content missing rows (an item or manifest whose
     * DirectoryManifest has not arrived), which only block_ref owners have.
     * A run's or a collection's missing rows stay. The caller owns the
     * transaction.
     */
    private static void dropStorage(Connection c) {
        for( String table : STORAGE_TABLES )
            exec(c, "DELETE FROM ${table}".toString())
        exec(c, '''DELETE FROM missing WHERE have_cid IN (SELECT owner_cid FROM block_ref)
                   AND have_cid NOT IN (SELECT completion_cid FROM run) AND have_cid NOT IN (SELECT collection_cid FROM collection)''')
        exec(c, 'DELETE FROM block')
        exec(c, 'DELETE FROM block_ref')
    }

    /** storage_omitted, and the cap in force, which the page names. With no cap there is no snapshot_max_bytes row. */
    private static void leanMeta(Connection c, String omitted, long maxBytes) {
        update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) STORAGE_OMITTED_KEY, omitted])
        if( maxBytes > 0 )
            update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) MAX_BYTES_KEY, String.valueOf(maxBytes)])
    }

    /** The build file's size in pages, including this transaction's own. */
    private static long pageCount(Connection c) {
        return countLong(c, 'PRAGMA page_count')
    }

    /**
     * The roots (P2 ruling R3) and the storage aggregates of the member, from
     * the snapshot's own rows, so every figure is this store's alone. Runs
     * first by finished_at, then Selections, then Handoffs, then one pin root
     * per (subject, asserter), numbered in that order. A Handoff reaches its
     * files and the items of its Selection, whatever the Selection's own
     * state, and a clean delete frees it, as Mark has it (P4 R5). A
     * directory or an OutputItem that has not arrived, or a run's
     * OutputCollection, makes the store, and each root that reaches it,
     * partial. With {@code presence}, a block a sweep
     * has taken (a file or a DirectoryManifest) counts in no figure but
     * collection_size, which describes the records; item_size is what the
     * item has stored here. A manifest a sweep took is a dead end, and each
     * root whose walk meets it, and the store, is partial (follow-up B1, B2).
     * project_stats comes from `memberships`, the composition's (P4 plan 1.4).
     * The caller owns the transaction.
     */
    private static void writeStorage(Connection c, Presence presence, List<StorageStats.Membership> memberships) {
        final Map<String, Long> rawSizes = new HashMap<String, Long>()
        final Set<String> manifests = new HashSet<String>()
        each(c, 'SELECT cid, size, codec FROM block') { ResultSet rs ->
            final Object size = rs.getObject(2)
            if( rs.getString(3) == 'raw' ) rawSizes.put(rs.getString(1), size == null ? null : ((Number) size).longValue())
            else manifests.add(rs.getString(1))
        }
        final Map<String, List<String>> refs = new HashMap<String, List<String>>()
        final Map<String, List<String>> names = new HashMap<String, List<String>>()
        each(c, 'SELECT owner_cid, cid, name FROM block_ref') { ResultSet rs ->
            refs.computeIfAbsent(rs.getString(1)) { new ArrayList<String>(2) }.add(rs.getString(2))
            if( rs.getString(3) != null )
                names.computeIfAbsent(rs.getString(2)) { new ArrayList<String>(1) }.add(rs.getString(3))
        }
        final Map<String, List<String>> collectionItems = new TreeMap<String, List<String>>()
        final Map<String, List<String>> runItems = new HashMap<String, List<String>>()
        each(c, """SELECT c.collection_cid, c.completion_cid, ci.item_cid FROM collection c
                   JOIN collection_item ci ON ci.collection_cid = c.collection_cid WHERE c.kind = 'output'""") { ResultSet rs ->
            collectionItems.computeIfAbsent(rs.getString(1)) { new ArrayList<String>() }.add(rs.getString(3))
            runItems.computeIfAbsent(rs.getString(2)) { new ArrayList<String>() }.add(rs.getString(3))
        }
        final Map<String, String[]> claims = new HashMap<String, String[]>()
        each(c, 'SELECT claim_cid, timestamp, asserted_by FROM claim') { ResultSet rs ->
            claims.put(rs.getString(1), [rs.getString(2), rs.getString(3)] as String[])
        }
        final Map<String, ClaimState> states = new HashMap<String, ClaimState>()
        each(c, 'SELECT DISTINCT subject_cid FROM claim') { ResultSet rs -> states.put(rs.getString(1), null) }
        for( String subject : new ArrayList<String>(states.keySet()) )
            states.put(subject, ClaimCurrent.load(c, subject))

        final List<StorageStats.Root> roots = new ArrayList<StorageStats.Root>()
        final Map<String, List<String>> seedsOf = new HashMap<String, List<String>>()
        each(c, 'SELECT completion_cid, asserted_by, pipeline, finished_at FROM run ORDER BY finished_at, completion_cid') { ResultSet rs ->
            final String cid = rs.getString(1)
            final ClaimState st = states.get(cid)
            final List<String> seeds = runItems.get(cid) ?: new ArrayList<String>()
            seedsOf.put(cid, seeds)
            final String state = st?.hidden ? StorageStats.DELETED : st?.released ? StorageStats.RELEASED : StorageStats.HELD
            final String since = state == StorageStats.DELETED ? newest(claims, st.deletionClaims)
                : state == StorageStats.RELEASED ? newest(claims, st.retainClaims)
                : rs.getString(4)
            roots.add(new StorageStats.Root(roots.size() + 1, cid, StorageStats.RUN, rs.getString(2), state, since, rs.getString(3), seeds))
        }
        final List<String[]> selections = new ArrayList<String[]>()
        each(c, """SELECT s.collection_cid, s.asserted_by, (SELECT min(written_at) FROM log_entry l WHERE l.cid = s.collection_cid)
                   FROM collection s WHERE s.kind = 'selection' ORDER BY s.collection_cid""") { ResultSet rs ->
            selections.add([rs.getString(1), rs.getString(2), rs.getString(3)] as String[])
        }
        for( String[] sel : selections ) {
            final ClaimState st = states.get(sel[0])
            final List<String> seeds = selectionItems(c, sel[0])
            seedsOf.put(sel[0], seeds)
            final boolean deleted = st?.hidden
            roots.add(new StorageStats.Root(roots.size() + 1, sel[0], StorageStats.SELECTION, sel[1],
                deleted ? StorageStats.DELETED : StorageStats.HELD, deleted ? newest(claims, st.deletionClaims) : sel[2], null, seeds))
        }
        final List<String[]> handoffs = new ArrayList<String[]>()
        each(c, '''SELECT h.cid, h.asserted_by, h.selection_cid, (SELECT min(written_at) FROM log_entry l WHERE l.cid = h.cid)
                   FROM handoff h ORDER BY h.cid''') { ResultSet rs ->
            handoffs.add([rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)] as String[])
        }
        for( String[] ho : handoffs ) {
            // Its files through its own block_ref rows, and its Selection whole, as Mark keeps them (P4 R5).
            final List<String> seeds = new ArrayList<String>()
            seeds.add(ho[0])
            seeds.addAll(selectionItems(c, ho[2]))
            seedsOf.put(ho[0], seeds)
            final ClaimState st = states.get(ho[0])
            final boolean deleted = st?.hidden
            roots.add(new StorageStats.Root(roots.size() + 1, ho[0], StorageStats.HANDOFF, ho[1],
                deleted ? StorageStats.DELETED : StorageStats.HELD, deleted ? newest(claims, st.deletionClaims) : ho[3], null, seeds))
        }
        for( String subject : new TreeSet<String>(states.keySet()) ) {
            final ClaimState st = states.get(subject)
            if( !st.pinned )
                continue
            // One root per asserter, however many current pins they hold on it.
            final Map<String, List<String>> byAsserter = new TreeMap<String, List<String>>()
            for( String pin : st.pinClaims )
                byAsserter.computeIfAbsent(StorageStats.asserter(claims.get(pin)?.getAt(1))) { new ArrayList<String>() }.add(pin)
            final List<String> seeds = seedsOf.containsKey(subject) ? seedsOf.get(subject)
                : collectionItems.containsKey(subject) ? collectionItems.get(subject)
                : [subject]
            for( List<String> pins : byAsserter.values() ) {
                // since and asserted_by are the newest pin's.
                String since = null, assertedBy = null
                for( String pin : pins ) {
                    final String[] k = claims.get(pin)
                    if( k != null && (since == null || k[0] > since) ) { since = k[0]; assertedBy = k[1] }
                }
                roots.add(new StorageStats.Root(roots.size() + 1, subject, StorageStats.PIN, assertedBy, StorageStats.PINNED, since, null, seeds))
            }
        }

        final Set<String> absent = presence == null ? Collections.<String> emptySet()
            : sweptBlocks(roots, rawSizes, refs, manifests, exposedSubjects(c), presence)
        final StorageStats stats = StorageStats.compute(rawSizes, refs, names, roots, collectionItems, manifests, absent, memberships)

        // A content missing row: an item or manifest whose DirectoryManifest
        // has not arrived. And an output collection's missing row: an
        // OutputItem that has not arrived, a member of the collection, so a
        // root that holds the collection reaches it.
        final Set<String> waiting = new HashSet<String>()
        each(c, 'SELECT DISTINCT have_cid FROM missing WHERE have_cid IN (SELECT owner_cid FROM block_ref)') { ResultSet rs -> waiting.add(rs.getString(1)) }
        each(c, "SELECT DISTINCT needed_cid FROM missing WHERE have_cid IN (SELECT collection_cid FROM collection WHERE kind = 'output')") { ResultSet rs ->
            waiting.add(rs.getString(1))
        }
        // A run whose OutputCollection has not arrived: its manifest's missing row says nothing of its files.
        final Set<String> waitingRuns = new HashSet<String>()
        each(c, '''SELECT DISTINCT m.have_cid FROM missing m JOIN run r ON r.completion_cid = m.have_cid
                   WHERE m.needed_cid IS NOT r.manifest_cid''') { ResultSet rs -> waitingRuns.add(rs.getString(1)) }
        final Map<Integer, Integer> partialRoots = new HashMap<Integer, Integer>()
        for( StorageStats.Root root : roots )
            if( (root.kind != StorageStats.SELECTION && waitingRuns.contains(root.cid))
                    || (waiting && reachesAny(root.seeds, rawSizes, refs, waiting)) )
                partialRoots.put(root.id, 1)

        for( StorageStats.Root root : roots )
            update(c, 'INSERT INTO root(id, cid, kind, asserted_by, state, since) VALUES (?, ?, ?, ?, ?, ?)',
                [(Object) root.id, root.cid, root.kind, root.assertedBy, root.state, root.since])
        final Map store = new LinkedHashMap(stats.store)
        if( waiting || waitingRuns ) store.put('partial', 1)
        insert(c, 'store_stats', [store] as List<Map>)
        final List<Map> rootStats = new ArrayList<Map>()
        for( Map row : stats.rootStats ) {
            final Map out = new LinkedHashMap(row)
            if( partialRoots.containsKey((Integer) row.get('root_id')) ) out.put('partial', 1)
            rootStats.add(out)
        }
        insert(c, 'root_stats', rootStats)
        insert(c, 'root_shared', stats.rootShared)
        insert(c, 'asserter_stats', stats.asserters)
        insert(c, 'pipeline_stats', stats.pipelines)
        insert(c, 'collection_size', stats.collectionSizes)
        insert(c, 'item_size', stats.itemSizes)
        insert(c, 'kind_stats', stats.kinds)
        insert(c, 'pinned_overlap', stats.pinnedOverlap)
        insert(c, 'project_stats', stats.projects)
    }

    /**
     * The composition's current membership Claims, from the attached cache
     * ({@link #MEMBERSHIPS}). The value is the Project link as DAG-JSON
     * text; one that is not a link is skipped.
     */
    private static List<StorageStats.Membership> memberships(Connection c) {
        final List<StorageStats.Membership> out = new ArrayList<StorageStats.Membership>()
        each(c, MEMBERSHIPS) { ResultSet rs ->
            final String project = projectOf(rs.getString(2))
            if( project != null )
                out.add(new StorageStats.Membership(project, rs.getString(1)))
        }
        return out
    }

    private static String projectOf(String value) {
        try {
            final Object link = DagJson.decode(value)
            return link instanceof Cid ? link.toString() : null
        }
        catch( DagJson.DagJsonException e ) {
            return null
        }
    }

    /**
     * The runs and Selections whose content a held root may reach though the
     * member lacks it. Those ever released or deleted: any retain Claim (set
     * or del) or deletion Claim (delete or del) in their history, current or
     * superseded, among the member's Claims, since a sweep may have taken
     * what only they reached at the time. And the roots and runs named by a
     * Transfer the member logged (P3 plan D8), since a copy carries a run's
     * every record but only the files its depth reaches. The snapshot's
     * `transfer_root` rows are already only the member's.
     */
    private static Set<String> exposedSubjects(Connection c) {
        final Set<String> out = new HashSet<String>()
        final PreparedStatement s = c.prepareStatement('''SELECT DISTINCT subject_cid FROM claim
            WHERE attribute = ? OR (attribute IS NULL AND verb IN (?, ?))
            UNION SELECT root_cid FROM transfer_root''')
        try {
            s.setString(1, Claim.RETAIN); s.setString(2, Claim.DELETE); s.setString(3, Claim.DEL)
            final ResultSet rs = s.executeQuery()
            while( rs.next() )
                out.add(rs.getString(1))
        }
        finally {
            s.close()
        }
        return out
    }

    /**
     * The blocks a sweep has taken from the member's store, raw files and
     * DirectoryManifests (follow-up B1), which leave every storage figure. A
     * block only released or deleted roots reach is looked up unless an
     * absence is already recorded for it. A recorded absence stands whatever
     * the roots' state, since a Claim that restores or pins a root brings no
     * bytes back (fix round 3, item 1); one a held or pinned root now reaches
     * is looked at again at each build, and only a present answer drops it.
     * One no root reaches any more is dropped.
     *
     * A cache with no record for a swept block (reindexed, seeded, or another
     * machine's) would count it again once a held or pinned root reaches it.
     * So a block a held or pinned root reaches under a run or Selection ever
     * released or deleted, with no record either way, is looked up once: gone
     * is recorded in `block_absent`, there in `block_present`, which stands
     * while a held or pinned root reaches it. A run or root a Transfer into
     * the member names is exposed the same way, so the files a partial copy
     * left behind leave the figures as swept ones do. With nothing releasing,
     * nothing exposed and nothing recorded, nothing is walked or asked.
     *
     * Every walk stops at a manifest believed absent, as the sweep's mark
     * does, since a warm cache still holds its edges. A lookup that finds a
     * manifest gone (or a recorded one back) changes what the walks reach, so
     * they are walked again and only blocks not yet asked are asked, until
     * the absent manifests settle; each block is asked at most once a build.
     */
    private static Set<String> sweptBlocks(List<StorageStats.Root> roots, Map<String, Long> rawSizes, Map<String, List<String>> refs,
                                           Set<String> manifests, Set<String> exposed, Presence presence) {
        final List<String> contentSeeds = new ArrayList<String>()
        final List<String> otherSeeds = new ArrayList<String>()
        final List<String> exposedSeeds = new ArrayList<String>()
        for( StorageStats.Root root : roots ) {
            final List<String> seeds = root.seeds ?: Collections.<String> emptyList()
            (StorageStats.isContent(root) ? contentSeeds : otherSeeds).addAll(seeds)
            if( root.kind != StorageStats.PIN && exposed.contains(root.cid) )
                exposedSeeds.addAll(seeds)
        }
        final Set<String> known = presence.known()
        final Set<String> present = presence.knownPresent()
        final Presence.Answers answers = new Presence.Answers()
        final Set<String> asked = new HashSet<String>()
        Set<String> stops = absentManifests(known, manifests, asked, answers)
        Set<String> candidates
        Set<String> content
        while( true ) {
            candidates = otherSeeds ? StorageStats.content(otherSeeds, rawSizes, refs, manifests, stops) : new HashSet<String>()
            if( asked.isEmpty() && !candidates && !known && !present && !exposedSeeds )
                return Collections.<String> emptySet()
            content = StorageStats.content(contentSeeds, rawSizes, refs, manifests, stops)
            candidates.removeAll(content)
            // A cold cache: what a held or pinned root reaches under a run or Selection that was ever exposed, with no record either way.
            final Set<String> cold = exposedSeeds ? StorageStats.content(exposedSeeds, rawSizes, refs, manifests, stops) : new HashSet<String>()
            cold.retainAll(content)
            final List<String> ask = new ArrayList<String>()
            for( String cid : candidates )
                if( !known.contains(cid) && !asked.contains(cid) )
                    ask.add(cid)
            for( String cid : content )
                if( (known.contains(cid) || (cold.contains(cid) && !present.contains(cid))) && !asked.contains(cid) )
                    ask.add(cid)
            if( ask.isEmpty() )
                break
            asked.addAll(ask)
            answers.addAll(presence.lookUp(ask))
            final Set<String> next = absentManifests(known, manifests, asked, answers)
            if( next == stops )
                break
            stops = next
        }
        final Set<String> recheck = new HashSet<String>(known)
        recheck.retainAll(content)
        final Set<String> stale = new HashSet<String>(known)
        stale.removeAll(candidates)
        stale.removeAll(recheck)
        presence.forget(stale)
        // A presence stands while a held or pinned root reaches the block; a releasing-only one is checked as a candidate.
        final Set<String> stalePresent = new HashSet<String>(present)
        stalePresent.removeAll(content)
        presence.forgetPresent(stalePresent)
        presence.settle(answers, recheck, candidates, content)
        // A recorded absence among the candidates stands; a rechecked one is absent only if found gone again.
        final Set<String> out = new HashSet<String>(known)
        out.retainAll(candidates)
        out.addAll(answers.gone)
        return out
    }

    /**
     * The manifests believed absent: recorded absences not asked this build,
     * and those a lookup found gone. A failed lookup counts as present, as
     * everywhere else.
     */
    private static Set<String> absentManifests(Set<String> known, Set<String> manifests, Set<String> asked, Presence.Answers answers) {
        final Set<String> out = new HashSet<String>()
        for( String cid : known )
            if( manifests.contains(cid) && !asked.contains(cid) )
                out.add(cid)
        for( String cid : answers.gone )
            if( manifests.contains(cid) )
                out.add(cid)
        return out
    }

    /**
     * Whether blocks the member may lack are still in its store: those a
     * sweep may have taken (under a run or Selection ever released or
     * deleted), and those of a root or run a Transfer the member logged
     * names, since a copy carries a run's every record but only the files
     * its depth reaches (P3 plan D8; {@link #exposedSubjects}).
     * A presence under a held or pinned root is kept in `block_present` and
     * not asked again while such a root reaches it.
     * An absence is kept in the cache's `block_absent`. While only released
     * or deleted roots reach the block it is never asked again; once a held
     * or pinned root reaches it, it is asked at each build, since only a new
     * put can bring it back. A presence is asked again at every build, since
     * a later sweep may take the block. The questions go LOOKUPS_AT_ONCE at a
     * time, so on S3 their wall-clock cost is bounded. A lookup that fails
     * counts as present and changes no record: failing to look is no
     * evidence either way. An Error (an OutOfMemoryError) reaches the build.
     */
    @CompileStatic
    private static final class Presence {
        static final int LOOKUPS_AT_ONCE = 16

        final Index index
        final String member
        final Closure<Boolean> present
        /** The member's retention storage, for its sweep.lock; null records nothing present. */
        final RetentionStorage sweepLock
        /** `block_present_generation` before the build copied the roots, or null when it could not be read (then nothing is recorded present). */
        final String generation
        /** Whether a presence may be recorded: the generation was read, and no sweep may have been deleting when the build started. */
        final boolean mayRecord

        /**
         * Made before the build copies the roots, so the generation it reads,
         * and then the sweep lock, are from before them.
         *
         * Why a lock seen at neither read means no sweep deletes a block this
         * build records present (the follow-up to fix round 3):
         * - Every Claim the build sees was ingested before the generation was
         *   read (one ingested later moves it, and recordPresentIf refuses),
         *   so it was in the Store Log before the build-start lock read.
         * - A sweep that takes the lock after that read lists the Store Logs
         *   after taking it and marks from them (Sweep.applyLocked: logs(),
         *   then Mark.of), so the block such a Claim holds is live to its
         *   mark: SweepPlan.of puts no live block in `due`, and deleteDue
         *   drops from each batch whatever the mark, extended before the
         *   batch, holds live.
         * - Only a sweep whose mark predates the Claim can delete the block,
         *   and it holds the lock from before its mark until after its
         *   deletes (Sweep.apply releases in its finally), so the read here
         *   sees it. A lock released just now may be one Sweep.abandon let go
         *   with a batch in flight, so it counts too (SweepLock.mayBeSweeping).
         * The lock is read again just before recording, for a sweep that
         * started meanwhile. One assumption remains, the sweep's own, which
         * the memo inherits: a sweep suspended mid-batch for longer than
         * SweepLock.STALE_MILLIS, then taken over and released, can resume
         * deleting. A lock a crashed sweep left held stops recording
         * until a sweep takes it over and releases it: lookups go on, and the
         * figures are right, only slower.
         */
        Presence(Index index, String member, Closure<Boolean> present, RetentionStorage sweepLock) {
            this.index = index; this.member = member; this.present = present; this.sweepLock = sweepLock
            String g = null
            try {
                g = index.presentGeneration()
            }
            catch( RuntimeException e ) {
                log.debug("could not read block_present_generation", e)
            }
            this.generation = g
            // After the generation: the order the argument above needs. With no generation nothing is recorded, so no read.
            this.mayRecord = g != null && !mayBeSweeping()
        }

        /** The member's sweep.lock says a sweep may be deleting; so does no storage, or one that cannot be read. */
        private boolean mayBeSweeping() {
            if( sweepLock == null )
                return true
            try {
                return SweepLock.mayBeSweeping(sweepLock)
            }
            catch( Exception e ) {
                log.debug("could not read the sweep.lock of '${member}'; recording no presence", e)
                return true
            }
        }

        /** The member's recorded absences; none when the cache cannot say. */
        Set<String> known() {
            try {
                return index.absentBlocks(member)
            }
            catch( RuntimeException e ) {
                log.debug("could not read the absent blocks of '${member}'", e)
                return new HashSet<String>()
            }
        }

        void forget(Set<String> cids) {
            try {
                index.clearAbsent(member, cids)
            }
            catch( RuntimeException e ) {
                log.debug("could not drop absent blocks of '${member}'", e)
            }
        }

        /** The member's recorded presences (`block_present`); none when the cache cannot say. */
        Set<String> knownPresent() {
            try {
                return index.presentBlocks(member)
            }
            catch( RuntimeException e ) {
                log.debug("could not read the present blocks of '${member}'", e)
                return new HashSet<String>()
            }
        }

        void forgetPresent(Set<String> cids) {
            try {
                index.clearPresent(member, cids)
            }
            catch( RuntimeException e ) {
                log.debug("could not drop present blocks of '${member}'", e)
            }
        }

        private void recordPresent(Collection<String> cids) {
            if( !mayRecord || !cids )
                return
            if( mayBeSweeping() ) {
                log.debug("a sweep of '${member}' may have started during the build; ${cids.size()} presence(s) not recorded")
                return
            }
            try {
                if( !index.recordPresentIf(member, cids, generation) && cids )
                    log.debug("block_present of '${member}' changed during the build; ${cids.size()} presence(s) not recorded")
            }
            catch( RuntimeException e ) {
                log.debug("could not record present blocks of '${member}'", e)
            }
        }

        /**
         * Records what this build's lookups found. A block found gone is
         * recorded absent if a root still reaches it (a rechecked one already
         * is); a rechecked one found there loses its absence. One a held or
         * pinned root reaches and is found there is recorded present.
         */
        void settle(Answers answers, Set<String> recheck, Set<String> candidates, Set<String> content) {
            final List<String> found = new ArrayList<String>()
            for( String cid : answers.gone )
                if( !recheck.contains(cid) && (candidates.contains(cid) || content.contains(cid)) )
                    found.add(cid)
            final Set<String> back = new HashSet<String>()
            final List<String> held = new ArrayList<String>()
            for( String cid : answers.there ) {
                if( recheck.contains(cid) ) back.add(cid)
                if( content.contains(cid) ) held.add(cid)
            }
            try {
                index.recordAbsent(member, found)
            }
            catch( RuntimeException e ) {
                log.debug("could not record absent blocks of '${member}'", e)
            }
            forget(back)
            recordPresent(held)
        }

        /** The cids a lookup found gone, and those it found there; a failed lookup is in neither. */
        @CompileStatic
        static final class Answers {
            final List<String> gone = new ArrayList<String>()
            final List<String> there = new ArrayList<String>()

            void add(String cid, Boolean answer) {
                if( answer == null ) return
                (answer ? there : gone).add(cid)
            }

            void addAll(Answers more) {
                gone.addAll(more.gone)
                there.addAll(more.there)
            }
        }

        /** Asks the store about each cid, LOOKUPS_AT_ONCE at a time. */
        Answers lookUp(List<String> cids) {
            final Answers out = new Answers()
            if( cids.size() <= 1 ) {
                for( String cid : cids )
                    out.add(cid, isPresent(cid))
                return out
            }
            final AtomicReference<Throwable> failed = new AtomicReference<Throwable>()
            final ExecutorService pool = Executors.newFixedThreadPool(Math.min(LOOKUPS_AT_ONCE, cids.size()), { Runnable r ->
                final Thread t = new Thread(r, 'nf-blocks-presence')
                t.daemon = true
                t
            } as ThreadFactory)
            try {
                final List<Future<Boolean>> answers = new ArrayList<Future<Boolean>>(cids.size())
                for( String cid : cids ) {
                    final String each = cid
                    answers.add(pool.submit({ ->
                        // After an Error, the rest are not asked.
                        if( failed.get() != null ) return (Boolean) null
                        try {
                            return isPresent(each)
                        }
                        catch( Throwable e ) {
                            failed.compareAndSet(null, e)
                            throw e
                        }
                    } as Callable<Boolean>))
                }
                for( int i = 0; i < cids.size(); i++ ) {
                    try {
                        out.add(cids[i], answers[i].get())
                    }
                    catch( ExecutionException e ) {
                        if( e.cause instanceof Error ) throw (Error) e.cause
                        throw new IllegalStateException(e.cause)
                    }
                }
                return out
            }
            finally {
                pool.shutdownNow()
            }
        }

        /** True or false as the store answers; null when the lookup failed, which counts as present. */
        private Boolean isPresent(String cid) {
            try {
                return Boolean.valueOf(present.call(Cid.parse(cid)))
            }
            catch( Exception e ) {
                log.debug("could not tell whether ${cid} is still in the store of '${member}'", e)
                return null
            }
        }
    }

    /** The newest timestamp among these Claims, or null. Timestamps are ISO-8601 UTC, so text order is time order. */
    private static String newest(Map<String, String[]> claims, List<String> cids) {
        String out = null
        for( String cid : cids ) {
            final String t = claims.get(cid)?.getAt(0)
            if( t != null && (out == null || t > out) ) out = t
        }
        return out
    }

    private static List<String> selectionItems(Connection c, String selection) {
        final List<String> out = new ArrayList<String>()
        final PreparedStatement s = c.prepareStatement(Index.SQL_SELECTION_ITEMS)
        try {
            s.setString(1, selection)
            final ResultSet rs = s.executeQuery()
            while( rs.next() )
                out.add(rs.getString(1))
        }
        finally {
            s.close()
        }
        return out
    }

    /** True when the walk from seeds through refs passes an owner in `owners`. Raw blocks end the walk; a cycle ends at the visited set. */
    private static boolean reachesAny(Collection<String> seeds, Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> owners) {
        final Set<String> seen = new HashSet<String>()
        final Deque<String> todo = new ArrayDeque<String>(seeds)
        while( !todo.isEmpty() ) {
            final String cid = todo.pop()
            if( !seen.add(cid) || rawSizes.containsKey(cid) ) continue
            if( owners.contains(cid) ) return true
            final List<String> next = refs.get(cid)
            if( next ) todo.addAll(next)
        }
        return false
    }

    /** Inserts rows keyed by column name; every row of one call has the same keys. */
    private static void insert(Connection c, String table, List<Map> rows) {
        if( !rows )
            return
        final List<String> columns = rows[0].keySet().collect { Object k -> k.toString() }
        final PreparedStatement s = c.prepareStatement(
            "INSERT INTO ${table}(${columns.join(', ')}) VALUES (${columns.collect { '?' }.join(', ')})".toString())
        try {
            for( Map row : rows ) {
                for( int i = 0; i < columns.size(); i++ )
                    s.setObject(i + 1, row.get(columns[i]))
                s.addBatch()
            }
            s.executeBatch()
        }
        finally {
            s.close()
        }
    }

    private static void each(Connection c, String sql, Closure body) {
        final Statement s = c.createStatement()
        try {
            final ResultSet rs = s.executeQuery(sql)
            while( rs.next() )
                body.call(rs)
        }
        finally {
            s.close()
        }
    }

    /** Writes the page beside the snapshot when its bytes differ. True when it wrote. */
    static boolean writePage(Path memberRoot, byte[] page) {
        final Path target = memberRoot.resolve(PAGE_NAME)
        if( Files.isRegularFile(target) && Arrays.equals(Files.readAllBytes(target), page) )
            return false
        Files.createDirectories(memberRoot)
        final Path temp = memberRoot.resolve(".tmp-${token()}.html")
        try {
            Files.write(temp, page)
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return true
        }
        finally {
            Files.deleteIfExists(temp)
        }
    }

    /** The page this build of the plugin carries, or null in a build without one. */
    static byte[] bundledPage() {
        final InputStream input = IndexSnapshot.getResourceAsStream(PAGE_RESOURCE)
        if( input == null )
            return null
        try {
            return input.readAllBytes()
        }
        finally {
            input.close()
        }
    }

    private static void exec(Connection c, String sql) {
        final Statement s = c.createStatement()
        try {
            s.execute(sql)
        }
        finally {
            s.close()
        }
    }

    private static void update(Connection c, String sql, List<Object> parameters) {
        final PreparedStatement s = c.prepareStatement(sql)
        try {
            for( int i = 0; i < parameters.size(); i++ )
                s.setObject(i + 1, parameters[i])
            s.execute()
        }
        finally {
            s.close()
        }
    }

    private static long countLong(Connection c, String sql) {
        final Statement s = c.createStatement()
        try {
            final ResultSet rs = s.executeQuery(sql)
            return rs.next() ? rs.getLong(1) : 0L
        }
        finally {
            s.close()
        }
    }

    private static int count(Connection c, String sql) {
        final Statement s = c.createStatement()
        try {
            final ResultSet rs = s.executeQuery(sql)
            return rs.next() ? rs.getInt(1) : 0
        }
        finally {
            s.close()
        }
    }

    private static String token() {
        final byte[] bytes = new byte[6]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }
}
