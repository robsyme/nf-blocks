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
import java.sql.Statement
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.transform.CompileStatic
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
    static final String WRITTEN_AT_KEY = 'snapshot_written_at'
    static final String PAGE_NAME = 'index.html'
    static final String PAGE_RESOURCE = '/robsyme/cas/explorer/index.html'

    /** Why a write kept the old snapshot (Result.skipped). */
    static final String OVER_CAP = 'over_cap'
    static final String FEWER_RUNS = 'fewer_runs'
    static final String REPLACED_MEANWHILE = 'replaced_meanwhile'
    static final String CATCH_UP_FAILED = 'catch_up_failed'

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

    /** What one write did. */
    @CompileStatic
    static final class Result {
        final boolean written
        final Path path
        final long bytes
        final int runs
        final String watermark
        /** Null when written; otherwise the guard that kept the old snapshot (OVER_CAP, FEWER_RUNS, REPLACED_MEANWHILE, CATCH_UP_FAILED). */
        final String skipped

        Result(boolean written, Path path, long bytes, int runs, String watermark, String skipped) {
            this.written = written
            this.path = path
            this.bytes = bytes
            this.runs = runs
            this.watermark = watermark
            this.skipped = skipped
        }

        @Override
        String toString() { "IndexSnapshot.Result[written=$written, path=$path, bytes=$bytes, runs=$runs, skipped=$skipped]" }
    }

    static String relativePath() { "index/v${Index.SCHEMA_VERSION}.sqlite" }

    static Path pathIn(Path memberRoot) { memberRoot.resolve(relativePath()) }

    @CompileStatic
    static final class Built {
        final Path file; final long bytes; final int runs; final String watermark
        Built(Path file, long bytes, int runs, String watermark) { this.file = file; this.bytes = bytes; this.runs = runs; this.watermark = watermark }
    }

    /** The member's snapshot as a local file in tempDir; the caller deletes it. */
    static Built build(Index index, String member, Path tempDir) {
        Files.createDirectories(tempDir)
        final String token = token()
        final Path buildFile = tempDir.resolve(".tmp-${token}.build")
        final Path vacuumed = tempDir.resolve("nf-blocks-snapshot-${token}.sqlite")
        try {
            final String watermark = index.watermark(member)
            final int runs = buildAndVacuum(buildFile, vacuumed, index.file, member, watermark)
            return new Built(vacuumed, Files.size(vacuumed), runs, watermark)
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

    /**
     * Writes the member's snapshot into storage unless a guard keeps the old
     * one (DESIGN.md §15, ticket 04 decision 3): the cap (maxBytes > 0), fewer
     * run rows than base, or another writer's version in place of base.
     */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir) {
        if( maxBytes > 0 && base != null && base.bytes >= maxBytes )
            return new Result(false, null, base.bytes, -1, null, OVER_CAP)
        final Built built = build(index, member, tempDir)
        try {
            if( maxBytes > 0 && built.bytes > maxBytes )
                return new Result(false, null, built.bytes, built.runs, built.watermark, OVER_CAP)
            if( base != null && base.runs > built.runs )
                return new Result(false, null, built.bytes, built.runs, built.watermark, FEWER_RUNS)
            if( !storage.replace(built.file, built.runs, base) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, REPLACED_MEANWHILE)
            return new Result(true, null, built.bytes, built.runs, built.watermark, null)
        }
        finally {
            Files.deleteIfExists(built.file)
        }
    }

    /** The old entry point: a local member, its own base, temp files beside it. */
    static Result write(Index index, String member, Path memberRoot, long maxBytes) {
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(memberRoot)
        final Path temp = pathIn(memberRoot).parent
        final Result r = write(index, member, storage, maxBytes, storage.base(temp), temp)
        return new Result(r.written, pathIn(memberRoot), r.bytes, r.runs, r.watermark, r.skipped)
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

    private static int buildAndVacuum(Path build, Path out, Path cacheFile, String member, String watermark) {
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
            if( watermark != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WATERMARK_KEY, watermark])
            update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WRITTEN_AT_KEY, MILLIS.format(Instant.now())])
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
            c.setAutoCommit(true)
            final int runs = count(c, 'SELECT count(*) FROM run')
            exec(c, "PRAGMA page_size=${PAGE_SIZE}".toString())
            update(c, 'VACUUM INTO ?', [(Object) out.toAbsolutePath().toString()])
            return runs
        }
        finally {
            c.close()
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
