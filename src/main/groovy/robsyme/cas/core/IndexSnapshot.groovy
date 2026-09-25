package robsyme.cas.core

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.SecureRandom
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
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

    private static final SecureRandom RANDOM = new SecureRandom()
    private static final DateTimeFormatter MILLIS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    /** The member's runs; alias written as NULL, since an alias is a local label. */
    private static final String COPY_RUNS = '''
        INSERT INTO main.run
        SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
               run_name, asserted_by, status, possibly_incomplete, finished_at, NULL
        FROM src.run WHERE member = ?'''

    /** Everything those runs reach, in dependency order. */
    private static final List<String> COPY_CLOSURE = [
        'INSERT INTO main.collection SELECT * FROM src.collection WHERE completion_cid IN (SELECT completion_cid FROM main.run)',
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
        /** Null when written; `over_cap` when the cap kept the old snapshot. */
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

    /**
     * Writes the snapshot of {@code member} into {@code memberRoot}. With a
     * positive {@code maxBytes}, an existing snapshot at or over it is left
     * alone, and a new one over it is discarded, keeping the old.
     */
    static Result write(Index index, String member, Path memberRoot, long maxBytes) {
        final Path target = pathIn(memberRoot)
        if( maxBytes > 0 && Files.isRegularFile(target) && Files.size(target) >= maxBytes )
            return new Result(false, target, Files.size(target), -1, null, 'over_cap')
        Files.createDirectories(target.parent)
        final String token = token()
        final Path build = target.resolveSibling(".tmp-${token}.build")
        final Path vacuumed = target.resolveSibling(".tmp-${token}.sqlite")
        try {
            final String watermark = index.watermark(member)
            final int runs = buildAndVacuum(build, vacuumed, index.file, member, watermark)
            final long bytes = Files.size(vacuumed)
            if( maxBytes > 0 && bytes > maxBytes )
                return new Result(false, target, bytes, runs, watermark, 'over_cap')
            if( FileSystems.default.supportedFileAttributeViews().contains('posix') )
                Files.setPosixFilePermissions(vacuumed, PosixFilePermissions.fromString('rw-r--r--'))
            Files.move(vacuumed, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return new Result(true, target, bytes, runs, watermark, null)
        }
        finally {
            for( Path path : [build, vacuumed] )
                for( String suffix : ['', '-journal', '-wal', '-shm'] )
                    Files.deleteIfExists(path.resolveSibling(path.fileName.toString() + suffix))
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
            update(c, COPY_RUNS, [(Object) member])
            for( String sql : COPY_CLOSURE )
                exec(c, sql)
            if( watermark != null )
                update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WATERMARK_KEY, watermark])
            update(c, 'INSERT INTO meta(key, value) VALUES (?, ?)', [(Object) WRITTEN_AT_KEY, MILLIS.format(Instant.now())])
            c.commit()
            c.setAutoCommit(true)
            exec(c, 'DETACH DATABASE src')
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
