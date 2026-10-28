package robsyme.cas.core

import java.nio.file.FileAlreadyExistsException
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
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j

/**
 * The derived index (DESIGN.md §12): a SQLite file, per user, holding what the
 * blocks already say so that three questions can be answered without a scan.
 * Which runs produced this exact content, what is the latest successful run of
 * a pipeline, and which items of an output carry a given metadata value.
 *
 * Everything here is rebuildable from `bafy…` blocks alone, so the file may be
 * deleted at any time; a schema it does not recognise is deleted rather than
 * migrated. Blocks that have not arrived leave a row in `missing` and never
 * stop an ingest.
 */
@Slf4j
@CompileStatic
class Index implements Closeable {

    /** Bumped whenever the schema below changes; a mismatch deletes and recreates. */
    static final int SCHEMA_VERSION = 7

    // The SQL the block explorer's page runs too (DESIGN.md §12, §15). The page
    // holds a copy in web/src/queries.json; ExplorerQueriesTest pins them equal.

    static final String SQL_PRODUCERS_OF =
        'SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?'

    // Query 2 leaves out a run with a current delete Claim, a conflicted
    // deletion included (lineage spec §9, block explorer spec section 8).
    static final String SQL_LATEST_SUCCESSFUL_RUN =
        "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 AND NOT EXISTS (SELECT 1 FROM claim_current cc JOIN claim k ON k.claim_cid = cc.claim_cid WHERE cc.subject_cid = run.completion_cid AND cc.attribute IS NULL AND k.verb = 'delete') ORDER BY finished_at DESC, completion_cid ASC LIMIT 1"

    /** Every item a Selection reaches, nested Selections included, each once (spec section 11). */
    static final String SQL_SELECTION_ITEMS = '''WITH RECURSIVE reach(cid) AS (
            SELECT ? UNION SELECT s.child_cid FROM selection_child s JOIN reach r ON s.parent_cid = r.cid)
        SELECT DISTINCT ci.item_cid FROM collection_item ci JOIN reach r ON ci.collection_cid = r.cid ORDER BY ci.item_cid'''

    /** {@link #SQL_SELECTION_ITEMS} with the collections each item was picked through; a NULL via is a pick by query. */
    private static final String SQL_SELECTION_VIAS = '''WITH RECURSIVE reach(cid) AS (
            SELECT ? UNION SELECT s.child_cid FROM selection_child s JOIN reach r ON s.parent_cid = r.cid)
        SELECT DISTINCT ci.item_cid, ci.via_cid FROM collection_item ci JOIN reach r ON ci.collection_cid = r.cid ORDER BY ci.item_cid, ci.via_cid'''

    private static final String SQL_SELECTIONS_NOT_INDEXED = '''WITH RECURSIVE reach(cid) AS (
            SELECT ? UNION SELECT s.child_cid FROM selection_child s JOIN reach r ON s.parent_cid = r.cid)
        SELECT r.cid FROM reach r LEFT JOIN collection c ON c.collection_cid = r.cid AND c.kind = 'selection'
        WHERE c.collection_cid IS NULL ORDER BY r.cid'''

    private static final String SQL_CONFLICTED_DELETIONS_OF_PIPELINE =
        "SELECT DISTINCT run.completion_cid FROM run JOIN claim_current cc ON cc.subject_cid = run.completion_cid WHERE run.pipeline = ? AND cc.attribute IS NULL AND cc.conflicted = 1"

    // ci.collection_cid rides along so the page can link each item without a
    // second lookup; it is on the collection_item row already read, so it costs no page.
    static final String SQL_ITEMS_BASE =
        'SELECT ci.item_cid, ci.collection_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?'

    static final String SQL_ITEMS_PREDICATE =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?)'

    static final String SQL_ITEMS_PREDICATE_NULL =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value IS NULL)'

    // A range compares an int or float value as a number (Conditions, DESIGN.md
    // §12 amended 2026-10-24). One clause per operator, so no SQL is built from input.
    static final Map<String, String> SQL_ITEMS_RANGE = Conditions.RANGE_OPS.collectEntries { String op ->
        [(op): " AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type IN ('int', 'float') AND CAST(a.value AS REAL) ${op} ?)".toString()]
    }.asImmutable() as Map<String, String>

    // != keeps every item with no row equal on path, type and value, so an item
    // without the path matches (DESIGN.md §12 amended 2026-10-24).
    static final String SQL_ITEMS_NOT_EQUAL =
        ' AND NOT EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?)'

    static final String SQL_ITEMS_ORDER = ' ORDER BY ci.item_cid'

    static final String SQL_COLLECTIONS_OF =
        'SELECT output_name, collection_cid FROM collection WHERE completion_cid = ? ORDER BY output_name'

    // nf-blocks:items (decision 7 of the milestone 3 plan): conditions match
    // the value text of any type. DISTINCT, since a collection may list an item twice.
    private static final String SQL_ITEM_HITS_BASE =
        'SELECT DISTINCT ci.collection_cid, ci.item_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?'

    private static final String SQL_ITEM_HITS_TEXT =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.value = ?)'

    private static final String SQL_ITEM_HITS_NOT_TEXT =
        ' AND NOT EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.value = ?)'

    private static final String SQL_ITEM_HITS_ORDER = ' ORDER BY ci.item_cid, ci.collection_cid'

    // nf-blocks:copy's closure (P3 plan 1.4, CopyClosure).
    static final String SQL_BLOCK_SIZE = 'SELECT size FROM block WHERE cid = ?'

    static final String SQL_ITEM_ATTR =
        'SELECT type, value FROM item_attr WHERE item_cid = ? AND path = ? AND truncated = 0 ORDER BY type, value'

    static final String SQL_RUN_ITEMS_WITH_ATTR =
        '''SELECT DISTINCT ci.item_cid FROM collection c
           JOIN collection_item ci ON ci.collection_cid = c.collection_cid
           JOIN item_attr a ON a.item_cid = ci.item_cid
           WHERE c.completion_cid = ? AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?
           ORDER BY ci.item_cid'''

    static final String SQL_OUTPUT_COLLECTIONS_OF_ITEM =
        """SELECT DISTINCT c.collection_cid, c.completion_cid FROM collection_item ci
           JOIN collection c ON c.collection_cid = ci.collection_cid
           WHERE ci.item_cid = ? AND c.kind = 'output' ORDER BY c.collection_cid"""

    static final String SQL_CLAIMS_LOGGED_IN =
        '''SELECT k.claim_cid, k.subject_cid, k.verb, k.attribute, k.value FROM claim k
           JOIN log_entry e ON e.cid = k.claim_cid WHERE e.member = ? ORDER BY k.claim_cid'''

    static final String SQL_CLAIM_SUPERSEDES_LOGGED_IN =
        '''SELECT s.claim_cid, s.superseded_cid FROM claim_supersedes s
           JOIN log_entry e ON e.cid = s.claim_cid WHERE e.member = ?'''

    static final String SQL_HANDOFFS_LOGGED_IN =
        '''SELECT h.selection_cid, h.cid FROM handoff h
           JOIN log_entry e ON e.cid = h.cid WHERE e.member = ? AND e.kind = 'handoff' ORDER BY h.selection_cid, h.cid'''

    static final String SQL_CLAIM =
        'SELECT subject_cid, verb, attribute, value, timestamp, asserted_by FROM claim WHERE claim_cid = ?'

    static final String SQL_CLAIM_SUPERSEDES = 'SELECT superseded_cid FROM claim_supersedes WHERE claim_cid = ?'

    private static final String META_WATERMARK = 'store_log_watermark'
    private static final String META_SCANNED = 'block_scan'
    /** The `block_scan` value of a scan that found no block of the scanned kinds (P3 plan T10); otherwise 'done'. */
    private static final String SCANNED_EMPTY = 'empty'
    private static final String META_STALE = 'stale'
    private static final String META_SEEDED = 'seeded_from'
    private static final String META_STORE_ID = 'store_id'
    private static final org.slf4j.Logger CONSOLE = org.slf4j.LoggerFactory.getLogger('nextflow.cas')

    /** A metadata block far larger than this is not one of ours; refuse to hold it. */
    private static final long MAX_BLOCK_BYTES = 64L * 1024 * 1024

    private static final SecureRandom RANDOM = new SecureRandom()

    static {
        try {
            Class.forName('org.sqlite.JDBC')
        }
        catch( ClassNotFoundException e ) {
            log.warn('the sqlite driver is not on the classpath; the index will not open', e)
        }
    }

    /**
     * Where the index sends its lines for the person at the terminal: the
     * missing-snapshot warning and the rebuild notice. CasSession points it at
     * its session's notices when it opens the index, so a command's lines go
     * to the command's stderr and a run's to the console.
     */
    static interface Notice {
        void warn(String message)
        void info(String message)
    }

    /** Without a session's notices, the nextflow.cas console logger. */
    static final Notice CONSOLE_NOTICE = new Notice() {
        @Override void warn(String message) { CONSOLE.warn(message) }
        @Override void info(String message) { CONSOLE.info(message) }
    }

    private final Path file
    private Connection connection
    private volatile Notice notice = CONSOLE_NOTICE

    /** Sends this index's terminal lines to {@code notice} instead of the console logger. */
    void setNotice(Notice notice) { this.notice = notice ?: CONSOLE_NOTICE }

    private Index(Path file, Connection connection) {
        this.file = file
        this.connection = connection
    }

    /** The file this index writes to. */
    Path getFile() { file }

    // ------------------------------------------------------------- lifecycle

    /** One monitor per cache file, so the openers in this JVM (the explore server's requests) take turns. */
    private static final ConcurrentHashMap<Path, Object> OPENING = new ConcurrentHashMap<Path, Object>()

    /**
     * Opens the index at {@code sqliteFile}, creating the schema when it is
     * new and recreating it from empty when the stored schema version is not
     * this build's. There is no migration path, by design.
     *
     * Never deletes a database another opener is creating or writing (P3
     * final review item 4). Openers in this JVM take turns per file. A new
     * cache is built whole under a temporary name and linked into place, so
     * the file at the path always has its schema. A file that reads as stale
     * or unreadable is asked again under the write lock before it is
     * recreated, which waits for a creator still committing its schema; a
     * lock that stays busy past the busy timeout throws, and nothing is
     * deleted.
     */
    static Index open(Path sqliteFile) {
        final Object turn = OPENING.computeIfAbsent(sqliteFile.toAbsolutePath().normalize()) { Path p -> new Object() }
        synchronized( turn ) {
            if( sqliteFile.parent != null )
                Files.createDirectories(sqliteFile.parent)
            for( int attempt = 1; ; attempt++ ) {
                if( Files.exists(sqliteFile) ) {
                    final Index index = openExisting(sqliteFile)
                    if( index != null )
                        return index
                }
                if( attempt == 2 )
                    throw new IllegalStateException("index at $sqliteFile was created, but does not open as schema version $SCHEMA_VERSION")
                publishNew(sqliteFile)
            }
        }
    }

    /** The cache at {@code sqliteFile} when it is current, else null once it is deleted. */
    private static Index openExisting(Path sqliteFile) {
        Connection existing = null
        boolean current = false
        String why = null
        try {
            existing = connect(sqliteFile)
            final int version = schemaVersionOf(existing)
            current = version == SCHEMA_VERSION
            if( !current )
                why = "has schema version $version, not $SCHEMA_VERSION"
        }
        catch( SQLException e ) {
            if( busy(e) ) {
                closeQuietly(existing)
                throw busyIndex(sqliteFile, e)
            }
            why = "could not be opened (${e.message})"
        }
        if( !current ) {
            closeQuietly(existing)
            existing = null
            if( stillStale(sqliteFile) )
                log.warn("index at $sqliteFile $why; recreating it")
            else {
                current = true
                try {
                    existing = connect(sqliteFile)
                }
                catch( SQLException e ) {
                    throw busy(e) ? busyIndex(sqliteFile, e) : new IllegalStateException("index at $sqliteFile could not be opened: ${e.message}", e)
                }
            }
        }
        // A cache-only table that cannot be made is no reason to drop the cache.
        if( current )
            return withCacheOnly(new Index(sqliteFile, existing))
        closeQuietly(existing)
        deleteDatabase(sqliteFile)
        return null
    }

    /**
     * Asks the file's schema version again holding the write lock, which waits
     * (the busy timeout) for a connection still committing a schema. True when
     * it is still not this build's, or the file is unreadable; a lock that
     * stays busy throws.
     */
    private static boolean stillStale(Path sqliteFile) {
        Connection c = null
        try {
            c = connect(sqliteFile)
            final Statement statement = c.createStatement()
            try {
                statement.execute('BEGIN IMMEDIATE')
                try {
                    return versionOrNone(c) != SCHEMA_VERSION
                }
                finally {
                    statement.execute('ROLLBACK')
                }
            }
            finally {
                statement.close()
            }
        }
        catch( SQLException e ) {
            if( busy(e) )
                throw busyIndex(sqliteFile, e)
            return true
        }
        finally {
            closeQuietly(c)
        }
    }

    /** The schema version, or -1 when the file has no schema_version table. */
    private static int versionOrNone(Connection connection) throws SQLException {
        final Statement look = connection.createStatement()
        try {
            final ResultSet rs = look.executeQuery("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'")
            if( !rs.next() || rs.getInt(1) == 0 )
                return -1
        }
        finally {
            look.close()
        }
        return schemaVersionOf(connection)
    }

    private static IllegalStateException busyIndex(Path sqliteFile, SQLException e) {
        return new IllegalStateException("index at $sqliteFile is busy: another process is creating or writing it (${e.message}); " +
            'it was left as it is, try again', e)
    }

    /**
     * Builds a new cache under a temporary name beside the file, the schema
     * and its version row in one transaction, then links it into place. When
     * another process put a cache there first, that one stands. A file system
     * without hard links gets a rename, made only when the path is still free.
     */
    private static void publishNew(Path sqliteFile) {
        final Path temp = sqliteFile.resolveSibling(sqliteFile.fileName.toString() + '.creating-' + token())
        try {
            final Connection created = connect(temp)
            try {
                createSchema(created)
            }
            finally {
                closeQuietly(created)
            }
            try {
                Files.createLink(sqliteFile, temp)
            }
            catch( FileAlreadyExistsException e ) {
                // Another process published its cache first; it is opened instead.
            }
            catch( UnsupportedOperationException | IOException e ) {
                if( !Files.exists(sqliteFile) )
                    Files.move(temp, sqliteFile, StandardCopyOption.ATOMIC_MOVE)
            }
        }
        finally {
            deleteDatabase(temp)
        }
    }

    /**
     * Tables this cache keeps for itself, outside {@link #ddl}, so a snapshot
     * never carries them and a seeded cache starts without them. Created if
     * absent, so a cache of this schema version gains them in place.
     *
     * `block_absent`: blocks a snapshot build of the member found gone from
     * its store (fix round 2, B1). Keyed by member as well as cid, because one
     * cache serves every member of a composition and a block one store lost
     * may be in another.
     *
     * `block_present`: blocks a held or pinned root reaches, under a run or
     * Selection that was ever released or deleted, that a snapshot build of
     * the member found still in its store (fix round 3). Such a block is not
     * asked about again while a held or pinned root reaches it, so a cold
     * cache pays for the check once. Every row of every member goes when any
     * Claim is ingested or newly logged, at a seed and at a forget: any Claim
     * can supersede a pin or a release.
     */
    static final List<String> CACHE_ONLY_DDL = [
        'CREATE TABLE IF NOT EXISTS block_absent(member TEXT, cid TEXT, PRIMARY KEY(member, cid))',
        'CREATE TABLE IF NOT EXISTS block_present(member TEXT, cid TEXT, PRIMARY KEY(member, cid))',
    ]

    /** False when CACHE_ONLY_DDL could not run at open: this index then remembers no absent or present block. */
    private boolean absentMemo = true

    /**
     * Runs CACHE_ONLY_DDL. When it fails (SQLITE_BUSY past the busy timeout,
     * a read-only file), the index still opens with every row it holds, and
     * only the absent-block memo is off: each snapshot build asks the store
     * again. One warning per file.
     */
    private static Index withCacheOnly(Index index) {
        try {
            final Statement statement = index.connection.createStatement()
            try {
                for( String sql : CACHE_ONLY_DDL )
                    statement.executeUpdate(sql)
            }
            finally {
                statement.close()
            }
        }
        catch( SQLException e ) {
            index.absentMemo = false
            warnOnce('cache-only:' + index.file, "index at ${index.file}: could not add its block_absent and block_present tables (${e.message}); " +
                'snapshots of this session ask the store again about every block a sweep may have taken')
        }
        return index
    }

    @Override
    void close() {
        closeQuietly(connection)
        connection = null
    }

    private static Connection connect(Path file) {
        final Connection connection = DriverManager.getConnection("jdbc:sqlite:${file.toAbsolutePath()}".toString())
        final Statement statement = connection.createStatement()
        try {
            // WAL so a run writing its rows does not block another reading them.
            statement.execute('PRAGMA journal_mode=WAL')
            statement.execute('PRAGMA synchronous=NORMAL')
            // WAL still serialises writers; without a busy timeout the second of
            // two runs finishing at once gets SQLITE_BUSY instead of waiting its
            // turn. A few seconds covers a run's ingest transaction.
            statement.execute('PRAGMA busy_timeout=5000')
        }
        finally {
            statement.close()
        }
        return connection
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection?.close()
        }
        catch( SQLException e ) {
            log.debug("closing the index connection failed: ${e.message}")
        }
    }

    /** Removes the database and the sidecars WAL mode leaves beside it. */
    private static void deleteDatabase(Path file) {
        final String name = file.fileName.toString()
        for( String suffix : ['', '-wal', '-shm', '-journal'] )
            Files.deleteIfExists(file.resolveSibling(name + suffix))
    }

    private static int schemaVersionOf(Connection connection) {
        final Statement statement = connection.createStatement()
        try {
            final ResultSet rs = statement.executeQuery('SELECT version FROM schema_version')
            return rs.next() ? rs.getInt(1) : -1
        }
        finally {
            statement.close()
        }
    }

    private static boolean busy(SQLException e) {
        // The primary result code; extended codes (SQLITE_BUSY_SNAPSHOT, ...) keep it in the low byte.
        return (e.errorCode & 0xff) == SQLITE_BUSY
    }

    private static final int SQLITE_BUSY = 5

    /** Every CREATE statement of the schema of DESIGN.md §12 and block explorer spec section 11, in order. */
    static List<String> ddl() {
        return [
            'CREATE TABLE schema_version(version INTEGER NOT NULL)',
            '''CREATE TABLE run(
                 completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,
                 commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,
                 status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)''',
            'CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)',
            'CREATE INDEX run_nf_run_hash ON run(nf_run_hash)',
            'CREATE INDEX run_manifest_cid ON run(manifest_cid)',
            // kind: 'output' | 'selection'. A Selection has no run and no output name.
            '''CREATE TABLE collection(
                 collection_cid TEXT PRIMARY KEY, kind TEXT, completion_cid TEXT, output_name TEXT, asserted_by TEXT)''',
            'CREATE TABLE item(item_cid TEXT PRIMARY KEY)',
            // One row per (collection, item, via); via_cid is NULL for an Output
            // Collection and for a Selection item chosen by query.
            'CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT, via_cid TEXT)',
            'CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)',
            'CREATE INDEX collection_item_collection ON collection_item(collection_cid)',
            'CREATE INDEX collection_item_item ON collection_item(item_cid)',
            'CREATE TABLE selection_child(parent_cid TEXT, child_cid TEXT)',
            'CREATE INDEX selection_child_child ON selection_child(child_cid)',
            'CREATE TABLE selection_derived(selection_cid TEXT, derived_from_cid TEXT)',
            '''CREATE TABLE producer(
                 content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)''',
            'CREATE INDEX producer_content_cid ON producer(content_cid)',
            'CREATE TABLE consumer(content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT)',
            'CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)',
            'CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)',
            // written_at: ISO-8601 UTC, milliseconds; the earliest entry per (cid, member).
            'CREATE TABLE log_entry(cid TEXT, kind TEXT, member TEXT, written_at TEXT)',
            'CREATE UNIQUE INDEX log_entry_cid_member ON log_entry(cid, member)',
            'CREATE INDEX log_entry_kind_written_at ON log_entry(kind, written_at DESC)',
            '''CREATE TABLE claim(
                 claim_cid TEXT PRIMARY KEY, subject_cid TEXT, verb TEXT, attribute TEXT, value TEXT,
                 timestamp TEXT, asserted_by TEXT)''',
            'CREATE INDEX claim_subject ON claim(subject_cid)',
            'CREATE TABLE claim_supersedes(claim_cid TEXT, superseded_cid TEXT)',
            'CREATE INDEX claim_supersedes_superseded ON claim_supersedes(superseded_cid)',
            '''CREATE TABLE claim_current(
                 subject_cid TEXT, attribute TEXT, value TEXT, claim_cid TEXT, conflicted INTEGER)''',
            'CREATE INDEX claim_current_subject ON claim_current(subject_cid, attribute)',
            'CREATE TABLE missing(have_cid TEXT, needed_cid TEXT)',
            '''CREATE TABLE nf_record(
                 key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,
                 labels_json TEXT, block_cid TEXT)''',
            // Not in §12's schema: the watermark and the stale mark, which are
            // this build's own bookkeeping rather than indexed block content.
            'CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)',
            // Schema 4, storage accounting. Ingest fills block and block_ref:
            // every block an item reaches through its Leaves and
            // DirectoryManifests, and which item or manifest names it.
            // codec: 'raw' | 'dag-cbor'.
            'CREATE TABLE block(cid TEXT PRIMARY KEY, size INTEGER, codec TEXT)',
            'CREATE TABLE block_ref(owner_cid TEXT, cid TEXT, name TEXT)',
            'CREATE INDEX block_ref_owner ON block_ref(owner_cid)',
            // The rest are written by the snapshot build only and stay empty in the cache.
            'CREATE TABLE root(id INTEGER PRIMARY KEY, cid TEXT, kind TEXT, asserted_by TEXT, state TEXT, since TEXT)',
            'CREATE INDEX root_cid ON root(cid)',
            'CREATE INDEX root_kind ON root(kind)',
            '''CREATE TABLE root_stats(root_id INTEGER PRIMARY KEY, reachable_bytes INTEGER, reachable_blocks INTEGER,
                 exclusive_bytes INTEGER, exclusive_blocks INTEGER, pinned_bytes INTEGER, partial INTEGER)''',
            'CREATE TABLE root_shared(root_id INTEGER, other_root_id INTEGER, bytes INTEGER)',
            'CREATE INDEX root_shared_root ON root_shared(root_id)',
            '''CREATE TABLE store_stats(stored_bytes INTEGER, stored_blocks INTEGER, referenced_bytes INTEGER,
                 pinned_bytes INTEGER, held_bytes INTEGER, releasing_bytes INTEGER, shared_bytes INTEGER, partial INTEGER)''',
            '''CREATE TABLE asserter_stats(asserted_by TEXT PRIMARY KEY, reachable_bytes INTEGER, exclusive_bytes INTEGER,
                 pinned_bytes INTEGER, releasing_bytes INTEGER)''',
            'CREATE TABLE pipeline_stats(pipeline TEXT PRIMARY KEY, runs INTEGER, bytes INTEGER, blocks INTEGER, releasing_bytes INTEGER)',
            'CREATE TABLE collection_size(collection_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER)',
            'CREATE TABLE item_size(item_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER)',
            'CREATE TABLE kind_stats(kind TEXT PRIMARY KEY, files INTEGER, bytes INTEGER)',
            'CREATE TABLE pinned_overlap(subject_cid TEXT, asserted_by TEXT, bytes INTEGER)',
            'CREATE INDEX pinned_overlap_subject ON pinned_overlap(subject_cid)',
            // What a `transfer` entry fills (P3 plan 1.5). `claims` is the
            // Transfer's categories comma-joined, `at` its timestamp; `role` is
            // 'root' for a root asked for and 'run' for one of its `runs`.
            '''CREATE TABLE transfer(
             cid TEXT PRIMARY KEY, from_member TEXT, to_member TEXT, asserted_by TEXT, depth TEXT, claims TEXT,
             blocks INTEGER, copied INTEGER, bytes INTEGER, at TEXT)''',
            'CREATE TABLE transfer_root(transfer_cid TEXT, root_cid TEXT, role TEXT)',
            'CREATE INDEX transfer_root_root ON transfer_root(root_cid)',
            // Explorer v0.3 P4 (plan 1.3-1.5). `project`: one row per Project
            // block a `project` entry announced. `project_stats`: per project,
            // written by the snapshot build only, from the current membership
            // Claims. `run_input`: one row per InputSet input of an indexed
            // run; `address` is null when the run did not hash it, `position`
            // is the input's index in the InputSet's `inputs`. `handoff`:
            // one row per Handoff block a `handoff` entry announced.
            // `claim_current_attribute_value`: "members of a project" without a scan.
            'CREATE TABLE project(project_cid TEXT PRIMARY KEY, name TEXT, description TEXT, created_by TEXT, created_at TEXT)',
            '''CREATE TABLE project_stats(project_cid TEXT PRIMARY KEY, reachable_bytes INTEGER, pinned_bytes INTEGER,
             held_bytes INTEGER, releasing_bytes INTEGER, roots INTEGER)''',
            'CREATE TABLE run_input(completion_cid TEXT, role TEXT, name TEXT, address TEXT, size INTEGER, sample TEXT, position INTEGER)',
            'CREATE INDEX run_input_address ON run_input(address, role, completion_cid)',
            'CREATE INDEX run_input_completion ON run_input(completion_cid)',
            '''CREATE TABLE handoff(
             cid TEXT PRIMARY KEY, template TEXT, selection_cid TEXT, project_cid TEXT, asserted_by TEXT, at TEXT)''',
            'CREATE INDEX claim_current_attribute_value ON claim_current(attribute, value)',
            // Cross-run lineage (spec §5.4), keyed by position in the run's
            // InputSet. `input_origin`: one row per (input, Origin), the
            // collection and Selection null when the Origin has none.
            // `item_input`: one row per item of the run's ItemLineage,
            // `positions` the JSON array of its input positions. `item_task`:
            // one row per (item, task that made it).
            'CREATE TABLE input_origin(completion_cid TEXT, position INTEGER, item_cid TEXT, collection_cid TEXT, selection_cid TEXT)',
            'CREATE INDEX input_origin_item ON input_origin(item_cid)',
            'CREATE INDEX input_origin_completion ON input_origin(completion_cid, position)',
            'CREATE TABLE item_input(completion_cid TEXT, item_cid TEXT, positions TEXT)',
            'CREATE INDEX item_input_item ON item_input(item_cid)',
            'CREATE INDEX item_input_completion ON item_input(completion_cid)',
            'CREATE TABLE item_task(completion_cid TEXT, item_cid TEXT, task_hash TEXT, process TEXT, tag TEXT)',
            'CREATE INDEX item_task_item ON item_task(item_cid)',
            'CREATE INDEX item_task_completion ON item_task(completion_cid)',
        ]
    }

    /** The schema of DESIGN.md §12, verbatim in its column names, and its version row, in one transaction. */
    private static void createSchema(Connection connection) {
        connection.setAutoCommit(false)
        try {
            final Statement statement = connection.createStatement()
            try {
                for( String sql : ddl() )
                    statement.executeUpdate(sql)
            }
            finally {
                statement.close()
            }
            final PreparedStatement version = connection.prepareStatement('INSERT INTO schema_version(version) VALUES (?)')
            try {
                version.setInt(1, SCHEMA_VERSION)
                version.executeUpdate()
            }
            finally {
                version.close()
            }
            connection.commit()
        }
        catch( SQLException e ) {
            connection.rollback()
            throw e
        }
        finally {
            connection.setAutoCommit(true)
        }
    }

    // ---------------------------------------------------------------- ingest

    /**
     * Indexes one RunCompletion and its closure: the RunManifest, every
     * OutputCollection, every OutputItem and every Leaf, the blocks the
     * Leaves reach through DirectoryManifests, and its InputSet and
     * ItemLineage when it links them ({@link #ingestInputSet},
     * {@link #ingestItemLineage}). Idempotent: the run's rows are
     * replaced, not added to. A referenced block that has not arrived leaves a
     * `missing(have, needed)` row and the rest of the run is indexed.
     */
    void ingestRun(BlockStore store, Cid completion, String member) {
        final Map completionBlock = readBlock(store, completion, 'RunCompletion')
        withTransaction {
            clearRun(completion)
            if( completionBlock == null ) {
                warnOnce(completion.toString(), "run completion $completion has not arrived; nothing of it can be indexed until it does")
                insertMissing(null, completion)
                return
            }
            final Cid manifestCid = asCid(completionBlock.get('run'))
            Map manifest = null
            if( manifestCid != null ) {
                manifest = readBlock(store, manifestCid, 'RunManifest')
                if( manifest == null )
                    insertMissing(completion, manifestCid)
            }
            insertRun(completion, manifestCid, manifest, completionBlock, member)
            for( Object link : asList(completionBlock.get('collections')) ) {
                final Cid collectionCid = asCid(link)
                if( collectionCid == null )
                    continue
                ingestCollection(store, completion, collectionCid)
            }
            final Cid inputSet = asCid(completionBlock.get('input_set'))
            final boolean positioned = inputSet == null || ingestInputSet(store, completion, inputSet)
            final Cid itemLineage = asCid(completionBlock.get('item_lineage'))
            if( itemLineage != null && positioned )
                ingestItemLineage(store, completion, itemLineage, inputSet)
            clearAbsentNamedBy(store, completion, member)
        }
    }

    private void ingestCollection(BlockStore store, Cid completion, Cid collectionCid) {
        final Map collection = readBlock(store, collectionCid, 'OutputCollection')
        if( collection == null ) {
            insertMissing(completion, collectionCid)
            return
        }
        update('INSERT OR REPLACE INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, ?, ?, ?, ?)',
            [collectionCid.toString(), 'output', completion.toString(), text(collection.get('name')), text(collection.get('asserted_by'))])
        for( Object link : asList(collection.get('items')) ) {
            // A null entry is a hole: Nextflow handed us a null item and §6
            // keeps its position rather than compacting it away.
            final Cid itemCid = asCid(link)
            if( itemCid == null )
                continue
            update('INSERT OR IGNORE INTO item(item_cid) VALUES (?)', [itemCid.toString()])
            update('INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, NULL)',
                [collectionCid.toString(), itemCid.toString()])
            final Map item = readBlock(store, itemCid, 'OutputItem')
            if( item == null ) {
                // The membership arrived even though the item did not.
                insertMissing(collectionCid, itemCid)
                continue
            }
            ingestItem(store, itemCid, collectionCid, completion, item.get('value'))
        }
    }

    private void ingestItem(BlockStore store, Cid itemCid, Cid collectionCid, Cid completion, Object value) {
        // Attributes are a function of the item block alone, so they are
        // rewritten rather than accumulated across the runs that share it.
        update('DELETE FROM item_attr WHERE item_cid = ?', [itemCid.toString()])
        for( AttrRow row : MetadataView.flatten(MetadataView.of(value)) )
            update('INSERT INTO item_attr(item_cid, path, type, value, truncated) VALUES (?, ?, ?, ?, ?)',
                [itemCid.toString(), row.path, row.type, row.value, (Object) row.truncated])
        // block_ref rows are facts about an immutable block, written once per
        // owner: another run sharing the item, or this run again, adds none.
        // A directory that had not arrived is filled by retryMissingContent.
        final boolean refsWritten = hasRefs(itemCid)
        final Set<Cid> seen = new HashSet<Cid>()
        for( Map leaf : leavesOf(value) ) {
            final Cid address = asCid(leaf.get('address'))
            // An unaddressed, declined or never published leaf has nothing
            // content-derived to answer with, so it is not a producer.
            if( address == null )
                continue
            update('INSERT INTO producer(content_cid, item_cid, collection_cid, completion_cid, filename) VALUES (?, ?, ?, ?, ?)',
                [address.toString(), itemCid.toString(), collectionCid.toString(), completion.toString(),
                 text(leaf.get('name'))])
            if( !refsWritten )
                ingestContent(store, itemCid, address, text(leaf.get('name')), asLong(leaf.get('size')), seen)
        }
    }

    /** The meta key prefixes of a run's wait records (see {@link #readRunRecord}); IndexSnapshot and the seed copy them with the run. */
    @PackageScope static final String INPUTSET_WAIT = 'inputset_wait:'
    @PackageScope static final String ITEMLINEAGE_WAIT = 'itemlineage_wait:'

    /** A record a RunCompletion links beside its collections, as {@link #readRunRecord} waits for it and warns about it. */
    @Canonical
    private static final class RunRecord {
        String kind
        String waitPrefix
        /** How a warning names the record, and what a run is indexed without when it is malformed. */
        String what
        String without
    }

    private static final RunRecord INPUT_SET_RECORD = new RunRecord(Records.INPUT_SET, INPUTSET_WAIT, 'input set', 'its inputs')
    private static final RunRecord ITEM_LINEAGE_RECORD = new RunRecord(Records.ITEM_LINEAGE, ITEMLINEAGE_WAIT, 'item lineage', 'item lineage')

    /**
     * A run's InputSet or ItemLineage, parsed, or null when there is none to
     * index for now. Inputs and lineage are what the run read, not what it
     * owns or the store holds, so neither the record nor anything it names is
     * a `missing`, `block` or `block_ref` row (R2): it says nothing of stored
     * bytes and must not make storage figures partial. One not arrived, or one
     * that cannot be read for now, leaves a wait record, meta
     * `<record.waitPrefix><completion>` = its cid, which {@link #retryWaiting} asks
     * about at every catch-up of the run's member. One that is there but does
     * not parse is warned about once and skipped, with no wait record. Clears
     * the run's previous wait record first; the caller owns the transaction.
     */
    private <T> T readRunRecord(BlockStore store, Cid completion, Cid cid, RunRecord record, Closure<T> parse) {
        final String wait = record.waitPrefix + completion
        update('DELETE FROM meta WHERE key = ?', [(Object) wait])
        T parsed = null
        String refused = null
        try {
            final Map block = readBlock(store, cid, record.kind, true)
            if( block != null ) {
                try {
                    parsed = parse.call(block)
                }
                catch( RuntimeException e ) {
                    refused = e.message
                }
            }
            else if( store.has(cid) )
                refused = "not an ${record.kind} block of at most ${MAX_BLOCK_BYTES} bytes"
        }
        catch( IOException | UncheckedIOException e ) {
            warnOnce(cid.toString(), "${record.what} ${cid} of run ${completion} could not be read (${e.message}); it will be retried")
            setMeta(wait, cid.toString())
            return null
        }
        if( refused != null ) {
            warnOnce("${record.kind}-malformed:${cid}".toString(), "${record.what} ${cid} of run ${completion} is not a readable ${record.kind} block (${refused}); " +
                "the run is indexed without ${record.without}")
            return null
        }
        if( parsed == null )
            setMeta(wait, cid.toString())
        return parsed
    }

    /**
     * The run's InputSet (P4 plan 1.3, cross-run lineage spec §5.4): one
     * `run_input` row per input at its position in `inputs`, one
     * `input_origin` row per Origin of each, and one `consumer` row per
     * distinct (address, name, role) of the addressed ones, `how` the role.
     * Waits and warns as {@link #readRunRecord} says; the run is indexed
     * either way. A block whose inputs are not in canonical order (or hold
     * two equal) is warned about once and nothing of it is written: an
     * ItemLineage's positions index the block's list, the rows would index
     * the sorted one. False then, and the run's lineage rows and wait record
     * go too, so the caller indexes no lineage; true otherwise. The caller
     * owns the transaction.
     */
    private boolean ingestInputSet(BlockStore store, Cid completion, Cid inputSetCid) {
        final String c = completion.toString()
        update('DELETE FROM run_input WHERE completion_cid = ?', [(Object) c])
        update('DELETE FROM input_origin WHERE completion_cid = ?', [(Object) c])
        update('DELETE FROM consumer WHERE completion_cid = ?', [(Object) c])
        boolean reordered = false
        final InputSet set = readRunRecord(store, completion, inputSetCid, INPUT_SET_RECORD) { Map block ->
            final InputSet parsed = InputSet.fromCbor(block)
            reordered = !parsed.sameOrderAs(block)
            parsed
        }
        if( reordered ) {
            warnOnce("input-set-order:${inputSetCid}".toString(), "input set ${inputSetCid} of run ${completion} does not list its inputs in canonical order, " +
                "so its positions cannot be trusted; the run is indexed without its inputs or item lineage")
            update('DELETE FROM item_input WHERE completion_cid = ?', [(Object) c])
            update('DELETE FROM item_task WHERE completion_cid = ?', [(Object) c])
            update('DELETE FROM meta WHERE key = ?', [(Object) (ITEMLINEAGE_WAIT + c)])
            return false
        }
        if( set == null )
            return true
        final Set<List<String>> consumers = new LinkedHashSet<List<String>>()
        for( int position = 0; position < set.inputs.size(); position++ ) {
            final InputSet.Input input = set.inputs.get(position)
            update('INSERT INTO run_input(completion_cid, role, name, address, size, sample, position) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [(Object) c, input.role, input.name, input.address?.toString(), input.size, input.sample, position])
            for( InputSet.Origin origin : input.from ?: Collections.<InputSet.Origin> emptyList() )
                update('INSERT INTO input_origin(completion_cid, position, item_cid, collection_cid, selection_cid) VALUES (?, ?, ?, ?, ?)',
                    [(Object) c, position, origin.item.toString(), origin.collection?.toString(), origin.selection?.toString()])
            if( input.address != null )
                consumers.add([input.address.toString(), input.name, input.role])
        }
        for( List<String> each : consumers )
            update('INSERT INTO consumer(content_cid, completion_cid, name, how) VALUES (?, ?, ?, ?)', [(Object) each[0], c, each[1], each[2]])
        return true
    }

    /**
     * The run's ItemLineage (cross-run lineage spec §5.4): one `item_input`
     * row per item, its input positions as a JSON array, and one `item_task`
     * row per task that made it. Its positions index the InputSet it names,
     * so a lineage naming another InputSet than the run's
     * ({@code inputSetCid}) is warned about once and nothing of it is
     * written. Waits and warns as {@link #readRunRecord} says; the run is
     * indexed either way. The caller owns the transaction.
     */
    private void ingestItemLineage(BlockStore store, Cid completion, Cid lineageCid, Cid inputSetCid) {
        final String c = completion.toString()
        update('DELETE FROM item_input WHERE completion_cid = ?', [(Object) c])
        update('DELETE FROM item_task WHERE completion_cid = ?', [(Object) c])
        final ItemLineage lineage = readRunRecord(store, completion, lineageCid, ITEM_LINEAGE_RECORD) { Map block -> ItemLineage.fromCbor(block) }
        if( lineage == null )
            return
        if( lineage.inputSet != inputSetCid ) {
            warnOnce("item-lineage-input-set:${lineageCid}".toString(), "item lineage ${lineageCid} of run ${completion} names input set ${lineage.inputSet}, " +
                "not the run's ${inputSetCid}; the run is indexed without item lineage")
            return
        }
        for( ItemLineage.Derived derived : lineage.items ) {
            final String item = derived.item.toString()
            update('INSERT INTO item_input(completion_cid, item_cid, positions) VALUES (?, ?, ?)', [(Object) c, item, JsonOutput.toJson(derived.inputs)])
            for( ItemLineage.TaskRef task : derived.tasks )
                update('INSERT INTO item_task(completion_cid, item_cid, task_hash, process, tag) VALUES (?, ?, ?, ?, ?)',
                    [(Object) c, item, task.hash, task.process, task.tag])
        }
    }

    /**
     * The InputSet wait records of the member's runs ({@link #readRunRecord}).
     * Nothing else of a run is asked about here: a missing manifest or
     * OutputCollection waits as before.
     */
    private void retryMissingInputSets(BlockStore store, String member) {
        retryWaiting(store, member, INPUTSET_WAIT) { Cid completion, Cid inputSet -> ingestInputSet(store, completion, inputSet) }
    }

    /** The ItemLineage wait records of the member's runs, each checked against the InputSet its RunCompletion links. */
    private void retryMissingItemLineages(BlockStore store, String member) {
        retryWaiting(store, member, ITEMLINEAGE_WAIT) { Cid completion, Cid lineage ->
            final Map completionBlock = readBlock(store, completion, 'RunCompletion')
            // An indexed run's completion has arrived; one unreadable for now keeps its wait record.
            if( completionBlock != null )
                ingestItemLineage(store, completion, lineage, asCid(completionBlock.get('input_set')))
        }
    }

    /**
     * Each wait record under {@code prefix} of the member's runs: its record
     * is asked about at every catch-up of the member, one `has` each, and
     * handed to {@code ingest} (completion, record cid) in its own
     * transaction once it is there, which clears the wait record (a malformed
     * one clears it too, with its warning).
     */
    private void retryWaiting(BlockStore store, String member, String prefix, Closure ingest) {
        final List<Cid[]> waiting = new ArrayList<Cid[]>()
        // The GLOB prefix is searched in meta's key index; a run is looked up by its key.
        query('''SELECT r.completion_cid, m.value FROM meta m JOIN run r ON r.completion_cid = substr(m.key, ?)
                 WHERE m.key GLOB ? AND (r.member = ? OR r.completion_cid IN (SELECT cid FROM log_entry WHERE member = ? AND kind = 'run'))
                 ORDER BY r.completion_cid''', [(Object) (prefix.length() + 1), prefix + '*', member, member]) { ResultSet rs ->
            waiting.add([Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2))] as Cid[])
        }
        for( Cid[] each : waiting ) {
            final Cid completion = each[0]
            final Cid record = each[1]
            try {
                if( !store.has(record) )
                    continue
            }
            catch( IOException | UncheckedIOException e ) {
                warnOnce(record.toString(), "block ${record} is still unreachable (${e.message}); will retry")
                continue
            }
            withTransaction { ingest.call(completion, record) }
        }
    }

    /** Records that {@code owner} names {@code address}, then the block itself. */
    private void ingestContent(BlockStore store, Cid owner, Cid address, String name, Long size, Set<Cid> seen) {
        update('INSERT INTO block_ref(owner_cid, cid, name) VALUES (?, ?, ?)', [owner.toString(), address.toString(), name])
        ingestBlock(store, owner, address, size, seen)
    }

    /**
     * Records the block at {@code address}, which {@code owner} names. A raw
     * block is a file, sized by the Leaf or entry when it says, else by the
     * store when the block is there, else NULL. A dag-cbor one is read as a
     * DirectoryManifest and its entries recorded the same way, each manifest
     * once: `seen`, and the manifest's own block row, written only once it
     * has been read as a DirectoryManifest, stop a cycle and a second read.
     * One that has not arrived leaves `missing(owner, manifest)`, and so does
     * one over MAX_BLOCK_BYTES, which is not read: its files are unknown.
     */
    private void ingestBlock(BlockStore store, Cid owner, Cid address, Long size, Set<Cid> seen) {
        if( address.isRaw() ) {
            insertBlock(address, size != null ? size : sizeIfPresent(store, address), 'raw')
            return
        }
        if( !address.isDagCbor() || !seen.add(address) )
            return
        if( wasRead(address) || hasRefs(address) ) {
            deleteMissing(owner, address)
            return
        }
        final Map manifest
        try {
            manifest = readBlock(store, address, Records.DIRECTORY_MANIFEST, true)
        }
        catch( IOException | UncheckedIOException e ) {
            // There but unreadable for now: missing, so retryMissingContent
            // tries again. The owner's block_ref rows are written already.
            warnOnce(address.toString(), "directory manifest ${address} could not be read (${e.message}); it will be retried")
            if( !isMissing(owner, address) )
                insertMissing(owner, address)
            return
        }
        if( manifest == null ) {
            if( !store.has(address) ) {
                if( !isMissing(owner, address) )
                    insertMissing(owner, address)
                return
            }
            // There, but nothing readBlock could read. No block row, so the
            // next owner reads it again: the null may have been a race with
            // the store, or a limit a later build lifts. One over the limit
            // names files nobody knows, so its owner waits for it.
            if( store.size(address) > MAX_BLOCK_BYTES && !isMissing(owner, address) )
                insertMissing(owner, address)
            return
        }
        deleteMissing(owner, address)
        insertBlock(address, store.size(address), 'dag-cbor')
        for( Object each : asList(manifest.get('entries')) ) {
            if( !(each instanceof Map) )
                continue
            final Map entry = (Map) each
            // Only a directory entry (a manifest) and a regular one (a raw
            // file; `executable` at schema 1) carry an address, so a symlink
            // or an unresolvable entry is skipped, as web/src/sizes.js does.
            final Cid entryAddress = asCid(entry.get('address'))
            if( entryAddress != null )
                ingestContent(store, address, entryAddress, text(entry.get('name')), asLong(entry.get('size')), seen)
        }
    }

    /**
     * A DirectoryManifest that had not arrived when its owner was indexed
     * left `missing(owner, manifest)`. Retried on every catch-up, so the
     * directory's files are recorded once the manifest lands. One over
     * MAX_BLOCK_BYTES is looked at again too (a HEAD for its size, never a
     * read), so a limit a later build lifts lets it in.
     */
    private void retryMissingContent(BlockStore store) {
        final List<Cid[]> waiting = new ArrayList<Cid[]>()
        query('''SELECT DISTINCT m.have_cid, m.needed_cid FROM missing m
                 JOIN block_ref r ON r.owner_cid = m.have_cid AND r.cid = m.needed_cid''', []) { ResultSet rs ->
            waiting.add([Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2))] as Cid[])
        }
        for( Cid[] each : waiting ) {
            try {
                if( store.has(each[1]) )
                    withTransaction { ingestBlock(store, each[0], each[1], null, new HashSet<Cid>()) }
            }
            catch( IOException | UncheckedIOException e ) {
                warnOnce(each[1].toString(), "directory manifest ${each[1]} is still unreachable (${e.message}); will retry")
            }
        }
    }

    /** True when a dag-cbor block row says the manifest was read: ingestBlock writes one only then. */
    private boolean wasRead(Cid cid) {
        return anyRow("SELECT 1 FROM block WHERE cid = ? AND codec = 'dag-cbor' LIMIT 1", [(Object) cid.toString()])
    }

    private boolean hasRefs(Cid owner) {
        boolean found = false
        query('SELECT 1 FROM block_ref WHERE owner_cid = ? LIMIT 1', [owner.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /** The first known size wins: a later size only fills a NULL. */
    private void insertBlock(Cid cid, Long size, String codec) {
        update('''INSERT INTO block(cid, size, codec) VALUES (?, ?, ?)
                  ON CONFLICT(cid) DO UPDATE SET size = excluded.size WHERE block.size IS NULL''',
            [cid.toString(), (Object) size, codec])
    }

    private static Long sizeIfPresent(BlockStore store, Cid cid) {
        return store.has(cid) ? store.size(cid) : null
    }

    private boolean isMissing(Cid have, Cid needed) {
        boolean found = false
        query('SELECT 1 FROM missing WHERE have_cid = ? AND needed_cid = ? LIMIT 1', [have.toString(), needed.toString()]) { ResultSet rs -> found = true }
        return found
    }

    private void deleteMissing(Cid have, Cid needed) {
        update('DELETE FROM missing WHERE have_cid = ? AND needed_cid = ?', [have.toString(), needed.toString()])
    }

    private void insertRun(Cid completion, Cid manifestCid, Map manifest, Map completionBlock, String member) {
        update('''INSERT OR REPLACE INTO run(
                    completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash,
                    session_id, run_name, asserted_by, status, possibly_incomplete, finished_at, member)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', [
            completion.toString(),
            manifestCid?.toString(),
            manifest == null ? null : text(manifest.get('pipeline')),
            manifest == null ? null : text(manifest.get('revision')),
            manifest == null ? null : text(manifest.get('commit_id')),
            manifest == null ? null : text(manifest.get('nf_run_hash')),
            manifest == null ? null : text(manifest.get('session_id')),
            manifest == null ? null : text(manifest.get('run_name')),
            text(completionBlock.get('asserted_by')),
            text(completionBlock.get('status')),
            (Object) (truth(completionBlock.get('possibly_incomplete')) ? 1 : 0),
            text(completionBlock.get('finished_at')),
            member,
        ])
    }

    /**
     * Everything this run asserted, so an ingest can replace rather than add.
     * block and block_ref rows describe immutable blocks, not the run, and stay.
     */
    private void clearRun(Cid completion) {
        final String text = completion.toString()
        update('''DELETE FROM missing
                  WHERE have_cid = ?
                     OR (have_cid IS NULL AND needed_cid = ?)
                     OR have_cid IN (SELECT collection_cid FROM collection WHERE completion_cid = ?)''',
            [text, text, text])
        update('DELETE FROM collection_item WHERE collection_cid IN (SELECT collection_cid FROM collection WHERE completion_cid = ?)', [text])
        update('DELETE FROM producer WHERE completion_cid = ?', [text])
        update('DELETE FROM consumer WHERE completion_cid = ?', [text])
        update('DELETE FROM run_input WHERE completion_cid = ?', [text])
        update('DELETE FROM input_origin WHERE completion_cid = ?', [text])
        update('DELETE FROM item_input WHERE completion_cid = ?', [text])
        update('DELETE FROM item_task WHERE completion_cid = ?', [text])
        update('DELETE FROM meta WHERE key = ?', [(Object) (INPUTSET_WAIT + text)])
        update('DELETE FROM meta WHERE key = ?', [(Object) (ITEMLINEAGE_WAIT + text)])
        update('DELETE FROM collection WHERE completion_cid = ?', [text])
        update('DELETE FROM run WHERE completion_cid = ?', [text])
    }

    private void insertMissing(Cid have, Cid needed) {
        update('INSERT INTO missing(have_cid, needed_cid) VALUES (?, ?)', [have?.toString(), needed.toString()])
    }

    /**
     * Ingests every Store Log entry this index has not seen, oldest first,
     * then advances the watermark to the newest entry listed. Re-reads the
     * overlap window before the watermark (StoreLog.OVERLAP_MILLIS) so an entry
     * written behind it is not skipped, and skips runs already indexed so the
     * overlap costs a listing, never a block read. `run`, `selection`,
     * `claim`, `transfer`, `project` and `handoff` entries are ingested;
     * `forget` entries are applied.
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member) {
        catchUp(store, storeLog, member, store)
    }

    /**
     * The 3-arg {@code catchUp}, with {@code holders} the store a `forget`
     * entry asks whether a listed cid is still held: the whole composition,
     * so a cid another member holds is not forgotten (DESIGN.md §20 decision 8).
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member, BlockStore holders) {
        // The watermark is per member: in a composition each member's log
        // advances independently (DESIGN.md §12).
        scanOnce(store, member)
        retryMissingLogged(store, member, holders)
        retryMissingContent(store)
        retryMissingInputSets(store, member)
        retryMissingItemLineages(store, member)
        final String key = watermarkKey(member)
        final List<StoreLogEntry> entries = storeLog.entriesSince(meta(key))
        if( !entries )
            return
        // entriesSince is newest first; ingest in the order they were written.
        for( int i = entries.size() - 1; i >= 0; i-- )
            ingestEntry(store, entries[i], member, holders)
        setMeta(key, entries[0].name)
    }

    /** Logs one Store Log entry for {@code member} and ingests its block unless already indexed. */
    private void ingestEntry(BlockStore store, StoreLogEntry entry, String member, BlockStore holders) {
        logEntry(entry, member)
        ingestLogged(store, entry.kind, entry.cid, member, holders)
    }

    private void logEntry(StoreLogEntry entry, String member) {
        // A Claim another member's log brought in is indexed already; logged here too, it now counts for
        // this member's snapshot and may change what is held there, so its entry and the drop go together.
        if( entry.kind == StoreLogKind.CLAIM && firstLogEntry(entry.cid, member) == null ) {
            withTransaction {
                recordLogEntry(entry, member)
                dropPresent()
            }
        }
        else
            recordLogEntry(entry, member)
    }

    /** Why a snapshot could not seed the cache: absent, schema_version <n>, unreadable (ticket 04 decision 2). */
    static class SnapshotUnusable extends Exception {
        final String reason
        SnapshotUnusable(String reason, Throwable cause = null) { super(reason, cause); this.reason = reason }
    }

    String metaValue(String key) { meta(key) }

    /** Sets a cache meta value, or removes it when {@code value} is null. The snapshot build keeps its per-member notes here (fix round 2, B2). */
    @PackageScope
    void setMetaValue(String key, String value) {
        if( value == null )
            update('DELETE FROM meta WHERE key = ?', [(Object) key])
        else
            setMeta(key, value)
    }

    /** How many run rows a snapshot of {@code member} would copy: the same rows as IndexSnapshot's COPY_RUNS. */
    @PackageScope
    int memberRunCount(String member) {
        final int[] n = new int[1]
        query('''SELECT count(*) FROM run
                 WHERE member = ? OR completion_cid IN (SELECT cid FROM log_entry WHERE member = ? AND kind = 'run')''',
            [(Object) member, member]) { ResultSet rs -> n[0] = rs.getInt(1) }
        return n[0]
    }

    /**
     * catchUp, seeding first when this index has never caught `member` up
     * (ticket 04 decisions 1, 6): its snapshot's rows, its watermark, and the
     * block scan marked done, so only the Store Log tail (with the 10-minute
     * overlap) is read. Without a usable snapshot, the full scan, with a
     * warning when the member's Store Log is not empty.
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir) {
        catchUp(store, storeLog, member, snapshots, tempDir, Collections.<String> emptySet(), store)
    }

    /**
     * As the 5-arg {@code catchUp}, but told which Store Log entries this run
     * itself just wrote (its own writable member's, only): those don't count
     * as evidence a snapshot should exist, so a brand-new store's first run
     * does not warn (ticket 18, decision from the sarek measurement).
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir, Set<String> quietFor) {
        catchUp(store, storeLog, member, snapshots, tempDir, quietFor, store)
    }

    /**
     * As the 6-arg {@code catchUp}, with {@code holders} the store a `forget`
     * entry asks whether a listed cid is still held: the whole composition, so
     * a cid another member holds, or one written again since, is not forgotten
     * (DESIGN.md §20 decision 8).
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir, Set<String> quietFor, BlockStore holders) {
        if( snapshots != null ) {
            // Without a usable snapshot the full scan below still runs (ticket 04 decision 2).
            final String reason = trySeed(store, storeLog, member, snapshots, tempDir)
            if( reason != null )
                warnUnusable(storeLog, member, reason, quietFor)
        }
        catchUp(store, storeLog, member, holders)
    }

    /**
     * Seeds {@code member} from its snapshot when this cache has no watermark
     * for it (ticket 04 decision 6: an absent watermark alone triggers
     * seeding). Null once seeded, or when the member already has a watermark;
     * else why the snapshot could not be used: `absent`, `unreadable`,
     * `schema_version <n>`, or that it is from another incarnation of the
     * member. Never scans and never warns: the caller decides what an
     * unusable snapshot means.
     */
    String trySeed(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir) {
        if( meta(watermarkKey(member)) != null )
            return null
        Path file = null
        try {
            file = snapshots.fetch(tempDir)
            if( file == null )
                return 'absent'
            seedFrom(file, member, storeLog)
            return null
        }
        catch( SnapshotUnusable e ) {
            return e.reason
        }
        catch( Exception e ) {
            // An SDK refusal (a 403 on index/*, a 5xx after retries) is unchecked; any of them
            // leaves the snapshot unreadable.
            log.debug("fetching the Index Snapshot of '${member}' failed", e)
            return 'unreadable'
        }
        finally {
            if( file != null ) Files.deleteIfExists(file)
        }
    }

    /** Whether this cache holds {@code member}: a watermark for it, or a block scan of it, however it got there. */
    boolean isIndexed(String member) {
        return meta(watermarkKey(member)) != null || meta(META_SCANNED + ':' + member) != null
    }

    /**
     * Indexes {@code member} from every block, telling {@code progress} how
     * far it has got (background store indexing, spec §6.3): the listing,
     * each metadata block read, each record ingested, then the Store Log.
     * Unlike catch-up's scan, a block that cannot be read stops it, while
     * listing, reading or ingesting, so a member whose reads start failing
     * part way (expired credentials) is never left marked as indexed in part
     * (spec §4). A block that is absent is not a failure: one listed but gone
     * by its read (a sweep) is skipped, and one a record names is `missing`,
     * as in catch-up. One that reads but does not decode is skipped. Whatever
     * {@code progress} throws stops it too. Only at the end does one
     * transaction set the watermark and `block_scan`; the rows ingested before
     * a failure stay committed, so a caller scans into a scratch Index and
     * seeds its own cache from that.
     */
    void indexMember(BlockStore store, StoreLog storeLog, String member, BlockStore holders, IndexProgress progress) throws IOException {
        // The ingest paths keep going past a read that fails, as catch-up needs; this one
        // remembers the first such failure and indexMember rethrows it after each step.
        final ReadFailures reads = new ReadFailures(store)
        final Map<String, List<Cid>> found = blocksOfKinds(reads, SCANNED_KINDS, progress)
        long total = 0L
        for( List<Cid> cids : found.values() )
            total += cids.size()
        long done = 0L
        progress.report(IndexProgress.INGESTING, done, total)
        for( String kind : SCANNED_KINDS ) {
            for( Cid cid : found.get(kind) ) {
                ingestLogged(reads, SCANNED_LOG_KINDS.get(kind), cid, member, holders)
                reads.rethrow()
                progress.report(IndexProgress.INGESTING, ++done, total)
            }
        }
        progress.report(IndexProgress.LOG, -1L, -1L)
        retryMissingLogged(reads, member, holders)
        retryMissingContent(reads)
        retryMissingInputSets(reads, member)
        retryMissingItemLineages(reads, member)
        reads.rethrow()
        final List<StoreLogEntry> entries = storeLog.entriesSince(null)
        // entriesSince is newest first; ingest in the order they were written.
        for( int i = entries.size() - 1; i >= 0; i-- ) {
            ingestEntry(reads, entries[i], member, holders)
            reads.rethrow()
        }
        final String scanned = total == 0L ? SCANNED_EMPTY : 'done'
        withTransaction {
            if( entries )
                setMeta(watermarkKey(member), entries[0].name)
            setMeta(META_SCANNED + ':' + member, scanned)
        }
    }

    /**
     * A store that remembers the first read of it that failed for any reason
     * but absence ({@link NoSuchBlockException}), which the ingest paths
     * record as `missing` and pass over. Once one has failed, every later
     * read fails the same way at once, so a member whose credentials expired
     * is not read to the end before {@link #rethrow} stops the scan.
     */
    private static final class ReadFailures implements BlockStore {
        private final BlockStore store
        private Exception failure

        ReadFailures(BlockStore store) { this.store = store }

        /** True once a read has failed, after which every read fails the same way. */
        boolean failed() { failure != null }

        /** Throws the first read failure, if there was one. */
        void rethrow() throws IOException {
            if( failure instanceof IOException )
                throw (IOException) failure
            if( failure instanceof RuntimeException )
                throw (RuntimeException) failure
            if( failure != null )
                throw new IOException(failure.message, failure)
        }

        private <T> T read(Closure<T> call) {
            rethrow()
            try {
                return call.call()
            }
            catch( NoSuchBlockException e ) {
                throw e
            }
            catch( Exception e ) {
                failure = e
                throw e
            }
        }

        @Override String alias() { store.alias() }
        @Override boolean has(Cid cid) { read { store.has(cid) } }
        @Override long size(Cid cid) { read { store.size(cid) } }
        @Override long lastModifiedMillis(Cid cid) { read { store.lastModifiedMillis(cid) } }
        @Override Stream<Cid> listBlocks() { read { store.listBlocks() } }
        @Override boolean isWritable() { store.isWritable() }
        @Override void put(Cid cid, InputStream input, long expectedSize) { store.put(cid, input, expectedSize) }
        @Override Cid putStreaming(InputStream input) { store.putStreaming(input) }
        @Override Cid putDagCbor(Object value) { store.putDagCbor(value) }

        @Override
        InputStream open(Cid cid) {
            final InputStream input = read { store.open(cid) }
            return new FilterInputStream(input) {
                @Override int read() throws IOException { ReadFailures.this.read { input.read() } }
                @Override int read(byte[] b, int off, int len) throws IOException { ReadFailures.this.read { input.read(b, off, len) } }
            }
        }
    }

    /**
     * Silent decision 17: one warning, and only when the member's Store Log
     * holds an entry beyond the ones this run itself just wrote (ticket 18):
     * a run's own first entry is no evidence that a snapshot should already
     * exist.
     */
    private void warnUnusable(StoreLog storeLog, String member, String reason, Set<String> quietFor) {
        if( storeLog.read().any { StoreLogEntry e -> !quietFor.contains(e.cid.toString()) } )
            notice.warn("store member '${member}' has no usable Index Snapshot (${reason}); indexing it from every block instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.".toString())
    }

    /** The per-owner copies, each limited to what this index does not hold yet (silent decision 17). */
    private static final List<String> SEED = [
        'CREATE TEMP TABLE seed_run(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_coll(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_item(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_claim(cid TEXT PRIMARY KEY)',
        'INSERT INTO seed_run SELECT completion_cid FROM snap.run WHERE completion_cid NOT IN (SELECT completion_cid FROM main.run)',
        '''INSERT INTO main.run SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
             run_name, asserted_by, status, possibly_incomplete, finished_at, :member FROM snap.run WHERE completion_cid IN seed_run''',
        'INSERT INTO seed_coll SELECT collection_cid FROM snap.collection WHERE collection_cid NOT IN (SELECT collection_cid FROM main.collection)',
        'INSERT INTO main.collection SELECT * FROM snap.collection WHERE collection_cid IN seed_coll',
        'INSERT INTO main.collection_item SELECT * FROM snap.collection_item WHERE collection_cid IN seed_coll',
        'INSERT INTO seed_item SELECT item_cid FROM snap.item WHERE item_cid NOT IN (SELECT item_cid FROM main.item)',
        'INSERT INTO main.item SELECT cid FROM seed_item',
        'INSERT INTO main.item_attr SELECT * FROM snap.item_attr WHERE item_cid IN seed_item',
        'INSERT INTO main.producer SELECT * FROM snap.producer WHERE completion_cid IN seed_run',
        'INSERT INTO main.consumer SELECT * FROM snap.consumer WHERE completion_cid IN seed_run',
        'INSERT INTO main.missing SELECT * FROM snap.missing WHERE have_cid IN seed_run OR have_cid IN seed_coll',
        // Content, per owner (an item or a DirectoryManifest): an owner this
        // index has block_ref rows for keeps them, as ingestItem would. A
        // block's size follows insertBlock's rule: the first known size wins.
        'CREATE TEMP TABLE seed_owner(cid TEXT PRIMARY KEY)',
        'INSERT INTO seed_owner SELECT DISTINCT owner_cid FROM snap.block_ref WHERE owner_cid NOT IN (SELECT owner_cid FROM main.block_ref)',
        'INSERT INTO main.block_ref SELECT owner_cid, cid, name FROM snap.block_ref WHERE owner_cid IN seed_owner',
        '''INSERT INTO main.block(cid, size, codec) SELECT cid, size, codec FROM snap.block WHERE true
             ON CONFLICT(cid) DO UPDATE SET size = excluded.size WHERE block.size IS NULL''',
        'INSERT INTO main.missing SELECT * FROM snap.missing WHERE have_cid IN seed_owner',
        'INSERT INTO main.selection_child SELECT * FROM snap.selection_child WHERE parent_cid IN seed_coll',
        'INSERT INTO main.selection_derived SELECT * FROM snap.selection_derived WHERE selection_cid IN seed_coll',
        'INSERT INTO seed_claim SELECT claim_cid FROM snap.claim WHERE claim_cid NOT IN (SELECT claim_cid FROM main.claim)',
        'INSERT INTO main.claim SELECT * FROM snap.claim WHERE claim_cid IN seed_claim',
        'INSERT INTO main.claim_supersedes SELECT * FROM snap.claim_supersedes WHERE claim_cid IN seed_claim',
        'INSERT OR IGNORE INTO main.log_entry SELECT cid, kind, :member, written_at FROM snap.log_entry',
        // A logged block the snapshot's writer could not read left it only
        // missing(NULL, cid), which the snapshot does not carry. Without this
        // row the seeded watermark is past the entry and retryMissingLogged
        // never ingests the block once it arrives; a full scan would.
        '''INSERT INTO main.missing(have_cid, needed_cid) SELECT DISTINCT NULL, cid FROM snap.log_entry
             WHERE kind IN ('run', 'selection', 'claim', 'transfer', 'project', 'handoff')
               AND cid NOT IN (SELECT completion_cid FROM main.run)
               AND cid NOT IN (SELECT collection_cid FROM main.collection)
               AND cid NOT IN (SELECT claim_cid FROM main.claim)
               AND cid NOT IN (SELECT cid FROM main.transfer)
               AND cid NOT IN (SELECT project_cid FROM main.project)
               AND cid NOT IN (SELECT cid FROM main.handoff)
               AND cid NOT IN (SELECT needed_cid FROM main.missing WHERE have_cid IS NULL)''',
    ]

    /**
     * The Transfers, run before SEED so its last statement sees them. Roots
     * first, while main.transfer still tells a seeded Transfer from one this
     * index holds.
     */
    private static final List<String> SEED_TRANSFERS = [
        'INSERT INTO main.transfer_root SELECT * FROM snap.transfer_root WHERE transfer_cid NOT IN (SELECT cid FROM main.transfer)',
        'INSERT INTO main.transfer SELECT * FROM snap.transfer WHERE cid NOT IN (SELECT cid FROM main.transfer)',
    ]

    /** The Projects and Handoffs, run before SEED so its last statement sees them. */
    private static final List<String> SEED_P4_ROOTS = [
        'INSERT INTO main.project SELECT * FROM snap.project WHERE project_cid NOT IN (SELECT project_cid FROM main.project)',
        'INSERT INTO main.handoff SELECT * FROM snap.handoff WHERE cid NOT IN (SELECT cid FROM main.handoff)',
    ]

    /** The seeded runs' inputs, input origins and item lineage, after SEED has filled seed_run. Their consumer rows are in SEED. */
    private static final List<String> SEED_RUN_RECORDS = ['run_input', 'input_origin', 'item_input', 'item_task'].collect { String t ->
        "INSERT INTO main.${t} SELECT * FROM snap.${t} WHERE completion_cid IN seed_run".toString()
    }.asImmutable()

    /** The seeded runs' InputSet wait records, which the snapshot carries in its meta. */
    private static final String SEED_INPUTSET_WAITS = """INSERT OR REPLACE INTO main.meta(key, value)
        SELECT key, value FROM snap.meta WHERE key GLOB '${INPUTSET_WAIT}*' AND substr(key, ${INPUTSET_WAIT.length() + 1}) IN seed_run""".toString()

    /** The seeded runs' ItemLineage wait records. */
    private static final String SEED_ITEMLINEAGE_WAITS = """INSERT OR REPLACE INTO main.meta(key, value)
        SELECT key, value FROM snap.meta WHERE key GLOB '${ITEMLINEAGE_WAIT}*' AND substr(key, ${ITEMLINEAGE_WAIT.length() + 1}) IN seed_run""".toString()

    private static final String OTHER_INCARNATION = 'written for another incarnation of the member'
    private static final String EARLIER_INCARNATION = 'so it may be from an earlier incarnation'

    /** Why a snapshot whose store_id is not the member's is refused (DESIGN.md §20 decision 5). */
    static String otherIncarnation(String snapshotId, String memberId) {
        return "${OTHER_INCARNATION} (store_id ${snapshotId}, the member's is ${memberId})".toString()
    }

    /** Why a snapshot with no store_id whose watermark the member's Store Log lacks is refused (DESIGN.md §20 decision 9). */
    static String earlierIncarnation(String watermark) {
        return "its watermark ${watermark} is not in the member's Store Log, ${EARLIER_INCARNATION}".toString()
    }

    /** True for a reason from {@link #otherIncarnation} or {@link #earlierIncarnation}. */
    static boolean isIncarnationReason(String reason) {
        return reason != null && (reason.startsWith(OTHER_INCARNATION) || reason.endsWith(EARLIER_INCARNATION))
    }

    /**
     * Copies a member's Index Snapshot into this index (ticket 04 decision 1).
     * Trusted when its schema_version matches (decision 10); claim_current is
     * recomputed for every seeded subject (decision 9). True when it seeded.
     */
    boolean seedFrom(Path snapshotFile, String member) throws SnapshotUnusable {
        return seedFrom(snapshotFile, member, null)
    }

    /**
     * As the 2-arg {@code seedFrom}, and refusing a snapshot written for another
     * incarnation of the member (DESIGN.md §20 decisions 5, 9): its store_id
     * differs from the cache's, or it has none and its watermark entry is not
     * in the member's Store Log (checked only when storeLog is given).
     */
    boolean seedFrom(Path snapshotFile, String member, StoreLog storeLog) throws SnapshotUnusable {
        try {
            update('ATTACH DATABASE ? AS snap', [(Object) "file:${snapshotFile.toAbsolutePath()}?mode=ro".toString()])
        }
        catch( Exception e ) {
            throw new SnapshotUnusable('unreadable', e)
        }
        try {
            final List<Integer> versions = []
            try {
                query('SELECT version FROM snap.schema_version', []) { ResultSet rs -> versions.add(rs.getInt(1)) }
            }
            catch( Exception e ) {
                throw new SnapshotUnusable('unreadable', e)
            }
            if( versions != [SCHEMA_VERSION] )
                throw new SnapshotUnusable("schema_version ${versions ? versions[0] : 'missing'}")
            final Map<String, String> snapMeta = [:]
            try {
                query('SELECT key, value FROM snap.meta', []) { ResultSet rs -> snapMeta.put(rs.getString(1), rs.getString(2)) }
            }
            catch( IllegalStateException | SQLException e ) {
                throw new SnapshotUnusable('unreadable', e)
            }
            final String snapId = snapMeta[IndexSnapshot.STORE_ID_KEY]
            final String memberId = storeIdOf(member)
            if( snapId != null && memberId != null && snapId != memberId )
                throw new SnapshotUnusable(otherIncarnation(snapId, memberId))
            final String snapWatermark = snapMeta[IndexSnapshot.WATERMARK_KEY]
            if( snapId == null && snapWatermark != null && storeLog != null && !storeLog.has(snapWatermark) )
                throw new SnapshotUnusable(earlierIncarnation(snapWatermark))
            try {
                withTransaction {
                    for( String sql : SEED_TRANSFERS + SEED_P4_ROOTS )
                        update(sql, [])
                    for( String sql : SEED )
                        update(sql.replace(':member', '?'), sql.contains(':member') ? [(Object) member] : [])
                    for( String sql : SEED_RUN_RECORDS )
                        update(sql, [])
                    update(SEED_INPUTSET_WAITS, [])
                    update(SEED_ITEMLINEAGE_WAITS, [])
                    final List<String> subjects = []
                    query('SELECT DISTINCT subject_cid FROM main.claim WHERE claim_cid IN seed_claim', []) { ResultSet rs -> subjects.add(rs.getString(1)) }
                    for( String subject : subjects )
                        ClaimCurrent.rewrite(connection, subject)
                    // Seeded Claims arrive without ingestClaim: any of them may have made a block releasing-only.
                    dropPresent()
                    if( snapMeta[IndexSnapshot.WATERMARK_KEY] )
                        setMeta(watermarkKey(member), snapMeta[IndexSnapshot.WATERMARK_KEY])
                    setMeta(META_SCANNED + ':' + member, 'done')
                    setMeta(META_SEEDED + ':' + member, snapMeta[IndexSnapshot.WRITTEN_AT_KEY] ?: 'unknown')
                    // A lean snapshot carries no content edges, so none were seeded, and storage
                    // figures built from this cache would leave the seeded runs' files out. Its
                    // snapshots stay lean (seeded_lean) until a reset and a full ingest:
                    // `reindex --seed false`. The value kept is the seeding snapshot's reason.
                    if( snapMeta[IndexSnapshot.STORAGE_OMITTED_KEY] )
                        setMeta(IndexSnapshot.SEEDED_LEAN_KEY + ':' + member, snapMeta[IndexSnapshot.STORAGE_OMITTED_KEY])
                    for( String t : ['seed_run', 'seed_coll', 'seed_item', 'seed_claim', 'seed_owner'] )
                        update("DROP TABLE temp.${t}".toString(), [])
                }
            }
            catch( IllegalStateException | SQLException e ) {
                // Index.update and Index.query wrap a failed statement in an
                // IllegalStateException; one that fails to prepare throws the
                // SQLException itself.
                throw new SnapshotUnusable('unreadable', e)
            }
            return true
        }
        finally {
            update('DETACH DATABASE snap', [])
        }
    }

    /**
     * Ingests one block a Store Log announced, unless it is already indexed.
     * Every catch-up, retry and scan path comes through here, so a new kind
     * is one branch. A block that cannot be read is recorded as `missing`.
     */
    private void ingestLogged(BlockStore store, StoreLogKind kind, Cid cid, String member, BlockStore holders) {
        switch( kind ) {
            case StoreLogKind.RUN:
                if( !isRunIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.SELECTION:
                if( !isSelectionIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.CLAIM:
                if( !isClaimIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.FORGET:
                if( meta(forgetKey(member, cid)) == null )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.TRANSFER:
                if( !isTransferIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.PROJECT:
                if( !isProjectIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            case StoreLogKind.HANDOFF:
                if( !isHandoffIndexed(cid) )
                    ingestTolerant(store, kind, cid, member, holders)
                return
            default:
                log.debug("store log entry for ${cid} is a ${kind.token}; not indexed by this build yet")
        }
    }

    /**
     * The first catch-up for a member this index has never scanned finds its
     * runs from the blocks: a new index, one recreated for a schema change,
     * or a store written before the Store Log existed, whose runs no log
     * lists. Recorded per member, so it happens once: 'empty' when it found
     * none of the scanned kinds, which {@link #reconcileStoreIds} reads as no
     * row of the member, else 'done'.
     */
    private void scanOnce(BlockStore store, String member) {
        final String key = META_SCANNED + ':' + member
        if( meta(key) != null )
            return
        final Map<String, List<Cid>> found
        try {
            found = blocksOfKinds(store, SCANNED_KINDS, null)
        }
        catch( IOException | UncheckedIOException e ) {
            // Not marked done, so the next catch-up scans again.
            log.warn("could not scan the blocks of store member '$member' (${e.message}); will retry")
            return
        }
        for( String kind : SCANNED_KINDS ) {
            for( Cid cid : found.get(kind) )
                ingestLogged(store, SCANNED_LOG_KINDS.get(kind), cid, member, store)
        }
        setMeta(key, found.values().every { List<Cid> cids -> cids.isEmpty() } ? SCANNED_EMPTY : 'done')
    }

    /**
     * A logged block that had not arrived was recorded as `missing`. Retried
     * on every catch-up, so it is indexed once its block lands even after the
     * overlap window has moved past its entry. A `missing` row with no
     * `log_entry` (a run found by the block scan, not the log) defaults to RUN.
     */
    private void retryMissingLogged(BlockStore store, String member, BlockStore holders) {
        final Map<Cid, StoreLogKind> waiting = new LinkedHashMap<Cid, StoreLogKind>()
        query('''SELECT DISTINCT m.needed_cid, l.kind FROM missing m
                 LEFT JOIN log_entry l ON l.cid = m.needed_cid
                 WHERE m.have_cid IS NULL''', []) { ResultSet rs ->
            final StoreLogKind kind = rs.getString(2) == null ? StoreLogKind.RUN : StoreLogKind.fromToken(rs.getString(2))
            waiting.put(Cid.parse(rs.getString(1)), kind ?: StoreLogKind.RUN)
        }
        for( Map.Entry<Cid, StoreLogKind> each : waiting.entrySet() ) {
            try {
                if( store.has(each.key) )
                    ingestLogged(store, each.value, each.key, member, holders)
            }
            catch( IOException | UncheckedIOException e ) {
                warnOnce(each.key.toString(), "block ${each.key} is still unreachable (${e.message}); will retry")
            }
        }
    }

    /**
     * Ingests one logged block. A block that cannot be read (a permission
     * error, a stale network handle) is not an absence: it is recorded as
     * `missing` so later catch-ups retry it, and the rest of the member still ingests.
     */
    private void ingestTolerant(BlockStore store, StoreLogKind kind, Cid cid, String member, BlockStore holders) {
        try {
            switch( kind ) {
                case StoreLogKind.RUN: ingestRun(store, cid, member); break
                case StoreLogKind.SELECTION: ingestSelection(store, cid, member); break
                case StoreLogKind.CLAIM: ingestClaim(store, cid, member); break
                case StoreLogKind.FORGET: applyForget(store, holders, cid, member); break
                case StoreLogKind.TRANSFER: ingestTransfer(store, cid); break
                case StoreLogKind.PROJECT: ingestProject(store, cid); break
                case StoreLogKind.HANDOFF: ingestHandoff(store, cid); break
                default: return
            }
        }
        catch( IOException | UncheckedIOException e ) {
            warnOnce(cid.toString(), "${kind.token} ${cid} could not be read (${e.message}); it will be retried")
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [cid.toString()])
            insertMissing(null, cid)
        }
    }

    private static String forgetKey(String member, Cid forget) { "forget:${member}:${forget}".toString() }

    /**
     * DESIGN.md §20 decision 8: forgets what one sweep batch deleted, except a
     * cid some member holds now (still elsewhere, or written again since).
     * Recorded per member, so the overlap re-read and a retry apply it once.
     * An absent Forget block is `missing` and retried; one that is there but
     * does not read as a Forget is skipped, recorded with 0 runs so it is not
     * read again. Only cids this cache has rows for are looked up, and a
     * lookup that fails keeps the cid's rows.
     */
    private void applyForget(BlockStore store, BlockStore holders, Cid forgetCid, String member) {
        final Map block = readBlock(store, forgetCid, Records.FORGET, true)
        if( block == null && !store.has(forgetCid) ) {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [forgetCid.toString()])
            insertMissing(null, forgetCid)
            return
        }
        final StoreLogEntry entry = firstLogEntry(forgetCid, member)
        final Forget parsed
        try {
            if( block == null )
                throw new IllegalArgumentException("not a ${Records.FORGET} block of at most ${MAX_BLOCK_BYTES} bytes")
            parsed = Forget.fromCbor(block)
        }
        catch( RuntimeException e ) {
            warnOnce('forget-malformed:' + forgetCid, "forget ${forgetCid} is not a readable Forget block (${e.message}); it is skipped")
            setMeta(forgetKey(member, forgetCid), "${entry?.name ?: ''} 0".toString())
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [forgetCid.toString()])
            return
        }
        final List<Cid> gone = new ArrayList<Cid>()
        int unchecked = 0
        String why = null
        for( Cid c : parsed.deleted ) {
            if( !hasRowsKeyedBy(c) )
                continue
            try {
                if( !holders.has(c) )
                    gone.add(c)
            }
            catch( Exception e ) {
                // An SDK refusal (an S3 403) is unchecked too. Failing to look is
                // not evidence the block went: keep its rows (review item 2).
                unchecked++
                why = why ?: e.message
                log.debug("could not tell whether ${c} is still held", e)
            }
        }
        if( unchecked )
            warnOnce('forget-unchecked:' + forgetCid, "forget ${forgetCid}: could not tell whether ${unchecked} listed block(s) are still held (${why}); their rows are kept")
        int runs = 0
        for( Cid c : gone )
            if( isRunIndexed(c) )
                runs++
        forget(gone)
        setMeta(forgetKey(member, forgetCid), "${entry?.name ?: ''} ${runs}".toString())
        update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [forgetCid.toString()])
    }

    /**
     * Runs that `forget` entries of {@code member} removed, counting only entries
     * newer than {@code baseWatermark} (all of them when it is null): what the
     * snapshot guard adds back before comparing run counts (§20 decision 10).
     */
    int forgottenRunsSince(String member, String baseWatermark) {
        final String floor = baseWatermark == null ? null : StoreLog.parse(baseWatermark)?.reverseTs
        final String prefix = "forget:${member}:".toString()
        int n = 0
        query('SELECT value FROM meta WHERE substr(key, 1, ?) = ?', [(Object) prefix.length(), prefix]) { ResultSet rs ->
            final String[] parts = rs.getString(1).split(' ')
            if( parts.length != 2 )
                return
            final String rts = parts[0] ? StoreLog.parse(parts[0])?.reverseTs : null
            if( floor == null || (rts != null && rts < floor) )
                n += parts[1] as int
        }
        return n
    }

    /** Keys already warned about, so a recurring condition warns once per JVM. */
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    private static void warnOnce(String key, String message) {
        if( WARNED.add(key) )
            log.warn(message)
        else
            log.debug(message)
    }

    /**
     * True when this cache holds a row a forget of {@code cid} would remove:
     * a run, collection, item, Claim, Transfer, Project or Handoff keyed by it, or its Store
     * Log entry. Anything else (a DirectoryManifest, a block this cache never
     * indexed) is a no-op to forget, so it needs no `has` (review item 3).
     */
    private boolean hasRowsKeyedBy(Cid cid) {
        final String t = cid.toString()
        boolean found = false
        query('''SELECT 1 FROM run WHERE completion_cid = ?
                 UNION ALL SELECT 1 FROM collection WHERE collection_cid = ?
                 UNION ALL SELECT 1 FROM item WHERE item_cid = ?
                 UNION ALL SELECT 1 FROM claim WHERE claim_cid = ?
                 UNION ALL SELECT 1 FROM log_entry WHERE cid = ?
                 LIMIT 1''', [(Object) t, t, t, t, t]) { ResultSet rs -> found = true }
        return found || isTransferIndexed(cid) || isProjectIndexed(cid) || isHandoffIndexed(cid)
    }

    /** True when a RunCompletion already has its `run` row. */
    boolean isRunIndexed(Cid completion) {
        boolean found = false
        query('SELECT 1 FROM run WHERE completion_cid = ?', [completion.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Indexes one Claim and rewrites its subject's claim_current. Idempotent.
     * An absent block is a `missing` row, retried by later catch-ups.
     */
    void ingestClaim(BlockStore store, Cid claimCid, String member) {
        final Map block = readBlock(store, claimCid, Records.CLAIM)
        withTransaction {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [claimCid.toString()])
            if( block == null ) {
                warnOnce(claimCid.toString(), "claim ${claimCid} has not arrived; it is indexed when it does")
                insertMissing(null, claimCid)
                return
            }
            final Claim claim = Claim.fromCbor(block)
            update('INSERT OR REPLACE INTO claim(claim_cid, subject_cid, verb, attribute, value, timestamp, asserted_by) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [claimCid.toString(), claim.subject.toString(), claim.verb, claim.attribute, valueText(claim.value), claim.timestamp, claim.assertedBy])
            update('DELETE FROM claim_supersedes WHERE claim_cid = ?', [claimCid.toString()])
            for( Cid superseded : claim.supersedes )
                update('INSERT INTO claim_supersedes(claim_cid, superseded_cid) VALUES (?, ?)', [claimCid.toString(), superseded.toString()])
            ClaimCurrent.rewrite(connection, claim.subject.toString())
            dropPresent()
        }
    }

    /**
     * Forgets every presence recorded, for every member (fix round 3). Any
     * Claim can supersede a pin or a release, whatever its own attribute, and
     * a Claim may count in a member other than the one it was ingested under,
     * so after any Claim a block may have become releasing-only and a sweep
     * taken it with no build in between. The next build of each member looks
     * at its exposed held blocks again. The caller owns the transaction.
     */
    private void dropPresent() {
        if( !absentMemo )
            return
        update('DELETE FROM block_present', [])
        // A build that read the roots before this drop must not record what it found under them.
        update('''INSERT INTO meta(key, value) VALUES (?, '1')
                  ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + 1''', [(Object) PRESENT_GENERATION_KEY])
    }

    /**
     * The cache meta key counting the drops of `block_present` (fix round 3):
     * a build reads it before it copies the roots, and records presences only
     * if it is unchanged, so a drop by another process in between wins.
     */
    static final String PRESENT_GENERATION_KEY = 'block_present_generation'

    /** The current `block_present_generation`, '0' when no drop has happened yet. */
    String presentGeneration() {
        return meta(PRESENT_GENERATION_KEY) ?: '0'
    }

    boolean isClaimIndexed(Cid claimCid) {
        boolean found = false
        query('SELECT 1 FROM claim WHERE claim_cid = ?', [claimCid.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Indexes one Transfer (P3 plan 1.5): its `transfer` row and a
     * `transfer_root` row per root ('root') and per run ('run'). Idempotent.
     * An absent block is a `missing` row, retried by later catch-ups; a read
     * that fails is thrown, so ingestTolerant records it the same way. A block
     * that is there but is not a Transfer this build reads is skipped with one
     * warning and not retried, as a malformed Forget is. A Transfer changes
     * no retention, so `block_present` stays as it is.
     */
    private void ingestTransfer(BlockStore store, Cid transferCid) {
        final String text = transferCid.toString()
        final Map block = readBlock(store, transferCid, Records.TRANSFER, true)
        Transfer transfer = null
        String refused = null
        if( block != null ) {
            try {
                transfer = Transfer.fromCbor(block)
            }
            catch( IllegalArgumentException e ) {
                refused = e.message
            }
        }
        else if( store.has(transferCid) )
            refused = "not a ${Records.TRANSFER} block of at most ${MAX_BLOCK_BYTES} bytes"
        withTransaction {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [(Object) text])
            update('DELETE FROM transfer_root WHERE transfer_cid = ?', [(Object) text])
            update('DELETE FROM transfer WHERE cid = ?', [(Object) text])
            if( refused != null ) {
                warnOnce('transfer-malformed:' + text, "transfer ${text} is not a readable Transfer block (${refused}); it is skipped")
                return
            }
            if( transfer == null ) {
                warnOnce(text, "transfer ${text} has not arrived; it is indexed when it does")
                insertMissing(null, transferCid)
                return
            }
            update('''INSERT INTO transfer(cid, from_member, to_member, asserted_by, depth, claims, blocks, copied, bytes, at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                [(Object) text, transfer.from, transfer.to, transfer.assertedBy, transfer.depth, transfer.claims.join(','),
                 transfer.blocks, transfer.copied, transfer.bytes, transfer.timestamp])
            for( Cid root : transfer.roots )
                update("INSERT INTO transfer_root(transfer_cid, root_cid, role) VALUES (?, ?, 'root')", [(Object) text, root.toString()])
            for( Cid run : transfer.runs )
                update("INSERT INTO transfer_root(transfer_cid, root_cid, role) VALUES (?, ?, 'run')", [(Object) text, run.toString()])
        }
    }

    boolean isTransferIndexed(Cid transferCid) {
        boolean found = false
        query('SELECT 1 FROM transfer WHERE cid = ?', [(Object) transferCid.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Indexes one Project (P4 plan 1.4): its `project` row, `created_by` the
     * block's `asserted_by`. As {@link #ingestTransfer}: idempotent, an
     * absent block waits as `missing(NULL, cid)`, and a block that does not
     * read as a Project is warned about once and skipped. A Project changes
     * no retention.
     */
    private void ingestProject(BlockStore store, Cid projectCid) {
        ingestP4Root(store, projectCid, Records.PROJECT, 'DELETE FROM project WHERE project_cid = ?',
            'INSERT INTO project(project_cid, name, description, created_by, created_at) VALUES (?, ?, ?, ?, ?)') { Map block ->
            final Project p = Project.fromCbor(block)
            return [(Object) p.name, p.description, p.assertedBy, p.createdAt]
        }
    }

    /**
     * Indexes one Handoff (P4 plan 1.5, D7): its `handoff` row, `at` the
     * block's `timestamp`, `project_cid` NULL when it names no Project. As
     * {@link #ingestProject}. Its files are content it roots (P4 R5), so
     * they are recorded as an item's leaves are: `block_ref(handoff, file,
     * name)` and the file's `block` row sized by its Leaf, once per Handoff.
     */
    private void ingestHandoff(BlockStore store, Cid handoffCid) {
        Handoff read = null
        ingestP4Root(store, handoffCid, Records.HANDOFF, 'DELETE FROM handoff WHERE cid = ?',
            'INSERT INTO handoff(cid, template, selection_cid, project_cid, asserted_by, at) VALUES (?, ?, ?, ?, ?, ?)', { Map block ->
            final Handoff h = Handoff.fromCbor(block)
            read = h
            return [(Object) h.template, h.selection.toString(), h.project?.toString(), h.assertedBy, h.timestamp]
        }) { ->
            if( hasRefs(handoffCid) )
                return
            final Set<Cid> seen = new HashSet<Cid>()
            for( Leaf f : read.files )
                ingestContent(store, handoffCid, f.address, f.name, f.size, seen)
        }
    }

    /**
     * The shared part of {@link #ingestProject} and {@link #ingestHandoff}:
     * {@code values} turns the block into the row after its cid, and throws
     * when the block does not read as one. {@code also}, when given, runs in
     * the same transaction once the row is in.
     */
    private void ingestP4Root(BlockStore store, Cid cid, String kind, String delete, String insert, Closure<List<Object>> values,
                              Closure also = null) {
        final String text = cid.toString()
        final Map block = readBlock(store, cid, kind, true)
        List<Object> row = null
        String refused = null
        if( block != null ) {
            try {
                row = values.call(block)
            }
            catch( RuntimeException e ) {
                refused = e.message
            }
        }
        else if( store.has(cid) )
            refused = "not a ${kind} block of at most ${MAX_BLOCK_BYTES} bytes"
        withTransaction {
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [(Object) text])
            update(delete, [(Object) text])
            if( refused != null ) {
                warnOnce(kind + '-malformed:' + text, "${kind.toLowerCase()} ${text} is not a readable ${kind} block (${refused}); it is skipped")
                return
            }
            if( row == null ) {
                warnOnce(text, "${kind.toLowerCase()} ${text} has not arrived; it is indexed when it does")
                insertMissing(null, cid)
                return
            }
            final List<Object> parameters = new ArrayList<Object>()
            parameters.add(text)
            parameters.addAll(row)
            update(insert, parameters)
            if( also != null )
                also.call()
        }
    }

    boolean isProjectIndexed(Cid projectCid) {
        return anyRow('SELECT 1 FROM project WHERE project_cid = ?', [(Object) projectCid.toString()])
    }

    boolean isHandoffIndexed(Cid handoffCid) {
        return anyRow('SELECT 1 FROM handoff WHERE cid = ?', [(Object) handoffCid.toString()])
    }

    static final String SQL_RUN_INPUTS_OF =
        'SELECT completion_cid, role, name, address, size, sample FROM run_input WHERE completion_cid = ? ORDER BY role, name, address, sample'

    /** The run's inputs as its InputSet listed them (P4 plan 1.3); none for a run without one. */
    List<RunInputRow> runInputs(Cid completion) {
        final List<RunInputRow> out = new ArrayList<RunInputRow>()
        query(SQL_RUN_INPUTS_OF, [(Object) completion.toString()]) { ResultSet rs ->
            final String address = rs.getString(4)
            final long size = rs.getLong(5)
            final Long sized = rs.wasNull() ? null : size
            out.add(new RunInputRow(Cid.parse(rs.getString(1)), rs.getString(2), rs.getString(3),
                address == null ? null : Cid.parse(address), sized, rs.getString(6)))
        }
        return out
    }

    /**
     * The Claims logged in {@code member} that supersede {@code claim}. Put
     * checks staleness against the writable member only, the Claims the page
     * can see there (DESIGN.md §16 decision 22).
     */
    List<Cid> supersedersOf(Cid claim, String member) {
        final List<Cid> out = new ArrayList<Cid>()
        query('''SELECT s.claim_cid FROM claim_supersedes s
                 JOIN log_entry e ON e.cid = s.claim_cid AND e.member = ?
                 WHERE s.superseded_cid = ? ORDER BY s.claim_cid''', [member, claim.toString()]) { ResultSet rs ->
            out.add(Cid.parse(rs.getString(1)))
        }
        return out
    }

    /** The subject's current state from every Claim this index holds for it. */
    ClaimState claimState(Cid subject) {
        return ClaimCurrent.load(connection, subject.toString())
    }

    /**
     * Indexes one Selection. Idempotent: its rows are replaced. The item
     * blocks are not read here: an item's metadata is indexed with the run
     * that produced it, and a Selection may name items another member holds.
     */
    void ingestSelection(BlockStore store, Cid selectionCid, String member) {
        final Map block = readBlock(store, selectionCid, Records.SELECTION)
        withTransaction {
            final String text = selectionCid.toString()
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [text])
            update('DELETE FROM collection_item WHERE collection_cid = ?', [text])
            update('DELETE FROM selection_child WHERE parent_cid = ?', [text])
            update('DELETE FROM selection_derived WHERE selection_cid = ?', [text])
            update("DELETE FROM collection WHERE collection_cid = ? AND kind = 'selection'", [text])
            if( block == null ) {
                warnOnce(text, "selection ${selectionCid} has not arrived; it is indexed when it does")
                insertMissing(null, selectionCid)
                return
            }
            final Selection selection = Selection.fromCbor(block)
            update("INSERT INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, 'selection', NULL, NULL, ?)",
                [text, selection.assertedBy])
            for( Selection.Member m : selection.members ) {
                if( m.nested ) {
                    update('INSERT INTO selection_child(parent_cid, child_cid) VALUES (?, ?)', [text, m.address.toString()])
                    continue
                }
                update('INSERT OR IGNORE INTO item(item_cid) VALUES (?)', [m.address.toString()])
                if( m.via.isEmpty() )
                    update('INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, NULL)', [text, m.address.toString()])
                for( Cid via : m.via )
                    update('INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, ?)', [text, m.address.toString(), via.toString()])
            }
            for( Cid derived : selection.derivedFrom )
                update('INSERT INTO selection_derived(selection_cid, derived_from_cid) VALUES (?, ?)', [text, derived.toString()])
        }
    }

    boolean isSelectionIndexed(Cid selectionCid) {
        boolean found = false
        query("SELECT 1 FROM collection WHERE collection_cid = ? AND kind = 'selection'", [selectionCid.toString()]) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Indexes a Selection block present in {@code store} but not yet indexed
     * (copied into the composition without its Store Log entry, block
     * explorer spec section 11), and every nested one the store holds; a
     * nested one the store lacks is left for {@link #selectionItems} to name.
     * Both {@code fromStore(selection:)} (CasExtension) and the samplesheet
     * export (ExploreCommand) call this before reading selectionItems, so the
     * two callers cannot index differently (final review finding 4).
     */
    void ensureSelectionIndexed(BlockStore store, Cid selection, String member) {
        ensureSelectionIndexedRecursive(store, selection, member, new HashSet<Cid>())
    }

    private void ensureSelectionIndexedRecursive(BlockStore store, Cid selection, String member, Set<Cid> seen) {
        if( !seen.add(selection) )
            return
        final Map block = readBlock(store, selection, Records.SELECTION)
        if( block == null )
            return
        if( !isSelectionIndexed(selection) )
            ingestSelection(store, selection, member)
        for( Selection.Member m : Selection.fromCbor(block).members )
            if( m.nested )
                ensureSelectionIndexedRecursive(store, m.address, member, seen)
    }

    /**
     * Every distinct item the Selection reaches, sorted by CID string. Refuses
     * to answer with a partial set: a Selection reached but not indexed (not
     * arrived, or held in a member this composition does not include) fails.
     */
    List<Cid> selectionItems(Cid selectionCid) {
        requireSelectionsIndexed(selectionCid)
        final List<Cid> items = new ArrayList<Cid>()
        query(SQL_SELECTION_ITEMS, [selectionCid.toString()]) { ResultSet rs -> items.add(Cid.parse(rs.getString(1))) }
        return items
    }

    /**
     * Every item the Selection reaches, in {@link #selectionItems} order, each
     * with the distinct collections it was picked through, sorted; {@code []}
     * when it was picked by query only. Refuses a partial answer as
     * {@code selectionItems} does.
     */
    Map<Cid, List<Cid>> selectionVias(Cid selectionCid) {
        requireSelectionsIndexed(selectionCid)
        final Map<Cid, List<Cid>> vias = new LinkedHashMap<Cid, List<Cid>>()
        query(SQL_SELECTION_VIAS, [selectionCid.toString()]) { ResultSet rs ->
            final List<Cid> forItem = vias.computeIfAbsent(Cid.parse(rs.getString(1))) { Cid k -> new ArrayList<Cid>() }
            final String via = rs.getString(2)
            if( via != null )
                forItem.add(Cid.parse(via))
        }
        return vias
    }

    private void requireSelectionsIndexed(Cid selectionCid) {
        final List<String> absent = new ArrayList<String>()
        query(SQL_SELECTIONS_NOT_INDEXED, [selectionCid.toString()]) { ResultSet rs -> absent.add(rs.getString(1)) }
        if( absent )
            throw new IllegalStateException("selection ${selectionCid} reaches ${absent.size() == 1 ? 'a Selection' : 'Selections'} " +
                "this index does not hold: ${absent.join(', ')}; it may be in a member this composition does not include")
    }

    /** decision 4: a string as itself, null as NULL, anything else as DAG-JSON text. */
    static String valueText(Object value) {
        if( value == null )
            return null
        return value instanceof String ? (String) value : DagJson.encodeToString(value)
    }

    private static final java.time.format.DateTimeFormatter ISO_MILLIS =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC)

    /** ISO-8601 UTC with milliseconds, the form of every timestamp in a block and in log_entry. */
    static String isoMillis(long millis) {
        return ISO_MILLIS.format(java.time.Instant.ofEpochMilli(millis))
    }

    /**
     * Records that {@code member}'s Store Log announced {@code entry}. Idempotent;
     * keeps the earliest time, which is when the member first saw the block
     * (block explorer spec section 11). ISO text of one fixed width sorts as time.
     */
    void recordLogEntry(StoreLogEntry entry, String member) {
        update('''INSERT INTO log_entry(cid, kind, member, written_at) VALUES (?, ?, ?, ?)
                  ON CONFLICT(cid, member) DO UPDATE SET written_at = min(written_at, excluded.written_at)''',
            [entry.cid.toString(), entry.kind.token, member, isoMillis(entry.writtenAtMillis)])
    }

    /** The earliest Store Log entry of {@code cid} in {@code member}, rebuilt from its row, or null. */
    StoreLogEntry firstLogEntry(Cid cid, String member) {
        final List<StoreLogEntry> found = new ArrayList<StoreLogEntry>()
        query('SELECT kind, written_at FROM log_entry WHERE cid = ? AND member = ?', [cid.toString(), member]) { ResultSet rs ->
            final StoreLogKind kind = StoreLogKind.fromToken(rs.getString(1))
            final long millis = java.time.Instant.parse(rs.getString(2)).toEpochMilli()
            found.add(StoreLog.parse(StoreLog.entryName(kind, cid, millis)))
        }
        return found ? found[0] : null
    }

    private static String watermarkKey(String member) {
        return META_WATERMARK + ':' + member
    }

    /** The member's Store Log watermark, the newest entry name catch-up has read, or null. */
    String watermark(String member) {
        return meta(watermarkKey(member))
    }

    /** The member's store-id as this cache last recorded it, or null (DESIGN.md §20 decision 3). */
    String storeIdOf(String member) { meta(META_STORE_ID + ':' + member) }

    /**
     * Records {@code id} as the store-id of {@code member} when this cache has
     * none for it, without {@link #reconcileStoreIds}'s look at the rows it
     * holds: for a writer that has just given the member its first id and
     * knows those rows came from that member (an explorer push, background
     * store indexing spec §6.5). A different recorded id is left for
     * {@link #reconcileStoreIds} to judge.
     */
    void adoptStoreId(String member, String id) {
        if( id != null && storeIdOf(member) == null )
            setMeta(META_STORE_ID + ':' + member, id)
    }

    /**
     * Empties every table but schema_version, meta included: a cold cache (plan
     * decision 1). The one meta row kept is `block_present_generation`, moved
     * on by one, so a build in another process that read it before the reset
     * records no presence into the emptied cache.
     */
    void reset() {
        withTransaction {
            // A value that is not a number (by hand, or a damaged cache) counts as 0: the reset must not fail on derived data.
            long generation = 0L
            try {
                generation = Long.parseLong(presentGeneration())
            }
            catch( NumberFormatException e ) {
                log.debug("block_present_generation is not a number; counting it as 0")
            }
            for( String sql : ddl() ) {
                final java.util.regex.Matcher m = sql =~ /^CREATE TABLE (\w+)[\s(]/
                if( m.find() && m.group(1) != 'schema_version' )
                    update("DELETE FROM ${m.group(1)}".toString(), [])
            }
            if( absentMemo ) {
                update('DELETE FROM block_absent', [])
                update('DELETE FROM block_present', [])
            }
            update('INSERT INTO meta(key, value) VALUES (?, ?)',
                [(Object) PRESENT_GENERATION_KEY, String.valueOf(generation + 1)])
        }
        deleteLocalSnapshots()
    }

    /** The local Index Snapshots beside this cache describe what it held, so they go with a reset (spec §6.4). */
    private void deleteLocalSnapshots() {
        final Path dir = IndexPaths.localSnapshotsDir(file)
        if( !Files.exists(dir) )
            return
        try {
            final Stream<Path> paths = Files.walk(dir)
            try {
                for( Path each : paths.sorted(Comparator.reverseOrder()).toList() )
                    Files.delete(each)
            }
            finally {
                paths.close()
            }
        }
        catch( IOException | UncheckedIOException e ) {
            // Never served once the cache no longer holds the member, so a leftover file does no harm.
            log.debug("could not delete the local Index Snapshots in ${dir}: ${e.message}")
        }
    }

    /**
     * DESIGN.md §20 decision 3 and plan decisions 1 to 4. {@code ids}: alias to
     * the member's id now, null when it has none; a member whose id could not be
     * read is left out. {@code watermarkLogged(alias, watermark)} answers whether
     * that entry is still in the member's log, or null when it could not tell.
     * Resets the whole cache when any member was replaced, then records every
     * id it may. A member with no recorded id and no watermark that the cache
     * attributes no row to holds nothing here, so its first id is recorded with
     * no reset, whether or not its blocks were scanned (P3 plan T10). Returns
     * why it reset, or null.
     */
    String reconcileStoreIds(Map<String, String> ids, Closure<Boolean> watermarkLogged) {
        String reason = null
        final Map<String, String> record = new LinkedHashMap<String, String>()
        for( Map.Entry<String, String> e : ids.entrySet() ) {
            final String alias = e.key
            final String now = e.value
            final String cached = storeIdOf(alias)
            if( cached != null ) {
                if( now != cached && reason == null )
                    reason = "store member '${alias}' was replaced (store-id ${cached} is now ${now ?: 'absent'})".toString()
                if( now != null ) record.put(alias, now)
                continue
            }
            if( now == null )
                continue
            final String wm = watermark(alias)
            final boolean holdsNothing = wm == null && !attributesRowsTo(alias)
            if( holdsNothing ) {
                record.put(alias, now)
                continue
            }
            final Boolean logged = wm == null ? Boolean.FALSE : watermarkLogged.call(alias, wm)
            if( logged == null )
                continue
            if( !logged && reason == null )
                reason = "store member '${alias}' has a store-id this cache never saw, and its rows cannot be shown to be from it (watermark ${wm ?: 'none'} is not in its Store Log)".toString()
            record.put(alias, now)
        }
        if( reason != null ) {
            notice.info("rebuilding the cache index ${file}: ${reason}".toString())
            reset()
        }
        for( Map.Entry<String, String> e : record.entrySet() )
            setMeta(META_STORE_ID + ':' + e.key, e.value)
        return reason
    }

    /**
     * Whether this cache holds a row it attributes to {@code member} (P3 plan
     * T10). Only a scan, a seed or a rebuild of the member brings rows it
     * cannot date, and each marks `block_scan`: 'empty' for a scan that found
     * none of the scanned kinds, 'done' otherwise. After 'done', the member's
     * rows are its run rows (`run.member`), its `log_entry` rows, and any
     * Selection, Claim, Transfer, Project or Handoff no `log_entry` row announces, since nothing
     * records which member such a row came from. Rows a writer ingested itself
     * (a run's {@code ingestRun}, a put) are not counted: the writer created
     * the member's id before it wrote (DESIGN.md §20 decision 2).
     */
    private boolean attributesRowsTo(String member) {
        // Only a catch-up of the member writes its log_entry rows, so they count whatever the scan said:
        // an 'empty' scan whose log catch-up threw before a watermark still leaves them.
        if( anyRow('SELECT 1 FROM log_entry WHERE member = ? LIMIT 1', [(Object) member]) )
            return true
        final String scanned = meta(META_SCANNED + ':' + member)
        if( scanned == null || scanned == SCANNED_EMPTY )
            return false
        if( anyRow('SELECT 1 FROM run WHERE member = ? LIMIT 1', [(Object) member]) )
            return true
        return anyRow('''SELECT 1 FROM collection c WHERE c.kind = 'selection'
                           AND NOT EXISTS (SELECT 1 FROM log_entry e WHERE e.cid = c.collection_cid)
                         UNION ALL
                         SELECT 1 FROM claim c WHERE NOT EXISTS (SELECT 1 FROM log_entry e WHERE e.cid = c.claim_cid)
                         UNION ALL
                         SELECT 1 FROM transfer t WHERE NOT EXISTS (SELECT 1 FROM log_entry e WHERE e.cid = t.cid)
                         UNION ALL
                         SELECT 1 FROM project p WHERE NOT EXISTS (SELECT 1 FROM log_entry e WHERE e.cid = p.project_cid)
                         UNION ALL
                         SELECT 1 FROM handoff h WHERE NOT EXISTS (SELECT 1 FROM log_entry e WHERE e.cid = h.cid)
                         LIMIT 1''', [])
    }

    private boolean anyRow(String sql, List<Object> parameters) {
        boolean found = false
        query(sql, parameters) { ResultSet rs -> found = true }
        return found
    }

    /**
     * Rebuilds from the store alone into a temporary file beside the target,
     * then renames it into place. Reads only DAG-CBOR blocks, never Nextflow's
     * records, whose numbers have been through a Gson round trip.
     */
    void rebuild(BlockStore store, String member) {
        final Path temp = file.resolveSibling(file.fileName.toString() + '.rebuild-' + token())
        deleteDatabase(temp)
        final Index fresh = Index.open(temp)
        try {
            final Map<String, List<Cid>> found = blocksOfKinds(store, SCANNED_KINDS, null)
            for( Cid completion : found.get(Records.RUN_COMPLETION) )
                fresh.ingestRun(store, completion, member)
            for( Cid selection : found.get(Records.SELECTION) )
                fresh.ingestSelection(store, selection, member)
            for( Cid claim : found.get(Records.CLAIM) )
                fresh.ingestClaim(store, claim, member)
            for( Cid transfer : found.get(Records.TRANSFER) )
                fresh.ingestTolerant(store, StoreLogKind.TRANSFER, transfer, member, store)
            for( Cid project : found.get(Records.PROJECT) )
                fresh.ingestTolerant(store, StoreLogKind.PROJECT, project, member, store)
            for( Cid handoff : found.get(Records.HANDOFF) )
                fresh.ingestTolerant(store, StoreLogKind.HANDOFF, handoff, member, store)
            fresh.setMeta(META_SCANNED + ':' + member, 'done')
            try {
                for( StoreLogEntry entry : StoreLog.read(store) )
                    fresh.recordLogEntry(entry, member)
            }
            catch( Exception e ) {
                // A member with no log/ directory lists empty rather than throwing,
                // so reaching this catch is a real failure, not an absent log
                // (DESIGN.md §0 rule 3: derived-structure failures log at warn).
                warnOnce('rebuild-log:' + member, "could not read the Store Log to fill log_entry: ${e.message}")
            }
            // Carry the Store Log watermark forward to the newest logged entry, so
            // the next catchUp reads only what arrives after this rebuild rather
            // than re-scanning the whole log. Correctness-safe either way.
            carryWatermark(fresh, store, member)
        }
        finally {
            fresh.close()
        }
        closeQuietly(connection)
        connection = null
        deleteDatabase(file)
        Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        deleteDatabase(temp)
        connection = connect(file)
    }

    /** Sets the fresh index's watermark for this member to the newest Store Log entry, if any. */
    private static void carryWatermark(Index fresh, BlockStore store, String member) {
        try {
            final List<StoreLogEntry> entries = StoreLog.read(store)
            if( entries )
                fresh.setMeta(watermarkKey(member), entries[0].name)
        }
        catch( Exception e ) {
            // The Store Log is derived; a store with no log storage just
            // leaves the watermark empty, which is correct, only slower.
            log.debug("could not read the Store Log to carry the watermark forward: ${e.message}")
        }
    }

    /**
     * The blocks of the kinds a store announces, found by decoding every
     * `bafy…` block once. Without {@code progress}, a block that cannot be read
     * is passed over (catch-up's scan and rebuild); with it, the read failure
     * propagates and each step is reported.
     */
    private static Map<String, List<Cid>> blocksOfKinds(BlockStore store, Collection<String> kinds, IndexProgress progress) {
        final Map<String, List<Cid>> found = new LinkedHashMap<String, List<Cid>>()
        for( String kind : kinds )
            found.put(kind, new ArrayList<Cid>())
        if( progress != null )
            progress.report(IndexProgress.LISTING, -1L, -1L)
        final List<Cid> metadata = new ArrayList<Cid>()
        final Stream<Cid> blocks = store.listBlocks()
        try {
            for( Object element : blocks.toList() ) {
                final Cid cid = (Cid) element
                if( cid.isDagCbor() )
                    metadata.add(cid)
            }
        }
        finally {
            blocks.close()
        }
        final long total = metadata.size()
        long done = 0L
        if( progress != null )
            progress.report(IndexProgress.READING, done, total)
        for( Cid cid : metadata ) {
            final String kind = progress == null ? kindAt(store, cid) : readKind(store, cid)
            if( kind != null && found.containsKey(kind) )
                found.get(kind).add(cid)
            if( progress != null )
                progress.report(IndexProgress.READING, ++done, total)
        }
        for( List<Cid> list : found.values() )
            Collections.sort(list)
        return found
    }

    /** The kind of a metadata block, or null when it cannot be read or has none. */
    private static String kindAt(BlockStore store, Cid cid) {
        try {
            return readKind(store, cid)
        }
        catch( Exception e ) {
            return null
        }
    }

    /**
     * The kind of a metadata block, or null when it is absent (listed, then
     * swept before this read), too large to be a record, does not decode, or
     * has none. A block that is there but cannot be read throws.
     */
    private static String readKind(BlockStore store, Cid cid) throws IOException {
        final byte[] bytes
        try {
            if( store.size(cid) > MAX_BLOCK_BYTES )
                return null
            bytes = store.open(cid).withCloseable { InputStream input -> input.readAllBytes() }
        }
        catch( NoSuchBlockException e ) {
            return null
        }
        try {
            final Object value = DagCbor.decode(bytes)
            return value instanceof Map ? Records.kindOf((Map) value) : null
        }
        catch( Exception e ) {
            return null
        }
    }

    /**
     * What the scan and rebuild ingest, in dependency order: Claims refer to
     * anything, so after runs and Selections. A Transfer, a Project and a
     * Handoff depend on nothing indexed; a scan also finds one whose entry a
     * stopped writer never wrote.
     */
    private static final List<String> SCANNED_KINDS = [Records.RUN_COMPLETION, Records.SELECTION, Records.CLAIM, Records.TRANSFER,
                                                       Records.PROJECT, Records.HANDOFF]

    /** The Store Log kind each scanned record is ingested as. */
    private static final Map<String, StoreLogKind> SCANNED_LOG_KINDS = [
        (Records.RUN_COMPLETION): StoreLogKind.RUN, (Records.SELECTION): StoreLogKind.SELECTION, (Records.CLAIM): StoreLogKind.CLAIM,
        (Records.TRANSFER): StoreLogKind.TRANSFER, (Records.PROJECT): StoreLogKind.PROJECT, (Records.HANDOFF): StoreLogKind.HANDOFF]

    // --------------------------------------------------------------- queries

    /** Every item, in every run, that carries this content address. */
    List<ProducerRow> producersOf(Cid content) {
        final List<ProducerRow> rows = new ArrayList<ProducerRow>()
        query(SQL_PRODUCERS_OF, [content.toString()]) { ResultSet rs ->
            rows.add(new ProducerRow(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2)),
                Cid.parse(rs.getString(3)), Cid.parse(rs.getString(4)), rs.getString(5)))
        }
        return rows
    }

    /**
     * The most recent run of a pipeline that succeeded and was not marked
     * possibly incomplete. Ties on the finish time break by address.
     */
    Optional<Cid> latestSuccessfulRun(String pipeline) {
        query(SQL_CONFLICTED_DELETIONS_OF_PIPELINE, [pipeline]) { ResultSet rs ->
            warnOnce('conflicted-deletion:' + rs.getString(1),
                "run ${rs.getString(1)} of pipeline '${pipeline}' has a conflicted deletion (more than one current Claim); " +
                'the latest successful run leaves it out until a Claim superseding both resolves it')
        }
        return firstCid(SQL_LATEST_SUCCESSFUL_RUN, [pipeline])
    }

    /**
     * The items of one output of one run, filtered by the metadata view. Each
     * key of {@code where} is one `item_attr` match on path, type and value, or,
     * for a key ending in an operator ({@code 'depth >='}), a numeric comparison
     * with an int or float value, or, for a key ending in {@code !=}, every item
     * with no equal row, items without the path included; several keys
     * intersect. An empty predicate is every item.
     */
    List<Cid> items(Cid completion, String outputName, Map<String, Object> where) {
        return itemRefs(completion, outputName, where)*.item
    }

    /** An item of one output of a run, with the collection that holds it. */
    @Canonical
    static final class ItemRef {
        Cid item
        Cid collection
    }

    /** {@link #items} with each item's collection; the same predicate, the same order. */
    List<ItemRef> itemRefs(Cid completion, String outputName, Map<String, Object> where) {
        final StringBuilder sql = new StringBuilder(SQL_ITEMS_BASE)
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        for( Map.Entry<String, Object> entry : (where ?: [:]).entrySet() ) {
            if( entry.value == null )
                throw new IllegalArgumentException("the predicate on '${entry.key}' is null, which matches nothing: a null field counts as absent and is not indexed (DESIGN.md §12)")
            final List<String> key = Conditions.splitKey(entry.key)
            if( key[1] == Conditions.NOT_EQUAL ) {
                final AttrRow row = MetadataView.scalar(key[0], entry.value)
                if( row.truncated )
                    throw new IllegalArgumentException("the predicate '${entry.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes; values that long are stored as digests, so != cannot tell which items hold it")
                sql.append(SQL_ITEMS_NOT_EQUAL)
                parameters.addAll([row.path, row.type, row.value] as List<Object>)
                continue
            }
            if( key[1] != '=' ) {
                if( !(entry.value instanceof Number) )
                    throw new IllegalArgumentException("the predicate '${entry.key}' compares with ${key[1]}, so its value must be a number, got '${entry.value}'")
                sql.append(SQL_ITEMS_RANGE[key[1]])
                parameters.add(key[0])
                parameters.add(((Number) entry.value).doubleValue())
                continue
            }
            final AttrRow row = MetadataView.scalar(entry.key, entry.value)
            if( row.truncated ) {
                // A value stored as a digest refuses equality, so nothing can match.
                log.warn("the predicate on '${entry.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes and cannot be matched")
                return []
            }
            parameters.add(row.path)
            parameters.add(row.type)
            if( row.value == null ) {
                sql.append(SQL_ITEMS_PREDICATE_NULL)
            }
            else {
                sql.append(SQL_ITEMS_PREDICATE)
                parameters.add(row.value)
            }
        }
        sql.append(SQL_ITEMS_ORDER)
        final List<ItemRef> items = new ArrayList<ItemRef>()
        query(sql.toString(), parameters) { ResultSet rs -> items.add(new ItemRef(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2)))) }
        return items
    }

    /**
     * The items of one output of one run whose metadata view holds, for every
     * condition, a row with that path and that value text, of any type
     * (ticket 05 Q4: {@code lane=2} finds the int 2 and the string "2").
     * A truncated row never matches. Each hit carries its collection. Sorted
     * by item CID, then collection CID.
     */
    List<ItemHit> itemHitsByText(Cid completion, String outputName, Map<String, String> conditions) {
        final StringBuilder sql = new StringBuilder(SQL_ITEM_HITS_BASE)
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        final Map<String, String> all = conditions ?: Collections.<String, String> emptyMap()
        for( Map.Entry<String, String> condition : all.entrySet() ) {
            final List<String> key = Conditions.splitKey(condition.key)
            if( key[1] == Conditions.NOT_EQUAL ) {
                if( condition.value == null || condition.value.getBytes('UTF-8').length > MetadataView.VALUE_CAP_BYTES )
                    throw new IllegalArgumentException("the condition '${condition.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes; values that long are stored as digests, so != cannot tell which items hold it")
                sql.append(SQL_ITEM_HITS_NOT_TEXT)
                parameters.add(key[0])
                parameters.add(condition.value)
                continue
            }
            if( key[1] != '=' ) {
                final BigDecimal bound
                try {
                    bound = Conditions.number(condition.value ?: '')
                }
                catch( NumberFormatException ignored ) {
                    throw new IllegalArgumentException("the condition '${condition.key}' compares with ${key[1]}, so its value must be a number, got '${condition.value}'")
                }
                sql.append(SQL_ITEMS_RANGE[key[1]])
                parameters.add(key[0])
                parameters.add(bound.doubleValue())
                continue
            }
            if( condition.value != null && condition.value.getBytes('UTF-8').length > MetadataView.VALUE_CAP_BYTES ) {
                // Stored as a digest with truncated = 1, which never matches.
                log.warn("the condition on '${condition.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes and cannot be matched")
                return []
            }
            sql.append(SQL_ITEM_HITS_TEXT)
            parameters.add(condition.key)
            parameters.add(condition.value)
        }
        sql.append(SQL_ITEM_HITS_ORDER)
        final List<ItemHit> hits = new ArrayList<ItemHit>()
        query(sql.toString(), parameters) { ResultSet rs ->
            hits.add(new ItemHit(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2))))
        }
        return hits
    }

    /** The run whose RunManifest carries this Nextflow run hash (`lid://<hash>`). */
    Optional<Cid> runByNextflowHash(String nfRunHash) {
        return firstCid('SELECT completion_cid FROM run WHERE nf_run_hash = ? ORDER BY finished_at DESC, completion_cid ASC LIMIT 1',
            [nfRunHash])
    }

    /** The run completing this RunManifest. */
    Optional<Cid> runByManifest(Cid manifest) {
        return firstCid('SELECT completion_cid FROM run WHERE manifest_cid = ? ORDER BY finished_at DESC, completion_cid ASC LIMIT 1',
            [manifest.toString()])
    }

    /** The run's outputs by name. */
    Map<String, Cid> collectionsOf(Cid completion) {
        final Map<String, Cid> collections = new LinkedHashMap<String, Cid>()
        query(SQL_COLLECTIONS_OF, [completion.toString()]) { ResultSet rs ->
            collections.put(rs.getString(1), Cid.parse(rs.getString(2)))
        }
        return collections
    }

    /** The size the block table records for a block, or null when it has none (P3 plan 1.3 sizes). */
    Long blockSize(Cid cid) {
        final List<Long> sizes = new ArrayList<Long>()
        query(SQL_BLOCK_SIZE, [(Object) cid.toString()]) { ResultSet rs ->
            final long size = rs.getLong(1)
            sizes.add(rs.wasNull() ? null : size)
        }
        return sizes ? sizes[0] : null
    }

    /** An item's untruncated `item_attr` rows at one path, sorted by type and value. */
    List<AttrRow> attrsOf(Cid item, String path) {
        final List<AttrRow> rows = new ArrayList<AttrRow>()
        query(SQL_ITEM_ATTR, [item.toString(), path] as List<Object>) { ResultSet rs ->
            rows.add(new AttrRow(path, rs.getString(1), rs.getString(2), 0))
        }
        return rows
    }

    /** Every item of the run, in any of its outputs, with an untruncated row equal on path, type and value; sorted by cid. */
    List<Cid> runItemsWith(Cid completion, String path, String type, String value) {
        final List<Cid> items = new ArrayList<Cid>()
        query(SQL_RUN_ITEMS_WITH_ATTR, [completion.toString(), path, type, value] as List<Object>) { ResultSet rs ->
            items.add(Cid.parse(rs.getString(1)))
        }
        return items
    }

    /** The OutputCollections that list the item, each with its run's RunCompletion, sorted by collection cid. */
    Map<Cid, Cid> outputCollectionsOf(Cid item) {
        final Map<Cid, Cid> out = new LinkedHashMap<Cid, Cid>()
        query(SQL_OUTPUT_COLLECTIONS_OF_ITEM, [(Object) item.toString()]) { ResultSet rs ->
            out.put(Cid.parse(rs.getString(1)), Cid.parse(rs.getString(2)))
        }
        return out
    }

    /**
     * Every Claim a `claim` entry of {@code member}'s Store Log announced, as
     * ClaimState rows grouped by subject (P3 plan 1.4: only the source's own
     * Claims decide what is current for a copy from it). `supersedes` lists
     * every link the Claim makes, whoever logged the Claim it names.
     */
    Map<Cid, List<ClaimState.Row>> claimsLoggedIn(String member) {
        final Map<String, ClaimState.Row> rows = new LinkedHashMap<String, ClaimState.Row>()
        final Map<String, Cid> subjects = new HashMap<String, Cid>()
        query(SQL_CLAIMS_LOGGED_IN, [(Object) member]) { ResultSet rs ->
            final String cid = rs.getString(1)
            rows.put(cid, new ClaimState.Row(cid, rs.getString(3), rs.getString(4), rs.getString(5), new ArrayList<String>()))
            subjects.put(cid, Cid.parse(rs.getString(2)))
        }
        query(SQL_CLAIM_SUPERSEDES_LOGGED_IN, [(Object) member]) { ResultSet rs ->
            rows.get(rs.getString(1))?.supersedes?.add(rs.getString(2))
        }
        final Map<Cid, List<ClaimState.Row>> bySubject = new LinkedHashMap<Cid, List<ClaimState.Row>>()
        for( ClaimState.Row row : rows.values() ) {
            Collections.sort(row.supersedes)
            bySubject.computeIfAbsent(subjects.get(row.cid)) { Cid s -> new ArrayList<ClaimState.Row>() }.add(row)
        }
        return bySubject
    }

    /**
     * Every Handoff a `handoff` entry of {@code member}'s Store Log announced,
     * by the Selection it cites, each list sorted by cid (a copy from that
     * member carries the hand-offs of the Selections it carries).
     */
    Map<Cid, List<Cid>> handoffsLoggedIn(String member) {
        final Map<Cid, List<Cid>> bySelection = new LinkedHashMap<Cid, List<Cid>>()
        query(SQL_HANDOFFS_LOGGED_IN, [(Object) member]) { ResultSet rs ->
            bySelection.computeIfAbsent(Cid.parse(rs.getString(1))) { Cid s -> new ArrayList<Cid>() }.add(Cid.parse(rs.getString(2)))
        }
        return bySelection
    }

    /** One Claim as the index holds it, its value as text (decision 4); null when it is not indexed. */
    Claim claimOf(Cid claimCid) {
        final List<Claim> found = new ArrayList<Claim>()
        final List<Cid> supersedes = new ArrayList<Cid>()
        query(SQL_CLAIM_SUPERSEDES, [(Object) claimCid.toString()]) { ResultSet rs ->
            supersedes.add(Cid.parse(rs.getString(1)))
        }
        query(SQL_CLAIM, [(Object) claimCid.toString()]) { ResultSet rs ->
            found.add(new Claim(rs.getString(6), Cid.parse(rs.getString(1)), rs.getString(2), rs.getString(3), rs.getString(4),
                supersedes, rs.getString(5)))
        }
        return found ? found[0] : null
    }

    /** Distinct pipelines any run names, sorted. */
    List<String> pipelines() {
        final List<String> out = []
        query('SELECT DISTINCT pipeline FROM run WHERE pipeline IS NOT NULL ORDER BY pipeline', []) { ResultSet rs -> out.add(rs.getString(1)) }
        return out
    }

    /** Every run of the pipeline, newest finished_at first, ties by completion cid. */
    List<IndexedRun> runsOf(String pipeline) {
        final List<IndexedRun> out = []
        query('SELECT completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at FROM run ' +
              'WHERE pipeline = ? ORDER BY finished_at DESC, completion_cid', [pipeline]) { ResultSet rs ->
            out.add(new IndexedRun(Cid.parse(rs.getString(1)), rs.getString(2), rs.getString(3), rs.getString(4),
                rs.getInt(5) != 0, rs.getString(6)))
        }
        return out
    }

    /**
     * Removes every row keyed by these cids as a run (its inputs, consumer
     * rows and InputSet wait record with it), collection, item, Selection,
     * Claim, Transfer, Project or Handoff, and their claim_current, in one
     * transaction.
     * Producer rows are kept (plan decision 7): after a sweep deletes
     * blocks, their rows go too, so a snapshot seeds nothing whose blocks
     * are gone, but `producersOf` still answers for content a forgotten run
     * once produced.
     */
    void forget(Collection<Cid> cids) {
        if( !cids )
            return
        withTransaction {
            for( Cid c : cids ) {
                final String t = c.toString()
                for( String sql : [
                        'DELETE FROM run WHERE completion_cid = ?',
                        'DELETE FROM collection WHERE collection_cid = ?',
                        'DELETE FROM collection_item WHERE collection_cid = ?',
                        'DELETE FROM collection_item WHERE item_cid = ?',
                        'DELETE FROM item WHERE item_cid = ?',
                        'DELETE FROM item_attr WHERE item_cid = ?',
                        'DELETE FROM selection_child WHERE parent_cid = ?',
                        'DELETE FROM selection_derived WHERE selection_cid = ?',
                        'DELETE FROM claim_supersedes WHERE claim_cid = ?',
                        'DELETE FROM claim_current WHERE claim_cid = ?',
                        'DELETE FROM claim_current WHERE subject_cid = ?',
                        'DELETE FROM claim WHERE claim_cid = ?',
                        'DELETE FROM transfer_root WHERE transfer_cid = ?',
                        'DELETE FROM transfer WHERE cid = ?',
                        'DELETE FROM consumer WHERE completion_cid = ?',
                        'DELETE FROM run_input WHERE completion_cid = ?',
                        'DELETE FROM input_origin WHERE completion_cid = ?',
                        'DELETE FROM item_input WHERE completion_cid = ?',
                        'DELETE FROM item_task WHERE completion_cid = ?',
                        'DELETE FROM project WHERE project_cid = ?',
                        'DELETE FROM handoff WHERE cid = ?',
                        'DELETE FROM log_entry WHERE cid = ?',
                        'DELETE FROM missing WHERE have_cid = ? OR needed_cid = ?'] )
                    update(sql, sql.count('?') == 2 ? [t, t] as List<Object> : [t] as List<Object>)
                update('DELETE FROM meta WHERE key = ?', [(Object) (INPUTSET_WAIT + t)])
                update('DELETE FROM meta WHERE key = ?', [(Object) (ITEMLINEAGE_WAIT + t)])
            }
            dropUnreachableContentMissing()
            // A forgotten pin or retain Claim may leave a block releasing-only: every member looks again.
            dropPresent()
            // A subject some of whose Claims went keeps the rest: recompute its current state.
            final Set<String> subjects = new LinkedHashSet<String>()
            query('SELECT DISTINCT subject_cid FROM claim', []) { ResultSet rs -> subjects.add(rs.getString(1)) }
            for( String s : subjects )
                ClaimCurrent.rewrite(connection, s)
        }
    }

    /**
     * A directory's `missing(owner, manifest)` row whose owner no remaining
     * item reaches would be retried at every catch-up for nothing: drops it.
     * The block and block_ref rows stay, as facts about immutable blocks.
     */
    private void dropUnreachableContentMissing() {
        boolean any = false
        query('SELECT 1 FROM missing WHERE have_cid IN (SELECT owner_cid FROM block_ref) LIMIT 1', []) { ResultSet rs -> any = true }
        if( !any )
            return
        update('''DELETE FROM missing WHERE have_cid IN (SELECT owner_cid FROM block_ref) AND have_cid NOT IN (
                      WITH RECURSIVE owner(cid) AS (
                          SELECT item_cid FROM collection_item
                          UNION
                          SELECT cid FROM handoff
                          UNION
                          SELECT r.cid FROM block_ref r JOIN owner o ON r.owner_cid = o.cid
                          JOIN block b ON b.cid = r.cid AND b.codec = 'dag-cbor')
                      SELECT cid FROM owner)''', [])
    }

    /** Marks the index as out of step with the store, so a rebuild is owed. */
    void markStale() { setMeta(META_STALE, '1') }

    boolean isStale() { meta(META_STALE) == '1' }

    /** The cids a snapshot build of {@code member} found gone from its store (`block_absent`). */
    Set<String> absentBlocks(String member) {
        final Set<String> out = new HashSet<String>()
        if( !absentMemo )
            return out
        query('SELECT cid FROM block_absent WHERE member = ?', [(Object) member]) { ResultSet rs -> out.add(rs.getString(1)) }
        return out
    }

    /**
     * Records that these cids are gone from {@code member}'s store. A build
     * asks again only about one a held or pinned root reaches.
     */
    void recordAbsent(String member, Collection<String> cids) {
        if( !cids || !absentMemo )
            return
        withTransaction {
            for( String cid : cids )
                update('INSERT OR IGNORE INTO block_absent(member, cid) VALUES (?, ?)', [(Object) member, cid])
        }
    }

    /**
     * Forgets these absences of {@code member}, in one statement: a block a
     * look found back in the store, or one no root reaches any more, is
     * looked up afresh if a released root reaches it later.
     */
    void clearAbsent(String member, Collection<String> cids) {
        if( !cids || !absentMemo )
            return
        update('DELETE FROM block_absent WHERE member = ? AND cid IN (SELECT value FROM json_each(?))',
            [(Object) member, groovy.json.JsonOutput.toJson(new ArrayList<String>(cids))])
    }

    /** The cids a snapshot build of {@code member} found still in its store under a held or pinned root (`block_present`). */
    Set<String> presentBlocks(String member) {
        final Set<String> out = new HashSet<String>()
        if( !absentMemo )
            return out
        query('SELECT cid FROM block_present WHERE member = ?', [(Object) member]) { ResultSet rs -> out.add(rs.getString(1)) }
        return out
    }

    /**
     * As {@link #recordPresent(String, Collection)}, only while
     * `block_present_generation` is still {@code generation}, checked in the
     * same transaction as the insert. False when it changed (a Claim, a seed
     * or a forget since the build read the roots): nothing is recorded, and
     * the next build looks again.
     */
    boolean recordPresentIf(String member, Collection<String> cids, String generation) {
        if( !cids || !absentMemo )
            return false
        boolean recorded = false
        withTransaction {
            if( presentGeneration() != generation )
                return
            for( String cid : cids )
                update('INSERT OR IGNORE INTO block_present(member, cid) VALUES (?, ?)', [(Object) member, cid])
            recorded = true
        }
        return recorded
    }

    /** Records that these cids are in {@code member}'s store, so no later build asks while a held or pinned root reaches them. */
    void recordPresent(String member, Collection<String> cids) {
        if( !cids || !absentMemo )
            return
        withTransaction {
            for( String cid : cids )
                update('INSERT OR IGNORE INTO block_present(member, cid) VALUES (?, ?)', [(Object) member, cid])
        }
    }

    /** Forgets these presences of {@code member}, in one statement: a block only released or deleted roots reach, or none. */
    void clearPresent(String member, Collection<String> cids) {
        if( !cids || !absentMemo )
            return
        update('DELETE FROM block_present WHERE member = ? AND cid IN (SELECT value FROM json_each(?))',
            [(Object) member, groovy.json.JsonOutput.toJson(new ArrayList<String>(cids))])
    }

    /**
     * A run that names a block recorded as absent may have written it again.
     * Every block the run's items reach is looked up in the member's `block_absent`, and
     * one now in the store loses its row. A cache with no absent blocks, the
     * usual case, does no more than one query.
     */
    private void clearAbsentNamedBy(BlockStore store, Cid completion, String member) {
        if( !absentMemo )
            return
        boolean any = false
        query('SELECT 1 FROM block_absent WHERE member = ? LIMIT 1', [(Object) member]) { ResultSet rs -> any = true }
        if( !any )
            return
        final List<String> named = new ArrayList<String>()
        query('''WITH RECURSIVE owner(cid) AS (
                     SELECT ci.item_cid FROM collection c JOIN collection_item ci ON ci.collection_cid = c.collection_cid
                     WHERE c.completion_cid = ?
                     UNION
                     SELECT r.cid FROM block_ref r JOIN owner o ON r.owner_cid = o.cid
                     JOIN block b ON b.cid = r.cid AND b.codec = 'dag-cbor')
                 SELECT DISTINCT r.cid FROM owner o JOIN block_ref r ON r.owner_cid = o.cid
                 WHERE r.cid IN (SELECT cid FROM block_absent WHERE member = ?)''', [(Object) completion.toString(), member]) { ResultSet rs ->
            named.add(rs.getString(1))
        }
        for( String cid : named ) {
            try {
                if( store.has(Cid.parse(cid)) )
                    update('DELETE FROM block_absent WHERE member = ? AND cid = ?', [(Object) member, cid])
            }
            catch( Exception e ) {
                // Failing to look keeps the row; the block stays out of the figures until a later run names it.
                log.debug("could not tell whether ${cid} is back in the store", e)
            }
        }
    }

    /** Rows in one of the tables of ddl(); for tests. */
    @groovy.transform.PackageScope
    int countRows(String table) {
        if( !ddl().any { String sql -> sql.startsWith("CREATE TABLE ${table}(".toString()) } )
            throw new IllegalArgumentException("not an index table: ${table}")
        final List<Integer> n = []
        query("SELECT count(*) FROM ${table}".toString(), []) { ResultSet rs -> n.add(rs.getInt(1)) }
        return n[0]
    }

    // --------------------------------------------------------------- plumbing

    private String meta(String key) {
        final List<String> values = new ArrayList<String>()
        query('SELECT value FROM meta WHERE key = ?', [key]) { ResultSet rs -> values.add(rs.getString(1)) }
        return values ? values[0] : null
    }

    private void setMeta(String key, String value) {
        update('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)', [key, value])
    }

    private Optional<Cid> firstCid(String sql, List<Object> parameters) {
        final List<Cid> found = new ArrayList<Cid>()
        query(sql, parameters) { ResultSet rs -> found.add(Cid.parse(rs.getString(1))) }
        return found ? Optional.of(found[0]) : Optional.<Cid> empty()
    }

    private void query(String sql, List<Object> parameters, Closure consume) {
        final PreparedStatement statement = connection.prepareStatement(sql)
        try {
            bind(statement, parameters)
            final ResultSet rs = statement.executeQuery()
            while( rs.next() )
                consume.call(rs)
        }
        catch( SQLException e ) {
            throw new IllegalStateException("index query failed: ${e.message}", e)
        }
        finally {
            statement.close()
        }
    }

    private void update(String sql, List<Object> parameters) {
        final PreparedStatement statement = connection.prepareStatement(sql)
        try {
            bind(statement, parameters)
            statement.executeUpdate()
        }
        catch( SQLException e ) {
            throw new IllegalStateException("index write failed: ${e.message}", e)
        }
        finally {
            statement.close()
        }
    }

    private static void bind(PreparedStatement statement, List<Object> parameters) {
        for( int i = 0; i < parameters.size(); i++ )
            statement.setObject(i + 1, parameters[i])
    }

    private void withTransaction(Closure work) {
        try {
            connection.setAutoCommit(false)
            work.call()
            connection.commit()
        }
        catch( Exception e ) {
            try {
                connection.rollback()
            }
            catch( SQLException rollbackFailure ) {
                log.debug("rolling the index transaction back failed: ${rollbackFailure.message}")
            }
            throw e
        }
        finally {
            connection.setAutoCommit(true)
        }
    }

    // ------------------------------------------------------------ block reads

    /**
     * The decoded block when it has arrived and is of the expected kind, else
     * null. Absence is a fact to record, never a failure to raise.
     */
    private static Map readBlock(BlockStore store, Cid cid, String expectedKind) {
        return readBlock(store, cid, expectedKind, false)
    }

    /**
     * As the 3-arg {@code readBlock}; with {@code ioFails} a read that fails
     * (an IOException other than absence) is thrown, not returned as null, so
     * the caller can tell a block that is there but unreadable for now from
     * one that will never decode.
     */
    private static Map readBlock(BlockStore store, Cid cid, String expectedKind, boolean ioFails) {
        if( cid == null || !cid.isDagCbor() || !store.has(cid) )
            return null
        try {
            if( store.size(cid) > MAX_BLOCK_BYTES ) {
                log.warn("block $cid is larger than $MAX_BLOCK_BYTES bytes; it is not a metadata block")
                return null
            }
            byte[] bytes
            final InputStream input = store.open(cid)
            try {
                bytes = input.readAllBytes()
            }
            finally {
                input.close()
            }
            final Object value = DagCbor.decode(bytes)
            if( !(value instanceof Map) )
                return null
            final Map block = (Map) value
            return block.get('kind') == expectedKind ? block : null
        }
        catch( NoSuchBlockException e ) {
            return null
        }
        catch( IOException | UncheckedIOException e ) {
            if( ioFails )
                throw e
            if( !failedRead(store) )
                log.warn("block $cid could not be decoded as $expectedKind: ${e.message}")
            return null
        }
        catch( Exception e ) {
            if( !failedRead(store) )
                log.warn("block $cid could not be decoded as $expectedKind: ${e.message}")
            return null
        }
    }

    /** True when {@code store} is an indexMember scan whose reads have failed: indexMember throws that failure, so a block it stopped is not a decoding problem. */
    private static boolean failedRead(BlockStore store) {
        return store instanceof ReadFailures && ((ReadFailures) store).failed()
    }

    /** Every Leaf in an item's value, depth first (DESIGN.md §6). */
    static List<Map> leavesOf(Object value) {
        final List<Map> leaves = new ArrayList<Map>()
        collectLeaves(value, leaves)
        return leaves
    }

    private static void collectLeaves(Object value, List<Map> leaves) {
        if( MetadataView.isLeaf(value) ) {
            leaves.add((Map) value)
            return
        }
        if( value instanceof Map ) {
            for( Object entry : ((Map) value).entrySet() )
                collectLeaves(((Map.Entry) entry).value, leaves)
            return
        }
        if( value instanceof List )
            for( Object element : (List) value )
                collectLeaves(element, leaves)
    }

    private static List asList(Object value) {
        return value instanceof List ? (List) value : Collections.emptyList()
    }

    private static Cid asCid(Object value) {
        if( value instanceof Cid )
            return (Cid) value
        if( value instanceof String && Cid.isCid((String) value) )
            return Cid.parse((String) value)
        return null
    }

    private static Long asLong(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : null
    }

    private static String text(Object value) {
        return value == null ? null : value.toString()
    }

    private static boolean truth(Object value) {
        if( value instanceof Boolean )
            return (Boolean) value
        if( value instanceof Number )
            return ((Number) value).longValue() != 0
        return false
    }

    private static String token() {
        final byte[] bytes = new byte[6]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }

    @Override
    String toString() { "Index[$file]" }
}
