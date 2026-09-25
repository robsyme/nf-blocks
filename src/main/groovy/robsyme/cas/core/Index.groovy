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
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

import groovy.transform.CompileStatic
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
    static final int SCHEMA_VERSION = 3

    // The SQL the block explorer's page runs too (DESIGN.md §12, §15). The page
    // holds a copy in web/src/queries.json; ExplorerQueriesTest pins them equal.

    static final String SQL_PRODUCERS_OF =
        'SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?'

    static final String SQL_LATEST_SUCCESSFUL_RUN =
        "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1"

    // ci.collection_cid rides along so the page can link each item without a
    // second lookup; it is on the collection_item row already read, so it costs no page.
    static final String SQL_ITEMS_BASE =
        'SELECT ci.item_cid, ci.collection_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ?'

    static final String SQL_ITEMS_PREDICATE =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?)'

    static final String SQL_ITEMS_PREDICATE_NULL =
        ' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value IS NULL)'

    static final String SQL_ITEMS_ORDER = ' ORDER BY ci.item_cid'

    static final String SQL_COLLECTIONS_OF =
        'SELECT output_name, collection_cid FROM collection WHERE completion_cid = ? ORDER BY output_name'

    private static final String META_WATERMARK = 'store_log_watermark'
    private static final String META_SCANNED = 'block_scan'
    private static final String META_STALE = 'stale'

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

    private final Path file
    private Connection connection

    private Index(Path file, Connection connection) {
        this.file = file
        this.connection = connection
    }

    /** The file this index writes to. */
    Path getFile() { file }

    // ------------------------------------------------------------- lifecycle

    /**
     * Opens the index at {@code sqliteFile}, creating the schema when it is
     * new and recreating it from empty when the stored schema version is not
     * this build's. There is no migration path, by design.
     */
    static Index open(Path sqliteFile) {
        if( sqliteFile.parent != null )
            Files.createDirectories(sqliteFile.parent)
        if( Files.exists(sqliteFile) ) {
            Connection existing = null
            try {
                existing = connect(sqliteFile)
                final int version = schemaVersionOf(existing)
                if( version == SCHEMA_VERSION )
                    return new Index(sqliteFile, existing)
                log.warn("index at $sqliteFile has schema version $version, not $SCHEMA_VERSION; recreating it")
            }
            catch( SQLException e ) {
                log.warn("index at $sqliteFile could not be opened (${e.message}); recreating it")
            }
            closeQuietly(existing)
            deleteDatabase(sqliteFile)
        }
        final Connection created = connect(sqliteFile)
        createSchema(created)
        return new Index(sqliteFile, created)
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
        ]
    }

    /** The schema of DESIGN.md §12, verbatim in its column names. */
    private static void createSchema(Connection connection) {
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
    }

    // ---------------------------------------------------------------- ingest

    /**
     * Indexes one RunCompletion and its closure: the RunManifest, every
     * OutputCollection, every OutputItem and every Leaf. Idempotent: the run's
     * rows are replaced, not added to. A referenced block that has not arrived
     * leaves a `missing(have, needed)` row and the rest of the run is indexed.
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
            ingestItem(itemCid, collectionCid, completion, item.get('value'))
        }
    }

    private void ingestItem(Cid itemCid, Cid collectionCid, Cid completion, Object value) {
        // Attributes are a function of the item block alone, so they are
        // rewritten rather than accumulated across the runs that share it.
        update('DELETE FROM item_attr WHERE item_cid = ?', [itemCid.toString()])
        for( AttrRow row : MetadataView.flatten(MetadataView.of(value)) )
            update('INSERT INTO item_attr(item_cid, path, type, value, truncated) VALUES (?, ?, ?, ?, ?)',
                [itemCid.toString(), row.path, row.type, row.value, (Object) row.truncated])
        for( Map leaf : leavesOf(value) ) {
            final Cid address = asCid(leaf.get('address'))
            // An unaddressed, declined or never published leaf has nothing
            // content-derived to answer with, so it is not a producer.
            if( address == null )
                continue
            update('INSERT INTO producer(content_cid, item_cid, collection_cid, completion_cid, filename) VALUES (?, ?, ?, ?, ?)',
                [address.toString(), itemCid.toString(), collectionCid.toString(), completion.toString(),
                 text(leaf.get('name'))])
        }
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

    /** Everything this run asserted, so an ingest can replace rather than add. */
    private void clearRun(Cid completion) {
        final String text = completion.toString()
        update('''DELETE FROM missing
                  WHERE have_cid = ?
                     OR (have_cid IS NULL AND needed_cid = ?)
                     OR have_cid IN (SELECT collection_cid FROM collection WHERE completion_cid = ?)''',
            [text, text, text])
        update('DELETE FROM collection_item WHERE collection_cid IN (SELECT collection_cid FROM collection WHERE completion_cid = ?)', [text])
        update('DELETE FROM producer WHERE completion_cid = ?', [text])
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
     * overlap costs a listing, never a block read. Only `run` entries are
     * ingested for now; Selections and Claims arrive with the explorer.
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member) {
        // The watermark is per member: in a composition each member's log
        // advances independently (DESIGN.md §12).
        scanOnce(store, member)
        retryMissingLogged(store, member)
        final String key = watermarkKey(member)
        final List<StoreLogEntry> entries = storeLog.entriesSince(meta(key))
        if( !entries )
            return
        // entriesSince is newest first; ingest in the order they were written.
        for( int i = entries.size() - 1; i >= 0; i-- ) {
            final StoreLogEntry entry = entries[i]
            recordLogEntry(entry, member)
            ingestLogged(store, entry.kind, entry.cid, member)
        }
        setMeta(key, entries[0].name)
    }

    /**
     * Ingests one block a Store Log announced, unless it is already indexed.
     * Every catch-up, retry and scan path comes through here, so a new kind
     * is one branch. A block that cannot be read is recorded as `missing`.
     */
    private void ingestLogged(BlockStore store, StoreLogKind kind, Cid cid, String member) {
        switch( kind ) {
            case StoreLogKind.RUN:
                if( !isRunIndexed(cid) )
                    ingestTolerant(store, cid, member)
                return
            default:
                log.debug("store log entry for ${cid} is a ${kind.token}; ingested from Task 4 and Task 6 on")
        }
    }

    /**
     * The first catch-up for a member this index has never scanned finds its
     * runs from the blocks: a new index, one recreated for a schema change,
     * or a store written before the Store Log existed, whose runs no log
     * lists. Recorded per member, so it happens once.
     */
    private void scanOnce(BlockStore store, String member) {
        final String key = META_SCANNED + ':' + member
        if( meta(key) != null )
            return
        final List<Cid> completions
        try {
            completions = runCompletionsIn(store)
        }
        catch( IOException | UncheckedIOException e ) {
            // Not marked done, so the next catch-up scans again.
            log.warn("could not scan the blocks of store member '$member' (${e.message}); will retry")
            return
        }
        for( Cid completion : completions )
            if( !isRunIndexed(completion) )
                ingestTolerant(store, completion, member)
        setMeta(key, 'done')
    }

    /**
     * A logged block that had not arrived was recorded as `missing`. Retried
     * on every catch-up, so it is indexed once its block lands even after the
     * overlap window has moved past its entry. A `missing` row with no
     * `log_entry` (a run found by the block scan, not the log) defaults to RUN.
     */
    private void retryMissingLogged(BlockStore store, String member) {
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
                    ingestLogged(store, each.value, each.key, member)
            }
            catch( IOException | UncheckedIOException e ) {
                warnOnce(each.key.toString(), "block ${each.key} is still unreachable (${e.message}); will retry")
            }
        }
    }

    /**
     * Ingests one logged run. A block that cannot be read (a permission error,
     * a stale network handle) is not an absence: it is recorded as `missing`
     * so later catch-ups retry it, and the rest of the member still ingests.
     */
    private void ingestTolerant(BlockStore store, Cid completion, String member) {
        try {
            ingestRun(store, completion, member)
        }
        catch( IOException | UncheckedIOException e ) {
            warnOnce(completion.toString(), "run completion $completion could not be read (${e.message}); it will be retried")
            update('DELETE FROM missing WHERE have_cid IS NULL AND needed_cid = ?', [completion.toString()])
            insertMissing(null, completion)
        }
    }

    /** Keys already warned about, so a recurring condition warns once per JVM. */
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    private static void warnOnce(String key, String message) {
        if( WARNED.add(key) )
            log.warn(message)
        else
            log.debug(message)
    }

    /** True when a RunCompletion already has its `run` row. */
    boolean isRunIndexed(Cid completion) {
        boolean found = false
        query('SELECT 1 FROM run WHERE completion_cid = ?', [completion.toString()]) { ResultSet rs -> found = true }
        return found
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
            for( Cid completion : runCompletionsIn(store) )
                fresh.ingestRun(store, completion, member)
            fresh.setMeta(META_SCANNED + ':' + member, 'done')
            try {
                for( StoreLogEntry entry : StoreLog.read(store) )
                    fresh.recordLogEntry(entry, member)
            }
            catch( Exception e ) {
                log.debug("could not read the Store Log to fill log_entry: ${e.message}")
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

    /** Every RunCompletion in the store, found by decoding each `bafy…` block. */
    private static List<Cid> runCompletionsIn(BlockStore store) {
        final List<Cid> found = new ArrayList<Cid>()
        final Stream<Cid> blocks = store.listBlocks()
        try {
            for( Object element : blocks.toList() ) {
                final Cid cid = (Cid) element
                if( !cid.isDagCbor() )
                    continue
                if( readBlock(store, cid, 'RunCompletion') != null )
                    found.add(cid)
            }
        }
        finally {
            blocks.close()
        }
        Collections.sort(found)
        return found
    }

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
        return firstCid(SQL_LATEST_SUCCESSFUL_RUN, [pipeline])
    }

    /**
     * The items of one output of one run, filtered by the metadata view. Each
     * key of {@code where} is one `item_attr` match on path, type and value;
     * several keys intersect. An empty predicate is every item.
     */
    List<Cid> items(Cid completion, String outputName, Map<String, Object> where) {
        final StringBuilder sql = new StringBuilder(SQL_ITEMS_BASE)
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        for( Map.Entry<String, Object> entry : (where ?: [:]).entrySet() ) {
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
        // SQL_ITEMS_BASE also selects ci.collection_cid; only column 1 (item_cid)
        // is read here, since Index.items returns items, not (item, collection) pairs.
        final List<Cid> items = new ArrayList<Cid>()
        query(sql.toString(), parameters) { ResultSet rs -> items.add(Cid.parse(rs.getString(1))) }
        return items
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

    /** Marks the index as out of step with the store, so a rebuild is owed. */
    void markStale() { setMeta(META_STALE, '1') }

    boolean isStale() { meta(META_STALE) == '1' }

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
        catch( Exception e ) {
            log.warn("block $cid could not be decoded as $expectedKind: ${e.message}")
            return null
        }
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
