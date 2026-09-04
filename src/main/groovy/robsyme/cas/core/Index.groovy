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
    static final int SCHEMA_VERSION = 1

    private static final String META_WATERMARK = 'run_log_watermark'
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

    /** The schema of DESIGN.md §12, verbatim in its column names. */
    private static void createSchema(Connection connection) {
        final List<String> ddl = [
            'CREATE TABLE schema_version(version INTEGER NOT NULL)',
            '''CREATE TABLE run(
                 completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,
                 commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,
                 status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)''',
            'CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)',
            'CREATE INDEX run_nf_run_hash ON run(nf_run_hash)',
            'CREATE INDEX run_manifest_cid ON run(manifest_cid)',
            'CREATE TABLE collection(collection_cid TEXT PRIMARY KEY, completion_cid TEXT, output_name TEXT)',
            'CREATE TABLE item(item_cid TEXT PRIMARY KEY)',
            'CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT)',
            '''CREATE TABLE producer(
                 content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)''',
            'CREATE INDEX producer_content_cid ON producer(content_cid)',
            'CREATE TABLE consumer(content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT)',
            'CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)',
            'CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)',
            '''CREATE TABLE claim_current(
                 subject_cid TEXT, attribute TEXT, value TEXT, claim_cid TEXT, conflicted INTEGER)''',
            'CREATE TABLE missing(have_cid TEXT, needed_cid TEXT)',
            '''CREATE TABLE nf_record(
                 key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,
                 labels_json TEXT, block_cid TEXT)''',
            // Not in §12's schema: the watermark and the stale mark, which are
            // this build's own bookkeeping rather than indexed block content.
            'CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)',
        ]
        final Statement statement = connection.createStatement()
        try {
            for( String sql : ddl )
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
                log.warn("run completion $completion has not arrived; nothing of it can be indexed")
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
        update('INSERT OR REPLACE INTO collection(collection_cid, completion_cid, output_name) VALUES (?, ?, ?)',
            [collectionCid.toString(), completion.toString(), text(collection.get('name'))])
        for( Object link : asList(collection.get('items')) ) {
            // A null entry is a hole: Nextflow handed us a null item and §6
            // keeps its position rather than compacting it away.
            final Cid itemCid = asCid(link)
            if( itemCid == null )
                continue
            update('INSERT OR IGNORE INTO item(item_cid) VALUES (?)', [itemCid.toString()])
            update('INSERT INTO collection_item(collection_cid, item_cid) VALUES (?, ?)',
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
     * Ingests every Run Log entry newer than the stored watermark, oldest
     * first, then advances the watermark to the newest entry seen.
     */
    void catchUp(BlockStore store, RunLog log, String member) {
        final List<RunLogEntry> entries = log.entriesAfter(meta(META_WATERMARK))
        if( !entries )
            return
        // entriesAfter is newest first; ingest in the order the runs finished.
        for( int i = entries.size() - 1; i >= 0; i-- )
            ingestRun(store, entries[i].cid, member)
        setMeta(META_WATERMARK, entries[0].name)
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
        query('SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?',
            [content.toString()]) { ResultSet rs ->
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
        return firstCid('''SELECT completion_cid FROM run
                           WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0
                           ORDER BY finished_at DESC, completion_cid ASC LIMIT 1''', [pipeline])
    }

    /**
     * The items of one output of one run, filtered by the metadata view. Each
     * key of {@code where} is one `item_attr` match on path, type and value;
     * several keys intersect. An empty predicate is every item.
     */
    List<Cid> items(Cid completion, String outputName, Map<String, Object> where) {
        final StringBuilder sql = new StringBuilder(
            '''SELECT ci.item_cid FROM collection_item ci
               JOIN collection c ON c.collection_cid = ci.collection_cid
               WHERE c.completion_cid = ? AND c.output_name = ?''')
        final List<Object> parameters = new ArrayList<Object>([completion.toString(), outputName] as List<Object>)
        for( Map.Entry<String, Object> entry : (where ?: [:]).entrySet() ) {
            final AttrRow row = MetadataView.scalar(entry.key, entry.value)
            if( row.truncated ) {
                // A value stored as a digest refuses equality, so nothing can match.
                log.warn("the predicate on '${entry.key}' is longer than ${MetadataView.VALUE_CAP_BYTES} bytes and cannot be matched")
                return []
            }
            sql.append(' AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ?')
            parameters.add(row.path)
            parameters.add(row.type)
            if( row.value == null ) {
                sql.append(' AND a.value IS NULL)')
            }
            else {
                sql.append(' AND a.value = ?)')
                parameters.add(row.value)
            }
        }
        sql.append(' ORDER BY ci.item_cid')
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
        query('SELECT output_name, collection_cid FROM collection WHERE completion_cid = ? ORDER BY output_name',
            [completion.toString()]) { ResultSet rs ->
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
