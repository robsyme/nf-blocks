package robsyme.cas.lineage

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Stream

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortOperationException
import nextflow.exception.AbortRunException
import nextflow.file.FileHelper
import nextflow.lineage.DefaultLinStore
import nextflow.lineage.LinHistoryLog
import nextflow.lineage.LinStore
import nextflow.lineage.config.LineageConfig
import nextflow.lineage.model.v1beta1.Checksum
import nextflow.lineage.model.v1beta1.FileOutput
import nextflow.lineage.model.v1beta1.WorkflowRun
import nextflow.lineage.serde.LinSerializable
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.Coordinates
import robsyme.cas.core.StoreRef

/**
 * The lineage store behind `lineage.store.location = 'cas://<alias>'`.
 *
 * Nextflow's own records live in `<writable>/nf`, the layout
 * {@link DefaultLinStore} writes, so `lid://` reads keep working
 * (DESIGN.md sections 5 and 10). The one thing this store changes on the way
 * to the delegate is the {@code path} of a published {@link FileOutput}: a
 * Publish Coordinate (`cas://<alias>/…`) is a mutable name the next run
 * overwrites, so it is rewritten to the immutable Store URI (`cas://<cid>/…`)
 * the coordinate now resolves to, and the fingerprint recomputed against it so
 * a `lid://` read does not warn.
 */
@Slf4j
@CompileStatic
class CasLinStore implements LinStore {

    /** Sub-directory of the writable member holding Nextflow's own lineage records. */
    static final String NEXTFLOW_RECORDS = 'nf'

    private static final String SCHEME_PREFIX = Coordinates.SCHEME + '://'

    private CasConfig casConfig

    /** The writable member's records, where every save lands. */
    private DefaultLinStore delegate

    /**
     * Every member's records in resolve order, writable first. A `lid://` from a
     * run recorded in a read-only member (a producer store mounted here) resolves
     * because reads consult them all, first hit wins (DESIGN.md §12).
     */
    private List<DefaultLinStore> readers

    CasConfig getCasConfig() { casConfig }

    Path getRecordsLocation() { delegate?.location }

    @Override
    CasLinStore open(LineageConfig config) {
        this.casConfig = CasConfig.from(sessionConfig(), config?.store?.location)
        final records = casConfig.writableLocation.resolve(NEXTFLOW_RECORDS)
        try {
            Files.createDirectories(records)
        }
        catch( IOException e ) {
            throw new AbortOperationException("Unable to create lineage store directory: ${records} -- cause: ${e.message}", e)
        }
        this.delegate = openRecords(records)
        // Build the reader chain: the writable member, then every other member
        // that already holds an nf/ tree. A member with none contributes no
        // records and is skipped, so we never write into a read-only member.
        final List<DefaultLinStore> chain = new ArrayList<DefaultLinStore>()
        chain.add(delegate)
        for( String alias : casConfig.members ) {
            if( alias == casConfig.writableAlias )
                continue
            final Path memberRecords = casConfig.locationOf(alias).resolve(NEXTFLOW_RECORDS)
            if( Files.isDirectory(memberRecords) )
                chain.add(openRecords(memberRecords))
        }
        this.readers = Collections.unmodifiableList(chain)
        log.debug "Lineage records for store '${casConfig.writableAlias}' at ${records}; ${readers.size()} member(s) readable"
        return this
    }

    private static DefaultLinStore openRecords(Path location) {
        return new DefaultLinStore().open(new LineageConfig([enabled: true, store: [location: location.toString()]]))
    }

    private static Map sessionConfig() {
        final session = Global.session as Session
        return session?.config ?: Collections.emptyMap()
    }

    @Override
    void save(String key, LinSerializable value) {
        try {
            if( value instanceof WorkflowRun ) {
                // The WorkflowRun save is where Nextflow first tells us its run
                // key (LinObserver.executionHash); the observer's join needs it.
                CasSession.current().setNextflowRunKey(key)
                delegate.save(key, value)
                return
            }
            if( value instanceof FileOutput ) {
                delegate.save(key, addressed((FileOutput) value))
                return
            }
            delegate.save(key, value)
        }
        catch( IOException e ) {
            throw new AbortRunException("Unable to save lineage record '${key}': ${e.message}", e)
        }
    }

    /**
     * Returns the record to delegate: unchanged unless its {@code path} is a
     * Publish Coordinate, in which case the coordinate is replaced by the Store
     * URI it resolves to and the fingerprint recomputed. A published
     * coordinate with no recorded address is provenance loss and aborts the run.
     */
    private FileOutput addressed(FileOutput output) {
        final path = output.path
        if( !path || !path.startsWith(SCHEME_PREFIX) )
            return output
        final authority = authorityOf(path)
        // A Store URI already; nothing to rewrite. Told from a coordinate by
        // its authority being a content address (DESIGN.md §7, §10).
        if( authority == null || Cid.isCid(authority) )
            return output

        final joinKey = Coordinates.key(path)
        final StoreRef ref = addressFor(joinKey)
        if( ref == null )
            throw new AbortRunException("Published output '${path}' has no recorded address: neither a publish this run nor a pointer file names it")

        final newPath = ref.toString()
        output.path = newPath
        output.checksum = Checksum.ofNextflow(FileHelper.asPath(newPath))
        return output
    }

    /** The address for a coordinate: the publish recorded this run, else its pointer file. */
    private StoreRef addressFor(String joinKey) {
        final session = CasSession.current()
        final publish = session.publishFor(joinKey)
        if( publish != null )
            return publish.ref
        // The JVM may have lost the in-memory publish (a resumed or re-read
        // run); the pointer file under coords/ is the durable record.
        return session.coordinates.read(coordinateRelPath(joinKey)).orElse(null)
    }

    /** The authority of a `cas://` uri: the store alias or the content address. */
    private static String authorityOf(String casUri) {
        final rest = casUri.substring(SCHEME_PREFIX.length())
        final slash = rest.indexOf('/')
        final authority = slash < 0 ? rest : rest.substring(0, slash)
        return authority ?: null
    }

    /** The coordinate's path within the writable member, i.e. its key minus `cas://<alias>/`. */
    private static String coordinateRelPath(String joinKey) {
        final rest = joinKey.substring(SCHEME_PREFIX.length())
        final slash = rest.indexOf('/')
        return slash < 0 ? '' : rest.substring(slash + 1)
    }

    @Override
    LinSerializable load(String key) {
        for( DefaultLinStore reader : readers ) {
            final LinSerializable found = reader.load(key)
            if( found != null )
                return found
        }
        return null
    }

    @Override
    LinHistoryLog getHistoryLog() {
        // The launching user's own history is the writable member's.
        return delegate.getHistoryLog()
    }

    @Override
    Stream<String> search(Map<String, List<String>> params) {
        Stream<String> all = Stream.<String>empty()
        for( DefaultLinStore reader : readers )
            all = Stream.concat(all, reader.search(params))
        // A block that is present in several members shares one key; one hit is enough.
        return all.distinct()
    }

    @Override
    Stream<String> getSubKeys(String parentKey) {
        Stream<String> all = Stream.<String>empty()
        for( DefaultLinStore reader : readers )
            all = Stream.concat(all, reader.getSubKeys(parentKey))
        return all.distinct()
    }

    @Override
    void close() throws IOException {
        // Nextflow never calls this, but a member store may hold resources.
        for( DefaultLinStore reader : (readers ?: Collections.<DefaultLinStore>emptyList()) )
            reader.close()
    }
}
