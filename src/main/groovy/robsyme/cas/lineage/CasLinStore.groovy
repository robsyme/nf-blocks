package robsyme.cas.lineage

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Stream

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortOperationException
import nextflow.lineage.DefaultLinStore
import nextflow.lineage.LinHistoryLog
import nextflow.lineage.LinStore
import nextflow.lineage.config.LineageConfig
import nextflow.lineage.serde.LinSerializable
import robsyme.cas.CasConfig

/**
 * The lineage store behind `lineage.store.location = 'cas://<alias>'`.
 *
 * Nextflow's own records live in `<writable>/nf`, the layout
 * {@link DefaultLinStore} writes, so `lid://` reads keep working
 * (DESIGN.md sections 5 and 10). Rewriting Publish Coordinates to Store URIs
 * and writing lineage blocks arrives with the observer; for now every call is
 * delegated unchanged.
 */
@Slf4j
@CompileStatic
class CasLinStore implements LinStore {

    /** Sub-directory of the writable member holding Nextflow's own lineage records. */
    static final String NEXTFLOW_RECORDS = 'nf'

    private CasConfig casConfig

    private DefaultLinStore delegate

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
        this.delegate = new DefaultLinStore().open(new LineageConfig([enabled: true, store: [location: records.toString()]]))
        log.debug "Lineage records for store '${casConfig.writableAlias}' at ${records}"
        return this
    }

    private static Map sessionConfig() {
        final session = Global.session as Session
        return session?.config ?: Collections.emptyMap()
    }

    @Override
    void save(String key, LinSerializable value) {
        delegate.save(key, value)
    }

    @Override
    LinSerializable load(String key) {
        return delegate.load(key)
    }

    @Override
    LinHistoryLog getHistoryLog() {
        return delegate.getHistoryLog()
    }

    @Override
    Stream<String> search(Map<String, List<String>> params) {
        return delegate.search(params)
    }

    @Override
    Stream<String> getSubKeys(String parentKey) {
        return delegate.getSubKeys(parentKey)
    }

    @Override
    void close() throws IOException {
        delegate?.close()
    }
}
