package robsyme.cas.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2
import robsyme.cas.CasConfig

/**
 * Registers the one {@link CasObserver} of a run (DESIGN.md §11), after giving
 * an unset {@code outputDir} its default (ticket 02). Listed in
 * {@code extensionPoints}; {@code @Extension} alone registers nothing.
 */
@Slf4j
@CompileStatic
class CasObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        defaultOutputDir(session)
        return Collections.<TraceObserverV2> singletonList(new CasObserver())
    }

    /**
     * An unset {@code outputDir} means the writable member's alias. This runs
     * inside {@code Session.init}'s {@code createObserversV2()}
     * (Session.groovy:469, 514), before {@code new WorkflowMetadata} copies
     * {@code session.outputDir} (Session.groovy:472, WorkflowMetadata.groovy:271),
     * so {@code workflow.outputDir}, the lineage WorkflowRun record and Platform
     * payloads all see one value.
     *
     * It changes nothing unless lineage is enabled, {@code lineage.store.location}
     * is a bare {@code cas://<alias>} (the CasLinStoreFactory.canOpen test plus
     * CasConfig's alias rule) and the config sets no {@code outputDir};
     * {@code -output-dir} counts as set, since ConfigBuilder.groovy:570-571 copies
     * it into the config. {@code session.config} is left as written, so the
     * RunManifest records the config the user wrote. It never throws: a failure
     * leaves {@code outputDir} alone and {@code CasObserver.onFlowCreate} judges
     * the run as before.
     */
    static void defaultOutputDir(Session session) {
        final Map config = session?.config
        if( config == null || config.get('outputDir') )
            return
        final Object lineage = config.get('lineage')
        if( !(lineage instanceof Map) || !isTrue(((Map) lineage).get('enabled')) )
            return
        final Object store = ((Map) lineage).get('store')
        final String location = store instanceof Map ? ((Map) store).get('location') as String : null
        if( !location?.startsWith('cas://') )
            return
        final String alias = CasConfig.aliasOf(location)
        if( !alias )
            return
        final String target = "cas://${alias}".toString()
        try {
            // The same conversion Session.groovy:418 applies to an explicit value.
            session.outputDir = FileHelper.toCanonicalPath(target)
        }
        catch( Exception e ) {
            log.warn("outputDir not set, and ${target} could not be resolved as its default: ${e.message}", e)
            return
        }
        log.info("outputDir not set; publishing to ${target}")
    }

    private static boolean isTrue(Object value) {
        return value instanceof Boolean ? (Boolean) value : value?.toString() == 'true'
    }
}
