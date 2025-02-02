package nextflow.blockstore

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver

/**
 * Implements the blockstore observer
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@CompileStatic
class BlockstoreObserver implements TraceObserver {
    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
    }
} 