package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory
import nextflow.blocks.fs.BlocksFileSystemProvider
import java.lang.reflect.Field
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Implements the blockstore observer factory
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@Slf4j
@CompileStatic
class BlocksFactory implements TraceObserverFactory {
    @Override
    Collection<TraceObserver> create(Session session) {
        log.info "Creating blocks factory"
        
        // With the new URI-based approach, we don't need to create block stores here
        // Block stores are created dynamically based on the URI scheme
        // For now, we can return an empty observer or skip block store creation
        
        // Create and return the observer without a specific block store
        // The observer can be enhanced later to track publication events
        return [new BlocksObserver(null, session)] as Collection<TraceObserver>
    }


} 