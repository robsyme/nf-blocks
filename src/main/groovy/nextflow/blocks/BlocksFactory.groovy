package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

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
        final observer = createBlocksObserver(session.config)
        return observer ? [observer] : []
    }

    protected TraceObserver createBlocksObserver(Map config) {
        // Check if blocks are enabled (default to true)
        final enabled = config.navigate('blocks.enabled', true) as Boolean
        if (!enabled) {
            return null
        }

        // Get block store configuration
        final storeConfig = config.navigate('blocks.store', [:]) as Map
        final storeType = storeConfig.type as String ?: 'fs'
        
        // For file system store, get path (default to .nextflow/blocks)
        if (storeType == 'fs') {
            final path = storeConfig.path as String ?: '.nextflow/blocks'
            log.debug "Configuring file system block store at: ${path}"
            
            // Create FileSystemBlockStore instance
            def blockStore = new FileSystemBlockStore(path)
            return new BlocksObserver(blockStore)
        }
        
        // For now, only support file system store
        if (storeType != 'fs') {
            log.warn "Unsupported block store type: ${storeType}"
            return null
        }

        return null // Should not reach here
    }
} 