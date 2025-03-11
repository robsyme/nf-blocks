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
        // Get the block store configuration from nextflow.config
        Map config = session.config.navigate('blocks.store') as Map ?: [:]
        String type = config.type as String ?: 'local'
        String pathStr = config.path as String ?: "${session.workDir}/blocks"
        
        // Get UnixFS options if available
        Map unixfsOptions = config.navigate('unixfs') as Map ?: [:]

        // Create the appropriate block store
        BlockStore blockStore
        switch (type) {
            case 'ipfs':
                blockStore = new IpfsBlockStore(pathStr)
                log.info "Using IPFS block store at: ${pathStr}"
                break
            case 'local':
            case 'fs':
                blockStore = new LocalBlockStore(pathStr, unixfsOptions)
                log.info "Using local file system block store at: ${pathStr}"
                if (unixfsOptions.chunkSize) {
                    log.info "UnixFS chunk size: ${unixfsOptions.chunkSize} bytes"
                }
                break
            default:
                throw new IllegalArgumentException("Unknown block store type: ${type}. Supported types: 'ipfs', 'local', 'fs'")
        }

        // Create and return the observer
        return [new BlocksObserver(blockStore, session)] as Collection<TraceObserver>
    }
} 