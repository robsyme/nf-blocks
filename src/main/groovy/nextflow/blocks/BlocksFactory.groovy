package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

import java.nio.file.Path
import java.nio.file.Paths

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
        String type = config.type as String ?: 'fs'
        String pathStr = config.path as String ?: "${session.workDir}/blocks"

        // Create the appropriate block store
        BlockStore blockStore
        switch (type) {
            case 'fs':
                Path storePath = Paths.get(pathStr)
                blockStore = new FileSystemBlockStore(storePath)
                break
            case 'ipfs':
                blockStore = new IpfsBlockStore(pathStr)
                break
            default:
                throw new IllegalArgumentException("Unknown block store type: ${type}")
        }

        // Create and return the observer
        return [new BlocksObserver(blockStore, session)]
    }
} 