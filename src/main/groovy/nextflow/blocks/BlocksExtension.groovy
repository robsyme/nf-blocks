package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import io.ipfs.api.cbor.CborObject
import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import nextflow.Channel
import nextflow.extension.CH
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session
import nextflow.blocks.fs.BlocksFileSystemProvider

import java.lang.reflect.Field
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Implements the blockstore extension
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@Slf4j
@CompileStatic
class BlocksExtension extends PluginExtensionPoint {
    private Session session
    private BlockStore blockStore

    @Override
    protected void init(Session session) {
        this.session = session
        log.info "BlocksExtension initialized - using URI-based block stores"
    }


    // TODO: Implement fromCid method for new URI-based approach
    // /**
    //  * Creates a channel from a single CID
    //  */
    // @Factory
    // DataflowWriteChannel fromCid(String cidString) {
    //     // Implementation needed for URI-based approach
    // }

    /**
     * Helper method to decode CBOR values into native types
     */
    private Object decodeCborValue(CborObject cbor) {
        log.trace "Decoding CBOR value: ${cbor}"
        if (cbor instanceof CborObject.CborString) {
            return (cbor as CborObject.CborString).value
        }
        if (cbor instanceof CborObject.CborLong) {
            return (cbor as CborObject.CborLong).value
        }
        if (cbor instanceof CborObject.CborByteArray) {
            return (cbor as CborObject.CborByteArray).value
        }
        if (cbor instanceof CborObject.CborList) {
            return (cbor as CborObject.CborList).value.collect { decodeCborValue(it) }
        }
        if (cbor instanceof CborObject.CborMap) {
            def map = (cbor as CborObject.CborMap).values
            return map.collectEntries { k, v -> 
                [(decodeCborValue(k)): decodeCborValue(v)]
            }
        }
        if (cbor instanceof CborObject.CborBoolean) {
            return (cbor as CborObject.CborBoolean).value
        }
        if (cbor == null || cbor instanceof CborObject.CborNull) {
            return null
        }
        return cbor.toString()
    }
        
    /**
     * Adds a file to the block store and returns the CID
     * 
     * @param path The path to the file to add
     * @return The CID of the added file
     */
    @Factory
    String addFile(String path) {
        try {
            MerkleNode node = blockStore.addPath(new java.io.File(path).toPath())
            return node.hash.toString()
        } catch (Exception e) {
            log.error("Error adding file to store: ${path}", e)
            throw e
        }
    }

    /**
     * Updates the configuration options for the block store
     * 
     * @param options Map of options to update
     */
    @Factory
    void updateConfig(Map options) {
        if (blockStore instanceof LocalBlockStore) {
            (blockStore as LocalBlockStore).updateOptions(options)
        } else {
            log.warn "Configuration updates are only supported for local block stores"
        }
    }
} 