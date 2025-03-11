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

        // Get the BlockStore instance from the session's container
        Map config = session.config.navigate('blocks.store') as Map ?: [:]
        String type = config.type as String ?: 'local'
        String pathStr = config.path as String ?: "${session.workDir}/blocks"
        
        // Get UnixFS options if available
        Map unixfsOptions = config.navigate('unixfs') as Map ?: [:]

        // Create the appropriate block store
        switch (type) {
            case 'ipfs':
                this.blockStore = new IpfsBlockStore(pathStr)
                log.info "Using IPFS block store at: ${pathStr}"
                break
            case 'local':
            case 'fs':
                this.blockStore = new LocalBlockStore(pathStr, unixfsOptions)
                log.info "Using local file system block store at: ${pathStr}"
                if (unixfsOptions.chunkSize) {
                    log.info "UnixFS chunk size: ${unixfsOptions.chunkSize} bytes"
                }
                break
            default:
                throw new IllegalArgumentException("Unknown block store type: ${type}. Supported types: 'ipfs', 'local', 'fs'")
        }
    }

    /**
     * Creates a channel from a single CID
     */
    @Factory
    DataflowWriteChannel fromCid(String cidString) {
        final channel = CH.create()
        
        session.addIgniter((action) -> {
            try {
                Cid cid = Cid.decode(cidString)
                // Since Cid extends Multihash, we can use it directly with blockStore.get()
                MerkleNode block = blockStore.get(cid)
                
                byte[] blockData = block?.data?.orElseThrow { new RuntimeException("Block data is null for CID ${cidString}") }
                def cbor = CborObject.fromByteArray(blockData)
                def decodedValue = decodeCborValue(cbor)
                channel.bind(decodedValue)
                
                // Close the channel after processing all CIDs
                channel.bind(Channel.STOP)
            }
            catch (Exception e) {
                log.error("Error processing CID(s) ${cidString}", e)
                channel.bind(Channel.STOP)
            }
        })
        
        return channel
    }

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
     * Adds data to the block store and returns the CID
     * 
     * @param data The data to add to the block store
     * @param options Optional parameters for adding the block (e.g., codec)
     * @return The CID of the added data
     */
    @Factory
    String addBlock(byte[] data, Map options = [:]) {
        try {
            MerkleNode node = blockStore.add(data, options)
            return node.hash.toString()
        } catch (Exception e) {
            log.error("Error adding block to store", e)
            throw e
        }
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