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
        log.info "Initializing BlocksExtension"
        this.session = session

        // Get the BlockStore instance from the session's container
        Map config = session.config.navigate('blocks.store') as Map ?: [:]
        String type = config.type as String ?: 'fs'
        String pathStr = config.path as String ?: "${session.workDir}/blocks"

        // Create the appropriate block store
        switch (type) {
            case 'ipfs':
                this.blockStore = new IpfsBlockStore(pathStr)
                break
            default:
                throw new IllegalArgumentException("Unknown block store type: ${type}")
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
                log.info "CID: ${cid}"
                MerkleNode block = blockStore.get(cid)
                log.info "Block: ${block}"
                byte[] blockData = block?.data?.orElseThrow { new RuntimeException("Block data is null for CID ${cidString}") }
                def cbor = CborObject.fromByteArray(blockData)
                log.info "CBOR: ${cbor}"
                def decodedValue = decodeCborValue(cbor)
                log.info "Decoded value: ${decodedValue}"
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
        log.info "Decoding CBOR value: ${cbor}"
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
} 