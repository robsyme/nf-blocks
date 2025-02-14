package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import nextflow.cbor.CborConverter
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import java.security.MessageDigest

/**
 * Implements the blockstore observer
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@Slf4j
@CompileStatic
class BlocksObserver implements TraceObserver {
    private final BlockStore blockStore

    BlocksObserver(BlockStore blockStore) {
        this.blockStore = blockStore
    }

    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
    }

    /**
     * Create a CID for a block using SHA-256 and DAG-CBOR codec
     */
    private Cid createCid(byte[] block) {
        def digest = MessageDigest.getInstance("SHA-256")
        def hash = digest.digest(block)
        def mh = new Multihash(Multihash.Type.sha2_256, hash)
        return Cid.buildCidV1(Cid.Codec.DagCbor, mh.getType(), hash)
    }

    /**
     * Called before a task is submitted to the executor
     */
    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        final task = handler.task
        log.debug "Creating DAG-CBOR block for task: ${task.name}"

        // Create a map of task inputs
        def inputs = [
            name: task.name,
            script: task.script,
            container: task.container,
        ]

        // Convert to CBOR
        def block = CborConverter.toCbor(inputs).toByteArray()
        
        // Create CID and store block
        def cid = createCid(block)
        blockStore.putBlock(cid, block)
        log.debug "Task block created with CID: ${cid}"
    }
} 