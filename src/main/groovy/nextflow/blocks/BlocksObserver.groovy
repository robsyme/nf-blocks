package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import io.ipfs.api.cbor.CborObject
import io.ipfs.api.cbor.CborEncoder
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
    private final Session session

    BlocksObserver(BlockStore blockStore, Session session) {
        this.blockStore = blockStore
        this.session = session
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
     * Collect all task inputs that contribute to the task hash
     */
    private Map<String, Object> collectTaskInputs(TaskRun task) {
        def inputs = [
            // Core task identity
            sessionId: session.uniqueId,
            name: task.name,
            source: task.source,

            // Container configuration
            container: task.isContainerEnabled() ? task.getContainerFingerprint() : null,

            // Task inputs
            inputs: task.inputs.collect { input ->
                [
                    name: input.key.name,
                    value: input.value?.toString()
                ]
            },

            // Environment configuration
            conda: task.getCondaEnv(),
            spack: task.getSpackEnv(),
            architecture: task.getConfig().getArchitecture(),
            modules: task.getConfig().getModule() as List,

            // Execution mode
            stubRun: session.stubRun
        ] as Map<String, Object>

        // Remove null values to keep the block clean
        return inputs.findAll { k, v -> v != null } as Map<String, Object>
    }

    /**
     * Called before a task is submitted to the executor
     */
    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        final task = handler.task
        log.debug "Creating DAG-CBOR block for task: ${task.name}"

        // Collect all task inputs and convert to CBOR
        def inputs = collectTaskInputs(task)
        def cborMap = CborObject.CborMap.build(convertMapToCbor(inputs))
        def block = cborMap.toByteArray()
        
        // Create CID and store block
        def cid = createCid(block)
        blockStore.putBlock(cid, block)
        log.debug "Task block created with CID: ${cid}"
    }

    /**
     * Convert a Map to a Map<String, CborObject>
     */
    private Map<String, CborObject> convertMapToCbor(Map<String, Object> map) {
        map.collectEntries { k, v -> [(k): convertToCborObject(v)] }
    }

    /**
     * Convert a Groovy object to a CborObject
     */
    private CborObject convertToCborObject(Object value) {
        switch (value) {
            case String:
                return new CborObject.CborString(value as String)
            case Number:
                return new CborObject.CborLong(value as Long)
            case Boolean:
                return new CborObject.CborBoolean(value as boolean)
            case List:
                def list = (value as List).collect { convertToCborObject(it) }
                return new CborObject.CborList(list)
            case Map:
                return CborObject.CborMap.build(convertMapToCbor(value as Map))
            case byte[]:
                return new CborObject.CborByteArray(value as byte[])
            case null:
                return new CborObject.CborNull()
            default:
                return new CborObject.CborString(value.toString())
        }
    }
} 