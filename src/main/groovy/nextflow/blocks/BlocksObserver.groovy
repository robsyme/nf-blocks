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
import nextflow.processor.TaskPath
import nextflow.file.FileHolder

import java.security.MessageDigest
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes

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
    private Cid workflowRunCid

    BlocksObserver(BlockStore blockStore, Session session) {
        this.blockStore = blockStore
        this.session = session
    }

    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
        
        // Collect workflow metadata
        def metadata = session.workflowMetadata
        def runInfo = [
            // Session info
            sessionId: session.uniqueId,
            runName: session.runName,
            commandLine: session.commandLine,
            scriptName: session.scriptName,
            profile: session.profile,
            
            // Configuration
            config: session.config
        ].findAll { k, v -> v != null } as Map<String, Object>

        // Workflow metadata
        runInfo.workflow = [
            runName: metadata.runName,
            scriptId: metadata.scriptId,
            scriptFile: metadata.scriptFile?.toString(),
            scriptName: metadata.scriptName,
            repository: metadata.repository,
            commitId: metadata.commitId,
            revision: metadata.revision,
            start: metadata.start?.toString(),
            container: processContainerConfig(metadata.container)
        ].findAll { k, v -> v != null }
        
        // Convert to CBOR and store
        def cborMap = CborObject.CborMap.build(convertMapToCbor(runInfo))
        def block = cborMap.toByteArray()
        workflowRunCid = createCid(block)
        blockStore.putBlock(workflowRunCid, block)
        log.trace "Stored workflow run block: CID=${workflowRunCid}"
    }

    /**
     * Process container configuration which might be a single string or a map
     */
    private Object processContainerConfig(def container) {
        if (container instanceof Map) {
            return container.collectEntries { process, image ->
                [(process.toString()): image.toString()]
            }
        }
        return container?.toString()
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
            script: task.source,

            // Container configuration
            container: task.isContainerEnabled() ? task.getContainerFingerprint() : null,

            // Task inputs - handle InParam interface
            inputs: task.inputs.collect { inParam, value ->
                [
                    name: inParam.name,
                    index: inParam.index,
                    mapIndex: inParam.mapIndex,
                    value: processInputValue(value)
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
     * Process an input value, handling special cases like FileHolder, Lists, and Maps
     */
    private Object processInputValue(Object value) {
        if (value == null) return null
        
        switch(value) {
            case List:
                return (value as List).collect { processInputValue(it) }
            case Map:
                return (value as Map).collect { entry -> 
                    [
                        name: entry.key,
                        value: processInputValue(entry.value)
                    ]
                }
            case FileHolder:
                def path = (value as FileHolder).sourceObj as Path
                def attrs = Files.readAttributes(path, BasicFileAttributes)
                return [
                    path: path.toString(),
                    size: attrs.size(),
                    lastModified: attrs.lastModifiedTime()?.toMillis(),
                    isDirectory: attrs.isDirectory()
                ]
            default:
                return value.toString()
        }
    }

    /**
     * Called before a task is submitted to the executor
     */
    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        final task = handler.task
        log.trace "Creating DAG-CBOR block for task: ${task.name} [${task.hash}]"

        // Collect all task inputs and convert to CBOR
        def inputs = collectTaskInputs(task)
        def cborMap = CborObject.CborMap.build(convertMapToCbor(inputs))
        def block = cborMap.toByteArray()
        
        // Create CID and store block
        def cid = createCid(block)
        blockStore.putBlock(cid, block)
        log.trace "Stored task block: CID=${cid} size=${block.length}bytes task=${task.name}"
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
            case Cid:
                return new CborObject.CborMerkleLink(value as Cid)
            case null:
                return new CborObject.CborNull()
            default:
                return new CborObject.CborString(value.toString())
        }
    }

    /**
     * Get the CID of the workflow run block
     */
    Cid getWorkflowRunCid() {
        return workflowRunCid
    }

    @Override
    void onWorkflowPublish(Object value) {
        log.trace "Publishing workflow object: ${value} (${value.getClass()})"

        // Create a block representing this published value
        def publishInfo = [
            type: 'publish',
            timestamp: System.currentTimeMillis(),
            workflowRun: workflowRunCid,  // Just use the Cid directly
            value: processPublishedValue(value)
        ] as Map<String, Object>

        // Convert to CBOR and store - this will handle converting the Cid to a CborMerkleLink
        def cborMap = CborObject.CborMap.build(convertMapToCbor(publishInfo))
        def block = cborMap.toByteArray()
        def cid = createCid(block)
        blockStore.putBlock(cid, block)
        log.trace "Stored publish block: CID=${cid}"
    }

    /**
     * Process a published value, handling special cases like Paths, Lists, and Maps
     */
    private Object processPublishedValue(Object value) {
        if (value == null) return null

        switch(value) {
            case Path:
                return processPublishedPath(value as Path)
            case FileHolder:
                return processPublishedPath((value as FileHolder).sourceObj as Path)
            case List:
                return [
                    type: 'list',
                    items: (value as List).collect { processPublishedValue(it) }
                ]
            case Map:
                return [
                    type: 'map',
                    entries: (value as Map).collect { k, v -> 
                        [key: processPublishedValue(k), value: processPublishedValue(v)]
                    }
                ]
            case Number:
            case Boolean:
            case String:
                return [
                    type: 'value',
                    valueType: value.getClass().simpleName,
                    value: value.toString()
                ]
            default:
                return [
                    type: 'value',
                    valueType: value.getClass().name,
                    value: value.toString()
                ]
        }
    }

    /**
     * Process a published Path, creating a UnixFS block if it exists
     */
    private Object processPublishedPath(Path path) {
        if (!Files.exists(path)) {
            return [
                type: 'path',
                exists: false,
                path: path.toString()
            ]
        }

        // Read file attributes
        def attrs = Files.readAttributes(path, BasicFileAttributes)
        def result = [
            type: 'path',
            exists: true,
            path: path.toString(),
            size: attrs.size(),
            lastModified: attrs.lastModifiedTime()?.toMillis(),
            isDirectory: attrs.isDirectory()
        ] as Map<String, Object>

        // If it's a regular file, create a UnixFS block
        if (attrs.isRegularFile()) {
            def fileBytes = Files.readAllBytes(path)
            def fileCid = createUnixFsCid(fileBytes)
            blockStore.putBlock(fileCid, fileBytes)
            
            // Create a map entry for the CID instead of direct assignment
            result.put('content', [
                type: 'link',
                codec: 'raw',
                cid: fileCid.toString()
            ])
        }

        return result
    }

    /**
     * Create a CID for a UnixFS block using SHA-256 and Raw codec
     */
    private Cid createUnixFsCid(byte[] block) {
        def digest = MessageDigest.getInstance("SHA-256")
        def hash = digest.digest(block)
        def mh = new Multihash(Multihash.Type.sha2_256, hash)
        return Cid.buildCidV1(Cid.Codec.Raw, mh.getType(), hash)
    }

    @Override
    void onFilePublish(Path destination, Path source) {
        // Create new block for each published file
        log.debug "Publishing file: ${source} -> ${destination}"
    }    
} 