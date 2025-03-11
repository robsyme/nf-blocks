package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.cbor.CborObject
import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.Path
import nextflow.file.FileHolder
import nextflow.processor.TaskHandler
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

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
    private final List<Multihash> publishedBlocks = []
    private final List<Multihash> taskBlocks = []
    BlocksObserver(BlockStore blockStore, Session session) {
        this.blockStore = blockStore
        this.session = session
    }

    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
        
        def metadata = session.workflowMetadata
        def runInfo = [
            sessionId: session.uniqueId,
            runName: session.runName,
            commandLine: session.commandLine,
            scriptName: session.scriptName,
            profile: session.profile,
            config: session.config,
            workflow: [
                runName: metadata.runName,
                scriptId: metadata.scriptId,
                scriptFile: metadata.scriptFile?.toString(),
                scriptName: metadata.scriptName,
                repository: metadata.repository,
                commitId: metadata.commitId,
                revision: metadata.revision,
                start: metadata.start?.toString(),
                container: metadata.container instanceof Map ? 
                    (metadata.container as Map).collectEntries { k, v -> [(k.toString()): v.toString()] } :
                    metadata.container?.toString()
            ].findAll { k, v -> v }
        ].findAll { k, v -> v }

        def node = storeCborBlock(runInfo)
        workflowRunCid = Cid.build(1, Cid.Codec.DagCbor, node.hash)
        log.trace "Stored workflow run block: CID=${node.hash}"
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        final task = handler.task
        log.trace "Creating DAG-CBOR block for task: ${task.name} [${task.hash}]"

        def inputs = [
            sessionId: session.uniqueId,
            name: task.name,
            script: task.source,
            container: task.isContainerEnabled() ? task.getContainerFingerprint() : null,
            inputs: task.inputs.collect { inParam, value ->
                [
                    name: inParam.name,
                    index: inParam.index,
                    mapIndex: inParam.mapIndex,
                    value: processValue(value)
                ]
            },
            conda: task.getCondaEnv(),
            spack: task.getSpackEnv(),
            architecture: task.getConfig().getArchitecture(),
            modules: task.getConfig().getModule() as List,
            stubRun: session.stubRun
        ].findAll { k, v -> v }

        MerkleNode node = storeCborBlock(inputs)
        taskBlocks.add(node.hash)
        log.trace "Stored task block: CID=${node.hash} task=${task.name}"
    }

    @Override
    void onWorkflowPublish(Object value) {
        log.trace "Publishing workflow object: ${value} (${value.getClass()})"
        MerkleNode node = storeCborBlock(value)
        log.trace "Stored publish block: CID=${node.hash}"
        
        publishedBlocks.add(node.hash)
    }
    
    @Override
    void onFlowComplete() {
        def root = [
            outputs: publishedBlocks,
            workflowRun: workflowRunCid,
            tasks: taskBlocks
        ]
        MerkleNode rootNode = storeCborBlock(root)
        log.info "Created root block with CID: ${rootNode.hash}"
    }

    private MerkleNode storeCborBlock(Object value) {
        def block = convertToCbor(value)
        blockStore.add(block.toByteArray())
    }

    private Object processValue(Object value) {
        switch(value) {
            case List: 
                return ((List)value).collect { processValue(it) }
            case Map: 
                return ((Map)value).collectEntries { k, v -> [k, processValue(v)] } as Map<String, Object>
            case FileHolder:
                def path = (value as FileHolder).sourceObj as Path
                def attrs = Files.readAttributes(path, BasicFileAttributes)
                return [
                    path: path.toString(),
                    size: attrs.size(),
                    lastModified: attrs.lastModifiedTime()?.toMillis(),
                    isDirectory: attrs.isDirectory()
                ] as Map<String, Object>
            default: 
                return value.toString()
        }
    }

    private CborObject convertToCbor(Object value) {
        switch(value) {
            case String: 
                return new CborObject.CborString((String)value)
            case Number: 
                return new CborObject.CborLong(((Number)value).longValue())
            case Boolean: 
                return new CborObject.CborBoolean((Boolean)value)
            case List: 
                List<CborObject> list = ((List)value).collect { convertToCbor(it) }
                return new CborObject.CborList(list)
            case Map: 
                Map<String, CborObject> map = ((Map)value).collectEntries { k, v -> 
                    [(k.toString()): convertToCbor(v)] 
                } as Map<String, CborObject>
                return CborObject.CborMap.build(map)
            case byte[]: 
                return new CborObject.CborByteArray((byte[])value)
            case Cid: 
                Cid cid = (Cid)value
                return new CborObject.CborMerkleLink(cid)
            case null: 
                return new CborObject.CborNull()
            case Path:
            case FileHolder:
                def path = value instanceof Path ? value : (value as FileHolder).sourceObj
                return convertToCbor(processPath(path as Path))
            default: 
                return new CborObject.CborString(value.toString())
        }
    }

    private Map<String, Object> processPath(Path path) {
        log.trace "Processing path: ${path}"
        if (!Files.exists(path)) {
            return [type: 'path', exists: false, path: path.toString()] as Map<String, Object>
        }

        def fileName = path.fileName.toString()
        def node = blockStore.addPath(path)
            
        return [(fileName): node.hash] as Map<String, Object>
    }
    Cid getWorkflowRunCid() { workflowRunCid }
} 