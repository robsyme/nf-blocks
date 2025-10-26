/*
 * Copyright 2025, Rob Syme (rob.syme@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import robsyme.plugin.config.BlocksConfig
import robsyme.plugin.hash.CLIHashComputer
import robsyme.plugin.hash.HashComputer
import robsyme.plugin.ipld.IPLDSerializer
import robsyme.plugin.ipld.Link
import robsyme.plugin.ipld.OutputMetadata
import robsyme.plugin.ipld.RunManifest
import robsyme.plugin.metadata.MetadataExtractor
import robsyme.plugin.ref.RefManager
import robsyme.plugin.storage.BlobStore
import robsyme.plugin.storage.RefStore
import robsyme.plugin.storage.StorageBackendFactory
import robsyme.plugin.uploader.ContentAddressedUploader

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Trace observer that intercepts workflow execution events
 * and publishes outputs as content-addressed blobs with IPLD manifests.
 */
@Slf4j
@CompileStatic
class BlocksObserver implements TraceObserver {

    private Session session
    private BlocksConfig config
    private boolean enabled

    // Storage backends
    private BlobStore blobStore
    private RefStore refStore
    private RefManager refManager

    // Upload pipeline
    private ContentAddressedUploader uploader
    private HashComputer hashComputer
    private IPLDSerializer serializer

    // Track outputs during workflow execution
    // Map of output path -> (metadata CID, metadata object)
    private Map<String, Tuple2<String, OutputMetadata>> outputs = new ConcurrentHashMap<>()

    // Run metadata
    private String runId
    private Instant startTime

    BlocksObserver(Session session) {
        this.session = session
    }

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.startTime = Instant.now()

        // Load configuration
        def configMap = session.config?.blocks as Map
        config = BlocksConfig.parse(configMap)

        log.debug "Blocks configuration: enabled=${config.enabled}, storage=${config.storage?.uri}"

        if (!config.enabled) {
            log.info "Blocks plugin is disabled"
            this.enabled = false
            return
        }

        try {
            config.validate()
            initializeBackends()
            this.enabled = true
            this.runId = MetadataExtractor.generateRunId(session)
            log.info "Blocks plugin initialized: runId=${runId}, storage=${config.storage.uri}"
        } catch (Exception e) {
            log.error "Failed to initialize Blocks plugin: ${e.message}", e
            this.enabled = false
        }
    }

    @Override
    void onFlowComplete() {
        if (!enabled) {
            return
        }

        log.info "Workflow complete. Building final manifest for run: ${runId}"

        try {
            // Build and upload run manifest
            def manifestCid = buildAndUploadManifest()

            // Update refs
            updateRefs(manifestCid)

            log.info "Successfully published run manifest: ${manifestCid}"
            log.info "Outputs published: ${outputs.size()} files"

            // Close resources
            cleanup()
        } catch (Exception e) {
            log.error "Failed to finalize blocks publication: ${e.message}", e
        }
    }

    /**
     * Process a single output file
     *
     * Note: For MVP, output file processing will be triggered manually in integration tests
     * or through file system watching. The TraceObserver hooks don't provide easy access
     * to output files on a per-task basis in the current Nextflow version.
     */
    private void processOutputFile(Path file, TaskRun task) {
        if (!Files.exists(file) || !Files.isRegularFile(file)) {
            log.debug "Skipping non-regular file: ${file}"
            return
        }

        log.debug "Uploading file: ${file}"

        // Extract metadata
        def sampleMeta = MetadataExtractor.extractSampleMetadata(task)
        def processMeta = MetadataExtractor.extractProcessMetadata(task)

        // Determine file type (MIME type)
        def fileName = file.fileName.toString()
        def fileType = guessFileType(fileName)

        // Upload file
        def result = uploader.upload(file, fileName, fileType, sampleMeta, processMeta)

        // Set producedByRun link (will be set to actual CID later)
        // For now, just store the metadata
        def outputMeta = result.metadata

        // Serialize metadata and upload
        def (metadataCid, metadataBytes) = serializer.serializeAndComputeCID(outputMeta)
        blobStore.writeBlob(metadataCid, new ByteArrayInputStream(metadataBytes))

        log.debug "Uploaded file ${fileName}: data=${result.dataCid}, metadata=${metadataCid}"

        // Store for manifest building
        // Use a simple path (could be enhanced to preserve directory structure)
        def outputPath = fileName
        outputs[outputPath] = new Tuple2<>(metadataCid, outputMeta)
    }

    /**
     * Build and upload the run manifest
     */
    private String buildAndUploadManifest() {
        def manifest = new RunManifest()

        // Basic info
        manifest.runId = runId
        manifest.timestamp = startTime.toString()

        // Workflow info
        def workflowMeta = MetadataExtractor.extractWorkflowMetadata(session)
        manifest.workflow = new RunManifest.WorkflowInfo(
            name: workflowMeta.name as String,
            version: workflowMeta.version as String,
            repository: workflowMeta.repository as String,
            revision: workflowMeta.revision as String
        )

        // Execution info
        def execMeta = MetadataExtractor.extractExecutionMetadata(session)
        manifest.execution = new RunManifest.ExecutionInfo(
            startedAt: execMeta.started_at as String,
            completedAt: execMeta.completed_at as String,
            durationSeconds: execMeta.duration_seconds as Long,
            status: execMeta.status as String,
            exitCode: execMeta.exit_code as Integer,
            executor: execMeta.executor as String,
            nextflowVersion: execMeta.nextflow_version as String
        )

        // Project and user
        manifest.project = MetadataExtractor.extractProjectId(session)
        manifest.user = session.workflowMetadata?.userName ?: System.getProperty('user.name')

        // Outputs (path -> metadata CID links)
        manifest.outputs = outputs.collectEntries { path, tuple ->
            [(path): Link.of(tuple.v1)]
        } as Map<String, Link>

        // Parent link (get from workflow ref)
        def workflowName = manifest.workflow.name
        manifest.parent = refManager.updateWorkflowRef(workflowName, 'placeholder', 'latest')

        // Serialize and upload manifest
        def (manifestCid, manifestBytes) = serializer.serializeAndComputeCID(manifest)
        blobStore.writeBlob(manifestCid, new ByteArrayInputStream(manifestBytes))

        log.info "Run manifest uploaded: ${manifestCid}"

        return manifestCid
    }

    /**
     * Update refs to point to the new manifest
     */
    private void updateRefs(String manifestCid) {
        def workflowName = session.workflowMetadata?.scriptName ?: 'default'
        def projectId = MetadataExtractor.extractProjectId(session)

        // Update workflow ref
        refManager.updateWorkflowRef(workflowName, manifestCid, 'latest')
        log.debug "Updated ref: workflows/${workflowName}/latest -> ${manifestCid}"

        // Update project ref
        refManager.updateProjectRef(projectId, manifestCid)
        log.debug "Updated ref: projects/${projectId}/latest -> ${manifestCid}"

        // Create permanent run ref
        refManager.createRunRef(runId, manifestCid)
        log.debug "Created ref: runs/${runId} -> ${manifestCid}"
    }

    /**
     * Initialize storage backends and upload pipeline
     */
    private void initializeBackends() {
        // Create storage backends
        blobStore = StorageBackendFactory.createBlobStore(config.storage.uri)
        refStore = StorageBackendFactory.createRefStore(config.refs.uri)

        // Create managers
        refManager = new RefManager(refStore)

        // Create upload pipeline
        hashComputer = new CLIHashComputer()
        uploader = new ContentAddressedUploader(blobStore, hashComputer)
        serializer = new IPLDSerializer(hashComputer)

        // Check if hash computer is available
        if (!uploader.isAvailable()) {
            log.warn "Hash computer not available (b3sum/bao not found). Some features may not work."
        }
    }

    /**
     * Guess file MIME type from extension
     */
    private static String guessFileType(String fileName) {
        def lowerName = fileName.toLowerCase()

        if (lowerName.endsWith('.bam')) return 'application/x-bam'
        if (lowerName.endsWith('.sam')) return 'application/x-sam'
        if (lowerName.endsWith('.fastq.gz') || lowerName.endsWith('.fq.gz')) return 'application/gzip'
        if (lowerName.endsWith('.fastq') || lowerName.endsWith('.fq')) return 'text/plain'
        if (lowerName.endsWith('.vcf.gz')) return 'application/gzip'
        if (lowerName.endsWith('.vcf')) return 'text/plain'
        if (lowerName.endsWith('.bed')) return 'text/plain'
        if (lowerName.endsWith('.csv')) return 'text/csv'
        if (lowerName.endsWith('.tsv')) return 'text/tab-separated-values'
        if (lowerName.endsWith('.json')) return 'application/json'
        if (lowerName.endsWith('.html')) return 'text/html'
        if (lowerName.endsWith('.pdf')) return 'application/pdf'

        return 'application/octet-stream'
    }

    /**
     * Clean up resources
     */
    private void cleanup() {
        try {
            blobStore?.close()
            refStore?.close()
        } catch (Exception e) {
            log.debug "Error closing resources: ${e.message}"
        }
    }

    /**
     * Get the loaded configuration (for testing)
     */
    BlocksConfig getConfig() {
        return config
    }
}
