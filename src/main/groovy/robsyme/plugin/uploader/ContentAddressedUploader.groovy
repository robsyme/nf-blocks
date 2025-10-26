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

package robsyme.plugin.uploader

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.plugin.hash.HashComputer
import robsyme.plugin.hash.HashResult
import robsyme.plugin.ipld.CID
import robsyme.plugin.ipld.Link
import robsyme.plugin.ipld.OutputMetadata
import robsyme.plugin.storage.BlobStore

import java.nio.file.Path
import java.time.Instant

/**
 * Uploads files to content-addressed storage in a single pass.
 *
 * Process:
 * 1. Compute BLAKE3 hash and Bao outboard
 * 2. Generate CID from hash
 * 3. Upload file blob to storage
 * 4. Upload Bao outboard to storage
 * 5. Create and serialize OutputMetadata
 * 6. Upload metadata object to storage
 * 7. Return UploadResult with all CIDs
 *
 * This is designed to be efficient: the file is read once for hashing,
 * then uploaded to storage.
 */
@Slf4j
@CompileStatic
class ContentAddressedUploader {

    private final BlobStore blobStore
    private final HashComputer hashComputer

    ContentAddressedUploader(BlobStore blobStore, HashComputer hashComputer) {
        this.blobStore = blobStore
        this.hashComputer = hashComputer
    }

    /**
     * Upload a file and its metadata to content-addressed storage
     *
     * @param file The file to upload
     * @param fileName Original file name (for metadata)
     * @param fileType MIME type or file type (optional)
     * @param sampleMetadata Sample metadata from Nextflow meta Map (optional)
     * @param processMetadata Process metadata (optional)
     * @return UploadResult with CIDs and metadata
     */
    UploadResult upload(
        Path file,
        String fileName,
        String fileType = null,
        Map<String, Object> sampleMetadata = null,
        Map<String, Object> processMetadata = null
    ) {
        log.debug "Uploading file: $file as $fileName"

        // Step 1: Compute hash and outboard
        log.debug "Computing BLAKE3 hash and Bao outboard..."
        HashResult hashResult
        try {
            hashResult = hashComputer.compute(file)
        } catch (Exception e) {
            log.error "Failed to compute hash for $fileName: ${e.message}", e
            throw new RuntimeException("Failed to compute hash for $fileName", e)
        }

        // Step 2: Generate CID for the data blob
        def dataCid = CID.fromHash(hashResult.hash)
        log.debug "Data CID: $dataCid"

        // Step 3: Upload blob to storage
        log.debug "Uploading blob to storage..."
        def blobUploaded = blobStore.writeBlob(dataCid, file)
        if (!blobUploaded) {
            throw new RuntimeException("Failed to upload blob for $fileName")
        }

        // Step 4: Upload outboard to storage
        log.debug "Uploading outboard to storage..."
        def outboardUploaded = blobStore.writeOutboard(dataCid, hashResult.outboard)
        if (!outboardUploaded) {
            log.warn "Failed to upload outboard for $fileName (non-fatal)"
        }

        // Step 5: Create OutputMetadata
        def metadata = new OutputMetadata()
        metadata.fileName = fileName
        metadata.fileType = fileType
        metadata.sampleMetadata = sampleMetadata
        metadata.processMetadata = processMetadata
        metadata.dataBlob = Link.of(dataCid)
        metadata.dataSizeBytes = hashResult.size
        metadata.producedAt = Instant.now().toString()

        // Step 6: Return result
        // Note: metadata CID will be computed and uploaded by the caller
        // after setting the producedByRun link
        return new UploadResult(
            dataCid: dataCid,
            dataSize: hashResult.size,
            metadata: metadata
        )
    }

    /**
     * Check if the uploader is available (hash computer works)
     */
    boolean isAvailable() {
        return hashComputer.isAvailable()
    }

    /**
     * Result of uploading a file
     */
    @CompileStatic
    static class UploadResult {
        /**
         * CID of the data blob
         */
        String dataCid

        /**
         * Size of the data blob in bytes
         */
        long dataSize

        /**
         * Output metadata (not yet uploaded, needs producedByRun link)
         */
        OutputMetadata metadata
    }
}
