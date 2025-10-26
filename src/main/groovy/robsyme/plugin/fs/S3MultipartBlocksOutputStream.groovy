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

package robsyme.plugin.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.plugin.ipld.CID
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

/**
 * OutputStream that uploads to S3 using multipart upload
 * while calculating BLAKE3 hash in parallel.
 *
 * Flow:
 * 1. CreateMultipartUpload to .staging/{uuid}
 * 2. Buffer 5MB chunks and upload as parts
 * 3. Calculate hash as data streams through
 * 4. On close: CompleteMultipartUpload (atomic commit to staging)
 * 5. CopyObject to blobs/{hash} (server-side, fast)
 * 6. DeleteObject from staging
 */
@Slf4j
@CompileStatic
class S3MultipartBlocksOutputStream extends OutputStream {

    private static final int PART_SIZE = 5 * 1024 * 1024 // 5 MiB minimum
    private static final int BUFFER_SIZE = 5 * 1024 * 1024 // 5 MiB

    private final S3Client s3Client
    private final String bucket
    private final String stagingKey
    private final String finalKeyPrefix // e.g., "blobs/"

    private String uploadId
    private List<CompletedPart> completedParts = []
    private int partNumber = 1

    private ByteArrayOutputStream buffer
    private Process b3sumProcess
    private OutputStream hashInput
    private long totalBytesWritten = 0

    private boolean closed = false
    private boolean aborted = false

    S3MultipartBlocksOutputStream(
        S3Client s3Client,
        String bucket,
        String stagingKey,
        String finalKeyPrefix
    ) {
        this.s3Client = s3Client
        this.bucket = bucket
        this.stagingKey = stagingKey
        this.finalKeyPrefix = finalKeyPrefix
        this.buffer = new ByteArrayOutputStream(BUFFER_SIZE)

        // Start b3sum process for streaming BLAKE3 hash calculation
        this.b3sumProcess = new ProcessBuilder('b3sum', '--no-names')
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start()

        this.hashInput = b3sumProcess.outputStream

        // Start multipart upload
        initializeMultipartUpload()
    }

    private void initializeMultipartUpload() {
        log.debug "Starting multipart upload to staging: s3://${bucket}/${stagingKey}"

        def request = CreateMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(stagingKey)
            .build()

        def response = s3Client.createMultipartUpload(request)
        this.uploadId = response.uploadId()

        log.debug "Created multipart upload with ID: ${uploadId}"
    }

    @Override
    void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }

        buffer.write(b)
        hashInput.write(b)
        totalBytesWritten++

        // If buffer is full, upload as a part
        if (buffer.size() >= PART_SIZE) {
            uploadPart()
        }
    }

    @Override
    void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }

        buffer.write(b, off, len)
        hashInput.write(b, off, len)
        totalBytesWritten += len

        // Upload parts as buffer fills
        while (buffer.size() >= PART_SIZE) {
            uploadPart()
        }
    }

    private void uploadPart() throws IOException {
        if (buffer.size() == 0) {
            return
        }

        byte[] data = buffer.toByteArray()
        buffer.reset()

        log.debug "Uploading part ${partNumber} (${data.length} bytes)"

        try {
            def request = UploadPartRequest.builder()
                .bucket(bucket)
                .key(stagingKey)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) data.length)
                .build()

            def response = s3Client.uploadPart(
                request,
                RequestBody.fromBytes(data)
            )

            completedParts.add(
                CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(response.eTag())
                    .build()
            )

            partNumber++
        } catch (Exception e) {
            log.error "Failed to upload part ${partNumber}: ${e.message}", e
            abortUpload()
            throw new IOException("Failed to upload part ${partNumber}", e)
        }
    }

    @Override
    void close() throws IOException {
        if (closed) {
            return
        }

        try {
            // Upload any remaining data as final part
            if (buffer.size() > 0) {
                uploadPart()
            }

            // Close hash input to signal end of data
            hashInput.close()

            // Read BLAKE3 hash from b3sum output
            def hashOutput = b3sumProcess.inputStream.text.trim()
            b3sumProcess.waitFor()

            if (b3sumProcess.exitValue() != 0) {
                throw new IOException("b3sum process failed with exit code ${b3sumProcess.exitValue()}")
            }

            log.debug "BLAKE3 hash: ${hashOutput}"

            // Convert hash to CID
            def cid = CID.fromHash(hashOutput)

            log.info "Upload complete. Total bytes: ${totalBytesWritten}, CID: ${cid}"

            // Complete multipart upload (atomic commit to staging)
            completeMultipartUpload()

            // Move from staging to final CID-addressed location
            moveToFinalLocation(cid)

        } catch (Exception e) {
            log.error "Failed to close stream: ${e.message}", e
            abortUpload()
            throw new IOException("Failed to finalize upload", e)
        } finally {
            closed = true
        }
    }

    private void completeMultipartUpload() {
        log.debug "Completing multipart upload to staging: ${stagingKey}"

        def request = CompleteMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(stagingKey)
            .uploadId(uploadId)
            .multipartUpload(
                CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build()
            )
            .build()

        def response = s3Client.completeMultipartUpload(request)
        log.debug "Multipart upload completed: ${response.eTag()}"
    }

    private void moveToFinalLocation(String cid) {
        def finalKey = "${finalKeyPrefix}${cid}"
        log.debug "Moving from staging to final CID location: s3://${bucket}/${finalKey}"

        // Server-side copy (atomic, no data transfer)
        def copyRequest = CopyObjectRequest.builder()
            .sourceBucket(bucket)
            .sourceKey(stagingKey)
            .destinationBucket(bucket)
            .destinationKey(finalKey)
            .build()

        s3Client.copyObject(copyRequest)
        log.debug "Copied to final CID location: ${finalKey}"

        // Delete staging object
        def deleteRequest = DeleteObjectRequest.builder()
            .bucket(bucket)
            .key(stagingKey)
            .build()

        s3Client.deleteObject(deleteRequest)
        log.debug "Deleted staging object: ${stagingKey}"
    }

    private void abortUpload() {
        if (aborted || uploadId == null) {
            return
        }

        try {
            log.warn "Aborting multipart upload: ${uploadId}"

            def request = AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(stagingKey)
                .uploadId(uploadId)
                .build()

            s3Client.abortMultipartUpload(request)
            aborted = true

            log.debug "Multipart upload aborted successfully"
        } catch (Exception e) {
            log.error "Failed to abort multipart upload: ${e.message}", e
        }
    }

    @Override
    void flush() throws IOException {
        // Flush both buffer and hash input
        hashInput.flush()
        // For multipart upload, buffer flush is a no-op
        // Parts are uploaded when buffer fills
    }
}
