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

package robsyme.plugin.storage.impl

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.plugin.storage.BlobStore
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.file.Files
import java.nio.file.Path

/**
 * S3-backed blob storage implementation.
 *
 * Stores blobs in S3 with the following structure:
 * - s3://bucket/prefix/blobs/{hash}
 * - s3://bucket/prefix/blobs/{hash}.obao
 */
@Slf4j
@CompileStatic
class S3BlobStore implements BlobStore {

    private final S3Client s3Client
    private final String bucket
    private final String prefix

    /**
     * Create an S3 blob store
     *
     * @param s3Client The S3 client to use
     * @param bucket The S3 bucket name
     * @param prefix Optional prefix for all blobs (e.g., "project-a")
     */
    S3BlobStore(S3Client s3Client, String bucket, String prefix = '') {
        this.s3Client = s3Client
        this.bucket = bucket
        this.prefix = prefix ? prefix.replaceAll('/$', '') : ''
    }

    @Override
    boolean writeBlob(String hash, InputStream data) {
        def key = getBlobKey(hash)
        log.debug "Writing blob to S3: s3://$bucket/$key"

        try {
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(),
                RequestBody.fromInputStream(data, data.available())
            )
            return true
        } catch (Exception e) {
            log.error "Failed to write blob $hash to S3: ${e.message}", e
            return false
        }
    }

    @Override
    boolean writeBlob(String hash, Path file) {
        def key = getBlobKey(hash)
        log.debug "Writing blob from file to S3: s3://$bucket/$key"

        try {
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(),
                RequestBody.fromFile(file)
            )
            return true
        } catch (Exception e) {
            log.error "Failed to write blob $hash from file to S3: ${e.message}", e
            return false
        }
    }

    @Override
    boolean writeOutboard(String hash, byte[] outboard) {
        def key = getOutboardKey(hash)
        log.debug "Writing outboard to S3: s3://$bucket/$key"

        try {
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(),
                RequestBody.fromBytes(outboard)
            )
            return true
        } catch (Exception e) {
            log.error "Failed to write outboard for $hash to S3: ${e.message}", e
            return false
        }
    }

    @Override
    InputStream readBlob(String hash) {
        def key = getBlobKey(hash)
        log.debug "Reading blob from S3: s3://$bucket/$key"

        try {
            def response = s3Client.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            return response
        } catch (NoSuchKeyException e) {
            log.debug "Blob not found: $hash"
            return null
        } catch (Exception e) {
            log.error "Failed to read blob $hash from S3: ${e.message}", e
            return null
        }
    }

    @Override
    byte[] readOutboard(String hash) {
        def key = getOutboardKey(hash)
        log.debug "Reading outboard from S3: s3://$bucket/$key"

        try {
            def response = s3Client.getObjectAsBytes(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            return response.asByteArray()
        } catch (NoSuchKeyException e) {
            log.debug "Outboard not found: $hash"
            return null
        } catch (Exception e) {
            log.error "Failed to read outboard for $hash from S3: ${e.message}", e
            return null
        }
    }

    @Override
    boolean exists(String hash) {
        def key = getBlobKey(hash)

        try {
            s3Client.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            return true
        } catch (NoSuchKeyException e) {
            return false
        } catch (Exception e) {
            log.error "Failed to check existence of blob $hash: ${e.message}", e
            return false
        }
    }

    @Override
    boolean deleteBlob(String hash) {
        def blobKey = getBlobKey(hash)
        def outboardKey = getOutboardKey(hash)

        try {
            // Delete both blob and outboard
            s3Client.deleteObjects(
                DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(
                        Delete.builder()
                            .objects(
                                ObjectIdentifier.builder().key(blobKey).build(),
                                ObjectIdentifier.builder().key(outboardKey).build()
                            )
                            .build()
                    )
                    .build()
            )
            log.debug "Deleted blob and outboard: $hash"
            return true
        } catch (Exception e) {
            log.error "Failed to delete blob $hash from S3: ${e.message}", e
            return false
        }
    }

    @Override
    long getBlobSize(String hash) {
        def key = getBlobKey(hash)

        try {
            def response = s3Client.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            return response.contentLength()
        } catch (NoSuchKeyException e) {
            return -1
        } catch (Exception e) {
            log.error "Failed to get size of blob $hash: ${e.message}", e
            return -1
        }
    }

    @Override
    void close() {
        s3Client.close()
    }

    /**
     * Get the S3 key for a blob
     */
    private String getBlobKey(String hash) {
        return prefix ? "${prefix}/blobs/${hash}" : "blobs/${hash}"
    }

    /**
     * Get the S3 key for an outboard file
     */
    private String getOutboardKey(String hash) {
        return prefix ? "${prefix}/blobs/${hash}.obao" : "blobs/${hash}.obao"
    }
}
