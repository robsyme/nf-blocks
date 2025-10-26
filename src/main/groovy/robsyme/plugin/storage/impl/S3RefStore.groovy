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
import robsyme.plugin.storage.RefStore
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.charset.StandardCharsets

/**
 * S3-backed ref storage implementation.
 *
 * Stores refs as small text files in S3:
 * - s3://bucket/prefix/refs/workflows/{name}/latest
 * - s3://bucket/prefix/refs/projects/{id}/latest
 * - s3://bucket/prefix/refs/runs/{run-id}
 *
 * Each ref file contains a single line with the CID it points to.
 */
@Slf4j
@CompileStatic
class S3RefStore implements RefStore {

    private final S3Client s3Client
    private final String bucket
    private final String prefix

    /**
     * Create an S3 ref store
     *
     * @param s3Client The S3 client to use
     * @param bucket The S3 bucket name
     * @param prefix Optional prefix for all refs (e.g., "project-a")
     */
    S3RefStore(S3Client s3Client, String bucket, String prefix = '') {
        this.s3Client = s3Client
        this.bucket = bucket
        this.prefix = prefix ? prefix.replaceAll('/$', '') : ''
    }

    @Override
    boolean writeRef(String refPath, String cid) {
        def key = getRefKey(refPath)
        log.debug "Writing ref to S3: s3://$bucket/$key -> $cid"

        try {
            // Write ref as plain text file with just the CID
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType("text/plain")
                    .build(),
                RequestBody.fromString(cid, StandardCharsets.UTF_8)
            )
            return true
        } catch (Exception e) {
            log.error "Failed to write ref $refPath to S3: ${e.message}", e
            return false
        }
    }

    @Override
    String readRef(String refPath) {
        def key = getRefKey(refPath)
        log.debug "Reading ref from S3: s3://$bucket/$key"

        try {
            def response = s3Client.getObjectAsBytes(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            return response.asString(StandardCharsets.UTF_8).trim()
        } catch (NoSuchKeyException e) {
            log.debug "Ref not found: $refPath"
            return null
        } catch (Exception e) {
            log.error "Failed to read ref $refPath from S3: ${e.message}", e
            return null
        }
    }

    @Override
    boolean exists(String refPath) {
        def key = getRefKey(refPath)

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
            log.error "Failed to check existence of ref $refPath: ${e.message}", e
            return false
        }
    }

    @Override
    List<String> listRefs(String prefix) {
        def fullPrefix = getRefKey(prefix)
        log.debug "Listing refs under: s3://$bucket/$fullPrefix"

        try {
            List<String> result = []
            def request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(fullPrefix)
                .build()

            def response = s3Client.listObjectsV2(request)

            while (true) {
                response.contents().each { s3Object ->
                    // Convert S3 key back to ref path
                    String refPath = s3Object.key()
                    if (this.prefix) {
                        refPath = refPath.substring("${this.prefix}/refs/".length())
                    } else {
                        refPath = refPath.substring("refs/".length())
                    }
                    result << refPath
                }

                if (!response.isTruncated()) {
                    break
                }

                request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(fullPrefix)
                    .continuationToken(response.nextContinuationToken())
                    .build()

                response = s3Client.listObjectsV2(request)
            }

            return result
        } catch (Exception e) {
            log.error "Failed to list refs under $prefix: ${e.message}", e
            return []
        }
    }

    @Override
    boolean deleteRef(String refPath) {
        def key = getRefKey(refPath)

        try {
            s3Client.deleteObject(
                DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            log.debug "Deleted ref: $refPath"
            return true
        } catch (Exception e) {
            log.error "Failed to delete ref $refPath from S3: ${e.message}", e
            return false
        }
    }

    @Override
    void close() {
        s3Client.close()
    }

    /**
     * Get the S3 key for a ref
     */
    private String getRefKey(String refPath) {
        // Remove leading slash if present
        def cleanPath = refPath.startsWith('/') ? refPath.substring(1) : refPath

        return prefix ? "${prefix}/refs/${cleanPath}" : "refs/${cleanPath}"
    }
}
