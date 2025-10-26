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

package robsyme.plugin.storage

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Factory for creating storage backend implementations based on URI schemes.
 *
 * Supports:
 * - blocks+s3://bucket/prefix - S3 blob storage
 * - blocks+file:///path/to/dir - Local filesystem blob storage
 * - refs+s3://bucket/prefix - S3 ref storage
 * - refs+file:///path/to/dir - Local filesystem ref storage
 */
@Slf4j
@CompileStatic
class StorageBackendFactory {

    /**
     * Create a BlobStore from a URI
     *
     * @param uri The storage URI (e.g., "blocks+s3://my-bucket")
     * @return A BlobStore implementation
     */
    static BlobStore createBlobStore(String uri) {
        if (!uri) {
            throw new IllegalArgumentException("URI cannot be null or empty")
        }

        // Parse URI format: blocks+<scheme>://<rest>
        if (!uri.startsWith('blocks+')) {
            throw new IllegalArgumentException("BlobStore URI must start with 'blocks+'. Got: $uri")
        }

        def actualUri = uri.substring('blocks+'.length())
        def parsed = new URI(actualUri)
        def scheme = parsed.scheme

        switch (scheme) {
            case 's3':
                return createS3BlobStore(parsed)
            case 'file':
                return createFileBlobStore(parsed)
            default:
                throw new IllegalArgumentException("Unsupported blob storage scheme: $scheme")
        }
    }

    /**
     * Create a RefStore from a URI
     *
     * @param uri The storage URI (e.g., "refs+s3://my-bucket")
     * @return A RefStore implementation
     */
    static RefStore createRefStore(String uri) {
        if (!uri) {
            throw new IllegalArgumentException("URI cannot be null or empty")
        }

        // Parse URI format: refs+<scheme>://<rest>
        if (!uri.startsWith('refs+')) {
            throw new IllegalArgumentException("RefStore URI must start with 'refs+'. Got: $uri")
        }

        def actualUri = uri.substring('refs+'.length())
        def parsed = new URI(actualUri)
        def scheme = parsed.scheme

        switch (scheme) {
            case 's3':
                return createS3RefStore(parsed)
            case 'file':
                return createFileRefStore(parsed)
            default:
                throw new IllegalArgumentException("Unsupported ref storage scheme: $scheme")
        }
    }

    private static BlobStore createS3BlobStore(URI uri) {
        def bucket = uri.host
        def prefix = uri.path ? uri.path.substring(1) : ''  // Remove leading /

        log.debug "Creating S3BlobStore: bucket=$bucket, prefix=$prefix"

        // Create S3 client using default credentials provider
        def s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
            .build()

        return new robsyme.plugin.storage.impl.S3BlobStore(s3Client, bucket, prefix)
    }

    private static RefStore createS3RefStore(URI uri) {
        def bucket = uri.host
        def prefix = uri.path ? uri.path.substring(1) : ''

        log.debug "Creating S3RefStore: bucket=$bucket, prefix=$prefix"

        // Create S3 client using default credentials provider
        def s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
            .build()

        return new robsyme.plugin.storage.impl.S3RefStore(s3Client, bucket, prefix)
    }

    private static BlobStore createFileBlobStore(URI uri) {
        def path = uri.path

        log.debug "Creating FileBlobStore: path=$path"

        return new robsyme.plugin.storage.impl.FileBlobStore(path)
    }

    private static RefStore createFileRefStore(URI uri) {
        def path = uri.path

        log.debug "Creating FileRefStore: path=$path"

        return new robsyme.plugin.storage.impl.FileRefStore(path)
    }
}
