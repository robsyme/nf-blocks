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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * Local filesystem-backed blob storage implementation.
 *
 * Stores blobs in a local directory with the following structure:
 * - {basePath}/blobs/{hash}
 * - {basePath}/blobs/{hash}.obao
 *
 * Useful for local testing and caching.
 */
@Slf4j
@CompileStatic
class FileBlobStore implements BlobStore {

    private final Path basePath
    private final Path blobsDir

    /**
     * Create a file-based blob store
     *
     * @param basePath The base directory for blob storage
     */
    FileBlobStore(Path basePath) {
        this.basePath = basePath
        this.blobsDir = basePath.resolve("blobs")

        // Create directories if they don't exist
        Files.createDirectories(blobsDir)
    }

    /**
     * Create a file-based blob store from a string path
     */
    FileBlobStore(String basePath) {
        this(Paths.get(basePath))
    }

    @Override
    boolean writeBlob(String hash, InputStream data) {
        def blobPath = getBlobPath(hash)
        log.debug "Writing blob to file: $blobPath"

        try {
            // Ensure parent directory exists
            Files.createDirectories(blobPath.parent)

            // Write to temp file first, then move atomically
            def tempPath = blobPath.parent.resolve("${hash}.tmp")
            Files.copy(data, tempPath, StandardCopyOption.REPLACE_EXISTING)
            Files.move(tempPath, blobPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)

            return true
        } catch (Exception e) {
            log.error "Failed to write blob $hash to file: ${e.message}", e
            return false
        }
    }

    @Override
    boolean writeBlob(String hash, Path file) {
        def blobPath = getBlobPath(hash)
        log.debug "Writing blob from file: $file -> $blobPath"

        try {
            // Ensure parent directory exists
            Files.createDirectories(blobPath.parent)

            // Copy file
            Files.copy(file, blobPath, StandardCopyOption.REPLACE_EXISTING)

            return true
        } catch (Exception e) {
            log.error "Failed to write blob $hash from file: ${e.message}", e
            return false
        }
    }

    @Override
    boolean writeOutboard(String hash, byte[] outboard) {
        def outboardPath = getOutboardPath(hash)
        log.debug "Writing outboard to file: $outboardPath"

        try {
            // Ensure parent directory exists
            Files.createDirectories(outboardPath.parent)

            Files.write(outboardPath, outboard)
            return true
        } catch (Exception e) {
            log.error "Failed to write outboard for $hash: ${e.message}", e
            return false
        }
    }

    @Override
    InputStream readBlob(String hash) {
        def blobPath = getBlobPath(hash)
        log.debug "Reading blob from file: $blobPath"

        try {
            if (!Files.exists(blobPath)) {
                log.debug "Blob not found: $hash"
                return null
            }

            return Files.newInputStream(blobPath)
        } catch (Exception e) {
            log.error "Failed to read blob $hash from file: ${e.message}", e
            return null
        }
    }

    @Override
    byte[] readOutboard(String hash) {
        def outboardPath = getOutboardPath(hash)
        log.debug "Reading outboard from file: $outboardPath"

        try {
            if (!Files.exists(outboardPath)) {
                log.debug "Outboard not found: $hash"
                return null
            }

            return Files.readAllBytes(outboardPath)
        } catch (Exception e) {
            log.error "Failed to read outboard for $hash: ${e.message}", e
            return null
        }
    }

    @Override
    boolean exists(String hash) {
        def blobPath = getBlobPath(hash)
        return Files.exists(blobPath)
    }

    @Override
    boolean deleteBlob(String hash) {
        def blobPath = getBlobPath(hash)
        def outboardPath = getOutboardPath(hash)

        try {
            boolean deletedBlob = false
            boolean deletedOutboard = false

            if (Files.exists(blobPath)) {
                Files.delete(blobPath)
                deletedBlob = true
            }

            if (Files.exists(outboardPath)) {
                Files.delete(outboardPath)
                deletedOutboard = true
            }

            log.debug "Deleted blob and outboard: $hash"
            return deletedBlob || deletedOutboard
        } catch (Exception e) {
            log.error "Failed to delete blob $hash: ${e.message}", e
            return false
        }
    }

    @Override
    long getBlobSize(String hash) {
        def blobPath = getBlobPath(hash)

        try {
            if (!Files.exists(blobPath)) {
                return -1
            }

            return Files.size(blobPath)
        } catch (Exception e) {
            log.error "Failed to get size of blob $hash: ${e.message}", e
            return -1
        }
    }

    @Override
    void close() {
        // Nothing to close for file-based storage
    }

    /**
     * Get the path for a blob file
     */
    private Path getBlobPath(String hash) {
        return blobsDir.resolve(hash)
    }

    /**
     * Get the path for an outboard file
     */
    private Path getOutboardPath(String hash) {
        return blobsDir.resolve("${hash}.obao")
    }
}
