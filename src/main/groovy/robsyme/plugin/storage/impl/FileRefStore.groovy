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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.stream.Collectors

/**
 * Local filesystem-backed ref storage implementation.
 *
 * Stores refs in a local directory with the following structure:
 * - {basePath}/refs/workflows/{name}/latest
 * - {basePath}/refs/projects/{id}/latest
 * - {basePath}/refs/runs/{run-id}
 *
 * Each ref file contains a single line with the CID it points to.
 */
@Slf4j
@CompileStatic
class FileRefStore implements RefStore {

    private final Path basePath
    private final Path refsDir

    /**
     * Create a file-based ref store
     *
     * @param basePath The base directory for ref storage
     */
    FileRefStore(Path basePath) {
        this.basePath = basePath
        this.refsDir = basePath.resolve("refs")

        // Create directories if they don't exist
        Files.createDirectories(refsDir)
    }

    /**
     * Create a file-based ref store from a string path
     */
    FileRefStore(String basePath) {
        this(Paths.get(basePath))
    }

    @Override
    boolean writeRef(String refPath, String cid) {
        def refFile = getRefPath(refPath)
        log.debug "Writing ref to file: $refFile -> $cid"

        try {
            // Ensure parent directories exist
            Files.createDirectories(refFile.parent)

            // Write to temp file first, then move atomically
            def tempPath = refFile.parent.resolve("${refFile.fileName}.tmp")
            Files.write(tempPath, cid.getBytes(StandardCharsets.UTF_8))
            Files.move(tempPath, refFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)

            return true
        } catch (Exception e) {
            log.error "Failed to write ref $refPath: ${e.message}", e
            return false
        }
    }

    @Override
    String readRef(String refPath) {
        def refFile = getRefPath(refPath)
        log.debug "Reading ref from file: $refFile"

        try {
            if (!Files.exists(refFile)) {
                log.debug "Ref not found: $refPath"
                return null
            }

            def content = new String(Files.readAllBytes(refFile), StandardCharsets.UTF_8)
            return content.trim()
        } catch (Exception e) {
            log.error "Failed to read ref $refPath: ${e.message}", e
            return null
        }
    }

    @Override
    boolean exists(String refPath) {
        def refFile = getRefPath(refPath)
        return Files.exists(refFile)
    }

    @Override
    List<String> listRefs(String prefix) {
        def prefixPath = prefix ? refsDir.resolve(prefix) : refsDir
        log.debug "Listing refs under: $prefixPath"

        try {
            if (!Files.exists(prefixPath)) {
                return []
            }

            return Files.walk(prefixPath)
                .filter { path -> Files.isRegularFile(path) }
                .map { path -> refsDir.relativize(path).toString() }
                .collect(Collectors.toList())
        } catch (Exception e) {
            log.error "Failed to list refs under $prefix: ${e.message}", e
            return []
        }
    }

    @Override
    boolean deleteRef(String refPath) {
        def refFile = getRefPath(refPath)

        try {
            if (!Files.exists(refFile)) {
                return false
            }

            Files.delete(refFile)
            log.debug "Deleted ref: $refPath"

            // Clean up empty parent directories
            cleanupEmptyDirs(refFile.parent)

            return true
        } catch (Exception e) {
            log.error "Failed to delete ref $refPath: ${e.message}", e
            return false
        }
    }

    @Override
    void close() {
        // Nothing to close for file-based storage
    }

    /**
     * Get the path for a ref file
     */
    private Path getRefPath(String refPath) {
        // Remove leading slash if present
        def cleanPath = refPath.startsWith('/') ? refPath.substring(1) : refPath
        return refsDir.resolve(cleanPath)
    }

    /**
     * Clean up empty parent directories up to refsDir
     */
    private void cleanupEmptyDirs(Path dir) {
        try {
            while (dir != refsDir && dir.startsWith(refsDir)) {
                if (Files.list(dir).findAny().isPresent()) {
                    // Directory not empty, stop
                    break
                }

                Files.delete(dir)
                dir = dir.parent
            }
        } catch (Exception e) {
            // Ignore cleanup errors
            log.debug "Failed to cleanup empty directories: ${e.message}"
        }
    }
}
