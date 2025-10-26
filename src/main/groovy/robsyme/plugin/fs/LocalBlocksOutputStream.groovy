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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * OutputStream for local filesystem that writes to temp location
 * while calculating BLAKE3 hash, then atomically moves to final
 * hash-addressed location.
 *
 * Flow:
 * 1. Write to {basePath}/.staging/{uuid}.tmp
 * 2. Calculate hash as data streams through
 * 3. On close: atomic rename to {basePath}/blobs/{hash}
 */
@Slf4j
@CompileStatic
class LocalBlocksOutputStream extends OutputStream {

    private final Path tempPath
    private final Path baseDir
    private final String finalPrefix // e.g., "blobs"

    private final FileOutputStream fileStream
    private final Process b3sumProcess
    private final OutputStream hashInput

    private long totalBytesWritten = 0
    private boolean closed = false

    LocalBlocksOutputStream(Path baseDir, Path tempPath, String finalPrefix) {
        this.baseDir = baseDir
        this.tempPath = tempPath
        this.finalPrefix = finalPrefix

        // Ensure parent directory exists
        Files.createDirectories(tempPath.parent)

        // Open file stream
        this.fileStream = new FileOutputStream(tempPath.toFile())

        // Start b3sum process for streaming hash calculation
        this.b3sumProcess = new ProcessBuilder('b3sum', '--no-names')
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start()

        this.hashInput = b3sumProcess.outputStream

        log.debug "Started streaming write to temp: ${tempPath}"
    }

    @Override
    void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }

        fileStream.write(b)
        hashInput.write(b)
        totalBytesWritten++
    }

    @Override
    void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed")
        }

        fileStream.write(b, off, len)
        hashInput.write(b, off, len)
        totalBytesWritten += len
    }

    @Override
    void close() throws IOException {
        if (closed) {
            return
        }

        try {
            // Close the file stream
            fileStream.close()

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

            log.info "Write complete. Total bytes: ${totalBytesWritten}, CID: ${cid}"

            // Atomic move to final CID-addressed location
            def finalDir = baseDir.resolve(finalPrefix)
            Files.createDirectories(finalDir)

            def finalPath = finalDir.resolve(cid)

            log.debug "Atomically moving to final location: ${finalPath}"
            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)

            log.debug "File successfully stored at: ${finalPath}"

        } catch (Exception e) {
            log.error "Failed to finalize write: ${e.message}", e

            // Cleanup temp file on error
            try {
                Files.deleteIfExists(tempPath)
            } catch (Exception cleanupEx) {
                log.warn "Failed to cleanup temp file: ${tempPath}"
            }

            throw new IOException("Failed to finalize write", e)
        } finally {
            closed = true
        }
    }

    @Override
    void flush() throws IOException {
        fileStream.flush()
        hashInput.flush()
    }
}
