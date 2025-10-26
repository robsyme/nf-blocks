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

package robsyme.plugin.hash

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.Files
import java.nio.file.Path

/**
 * CLI-based hash computer that shells out to b3sum and bao commands.
 *
 * Requires:
 * - b3sum (BLAKE3 hash tool)
 * - bao (Bao tree hash tool)
 *
 * Install via:
 * - b3sum: cargo install b3sum
 * - bao: cargo install bao-tree
 */
@Slf4j
@CompileStatic
class CLIHashComputer implements HashComputer {

    private final String b3sumCommand
    private final String baoCommand

    /**
     * Create a CLI hash computer with default command names
     */
    CLIHashComputer() {
        this('b3sum', 'bao')
    }

    /**
     * Create a CLI hash computer with custom command paths
     */
    CLIHashComputer(String b3sumCommand, String baoCommand) {
        this.b3sumCommand = b3sumCommand
        this.baoCommand = baoCommand
    }

    @Override
    HashResult compute(Path file) {
        log.debug "Computing hash and outboard for: $file"

        // Compute BLAKE3 hash
        def hash = computeHash(file)
        if (!hash) {
            throw new RuntimeException("Failed to compute BLAKE3 hash for: $file")
        }

        // Compute Bao outboard
        def outboard = computeOutboard(file)
        if (!outboard) {
            throw new RuntimeException("Failed to compute Bao outboard for: $file")
        }

        // Get file size
        def size = Files.size(file)

        return new HashResult(hash, outboard, size)
    }

    @Override
    String computeHash(Path file) {
        try {
            def process = [b3sumCommand, '--no-names', file.toString()].execute()
            process.waitFor()

            if (process.exitValue() != 0) {
                def error = process.err.text
                log.error "b3sum failed: $error"
                return null
            }

            def output = process.text.trim()
            return output.split(/\s+/)[0]  // First field is the hash
        } catch (Exception e) {
            log.error "Failed to run b3sum: ${e.message}", e
            return null
        }
    }

    /**
     * Compute Bao outboard for a file
     *
     * @param file The file to process
     * @return The outboard data, or null if failed
     */
    private byte[] computeOutboard(Path file) {
        try {
            // Create temp file for outboard
            def tempOutboard = Files.createTempFile('bao-', '.obao')

            try {
                // Run: bao encode <input> <outboard>
                def process = [baoCommand, 'encode', file.toString(), '--outboard', tempOutboard.toString()].execute()
                process.waitFor()

                if (process.exitValue() != 0) {
                    def error = process.err.text
                    log.error "bao encode failed: $error"
                    return null
                }

                // Read outboard data
                return Files.readAllBytes(tempOutboard)
            } finally {
                // Clean up temp file
                Files.deleteIfExists(tempOutboard)
            }
        } catch (Exception e) {
            log.error "Failed to compute Bao outboard: ${e.message}", e
            return null
        }
    }

    @Override
    boolean isAvailable() {
        return isCommandAvailable(b3sumCommand) && isCommandAvailable(baoCommand)
    }

    /**
     * Check if a command is available in PATH
     */
    private boolean isCommandAvailable(String command) {
        try {
            def process = [command, '--version'].execute()
            process.waitFor()
            return process.exitValue() == 0
        } catch (Exception e) {
            log.debug "Command not available: $command"
            return false
        }
    }
}
