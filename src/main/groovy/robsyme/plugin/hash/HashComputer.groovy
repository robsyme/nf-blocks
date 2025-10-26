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

import java.nio.file.Path

/**
 * Interface for computing content hashes and Bao outboards.
 *
 * Implementations can use different approaches:
 * - CLI-based (shell out to b3sum/bao)
 * - JVM-based (pure Java/Groovy libraries)
 */
interface HashComputer {

    /**
     * Compute BLAKE3 hash and Bao outboard for a file
     *
     * @param file The file to hash
     * @return HashResult containing the hash, outboard, and file size
     */
    HashResult compute(Path file)

    /**
     * Compute only the BLAKE3 hash (no outboard)
     *
     * @param file The file to hash
     * @return The BLAKE3 hash in hexadecimal format
     */
    String computeHash(Path file)

    /**
     * Check if the hash computer is available (e.g., required CLI tools installed)
     *
     * @return true if the hash computer can be used
     */
    boolean isAvailable()
}
