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
import groovy.transform.Immutable

/**
 * Result of computing a content hash and Bao outboard
 */
@Immutable
@CompileStatic
class HashResult {
    /**
     * The BLAKE3 hash in hexadecimal format
     */
    String hash

    /**
     * The Bao outboard data
     */
    byte[] outboard

    /**
     * File size in bytes
     */
    long size
}
