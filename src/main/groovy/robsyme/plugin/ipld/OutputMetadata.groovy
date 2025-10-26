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

package robsyme.plugin.ipld

import groovy.transform.CompileStatic

/**
 * IPLD metadata object for a single output file.
 *
 * Each published file gets a rich metadata object that includes:
 * - File information (name, type, size)
 * - Sample metadata (from Nextflow's meta Map)
 * - Process metadata (process name, container, resources)
 * - Link to the actual data blob
 * - Link back to the run manifest (provenance)
 */
@CompileStatic
class OutputMetadata {
    /**
     * Type identifier for IPLD
     */
    String type = "NextflowOutputFile"

    /**
     * Original file name
     */
    String fileName

    /**
     * MIME type or file type
     */
    String fileType

    /**
     * Sample metadata extracted from Nextflow's meta Map
     * Can contain arbitrary nested structure
     */
    Map<String, Object> sampleMetadata

    /**
     * Process metadata (name, container, resources, duration)
     */
    Map<String, Object> processMetadata

    /**
     * Link to the actual data blob (CID of the file content)
     */
    Link dataBlob

    /**
     * Size of the data blob in bytes
     */
    Long dataSizeBytes

    /**
     * Hash algorithm used (always "blake3" for now)
     */
    String dataHashAlgorithm = "blake3"

    /**
     * Link back to the run manifest (provenance)
     */
    Link producedByRun

    /**
     * Timestamp when this output was produced (ISO 8601)
     */
    String producedAt

    /**
     * Convert to a map for serialization
     */
    Map<String, Object> toMap() {
        def map = [
            '@type': type,
            'file_name': fileName,
            'file_type': fileType,
            'data_blob': dataBlob?.toMap(),
            'data_size_bytes': dataSizeBytes,
            'data_hash_algorithm': dataHashAlgorithm,
            'produced_at': producedAt
        ]

        if (sampleMetadata) {
            map['sample_metadata'] = sampleMetadata
        }

        if (processMetadata) {
            map['process_metadata'] = processMetadata
        }

        if (producedByRun) {
            map['produced_by_run'] = producedByRun.toMap()
        }

        return map
    }

    /**
     * Create from a map (for deserialization)
     */
    static OutputMetadata fromMap(Map<String, Object> map) {
        def metadata = new OutputMetadata()
        metadata.type = map['@type'] as String ?: "NextflowOutputFile"
        metadata.fileName = map['file_name'] as String
        metadata.fileType = map['file_type'] as String
        metadata.sampleMetadata = map['sample_metadata'] as Map<String, Object>
        metadata.processMetadata = map['process_metadata'] as Map<String, Object>
        metadata.dataSizeBytes = map['data_size_bytes'] as Long
        metadata.dataHashAlgorithm = map['data_hash_algorithm'] as String ?: "blake3"
        metadata.producedAt = map['produced_at'] as String

        if (map['data_blob']) {
            metadata.dataBlob = Link.fromMap(map['data_blob'] as Map<String, String>)
        }

        if (map['produced_by_run']) {
            metadata.producedByRun = Link.fromMap(map['produced_by_run'] as Map<String, String>)
        }

        return metadata
    }
}
