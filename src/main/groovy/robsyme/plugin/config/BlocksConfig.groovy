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

package robsyme.plugin.config

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Configuration for the Blocks plugin
 *
 * Example configuration:
 * <pre>
 * blocks {
 *     enabled = true
 *     storage {
 *         uri = 'blocks+s3://my-org-bucket'
 *     }
 *     readLocations = [
 *         'blocks+file:///mnt/local-cache',
 *         'blocks+s3://my-org-bucket'
 *     ]
 *     refs {
 *         uri = 'refs+s3://my-org-bucket'
 *     }
 * }
 * </pre>
 */
@Slf4j
@CompileStatic
class BlocksConfig {

    /**
     * Whether the plugin is enabled
     */
    boolean enabled

    /**
     * Storage configuration for writing blobs
     */
    StorageConfig storage

    /**
     * List of read locations (checked in order)
     */
    List<String> readLocations

    /**
     * Refs storage configuration
     */
    RefsConfig refs

    /**
     * Parse configuration from a map
     */
    static BlocksConfig parse(Map config) {
        if (!config) {
            return new BlocksConfig(enabled: false)
        }

        def storage = config.storage instanceof Map
            ? StorageConfig.parse(config.storage as Map)
            : new StorageConfig()

        def refs = config.refs instanceof Map
            ? RefsConfig.parse(config.refs as Map)
            : new RefsConfig()

        def readLocations = config.readLocations instanceof List
            ? (config.readLocations as List).collect { it.toString() }
            : []

        return new BlocksConfig(
            enabled: config.enabled ? true : false,
            storage: storage,
            refs: refs,
            readLocations: readLocations
        )
    }

    /**
     * Validate the configuration
     */
    void validate() {
        if (!enabled) {
            return
        }

        if (!storage?.uri) {
            throw new IllegalArgumentException("blocks.storage.uri is required when blocks.enabled = true")
        }

        if (!refs?.uri) {
            throw new IllegalArgumentException("blocks.refs.uri is required when blocks.enabled = true")
        }

        // Validate URIs have proper format
        validateUri(storage.uri, "blocks.storage.uri")
        validateUri(refs.uri, "blocks.refs.uri")
        readLocations.eachWithIndex { uri, idx ->
            validateUri(uri, "blocks.readLocations[$idx]")
        }
    }

    private static void validateUri(String uri, String configPath) {
        if (!uri) {
            throw new IllegalArgumentException("$configPath cannot be empty")
        }

        // Check for blocks+ or refs+ prefix
        def validPrefixes = ['blocks+', 'refs+']
        if (!validPrefixes.any { uri.startsWith(it) }) {
            throw new IllegalArgumentException(
                "$configPath must start with 'blocks+' or 'refs+'. Got: $uri"
            )
        }

        // Try to parse as URI
        try {
            new URI(uri.replaceFirst(/^(blocks|refs)\+/, ''))
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "$configPath is not a valid URI: $uri", e
            )
        }
    }

    /**
     * Storage configuration
     */
    static class StorageConfig {
        String uri

        static StorageConfig parse(Map config) {
            return new StorageConfig(
                uri: config.uri?.toString()
            )
        }
    }

    /**
     * Refs configuration
     */
    static class RefsConfig {
        String uri

        static RefsConfig parse(Map config) {
            return new RefsConfig(
                uri: config.uri?.toString()
            )
        }
    }
}
