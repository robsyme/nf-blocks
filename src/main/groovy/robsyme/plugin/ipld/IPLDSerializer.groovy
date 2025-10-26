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
import groovy.util.logging.Slf4j
import peergos.shared.cbor.CborObject
import robsyme.plugin.hash.CLIHashComputer
import robsyme.plugin.hash.HashComputer

import java.nio.file.Files
import java.nio.file.Path

/**
 * Serializer for IPLD objects to DAG-CBOR format.
 *
 * Uses the official java-ipld-cbor library which ensures:
 * - Deterministic encoding (same object always produces same bytes)
 * - Definite-length encoding only (no indefinite-length constructs)
 * - Canonical ordering of map keys (lexicographic)
 *
 * This produces proper DAG-CBOR that is compatible with IPFS and other IPLD tools.
 */
@Slf4j
@CompileStatic
class IPLDSerializer {

    private final HashComputer hashComputer

    IPLDSerializer() {
        this(new CLIHashComputer())
    }

    IPLDSerializer(HashComputer hashComputer) {
        this.hashComputer = hashComputer
    }

    /**
     * Serialize an OutputMetadata object to CBOR bytes
     *
     * @param metadata The metadata to serialize
     * @return DAG-CBOR-encoded bytes
     */
    byte[] serialize(OutputMetadata metadata) {
        def map = metadata.toMap()
        def cborObj = toCborObject(map)
        return cborObj.toByteArray()
    }

    /**
     * Serialize a RunManifest object to CBOR bytes
     *
     * @param manifest The manifest to serialize
     * @return DAG-CBOR-encoded bytes
     */
    byte[] serialize(RunManifest manifest) {
        def map = manifest.toMap()
        def cborObj = toCborObject(map)
        return cborObj.toByteArray()
    }

    /**
     * Serialize any map to CBOR bytes
     *
     * @param data The data to serialize
     * @return DAG-CBOR-encoded bytes
     */
    byte[] serializeMap(Map<String, Object> data) {
        def cborObj = toCborObject(data)
        return cborObj.toByteArray()
    }

    /**
     * Convert a Java object to CborObject recursively
     *
     * @param obj The object to convert
     * @return The CborObject representation
     */
    private CborObject toCborObject(Object obj) {
        if (obj == null) {
            return new CborObject.CborNull()
        } else if (obj instanceof Map) {
            return mapToCbor((Map<String, Object>) obj)
        } else if (obj instanceof List) {
            return listToCbor((List<Object>) obj)
        } else if (obj instanceof String) {
            return new CborObject.CborString((String) obj)
        } else if (obj instanceof Boolean) {
            return new CborObject.CborBoolean((Boolean) obj)
        } else if (obj instanceof Long || obj instanceof Integer) {
            return new CborObject.CborLong(((Number) obj).longValue())
        } else if (obj instanceof Double || obj instanceof Float) {
            // DAG-CBOR doesn't support floats, convert to string for now
            // TODO: Decide on proper handling of floating point numbers
            return new CborObject.CborString(obj.toString())
        } else if (obj instanceof byte[]) {
            return new CborObject.CborByteArray((byte[]) obj)
        } else {
            // Fallback: convert to string
            log.warn "Unknown type ${obj.class.simpleName}, converting to string"
            return new CborObject.CborString(obj.toString())
        }
    }

    /**
     * Convert a Map to CborMap with sorted keys
     */
    private CborObject.CborMap mapToCbor(Map<String, Object> map) {
        // Convert all values to CborObject (which are Cborable)
        Map<String, CborObject> cborMap = new LinkedHashMap<>()
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            cborMap.put(entry.key, toCborObject(entry.value))
        }

        // Use CborMap.build() which handles String keys and sorting
        return CborObject.CborMap.build(cborMap)
    }

    /**
     * Convert a List to CborList
     */
    private CborObject.CborList listToCbor(List<Object> list) {
        List<CborObject> cborList = new ArrayList<>()
        for (Object item : list) {
            cborList.add(toCborObject(item))
        }
        return new CborObject.CborList(cborList)
    }

    /**
     * Deserialize CBOR bytes to OutputMetadata
     *
     * @param bytes The CBOR bytes
     * @return The deserialized metadata
     */
    OutputMetadata deserializeOutputMetadata(byte[] bytes) {
        def cborObj = CborObject.fromByteArray(bytes)
        def map = fromCborObject(cborObj) as Map<String, Object>
        return OutputMetadata.fromMap(map)
    }

    /**
     * Deserialize CBOR bytes to RunManifest
     *
     * @param bytes The CBOR bytes
     * @return The deserialized manifest
     */
    RunManifest deserializeRunManifest(byte[] bytes) {
        def cborObj = CborObject.fromByteArray(bytes)
        def map = fromCborObject(cborObj) as Map<String, Object>
        return RunManifest.fromMap(map)
    }

    /**
     * Deserialize CBOR bytes to a Map
     *
     * @param bytes The CBOR bytes
     * @return The deserialized map
     */
    Map<String, Object> deserializeMap(byte[] bytes) {
        def cborObj = CborObject.fromByteArray(bytes)
        return fromCborObject(cborObj) as Map<String, Object>
    }

    /**
     * Convert a CborObject back to Java objects
     *
     * @param cbor The CBOR object
     * @return The Java object
     */
    private Object fromCborObject(CborObject cbor) {
        if (cbor instanceof CborObject.CborNull) {
            return null
        } else if (cbor instanceof CborObject.CborMap) {
            return cborMapToMap((CborObject.CborMap) cbor)
        } else if (cbor instanceof CborObject.CborList) {
            return cborListToList((CborObject.CborList) cbor)
        } else if (cbor instanceof CborObject.CborString) {
            return ((CborObject.CborString) cbor).value
        } else if (cbor instanceof CborObject.CborBoolean) {
            return ((CborObject.CborBoolean) cbor).value
        } else if (cbor instanceof CborObject.CborLong) {
            return ((CborObject.CborLong) cbor).value
        } else if (cbor instanceof CborObject.CborByteArray) {
            return ((CborObject.CborByteArray) cbor).value
        } else {
            log.warn "Unknown CborObject type ${cbor.class.simpleName}"
            return null
        }
    }

    /**
     * Convert CborMap to Java Map
     */
    private Map<String, Object> cborMapToMap(CborObject.CborMap cborMap) {
        Map<String, Object> result = new LinkedHashMap<>()
        // cborMap.values is a SortedMap<CborObject, ? extends Cborable>
        for (Map.Entry entry : cborMap.values.entrySet()) {
            // Keys are CborObject (typically CborString)
            def keyCbor = (CborObject) entry.key
            String keyString = fromCborObject(keyCbor) as String

            // Values are Cborable (which extend CborObject)
            def valueCbor = (CborObject) entry.value
            result.put(keyString, fromCborObject(valueCbor))
        }
        return result
    }

    /**
     * Convert CborList to Java List
     */
    private List<Object> cborListToList(CborObject.CborList cborList) {
        List<Object> result = new ArrayList<>()
        for (Object item : cborList.value) {
            // Items are Cborable, need to convert to CborObject first
            if (item instanceof CborObject) {
                result.add(fromCborObject((CborObject) item))
            } else {
                result.add(item)
            }
        }
        return result
    }

    /**
     * Serialize an object and compute its CID
     *
     * @param metadata The metadata to serialize
     * @return A tuple of [CID, CBOR bytes]
     */
    Tuple2<String, byte[]> serializeAndComputeCID(OutputMetadata metadata) {
        def bytes = serialize(metadata)
        def cid = computeCIDForBytes(bytes)
        return new Tuple2<>(cid, bytes)
    }

    /**
     * Serialize a manifest and compute its CID
     *
     * @param manifest The manifest to serialize
     * @return A tuple of [CID, CBOR bytes]
     */
    Tuple2<String, byte[]> serializeAndComputeCID(RunManifest manifest) {
        def bytes = serialize(manifest)
        def cid = computeCIDForBytes(bytes)
        return new Tuple2<>(cid, bytes)
    }

    /**
     * Compute CID for CBOR bytes
     *
     * This writes the bytes to a temp file, computes the BLAKE3 hash,
     * then generates the CID with the DAG-CBOR codec.
     *
     * @param bytes The CBOR bytes
     * @return The CID
     */
    private String computeCIDForBytes(byte[] bytes) {
        // Write to temp file for hashing
        Path tempFile = Files.createTempFile('ipld-', '.cbor')
        try {
            Files.write(tempFile, bytes)

            // Compute BLAKE3 hash
            def hash = hashComputer.computeHash(tempFile)

            // Generate CID with DAG-CBOR codec (0x71)
            return CID.fromHash(hash, 0x71)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }
}
