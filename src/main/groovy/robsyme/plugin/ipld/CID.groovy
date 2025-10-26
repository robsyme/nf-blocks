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
import org.apache.commons.codec.binary.Base32

/**
 * Content Identifier (CID) utilities for IPLD.
 *
 * Implements CIDv1 with BLAKE3 hashing.
 * Format: bafkr4i{base32-encoded-cid-bytes}
 *
 * CID structure:
 * - Version: 1 (0x01)
 * - Codec: raw (0x55) - for raw data blobs
 * - Multihash: BLAKE3 (0x1e) + length + hash bytes
 */
@CompileStatic
class CID {

    // Multicodec codes
    private static final int CODEC_RAW = 0x55
    private static final int CODEC_DAG_CBOR = 0x71

    // Multihash codes
    private static final int HASH_BLAKE3 = 0x1e

    // BLAKE3 hash length (32 bytes)
    private static final int BLAKE3_LENGTH = 32

    /**
     * Create a CID from a BLAKE3 hash (hex string)
     *
     * @param hash The BLAKE3 hash in hexadecimal format
     * @param codec The multicodec to use (default: raw)
     * @return The CID string (e.g., "bafkr4i...")
     */
    static String fromHash(String hash, int codec = CODEC_RAW) {
        // Convert hex hash to bytes
        def hashBytes = hexToBytes(hash)

        if (hashBytes.length != BLAKE3_LENGTH) {
            throw new IllegalArgumentException(
                "BLAKE3 hash must be 32 bytes, got ${hashBytes.length}"
            )
        }

        return encodeCID(codec, HASH_BLAKE3, hashBytes)
    }

    /**
     * Create a CID from raw data by computing its BLAKE3 hash
     *
     * Note: This requires the hash to be pre-computed. Use HashComputer instead.
     *
     * @param hash The pre-computed BLAKE3 hash
     * @param codec The multicodec to use
     * @return The CID string
     */
    static String fromBytes(byte[] hashBytes, int codec = CODEC_RAW) {
        if (hashBytes.length != BLAKE3_LENGTH) {
            throw new IllegalArgumentException(
                "BLAKE3 hash must be 32 bytes, got ${hashBytes.length}"
            )
        }

        return encodeCID(codec, HASH_BLAKE3, hashBytes)
    }

    /**
     * Encode a CID from its components
     *
     * @param codec The multicodec code
     * @param hashType The multihash type code
     * @param hashBytes The hash bytes
     * @return The CID string (base32-encoded)
     */
    private static String encodeCID(int codec, int hashType, byte[] hashBytes) {
        def baos = new ByteArrayOutputStream()

        // Write CID version (1)
        writeVarint(baos, 1)

        // Write codec
        writeVarint(baos, codec)

        // Write multihash
        writeVarint(baos, hashType)  // Hash type
        writeVarint(baos, hashBytes.length)  // Hash length
        baos.write(hashBytes)  // Hash bytes

        // Encode as base32 lowercase
        def cidBytes = baos.toByteArray()
        def base32 = new Base32()
        def encoded = base32.encodeAsString(cidBytes).toLowerCase()

        // Remove padding
        encoded = encoded.replaceAll('=', '')

        // Add multibase prefix 'b' for base32 lowercase
        return 'b' + encoded
    }

    /**
     * Decode a CID string to its components
     *
     * @param cid The CID string (e.g., "bafkr4i...")
     * @return Map with keys: version, codec, hashType, hash (hex string)
     */
    static Map<String, Object> decode(String cid) {
        if (!cid || cid.length() < 2) {
            throw new IllegalArgumentException("Invalid CID: too short")
        }

        // Check multibase prefix
        def prefix = cid.charAt(0)
        if (prefix != 'b' as char) {
            throw new IllegalArgumentException(
                "Unsupported multibase: $prefix (only 'b' base32 supported)"
            )
        }

        // Decode base32
        def base32 = new Base32()
        def encoded = cid.substring(1).toUpperCase()

        // Add padding if needed
        def paddingNeeded = (8 - (encoded.length() % 8)) % 8
        encoded += '=' * paddingNeeded

        def cidBytes = base32.decode(encoded)
        def bais = new ByteArrayInputStream(cidBytes)

        // Read CID version
        def version = readVarint(bais)

        // Read codec
        def codec = readVarint(bais)

        // Read multihash
        def hashType = readVarint(bais)
        def hashLength = readVarint(bais)
        def hashBytes = new byte[hashLength]
        bais.read(hashBytes)

        return [
            version: version,
            codec: codec,
            hashType: hashType,
            hash: bytesToHex(hashBytes)
        ]
    }

    /**
     * Write an unsigned varint to a stream
     */
    private static void writeVarint(OutputStream os, long value) {
        while (value >= 0x80) {
            os.write((byte)((value & 0x7F) | 0x80))
            value >>= 7
        }
        os.write((byte)value)
    }

    /**
     * Read an unsigned varint from a stream
     */
    private static long readVarint(InputStream is) {
        long result = 0
        int shift = 0
        int b

        while (true) {
            b = is.read()
            if (b == -1) {
                throw new EOFException("Unexpected end of varint")
            }

            result |= (long)(b & 0x7F) << shift
            if ((b & 0x80) == 0) {
                break
            }

            shift += 7
        }

        return result
    }

    /**
     * Convert hex string to bytes
     */
    private static byte[] hexToBytes(String hex) {
        def length = hex.length()
        def bytes = new byte[(int)(length / 2)]

        for (int i = 0; i < length; i += 2) {
            bytes[(int)(i / 2)] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16))
        }

        return bytes
    }

    /**
     * Convert bytes to hex string
     */
    private static String bytesToHex(byte[] bytes) {
        def hexChars = new StringBuilder()
        for (byte b : bytes) {
            hexChars.append(String.format('%02x', b))
        }
        return hexChars.toString()
    }

    /**
     * Check if a string is a valid CID
     */
    static boolean isValid(String cid) {
        try {
            decode(cid)
            return true
        } catch (Exception e) {
            return false
        }
    }
}
