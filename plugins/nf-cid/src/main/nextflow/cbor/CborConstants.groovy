package nextflow.cbor

/**
 * Constants for CBOR encoding as defined in RFC 7049.
 */
class CborConstants {
    // Major types
    static final int MT_UNSIGNED_INT = 0
    static final int MT_NEGATIVE_INT = 1
    static final int MT_BYTE_STRING = 2
    static final int MT_TEXT_STRING = 3
    static final int MT_ARRAY = 4
    static final int MT_MAP = 5
    static final int MT_TAG = 6
    static final int MT_FLOAT_OR_SIMPLE = 7

    // Additional information values
    static final int AI_1_BYTE = 24
    static final int AI_2_BYTES = 25
    static final int AI_4_BYTES = 26
    static final int AI_8_BYTES = 27

    /**
     * Create initial byte with major type and additional info
     */
    static byte initialByte(int majorType, int additionalInfo) {
        return ((majorType << 5) | (additionalInfo & 0x1f)) as byte
    }

    /**
     * Write an unsigned integer as the minimal number of bytes
     */
    static byte[] encodeUnsignedInteger(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Value cannot be negative: " + value)
        }
        if (value <= 23) {
            return [initialByte(MT_UNSIGNED_INT, value as int)] as byte[]
        }
        if (value <= 0xFF) {
            return [
                initialByte(MT_UNSIGNED_INT, AI_1_BYTE),
                value as byte
            ] as byte[]
        }
        if (value <= 0xFFFF) {
            return [
                initialByte(MT_UNSIGNED_INT, AI_2_BYTES),
                (value >>> 8) as byte,
                value as byte
            ] as byte[]
        }
        if (value <= 0xFFFFFFFFL) {
            return [
                initialByte(MT_UNSIGNED_INT, AI_4_BYTES),
                (value >>> 24) as byte,
                (value >>> 16) as byte,
                (value >>> 8) as byte,
                value as byte
            ] as byte[]
        }
        return [
            initialByte(MT_UNSIGNED_INT, AI_8_BYTES),
            (value >>> 56) as byte,
            (value >>> 48) as byte,
            (value >>> 40) as byte,
            (value >>> 32) as byte,
            (value >>> 24) as byte,
            (value >>> 16) as byte,
            (value >>> 8) as byte,
            value as byte
        ] as byte[]
    }

    /**
     * Write a length prefix for strings, arrays, and maps
     */
    static byte[] encodeLength(int majorType, long length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length)
        }
        if (length <= 23) {
            return [initialByte(majorType, length as int)] as byte[]
        }
        if (length <= 0xFF) {
            return [
                initialByte(majorType, AI_1_BYTE),
                length as byte
            ] as byte[]
        }
        if (length <= 0xFFFF) {
            return [
                initialByte(majorType, AI_2_BYTES),
                (length >>> 8) as byte,
                length as byte
            ] as byte[]
        }
        if (length <= 0xFFFFFFFFL) {
            return [
                initialByte(majorType, AI_4_BYTES),
                (length >>> 24) as byte,
                (length >>> 16) as byte,
                (length >>> 8) as byte,
                length as byte
            ] as byte[]
        }
        throw new IllegalArgumentException("Length too large: " + length)
    }
} 