package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * Unsigned LEB128, the multiformats varint. Values are non-negative and at
 * most 63 bits wide, which covers every code point used by CIDv1.
 */
@CompileStatic
class Varint {

    static byte[] encode(long value) {
        if( value < 0 )
            throw new IllegalArgumentException("varint value must not be negative: $value")
        def out = new ByteArrayOutputStream(10)
        long v = value
        while( v >= 0x80 ) {
            out.write((int) ((v & 0x7f) | 0x80))
            v >>>= 7
        }
        out.write((int) v)
        return out.toByteArray()
    }

    /**
     * Reads one varint from {@code bytes} starting at {@code pos[0]} and
     * advances {@code pos[0]} past it.
     */
    static long read(byte[] bytes, int[] pos) {
        long result = 0
        int shift = 0
        int i = pos[0]
        while( true ) {
            if( i >= bytes.length )
                throw new IllegalArgumentException('truncated varint')
            if( shift > 56 )
                throw new IllegalArgumentException('varint wider than 63 bits')
            int b = bytes[i] & 0xff
            result |= ((long) (b & 0x7f)) << shift
            i++
            if( (b & 0x80) == 0 ) {
                if( b == 0 && shift > 0 )
                    throw new IllegalArgumentException('varint is not minimally encoded')
                break
            }
            shift += 7
        }
        pos[0] = i
        return result
    }
}
