package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * base32 lower-case, RFC 4648 alphabet, no padding. This is multibase code
 * 'b', the only text encoding nf-blocks writes or reads.
 */
@CompileStatic
class Multibase {

    static final String BASE32_ALPHABET = 'abcdefghijklmnopqrstuvwxyz234567'

    private static final int[] DECODE = new int[128]

    static {
        for( int i = 0; i < DECODE.length; i++ ) DECODE[i] = -1
        for( int i = 0; i < BASE32_ALPHABET.length(); i++ )
            DECODE[(int) BASE32_ALPHABET.charAt(i)] = i
    }

    static String base32Encode(byte[] data) {
        def sb = new StringBuilder((data.length * 8 + 4) / 5 as int)
        int buffer = 0
        int bits = 0
        for( byte b : data ) {
            buffer = (buffer << 8) | (b & 0xff)
            bits += 8
            while( bits >= 5 ) {
                bits -= 5
                sb.append(BASE32_ALPHABET.charAt((buffer >>> bits) & 0x1f))
            }
        }
        if( bits > 0 )
            sb.append(BASE32_ALPHABET.charAt((buffer << (5 - bits)) & 0x1f))
        return sb.toString()
    }

    static byte[] base32Decode(String text) {
        if( text == null )
            throw new IllegalArgumentException('base32 input is null')
        final int rem = text.length() % 8
        if( rem == 1 || rem == 3 || rem == 6 )
            throw new IllegalArgumentException("not a base32 string: impossible length ${text.length()}")
        def out = new ByteArrayOutputStream(text.length() * 5 / 8 as int)
        int buffer = 0
        int bits = 0
        for( int i = 0; i < text.length(); i++ ) {
            char c = text.charAt(i)
            int v = c < 128 ? DECODE[(int) c] : -1
            if( v < 0 )
                throw new IllegalArgumentException("not a base32 character: '$c'")
            buffer = (buffer << 5) | v
            bits += 5
            if( bits >= 8 ) {
                bits -= 8
                out.write((buffer >>> bits) & 0xff)
            }
        }
        if( bits > 0 && ((buffer << (8 - bits)) & 0xff) != 0 )
            throw new IllegalArgumentException('base32 string has non-zero padding bits')
        return out.toByteArray()
    }
}
