package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * A CIDv1 over a sha2-256 multihash, written as multibase base32 lower
 * (prefix 'b'). 59 characters: `bafk…` for raw content, `bafy…` for
 * dag-cbor metadata. DESIGN.md §3.
 */
@CompileStatic
final class Cid implements Comparable<Cid> {

    /** multicodec `raw`: the bytes of a file */
    static final int RAW = 0x55
    /** multicodec `dag-cbor`: a metadata block */
    static final int DAG_CBOR = 0x71

    private static final int VERSION = 1
    private static final int SHA2_256 = 0x12
    private static final int DIGEST_LENGTH = 32
    private static final String BASE32_PREFIX = 'b'

    final int codec
    private final byte[] digestBytes
    private final String text

    private Cid(int codec, byte[] digest) {
        this.codec = codec
        this.digestBytes = digest
        this.text = BASE32_PREFIX + Multibase.base32Encode(binary(codec, digest))
    }

    static Cid of(int codec, byte[] sha256) {
        if( sha256 == null || sha256.length != DIGEST_LENGTH )
            throw new IllegalArgumentException("sha2-256 digest must be $DIGEST_LENGTH bytes, got ${sha256?.length}")
        checkCodec(codec)
        return new Cid(codec, Arrays.copyOf(sha256, DIGEST_LENGTH))
    }

    static Cid parse(String text) {
        if( !text )
            throw new IllegalArgumentException("not a cid: ${text == null ? 'null' : "'$text'"}")
        if( text.charAt(0) != ('b' as char) )
            throw new IllegalArgumentException("not a base32 cid (multibase prefix '${text.charAt(0)}'): '$text'")
        final byte[] bytes = Multibase.base32Decode(text.substring(1))
        final int[] pos = [0] as int[]
        final long version = Varint.read(bytes, pos)
        if( version != VERSION )
            throw new IllegalArgumentException("unsupported cid version $version: '$text'")
        final long codec = Varint.read(bytes, pos)
        checkCodec(codec)
        final long hashCode = Varint.read(bytes, pos)
        if( hashCode != SHA2_256 )
            throw new IllegalArgumentException("unsupported multihash code $hashCode: '$text'")
        final long length = Varint.read(bytes, pos)
        if( length != DIGEST_LENGTH )
            throw new IllegalArgumentException("unsupported digest length $length: '$text'")
        if( bytes.length - pos[0] != DIGEST_LENGTH )
            throw new IllegalArgumentException("cid has ${bytes.length - pos[0]} digest bytes, expected $DIGEST_LENGTH: '$text'")
        return new Cid((int) codec, Arrays.copyOfRange(bytes, pos[0], bytes.length))
    }

    /** True when the argument is a well-formed cid text form. */
    /** nf-blocks addresses file content and metadata blocks, and nothing else. */
    private static void checkCodec(long codec) {
        if( codec != RAW && codec != DAG_CBOR )
            throw new IllegalArgumentException("unsupported cid codec $codec: nf-blocks addresses only raw ($RAW) and dag-cbor ($DAG_CBOR)")
    }

    static boolean isCid(String text) {
        try {
            parse(text)
            return true
        }
        catch( IllegalArgumentException e ) {
            return false
        }
    }

    private static byte[] binary(int codec, byte[] digest) {
        def out = new ByteArrayOutputStream(40)
        out.write(Varint.encode(VERSION))
        out.write(Varint.encode(codec))
        out.write(Varint.encode(SHA2_256))
        out.write(Varint.encode(DIGEST_LENGTH))
        out.write(digest)
        return out.toByteArray()
    }

    /** The binary form: varint(1) ‖ varint(codec) ‖ multihash, no multibase prefix. */
    byte[] bytes() {
        return binary(codec, digestBytes)
    }

    byte[] getDigest() {
        return Arrays.copyOf(digestBytes, DIGEST_LENGTH)
    }

    boolean isRaw() { codec == RAW }

    boolean isDagCbor() { codec == DAG_CBOR }

    @Override
    String toString() { text }

    @Override
    boolean equals(Object other) {
        return other instanceof Cid && ((Cid) other).text == text
    }

    @Override
    int hashCode() { text.hashCode() }

    @Override
    int compareTo(Cid other) { text <=> other.text }
}
