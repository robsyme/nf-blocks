package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

import groovy.transform.CompileStatic

/**
 * Strict canonical DAG-CBOR (DESIGN.md §4): definite lengths everywhere,
 * smallest-width integers, 64-bit floats only, text map keys sorted by
 * length then bytewise, links as tag 42. Anything else is refused, on the
 * way in and on the way out, so that a block has exactly one encoding and
 * therefore exactly one address.
 */
@CompileStatic
class DagCbor {

    private static final int MAJOR_UNSIGNED = 0
    private static final int MAJOR_NEGATIVE = 1
    private static final int MAJOR_BYTES = 2
    private static final int MAJOR_TEXT = 3
    private static final int MAJOR_ARRAY = 4
    private static final int MAJOR_MAP = 5
    private static final int MAJOR_TAG = 6
    private static final int MAJOR_SIMPLE = 7

    private static final int TAG_CID = 42
    private static final int INDEFINITE = 31

    private static final BigInteger UINT64_MAX = new BigInteger('18446744073709551615')

    /** The smallest argument each head width is allowed to carry. */
    private static final long[] MINIMUM_FOR_WIDTH = [24L, 0x100L, 0x10000L, 0x100000000L] as long[]

    static byte[] encode(Object value) {
        def out = new ByteArrayOutputStream(256)
        writeValue(out, value)
        return out.toByteArray()
    }

    static Object decode(byte[] encoded) {
        if( encoded == null )
            throw new IllegalArgumentException('cannot decode null')
        def reader = new Reader(encoded)
        Object value = reader.readValue()
        if( reader.pos != encoded.length )
            throw new IllegalArgumentException("${encoded.length - reader.pos} trailing byte(s) after the top-level item")
        return value
    }

    static Cid cidOf(byte[] encoded) {
        return Cid.of(Cid.DAG_CBOR, Hashing.sha256(encoded))
    }

    /**
     * UTF-8 for a string that must be well-formed UTF-16. A lone surrogate
     * would otherwise be written as '?', so two different keys could collide
     * and a block would no longer say what it was given.
     */
    private static byte[] utf8(String text) {
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(text))
            byte[] bytes = new byte[buffer.remaining()]
            buffer.get(bytes)
            return bytes
        }
        catch( CharacterCodingException e ) {
            throw new IllegalArgumentException("dag-cbor cannot encode a string that is not valid unicode: ${e.message}")
        }
    }

    private static String fromUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString()
        }
        catch( CharacterCodingException e ) {
            throw new IllegalArgumentException("dag-cbor text is not valid utf-8: ${e.message}")
        }
    }

    // ---------------------------------------------------------------- encode

    private static void writeValue(ByteArrayOutputStream out, Object value) {
        if( value == null ) {
            out.write(0xf6)
        }
        else if( value instanceof Boolean ) {
            out.write(((Boolean) value) ? 0xf5 : 0xf4)
        }
        else if( value instanceof Cid ) {
            writeHead(out, MAJOR_TAG, TAG_CID)
            byte[] link = ((Cid) value).bytes()
            writeHead(out, MAJOR_BYTES, link.length + 1L)
            out.write(0x00)          // the identity multibase prefix DAG-CBOR links carry
            out.write(link, 0, link.length)
        }
        else if( value instanceof BigInteger ) {
            writeBigInteger(out, (BigInteger) value)
        }
        else if( value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte ) {
            writeLong(out, ((Number) value).longValue())
        }
        else if( value instanceof Double || value instanceof Float || value instanceof BigDecimal ) {
            // BigDecimal is widened to a 64-bit float and is therefore lossy;
            // no Nextflow structure measured so far carries one.
            writeDouble(out, ((Number) value).doubleValue())
        }
        else if( value instanceof byte[] ) {
            byte[] bytes = (byte[]) value
            writeHead(out, MAJOR_BYTES, bytes.length)
            out.write(bytes, 0, bytes.length)
        }
        else if( value instanceof CharSequence ) {
            byte[] bytes = utf8(value.toString())
            writeHead(out, MAJOR_TEXT, bytes.length)
            out.write(bytes, 0, bytes.length)
        }
        else if( value instanceof Map ) {
            writeMap(out, (Map) value)
        }
        else if( value instanceof List ) {
            // A List only: a Set or a Queue has no order the reader could
            // reproduce, so it has no canonical encoding.
            List items = (List) value
            writeHead(out, MAJOR_ARRAY, items.size())
            for( Object item : items )
                writeValue(out, item)
        }
        else {
            throw new IllegalArgumentException("dag-cbor cannot encode a ${value.getClass().name}")
        }
    }

    private static void writeMap(ByteArrayOutputStream out, Map map) {
        // The pairs travel together through the sort: re-deriving a key from
        // its bytes would lose the association if two keys ever encoded alike.
        List<Object[]> pairs = new ArrayList<>(map.size())
        Set<String> seen = new HashSet<>(map.size())
        for( Object entry : map.entrySet() ) {
            Object rawKey = ((Map.Entry) entry).key
            if( !(rawKey instanceof CharSequence) )
                throw new IllegalArgumentException("dag-cbor map keys must be strings, got a ${rawKey == null ? 'null' : rawKey.getClass().name}")
            String key = rawKey.toString()
            if( key == '/' )
                throw new IllegalArgumentException('a dag-cbor map may not have the key "/"')
            if( !seen.add(key) )
                throw new IllegalArgumentException("duplicate map key '$key'")
            pairs.add([utf8(key), ((Map.Entry) entry).value] as Object[])
        }
        pairs.sort(new Comparator<Object[]>() {
            @Override int compare(Object[] a, Object[] b) { compareKeys((byte[]) a[0], (byte[]) b[0]) }
        })
        writeHead(out, MAJOR_MAP, pairs.size())
        for( Object[] pair : pairs ) {
            byte[] key = (byte[]) pair[0]
            writeHead(out, MAJOR_TEXT, key.length)
            out.write(key, 0, key.length)
            writeValue(out, pair[1])
        }
    }

    /** Canonical DAG-CBOR key order: shorter first, then unsigned bytewise. */
    private static int compareKeys(byte[] a, byte[] b) {
        if( a.length != b.length )
            return a.length <=> b.length
        for( int i = 0; i < a.length; i++ ) {
            int d = (a[i] & 0xff) - (b[i] & 0xff)
            if( d != 0 ) return d
        }
        return 0
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        if( value >= 0 )
            writeHead(out, MAJOR_UNSIGNED, value)
        else
            writeHead(out, MAJOR_NEGATIVE, -1L - value)
    }

    private static void writeBigInteger(ByteArrayOutputStream out, BigInteger value) {
        if( value.bitLength() < 64 ) {
            writeLong(out, value.longValueExact())
            return
        }
        if( value.signum() > 0 ) {
            if( value > UINT64_MAX )
                throw new IllegalArgumentException("integer out of dag-cbor range: $value")
            writeHead(out, MAJOR_UNSIGNED, value.longValue())
        }
        else {
            BigInteger n = BigInteger.valueOf(-1L) - value
            if( n > UINT64_MAX )
                throw new IllegalArgumentException("integer out of dag-cbor range: $value")
            writeHead(out, MAJOR_NEGATIVE, n.longValue())
        }
    }

    private static void writeDouble(ByteArrayOutputStream out, double value) {
        if( Double.isNaN(value) || Double.isInfinite(value) )
            throw new IllegalArgumentException("dag-cbor cannot encode $value")
        out.write(0xfb)
        long bits = Double.doubleToRawLongBits(value)
        for( int shift = 56; shift >= 0; shift -= 8 )
            out.write((int) ((bits >>> shift) & 0xff))
    }

    /** Writes a major type and its argument in the smallest width that holds it. */
    private static void writeHead(ByteArrayOutputStream out, int major, long argument) {
        final int base = major << 5
        if( Long.compareUnsigned(argument, 24L) < 0 ) {
            out.write(base | (int) argument)
        }
        else if( Long.compareUnsigned(argument, 0xffL) <= 0 ) {
            out.write(base | 24)
            out.write((int) (argument & 0xff))
        }
        else if( Long.compareUnsigned(argument, 0xffffL) <= 0 ) {
            out.write(base | 25)
            writeBigEndian(out, argument, 2)
        }
        else if( Long.compareUnsigned(argument, 0xffffffffL) <= 0 ) {
            out.write(base | 26)
            writeBigEndian(out, argument, 4)
        }
        else {
            out.write(base | 27)
            writeBigEndian(out, argument, 8)
        }
    }

    private static void writeBigEndian(ByteArrayOutputStream out, long value, int width) {
        for( int shift = (width - 1) * 8; shift >= 0; shift -= 8 )
            out.write((int) ((value >>> shift) & 0xff))
    }

    // ---------------------------------------------------------------- decode

    @CompileStatic
    private static class Reader {

        private final byte[] buf
        int pos = 0

        Reader(byte[] buf) { this.buf = buf }

        Object readValue() {
            final int head = readByte()
            final int major = head >>> 5
            final int info = head & 0x1f
            if( major == MAJOR_SIMPLE )
                return readSimple(info)
            final long arg = readArgument(info)
            switch( major ) {
                case MAJOR_UNSIGNED:
                    return arg >= 0 ? (Object) Long.valueOf(arg) : (Object) unsigned(arg)
                case MAJOR_NEGATIVE:
                    return arg >= 0 ? (Object) Long.valueOf(-1L - arg) : (Object) (BigInteger.valueOf(-1L) - unsigned(arg))
                case MAJOR_BYTES:
                    return readBytes(arg)
                case MAJOR_TEXT:
                    return fromUtf8(readBytes(arg))
                case MAJOR_ARRAY:
                    return readArray(arg)
                case MAJOR_MAP:
                    return readMap(arg)
                case MAJOR_TAG:
                    return readTagged(arg)
                default:
                    throw new IllegalArgumentException("unsupported cbor major type $major")
            }
        }

        private int readByte() {
            if( pos >= buf.length )
                throw new IllegalArgumentException('truncated cbor item')
            return buf[pos++] & 0xff
        }

        private long readArgument(int info) {
            if( info < 24 ) return info
            if( info == INDEFINITE )
                throw new IllegalArgumentException('indefinite lengths are not valid dag-cbor')
            if( info > 27 )
                throw new IllegalArgumentException("reserved cbor additional information $info")
            final int width = 1 << (info - 24)
            long value = 0
            for( int i = 0; i < width; i++ )
                value = (value << 8) | readByte()
            // Canonical dag-cbor: the head must be the narrowest one that fits,
            // or the same value would have more than one encoding.
            final long floor = MINIMUM_FOR_WIDTH[info - 24]
            if( Long.compareUnsigned(value, floor) < 0 )
                throw new IllegalArgumentException("non-minimal cbor head: $value does not need ${width} byte(s)")
            return value
        }

        private static BigInteger unsigned(long value) {
            return BigInteger.valueOf(value >>> 32).shiftLeft(32).or(BigInteger.valueOf(value & 0xffffffffL))
        }

        private byte[] readBytes(long length) {
            if( length < 0 || length > Integer.MAX_VALUE )
                throw new IllegalArgumentException("cbor string length $length is out of range")
            final int n = (int) length
            if( buf.length - pos < n )
                throw new IllegalArgumentException("truncated cbor string: $n bytes announced, ${buf.length - pos} available")
            byte[] out = Arrays.copyOfRange(buf, pos, pos + n)
            pos += n
            return out
        }

        private List<Object> readArray(long length) {
            if( length < 0 || length > Integer.MAX_VALUE )
                throw new IllegalArgumentException("cbor array length $length is out of range")
            def items = new ArrayList<Object>((int) Math.min(length, 64L))
            for( long i = 0; i < length; i++ )
                items.add(readValue())
            return items
        }

        private Map<String, Object> readMap(long length) {
            if( length < 0 || length > Integer.MAX_VALUE )
                throw new IllegalArgumentException("cbor map length $length is out of range")
            def map = new LinkedHashMap<String, Object>()
            byte[] previous = null
            for( long i = 0; i < length; i++ ) {
                final int head = readByte()
                if( (head >>> 5) != MAJOR_TEXT )
                    throw new IllegalArgumentException("dag-cbor map keys must be text strings, found major type ${head >>> 5}")
                byte[] key = readBytes(readArgument(head & 0x1f))
                if( previous != null && compareKeys(previous, key) >= 0 )
                    throw new IllegalArgumentException('dag-cbor map keys must be strictly ordered by length then bytes')
                previous = key
                String name = fromUtf8(key)
                if( name == '/' )
                    throw new IllegalArgumentException('a dag-cbor map may not have the key "/"')
                map.put(name, readValue())
            }
            return map
        }

        private Cid readTagged(long tag) {
            if( tag != TAG_CID )
                throw new IllegalArgumentException("only tag $TAG_CID (cid) is valid dag-cbor, found tag $tag")
            final int head = readByte()
            if( (head >>> 5) != MAJOR_BYTES )
                throw new IllegalArgumentException('a tag 42 must wrap a byte string')
            byte[] link = readBytes(readArgument(head & 0x1f))
            if( link.length < 1 || link[0] != (byte) 0x00 )
                throw new IllegalArgumentException('a tag 42 byte string must start with the 0x00 multibase prefix')
            return Cid.parse('b' + Multibase.base32Encode(Arrays.copyOfRange(link, 1, link.length)))
        }

        private Object readSimple(int info) {
            switch( info ) {
                case 20: return Boolean.FALSE
                case 21: return Boolean.TRUE
                case 22: return null
                case 27:
                    long bits = 0
                    for( int i = 0; i < 8; i++ )
                        bits = (bits << 8) | readByte()
                    double value = Double.longBitsToDouble(bits)
                    if( Double.isNaN(value) || Double.isInfinite(value) )
                        throw new IllegalArgumentException('dag-cbor cannot carry NaN or an infinity')
                    return value
                case 25:
                case 26:
                    throw new IllegalArgumentException('dag-cbor floats must be 64-bit')
                default:
                    throw new IllegalArgumentException("unsupported cbor simple value $info")
            }
        }
    }
}
