package robsyme.cas.core

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
            byte[] bytes = value.toString().getBytes('UTF-8')
            writeHead(out, MAJOR_TEXT, bytes.length)
            out.write(bytes, 0, bytes.length)
        }
        else if( value instanceof Map ) {
            writeMap(out, (Map) value)
        }
        else if( value instanceof Collection ) {
            Collection items = (Collection) value
            writeHead(out, MAJOR_ARRAY, items.size())
            for( Object item : items )
                writeValue(out, item)
        }
        else {
            throw new IllegalArgumentException("dag-cbor cannot encode a ${value.getClass().name}")
        }
    }

    private static void writeMap(ByteArrayOutputStream out, Map map) {
        List<byte[]> keys = new ArrayList<>(map.size())
        Map<String, Object> byKey = new HashMap<>(map.size())
        for( Object entry : map.entrySet() ) {
            Object rawKey = ((Map.Entry) entry).key
            if( !(rawKey instanceof CharSequence) )
                throw new IllegalArgumentException("dag-cbor map keys must be strings, got a ${rawKey == null ? 'null' : rawKey.getClass().name}")
            String key = rawKey.toString()
            if( key == '/' )
                throw new IllegalArgumentException('a dag-cbor map may not have the key "/"')
            if( byKey.containsKey(key) )
                throw new IllegalArgumentException("duplicate map key '$key'")
            byKey.put(key, ((Map.Entry) entry).value)
            keys.add(key.getBytes('UTF-8'))
        }
        keys.sort(new Comparator<byte[]>() {
            @Override int compare(byte[] a, byte[] b) { compareKeys(a, b) }
        })
        writeHead(out, MAJOR_MAP, keys.size())
        for( byte[] key : keys ) {
            writeHead(out, MAJOR_TEXT, key.length)
            out.write(key, 0, key.length)
            writeValue(out, byKey.get(new String(key, 'UTF-8')))
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
                    return new String(readBytes(arg), 'UTF-8')
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
                String name = new String(key, 'UTF-8')
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
