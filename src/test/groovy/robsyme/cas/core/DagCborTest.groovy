package robsyme.cas.core

import spock.lang.Specification

/** DESIGN.md §4: strict, canonical DAG-CBOR. */
class DagCborTest extends Specification {

    private static String hex(byte[] data) {
        StringBuilder sb = new StringBuilder()
        for( byte b : data ) sb.append(String.format('%02x', b))
        sb.toString()
    }

    private static byte[] bin(String hex) {
        byte[] out = new byte[hex.length().intdiv(2)]
        for( int i = 0; i < out.length; i++ )
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16)
        return out
    }

    def 'the empty map encodes to a0 and has the known cid'() {
        expect:
        hex(DagCbor.encode([:])) == 'a0'
        DagCbor.cidOf(bin('a0')).toString() == 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
        DagCbor.cidOf(bin('a0')).isDagCbor()
        DagCbor.decode(bin('a0')) == [:]
    }

    def 'the external vector encodes and decodes'() {
        given:
        def value = ['a': 1, 'b': [true, null, 'x']]

        expect:
        hex(DagCbor.encode(value)) == 'a2616101616283f5f66178'
        DagCbor.decode(bin('a2616101616283f5f66178')) == ['a': 1L, 'b': [true, null, 'x']]
    }

    def 'map keys are sorted by length then bytewise'() {
        when:
        def encoded = DagCbor.encode(['b': 1, 'a': 2, 'aa': 3])

        then:
        hex(encoded) == 'a3' + '6161' + '02' + '6162' + '01' + '626161' + '03'

        and: 'decoding preserves that order'
        new ArrayList<String>(((Map) DagCbor.decode(encoded)).keySet()) == ['a', 'b', 'aa']
    }

    def 'integer #value encodes to #expected using the smallest width'() {
        expect:
        hex(DagCbor.encode(value)) == expected

        and: 'and decodes back to the same number'
        DagCbor.decode(bin(expected)) == value as Long

        where:
        value                | expected
        0                    | '00'
        23                   | '17'
        24                   | '1818'
        255                  | '18ff'
        256                  | '190100'
        65535                | '19ffff'
        65536                | '1a00010000'
        -1                   | '20'
        -24                  | '37'
        -25                  | '3818'
        -256                 | '38ff'
        4294967296L          | '1b0000000100000000'
        4294967295L          | '1affffffff'
        Long.MAX_VALUE       | '1b7fffffffffffffff'
        Long.MIN_VALUE       | '3b7fffffffffffffff'
    }

    def 'every decoded integer is a Long'() {
        expect:
        DagCbor.decode(bin(encoded)).class == Long

        where:
        encoded << ['00', '17', '1818', '190100', '1a00010000', '1b0000000100000000', '20', '3b7fffffffffffffff']
    }

    def 'big integers beyond long range use the full width and decode to BigInteger'() {
        given: 'the largest and smallest values dag-cbor admits'
        def max = new BigInteger('18446744073709551615')   // 2^64 - 1
        def min = new BigInteger('-18446744073709551616')  // -2^64

        expect:
        hex(DagCbor.encode(max)) == '1bffffffffffffffff'
        hex(DagCbor.encode(min)) == '3bffffffffffffffff'
        DagCbor.decode(bin('1bffffffffffffffff')) == max
        DagCbor.decode(bin('3bffffffffffffffff')) == min
        DagCbor.decode(bin('1bffffffffffffffff')).class == BigInteger

        and: 'a BigInteger that fits in a long still becomes a Long'
        hex(DagCbor.encode(BigInteger.valueOf(24))) == '1818'
        DagCbor.decode(bin('1818')).class == Long
    }

    def 'integers outside +-2^64 are rejected'() {
        when:
        DagCbor.encode(new BigInteger('18446744073709551616'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'doubles are always nine bytes'() {
        expect:
        hex(DagCbor.encode(value)) == expected
        DagCbor.encode(value).length == 9
        DagCbor.decode(bin(expected)) == value

        where:
        value  | expected
        0.0d   | 'fb0000000000000000'
        1.0d   | 'fb3ff0000000000000'
        1.5d   | 'fb3ff8000000000000'
        -0.5d  | 'fbbfe0000000000000'
    }

    def 'Float and BigDecimal are widened to Double (lossy for BigDecimal)'() {
        expect:
        hex(DagCbor.encode(1.5f)) == 'fb3ff8000000000000'
        hex(DagCbor.encode(new BigDecimal('1.5'))) == 'fb3ff8000000000000'
        DagCbor.decode(DagCbor.encode(1.5f)).class == Double
    }

    def 'NaN and infinities are rejected on encode'() {
        when:
        DagCbor.encode(value)

        then:
        thrown(IllegalArgumentException)

        where:
        value << [Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY]
    }

    def 'simple values, byte strings and nested lists round-trip'() {
        expect:
        hex(DagCbor.encode(null)) == 'f6'
        hex(DagCbor.encode(true)) == 'f5'
        hex(DagCbor.encode(false)) == 'f4'
        hex(DagCbor.encode([1, 2, 3] as byte[])) == '43010203'
        hex(DagCbor.encode([])) == '80'
        hex(DagCbor.encode([[1, 2], []])) == '82' + '820102' + '80'
        hex(DagCbor.encode('x')) == '6178'
        hex(DagCbor.encode('é')) == '62c3a9'

        and:
        DagCbor.decode(bin('f6')) == null
        DagCbor.decode(bin('f5')) == true
        DagCbor.decode(bin('f4')) == false
        Arrays.equals((byte[]) DagCbor.decode(bin('43010203')), [1, 2, 3] as byte[])
        DagCbor.decode(bin('82' + '820102' + '80')) == [[1L, 2L], []]
        DagCbor.decode(bin('62c3a9')) == 'é'
    }

    def 'decoding produces LinkedHashMap and ArrayList'() {
        given:
        def decoded = DagCbor.decode(DagCbor.encode(['a': [1, 2]]))

        expect:
        decoded.getClass() == LinkedHashMap
        decoded['a'].getClass() == ArrayList
    }

    def 'a Cid is a tag 42 over a byte string prefixed with 0x00'() {
        given:
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        when:
        def encoded = DagCbor.encode(['link': cid])

        then: 'tag 42, byte string of 37 bytes, the 0x00 multibase-binary prefix, then the cid'
        hex(encoded) == 'a1' + '646c696e6b' + 'd82a' + '5825' + '00' + hex(cid.bytes())

        and:
        DagCbor.decode(encoded) == ['link': cid]
        DagCbor.decode(encoded)['link'].class == Cid
    }

    def 'a map key "/" is rejected on encode'() {
        when:
        DagCbor.encode(['/': 'x'])

        then:
        thrown(IllegalArgumentException)
    }

    def 'a map key "/" is rejected on decode'() {
        when: 'a1 612f 6178 is {"/": "x"}'
        DagCbor.decode(bin('a1612f6178'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'decode rejects #why'() {
        when:
        DagCbor.decode(bin(encoded))

        then:
        thrown(IllegalArgumentException)

        where:
        why                          | encoded
        'tag 43'                     | 'd82b4101'
        'a tag over a non-byte-string' | 'd82a01'
        'a tag 42 without the 0x00 prefix' | 'd82a5825' + '01' + '01551220' + ('00' * 32)
        'an indefinite-length array'   | '9f01ff'
        'an indefinite-length map'     | 'bf6161 01ff'.replace(' ', '')
        'an indefinite-length string'  | '7f6161ff'
        'an indefinite-length byte string' | '5f4101ff'
        'a half-precision float'       | 'f93c00'
        'a single-precision float'     | 'fa3f800000'
        'a NaN double'                 | 'fb7ff8000000000000'
        'an infinite double'           | 'fb7ff0000000000000'
        'the undefined simple value'   | 'f7'
        'a reserved additional info'   | '1c'
        'unsorted map keys'            | 'a2' + '6162' + '01' + '6161' + '02'
        'keys ordered by bytes not length' | 'a2' + '626161' + '01' + '6162' + '02'
        'duplicate map keys'           | 'a2' + '6161' + '01' + '6161' + '02'
        'a non-string map key'         | 'a2' + '01' + '01' + '02' + '02'
        'trailing bytes'               | 'a000'
        'a truncated item'             | '1a0001'
    }

    def 'map keys sort by utf-8 length then by unsigned bytes'() {
        when: "'z' is one byte, 'zz' and 'e-acute' are two, and c3 sorts after 7a"
        def encoded = DagCbor.encode(['\u00e9': 3, 'zz': 2, 'z': 1])

        then:
        hex(encoded) == 'a3' + '617a' + '01' + '627a7a' + '02' + '62c3a9' + '03'

        and:
        new ArrayList<String>(((Map) DagCbor.decode(encoded)).keySet()) == ['z', 'zz', '\u00e9']
    }

    def 'a key outside the basic plane keeps its own value'() {
        given: 'a surrogate pair, one code point, four utf-8 bytes'
        def grin = '\ud83d\ude00'

        when:
        def encoded = DagCbor.encode([(grin): 'x', 'a': 'y'])

        then:
        hex(encoded) == 'a2' + '6161' + '6179' + '64f09f9880' + '6178'

        and:
        DagCbor.decode(encoded) == [('a'): 'y', (grin): 'x']
    }

    def 'an unpaired surrogate is refused rather than silently mangled'() {
        when: 'two distinct keys that both encode to "?" without the check'
        DagCbor.encode(['\ud800': 'x', '\udc00': 'y'])

        then:
        thrown(IllegalArgumentException)

        when:
        DagCbor.encode(['a': '\ud800'])

        then:
        thrown(IllegalArgumentException)

        when:
        DagCbor.encode('\udfff')

        then:
        thrown(IllegalArgumentException)
    }

    def 'decode rejects text that is not valid utf-8'() {
        when: 'a one byte text string holding 0xff'
        DagCbor.decode(bin('61ff'))

        then:
        thrown(IllegalArgumentException)

        when: 'and the same as a map key'
        DagCbor.decode(bin('a161ff01'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'decode rejects a non-minimal head: #why'() {
        when:
        DagCbor.decode(bin(encoded))

        then:
        thrown(IllegalArgumentException)

        where:
        why                                | encoded
        'unsigned 5 in a one-byte head'    | '1805'
        'unsigned 5 in a two-byte head'    | '190005'
        'unsigned 5 in a four-byte head'   | '1a00000005'
        'unsigned 5 in an eight-byte head' | '1b0000000000000005'
        'unsigned 255 in a two-byte head'  | '1900ff'
        'negative 5 in a one-byte head'    | '3805'
        'a byte string length'             | '580105'
        'a text string length'             | '780161'
        'an array length'                  | '980100'
        'a map length'                     | 'b801616100'
        'a map value'                      | 'a161611805'
        'a tag 42 in a two-byte head'      | 'd9002a5825' + '00' + '01551220' + ('00' * 32)
    }

    def 'encode takes a List but not any other collection'() {
        given:
        def set = new LinkedHashSet<Integer>([1, 2])

        expect:
        hex(DagCbor.encode(new LinkedList<Integer>([1, 2]))) == '820102'

        when: 'a set has no defined order, so it has no canonical encoding'
        DagCbor.encode(set)

        then:
        thrown(IllegalArgumentException)
    }

    def 'encode rejects an unsupported value type'() {
        when:
        DagCbor.encode(new Object())

        then:
        thrown(IllegalArgumentException)
    }

    def 'encode rejects a non-string map key'() {
        when:
        DagCbor.encode([(1): 'x'])

        then:
        thrown(IllegalArgumentException)
    }

    def 'a nested record-shaped structure round-trips'() {
        given:
        def value = [
            'kind'  : 'DirectoryManifest',
            'schema': 1L,
            'entries': [
                ['name': 'a.txt', 'mode': 'regular', 'size': 12L,
                 'address': Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'),
                 'target': null],
            ],
        ]

        when:
        def encoded = DagCbor.encode(value)

        then:
        DagCbor.decode(encoded) == value

        and: 'encoding is deterministic regardless of insertion order'
        def shuffled = ['entries': value.entries, 'schema': 1L, 'kind': 'DirectoryManifest']
        hex(DagCbor.encode(shuffled)) == hex(encoded)

        and:
        DagCbor.cidOf(encoded) == DagCbor.cidOf(DagCbor.encode(shuffled))
    }
}
