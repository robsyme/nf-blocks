package nextflow.cbor

import spock.lang.Specification
import spock.lang.Unroll

class CborObjectTest extends Specification {

    def "should encode positive integers"() {
        expect:
        new CborObject.CborLong(value).toByteArray() == expected as byte[]

        where:
        value | expected
        0     | [0x00]
        23    | [0x17]
        24    | [0x18, 0x18]
        255   | [0x18, 0xff]
        256   | [0x19, 0x01, 0x00]
        65535 | [0x19, 0xff, 0xff]
        65536 | [0x1a, 0x00, 0x01, 0x00, 0x00]
    }

    def "should encode negative integers"() {
        expect:
        new CborObject.CborLong(value).toByteArray() == expected as byte[]

        where:
        value | expected
        -1    | [0x20]
        -24   | [0x37]
        -25   | [0x38, 0x18]
        -256  | [0x38, 0xff]
        -257  | [0x39, 0x01, 0x00]
    }

    def "should encode strings"() {
        expect:
        new CborObject.CborString(value).toByteArray() == expected

        where:
        value     | expected
        ""        | [0x60] as byte[]
        "a"       | buildBytes(0x61, "a")
        "IETF"    | buildBytes(0x64, "IETF")
        "hello"   | buildBytes(0x65, "hello")
        "你好"     | buildBytes(0x66, "你好")
    }

    def "should encode byte arrays"() {
        expect:
        new CborObject.CborByteArray(value as byte[]).toByteArray() == expected as byte[]

        where:
        value        | expected
        []           | [0x40]
        [1]          | [0x41, 1]
        [1, 2, 3, 4] | [0x44, 1, 2, 3, 4]
    }

    def "should encode arrays"() {
        given:
        def empty = new CborObject.CborList([])
        def nums = new CborObject.CborList([
            new CborObject.CborLong(1),
            new CborObject.CborLong(2),
            new CborObject.CborLong(3)
        ])
        def mixed = new CborObject.CborList([
            new CborObject.CborString("hello"),
            new CborObject.CborLong(42)
        ])

        expect:
        empty.toByteArray() == [0x80] as byte[]
        nums.toByteArray() == [0x83, 0x01, 0x02, 0x03] as byte[]
        mixed.toByteArray() == buildBytes(
            [0x82] as byte[],
            buildBytes(0x65, "hello"),
            [0x18, 0x2a] as byte[]
        )
    }

    def "should encode maps"() {
        given:
        def empty = new CborObject.CborMap([:])
        def simple = new CborObject.CborMap([
            (new CborObject.CborString("a")): new CborObject.CborLong(1),
            (new CborObject.CborString("b")): new CborObject.CborLong(2)
        ])

        expect:
        empty.toByteArray() == [0xa0] as byte[]
        simple.toByteArray() == buildBytes(
            [0xa2] as byte[],
            buildBytes(0x61, "a"),
            [0x01] as byte[],
            buildBytes(0x61, "b"),
            [0x02] as byte[]
        )
    }

    def "should convert Groovy objects via CborConverter"() {
        expect:
        CborConverter.toCbor(input).toByteArray() == expected

        where:
        input                  | expected
        "test"                 | buildBytes(0x64, "test")
        42                     | [0x18, 0x2a] as byte[]
        [1, 2, 3]              | [0x83, 0x01, 0x02, 0x03] as byte[]
        [a: 1, b: 2]           | buildBytes(
                                    [0xa2] as byte[],
                                    buildBytes(0x61, "a"),
                                    [0x01] as byte[],
                                    buildBytes(0x61, "b"),
                                    [0x02] as byte[]
                                )
        [1] as byte[]          | [0x41, 0x01] as byte[]
    }

    def "should throw exception for unsupported types"() {
        when:
        CborConverter.toCbor(new Object())

        then:
        thrown(IllegalArgumentException)
    }

    def "should encode strings correctly"() {
        when:
        def str = new CborObject.CborString("hello")
        def bytes = str.toByteArray()
        
        then:
        bytes[0] == 0x65 as byte  // MT_TEXT_STRING | 5 (length)
        bytes[1..5] == "hello".getBytes("UTF-8")
    }
    
    def "should encode integers with minimal bytes"() {
        expect:
        new CborObject.CborLong(0).toByteArray() == [0x00] as byte[]
        new CborObject.CborLong(23).toByteArray() == [0x17] as byte[]
        new CborObject.CborLong(24).toByteArray() == [0x18, 0x18] as byte[]
        new CborObject.CborLong(255).toByteArray() == [0x18, 0xff] as byte[]
        new CborObject.CborLong(256).toByteArray() == [0x19, 0x01, 0x00] as byte[]
        new CborObject.CborLong(-1).toByteArray() == [0x20] as byte[]
        new CborObject.CborLong(-24).toByteArray() == [0x37] as byte[]
        new CborObject.CborLong(-25).toByteArray() == [0x38, 0x18] as byte[]
    }
    
    def "should encode floats as 64-bit double precision"() {
        when:
        def float32 = new CborObject.CborFloat(3.14f)
        def double64 = new CborObject.CborFloat(3.14d)
        
        then:
        float32.toByteArray()[0] == 0xfb as byte  // MT_FLOAT_OR_SIMPLE | AI_8_BYTES
        double64.toByteArray()[0] == 0xfb as byte
        float32.toByteArray().length == 9  // 1 byte header + 8 bytes data
        double64.toByteArray().length == 9
    }
    
    def "should reject IEEE 754 special values"() {
        when:
        new CborObject.CborFloat(value)
        
        then:
        thrown(IllegalArgumentException)
        
        where:
        value << [Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY]
    }
    
    def "should enforce string keys in maps"() {
        when:
        new CborObject.CborMap([
            (new CborObject.CborLong(1)): new CborObject.CborString("value")
        ] as Map<CborObject, CborObject>)
        
        then:
        thrown(IllegalArgumentException)
    }
    
    def "should sort map keys by length first, then lexicographically"() {
        given:
        def map = new CborObject.CborMap([
            (new CborObject.CborString("bbb")): new CborObject.CborString("1"),
            (new CborObject.CborString("a")): new CborObject.CborString("2"),
            (new CborObject.CborString("aa")): new CborObject.CborString("3"),
            (new CborObject.CborString("aaa")): new CborObject.CborString("4"),
        ])
        
        when:
        def bytes = map.toByteArray()
        def keys = []
        def currentPos = 1  // Skip initial map length byte
        
        // Extract keys from the byte array
        4.times {
            def keyLength = bytes[currentPos] & 0x1f
            currentPos++
            keys << new String(bytes[currentPos..(currentPos + keyLength - 1)] as byte[], "UTF-8")
            currentPos += keyLength + 2  // Skip key bytes and value (which is single digit + length byte)
        }
        
        then:
        keys == ["a", "aa", "aaa", "bbb"]  // Should be sorted by length first, then lexicographically
    }
    
    def "should encode CID tag correctly"() {
        given:
        def cidBytes = [0x01, 0x02, 0x03] as byte[]
        def tag = new CborObject.CborTag(42, new CborObject.CborByteArray(cidBytes))
        
        when:
        def bytes = tag.toByteArray()
        
        then:
        bytes[0..1] == [0xd8, 0x2a] as byte[]  // Tag 42 in canonical form
        bytes[2] == 0x43 as byte  // Byte string of length 3
        bytes[3..5] == cidBytes
    }
    
    def "should reject non-CID tags"() {
        when:
        new CborObject.CborTag(invalidTag, new CborObject.CborString("test"))
        
        then:
        thrown(IllegalArgumentException)
        
        where:
        invalidTag << [0, 1, 41, 43, 1000]
    }

    def "should match known base64-encoded DAG-CBOR objects"() {
        expect:
        CborConverter.toCbor(value).toByteArray().encodeBase64().toString() == expectedBase64
        
        where:
        value                                                   | expectedBase64
        [2]                                                     | "gQI="
        true                                                    | "9Q=="
        false                                                   | "9A=="
        ""                                                      | "YA=="
        -501                                                    | "OQH0"
        -2784428724                                             | "OqX3ArM="
        [255]                                                   | "gRj/"
        ["array",["of",[5,["nested",["arrays","!"]]]]]          | "gmVhcnJheYJib2aCBYJmbmVzdGVkgmZhcnJheXNhIQ=="
        ["a":1]                                                 | "oWFhAQ=="
        [:]                                                     | "oA=="
        ["object":["with":["4":"nested","objects":["!":"!"]]]]  | "oWZvYmplY3ShZHdpdGiiYTRmbmVzdGVkZ29iamVjdHOhYSFhIQ=="
        "Čaues ßvěte!"                                          | "b8SMYXVlcyDDn3bEm3RlIQ=="                
        ["aaaaaa":6,"aaaaab":7,"aaaaac":8,"aaaabb":9,"bbbbb":5,"cccc":4,"ddd":3,"ee":2,"f":1]   | "qWFmAWJlZQJjZGRkA2RjY2NjBGViYmJiYgVmYWFhYWFhBmZhYWFhYWIHZmFhYWFhYwhmYWFhYWJiCQ=="
    }

    def "should encode complex structures to known base64"() {
        given:
        def nested = new CborObject.CborList([
            new CborObject.CborString("hello"),
            new CborObject.CborList([
                new CborObject.CborLong(1),
                new CborObject.CborLong(2)
            ])
        ])
        
        expect:
        nested.toByteArray().encodeBase64().toString() == "gmVoZWxsb4IBAg=="  // ["hello", [1,2]]
        
        and: "map with sorted keys"
        def map = new CborObject.CborMap([
            (new CborObject.CborString("a")): new CborObject.CborLong(1),
            (new CborObject.CborString("b")): new CborObject.CborLong(2)
        ])
        map.toByteArray().encodeBase64().toString() == "omFhAWFiAg=="  // {"a":1,"b":2}
    }

    def "should encode CID tag to known base64"() {
        given:
        def cidBytes = [0x01, 0x55, 0x00] as byte[]  // Simple example CID bytes
        def tag = new CborObject.CborTag(42, new CborObject.CborByteArray(cidBytes))
        
        expect:
        tag.toByteArray().encodeBase64().toString() == "2CpDAVUA"  // Tag 42 with bytes [0x01, 0x55, 0x00]
    }

    def "should encode boolean values correctly"() {
        expect:
        new CborObject.CborBoolean(true).toByteArray() == [0xf5] as byte[]
        new CborObject.CborBoolean(false).toByteArray() == [0xf4] as byte[]
    }

    def "should convert boolean values via CborConverter"() {
        expect:
        CborConverter.toCbor(true).toByteArray() == [0xf5] as byte[]
        CborConverter.toCbor(false).toByteArray() == [0xf4] as byte[]
    }

    def "should encode boolean values to known base64"() {
        expect:
        CborConverter.toCbor(true).toByteArray().encodeBase64().toString() == "9Q=="   // true
        CborConverter.toCbor(false).toByteArray().encodeBase64().toString() == "9A=="  // false
    }

    private byte[] buildBytes(int prefix, String str) {
        def baos = new ByteArrayOutputStream()
        baos.write(prefix)
        baos.write(str.getBytes("UTF-8"))
        return baos.toByteArray()
    }

    private byte[] buildBytes(byte[]... arrays) {
        def baos = new ByteArrayOutputStream()
        arrays.each { baos.write(it) }
        return baos.toByteArray()
    }
}