package nextflow.cbor

import spock.lang.Specification
import spock.lang.Unroll

class CborObjectTest extends Specification {
    
    def "should encode positive integers"() {
        expect:
        new CborObject.CborLong(value).toByteArray() == expected as byte[]
        
        where:
        value | expected
        0     | [0x00]                    // 0
        23    | [0x17]                    // 23
        24    | [0x18, 0x18]              // 24
        255   | [0x18, 0xff]              // 255
        256   | [0x19, 0x01, 0x00]        // 256
        65535 | [0x19, 0xff, 0xff]        // 65535
        65536 | [0x1a, 0x00, 0x01, 0x00, 0x00] // 65536
    }
    
    def "should encode negative integers"() {
        expect:
        new CborObject.CborLong(value).toByteArray() == expected as byte[]
        
        where:
        value | expected
        -1    | [0x20]                    // -1
        -24   | [0x37]                    // -24
        -25   | [0x38, 0x18]              // -25
        -256  | [0x38, 0xff]              // -256
        -257  | [0x39, 0x01, 0x00]        // -257
    }
    
    def "should encode strings"() {
        expect:
        new CborObject.CborString(value).toByteArray() == expected
        
        where:
        value     | expected
        ""        | [0x60] as byte[]                          // Empty string
        "a"       | buildBytes(0x61, "a")                     // Single char
        "IETF"    | buildBytes(0x64, "IETF")                 // 4 chars
        "hello"   | buildBytes(0x65, "hello")                // 5 chars
        "你好"    | buildBytes(0x66, "你好")                 // UTF-8 chars
    }
    
    def "should encode byte arrays"() {
        expect:
        new CborObject.CborByteArray(value as byte[]).toByteArray() == expected as byte[]
        
        where:
        value           | expected
        []              | [0x40]                // Empty byte string
        [1]             | [0x41, 1]             // Single byte
        [1, 2, 3, 4]    | [0x44, 1, 2, 3, 4]    // 4 bytes
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
        empty.toByteArray() == [0x80] as byte[]  // Empty array
        nums.toByteArray() == [0x83, 0x01, 0x02, 0x03] as byte[]  // Array of 3 numbers
        mixed.toByteArray() == buildBytes(
            [0x82] as byte[],             // Array of 2 items
            buildBytes(0x65, "hello"),    // String "hello"
            [0x18, 0x2a] as byte[]        // Integer 42
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
        empty.toByteArray() == [0xa0] as byte[]  // Empty map
        simple.toByteArray() == buildBytes(
            [0xa2] as byte[],                    // Map of 2 pairs
            buildBytes(0x61, "a"),               // Key "a"
            [0x01] as byte[],                    // Value 1
            buildBytes(0x61, "b"),               // Key "b"
            [0x02] as byte[]                     // Value 2
        )
    }
    
    def "should convert Groovy objects via CborConverter"() {
        expect:
        CborConverter.toCbor(input).toByteArray() == expected
        
        where:
        input                   | expected
        "test"                  | buildBytes(0x64, "test")
        42                      | [0x18, 0x2a] as byte[]
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