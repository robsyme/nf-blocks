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