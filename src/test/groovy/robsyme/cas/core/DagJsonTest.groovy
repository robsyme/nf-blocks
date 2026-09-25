// src/test/groovy/robsyme/cas/core/DagJsonTest.groovy
package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification

/** Spec section 9.1: requests and responses are DAG-JSON, as @ipld/dag-json writes it. */
class DagJsonTest extends Specification {

    static final String LINK = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'

    static List<Map> vectors() {
        (List<Map>) new JsonSlurper().parse(Path.of('web/test/fixtures/dag-json-vectors.json').toFile())
    }

    def 'every @ipld/dag-json vector decodes and re-encodes to the same text (#v.name)'() {
        expect:
        DagJson.encodeToString(DagJson.decode((String) v.json)) == v.json

        where:
        v << vectors()
    }

    def 'links and bytes decode to Cid and byte[]'() {
        when:
        final Map m = (Map) DagJson.decode('{"l":{"/":"' + LINK + '"},"b":{"/":{"bytes":"AQID+g"}},"p":{"/":{"bytes":"AQID+g=="}}}')

        then:
        m.l == Cid.parse(LINK)
        m.b == [1, 2, 3, (byte) 250] as byte[]
        m.p == [1, 2, 3, (byte) 250] as byte[]
    }

    def 'numbers: integers are Long, BigInteger past Long, anything with a point or exponent Double'() {
        expect:
        DagJson.decode('7') == 7L
        DagJson.decode('-9223372036854775808') == Long.MIN_VALUE
        DagJson.decode('18446744073709551615') == new BigInteger('18446744073709551615')
        DagJson.decode('1.5') == 1.5d
        DagJson.decode('1e21') == 1e21d
        DagJson.decode('1.0') instanceof Double
    }

    def 'strings keep every escape and every code point'() {
        expect:
        DagJson.decode('"q\\"\\\\\\n\\u0001\\u00e9\\ud834\\udd1e"') == 'q"\\\n\u0001é𝄞'
        DagJson.encodeToString('a\u001fb') == '"a\\u001fb"'
        DagJson.encodeToString('tumour, "batch 2"\n') == '"tumour, \\"batch 2\\"\\n"'
    }

    def 'maps encode with keys sorted by UTF-8 bytes'() {
        expect:
        DagJson.encodeToString([z: 1, 'é': 2, B: 3, a: [b: 1, a: 2]]) == '{"B":3,"a":{"a":2,"b":1},"z":1,"é":2}'
    }

    def 'refused, with where (#text)'() {
        when:
        DagJson.decode(text)

        then:
        final DagJson.DagJsonException e = thrown()
        e.at == at

        where:
        text                                    | at
        '{"/":5}'                               | ''
        '{"a":{"/":"not a cid"}}'               | '/a'
        '{"a":{"/":"' + LINK + '","x":1}}'      | '/a'
        '{"a":{"/":{"bytes":"@@"}}}'            | '/a'
        '{"a":1,"a":2}'                         | '/a'
        '[1,2,'                                 | '/2'
        '{"a":[1,{"b":tru}]}'                   | '/a/1/b'
        '"\u0001"'                              | ''
        '01'                                    | ''
        'NaN'                                   | ''
        '1 2'                                   | ''
        ''                                      | ''
        '{"k~/":x}'                             | '/k~0~1'
    }

    def 'deep nesting is refused, not a stack overflow (Review Focus 4)'() {
        when:
        DagJson.decode('[' * 10_000 + ']' * 10_000)

        then:
        final DagJson.DagJsonException e = thrown()
        e.message.contains('nested deeper than 64')
    }

    def 'bytes that are not UTF-8 are refused'() {
        when:
        DagJson.decode([0x22, 0xff, 0x22] as byte[])

        then:
        thrown(DagJson.DagJsonException)
    }

    def 'a "/" key and non-finite floats cannot be encoded'() {
        when:
        DagJson.encode(value)

        then:
        thrown(IllegalArgumentException)

        where:
        value << [['/': 'x'], Double.NaN, Double.POSITIVE_INFINITY]
    }
}
