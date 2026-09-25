package robsyme.cas.explore

import spock.lang.Specification

class ByteRangeTest extends Specification {

    def 'single ranges parse (#header)'() {
        when:
        final ByteRange range = ByteRange.parse(header, 1000)

        then:
        [range.start, range.end, range.length] == [start, end, end - start + 1]

        where:
        header          | start | end
        'bytes=0-99'    | 0     | 99
        'bytes=900-'    | 900   | 999
        'bytes=-100'    | 900   | 999
        'bytes=990-2000'| 990   | 999
        'bytes=-5000'   | 0     | 999
    }

    def 'no header, or a form this server does not honour, means the whole file (#header)'() {
        expect:
        ByteRange.parse(header, 1000) == null

        where:
        header << [null, '', 'bytes=0-1,5-6', 'items=0-1', 'bytes=-', 'bytes=50-10', 'bytes=99999999999999999999-']
    }

    def 'a range starting past the end is unsatisfiable (#header)'() {
        when:
        ByteRange.parse(header, 1000)

        then:
        thrown(ByteRange.Unsatisfiable)

        where:
        header << ['bytes=1000-', 'bytes=5000-6000', 'bytes=-0']
    }
}
