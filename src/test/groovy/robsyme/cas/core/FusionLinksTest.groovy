package robsyme.cas.core

import spock.lang.Specification

class FusionLinksTest extends Specification {

    def 'a listing is names separated by newlines, with or without a trailing newline'() {
        expect:
        FusionLinks.parse('rel.txt\ndangling.txt\ndirlink'.bytes).names == ['rel.txt', 'dangling.txt', 'dirlink'] as Set
        FusionLinks.parse('up.txt\n'.bytes).names == ['up.txt'] as Set
        FusionLinks.parse(new byte[0]).ok
        FusionLinks.parse('name with space.txt'.bytes).names == ['name with space.txt'] as Set
    }

    def 'an unparseable listing says why (#why)'() {
        expect:
        !FusionLinks.parse(body).ok
        FusionLinks.parse(body).problem.contains(why)

        where:
        why            | body
        'NUL'          | 'a\u0000b'.bytes
        'UTF-8'        | [0x61, 0xff, 0x62] as byte[]
        'empty name'   | 'a\n\nb'.bytes
        'path segment' | 'a/b'.bytes
        'path segment' | '..'.bytes
        'bytes'        | new byte[FusionLinks.MAX_SIDECAR_BYTES + 1]
    }

    def 'a link body is a target only when short, UTF-8 and free of NUL'() {
        expect:
        FusionLinks.target('../target.txt'.bytes) == '../target.txt'
        FusionLinks.target(('x' * 4097).bytes) == null
        FusionLinks.target('a\u0000'.bytes) == null
        FusionLinks.target(new byte[0]) == null
    }
}
