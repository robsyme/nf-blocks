package robsyme.cas.core

import java.nio.file.Path
import java.nio.file.Paths

import spock.lang.Specification

/** DESIGN.md §7: the join key. One canonical form for every publish target. */
class CoordinatesTest extends Specification {

    def 'dot and dot-dot segments are resolved'() {
        expect:
        Coordinates.key('cas://lab/a/./b/../c.txt') == 'cas://lab/a/c.txt'
    }

    def 'a trailing slash is dropped'() {
        expect:
        Coordinates.key('cas://lab/aligned/') == 'cas://lab/aligned'
        Coordinates.key('cas://lab/') == 'cas://lab'
        Coordinates.key('cas://lab') == 'cas://lab'
    }

    def 'duplicate slashes collapse'() {
        expect:
        Coordinates.key('cas://lab//aligned///A//A.bam') == 'cas://lab/aligned/A/A.bam'
    }

    def 'a leading dot-dot cannot escape the store root'() {
        expect:
        Coordinates.key('cas://lab/../../a.txt') == 'cas://lab/a.txt'
    }

    def 'a Store URI is not a coordinate'() {
        given: 'a real dag-cbor cid, the address of the empty map'
        final String cid = DagCbor.cidOf(DagCbor.encode([:])).toString()

        when:
        Coordinates.key("cas://$cid/A.bam".toString())

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(cid)
    }

    def 'a non-cas uri is not a coordinate'() {
        when:
        Coordinates.key('file:///data/a.txt')

        then:
        thrown(IllegalArgumentException)
    }

    def 'a uri argument gives the same key as its string'() {
        given:
        final URI uri = URI.create('cas://lab/a/./b/../c.txt')

        expect:
        Coordinates.key(uri) == Coordinates.key('cas://lab/a/./b/../c.txt')
    }

    def 'segments keep the characters java.net.URI would percent-encode'() {
        given: 'a name with a space and a hash, as Nextflow file names may have'
        final URI escaped = new URI('cas', 'lab', '/aligned/a b#c.txt', null)

        expect: 'the escaped uri really is escaped'
        escaped.toString() == 'cas://lab/aligned/a%20b%23c.txt'

        and: 'yet the key carries the raw name, from either form'
        Coordinates.key(escaped) == 'cas://lab/aligned/a b#c.txt'
        Coordinates.key('cas://lab/aligned/a b#c.txt') == 'cas://lab/aligned/a b#c.txt'
    }

    def 'a path is accepted through its uri'() {
        given:
        final Path path = Paths.get('/data/a.txt')

        when: 'a path outside the cas scheme'
        Coordinates.key(path)

        then:
        thrown(IllegalArgumentException)
    }

    def 'an empty or null argument is rejected'() {
        when:
        Coordinates.key((String) text)

        then:
        thrown(IllegalArgumentException)

        where:
        text << [null, '', 'lab/a.txt', 'cas://']
    }
}
