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

    // ---- StoreRef: the text a Pointer File holds (DESIGN.md §7) ----

    static Cid rawCid() { Hashing.hashRaw(new ByteArrayInputStream('hello\n'.bytes), new byte[1024]) }

    static Cid manifestCid() { DagCbor.cidOf(DagCbor.encode([kind: 'DirectoryManifest', schema: 1, entries: []])) }

    def 'a raw store reference stringifies as cas://cid/name'() {
        given:
        final Cid cid = rawCid()

        expect:
        new StoreRef(cid, 'A.bam').toString() == "cas://$cid/A.bam"
    }

    def 'a raw store reference must carry the name it was published under'() {
        when:
        new StoreRef(rawCid(), null)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('name')
    }

    def 'a directory reference may go without a name'() {
        given:
        final Cid cid = manifestCid()

        expect:
        new StoreRef(cid, null).toString() == "cas://$cid"
        new StoreRef(cid, 'qc').toString() == "cas://$cid/qc"
    }

    def 'a store reference round-trips through its text form'() {
        given:
        final StoreRef ref = new StoreRef(cid, name)

        expect:
        StoreRef.parse(ref.toString()) == ref
        StoreRef.parse(ref.toString()).cid == cid
        StoreRef.parse(ref.toString()).name == name

        where:
        cid           | name
        rawCid()      | 'A.bam'
        rawCid()      | 'a b#c.txt'
        manifestCid() | 'qc'
        manifestCid() | null
    }

    def 'parsing rejects text that is not a store uri'() {
        when:
        StoreRef.parse(text)

        then:
        thrown(IllegalArgumentException)

        where:
        text << [
            null,
            '',
            'cas://lab/A.bam',                        // an alias, not a content address
            "cas://${rawCid()}/qc/A.bam".toString(),  // more than one segment
            "cas://${rawCid()}".toString(),           // a raw block with no name
            "file://${rawCid()}/A.bam".toString(),
        ]
    }

    def 'a store reference knows whether it addresses a directory'() {
        expect:
        !new StoreRef(rawCid(), 'A.bam').isDirectory()
        new StoreRef(manifestCid(), 'qc').isDirectory()
    }
}
