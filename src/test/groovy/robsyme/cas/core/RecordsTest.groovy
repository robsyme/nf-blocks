package robsyme.cas.core

import spock.lang.Specification

/** DESIGN.md §6: the block kinds, their fields and what identity they carry. */
class RecordsTest extends Specification {

    static Cid raw(String text) {
        Hashing.hashRaw(new ByteArrayInputStream(text.getBytes('UTF-8')), new byte[1024])
    }

    static Cid dag(Object value) {
        DagCbor.cidOf(DagCbor.encode(value))
    }

    /** Encodes and decodes a record's map exactly as the store would. */
    static Map roundTrip(Map cbor) {
        return (Map) DagCbor.decode(DagCbor.encode(cbor))
    }

    // ---- DirectoryManifest ----

    def 'a manifest entry carries name, mode, size, address and target'() {
        given:
        final Cid address = raw('one')
        final ManifestEntry entry = ManifestEntry.regular('a.txt', address, 3L)

        expect: 'every field is present, an absent address is never an absent key'
        entry.toCbor() == [name: 'a.txt', mode: 'regular', size: 3L, address: address, target: null]
    }

    def 'every mode has its own shape'() {
        expect:
        ManifestEntry.executable('run.sh', raw('x'), 1L).toCbor().mode == 'executable'
        ManifestEntry.directory('nested', dag([:])).toCbor().mode == 'directory'
        ManifestEntry.directory('nested', dag([:])).toCbor().size == 0L
        ManifestEntry.symlink('alias.txt', 'summary.txt').toCbor().mode == 'symlink'
        ManifestEntry.symlink('alias.txt', 'summary.txt').toCbor().target == 'summary.txt'
        ManifestEntry.symlink('alias.txt', 'summary.txt').toCbor().address == null
        ManifestEntry.unresolvable('gone.txt', '../nowhere').toCbor().mode == 'unresolvable'
    }

    def 'a symlink entry sizes its target text in bytes'() {
        expect:
        ManifestEntry.symlink('alias.txt', 'sü.txt').toCbor().size == 7L
    }

    def 'a regular entry needs a raw address'() {
        when:
        ManifestEntry.regular('a.txt', dag([:]), 3L)

        then:
        thrown(IllegalArgumentException)

        when:
        ManifestEntry.regular('a.txt', null, 3L)

        then:
        thrown(IllegalArgumentException)
    }

    def 'a directory entry needs a dag-cbor address'() {
        when:
        ManifestEntry.directory('nested', raw('one'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'a name is one path segment'() {
        when:
        ManifestEntry.regular(name, raw('one'), 1L)

        then:
        thrown(IllegalArgumentException)

        where:
        name << [null, '', 'a/b', '.', '..']
    }

    def 'a manifest sorts its entries by the utf-8 bytes of their names'() {
        given:
        final DirectoryManifest manifest = new DirectoryManifest([
            ManifestEntry.regular('b.txt', raw('b'), 1L),
            ManifestEntry.regular('a.txt', raw('a'), 1L),
            ManifestEntry.regular('Z.txt', raw('z'), 1L),
        ])

        expect:
        manifest.entries*.name == ['Z.txt', 'a.txt', 'b.txt']
    }

    def 'a manifest refuses two entries with the same name'() {
        when:
        new DirectoryManifest([
            ManifestEntry.regular('a.txt', raw('a'), 1L),
            ManifestEntry.regular('a.txt', raw('b'), 1L),
        ])

        then:
        thrown(IllegalArgumentException)
    }

    def 'a manifest round-trips through dag-cbor'() {
        given:
        final DirectoryManifest manifest = new DirectoryManifest([
            ManifestEntry.regular('a.txt', raw('a'), 1L),
            ManifestEntry.directory('nested', dag([kind: 'DirectoryManifest', schema: 1, entries: []])),
            ManifestEntry.symlink('alias.txt', 'a.txt'),
            ManifestEntry.unresolvable('gone.txt', '../nowhere'),
        ])

        when:
        final Map decoded = roundTrip(manifest.toCbor())

        then:
        Records.kindOf(decoded) == 'DirectoryManifest'
        decoded.schema == 1L
        DirectoryManifest.fromCbor(decoded).entries == manifest.entries

        and: 're-encoding a decoded manifest reproduces its address'
        dag(DirectoryManifest.fromCbor(decoded).toCbor()) == dag(manifest.toCbor())
    }

    def 'an empty manifest is a perfectly good block'() {
        expect:
        new DirectoryManifest([]).toCbor().entries == []
        DirectoryManifest.fromCbor(roundTrip(new DirectoryManifest([]).toCbor())).entries == []
    }

    def 'a manifest carries no asserted_by, so identical content is one block'() {
        expect:
        !new DirectoryManifest([]).toCbor().containsKey('asserted_by')
    }

    // ---- Anomalies ----

    def 'anomaly counters round-trip and add up'() {
        given:
        final Anomalies one = new Anomalies(1, 0, 2, 0)
        final Anomalies two = new Anomalies(0, 3, 0, 4)

        expect:
        one.toCbor() == [unresolvable: 1L, unaddressed: 0L, declined: 2L, never_published: 0L]
        Anomalies.fromCbor(roundTrip(one.toCbor())) == one
        one.plus(two) == new Anomalies(1, 3, 2, 4)
        Anomalies.NONE == new Anomalies(0, 0, 0, 0)
    }

    def 'every anomaly counter is required'() {
        when:
        Anomalies.fromCbor([unresolvable: 1L, unaddressed: 0L, declined: 2L])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('never_published')
    }
}
