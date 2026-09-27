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

    // ---- Leaf ----

    def 'a leaf without an address must say why'() {
        when:
        new Leaf('A.bam', null, null, null, null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'a leaf with an address has nothing to explain'() {
        when:
        new Leaf('A.bam', raw('a'), 1L, 'head-node', 'declined')

        then:
        thrown(IllegalArgumentException)
    }

    def 'a leaf reason and provider come from a closed set'() {
        when:
        Leaf.without('A.bam', 'lost')

        then:
        thrown(IllegalArgumentException)

        when:
        Leaf.of('A.bam', raw('a'), 1L, 'somewhere-else')

        then:
        thrown(IllegalArgumentException)
    }

    def 'a leaf round-trips, with an absent address never an absent field'() {
        given:
        final Leaf leaf = Leaf.of('A.bam', raw('a'), 12L, 'head-node')

        expect:
        leaf.toCbor() == [kind: 'Leaf', name: 'A.bam', address: raw('a'), size: 12L, provider: 'head-node', reason: null]
        Leaf.fromCbor(roundTrip(leaf.toCbor())) == leaf

        and:
        Leaf.declined().toCbor() == [kind: 'Leaf', name: null, address: null, size: null, provider: null, reason: 'declined']
        Leaf.fromCbor(roundTrip(Leaf.declined().toCbor())) == Leaf.declined()
        Leaf.fromCbor(roundTrip(Leaf.without('A.bam', 'never_published').toCbor())) == Leaf.without('A.bam', 'never_published')
    }

    // ---- OutputItem ----

    def 'an item keeps the shape of the channel item, with its files as leaves'() {
        given:
        final Leaf leaf = Leaf.of('A.bam', raw('bam'), 3L, 'head-node')
        final OutputItem item = OutputItem.of([[sample: 'A', lane: 1L], leaf])

        when:
        final Map cbor = item.toCbor()

        then:
        Records.kindOf(cbor) == 'OutputItem'
        cbor.value instanceof List
        cbor.value[0] == [sample: 'A', lane: 1L]
        cbor.value[1] == leaf.toCbor()
        Records.kindOf((Map) cbor.value[1]) == 'Leaf'

        and:
        OutputItem.fromCbor(roundTrip(cbor)) == item
        OutputItem.fromCbor(roundTrip(cbor)).value[1] instanceof Leaf
    }

    def 'a meta map that happens to carry a Leaf kind is not a leaf'() {
        given: 'the caller marked no leaf here, so there is none'
        final OutputItem item = OutputItem.of([[kind: 'Leaf', id: 'A'], Leaf.declined()])

        when:
        final Map cbor = item.toCbor()

        then:
        cbor.value[0] == [kind: 'Leaf', id: 'A']

        and: 'and it survives decoding as the map it is'
        final OutputItem back = OutputItem.fromCbor(roundTrip(cbor))
        back.value[0] == [kind: 'Leaf', id: 'A']
        !(back.value[0] instanceof Leaf)
        back.value[1] instanceof Leaf
        back.leaves() == [Leaf.declined()]
    }

    def 'leaves come out in the order a reader meets them'() {
        given:
        final Leaf first = Leaf.of('A_1.fq', raw('1'), 1L, 'head-node')
        final Leaf second = Leaf.of('A_2.fq', raw('2'), 1L, 'head-node')
        final Leaf third = Leaf.declined()

        expect:
        OutputItem.of([[id: 'A'], [first, second], [report: third]]).leaves() == [first, second, third]
    }

    def 'two items with the same metadata over the same content are one block'() {
        given: 'two runs, each building its own item'
        final OutputItem one = OutputItem.of([[sample: 'A', lane: 1L], Leaf.of('A.bam', raw('bam'), 3L, 'head-node')])
        final OutputItem two = OutputItem.of([[sample: 'A', lane: 1L], Leaf.of('A.bam', raw('bam'), 3L, 'head-node')])

        expect:
        dag(one.toCbor()) == dag(two.toCbor())

        and: 'while different metadata over the same content is a different item'
        dag(OutputItem.of([[sample: 'B', lane: 1L], Leaf.of('A.bam', raw('bam'), 3L, 'head-node')]).toCbor()) != dag(one.toCbor())
    }

    def 'an item carries no asserted_by, so it deduplicates across runs'() {
        expect:
        !OutputItem.of([[sample: 'A'], Leaf.declined()]).toCbor().containsKey('asserted_by')
    }

    // ---- OutputCollection ----

    def 'items are sorted by address and the publish paths follow them'() {
        given:
        final Cid runCid = dag([kind: 'RunManifest'])
        final List<Cid> items = [dag([n: 3]), dag([n: 1]), dag([n: 2])]
        final List<List<String>> paths = [['aligned/three.bam'], ['aligned/one.bam'], ['aligned/two.bam']]
        final Map<Cid, List<String>> expected = [(items[0]): paths[0], (items[1]): paths[1], (items[2]): paths[2]]

        when:
        final OutputCollection collection = new OutputCollection('gate', runCid, 'aligned', items, paths)

        then:
        collection.items == items.toSorted { it.toString() }

        and: 'each item still has its own paths'
        collection.items.eachWithIndex { Cid cid, int i -> assert collection.paths[i] == expected[cid] }

        and:
        collection.toCbor().asserted_by == 'gate'
        OutputCollection.fromCbor(roundTrip(collection.toCbor())) == collection
    }

    def 'items sharing a cid break the tie by their path list, not by arrival'() {
        given: 'the same content published twice under different paths, in two orders'
        final Cid shared = dag([n: 1])
        final List<List<String>> earlyFirst = [['aligned/a.bam'], ['aligned/b.bam']]
        final List<List<String>> lateFirst = [['aligned/b.bam'], ['aligned/a.bam']]

        when:
        final OutputCollection one = new OutputCollection('gate', dag([:]), 'aligned', [shared, shared], earlyFirst)
        final OutputCollection two = new OutputCollection('gate', dag([:]), 'aligned', [shared, shared], lateFirst)

        then: 'both settle on the same order, so the collection address is reproducible'
        one.paths == [['aligned/a.bam'], ['aligned/b.bam']]
        two.paths == [['aligned/a.bam'], ['aligned/b.bam']]
        dag(one.toCbor()) == dag(two.toCbor())
    }

    def 'a hole keeps its place in the sort rather than being compacted away'() {
        given:
        final OutputCollection collection = new OutputCollection(
            'gate', dag([kind: 'RunManifest']), 'aligned', [dag([n: 1]), null], [['aligned/one.bam'], null])

        expect:
        collection.items.size() == 2
        collection.items[0] == null
        collection.paths[0] == null
        OutputCollection.fromCbor(roundTrip(collection.toCbor())) == collection
    }

    def 'a collection needs one path list per item'() {
        when:
        new OutputCollection('gate', dag([kind: 'RunManifest']), 'aligned', [dag([n: 1])], [])

        then:
        thrown(IllegalArgumentException)
    }

    def 'a collection needs its run, its name and an asserted_by'() {
        when:
        new OutputCollection(assertedBy, run, name, [], [])

        then:
        thrown(IllegalArgumentException)

        where:
        assertedBy | run                        | name
        null       | dag([kind: 'RunManifest']) | 'aligned'
        'gate'     | null                       | 'aligned'
        'gate'     | dag([kind: 'RunManifest']) | null
    }

    // ---- RunManifest ----

    static Map manifestArgs() {
        return [
            assertedBy     : 'gate',
            pipeline       : 'test-pipeline',
            repository     : null,
            revision       : null,
            commitId       : null,
            runName        : 'cheeky_curie',
            nfRunHash      : 'a1b2c3',
            sessionId      : '9c1e-…',
            resumed        : false,
            nextflowVersion: '26.04.6',
            params         : [genome: '/data/genome.fa', depth: 30L],
            config         : "workDir = '/scratch'\nprocess {\n    cpus = 2\n}\n",
            script         : null,
            startedAt      : '2026-09-03T12:00:00.000Z',
        ]
    }

    def 'a run manifest round-trips and scrubs what it was handed'() {
        given:
        final RunManifest manifest = new RunManifest(manifestArgs())

        expect: 'the scrub happened here, so a caller cannot forget it'
        manifest.params == [genome: '[redacted-location]', depth: 30L]
        manifest.config == "workDir = '[redacted-location]'\nprocess {\n    cpus = 2\n}\n"

        and:
        manifest.toCbor().asserted_by == 'gate'
        manifest.toCbor().nf_run_hash == 'a1b2c3'
        manifest.toCbor().started_at == '2026-09-03T12:00:00.000Z'
        manifest.toCbor().resumed == false

        and:
        RunManifest.fromCbor(roundTrip(manifest.toCbor())) == manifest
        dag(RunManifest.fromCbor(roundTrip(manifest.toCbor())).toCbor()) == dag(manifest.toCbor())
    }

    def 'a run manifest written before config became text still decodes'() {
        given: 'an old block, config held as a map'
        final Map<String, Object> old = new RunManifest(manifestArgs()).toCbor()
        old.put('config', [process: [cpus: 2L]])

        when:
        final RunManifest decoded = RunManifest.fromCbor(roundTrip(old))

        then:
        decoded.config == [process: [cpus: 2L]]
        dag(decoded.toCbor()) == dag(old)
    }

    def 'a run manifest config that is neither text nor a map is refused'() {
        when:
        new RunManifest(manifestArgs() + [config: 7])

        then:
        thrown(IllegalArgumentException)
    }

    def 'a run manifest needs the facts that identify the run'() {
        when:
        new RunManifest(manifestArgs() + [(field): null])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(field)

        where:
        field << ['assertedBy', 'pipeline', 'runName', 'nfRunHash', 'sessionId', 'nextflowVersion', 'startedAt']
    }

    // ---- RunCompletion ----

    static Map completionArgs() {
        return [
            assertedBy        : 'gate',
            run               : dag([kind: 'RunManifest']),
            collections       : [dag([kind: 'OutputCollection', name: 'aligned'])],
            inputSet          : null,
            status            : 'succeeded',
            exitStatus        : 0,
            possiblyIncomplete: false,
            startedAt         : '2026-09-03T12:00:00.000Z',
            finishedAt        : '2026-09-03T12:01:00.000Z',
            anomalies         : new Anomalies(1, 0, 2, 0),
            error             : null,
        ]
    }

    def 'a run completion round-trips with its anomaly counters'() {
        given:
        final RunCompletion completion = new RunCompletion(completionArgs())

        expect:
        completion.toCbor().asserted_by == 'gate'
        completion.toCbor().anomalies == [unresolvable: 1L, unaddressed: 0L, declined: 2L, never_published: 0L]
        completion.toCbor().possibly_incomplete == false
        completion.toCbor().exit_status == 0L
        completion.isSuccessful()

        and:
        RunCompletion.fromCbor(roundTrip(completion.toCbor())) == completion
        dag(RunCompletion.fromCbor(roundTrip(completion.toCbor())).toCbor()) == dag(completion.toCbor())
    }

    def 'a possibly incomplete run is not a successful one'() {
        expect:
        !new RunCompletion(completionArgs() + [possiblyIncomplete: true]).isSuccessful()
        !new RunCompletion(completionArgs() + [status: 'failed', exitStatus: 1, error: 'boom']).isSuccessful()
    }

    def 'a run completion needs its anomaly counters'() {
        when:
        new RunCompletion(completionArgs() + [anomalies: null])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('anomal')
    }

    def 'a run completion status comes from a closed set'() {
        when:
        new RunCompletion(completionArgs() + [status: 'partly'])

        then:
        thrown(IllegalArgumentException)
    }

    // ---- kind ----

    def 'the kind of a block is read from its kind field'() {
        expect:
        Records.kindOf([kind: 'RunCompletion', schema: 1L]) == 'RunCompletion'
        Records.kindOf([schema: 1L]) == null
        Records.kindOf(null) == null
        Records.kindOf([kind: 7L]) == null
    }

    def 'only the three run kinds carry an asserted_by'() {
        expect:
        new DirectoryManifest([]).toCbor().containsKey('asserted_by') == false
        OutputItem.of('x').toCbor().containsKey('asserted_by') == false
        new OutputCollection('gate', dag([:]), 'aligned', [], []).toCbor().containsKey('asserted_by')
        new RunManifest(manifestArgs()).toCbor().containsKey('asserted_by')
        new RunCompletion(completionArgs()).toCbor().containsKey('asserted_by')
    }
}
