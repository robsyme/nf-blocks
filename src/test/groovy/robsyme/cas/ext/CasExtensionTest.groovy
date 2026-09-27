package robsyme.cas.ext

import java.nio.file.Path

import nextflow.Channel
import nextflow.Global
import nextflow.Session
import nextflow.extension.CH
import nextflow.util.RecordMap
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.core.Anomalies
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunCompletion
import robsyme.cas.core.RunManifest
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.nio.CasFileSystemProvider
import robsyme.cas.nio.CasPath
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The factory is driven over a real store and index built in a temp dir: each
 * run's blocks are written with the record classes and ingested, then
 * {@code fromStore} resolves and emits real restored paths. DESIGN.md §13.
 */
class CasExtensionTest extends Specification {

    static final String NOW = '2026-09-03T00:00:00Z'

    @TempDir
    Path tempDir

    Session session
    CasSession cas
    CasExtension ext
    Path indexFile
    Closure igniter

    def setupSpec() {
        // restore() resolves cas://<cid>/<name> through CasPlugin.provider(); in
        // this test JVM the reflective installer is unavailable, so prime the
        // provider cache directly (see CasLinStoreTest for the same seam).
        final field = CasPlugin.getDeclaredField('provider')
        field.setAccessible(true)
        field.set(null, new CasFileSystemProvider())
    }

    def setup() {
        indexFile = tempDir.resolve('index.sqlite')
        final cfg = [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                resolve: ['lab'],
                asserted_by: 'test',
                index: [path: indexFile.toString()],
            ],
        ]
        session = Mock(Session) {
            getConfig() >> cfg
            addIgniter(_) >> { Closure c -> igniter = c }
        }
        Global.session = session
        cas = CasSession.of(session)
        ext = new CasExtension()
        ext.init(session)
    }

    def cleanup() {
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private static Cid rawCid(int fill) {
        final byte[] digest = new byte[32]
        Arrays.fill(digest, (byte) fill)
        return Cid.of(Cid.RAW, digest)
    }

    /** Writes a manifest, items, collection and (optionally) a completion; ingests it. */
    private Map storeRun(Map args) {
        final Cid manifest = cas.store.putDagCbor(new RunManifest([
            assertedBy     : 'test',
            pipeline       : args.pipeline,
            runName        : 'r',
            nfRunHash      : args.nfHash,
            sessionId      : 's',
            nextflowVersion: '26.04.6',
            params         : [:],
            config         : [:],
            startedAt      : NOW,
        ]).toCbor())

        final List<Cid> itemCids = []
        final List<List<String>> paths = []
        (args.items as List<Map>).each { Map spec ->
            final Leaf leaf = spec.reason
                ? Leaf.without(spec.name as String, spec.reason as String)
                : Leaf.of(spec.name as String, spec.content as Cid, spec.size as Long, 'head-node')
            // `shape`, when given, builds the item's whole value around its leaf.
            final OutputItem item = OutputItem.of(spec.shape ? ((Closure) spec.shape).call(leaf) : [spec.meta, leaf])
            itemCids << cas.store.putDagCbor(item.toCbor())
            paths << [spec.path as String]
        }
        final Cid collection = cas.store.putDagCbor(
            new OutputCollection('test', manifest, args.output as String, itemCids, paths).toCbor())

        if( !args.withCompletion )
            return [manifest: manifest]

        final Cid completion = cas.store.putDagCbor(new RunCompletion([
            assertedBy        : 'test',
            run               : manifest,
            collections       : [collection],
            status            : RunCompletion.SUCCEEDED,
            possiblyIncomplete: false,
            startedAt         : NOW,
            finishedAt        : NOW,
            anomalies         : Anomalies.NONE,
        ]).toCbor())
        final Index index = Index.open(indexFile)
        try {
            index.ingestRun(cas.store, completion, 'lab')
        }
        finally {
            index.close()
        }
        return [manifest: manifest, completion: completion]
    }

    private Map alignedRun(String pipeline = 'p', String nfHash = 'nfhash1') {
        return storeRun(
            pipeline: pipeline, nfHash: nfHash, output: 'aligned', withCompletion: true,
            items: [
                [meta: [sample: 'A'], name: 'A.bam', content: rawCid(1), size: 10L, path: 'aligned/A/A.bam'],
                [meta: [sample: 'B'], name: 'B.bam', content: rawCid(2), size: 20L, path: 'aligned/B/B.bam'],
                [meta: [sample: 'C'], name: 'C.bam', content: rawCid(3), size: 30L, path: 'aligned/C/C.bam'],
            ])
    }

    /**
     * Attaches a read subscriber, then runs the captured igniter so the
     * broadcast delivers -- mirroring how Nextflow wires the DAG then ignites.
     */
    private List drain(channel) {
        final rc = CH.getReadChannel(channel)
        igniter.call()
        final List out = []
        Object v
        while( (v = rc.val) != Channel.STOP )
            out << v
        return out
    }

    def "run:'latest' with a where predicate emits the one matching item as a cas path"() {
        given:
        alignedRun('p', 'nfhash1')

        when:
        final channel = ext.fromStore(run: 'latest', pipeline: 'p', output: 'aligned', where: [sample: 'B'])
        final items = drain(channel)

        then:
        items.size() == 1
        final tuple = items[0] as List
        tuple[0] == [sample: 'B']
        tuple[1] instanceof CasPath
        tuple[1].toString() == "cas://${rawCid(2)}/B.bam".toString()
    }

    def 'a RunCompletion store uri resolves and emits every item of the output'() {
        given:
        final run = alignedRun()

        when:
        final channel = ext.fromStore(run: "cas://${run.completion}".toString(), output: 'aligned')
        final items = drain(channel)

        then:
        items.size() == 3
    }

    def 'a lid run hash resolves to the run'() {
        given:
        alignedRun('p', 'nfhashX')

        when:
        final channel = ext.fromStore(run: 'lid://nfhashX', output: 'aligned', where: [sample: 'A'])
        final items = drain(channel)

        then:
        items.size() == 1
        (items[0] as List)[1].toString() == "cas://${rawCid(1)}/A.bam".toString()
    }

    def 'a run manifest with no RunCompletion is an error'() {
        given:
        final run = storeRun(
            pipeline: 'p', nfHash: 'nfhashNC', output: 'aligned', withCompletion: false,
            items: [[meta: [sample: 'A'], name: 'A.bam', content: rawCid(1), size: 10L, path: 'aligned/A/A.bam']])

        when:
        ext.fromStore(run: "cas://${run.manifest}".toString(), output: 'aligned')

        then:
        thrown(IllegalStateException)
    }

    def 'an unaddressed leaf is an error naming the item'() {
        given:
        storeRun(
            pipeline: 'p', nfHash: 'nfhashU', output: 'reports', withCompletion: true,
            items: [[meta: [sample: 'A'], name: 'A.report', reason: Leaf.UNADDRESSED, path: 'reports/A/A.report']])

        when:
        final channel = ext.fromStore(run: 'lid://nfhashU', output: 'reports')
        drain(channel)

        then:
        thrown(IllegalStateException)
    }

    def 'a declined leaf emits null in place'() {
        given:
        storeRun(
            pipeline: 'p', nfHash: 'nfhashD', output: 'maybe', withCompletion: true,
            items: [[meta: [sample: 'A'], name: null, reason: Leaf.DECLINED, path: null]])

        when:
        final channel = ext.fromStore(run: 'lid://nfhashD', output: 'maybe')
        final items = drain(channel)

        then:
        items.size() == 1
        (items[0] as List)[1] == null
    }

    def "fromStore(run: 'latest') without a pipeline is refused by the shared resolver"() {
        given:
        alignedRun()

        when:
        ext.fromStore(run: 'latest', output: 'aligned')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains("run reference 'latest' needs a pipeline")
    }

    // -------------------------------------------------------- fromStore(selection:)

    private Cid itemOf(Cid completion, String sample) {
        final Index index = Index.open(indexFile)
        try {
            return index.items(completion, 'aligned', [sample: sample])[0]
        }
        finally {
            index.close()
        }
    }

    private Cid alignedCollectionOf(Cid completion) {
        final Index index = Index.open(indexFile)
        try {
            return index.collectionsOf(completion).aligned
        }
        finally {
            index.close()
        }
    }

    private Cid storeSelection(List<Selection.Member> members, boolean logged = true) {
        final Cid cid = cas.store.putDagCbor(new Selection('test', members, []).toCbor())
        if( logged )
            StoreLog.append(cas.store, StoreLogKind.SELECTION, cid, System.currentTimeMillis())
        return cid
    }

    def 'fromStore(selection:) flattens nesting and emits each item once, restored (spec section 10)'() {
        given:
        final Map run = alignedRun()
        final Cid coll = alignedCollectionOf((Cid) run.completion)
        final Cid a = itemOf((Cid) run.completion, 'A'), b = itemOf((Cid) run.completion, 'B'), c = itemOf((Cid) run.completion, 'C')
        final Cid inner = storeSelection([Selection.item(a, [coll]), Selection.item(b, [coll])])
        final Cid outer = storeSelection([Selection.selection(inner), Selection.item(b, []), Selection.item(c, [coll])])

        when:
        final List items = drain(ext.fromStore(selection: "cas://${outer}".toString()))

        then: 'A, B and C once each, in item-CID order'
        final Map<Cid, Map> metaOf = [(a): [sample: 'A'], (b): [sample: 'B'], (c): [sample: 'C']]
        items.collect { (it as List)[0] } == [a, b, c].sort { it.toString() }.collect { metaOf[it] }
        items.every { (it as List)[1] instanceof CasPath }
    }

    def 'a bare cid works, and a Selection whose block was never logged is read too'() {
        given:
        final Map run = alignedRun()
        final Cid a = itemOf((Cid) run.completion, 'A')
        final Cid s = storeSelection([Selection.item(a, [])], false)

        expect:
        drain(ext.fromStore(selection: s.toString())).size() == 1
    }

    def 'a deleted Selection still emits its items (spec section 10: with a warning)'() {
        given:
        final Map run = alignedRun()
        final Cid s = storeSelection([Selection.item(itemOf((Cid) run.completion, 'A'), [])])
        final Cid delete = cas.store.putDagCbor(Fixtures.claim(s, 'delete', null, null, []))
        StoreLog.append(cas.store, StoreLogKind.CLAIM, delete, System.currentTimeMillis())

        expect:
        drain(ext.fromStore(selection: s.toString())).size() == 1
    }

    def 'a nested Selection the composition does not hold fails at the call, naming it (Review Focus 5)'() {
        given:
        final Map run = alignedRun()
        final Cid absent = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid s = storeSelection([Selection.selection(absent), Selection.item(itemOf((Cid) run.completion, 'A'), [])])

        when:
        ext.fromStore(selection: s.toString())

        then:
        final IllegalStateException e = thrown()
        e.message.contains(absent.toString())
    }

    def 'selection refuses run, output, where and pipeline beside it, and an address that is not a Selection'() {
        given:
        final Map run = alignedRun()

        when:
        ext.fromStore(opts)

        then:
        thrown(IllegalArgumentException)

        where:
        opts << [
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', output: 'aligned'],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', run: 'latest'],
            [selection: 'not a cid'],
        ]
    }

    def 'a RunCompletion address is not a Selection'() {
        given:
        final Map run = alignedRun()

        when:
        ext.fromStore(selection: run.completion.toString())

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('not a Selection')
    }

    def 'fromStore with none of selection, run or output says what it takes (ticket 07 Q3): #opts'() {
        when:
        ext.fromStore(opts)

        then:
        final IllegalArgumentException e = thrown()
        e.message == "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output: <name>`; optionally `where: [...]` and `records: true`."
        e.message == CasExtension.USAGE

        where:
        opts << [null, [:], [where: [sample: 'A']], [pipeline: 'p'], [records: true]]
    }

    def 'a run with no output, and an output with no run, keep their own messages'() {
        when:
        ext.fromStore(run: 'latest', pipeline: 'p')

        then:
        final IllegalArgumentException noOutput = thrown()
        noOutput.message.contains("'output'")

        when:
        ext.fromStore(output: 'aligned')

        then:
        final IllegalArgumentException noRun = thrown()
        noRun.message.contains("'run'")
    }

    // ------------------------------------------------------------ records: true

    /** One item whose value is a map holding a nested map, a list of maps and a Leaf inside a list. */
    private Map peopleRun() {
        return storeRun(
            pipeline: 'p', nfHash: 'nfhashP', output: 'people', withCompletion: true,
            items: [[name: 'A.bam', content: rawCid(4), size: 40L, path: 'people/A/A.bam',
                     shape: { Leaf bam -> [id: 'A', person: [name: 'Ada', langs: [[lang: 'en'], [lang: 'fr']]], files: [bam]] }]])
    }

    private Cid peopleItemOf(Cid completion) {
        final Index index = Index.open(indexFile)
        try {
            return index.items(completion, 'people', [:])[0]
        }
        finally {
            index.close()
        }
    }

    private static void assertRecordsThroughout(Map item) {
        assert item instanceof RecordMap
        assert item.person instanceof RecordMap
        assert (item.person as Map).langs instanceof List
        assert ((item.person as Map).langs as List).every { it instanceof RecordMap }
        assert item.files instanceof List
        assert (item.files as List)[0] instanceof CasPath
        assert (item.files as List)[0].toString() == "cas://${rawCid(4)}/A.bam".toString()
        assert item == [id: 'A', person: [name: 'Ada', langs: [[lang: 'en'], [lang: 'fr']]], files: [(item.files as List)[0]]]
    }

    def 'records: true restores every non-Leaf map as a RecordMap at any depth; leaves are still paths'() {
        given:
        peopleRun()

        when:
        final List items = drain(ext.fromStore(run: 'lid://nfhashP', output: 'people', records: true))

        then:
        items.size() == 1
        assertRecordsThroughout(items[0] as Map)
    }

    def 'records: true on a tuple item: the list stays a list, the map in it is a RecordMap'() {
        given:
        alignedRun('p', 'nfhashT')

        when:
        final List items = drain(ext.fromStore(run: 'lid://nfhashT', output: 'aligned', where: [sample: 'B'], records: true))
        final List tuple = items[0] as List

        then:
        items.size() == 1
        tuple.getClass() == ArrayList
        tuple[0] instanceof RecordMap
        tuple[0] == [sample: 'B']
        tuple[1] instanceof CasPath
    }

    def 'records: true through selection: gives the same records'() {
        given:
        final Map run = peopleRun()
        final Cid s = storeSelection([Selection.item(peopleItemOf((Cid) run.completion), [])])

        when:
        final List items = drain(ext.fromStore(selection: s.toString(), records: true))

        then:
        items.size() == 1
        assertRecordsThroughout(items[0] as Map)
    }

    def 'a restored record is immutable: put throws UnsupportedOperationException'() {
        given:
        peopleRun()
        final Map item = drain(ext.fromStore(run: 'lid://nfhashP', output: 'people', records: true))[0] as Map

        when:
        item.put('x', 1)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'without records, or with records: false, maps are plain LinkedHashMaps on both entry points (#how)'() {
        given:
        final Map run = peopleRun()
        final Cid s = storeSelection([Selection.item(peopleItemOf((Cid) run.completion), [])])

        when:
        final Map viaRun = drain(ext.fromStore([run: 'lid://nfhashP', output: 'people'] + extra))[0] as Map
        final Map viaSelection = drain(ext.fromStore([selection: s.toString()] + extra))[0] as Map

        then:
        [viaRun, viaSelection].every { Map item ->
            item.getClass() == LinkedHashMap &&
                (item.person as Map).getClass() == LinkedHashMap &&
                ((item.person as Map).langs as List).every { it.getClass() == LinkedHashMap } &&
                (item.files as List)[0] instanceof CasPath
        }
        viaRun.put('x', 1) == null

        where:
        how               | extra
        'absent'          | [:]
        'records: false'  | [records: false]
    }

    def 'records refuses anything but true or false, before reading the store: #opts'() {
        when:
        ext.fromStore(opts)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('records')
        e.message.contains('true or false')

        where:
        opts << [
            [run: 'lid://no-such-run', output: 'aligned', records: 'true'],
            [run: 'lid://no-such-run', output: 'aligned', records: 1],
            [run: 'lid://no-such-run', output: 'aligned', records: null],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', records: 'yes'],
            [selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua', records: [true]],
        ]
    }
}
