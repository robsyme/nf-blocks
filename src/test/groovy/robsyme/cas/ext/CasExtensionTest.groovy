package robsyme.cas.ext

import java.nio.file.Path

import nextflow.Channel
import nextflow.Global
import nextflow.Session
import nextflow.extension.CH
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.core.Anomalies
import robsyme.cas.core.Cid
import robsyme.cas.core.Index
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunCompletion
import robsyme.cas.core.RunManifest
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
            final OutputItem item = OutputItem.of([spec.meta, leaf])
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
}
