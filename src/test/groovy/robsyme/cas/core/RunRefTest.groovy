package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Decision 6 of the milestone 3 plan: the one run-reference resolver behind fromStore(run:) and nf-blocks:items --run. */
class RunRefTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
    }

    def cleanup() {
        index?.close()
    }

    /** One run of pipeline `p` with an empty `aligned` output; ingested unless {@code complete} is false. */
    private Map run(String hash, String finishedAt, boolean complete = true) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(nf_run_hash: hash, run_name: "run-${hash}".toString()))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', []))
        if( !complete )
            return [manifest: manifest, collection: collection]
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection], [finished_at: finishedAt]))
        index.ingestRun(store, completion, 'lab')
        return [manifest: manifest, collection: collection, completion: completion]
    }

    def 'latest with a pipeline is the most recent successful run; lid:// and both cas:// kinds name one run'() {
        given:
        final Map older = run('h1', '2026-09-03T10:05:00.000Z')
        final Map newer = run('h2', '2026-09-04T10:05:00.000Z')

        expect:
        RunRef.resolve(index, store, 'latest', 'p') == newer.completion
        RunRef.resolve(index, store, 'lid://h1', null) == older.completion
        RunRef.resolve(index, store, "cas://${older.completion}".toString(), null) == older.completion
        RunRef.resolve(index, store, "cas://${newer.manifest}".toString(), null) == newer.completion
    }

    def 'a malformed reference is an IllegalArgumentException that says run reference'() {
        when:
        RunRef.resolve(index, store, ref, pipeline)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('run reference')
        !e.message.contains('channel.fromStore')

        where:
        ref               | pipeline
        null              | null
        ''                | null
        'latest'          | null
        'lid://'          | null
        'bogus'           | null
        'cas://not-a-cid' | null
    }

    def 'a cas:// reference to a block that is not a run is an IllegalArgumentException naming its kind'() {
        given:
        final Map r = run('h1', '2026-09-03T10:05:00.000Z')

        when:
        RunRef.resolve(index, store, "cas://${r.collection}".toString(), null)

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('OutputCollection')
        e.message.contains('run reference')
    }

    def 'a reference the index or the composition cannot resolve is an IllegalStateException that says run reference'() {
        given:
        run('h1', '2026-09-03T10:05:00.000Z')
        final Map unfinished = run('h3', null, false)
        final Cid absent = Fixtures.cidOf([kind: 'RunCompletion', n: 1])

        when:
        RunRef.resolve(index, store, ref.call(unfinished, absent) as String, pipeline)

        then:
        final IllegalStateException e = thrown()
        e.message.contains('run reference')
        !e.message.contains('channel.fromStore')
        e.message.contains(named.call(unfinished, absent) as String)

        where:
        ref                                            | pipeline | named
        ({ Map u, Cid a -> 'latest' })                  | 'nobody' | ({ Map u, Cid a -> 'nobody' })
        ({ Map u, Cid a -> 'lid://nope' })               | null     | ({ Map u, Cid a -> 'nope' })
        ({ Map u, Cid a -> "cas://${u.manifest}" })      | null     | ({ Map u, Cid a -> 'did not finish' })
        ({ Map u, Cid a -> "cas://${a}" })               | null     | ({ Map u, Cid a -> a.toString() })
    }
}
