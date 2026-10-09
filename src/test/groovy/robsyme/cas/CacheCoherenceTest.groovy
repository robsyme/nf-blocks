package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20: a member deleted and recreated at the same location. */
class CacheCoherenceTest extends Specification {

    @TempDir Path tmp

    private CasSession session() {
        new CasSession(CasConfig.from([
            cas: [
                stores: [lab: [location: tmp.resolve('store').toString()]],
                index : [path: tmp.resolve('cache.sqlite').toString()],
            ],
        ], 'cas://lab'))
    }

    private Cid logRun(String name) {
        final LocalBlockStore store = new LocalBlockStore(tmp.resolve('store'), 'lab', true)
        final Cid m = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(store, StoreLogKind.RUN, rc, System.currentTimeMillis())
        return rc
    }

    private List<String> runsIn(Index index) {
        final List<String> out = []
        for( String p : index.pipelines() )
            out.addAll(index.runsOf(p)*.completion*.toString())
        return out
    }

    def 'catching up creates the writable member\'s store-id and records it'() {
        given:
        logRun('r1')
        final CasSession cas = session()
        final Index index = cas.openIndex()

        when:
        cas.catchUpIndex(index)

        then:
        final String id = Files.readString(tmp.resolve('store/store-id')).trim()
        index.storeIdOf('lab') == id
        cas.writableStoreId() == id

        cleanup:
        index.close()
    }

    def 'a recreated member shows only its own runs'() {
        given: 'two runs in the first incarnation, caught up'
        logRun('old1'); logRun('old2')
        final CasSession first = session()
        Index index = first.openIndex()
        first.catchUpIndex(index)
        assert runsIn(index).size() == 2
        index.close()

        when: 'the member is deleted and one run written at the same location'
        tmp.resolve('store').toFile().deleteDir()
        final Cid fresh = logRun('new1')
        final CasSession cas = session()
        index = cas.openIndex()
        cas.catchUpIndex(index)

        then:
        runsIn(index) == [fresh.toString()]
        index.storeIdOf('lab') == Files.readString(tmp.resolve('store/store-id')).trim()

        cleanup:
        index?.close()
    }
}
