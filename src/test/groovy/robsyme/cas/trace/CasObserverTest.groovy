package robsyme.cas.trace

import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import nextflow.script.WorkflowMetadata
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The observer is driven directly over a real {@link CasSession} (a real store
 * and coordinate tree under a temp dir) and a mock Nextflow {@link Session}.
 * Every assertion reads a real block back out of the store, never a mock.
 * DESIGN.md §11.
 */
class CasObserverTest extends Specification {

    @TempDir
    Path tempDir

    Session session
    CasSession cas
    CasObserver observer
    Path indexFile

    private Map config(String outputDir = 'cas://lab') {
        indexFile = tempDir.resolve('index.sqlite')
        return [
            lineage: [store: [location: 'cas://lab']],
            outputDir: outputDir,
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                resolve: ['lab'],
                asserted_by: 'test',
                index: [path: indexFile.toString()],
            ],
        ]
    }

    /**
     * A metadata stub with the {@link java.time.OffsetDateTime} getters pinned
     * to null (OffsetDateTime cannot be mocked, so an auto-dummy would blow up)
     * -- the observer then falls back to "now" for the timestamps.
     */
    private WorkflowMetadata meta(Integer exitStatus = null) {
        return Stub(WorkflowMetadata) {
            getStart() >> null
            getComplete() >> null
            getManifest() >> null
            getNextflow() >> null
            getExitStatus() >> exitStatus
        }
    }

    private void bind(Map cfg, WorkflowMetadata metadata = meta()) {
        session = Mock(Session) {
            getConfig() >> cfg
            getRunName() >> 'test-run'
            getUniqueId() >> UUID.fromString('00000000-0000-0000-0000-0000000000ab')
            getWorkflowMetadata() >> metadata
        }
        Global.session = session
        cas = CasSession.of(session)
        observer = new CasObserver()
    }

    def cleanup() {
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private Map readBlock(Cid cid) {
        final InputStream input = cas.store.open(cid)
        try {
            return (Map) DagCbor.decode(input.readAllBytes())
        }
        finally {
            input.close()
        }
    }

    private List<Map> blocksOfKind(String kind) {
        final List<Map> found = new ArrayList<Map>()
        final stream = cas.store.listBlocks()
        try {
            for( Object element : stream.toList() ) {
                final Cid cid = (Cid) element
                if( !cid.isDagCbor() )
                    continue
                final Map block = readBlock(cid)
                if( block.get('kind') == kind )
                    found.add(block)
            }
        }
        finally {
            stream.close()
        }
        return found
    }

    def 'onFlowBegin writes a RunManifest with the nextflow run hash from the session'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()

        then:
        final manifests = blocksOfKind('RunManifest')
        manifests.size() == 1
        manifests[0].get('nf_run_hash') == 'nfhash123'
        manifests[0].get('asserted_by') == 'test'
        cas.getRunManifest() != null
    }

    def 'onFlowComplete twice writes exactly one RunCompletion and one Store Log entry'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()
        observer.onFlowComplete()

        then: 'the second call is a no-op (the completion latch already fired)'
        blocksOfKind('RunCompletion').size() == 1
        StoreLog.read(cas.store).size() == 1

        and: 'a successful run'
        blocksOfKind('RunCompletion')[0].get('status') == 'succeeded'
        blocksOfKind('RunCompletion')[0].get('possibly_incomplete') == false
    }


    def 'the Store Log entry is stamped with the time it was written, not the run finish time'() {
        given: 'the usual binding, then an observer whose clock is fixed'
        final long fixed = 1_800_000_000_000L
        bind(config())
        observer = new CasObserver() {
            @Override
            protected long nowMillis() { return fixed }
        }
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final entries = StoreLog.read(cas.store)
        entries.size() == 1
        entries[0].kind == StoreLogKind.RUN
        entries[0].writtenAtMillis == fixed
    }

    def 'a failed session yields a failed, possibly-incomplete RunCompletion'() {
        given:
        bind(config(), meta(7))
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final completion = blocksOfKind('RunCompletion')[0]
        completion.get('status') == 'failed'
        completion.get('possibly_incomplete') == true
        completion.get('exit_status') == 7L
    }

    def 'the run is ingested into the index'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final index = Index.open(indexFile)
        try {
            index.runByNextflowHash('nfhash123').isPresent()
        }
        finally {
            index.close()
        }
    }

    def 'an index failure is logged and does not propagate or lose the completion'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        // A subclass whose index cannot be opened, standing in for a broken index.
        observer = new CasObserver() {
            @Override
            protected Index openIndex() {
                throw new IllegalStateException('boom')
            }
        }

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then: 'no exception, and the provenance block was still written'
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
    }

    def 'a mismatched outputDir alias aborts at onFlowCreate'() {
        given:
        bind(config('cas://other'))

        when:
        observer.onFlowCreate(session)

        then:
        thrown(AbortRunException)
    }
}
