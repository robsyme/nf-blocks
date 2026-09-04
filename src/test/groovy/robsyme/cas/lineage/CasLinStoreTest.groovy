package robsyme.cas.lineage

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import nextflow.file.FileHelper
import nextflow.lineage.DefaultLinStore
import nextflow.lineage.config.LineageConfig
import nextflow.lineage.model.v1beta1.Checksum
import nextflow.lineage.model.v1beta1.FileOutput
import nextflow.lineage.model.v1beta1.TaskRun
import nextflow.lineage.model.v1beta1.WorkflowRun
import nextflow.lineage.serde.LinSerializable
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.Coordinates
import robsyme.cas.core.StoreRef
import robsyme.cas.nio.CasFileSystemProvider
import spock.lang.Specification
import spock.lang.TempDir

/**
 * `CasLinStore` is exercised directly, driving a real {@link DefaultLinStore}
 * delegate under {@code <writable>/nf} and a real {@link CasSession} built from
 * a mock Nextflow {@link Session}. Every rewrite assertion reads the record
 * back through the delegate rather than trusting a mock, and recomputes the
 * fingerprint with the same production path the store uses. DESIGN.md §10.
 */
class CasLinStoreTest extends Specification {

    @TempDir
    Path tempDir

    Session session

    CasSession casSession

    CasLinStore store

    def setupSpec() {
        // The store's rewrite (and this spec's fingerprint check) resolves
        // `cas://<cid>/<name>` through `FileHelper.asPath`, which reaches the
        // registered CasPathFactory and thence `CasPlugin.provider()`. In the
        // JVM Nextflow launches that installs the provider by reflection into
        // `java.nio.file.spi` (needs `--add-opens`, absent from this test JVM),
        // so CasPlugin's own provider cache is primed here instead -- reflecting
        // into our own class, unlike the java.base field, is permitted.
        final field = CasPlugin.getDeclaredField('provider')
        field.setAccessible(true)
        field.set(null, new CasFileSystemProvider())
    }

    def setup() {
        def cfg = [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.toString()]],
                resolve: ['lab'],
            ],
        ]
        session = Mock(Session) { getConfig() >> cfg }
        Global.session = session
        casSession = CasSession.of(session)
        store = new CasLinStore().open(new LineageConfig([store: [location: 'cas://lab']]))
    }

    def cleanup() {
        CasSession.unbind(session)
        Global.session = null
    }

    private static Cid rawCid(byte fill) {
        final digest = new byte[32]
        Arrays.fill(digest, fill)
        return Cid.of(Cid.RAW, digest)
    }

    def 'open creates the nf records dir and a task run round-trips through the delegate'() {
        given:
        final task = new TaskRun(sessionId: 's1', name: 'ALIGN')

        expect: 'open() has created <writable>/nf'
        store.recordsLocation == casSession.config.writableLocation.resolve('nf')
        Files.isDirectory(store.recordsLocation)

        when:
        store.save('abc123', task)

        then: 'the record lands at nf/<key>/.data.json and round-trips'
        Files.exists(store.recordsLocation.resolve('abc123/.data.json'))
        (store.load('abc123') as TaskRun) == task
    }

    def 'a workflow run records its key as the nextflow run key'() {
        when:
        store.save('run-7f3a', new WorkflowRun(name: 'nice_curie'))

        then:
        casSession.nextflowRunKey == 'run-7f3a'
        and: 'the record is still delegated'
        store.load('run-7f3a') != null
    }

    def 'a published coordinate is rewritten to the store uri recorded in this run'() {
        given:
        final cid = rawCid((byte) 0x11)
        final ref = new StoreRef(cid, 'A.bam')
        casSession.recordPublish(Coordinates.key('cas://lab/aligned/A/A.bam'),
            new CasSession.Publish(ref, 42L, 'head-node'))
        final output = new FileOutput(path: 'cas://lab/aligned/A/A.bam', size: 42L, source: 'run-7f3a')

        when:
        store.save('run-7f3a/aligned/A/A.bam', output)
        final loaded = store.load('run-7f3a/aligned/A/A.bam') as FileOutput

        then: 'the delegated path is the immutable store uri, not the coordinate'
        loaded.path == "cas://${cid}/A.bam".toString()
        and: 'the fingerprint was recomputed against that store uri'
        loaded.checksum.value == Checksum.ofNextflow(FileHelper.asPath(loaded.path)).value
        and: 'the untouched fields survive'
        loaded.size == 42L
        loaded.source == 'run-7f3a'
    }

    def 'a published coordinate is rewritten from the pointer file when the run lost the publish'() {
        given: 'only a pointer file, no in-memory publish'
        final cid = rawCid((byte) 0x22)
        casSession.coordinates.write('aligned/B/B.bam', new StoreRef(cid, 'B.bam'))
        final output = new FileOutput(path: 'cas://lab/aligned/B/B.bam', size: 7L)

        when:
        store.save('run-7f3a/aligned/B/B.bam', output)
        final loaded = store.load('run-7f3a/aligned/B/B.bam') as FileOutput

        then:
        loaded.path == "cas://${cid}/B.bam".toString()
        loaded.checksum.value == Checksum.ofNextflow(FileHelper.asPath(loaded.path)).value
    }

    def 'a published coordinate with no address anywhere aborts the run'() {
        given: 'neither a publish nor a pointer file for this coordinate'
        final output = new FileOutput(path: 'cas://lab/orphan/C.bam', size: 1L)

        when:
        store.save('run-7f3a/orphan/C.bam', output)

        then:
        thrown(AbortRunException)
    }

    def 'a file output already at a store uri is delegated unchanged'() {
        given:
        final cid = rawCid((byte) 0x33)
        final uri = "cas://${cid}/D.bam".toString()
        final preset = new Checksum('preset-value', 'nextflow', 'standard')
        final output = new FileOutput(path: uri, size: 3L, checksum: preset)

        when:
        store.save('run-7f3a/D.bam', output)
        final loaded = store.load('run-7f3a/D.bam') as FileOutput

        then: 'the store uri and its checksum are left as they were, not recomputed'
        loaded.path == uri
        loaded.checksum == preset
    }

    def 'search by type and getSubKeys reach the delegate'() {
        given: 'a run with one non-cas file output under it'
        store.save('run-7f3a', new WorkflowRun(name: 'nice_curie'))
        store.save('run-7f3a/out', new FileOutput(path: '/data/out/file.bam', size: 5L))

        when:
        final byType = store.search([type: ['FileOutput']]).collect(Collectors.toList())

        then: 'the file output key is found by decoded type'
        byType.contains('run-7f3a/out')
        !byType.contains('run-7f3a')

        when:
        final children = store.getSubKeys('run-7f3a').collect(Collectors.toList())

        then:
        children.contains('run-7f3a/out')
    }

    def 'an io error from the delegate surfaces as an abort'() {
        given: 'a delegate that fails every save'
        store.@delegate = new DefaultLinStore() {
            @Override
            void save(String key, LinSerializable value) { throw new IOException('disk full') }
        }

        when:
        store.save('run-7f3a', new TaskRun(name: 'ALIGN'))

        then:
        thrown(AbortRunException)
    }
}
