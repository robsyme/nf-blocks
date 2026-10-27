package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.LocalSnapshotStorage
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * A copy under the explorer's view of which members are indexed
 * (background store indexing, spec §6.1, §6.3): the members it lists as not
 * indexed are left out of the copy's catch-up, a member indexed only on this
 * machine seeds from its local snapshot, and an unindexed destination gets no
 * snapshot from the copy.
 *
 * `lab` holds one logged run, `cloud` and `other` are empty local members.
 */
class CopyServiceIndexingTest extends Specification {

    @TempDir Path tmp

    Path labRoot, cloudRoot, otherRoot
    Cid completion, otherCompletion
    ByteArrayOutputStream err = new ByteArrayOutputStream()
    long now = 1_760_000_000_000L
    Closure<Long> clock = { -> now += 1000 } as Closure<Long>

    def setup() {
        CasSession.platformIdentity = { String e, String t -> throw new IOException('no platform in tests') } as PlatformIdentity
        CasSession.gitEmail = { -> 'someone@example.org' } as GitEmail
        CasSession.env = [:]
        labRoot = tmp.resolve('lab')
        cloudRoot = tmp.resolve('cloud')
        otherRoot = tmp.resolve('other')
        completion = logRun(labRoot, 'lab-1')
        otherCompletion = logRun(otherRoot, 'other-1')
    }

    private Cid logRun(Path root, String run) {
        final LocalBlockStore member = new LocalBlockStore(root, root.fileName.toString(), true)
        final Cid script = member.putStreaming(new ByteArrayInputStream("workflow { ${run} }".getBytes('UTF-8')))
        final Cid m = member.putDagCbor(Fixtures.runManifest(run_name: run, nf_run_hash: run, script: script))
        final Cid rc = member.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(member, StoreLogKind.RUN, rc, now += 1000)
        return rc
    }

    private Map config() {
        return [lineage: [store: [location: 'cas://lab']],
                cas    : [stores: [lab  : [location: labRoot.toString()],
                                   cloud: [location: cloudRoot.toString()],
                                   other: [location: otherRoot.toString()]],
                          resolve: ['lab', 'other'], asserted_by: 'copier',
                          index: [path: tmp.resolve('cache/index.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
    }

    private Index copyCache() { Index.open(tmp.resolve('cache/index.sqlite')) }

    /** A snapshot of {@code alias} built by a scan in a cache of its own, kept under {@code root}. */
    private Path buildLocalSnapshot(String alias, Path memberRoot, Path root = tmp.resolve("local/${alias}")) {
        final Index scratch = Index.open(tmp.resolve("scratch-${UUID.randomUUID()}.sqlite"))
        try {
            final LocalBlockStore member = new LocalBlockStore(memberRoot, alias, false)
            scratch.catchUp(member, StoreLog.of(member), alias)
            assert IndexSnapshot.write(scratch, alias, new LocalSnapshotStorage(root), 0L, null, tmp, null).written
        }
        finally {
            scratch.close()
        }
        return root
    }

    private CopyService.Outcome run(Map request, CopyService.Indexing indexing) {
        return CopyService.execute(config(), request, false, new PrintStream(err, true, 'UTF-8'), null, indexing)
    }

    private String lines() { err.toString('UTF-8') }

    def 'a copy into an unindexed destination does not scan it and says so'() {
        when:
        final CopyService.Outcome out = run([root: "run:${completion}".toString(), to: 'cloud', from: 'lab'],
            new CopyService.Indexing([cloud: 'absent'], [:]))
        final Index cache = copyCache()

        then:
        out.status == 200 && out.body.snapshot == 'not_indexed'
        !Files.exists(cloudRoot.resolve('index/v7.sqlite'))
        lines().contains("nf-blocks:copy: 'cloud' has no Index Snapshot, so this copy did not write one; index it from the explorer's Storage page")
        StoreLog.read(new LocalBlockStore(cloudRoot, 'cloud', false)).any { it.kind == StoreLogKind.TRANSFER }

        and: 'the copy did not scan the destination into its cache'
        !cache.isIndexed('cloud')

        cleanup:
        cache?.close()
    }

    def 'a copy from a member indexed only on this machine seeds from its local snapshot'() {
        given:
        final Path root = buildLocalSnapshot('other', otherRoot)

        when:
        final CopyService.Outcome out = run([root: "run:${otherCompletion}".toString(), to: 'cloud', from: 'other'],
            new CopyService.Indexing([:], [other: root]))
        final Index cache = copyCache()

        then:
        out.status == 200
        cache.metaValue('seeded_from:other') != null

        cleanup:
        cache?.close()
    }

    def 'a copy into a destination indexed only on this machine writes it no snapshot'() {
        given:
        final Path root = buildLocalSnapshot('cloud', cloudRoot)

        when:
        final CopyService.Outcome out = run([root: "run:${completion}".toString(), to: 'cloud', from: 'lab'],
            new CopyService.Indexing([:], [cloud: root]))

        then:
        out.status == 200 && out.body.snapshot == 'not_indexed'
        !Files.exists(cloudRoot.resolve('index/v7.sqlite'))
    }

    def 'a copy into a member outside cas.resolve with no snapshot of its own does not scan it and says so'() {
        when:
        final CopyService.Outcome out = run([root: "run:${completion}".toString(), to: 'cloud', from: 'lab'],
            new CopyService.Indexing([:], [:]))
        final Index cache = copyCache()

        then:
        out.status == 200 && out.body.snapshot == 'not_indexed'
        !Files.exists(cloudRoot.resolve('index/v7.sqlite'))
        !cache.isIndexed('cloud')

        cleanup:
        cache?.close()
    }

    def 'a copy from a member the explorer does not hold, of a run only the cache can resolve, says why'() {
        given: 'cloud, outside cas.resolve, holds a logged run and no snapshot'
        final LocalBlockStore cloud = new LocalBlockStore(cloudRoot, 'cloud', true)
        final Cid manifest = cloud.putDagCbor(Fixtures.runManifest(run_name: 'cloud-1', nf_run_hash: 'cloud-1'))
        StoreLog.append(cloud, StoreLogKind.RUN, cloud.putDagCbor(Fixtures.runCompletion(manifest, [])), now += 1000)

        when: 'the run is named by its RunManifest'
        final CopyService.Outcome out = run([root: manifest.toString(), to: 'other', from: 'cloud'], new CopyService.Indexing([:], [:]))

        then:
        out.status == 400
        out.body.error == 'not_indexed'
        out.body.message == "'cloud' has no Index Snapshot, so this copy cannot find ${manifest} in it; " +
            "index it from the explorer's Storage page, or run nf-blocks:snapshot against it"
    }

    def 'a member the explorer lists as not indexed is left out of the copy catch-up'() {
        given: 'cloud has a snapshot of its own'
        buildLocalSnapshot('cloud', cloudRoot, cloudRoot)

        when:
        final CopyService.Outcome out = run([root: "run:${completion}".toString(), to: 'cloud', from: 'lab'],
            new CopyService.Indexing([other: 'absent'], [:]))
        final Index cache = copyCache()

        then:
        out.status == 200 && out.body.snapshot == 'written'
        !cache.isIndexed('other')

        cleanup:
        cache?.close()
    }

    def 'without explorer indexing a copy is as before'() {
        when:
        final CopyService.Outcome out = CopyService.execute(config(), [root: "run:${completion}".toString(), to: 'cloud', from: 'lab'],
            false, new PrintStream(err, true, 'UTF-8'), null)
        final Index cache = copyCache()

        then:
        out.status == 200 && out.body.snapshot == 'written'
        cache.isIndexed('other')

        cleanup:
        cache?.close()
    }
}
