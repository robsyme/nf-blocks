package robsyme.cas.core

import java.nio.file.Path
import java.time.Instant

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Prune.plan over a real Index fed from a RetentionFixture (ticket 21 answer
 * 5, plan decision 12). Four runs of pipeline `p`, oldest to newest: s1
 * (successful), f2 (failed), s3 (successful), s4 (successful, newest).
 */
class PruneTest extends Specification {

    @TempDir
    Path tempDir

    RetentionFixture f
    Index index
    long now

    Cid s1
    Cid f2
    Cid s3
    Cid s4

    def setup() {
        f = new RetentionFixture(tempDir.resolve('store'))
        index = Index.open(tempDir.resolve('cache').resolve('index.sqlite'))
        now = Instant.parse('2026-09-29T00:00:00.000Z').toEpochMilli()

        s1 = f.run('s1', [[a: f.raw('s1-a')]],
            [pipeline: 'p', status: 'succeeded', finishedAt: Index.isoMillis(now - 5 * SweepPolicy.DAY)]).completion as Cid
        f2 = f.run('f2', [[a: f.raw('f2-a')]],
            [pipeline: 'p', status: 'failed', finishedAt: Index.isoMillis(now - 3 * SweepPolicy.DAY)]).completion as Cid
        s3 = f.run('s3', [[a: f.raw('s3-a')]],
            [pipeline: 'p', status: 'succeeded', finishedAt: Index.isoMillis(now - 2 * SweepPolicy.DAY)]).completion as Cid
        s4 = f.run('s4', [[a: f.raw('s4-a')]],
            [pipeline: 'p', status: 'succeeded', finishedAt: Index.isoMillis(now - 1 * SweepPolicy.DAY)]).completion as Cid

        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
    }

    def cleanup() {
        index?.close()
    }

    def 'keep-last keeps the newest N successful runs and releases the rest, failed ones included'() {
        when:
        final List<PruneDecision> d = new Prune(index).plan('p', 2, null, now)

        then:
        d.collectEntries { [(it.run.runName): it.action] } == [s4: 'keep', s3: 'keep', f2: 'release', s1: 'release']
    }

    def 'keep-newer releases runs finished before the cutoff'() {
        when:
        final List<PruneDecision> d = new Prune(index).plan('p', null, 2 * SweepPolicy.DAY, now)

        then: 'runs finished 1, 2, 3 and 5 days ago'
        d.findAll { it.action == 'release' }*.run*.runName.sort() == ['f2', 's1']
    }

    def 'either policy keeping a run keeps it'() {
        expect:
        new Prune(index).plan('p', 1, 4 * SweepPolicy.DAY, now).findAll { it.action == 'keep' }*.run*.runName.sort() == ['f2', 's3', 's4']
    }

    def 'deleted, already released and conflicted runs are skipped; a pinned run is released with a note'() {
        given:
        f.claim(s1, 'delete', null, null)
        f.claim(f2, 'set', 'retain', 'lineage')
        f.claim(s3, 'set', 'retain', 'lineage'); f.claim(s3, 'set', 'retain', 'lineage')   // two blocks: timestamps differ
        f.claim(s4, 'add', 'pin', 'paper')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        when:
        final Map<String, PruneDecision> d = new Prune(index).plan('p', 0, null, now).collectEntries { [(it.run.runName): it] }

        then:
        d.s1.action == 'skip' && d.s1.reason == 'deleted'
        d.f2.action == 'skip' && d.f2.reason == 'already released'
        d.s3.action == 'skip' && d.s3.reason.contains('conflict')
        d.s4.action == 'release' && d.s4.reason.contains('pinned')
    }

    def 'a release after a restore supersedes the del'() {
        given:
        final Cid release = f.claim(s1, 'set', 'retain', 'lineage')
        final Cid restore = f.claim(s1, 'del', 'retain', null, [release])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        when:
        final PruneDecision d = new Prune(index).plan('p', 0, null, now).find { it.run.runName == 's1' }
        final Map req = Prune.request(d, '2026-09-30T12:00:00.000Z')

        then:
        d.action == 'release'
        req == [kind: 'Claim', subject: s1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [restore],
                timestamp: '2026-09-30T12:00:00.000Z']
    }

    def 'no policy is refused'() {
        when:
        new Prune(index).plan(null, null, null, now)

        then:
        thrown(IllegalArgumentException)
    }
}
