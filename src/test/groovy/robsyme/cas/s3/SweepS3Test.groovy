package robsyme.cas.s3

import java.nio.file.Path

import groovy.json.JsonSlurper
import robsyme.cas.core.Cid
import robsyme.cas.core.RetentionFixture
import robsyme.cas.core.StoreLog
import robsyme.cas.core.Sweep
import robsyme.cas.core.SweepPolicy
import robsyme.cas.core.SweepReport
import spock.lang.Specification
import spock.lang.TempDir

class SweepS3Test extends Specification {

    @TempDir Path tmp
    MemoryS3Ops ops = new MemoryS3Ops('b')
    S3BlockStore store
    RetentionFixture f
    Closure<Void> say = { String s -> null } as Closure<Void>
    Closure<Void> sleeper = { long ms -> null } as Closure<Void>

    def setup() {
        store = new S3BlockStore(ops, 'm/', 'lab', true, tmp)
        f = new RetentionFixture(store)
    }

    private SweepReport apply() { new Sweep(store, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper) }

    def 'a release is ledgered under trash/, deleted in one DeleteObjects past the deadline, and old uploads aborted'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        final Map gone = f.run('gone', [[g: f.raw('gone')]])
        f.claim(gone.completion, 'delete', null, null)
        final String upload = ops.createMultipart('m/blocks/zz/half-written', null)
        ops.advance(15 * SweepPolicy.DAY)

        when:
        final SweepReport first = apply()
        final List<String> ledgers = ops.objects.keySet().findAll { it.startsWith('m/trash/') }.toList()

        then:
        first.applied
        ledgers.size() == 1
        first.trashed > 1
        first.scratch == 1
        ops.calls.contains('ABORT m/blocks/zz/half-written')
        ops.listUploads('m/') == []
        store.has(only)
        !ops.calls.any { it.startsWith('DELETEMANY') }
        new JsonSlurper().parse(ops.objects['m/sweep.lock'].bytes).state == 'released'

        when:
        ops.advance(15 * SweepPolicy.DAY)
        ops.calls.clear()
        final SweepReport second = apply()

        then:
        second.applied
        ops.calls.count { it.startsWith('DELETEMANY') } == 1
        second.deleted.size() == first.trashed
        !store.has(only)
        !store.has((Cid) gone.completion)
        store.has((Cid) b.completion)
        !StoreLog.read(store).any { it.cid == gone.completion }
        !ops.objects.keySet().any { it.startsWith('m/trash/') }
        new JsonSlurper().parse(ops.objects['m/sweep.lock'].bytes).state == 'released'
    }

    def 'a dry run over S3 writes nothing'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ops.advance(15 * SweepPolicy.DAY)
        final Map<String, String> before = ops.objects.collectEntries { k, v -> [(k): v.etag] }
        ops.calls.clear()

        when:
        final SweepReport r = new Sweep(store, SweepPolicy.defaults()).dryRun()

        then:
        r.dead == 1
        r.trashed == 1
        ops.objects.collectEntries { k, v -> [(k): v.etag] } == before
        !ops.calls.any { it.startsWith('PUT') || it.startsWith('DELETE') || it.startsWith('ABORT') }
        r.toText().contains('memory://b/m/')
    }
}
