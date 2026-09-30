package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

class SweepPlanTest extends Specification {

    @TempDir Path root
    RetentionFixture f

    def setup() { f = new RetentionFixture(root) }

    def 'dead, young, due, waiting, rescued and trash are what the plan says'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Mark m = Mark.of(f.store, f.logs(), 2)
        final long now = 2_000_000_000_000L
        final long old = now - 15 * SweepPolicy.DAY
        final Cid liveOne = (Cid) r.completion
        final Cid dueOne = f.raw('due')
        final Cid waitOne = f.raw('wait')
        final Cid newOne = f.raw('new')
        final Cid youngOne = f.raw('young')
        final List<BlockStat> stats = [new BlockStat(liveOne, 10, old), new BlockStat(dueOne, 3, old),
            new BlockStat(waitOne, 4, old), new BlockStat(newOne, 3, old), new BlockStat(youngOne, 5, now - 1000)]
        final List<TrashLedger> ledgers = [
            new TrashLedger('s1', now - 1, 'x', [(dueOne): 3L, (liveOne): 10L]),
            new TrashLedger('s2', now + SweepPolicy.DAY, 'x', [(waitOne): 4L])]

        when:
        final SweepPlan p = SweepPlan.of(m, stats, ledgers, now, SweepPolicy.defaults(), 0L)

        then:
        p.dead*.cid as Set == [dueOne, waitOne, newOne, youngOne] as Set
        p.young*.cid == [youngOne]
        p.due*.cid == [dueOne]
        p.waiting*.cid == [waitOne]
        p.rescued == [liveOne] as Set
        p.trash*.cid == [newOne]
    }

    def 'a ledgered block that is no longer listed is rescued, never due'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        final Mark m = Mark.of(f.store, f.logs(), 2)
        final long now = 2_000_000_000_000L
        final Cid gone = f.raw('gone')

        when:
        final SweepPlan p = SweepPlan.of(m, [], [new TrashLedger('s1', now - 1, 'x', [(gone): 4L])], now, SweepPolicy.defaults(), 0L)

        then:
        p.due == []
        p.rescued == [gone] as Set
    }

    def 'the budget takes blocks in address order while they fit'() {
        given: 'three dead old blocks of 4 bytes and a budget of 9'
        f.run('a', [[x: f.raw('x')]])
        final Mark m = Mark.of(f.store, f.logs(), 2)
        final long now = 2_000_000_000_000L
        final long old = now - 15 * SweepPolicy.DAY
        final List<BlockStat> stats = [f.raw('one'), f.raw('two'), f.raw('three')].collect { Cid c -> new BlockStat(c, 4, old) }

        when:
        final SweepPlan p = SweepPlan.of(m, stats, [], now, SweepPolicy.defaults(), 9L)

        then:
        p.trash.size() == 2
        p.overBudget.size() == 1
        p.trash*.cid == stats*.cid.sort { it.toString() }.take(2)
    }

    def 'policy: the age floor may not be under the stale threshold; grace may be zero; either under a day warns'() {
        when:
        new SweepPolicy(599_999L, 0L).validate()

        then:
        thrown(IllegalArgumentException)

        when:
        new SweepPolicy(600_000L, -1L).validate()

        then:
        thrown(IllegalArgumentException)

        expect:
        new SweepPolicy(600_000L, 0L).warnings().size() == 2
        SweepPolicy.defaults().warnings() == []
    }
}
