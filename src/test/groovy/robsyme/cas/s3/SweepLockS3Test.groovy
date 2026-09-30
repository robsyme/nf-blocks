package robsyme.cas.s3

import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.SweepLock
import spock.lang.Specification

class SweepLockS3Test extends Specification {

    MemoryS3Ops ops = new MemoryS3Ops('b')
    RetentionStorage storage
    Closure<Long> clock = { -> System.currentTimeMillis() }

    def setup() {
        storage = new S3RetentionStorage(ops, 'm/')
    }

    def 'an absent lock is taken, heartbeats, and is released'() {
        given:
        final lock = new SweepLock(storage, 'sweep-1', clock)

        expect:
        lock.take() == null
        lock.held
        lock.heartbeat()
        SweepLock.holder(storage).sweepId == 'sweep-1'

        when:
        lock.release()

        then:
        !lock.held
        SweepLock.holder(storage) == null
    }

    def 'a fresh lock held by another sweep is refused, naming it'() {
        given:
        new SweepLock(storage, 'first', clock).take()

        when:
        final SweepLock.Holder h = new SweepLock(storage, 'second', clock).take()

        then:
        h.sweepId == 'first'
        h.ageMillis >= 0
    }

    def 'a released lock is taken, not refused'() {
        given:
        final first = new SweepLock(storage, 'first', clock)
        first.take()
        first.release()

        expect:
        new SweepLock(storage, 'second', clock).take() == null
        SweepLock.holder(storage).sweepId == 'second'
    }

    def 'a lock whose heartbeat stopped over ten minutes ago is taken over'() {
        given:
        new SweepLock(storage, 'crashed', clock).take()
        ops.advance(601_000L)

        expect:
        SweepLock.holder(storage) == null
        new SweepLock(storage, 'next', clock).take() == null
        SweepLock.holder(storage).sweepId == 'next'
    }

    def 'a stale S3 lock is judged by S3 Date, not this machine clock'() {
        given:
        new SweepLock(storage, 'first', { -> 0L }).take()   // a client clock far in the past changes nothing
        ops.advance(599_000L)

        expect: 'nine minutes 59 seconds by S3: still fresh'
        SweepLock.holder(storage).sweepId == 'first'

        when:
        ops.advance(2_000L)

        then:
        SweepLock.holder(storage) == null
    }
}
