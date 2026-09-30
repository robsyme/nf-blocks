package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

class SweepLockTest extends Specification {

    @TempDir Path root
    RetentionStorage storage
    Closure<Long> clock = { -> System.currentTimeMillis() }

    def setup() {
        storage = new LocalRetentionStorage(root)
    }

    def 'an absent lock is taken, heartbeats, and is released'() {
        given:
        final lock = new SweepLock(storage, 'sweep-1', { -> System.currentTimeMillis() })

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
        new JsonSlurper().parse(storage.readLock().body).state == 'released'
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
        Files.setLastModifiedTime(root.resolve('sweep.lock'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))

        expect:
        SweepLock.holder(storage) == null
        new SweepLock(storage, 'next', clock).take() == null
        SweepLock.holder(storage).sweepId == 'next'
    }

    def 'a heartbeat that meets a takeover reports the lock lost'() {
        given:
        final slow = new SweepLock(storage, 'slow', clock)
        slow.take()
        Files.setLastModifiedTime(root.resolve('sweep.lock'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        new SweepLock(storage, 'fast', clock).take()

        expect:
        !slow.heartbeat()
        !slow.held

        when: 'a lost lock is not released over its new holder'
        slow.release()

        then:
        SweepLock.holder(storage).sweepId == 'fast'
    }

    def 'sweep ids sort by time and are unique'() {
        expect:
        SweepLock.newSweepId(1_790_000_000_000L) ==~ /20260921T\d{6}Z-[a-z2-7]{8}/
        SweepLock.newSweepId(1L) != SweepLock.newSweepId(1L)
    }

    def 'take gives up after three attempts, without recursing without bound, when the lock keeps looking deleted'() {
        given:
        final RetentionStorage flaky = Mock(RetentionStorage)

        when:
        final SweepLock.Holder h = new SweepLock(flaky, 'x', clock).take()

        then:
        3 * flaky.createLock(_) >> null
        3 * flaky.readLock() >> null
        h == new SweepLock.Holder('unknown', null, 0L)
    }
}
