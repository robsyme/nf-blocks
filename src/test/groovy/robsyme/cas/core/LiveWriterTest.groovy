package robsyme.cas.core

import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir

class LiveWriterTest extends Specification {

    @TempDir Path root
    RetentionStorage storage
    Closure<Long> clock = { -> System.currentTimeMillis() }
    ScheduledExecutorService executor

    def setup() {
        storage = new LocalRetentionStorage(root)
        executor = Executors.newSingleThreadScheduledExecutor { Runnable r ->
            final Thread t = new Thread(r, 'live-writer-test')
            t.daemon = true
            return t
        }
    }

    def cleanup() {
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
    }

    def 'a run registers, waits while a fresh lock is held, then proceeds; close deregisters'() {
        given:
        final sweep = new SweepLock(storage, 'sweep-1', clock)
        sweep.take()
        final List<String> said = []
        int sleeps = 0
        final writer = new LiveWriter(storage, 'sess', [run_name: 'r', pipeline: 'p', started_at: 't'],
            { String m -> said << m }, { long ms -> if( ++sleeps == 2 ) sweep.release() }, executor)

        when:
        writer.start()

        then:
        storage.listLive()*.name == ['sess']
        sleeps == 2
        said.size() == 1
        said[0].contains('sweep-1')
        said[0].contains('waiting')

        when:
        writer.close()
        writer.close()

        then:
        storage.listLive() == []
    }

    def 'no lock, no wait'() {
        given:
        int sleeps = 0
        final writer = new LiveWriter(storage, 'sess', [:], { String m -> }, { long ms -> sleeps++ }, executor)

        when:
        writer.start()

        then:
        sleeps == 0
        storage.listLive()*.name == ['sess']

        cleanup:
        writer.close()
    }

    def 'a registration that cannot be written warns and the run goes on'() {
        given:
        final RetentionStorage broken = Stub(RetentionStorage) { putLive(_, _) >> { throw new IOException('denied') } }
        final List<String> said = []

        when:
        new LiveWriter(broken, 'sess', [:], { String m -> said << m }, { long ms -> }, executor).start()

        then:
        notThrown(Exception)
        said.any { it.contains('could not register') && it.contains('denied') }
    }

    def 'a heartbeat failure streak warns once, and a later success reports recovery once'() {
        given: 'registration (the first putLive) succeeds; the next two beats fail, the third succeeds'
        int calls = 0
        final RetentionStorage flaky = Stub(RetentionStorage) {
            putLive(_, _) >> {
                calls++
                if( calls == 2 || calls == 3 )
                    throw new IOException('denied')
            }
            readLock() >> null   // no sweep lock: start()'s wait loop returns at once
        }
        List<String> said = []
        def writer = new LiveWriter(flaky, 'sess', [:], { String m -> said << m }, { long ms -> }, executor)
        writer.start()

        when: 'the first failing beat'
        writer.beat()

        then:
        said.size() == 1
        said[0].contains('could not update')
        said[0].contains('denied')
        said[0].contains('a sweep may treat this run as ended 10 minutes after its last heartbeat')

        when: 'a second beat, still failing: no repeat warning'
        writer.beat()

        then:
        said.size() == 1

        when: 'a beat that succeeds again'
        writer.beat()

        then:
        said.size() == 2
        said[1].contains('recovered')

        cleanup:
        writer.close()
    }

    def 'a beat after close writes nothing, so a closed run cannot come back to life'() {
        given:
        final writer = new LiveWriter(storage, 'sess', [:], { String m -> }, { long ms -> }, executor)
        writer.start()

        when:
        writer.close()
        writer.beat()

        then:
        storage.listLive() == []
    }
}
