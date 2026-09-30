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
}
