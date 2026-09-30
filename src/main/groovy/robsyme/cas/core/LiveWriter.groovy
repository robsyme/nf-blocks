package robsyme.cas.core

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

import groovy.json.JsonOutput
import groovy.transform.CompileStatic

/**
 * A run as a Live Writer (ticket 20 answers 1 and 5): live/<session> is written
 * first, then sweep.lock is read, waiting while a fresh one is held, so a sweep
 * and a run always see each other. Heartbeat every 60 s by plain PUT; deleted
 * at the end. Never fails the run (plan decision 11).
 */
@CompileStatic
class LiveWriter implements Closeable {

    static final long POLL_MILLIS = 30_000L

    private final RetentionStorage storage
    private final String session
    private final byte[] body
    private final Closure<Void> say
    private final Closure<Void> sleeper
    private final ScheduledExecutorService heartbeats
    private volatile ScheduledFuture<?> beating
    private volatile boolean registered
    private boolean beatFailing

    LiveWriter(RetentionStorage storage, String session, Map<String, String> info, Closure<Void> say,
               Closure<Void> sleeper, ScheduledExecutorService heartbeats) {
        this.storage = storage
        this.session = session
        final Map<String, String> content = new LinkedHashMap<String, String>(info)
        content.put('session', session)
        this.body = JsonOutput.toJson(content).getBytes('UTF-8')
        this.say = say
        this.sleeper = sleeper
        this.heartbeats = heartbeats
    }

    void start() {
        try {
            storage.putLive(session, body)
        }
        catch( Exception e ) {
            say.call("nf-blocks could not register this run in ${storage.describe()}/live/ (${unwrap(e).message}); a sweep started now would not wait for it".toString())
            return
        }
        synchronized( this ) {
            // Registering and scheduling under the same lock close() uses closes the
            // window where a close() landing between the two would leave the
            // heartbeat scheduled (and so able to write live/ again) after close.
            registered = true
            beating = heartbeats.scheduleAtFixedRate({ -> beat() } as Runnable,
                SweepLock.HEARTBEAT_MILLIS, SweepLock.HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        }
        boolean told = false
        while( true ) {
            final SweepLock.Holder h
            try {
                h = SweepLock.holder(storage)
            }
            catch( Exception e ) {
                say.call("nf-blocks could not read ${storage.describe()}/sweep.lock (${unwrap(e).message}); not waiting".toString())
                return
            }
            if( h == null )
                return
            if( !told ) {
                say.call("sweep ${h.sweepId} holds ${storage.describe()}/sweep.lock (started ${h.startedAt}); this run is registered and waiting, checking every 30 s. The sweep stops at its next batch, or its lock goes stale 10 minutes after its last heartbeat.".toString())
                told = true
            }
            sleeper.call(POLL_MILLIS)
        }
    }

    /**
     * Package-visible so a test can drive it directly rather than waiting on the
     * scheduler. Synchronized with close(): a beat that lands after close() sees
     * registered false and writes nothing, so a closed run's registration cannot
     * come back to life for the ten minutes before it would go stale.
     */
    synchronized void beat() {
        if( !registered )
            return
        try {
            storage.putLive(session, body)
            if( beatFailing ) {
                beatFailing = false
                say.call("nf-blocks can write ${storage.describe()}/live/${session} again; this run's heartbeat has recovered".toString())
            }
        }
        catch( Exception e ) {
            if( !beatFailing ) {
                beatFailing = true
                say.call("nf-blocks could not update this run's heartbeat at ${storage.describe()}/live/${session} (${unwrap(e).message}); a sweep may treat this run as ended 10 minutes after its last heartbeat".toString())
            }
        }
    }

    synchronized void close() {
        beating?.cancel(false)
        beating = null
        if( !registered )
            return
        registered = false
        try {
            storage.deleteLive(session)
        }
        catch( Exception e ) {
            say.call("nf-blocks could not remove this run's registration ${storage.describe()}/live/${session} (${unwrap(e).message}); the next sweep deletes it once it is stale".toString())
        }
    }

    /** Spock's Stub() of an interface is a JDK Proxy, which wraps a checked exception an interface method does not declare. */
    private static Throwable unwrap(Throwable t) {
        return t instanceof UndeclaredThrowableException && t.cause != null ? t.cause : t
    }
}
