package robsyme.cas.core

import java.security.SecureRandom
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The store-wide sweep lock (ticket 20 answers 5 and 6): sweep.lock under the
 * writable member, taken create-if-absent, heartbeated and taken over at its
 * version, released by a `released` body. Stale after 10 minutes without a
 * heartbeat, by the store's clock.
 */
@CompileStatic
class SweepLock {

    static final long HEARTBEAT_MILLIS = 60_000L
    static final long STALE_MILLIS = 600_000L
    private static final SecureRandom RANDOM = new SecureRandom()
    private static final DateTimeFormatter ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)

    @Canonical
    @CompileStatic
    static class Holder {
        String sweepId
        String startedAt
        long ageMillis
    }

    private final RetentionStorage storage
    private final String sweepId
    private final Closure<Long> localClock
    private String version
    private int beat
    private String startedAt

    SweepLock(RetentionStorage storage, String sweepId, Closure<Long> localClock) {
        this.storage = storage
        this.sweepId = sweepId
        this.localClock = localClock
    }

    static String newSweepId(long nowMillis) {
        final byte[] bytes = new byte[5]
        RANDOM.nextBytes(bytes)
        return ID_TIME.format(Instant.ofEpochMilli(nowMillis)) + '-' + Multibase.base32Encode(bytes).substring(0, 8)
    }

    synchronized boolean isHeld() { version != null }

    /**
     * Bounded at 3 attempts: a lock that keeps looking created-then-deleted (a
     * hand-deleted file, or a takeover racing us every time) must not recurse
     * without limit. Giving up returns an 'unknown' holder rather than the
     * lock itself, since after 3 straight losses the true state is unclear.
     */
    synchronized Holder take() {
        startedAt = Index.isoMillis(localClock.call())
        beat = 0
        for( int attempt = 0; attempt < 3; attempt++ ) {
            final String created = storage.createLock(body('held'))
            if( created != null ) {
                version = created
                return null
            }
            final Versioned current = storage.readLock()
            if( current == null )
                continue          // released and deleted by hand between the two calls: try again
            final Holder h = holderOf(current, storage.nowMillis())
            if( h != null )
                return h
            version = storage.replaceLock(current.version, body('held'))
            if( version != null )
                return null
            // lost the race to replace it: loop and try again
        }
        return new Holder('unknown', null, 0L)
    }

    synchronized boolean heartbeat() {
        if( version == null )
            return false
        beat++
        version = storage.replaceLock(version, body('held'))
        return version != null
    }

    synchronized void release() {
        if( version == null )
            return
        storage.replaceLock(version, body('released'))
        version = null
    }

    static Holder holder(RetentionStorage storage) {
        final Versioned current = storage.readLock()
        return current == null ? null : holderOf(current, storage.nowMillis())
    }

    /** The holder when the lock is held and fresh; null when released or stale. */
    private static Holder holderOf(Versioned lock, long nowMillis) {
        Map parsed
        try {
            parsed = (Map) new JsonSlurper().parse(lock.body)
        }
        catch( Exception e ) {
            parsed = [:]    // unreadable: judged by age alone
        }
        final long age = nowMillis - lock.lastModifiedMillis
        if( parsed.state == 'released' || age > STALE_MILLIS )
            return null
        return new Holder(String.valueOf(parsed.sweep ?: 'unknown'), parsed.started_at as String, age)
    }

    private byte[] body(String state) {
        return JsonOutput.toJson([sweep: sweepId, state: state, started_at: startedAt, beat: beat]).getBytes('UTF-8')
    }
}
