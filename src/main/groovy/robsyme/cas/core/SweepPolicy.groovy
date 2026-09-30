package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** How old a dead block must be before a sweep trashes it, and how long it waits in Trash before deletion (ticket 20 answer 3). */
@Canonical
@CompileStatic
class SweepPolicy {
    static final long DAY = 86_400_000L
    long ageFloorMillis
    long graceMillis

    static SweepPolicy defaults() { new SweepPolicy(14 * DAY, 14 * DAY) }

    /** Plan decision 8: the age floor covers a run that looks dead before it is. */
    void validate() {
        if( ageFloorMillis < SweepLock.STALE_MILLIS )
            throw new IllegalArgumentException("cas.sweep.ageFloor must be at least 10m, the time after which a run's registration counts as dead; got ${ageFloorMillis} ms")
        if( graceMillis < 0 )
            throw new IllegalArgumentException("cas.sweep.grace cannot be negative; got ${graceMillis} ms")
    }

    List<String> warnings() {
        final List<String> out = []
        if( ageFloorMillis < DAY )
            out << "cas.sweep.ageFloor is under a day: a block written by a pipeline that does not register here (a reader, a put) could be trashed soon after it is written".toString()
        if( graceMillis < DAY )
            out << "cas.sweep.grace is under a day: trashed blocks can be deleted by the next sweep, leaving little time to restore them".toString()
        return out
    }
}
