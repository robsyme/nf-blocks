package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * How far one member's scan has got (background store indexing, spec §6.3).
 * {@link Index#indexMember} calls it as the scan moves, with -1 for a count it
 * does not know yet. Whatever it throws propagates out of the scan, which is
 * how a job stops one.
 */
@CompileStatic
interface IndexProgress {

    static final String LISTING = 'listing'
    static final String READING = 'reading'
    static final String INGESTING = 'ingesting'
    static final String LOG = 'log'
    static final String SNAPSHOT = 'snapshot'
    static final String PUSH = 'push'

    void report(String phase, long done, long total)
}
