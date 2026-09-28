package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The writer's clock against S3's (ticket 03 decision 2). Store Log entries
 * carry the writer's clock and a catch-up re-reads 10 minutes behind a
 * watermark, so an entry more than that behind others' could be missed by
 * fromStore(latest); 5 minutes leaves room for two writers skewed opposite ways.
 */
@CompileStatic
class ClockSkew {
    enum Verdict { OK, WARN, ABORT }

    static final long WARN_MILLIS = 60_000L
    static final long ABORT_MILLIS = 300_000L

    static Verdict judge(long localMillis, long serverMillis) {
        final long skew = Math.abs(localMillis - serverMillis)
        return skew > ABORT_MILLIS ? Verdict.ABORT : skew > WARN_MILLIS ? Verdict.WARN : Verdict.OK
    }

    static String describe(long localMillis, long serverMillis) {
        final long s = Math.abs(localMillis - serverMillis).intdiv(1000L)
        final String way = localMillis >= serverMillis ? 'ahead of' : 'behind'
        return "this machine's clock is ${s} s ${way} S3's (the Date header of its first response); Store Log entries " +
            'are stamped with the local clock, and a skew over 5 minutes can hide a run from fromStore(latest). Set the clock by NTP.'
    }
}
