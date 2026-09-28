package robsyme.cas.core

import spock.lang.Specification

/** Ticket 03 decision 2: warn above 1 minute, abort above 5. */
class ClockSkewTest extends Specification {

    def 'skew of #seconds s is #verdict'() {
        expect:
        ClockSkew.judge(1_000_000_000L + seconds * 1000L, 1_000_000_000L) == verdict
        ClockSkew.judge(1_000_000_000L - seconds * 1000L, 1_000_000_000L) == verdict

        where:
        seconds | verdict
        0       | ClockSkew.Verdict.OK
        60      | ClockSkew.Verdict.OK
        61      | ClockSkew.Verdict.WARN
        300     | ClockSkew.Verdict.WARN
        301     | ClockSkew.Verdict.ABORT
    }

    def 'the description names the direction, the size and the fix'() {
        expect:
        ClockSkew.describe(1_000_400_000L, 1_000_000_000L).contains('400 s ahead of')
        ClockSkew.describe(1_000_000_000L, 1_000_400_000L).contains('400 s behind')
        ClockSkew.describe(0L, 1L).contains('NTP')
    }
}
