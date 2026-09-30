package robsyme.cas

import nextflow.util.Duration
import robsyme.cas.core.SweepPolicy
import spock.lang.Specification

class SweepSettingsTest extends Specification {

    def 'defaults are 14 days each; durations and text are read; bad values refused'() {
        expect:
        SweepSettings.of([:]) == SweepPolicy.defaults()
        SweepSettings.of([cas: [sweep: [ageFloor: '1h', grace: '0s']]]) == new SweepPolicy(3_600_000L, 0L)
        SweepSettings.of([cas: [sweep: [ageFloor: Duration.of('2d')]]]).ageFloorMillis == 2 * SweepPolicy.DAY

        when:
        SweepSettings.of([cas: [sweep: [ageFloor: '5m']]])

        then:
        thrown(IllegalArgumentException)
    }
}
