package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.util.Duration
import robsyme.cas.core.SweepPolicy

/** {@code cas.sweep.ageFloor} and {@code cas.sweep.grace} as a {@link SweepPolicy} (plan decision 8). */
@CompileStatic
class SweepSettings {

    static SweepPolicy of(Map config) {
        final Map sweep = scopeOf(config)
        final SweepPolicy defaults = SweepPolicy.defaults()
        final SweepPolicy policy = new SweepPolicy(
            millis(sweep.get('ageFloor'), defaults.ageFloorMillis, 'cas.sweep.ageFloor'),
            millis(sweep.get('grace'), defaults.graceMillis, 'cas.sweep.grace'))
        policy.validate()
        return policy
    }

    /** {@code cas.sweep.grace} alone, in milliseconds, for a writer that needs no age floor (the Index Snapshot, P2 ruling R7). */
    static long graceMillis(Map config) {
        final long grace = millis(scopeOf(config).get('grace'), SweepPolicy.defaults().graceMillis, 'cas.sweep.grace')
        if( grace < 0 )
            throw new IllegalArgumentException("cas.sweep.grace cannot be negative; got ${grace} ms")
        return grace
    }

    private static Map scopeOf(Map config) {
        final Object scope = ((Map) (config?.get('cas') ?: [:])).get('sweep')
        return scope instanceof Map ? (Map) scope : [:]
    }

    private static long millis(Object value, long fallback, String key) {
        if( value == null )
            return fallback
        if( value instanceof Duration )
            return ((Duration) value).toMillis()
        try {
            return Duration.of(value.toString()).toMillis()
        }
        catch( IllegalArgumentException e ) {
            throw new IllegalArgumentException("${key} is a duration such as '14d' or '12h'; got '${value}'")
        }
    }
}
