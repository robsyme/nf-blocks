package robsyme.cas.lineage

import nextflow.lineage.config.LineageConfig
import nextflow.plugin.Priority
import spock.lang.Specification

/**
 * `canOpen` runs at Session.init for every lineage-enabled run, including runs
 * that have nothing to do with this plugin, so it must be total over every
 * config. DESIGN.md section 2.
 */
class CasLinStoreFactoryTest extends Specification {

    def 'claims a cas lineage store location'() {
        expect:
        new CasLinStoreFactory().canOpen(new LineageConfig([store: [location: 'cas://lab']]))
    }

    def 'declines a location owned by another store'() {
        expect:
        !new CasLinStoreFactory().canOpen(new LineageConfig([store: [location: location]]))

        where:
        location << ['file:///x', './.lineage', 's3://bucket/lineage', 'jdbc:h2:mem:testdb', '']
    }

    def 'declines an unset location without throwing'() {
        expect:
        !new CasLinStoreFactory().canOpen(new LineageConfig([:]))
    }

    def 'declines a config whose store scope is absent without throwing'() {
        expect:
        !new CasLinStoreFactory().canOpen(new LineageConfig())
    }

    def 'outranks the default lineage store factory'() {
        given:
        def priority = CasLinStoreFactory.getAnnotation(Priority)

        expect:
        priority != null
        priority.value() == -10
    }
}
