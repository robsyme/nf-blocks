package robsyme.cas.lineage

import java.nio.file.Files
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import nextflow.lineage.config.LineageConfig
import nextflow.plugin.Priority
import spock.lang.Specification
import spock.lang.TempDir

/**
 * `canOpen` runs at Session.init for every lineage-enabled run, including runs
 * that have nothing to do with this plugin, so it must be total over every
 * config. DESIGN.md section 2.
 */
class CasLinStoreFactoryTest extends Specification {

    @TempDir
    Path tempDir

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

    def 'newInstance opens a cas lineage store rooted at the writable member'() {
        given:
        def cfg = [
            lineage: [store: [location: 'cas://lab']],
            cas: [stores: [lab: [location: tempDir.toString()]]],
        ]
        final session = Mock(Session) { getConfig() >> cfg }
        Global.session = session

        when:
        final store = new CasLinStoreFactory().newInstance(new LineageConfig([store: [location: 'cas://lab']]))

        then:
        store instanceof CasLinStore
        Files.isDirectory((store as CasLinStore).recordsLocation)
        (store as CasLinStore).recordsLocation.fileName.toString() == 'nf'

        cleanup:
        Global.session = null
    }
}
