package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime

import groovy.json.JsonOutput
import spock.lang.Specification
import spock.lang.TempDir

class LiveRegistryTest extends Specification {

    @TempDir Path root
    RetentionStorage storage

    def setup() {
        storage = new LocalRetentionStorage(root)
    }

    def 'registrations are fresh until ten minutes without a heartbeat, then stale and deletable'() {
        given:
        storage.putLive('s1', JsonOutput.toJson([session: 's1', run_name: 'happy_turing', pipeline: 'p', started_at: 'x']).bytes)
        storage.putLive('s2', '{}'.bytes)
        Files.setLastModifiedTime(root.resolve('live/s2'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        final registry = new LiveRegistry(storage)

        expect:
        registry.fresh()*.session == ['s1']
        registry.fresh()[0].runName == 'happy_turing'
        registry.stale()*.session == ['s2']

        when:
        registry.deleteStale()

        then:
        storage.listLive()*.name == ['s1']
    }
}
