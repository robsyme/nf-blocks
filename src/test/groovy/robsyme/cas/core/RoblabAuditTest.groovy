package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Collectors

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/**
 * P3 plan section 1.6: the RunManifest audit over the real roblab store.
 * Opt-in, since the store is local to one machine:
 *
 *   ./gradlew test --tests robsyme.cas.core.RoblabAuditTest -Droblab.store=/tmp/cas-exploration/store
 *
 * Only the store's blocks/ directory is copied to a scratch directory, and
 * everything runs against the copy: the original is never opened.
 */
@Requires({ System.getProperty('roblab.store') })
class RoblabAuditTest extends Specification {

    @TempDir Path tmp

    private static void copyTree(Path from, Path to) {
        Files.walk(from).withCloseable { stream ->
            stream.forEach { Path p ->
                final Path target = to.resolve(from.relativize(p).toString())
                if( Files.isDirectory(p) ) Files.createDirectories(target)
                else Files.copy(p, target)
            }
        }
    }

    def 'the audit over the roblab RunManifests'() {
        given:
        final Path copy = tmp.resolve('store')
        copyTree(Paths.get(System.getProperty('roblab.store')).resolve('blocks'), copy.resolve('blocks'))
        final LocalBlockStore store = new LocalBlockStore(copy, 'roblab', false)
        final List<Cid> cids = store.listBlocks().withCloseable { it.collect(Collectors.toList()) }

        when:
        int manifests = 0
        final List<RunManifestAudit.Finding> findings = []
        for( Cid cid : cids ) {
            if( cid.codec != Cid.DAG_CBOR )
                continue
            final Object block = store.open(cid).withCloseable { DagCbor.decode(it.bytes) }
            if( !(block instanceof Map) || Records.kindOf((Map) block) != Records.RUN_MANIFEST )
                continue
            manifests++
            findings.addAll(RunManifestAudit.audit(cid, (Map) block))
        }
        println "roblab RunManifests audited: ${manifests}, findings: ${findings.size()}"
        findings.each { println "roblab finding: ${new groovy.json.JsonBuilder(it.toMap()).toString()}" }

        then:
        manifests > 0
        findings.isEmpty()
    }
}
