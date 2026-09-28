package robsyme.cas.nio

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.LocalCoordinateTree
import robsyme.cas.core.DirectoryManifest
import robsyme.cas.core.Leaf
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.ManifestEntry
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.RunManifest
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §7: an Item Occurrence read through the provider. */
class CasOccurrenceTest extends Specification {

    @TempDir Path tmp

    CasFileSystemProvider provider
    LocalBlockStore store
    Session session
    Cid bam, qcDir, collection, item, dupItem, dupCollection

    def setup() {
        final storeDir = tmp.resolve('store')
        Files.createDirectories(storeDir)
        final config = CasConfig.from(
                [lineage: [store: [location: 'cas://lab']], cas: [stores: [lab: [location: storeDir.toString()]]]],
                'cas://lab')
        store = new LocalBlockStore(storeDir, 'lab', true)
        session = Mock(Session)
        Global.session = session
        CasSession.bind(session, new CasSession(config, store, new LocalCoordinateTree(storeDir.resolve('coords'))))
        provider = new CasFileSystemProvider()

        bam = store.putStreaming(new ByteArrayInputStream('BAM A\n'.bytes))
        final Cid summary = store.putStreaming(new ByteArrayInputStream('summary\n'.bytes))
        qcDir = store.putDagCbor(new DirectoryManifest([ManifestEntry.regular('summary.txt', summary, 8L)]).toCbor())
        final Cid manifest = store.putDagCbor(new RunManifest([assertedBy: 'test', pipeline: 'p', runName: 'r', nfRunHash: 'h',
            sessionId: 's', nextflowVersion: '26.04.6', params: [:], config: [:], startedAt: '2026-09-25T00:00:00.000Z']).toCbor())
        item = store.putDagCbor(OutputItem.of([[sample: 'A'], Leaf.of('A.bam', bam, 6L), Leaf.of('A_qc', qcDir, 0L)]).toCbor())
        collection = store.putDagCbor(new OutputCollection('test', manifest, 'aligned', [item], [['aligned/A/A.bam', 'qc/A']]).toCbor())
        dupItem = store.putDagCbor(OutputItem.of([[sample: 'D'], Leaf.of('x.txt', bam, 6L), Leaf.of('x.txt', bam, 6L)]).toCbor())
        dupCollection = store.putDagCbor(new OutputCollection('test', manifest, 'dups', [dupItem], [['d/1/x.txt', 'd/2/x.txt']]).toCbor())
    }

    def cleanup() {
        CasSession.unbind(session)
        Global.session = null
    }

    private CasPath p(String uri) { (CasPath) provider.getPath(URI.create(uri)) }

    def 'an occurrence with a leaf name is that file'() {
        when:
        final CasPath path = p("cas://${collection}/${item}/A.bam")

        then:
        provider.readAttributes(path, java.nio.file.attribute.BasicFileAttributes).isRegularFile()
        provider.newInputStream(path).text == 'BAM A\n'
    }

    def 'an occurrence without a leaf is a directory of its leaves by name (decision 17)'() {
        when:
        final CasPath path = p("cas://${collection}/${item}")
        final List<String> names = provider.newDirectoryStream(path, null).collect { it.fileName.toString() }.sort()

        then:
        provider.readAttributes(path, java.nio.file.attribute.BasicFileAttributes).isDirectory()
        names == ['A.bam', 'A_qc']
    }

    def 'a directory leaf is walked further'() {
        expect:
        provider.newInputStream(p("cas://${collection}/${item}/A_qc/summary.txt")).text == 'summary\n'
    }

    def 'download of an occurrence stages its leaves'() {
        given:
        final Path target = tmp.resolve('staged')

        when:
        provider.download(p("cas://${collection}/${item}"), target)

        then:
        Files.readString(target.resolve('A.bam')) == 'BAM A\n'
        Files.readString(target.resolve('A_qc/summary.txt')) == 'summary\n'
    }

    def 'a first segment that is not an item of the collection names both readings'() {
        when:
        provider.readAttributes(p("cas://${collection}/aligned/A/A.bam"), java.nio.file.attribute.BasicFileAttributes)

        then:
        final IOException e = thrown()
        e.message.contains('neither an item of collection')
        e.message.contains('publish path')
    }

    def 'an unknown leaf name is absent'() {
        when:
        provider.readAttributes(p("cas://${collection}/${item}/nope.bam"), java.nio.file.attribute.BasicFileAttributes)

        then:
        thrown(NoSuchFileException)
    }

    def 'a leaf name two leaves share is refused, naming both positions'() {
        when:
        provider.readAttributes(p("cas://${dupCollection}/${dupItem}/x.txt"), java.nio.file.attribute.BasicFileAttributes)

        then:
        final IOException e = thrown()
        e.message.contains("'x.txt'")
        e.message.contains('1') && e.message.contains('2')
    }

    // ------------------------------------------------ Item Leaf (final review I1)

    def 'an Item Leaf names a leaf without a collection: a file, a directory listed and walked, named by the leaf'() {
        when:
        final CasPath dir = p("cas://${item}/A_qc")

        then:
        provider.newInputStream(p("cas://${item}/A.bam")).text == 'BAM A\n'
        provider.readAttributes(dir, java.nio.file.attribute.BasicFileAttributes).isDirectory()
        dir.fileName.toString() == 'A_qc'
        provider.newDirectoryStream(dir, null).collect { it.toString() } == ["cas://${item}/A_qc/summary.txt".toString()]
        provider.newInputStream(p("cas://${item}/A_qc/summary.txt")).text == 'summary\n'
    }

    def 'an Item Leaf downloads a directory leaf under the target name'() {
        given:
        final Path target = tmp.resolve('staged/A_qc')

        when:
        provider.download(p("cas://${item}/A_qc"), target)

        then:
        Files.readString(target.resolve('summary.txt')) == 'summary\n'
    }

    def 'an Output Item with no leaf name asks for one; an unknown name is absent; a shared name is refused'() {
        when:
        provider.readAttributes(p("cas://${item}"), java.nio.file.attribute.BasicFileAttributes)
        then:
        final IOException e = thrown()
        e.message.contains('is an Output Item')

        when:
        provider.readAttributes(p("cas://${item}/nope"), java.nio.file.attribute.BasicFileAttributes)
        then:
        thrown(NoSuchFileException)

        when:
        provider.readAttributes(p("cas://${dupItem}/x.txt"), java.nio.file.attribute.BasicFileAttributes)
        then:
        final IOException shared = thrown()
        shared.message.contains("named 'x.txt'")
    }
}
