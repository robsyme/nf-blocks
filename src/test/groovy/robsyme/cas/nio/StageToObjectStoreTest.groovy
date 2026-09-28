package robsyme.cas.nio

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.*
import spock.lang.Specification
import spock.lang.TempDir

/** download() into a non-default filesystem: links become copies (ticket 15 decision 7). */
class StageToObjectStoreTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore store
    Session session
    FileSystem zip
    CasFileSystemProvider provider = new CasFileSystemProvider()

    def setup() {
        store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        final CasConfig config = CasConfig.from([cas: [stores: [lab: [location: store.root.toString()]]]], 'cas://lab')
        session = Mock(Session); Global.session = session
        CasSession.bind(session, new CasSession(config, store, new LocalCoordinateTree(store.root.resolve('coords'))))
        zip = FileSystems.newFileSystem(URI.create('jar:' + tmp.resolve('work.zip').toUri()), [create: 'true'])
    }

    def cleanup() { zip?.close(); CasSession.unbind(session); Global.session = null }

    def 'a manifest with in-tree links stages onto an object store with each link as a copy of its target'() {
        given:
        final Path src = tmp.resolve('A_qc')
        Files.createDirectories(src.resolve('nested'))
        Files.writeString(src.resolve('summary.txt'), 'summary\n')
        Files.writeString(src.resolve('nested/n.txt'), 'n\n')
        Files.createSymbolicLink(src.resolve('alias.txt'), Path.of('summary.txt'))
        Files.createSymbolicLink(src.resolve('dirlink'), Path.of('nested'))
        final Cid manifest = new DirectoryManifestBuilder(store).build(src).cid
        final Path target = zip.getPath('/stage/A_qc')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), target)

        then:
        Files.readString(target.resolve('alias.txt')) == 'summary\n'
        Files.readString(target.resolve('dirlink/n.txt')) == 'n\n'
        !Files.isSymbolicLink(target.resolve('alias.txt'))
    }

    def 'staged locally, links stay links'() {
        given:
        final Path src = tmp.resolve('B_qc')
        Files.createDirectories(src)
        Files.writeString(src.resolve('summary.txt'), 's\n')
        Files.createSymbolicLink(src.resolve('alias.txt'), Path.of('summary.txt'))
        final Cid manifest = new DirectoryManifestBuilder(store).build(src).cid

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), tmp.resolve('staged'))

        then:
        Files.isSymbolicLink(tmp.resolve('staged/alias.txt'))
    }
}
