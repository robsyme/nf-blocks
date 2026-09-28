package robsyme.cas.nio

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
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

    def 'a .. after a directory link climbs from where the link landed, as POSIX does (final review I7)'() {
        given: 'dirlink -> nested/deep, nested/deep/x -> ../y, and links through dirlink; root y and f differ from nested y and f'
        final Path src = tmp.resolve('E_qc')
        Files.createDirectories(src.resolve('nested/deep'))
        Files.writeString(src.resolve('y'), 'root y\n')
        Files.writeString(src.resolve('f'), 'root f\n')
        Files.writeString(src.resolve('nested/y'), 'nested y\n')
        Files.writeString(src.resolve('nested/f'), 'nested f\n')
        Files.createSymbolicLink(src.resolve('nested/deep/x'), Path.of('../y'))
        Files.createSymbolicLink(src.resolve('dirlink'), Path.of('nested/deep'))
        Files.createSymbolicLink(src.resolve('l'), Path.of('dirlink/x'))
        Files.createSymbolicLink(src.resolve('m'), Path.of('dirlink/../f'))
        final Cid manifest = new DirectoryManifestBuilder(store).build(src).cid
        final Path onObjects = zip.getPath('/stage/E_qc')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), onObjects)

        then: 'each staged copy holds what the same link reads on local disk'
        Files.readString(onObjects.resolve('dirlink/x')) == 'nested y\n'
        Files.readString(onObjects.resolve('l')) == 'nested y\n'
        Files.readString(onObjects.resolve('m')) == 'nested f\n'
        ['dirlink/x', 'l', 'm'].every { Files.readString(onObjects.resolve(it)) == Files.readString(src.resolve(it)) }
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

    // The next two are built by hand rather than through a real on-disk symlink
    // and DirectoryManifestBuilder: on this sandbox, Spock's own @TempDir cleanup
    // cannot cope with a real filesystem symlink pointing back to one of its
    // ancestors (it fails walking the tree afterwards, an artifact of the test
    // harness, not of the code under test). A manifest is just bytes, and
    // DirectoryManifestBuilder would encode exactly this shape for `nested/up ->
    // ..` or `self -> .` (resolvesInside is true for a link back to an ancestor),
    // so building it directly exercises the same resolveInTree/materialiseDirectory
    // path without touching the real filesystem at all.

    def 'a link back to an ancestor directory aborts rather than recursing forever'() {
        given:
        final Cid nested = store.putDagCbor(new DirectoryManifest([ManifestEntry.symlink('up', '..')]).toCbor())
        final Cid manifest = store.putDagCbor(new DirectoryManifest([ManifestEntry.directory('nested', nested)]).toCbor())
        final Path target = zip.getPath('/stage/C_qc')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), target)

        then:
        final AbortRunException e = thrown()
        e.message.contains('up')
        e.message.contains('already being materialised')
    }

    def 'a link to the directory it is itself in aborts rather than recursing forever'() {
        given:
        final Cid manifest = store.putDagCbor(new DirectoryManifest([ManifestEntry.symlink('self', '.')]).toCbor())
        final Path target = zip.getPath('/stage/D_qc')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), target)

        then:
        final AbortRunException e = thrown()
        e.message.contains('self')
        e.message.contains('already being materialised')
    }

    def 'a link that climbs above the root aborts rather than being staged'() {
        given:
        // Built by hand: DirectoryManifestBuilder never emits a symlink entry that
        // escapes the walked tree (it follows and inlines it instead), but a manifest
        // is just bytes, so resolveInTree must still refuse one rather than reading
        // outside the root it was asked to stage.
        final Cid fileCid = store.putStreaming(new ByteArrayInputStream('x'.bytes))
        final Cid nested = store.putDagCbor(new DirectoryManifest([ManifestEntry.regular('n.txt', fileCid, 1L)]).toCbor())
        final Cid manifest = store.putDagCbor(new DirectoryManifest([
            ManifestEntry.directory('nested', nested),
            ManifestEntry.symlink('escapes', '../../outside'),
        ]).toCbor())
        final Path target = zip.getPath('/stage/escape')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), target)

        then:
        final AbortRunException e = thrown()
        e.message.contains('does not resolve inside the tree')
    }
}
