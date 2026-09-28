package robsyme.cas.nio

import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes

import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.LocalCoordinateTree
import robsyme.cas.core.DagCbor
import robsyme.cas.core.DirectoryManifest
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.ManifestEntry
import robsyme.cas.core.StoreRef
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Drives the provider directly against a real {@link LocalBlockStore} and
 * {@link CoordinateTree} in a temp dir, via a {@link CasSession} test double
 * bound to a mock session. It never goes through {@code FileSystems} or
 * {@code installedProviders} -- the Gate covers the real registration; these
 * tests cover the behaviour (DESIGN.md section 8).
 */
class CasFileSystemProviderTest extends Specification {

    @TempDir Path tmp

    CasFileSystemProvider provider
    LocalBlockStore store
    CoordinateTree coords
    Session session

    def setup() {
        final storeDir = tmp.resolve('store')
        Files.createDirectories(storeDir)
        final config = CasConfig.from(
                [lineage: [store: [location: 'cas://lab']], cas: [stores: [lab: [location: storeDir.toString()]]]],
                'cas://lab')
        store = new LocalBlockStore(storeDir, 'lab', true)
        coords = new LocalCoordinateTree(storeDir.resolve('coords'))
        session = Mock(Session)
        Global.session = session
        CasSession.bind(session, new CasSession(config, store, coords))
        provider = new CasFileSystemProvider()
    }

    def cleanup() {
        CasSession.unbind(session)
        Global.session = null
    }

    private CasPath p(String uri) {
        return (CasPath) provider.getPath(URI.create(uri))
    }

    private Path sourceFile(String name, String content) {
        final f = tmp.resolve(name)
        Files.createDirectories(f.parent ?: tmp)
        Files.writeString(f, content)
        return f
    }

    private CasSession sess() { CasSession.current() }

    // -------------------------------------------------------------------- upload

    def 'upload of a regular file hashes a block, writes a pointer and records the publish'() {
        given:
        def src = sourceFile('work/A.bam', 'BAMBAM\n')
        def target = p('cas://lab/aligned/A/A.bam')

        when:
        provider.upload(src, target)

        then:
        // a block is present under blocks/
        def key = 'cas://lab/aligned/A/A.bam'
        def publish = sess().publishFor(key)
        publish != null
        publish.size == Files.size(src)
        publish.provider == 'head-node'
        store.has(publish.ref.cid)

        and: 'the pointer file is a one-line Store URI with the presented name'
        def pointer = coords.pointerPath('aligned/A/A.bam')
        Files.readString(pointer).trim() == "cas://${publish.ref.cid}/A.bam"
        StoreRef.parse(Files.readString(pointer).trim()).name == 'A.bam'
    }

    def 'upload throws FileAlreadyExistsException before opening the source when the coordinate exists'() {
        given:
        provider.upload(sourceFile('work/A.bam', 'first\n'), p('cas://lab/aligned/A/A.bam'))
        and: 'a source that would fail if a single byte were read'
        def unreadable = tmp.resolve('does-not-exist.bam')

        when:
        provider.upload(unreadable, p('cas://lab/aligned/A/A.bam'))

        then:
        def e = thrown(FileAlreadyExistsException)
        e.message.contains('cas://lab/aligned/A/A.bam')
    }

    def 'upload of an unreadable source surfaces the IOException and writes no pointer'() {
        given:
        def missing = tmp.resolve('missing.bam')

        when:
        provider.upload(missing, p('cas://lab/aligned/A/A.bam'))

        then:
        thrown(IOException)
        !coords.exists('aligned/A/A.bam')
    }

    def 'upload of a directory stores every file as a block, keeps an internal symlink and counts a dangling one'() {
        given:
        def dir = tmp.resolve('work/bundle')
        Files.createDirectories(dir.resolve('sub'))
        Files.writeString(dir.resolve('a.txt'), 'alpha\n')
        Files.writeString(dir.resolve('sub/b.txt'), 'beta\n')
        Files.createSymbolicLink(dir.resolve('link'), Paths.get('a.txt'))        // internal, relative
        Files.createSymbolicLink(dir.resolve('dead'), Paths.get('nowhere.txt'))  // dangling
        def target = p('cas://lab/bundles/out')

        when:
        provider.upload(dir, target)

        then: 'the pointer is a manifest cid'
        def ref = coords.read('bundles/out').get()
        ref.isDirectory()
        ref.name == 'out'

        and: 'every file is a real block'
        def manifest = manifestAt(ref.cid)
        def a = manifest.entry('a.txt')
        a.mode == ManifestEntry.REGULAR
        store.has(a.address)

        and: 'the internal symlink is kept, the dangling one is counted'
        manifest.entry('link').mode == ManifestEntry.SYMLINK
        manifest.entry('link').target == 'a.txt'
        manifest.entry('dead').mode == ManifestEntry.UNRESOLVABLE
        sess().uploadAnomaliesFor('cas://lab/bundles/out').unresolvable == 1

        and: 'the subdirectory is a nested manifest with its own block'
        def sub = manifest.entry('sub')
        sub.mode == ManifestEntry.DIRECTORY
        manifestAt(sub.address).entry('b.txt').mode == ManifestEntry.REGULAR
    }

    def 'a directory publish addresses every file inside through the run addresser; the leaf is head-node, the files are its contents (silent decision 3)'() {
        given:
        def dir = tmp.resolve('work/trio')
        Files.createDirectories(dir.resolve('sub'))
        Files.writeString(dir.resolve('a.txt'), 'alpha\n')
        Files.writeString(dir.resolve('b.txt'), 'beta\n')
        Files.writeString(dir.resolve('sub/c.txt'), 'gamma\n')
        def key = 'cas://lab/trio/out'

        when:
        provider.upload(dir, p(key))

        then: 'every file inside went through the addresser'
        sess().addresser.counts == ['head-node': 3]

        and: "the directory leaf's address is its manifest, always head-node"
        sess().publishFor(key).provider == 'head-node'
        sess().publishFor(key).ref.cid.isDagCbor()

        and: 'the files inside are recorded as its contents, sorted by string'
        def cids = ['alpha\n', 'beta\n', 'gamma\n'].collect { store.putStreaming(new ByteArrayInputStream(it.bytes)) }
        sess().publishFor(key).contents == ['head-node': cids.sort { it.toString() }]
    }

    private DirectoryManifest manifestAt(Cid cid) {
        return DirectoryManifest.fromCbor((Map) DagCbor.decode(store.open(cid).bytes))
    }

    // -------------------------------------------------------------------- read

    def 'readAttributes of a raw Store URI tells size, regular and a stable mtime'() {
        given:
        provider.upload(sourceFile('work/A.bam', 'hello\n'), p('cas://lab/aligned/A/A.bam'))
        def cid = coords.read('aligned/A/A.bam').get().cid
        def uri = p("cas://${cid}/A.bam")

        when:
        def a = provider.readAttributes(uri, BasicFileAttributes)

        then:
        a.isRegularFile()
        !a.isDirectory()
        a.size() == 6
        a.lastModifiedTime() == provider.readAttributes(uri, BasicFileAttributes).lastModifiedTime()
    }

    def 'a manifest Store URI is a directory, but a directory coordinate is a single pointer file'() {
        given:
        def dir = tmp.resolve('work/bundle2')
        Files.createDirectories(dir)
        Files.writeString(dir.resolve('a.txt'), 'x\n')
        provider.upload(dir, p('cas://lab/b/out'))
        def manifestCid = coords.read('b/out').get().cid

        expect: 'the manifest cid, addressed as a Store URI, is a directory'
        provider.readAttributes(p("cas://${manifestCid}"), BasicFileAttributes).isDirectory()

        and: 'the directory coordinate is a single pointer file, not a browsable directory'
        // The published directory tree lives only under the Store URI. In the mutable
        // coordinate namespace it is one indivisible pointer, so an overwrite deletes
        // the pointer rather than recursing into immutable manifest blocks.
        def da = provider.readAttributes(p('cas://lab/b/out'), BasicFileAttributes)
        !da.isDirectory()
        da.isRegularFile()

        and: 'a coordinate for a file resolves to the block'
        provider.upload(sourceFile('work/f.txt', 'data\n'), p('cas://lab/c/f.txt'))
        def fa = provider.readAttributes(p('cas://lab/c/f.txt'), BasicFileAttributes)
        fa.isRegularFile()
        fa.size() == 5
    }

    def 're-publishing a directory deletes only the pointer, never a block, so a second run survives'() {
        given: 'a directory published once, as Nextflow does on the first run'
        def dir = tmp.resolve('work/qc/A_qc')
        Files.createDirectories(dir.resolve('nested'))
        Files.writeString(dir.resolve('summary.txt'), 'summary\n')
        Files.writeString(dir.resolve('nested/detail.txt'), 'detail\n')
        Files.createSymbolicLink(dir.resolve('alias.txt'), Paths.get('summary.txt'))
        provider.upload(dir, p('cas://lab/qc/A/A_qc'))
        def manifestCid = coords.read('qc/A/A_qc').get().cid
        def blocksBefore = store.listBlocks().collect { it.toString() }.toSet()

        when: 'the second run publishes the same directory to the existing coordinate'
        provider.upload(dir, p('cas://lab/qc/A/A_qc'))

        then: 'the upload refuses before touching bytes, as it does for a file'
        // Nextflow catches this and then runs its overwrite path (deletePath + re-copy).
        thrown(FileAlreadyExistsException)

        and: 'readAttributes reports a regular file, so deletePath will not recurse'
        // A single Files.delete on the pointer -- the whole point of the fix; a
        // directory here would recurse into the immutable manifest blocks and abort.
        !provider.readAttributes(p('cas://lab/qc/A/A_qc'), BasicFileAttributes).isDirectory()

        when: 'Nextflow deletes the existing directory coordinate before re-copying'
        provider.delete(p('cas://lab/qc/A/A_qc'))

        then: 'the pointer is gone but every content block survives'
        !coords.exists('qc/A/A_qc')
        store.listBlocks().collect { it.toString() }.toSet() == blocksBefore
        store.has(manifestCid)

        when: 're-publishing writes a fresh pointer to the same manifest'
        provider.upload(dir, p('cas://lab/qc/A/A_qc'))

        then: 'the pointer points at the same manifest and no block was added'
        coords.read('qc/A/A_qc').get().cid == manifestCid
        store.listBlocks().collect { it.toString() }.toSet() == blocksBefore
    }

    def 'newInputStream streams the exact block bytes'() {
        given:
        def bytes = 'the quick brown fox\n'
        provider.upload(sourceFile('work/g.txt', bytes), p('cas://lab/g.txt'))
        def cid = coords.read('g.txt').get().cid

        when:
        def read = provider.newInputStream(p("cas://${cid}/g.txt")).withStream { it.bytes }

        then:
        new String(read) == bytes
    }

    def 'newDirectoryStream lists manifest entries and coordinate-directory children'() {
        given:
        def dir = tmp.resolve('work/bundle3')
        Files.createDirectories(dir)
        Files.writeString(dir.resolve('a.txt'), 'a\n')
        Files.writeString(dir.resolve('b.txt'), 'b\n')
        provider.upload(dir, p('cas://lab/deep/out'))
        provider.upload(sourceFile('work/h.txt', 'h\n'), p('cas://lab/deep/h.txt'))
        def manifestCid = coords.read('deep/out').get().cid

        when: 'over a manifest Store URI'
        def manifestNames = names(provider.newDirectoryStream(p("cas://${manifestCid}"), null))

        then:
        manifestNames == ['a.txt', 'b.txt']

        when: 'over a real coordinate directory'
        def coordNames = names(provider.newDirectoryStream(p('cas://lab/deep'), null))

        then:
        coordNames.contains('out')
        coordNames.contains('h.txt')
    }

    private static List<String> names(DirectoryStream<Path> stream) {
        final List<String> out = new ArrayList<String>()
        try {
            for( Path child : stream )
                out.add(child.fileName.toString())
        } finally {
            stream.close()
        }
        return out.sort()
    }

    def 'checkAccess: NoSuchFile for an absent coordinate and an absent block, AccessDenied for WRITE on a Store URI'() {
        when: 'an absent coordinate'
        provider.checkAccess(p('cas://lab/never/there'))
        then:
        thrown(NoSuchFileException)

        when: 'an absent block'
        provider.checkAccess(p('cas://bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/x'))
        then:
        thrown(NoSuchFileException)

        when: 'WRITE on a Store URI'
        provider.upload(sourceFile('work/w.txt', 'w\n'), p('cas://lab/w.txt'))
        def cid = coords.read('w.txt').get().cid
        provider.checkAccess(p("cas://${cid}/w.txt"), AccessMode.WRITE)
        then:
        thrown(AccessDeniedException)
    }

    // -------------------------------------------------------------- write guard

    def 'every write to a Store URI is AccessDenied'() {
        given:
        provider.upload(sourceFile('work/s.txt', 's\n'), p('cas://lab/s.txt'))
        def cid = coords.read('s.txt').get().cid
        def uri = p("cas://${cid}/s.txt")

        when: provider.newOutputStream(uri)
        then: thrown(AccessDeniedException)

        when: provider.delete(uri)
        then: thrown(AccessDeniedException)

        when: provider.createDirectory(p("cas://${cid}"))
        then: thrown(AccessDeniedException)
    }

    def 'canUpload is true for a coordinate and false for a Store URI'() {
        expect:
        provider.canUpload(tmp.resolve('x'), p('cas://lab/a/b'))
        !provider.canUpload(tmp.resolve('x'), p('cas://bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/x'))
    }

    def 'delete of a coordinate removes only the pointer file'() {
        given:
        provider.upload(sourceFile('work/d.txt', 'd\n'), p('cas://lab/d.txt'))
        def cid = coords.read('d.txt').get().cid

        when:
        provider.delete(p('cas://lab/d.txt'))

        then: 'the pointer is gone but the block stays'
        !coords.exists('d.txt')
        store.has(cid)
    }

    // ------------------------------------------------- read-only members (final review I3)

    /** Rebinds the session with a second, read-only member `core` beside the writable `lab`; returns core's root. */
    private Path withReadOnlyCore() {
        final Path coreDir = Files.createDirectories(tmp.resolve('core'))
        final config = CasConfig.from(
                [lineage: [store: [location: 'cas://lab']],
                 cas: [stores: [lab: [location: tmp.resolve('store').toString()], core: [location: coreDir.toString()]],
                       resolve: ['lab', 'core']]],
                'cas://lab')
        CasSession.unbind(session)
        CasSession.bind(session, new CasSession(config, store, coords))
        return coreDir
    }

    /** Every file under dir, relative, so a refused write can be shown to have left nothing. */
    private static List<String> filesUnder(Path dir) {
        if( !Files.exists(dir) ) return []
        return Files.walk(dir).withCloseable { s -> s.filter { Files.isRegularFile(it) }.collect { dir.relativize(it).toString() }.sort() } as List<String>
    }

    def 'a write through a read-only alias is refused naming the writable member: #verb'() {
        given:
        final Path coreDir = withReadOnlyCore()
        final LocalCoordinateTree coreCoords = new LocalCoordinateTree(coreDir.resolve('coords'))
        coreCoords.write('kept.txt', new StoreRef(Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'), 'kept.txt'))
        final Path src = sourceFile('work/r.txt', 'r\n')
        final List<String> before = filesUnder(coreDir)

        when:
        action.call(provider, src, p(target))

        then:
        final AccessDeniedException e = thrown()
        e.message.contains("'core'")
        e.message.contains("'lab'")
        filesUnder(coreDir) == before
        sess().publishFor(target) == null

        where:
        verb              | target                  | action
        'upload file'     | 'cas://core/new.txt'    | { CasFileSystemProvider pr, Path s, CasPath t -> pr.upload(s, t) }
        'upload dir'      | 'cas://core/newdir'     | { CasFileSystemProvider pr, Path s, CasPath t -> pr.upload(s.parent, t) }
        'createDirectory' | 'cas://core/d'          | { CasFileSystemProvider pr, Path s, CasPath t -> pr.createDirectory(t) }
        'delete'          | 'cas://core/kept.txt'   | { CasFileSystemProvider pr, Path s, CasPath t -> pr.delete(t) }
        'deleteIfExists'  | 'cas://core/kept.txt'   | { CasFileSystemProvider pr, Path s, CasPath t -> pr.deleteIfExists(t) }
        'newOutputStream' | 'cas://core/stream.txt' | { CasFileSystemProvider pr, Path s, CasPath t -> pr.newOutputStream(t).close() }
    }

    def 'the writable member still takes writes when a read-only member is configured'() {
        given:
        withReadOnlyCore()

        when:
        provider.upload(sourceFile('work/w.txt', 'w\n'), p('cas://lab/w.txt'))
        provider.createDirectory(p('cas://lab/dir'))
        provider.delete(p('cas://lab/w.txt'))

        then:
        notThrown(AccessDeniedException)
        !coords.exists('w.txt')
    }

    // ------------------------------------------------------------------ download

    def 'download of a raw block on the same filesystem is a symlink to the read-only block'() {
        given:
        provider.upload(sourceFile('work/r.txt', 'raw-bytes\n'), p('cas://lab/r.txt'))
        def cid = coords.read('r.txt').get().cid
        def out = tmp.resolve('stage/r.txt')

        when:
        provider.download(p("cas://${cid}/r.txt"), out)

        then:
        Files.isSymbolicLink(out)
        Files.readString(out) == 'raw-bytes\n'
        !Files.isWritable(Files.readSymbolicLink(out))
    }

    def 'download of a manifest materialises the tree and recreates the internal symlink'() {
        given:
        def dir = tmp.resolve('work/mat')
        Files.createDirectories(dir.resolve('sub'))
        Files.writeString(dir.resolve('a.txt'), 'alpha\n')
        Files.writeString(dir.resolve('sub/b.txt'), 'beta\n')
        Files.createSymbolicLink(dir.resolve('link'), Paths.get('a.txt'))
        provider.upload(dir, p('cas://lab/mat/out'))
        def out = tmp.resolve('stage/mat')

        when:
        provider.download(p('cas://lab/mat/out'), out)

        then:
        Files.readString(out.resolve('a.txt')) == 'alpha\n'
        Files.readString(out.resolve('sub/b.txt')) == 'beta\n'
        Files.isSymbolicLink(out.resolve('link'))
        Files.readSymbolicLink(out.resolve('link')).toString() == 'a.txt'
    }

    def 'download of a manifest with an unresolvable entry aborts loudly naming the entry'() {
        given:
        def dir = tmp.resolve('work/bad')
        Files.createDirectories(dir)
        Files.writeString(dir.resolve('a.txt'), 'a\n')
        Files.createSymbolicLink(dir.resolve('dead'), Paths.get('gone.txt'))
        provider.upload(dir, p('cas://lab/bad/out'))

        when:
        provider.download(p('cas://lab/bad/out'), tmp.resolve('stage/bad'))

        then:
        def e = thrown(AbortRunException)
        e.message.contains('dead')
    }

    def 'download of an absent block is NoSuchFile naming the cid'() {
        given:
        def absent = 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'

        when:
        provider.download(p("cas://${absent}/x"), tmp.resolve('stage/x'))

        then:
        def e = thrown(NoSuchFileException)
        e.message.contains(absent)
    }

    // ------------------------------------------------------------------ identity

    def 'isSameFile compares by resolved content, not Path.equals'() {
        given:
        provider.upload(sourceFile('work/same.txt', 'same\n'), p('cas://lab/same.txt'))
        def cid = coords.read('same.txt').get().cid

        expect: 'a coordinate and the Store URI it resolves to are the same file'
        provider.isSameFile(p('cas://lab/same.txt'), p("cas://${cid}/same.txt"))

        and: 'two spellings of one coordinate are the same'
        provider.isSameFile(p('cas://lab/./same.txt'), p('cas://lab/same.txt'))

        and: 'a foreign path is never the same file'
        !provider.isSameFile(p('cas://lab/same.txt'), Paths.get('/tmp/same.txt'))
    }

    def 'setAttribute and getFileStore refuse rather than pretend'() {
        when: provider.setAttribute(p('cas://lab/x'), 'basic:lastModifiedTime', null)
        then: thrown(UnsupportedOperationException)

        when: provider.getFileStore(p('cas://lab/x'))
        then: thrown(UnsupportedOperationException)
    }
}
