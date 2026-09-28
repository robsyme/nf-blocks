package robsyme.cas.core

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * A directory as Fusion leaves it in a bucket (ticket 06 §6), walked from a
 * non-default filesystem, against the same tree built with real links on
 * local disk (ticket 15 decision 1: the backend is not provenance).
 */
class ObjectStoreManifestTest extends Specification {

    @TempDir Path work
    LocalBlockStore store
    FileSystem zip

    def setup() {
        store = new LocalBlockStore(work.resolve('store'), 'lab', true)
        zip = FileSystems.newFileSystem(URI.create('jar:' + work.resolve('bucket.zip').toUri()), [create: 'true'])
    }

    def cleanup() { zip?.close() }

    private Path obj(String key, String body) {
        final Path p = zip.getPath('/' + key)
        Files.createDirectories(p.parent)
        Files.writeString(p, body)
        return p
    }

    private DirectoryManifest read(Cid cid) {
        store.open(cid).withCloseable { DirectoryManifest.fromCbor((Map) DagCbor.decode(it.bytes)) }
    }

    /** The prototype-06 tree, minus the host-absolute link (decision 16 makes it unresolvable here, content locally). */
    private Path fusionTree() {
        obj('w/d/target.txt', 'target\n')
        obj('w/d/nested/deeper/deep.txt', 'deep\n')
        obj('w/d/name with space.txt', 'x\n')
        obj('w/d/rel.txt', 'target.txt')
        obj('w/d/nested/up.txt', '../target.txt')
        obj('w/d/dirlink', 'nested/deeper')
        obj('w/d/escape.txt', '../../../escape.txt')
        obj('w/d/dangling.txt', 'missing.txt')
        obj('w/d/other/spaced.txt', 'name with space.txt')
        obj('w/d/.fusion.symlinks', 'rel.txt\ndangling.txt\ndirlink\nescape.txt')
        obj('w/d/nested/.fusion.symlinks', 'up.txt')
        obj('w/d/other/.fusion.symlinks', 'spaced.txt')
        return zip.getPath('/w/d')
    }

    private Path localTree() {
        final Path d = work.resolve('local/w/d')
        Files.createDirectories(d.resolve('nested/deeper'))
        Files.createDirectories(d.resolve('other'))
        Files.writeString(d.resolve('target.txt'), 'target\n')
        Files.writeString(d.resolve('nested/deeper/deep.txt'), 'deep\n')
        Files.writeString(d.resolve('name with space.txt'), 'x\n')
        Files.createSymbolicLink(d.resolve('rel.txt'), Path.of('target.txt'))
        Files.createSymbolicLink(d.resolve('nested/up.txt'), Path.of('../target.txt'))
        Files.createSymbolicLink(d.resolve('dirlink'), Path.of('nested/deeper'))
        Files.createSymbolicLink(d.resolve('escape.txt'), Path.of('../../../escape.txt'))
        Files.createSymbolicLink(d.resolve('dangling.txt'), Path.of('missing.txt'))
        Files.createSymbolicLink(d.resolve('other/spaced.txt'), Path.of('name with space.txt'))
        return d
    }

    def 'a Fusion tree and the same tree on local disk are one Directory Manifest (ticket 15 decision 1)'() {
        when:
        final def fusion = new DirectoryManifestBuilder(store).build(fusionTree())
        final def local = new DirectoryManifestBuilder(store).build(localTree())

        then:
        fusion.cid == local.cid
        fusion.anomalies == local.anomalies
        final DirectoryManifest m = read(fusion.cid)
        m.entry('.fusion.symlinks') == null
        m.entry('rel.txt').mode == 'symlink' && m.entry('rel.txt').target == 'target.txt'
        m.entry('dirlink').mode == 'symlink'
        m.entry('dangling.txt').mode == 'unresolvable' && m.entry('dangling.txt').target == 'missing.txt'
        m.entry('escape.txt').target == Records.REDACTED_LOCATION
    }

    def 'an escaping link that exists is followed and stored as content; /fusion/s3 targets resolve by key'() {
        given:
        obj('w/outside.txt', 'out\n')
        obj('other-bucket/k/far.txt', 'far\n')
        obj('w/d/a.txt', '../outside.txt')
        obj('w/d/b.txt', '/fusion/s3/other-bucket/k/far.txt')
        obj('w/d/c.txt', '/etc/hostname')
        obj('w/d/.fusion.symlinks', 'a.txt\nb.txt\nc.txt')
        final Closure<Path> objectPath = { String uri ->
            uri.startsWith('s3://other-bucket/') ? zip.getPath('/other-bucket/' + uri.substring('s3://other-bucket/'.length())) : null
        }

        when:
        final def r = new DirectoryManifestBuilder(store, new HeadNodeAddresser(store), objectPath).build(zip.getPath('/w/d'))
        final DirectoryManifest m = read(r.cid)

        then:
        m.entry('a.txt').mode == 'regular' && store.open(m.entry('a.txt').address).text == 'out\n'
        m.entry('b.txt').mode == 'regular' && store.open(m.entry('b.txt').address).text == 'far\n'
        m.entry('c.txt').mode == 'unresolvable' && m.entry('c.txt').target == Records.REDACTED_LOCATION
        r.anomalies.unresolvable == 1
    }

    def 'a link whose target passes through a decoded directory link is a symlink, as a local run records it (final review I6)'() {
        given: 'via.txt -> dirlink/deep.txt, with dirlink -> nested/deeper, and a chain through two directory links'
        final Path fusion = fusionTree()
        obj('w/d/via.txt', 'dirlink/deep.txt')
        obj('w/d/hop', 'dirlink')
        obj('w/d/twice.txt', 'hop/deep.txt')
        obj('w/d/back.txt', 'dirlink/../up.txt')
        obj('w/d/.fusion.symlinks', 'rel.txt\ndangling.txt\ndirlink\nescape.txt\nvia.txt\nhop\ntwice.txt\nback.txt')
        final Path local = localTree()
        Files.createSymbolicLink(local.resolve('via.txt'), Path.of('dirlink/deep.txt'))
        Files.createSymbolicLink(local.resolve('hop'), Path.of('dirlink'))
        Files.createSymbolicLink(local.resolve('twice.txt'), Path.of('hop/deep.txt'))
        Files.createSymbolicLink(local.resolve('back.txt'), Path.of('dirlink/../up.txt'))

        when:
        final def f = new DirectoryManifestBuilder(store).build(fusion)
        final def l = new DirectoryManifestBuilder(store).build(local)
        final DirectoryManifest m = read(f.cid)

        then: 'back.txt climbs from the physical nested/deeper to nested/up.txt, itself a link to ../target.txt'
        m.entry('via.txt').mode == 'symlink' && m.entry('via.txt').target == 'dirlink/deep.txt'
        m.entry('twice.txt').mode == 'symlink' && m.entry('twice.txt').target == 'hop/deep.txt'
        m.entry('back.txt').mode == 'symlink' && m.entry('back.txt').target == 'dirlink/../up.txt'
        f.cid == l.cid
        f.anomalies == l.anomalies
    }

    def 'an escaping link to a directory is followed and stored as that directory (final review I6)'() {
        given:
        obj('ext/data/f.txt', 'f\n')
        obj('w/d/keep.txt', 'k\n')
        obj('w/d/outdir', '../../ext/data')
        obj('w/d/.fusion.symlinks', 'outdir')
        final Path local = Files.createDirectories(work.resolve('local/w/d'))
        Files.createDirectories(work.resolve('local/ext/data'))
        Files.writeString(work.resolve('local/ext/data/f.txt'), 'f\n')
        Files.writeString(local.resolve('keep.txt'), 'k\n')
        Files.createSymbolicLink(local.resolve('outdir'), Path.of('../../ext/data'))

        when:
        final def f = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final def l = new DirectoryManifestBuilder(store).build(local)
        final DirectoryManifest m = read(f.cid)

        then:
        m.entry('outdir').mode == 'directory'
        read(m.entry('outdir').address).entry('f.txt').mode == 'regular'
        f.cid == l.cid
        f.anomalies.unresolvable == 0
    }

    def 'a sidecar that lists itself does not enter the manifest (Task 3 minor)'() {
        given:
        obj('w/d/target.txt', 't\n')
        obj('w/d/rel.txt', 'target.txt')
        obj('w/d/.fusion.symlinks', 'rel.txt\n.fusion.symlinks')

        when:
        final def r = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final DirectoryManifest m = read(r.cid)

        then:
        m.entry('.fusion.symlinks') == null
        m.entry('rel.txt').mode == 'symlink'
        r.anomalies.unresolvable == 0
    }

    def 'a chain is followed, a cycle is unresolvable and counted (Review Focus 4)'() {
        given:
        obj('w/d/target.txt', 't\n')
        obj('w/d/a', 'b')
        obj('w/d/b', 'target.txt')
        obj('w/d/x', 'y')
        obj('w/d/y', 'x')
        obj('w/d/.fusion.symlinks', 'a\nb\nx\ny')

        when:
        final def r = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final DirectoryManifest m = read(r.cid)

        then:
        m.entry('a').mode == 'symlink' && m.entry('a').target == 'b'
        m.entry('b').mode == 'symlink'
        m.entry('x').mode == 'unresolvable' && m.entry('x').target == 'y'
        r.anomalies.unresolvable == 2
    }

    def 'a listed name without an object, a long body and an unparseable listing (ticket 15 decision 4)'() {
        given:
        obj('w/d/long.txt', 'x' * 5000)
        obj('w/d/.fusion.symlinks', 'long.txt\nghost.txt')
        obj('w/e/f.txt', 'f')
        obj('w/e/.fusion.symlinks', 'f.txt\u0000')

        when:
        final def d = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final def e = new DirectoryManifestBuilder(store).build(zip.getPath('/w/e'))

        then:
        read(d.cid).entry('ghost.txt').mode == 'unresolvable' && read(d.cid).entry('ghost.txt').target == null
        read(d.cid).entry('long.txt').target == Records.REDACTED_LOCATION
        d.anomalies.unresolvable == 2
        read(e.cid).entry('.fusion.symlinks').mode == 'regular'
        read(e.cid).entry('f.txt').mode == 'regular'
        e.anomalies.unresolvable == 1
    }

    def 'every file is regular, from an object store or local disk, and each goes through the addresser'() {
        given:
        obj('w/d/tool.sh', '#!/bin/sh\n')
        final Path localDir = Files.createDirectories(work.resolve('local-tool/d'))
        final Path localTool = Files.writeString(localDir.resolve('tool.sh'), '#!/bin/sh\n')
        localTool.toFile().setExecutable(true)
        final List<String> asked = []
        final FileAddresser counting = { Path f, long size -> asked << f.fileName.toString(); new HeadNodeAddresser(store).address(f, size) } as FileAddresser

        when:
        final def r = new DirectoryManifestBuilder(store, counting, null).build(zip.getPath('/w/d'))
        final def l = new DirectoryManifestBuilder(store, counting, null).build(localDir)

        then:
        read(r.cid).entry('tool.sh').mode == 'regular'
        read(l.cid).entry('tool.sh').mode == 'regular'
        r.cid == l.cid
        asked == ['tool.sh', 'tool.sh']
    }

    def 'every file in both walks goes through the addresser, whose provider counts the run logs (final review I5)'() {
        given:
        obj('w/d/a.txt', 'a\n')
        obj('w/d/sub/b.txt', 'b\n')
        final Path localDir = Files.createDirectories(work.resolve('local-prov/d/sub'))
        Files.writeString(localDir.parent.resolve('a.txt'), 'a\n')
        Files.writeString(localDir.resolve('b.txt'), 'b\n')
        final List<String> addressed = []
        final FileAddresser counting = { Path f, long size ->
            addressed << f.fileName.toString()
            new HeadNodeAddresser(store).address(f, size)
        } as FileAddresser

        when:
        final def r = new DirectoryManifestBuilder(store, counting, null).build(zip.getPath('/w/d'))
        final def l = new DirectoryManifestBuilder(store, counting, null).build(localDir.parent)

        then:
        addressed.sort() == ['a.txt', 'a.txt', 'b.txt', 'b.txt']
        r.cid == l.cid
    }

    def 'realOf falls back to the absolute path where toRealPath is unsupported (ticket 05)'() {
        given:
        final Path norm = Stub(Path)
        final Path abs = Stub(Path) { normalize() >> norm }
        final Path s3 = Stub(Path) {
            toRealPath(*_) >> { throw new UnsupportedOperationException() }
            toAbsolutePath() >> abs
        }

        expect:
        DirectoryManifestBuilder.realOf(s3).is(norm)
    }
}
