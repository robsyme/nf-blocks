package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Timeout

/** DESIGN.md §6: walking a published directory into a manifest. */
class DirectoryManifestBuilderTest extends Specification {

    @TempDir
    Path work

    LocalBlockStore store

    DirectoryManifestBuilder builder

    def setup() {
        store = new LocalBlockStore(work.resolve('store'), 'lab', true)
        builder = new DirectoryManifestBuilder(store)
    }

    private Path file(Path dir, String name, String content) {
        final Path p = dir.resolve(name)
        Files.createDirectories(p.parent)
        Files.writeString(p, content)
        return p
    }

    private DirectoryManifest read(Cid cid) {
        InputStream input = store.open(cid)
        try {
            return DirectoryManifest.fromCbor((Map) DagCbor.decode(input.bytes))
        }
        finally {
            input.close()
        }
    }

    private static Cid rawOf(String content) {
        Hashing.hashRaw(new ByteArrayInputStream(content.getBytes('UTF-8')), new byte[1024])
    }

    def 'entries are sorted by the utf-8 bytes of their names'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'b.txt', 'b')
        file(tree, 'a.txt', 'a')
        file(tree, 'Z.txt', 'z')

        when:
        final DirectoryManifestBuilder.Result result = builder.build(tree)

        then:
        read(result.cid).entries*.name == ['Z.txt', 'a.txt', 'b.txt']
        result.anomalies == Anomalies.NONE
    }

    def 'a file with the owner execute bit set is executable'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'plain.txt', 'p')
        final Path script = file(tree, 'run.sh', '#!/bin/sh\n')
        final Set<PosixFilePermission> perms = Files.getPosixFilePermissions(script)
        perms.add(PosixFilePermission.OWNER_EXECUTE)
        Files.setPosixFilePermissions(script, perms)

        when:
        final DirectoryManifest manifest = read(builder.build(tree).cid)

        then:
        manifest.entry('run.sh').mode == 'executable'
        manifest.entry('plain.txt').mode == 'regular'
    }

    def 'a regular entry carries the raw address and size of its content'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'a.txt', 'hello\n')

        when:
        final ManifestEntry entry = read(builder.build(tree).cid).entry('a.txt')

        then:
        entry.address == rawOf('hello\n')
        entry.size == 6L
        entry.target == null

        and: 'the bytes are in the store'
        store.has(entry.address)
        store.open(entry.address).text == 'hello\n'
    }

    def 'a subdirectory becomes a nested manifest'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'nested/detail.txt', 'detail')

        when:
        final ManifestEntry entry = read(builder.build(tree).cid).entry('nested')

        then:
        entry.mode == 'directory'
        entry.size == 0L
        entry.address.toString().startsWith('bafy')

        and:
        read(entry.address).entry('detail.txt').address == rawOf('detail')
    }

    def 'an empty directory has an address of its own'() {
        given:
        final Path tree = work.resolve('tree')
        Files.createDirectories(tree.resolve('empty'))

        when:
        final DirectoryManifest manifest = read(builder.build(tree).cid)

        then:
        read(manifest.entry('empty').address).entries == []
        read(manifest.entry('empty').address).toCbor().entries == []
    }

    def 'a relative link inside the tree stays a link, with no block of its own'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'summary.txt', 'summary')
        Files.createSymbolicLink(tree.resolve('alias.txt'), Paths.get('summary.txt'))

        when:
        final ManifestEntry entry = read(builder.build(tree).cid).entry('alias.txt')

        then:
        entry.mode == 'symlink'
        entry.target == 'summary.txt'
        entry.address == null
        entry.size == 'summary.txt'.bytes.length

        and: 'the link text was never stored as content'
        !store.has(rawOf('summary.txt'))

        and: 'what it points at was'
        store.has(rawOf('summary'))
    }

    def 'a link to an absolute path outside the tree is followed'() {
        given:
        final Path outside = file(work.resolve('elsewhere'), 'reference.fa', '>chr1\n')
        final Path tree = work.resolve('tree')
        Files.createDirectories(tree)
        Files.createSymbolicLink(tree.resolve('reference.fa'), outside.toAbsolutePath())

        when:
        final ManifestEntry entry = read(builder.build(tree).cid).entry('reference.fa')

        then:
        entry.mode == 'regular'
        entry.address == rawOf('>chr1\n')
        entry.size == 6L
        entry.target == null
        store.has(entry.address)
    }

    def 'a relative link escaping the tree is followed too'() {
        given:
        file(work.resolve('elsewhere'), 'reference.fa', '>chr2\n')
        final Path tree = work.resolve('tree')
        Files.createDirectories(tree)
        Files.createSymbolicLink(tree.resolve('reference.fa'), Paths.get('../elsewhere/reference.fa'))

        when:
        final ManifestEntry entry = read(builder.build(tree).cid).entry('reference.fa')

        then:
        entry.mode == 'regular'
        entry.address == rawOf('>chr2\n')
    }

    def 'a dangling link is unresolvable and counted'() {
        given:
        final Path tree = work.resolve('tree')
        Files.createDirectories(tree)
        Files.createSymbolicLink(tree.resolve('gone.txt'), Paths.get('nowhere.txt'))

        when:
        final DirectoryManifestBuilder.Result result = builder.build(tree)
        final ManifestEntry entry = read(result.cid).entry('gone.txt')

        then:
        entry.mode == 'unresolvable'
        entry.target == 'nowhere.txt'
        entry.address == null
        result.anomalies.unresolvable == 1
        result.anomalies.declined == 0
    }

    @Timeout(30)
    def 'a link cycle does not hang'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'a.txt', 'a')
        Files.createSymbolicLink(tree.resolve('loop'), Paths.get('.'))
        Files.createDirectories(tree.resolve('sub'))
        Files.createSymbolicLink(tree.resolve('sub').resolve('up'), tree.toAbsolutePath())

        when:
        final DirectoryManifest manifest = read(builder.build(tree).cid)

        then: 'the relative loop is kept as a link'
        manifest.entry('loop').mode == 'symlink'
        manifest.entry('loop').target == '.'

        and: 'the absolute one would be followed, and is refused instead of walked forever'
        read(manifest.entry('sub').address).entry('up').mode == 'unresolvable'

        cleanup: 'the loop confuses the temp directory cleanup, so break it here'
        Files.deleteIfExists(work.resolve('tree').resolve('loop'))
        Files.deleteIfExists(work.resolve('tree').resolve('sub').resolve('up'))
    }

    def 'a tree deeper than sixty-four levels is refused rather than truncated'() {
        given:
        Path deep = work.resolve('tree')
        70.times { deep = deep.resolve('d') }
        Files.createDirectories(deep)
        Files.writeString(deep.resolve('a.txt'), 'a')

        when:
        builder.build(work.resolve('tree'))

        then:
        def e = thrown(IOException)
        e.message.contains('64')
    }

    def 'the same tree built twice gives the same address'() {
        given:
        final Path first = work.resolve('first')
        file(first, 'a.txt', 'a')
        file(first, 'nested/detail.txt', 'detail')
        Files.createSymbolicLink(first.resolve('alias.txt'), Paths.get('a.txt'))

        final Path second = work.resolve('second')
        file(second, 'a.txt', 'a')
        file(second, 'nested/detail.txt', 'detail')
        Files.createSymbolicLink(second.resolve('alias.txt'), Paths.get('a.txt'))

        expect:
        builder.build(first).cid == builder.build(second).cid
    }

    def 'creation order does not change the address'() {
        given: 'the same three names, created in opposite orders'
        final Path first = work.resolve('first')
        ['a.txt', 'm.txt', 'z.txt'].each { file(first, it, it) }

        final Path second = work.resolve('second')
        ['z.txt', 'm.txt', 'a.txt'].each { file(second, it, it) }

        expect:
        builder.build(first).cid == builder.build(second).cid
    }

    def 'a manifest re-encoded from its decoded form keeps its address'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'a.txt', 'a')
        file(tree, 'nested/detail.txt', 'detail')
        Files.createSymbolicLink(tree.resolve('alias.txt'), Paths.get('a.txt'))
        Files.createSymbolicLink(tree.resolve('gone.txt'), Paths.get('nowhere'))

        when:
        final Cid cid = builder.build(tree).cid
        final Map decoded = (Map) DagCbor.decode(store.open(cid).bytes)

        then:
        DagCbor.cidOf(DagCbor.encode(DirectoryManifest.fromCbor(decoded).toCbor())) == cid
    }

    def 'every regular file in the tree has its block in the store afterwards'() {
        given:
        final Path tree = work.resolve('tree')
        file(tree, 'a.txt', 'a')
        file(tree, 'b.txt', 'b')
        file(tree, 'nested/detail.txt', 'detail')
        file(tree, 'nested/deeper/more.txt', 'more')

        when:
        builder.build(tree)

        then:
        ['a', 'b', 'detail', 'more'].every { store.has(rawOf(it)) }
    }

    def 'something that is neither a file nor a directory is unresolvable, not dropped'() {
        given: 'a fifo, if this machine can make one'
        final Path tree = work.resolve('tree')
        Files.createDirectories(tree)
        final int status = new ProcessBuilder('mkfifo', tree.resolve('pipe').toString()).start().waitFor()

        when:
        final DirectoryManifestBuilder.Result result = builder.build(tree)

        then:
        status != 0 || read(result.cid).entry('pipe').mode == 'unresolvable'
        status != 0 || result.anomalies.unresolvable == 1
    }

    def 'building something that is not a directory is an error'() {
        given:
        final Path tree = work.resolve('tree')
        final Path a = file(tree, 'a.txt', 'a')

        when:
        builder.build(a)

        then:
        thrown(IOException)
    }
}
