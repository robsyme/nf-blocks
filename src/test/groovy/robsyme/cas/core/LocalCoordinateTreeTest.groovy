package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §5, §7: the Pointer File tree under `coords/`. */
class LocalCoordinateTreeTest extends Specification {

    @TempDir
    Path root

    LocalCoordinateTree tree

    def setup() {
        tree = new LocalCoordinateTree(root.resolve('coords'))
    }

    private static Cid raw(String text) {
        Hashing.hashRaw(new ByteArrayInputStream(text.getBytes('UTF-8')), new byte[1024])
    }

    private static Cid manifest(int entries) {
        DagCbor.cidOf(DagCbor.encode([kind: 'DirectoryManifest', schema: 1, entries: (1..<(entries + 1)).collect { it as String }]))
    }

    def 'a written coordinate reads back'() {
        given:
        final StoreRef ref = new StoreRef(raw('one'), 'A.bam')

        when:
        tree.write('aligned/A/A.bam', ref)

        then:
        tree.read('aligned/A/A.bam').get() == ref
        tree.exists('aligned/A/A.bam')
    }

    def 'writing creates the parent directories'() {
        when:
        tree.write('qc/deep/deeper/report.html', new StoreRef(raw('r'), 'report.html'))

        then:
        Files.isDirectory(root.resolve('coords').resolve('qc').resolve('deep').resolve('deeper'))
    }

    def 'writing the same coordinate again replaces the pointer'() {
        given:
        tree.write('aligned/A.bam', new StoreRef(raw('one'), 'A.bam'))

        when:
        final StoreRef second = new StoreRef(raw('two'), 'A.bam')
        tree.write('aligned/A.bam', second)

        then:
        tree.read('aligned/A.bam').get() == second

        and: 'one line, not two'
        Files.readAllLines(root.resolve('coords').resolve('aligned').resolve('A.bam')).size() == 1
    }

    def 'a missing coordinate reads as empty'() {
        expect:
        tree.read('aligned/nothing.bam') == Optional.empty()
        !tree.exists('aligned/nothing.bam')
    }

    def 'a pointer naming a dag-cbor address is a directory coordinate'() {
        given:
        tree.write('qc', new StoreRef(manifest(2), 'qc'))
        tree.write('aligned/A.bam', new StoreRef(raw('one'), 'A.bam'))

        expect:
        tree.isDirectoryCoordinate('qc')
        !tree.isDirectoryCoordinate('aligned/A.bam')

        and: 'so is an intermediate segment, which is a real directory'
        tree.isDirectoryCoordinate('aligned')

        and:
        !tree.isDirectoryCoordinate('nothing')
    }

    def 'children lists the entries of a coordinate directory'() {
        given:
        tree.write('aligned/b.bam', new StoreRef(raw('b'), 'b.bam'))
        tree.write('aligned/a.bam', new StoreRef(raw('a'), 'a.bam'))
        tree.write('aligned/sub/c.bam', new StoreRef(raw('c'), 'c.bam'))

        expect:
        tree.children('aligned') == ['a.bam', 'b.bam', 'sub']
        tree.children('aligned/sub') == ['c.bam']

        and: 'a pointer file has no children of its own; the manifest names them'
        tree.children('aligned/a.bam') == []
        tree.children('nothing') == []
    }

    def 'the root of the tree is a coordinate directory too'() {
        given:
        tree.write('aligned/a.bam', new StoreRef(raw('a'), 'a.bam'))

        expect:
        tree.children('') == ['aligned']
        tree.isDirectoryCoordinate('')
    }

    def 'delete removes the pointer file and nothing else'() {
        given:
        tree.write('aligned/A.bam', new StoreRef(raw('one'), 'A.bam'))

        when:
        final boolean deleted = tree.delete('aligned/A.bam')

        then:
        deleted
        !tree.exists('aligned/A.bam')

        and: 'the directory that held it stays'
        Files.isDirectory(root.resolve('coords').resolve('aligned'))

        and: 'deleting again is not an error'
        !tree.delete('aligned/A.bam')
    }

    def 'delete refuses to remove a coordinate directory'() {
        given:
        tree.write('aligned/A.bam', new StoreRef(raw('one'), 'A.bam'))

        when:
        tree.delete('aligned')

        then:
        thrown(IOException)
        Files.isDirectory(root.resolve('coords').resolve('aligned'))
    }

    def 'a coordinate path is normalised the way a key is'() {
        given:
        tree.write('aligned/A/A.bam', new StoreRef(raw('one'), 'A.bam'))

        expect:
        tree.exists('/aligned/./A/../A/A.bam')
        tree.read('aligned//A//A.bam').isPresent()
    }

    def 'a coordinate path may not escape the tree'() {
        when:
        tree.write('../outside.txt', new StoreRef(raw('one'), 'outside.txt'))

        then:
        thrown(IllegalArgumentException)
        !Files.exists(root.resolve('outside.txt'))
    }

    @Requires({ System.getProperty('user.name') != 'root' })
    def 'an unreadable pointer surfaces an error rather than reading as absent'() {
        given:
        final Path pointer = root.resolve('coords').resolve('aligned').resolve('A.bam')
        Files.createDirectories(pointer.parent)
        Files.writeString(pointer, new StoreRef(raw('one'), 'A.bam').toString() + '\n')
        Files.setPosixFilePermissions(pointer, [] as Set)

        when:
        tree.read('aligned/A.bam')

        then: 'a permission error is not the same as absent'
        thrown(IOException)

        cleanup:
        Files.setPosixFilePermissions(
            root.resolve('coords').resolve('aligned').resolve('A.bam'),
            java.nio.file.attribute.PosixFilePermissions.fromString('rw-------'))
    }

    def 'a pointer file holding nonsense is reported with its path'() {
        given:
        final Path pointer = root.resolve('coords').resolve('aligned').resolve('A.bam')
        Files.createDirectories(pointer.parent)
        Files.writeString(pointer, 'not a store uri\n')

        when:
        tree.read('aligned/A.bam')

        then:
        def e = thrown(IOException)
        e.message.contains('aligned/A.bam')
    }
}
