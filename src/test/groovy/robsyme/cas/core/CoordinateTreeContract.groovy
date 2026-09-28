package robsyme.cas.core

import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException

import spock.lang.Specification

/** What every CoordinateTree does, so the S3 member and a local one cannot drift (ticket 02 decision 6). */
abstract class CoordinateTreeContract extends Specification {

    abstract CoordinateTree tree()

    static final StoreRef FILE = StoreRef.parse('cas://bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am/A.bam')
    static final StoreRef DIR = StoreRef.parse('cas://bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua/A_qc')

    def 'a written pointer reads back; the last write to one coordinate wins (ticket 03 decision 3)'() {
        given:
        final CoordinateTree t = tree()

        when:
        t.write('aligned/A/A.bam', DIR)
        t.write('aligned/A/A.bam', FILE)

        then:
        t.read('aligned/A/A.bam') == Optional.of(FILE)
        t.exists('aligned/A/A.bam') && t.exists('aligned/A') && t.exists('aligned')
        t.isDirectory('aligned/A') && !t.isDirectory('aligned/A/A.bam')
        t.children('aligned') == ['A']
        t.children('aligned/A') == ['A.bam']
        !t.exists('nope') && t.read('nope') == Optional.empty()
    }

    def 'a directory coordinate is a pointer at a manifest'() {
        given:
        final CoordinateTree t = tree()
        t.write('qc/A/A_qc', DIR)

        expect:
        t.isDirectoryCoordinate('qc/A/A_qc')
        t.isDirectoryCoordinate('qc/A')
        !t.isDirectoryCoordinate('qc/B')
    }

    def 'a pointer at a blocks a pointer at a/b'() {
        given:
        final CoordinateTree t = tree()
        t.write('a', FILE)

        when:
        t.write('a/b', FILE)

        then:
        thrown(FileAlreadyExistsException)
        t.read('a') == Optional.of(FILE)
    }

    def 'a non-empty a/ blocks a pointer at a'() {
        given:
        final CoordinateTree t = tree()
        t.write('a/b', FILE)

        when:
        t.write('a', FILE)

        then:
        thrown(DirectoryNotEmptyException)
        t.read('a/b') == Optional.of(FILE)
    }

    def 'delete removes a pointer only, and refuses a directory'() {
        given:
        final CoordinateTree t = tree()
        t.write('d/x', FILE)

        expect:
        !t.delete('d/y')
        t.delete('d/x')
        !t.exists('d/x')

        when:
        t.write('e/f', FILE)
        t.delete('e')

        then:
        thrown(IOException)
    }
}
