package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The mechanised form of DESIGN.md §0 rule 2. Runs only under
 * {@code ./gradlew memoryBoundTest}, in a JVM whose whole heap is smaller
 * than the file it hashes and stores, so any attempt to hold file content
 * in memory fails here rather than on somebody's real data.
 *
 * Never weaken this test.
 */
class MemoryBoundTest extends Specification {

    static final long FILE_SIZE = 256L << 20   // 256 MiB

    @Shared
    @TempDir
    Path tempDir

    @Shared
    Path bigFile

    @Shared
    Cid bigFileCid

    def setupSpec() {
        bigFile = tempDir.resolve('big.bin')
        def raf = new RandomAccessFile(bigFile.toFile(), 'rw')
        try {
            raf.setLength(FILE_SIZE)
            raf.seek(0)
            raf.write('nf-blocks memory bound\n'.getBytes('UTF-8'))
            raf.seek(FILE_SIZE - 8)
            raf.write([1, 2, 3, 4, 5, 6, 7, 8] as byte[])
        }
        finally {
            raf.close()
        }
    }

    def 'the heap really is smaller than the file'() {
        expect: 'otherwise everything below could pass while buffering the file'
        Runtime.runtime.maxMemory() < FILE_SIZE
        Files.size(bigFile) == FILE_SIZE
    }

    def 'hashing a file larger than the heap succeeds'() {
        expect:
        Runtime.runtime.maxMemory() < FILE_SIZE

        when:
        bigFileCid = Hashing.hashRaw(bigFile)

        then:
        bigFileCid.isRaw()
        bigFileCid.toString().length() == 59

        and: 'hashing it again gives the same address'
        Hashing.hashRaw(bigFile) == bigFileCid
    }

    def 'storing a file larger than the heap succeeds'() {
        given:
        def store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)

        expect:
        Runtime.runtime.maxMemory() < FILE_SIZE

        when:
        Cid cid = Files.newInputStream(bigFile).withCloseable { store.putStreaming(it) }

        then: 'the store computed the same address as the hasher'
        cid == bigFileCid

        and: 'and the block holds every byte'
        store.has(cid)
        store.size(cid) == FILE_SIZE
        Files.size(store.blockPath(cid)) == FILE_SIZE

        when: 'the block is read back through a fixed buffer'
        long total = 0
        def buffer = new byte[64 * 1024]
        store.open(cid).withCloseable { InputStream input ->
            int n
            while( (n = input.read(buffer, 0, buffer.length)) > 0 )
                total += n
        }

        then:
        total == FILE_SIZE
    }
}
