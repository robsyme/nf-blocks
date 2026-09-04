package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

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

    /** Computed here, streamed through MessageDigest, so no feature depends on another. */
    @Shared
    Cid expectedCid

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
        final MessageDigest digest = MessageDigest.getInstance('SHA-256')
        final byte[] scratch = new byte[64 * 1024]
        Files.newInputStream(bigFile).withCloseable { InputStream input ->
            int n
            while( (n = input.read(scratch, 0, scratch.length)) != -1 )
                digest.update(scratch, 0, n)
        }
        expectedCid = Cid.of(Cid.RAW, digest.digest())
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
        def cid = Hashing.hashRaw(bigFile)

        then: 'the same address MessageDigest gives for the same bytes'
        cid == expectedCid
        cid.isRaw()
        cid.toString().length() == 59

        and: 'hashing it again gives the same address'
        Hashing.hashRaw(bigFile) == cid
    }

    def 'storing a file larger than the heap succeeds'() {
        given:
        def store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)

        expect:
        Runtime.runtime.maxMemory() < FILE_SIZE

        when:
        Cid cid = Files.newInputStream(bigFile).withCloseable { store.putStreaming(it) }

        then: 'the store computed the same address as the reference digest'
        cid == expectedCid

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
