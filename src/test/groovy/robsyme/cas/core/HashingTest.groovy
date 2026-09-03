package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §0 rule 2 and §3: hashing streams through one fixed buffer and
 * never holds file content.
 */
class HashingTest extends Specification {

    @TempDir
    Path tempDir

    /** Fails the test if anything but the supplied buffer is read into. */
    private static class OneBufferOnly extends InputStream {
        private final InputStream delegate
        private final byte[] allowed
        int calls = 0

        OneBufferOnly(InputStream delegate, byte[] allowed) {
            this.delegate = delegate
            this.allowed = allowed
        }

        @Override
        int read() {
            throw new AssertionError('a streaming hash must not read one byte at a time' as Object)
        }

        @Override
        int read(byte[] target) {
            throw new AssertionError('a streaming hash must pass an explicit offset and length' as Object)
        }

        @Override
        int read(byte[] target, int off, int len) {
            assert target.is(allowed): 'the hasher allocated a buffer of its own'
            assert len <= allowed.length: "the hasher asked for $len bytes from a ${allowed.length} byte buffer"
            calls++
            return delegate.read(target, off, len)
        }

        @Override
        void close() { delegate.close() }
    }

    def 'hashRaw of "hello\\n" gives the known cid'() {
        given:
        def buffer = new byte[HashBufferPool.BUFFER_SIZE]

        when:
        def cid = Hashing.hashRaw(new ByteArrayInputStream('hello\n'.getBytes('UTF-8')), buffer)

        then:
        cid.toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
        cid.isRaw()
    }

    def 'hashRaw of an empty stream gives the empty cid'() {
        expect:
        Hashing.hashRaw(new ByteArrayInputStream(new byte[0]), new byte[1024]).toString() ==
            'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    }

    def 'a five megabyte file hashes to the same cid as MessageDigest over the file'() {
        given:
        def file = tempDir.resolve('random.bin')
        def random = new Random(42)
        def chunk = new byte[64 * 1024]
        file.withOutputStream { out ->
            80.times { random.nextBytes(chunk); out.write(chunk) }   // 5 MiB
        }

        and: 'the reference digest, computed independently'
        def digest = MessageDigest.getInstance('SHA-256')
        Files.newInputStream(file).withStream { input ->
            def scratch = new byte[8192]
            int n
            while( (n = input.read(scratch, 0, scratch.length)) > 0 )
                digest.update(scratch, 0, n)
        }
        def expected = Cid.of(Cid.RAW, digest.digest())

        expect:
        Files.size(file) == 5L * 1024 * 1024
        Hashing.hashRaw(file) == expected
        Files.newInputStream(file).withStream { Hashing.hashRaw(it, new byte[HashBufferPool.BUFFER_SIZE]) } == expected
    }

    def 'hashRaw reads only into the buffer it was given'() {
        given:
        def buffer = new byte[64 * 1024]
        def bytes = new byte[300 * 1024]
        new Random(7).nextBytes(bytes)
        def spy = new OneBufferOnly(new ByteArrayInputStream(bytes), buffer)

        when:
        def cid = Hashing.hashRaw(spy, buffer)

        then:
        cid == Cid.of(Cid.RAW, MessageDigest.getInstance('SHA-256').digest(bytes))
        spy.calls >= 5
    }

    def 'the shared buffer pool holds 32 buffers of one mebibyte'() {
        expect:
        HashBufferPool.BUFFER_SIZE == 1024 * 1024
        HashBufferPool.CAPACITY == 32
        HashBufferPool.shared().withBuffer { byte[] b -> b.length } == 1024 * 1024
        HashBufferPool.shared().withBuffer { byte[] b -> 'result' } == 'result'
    }

    def 'borrow blocks while the pool is empty and hands back the released buffer'() {
        given:
        def pool = new HashBufferPool(1, 16)
        def first = pool.borrow()
        def started = new CountDownLatch(1)
        def got = new CountDownLatch(1)
        byte[] second = null

        when:
        def waiter = Thread.start {
            started.countDown()
            second = pool.borrow()
            got.countDown()
        }

        then: 'the second borrow is still waiting'
        started.await(2, TimeUnit.SECONDS)
        !got.await(300, TimeUnit.MILLISECONDS)

        when:
        pool.release(first)

        then:
        got.await(2, TimeUnit.SECONDS)
        second.is(first)

        cleanup:
        waiter?.join(2000)
    }

    def 'withBuffer returns the buffer even when the body throws'() {
        given:
        def pool = new HashBufferPool(1, 16)

        when:
        pool.withBuffer { byte[] b -> throw new IllegalStateException('boom') }

        then:
        thrown(IllegalStateException)

        and: 'the buffer is available again'
        pool.borrow() != null
    }

    def 'hashRaw of a path borrows from the shared pool'() {
        given: 'the shared pool, drained'
        def pool = HashBufferPool.shared()
        def held = (1..HashBufferPool.CAPACITY).collect { pool.borrow() }
        def file = tempDir.resolve('small.txt')
        file.text = 'hello\n'
        def done = new CountDownLatch(1)
        Cid cid = null

        when:
        def worker = Thread.start {
            cid = Hashing.hashRaw(file)
            done.countDown()
        }

        then: 'it cannot proceed while every buffer is out on loan'
        !done.await(300, TimeUnit.MILLISECONDS)

        when:
        pool.release(held.remove(0))

        then:
        done.await(5, TimeUnit.SECONDS)
        cid.toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'

        cleanup:
        worker?.join(2000)
        held.each { pool.release(it) }
    }
}
