package robsyme.cas.core

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore

import groovy.transform.CompileStatic

/**
 * The memory bound of DESIGN.md §0 rule 2 made concrete: at most
 * {@link #CAPACITY} buffers of {@link #BUFFER_SIZE} bytes exist at once, so
 * at most that many hashes run concurrently. {@link #borrow} blocks rather
 * than allocating a 33rd buffer. Buffers are allocated on first use and kept
 * for reuse, so a run that hashes one file at a time costs 1 MiB.
 */
@CompileStatic
class HashBufferPool {

    static final int BUFFER_SIZE = 1024 * 1024
    static final int CAPACITY = 32

    private static final HashBufferPool SHARED = new HashBufferPool(CAPACITY, BUFFER_SIZE)

    private final Semaphore permits
    private final ConcurrentLinkedQueue<byte[]> free = new ConcurrentLinkedQueue<>()
    /** The buffers currently out on loan, compared by identity. */
    private final Set<byte[]> loaned = Collections.newSetFromMap(
            Collections.synchronizedMap(new IdentityHashMap<byte[], Boolean>()))
    private final int bufferSize

    HashBufferPool(int capacity, int bufferSize) {
        if( capacity < 1 ) throw new IllegalArgumentException("pool capacity must be positive: $capacity")
        if( bufferSize < 1 ) throw new IllegalArgumentException("buffer size must be positive: $bufferSize")
        this.permits = new Semaphore(capacity, true)
        this.bufferSize = bufferSize
    }

    static HashBufferPool shared() { SHARED }

    /** Takes a buffer, waiting until one is free. */
    byte[] borrow() {
        permits.acquire()
        byte[] buffer = free.poll()
        if( buffer == null )
            buffer = new byte[bufferSize]
        loaned.add(buffer)
        return buffer
    }

    /**
     * Gives a buffer back. A buffer this pool never lent, or one that was
     * already given back, is refused without inventing a permit; a buffer
     * that was lent always returns its permit, even if the pool declines to
     * keep it, because a lost permit shrinks the pool for the whole run.
     */
    void release(byte[] buffer) {
        if( buffer == null || !loaned.remove(buffer) )
            throw new IllegalArgumentException('this buffer was not borrowed from this pool, or has already been released')
        try {
            if( buffer.length == bufferSize )
                free.offer(buffer)
        }
        finally {
            permits.release()
        }
    }

    def <T> T withBuffer(Closure<T> action) {
        byte[] buffer = borrow()
        try {
            return action.call(buffer)
        }
        finally {
            release(buffer)
        }
    }
}
