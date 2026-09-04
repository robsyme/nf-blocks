package robsyme.cas.nio

import java.nio.ByteBuffer
import java.nio.channels.NonWritableChannelException
import java.nio.channels.SeekableByteChannel

import groovy.transform.CompileStatic

/**
 * A read-only, forward-only {@link SeekableByteChannel} over a block's stream,
 * used only when a block does not live on a local filesystem (an object-store
 * member, later). Local blocks are opened as real file channels instead, so
 * this never buffers a whole file: it reads through the underlying stream.
 *
 * Backward seeks are refused rather than silently faked, so a caller that needs
 * random access gets a clear failure instead of wrong bytes.
 */
@CompileStatic
class InputStreamByteChannel implements SeekableByteChannel {

    private final InputStream input
    private final long size
    private long position = 0
    private boolean open = true

    InputStreamByteChannel(InputStream input, long size) {
        this.input = input
        this.size = size
    }

    @Override
    int read(ByteBuffer dst) throws IOException {
        if( !open ) throw new java.nio.channels.ClosedChannelException()
        final byte[] tmp = new byte[Math.min(dst.remaining(), 8192)]
        final int n = input.read(tmp, 0, tmp.length)
        if( n < 0 )
            return -1
        dst.put(tmp, 0, n)
        position += n
        return n
    }

    @Override
    int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException()
    }

    @Override long position() throws IOException { position }

    @Override
    SeekableByteChannel position(long newPosition) throws IOException {
        if( newPosition == position )
            return this
        if( newPosition < position )
            throw new IOException("cas: a block channel cannot seek backwards (from ${position} to ${newPosition})")
        long toSkip = newPosition - position
        while( toSkip > 0 ) {
            final long skipped = input.skip(toSkip)
            if( skipped <= 0 ) {
                if( input.read() < 0 ) break
                toSkip--
                position++
            }
            else {
                toSkip -= skipped
                position += skipped
            }
        }
        return this
    }

    @Override long size() throws IOException { size }

    @Override
    SeekableByteChannel truncate(long s) throws IOException {
        throw new NonWritableChannelException()
    }

    @Override boolean isOpen() { open }

    @Override
    void close() throws IOException {
        open = false
        input.close()
    }
}
