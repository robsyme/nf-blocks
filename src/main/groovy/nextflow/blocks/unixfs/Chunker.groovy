package nextflow.blocks.unixfs

import groovy.transform.CompileStatic
import java.nio.ByteBuffer

/**
 * Interface for file chunking strategies
 */
@CompileStatic
interface Chunker {
    /**
     * Returns the next chunk of data or null if there are no more chunks
     */
    byte[] nextChunk()
    
    /**
     * Resets the chunker to start from the beginning
     */
    void reset()
}

/**
 * Factory for creating chunkers
 */
@CompileStatic
class ChunkerFactory {
    /**
     * Creates a fixed-size chunker with the given chunk size
     */
    static Chunker fixedSize(byte[] data, int chunkSize) {
        return new FixedSizeChunker(data, chunkSize)
    }
    
    /**
     * Creates a fixed-size chunker with the given chunk size
     */
    static Chunker fixedSize(InputStream inputStream, int chunkSize) {
        return new FixedSizeStreamChunker(inputStream, chunkSize)
    }
}

/**
 * A simple fixed-size chunker for byte arrays
 */
@CompileStatic
class FixedSizeChunker implements Chunker {
    private final byte[] data
    private final int chunkSize
    private int position = 0
    
    FixedSizeChunker(byte[] data, int chunkSize) {
        this.data = data
        this.chunkSize = chunkSize
    }
    
    @Override
    byte[] nextChunk() {
        if (position >= data.length) {
            return null
        }
        
        int remaining = data.length - position
        int size = Math.min(chunkSize, remaining)
        byte[] chunk = new byte[size]
        System.arraycopy(data, position, chunk, 0, size)
        position += size
        return chunk
    }
    
    @Override
    void reset() {
        position = 0
    }
}

/**
 * A fixed-size chunker for input streams
 */
@CompileStatic
class FixedSizeStreamChunker implements Chunker {
    private final InputStream inputStream
    private final int chunkSize
    private boolean finished = false
    
    FixedSizeStreamChunker(InputStream inputStream, int chunkSize) {
        this.inputStream = inputStream
        this.chunkSize = chunkSize
    }
    
    @Override
    byte[] nextChunk() {
        if (finished) {
            return null
        }
        
        byte[] chunk = new byte[chunkSize]
        int bytesRead = 0
        
        while (bytesRead < chunkSize) {
            int result = inputStream.read(chunk, bytesRead, chunkSize - bytesRead)
            if (result == -1) {
                // End of stream
                finished = true
                break
            }
            bytesRead += result
        }
        
        if (bytesRead == 0) {
            return null
        }
        
        if (bytesRead < chunkSize) {
            // Create a new array with the exact size
            byte[] exactChunk = new byte[bytesRead]
            System.arraycopy(chunk, 0, exactChunk, 0, bytesRead)
            return exactChunk
        }
        
        return chunk
    }
    
    @Override
    void reset() {
        try {
            if (inputStream.markSupported()) {
                inputStream.reset()
                finished = false
            } else {
                throw new UnsupportedOperationException("This input stream does not support reset")
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset input stream", e)
        }
    }
} 