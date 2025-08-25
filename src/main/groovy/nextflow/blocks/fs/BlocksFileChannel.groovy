package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileAttribute
import java.io.ByteArrayOutputStream

/**
 * Implementation of FileChannel for the BlocksFileSystem.
 */
@Slf4j
@CompileStatic
class BlocksFileChannel extends FileChannel {
    private final BlocksPath path
    private final boolean readable
    private final boolean writable
    private boolean isChannelOpen = true
    private long position = 0
    private ByteArrayOutputStream writeBuffer
    private byte[] content
    
    /**
     * Creates a new BlocksFileChannel.
     *
     * @param path The path to the file
     * @param options The open options
     * @param attrs The file attributes
     */
    BlocksFileChannel(BlocksPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) {
        this.path = path
        
        // Parse the options
        readable = !options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.READ)
        writable = options.contains(StandardOpenOption.WRITE) || 
                   options.contains(StandardOpenOption.APPEND) ||
                   options.contains(StandardOpenOption.CREATE) ||
                   options.contains(StandardOpenOption.CREATE_NEW)
                
        // Read the file content if readable
        if (readable) {
            // TODO: Get the file content from the block store
            // For now, use placeholder content
            content = new byte[0]
        }
        
        // Initialize the write buffer if writable
        if (writable) {
            writeBuffer = new ByteArrayOutputStream()
        }
        
        log.debug "Opened file channel for ${path} (readable: ${readable}, writable: ${writable})"
    }
    
    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     */
    @Override
    int read(ByteBuffer dst) throws IOException {
        checkClosed()
        
        if (!readable) {
            throw new IOException("Channel not open for reading")
        }
        
        // Check if we've reached the end of the file
        if (position >= size()) {
            return -1
        }
        
        // Calculate the number of bytes to read
        int remaining = dst.remaining()
        int available = (int) Math.min(remaining, size() - position)
        
        // Copy the bytes from the content to the buffer
        dst.put(content, (int) position, available)
        
        // Update the position
        position += available
        
        return available
    }
    
    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     */
    @Override
    long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        checkClosed()
        
        if (!readable) {
            throw new IOException("Channel not open for reading")
        }
        
        // Calculate the total number of bytes to read
        long totalBytes = 0
        for (int i = 0; i < length; i++) {
            int bytes = read(dsts[offset + i])
            if (bytes <= 0) {
                if (totalBytes == 0) {
                    return bytes
                }
                break
            }
            totalBytes += bytes
        }
        
        return totalBytes
    }
    
    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    @Override
    int write(ByteBuffer src) throws IOException {
        checkClosed()
        
        if (!writable) {
            throw new IOException("Channel not open for writing")
        }
        
        // Get the bytes from the buffer
        int remaining = src.remaining()
        byte[] bytes = new byte[remaining]
        src.get(bytes)
        
        // Write the bytes to the buffer
        writeBuffer.write(bytes)
        
        // Update the position
        position += remaining
        
        return remaining
    }
    
    /**
     * Writes a sequence of bytes to this channel from the given buffers.
     */
    @Override
    long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        checkClosed()
        
        if (!writable) {
            throw new IOException("Channel not open for writing")
        }
        
        // Write each buffer
        long totalBytes = 0
        for (int i = 0; i < length; i++) {
            int bytes = write(srcs[offset + i])
            totalBytes += bytes
        }
        
        return totalBytes
    }
    
    /**
     * Returns the current position of this channel.
     */
    @Override
    long position() throws IOException {
        checkClosed()
        return position
    }
    
    /**
     * Sets the position of this channel.
     */
    @Override
    FileChannel position(long newPosition) throws IOException {
        checkClosed()
        
        if (newPosition < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + newPosition)
        }
        
        this.position = newPosition
        return this
    }
    
    /**
     * Returns the current size of this channel's file.
     */
    @Override
    long size() throws IOException {
        checkClosed()
        
        if (writable) {
            return writeBuffer.size()
        } else {
            return content.length
        }
    }
    
    /**
     * Truncates this channel's file to the given size.
     */
    @Override
    FileChannel truncate(long size) throws IOException {
        checkClosed()
        
        if (!writable) {
            throw new IOException("Channel not open for writing")
        }
        
        // Truncate the buffer
        if (size < writeBuffer.size()) {
            byte[] truncated = new byte[(int) size]
            System.arraycopy(writeBuffer.toByteArray(), 0, truncated, 0, (int) size)
            writeBuffer = new ByteArrayOutputStream()
            writeBuffer.write(truncated)
        }
        
        // Update the position if needed
        if (position > size) {
            position = size
        }
        
        return this
    }
    
    /**
     * Forces any updates to this channel's file to be written to the storage device.
     */
    @Override
    void force(boolean metaData) throws IOException {
        checkClosed()
        
        if (!writable) {
            return
        }
        
        // No need to do anything, since we write to the block store on close
    }
    
    /**
     * Transfers bytes from this channel's file to the given writable byte channel.
     */
    @Override
    long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        checkClosed()
        
        if (!readable) {
            throw new IOException("Channel not open for reading")
        }
        
        // Check bounds
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position)
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative: " + count)
        }
        if (position >= size()) {
            return 0
        }
        
        // Calculate the number of bytes to transfer
        long transferred = Math.min(count, size() - position)
        
        // Create a buffer for the transfer
        ByteBuffer buffer = ByteBuffer.wrap(content, (int) position, (int) transferred)
        
        // Transfer the bytes
        return target.write(buffer)
    }
    
    /**
     * Transfers bytes into this channel's file from the given readable byte channel.
     */
    @Override
    long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        checkClosed()
        
        if (!writable) {
            throw new IOException("Channel not open for writing")
        }
        
        // Check bounds
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position)
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative: " + count)
        }
        
        // Create a buffer for the transfer
        ByteBuffer buffer = ByteBuffer.allocate((int) count)
        
        // Read from the source
        int bytesRead = src.read(buffer)
        if (bytesRead <= 0) {
            return 0
        }
        
        // Prepare the buffer for writing
        buffer.flip()
        
        // Save the current position
        long savedPosition = this.position
        
        try {
            // Set the position for writing
            this.position = position
            
            // Write to this channel
            return write(buffer)
        } finally {
            // Restore the position
            this.position = savedPosition
        }
    }
    
    /**
     * Reads a sequence of bytes from this channel into a buffer, starting at the given file position.
     */
    @Override
    int read(ByteBuffer dst, long position) throws IOException {
        checkClosed()
        
        if (!readable) {
            throw new IOException("Channel not open for reading")
        }
        
        // Check bounds
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position)
        }
        if (position >= size()) {
            return -1
        }
        
        // Save the current position
        long savedPosition = this.position
        
        try {
            // Set the position for reading
            this.position = position
            
            // Read from this channel
            return read(dst)
        } finally {
            // Restore the position
            this.position = savedPosition
        }
    }
    
    /**
     * Writes a sequence of bytes to this channel from a buffer, starting at the given file position.
     */
    @Override
    int write(ByteBuffer src, long position) throws IOException {
        checkClosed()
        
        if (!writable) {
            throw new IOException("Channel not open for writing")
        }
        
        // Check bounds
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + position)
        }
        
        // Save the current position
        long savedPosition = this.position
        
        try {
            // Set the position for writing
            this.position = position
            
            // Write to this channel
            return write(src)
        } finally {
            // Restore the position
            this.position = savedPosition
        }
    }
    
    /**
     * Maps a region of this channel's file directly into memory.
     */
    @Override
    MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Memory-mapped files not supported")
    }
    
    /**
     * Tries to acquire a lock on this channel's file.
     */
    @Override
    FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not supported")
    }
    
    /**
     * Acquires a lock on this channel's file.
     */
    @Override
    FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not supported")
    }
    
    /**
     * Implements the abstract implCloseChannel method from FileChannel.
     * This is called when the channel is closed.
     */
    @Override
    protected void implCloseChannel() throws IOException {
        if (!isChannelOpen) {
            return
        }
        
        // Write the content to the block store if writable
        if (writable && writeBuffer != null && writeBuffer.size() > 0) {
            log.debug "Closing file channel and writing ${writeBuffer.size()} bytes to ${path}"
            
            // Get the block store from the file system
            BlocksFileSystem blocksFs = (BlocksFileSystem) path.fileSystem
            def blockStore = blocksFs.getBlockStore()
            
            // Add the file content to the block store
            byte[] data = writeBuffer.toByteArray()
            def node = blockStore.add(data)
            
            // Track that this file has been written (for existence checks)
            BlocksFileSystemProvider provider = (BlocksFileSystemProvider)path.getFileSystem().provider()
            provider.trackWrittenFile(path.toString())
            provider.trackFileCid(path.toString(), node.hash.toString())
            
            log.debug "Added file ${path} to block store with CID: ${node.hash}"
        }
        
        isChannelOpen = false
    }
    
    /**
     * Checks if the channel is closed and throws an exception if it is.
     */
    private void checkClosed() throws IOException {
        if (!isOpen()) {
            throw new IOException("Channel is closed")
        }
    }
}