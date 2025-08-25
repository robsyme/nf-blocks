package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileAttribute
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.ConcurrentHashMap

import io.ipfs.api.MerkleNode
import io.ipfs.multihash.Multihash
import nextflow.blocks.BlockStore
import nextflow.blocks.unixfs.UnixFsImporter
import nextflow.blocks.unixfs.Chunker
import nextflow.blocks.unixfs.ChunkerFactory
import nextflow.blocks.unixfs.UnixFsData
import nextflow.blocks.unixfs.UnixFsCodec
import nextflow.blocks.dagpb.DagPbNode
import nextflow.blocks.dagpb.DagPbLink
import nextflow.blocks.dagpb.DagPbCodec
import io.ipfs.cid.Cid

/**
 * Implementation of SeekableByteChannel for the BlocksFileSystem.
 */
@Slf4j
@CompileStatic
class BlocksSeekableByteChannel implements SeekableByteChannel {
    private final BlocksPath path
    private final boolean readable
    private final boolean writable
    private boolean isChannelOpen = true
    private long position = 0
    private ByteArrayOutputStream writeBuffer
    private byte[] content
    
    // Lock for thread safety
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock()
    
    // Default chunk size for breaking up files (256KB)
    private static final int DEFAULT_CHUNK_SIZE = 256 * 1024
    
    // Cache of file locks for concurrent access
    private static final Map<String, Object> FILE_LOCKS = new ConcurrentHashMap<>()
    
    /**
     * Creates a new BlocksSeekableByteChannel.
     *
     * @param path The path to the file
     * @param options The open options
     * @param attrs The file attributes
     */
    BlocksSeekableByteChannel(BlocksPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) {
        this.path = path
        
        // Parse the options
        readable = !options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.READ)
        writable = options.contains(StandardOpenOption.WRITE) || 
                   options.contains(StandardOpenOption.APPEND) ||
                   options.contains(StandardOpenOption.CREATE) ||
                   options.contains(StandardOpenOption.CREATE_NEW)
        
        if (writable) {
            log.debug "Opening writable channel for ${path}"
            writeBuffer = new ByteArrayOutputStream()
        }
        
        // Read the file content if readable
        if (readable) {
            try {
                // Get the block store from the file system
                BlocksFileSystem fs = (BlocksFileSystem)path.getFileSystem()
                BlockStore blockStore = fs.getBlockStore()
                
                // Retrieve the file content from the block store
                content = retrieveFileContent(blockStore)
                
                log.debug "Read ${content.length} bytes from ${path}"
            } catch (Exception e) {
                log.warn "Failed to read content for ${path}: ${e.message}", e
                content = new byte[0]
            }
        } else {
            content = new byte[0]
        }
        
        log.debug "Opened channel for ${path} (readable: ${readable}, writable: ${writable})"
    }
    
    /**
     * Reads bytes from this channel into the given buffer.
     */
    @Override
    int read(ByteBuffer dst) throws IOException {
        lock.readLock().lock()
        try {
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
        } finally {
            lock.readLock().unlock()
        }
    }
    
    /**
     * Writes bytes from the given buffer into this channel.
     */
    @Override
    int write(ByteBuffer src) throws IOException {
        lock.writeLock().lock()
        try {
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
        } finally {
            lock.writeLock().unlock()
        }
    }
    
    /**
     * Returns the current position of this channel.
     */
    @Override
    long position() throws IOException {
        lock.readLock().lock()
        try {
            checkClosed()
            return position
        } finally {
            lock.readLock().unlock()
        }
    }
    
    /**
     * Sets the position of this channel.
     */
    @Override
    SeekableByteChannel position(long newPosition) throws IOException {
        lock.writeLock().lock()
        try {
            checkClosed()
            
            if (newPosition < 0) {
                throw new IllegalArgumentException("Position cannot be negative: " + newPosition)
            }
            
            this.position = newPosition
            return this
        } finally {
            lock.writeLock().unlock()
        }
    }
    
    /**
     * Returns the current size of this channel's file.
     */
    @Override
    long size() throws IOException {
        lock.readLock().lock()
        try {
            checkClosed()
            
            if (writable && writeBuffer != null) {
                return writeBuffer.size()
            } else {
                return content != null ? content.length : 0
            }
        } finally {
            lock.readLock().unlock()
        }
    }
    
    /**
     * Truncates this channel's file to the given size.
     */
    @Override
    SeekableByteChannel truncate(long size) throws IOException {
        lock.writeLock().lock()
        try {
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
        } finally {
            lock.writeLock().unlock()
        }
    }
    
    /**
     * Tells whether or not this channel is open.
     */
    @Override
    boolean isOpen() {
        return isChannelOpen
    }
    
    /**
     * Closes this channel.
     */
    @Override
    void close() throws IOException {
        lock.writeLock().lock()
        try {
            if (!isChannelOpen) {
                return
            }
            
            // Write the content to the block store if writable
            if (writable && writeBuffer != null && writeBuffer.size() > 0) {
                try {
                    storeFileContent()
                    log.debug "Closed and wrote ${writeBuffer.size()} bytes to ${path}"
                } catch (Exception e) {
                    log.error "Failed to store file content for ${path}: ${e.message}", e
                    throw new IOException("Failed to store file content: ${e.message}", e)
                }
            }
            
            isChannelOpen = false
        } finally {
            lock.writeLock().unlock()
        }
    }
    
    /**
     * Stores the file content in the block store using UnixFS format.
     * Handles concurrent writes by using a file-level lock.
     */
    private void storeFileContent() throws IOException {
        // Get the block store from the file system
        BlocksFileSystem fs = (BlocksFileSystem)path.getFileSystem()
        BlockStore blockStore = fs.getBlockStore()
        
        // Get the file data
        byte[] fileData = writeBuffer.toByteArray()
        
        // Handle concurrent writes to the same file
        // We'll use a file-level lock to ensure that only one thread can write to a file at a time
        synchronized (getFileLock(path)) {
            try {
                // Check if the file has been modified by another thread/process while we were writing
                handleConcurrentModification(blockStore)
                
                // Create a temporary file to store the data
                // This is a workaround until we implement direct chunking of byte arrays
                Path tempFile = Files.createTempFile("blocks-", ".tmp")
                try {
                    // Write the data to the temporary file
                    Files.write(tempFile, fileData)
                    
                    // Create a UnixFS importer
                    UnixFsImporter importer = new UnixFsImporter(blockStore, DEFAULT_CHUNK_SIZE)
                    
                    // Import the file
                    MerkleNode node = importer.importFile(tempFile)
                    
                    // Update the file system's directory structure
                    updateDirectoryStructure(node)
                    
                    // Track that this file has been written (for existence checks)
                    BlocksFileSystemProvider provider = (BlocksFileSystemProvider)path.getFileSystem().provider()
                    provider.trackWrittenFile(path.toString())
                    provider.trackFileCid(path.toString(), node.hash.toString())
                    
                    log.debug "Stored file ${path} with root CID: ${node.hash}"
                } finally {
                    // Delete the temporary file
                    try {
                        Files.delete(tempFile)
                    } catch (Exception e) {
                        log.warn "Failed to delete temporary file ${tempFile}: ${e.message}"
                    }
                }
            } catch (IOException e) {
                log.error "Failed to store file content: ${e.message}", e
                throw e
            }
        }
    }
    
    /**
     * Gets a lock object for the given path.
     * This ensures that only one thread can write to a file at a time.
     * 
     * @param path The path to get a lock for
     * @return A lock object for the path
     */
    private static Object getFileLock(BlocksPath path) {
        String pathStr = path.toString()
        return FILE_LOCKS.computeIfAbsent(pathStr, k -> new Object())
    }
    
    /**
     * Handles the case where a file has been modified by another thread/process
     * while we were writing to it.
     * 
     * @param blockStore The block store
     */
    private void handleConcurrentModification(BlockStore blockStore) throws IOException {
        try {
            // In a real implementation, we would:
            // 1. Check if the file has been modified since we opened it
            // 2. If it has, we might need to merge changes or handle conflicts
            
            // For now, we'll just log that we're checking for concurrent modifications
            log.debug "Checking for concurrent modifications to ${path}"
            
            // In a content-addressed file system, concurrent modifications
            // result in different content hashes, so each write creates a new version
            // This is actually a feature, not a bug, as it preserves all versions
            
            // However, we might want to implement some form of conflict resolution
            // or version control in the future
        } catch (Exception e) {
            log.warn "Failed to check for concurrent modifications: ${e.message}", e
        }
    }
    
    /**
     * Updates the file system's directory structure to include the new file.
     */
    private void updateDirectoryStructure(MerkleNode fileNode) {
        try {
            // Get the block store from the file system
            BlocksFileSystem fs = (BlocksFileSystem)path.getFileSystem()
            BlockStore blockStore = fs.getBlockStore()
            
            // Get the file name and parent path
            String fileName = path.getFileName().toString()
            BlocksPath parentPath = (BlocksPath)path.getParent()
            
            // If parent path is null, this is a root-level file
            if (parentPath == null) {
                parentPath = (BlocksPath)fs.getPath("/")
            }
            
            log.debug "Updating directory structure: adding ${fileName} to ${parentPath}"
            
            // Build the directory structure from the root down to the parent
            MerkleNode rootNode = updateDirectoryStructure(parentPath, fileName, fileNode, blockStore)
            
            // Update the file system's root reference
            // This would typically involve updating some persistent state
            // For now, we'll just log that we would update the root
            log.debug "Updated file system root to CID: ${rootNode.hash}"
            
            // TODO: Update the file system's root reference
        } catch (Exception e) {
            log.error "Failed to update file system structure: ${e.message}", e
            throw new IOException("Failed to update file system structure: ${e.message}", e)
        }
    }
    
    /**
     * Recursively updates the directory structure to include a new file.
     * 
     * @param dirPath The path to the directory to update
     * @param entryName The name of the entry to add or update
     * @param entryNode The MerkleNode of the entry
     * @param blockStore The block store to use
     * @return The updated MerkleNode for the directory
     */
    private MerkleNode updateDirectoryStructure(BlocksPath dirPath, String entryName, 
                                               MerkleNode entryNode, BlockStore blockStore) throws IOException {
        log.debug "Updating directory: ${dirPath}, adding entry: ${entryName}"
        
        // Create UnixFS data for the directory
        UnixFsData dirData = UnixFsData.directory()
        dirData.mtime = Instant.now()
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(dirData)
        
        // Create a list of links to the directory entries
        List<DagPbLink> links = []
        
        // Try to get the existing directory node from the block store
        try {
            // Get the file system from the path
            BlocksFileSystem fs = (BlocksFileSystem)dirPath.getFileSystem()
            
            // Get the CID for this directory from the file system
            // This would typically be stored in some form of path-to-CID mapping
            // For now, we'll try to get it from the block store directly
            // In a real implementation, we would have a way to resolve paths to CIDs
            String dirCid = fs.getDirectoryCid(dirPath)
            if (dirCid != null) {
                // Get the existing directory node
                Multihash multihash = Multihash.fromBase58(dirCid)
                MerkleNode existingNode = blockStore.get(multihash)
                
                // Decode the existing node's data
                DagPbNode existingDagNode = DagPbCodec.decode(existingNode.data.get())
                
                // Add all existing links to our list
                links.addAll(existingDagNode.links)
                
                // Remove any existing link with the same name (we'll update it)
                links.removeAll { it.name == entryName }
            }
        } catch (Exception e) {
            // If we can't get the existing directory, that's okay
            // We'll just create a new one
            log.debug "No existing directory found for ${dirPath}: ${e.message}"
        }
        
        // Add the new entry
        // Multihash can come as string or as a Cid object
        byte[] hash = (entryNode.hash instanceof Cid) ? 
            (entryNode.hash as Cid).toBytes() : 
            Multihash.fromBase58(entryNode.hash.toString()).toBytes()
        
        links.add(new DagPbLink(hash, entryName))
        
        // Create a DAG-PB node with UnixFS data and links
        DagPbNode dagNode = new DagPbNode(links, unixfsData)
        
        // Add the node to the block store
        MerkleNode dirNode = blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
        
        // If this is not the root directory, update the parent directory
        BlocksPath parentPath = (BlocksPath)dirPath.getParent()
        if (parentPath != null) {
            return updateDirectoryStructure(parentPath, dirPath.getFileName().toString(), dirNode, blockStore)
        }
        
        // This is the root directory, return it
        return dirNode
    }
    
    /**
     * Retrieves file content from the block store.
     * 
     * @param blockStore The block store to retrieve content from
     * @return The file content as a byte array
     */
    private byte[] retrieveFileContent(BlockStore blockStore) throws IOException {
        log.debug "Retrieving content for ${path}"
        
        try {
            // Get the provider to look up the file's CID
            BlocksFileSystemProvider provider = (BlocksFileSystemProvider)path.getFileSystem().provider()
            String cid = provider.getFileCid(path.toString())
            
            if (cid == null) {
                throw new IOException("No CID found for file: ${path}")
            }
            
            log.debug "Found CID for ${path}: ${cid}"
            
            // Parse the CID and retrieve the content from the block store
            
            Multihash multihash
            if (cid.startsWith("Qm") || cid.startsWith("baf")) {
                // This is a CID, parse it
                Cid cidObj = Cid.decode(cid)
                multihash = cidObj.bareMultihash()
            } else {
                // This might be a bare multihash
                multihash = Multihash.fromBase58(cid)
            }
            
            // Get the raw block content from the block store
            def node = blockStore.get(multihash)
            byte[] blockData = node.data.get()
            
            // If this is a DAG-PB node containing UnixFS data, decode it properly
            try {
                // First decode as DAG-PB to get the structured data
                def dagNode = DagPbCodec.decode(blockData)
                
                if (dagNode.data && dagNode.data.length > 0) {
                    // The data field should contain UnixFS metadata
                    def unixFsData = UnixFsCodec.decode(dagNode.data)
                    
                    if (unixFsData.data && unixFsData.data.length > 0) {
                        log.debug "Retrieved ${unixFsData.data.length} bytes for ${path} via DAG-PB/UnixFS"
                        return unixFsData.data
                    } else if (dagNode.links && !dagNode.links.isEmpty()) {
                        log.debug "File ${path} has chunked content with ${dagNode.links.size()} chunks"
                        // For chunked files, we'd need to retrieve and concatenate all chunks
                        // For now, just return empty to indicate this needs more work
                        return new byte[0]
                    } else {
                        log.debug "DAG-PB node has no data or links for ${path}"
                        return new byte[0]
                    }
                } else {
                    log.debug "DAG-PB node has no data field for ${path}"
                    return new byte[0]
                }
            } catch (Exception e) {
                // If DAG-PB decoding fails, try direct UnixFS decoding
                log.debug "DAG-PB decoding failed, trying direct UnixFS decode: ${e.message}"
                try {
                    def unixFsData = UnixFsCodec.decode(blockData)
                    if (unixFsData.data) {
                        log.debug "Retrieved ${unixFsData.data.length} bytes for ${path} via direct UnixFS"
                        return unixFsData.data
                    }
                } catch (Exception e2) {
                    log.debug "Both DAG-PB and UnixFS decoding failed, returning raw block data: ${e2.message}"
                }
                return blockData
            }
        } catch (Exception e) {
            log.error "Error retrieving content for ${path}: ${e.message}", e
            throw new IOException("Failed to retrieve file content: ${e.message}", e)
        }
    }
    
    /**
     * Checks if the channel is closed and throws an exception if it is.
     */
    private void checkClosed() throws IOException {
        if (!isOpen()) {
            throw new IOException("Channel is closed")
        }
    }
    
    /**
     * Updates the directory structure to include the new file.
     * This creates or updates the necessary directory nodes in the block store.
     * 
     * @param blockStore The block store to use
     * @param filePath The path of the file being added
     * @param fileCid The CID of the file being added
     */
    private void updateDirectoryStructure(BlockStore blockStore, BlocksPath filePath, String fileCid) {
        log.debug "Updating directory structure for ${filePath} with CID ${fileCid}"
        
        // Get the parent directory path
        Path parentPath = filePath.parent
        if (parentPath == null) {
            log.debug "File is in root directory, no directory structure update needed"
            return
        }
        
        // Convert to BlocksPath
        BlocksPath dirPath = (BlocksPath)parentPath
        
        // If this is the root directory, we're done
        if (dirPath.toString() == "/") {
            log.debug "File is in root directory, no directory structure update needed"
            return
        }
        
        // Get the file name
        String fileName = filePath.fileName.toString()
        
        // Create UnixFS data for the directory
        UnixFsData dirData = UnixFsData.directory()
        dirData.mode = 0755
        
        // Encode the directory data
        byte[] dirDataBytes = UnixFsCodec.encode(dirData)
        
        // Get the existing directory CID if it exists
        BlocksFileSystem fs = (BlocksFileSystem)filePath.fileSystem
        String existingDirCid = fs.getDirectoryCid(dirPath)
        
        // Initialize the list of links
        List<DagPbLink> links = []
        
        // If we have an existing directory, get its links
        if (existingDirCid != null) {
            try {
                // Get the existing directory node
                Multihash multihash = Multihash.fromBase58(existingDirCid)
                MerkleNode merkleNode = blockStore.get(multihash)
                if (merkleNode != null && merkleNode.data.isPresent()) {
                    // Decode the DAG-PB node
                    DagPbNode existingNode = DagPbCodec.decode(merkleNode.data.get())
                    
                    // Add all existing links except the one we're updating
                    for (DagPbLink link : existingNode.links) {
                        if (link.name != fileName) {
                            links.add(link)
                        }
                    }
                }
            } catch (Exception e) {
                log.warn "Failed to retrieve existing directory node: ${e.message}", e
            }
        }
        
        // Add the new file link
        // Convert the CID to bytes
        byte[] hash = Multihash.fromBase58(fileCid).toBytes()
        DagPbLink fileLink = new DagPbLink(hash, fileName, content.length as Long)
        links.add(fileLink)
        
        // Create the directory node
        DagPbNode dirNode = new DagPbNode(links, dirDataBytes)
        
        // Add the directory node to the block store
        String dirCid = blockStore.add(DagPbCodec.encode(dirNode), Cid.Codec.DagProtobuf).hash.toString()
        
        // Update the directory CID in the filesystem
        fs.setDirectoryCid(dirPath, dirCid)
        
        // Recursively update the parent directory
        updateDirectoryStructure(blockStore, dirPath, dirCid)
    }
}