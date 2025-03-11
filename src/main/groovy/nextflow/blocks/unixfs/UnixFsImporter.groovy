package nextflow.blocks.unixfs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import nextflow.blocks.BlockStore
import nextflow.blocks.dagpb.DagPbCodec
import nextflow.blocks.dagpb.DagPbLink
import nextflow.blocks.dagpb.DagPbNode
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
import java.time.ZoneOffset
import nextflow.blocks.unixfs.ChunkerFactory

/**
 * Handles importing files into a block store using UnixFS format
 */
@Slf4j
@CompileStatic
class UnixFsImporter {
    private static final int DEFAULT_CHUNK_SIZE = 256 * 1024 // 256KB chunks by default
    private final BlockStore blockStore
    private final int chunkSize
    
    UnixFsImporter(BlockStore blockStore, int chunkSize = DEFAULT_CHUNK_SIZE) {
        this.blockStore = blockStore
        this.chunkSize = chunkSize
    }
    
    /**
     * Imports a file into the block store using UnixFS format
     * @param filePath The path to the file to import
     * @return The MerkleNode representing the root of the imported file
     */
    MerkleNode importFile(Path filePath) {
        if (Files.isDirectory(filePath)) {
            return importDirectory(filePath)
        } else {
            return importRegularFile(filePath)
        }
    }
    
    /**
     * Imports a directory into the block store
     */
    private MerkleNode importDirectory(Path dirPath) {
        log.debug "Importing directory: ${dirPath}"
        
        // Get file attributes for the directory
        BasicFileAttributes attrs = Files.readAttributes(dirPath, BasicFileAttributes.class)
        
        // Create UnixFS data for the directory
        UnixFsData dirData = UnixFsData.directory()
        dirData.mode = getUnixMode(dirPath)
        dirData.mtime = attrs.lastModifiedTime().toInstant()
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(dirData)
        
        // Create a list of links to the directory entries
        List<DagPbLink> links = []
        
        // Process each entry in the directory
        Files.list(dirPath).forEach { entry ->
            String name = entry.fileName.toString()
            MerkleNode childNode = importFile(entry)
            
            // Multihash can come as string or as a Cid object
            byte[] hash = (childNode.hash instanceof Cid) ? 
                (childNode.hash as Cid).toBytes() : 
                Multihash.fromBase58(childNode.hash.toString()).toBytes()
            
            // Add a link to the child node
            links.add(new DagPbLink(hash, name))
        }
        
        // Create a DAG-PB node with UnixFS data and links
        DagPbNode dagNode = new DagPbNode(links, unixfsData)
        
        // Add the node to the block store
        return blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
    }
    
    /**
     * Imports a regular file into the block store
     */
    private MerkleNode importRegularFile(Path filePath) {
        log.debug "Importing file: ${filePath}"
        
        // Get file attributes
        BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class)
        long fileSize = attrs.size()
        
        // If file is small, import it as a single block
        if (fileSize <= chunkSize) {
            return importSmallFile(filePath, attrs)
        } else {
            return importLargeFile(filePath, attrs)
        }
    }
    
    /**
     * Imports a small file (size <= chunkSize) as a single block
     */
    private MerkleNode importSmallFile(Path filePath, BasicFileAttributes attrs) {
        byte[] fileData = Files.readAllBytes(filePath)
        
        // Create UnixFS data for the file
        UnixFsData fsData = UnixFsData.file(fileData, attrs.size())
        fsData.mode = getUnixMode(filePath)
        fsData.mtime = attrs.lastModifiedTime().toInstant()
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(fsData)
        
        // Create a DAG-PB node with UnixFS data
        DagPbNode dagNode = new DagPbNode([], unixfsData)
        
        // Add the node to the block store
        return blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
    }
    
    /**
     * Imports a large file (size > chunkSize) as a balanced tree of blocks
     */
    private MerkleNode importLargeFile(Path filePath, BasicFileAttributes attrs) {
        // Open the file as a stream and create a chunker
        InputStream fileStream = Files.newInputStream(filePath)
        Chunker chunker = ChunkerFactory.fixedSize(fileStream, chunkSize)
        
        // Import the file using the chunker
        try {
            return importWithChunker(chunker, attrs.size(), getUnixMode(filePath), attrs.lastModifiedTime().toInstant())
        } finally {
            fileStream.close()
        }
    }
    
    /**
     * Imports data using the provided chunker
     */
    private MerkleNode importWithChunker(Chunker chunker, long fileSize, Integer mode, Instant mtime) {
        // Store leaf nodes (chunks)
        List<MerkleNode> leafNodes = []
        List<Long> blockSizes = []
        
        // Process each chunk
        byte[] chunk
        while ((chunk = chunker.nextChunk()) != null) {
            // Create UnixFS data for the chunk
            UnixFsData chunkData = UnixFsData.file(chunk)
            
            // Encode the UnixFS data
            byte[] unixfsData = UnixFsCodec.encode(chunkData)
            
            // Create a DAG-PB node with UnixFS data
            DagPbNode dagNode = new DagPbNode([], unixfsData)
            
            // Add the node to the block store
            MerkleNode node = blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
            
            // Add to our list of leaf nodes
            leafNodes.add(node)
            blockSizes.add((long)chunk.length)
        }
        
        // If there's only one chunk, return it directly
        if (leafNodes.size() == 1) {
            return leafNodes[0]
        }
        
        // Create links to all the chunks
        List<DagPbLink> links = []
        
        for (int i = 0; i < leafNodes.size(); i++) {
            MerkleNode childNode = leafNodes[i]
            
            // Multihash can come as string or as a Cid object
            byte[] hash = (childNode.hash instanceof Cid) ? 
                (childNode.hash as Cid).toBytes() : 
                Multihash.fromBase58(childNode.hash.toString()).toBytes()
            
            // Add a link to the child node
            links.add(new DagPbLink(hash, null, blockSizes[i]))
        }
        
        // Create UnixFS data for the root node
        UnixFsData rootData = UnixFsData.file(null, fileSize)
        rootData.mode = mode
        rootData.mtime = mtime
        
        // Add block sizes
        blockSizes.each { size -> rootData.addBlockSize(size) }
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(rootData)
        
        // Create a DAG-PB node with UnixFS data and links to chunks
        DagPbNode dagNode = new DagPbNode(links, unixfsData)
        
        // Add the node to the block store
        return blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
    }
    
    /**
     * Gets UNIX mode from file
     */
    private Integer getUnixMode(Path path) {
        try {
            // Try to get POSIX file permissions if available
            def posixPermissions = Files.getPosixFilePermissions(path)
            int mode = 0
            
            // Owner permissions
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OWNER_READ))
                mode |= 0400
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OWNER_WRITE))
                mode |= 0200
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE))
                mode |= 0100
                
            // Group permissions
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.GROUP_READ))
                mode |= 0040
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.GROUP_WRITE))
                mode |= 0020
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE))
                mode |= 0010
                
            // Others permissions
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_READ))
                mode |= 0004
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE))
                mode |= 0002
            if (posixPermissions.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE))
                mode |= 0001
                
            // Add file type bits
            if (Files.isDirectory(path))
                mode |= 0040000  // S_IFDIR
            else if (Files.isRegularFile(path))
                mode |= 0100000  // S_IFREG
            else if (Files.isSymbolicLink(path))
                mode |= 0120000  // S_IFLNK
                
            return mode
        } catch (Exception e) {
            // On non-POSIX systems, use reasonable defaults
            if (Files.isDirectory(path))
                return 0755 | 0040000  // drwxr-xr-x
            else if (Files.isExecutable(path))
                return 0755 | 0100000  // -rwxr-xr-x
            else
                return 0644 | 0100000  // -rw-r--r--
        }
    }
} 