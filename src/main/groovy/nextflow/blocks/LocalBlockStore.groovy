package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import io.ipfs.multihash.Multihash.Type
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Optional
import java.util.Collections
import nextflow.blocks.unixfs.UnixFsImporter

/**
 * Implements a block store that uses the local filesystem as the backend
 */
@Slf4j
@CompileStatic
class LocalBlockStore implements BlockStore {
    // Default chunk size for breaking up files (256KB)
    private static final int DEFAULT_CHUNK_SIZE = 256 * 1024
    
    private final Path basePath
    private int chunkSize
    
    /**
     * Create a new LocalBlockStore
     * @param basePath The base directory where blocks will be stored
     * @param options Additional options like chunkSize
     */
    LocalBlockStore(Path basePath, Map options = [:]) {
        this.basePath = basePath
        this.chunkSize = options.chunkSize as Integer ?: DEFAULT_CHUNK_SIZE
        
        // Create the base directory if it doesn't exist
        if (!Files.exists(basePath)) {
            Files.createDirectories(basePath)
            log.debug "Created block store directory: ${basePath}"
        }
        
        log.debug "Using local block store at: ${basePath} with chunk size: ${chunkSize}"
    }
    
    /**
     * Create a new LocalBlockStore
     * @param basePathStr The base directory where blocks will be stored (as a string)
     * @param options Additional options like chunkSize
     */
    LocalBlockStore(String basePathStr, Map options = [:]) {
        this(Paths.get(basePathStr), options)
    }
    
    /**
     * Get the path where a block with the given CID would be stored
     * @param hash The multihash of the block
     * @return The path where the block would be stored
     */
    private Path getBlockPath(Multihash hash) {
        // If the hash is a Cid, extract the bare multihash
        Multihash multihash = (hash instanceof Cid) ? ((Cid)hash).bareMultihash() : hash
        
        // Use the last 2 characters of the hash as a directory name
        // This is better than using the first characters because CIDs often
        // share the same prefix (multibase prefix + multicodec)
        String hashStr = multihash.toString()
        String prefix = hashStr.substring(Math.max(0, hashStr.length() - 2))
        
        // Create the directory if it doesn't exist
        Path prefixDir = basePath.resolve(prefix)
        if (!Files.exists(prefixDir)) {
            Files.createDirectories(prefixDir)
        }
        
        return prefixDir.resolve(hashStr)
    }
    
    @Override
    MerkleNode get(Multihash hash) {
        log.trace "Getting block: ${hash}"
        
        Path blockPath = getBlockPath(hash)
        if (!Files.exists(blockPath)) {
            throw new RuntimeException("Block not found: ${hash}")
        }
        
        try {
            byte[] data = Files.readAllBytes(blockPath)
            
            return new MerkleNode(
                hash.toString(),                  // hash as String
                Optional.empty(),                 // name
                Optional.empty(),                 // size
                Optional.empty(),                 // largeSize
                Optional.empty(),                 // type
                Collections.emptyList(),          // links
                Optional.of(data)                 // data
            )
        } catch (IOException e) {
            throw new RuntimeException("Error reading block: ${hash}", e)
        }
    }
    
    @Override
    MerkleNode add(byte[] data) {
        return add(data, Cid.Codec.DagCbor)
    }
    
    @Override
    MerkleNode add(byte[] data, Cid.Codec codec) {
        try {
            // Calculate the multihash of the data using SHA-256
            // Compute SHA-256 hash using Java's MessageDigest
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256")
            byte[] hash = digest.digest(data)
            
            // Create a Multihash instance with the SHA-256 hash
            Multihash multihash = new Multihash(Type.sha2_256, hash)
            
            // Create a CID with the given codec
            Cid cid = Cid.build(1, codec, multihash)
            
            // Get the path where the block should be stored
            Path blockPath = getBlockPath(multihash)
            
            // Write the data to the file
            Files.write(blockPath, data)
            
            log.trace "Added block: ${cid} (${data.length} bytes)"
            log.info "🔗 BLOCK STORE: Added raw data block"
            log.info "   CID: ${cid}"
            log.info "   Size: ${data.length} bytes"
            log.info "   Codec: ${codec}"
            
            return new MerkleNode(
                cid.toString(),                   // hash as String
                Optional.empty(),                 // name
                Optional.of(data.length),         // size
                Optional.empty(),                 // largeSize
                Optional.empty(),                 // type
                Collections.emptyList(),          // links
                Optional.of(data)                 // data
            )
        } catch (IOException e) {
            throw new RuntimeException("Error adding block", e)
        }
    }
        
    @Override
    MerkleNode addPath(Path path) {
        log.trace "Adding path: ${path}"
        
        try {
            // Use the UnixFS importer to handle the file or directory
            UnixFsImporter importer = new UnixFsImporter(this, chunkSize)
            MerkleNode node = importer.importFile(path)
            
            // Log the path addition
            String type = Files.isDirectory(path) ? "directory" : "file"
            log.info "🗂️  BLOCK STORE: Added ${type} via UnixFS"
            log.info "   Path: ${path}"
            log.info "   CID: ${node.hash}"
            log.info "   Size: ${node.size.present ? node.size.get() : 'unknown'} bytes"
            log.info "   Links: ${node.links.size()}"
            
            return node
        } catch (IOException e) {
            throw new RuntimeException("Error importing file: ${path}", e)
        }
    }
    
    /**
     * Update configuration options for this block store
     * @param options Map of options to update
     */
    void updateOptions(Map options) {
        if (options.chunkSize != null) {
            this.chunkSize = options.chunkSize as Integer
            log.debug "Updated chunk size to: ${chunkSize}"
        }
        // Add other options here as needed
    }
} 