package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import io.ipfs.multibase.Multibase

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

/**
 * Implements a simple file system based block store
 */
@Slf4j
@CompileStatic
class FileSystemBlockStore implements BlockStore {
    private final Path storePath

    /**
     * Create a new FileSystemBlockStore
     * @param path Directory path where blocks will be stored
     */
    FileSystemBlockStore(Path path) {
        this.storePath = path.toAbsolutePath()
        initializeStore()
    }

    /**
     * Create a new FileSystemBlockStore
     * @param path String path where blocks will be stored
     */
    FileSystemBlockStore(String path) {
        this(Paths.get(path))
    }

    /**
     * Initialize the block store directory
     */
    private void initializeStore() {
        if (!Files.exists(storePath)) {
            log.debug "Creating block store directory: ${storePath}"
            Files.createDirectories(storePath)
        }
    }

    /**
     * Get the appropriate MessageDigest for a given Multihash type
     */
    private MessageDigest getDigestForType(Multihash.Type type) {
        String algorithm = switch(type) {
            case Multihash.Type.sha1 -> "SHA-1"
            case Multihash.Type.sha2_256 -> "SHA-256"
            case Multihash.Type.sha2_512 -> "SHA-512"
            case Multihash.Type.sha3_224 -> "SHA3-224"
            case Multihash.Type.sha3_256 -> "SHA3-256"
            case Multihash.Type.sha3_512 -> "SHA3-512"
            default -> throw new IllegalArgumentException("Unsupported hash algorithm: ${type}")
        }
        
        try {
            return MessageDigest.getInstance(algorithm)
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Hash algorithm not available in this JVM: ${algorithm}", e)
        }
    }

    /**
     * Verify that a block's content matches the hash in its CID
     */
    private boolean verifyBlockHash(Cid cid, byte[] block) {
        def digest = getDigestForType(cid.type)
        def hash = digest.digest(block)
        return Arrays.equals(hash, cid.hash)
    }

    /**
     * Get the block path for a given CID
     */
    private Path getBlockPath(Cid cid) {
        // Use Base32 encoding of the multihash for the filename
        def multihashBytes = new Multihash(cid.type, cid.hash).toBytes()
        def encodedHash = io.ipfs.multibase.Multibase.encode(io.ipfs.multibase.Multibase.Base.Base32, multihashBytes)
        return storePath.resolve(encodedHash)
    }

    /**
     * Store a block with its CID
     */
    @Override
    void putBlock(Cid cid, byte[] block) {
        // Validate that this is a proper CID
        try {
            Cid.cast(cid)
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Invalid CID format", e)
        }

        if (!verifyBlockHash(cid, block)) {
            throw new IllegalArgumentException("Block content does not match CID hash")
        }
        
        def blockPath = getBlockPath(cid)
        if (!Files.exists(blockPath)) {
            Files.write(blockPath, block)
            log.trace "Stored block: CID=${cid} size=${block.length}bytes path=${blockPath}"
        }
    }

    /**
     * Retrieve a block by its CID
     */
    @Override
    byte[] getBlock(Cid cid) {
        def blockPath = getBlockPath(cid)
        if (!Files.exists(blockPath)) {
            throw new NoSuchElementException("Block not found: ${cid}")
        }
        return Files.readAllBytes(blockPath)
    }

    /**
     * Check if a block exists
     */
    @Override
    boolean hasBlock(Cid cid) {
        return Files.exists(getBlockPath(cid))
    }

    /**
     * Add a file to the blockstore and return its CID
     * For now, this is a simple implementation that just copies the file
     * and uses a raw codec. Later we can implement proper UnixFS format.
     */
    @Override
    Cid putPath(Path path) {
        // Read the file
        byte[] fileBytes = Files.readAllBytes(path)
        
        // Create CID with raw codec
        def digest = MessageDigest.getInstance("SHA-256")
        def hash = digest.digest(fileBytes)
        def mh = new Multihash(Multihash.Type.sha2_256, hash)
        def cid = Cid.buildCidV1(Cid.Codec.Raw, mh.getType(), hash)
        
        // Store the block
        putBlock(cid, fileBytes)
        
        return cid
    }
} 