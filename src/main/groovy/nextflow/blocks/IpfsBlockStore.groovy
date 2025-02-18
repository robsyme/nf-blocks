package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.IPFS
import io.ipfs.api.NamedStreamable
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.Arrays
import java.util.Optional
import java.nio.file.Path
import java.nio.file.Files
import java.util.stream.Collectors

/**
 * Implements a block store that uses IPFS as the backend
 */
@Slf4j
@CompileStatic
class IpfsBlockStore implements BlockStore {
    private final IPFS ipfs
    
    /**
     * Create a new IpfsBlockStore
     * @param multiaddr The multiaddr of the IPFS node (e.g. "/ip4/127.0.0.1/tcp/5001")
     */
    IpfsBlockStore(String multiaddr) {
        this(new IPFS(multiaddr))
    }

    /**
     * Create a new IpfsBlockStore using default localhost settings
     */
    IpfsBlockStore() {
        this("/ip4/127.0.0.1/tcp/5001")
    }

    /**
     * Create a new IpfsBlockStore with a provided IPFS instance (for testing)
     * @param ipfs The IPFS instance to use
     * @param skipValidation Whether to skip connection validation (for testing)
     */
    IpfsBlockStore(IPFS ipfs, boolean skipValidation) {
        this.ipfs = ipfs
        if (!skipValidation) {
            validateConnection()
        }
    }

    /**
     * Create a new IpfsBlockStore with a provided IPFS instance (for testing)
     * @param ipfs The IPFS instance to use
     */
    IpfsBlockStore(IPFS ipfs) {
        this(ipfs, false)
    }

    /**
     * Validate that we can connect to the IPFS node
     */
    private void validateConnection() {
        try {
            ipfs.refs.local()
            log.debug "Successfully connected to IPFS node"
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to IPFS node. Is IPFS daemon running?", e)
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

        try {
            List<byte[]> blocks = [block] as List<byte[]>
            def result = ipfs.block.put(blocks, Optional.empty())
            
            // Verify the block was stored with the correct CID
            if (!hasBlock(cid)) {
                throw new IllegalStateException("Block was stored but cannot be retrieved with the specified CID")
            }
            
            log.trace "Stored block: CID=${cid} size=${block.length}bytes store=ipfs"
        } catch (IOException e) {
            throw new IllegalStateException("Failed to store block in IPFS", e)
        }
    }

    /**
     * Retrieve a block by its CID
     */
    @Override
    byte[] getBlock(Cid cid) {
        try {
            return ipfs.block.get(new Multihash(cid.type, cid.hash))
        } catch (IOException | NoSuchElementException | RuntimeException e) {
            if (e.cause instanceof java.net.SocketTimeoutException) {
                throw new NoSuchElementException("Block not found: ${cid}")
            }
            throw e
        }
    }

    /**
     * Check if a block exists
     */
    @Override
    boolean hasBlock(Cid cid) {
        try {
            ipfs.block.stat(new Multihash(cid.type, cid.hash))
            return true
        } catch (IOException e) {
            return false
        }
    }

    @Override
    Cid putPath(Path path) {
        log.trace "Adding to IPFS: ${path} (${path.getClass()})"
        
        // Create appropriate wrapper based on whether it's a file or directory
        def streamable = Files.isDirectory(path) 
            ? createDirWrapper(path)
            : new NamedStreamable.FileWrapper(path.toFile())
            
        // Add to IPFS - this will handle both files and directories
        def result = ipfs.add(streamable).last()
        def cid = Cid.decode(result.hash.toBase58())
        log.trace "Added to IPFS: ${path} -> ${cid}"
        
        return cid
    }

    /**
     * Create a DirWrapper for a directory path
     */
    private NamedStreamable.DirWrapper createDirWrapper(Path dirPath) {
        log.trace "Creating DirWrapper for: ${dirPath}"
        // Get all children as FileWrappers
        List<NamedStreamable> children = Files.list(dirPath)
            .map { path -> new NamedStreamable.FileWrapper(path.toFile()) as NamedStreamable }
            .collect(Collectors.toList())

        log.trace "Children: ${children}"
        return new NamedStreamable.DirWrapper(dirPath.fileName.toString(), children)
    }
} 