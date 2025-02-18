package nextflow.blocks

import io.ipfs.cid.Cid
import java.nio.file.Path

/**
 * Interface for block storage backends
 */
interface BlockStore {
    /**
     * Store a block with its CID. The implementation should verify that
     * the block content matches the hash in the CID.
     * @throws IllegalArgumentException if the block content does not match the CID
     */
    void putBlock(Cid cid, byte[] block)
    
    /**
     * Retrieve a block by its CID
     */
    byte[] getBlock(Cid cid)
    
    /**
     * Check if a block exists
     */
    boolean hasBlock(Cid cid)

    /**
     * Add a file to the blockstore and return its CID
     * The implementation should handle creating appropriate blocks
     * (e.g. UnixFS for IPFS)
     */
    Cid putFile(Path path)
} 