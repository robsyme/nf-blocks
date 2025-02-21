package nextflow.blocks

import io.ipfs.api.MerkleNode
import io.ipfs.multihash.Multihash
import java.nio.file.Path

/**
 * Interface for block storage backends
 */
interface BlockStore {
    // Files methods
    String chcid(String path, Map options)
    String cp(String source, String dest, boolean parents)
    Map flush(String path)
    List<Map> ls(String path, Map options)
    String mkdir(String path, boolean parents, Map options)
    String mv(String source, String dest)
    byte[] read(String path, Map options)
    String rm(String path, boolean recursive, boolean force)
    Map stat(String path, Map options)
    String write(String path, byte[] data, Map options)

    // Block methods - aligning with IPFS API
    MerkleNode get(Multihash hash)
    MerkleNode add(byte[] data)
    MerkleNode add(byte[] data, Map options)
    MerkleNode addPath(Path path)
} 