package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.blocks.BlockStore
import nextflow.blocks.dagpb.DagPbCodec
import nextflow.blocks.dagpb.DagPbLink
import nextflow.blocks.dagpb.DagPbNode
import nextflow.blocks.unixfs.UnixFsCodec
import nextflow.blocks.unixfs.UnixFsData
import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import java.time.Instant

/**
 * Manages a global filesystem root CID that grows incrementally with each publication.
 * 
 * This class maintains a single DAG-PB directory tree representing the entire published
 * dataset, starting from an empty root and building up the directory structure as
 * files are published to various paths.
 */
@Slf4j
@CompileStatic
class FilesystemRoot {
    private final BlockStore blockStore
    private String rootCid
    
    /**
     * Create a new FilesystemRoot with an empty directory as the initial state
     */
    FilesystemRoot(BlockStore blockStore) {
        this.blockStore = blockStore
        this.rootCid = createEmptyDirectory()
        log.info "🌱 FILESYSTEM ROOT: Initialized empty root → ${rootCid}"
    }
    
    /**
     * Create a FilesystemRoot with an existing root CID
     */
    FilesystemRoot(BlockStore blockStore, String existingRootCid) {
        this.blockStore = blockStore
        this.rootCid = existingRootCid
        log.debug "📂 FILESYSTEM ROOT: Loaded existing root → ${rootCid}"
    }
    
    /**
     * Get the current root CID representing the entire filesystem
     */
    String getRootCid() {
        return rootCid
    }
    
    /**
     * Add a file to the filesystem at the specified path
     * @param path The target path (e.g., "/results/sample1/data.txt")
     * @param contentCid The CID of the file content
     * @return The new root CID after adding the file
     */
    String addFile(String path, String contentCid) {
        log.debug "📝 FILESYSTEM ROOT: Adding file ${path} → ${contentCid}"
        
        // Parse the path to get directory and filename
        PathInfo pathInfo = parsePath(path)
        
        // Update the filesystem tree and get new root CID
        String previousRootCid = rootCid
        rootCid = updateTree(pathInfo, contentCid, false)
        
        log.debug "📂 FILESYSTEM ROOT: File added ${path} → Root CID: ${previousRootCid} → ${rootCid}"
        return rootCid
    }
    
    /**
     * Add a directory to the filesystem at the specified path
     * @param path The target path (e.g., "/results/analysis")
     * @param directoryCid The CID of the directory content
     * @return The new root CID after adding the directory
     */
    String addDirectory(String path, String directoryCid) {
        log.debug "📁 FILESYSTEM ROOT: Adding directory ${path} → ${directoryCid}"
        
        // Parse the path to get parent directory and directory name
        PathInfo pathInfo = parsePath(path)
        
        // Update the filesystem tree and get new root CID
        String previousRootCid = rootCid
        rootCid = updateTree(pathInfo, directoryCid, true)
        
        log.debug "📂 FILESYSTEM ROOT: Directory added ${path} → Root CID: ${previousRootCid} → ${rootCid}"
        return rootCid
    }
    
    /**
     * Create an empty DAG-PB directory and return its CID
     */
    private String createEmptyDirectory() {
        // Create UnixFS data for an empty directory
        UnixFsData dirData = UnixFsData.directory()
        dirData.mode = 0755 | 0040000  // drwxr-xr-x
        dirData.mtime = Instant.now()
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(dirData)
        
        // Create a DAG-PB node with no links (empty directory)
        DagPbNode dagNode = new DagPbNode([], unixfsData)
        
        // Add the empty directory to the block store
        MerkleNode node = blockStore.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
        
        return node.hash.toString()
    }
    
    /**
     * Parse a filesystem path into its components
     */
    private PathInfo parsePath(String path) {
        // Remove leading slash and split into components
        String cleanPath = path.startsWith("/") ? path.substring(1) : path
        
        if (cleanPath.isEmpty()) {
            return new PathInfo([], "")
        }
        
        List<String> parts = cleanPath.split("/") as List<String>
        
        if (parts.size() == 1) {
            // File/directory in root
            return new PathInfo([], parts[0])
        } else {
            // File/directory in subdirectory
            List<String> directoryPath = parts.subList(0, parts.size() - 1)
            String name = parts.last()
            return new PathInfo(directoryPath, name)
        }
    }
    
    /**
     * Update the directory tree by adding a new entry at the specified path
     * @param pathInfo Parsed path information
     * @param contentCid CID of the content to add
     * @param isDirectory Whether the content is a directory
     * @return New root CID after the update
     */
    private String updateTree(PathInfo pathInfo, String contentCid, boolean isDirectory) {
        // Start from the root and navigate/create the directory tree
        return updateDirectoryRecursive(rootCid, pathInfo.directoryPath, 0, pathInfo.name, contentCid, isDirectory)
    }
    
    /**
     * Recursively navigate and update the directory tree
     * @param currentDirCid CID of the current directory node
     * @param pathComponents Path components to navigate
     * @param pathIndex Current index in the path components
     * @param targetName Name of the final file/directory to add
     * @param contentCid CID of the content to add
     * @param isDirectory Whether the content is a directory
     * @return New CID of the updated directory
     */
    private String updateDirectoryRecursive(String currentDirCid, List<String> pathComponents, int pathIndex, 
                                           String targetName, String contentCid, boolean isDirectory) {
        
        // Load the current directory
        DagPbNode currentDir = loadDirectory(currentDirCid)
        
        if (pathIndex >= pathComponents.size()) {
            // We've reached the target directory - add the new entry here
            return addEntryToDirectory(currentDir, targetName, contentCid)
        }
        
        // We need to navigate deeper - get the next path component
        String nextDirName = pathComponents[pathIndex]
        
        // Find existing subdirectory or create it
        DagPbLink existingLink = currentDir.links.find { it.name == nextDirName }
        String nextDirCid
        
        if (existingLink) {
            // Subdirectory exists - navigate into it
            nextDirCid = bytesToCidString(existingLink.hash)
        } else {
            // Subdirectory doesn't exist - create empty directory
            nextDirCid = createEmptyDirectory()
            log.debug "📁 Created intermediate directory: ${nextDirName} → ${nextDirCid}"
        }
        
        // Recursively update the subdirectory
        String updatedNextDirCid = updateDirectoryRecursive(nextDirCid, pathComponents, pathIndex + 1, 
                                                           targetName, contentCid, isDirectory)
        
        // Update the current directory with the new/updated subdirectory
        return addEntryToDirectory(currentDir, nextDirName, updatedNextDirCid)
    }
    
    /**
     * Add or update an entry in a directory and return the new directory CID
     */
    private String addEntryToDirectory(DagPbNode directory, String entryName, String entryCid) {
        // Create a new list of links, removing any existing entry with the same name
        List<DagPbLink> newLinks = directory.links.findAll { it.name != entryName }
        
        // Add the new entry
        byte[] entryCidBytes = cidStringToBytes(entryCid)
        newLinks.add(new DagPbLink(entryCidBytes, entryName, null))
        
        // Create the updated directory node
        DagPbNode updatedDirectory = new DagPbNode(newLinks, directory.data)
        
        // Store the updated directory and return its CID
        MerkleNode node = blockStore.add(DagPbCodec.encode(updatedDirectory), Cid.Codec.DagProtobuf)
        return node.hash.toString()
    }
    
    /**
     * Load a directory node from the block store
     */
    private DagPbNode loadDirectory(String directoryCid) {
        try {
            Multihash hash
            if (directoryCid.startsWith("baf")) {
                // CIDv1 format - decode CID and get multihash
                Cid cid = Cid.decode(directoryCid)
                hash = new Multihash(Multihash.Type.sha2_256, cid.getHash())
            } else {
                // CIDv0 format - direct base58 multihash
                hash = Multihash.fromBase58(directoryCid)
            }
            MerkleNode node = blockStore.get(hash)
            return DagPbCodec.decode(node.data.get())
        } catch (Exception e) {
            log.error "Failed to load directory ${directoryCid}: ${e.message}", e
            throw new RuntimeException("Failed to load directory: ${directoryCid}", e)
        }
    }
    
    /**
     * Convert CID string to bytes for DAG-PB links
     */
    private byte[] cidStringToBytes(String cidString) {
        try {
            if (cidString.startsWith("baf")) {
                Cid cid = Cid.decode(cidString)
                return cid.toBytes()
            } else {
                Multihash multihash = Multihash.fromBase58(cidString)
                return multihash.toBytes()
            }
        } catch (Exception e) {
            log.error "Failed to convert CID to bytes: ${cidString}", e
            throw new RuntimeException("Invalid CID: ${cidString}", e)
        }
    }
    
    /**
     * Convert bytes from DAG-PB link back to CID string
     */
    private String bytesToCidString(byte[] cidBytes) {
        try {
            // Try to parse as CID first (CIDv1 format)
            if (cidBytes.length > 32) {
                Cid cid = Cid.cast(cidBytes)
                return cid.toString()
            } else {
                // This is likely a raw multihash (CIDv0)
                // Convert to base58 directly
                return io.ipfs.multibase.Base58.encode(cidBytes)
            }
        } catch (Exception e) {
            log.error "Failed to convert bytes to CID: ${cidBytes.length} bytes", e
            throw new RuntimeException("Invalid CID bytes", e)
        }
    }
    
    /**
     * Helper class to hold parsed path information
     */
    @CompileStatic
    private static class PathInfo {
        final List<String> directoryPath
        final String name
        
        PathInfo(List<String> directoryPath, String name) {
            this.directoryPath = directoryPath
            this.name = name
        }
        
        @Override
        String toString() {
            return "PathInfo{directoryPath=${directoryPath}, name='${name}'}"
        }
    }
}