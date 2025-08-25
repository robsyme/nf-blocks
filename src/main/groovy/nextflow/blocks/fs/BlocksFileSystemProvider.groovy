package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.blocks.BlockStore
import nextflow.blocks.BlocksFactory
import nextflow.blocks.LocalBlockStore
import nextflow.blocks.IpfsBlockStore
import nextflow.blocks.dagpb.DagPbCodec
import nextflow.blocks.dagpb.DagPbLink
import nextflow.blocks.dagpb.DagPbNode
import nextflow.blocks.unixfs.UnixFsCodec
import nextflow.blocks.unixfs.UnixFsData
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import java.time.Instant

import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.FileChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystemNotFoundException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.ReadOnlyFileSystemException
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.regex.Pattern
import java.util.Set

import io.ipfs.api.MerkleNode
import nextflow.file.FileSystemTransferAware

/**
 * A FileSystemProvider implementation for enhanced blocks URIs with backend specification.
 * Supports schemes like blocks+file:// and blocks+http://
 */
@Slf4j
@CompileStatic
class BlocksFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {
    static final String SCHEME_PREFIX = "blocks+"
    
    // Cache of file systems by backend URI
    private final Map<String, BlocksFileSystem> fileSystems = new ConcurrentHashMap<>()
    
    // Cache of block stores by backend URI
    private final Map<String, BlockStore> blockStores = new ConcurrentHashMap<>()
    
    // Map to track directory paths -> root CID mappings for incremental updates
    private final Map<String, String> directoryRootCids = new ConcurrentHashMap<>()
    
    /**
     * Information extracted from a blocks+ URI
     */
    @CompileStatic
    static class BlocksUriInfo {
        final String backend        // "file", "http"
        final String backendUri     // Backend-specific URI
        final String blocksPath     // Path within the blocks filesystem
        final String cacheKey       // Key for caching block stores and file systems
        
        BlocksUriInfo(String backend, String backendUri, String blocksPath, String cacheKey) {
            this.backend = backend
            this.backendUri = backendUri
            this.blocksPath = blocksPath
            this.cacheKey = cacheKey
        }
        
        @Override
        String toString() {
            return "BlocksUriInfo{backend='${backend}', backendUri='${backendUri}', blocksPath='${blocksPath}'}"
        }
    }
    
    /**
     * Default constructor for service loading.
     */
    public BlocksFileSystemProvider() {
        log.debug "Creating BlocksFileSystemProvider"
    }
    
    /**
     * Returns the URI scheme that identifies this provider.
     */
    @Override
    String getScheme() {
        // This provider handles multiple schemes: blocks+file, blocks+http, etc.
        // Java's FileSystemProvider mechanism only supports one scheme per provider,
        // but we'll handle the routing internally in supportsScheme()
        return "blocks+file"
    }
    
    /**
     * Check if this provider supports the given scheme
     */
    boolean supportsScheme(String scheme) {
        if (scheme == null || !scheme.startsWith(SCHEME_PREFIX)) {
            return false
        }
        String backend = scheme.substring(SCHEME_PREFIX.length())
        return backend in ["file", "http"]
    }
    
    /**
     * Parse a blocks+ URI and extract backend information
     * 
     * Examples:
     *   blocks+file:///path/to/blocks-store/file.txt
     *   blocks+http://127.0.0.1:5001/file.txt
     */
    private BlocksUriInfo parseBlocksUri(URI uri) {
        String scheme = uri.scheme
        if (!supportsScheme(scheme)) {
            throw new IllegalArgumentException("Unsupported URI scheme: ${scheme}")
        }
        
        String backend = scheme.substring(SCHEME_PREFIX.length())
        String blocksPath = uri.path ?: "/"
        
        switch (backend) {
            case "file":
                // blocks+file:///store-name/path/file.txt
                // Extract store name and file path
                String fullPath = uri.path
                if (!fullPath || fullPath == "/") {
                    throw new IllegalArgumentException("Invalid file URI - no path specified: ${uri}")
                }
                
                // Split path: /store-name/path/file.txt -> ["", "store-name", "path/file.txt"]
                String[] pathParts = fullPath.split("/", 3) 
                if (pathParts.length < 2) {
                    throw new IllegalArgumentException("Invalid file URI - must specify store name: ${uri}")
                }
                
                String storeName = pathParts[1] // "store-name"
                String filePath = pathParts.length > 2 ? "/" + pathParts[2] : "/"
                
                // Create store path relative to launch directory
                String storePath = "./${storeName}" 
                String cacheKey = "file://${storeName}"
                
                return new BlocksUriInfo(backend, storePath, filePath, cacheKey)
                
            case "http":
                // blocks+http://127.0.0.1:5001/file.txt
                String host = uri.host ?: "localhost"
                int port = uri.port > 0 ? uri.port : 5001
                
                // Convert HTTP endpoint to IPFS multiaddress format
                String multiaddr = "/ip4/${host}/tcp/${port}"
                String cacheKey = "http://${host}:${port}"
                
                return new BlocksUriInfo(backend, multiaddr, blocksPath, cacheKey)
                
            default:
                throw new IllegalArgumentException("Unsupported blocks backend: ${backend}")
        }
    }
    
    /**
     * Create or get a cached block store for the given URI info
     */
    private BlockStore getOrCreateBlockStore(BlocksUriInfo uriInfo) {
        return blockStores.computeIfAbsent(uriInfo.cacheKey) { key ->
            log.info "Creating new ${uriInfo.backend} block store: ${uriInfo.backendUri}"
            
            switch (uriInfo.backend) {
                case "file":
                    return new LocalBlockStore(uriInfo.backendUri, [:])
                    
                case "http":
                    return new IpfsBlockStore(uriInfo.backendUri)
                    
                default:
                    throw new IllegalArgumentException("Unsupported backend: ${uriInfo.backend}")
            }
        }
    }
    
    /**
     * Constructs a new file system instance for the blocks+ URI scheme.
     */
    @Override
    synchronized FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        log.debug "Creating new blocks+ file system with URI: ${uri}, env: ${env}"
        
        BlocksUriInfo uriInfo = parseBlocksUri(uri)
        
        synchronized (fileSystems) {
            // Check if a file system already exists for this backend
            BlocksFileSystem fs = fileSystems.get(uriInfo.cacheKey)
            if (fs != null) {
                log.debug "File system already exists for backend: '${uriInfo.cacheKey}'"
                return fs
            }
            
            // Create the block store for this backend
            BlockStore blockStore = getOrCreateBlockStore(uriInfo)
            
            // Create and register the file system
            fs = new BlocksFileSystem(this, blockStore, env)
            fileSystems.put(uriInfo.cacheKey, fs)
            
            log.info "Created new blocks+ file system for ${uriInfo.backend} backend: ${uriInfo.backendUri}"
            return fs
        }
    }
    
    /**
     * Returns an existing file system identified by a URI.
     */
    @Override
    FileSystem getFileSystem(URI uri) {
        checkURI(uri)
        BlocksUriInfo uriInfo = parseBlocksUri(uri)
        
        synchronized (fileSystems) {
            BlocksFileSystem fs = fileSystems.get(uriInfo.cacheKey)
            if (fs == null) {
                // Create a new file system for this backend
                log.info "Creating new blocks+ file system for backend: '${uriInfo.cacheKey}'"
                
                // Create the block store for this backend
                BlockStore blockStore = getOrCreateBlockStore(uriInfo)
                
                // Create and register the file system
                fs = new BlocksFileSystem(this, blockStore, [:])
                fileSystems.put(uriInfo.cacheKey, fs)
            }
            return fs
        }
    }
    
    /**
     * Return a Path object by converting the given URI.
     */
    @Override
    Path getPath(URI uri) {
        checkURI(uri)
        
        // Parse the URI to get the blocks path
        BlocksUriInfo uriInfo = parseBlocksUri(uri)
        
        // Get the file system for the URI
        FileSystem fs = getFileSystem(uri)
        
        // Return the path within the blocks filesystem
        return fs.getPath(uriInfo.blocksPath)
    }
    
    /**
     * Opens a file, returning a seekable byte channel to access the file.
     * Since Nextflow only performs write operations, this is simplified for publication.
     */
    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        BlocksPath blocksPath = checkPath(path)
        
        log.debug "Opening channel for publication: ${blocksPath} with options: ${options}"
        return new BlocksSeekableByteChannel(blocksPath, options, attrs)
    }
    
    /**
     * Opens a directory, returning a DirectoryStream to iterate over the entries in the directory.
     * Not supported in write-only publication mode.
     */
    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        throw new UnsupportedOperationException("Directory listing not supported in write-only publication mode")
    }
    
    /**
     * Creates a new directory.
     */
    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        BlocksPath blocksPath = checkPath(dir)
        
        // For now, just log the directory creation and return success
        // In a real implementation, we would create a directory node in the block store
        log.debug "Creating directory: ${blocksPath}"
        
        // Since our filesystem is content-addressed, directories are created implicitly
        // when files are added to them. For now, we'll just pretend the directory was created.
        return
    }
    
    /**
     * Deletes a file.
     */
    @Override
    void delete(Path path) throws IOException {
        // In a content-addressed filesystem, we don't actually delete files
        // Just log the request and return
        log.debug "Ignoring request to delete: ${path}"
        return
    }
    
    /**
     * Copy a file to a target file - implements DAG-PB directory folding for publication.
     */
    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        log.info "🚀 PUBLICATION: ${source} → ${target}"
        
        // Check if the source is a blocks path
        if (source instanceof BlocksPath) {
            log.debug "Internal copy within blocks filesystem - no action needed"
            return
        }
        
        // External copy from another filesystem to blocks (publication)
        BlocksPath targetPath = checkPath(target)
        BlockStore store = ((BlocksFileSystem)targetPath.getFileSystem()).getBlockStore()
        
        try {
            // Add the content to get its CID
            MerkleNode contentNode = store.addPath(source)
            String contentCid = contentNode.hash.toString()
            
            // Extract directory path and filename from target
            String targetPathStr = targetPath.toString()
            String parentDir = getParentDirectory(targetPathStr)
            String fileName = getFileName(targetPathStr)
            
            // Create or update the directory structure
            String rootCid = foldIntoDirectory(store, parentDir, fileName, contentCid)
            
            // Log the publication result (block store already logged the content addition)
            String type = Files.isDirectory(source) ? "directory" : "file"
            log.info "📁 PUBLICATION: Published ${type} to ${parentDir} → Root CID: ${rootCid}"
            log.debug "   Content CID: ${contentCid}, Target: ${targetPath}"
            
        } catch (Exception e) {
            log.error "Failed to publish ${source} to ${target}: ${e.message}", e
            throw new IOException("Failed to publish ${source} to ${target}: ${e.message}", e)
        }
    }
    
    /**
     * Move or rename a file to a target file.
     */
    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        throw new ReadOnlyFileSystemException()
    }
    
    /**
     * Tests if two paths locate the same file.
     */
    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        if (path == path2) {
            return true
        }
        
        if (!(path instanceof BlocksPath && path2 instanceof BlocksPath)) {
            return false
        }
        
        // Check if both paths are in the same file system
        if (path.getFileSystem() != path2.getFileSystem()) {
            return false
        }
        
        // For content-addressed file systems, same path means same file
        return path.toString() == path2.toString()
    }
    
    /**
     * Tells whether or not a file is considered hidden.
     */
    @Override
    boolean isHidden(Path path) throws IOException {
        // Files in blocks:// are never hidden
        return false
    }
    
    /**
     * Returns a file store representing the file store where a Path is located.
     */
    @Override
    FileStore getFileStore(Path path) throws IOException {
        BlocksPath blocksPath = checkPath(path)
        
        // TODO: Create and return a BlocksFileStore instance
        throw new UnsupportedOperationException("FileStore not implemented yet")
    }
    
    /**
     * Checks access to a file.
     * In write-only publication mode, only WRITE access is supported.
     */
    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        BlocksPath blocksPath = checkPath(path)
        
        // Check access modes
        for (AccessMode mode : modes) {
            if (mode == AccessMode.READ || mode == AccessMode.EXECUTE) {
                throw new UnsupportedOperationException("Write-only filesystem - only WRITE access supported")
            }
        }
    }
    
    /**
     * Returns a file attribute view of a given type.
     */
    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        BlocksPath blocksPath = checkPath(path)
        
        // Only support basic attribute view for now
        if (type == BasicFileAttributeView.class) {
            return (V) new BlocksBasicFileAttributeView(blocksPath)
        }
        
        return null
    }
    
    /**
     * Reads a file's attributes as a bulk operation.
     * Not supported in write-only publication mode.
     */
    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Reading attributes not supported in write-only publication mode")
    }
    
    /**
     * Reads a set of file attributes as a bulk operation.
     * Not supported in write-only publication mode.
     */
    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Reading attributes not supported in write-only publication mode")
    }
    
    /**
     * Sets the value of a file attribute.
     */
    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        BlocksPath blocksPath = checkPath(path)
        
        // Currently, blocks:// is read-only
        throw new IOException("Read-only file system")
    }
    
    /**
     * Opens a file for reading, returning an asynchronous channel.
     */
    @Override
    AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, 
                                                       ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("Asynchronous I/O not supported")
    }
    
    /**
     * Opens or creates a file, returning a file channel.
     */
    @Override
    FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        BlocksPath blocksPath = checkPath(path)
        
        // Create a file channel
        return new BlocksFileChannel(blocksPath, options, attrs)
    }
    
    /**
     * Extract parent directory from a path string
     */
    private String getParentDirectory(String path) {
        int lastSlash = path.lastIndexOf('/')
        if (lastSlash <= 0) {
            return "/"
        }
        return path.substring(0, lastSlash)
    }
    
    /**
     * Extract filename from a path string
     */
    private String getFileName(String path) {
        int lastSlash = path.lastIndexOf('/')
        if (lastSlash < 0) {
            return path
        }
        return path.substring(lastSlash + 1)
    }
    
    /**
     * Fold a new file into a directory structure, creating DAG-PB directories as needed
     */
    private String foldIntoDirectory(BlockStore store, String directoryPath, String fileName, String contentCid) {
        // Get the current root CID for this directory (if any)
        String currentRootCid = directoryRootCids.get(directoryPath)
        
        // Create or update the directory structure with DAG-PB
        String newRootCid = createDirectoryWithFile(store, currentRootCid, fileName, contentCid)
        
        // Update the tracked root CID
        directoryRootCids.put(directoryPath, newRootCid)
        
        log.debug "Folded ${fileName} into ${directoryPath}: ${currentRootCid} → ${newRootCid}"
        return newRootCid
    }
    
    /**
     * Create a new DAG-PB directory with the given file, optionally merging with existing directory
     */
    private String createDirectoryWithFile(BlockStore store, String existingRootCid, String fileName, String contentCid) {
        log.debug "Creating directory with file: ${fileName} -> ${contentCid}, existing: ${existingRootCid}"
        
        List<DagPbLink> links = []
        
        // If there's an existing root CID, load it and merge its links
        if (existingRootCid) {
            try {
                // Parse existing CID and retrieve the directory node
                Multihash existingHash = Multihash.fromBase58(existingRootCid)
                MerkleNode existingNode = store.get(existingHash)
                
                // Decode the existing DAG-PB node
                DagPbNode existingDagNode = DagPbCodec.decode(existingNode.data.get())
                
                // Copy existing links, filtering out any with the same name
                links.addAll(existingDagNode.links.findAll { it.name != fileName })
                
                log.debug "Merged ${existingDagNode.links.size()} existing links, filtered out duplicates"
            } catch (Exception e) {
                log.warn "Failed to load existing directory CID ${existingRootCid}: ${e.message}"
                // Continue with empty links - will create a new directory
            }
        }
        
        // Add the new file link
        byte[] contentCidBytes = parseContentCidToBytes(contentCid)
        links.add(new DagPbLink(contentCidBytes, fileName, null))
        
        // Create UnixFS data for the directory
        UnixFsData dirData = UnixFsData.directory()
        dirData.mode = 0755 | 0040000  // drwxr-xr-x
        dirData.mtime = Instant.now()
        
        // Encode the UnixFS data
        byte[] unixfsData = UnixFsCodec.encode(dirData)
        
        // Create a DAG-PB node with UnixFS data and links
        DagPbNode dagNode = new DagPbNode(links, unixfsData)
        
        // Add the directory node to the block store
        MerkleNode directoryNode = store.add(DagPbCodec.encode(dagNode), Cid.Codec.DagProtobuf)
        
        String newRootCid = directoryNode.hash.toString()
        log.debug "Created directory node with CID: ${newRootCid}, links: ${links.size()}"
        
        return newRootCid
    }
    
    /**
     * Parse a content CID string to bytes for use in DAG-PB links
     */
    private byte[] parseContentCidToBytes(String contentCid) {
        try {
            // Try to parse as CIDv1 first
            if (contentCid.startsWith("baf")) {
                Cid cid = Cid.decode(contentCid)
                return cid.toBytes()
            } else {
                // Fallback to multihash parsing for CIDv0
                Multihash multihash = Multihash.fromBase58(contentCid)
                return multihash.toBytes()
            }
        } catch (Exception e) {
            log.error "Failed to parse content CID: ${contentCid}", e
            throw new RuntimeException("Invalid content CID: ${contentCid}", e)
        }
    }
    
    /**
     * Checks if a path exists in the file system.
     * In write-only publication mode, always returns false to allow publication.
     */
    boolean exists(BlocksPath path) {
        log.debug "Existence check for write-only filesystem: ${path} -> false"
        return false
    }
    
    /**
     * Converts a Path to a BlocksPath, or throws an exception if the conversion is not possible.
     */
    BlocksPath toBlocksPath(Path path) {
        if (path instanceof BlocksPath) {
            return (BlocksPath) path
        }
        throw new ProviderMismatchException("Path not from blocks provider: " + path)
    }
    
    /**
     * Checks that the given path is from this provider and returns it as a BlocksPath.
     */
    BlocksPath checkPath(Path path) {
        if (path == null) {
            throw new NullPointerException("Path cannot be null")
        }
        if (!(path instanceof BlocksPath)) {
            throw new ProviderMismatchException("Path not from blocks provider: " + path)
        }
        return (BlocksPath) path
    }
    
    /**
     * Checks if the URI is valid for this provider.
     */
    private void checkURI(URI uri) {
        if (!supportsScheme(uri.scheme)) {
            throw new IllegalArgumentException("Unsupported URI scheme: ${uri.scheme}")
        }
    }
    
    
    // FileSystemTransferAware implementation
    
    @Override
    boolean canUpload(Path source, Path target) {
        // We can upload TO blocks:// filesystem FROM any other filesystem
        return target.fileSystem.provider() instanceof BlocksFileSystemProvider
    }
    
    @Override
    boolean canDownload(Path source, Path target) {
        // We can download FROM blocks:// filesystem TO any other filesystem  
        return source.fileSystem.provider() instanceof BlocksFileSystemProvider
    }
    
    @Override
    void upload(Path source, Path target, CopyOption... options) throws IOException {
        log.info "🚀 UPLOAD: ${source} → ${target}"
        
        // This is the key method - called when copying FROM local filesystem TO blocks://
        BlocksPath targetPath = checkPath(target)
        BlockStore store = ((BlocksFileSystem)targetPath.getFileSystem()).getBlockStore()
        
        try {
            // Add content to the block store - the block store will handle logging
            MerkleNode node = store.addPath(source)
            log.debug "Upload complete: ${source} → ${target} (CID: ${node.hash})"
        } catch (Exception e) {
            log.error "Failed to upload ${source} to ${target}: ${e.message}", e
            throw new IOException("Failed to upload ${source} to ${target}: ${e.message}", e)
        }
    }
    
    @Override
    void download(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("Download not supported in write-only publication mode")
    }
}