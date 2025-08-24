package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.blocks.BlockStore

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider
import java.util.regex.Pattern
import java.util.concurrent.ConcurrentHashMap

/**
 * A FileSystem implementation that uses a BlockStore as the storage backend.
 */
@Slf4j
@CompileStatic
class BlocksFileSystem extends FileSystem {
    private final BlocksFileSystemProvider provider
    private BlockStore blockStore
    private final String separator = "/"
    private final boolean isReadOnly = true
    private boolean isOpen = true
    
    // Map to track directory CIDs
    private final Map<String, String> directoryCids = new ConcurrentHashMap<>()
    
    /**
     * Creates a new BlocksFileSystem.
     *
     * @param provider The provider that created this file system
     * @param blockStore The block store to use for storage
     * @param env Environment settings
     */
    BlocksFileSystem(BlocksFileSystemProvider provider, BlockStore blockStore, Map<String, ?> env) {
        this.provider = provider
        this.blockStore = blockStore
        
        log.debug "Created blocks:// file system with blockStore: ${blockStore}"
    }
    
    /**
     * Returns the provider that created this file system.
     */
    @Override
    FileSystemProvider provider() {
        return provider
    }
    
    /**
     * Closes this file system.
     */
    @Override
    void close() throws IOException {
        if (!isOpen) {
            return
        }
        
        isOpen = false
        log.debug "Closed blocks:// file system"
    }
    
    /**
     * Tells whether or not this file system is open.
     */
    @Override
    boolean isOpen() {
        return isOpen
    }
    
    /**
     * Tells whether or not this file system allows only read-only access to its file stores.
     */
    @Override
    boolean isReadOnly() {
        return isReadOnly
    }
    
    /**
     * Returns the name separator character used by this file system.
     */
    @Override
    String getSeparator() {
        return separator
    }
    
    /**
     * Returns an object to iterate over the paths of the root directories.
     */
    @Override
    Iterable<Path> getRootDirectories() {
        // Return a singleton list with just the root path
        return [new BlocksPath(this, "/")]
    }
    
    /**
     * Returns an object to iterate over the underlying file stores.
     */
    @Override
    Iterable<FileStore> getFileStores() {
        // For now, just return an empty list
        // In the future, we could create a BlocksFileStore implementation
        return []
    }
    
    /**
     * Returns the set of the names of the file attribute views supported by this file system.
     */
    @Override
    Set<String> supportedFileAttributeViews() {
        // For now, only support basic attributes
        return ["basic"] as Set
    }
    
    /**
     * Converts a path string, or a sequence of strings that when joined form a path string, to a Path.
     */
    @Override
    Path getPath(String first, String... more) {
        // Join the path components
        StringBuilder sb = new StringBuilder()
        sb.append(first)
        
        for (String part : more) {
            if (part.startsWith(separator)) {
                // If the part starts with the separator, use it as is
                sb.append(part)
            } else if (sb.length() == 0 || sb.charAt(sb.length() - 1) == separator.charAt(0)) {
                // If the previous path ends with a separator, or is empty, don't add another separator
                sb.append(part)
            } else {
                // Otherwise, add a separator between the parts
                sb.append(separator).append(part)
            }
        }
        
        return new BlocksPath(this, sb.toString())
    }
    
    /**
     * Returns a PathMatcher that performs match operations on the String representation of Path objects.
     */
    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        // Split the syntax and pattern
        int colonIndex = syntaxAndPattern.indexOf(':')
        if (colonIndex <= 0 || colonIndex == syntaxAndPattern.length() - 1) {
            throw new IllegalArgumentException("Invalid syntax-pattern pair: " + syntaxAndPattern)
        }
        
        String syntax = syntaxAndPattern.substring(0, colonIndex).toLowerCase()
        String pattern = syntaxAndPattern.substring(colonIndex + 1)
        
        // Handle different syntaxes
        switch (syntax) {
            case "glob":
                return new GlobPathMatcher(pattern)
            case "regex":
                return new RegexPathMatcher(pattern)
            default:
                throw new UnsupportedOperationException("Unsupported syntax: " + syntax)
        }
    }
    
    /**
     * Returns the UserPrincipalLookupService for this file system.
     */
    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("User principal lookup not supported")
    }
    
    /**
     * Constructs a new WatchService.
     */
    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Watch service not supported")
    }
    
    /**
     * Returns the CID for a directory path.
     * This is used to track the current state of directories in the filesystem.
     * 
     * @param path The directory path
     * @return The CID of the directory, or null if it doesn't exist
     */
    String getDirectoryCid(BlocksPath path) {
        // For now, we'll use a simple in-memory map to track directory CIDs
        // In a real implementation, this would be persisted to disk
        return directoryCids.get(path.toString())
    }
    
    /**
     * Sets the CID for a directory path.
     * This is used to track the current state of directories in the filesystem.
     * 
     * @param path The directory path
     * @param cid The CID of the directory
     */
    void setDirectoryCid(BlocksPath path, String cid) {
        // For now, we'll use a simple in-memory map to track directory CIDs
        // In a real implementation, this would be persisted to disk
        directoryCids.put(path.toString(), cid)
    }
    
    /**
     * Returns the block store used by this file system.
     */
    BlockStore getBlockStore() {
        return blockStore
    }
    
    /**
     * Updates the block store used by this file system.
     * This is used when the block store configuration changes.
     * 
     * @param newBlockStore The new block store to use
     */
    void updateBlockStore(BlockStore newBlockStore) {
        this.blockStore = newBlockStore
        log.debug "Updated block store in file system: ${newBlockStore.class.name}"
    }
    
    /**
     * Simple glob path matcher implementation.
     */
    private static class GlobPathMatcher implements PathMatcher {
        private final PathMatcher delegate
        
        GlobPathMatcher(String pattern) {
            // Convert glob pattern to regex pattern
            String regex = globToRegex(pattern)
            delegate = new RegexPathMatcher(regex)
        }
        
        @Override
        boolean matches(Path path) {
            return delegate.matches(path)
        }
        
        // Convert glob pattern to regex
        private String globToRegex(String glob) {
            StringBuilder regex = new StringBuilder("^")
            int i = 0
            int len = glob.length()
            
            while (i < len) {
                char c = glob.charAt(i++)
                
                switch (c) {
                    case '*':
                        if (i < len && glob.charAt(i) == '*') {
                            // ** means any number of directories
                            regex.append(".*")
                            i++
                        } else {
                            // * means any number of characters except directory separator
                            regex.append("[^/]*")
                        }
                        break
                    case '?':
                        // ? means any single character except directory separator
                        regex.append("[^/]")
                        break
                    case '[':
                        // Character class
                        regex.append('[')
                        if (i < len && glob.charAt(i) == '!') {
                            // Negated character class
                            regex.append('^')
                            i++
                        }
                        
                        // Copy character class contents
                        boolean inClass = true
                        while (i < len && inClass) {
                            char cc = glob.charAt(i++)
                            if (cc == ']') {
                                inClass = false
                            }
                            regex.append(cc)
                        }
                        break
                    case '{':
                        // Group
                        regex.append('(')
                        boolean inGroup = true
                        while (i < len && inGroup) {
                            char gc = glob.charAt(i++)
                            if (gc == '}') {
                                inGroup = false
                            } else if (gc == ',') {
                                regex.append('|')
                            } else {
                                regex.append(gc)
                            }
                        }
                        regex.append(')')
                        break
                    default:
                        // Escape special regex characters
                        if ('\\[]{}()*+?.^$|'.indexOf((int)c) != -1) {
                            regex.append('\\')
                        }
                        regex.append(c)
                }
            }
            
            regex.append('$')
            return regex.toString()
        }
    }
    
    /**
     * Simple regex path matcher implementation.
     */
    private static class RegexPathMatcher implements PathMatcher {
        private final Pattern pattern
        
        RegexPathMatcher(String pattern) {
            this.pattern = Pattern.compile(pattern)
        }
        
        @Override
        boolean matches(Path path) {
            return pattern.matcher(path.toString()).matches()
        }
    }
}