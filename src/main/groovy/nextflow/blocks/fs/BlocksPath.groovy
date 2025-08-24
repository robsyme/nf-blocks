package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j

import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.FileSystem
import java.io.IOException
import java.net.URI
import java.nio.file.NoSuchFileException
import java.util.Collections

/**
 * Implementation of the Path interface for the BlocksFileSystem.
 */
@Slf4j
@CompileStatic
@EqualsAndHashCode
class BlocksPath implements Path {
    private final BlocksFileSystem fs
    private final String path
    
    /**
     * Creates a new BlocksPath.
     *
     * @param fs The file system this path belongs to
     * @param path The path string
     */
    BlocksPath(BlocksFileSystem fs, String path) {
        this.fs = fs
        this.path = normalizePath(path)
    }
    
    /**
     * Normalize the path by removing redundant slashes and resolving . and .. segments.
     */
    private String normalizePath(String path) {
        // Handle empty path
        if (path == null || path.isEmpty()) {
            return ""
        }
        
        // Split the path into segments
        String[] segments = path.split("/")
        List<String> normalized = []
        
        // Process each segment
        for (String segment : segments) {
            if (segment.isEmpty() || segment == ".") {
                // Skip empty segments and current directory
                continue
            } else if (segment == "..") {
                // Go up one directory
                if (!normalized.isEmpty() && normalized.last() != "..") {
                    normalized.removeLast()
                } else if (!isAbsolute()) {
                    // For relative paths, keep .. segments
                    normalized.add("..")
                }
            } else {
                // Add normal segment
                normalized.add(segment)
            }
        }
        
        // Reconstruct the path
        StringBuilder result = new StringBuilder()
        
        // Add leading slash for absolute paths
        if (path.startsWith("/")) {
            result.append("/")
        }
        
        // Join the segments
        result.append(normalized.join("/"))
        
        return result.toString()
    }
    
    /**
     * Returns the file system that created this path.
     */
    @Override
    FileSystem getFileSystem() {
        return fs
    }
    
    /**
     * Tests if this path is absolute.
     */
    @Override
    boolean isAbsolute() {
        return path.startsWith("/")
    }
    
    /**
     * Returns the root component of this path, or null if this path does not have a root.
     */
    @Override
    Path getRoot() {
        if (isAbsolute()) {
            return new BlocksPath(fs, "/")
        }
        return null
    }
    
    /**
     * Returns the name of the file or directory denoted by this path.
     */
    @Override
    Path getFileName() {
        if (path.isEmpty()) {
            return this
        }
        
        // Remove trailing slash if present
        String pathStr = path
        if (pathStr.endsWith("/") && pathStr.length() > 1) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Find the last separator
        int lastSeparator = pathStr.lastIndexOf("/")
        if (lastSeparator == -1) {
            return this
        }
        
        // Return the last segment
        return new BlocksPath(fs, pathStr.substring(lastSeparator + 1))
    }
    
    /**
     * Returns the parent path, or null if this path does not have a parent.
     */
    @Override
    Path getParent() {
        if (path.isEmpty() || path.equals("/")) {
            return null
        }
        
        // Remove trailing slash if present
        String pathStr = path
        if (pathStr.endsWith("/") && pathStr.length() > 1) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Find the last separator
        int lastSeparator = pathStr.lastIndexOf("/")
        if (lastSeparator == -1) {
            return null
        }
        
        // Handle root directory
        if (lastSeparator == 0) {
            return new BlocksPath(fs, "/")
        }
        
        // Return the parent path
        return new BlocksPath(fs, pathStr.substring(0, lastSeparator))
    }
    
    /**
     * Returns the number of name elements in the path.
     */
    @Override
    int getNameCount() {
        if (path.isEmpty()) {
            return 0
        }
        
        // Handle root directory
        if (path.equals("/")) {
            return 0
        }
        
        // Split the path into segments
        String pathStr = path
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1)
        }
        if (pathStr.endsWith("/")) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Count non-empty segments
        String[] segments = pathStr.split("/")
        int count = 0
        for (String segment : segments) {
            if (!segment.isEmpty()) {
                count++
            }
        }
        
        return count
    }
    
    /**
     * Returns a name element of this path.
     */
    @Override
    Path getName(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Index must be non-negative")
        }
        
        // Handle root directory
        if (path.equals("/")) {
            throw new IllegalArgumentException("Root directory has no name elements")
        }
        
        // Split the path into segments
        String pathStr = path
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1)
        }
        if (pathStr.endsWith("/")) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Get the segments
        String[] segments = pathStr.split("/")
        List<String> nonEmptySegments = segments.findAll { !it.isEmpty() }
        
        if (index >= nonEmptySegments.size()) {
            throw new IllegalArgumentException("Index " + index + " is out of bounds")
        }
        
        return new BlocksPath(fs, nonEmptySegments[index])
    }
    
    /**
     * Returns a path that is a subsequence of the name elements of this path.
     */
    @Override
    Path subpath(int beginIndex, int endIndex) {
        if (beginIndex < 0) {
            throw new IllegalArgumentException("beginIndex must be non-negative")
        }
        if (endIndex <= beginIndex) {
            throw new IllegalArgumentException("endIndex must be greater than beginIndex")
        }
        
        // Handle root directory
        if (path.equals("/")) {
            throw new IllegalArgumentException("Root directory has no name elements")
        }
        
        // Split the path into segments
        String pathStr = path
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1)
        }
        if (pathStr.endsWith("/")) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Get the segments
        String[] segments = pathStr.split("/")
        List<String> nonEmptySegments = segments.findAll { !it.isEmpty() }
        
        if (beginIndex >= nonEmptySegments.size() || endIndex > nonEmptySegments.size()) {
            throw new IllegalArgumentException("Index out of bounds")
        }
        
        // Join the subpath segments
        String subpathStr = nonEmptySegments[beginIndex..<endIndex].join("/")
        return new BlocksPath(fs, subpathStr)
    }
    
    /**
     * Tests if this path starts with the given path.
     */
    @Override
    boolean startsWith(Path other) {
        if (!(other instanceof BlocksPath)) {
            return false
        }
        
        // Get the string representation of the paths
        String thisPath = this.toString()
        String otherPathStr = other.toString()
        
        // Check if this path starts with the other path
        return thisPath.startsWith(otherPathStr) && (
            thisPath.length() == otherPathStr.length() || 
            thisPath.charAt(otherPathStr.length()) == '/'
        )
    }
    
    /**
     * Tests if this path starts with the given string.
     */
    @Override
    boolean startsWith(String other) {
        return startsWith(new BlocksPath(fs, other))
    }
    
    /**
     * Tests if this path ends with the given path.
     */
    @Override
    boolean endsWith(Path other) {
        if (!(other instanceof BlocksPath)) {
            return false
        }
        
        // Get the string representation of the paths
        String thisPath = this.toString()
        String otherPathStr = other.toString()
        
        // Handle absolute paths
        if (other.isAbsolute()) {
            return thisPath.equals(otherPathStr)
        }
        
        // Check if this path ends with the other path
        return thisPath.endsWith(otherPathStr) && (
            thisPath.length() == otherPathStr.length() || 
            thisPath.charAt(thisPath.length() - otherPathStr.length() - 1) == '/'
        )
    }
    
    /**
     * Tests if this path ends with the given string.
     */
    @Override
    boolean endsWith(String other) {
        return endsWith(new BlocksPath(fs, other))
    }
    
    /**
     * Returns a path that is this path with redundant name elements eliminated.
     */
    @Override
    Path normalize() {
        // The path is already normalized during construction
        return this
    }
    
    /**
     * Resolves the given path against this path.
     */
    @Override
    Path resolve(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new IllegalArgumentException("Path must be a BlocksPath")
        }
        
        BlocksPath otherPath = (BlocksPath) other
        
        // If the other path is absolute, return it
        if (otherPath.isAbsolute()) {
            return otherPath
        }
        
        // If the other path is empty, return this path
        if (otherPath.toString().isEmpty()) {
            return this
        }
        
        // If this path is the root, just append the other path
        if (path.equals("/")) {
            return new BlocksPath(fs, path + otherPath.toString())
        }
        
        // Join the paths with a separator
        return new BlocksPath(fs, path + "/" + otherPath.toString())
    }
    
    /**
     * Resolves the given string against this path.
     */
    @Override
    Path resolve(String other) {
        return resolve(new BlocksPath(fs, other))
    }
    
    /**
     * Resolves the given path against this path's parent.
     */
    @Override
    Path resolveSibling(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new IllegalArgumentException("Path must be a BlocksPath")
        }
        
        Path parent = getParent()
        if (parent == null) {
            return other
        }
        
        return parent.resolve(other)
    }
    
    /**
     * Resolves the given string against this path's parent.
     */
    @Override
    Path resolveSibling(String other) {
        return resolveSibling(new BlocksPath(fs, other))
    }
    
    /**
     * Constructs a relative path between this path and a given path.
     */
    @Override
    Path relativize(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new IllegalArgumentException("Path must be a BlocksPath")
        }
        
        BlocksPath otherPath = (BlocksPath) other
        
        // Check if both paths are absolute or both are relative
        if (this.isAbsolute() != otherPath.isAbsolute()) {
            throw new IllegalArgumentException("Cannot relativize paths when one is absolute and the other is relative")
        }
        
        // Get the string representation of the paths
        String thisPath = this.toString()
        String otherPathStr = otherPath.toString()
        
        // Handle edge cases
        if (thisPath.equals(otherPathStr)) {
            return new BlocksPath(fs, "")
        }
        
        // Split the paths into components
        String[] thisComponents = thisPath.split("/")
        String[] otherComponents = otherPathStr.split("/")
        
        // Find the common prefix
        int commonPrefixLength = 0
        int minLength = Math.min(thisComponents.length, otherComponents.length)
        
        while (commonPrefixLength < minLength && 
               thisComponents[commonPrefixLength].equals(otherComponents[commonPrefixLength])) {
            commonPrefixLength++
        }
        
        // Build the relative path
        StringBuilder relPath = new StringBuilder()
        
        // Add ".." for each component in this path after the common prefix
        for (int i = commonPrefixLength; i < thisComponents.length; i++) {
            if (thisComponents[i].length() > 0) {
                if (relPath.length() > 0) {
                    relPath.append("/")
                }
                relPath.append("..")
            }
        }
        
        // Add components from the other path after the common prefix
        for (int i = commonPrefixLength; i < otherComponents.length; i++) {
            if (otherComponents[i].length() > 0) {
                if (relPath.length() > 0) {
                    relPath.append("/")
                }
                relPath.append(otherComponents[i])
            }
        }
        
        return new BlocksPath(fs, relPath.toString())
    }
    
    /**
     * Returns a URI to represent this path.
     */
    @Override
    URI toUri() {
        // For now, return a simple blocks+file URI
        // In a real implementation, this should reconstruct the original URI
        try {
            return new URI("blocks+file", null, toString(), null, null)
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid path for URI: " + toString(), e)
        }
    }
    
    /**
     * Returns a Path object representing the absolute path of this path.
     */
    @Override
    Path toAbsolutePath() {
        if (isAbsolute()) {
            return this
        }
        
        // Prepend a slash to make it absolute
        return new BlocksPath(fs, "/" + path)
    }
    
    /**
     * Returns the real path of an existing file.
     */
    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        // In blocks:// file system, there are no symbolic links, so the real path is the absolute path
        if (!fs.provider().exists(this)) {
            throw new NoSuchFileException(toString())
        }
        
        return toAbsolutePath()
    }
    
    /**
     * Registers the file located by this path with a watch service.
     */
    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException("Watch service not supported")
    }
    
    /**
     * Registers the file located by this path with a watch service.
     */
    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        return register(watcher, events, new WatchEvent.Modifier[0])
    }
    
    /**
     * Returns an iterator over the name elements of this path.
     */
    @Override
    Iterator<Path> iterator() {
        // Handle empty or root path
        if (path.isEmpty() || path.equals("/")) {
            return Collections.<Path>emptyList().iterator()
        }
        
        // Split the path into segments
        String pathStr = path
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1)
        }
        if (pathStr.endsWith("/")) {
            pathStr = pathStr.substring(0, pathStr.length() - 1)
        }
        
        // Create a path for each segment
        String[] segments = pathStr.split("/")
        List<Path> paths = []
        segments.findAll { !it.isEmpty() }.each { segment ->
            paths.add(new BlocksPath(fs, segment) as Path)
        }
        
        return paths.iterator()
    }
    
    /**
     * Compares two paths lexicographically.
     */
    @Override
    int compareTo(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new ClassCastException("Path must be a BlocksPath")
        }
        
        return toString().compareTo(other.toString())
    }
    
    /**
     * Returns the string representation of this path.
     */
    @Override
    String toString() {
        return path
    }
}