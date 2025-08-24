package nextflow.blocks.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

/**
 * Implementation of BasicFileAttributeView for the BlocksFileSystem.
 */
@Slf4j
@CompileStatic
class BlocksBasicFileAttributeView implements BasicFileAttributeView {
    private final BlocksPath path
    
    /**
     * Creates a new BlocksBasicFileAttributeView.
     *
     * @param path The path to the file
     */
    BlocksBasicFileAttributeView(BlocksPath path) {
        this.path = path
    }
    
    /**
     * Returns the name of this attribute view.
     */
    @Override
    String name() {
        return "basic"
    }
    
    /**
     * Reads the basic file attributes as a bulk operation.
     */
    @Override
    BasicFileAttributes readAttributes() throws IOException {
        return new BlocksBasicFileAttributes(path)
    }
    
    /**
     * Updates the file's last modified time, last access time, and creation time attributes.
     * This operation is not supported in the BlocksFileSystem as it is read-only.
     */
    @Override
    void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        // BlocksFileSystem is read-only
        throw new IOException("Read-only file system")
    }
}