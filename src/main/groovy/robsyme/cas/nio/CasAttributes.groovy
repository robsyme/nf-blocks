package robsyme.cas.nio

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

import groovy.transform.CompileStatic

/**
 * The attributes of a content-addressed thing, told from the block or the
 * manifest entry it resolves to (DESIGN.md section 8). Every value is a fact
 * about content: the size is the block's, the modification time is the block
 * file's (stable per address, because a block is never rewritten), and there
 * is no creation or access time to invent, so both echo the modification time.
 *
 * {@code fileKey} is the content address string, so two paths that name the
 * same block report the same key.
 */
@CompileStatic
class CasAttributes implements BasicFileAttributes {

    private final long size
    private final boolean regular
    private final boolean directory
    private final boolean symlink
    private final boolean executable
    private final FileTime modified
    private final Object fileKey

    CasAttributes(long size, boolean regular, boolean directory, boolean symlink, boolean executable, long modifiedMillis, Object fileKey) {
        this.size = size
        this.regular = regular
        this.directory = directory
        this.symlink = symlink
        this.executable = executable
        this.modified = FileTime.fromMillis(modifiedMillis)
        this.fileKey = fileKey
    }

    @Override FileTime lastModifiedTime() { modified }

    @Override FileTime lastAccessTime() { modified }

    @Override FileTime creationTime() { modified }

    @Override boolean isRegularFile() { regular }

    @Override boolean isDirectory() { directory }

    @Override boolean isSymbolicLink() { symlink }

    @Override boolean isOther() { !regular && !directory && !symlink }

    @Override long size() { size }

    @Override Object fileKey() { fileKey }

    /** Not part of {@link BasicFileAttributes}, but the manifest entry knows it. */
    boolean isExecutable() { executable }
}
