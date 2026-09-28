package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The Publish Coordinate tree of one member (DESIGN.md §5, §7): Pointer Files
 * at relative paths, overwritten by the next run to the same coordinate,
 * never blocks and never roots. A published directory is one pointer at a
 * DirectoryManifest. Local and S3 trees are held to one contract
 * (CoordinateTreeContract), conflicts included.
 */
@CompileStatic
interface CoordinateTree {
    Optional<StoreRef> read(String relPath)
    void write(String relPath, StoreRef ref)
    /** A pointer, or a directory on the way to one. */
    boolean exists(String relPath)
    /** A directory on the way to pointers (not a pointer at a manifest). */
    boolean isDirectory(String relPath)
    /** A directory on the way, or a pointer at a DirectoryManifest. */
    boolean isDirectoryCoordinate(String relPath)
    /** Names directly under a directory, sorted; empty for anything else. */
    List<String> children(String relPath)
    /** Removes a pointer only; an IOException for a directory. */
    boolean delete(String relPath)
    void createDirectories(String relPath)
    /** 0 when unknown. */
    long lastModifiedMillis(String relPath)
}
