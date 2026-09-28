package robsyme.cas.core

import java.nio.file.Path

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Where a member keeps its Index Snapshot and page (DESIGN.md §15). The
 * snapshot is always built locally (IndexSnapshot.build); this puts it in
 * place. A writer takes base() before its catch-up and replaces only that
 * version (ticket 03 decision 1, silent decision 16).
 */
@CompileStatic
interface SnapshotStorage {
    /** The snapshot there now, or null when there is none. */
    SnapshotBase base(Path tempDir)
    /** A local copy for reading, or null when there is none; the caller deletes it. */
    Path fetch(Path tempDir)
    /** Puts built in place if the snapshot is still base (null: still absent); false when another writer got there first. */
    boolean replace(Path built, int runs, SnapshotBase base)
    /** Writes the page when its bytes differ; true when it wrote. */
    boolean writePage(byte[] page)
    String describe()
}

/** A version of a snapshot: an opaque tag (an ETag, or size:mtime:inode), its size, its run rows (-1 when uncountable). */
@Canonical
@CompileStatic
class SnapshotBase {
    String tag
    long bytes
    int runs
}
