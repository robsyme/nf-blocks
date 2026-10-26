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
    /** A local copy of that version (base.tag), or null when there is none; it may throw when the snapshot was replaced. The caller deletes it. */
    Path fetchVersion(SnapshotBase base, Path tempDir)
    /**
     * Puts built in place if the snapshot is still base (null: still absent); false when another writer got there first.
     * {@code written} describes the file: its runs, store id and watermark; its tag and bytes are ignored.
     */
    boolean replace(Path built, SnapshotBase written, SnapshotBase base)
    /** Writes the page when its bytes differ; true when it wrote. */
    boolean writePage(byte[] page)
    String describe()
}

/**
 * A version of a snapshot: an opaque tag (an ETag, or size:mtime:inode), its size, its run rows (-1 when uncountable),
 * and the store id and Store Log watermark it was written with (null when unknown, §20 decision 10).
 * {@code figures} is true when it has storage figures, false when it is lean, and null when unknown
 * (an S3 object written before x-amz-meta-storage, fix round 3 item 3).
 */
@Canonical
@CompileStatic
class SnapshotBase {
    String tag
    long bytes
    int runs
    String storeId
    String watermark
    Boolean figures
}
