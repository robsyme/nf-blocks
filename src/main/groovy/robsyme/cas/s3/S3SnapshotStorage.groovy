package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

import groovy.transform.CompileStatic
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.SnapshotStorage

/**
 * The snapshot and page of an S3 member (ticket 02 decision 8, ticket 03
 * decision 1): one PutObject each, the snapshot `no-cache` with its run
 * count as x-amz-meta-runs (and its store id and watermark as
 * x-amz-meta-store-id and x-amz-meta-watermark, §20 decision 10, and
 * whether it has storage figures as x-amz-meta-storage, `full` or `lean`,
 * fix round 3 item 3) and If-Match on the ETag it replaces (If-None-Match
 * when there was none), the page `no-cache` and rewritten only when its
 * stored SHA-256 differs (an ETag is no MD5 under SSE-KMS or multipart).
 */
@CompileStatic
class S3SnapshotStorage implements SnapshotStorage {

    static final String NO_CACHE = 'no-cache'
    static final String STORAGE_FULL = 'full'
    static final String STORAGE_LEAN = 'lean'
    private final S3Ops ops
    private final String snapshotKey
    private final String pageKey

    S3SnapshotStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.snapshotKey = (prefix ?: '') + IndexSnapshot.relativePath()
        this.pageKey = (prefix ?: '') + IndexSnapshot.PAGE_NAME
    }

    @Override
    SnapshotBase base(Path tempDir) {
        final S3Head head = ops.head(snapshotKey)
        if( head == null ) return null
        final Map<String, String> meta = head.metadata ?: [:]
        final String runs = meta.get('runs')
        if( runs?.isInteger() )
            return new SnapshotBase(head.etag, head.size, runs as int, meta.get('store-id'), meta.get('watermark'), figures(meta.get('storage')))
        // Uploaded by something that does not record it (gate/cloud/s3tier.py): read it once.
        final Path copy = download(head.etag, tempDir)
        try {
            return copy == null ? new SnapshotBase(head.etag, head.size, -1) : IndexSnapshot.baseOf(copy, head.etag, head.size)
        }
        finally {
            if( copy != null ) Files.deleteIfExists(copy)
        }
    }

    /** x-amz-meta-storage: true for `full`, false for `lean`, null when it is missing (written before it was kept). */
    private static Boolean figures(String storage) {
        return storage == STORAGE_FULL ? Boolean.TRUE : storage == STORAGE_LEAN ? Boolean.FALSE : null
    }

    @Override
    Path fetch(Path tempDir) { download(null, tempDir) }

    /** That version only (If-Match on its ETag): a replaced snapshot throws S3PreconditionFailed. */
    @Override
    Path fetchVersion(SnapshotBase base, Path tempDir) { download(base.tag, tempDir) }

    private Path download(String etag, Path tempDir) {
        final InputStream in = ops.get(snapshotKey, etag, 0L, -1L)
        if( in == null ) return null
        Files.createDirectories(tempDir)
        final Path copy = Files.createTempFile(tempDir, 'nf-blocks-snapshot-', '.sqlite')
        in.withCloseable { Files.copy(it, copy, StandardCopyOption.REPLACE_EXISTING) }
        return copy
    }

    @Override
    boolean replace(Path built, SnapshotBase written, SnapshotBase base) {
        final S3PutOptions o = S3PutOptions.create().sha256().cacheControl(NO_CACHE)
            .contentType('application/vnd.sqlite3').meta('runs', String.valueOf(written.runs))
        if( written.storeId != null ) o.meta('store-id', written.storeId)
        if( written.watermark != null ) o.meta('watermark', written.watermark)
        if( written.figures != null ) o.meta('storage', written.figures ? STORAGE_FULL : STORAGE_LEAN)
        if( base == null ) o.ifNoneMatch() else o.ifMatch(base.tag)
        final S3Written w = ops.put(snapshotKey, S3Body.ofFile(built, 0L, Files.size(built)), o)
        return w.status == S3Written.Status.WRITTEN
    }

    @Override
    boolean writePage(byte[] page) {
        final String sha = Base64.encoder.encodeToString(MessageDigest.getInstance('SHA-256').digest(page))
        if( ops.head(pageKey)?.sha256 == sha ) return false
        ops.put(pageKey, S3Body.ofBytes(page), S3PutOptions.create().sha256().cacheControl(NO_CACHE).contentType('text/html; charset=utf-8'))
        return true
    }

    @Override String describe() { "${ops.describe()}/${snapshotKey}" }
}
