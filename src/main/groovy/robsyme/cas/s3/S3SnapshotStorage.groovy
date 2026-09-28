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
 * count as x-amz-meta-runs and If-Match on the ETag it replaces (If-None-Match
 * when there was none), the page `no-cache` and rewritten only when its
 * stored SHA-256 differs (an ETag is no MD5 under SSE-KMS or multipart).
 */
@CompileStatic
class S3SnapshotStorage implements SnapshotStorage {

    static final String NO_CACHE = 'no-cache'
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
        final String runs = head.metadata?.get('runs')
        if( runs?.isInteger() ) return new SnapshotBase(head.etag, head.size, runs as int)
        // Uploaded by something that does not record it (gate/cloud/s3tier.py): count it once.
        final Path copy = download(head.etag, tempDir)
        try {
            return new SnapshotBase(head.etag, head.size, copy == null ? -1 : IndexSnapshot.countRuns(copy))
        }
        finally {
            if( copy != null ) Files.deleteIfExists(copy)
        }
    }

    @Override
    Path fetch(Path tempDir) { download(null, tempDir) }

    private Path download(String etag, Path tempDir) {
        final InputStream in = ops.get(snapshotKey, etag, 0L, -1L)
        if( in == null ) return null
        Files.createDirectories(tempDir)
        final Path copy = Files.createTempFile(tempDir, 'nf-blocks-snapshot-', '.sqlite')
        in.withCloseable { Files.copy(it, copy, StandardCopyOption.REPLACE_EXISTING) }
        return copy
    }

    @Override
    boolean replace(Path built, int runs, SnapshotBase base) {
        final S3PutOptions o = S3PutOptions.create().sha256().cacheControl(NO_CACHE)
            .contentType('application/vnd.sqlite3').meta('runs', String.valueOf(runs))
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
