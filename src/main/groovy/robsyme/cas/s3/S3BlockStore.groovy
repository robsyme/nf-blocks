package robsyme.cas.s3

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.stream.Stream

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.HashBufferPool
import robsyme.cas.core.Hashing
import robsyme.cas.core.LoggedStore
import robsyme.cas.core.NoSuchBlockException
import robsyme.cas.core.Providers
import robsyme.cas.core.RetainedStore
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.StoreLogStorage

/**
 * A member's blocks in S3 (DESIGN.md §5, ticket 02): blocks/<xx>/<cid> under
 * the member prefix. "Already exists" is success (rule 4): a HEAD first at
 * 1 MiB and up, because S3 reads a whole body before a 412 (ticket 14), and
 * If-None-Match on every write for the race. Immutability is those
 * conditional writes; nothing is ever replaced, so LastModified is stable.
 *
 * Every body is a file range or encoded metadata bytes, never file content in
 * heap (DESIGN.md §0 rule 2): a stream is spooled to tmpDir through one pool
 * buffer, and a multipart upload sends ranges of that file.
 */
@Slf4j
@CompileStatic
class S3BlockStore implements BlockStore, LoggedStore, RetainedStore {

    static final long SINGLE_REQUEST_MAX = 5L << 30
    static final long HEAD_FIRST_BYTES = 1L << 20
    static final long MIN_PART = 64L << 20
    static final int MAX_PARTS = 10_000
    static final int ATTEMPTS = 3
    static final String IMMUTABLE = 'public, max-age=31536000, immutable'

    final S3Ops ops
    final String prefix
    private final String alias
    private final boolean writable
    private final Path tmpDir
    private final long singleRequestMax

    /** Storage class, SSE and requester pays are the S3Ops' own (SdkS3Ops applies them to every write). */
    S3BlockStore(S3Ops ops, String prefix, String alias, boolean writable, Path tmpDir,
                 long singleRequestMax = SINGLE_REQUEST_MAX) {
        this.ops = ops
        this.prefix = prefix ?: ''
        this.alias = alias
        this.writable = writable
        this.tmpDir = tmpDir
        this.singleRequestMax = singleRequestMax
    }

    /** The part size for a multipart upload of size bytes: 64 MiB, or larger so there are at most 10,000 parts. */
    static long partSize(long size) {
        return Math.max(MIN_PART, (long) Math.ceil(size / (double) MAX_PARTS))
    }

    String key(Cid cid) {
        final String text = cid.toString()
        return "${prefix}blocks/${text.substring(text.length() - 2)}/${text}".toString()
    }

    /** CopyObject's own limit (a source over 5 GiB is refused); a caller over this streams instead of calling copyOut. */
    long getSingleRequestMax() { singleRequestMax }

    @Override String alias() { alias }

    @Override boolean isWritable() { writable }

    @Override StoreLogStorage storeLogStorage() { new S3StoreLogStorage(ops, prefix) }

    @Override RetentionStorage retentionStorage() { new S3RetentionStorage(ops, prefix) }

    @Override boolean has(Cid cid) { ops.head(key(cid)) != null }

    private S3Head headOrThrow(Cid cid) {
        final S3Head h = ops.head(key(cid))
        if( h == null )
            throw new NoSuchBlockException(cid, alias)
        return h
    }

    @Override long size(Cid cid) { headOrThrow(cid).size }

    @Override long lastModifiedMillis(Cid cid) { headOrThrow(cid).lastModifiedMillis }

    @Override
    InputStream open(Cid cid) {
        final InputStream input = ops.get(key(cid), null, 0L, -1L)
        if( input == null )
            throw new NoSuchBlockException(cid, alias)
        return input
    }

    /** Bytes already said to hash to cid: spooled and checked first (rule 2), then placed. */
    @Override
    void put(Cid cid, InputStream input, long expectedSize) {
        checkWritable()
        // Already there: skip reading a large stream the HEAD-first rule would discard (ticket 02 answer 2).
        if( expectedSize >= HEAD_FIRST_BYTES && ops.head(key(cid)) != null )
            return
        withSpool(input) { Path spool, byte[] digest, long written ->
            if( !Arrays.equals(digest, cid.digest) )
                throw new BlockMismatchException(cid, "the bytes hash to ${Cid.of(cid.codec, digest)}")
            if( expectedSize >= 0 && written != expectedSize )
                throw new BlockMismatchException(cid, "$written bytes arrived, not the announced $expectedSize")
            placeFile(cid, spool, written)
            return cid
        }
    }

    @Override
    Cid putStreaming(InputStream input) {
        checkWritable()
        return withSpool(input) { Path spool, byte[] digest, long written ->
            final Cid cid = Cid.of(Cid.RAW, digest)
            placeFile(cid, spool, written)
            return cid
        }
    }

    /** A file on the default filesystem: hashed where it is, uploaded from it, no spool (silent decision 13). */
    Cid putFile(Path file) {
        checkWritable()
        final Cid cid = Hashing.hashRaw(file)
        placeFile(cid, file, Files.size(file))
        return cid
    }

    /**
     * An S3 source into this member without the bytes leaving S3 (ticket 16).
     * Up to the single-request limit, CopyObject with SHA-256: straight to the
     * final key when the node's digest names it (compared, deleted and refused
     * on mismatch), else through tmp/<uuid>. Above it, UploadPartCopy when a
     * node digest names the key; otherwise null, and the caller reads the bytes.
     */
    S3Copied copyFrom(String sourceBucket, String sourceKey, long size, Cid expected) {
        checkWritable()
        if( expected != null && ops.head(key(expected)) != null )
            return new S3Copied(expected, Providers.FUSION_NODE)
        if( size > singleRequestMax )
            return expected == null ? null : new S3Copied(partCopy(sourceBucket, sourceKey, size, expected), Providers.FUSION_NODE)
        if( expected != null ) {
            final String k = key(expected)
            final S3Written w = copyRetrying(sourceBucket, sourceKey, k)
            if( w.status == S3Written.Status.WRITTEN ) {
                final byte[] sha
                try {
                    sha = copiedSha256(w, sourceBucket, sourceKey)
                }
                catch( IOException e ) {
                    // Nothing confirmed the bytes at the address: remove them, and the caller reads the file.
                    throw removeOrRefuse(k, e)
                }
                if( !Arrays.equals(sha, expected.digest) )
                    throw removeOrRefuse(k, new BlockMismatchException(expected, ".command.cas says ${expected}, S3 hashed s3://${sourceBucket}/${sourceKey} as ${w.sha256}"))
            }
            return new S3Copied(expected, w.status == S3Written.Status.WRITTEN ? Providers.S3_COPY : Providers.FUSION_NODE)
        }
        final String staging = "${prefix}tmp/${UUID.randomUUID()}".toString()
        try {
            final S3Written staged = copyRetrying(sourceBucket, sourceKey, staging)
            final Cid cid = Cid.of(Cid.RAW, copiedSha256(staged, sourceBucket, sourceKey))
            if( ops.head(key(cid)) == null )
                copyRetrying(ops.bucket, staging, key(cid))
            return new S3Copied(cid, Providers.S3_COPY)
        }
        finally {
            try {
                ops.delete(staging)
            }
            catch( Exception e ) {
                // The tmp/ lifecycle rule removes it; a placed block is still placed, and a failure keeps its own cause.
                log.warn("could not delete the staging copy ${ops.describe()}/${staging} (${e.message}); the tmp/ lifecycle rule removes it")
            }
        }
    }

    /**
     * Deletes an unconfirmed copy at a block key and hands back cause to throw.
     * When the delete fails (the hardening bucket policy denies DeleteObject on
     * blocks/), the object stays where a later HEAD would accept it, so the
     * answer is an S3UnremovedCopyException naming the key, never a fallback.
     */
    private IOException removeOrRefuse(String k, IOException cause) {
        try {
            ops.delete(k)
            return cause
        }
        catch( Exception e ) {
            final S3UnremovedCopyException refused = new S3UnremovedCopyException("${ops.describe()}/${k}".toString(), cause)
            refused.addSuppressed(e)
            return refused
        }
    }

    /** A copy's full-object SHA-256; an IOException when S3 returned none, or a composite one (<base64>-<parts>). */
    private static byte[] copiedSha256(S3Written w, String sourceBucket, String sourceKey) {
        if( w.sha256 == null || w.sha256.contains('-') )
            throw new IOException("S3 returned no full-object SHA-256 copying s3://${sourceBucket}/${sourceKey} (${w.sha256})")
        return Base64.decoder.decode(w.sha256)
    }

    /**
     * An S3 refusal that is not a 412 or a 409 reaches here as the SDK's
     * unchecked exception; as an IOException the caller falls back to reading the bytes.
     */
    private static <T> T checked(String what, Closure<T> request) {
        try {
            return request.call()
        }
        catch( RuntimeException e ) {
            throw new IOException("${what}: ${e.message}", e)
        }
    }

    private S3Written copyRetrying(String bucket, String srcKey, String dstKey) {
        for( int attempt = 1; attempt <= ATTEMPTS; attempt++ ) {
            final S3Written w = checked("copying s3://${bucket}/${srcKey} to ${ops.describe()}/${dstKey}".toString()) {
                ops.copy(bucket, srcKey, dstKey, S3PutOptions.create().ifNoneMatch().sha256().cacheControl(IMMUTABLE))
            }
            if( w.status != S3Written.Status.CONFLICT ) return w
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${ATTEMPTS} times copying to ${ops.describe()}/${dstKey}")
    }

    private Cid partCopy(String bucket, String srcKey, long size, Cid expected) {
        final String k = key(expected)
        final String what = "copying s3://${bucket}/${srcKey} in parts to ${ops.describe()}/${k}".toString()
        final String id = checked(what) { ops.createMultipart(k, S3PutOptions.create().cacheControl(IMMUTABLE)) }
        try {
            final long part = partSize(size)
            final List<S3Part> parts = []
            long first = 0
            for( int n = 1; first < size; n++ ) {
                final long last = Math.min(first + part, size) - 1
                final int number = n
                final long from = first
                parts.add(checked(what) { ops.uploadPartCopy(k, id, number, bucket, srcKey, from, last) })
                first = last + 1
            }
            final S3Written done = checked(what) { ops.completeMultipart(k, id, parts, true) }
            if( done.status != S3Written.Status.WRITTEN )
                abortQuietly(k, id, null)
            return expected
        }
        catch( Exception e ) {
            abortQuietly(k, id, e)
            throw e
        }
    }

    /** An abort that fails leaves parts for the lifecycle rule; it never hides the failure that led here. */
    private void abortQuietly(String k, String id, Exception cause) {
        try {
            ops.abortMultipart(k, id)
        }
        catch( Exception e ) {
            if( cause != null ) cause.addSuppressed(e)
            log.warn("could not abort the multipart copy to ${ops.describe()}/${k} (${e.message})")
        }
    }

    /**
     * A block into another bucket without the bytes leaving S3, checked by
     * S3's SHA-256 (silent decision 9). An SDK refusal reaches the caller as
     * an IOException (not a BlockMismatchException), the same as every other
     * S3 write here, so it can fall back rather than abort. A composite
     * SHA-256 (over the single-request limit, which the caller is expected to
     * have already ruled out) is treated like a missing one: never decoded as
     * base64, since it is not a plain digest.
     */
    void copyOut(Cid cid, String targetBucket, String targetKey) {
        final S3Written w = checked("copying ${ops.describe()}/${key(cid)} to s3://${targetBucket}/${targetKey}".toString()) {
            ops.copyOut(key(cid), targetBucket, targetKey)
        }
        if( w.sha256 == null || w.sha256.contains('-') || Base64.decoder.decode(w.sha256) != cid.digest )
            throw new BlockMismatchException(cid, "S3 copied it to s3://${targetBucket}/${targetKey} as ${w.sha256}")
    }

    @Override
    Cid putDagCbor(Object value) {
        checkWritable()
        final byte[] encoded = DagCbor.encode(value)
        final Cid cid = DagCbor.cidOf(encoded)
        if( encoded.length > singleRequestMax )
            throw new IllegalStateException("a ${encoded.length}-byte metadata block is over the single-request limit")
        place(cid, S3Body.ofBytes(encoded), null)
        return cid
    }

    @Override
    Stream<Cid> listBlocks() {
        final String under = "${prefix}blocks/".toString()
        return ops.list(under, 0).stream()
            .map { S3Listed o -> o.key.substring(o.key.lastIndexOf('/') + 1) }
            .filter { String name -> Cid.isCid(name) }
            .map { String name -> Cid.parse(name) }
    }

    /**
     * Hashes input into a file in tmpDir through one pool buffer, hands the file,
     * its SHA-256 and its length to body, and deletes it. Only a failure to create
     * or write the spool names cas.tmpDir; a failing input, and whatever body
     * throws (a BlockMismatchException, a 409 IOException), propagate unchanged.
     */
    private <T> T withSpool(InputStream input, Closure<T> body) {
        Path spool = null
        try {
            try {
                Files.createDirectories(tmpDir)
                spool = Files.createTempFile(tmpDir, 'nf-blocks-', '.spool')
            }
            catch( IOException e ) {
                throw spoolFailure(e)
            }
            final Path into = spool
            final long[] written = new long[1]
            final byte[] digest = HashBufferPool.shared().withBuffer { byte[] buffer ->
                final MessageDigest md = MessageDigest.getInstance('SHA-256')
                openSpool(into).withCloseable { FileChannel ch ->
                    final ByteBuffer view = ByteBuffer.wrap(buffer)
                    int n
                    while( (n = input.read(buffer, 0, buffer.length)) != -1 ) {
                        if( n == 0 )
                            continue
                        md.update(buffer, 0, n)
                        view.limit(n).position(0)
                        try {
                            while( view.hasRemaining() )
                                ch.write(view)
                        }
                        catch( IOException e ) {
                            throw spoolFailure(e)
                        }
                        written[0] += n
                    }
                }
                return md.digest()
            } as byte[]
            return body.call(spool, digest, written[0])
        }
        finally {
            if( spool != null )
                Files.deleteIfExists(spool)
        }
    }

    private FileChannel openSpool(Path spool) {
        try {
            return FileChannel.open(spool, StandardOpenOption.WRITE)
        }
        catch( IOException e ) {
            throw spoolFailure(e)
        }
    }

    private IOException spoolFailure(IOException e) {
        return new IOException("could not spool to cas.tmpDir (${tmpDir}): ${e.message}", e)
    }

    private void placeFile(Cid cid, Path file, long length) {
        place(cid, S3Body.ofFile(file, 0L, length), file)
    }

    /**
     * Puts the body at the block's key unless it is there; a 412 is success, a 409
     * is retried. file is the body's source when it is a whole file, so a body over
     * the single-request limit can be sent as ranges of it.
     */
    private void place(Cid cid, S3Body body, Path file) {
        final String key = key(cid)
        if( body.length >= HEAD_FIRST_BYTES && ops.head(key) != null )
            return
        for( int attempt = 1; attempt <= ATTEMPTS; attempt++ ) {
            final boolean whole = body.length <= singleRequestMax
            final S3Written w = whole ? single(key, body) : multipart(key, file, body.length)
            if( w.status == S3Written.Status.EXISTS )
                return
            if( w.status == S3Written.Status.WRITTEN ) {
                // A multipart upload's SHA-256 is composite (<base64>-<parts>): nothing to compare.
                if( whole )
                    checkDigest(cid, key, w.sha256)
                return
            }
            log.debug("409 ConditionalRequestConflict writing ${key}; attempt ${attempt} of ${ATTEMPTS}")
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${ATTEMPTS} times writing ${ops.describe()}/${key}")
    }

    private S3Written single(String key, S3Body body) {
        return ops.put(key, body, S3PutOptions.create().ifNoneMatch().sha256().cacheControl(IMMUTABLE))
    }

    /**
     * Parts are ranges of the file, each reopened by the SDK on a retry, never
     * buffered. The upload asks for SHA-256 at creation so it agrees with the
     * SHA-256 SdkS3Ops sends on every part. Complete is conditional; a 412 or a
     * 409 there aborts the upload, so no parts are left behind.
     */
    private S3Written multipart(String key, Path file, long length) {
        final String id = ops.createMultipart(key, S3PutOptions.create().sha256().cacheControl(IMMUTABLE))
        try {
            final long part = partSize(length)
            final List<S3Part> parts = new ArrayList<S3Part>()
            long offset = 0
            for( int n = 1; offset < length; n++ ) {
                final long size = Math.min(part, length - offset)
                parts.add(ops.uploadPart(key, id, n, S3Body.ofFile(file, offset, size)))
                offset += size
            }
            final S3Written done = ops.completeMultipart(key, id, parts, true)
            if( done.status != S3Written.Status.WRITTEN )
                ops.abortMultipart(key, id)
            return done
        }
        catch( Exception e ) {
            ops.abortMultipart(key, id)
            throw e
        }
    }

    /**
     * S3 validated and stored a SHA-256 of what it received; it must be the
     * address (silent decision 13). A mismatch is deleted; one that cannot be
     * deleted is an S3UnremovedCopyException naming the key, as for a copy.
     */
    private void checkDigest(Cid cid, String key, String sha256) {
        if( sha256 == null )
            return
        if( !Arrays.equals(Base64.decoder.decode(sha256), cid.digest) )
            throw removeOrRefuse(key, new BlockMismatchException(cid, "S3 stored bytes whose SHA-256 is ${sha256}; the source changed while it was read"))
    }

    private void checkWritable() {
        if( !writable )
            throw new IllegalStateException("store '$alias' is read-only")
    }

    @Override String toString() { "S3BlockStore[$alias at ${ops.describe()}/${prefix}]" }
}
