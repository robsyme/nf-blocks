package robsyme.cas.s3

import java.security.MessageDigest

/**
 * An S3 bucket in memory, answering as the measured S3 does (ticket 14): a
 * conditional PUT reads the whole body before its 412, a copy or a PUT with
 * sha256 returns the full-object SHA-256, a multipart upload is checked only
 * at Complete. Counts every request and every body byte read.
 */
class MemoryS3Ops implements S3Ops {

    static class Obj {
        byte[] bytes
        String etag
        String sha256
        Map<String, String> metadata = [:]
        String cacheControl
        long lastModified
        String text() { new String(bytes, 'UTF-8') }
    }

    final String bucket
    final Map<String, Obj> objects = new TreeMap<>()
    final Map<String, Map<Integer, byte[]>> uploads = [:]
    final List<String> calls = []
    final Map<String, Integer> conflicts = [:]
    final Map<String, MemoryS3Ops> peers = [:]
    long pulledBytes = 0
    Long serverDateMillis = null
    boolean discard = false
    private long clock = 1_790_000_000_000L
    private int nextId = 0
    private final Map<String, S3Upload> openUploads = new TreeMap<>()

    MemoryS3Ops(String bucket) { this.bucket = bucket }

    void advance(long millis) { clock += millis }

    void putText(String key, String text) { store(key, text.getBytes('UTF-8'), S3PutOptions.create()) }

    private MemoryS3Ops bucketNamed(String name) { name == bucket ? this : peers[name] }

    private boolean conflict(String op) {
        final Integer left = conflicts[op]
        if( !left ) return false
        conflicts[op] = left - 1
        return true
    }

    private byte[] drain(S3Body body) {
        final ByteArrayOutputStream out = discard ? null : new ByteArrayOutputStream()
        final MessageDigest md = MessageDigest.getInstance('SHA-256')
        final byte[] buf = new byte[65536]
        body.open().withCloseable { InputStream in ->
            int n
            while( (n = in.read(buf)) > 0 ) { pulledBytes += n; md.update(buf, 0, n); out?.write(buf, 0, n) }
        }
        lastDigest = md.digest()
        return out == null ? new byte[0] : out.toByteArray()
    }
    private byte[] lastDigest

    private Obj store(String key, byte[] bytes, S3PutOptions o, byte[] digest = null) {
        final byte[] d = digest ?: MessageDigest.getInstance('SHA-256').digest(bytes)
        final Obj obj = new Obj(bytes: bytes, etag: '"' + (++nextId) + '-' + key.hashCode() + '"',
            sha256: o.sha256 ? Base64.encoder.encodeToString(d) : null,
            metadata: new LinkedHashMap<String, String>(o.metadata ?: [:]), cacheControl: o.cacheControl,
            lastModified: ++clock)
        objects[key] = obj
        return obj
    }

    private S3Written refused(String key, S3PutOptions o) {
        final Obj existing = objects[key]
        if( o.ifNoneMatch && existing != null ) return new S3Written(S3Written.Status.EXISTS, null, null)
        if( o.ifMatch != null && (existing == null || existing.etag != o.ifMatch) ) return new S3Written(S3Written.Status.EXISTS, null, null)
        return null
    }

    @Override S3Head head(String key) {
        calls << "HEAD ${key}".toString()
        final Obj o = objects[key]
        return o == null ? null : new S3Head((long) o.bytes.length, o.etag, o.sha256, o.metadata, o.lastModified)
    }

    @Override InputStream get(String key, String ifMatch, long start, long length) {
        calls << "GET ${key}".toString()
        final Obj o = objects[key]
        if( o == null ) return null
        if( ifMatch != null && ifMatch != o.etag ) throw new S3PreconditionFailed("${key} is no longer ${ifMatch}")
        final int from = (int) start
        final int to = length < 0 ? o.bytes.length : (int) Math.min(o.bytes.length, start + length)
        return new ByteArrayInputStream(Arrays.copyOfRange(o.bytes, from, to))
    }

    @Override S3Written put(String key, S3Body body, S3PutOptions o) {
        calls << "PUT ${key}".toString()
        final byte[] bytes = drain(body)
        if( conflict('PUT') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final S3Written no = refused(key, o)
        if( no != null ) return no
        final Obj obj = store(key, bytes, o, lastDigest)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override String createMultipart(String key, S3PutOptions o) {
        calls << "MPU ${key}".toString()
        final String id = "upload-${++nextId}".toString()
        uploads[id] = new TreeMap<Integer, byte[]>()
        mpuOptions[id] = o
        openUploads[id] = new S3Upload(key, id, ++clock)
        return id
    }
    private final Map<String, S3PutOptions> mpuOptions = [:]

    @Override S3Part uploadPart(String key, String uploadId, int n, S3Body body) {
        calls << "PART ${key} ${n}".toString()
        final byte[] bytes = drain(body)
        uploads[uploadId][n] = bytes
        return new S3Part(n, '"p' + n + '"', Base64.encoder.encodeToString(lastDigest))
    }

    @Override S3Part uploadPartCopy(String key, String uploadId, int n, String srcBucket, String srcKey, long first, long last) {
        calls << "PARTCOPY ${key} ${n}".toString()
        final Obj src = bucketNamed(srcBucket).objects[srcKey]
        uploads[uploadId][n] = Arrays.copyOfRange(src.bytes, (int) first, (int) last + 1)
        return new S3Part(n, '"c' + n + '"', null)
    }

    @Override S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch) {
        calls << "COMPLETE ${key}".toString()
        if( conflict('COMPLETE') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final S3PutOptions o = mpuOptions[uploadId]
        if( ifNoneMatch && objects.containsKey(key) ) return new S3Written(S3Written.Status.EXISTS, null, null)
        final ByteArrayOutputStream all = new ByteArrayOutputStream()
        parts.each { S3Part p -> all.write(uploads[uploadId][p.partNumber]) }
        uploads.remove(uploadId)
        // Multipart SHA-256 is composite on S3 (ticket 01): for an upload created with
        // SHA-256, Complete returns base64(SHA-256 of the part digests) + '-' + the part count.
        final Obj obj = store(key, all.toByteArray(), S3PutOptions.create().cacheControl(o?.cacheControl))
        String composite = null
        if( o?.sha256 ) {
            final MessageDigest md = MessageDigest.getInstance('SHA-256')
            parts.each { S3Part p -> md.update(Base64.decoder.decode(p.sha256)) }
            composite = Base64.encoder.encodeToString(md.digest()) + '-' + parts.size()
        }
        openUploads.remove(uploadId)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, composite)
    }

    @Override void abortMultipart(String key, String uploadId) {
        calls << "ABORT ${key}".toString()
        uploads.remove(uploadId)
        openUploads.remove(uploadId)
    }

    @Override S3Written copy(String srcBucket, String srcKey, String key, S3PutOptions o) {
        calls << "COPY ${srcBucket}/${srcKey} ${key}".toString()
        if( conflict('COPY') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final Obj src = bucketNamed(srcBucket)?.objects?.get(srcKey)
        if( src == null ) throw new FileNotFoundException("no such source s3://${srcBucket}/${srcKey}")
        final S3Written no = refused(key, o)
        if( no != null ) return no
        final Obj obj = store(key, src.bytes, o)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override S3Written copyOut(String key, String targetBucket, String targetKey) {
        calls << "COPYOUT ${key} ${targetBucket}/${targetKey}".toString()
        final Obj src = objects[key]
        if( src == null ) throw new FileNotFoundException("no such source ${describe()}/${key}")
        final MemoryS3Ops target = bucketNamed(targetBucket)
        final byte[] digest = MessageDigest.getInstance('SHA-256').digest(src.bytes)
        final Obj obj = target.store(targetKey, src.bytes, S3PutOptions.create().sha256(), digest)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override List<S3Listed> list(String prefix, int maxKeys) {
        calls << "LIST ${prefix}".toString()
        final List<S3Listed> out = objects.findAll { k, v -> k.startsWith(prefix) }
            .collect { k, v -> new S3Listed(k, (long) v.bytes.length, v.lastModified) }
        return maxKeys > 0 ? out.take(maxKeys) : out
    }

    @Override void delete(String key) {
        calls << "DELETE ${key}".toString()
        objects.remove(key)
    }

    @Override Long firstServerDateMillis() { serverDateMillis }

    @Override Long lastServerDateMillis() { serverDateMillis ?: clock }

    @Override List<String> deleteMany(List<String> keys) {
        if( keys.size() > 1000 ) throw new IllegalArgumentException("DeleteObjects takes at most 1,000 keys, got ${keys.size()}")
        calls << "DELETEMANY ${keys.size()}".toString()
        keys.each { objects.remove(it) }
        return []
    }

    @Override List<S3Upload> listUploads(String prefix) {
        calls << "LISTUPLOADS ${prefix}".toString()
        return openUploads.values().findAll { it.key.startsWith(prefix) }.toList()
    }

    @Override String describe() { "memory://${bucket}" }
}
