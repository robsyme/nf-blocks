package robsyme.cas.s3

import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** What HeadObject says of an object. sha256 is base64, null when S3 holds none. */
@Canonical
@CompileStatic
class S3Head {
    long size
    String etag
    String sha256
    Map<String, String> metadata
    long lastModifiedMillis
}

/**
 * A request body the SDK may read more than once (a retry reopens it), so it
 * is never a stream held in memory: a file range, or a small byte array of
 * metadata (never file content, DESIGN.md §0 rule 2).
 */
@CompileStatic
abstract class S3Body {

    abstract long getLength()

    /** A fresh stream over the whole body. */
    abstract InputStream open()

    static S3Body ofFile(Path file, long offset, long length) {
        return new S3Body() {
            @Override long getLength() { length }
            @Override InputStream open() {
                final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)
                channel.position(offset)
                return new BoundedInputStream(new BufferedInputStream(Channels.newInputStream(channel), 65536), length)
            }
        }
    }

    static S3Body ofBytes(byte[] bytes) {
        return new S3Body() {
            @Override long getLength() { (long) bytes.length }
            @Override InputStream open() { new ByteArrayInputStream(bytes) }
        }
    }

    /** At most n bytes of an underlying stream. */
    @CompileStatic
    static class BoundedInputStream extends FilterInputStream {
        private long left
        BoundedInputStream(InputStream in, long n) { super(in); this.left = n }
        @Override int read() throws IOException {
            if( left <= 0 ) return -1
            final int b = super.read()
            if( b >= 0 ) left--
            return b
        }
        @Override int read(byte[] b, int off, int len) throws IOException {
            if( left <= 0 ) return -1
            final int n = super.read(b, off, (int) Math.min((long) len, left))
            if( n > 0 ) left -= n
            return n
        }
    }
}

/** How a write is made. Built with create() and chained setters. */
@CompileStatic
class S3PutOptions {
    boolean ifNoneMatch
    String ifMatch
    boolean sha256
    String cacheControl
    String contentType
    Map<String, String> metadata = [:]

    static S3PutOptions create() { new S3PutOptions() }
    S3PutOptions ifNoneMatch() { this.ifNoneMatch = true; this }
    S3PutOptions ifMatch(String etag) { this.ifMatch = etag; this }
    S3PutOptions sha256() { this.sha256 = true; this }
    S3PutOptions cacheControl(String value) { this.cacheControl = value; this }
    S3PutOptions contentType(String value) { this.contentType = value; this }
    S3PutOptions meta(String key, String value) { this.metadata.put(key, value); this }
}

/** A write's outcome: EXISTS is S3's 412 (a precondition refused), CONFLICT its 409. */
@Canonical
@CompileStatic
class S3Written {
    enum Status { WRITTEN, EXISTS, CONFLICT }
    Status status
    String etag
    String sha256
}

@Canonical
@CompileStatic
class S3Part {
    int partNumber
    String etag
    String sha256
}

@Canonical
@CompileStatic
class S3Listed {
    String key
    long size
}

/** The per-request fields of the aws scope (ticket 01 Q1): nf-amazon's client carries none of them. */
@Canonical
@CompileStatic
class S3WriteOptions {
    static final S3WriteOptions NONE = new S3WriteOptions(null, null, null, false)
    String storageClass
    String sse
    String kmsKeyId
    boolean requesterPays
}

/** A read made with If-Match on a version the object no longer is. */
@CompileStatic
class S3PreconditionFailed extends IOException {
    S3PreconditionFailed(String message) { super(message) }
}
