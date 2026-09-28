package robsyme.cas.explore

import groovy.transform.CompileStatic
import robsyme.cas.s3.S3Head
import robsyme.cas.s3.S3Listed
import robsyme.cas.s3.S3Ops

/**
 * A member in a bucket, read with the user's own credentials so the page
 * never holds any (block explorer spec section 1.4), through the one S3 seam
 * (DESIGN.md §15). Every read is a ranged GetObject with If-Match on the ETag
 * the object had when opened, so a replaced object fails the read
 * (S3PreconditionFailed) instead of answering with the new object's bytes.
 */
@CompileStatic
class S3MemberFiles implements MemberFiles {

    private final S3Ops ops
    private final String prefix

    S3MemberFiles(S3Ops ops, String prefix) {
        this.ops = ops
        this.prefix = prefix
    }

    /**
     * {@code location.host} is null for a legal bucket name whose last label
     * starts with a digit ({@code data.2024}, {@code logs.v1.0}): those fail
     * java.net.URI's server-based authority syntax (RFC 2396 toplabel must
     * start with a letter), so URI falls back to registry-based parsing,
     * which leaves {@code host} null but still fills {@code authority} with
     * the same raw string. CasConfig's S3_LOCATION already refused anything
     * with '?', '#' or whitespace, so authority here is exactly the bucket.
     */
    static List<String> bucketAndPrefix(URI location) {
        String prefix = (location.path ?: '').replaceAll('^/+', '')
        if( prefix && !prefix.endsWith('/') )
            prefix += '/'
        return [location.authority, prefix]
    }

    /**
     * HeadObject for the size and ETag; every read is a ranged GetObject with
     * If-Match on that ETag, so an object replaced after it was opened fails
     * the read (S3PreconditionFailed) instead of answering with the new
     * object's bytes.
     */
    @Override
    MemberFiles.Opened open(String rel) {
        final String key = prefix + rel
        final S3Head head = ops.head(key)
        return head == null ? null : new Opened(ops, key, head.size, head.etag)
    }

    @CompileStatic
    private static class Opened implements MemberFiles.Opened {
        private final S3Ops ops
        private final String key
        final long size
        final String tag
        Opened(S3Ops ops, String key, long size, String tag) { this.ops = ops; this.key = key; this.size = size; this.tag = tag }
        @Override InputStream read(long start, long length) { ops.get(key, tag, start, length) }
        @Override void close() {}
    }

    @Override
    List<String> list(String dirRel) {
        final String under = prefix + dirRel.replaceAll('/+$', '') + '/'
        final List<String> names = []
        for( S3Listed object : ops.list(under, 0) ) {
            final String name = object.key.substring(under.length())
            if( name && !name.contains('/') )
                names.add(name)
        }
        return names
    }

    @Override
    String describe() { "s3://${ops.bucket}/${prefix}" }
}
