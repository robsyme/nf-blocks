package robsyme.cas

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/** An S3 member's location: a bucket and a key prefix that is empty or ends in '/' (DESIGN.md §2). */
@CompileStatic
@EqualsAndHashCode
class S3Location {
    final String bucket
    final String prefix

    private S3Location(String bucket, String prefix) { this.bucket = bucket; this.prefix = prefix }

    /** From an already validated s3://<bucket>[/<prefix>] (CasConfig.S3_LOCATION); a trailing slash is dropped. */
    static S3Location parse(String uri) {
        final String rest = uri.substring('s3://'.length())
        final int slash = rest.indexOf('/')
        if( slash < 0 )
            return new S3Location(rest, '')
        final String path = rest.substring(slash + 1).replaceAll('/+$', '')
        return new S3Location(rest.substring(0, slash), path ? path + '/' : '')
    }

    @Override
    String toString() { prefix ? "s3://${bucket}/${prefix[0..-2]}" : "s3://${bucket}" }
}
