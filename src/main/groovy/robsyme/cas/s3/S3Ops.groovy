package robsyme.cas.s3

import groovy.transform.CompileStatic

/**
 * Every S3 request nf-blocks makes, for one bucket (DESIGN.md §5). Keys are
 * relative to the bucket and never start with '/'. A 412 and a 409 are
 * answers, not failures: they come back as S3Written statuses. Anything else
 * S3 refuses is thrown.
 */
@CompileStatic
interface S3Ops {

    String getBucket()

    /** Null when the key is absent. */
    S3Head head(String key)

    /** The object's bytes from start, at most length (all when length < 0); null when absent. */
    InputStream get(String key, String ifMatch, long start, long length)

    S3Written put(String key, S3Body body, S3PutOptions options)

    String createMultipart(String key, S3PutOptions options)

    S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body)

    S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last)

    S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch)

    void abortMultipart(String key, String uploadId)

    /** A server-side copy into this bucket; no byte passes through the JVM. */
    S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions options)

    /** A server-side copy of a key in this bucket into another bucket; no byte passes through the JVM. */
    S3Written copyOut(String key, String targetBucket, String targetKey)

    /** Keys under prefix in lexicographic order, at most maxKeys (all when maxKeys <= 0). */
    List<S3Listed> list(String prefix, int maxKeys)

    void delete(String key)

    /** The Date header of the first response this instance saw, in epoch millis; null before one. */
    Long firstServerDateMillis()

    String describe()
}
