package robsyme.cas.s3

import groovy.transform.CompileStatic

/**
 * The S3Ops for a client nf-amazon built when no AWS credentials could be
 * found: such a client signs nothing for as long as it lives
 * (AwsClientFactory.getS3CredentialsProvider at v26.04.6 falls back to
 * anonymous access), so an explorer started before the user signed in would
 * be refused until it was restarted. A call refused for credentials opens the
 * client again; when the new client has credentials the call is asked once
 * more on it, and every later call goes to it. S3 did nothing with the
 * refused request, so asking again is safe for a write too.
 */
@CompileStatic
class ReopeningS3Ops implements S3Ops {

    private final S3Ops unsigned
    /** A client opened again; null while there are still no credentials. */
    private final Closure<S3Ops> reopen
    private volatile S3Ops current

    ReopeningS3Ops(S3Ops unsigned, Closure<S3Ops> reopen) {
        this.unsigned = unsigned
        this.reopen = reopen
        this.current = unsigned
    }

    private <T> T call(Closure<T> request) {
        final S3Ops asked = current
        try {
            return request.call(asked)
        }
        catch( Exception e ) {
            if( asked != unsigned || S3Failures.credentials(e) == null )
                throw e
            final S3Ops signed = reopened()
            if( signed == null )
                throw e
            return request.call(signed)
        }
    }

    private synchronized S3Ops reopened() {
        if( current == unsigned ) {
            final S3Ops fresh = reopen.call()
            if( fresh != null )
                current = fresh
        }
        return current == unsigned ? null : current
    }

    @Override
    String getBucket() { unsigned.bucket }

    @Override
    S3Head head(String key) { call { S3Ops o -> o.head(key) } }

    @Override
    InputStream get(String key, String ifMatch, long start, long length) { call { S3Ops o -> o.get(key, ifMatch, start, length) } }

    @Override
    S3Written put(String key, S3Body body, S3PutOptions options) { call { S3Ops o -> o.put(key, body, options) } }

    @Override
    String createMultipart(String key, S3PutOptions options) { call { S3Ops o -> o.createMultipart(key, options) } }

    @Override
    S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body) {
        call { S3Ops o -> o.uploadPart(key, uploadId, partNumber, body) }
    }

    @Override
    S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last) {
        call { S3Ops o -> o.uploadPartCopy(key, uploadId, partNumber, sourceBucket, sourceKey, first, last) }
    }

    @Override
    S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch) {
        call { S3Ops o -> o.completeMultipart(key, uploadId, parts, ifNoneMatch) }
    }

    @Override
    void abortMultipart(String key, String uploadId) { call { S3Ops o -> o.abortMultipart(key, uploadId) } }

    @Override
    S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions options) {
        call { S3Ops o -> o.copy(sourceBucket, sourceKey, key, options) }
    }

    @Override
    S3Written copyOut(String key, String targetBucket, String targetKey) { call { S3Ops o -> o.copyOut(key, targetBucket, targetKey) } }

    @Override
    List<S3Listed> list(String prefix, int maxKeys) { call { S3Ops o -> o.list(prefix, maxKeys) } }

    @Override
    void delete(String key) { call { S3Ops o -> o.delete(key) } }

    /** The unsigned client's first date when it saw one: the clock check is about the first response. */
    @Override
    Long firstServerDateMillis() { unsigned.firstServerDateMillis() ?: current.firstServerDateMillis() }

    @Override
    Long lastServerDateMillis() { current.lastServerDateMillis() ?: unsigned.lastServerDateMillis() }

    @Override
    List<String> deleteMany(List<String> keys) { call { S3Ops o -> o.deleteMany(keys) } }

    @Override
    List<S3Upload> listUploads(String prefix) { call { S3Ops o -> o.listUploads(prefix) } }

    @Override
    String describe() { current.describe() }
}
