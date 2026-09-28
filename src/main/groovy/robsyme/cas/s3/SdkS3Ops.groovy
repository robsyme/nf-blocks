package robsyme.cas.s3

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicReference

import groovy.transform.CompileStatic
import software.amazon.awssdk.core.SdkResponse
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.ContentStreamProvider
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

/**
 * S3Ops over the SDK client nf-amazon builds (S3Access). Adds the aws scope's
 * per-request fields to every write, asks HeadObject for the stored SHA-256,
 * and keeps the Date of the first response for the clock check (ticket 03 Q2).
 */
@CompileStatic
class SdkS3Ops implements S3Ops {

    final String bucket
    final S3WriteOptions options
    private final S3Client client
    private final AtomicReference<Long> firstDate = new AtomicReference<>()

    SdkS3Ops(S3Client client, String bucket, S3WriteOptions options) {
        this.client = client
        this.bucket = bucket
        this.options = options ?: S3WriteOptions.NONE
    }

    /** A 412 or a 409 as a write outcome; null for anything else. */
    static S3Written.Status statusOf(S3Exception e) {
        if( e.statusCode() == 412 ) return S3Written.Status.EXISTS
        if( e.statusCode() == 409 ) return S3Written.Status.CONFLICT
        return null
    }

    static Long parseDate(String text) {
        if( !text ) return null
        try {
            return ZonedDateTime.parse(text, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            return null
        }
    }

    private void note(SdkHttpResponse http) {
        if( http == null || firstDate.get() != null ) return
        final Long date = parseDate(http.firstMatchingHeader('Date').orElse(null))
        if( date != null ) firstDate.compareAndSet(null, date)
    }

    private void note(SdkResponse r) { note(r?.sdkHttpResponse()) }

    private void note(S3Exception e) { note(e.awsErrorDetails()?.sdkHttpResponse()) }

    private RequestPayer payer() { options.requesterPays ? RequestPayer.REQUESTER : null }

    private static RequestBody bodyOf(S3Body body, String contentType) {
        return RequestBody.fromContentProvider({ -> body.open() } as ContentStreamProvider, body.length,
            contentType ?: 'application/octet-stream')
    }

    @Override
    S3Head head(String key) {
        try {
            final HeadObjectResponse r = client.headObject(HeadObjectRequest.builder()
                .bucket(bucket).key(key).checksumMode(ChecksumMode.ENABLED).requestPayer(payer()).build())
            note(r)
            return new S3Head(r.contentLength(), r.eTag(), r.checksumSHA256(),
                r.metadata() ?: Collections.<String, String> emptyMap(), r.lastModified()?.toEpochMilli() ?: 0L)
        }
        catch( NoSuchKeyException e ) {
            note(e)
            return null
        }
        catch( S3Exception e ) {
            note(e)
            if( e.statusCode() == 404 ) return null
            throw e
        }
    }

    @Override
    InputStream get(String key, String ifMatch, long start, long length) {
        final GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(bucket).key(key).requestPayer(payer())
        if( ifMatch != null ) b.ifMatch(ifMatch)
        if( length >= 0 ) b.range("bytes=${start}-${start + length - 1}".toString())
        else if( start > 0 ) b.range("bytes=${start}-".toString())
        try {
            final def stream = client.getObject(b.build())
            note(stream.response())
            return stream
        }
        catch( NoSuchKeyException e ) {
            note(e)
            return null
        }
        catch( S3Exception e ) {
            note(e)
            if( e.statusCode() == 404 ) return null
            if( e.statusCode() == 412 ) throw new S3PreconditionFailed("s3://${bucket}/${key} is no longer ${ifMatch}")
            throw e
        }
    }

    @Override
    S3Written put(String key, S3Body body, S3PutOptions o) {
        final PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer()).cacheControl(o.cacheControl).contentType(o.contentType)
        if( o.metadata ) b.metadata(o.metadata)
        if( o.ifNoneMatch ) b.ifNoneMatch('*')
        if( o.ifMatch != null ) b.ifMatch(o.ifMatch)
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        try {
            final PutObjectResponse r = client.putObject(b.build(), bodyOf(body, o.contentType))
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.eTag(), r.checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    String createMultipart(String key, S3PutOptions o) {
        final CreateMultipartUploadRequest.Builder b = CreateMultipartUploadRequest.builder().bucket(bucket).key(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer()).cacheControl(o.cacheControl).contentType(o.contentType)
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        final CreateMultipartUploadResponse r = client.createMultipartUpload(b.build())
        note(r)
        return r.uploadId()
    }

    @Override
    S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body) {
        final UploadPartResponse r = client.uploadPart(UploadPartRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).partNumber(partNumber).contentLength(body.length)
            .checksumAlgorithm(ChecksumAlgorithm.SHA256).requestPayer(payer()).build(), bodyOf(body, null))
        note(r)
        return new S3Part(partNumber, r.eTag(), r.checksumSHA256())
    }

    @Override
    S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last) {
        final UploadPartCopyResponse r = client.uploadPartCopy(UploadPartCopyRequest.builder()
            .sourceBucket(sourceBucket).sourceKey(sourceKey).destinationBucket(bucket).destinationKey(key)
            .uploadId(uploadId).partNumber(partNumber).copySourceRange("bytes=${first}-${last}".toString())
            .requestPayer(payer()).build())
        note(r)
        return new S3Part(partNumber, r.copyPartResult().eTag(), r.copyPartResult().checksumSHA256())
    }

    @Override
    S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch) {
        final List<CompletedPart> completed = parts.collect { S3Part p ->
            CompletedPart.builder().partNumber(p.partNumber).eTag(p.etag).checksumSHA256(p.sha256).build()
        }
        final CompleteMultipartUploadRequest.Builder b = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).requestPayer(payer())
            .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
        if( ifNoneMatch ) b.ifNoneMatch('*')
        try {
            final CompleteMultipartUploadResponse r = client.completeMultipartUpload(b.build())
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.eTag(), r.checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    void abortMultipart(String key, String uploadId) {
        client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).requestPayer(payer()).build())
    }

    @Override
    S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions o) {
        final CopyObjectRequest.Builder b = CopyObjectRequest.builder()
            .sourceBucket(sourceBucket).sourceKey(sourceKey).destinationBucket(bucket).destinationKey(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer())
        if( o.cacheControl != null || o.contentType != null )
            b.metadataDirective(MetadataDirective.REPLACE).cacheControl(o.cacheControl)
                .contentType(o.contentType ?: 'application/octet-stream')
        if( o.ifNoneMatch ) b.ifNoneMatch('*')
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        try {
            final CopyObjectResponse r = client.copyObject(b.build())
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.copyObjectResult().eTag(), r.copyObjectResult().checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    List<S3Listed> list(String prefix, int maxKeys) {
        final ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).requestPayer(payer())
        final List<S3Listed> out = new ArrayList<S3Listed>()
        if( maxKeys > 0 ) {
            final ListObjectsV2Response r = client.listObjectsV2(b.maxKeys(maxKeys).build())
            note(r)
            for( S3Object o : r.contents() ) out.add(new S3Listed(o.key(), o.size()))
            return out
        }
        for( ListObjectsV2Response page : client.listObjectsV2Paginator(b.build()) ) {
            note(page)
            for( S3Object o : page.contents() ) out.add(new S3Listed(o.key(), o.size()))
        }
        return out
    }

    @Override
    void delete(String key) {
        note(client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).requestPayer(payer()).build()))
    }

    @Override
    Long firstServerDateMillis() { firstDate.get() }

    @Override
    String describe() { "s3://${bucket}" }
}
