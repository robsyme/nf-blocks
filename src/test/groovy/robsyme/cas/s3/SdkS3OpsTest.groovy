package robsyme.cas.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse
import software.amazon.awssdk.services.s3.model.S3Error
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification

/**
 * The headers SdkS3Ops puts on the wire, captured after signing and before
 * transmission, so nothing reaches a network. S3's answers are tier two's.
 */
class SdkS3OpsTest extends Specification {

    final List<SdkHttpRequest> sent = []

    private SdkS3Ops ops(S3WriteOptions options = S3WriteOptions.NONE) {
        final ExecutionInterceptor capture = new ExecutionInterceptor() {
            @Override
            void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
                sent << context.httpRequest()
                // SdkClientException itself, not a plain RuntimeException: the
                // pipeline's failure path (ThrowableUtils.failure) only leaves
                // a RuntimeException unwrapped when it already is one, so a
                // custom RuntimeException would surface as itself, not as the
                // SdkClientException every feature below asserts on.
                throw SdkClientException.create('stopped before transmission, by design')
            }
        }
        final S3Client client = S3Client.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create('http://127.0.0.1:9'))
            .forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('a', 'b')))
            .overrideConfiguration { it.addExecutionInterceptor(capture) }
            .build()
        return new SdkS3Ops(client, 'member', options)
    }

    private String header(String name) {
        sent.last().firstMatchingHeader(name).orElse(null)
    }

    def 'a block PUT is conditional, asks for SHA-256, and carries the aws scope fields'() {
        given:
        final SdkS3Ops s3 = ops(new S3WriteOptions('STANDARD_IA', 'aws:kms', 'key-1', true))

        when:
        s3.put('cas/blocks/am/x', S3Body.ofBytes('x'.bytes),
            S3PutOptions.create().ifNoneMatch().sha256().cacheControl('public, max-age=31536000, immutable'))

        then:
        thrown(SdkClientException)
        sent.last().method().name() == 'PUT'
        sent.last().encodedPath() == '/member/cas/blocks/am/x'
        header('If-None-Match') == '*'
        header('x-amz-sdk-checksum-algorithm') == 'SHA256'
        header('x-amz-storage-class') == 'STANDARD_IA'
        header('x-amz-server-side-encryption') == 'aws:kms'
        header('x-amz-server-side-encryption-aws-kms-key-id') == 'key-1'
        header('x-amz-request-payer') == 'requester'
        header('Cache-Control') == 'public, max-age=31536000, immutable'
    }

    def 'a PUT that names its own storage class overrides the aws scope class'() {
        given:
        final SdkS3Ops s3 = ops(new S3WriteOptions('STANDARD_IA', null, null, false))

        when:
        s3.put('cas/sweep.lock', S3Body.ofBytes('x'.bytes), S3PutOptions.create().ifNoneMatch().contentType('application/json').storageClass('STANDARD'))

        then:
        thrown(SdkClientException)
        header('x-amz-storage-class') == 'STANDARD'
    }

    def 'a snapshot PUT is conditional on the ETag it replaces and records the run count'() {
        when:
        ops().put('cas/index/v3.sqlite', S3Body.ofBytes('s'.bytes),
            S3PutOptions.create().ifMatch('"e1"').cacheControl('no-cache').meta('runs', '7'))

        then:
        thrown(SdkClientException)
        header('If-Match') == '"e1"'
        header('If-None-Match') == null
        header('x-amz-meta-runs') == '7'
        header('Cache-Control') == 'no-cache'
    }

    def 'a copy into the member names its source, asks for SHA-256, is conditional and replaces the metadata'() {
        when:
        ops().copy('work', 'w/ab/c/A.bam', 'cas/tmp/u', S3PutOptions.create().sha256().ifNoneMatch().cacheControl('no-cache'))

        then:
        thrown(SdkClientException)
        header('x-amz-copy-source') == 'work/w/ab/c/A.bam'
        header('x-amz-checksum-algorithm') == 'SHA256'
        header('If-None-Match') == '*'
        header('x-amz-metadata-directive') == 'REPLACE'
    }

    def 'copyOut names its source, the target bucket and key, and always asks for SHA-256'() {
        when:
        ops().copyOut('cas/blocks/am/x', 'work', 'stage/A.bam')

        then:
        thrown(SdkClientException)
        header('x-amz-copy-source') == 'member/cas/blocks/am/x'
        sent.last().encodedPath() == '/work/stage/A.bam'
        header('x-amz-checksum-algorithm') == 'SHA256'
        header('If-None-Match') == null
    }

    def 'HEAD asks for the stored SHA-256 (x-amz-checksum-mode)'() {
        when:
        ops().head('cas/index.html')

        then:
        thrown(SdkClientException)
        sent.last().method().name() == 'HEAD'
        header('x-amz-checksum-mode') == 'ENABLED'
    }

    def 'S3 status codes map to written, exists and conflict'() {
        expect:
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(412).build()) == S3Written.Status.EXISTS
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(409).build()) == S3Written.Status.CONFLICT
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(403).build()) == null
    }

    def 'a conditional PUT onto a key deleted since its HEAD (404) is a failed precondition, not an error (Task 7 carry)'() {
        expect:
        SdkS3Ops.putStatusOf(S3Exception.builder().statusCode(404).build(), S3PutOptions.create().ifMatch('"e1"')) == S3Written.Status.EXISTS
        SdkS3Ops.putStatusOf(S3Exception.builder().statusCode(404).build(), S3PutOptions.create()) == null
        SdkS3Ops.putStatusOf(S3Exception.builder().statusCode(412).build(), S3PutOptions.create()) == S3Written.Status.EXISTS
    }

    def 'the Date of the first response is kept and parsed'() {
        expect:
        SdkS3Ops.parseDate('Sun, 27 Sep 2026 10:00:00 GMT') == 1790503200000L
        SdkS3Ops.parseDate('garbage') == null
    }

    def 'deleteMany sends one quiet DeleteObjects and returns the keys that failed'() {
        given:
        final S3Client client = Mock()
        final ops = new SdkS3Ops(client, 'b', S3WriteOptions.NONE)

        when:
        final List<String> failed = ops.deleteMany(['k1', 'k2'])

        then:
        1 * client.deleteObjects({ DeleteObjectsRequest r ->
            r.bucket() == 'b' && r.delete().quiet() && r.delete().objects()*.key() == ['k1', 'k2'] }) >>
            DeleteObjectsResponse.builder().errors(S3Error.builder().key('k2').code('AccessDenied').build()).build()
        failed == ['k2']
    }
}
