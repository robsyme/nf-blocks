package robsyme.cas.s3

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification

/** Which S3 failures are about the user's AWS credentials, told in words that hold for any setup. */
class S3FailuresTest extends Specification {

    static S3Exception status(int code, String message = null) {
        return (S3Exception) S3Exception.builder().statusCode(code).message(message).build()
    }

    static S3Exception errorCode(int status, String code) {
        return (S3Exception) S3Exception.builder().statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build()).build()
    }

    def 'credentials: #why'() {
        expect:
        S3Failures.credentials(failure) == said

        where:
        why                                  | failure                                                                                              | said
        'a 403'                              | status(403, 'Forbidden (Service: S3, Status Code: 403, Request ID: X)')                              | S3Failures.REFUSED
        'a 401'                              | status(401)                                                                                          | S3Failures.REFUSED
        'an expired session token code'      | errorCode(400, 'ExpiredToken')                                                                       | S3Failures.REFUSED
        'an unknown access key'              | errorCode(403, 'InvalidAccessKeyId')                                                                 | S3Failures.REFUSED
        'an SSO login that expired'          | SdkClientException.create('Token has expired and refresh failed')                                    | S3Failures.MISSING
        'no credentials anywhere'            | SdkClientException.create('Unable to load credentials from any of the providers in the chain')      | S3Failures.MISSING
        'a 403 wrapped by the caller'        | new RuntimeException('copy failed', status(403))                                                     | S3Failures.REFUSED
        'a network failure'                  | SdkClientException.create('Unable to execute HTTP request: Connect to s3.amazonaws.com:443 failed') | null
        'a missing object'                   | status(404)                                                                                          | null
        'a server error'                     | status(500, 'We encountered an internal error')                                                      | null
        'nothing to do with AWS'             | new IOException('disk full')                                                                         | null
    }

    def 'the words name no profile, command or store of anyone in particular'() {
        expect:
        [S3Failures.REFUSED, S3Failures.MISSING].every { String s -> !s.contains('--profile') && !s.contains('sso login') }
    }

    def 'a cause is told without request ids'() {
        expect:
        S3Failures.cause(status(500, 'We encountered an internal error (Service: S3, Status Code: 500, Request ID: ABC, Extended Request ID: xyz=)')) ==
            'We encountered an internal error'
        S3Failures.cause(SdkClientException.create('Unable to execute HTTP request: Connect timed out')) ==
            'Unable to execute HTTP request: Connect timed out'
        S3Failures.cause(new IllegalStateException()) == 'IllegalStateException'
    }
}
