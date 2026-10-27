package robsyme.cas.s3

import java.util.regex.Pattern

import groovy.transform.CompileStatic
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.SdkClientException

/**
 * Which S3 failures are about the user's AWS credentials, in words that hold
 * for any setup: people sign in with SSO, access keys or an instance role, so
 * the words name no profile and no command. Callers add the remedy that fits
 * where the words are shown.
 */
@CompileStatic
class S3Failures {

    static final String REFUSED = 'AWS refused the credentials for this store: they may have expired, or may not grant access to it'
    static final String MISSING = 'no usable AWS credentials were found for this store: they are missing or have expired'

    /** Error codes S3 or STS answer for credentials that are wrong, expired or not allowed. */
    private static final Set<String> CREDENTIAL_CODES = ['AccessDenied', 'ExpiredToken', 'InvalidAccessKeyId', 'InvalidToken',
                                                         'SignatureDoesNotMatch', 'TokenRefreshRequired'] as Set<String>
    /** What the SDK says when it has no credentials to sign with, or an SSO login has lapsed. */
    private static final Pattern NO_CREDENTIALS = ~/(?i)credential|token|sso session/
    /** The SDK's trailer on a service error: request ids, of no use to the reader. */
    private static final Pattern TRAILER = ~/\s*\(Service: .*$/

    private S3Failures() {}

    /** {@link #REFUSED} or {@link #MISSING} when {@code t}, or anything that caused it, is a credentials failure; else null. */
    static String credentials(Throwable t) {
        for( Throwable c = t; c != null; c = c.cause ) {
            if( c instanceof AwsServiceException ) {
                final AwsServiceException e = (AwsServiceException) c
                if( e.statusCode() in [401, 403] || e.awsErrorDetails()?.errorCode() in CREDENTIAL_CODES )
                    return REFUSED
            }
            else if( c instanceof SdkClientException && NO_CREDENTIALS.matcher(c.message ?: '').find() )
                return MISSING
            if( c.cause == c )
                break
        }
        return null
    }

    /** A failure's message without the SDK's request-id trailer; the class name when there is no message. */
    static String cause(Throwable t) {
        final String message = t.message ? TRAILER.matcher(t.message).replaceFirst('') : null
        return message ?: (t.getClass().simpleName ?: t.getClass().name)
    }
}
