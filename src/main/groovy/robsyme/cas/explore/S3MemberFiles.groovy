package robsyme.cas.explore

import groovy.transform.CompileStatic
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object

/**
 * A member in a private bucket, read with the user's own credentials so the
 * page never holds any (block explorer spec section 1.4). Ranged GetObject:
 * nf-amazon's newByteChannel downloads the whole object, which a 580 MB
 * snapshot cannot afford (v26.04.6, S3FileSystemProvider.newByteChannel).
 */
@CompileStatic
class S3MemberFiles implements MemberFiles {

    private final S3Client client
    private final String bucket
    private final String prefix

    S3MemberFiles(S3Client client, String bucket, String prefix) {
        this.client = client
        this.bucket = bucket
        this.prefix = prefix
    }

    static S3MemberFiles open(URI location) {
        final List<String> parts = bucketAndPrefix(location)
        return new S3MemberFiles(defaultClient(), parts[0], parts[1])
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
     * The default credential chain (AWS_PROFILE, SSO, environment, instance
     * role), any region, cross-region access on so the bucket's region need not
     * be configured.
     */
    static S3Client defaultClient() {
        return withPluginLoader {
            S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(defaultRegion())
                .crossRegionAccessEnabled(true)
                .build()
        }
    }

    private static Region defaultRegion() {
        try {
            return new DefaultAwsRegionProviderChain().region
        }
        catch( SdkClientException e ) {
            return Region.US_EAST_1
        }
    }

    /**
     * The SDK finds its HTTP client and the SSO credential classes through the
     * thread's context class loader, which inside a Nextflow plugin is not the
     * plugin's. Every SDK call runs with the plugin's loader in place.
     */
    private static <T> T withPluginLoader(Closure<T> body) {
        final Thread thread = Thread.currentThread()
        final ClassLoader previous = thread.contextClassLoader
        thread.contextClassLoader = S3MemberFiles.classLoader
        try {
            return body.call()
        }
        finally {
            thread.contextClassLoader = previous
        }
    }

    @Override
    Long size(String rel) {
        return withPluginLoader {
            try {
                return client.headObject(HeadObjectRequest.builder().bucket(bucket).key(prefix + rel).build()).contentLength()
            }
            catch( NoSuchKeyException e ) {
                return (Long) null
            }
            catch( S3Exception e ) {
                if( e.statusCode() == 404 )
                    return (Long) null
                throw e
            }
        }
    }

    @Override
    InputStream open(String rel, long start, long length) {
        return withPluginLoader {
            (InputStream) client.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(prefix + rel)
                .range("bytes=${start}-${start + length - 1}".toString())
                .build())
        }
    }

    @Override
    List<String> list(String dirRel) {
        final String under = prefix + dirRel.replaceAll('/+$', '') + '/'
        return withPluginLoader {
            final List<String> names = []
            for( S3Object object : client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(under).build()).contents() ) {
                final String name = object.key().substring(under.length())
                if( name && !name.contains('/') )
                    names.add(name)
            }
            return names
        }
    }

    @Override
    String describe() { "s3://${bucket}/${prefix}" }
}
