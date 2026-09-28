package robsyme.cas.s3

import groovy.transform.CompileStatic
import nextflow.cloud.aws.AwsClientFactory
import nextflow.cloud.aws.config.AwsConfig
import nextflow.cloud.aws.nio.util.S3SyncClientConfiguration
import software.amazon.awssdk.services.s3.S3Client

/**
 * The S3 client for one bucket, built the way nf-amazon builds its own
 * (S3FileSystemProvider.createFileSystem, S3FileSystemProvider.java:756-780 at
 * v26.04.6), from the config map a run or a verb loaded: the aws scope's
 * credentials, profile (SSO included), region, endpoint and HTTP settings.
 * No session is needed, and the S3Client wrapper's listBuckets probe is
 * skipped. With no region anywhere, resolveS3Region() answers us-east-1 and
 * the client is cross-region.
 */
@CompileStatic
class S3Access {

    private S3Access() {}

    static S3Ops open(Map config, String bucket) {
        final AwsConfig aws = awsConfig(config)
        final Properties props = new Properties()
        for( Map.Entry e : (aws.getS3LegacyProperties() as Map).entrySet() )
            if( e.value != null )
                props.setProperty(e.key.toString(), e.value.toString())
        // As S3FileSystemProvider.java:763-766: `global` would override a custom endpoint.
        final boolean global = !aws.s3Config.isCustomEndpoint()
        final AwsClientFactory factory = new AwsClientFactory(aws, aws.resolveS3Region())
        final S3Client client = factory.getS3Client(S3SyncClientConfiguration.create(props), global)
        return new SdkS3Ops(client, bucket, writeOptionsOf(aws))
    }

    static S3WriteOptions writeOptions(Map config) {
        return writeOptionsOf(awsConfig(config))
    }

    // Named differently from writeOptions(Map): Groovy forbids a private and a
    // public method sharing a name (multimethod dispatch would be ambiguous).
    private static S3WriteOptions writeOptionsOf(AwsConfig aws) {
        final def s3 = aws.s3Config
        if( !s3.storageClass && !s3.storageEncryption && !s3.storageKmsKeyId && !s3.requesterPays )
            return S3WriteOptions.NONE
        return new S3WriteOptions(s3.storageClass, s3.storageEncryption, s3.storageKmsKeyId, s3.requesterPays ?: false)
    }

    private static AwsConfig awsConfig(Map config) {
        final Object scope = config?.get('aws')
        return new AwsConfig(scope instanceof Map ? (Map) scope : Collections.emptyMap())
    }
}
