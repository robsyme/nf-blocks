package robsyme.cas.s3

import spock.lang.Specification

/** The aws scope reaches nf-blocks' S3 requests through nf-amazon's own parsing (ticket 02 decision 9). */
class S3AccessTest extends Specification {

    def 'the aws scope gives the per-request fields; building the client touches no network'() {
        given:
        final Map config = [aws: [accessKey: 'a', secretKey: 'b', region: 'eu-west-1',
                                  client: [storageClass: 'ONEZONE_IA', storageEncryption: 'AES256', requesterPays: true]]]

        when:
        final S3Ops ops = S3Access.open(config, 'member-bucket')

        then:
        ops instanceof SdkS3Ops
        ops.bucket == 'member-bucket'
        ((SdkS3Ops) ops).options == new S3WriteOptions('ONEZONE_IA', 'AES256', null, true)
    }

    def 'no aws scope: no per-request fields'() {
        expect:
        S3Access.writeOptions([:]) == S3WriteOptions.NONE
    }
}
