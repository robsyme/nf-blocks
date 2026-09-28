package robsyme.cas.s3

import java.security.MessageDigest

import spock.lang.Specification

/** The test double behaves as the measured S3 does (ticket 14), or later tests prove nothing. */
class MemoryS3OpsTest extends Specification {

    MemoryS3Ops s3 = new MemoryS3Ops('member-bucket')

    private static String sha256b64(byte[] bytes) {
        Base64.encoder.encodeToString(MessageDigest.getInstance('SHA-256').digest(bytes))
    }

    def 'a conditional PUT onto an existing key reads the whole body, then answers EXISTS (ticket 14 item 1)'() {
        given:
        s3.put('k', S3Body.ofBytes('one'.bytes), S3PutOptions.create().ifNoneMatch())
        final long before = s3.pulledBytes

        when:
        final S3Written second = s3.put('k', S3Body.ofBytes('two!'.bytes), S3PutOptions.create().ifNoneMatch())

        then:
        second.status == S3Written.Status.EXISTS
        s3.pulledBytes - before == 4
        s3.objects['k'].text() == 'one'
    }

    def 'If-Match replaces only the version it names'() {
        given:
        final String etag = s3.put('k', S3Body.ofBytes('v1'.bytes), S3PutOptions.create()).etag

        expect:
        s3.put('k', S3Body.ofBytes('v2'.bytes), S3PutOptions.create().ifMatch('"stale"')).status == S3Written.Status.EXISTS
        s3.put('k', S3Body.ofBytes('v2'.bytes), S3PutOptions.create().ifMatch(etag)).status == S3Written.Status.WRITTEN
        s3.objects['k'].text() == 'v2'
    }

    def 'a PUT with sha256 stores and returns the full-object SHA-256 (ticket 14 item 4)'() {
        when:
        final S3Written w = s3.put('k', S3Body.ofBytes('hello\n'.bytes), S3PutOptions.create().sha256())

        then:
        w.sha256 == sha256b64('hello\n'.bytes)
        s3.head('k').sha256 == w.sha256
    }

    def 'a copy with sha256 returns the SHA-256 of a multipart source (ticket 14 item 2)'() {
        given:
        final MemoryS3Ops work = new MemoryS3Ops('work-bucket')
        s3.peers['work-bucket'] = work
        final String id = work.createMultipart('src', S3PutOptions.create())
        final S3Part p1 = work.uploadPart('src', id, 1, S3Body.ofBytes('abc'.bytes))
        final S3Part p2 = work.uploadPart('src', id, 2, S3Body.ofBytes('def'.bytes))
        work.completeMultipart('src', id, [p1, p2], true)

        when:
        final S3Written copied = s3.copy('work-bucket', 'src', 'dst', S3PutOptions.create().sha256().ifNoneMatch())

        then:
        work.objects['src'].text() == 'abcdef'
        copied.status == S3Written.Status.WRITTEN
        copied.sha256 == sha256b64('abcdef'.bytes)
        s3.copy('work-bucket', 'src', 'dst', S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.EXISTS
    }

    def 'an injected 409 is answered first, then the write succeeds'() {
        given:
        s3.conflicts['PUT'] = 1

        expect:
        s3.put('k', S3Body.ofBytes('x'.bytes), S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.CONFLICT
        s3.put('k', S3Body.ofBytes('x'.bytes), S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.WRITTEN
    }

    def 'list is lexicographic under a prefix and honours maxKeys; a ranged get with a stale If-Match fails'() {
        given:
        ['log/2', 'log/1', 'logx', 'log/3'].each { s3.putText(it, '') }
        s3.putText('blob', '0123456789')

        expect:
        s3.list('log/', 0)*.key == ['log/1', 'log/2', 'log/3']
        s3.list('log/', 2)*.key == ['log/1', 'log/2']
        s3.get('blob', s3.head('blob').etag, 2, 3).text == '234'
        s3.get('absent', null, 0, -1) == null

        when:
        s3.get('blob', '"other"', 0, -1)

        then:
        thrown(S3PreconditionFailed)
    }
}
