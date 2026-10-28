package robsyme.cas.s3

import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification

/** A client built before there were AWS credentials is opened again once there are (an explorer started before signing in). */
class ReopeningS3OpsTest extends Specification {

    /** The client nf-amazon builds with no credentials: S3 refuses every request it signs with none. */
    static class Unsigned extends MemoryS3Ops {
        int status = 403
        Unsigned() { super('member-bucket') }
        @Override S3Head head(String key) {
            calls << "HEAD ${key}".toString()
            throw S3FailuresTest.status(status, 'Forbidden (Service: S3, Status Code: 403, Request ID: X)')
        }
    }

    def 'a call refused for credentials is asked again on a client opened with them, and every later call goes to it'() {
        given:
        final Unsigned unsigned = new Unsigned()
        final MemoryS3Ops signed = new MemoryS3Ops('member-bucket')
        signed.objects['store-id'] = new MemoryS3Ops.Obj(bytes: 'id'.bytes, etag: '"e"')
        int opened = 0
        final S3Ops ops = new ReopeningS3Ops(unsigned, { -> opened++; signed })

        expect:
        ops.head('store-id').size == 2
        ops.head('store-id').size == 2
        opened == 1
        unsigned.calls == ['HEAD store-id']
    }

    def 'while there are still no credentials, the refusal stands and the next refused call tries again'() {
        given:
        int opened = 0
        final S3Ops ops = new ReopeningS3Ops(new Unsigned(), { -> opened++; null })

        when:
        ops.head('store-id')

        then:
        final S3Exception refused = thrown()
        refused.statusCode() == 403
        opened == 1

        when:
        ops.head('store-id')

        then:
        thrown(S3Exception)
        opened == 2
    }

    def 'a failure that is not about credentials does not open the client again'() {
        given:
        final Unsigned unsigned = new Unsigned(status: 500)
        int opened = 0
        final S3Ops ops = new ReopeningS3Ops(unsigned, { -> opened++; new MemoryS3Ops('member-bucket') })

        when:
        ops.head('store-id')

        then:
        thrown(S3Exception)
        opened == 0
    }

    def 'the first server date seen before the client was opened again is still the first'() {
        given:
        final Unsigned unsigned = new Unsigned(serverDateMillis: 1_000L)
        final MemoryS3Ops signed = new MemoryS3Ops('member-bucket')
        signed.serverDateMillis = 2_000L
        final S3Ops ops = new ReopeningS3Ops(unsigned, { -> signed })

        when:
        ops.list('', 0)
        ops.head('store-id')

        then:
        ops.firstServerDateMillis() == 1_000L
        ops.lastServerDateMillis() == 2_000L
    }
}
