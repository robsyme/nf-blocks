package robsyme.cas.explore

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import robsyme.cas.s3.S3Failures
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification

/** Whether a copy destination can be reached now, so the page can grey it out with a reason instead of failing the plan. */
class MemberProbeTest extends Specification {

    static class Fake implements MemberFiles {
        final AtomicInteger asked = new AtomicInteger()
        Closure<MemberFiles.Opened> answer = { -> null }
        @Override MemberFiles.Opened open(String rel) { asked.incrementAndGet(); assert rel == 'store-id'; answer.call() }
        @Override List<String> list(String dirRel) { [] }
        @Override String describe() { 's3://bucket/cas/' }
    }

    long now = 1_000_000L

    private MemberProbe probe(Map<String, MemberFiles> members, long timeoutMillis = 2_000) {
        return new MemberProbe(members, timeoutMillis, { -> now })
    }

    def 'a member that answers is reachable, whether or not it keeps a store-id'() {
        given:
        final Fake cloud = new Fake()

        expect:
        probe([cloud: cloud]).unreachable(['cloud']) == [:]
        cloud.asked.get() == 1
    }

    def 'refused credentials grey the member out with the general remedy'() {
        given:
        final Fake cloud = new Fake(answer: { -> throw S3Exception.builder().statusCode(403).message('Forbidden').build() })

        expect:
        probe([cloud: cloud]).unreachable(['cloud']) == [cloud: "${S3Failures.REFUSED}. Refresh your AWS credentials, then reload this page".toString()]
    }

    def 'missing or expired credentials say so'() {
        given:
        final Fake cloud = new Fake(answer: { -> throw SdkClientException.create('Token has expired and refresh failed') })

        expect:
        probe([cloud: cloud]).unreachable(['cloud']).cloud == "${S3Failures.MISSING}. Refresh your AWS credentials, then reload this page".toString()
    }

    def 'any other failure names its cause, without request ids'() {
        given:
        final Fake cloud = new Fake(answer: { ->
            throw S3Exception.builder().statusCode(500).message('We encountered an internal error (Service: S3, Status Code: 500, Request ID: ABC)').build() })

        expect:
        probe([cloud: cloud]).unreachable(['cloud']).cloud == "couldn't reach this store (We encountered an internal error). Reload this page to try again"
    }

    def 'a member that does not answer in time is unreachable, and the page is not held up'() {
        given:
        CountDownLatch release = new CountDownLatch(1)
        final Fake cloud = new Fake(answer: { -> release.await(10, TimeUnit.SECONDS); null })

        when:
        final long started = System.nanoTime()
        final Map<String, String> said = probe([cloud: cloud], 200).unreachable(['cloud'])
        final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

        then:
        said.cloud == "couldn't reach this store (it did not answer in time). Reload this page to try again"
        tookMillis < 5_000

        cleanup:
        release.countDown()
    }

    def 'members are asked at the same time, so two slow ones cost one wait'() {
        given:
        final CountDownLatch both = new CountDownLatch(2)
        final Closure<MemberFiles.Opened> meet = { -> both.countDown(); both.await(5, TimeUnit.SECONDS) ? null : null }
        final Fake a = new Fake(answer: meet)
        final Fake b = new Fake(answer: meet)

        expect: 'each waits for the other, which only works when they run together'
        probe([a: a, b: b], 3_000).unreachable(['a', 'b']) == [:]
        both.count == 0
    }

    def 'an answer is kept for a while, then asked again'() {
        given:
        final Fake cloud = new Fake(answer: { -> throw S3Exception.builder().statusCode(403).build() })
        final MemberProbe p = probe([cloud: cloud])

        when:
        p.unreachable(['cloud'])
        now += MemberProbe.KEEP_MILLIS - 1
        p.unreachable(['cloud'])

        then:
        cloud.asked.get() == 1

        when: 'the login was refreshed and the answer is stale'
        cloud.answer = { -> null }
        now += 2
        final Map<String, String> after = p.unreachable(['cloud'])

        then:
        cloud.asked.get() == 2
        after == [:]
    }

    def 'only the named members are asked'() {
        given:
        final Fake cloud = new Fake()
        final Fake other = new Fake()

        when:
        probe([cloud: cloud, other: other]).unreachable(['cloud'])

        then:
        cloud.asked.get() == 1
        other.asked.get() == 0
    }
}
