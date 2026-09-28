package robsyme.cas.s3

import spock.lang.Specification

class S3StoreLogStorageTest extends Specification {

    MemoryS3Ops s3 = new MemoryS3Ops('member')

    def 'an entry is a conditional empty PUT under <prefix>log/, and a second one is answered 412 as success'() {
        given:
        final S3StoreLogStorage log = new S3StoreLogStorage(s3, 'cas/')

        when:
        log.putEntry('0000000001000-run-x')
        log.putEntry('0000000001000-run-x')

        then:
        noExceptionThrown()
        s3.calls.count { it == 'PUT cas/log/0000000001000-run-x' } == 2
        s3.objects['cas/log/0000000001000-run-x'].bytes.length == 0
        s3.objects.size() == 1
    }

    def 'a bucket-root member writes log/ with no leading slash'() {
        when:
        new S3StoreLogStorage(s3, '').putEntry('e')
        new S3StoreLogStorage(s3, null).putEntry('f')

        then:
        s3.objects.keySet() == ['log/e', 'log/f'] as Set
    }

    def 'a 409 is tried up to ATTEMPTS times in all, then thrown'() {
        given:
        final S3StoreLogStorage log = new S3StoreLogStorage(s3, 'cas/')

        when: 'two 409s, then the third try writes'
        s3.conflicts['PUT'] = 2
        log.putEntry('a')

        then:
        s3.objects.containsKey('cas/log/a')

        when: 'three 409s use up the tries'
        s3.conflicts['PUT'] = S3BlockStore.ATTEMPTS
        log.putEntry('b')

        then:
        thrown(IOException)
        !s3.objects.containsKey('cas/log/b')
    }

    def 'listEntries names what is directly under <prefix>log/, without the prefix'() {
        given:
        s3.putText('cas/log/one', '')
        s3.putText('cas/log/two', '')
        s3.putText('cas/log/nested/three', '')
        s3.putText('cas/logbook', '')
        s3.putText('cas/blocks/xx/four', '')

        expect:
        new S3StoreLogStorage(s3, 'cas/').listEntries() as Set == ['one', 'two'] as Set
    }
}
