package robsyme.cas.s3

import spock.lang.Specification

class S3StoreIdStorageTest extends Specification {

    MemoryS3Ops s3 = new MemoryS3Ops('member')
    S3StoreIdStorage storage = new S3StoreIdStorage(s3, 'cas/')

    def 'absent reads null; create puts the id with a newline under the member prefix'() {
        expect:
        storage.read() == null
        storage.create('a' * 32) == 'a' * 32
        s3.objects['cas/store-id'].text() == 'a' * 32 + '\n'
        storage.read() == 'a' * 32
    }

    def 'S3: a lost If-None-Match race returns the winner\'s id'() {
        given:
        storage.create('a' * 32)

        expect:
        storage.create('b' * 32) == 'a' * 32
        s3.objects['cas/store-id'].text() == 'a' * 32 + '\n'
    }
}
