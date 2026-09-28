package robsyme.cas.s3

import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.CoordinateTreeContract
import robsyme.cas.core.StoreRef

class S3CoordinateTreeTest extends CoordinateTreeContract {

    MemoryS3Ops s3 = new MemoryS3Ops('member')

    @Override CoordinateTree tree() { new S3CoordinateTree(s3, 'cas/') }

    def 'pointers are cas/coords/<rel> with the local body and no directory markers'() {
        when:
        tree().write('aligned/A/A.bam', FILE)

        then:
        s3.objects.keySet() == ['cas/coords/aligned/A/A.bam'] as Set
        s3.objects['cas/coords/aligned/A/A.bam'].text() == FILE.toString() + '\n'
    }

    def 'a pointer shadowed by an ancestor pointer (written past the check) reads as absent and is reported (ticket 03 decision 4)'() {
        given:
        s3.putText('cas/coords/a', FILE.toString() + '\n')
        s3.putText('cas/coords/a/b', FILE.toString() + '\n')
        s3.putText('cas/coords/a/c/d', FILE.toString() + '\n')
        final S3CoordinateTree t = new S3CoordinateTree(s3, 'cas/')

        expect:
        t.read('a/b') == Optional.empty()
        t.read('a') == Optional.of(FILE)
        t.shadowedPointers(20) == ['a/b', 'a/c/d']
        t.shadowedPointers(1) == ['a/b']
    }
}
