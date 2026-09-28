package robsyme.cas.s3

import java.nio.file.Path
import java.security.MessageDigest

import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.Cid
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 16 decisions 3-6 on the double. */
class S3CopyTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops member = new MemoryS3Ops('member')
    MemoryS3Ops work = new MemoryS3Ops('work')
    S3BlockStore store

    def setup() {
        member.peers['work'] = work
        store = new S3BlockStore(member, 'cas/', 'lab', true, tmp, 1L << 20)
        work.putText('w/ab/cd/A.bam', 'bam-A')
    }

    private static Cid cidOf(String text) { Cid.of(Cid.RAW, MessageDigest.getInstance('SHA-256').digest(text.bytes)) }

    def 'no digest in advance: copy to tmp/ with SHA-256, then to the final key, staging deleted; provider s3-copy'() {
        when:
        final S3Copied c = store.copyFrom('work', 'w/ab/cd/A.bam', 5L, null)

        then:
        c == new S3Copied(cidOf('bam-A'), 's3-copy')
        member.objects[store.key(c.cid)].text() == 'bam-A'
        member.objects.keySet().every { !it.startsWith('cas/tmp/') }
        member.calls.count { it.startsWith('COPY ') } == 2
        work.pulledBytes == 0
    }

    def 'a node digest: one copy straight to the final key, compared; s3-copy'() {
        expect:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('bam-A')) == new S3Copied(cidOf('bam-A'), 's3-copy')
        member.calls.count { it.startsWith('COPY ') } == 1
    }

    def 'a node digest for a block already there: a HEAD and nothing else; fusion-node'() {
        given:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, null)
        member.calls.clear()

        expect:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('bam-A')) == new S3Copied(cidOf('bam-A'), 'fusion-node')
        member.calls == ["HEAD ${store.key(cidOf('bam-A'))}".toString()]
    }

    def 'a node digest that disagrees with S3: the copy is deleted and the publish fails'() {
        when:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('something else'))

        then:
        thrown(BlockMismatchException)
        member.objects.isEmpty()
    }

    def 'above the limit: UploadPartCopy with a node digest (fusion-node); without one, null'() {
        given:
        work.putText('w/big', 'x' * (3 << 20))

        expect:
        store.copyFrom('work', 'w/big', 3L << 20, null) == null
        store.copyFrom('work', 'w/big', 3L << 20, cidOf('x' * (3 << 20))) == new S3Copied(cidOf('x' * (3 << 20)), 'fusion-node')
        member.calls.count { it.startsWith('PARTCOPY ') } == 1
    }
}
