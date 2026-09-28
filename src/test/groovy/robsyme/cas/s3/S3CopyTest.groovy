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

    private S3BlockStore storeOver(MemoryS3Ops ops) {
        ops.peers['work'] = work
        return new S3BlockStore(ops, 'cas/', 'lab', true, tmp, 1L << 20)
    }

    def 'an SDK refusal that is not a 412 or 409 arrives as an IOException keeping its cause'() {
        given:
        final MemoryS3Ops denying = new MemoryS3Ops('member') {
            @Override S3Written copy(String sb, String sk, String key, S3PutOptions o) { throw new IllegalStateException('Access Denied (403)') }
        }

        when:
        storeOver(denying).copyFrom('work', 'w/ab/cd/A.bam', 5L, expected)

        then:
        final IOException e = thrown()
        e.message.contains('Access Denied (403)')
        e.cause instanceof IllegalStateException

        where:
        expected << [null, cidOf('bam-A')]
    }

    def 'a part copy refused by the SDK is an IOException, and the upload is aborted'() {
        given:
        work.putText('w/big', 'x' * (3 << 20))
        final MemoryS3Ops denying = new MemoryS3Ops('member') {
            @Override S3Part uploadPartCopy(String key, String id, int n, String sb, String sk, long first, long last) { throw new IllegalStateException('Slow Down (503)') }
        }

        when:
        storeOver(denying).copyFrom('work', 'w/big', 3L << 20, cidOf('x' * (3 << 20)))

        then:
        final IOException e = thrown()
        e.cause instanceof IllegalStateException
        denying.calls.count { it.startsWith('ABORT ') } == 1
        denying.objects.isEmpty()
    }

    def 'a copy with no full-object SHA-256 is an IOException and leaves nothing at the final key (sha256 #sha)'() {
        given:
        final String returned = sha     // a data variable is not visible inside an anonymous class
        final MemoryS3Ops mute = new MemoryS3Ops('member') {
            @Override S3Written copy(String sb, String sk, String key, S3PutOptions o) {
                final S3Written w = super.copy(sb, sk, key, o)
                return new S3Written(w.status, w.etag, returned)
            }
        }
        final S3BlockStore b = storeOver(mute)

        when:
        b.copyFrom('work', 'w/ab/cd/A.bam', 5L, expected)

        then:
        final IOException e = thrown()
        !(e instanceof BlockMismatchException)
        e.message.contains('no full-object SHA-256')
        mute.objects.isEmpty()

        where:
        sha     | expected
        null    | cidOf('bam-A')
        'abc-2' | cidOf('bam-A')
        null    | null
        'abc-2' | null
    }

    def 'a mismatch whose delete is refused is an S3UnremovedCopyException naming the key, the mismatch its cause'() {
        given:
        final MemoryS3Ops guarded = new MemoryS3Ops('member') {
            @Override void delete(String key) { throw new IllegalStateException('Access Denied (403) by bucket policy') }
        }
        final S3BlockStore b = storeOver(guarded)
        final Cid wrong = cidOf('something else')

        when:
        b.copyFrom('work', 'w/ab/cd/A.bam', 5L, wrong)

        then:
        final S3UnremovedCopyException e = thrown()
        e.location == "memory://member/${b.key(wrong)}"
        e.message.contains(b.key(wrong))
        e.cause instanceof BlockMismatchException
        e.suppressed*.message == ['Access Denied (403) by bucket policy']
        guarded.objects.containsKey(b.key(wrong))
    }

    def 'a staging copy that cannot be deleted is left to the lifecycle rule; the block is still placed'() {
        given:
        final MemoryS3Ops guarded = new MemoryS3Ops('member') {
            @Override void delete(String key) { throw new IllegalStateException('Slow Down (503)') }
        }
        final S3BlockStore b = storeOver(guarded)

        expect:
        b.copyFrom('work', 'w/ab/cd/A.bam', 5L, null) == new S3Copied(cidOf('bam-A'), 's3-copy')
        guarded.objects[b.key(cidOf('bam-A'))].text() == 'bam-A'
    }
}
