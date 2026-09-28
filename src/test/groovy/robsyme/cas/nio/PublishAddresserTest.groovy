package robsyme.cas.nio

import java.nio.file.Files
import java.nio.file.Path

import nextflow.exception.AbortRunException
import robsyme.cas.core.*
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

class PublishAddresserTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore lab

    def setup() { lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true) }

    private Path taskFile(String rel, String text) {
        final Path f = tmp.resolve('work/ab/cdef').resolve(rel)
        Files.createDirectories(f.parent); Files.writeString(f, text); f
    }

    private void nodeDigest(String rel, String text) {
        final String hex = java.security.MessageDigest.getInstance('SHA-256').digest(text.bytes).encodeHex().toString()
        Files.writeString(tmp.resolve('work/ab/cdef/.command.cas'), "${hex}  ${rel}\n")
    }

    def 'nodeHash off: the head node streams and hashes; bytes are counted'() {
        given:
        final PublishAddresser a = new PublishAddresser(lab, lab, false, tmp.resolve('work'))

        when:
        final Addressed r = a.address(taskFile('A.bam', 'bam-A'), 5L)

        then:
        r.provider == Providers.HEAD_NODE
        a.headNodeBytes == 5L
        a.counts == ['head-node': 1]
    }

    // setReadable(false) does nothing for root: a read would succeed and the feature prove nothing.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'a node digest for a block the member holds: no byte read; fusion-node'() {
        given:
        lab.putStreaming(new ByteArrayInputStream('bam-A'.bytes))
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-A')
        final PublishAddresser a = new PublishAddresser(lab, lab, true, tmp.resolve('work'))
        f.toFile().setReadable(false)     // a read would fail

        expect:
        a.address(f, 5L).provider == Providers.FUSION_NODE
        a.headNodeBytes == 0L
    }

    def 'a node digest for a block the member lacks: the head node reads, and must agree'() {
        given:
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-B')

        when:
        new PublishAddresser(lab, lab, true, tmp.resolve('work')).address(f, 5L)

        then:
        final AbortRunException e = thrown()
        e.message.contains('.command.cas')
        e.message.contains('A.bam')
    }

    // Task 9 review: a declared glob can match a copied input, so .command.cas holds lines for files never published.
    def 'a node digest is found by the published file path relative to the task directory, never by line position'() {
        given:
        final Closure<String> hex = { String t -> java.security.MessageDigest.getInstance('SHA-256').digest(t.bytes).encodeHex().toString() }
        lab.putStreaming(new ByteArrayInputStream('bam-A'.bytes))
        final Path f = taskFile('out/A.bam', 'bam-A')
        taskFile('A.bam', 'staged input')
        Files.writeString(tmp.resolve('work/ab/cdef/.command.cas'),
            "${hex('staged input')}  A.bam\n${hex('other')}  in.fastq\n${hex('bam-A')}  out/A.bam\n")
        final PublishAddresser a = new PublishAddresser(lab, lab, true, tmp.resolve('work'))

        when:
        final Addressed r = a.address(f, 5L)

        then:
        r.provider == Providers.FUSION_NODE
        r.cid == lab.putStreaming(new ByteArrayInputStream('bam-A'.bytes))
        a.headNodeBytes == 0L
    }

    def '.command.cas is read once per task directory'() {
        given:
        final Path x = taskFile('x', 'x'); final Path y = taskFile('y', 'y')
        final int[] reads = [0]
        final PublishAddresser a = new PublishAddresser(lab, lab, true, tmp.resolve('work')) {
            @Override protected InputStream openDigests(Path file) { reads[0]++; super.openDigests(file) }
        }

        when:
        a.address(x, 1L); a.address(y, 1L)

        then:
        reads[0] == 1
    }

    // setWritable(false) does nothing for root, so the spool would succeed.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'over the limit, no node digest, S3 member: the object is spooled through cas.tmpDir, and an unwritable tmpDir aborts naming it (Review Focus 1)'() {
        given:
        final Path readOnly = Files.createDirectories(tmp.resolve('ro'))
        readOnly.toFile().setWritable(false)
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member')
        s3.peers['work'] = new robsyme.cas.s3.MemoryS3Ops('work')
        s3.peers['work'].putText('w/big', 'x' * (2 << 20))
        final robsyme.cas.s3.S3BlockStore member = new robsyme.cas.s3.S3BlockStore(s3, 'cas/', 'lab', true,
            readOnly, 1L << 20)
        final Path big = taskFile('big', 'x' * (2 << 20))
        // The local file stands in for s3://work/w/big: copyFrom answers null (over the limit, no digest).
        final PublishAddresser a = new PublishAddresser(member, member, false, tmp.resolve('work'), 1L << 20) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/big'] }
        }

        when:
        a.address(big, 2L << 20)

        then:
        final AbortRunException e = thrown()
        e.message.contains('cas.tmpDir')
        e.message.contains(String.valueOf(2L << 20))
        s3.objects.isEmpty()

        cleanup:
        readOnly.toFile().setWritable(true)
    }

    private robsyme.cas.s3.S3BlockStore s3Member(robsyme.cas.s3.MemoryS3Ops s3, String text) {
        s3.peers['work'] = new robsyme.cas.s3.MemoryS3Ops('work')
        s3.peers['work'].putText('w/ab/cdef/A.bam', text)
        return new robsyme.cas.s3.S3BlockStore(s3, 'cas/', 'lab', true, tmp.resolve('spool'))
    }

    def 'a copy whose SHA-256 disagrees with .command.cas aborts the run, naming the file and .command.cas (ticket 16 decision 3)'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member')
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-B')
        final PublishAddresser a = new PublishAddresser(member, member, true, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        a.address(f, 5L)

        then:
        final AbortRunException e = thrown()
        e.message.contains('A.bam')
        e.message.contains('.command.cas')
        e.cause instanceof BlockMismatchException
    }

    def 'a copy that fails for another reason warns and falls back to the head-node read'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member') {
            @Override robsyme.cas.s3.S3Written copy(String sb, String sk, String key, robsyme.cas.s3.S3PutOptions o) {
                throw new IOException('S3 is having a day')
            }
        }
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        final PublishAddresser a = new PublishAddresser(member, member, false, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        final Addressed r = a.address(f, 5L)

        then:
        r.provider == Providers.HEAD_NODE
        a.headNodeBytes == 5L
        s3.objects[member.key(r.cid)].text() == 'bam-A'
    }

    def 'a copy the SDK refuses with a RuntimeException falls back to the head-node read'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member') {
            @Override robsyme.cas.s3.S3Written copy(String sb, String sk, String key, robsyme.cas.s3.S3PutOptions o) {
                throw new IllegalStateException('Access Denied (403)')
            }
        }
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        final PublishAddresser a = new PublishAddresser(member, member, false, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        final Addressed r = a.address(f, 5L)

        then:
        r.provider == Providers.HEAD_NODE
        a.headNodeBytes == 5L
        s3.objects[member.key(r.cid)].text() == 'bam-A'
    }

    def 'a copy with no full-object SHA-256 (#sha) falls back to the head node, which is held to the node digest'() {
        given:
        final String returned = sha     // a data variable is not visible inside an anonymous class
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member') {
            @Override robsyme.cas.s3.S3Written copy(String sb, String sk, String key, robsyme.cas.s3.S3PutOptions o) {
                final robsyme.cas.s3.S3Written w = super.copy(sb, sk, key, o)
                return new robsyme.cas.s3.S3Written(w.status, w.etag, returned)
            }
        }
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', said)
        final PublishAddresser a = new PublishAddresser(member, member, true, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        Addressed r = null
        String failure = null
        try { r = a.address(f, 5L) } catch( AbortRunException e ) { failure = e.message }

        then:
        (failure == null) == agrees
        agrees ? r.provider == Providers.HEAD_NODE && a.headNodeBytes == 5L : failure.contains('.command.cas') && failure.contains('the head node hashed')

        where:
        sha     | said    | agrees
        null    | 'bam-A' | true
        'abc-2' | 'bam-A' | true
        null    | 'bam-B' | false
    }

    def 'a mismatch whose copy cannot be deleted aborts naming the key, and the key is never taken as the address'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member') {
            @Override void delete(String key) { throw new IllegalStateException('Access Denied (403) by bucket policy') }
        }
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-B')
        final Cid said = Cid.of(Cid.RAW, java.security.MessageDigest.getInstance('SHA-256').digest('bam-B'.bytes))
        final PublishAddresser a = new PublishAddresser(member, member, true, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        a.address(f, 5L)

        then:
        final AbortRunException e = thrown()
        e.message.contains('A.bam')
        e.message.contains(member.key(said))
        e.cause instanceof robsyme.cas.s3.S3UnremovedCopyException
        s3.calls.count { it == "HEAD ${member.key(said)}".toString() } == 1
        a.counts.isEmpty()
    }
}
