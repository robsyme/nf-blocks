package robsyme.cas.nio

import java.nio.file.DirectoryNotEmptyException
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.LocalCoordinateTree
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3Written
import spock.lang.Specification
import spock.lang.TempDir

/**
 * materialiseFile's S3-to-S3 branch (silent decision 9): a member already
 * holding the block in S3 is asked to copy it out server-side rather than
 * having the head node read it. Driven entirely through MemoryS3Ops (unit
 * tests never reach AWS); a fixed s3:// URI stands in for a real S3 nio Path
 * via the provider's test-only s3UriOf seam, so {@code target} stays a plain
 * local file the test can inspect directly.
 */
class StageOutOfS3Test extends Specification {

    @TempDir Path tmp
    Session session
    CasFileSystemProvider provider

    private static Cid cidOf(String text) { Cid.of(Cid.RAW, MessageDigest.getInstance('SHA-256').digest(text.bytes)) }

    def setup() {
        session = Mock(Session)
        Global.session = session
        provider = new CasFileSystemProvider() {
            @Override protected String s3UriOf(Path target) { 's3://work/stage/A.bam' }
        }
    }

    def cleanup() { CasSession.unbind(session); Global.session = null }

    /** Binds a store built over member, with a 'work' peer bucket for the copy's destination. */
    private S3BlockStore storeOver(MemoryS3Ops member, long singleRequestMax = 1L << 20) {
        member.peers['work'] = new MemoryS3Ops('work')
        final S3BlockStore store = new S3BlockStore(member, 'cas/', 'lab', true, tmp.resolve('spool'), singleRequestMax)
        final CasConfig config = CasConfig.from([cas: [stores: [lab: [location: tmp.resolve('lab').toString()]]]], 'cas://lab')
        CasSession.bind(session, new CasSession(config, store, new LocalCoordinateTree(tmp.resolve('coords'))))
        return store
    }

    def 'a copyOut digest mismatch deletes the target and aborts'() {
        given:
        final MemoryS3Ops member = new MemoryS3Ops('member') {
            @Override S3Written copyOut(String key, String targetBucket, String targetKey) {
                final S3Written w = super.copyOut(key, targetBucket, targetKey)
                return new S3Written(w.status, w.etag, Base64.encoder.encodeToString(cidOf('something else').digest))
            }
        }
        final S3BlockStore store = storeOver(member)
        final Cid cid = cidOf('bam-A')
        member.putText(store.key(cid), 'bam-A')
        final Path target = Files.createFile(tmp.resolve('A.bam'))

        when:
        provider.download(provider.getPath(URI.create("cas://${cid}")), target)

        then:
        thrown(AbortRunException)
        !Files.exists(target)
    }

    def 'a copyOut digest mismatch whose delete fails is an abort with the delete failure suppressed'() {
        given:
        final MemoryS3Ops member = new MemoryS3Ops('member') {
            @Override S3Written copyOut(String key, String targetBucket, String targetKey) {
                final S3Written w = super.copyOut(key, targetBucket, targetKey)
                return new S3Written(w.status, w.etag, Base64.encoder.encodeToString(cidOf('something else').digest))
            }
        }
        final S3BlockStore store = storeOver(member)
        final Cid cid = cidOf('bam-A')
        member.putText(store.key(cid), 'bam-A')
        // A non-empty directory at target: Files.deleteIfExists(target) throws
        // DirectoryNotEmptyException rather than deleting it, a portable way to
        // make the delete fail without relying on filesystem permissions.
        final Path target = Files.createDirectory(tmp.resolve('A.bam'))
        Files.createFile(target.resolve('inner'))

        when:
        provider.download(provider.getPath(URI.create("cas://${cid}")), target)

        then:
        final AbortRunException e = thrown()
        e.suppressed.any { it instanceof DirectoryNotEmptyException }
        Files.exists(target)   // the failed delete left it in place
    }

    def 'an SDK RuntimeException from copyOut falls back to streaming the block'() {
        given:
        final MemoryS3Ops member = new MemoryS3Ops('member') {
            @Override S3Written copyOut(String key, String targetBucket, String targetKey) { throw new IllegalStateException('Access Denied (403)') }
        }
        final S3BlockStore store = storeOver(member)
        final Cid cid = cidOf('bam-A')
        member.putText(store.key(cid), 'bam-A')
        final Path target = tmp.resolve('A.bam')

        when:
        provider.download(provider.getPath(URI.create("cas://${cid}")), target)

        then:
        noExceptionThrown()
        Files.readString(target) == 'bam-A'
    }

    def 'a block over the single-request limit streams rather than calling copyOut'() {
        given:
        final MemoryS3Ops member = new MemoryS3Ops('member')
        final S3BlockStore store = storeOver(member, 10L)
        final String text = 'x' * 20
        final Cid cid = cidOf(text)
        member.putText(store.key(cid), text)
        final Path target = tmp.resolve('big.bam')

        when:
        provider.download(provider.getPath(URI.create("cas://${cid}")), target)

        then:
        Files.readString(target) == text
        member.calls.every { !it.startsWith('COPYOUT') }
    }
}
