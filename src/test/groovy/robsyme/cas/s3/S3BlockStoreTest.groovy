package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Hashing
import robsyme.cas.core.NoSuchBlockException
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

class S3BlockStoreTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops s3 = new MemoryS3Ops('member')

    private S3BlockStore store(String prefix = 'cas/', long limit = S3BlockStore.SINGLE_REQUEST_MAX) {
        new S3BlockStore(s3, prefix, 'lab', true, tmp, limit)
    }

    private Path file(String name, byte[] bytes) { Files.write(tmp.resolve(name), bytes) }

    private static byte[] bytes(int n) { (0..<n).collect { (byte) (it * 31) } as byte[] }

    def 'the layout is the local one under the prefix; a bucket-root member has no leading slash (Review Focus 5)'() {
        given:
        final Cid cid = Hashing.hashRaw(new ByteArrayInputStream('hello\n'.bytes), new byte[1024])

        expect:
        store('cas/').key(cid) == "cas/blocks/${cid.toString()[-2..-1]}/${cid}"
        store('').key(cid) == "blocks/${cid.toString()[-2..-1]}/${cid}"
    }

    def 'putStreaming spools, uploads once with SHA-256 and immutable caching; under 1 MiB a second put is a conditional PUT answered 412'() {
        given:
        final S3BlockStore b = store()

        when:
        final Cid cid = b.putStreaming(new ByteArrayInputStream('hello\n'.bytes))
        final Cid again = b.putStreaming(new ByteArrayInputStream('hello\n'.bytes))

        then:
        cid.toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
        again == cid
        s3.objects[b.key(cid)].text() == 'hello\n'
        s3.objects[b.key(cid)].cacheControl == S3BlockStore.IMMUTABLE
        s3.calls.count { it.startsWith('PUT ') } == 2          // under 1 MiB: conditional PUT alone (ticket 02 decision 2)
        b.has(cid) && b.size(cid) == 6L
        b.open(cid).text == 'hello\n'
        Files.list(tmp).count() == 0                            // the spool file is gone
    }

    def 'a block of 1 MiB or more is looked for before any body is sent (ticket 14 item 1)'() {
        given:
        final S3BlockStore b = store()
        final Path f = file('big', bytes(2 << 20))
        final Cid cid = b.putFile(f)
        final long pulled = s3.pulledBytes

        when:
        b.putFile(f)

        then:
        s3.pulledBytes == pulled
        s3.calls.last() == "HEAD ${b.key(cid)}".toString()
    }

    def 'put of a known block of 1 MiB or more HEADs and reads nothing from its stream (ticket 02 answer 2)'() {
        given:
        final S3BlockStore b = store()
        final byte[] content = bytes(2 << 20)
        final Cid cid = Hashing.hashRaw(new ByteArrayInputStream(content), new byte[1 << 20])
        b.put(cid, new ByteArrayInputStream(content), content.length)
        final ByteArrayInputStream second = new ByteArrayInputStream(content)

        when:
        b.put(cid, second, content.length)

        then:
        second.available() == content.length
        s3.calls.last() == "HEAD ${b.key(cid)}".toString()
        s3.calls.count { it.startsWith('PUT ') } == 1
    }

    def 'a write that meets a 409 is tried up to three times in all'() {
        given:
        s3.conflicts['PUT'] = 2

        expect: 'two 409s, then the third try writes'
        store().putDagCbor([kind: 'x']) == DagCbor.cidOf(DagCbor.encode([kind: 'x']))

        when: 'three 409s use up the three tries'
        s3.conflicts['PUT'] = 3
        store().putDagCbor([kind: 'y'])

        then:
        thrown(IOException)
    }

    def 'put verifies the announced address and size before anything is uploaded'() {
        when:
        store().put(Cid.of(Cid.RAW, new byte[32]), new ByteArrayInputStream('x'.bytes), 1L)

        then:
        thrown(BlockMismatchException)
        !s3.calls.any { it.startsWith('PUT ') }
    }

    // PublishAddresser keys its cas.tmpDir abort on this message, so only a spool failure may carry it.
    def "a failing caller stream surfaces its own message, not cas.tmpDir's"() {
        given:
        final InputStream failing = new InputStream() {
            @Override int read() { throw new IOException('the source went away') }
            @Override int read(byte[] b, int off, int len) { throw new IOException('the source went away') }
        }

        when:
        store().putStreaming(failing)

        then:
        final IOException e = thrown()
        e.message == 'the source went away'
        !s3.calls.any { it.startsWith('PUT ') }
        Files.list(tmp).count() == 0
    }

    // setWritable(false) does nothing for root, so the spool would succeed.
    @spock.lang.Requires({ System.getProperty('user.name') != 'root' })
    def 'an unwritable tmpDir surfaces "could not spool to cas.tmpDir"'() {
        given:
        final Path readOnly = Files.createDirectories(tmp.resolve('ro'))
        readOnly.toFile().setWritable(false)
        final S3BlockStore b = new S3BlockStore(s3, 'cas/', 'lab', true, readOnly)

        when:
        b.putStreaming(new ByteArrayInputStream('abc'.bytes))

        then:
        final IOException e = thrown()
        e.message.startsWith('could not spool to cas.tmpDir')
        e.message.contains(readOnly.toString())
        s3.objects.isEmpty()

        cleanup:
        readOnly.toFile().setWritable(true)
    }

    def 'a returned ChecksumSHA256 that differs from the address deletes the object and fails (silent decision 13)'() {
        given:
        final MemoryS3Ops lying = new MemoryS3Ops('member') {
            @Override S3Written put(String key, S3Body body, S3PutOptions o) {
                final S3Written w = super.put(key, body, o)
                return new S3Written(w.status, w.etag, Base64.encoder.encodeToString(new byte[32]))
            }
        }
        final S3BlockStore b = new S3BlockStore(lying, 'cas/', 'lab', true, tmp)

        when:
        b.putFile(file('a', 'abc'.bytes))

        then:
        thrown(BlockMismatchException)
        lying.objects.isEmpty()
    }

    def 'above the single-request limit: multipart from file ranges, each part hashed, completed conditionally'() {
        given:
        final S3BlockStore b = store('cas/', 1L << 20)
        final byte[] content = bytes(3 << 20)

        when:
        final Cid cid = b.putFile(file('large', content))

        then:
        s3.objects[b.key(cid)].bytes == content
        s3.calls.count { it.startsWith('PART ') } == 1          // max(64 MiB, ceil(3 MiB / 10000)): one part
        s3.calls.count { it.startsWith('COMPLETE ') } == 1
        !s3.calls.any { it.startsWith('DELETE ') }             // the composite SHA-256 is not compared
        S3BlockStore.partSize(1L << 40) == (long) Math.ceil((1L << 40) / 10000.0d)
        S3BlockStore.partSize(100L << 30) == S3BlockStore.MIN_PART
    }

    def 'a multipart upload that loses the race at Complete is aborted and counts as written'() {
        given:
        final byte[] content = bytes(2 << 20)
        final Cid cid = Hashing.hashRaw(new ByteArrayInputStream(content), new byte[1 << 20])
        final MemoryS3Ops racing = new MemoryS3Ops('member') {
            @Override String createMultipart(String key, S3PutOptions o) {
                putText(key, 'another writer, same address')   // lands after our HEAD
                return super.createMultipart(key, o)
            }
        }
        final S3BlockStore b = new S3BlockStore(racing, 'cas/', 'lab', true, tmp, 1L << 20)

        when:
        b.putFile(file('large', content))

        then:
        noExceptionThrown()
        racing.calls.count { it.startsWith('ABORT ') } == 1
        racing.uploads.isEmpty()
        racing.objects[b.key(cid)].text() == 'another writer, same address'
    }

    def 'a read-only member refuses writes; an absent block is NoSuchBlockException'() {
        given:
        final S3BlockStore ro = new S3BlockStore(s3, 'cas/', 'shared', false, tmp)

        when:
        ro.putDagCbor([kind: 'x'])
        then:
        thrown(IllegalStateException)

        when:
        ro.size(Cid.of(Cid.RAW, new byte[32]))
        then:
        thrown(NoSuchBlockException)
    }

    def 'listBlocks lists blocks/ only; the Store Log is empty objects under log/'() {
        given:
        final S3BlockStore b = store()
        final Cid a = b.putDagCbor([kind: 'a'])
        s3.putText('cas/tmp/stray', 'x')

        when:
        StoreLog.append(b, StoreLogKind.RUN, a, 1_000L)
        StoreLog.append(b, StoreLogKind.RUN, a, 1_000L)

        then:
        b.listBlocks().toList() == [a]
        StoreLog.read(b)*.cid == [a]
        s3.list('cas/log/', 0).size() == 1
    }
}
