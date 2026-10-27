package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import robsyme.cas.core.SnapshotBase
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 02 decision 8, ticket 03 decision 1. */
class S3SnapshotStorageTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops s3 = new MemoryS3Ops('member')
    S3SnapshotStorage storage = new S3SnapshotStorage(s3, 'cas/')

    private Path built(String text) { Files.writeString(tmp.resolve("b-${text}.sqlite"), text) }

    def 'no snapshot: base is null and replace is If-None-Match, no-cache, with the run count'() {
        expect:
        storage.base(tmp) == null
        storage.replace(built('one'), new SnapshotBase(null, 0L, 3), null)
        s3.objects['cas/index/v7.sqlite'].cacheControl == 'no-cache'
        s3.objects['cas/index/v7.sqlite'].metadata == [runs: '3']
        storage.base(tmp).runs == 3
        storage.base(tmp).tag == s3.objects['cas/index/v7.sqlite'].etag
    }

    def 'replace is conditional on the ETag it replaces; a stale one is refused and changes nothing'() {
        given:
        storage.replace(built('one'), new SnapshotBase(null, 0L, 1), null)
        final SnapshotBase stale = storage.base(tmp)
        storage.replace(built('two'), new SnapshotBase(null, 0L, 2), stale)

        expect:
        !storage.replace(built('three'), new SnapshotBase(null, 0L, 3), stale)
        s3.objects['cas/index/v7.sqlite'].text() == 'two'
    }

    def 'a snapshot uploaded without x-amz-meta-runs is downloaded once and counted'() {
        given:
        final Path real = tmp.resolve('real.sqlite')
        java.sql.DriverManager.getConnection("jdbc:sqlite:${real}").withCloseable { c ->
            c.createStatement().execute('CREATE TABLE run(completion_cid TEXT)')
            c.createStatement().execute("INSERT INTO run VALUES ('a'), ('b')")
        }
        s3.objects['cas/index/v7.sqlite'] = new MemoryS3Ops.Obj(bytes: Files.readAllBytes(real), etag: '"x"', metadata: [:])

        expect:
        storage.base(tmp).runs == 2
        s3.calls.count { it == 'GET cas/index/v7.sqlite' } == 1
    }

    def 'the page is rewritten only when its stored SHA-256 differs, with no-cache and a text/html type'() {
        given:
        final byte[] page = '<html>1</html>'.bytes

        expect:
        storage.writePage(page)
        !storage.writePage(page)
        storage.writePage('<html>2</html>'.bytes)
        s3.objects['cas/index.html'].cacheControl == 'no-cache'
        s3.calls.count { it == 'PUT cas/index.html' } == 2
    }

    def 'fetch downloads to a local file, or answers null'() {
        expect:
        storage.fetch(tmp) == null

        when:
        storage.replace(built('one'), new SnapshotBase(null, 0L, 1), null)
        final Path f = storage.fetch(tmp)

        then:
        f.text == 'one'
    }

    def 'store id and watermark ride as metadata and come back in base'() {
        expect:
        storage.replace(built('one'), new SnapshotBase(null, 0L, 2, 'a' * 32, '9999999999990-run-x'), null)
        s3.objects['cas/index/v7.sqlite'].metadata == [runs: '2', 'store-id': 'a' * 32, watermark: '9999999999990-run-x']
        storage.base(tmp) == new SnapshotBase(s3.objects['cas/index/v7.sqlite'].etag, s3.objects['cas/index/v7.sqlite'].bytes.length, 2, 'a' * 32, '9999999999990-run-x')
    }

    def 'whether it has storage figures rides as x-amz-meta-storage, and a base without it does not say (fix round 3, item 3)'() {
        expect:
        storage.replace(built('full'), new SnapshotBase(null, 0L, 2, null, null, true), null)
        s3.objects['cas/index/v7.sqlite'].metadata == [runs: '2', storage: 'full']
        storage.base(tmp).figures == Boolean.TRUE

        storage.replace(built('lean'), new SnapshotBase(null, 0L, 2, null, null, false), storage.base(tmp))
        s3.objects['cas/index/v7.sqlite'].metadata == [runs: '2', storage: 'lean']
        storage.base(tmp).figures == Boolean.FALSE

        storage.replace(built('older'), new SnapshotBase(null, 0L, 2), storage.base(tmp))
        storage.base(tmp).figures == null
    }
}
