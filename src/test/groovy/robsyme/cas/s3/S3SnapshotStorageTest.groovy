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
        storage.replace(built('one'), 3, null)
        s3.objects['cas/index/v3.sqlite'].cacheControl == 'no-cache'
        s3.objects['cas/index/v3.sqlite'].metadata == [runs: '3']
        storage.base(tmp).runs == 3
        storage.base(tmp).tag == s3.objects['cas/index/v3.sqlite'].etag
    }

    def 'replace is conditional on the ETag it replaces; a stale one is refused and changes nothing'() {
        given:
        storage.replace(built('one'), 1, null)
        final SnapshotBase stale = storage.base(tmp)
        storage.replace(built('two'), 2, stale)

        expect:
        !storage.replace(built('three'), 3, stale)
        s3.objects['cas/index/v3.sqlite'].text() == 'two'
    }

    def 'a snapshot uploaded without x-amz-meta-runs is downloaded once and counted'() {
        given:
        final Path real = tmp.resolve('real.sqlite')
        java.sql.DriverManager.getConnection("jdbc:sqlite:${real}").withCloseable { c ->
            c.createStatement().execute('CREATE TABLE run(completion_cid TEXT)')
            c.createStatement().execute("INSERT INTO run VALUES ('a'), ('b')")
        }
        s3.objects['cas/index/v3.sqlite'] = new MemoryS3Ops.Obj(bytes: Files.readAllBytes(real), etag: '"x"', metadata: [:])

        expect:
        storage.base(tmp).runs == 2
        s3.calls.count { it == 'GET cas/index/v3.sqlite' } == 1
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
        storage.replace(built('one'), 1, null)
        final Path f = storage.fetch(tmp)

        then:
        f.text == 'one'
    }
}
