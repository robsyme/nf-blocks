package robsyme.cas.explore

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Specification

/** S3MemberFiles through the real SDK, against a fake S3 on loopback. */
class S3MemberFilesTest extends Specification {

    static final String CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'

    FakeS3 s3
    S3Client client
    byte[] snapshot = (0..<10240).collect { (byte) (it & 0xff) } as byte[]

    def setup() {
        s3 = new FakeS3().start()
        client = S3Client.builder()
            .endpointOverride(URI.create(s3.url))
            .forcePathStyle(true)
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('test', 'test')))
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build()
        s3.objects['bucket/member/index/v2.sqlite'] = snapshot
        s3.objects["bucket/member/blocks/${CID[-2..-1]}/${CID}".toString()] = [0xa0] as byte[]
        (1..5).each { s3.objects["bucket/member/log/823277415999${it}-run-${CID}".toString()] = new byte[0] }
        s3.objects['bucket/elsewhere/log/x'] = new byte[0]
    }

    def cleanup() {
        client?.close()
        s3?.stop()
    }

    def 'size, ranged reads and a paged listing under the member prefix'() {
        given:
        s3.pageSize = 2
        final S3MemberFiles files = new S3MemberFiles(client, 'bucket', 'member/')

        when:
        final List<String> logNames = files.list('log')

        then:
        files.open('index/v2.sqlite').size == 10240L
        files.open('index/v3.sqlite') == null
        files.open('index/v2.sqlite').read(4096, 100).withCloseable { it.readAllBytes() } == Arrays.copyOfRange(snapshot, 4096, 4196)
        logNames.size() == 5
        logNames.every { it.endsWith(CID) && !it.contains('/') }
        // One full paginated traversal of 5 keys at page size 2: three requests.
        s3.requests.count { it.startsWith('GET /bucket?') } == 3
    }

    def 'an opened object reads only the version it opened: a replaced object fails the read (final review finding 1)'() {
        given:
        final S3MemberFiles files = new S3MemberFiles(client, 'bucket', 'member/')
        final MemberFiles.Opened opened = files.open('index/v2.sqlite')
        final String tag = opened.tag

        when:
        s3.objects['bucket/member/index/v2.sqlite'] = snapshot.collect { byte b -> (byte) (b ^ 1) } as byte[]
        opened.read(0, 100).withCloseable { it.readAllBytes() }

        then:
        thrown(Exception)
        tag == s3.etagOf(snapshot)
        files.open('index/v2.sqlite').tag != tag
    }

    def 'open parses bucket and prefix from the URI'() {
        expect:
        S3MemberFiles.bucketAndPrefix(URI.create(uri)) == expected

        where:
        uri                      | expected
        's3://bucket'            | ['bucket', '']
        's3://bucket/'           | ['bucket', '']
        's3://bucket/member'     | ['bucket', 'member/']
        's3://bucket/a/b/'       | ['bucket', 'a/b/']
        // A last dotted label that starts with a digit fails java.net.URI's
        // server-based authority syntax, so getHost() would be null here;
        // getAuthority() still carries the bucket name correctly.
        's3://data.2024/x'       | ['data.2024', 'x/']
        's3://logs.v1.0/a/b/'    | ['logs.v1.0', 'a/b/']
    }

    def 'explore serves an S3 member with Range, end to end'() {
        given:
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        members.put('priv', new S3MemberFiles(client, 'bucket', 'member/'))
        final ExploreServer server = new ExploreServer(members, 'lab', 'x'.bytes).start(0)

        when:
        final def r = RawHttp.send(server.port, 'GET', '/m/priv/index/v2.sqlite', [Range: 'bytes=0-4095'])
        final def log = RawHttp.send(server.port, 'GET', '/m/priv/log/')

        then:
        r.status == 206
        r.body == Arrays.copyOfRange(snapshot, 0, 4096)
        r.headers['etag'] == s3.etagOf(snapshot)
        log.text().count(CID) == 5

        cleanup:
        server.stop()
    }
}
