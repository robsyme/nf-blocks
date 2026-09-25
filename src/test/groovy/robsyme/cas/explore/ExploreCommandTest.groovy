package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.CasConfig
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.cli.CasCommands
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Specification
import spock.lang.TempDir

class ExploreCommandTest extends Specification {

    @TempDir
    Path tempDir

    ExploreCommand.Started started
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    def cleanup() {
        started?.server?.stop()
    }

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('lab').toString()],
                         shared: [location: tempDir.resolve('shared').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
            ],
        ]
    }

    def 'explore rewrites the writable snapshot at start, then serves every member'() {
        given:
        final LocalBlockStore lab = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', []))
        final Cid completion = lab.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(lab, StoreLogKind.RUN, completion, System.currentTimeMillis())
        Files.createDirectories(tempDir.resolve('shared'))

        when:
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))

        then:
        Files.isRegularFile(tempDir.resolve('lab/index/v3.sqlite'))
        out.toString().trim() == "nf-blocks explorer: ${started.server.url}"
        RawHttp.send(started.server.port, 'GET', '/m/lab/index/v3.sqlite').status == 200
        RawHttp.send(started.server.port, 'GET', '/members.json').text().contains('"shared"')
    }

    def 'membersOf builds S3MemberFiles for a remote alias and LocalMemberFiles for a local one, writable first'() {
        given:
        // No real client is ever called: the factory hands membersOf a client
        // built with an explicit region and static credentials, so building
        // it (and this test) never reaches the network or IMDS.
        S3Client noNetworkClient = S3Client.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('test', 'test')))
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build()
        final CasConfig config = CasConfig.from([cas: [stores: [
            lab : [location: tempDir.resolve('lab').toString()],
            priv: [location: 's3://bucket/member'],
        ]]], 'cas://lab')

        when:
        final LinkedHashMap<String, MemberFiles> members = ExploreCommand.membersOf(config, { -> noNetworkClient })

        then:
        members.keySet().toList() == ['lab', 'priv']
        members['lab'] instanceof LocalMemberFiles
        members['priv'] instanceof S3MemberFiles

        cleanup:
        noNetworkClient?.close()
    }

    def 'explore takes only --port'() {
        expect:
        new CasCommands().run('explore', ['--prot', '1'], config(), new PrintStream(out, true), new PrintStream(err, true)) == 2
        err.toString().contains('--prot')
    }
}
