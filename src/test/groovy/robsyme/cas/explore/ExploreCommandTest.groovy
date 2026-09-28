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
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3Ops
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
        out.toString().trim() ==~ /nf-blocks explorer: http:\/\/127\.0\.0\.1:\d+\/\?token=[a-z2-7]{26}/
        RawHttp.send(started.server.port, 'GET', '/m/lab/index/v3.sqlite').status == 200
        RawHttp.send(started.server.port, 'GET', '/members.json').text().contains('"shared"')
    }

    def 'the samplesheet export indexes a Selection whose block was never logged, as fromStore(selection:) does (final review finding 4)'() {
        given:
        final LocalBlockStore lab = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid item = lab.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        final Cid collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        Files.createDirectories(tempDir.resolve('shared'))
        // Starting once first lets the index's one-time block scan (Index.scanOnce)
        // finish with nothing to find, so the Selection added below stays truly
        // unindexed rather than being swept up by that scan.
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))
        // Copied straight into the store afterwards, with no Store Log entry: never logged.
        final Cid selection = lab.putDagCbor(Fixtures.selection([[item: [address: item, via: [collection]]]]))

        when:
        final def csv = RawHttp.send(started.server.port, 'GET', "/api/samplesheet/${selection}.csv")

        then:
        csv.status == 200
        csv.text().readLines()[0] == 'sample'
        csv.text().readLines()[1] == 'A'
    }

    def 'membersOf builds S3MemberFiles for a remote alias and LocalMemberFiles for a local one, writable first'() {
        given:
        final CasConfig config = CasConfig.from([cas: [stores: [
            lab : [location: tempDir.resolve('lab').toString()],
            priv: [location: 's3://bucket/member'],
        ]]], 'cas://lab')
        final List<String> asked = []

        when:
        final LinkedHashMap<String, MemberFiles> members = ExploreCommand.membersOf(config,
            { String bucket -> asked << bucket; new MemoryS3Ops(bucket) } as Closure<S3Ops>)

        then:
        members.keySet().toList() == ['lab', 'priv']
        members['lab'] instanceof LocalMemberFiles
        members['priv'] instanceof S3MemberFiles
        asked == ['bucket']
    }

    def 'explore takes only --port'() {
        expect:
        new CasCommands().run('explore', ['--prot', '1'], config(), new PrintStream(out, true), new PrintStream(err, true)) == 2
        err.toString().contains('--prot')
    }
}
