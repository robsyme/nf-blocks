package robsyme.cas.explore

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.GitEmail
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
        try {
            started?.server?.stop()
        }
        finally {
            joinIdentity()
        }
    }

    /**
     * Waits for explore's nf-blocks-identity thread. It resolves against this
     * spec's identity seams, which IdentitySeamsExtension puts back once the
     * spec ends, so it must not outlive the spec.
     */
    private void joinIdentity() {
        final Thread t = started?.identity
        t?.join(15_000)
        assert !t?.alive
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
        Files.isRegularFile(tempDir.resolve('lab/index/v6.sqlite'))
        out.toString().trim() ==~ /nf-blocks explorer: http:\/\/127\.0\.0\.1:\d+\/\?token=[a-z2-7]{26}/
        RawHttp.send(started.server.port, 'GET', '/m/lab/index/v6.sqlite').status == 200
        RawHttp.send(started.server.port, 'GET', '/members.json').text().contains('"shared"')
    }

    def 'explore listens before identity resolves; stdout is the URL line and the signing line goes to stderr'() {
        given: 'a git double that takes a while to answer'
        Files.createDirectories(tempDir.resolve('shared'))
        final CountDownLatch release = new CountDownLatch(1)
        final AtomicInteger gitCalls = new AtomicInteger()
        CasSession.gitEmail = { -> gitCalls.incrementAndGet(); release.await(10, TimeUnit.SECONDS); 'rob@example.org' } as GitEmail

        when:
        final long t0 = System.nanoTime()
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))
        final long startMillis = (System.nanoTime() - t0).intdiv(1_000_000L)

        then: 'it listens and serves the page while git has not answered'
        startMillis < 5000
        RawHttp.send(started.server.port, 'GET', '/').status == 200
        !err.toString().contains('signing as')

        when:
        release.countDown()
        final String members = RawHttp.send(started.server.port, 'GET', '/members.json').text()

        then:
        members.contains('"principal":"rob@example.org"')
        gitCalls.get() == 1
        out.toString().readLines().size() == 1
        out.toString() ==~ /nf-blocks explorer: http:\/\/127\.0\.0\.1:\d+\/\?token=[a-z2-7]{26}\n/
        err.toString().readLines().contains('nf-blocks: signing as rob@example.org (from git config user.email)')
    }

    def 'identity resolution starts only after the URL line is out, so stderr\'s signing line follows it'() {
        given: 'a slow stdout, and a git double that notes what stdout held when it was asked'
        Files.createDirectories(tempDir.resolve('shared'))
        final OutputStream slow = new OutputStream() {
            @Override void write(int b) { Thread.sleep(1); out.write(b) }
        }
        final CountDownLatch asked = new CountDownLatch(1)
        String seen = null
        CasSession.gitEmail = { -> seen = out.toString(); asked.countDown(); 'rob@example.org' } as GitEmail

        when:
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(slow, true), new PrintStream(err, true))

        then:
        asked.await(10, TimeUnit.SECONDS)
        seen ==~ /nf-blocks explorer: http:\/\/127\.0\.0\.1:\d+\/\?token=[a-z2-7]{26}\n/
    }

    def 'the identity thread is joined before the spec ends, so it never resolves against restored seams'() {
        given: 'a git double that takes a moment to answer'
        Files.createDirectories(tempDir.resolve('shared'))
        CasSession.gitEmail = { -> Thread.sleep(300); 'rob@example.org' } as GitEmail

        when:
        started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))

        then:
        started.identity.name == 'nf-blocks-identity'

        when:
        joinIdentity()

        then:
        !Thread.allStackTraces.keySet().any { Thread t -> t.name == 'nf-blocks-identity' && t.alive }
        err.toString().readLines().contains('nf-blocks: signing as rob@example.org (from git config user.email)')
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

    def 'explore takes POST /api/handoff, signed by the session principal, into the writable member'() {
        given:
        final LocalBlockStore lab = new LocalBlockStore(tempDir.resolve('lab'), 'lab', true)
        final Cid manifest = lab.putDagCbor(Fixtures.runManifest())
        final Cid item = lab.putDagCbor(Fixtures.outputItem([[sample: 'A']]))
        final Cid collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        final Cid selection = lab.putDagCbor(Fixtures.selection([[item: [address: item, via: [collection]]]]))
        Files.createDirectories(tempDir.resolve('shared'))
        final Map cfg = config()
        ((Map) cfg.cas).asserted_by = 'p4-test@example.org'
        started = ExploreCommand.start(['--port', '0'], cfg, new PrintStream(out, true), new PrintStream(err, true))
        final String token = started.server.launchUrl.split('token=')[1]
        final byte[] body = groovy.json.JsonOutput.toJson([template: 'nf-core/differentialabundance', selection: selection.toString(),
            project: null, files: [[name: 'samplesheet.csv', text: 'sample\nA\n']], params: [input: [file: 'samplesheet.csv']],
            timestamp: robsyme.cas.core.Index.isoMillis(System.currentTimeMillis())]).getBytes('UTF-8')

        when:
        final def r = RawHttp.send(started.server.port, 'POST', '/api/handoff',
            ['Content-Type': 'application/json', 'X-NF-Blocks-Token': token], body)
        final Map answer = (Map) new groovy.json.JsonSlurper().parse(r.body)
        final Cid handoff = Cid.parse((String) answer.address)

        then:
        r.status == 200
        answer.written == true
        lab.has(handoff)
        robsyme.cas.core.DagCbor.decode(lab.open(handoff).readAllBytes()).asserted_by == 'p4-test@example.org'
        StoreLog.read(lab).any { it.kind == StoreLogKind.HANDOFF && it.cid == handoff }
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

    def 'a failed listing for shadowed coordinates warns on stderr and explore still starts (rule 3)'() {
        given:
        MemoryS3Ops bucket = new MemoryS3Ops('member') {
            @Override List<robsyme.cas.s3.S3Listed> list(String prefix, int maxKeys) {
                if( prefix.startsWith('cas/coords') ) throw new IOException('403 Forbidden')
                return super.list(prefix, maxKeys)
            }
        }
        Closure<S3Ops> saved = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        Map cfg = [lineage: [store: [location: 'cas://lab']],
                   cas: [stores: [lab: [location: 's3://member/cas']], index: [path: tempDir.resolve('s3-cache.sqlite').toString()],
                         tmpDir: tempDir.resolve('t').toString()]]

        when:
        started = ExploreCommand.start([], cfg, new PrintStream(out, true), new PrintStream(err, true))

        then:
        started.server.port > 0
        err.toString().contains("nf-blocks:explore: could not look for shadowed coordinates in 'lab' (403 Forbidden); continuing")

        cleanup:
        started?.index?.close()
        CasSession.s3OpsFactory = saved
    }

    def 'members.json says each member kind, the S3 location, and whether a copy may write it'() {
        given:
        Closure<S3Ops> saved = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map c, String name -> new MemoryS3Ops(name) } as Closure<S3Ops>
        Files.createDirectories(tempDir.resolve('shared'))
        Map cfg = [lineage: [store: [location: 'cas://lab']],
                   cas: [stores: [lab: [location: tempDir.resolve('lab').toString()],
                                  shared: [location: tempDir.resolve('shared').toString(), readOnly: true],
                                  org: [location: 's3://lab-blocks/cas'],
                                  frozen: [location: 's3://archive-blocks', readOnly: true]],
                         index: [path: tempDir.resolve('cache/index.sqlite').toString()],
                         tmpDir: tempDir.resolve('t').toString()]]

        when:
        started = ExploreCommand.start([], cfg, new PrintStream(out, true), new PrintStream(err, true))
        final String body = RawHttp.send(started.server.port, 'GET', '/members.json').text()
        final Map json = new groovy.json.JsonSlurper().parseText(body) as Map

        then:
        json.copy == true
        json.members*.alias == ['lab', 'shared', 'org', 'frozen']
        json.members[0] == [alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null, copy_target: true, copy_reason: null]
        json.members[1] == [alias: 'shared', writable: false, base: 'm/shared/', kind: 'local', location: null, copy_target: false, copy_reason: 'readOnly is set for this store']
        json.members[2] == [alias: 'org', writable: false, base: 'm/org/', kind: 's3', location: 's3://lab-blocks/cas', copy_target: true, copy_reason: null]
        json.members[3] == [alias: 'frozen', writable: false, base: 'm/frozen/', kind: 's3', location: 's3://archive-blocks', copy_target: false, copy_reason: 'readOnly is set for this store']
        !body.contains(tempDir.toString())

        cleanup:
        started?.index?.close()
        CasSession.s3OpsFactory = saved
    }

    def 'explore takes only --port'() {
        expect:
        new CasCommands().run('explore', ['--prot', '1'], config(), new PrintStream(out, true), new PrintStream(err, true)) == 2
        err.toString().contains('--prot')
    }
}
