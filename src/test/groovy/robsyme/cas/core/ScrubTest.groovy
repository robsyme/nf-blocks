package robsyme.cas.core

import java.nio.file.Path
import java.nio.file.Paths

import spock.lang.Specification

/**
 * DESIGN.md §6, the portability scrub. A run's `params` and `config` describe
 * the run, not the machine it ran on: an absolute path, a bucket or the OS
 * user name in a block would make the block unusable to anyone else and would
 * break Gate assertion 10.
 */
class ScrubTest extends Specification {

    def 'the store-local config scopes are dropped'() {
        given:
        final Map config = [
            cas        : [stores: [lab: [location: '/data/cas']]],
            lineage    : [enabled: true],
            workDir    : '/scratch/work',
            outputDir  : 'cas://lab',
            launchDir  : '/home/x',
            projectDir : '/home/x/pipeline',
            homeDir    : '/home/x',
            configFiles: ['/home/x/nextflow.config'],
            scriptFile : '/home/x/main.nf',
            commandLine: 'nextflow run .',
            runName    : 'cheeky_curie',
            resume     : true,
            process    : [cpus: 2],
        ]

        when:
        final Map scrubbed = (Map) Records.scrub(config)

        then:
        scrubbed.keySet() == ['process'] as Set
        scrubbed.process == [cpus: 2]
    }

    def 'the scopes are dropped only at the top level'() {
        expect:
        Records.scrub([params: [outputDir: 'results', resume: true]]) == [params: [outputDir: 'results', resume: true]]
    }

    def 'a Path value becomes a string'() {
        given:
        final Path path = Paths.get('reference/genome.fa')

        expect:
        Records.scrub([genome: path]) == [genome: 'reference/genome.fa']
    }

    def 'an absolute path is redacted'() {
        expect:
        Records.scrub([genome: '/Users/x/y']) == [genome: '[redacted-location]']
        Records.scrub([genome: Paths.get('/Users/x/y')]) == [genome: '[redacted-location]']
    }

    def 'a location in another scheme is redacted'() {
        expect:
        Records.scrub([genome: 's3://bucket/x']) == [genome: '[redacted-location]']
        Records.scrub([genome: 'az://container/x']) == [genome: '[redacted-location]']
        Records.scrub([genome: 'file:///data/x']) == [genome: '[redacted-location]']
    }

    def 'lineage and store references stay, they are location-free'() {
        given:
        final String cas = 'cas://bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am/A.bam'

        expect:
        Records.scrub([from: 'lid://abc/x']) == [from: 'lid://abc/x']
        Records.scrub([from: cas]) == [from: cas]
    }

    def 'a relative path is left alone'() {
        expect:
        Records.scrub([genome: 'reference/genome.fa']) == [genome: 'reference/genome.fa']
        Records.scrub([sample: 'A']) == [sample: 'A']
    }

    def 'the os user name is redacted wherever it appears'() {
        given:
        final String user = System.getProperty('user.name')

        expect:
        user
        Records.scrub([owner: user]) == [owner: '[redacted-user]']
        Records.scrub([owners: [user, 'someone-else']]) == [owners: ['[redacted-user]', 'someone-else']]
    }

    def 'nested maps and lists are scrubbed all the way down'() {
        given:
        final Map params = [
            samples: [
                [id: 'A', reads: ['/data/A_1.fq', '/data/A_2.fq']],
                [id: 'B', reads: ['reads/B_1.fq']],
            ],
            options: [store: [dir: 's3://bucket/out'], depth: 30],
        ]

        expect:
        Records.scrub(params) == [
            samples: [
                [id: 'A', reads: ['[redacted-location]', '[redacted-location]']],
                [id: 'B', reads: ['reads/B_1.fq']],
            ],
            options: [store: [dir: '[redacted-location]'], depth: 30],
        ]
    }

    def 'numbers, booleans and nulls are untouched'() {
        expect:
        Records.scrub([cpus: 2, ratio: 0.5, single_end: false, missing: null]) == [cpus: 2, ratio: 0.5, single_end: false, missing: null]
    }

    def 'the input is not mutated'() {
        given:
        final Map inner = [dir: '/data/out']
        final List reads = ['/data/A_1.fq']
        final Map params = [workDir: '/scratch', options: inner, reads: reads]

        when:
        Records.scrub(params)

        then:
        params == [workDir: '/scratch', options: [dir: '/data/out'], reads: ['/data/A_1.fq']]
        inner.dir == '/data/out'
        reads == ['/data/A_1.fq']
    }

    def 'scrubbing twice changes nothing more'() {
        given:
        final Map params = [genome: '/data/genome.fa', owner: System.getProperty('user.name'), depth: 30]

        expect:
        Records.scrub(Records.scrub(params)) == Records.scrub(params)
    }

    def 'a value that is not a container is scrubbed on its own'() {
        expect:
        Records.scrub('/data/x') == '[redacted-location]'
        Records.scrub(null) == null
        Records.scrub(7) == 7
    }

    def 'scrubText redacts an absolute path or user name embedded mid-message'() {
        given:
        final String user = System.getProperty('user.name')

        expect: 'a path token anywhere in the message goes, its trailing punctuation kept'
        Records.scrubText("Failed to publish file: /scratch/run/work/ab/cd/A_qc; to: cas://lab/qc/A/A_qc [copy]") ==
            "Failed to publish file: [redacted-location]; to: cas://lab/qc/A/A_qc [copy]"

        and: 'a non-portable scheme token goes, a lid/cas token stays'
        Records.scrubText("staged s3://bucket/x and cas://bafk/y") == "staged [redacted-location] and cas://bafk/y"

        and: 'a standalone user token goes'
        Records.scrubText("ran by ${user}".toString()) == 'ran by [redacted-user]'

        and: 'a work-dir line leaks neither the path nor the user embedded in it'
        final String msg = Records.scrubText("Work dir: /home/${user}/proj/work/xx".toString())
        !msg.contains(user)
        !msg.contains('/home/')

        and: 'null passes through'
        Records.scrubText(null) == null
    }

    def 'scrubText redacts a quoted or bracketed path, as config text writes one'() {
        given:
        final String user = System.getProperty('user.name')

        expect:
        Records.scrubText("workDir = '/Users/x/work'") == "workDir = '[redacted-location]'"
        Records.scrubText('location = "/data/cas"') == 'location = "[redacted-location]"'
        Records.scrubText("files = ['/a/b', 's3://bucket/c']") == "files = ['[redacted-location]', '[redacted-location]']"
        Records.scrubText("owner = '${user}'".toString()) == "owner = '[redacted-user]'"

        and: 'code and portable values keep their shape'
        Records.scrubText("ext.args = { \"--x ${'$'}{task.cpus}\" }") == "ext.args = { \"--x ${'$'}{task.cpus}\" }"
        Records.scrubText("input = 'cas://bafk/y'") == "input = 'cas://bafk/y'"

        and: 'idempotent'
        Records.scrubText(Records.scrubText("workDir = '/Users/x/work'")) == "workDir = '[redacted-location]'"
    }

    def 'a value dag-cbor cannot encode is recorded as its text'() {
        given:
        final Closure c = { -> 1 }

        expect:
        Records.scrub([memory: nextflow.util.MemoryUnit.of('8 GB')]) == [memory: '8 GB']
        Records.scrub([time: nextflow.util.Duration.of('2h')]) == [time: '2h']
        Records.scrub([f: c]).f instanceof String

        and: 'what it can encode is kept as it is'
        Records.scrub([b: true, n: 2.5, i: 3L]) == [b: true, n: 2.5, i: 3L]
    }

    def 'a RunCompletion scrubs its error field so a failed run never leaks the launch path'() {
        given:
        final Cid run = Cid.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')
        final RunCompletion rc = new RunCompletion(
            assertedBy: 'gate', run: run, collections: [], status: RunCompletion.FAILED,
            exitStatus: 7, possiblyIncomplete: true, startedAt: '2026-09-03T00:00:00.000Z',
            finishedAt: '2026-09-03T00:00:01.000Z', anomalies: Anomalies.NONE,
            error: "Process failed in /scratch/gate-root/work/ab/cd; exit 7")

        expect:
        rc.error == "Process failed in [redacted-location]; exit 7"
        !rc.error.contains('/scratch')
    }
    def 'scrubText redacts a path, a non-portable URI or the user name after = or : inside a token'() {
        given:
        final String user = System.getProperty('user.name')

        expect:
        Records.scrubText("containerOptions = '--volume=/home/x/data:/data'") ==
            "containerOptions = '--volume=[redacted-location]:[redacted-location]'"
        Records.scrubText("beforeScript = 'export TMPDIR=/scratch/x'") == "beforeScript = 'export TMPDIR=[redacted-location]'"
        Records.scrubText("clusterOptions = '--account=${user}'".toString()) == "clusterOptions = '--account=[redacted-user]'"
        Records.scrubText("args = '--in=s3://bucket/x --ref=file:///data/r'") == "args = '--in=[redacted-location] --ref=[redacted-location]'"
        Records.scrubText("mount = '${user}:/data'".toString()) == "mount = '[redacted-user]:[redacted-location]'"

        and: 'portable references, times and other colons keep their shape'
        Records.scrubText("args = '--from=cas://bafk/y --lid=lid://abc/x'") == "args = '--from=cas://bafk/y --lid=lid://abc/x'"
        Records.scrubText("time = '10:00:00' tag = 'a:b=c'") == "time = '10:00:00' tag = 'a:b=c'"

        and: 'idempotent'
        final String once = Records.scrubText("containerOptions = '--volume=/home/x/data:/data --account=${user}'".toString())
        Records.scrubText(once) == once
    }

    def 'scrubConfigText redacts the value of any assignment whose key names a secret'() {
        given:
        final String text = """\
            env {
                FOO_API_KEY = 'sk-live-123'
                GITHUB_PAT = "ghp_abc"
                MY_TOKEN = 'tok'
            }
            azure.storage.accountKey = 'acct=='
            azure {
                batch {
                    accountKey = 'batchkey'
                }
            }
            aws {
                accessKey = 'AKIA1'
                secretKey = 'SECRET1'
            }
            tower.accessToken = 'twr'
            db.password = 'pw1'
            db.passwd = 'pw2'
            git.credentials = 'cred'
            other = [SERVICE_SECRET: 'svc', plain: 'keep']
            publishDir.path = 'results'
            params.pattern = '*.bam'
            process.cpus = 2
            """.stripIndent()

        when:
        final String scrubbed = Records.scrubConfigText(text)

        then:
        ['sk-live-123', 'ghp_abc', "'tok'", 'acct==', 'batchkey', 'AKIA1', 'SECRET1', 'twr', 'pw1', 'pw2', "'cred'", 'svc'].every { String secret ->
            !scrubbed.contains(secret)
        }
        scrubbed.contains("FOO_API_KEY = '[secret]'")
        scrubbed.contains("GITHUB_PAT = '[secret]'")
        scrubbed.contains("azure.storage.accountKey = '[secret]'")
        scrubbed.contains("        accountKey = '[secret]'")
        scrubbed.contains("other = [SERVICE_SECRET: '[secret]', plain: 'keep']")

        and: 'a key that only contains "pat" inside another word is not a secret'
        scrubbed.contains("publishDir.path = 'results'")
        scrubbed.contains("params.pattern = '*.bam'")
        scrubbed.contains('process.cpus = 2')

        and: 'idempotent, and the path and user scrub still applies'
        Records.scrubConfigText(scrubbed) == scrubbed
        Records.scrubConfigText("workDir = '/x/work'\napiKey = '/x/key'") == "workDir = '[redacted-location]'\napiKey = '[secret]'"
        Records.scrubConfigText(null) == null
    }

    def 'a RunManifest records its config text through scrubConfigText'() {
        when:
        final RunManifest manifest = new RunManifest(RecordsTest.manifestArgs() +
            [config: "env {\n    FOO_API_KEY = 'sk-live-123'\n}\nprocess.containerOptions = '--volume=/home/x:/data'\n"])

        then:
        manifest.config == "env {\n    FOO_API_KEY = '[secret]'\n}\nprocess.containerOptions = '--volume=[redacted-location]:[redacted-location]'\n"
    }
}
