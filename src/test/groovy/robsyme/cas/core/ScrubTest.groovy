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

    String saved

    def 'scrubText leaves @<user> alone, a handle rather than a leak, and still redacts a path after an @'() {
        given:
        saved = System.getProperty('user.name')
        System.setProperty('user.name', 'alice')

        expect:
        Records.scrubText("manifest { contributors = [[name:'Alice Example', affiliation:'Example Lab', github:'@alice', contribution:['contributor'], orcid:'']] }") ==
            "manifest { contributors = [[name:'Alice Example', affiliation:'Example Lab', github:'@alice', contribution:['contributor'], orcid:'']] }"
        Records.scrubText("env.ARGS = '@/home/someone/args.txt'") == "env.ARGS = '@[redacted-location]'"
        Records.scrubText("mail = 'alice@example.org'") == "mail = '[redacted-user]@example.org'"

        cleanup:
        System.setProperty('user.name', saved)
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
        Records.scrubConfigText("workDir = '/x/work'\napiKey = 'k/x/key'") == "workDir = '[redacted-location]'\napiKey = '[secret]'"

        and: 'a location under a secret-named key is judged as a location'
        Records.scrubConfigText("apiKey = '/x/key'\nenv.PWD = '/home/x'") == "apiKey = '[redacted-location]'\nenv.PWD = '[redacted-location]'"
        Records.scrubConfigText(null) == null
    }

    def 'a RunManifest records its config text through scrubConfigText'() {
        when:
        final RunManifest manifest = new RunManifest(RecordsTest.manifestArgs() +
            [config: "env {\n    FOO_API_KEY = 'sk-live-123'\n}\nprocess.containerOptions = '--volume=/home/x:/data'\n"])

        then:
        manifest.config == "env {\n    FOO_API_KEY = '[secret]'\n}\nprocess.containerOptions = '--volume=[redacted-location]:[redacted-location]'\n"
    }

    def 'a credential-shaped value is redacted whatever key it sits under'() {
        given:
        final String akia = 'AKIAIOSFODNN7EXAMPLE'
        final String ghp = 'ghp_' + 'a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5P6q7R8'
        final String pat = 'github_pat_' + '11ABCDEFG0abcdefghij_klmnopqrstuvwxyz'
        final String tower = 'eyJ0aWQiOjEyMzR9LmFiY2RlZmdoaWprbG1ub3A'
        final String pem = '-----BEGIN RSA PRIVATE KEY-----' + '\nMIIEowIBAAKCAQEA/abc\n-----END RSA PRIVATE KEY-----'

        expect: 'params values'
        Records.scrub([a: akia, b: 'ASIAIOSFODNN7EXAMPLE', c: ghp, d: pat, e: tower, f: pem]) ==
            [a: '[secret]', b: '[secret]', c: '[secret]', d: '[secret]', e: '[secret]', f: '[secret]']

        and: 'config text, including a key block that spans lines'
        Records.scrubConfigText("env.NOTE = '${akia}'\nenv.OTHER = \"${ghp}\"\nenv.T = '${tower}'\ncert = \'\'\'${pem}\'\'\'\nprocess.cpus = 2") ==
            "env.NOTE = '[secret]'\nenv.OTHER = \"[secret]\"\nenv.T = '[secret]'\ncert = \'\'\'[secret]\'\'\'\nprocess.cpus = 2"

        and: 'idempotent'
        Records.scrubText(Records.scrubText("x ${akia}")) == 'x [secret]'
    }

    def 'a params or Map-config value under a secret-named key is redacted'() {
        expect:
        Records.scrub([github_token: 'abc', db_password: 'hunter2', nested: [client_secret: 's', ok: 'fine']]) ==
            [github_token: '[secret]', db_password: '[secret]', nested: [client_secret: '[secret]', ok: 'fine']]

        and: 'a plain boolean or number, as a value or as text, is no secret'
        Records.scrub([apiKey: 123, token: 0.05, filter_pass: true, db_pass: 'false', password: '-1', key: '1e-5']) ==
            [apiKey: 123, token: 0.05, filter_pass: true, db_pass: 'false', password: '-1', key: '1e-5']

        and: 'an empty value, a boolean and a container are left to their own contents'
        Records.scrub([token: '', skip_keys: true, credentials: [user: 'u', path: '/x']]) ==
            [token: '', skip_keys: true, credentials: [user: 'u', path: '[redacted-location]']]

        and: 'idempotent'
        Records.scrub(Records.scrub([db_password: 'hunter2'])) == [db_password: '[secret]']
    }

    def 'a list under a secret-named key has each element redacted as a scalar would be; a map in it keeps its own rule'() {
        expect:
        Records.scrub([tokens: ['abc', '', 7, true, null, [inner: 'x', api_key: 'k'], ['deeper']], keys: ['k1'] as String[], ok: ['plain']]) ==
            [tokens: ['[secret]', '', 7, true, null, [inner: 'x', api_key: '[secret]'], ['[secret]']], keys: ['[secret]'], ok: ['plain']]

        and: 'idempotent'
        Records.scrub(Records.scrub([tokens: ['abc', [inner: '/x']]])) == [tokens: ['[secret]', [inner: '[redacted-location]']]]
    }

    def 'a path after a comma, a semicolon or an @, or glued to a short flag, is redacted, and the user name inside a token too'() {
        given:
        final String user = System.getProperty('user.name')

        expect: 'params strings'
        Records.scrub([a: 'a.csv,/home/someone/b.csv', b: "--bind a,/scratch/${user}/x".toString(), c: '-B/home/someone/data',
                       d: '@/home/someone/args.txt', e: "~${user}/data".toString(), f: '-I/usr/include -L/opt/lib', g: 'x;/tmp/y',
                       h: "${user}@example.org".toString(), i: "../home/${user}/x".toString()]) ==
            // F2: `~<user>/data` is a home path and goes whole (was `~[redacted-user]/data`)
            [a: 'a.csv,[redacted-location]', b: '--bind a,[redacted-location]', c: '-B[redacted-location]',
             d: '@[redacted-location]', e: '[redacted-location]', f: '-I[redacted-location] -L[redacted-location]', g: 'x;[redacted-location]',
             h: '[redacted-user]@example.org', i: '../home/[redacted-user]/x']

        and: 'config text, where a list under a secret-named key has each element redacted'
        Records.scrubConfigText("params.passwords = ['hunter2', \"x\"]\nprocess.secrets = [ 'a',\n  'b' ]\nparams.inputs = ['a', 'b']") ==
            "params.passwords = ['[secret]', \"[secret]\"]\nprocess.secrets = [ '[secret]',\n  '[secret]' ]\nparams.inputs = ['a', 'b']"

        and: 'words that only contain the user name, and relative paths, keep their shape'
        Records.scrubText("a,b c;d -x x@y ${user}x x${user}".toString()) == "a,b c;d -x x@y ${user}x x${user}".toString()

        and: 'idempotent'
        final String once = Records.scrubConfigText("params.passwords = ['hunter2']\nx = '-B/a,/b;@/c ~${user}/d'".toString())
        Records.scrubConfigText(once) == once
    }

    def 'a params string is scrubbed token by token, keeping its shape'() {
        given:
        final String user = System.getProperty('user.name')

        expect:
        Records.scrub([a: '--ref /data/g.fa', b: "-v /home/${user}:/data".toString(), c: 'plain words', d: 'cas://x --out /y'])  ==
            [a: '--ref [redacted-location]', b: '-v [redacted-location]:[redacted-location]', c: 'plain words', d: 'cas://x --out [redacted-location]']
        Records.scrub([a: '/data/with space/x']) == [a: '[redacted-location]']
    }

    def 'a map key that is a location is redacted, numbered so no entry is lost'() {
        expect:
        Records.scrub(['/x/a.fq': 1]) == ['[redacted-location]': 1]
        Records.scrub([files: ['/a': 1, '[redacted-location]': 0, '~/b': 2, 's3://bucket/c': 3, 'rel/d': 4]]) ==
            [files: ['[redacted-location]-2': 1, '[redacted-location]': 0, '[redacted-location]-3': 2, '[redacted-location]-4': 3, 'rel/d': 4]]

        and: 'the value under a redacted key is still scrubbed by its own key and contents'
        Records.scrub([m: ['C:\\x': '/y', '\\\\host\\share': [token: 'abc']]]) ==
            [m: ['[redacted-location]': '[redacted-location]', '[redacted-location]-2': [token: '[secret]']]]

        and: 'idempotent'
        Records.scrub(Records.scrub([files: ['/a': 1, '/b': 2]])) == [files: ['[redacted-location]': 1, '[redacted-location]-2': 2]]
    }

    def 'a Windows, UNC or home-relative path is redacted, in params strings and config text'() {
        expect:
        Records.scrub([a: 'C:\\Program Files\\x', b: '\\\\host\\share\\x', c: '~/x', d: '~someone/x', e: '--in a.csv,C:\\y --ref ~/x']) ==
            [a: '[redacted-location]', b: '[redacted-location]', c: '[redacted-location]', d: '[redacted-location]', e: '--in a.csv,[redacted-location] --ref [redacted-location]']
        Records.scrubConfigText("a = 'C:\\\\x\\\\y'\nb = '\\\\\\\\host\\\\share'\nc = \"~/x:~bob/y\"") ==
            "a = '[redacted-location]'\nb = '[redacted-location]'\nc = \"[redacted-location]:[redacted-location]\""

        and: 'a tilde that does not start a home path, and a regex escape, stay'
        Records.scrub([a: '~1.2', b: 'a~b/c', c: '\\d+', d: '-1/3', e: 'x:1/3']) == [a: '~1.2', b: 'a~b/c', c: '\\d+', d: '-1/3', e: 'x:1/3']
    }

    def 'a secret-named flag loses its value, after = or in the next token'() {
        expect:
        Records.scrub([a: '--api-key=abc --password hunter2 --threads 4', b: '--auth=tok --no-token --verbose', c: '--token $TOKEN',
                       d: '--apiKey abc --github-pat abc --client-secret abc --db.passwd abc']) ==
            [a: '--api-key=[secret] --password [secret] --threads 4', b: '--auth=[secret] --no-token --verbose', c: '--token $TOKEN',
             d: '--apiKey [secret] --github-pat [secret] --client-secret [secret] --db.passwd [secret]']
        // P4 L1: `--secret-file` names where a secret is kept, so its value is judged as any other: a path goes as a location.
        Records.scrubConfigText("process.ext.args = '--secret-file=/x --keep 3'") == "process.ext.args = '--secret-file=[redacted-location] --keep 3'"

        and: 'the next token is taken only when the last segment of the name is a secret word; auth unless a negation precedes it'
        Records.scrub([a: '--skip-auth x.bam --no-auth x --disable-auth y --sort-keys in.json --keyfile r.key --token-col 3 --max-tokens 4096',
                       b: '--auth tok --basic-auth u:p --proxy-auth tok --http-auth tok']) ==
            [a: '--skip-auth x.bam --no-auth x --disable-auth y --sort-keys in.json --keyfile r.key --token-col 3 --max-tokens 4096',
             b: '--auth [secret] --basic-auth [secret] --proxy-auth [secret] --http-auth [secret]']

        and: 'a quoted value, with its quotes kept'
        Records.scrub([a: '--password \'hunter2\' --token "abc" --api-key=\'x y\'']) == [a: '--password \'[secret]\' --token "[secret]" --api-key=\'[secret]\'']

        and: 'a secret flag takes `Bearer <token>` or `Basic <value>` whole; after a non-secret flag, their own rules judge'
        Records.scrub([a: '--token Bearer abcdefghij0123456789', b: '--header Basic dXNlcjpwYXNz', c: '--auth Basic Statistics',
                       d: '--auth Basic dTpwYXNz', e: "--auth 'Basic dXNlcjpwYXNzd29yZA'", f: '--header Basic Statistics']) ==
            [a: '--token [secret]', b: '--header Basic [secret]', c: '--auth [secret]',
             d: '--auth [secret]', e: "--auth '[secret]'", f: '--header Basic Statistics']

        and: 'a secret flag inside a non-secret flag\'s quoted value is still found'
        Records.scrub([a: '--extra "foo --password hunter2"', b: "--extra 'run --token abc123'", c: '--extra="foo --password hunter2"',
                       d: '--wrapper "java -jar x.jar --password hunter2"', e: '--extra "foo --threads 4"']) ==
            [a: '--extra "foo --password [secret]"', b: "--extra 'run --token [secret]'", c: '--extra="foo --password [secret]"',
             d: '--wrapper "java -jar x.jar --password [secret]"', e: '--extra "foo --threads 4"']

        and: 'a -D property with a secret name, in a params string or a quoted config value; others stay'
        Records.scrub([a: '-Dfoo.password=abc -Xmx2g', b: '-Dfile.encoding=UTF-8', c: '-Dapi.token="a b" -Dx=1',
                       d: '-Dfoo.password=abc,-Dbar.token=def', e: '-Dfoo.password=abc,-Dfile.encoding=UTF-8']) ==
            [a: '-Dfoo.password=[secret] -Xmx2g', b: '-Dfile.encoding=UTF-8', c: '-Dapi.token="[secret]" -Dx=1',
             d: '-Dfoo.password=[secret],-Dbar.token=[secret]', e: '-Dfoo.password=[secret],-Dfile.encoding=UTF-8']
        Records.scrubConfigText("process.ext.args = '-Dfoo.password=abc'\nenv.JAVA_OPTS = '-Dapi.token=abc -Xmx2g -Dfile.encoding=UTF-8'") ==
            "process.ext.args = '-Dfoo.password=[secret]'\nenv.JAVA_OPTS = '-Dapi.token=[secret] -Xmx2g -Dfile.encoding=UTF-8'"

        and: 'a single-dash system property in config text is an assignment'
        Records.scrubConfigText("params.java = '''\n  -Dfoo.password=abc\n'''") == "params.java = '''\n  -Dfoo.password='[secret]'\n'''"
        Records.scrubConfigText('process.ext.args = """\n  -Dapi.token=abc --x 1\n"""') == 'process.ext.args = """\n  -Dapi.token=\'[secret]\' --x 1\n"""'
    }

    def 'Bearer, in any case, and Basic credentials are redacted; prose is not'() {
        expect:
        Records.scrub([a: 'Authorization: Basic dXNlcjpwYXNz', b: 'bearer abcdefghij0123456789', c: 'BEARER eyJhbGciOi.eyJzdWIi.c2ln',
                       d: 'Basic Statistics', e: 'the bearer instruments', f: 'basic dXNlcjpwYXNz',
                       g: 'Basic QualityControl', h: 'Basic MultiQC1', i: 'Basic dXNlcjpwYXNzd29yZA==', j: 'Basic DataAnalysis',
                       k: 'Basic dXNlcjpwYXNzd29yZA', l: 'Basic YWRtaW46YWRtaW4', m: 'Basic dTpwYXNz', n: 'Basic YWI6Y2Rl', o: 'Basic cm9vdDp4',
                       p: 'Basic SNPsIndelsQC', q: 'Basic FastQCReport', r: 'Basic StatisticsReport']) ==
            [a: 'Authorization: Basic [secret]', b: 'bearer [secret]', c: 'BEARER [secret]',
             d: 'Basic Statistics', e: 'the bearer instruments', f: 'basic [secret]',
             g: 'Basic QualityControl', h: 'Basic MultiQC1', i: 'Basic [secret]', j: 'Basic DataAnalysis',
             k: 'Basic [secret]', l: 'Basic [secret]', m: 'Basic [secret]', n: 'Basic [secret]', o: 'Basic [secret]',
             p: 'Basic SNPsIndelsQC', q: 'Basic FastQCReport', r: 'Basic StatisticsReport']
    }

    def 'a -D property or a bare config value with a secret name goes whole, commas and all, up to another -D property'() {
        expect:
        Records.scrub([a: '-Dfoo.password=hunter,2 -Xmx2g', b: '-Dfoo.password=,abc', c: '-Djavax.net.ssl.keyStorePassword=p@ss,w0rd',
                       d: '-Dfoo.password=abc,-Dbar.token=def', e: '-Dfoo.password=abc,-Dfile.encoding=UTF-8', f: '-Dfile.encoding=UTF-8,x']) ==
            [a: '-Dfoo.password=[secret] -Xmx2g', b: '-Dfoo.password=[secret]', c: '-Djavax.net.ssl.keyStorePassword=[secret]',
             d: '-Dfoo.password=[secret],-Dbar.token=[secret]', e: '-Dfoo.password=[secret],-Dfile.encoding=UTF-8', f: '-Dfile.encoding=UTF-8,x']
        Records.scrubConfigText("env.X = '-Dfoo.password=hunter,2 -Xmx2g'") == "env.X = '-Dfoo.password=[secret] -Xmx2g'"

        and: 'in a multi-line string, where the assignment rule takes a bare value'
        Records.scrubConfigText("params.java = '''\n  -Dfoo.password=abc,def\n  -Dapi.token=abc,-Dbar.secret=def,-Dfile.encoding=UTF-8\n'''") ==
            "params.java = '''\n  -Dfoo.password='[secret]'\n  -Dapi.token='[secret]',-Dbar.secret='[secret]',-Dfile.encoding=UTF-8\n'''"
        Records.scrubConfigText('process.ext.args = """\n  password=hunter,2 --x 1\n"""') == 'process.ext.args = """\n  password=\'[secret]\' --x 1\n"""'

        and: 'a secret after a non-secret bare value and a comma is still found; a comma before a space or a bracket ends a value'
        Records.scrubConfigText("params.x = '''\n  a=1,password=xyz\n  b=1;token=xyz\n'''") == "params.x = '''\n  a=1,password='[secret]'\n  b=1;token='[secret]'\n'''"
        Records.scrubConfigText("params.m = [token: abc, other: 1]\nparams.n = [token: abc,]") == "params.m = [token: '[secret]', other: 1]\nparams.n = [token: '[secret]',]"
    }

    def 'a secret assignment after a comma and punctuation is not taken into the bare value before it'() {
        expect:
        Records.scrubConfigText('x = a,(token=abc)') == "x = a,(token='[secret]')"
        Records.scrubConfigText("params.x = '''\n  a=1,(token=abc)\n'''") == "params.x = '''\n  a=1,(token='[secret]')\n'''"
        Records.scrubConfigText('opts=x,{token=abc}') == "opts=x,{token='[secret]'}"
        Records.scrubConfigText('opts=a,+token=abc') == "opts=a,+token='[secret]'"
        Records.scrubConfigText('x = a,$token=abc') == "x = a,\$token='[secret]'"

        and: 'a secret bare value keeps its commas and semicolons, and a comma before another key still splits'
        ['hunter,2', ',abc', 'p@ss,w0rd', 'abc,def', 'a;b'].every { String v ->
            Records.scrubConfigText("params.java = '''\n  password=${v}\n'''") == "params.java = '''\n  password='[secret]'\n'''"
        }
        Records.scrubConfigText("params.x = '''\n  a=1,password=x\n'''") == "params.x = '''\n  a=1,password='[secret]'\n'''"
        Records.scrubConfigText('params.m = [token:x,b:2]') == "params.m = [token:'[secret]',b:2]"
    }

    def 'a -D property in a multi-line string takes the -D value form: to a space or a quote, stopping only before another -D'() {
        expect:
        ['hunter,2', ',abc', 'p@ss,w0rd', 'abc,def', 'a;b', 'p,a:ss', 'abc,)def', 'abc,user=bob'].every { String v ->
            Records.scrubConfigText("params.java = '''\n  -Dfoo.password=${v}\n'''") == "params.java = '''\n  -Dfoo.password='[secret]'\n'''"
        }
        Records.scrubConfigText("params.java = '''\n  -Dfoo.password=abc,\"def\n'''") == "params.java = '''\n  -Dfoo.password='[secret]'\"def\n'''"

        and: 'as in params and a single-quoted config value'
        Records.scrub([a: '-Dfoo.password=p,a:ss', b: '-Dfoo.password=abc,)def', c: '-Dfoo.password=abc,user=bob', d: '-Dfoo.password=abc,"def']) ==
            [a: '-Dfoo.password=[secret]', b: '-Dfoo.password=[secret]', c: '-Dfoo.password=[secret]', d: '-Dfoo.password=[secret]"def']
        Records.scrubConfigText("env.X = '-Dfoo.password=p,a:ss -Dfoo.password=abc,)def -Dfoo.password=abc,user=bob'") ==
            "env.X = '-Dfoo.password=[secret] -Dfoo.password=[secret] -Dfoo.password=[secret]'"

        and: 'another -D property still ends the value, and a quoted value keeps its quotes'
        Records.scrubConfigText("params.java = '''\n  -Dapi.token=abc,-Dbar.secret=def,-Dfile.encoding=UTF-8 -Dx.password='a b'\n'''") ==
            "params.java = '''\n  -Dapi.token='[secret]',-Dbar.secret='[secret]',-Dfile.encoding=UTF-8 -Dx.password='[secret]'\n'''"
    }

    def 'a bare name=value with a secret-named name is a secret in a params string and a quoted config value'() {
        expect:
        Records.scrub([a: 'user=bob password=hunter2', b: '--db "host=h dbname=d password=secret123"', c: '-Dx=a,password=hunter2',
                       d: 'token=abc', e: '-Ddb.props=user=admin,password=hunter2']) ==
            [a: 'user=bob password=[secret]', b: '--db "host=h dbname=d password=[secret]"', c: '-Dx=a,password=[secret]',
             d: 'token=[secret]', e: '-Ddb.props=user=admin,password=[secret]']
        Records.scrubConfigText("env.X = 'token=abc'\nenv.Y = '-Ddb.props=user=admin,password=hunter2'\nparams.a = 'user=bob password=hunter2'\n" +
                                "params.b = '--db \"host=h dbname=d password=secret123\"'\nparams.c = '-Dx=a,password=hunter2'") ==
            "env.X = 'token=[secret]'\nenv.Y = '-Ddb.props=user=admin,password=[secret]'\nparams.a = 'user=bob password=[secret]'\n" +
            "params.b = '--db \"host=h dbname=d password=[secret]\"'\nparams.c = '-Dx=a,password=[secret]'"
    }

    def 'a bare name=value whose name does not end in a secret word stays'(String kept) {
        expect:
        Records.scrub([a: kept]).a == kept
        Records.scrubConfigText("process.ext.args = '${kept}'") == "process.ext.args = '${kept}'"

        where:
        kept << ['--trimmomatic-args SLIDINGWINDOW:4:15', '-q 20 --nextseq-trim=20', 'ktrim=r k=23 mink=11 hdist=1 ref=adapters',
                 'SORT_ORDER=coordinate', 'max_tokens=4096', 'key_column=sample tokenizer=bert keyfile=reads.key', 'skip_auth=true',
                 'monkey=1', '-O bam,level=5', 'x=1,y=2;z=3']
    }

    def 'a secret assignment inside a non-secret -D value is still found; a secret -D value goes whole'() {
        expect:
        ['-Da=1,password=x'                       : "-Da=1,password='[secret]'",
         '-Dx=a,token=abc'                        : "-Dx=a,token='[secret]'",
         '-Ddb.props=user=admin,password=hunter2' : "-Ddb.props=user=admin,password='[secret]'",
         '-Dconn=user=bob;password=hunter2'       : "-Dconn=user=bob;password='[secret]'",
         '-Dx=a,api_key: abc'                     : "-Dx=a,api_key: '[secret]'",
         "-Dx=a,token='abc'"                      : "-Dx=a,token='[secret]'",
         '-Dfoo.password=abc,user=bob'            : "-Dfoo.password='[secret]'",
         '-Dfile.encoding=UTF-8,x'                : '-Dfile.encoding=UTF-8,x',
        ].every { String raw, String scrubbed ->
            Records.scrubConfigText("params.java = '''\n  ${raw}\n'''") == "params.java = '''\n  ${scrubbed}\n'''"
        }
    }

    def 'a secret named argument in a call is not taken into the bare value before it'() {
        expect:
        Records.scrubConfigText("ext.args = { def o = foo(token: 'abc'); o }") == "ext.args = { def o = foo(token: '[secret]'); o }"
        Records.scrubConfigText("ext.args = {f(token:'abc')}") == "ext.args = {f(token:'[secret]')}"
        Records.scrubConfigText('x = f(token=abc)') == "x = f(token='[secret]')"
        Records.scrubConfigText("x = foo(token: 'abc')") == "x = foo(token: '[secret]')"
        Records.scrubConfigText("x = f({token: 'abc'})") == "x = f({token: '[secret]'})"

        and: 'so is one right after the separator, unless the key is a URI scheme'
        Records.scrubConfigText('x = (token=abc)') == "x = (token='[secret]')"
        Records.scrubConfigText('x = ((token=abc))') == "x = ((token='[secret]'))"
        Records.scrubConfigText('a: {password: abc}') == "a: {password: '[secret]'}"
        Records.scrubConfigText('x = Overview(Basic :(b--password=;') == "x = Overview(Basic :(b--password='[secret]'"

        and: 'a secret value that opens with a parenthesis is still taken'
        Records.scrubConfigText('password = (a:b)') == "password = '[secret]')"
        Records.scrubConfigText('token=(-Dx=1') == "token='[secret]'"
        Records.scrubConfigText('token= (?sig=abc') == "token= '[secret]'"
        Records.scrubConfigText('key = (dTpwYXNzba_C:x') == "key = '[secret]'"

        and: 'a parenthesis or brace in a secret value, or in a call with no key, stays part of the value'
        ['p(a)ss', 'ab{cd', 'f(x)', 'hunter,2', 'a;b'].every { String v ->
            Records.scrubConfigText("params.java = '''\n  password=${v}\n'''") == "params.java = '''\n  password='[secret]'\n'''"
        }
        Records.scrubConfigText('x.token=(http://u:p@h/x)') == "x.token='[secret]'"
        Records.scrubConfigText('x.token=(http://u:p@h/x') == "x.token='[secret]'"
        Records.scrubConfigText('x = ${foo}') == 'x = ${foo}'
        Records.scrubConfigText('x = f(a)') == 'x = f(a)'
    }

    def 'a map key the scrub would change is renamed with a marker that fits what it held; ordinary keys stay'() {
        given:
        final String user = System.getProperty('user.name')
        final Map files = [('/home/x/a.fq'): 1, (user): 2, '--in=/home/x/y': 3, 'a /home/x/b': 4, '~/c.fq': 5,
                           ('ghp_' + 'abcdefghijklmnopqrstuv'): 6, 's3://b/k': 7, sample: 8, 'rel.fq': 9, 7: 10, '[redacted-location]-3': 11]

        when:
        final Map scrubbed = (Map) Records.scrub([files: files]).files

        then: 'a credential is [secret] (its value is a number, which stays); a path [redacted-location]; the user name [redacted-user]; numbered past kept keys'
        scrubbed == ['[redacted-location]': 1, '[redacted-user]': 2, '[redacted-location]-2': 3, '[redacted-location]-4': 4,
                     '[redacted-location]-5': 5, '[secret]': 6, '[redacted-location]-6': 7, sample: 8, 'rel.fq': 9, 7: 10,
                     '[redacted-location]-3': 11]
        scrubbed.keySet().toList() == ['[redacted-location]', '[redacted-user]', '[redacted-location]-2', '[redacted-location]-4',
                                       '[redacted-location]-5', '[secret]', '[redacted-location]-6', 'sample', 'rel.fq', 7, '[redacted-location]-3']
        and: 'idempotent'
        Records.scrub([files: scrubbed]).files == scrubbed
        and: 'a Map config is named the same way'
        Records.scrub([process: [(user): 'x', ('/data/k'): 'y']]).process == ['[redacted-user]': 'x', '[redacted-location]': 'y']
    }

    def 'a secret flag whose value is a bare Bearer or Basic leaves the next flag alone; a quoted token after it goes'() {
        expect:
        Records.scrub([a: '--proxy-auth basic --threads 4', b: '--auth Bearer --token-file x.txt', c: '--auth basic $TOKEN',
                       d: '--auth basic -t 4', e: '--auth Basic', f: '--auth Basic dTpwYXNz --threads 4', g: '--password Basicfoo --x 1']) ==
            [a: '--proxy-auth [secret] --threads 4', b: '--auth [secret] --token-file x.txt', c: '--auth [secret] $TOKEN',
             d: '--auth [secret] -t 4', e: '--auth [secret]', f: '--auth [secret] --threads 4', g: '--password [secret] --x 1']
        Records.scrubConfigText("process.ext.args = '--proxy-auth basic --threads 4'") == "process.ext.args = '--proxy-auth [secret] --threads 4'"

        and: 'Bearer or Basic then a quoted token is one value'
        Records.scrub([a: '--auth Basic "dTpwYXNz"', b: '--token Bearer "abcdefghij0123456789" --threads 4', c: "--token Bearer 'abcdefghij0123456789'"]) ==
            [a: '--auth [secret]', b: '--token [secret] --threads 4', c: '--token [secret]']
    }

    def 'Basic counts only when its value decodes as strict UTF-8 to a user, a colon and a password'() {
        given: 'capitalised O-words, which base64 decodes to a byte of 0x80 or more and then a colon'
        final List<String> words = ['Overview', 'Options', 'Output', 'Online', 'Only', 'Origin', 'Overall', 'Ontology', 'Outline', 'Overlap']
        final String utf8 = Base64.encoder.encodeToString('jörg:pässwörd'.getBytes('UTF-8'))

        expect:
        words.every { !Leaks.isUserPass(it) }
        Records.scrub([v: words.collect { "Basic ${it}" }.join(', ')]).v == words.collect { "Basic ${it}" }.join(', ')

        and: 'an empty user is no user-pass; a UTF-8 user and password are'
        !Leaks.isUserPass(Base64.encoder.encodeToString(':pass'.bytes))
        Leaks.isUserPass(utf8)
        Records.scrub([a: "Basic ${utf8}".toString(), b: 'Basic dTpwYXNz', c: 'Basic YWRtaW46YWRtaW4']) ==
            [a: 'Basic [secret]', b: 'Basic [secret]', c: 'Basic [secret]']
    }

    def 'a UNC path needs a host and a share of two characters or more; a regex escape stays'() {
        expect:
        Records.scrubConfigText("a = '\\\\\\\\host\\\\share'\nb = '\\\\d\\\\d'\nc = '\\\\bword\\\\b'\nd = '\\\\w\\\\s'") ==
            "a = '[redacted-location]'\nb = '\\\\d\\\\d'\nc = '\\\\bword\\\\b'\nd = '\\\\w\\\\s'"
        Records.scrub([a: '\\\\host\\share', b: '\\\\h\\share', c: '\\\\host\\s']) == [a: '[redacted-location]', b: '\\\\h\\share', c: '\\\\host\\s']
    }

    def 'auth is a secret key segment, and author is not'() {
        expect:
        Records.isSecretKey('env.API_AUTH')
        Records.isSecretKey('basicAuth')
        Records.isSecretKey('auth')
        !Records.isSecretKey('author')
        !Records.isSecretKey('manifest.authors')
        !Records.isSecretKey('oauthor')
    }

    // ---- P4 L1: pass, pwd and passphrase; header and colon forms; three more token shapes

    /** The params and config text a RunManifest records, after a DAG-CBOR round trip. */
    private static Map recorded(Map params, String config) {
        final RunManifest manifest = new RunManifest(RecordsTest.manifestArgs() + [params: params, config: config])
        return (Map) DagCbor.decode(DagCbor.encode(manifest.toCbor()))
    }

    def 'a --passphrase, --pass or --pwd flag loses its value, after = or in the next token; a word that only contains pass keeps its'() {
        when:
        final Map block = recorded([
            a: '--passphrase hunter2 --threads 4', b: '--passphrase=hunter2', c: '--pass hunter2', d: '--pass=hunter2',
            e: '--pwd hunter2', f: '--pwd=hunter2', g: "--db-pass 'hunter 2'", h: '--mysql-pwd="x y"', i: '--ssh-passphrase abc',
            fp1: '--bypass x.bam', fp2: '--low-pass 0.5 --high-pass 30', fp3: '--compass n', fp4: '--passage 3',
            fp5: '--two-pass --passes 2', fp6: '--password-file pw.txt',
        ], "process.ext.args = '--pass hunter2 --two-pass'\nprocess.ext.args = '--pwd=hunter2 --bypass x'")

        then:
        block.params == [
            a: '--passphrase [secret] --threads 4', b: '--passphrase=[secret]', c: '--pass [secret]', d: '--pass=[secret]',
            e: '--pwd [secret]', f: '--pwd=[secret]', g: "--db-pass '[secret]'", h: '--mysql-pwd="[secret]"', i: '--ssh-passphrase [secret]',
            fp1: '--bypass x.bam', fp2: '--low-pass 0.5 --high-pass 30', fp3: '--compass n', fp4: '--passage 3',
            fp5: '--two-pass --passes 2', fp6: '--password-file pw.txt',
        ]
        block.config == "process.ext.args = '--pass [secret] --two-pass'\nprocess.ext.args = '--pwd=[secret] --bypass x'"
    }

    def 'an env name ending in PASS or PWD names a secret, in config text and in a params map'() {
        when:
        final Map block = recorded([env: [DB_PASS: 'hunter2', MYSQL_PWD: 'hunter2', BYPASS_QC: 'yes', COMPASS: 'n']],
            "env.DB_PASS = 'hunter2'\nenv.MYSQL_PWD = 'hunter2'\nenv { PGPASSWORD = 'x'\n  REDIS_PASS = 'y' }\nenv.BYPASS_QC = 'yes'")

        then:
        block.params == [env: [DB_PASS: '[secret]', MYSQL_PWD: '[secret]', BYPASS_QC: 'yes', COMPASS: 'n']]
        block.config == "env.DB_PASS = '[secret]'\nenv.MYSQL_PWD = '[secret]'\nenv { PGPASSWORD = '[secret]'\n  REDIS_PASS = '[secret]' }\nenv.BYPASS_QC = 'yes'"
    }

    def 'a header inside a params string loses its value, and keeps its name and scheme'() {
        when:
        final Map block = recorded([
            a: 'Authorization: token abc', b: 'Authorization: Bearer abc', c: "-H 'x-api-key: abc'", d: '-H "X-Auth-Token: abc"',
            e: "--header 'Authorization: token ghx123456abc'", f: 'Authorization: Bearer abcDEF123456', g: "curl -H 'PRIVATE-TOKEN: glx' -s",
            h: '-H "Proxy-Authorization: Basic Statistics"',
        ], "process.ext.args = \"-H 'x-api-key: abc' --retry 3\"\nenv.H = 'Authorization: token abc'")

        then:
        block.params == [
            a: 'Authorization: token [secret]', b: 'Authorization: Bearer [secret]', c: "-H 'x-api-key: [secret]'",
            d: '-H "X-Auth-Token: [secret]"', e: "--header 'Authorization: token [secret]'", f: 'Authorization: Bearer [secret]',
            g: "curl -H 'PRIVATE-TOKEN: [secret]' -s", h: '-H "Proxy-Authorization: Basic [secret]"',
        ]
        block.config == "process.ext.args = \"-H 'x-api-key: [secret]' --retry 3\"\nenv.H = 'Authorization: token [secret]'"
    }

    def 'a secret-named name, a colon and a value inside a params string is a secret; other colons stay'() {
        when:
        final Map block = recorded([
            a: 'password: hunter2', b: 'user: bob, password: hunter2, db: x', c: 'password:hunter2', d: '-pass pass:hunter2 -in x',
            e: 'api_key: abc123', f: 'auth: basic', g: 'token=abc&x=1', h: 'password: abc&x=1',
            fp1: 'SLIDINGWINDOW:4:15 LEADING:3', fp2: 'note: see docs', fp3: 'sort_key: asc', fp4: 'max_tokens: 4096',
            fp5: 'password_file: pw.txt', fp6: '-pass env:DB_PASSWORD', fp7: 'monkey: 1', fp8: 'time 12:30, ratio 1:2:3',
        ], "params.x = 'password: hunter2'\nparams.y = [password: 'hunter2']")

        then:
        block.params == [
            a: 'password: [secret]', b: 'user: bob, password: [secret], db: x', c: 'password:[secret]', d: '-pass pass:[secret] -in x',
            e: 'api_key: [secret]', f: 'auth: [secret]', g: 'token=[secret]&x=1', h: 'password: [secret]&x=1',
            fp1: 'SLIDINGWINDOW:4:15 LEADING:3', fp2: 'note: see docs', fp3: 'sort_key: asc', fp4: 'max_tokens: 4096',
            fp5: 'password_file: pw.txt', fp6: '-pass env:DB_PASSWORD', fp7: 'monkey: 1', fp8: 'time 12:30, ratio 1:2:3',
        ]
        block.config == "params.x = 'password: [secret]'\nparams.y = [password: '[secret]']"
    }

    def 'a Google, OpenAI or Anthropic key is redacted under any key; a near miss stays'() {
        given:
        final String google = 'AIza' + 'SyA1234567890abcdefghijklmnopqrstuv'    // AIza and 35
        final String proj = 'sk-proj-' + 'abcdefghijklmnopqrstuvwxyz0123456789ABCD'
        final String legacy = 'sk-' + 'aB3dE5gH7jK9mN1pQ3sT5vW7yZ9bC1dF2gH4jK6mN8pQ0'
        final String ant = 'sk-ant-' + 'api03-abcdefghijklmnopqrstuvwxyz0123456789'

        when:
        final Map block = recorded([
            a: google, b: proj, c: legacy, d: ant, e: "--model x --key-file k.json ${google}".toString(), f: "OPENAI=${proj}".toString(),
            fp1: 'AIza' + 'SyA1234567890abcdefghijklmnopqrstu', fp2: 'sk-learn', fp3: 'sk-ant-short', fp4: 'task-0123456789abcdefghij0123456789abcdefghij',
            fp5: 'sk-' + 'abcdefghijklmnopqrstuvwxyzabcdefghij', fp6: 'risk-0123456789abcdefghij0123456789abcdefghij',
        ], "env.X = '${google}'\nenv.Y = \"${ant}\"\nenv.Z = '${legacy}'".toString())

        then:
        block.params == [
            a: '[secret]', b: '[secret]', c: '[secret]', d: '[secret]', e: '--model x --key-file k.json [secret]', f: 'OPENAI=[secret]',
            fp1: 'AIza' + 'SyA1234567890abcdefghijklmnopqrstu', fp2: 'sk-learn', fp3: 'sk-ant-short', fp4: 'task-0123456789abcdefghij0123456789abcdefghij',
            fp5: 'sk-' + 'abcdefghijklmnopqrstuvwxyzabcdefghij', fp6: 'risk-0123456789abcdefghij0123456789abcdefghij',
        ]
        block.config == "env.X = '[secret]'\nenv.Y = \"[secret]\"\nenv.Z = '[secret]'"
    }

    def 'a key holding key only inside a word, or a qualified key, keeps its value; a secret-named one loses it'() {
        when:
        final Map block = recorded([
            monkey_reads: 'sample1', keep_tokens: 'yes', keystone: 'k1', turkey: 'gobble', keyfile: 'r.key', sort_keys: 'name',
            password_file_name: 'pw_list.txt', secretome_db: 'db1',
            api_key: 'abc', apiKey: 'abc', 'ssh-key': 'abc', key: 'abc', apikey: 'abc', tokens: ['a', 'b'],
        ], "monkey_reads = 'sample1'\nkeep_tokens = 'yes'\nparams.keystone = 'k1'\napi_key = 'abc'\nsort_key = 'x'")

        then:
        block.params == [
            monkey_reads: 'sample1', keep_tokens: 'yes', keystone: 'k1', turkey: 'gobble', keyfile: 'r.key', sort_keys: 'name',
            password_file_name: 'pw_list.txt', secretome_db: 'db1',
            api_key: '[secret]', apiKey: '[secret]', 'ssh-key': '[secret]', key: '[secret]', apikey: '[secret]', tokens: ['[secret]', '[secret]'],
        ]
        block.config == "monkey_reads = 'sample1'\nkeep_tokens = 'yes'\nparams.keystone = 'k1'\napi_key = '[secret]'\nsort_key = 'x'"
    }

    // ---- R2: the integrator's third scrub probe (PGPASS, sshpass -p, Cookie; pass as a QC verdict, pat as a pattern)

    def 'a one-word name of a database or login tool and pass or pwd names a secret; a word that only ends in pass does not'() {
        when:
        final Map block = recorded([
            a: 'PGPASS=hunter2 psql', b: 'SSHPASS=hunter2 sshpass -e ssh h', c: 'MYSQLPWD=hunter2', d: '--dbpass hunter2',
            env: [PGPASS: 'hunter2', SSHPASS: 'hunter2', COMPASS: 'n', BYPASS: 'yes', OLDPWD: 'x'],
            fp1: 'COMPASS=n BYPASS=yes', fp2: '--overpass 3',
        ], "env.PGPASS = 'hunter2'\nenv { SSHPASS = 'hunter2'\n  ROOTPWD = 'x' }\nenv.COMPASS = 'n'")

        then:
        block.params == [
            a: 'PGPASS=[secret] psql', b: 'SSHPASS=[secret] sshpass -e ssh h', c: 'MYSQLPWD=[secret]', d: '--dbpass [secret]',
            env: [PGPASS: '[secret]', SSHPASS: '[secret]', COMPASS: 'n', BYPASS: 'yes', OLDPWD: 'x'],
            fp1: 'COMPASS=n BYPASS=yes', fp2: '--overpass 3',
        ]
        block.config == "env.PGPASS = '[secret]'\nenv { SSHPASS = '[secret]'\n  ROOTPWD = '[secret]' }\nenv.COMPASS = 'n'"
    }

    def 'sshpass -p loses its value, glued or not; a -p of any other tool is a thread count and stays'() {
        when:
        final Map block = recorded([
            a: 'sshpass -p hunter2 scp a b', b: 'sshpass -phunter2 ssh h', c: "sshpass -p 'hunter 2' ssh h", d: 'sshpass -v -p hunter2 ssh h',
            e: 'sshpass -p "x y" rsync a b',
            fp1: 'sshpass -f pw.txt ssh h', fp2: 'sshpass -e ssh h', fp3: 'sshpass -p $SSHPASS ssh h', fp4: 'bowtie2 -p 8 -x idx',
            fp5: 'salmon quant -p 8', fp6: 'mysql -u bob -p',
        ], "process.ext.args = 'sshpass -p hunter2 scp a b'\nprocess.ext.args = '-p 8 --very-sensitive'")

        then:
        block.params == [
            a: 'sshpass -p [secret] scp a b', b: 'sshpass -p[secret] ssh h', c: "sshpass -p '[secret]' ssh h", d: 'sshpass -v -p [secret] ssh h',
            e: 'sshpass -p "[secret]" rsync a b',
            fp1: 'sshpass -f pw.txt ssh h', fp2: 'sshpass -e ssh h', fp3: 'sshpass -p $SSHPASS ssh h', fp4: 'bowtie2 -p 8 -x idx',
            fp5: 'salmon quant -p 8', fp6: 'mysql -u bob -p',
        ]
        block.config == "process.ext.args = 'sshpass -p [secret] scp a b'\nprocess.ext.args = '-p 8 --very-sensitive'"
    }

    def 'a Cookie or Set-Cookie header loses its value, every pair of it; a cookie with no name=value stays'() {
        when:
        final Map block = recorded([
            a: "-H 'Cookie: session=abc123def456'", b: '-H "Cookie: session=abc; theme=dark" --retry 3', c: 'Set-Cookie: id=abc; Path=/; HttpOnly',
            d: 'cookie: sid=abc', e: "--header 'cookie:sid=abc'",
            fp1: 'cookie: chocolate chip', fp2: '--cookie-jar jar.txt', fp3: 'cookies=3',
        ], "process.ext.args = \"-H 'Cookie: session=abc' --retry 3\"")

        then:
        block.params == [
            a: "-H 'Cookie: [secret]'", b: '-H "Cookie: [secret]" --retry 3', c: 'Set-Cookie: [secret]',
            d: 'cookie: [secret]', e: "--header 'cookie:[secret]'",
            fp1: 'cookie: chocolate chip', fp2: '--cookie-jar jar.txt', fp3: 'cookies=3',
        ]
        block.config == "process.ext.args = \"-H 'Cookie: [secret]' --retry 3\""
    }

    def 'pass next to a data or filter word is a QC verdict and keeps its value; a pass of its own still goes'() {
        when:
        final Map block = recorded([
            pass_filter: 'true', vcf_pass_only: 'yes', fastq_pass: 'run1/fastq_pass', bam_pass: 'run1/bam_pass', fastq_fail: 'run1/fastq_fail',
            pod5_pass: 'run1/pod5_pass', a: '--fastq_pass run1/fastq_pass --pass-only', b: 'fastq_pass=run1/fastq_pass', c: '--pass-filter=PASS',
            db_pass: 'hunter2', pass: 'hunter2', d: '--pass hunter2', e: 'pass=hunter2',
        ], "params.fastq_pass = 'run1/fastq_pass'\nparams.vcf_pass_only = 'yes'\nparams.db_pass = 'hunter2'")

        then:
        block.params == [
            pass_filter: 'true', vcf_pass_only: 'yes', fastq_pass: 'run1/fastq_pass', bam_pass: 'run1/bam_pass', fastq_fail: 'run1/fastq_fail',
            pod5_pass: 'run1/pod5_pass', a: '--fastq_pass run1/fastq_pass --pass-only', b: 'fastq_pass=run1/fastq_pass', c: '--pass-filter=PASS',
            db_pass: '[secret]', pass: '[secret]', d: '--pass [secret]', e: 'pass=[secret]',
        ]
        block.config == "params.fastq_pass = 'run1/fastq_pass'\nparams.vcf_pass_only = 'yes'\nparams.db_pass = '[secret]'"
    }

    def 'a flag or name=value named pat alone whose value is a nucleotide word is a pattern and stays; any other value or name (GITHUB_PAT), or a token shape, goes'() {
        given:
        final String ghp = 'ghp_' + 'abcdefghijklmnopqrstuvwxyz0123456789'

        when:
        final Map block = recorded([
            fp1: '--pat GATC', fp2: '--pat=ACGTN', fp3: "--pat 'gatc'", fp4: 'pat=GATC', fp5: 'pat: AGATCGGAAGAGC', fp6: 'x.pat=GATC',
            a: '--pat abc123', b: "--pat ${ghp}".toString(), c: '--pat=xyz', d: 'GITHUB_PAT=abcdef', e: "--pat 'GATC 1'", f: '--pat abc',
            g: '--pat GaTc', h: 'GITHUB_PAT=GATTACA', i: '--adapter-pat RYKM',
        ], "process.ext.args = '--pat GATC'\nprocess.ext.args = '--pat abc123'")

        then:
        block.params == [
            fp1: '--pat GATC', fp2: '--pat=ACGTN', fp3: "--pat 'gatc'", fp4: 'pat=GATC', fp5: 'pat: AGATCGGAAGAGC', fp6: 'x.pat=GATC',
            a: '--pat [secret]', b: '--pat [secret]', c: '--pat=[secret]', d: 'GITHUB_PAT=[secret]', e: "--pat '[secret]'", f: '--pat [secret]',
            g: '--pat [secret]', h: 'GITHUB_PAT=[secret]', i: '--adapter-pat [secret]',
        ]
        block.config == "process.ext.args = '--pat GATC'\nprocess.ext.args = '--pat [secret]'"
    }

    def 'a numeric password is a secret: secrets beat false positives (#text)'() {
        expect:
        !Records.scrub([extra: text]).toString().contains('12345678')
        Records.scrub([password: 12345678]).password == Records.REDACTED_SECRET
        Records.scrub([apiKey: '12345']).apiKey == Records.REDACTED_SECRET
        Records.scrub([threads: 8, filter_pass: 'true', pass_threshold: 0.05]) == [threads: 8, filter_pass: 'true', pass_threshold: 0.05]
        RunManifestAudit.audit(null, [params: [password: 12345678]]).size() == 1

        where:
        text << ['password=12345678', '--password 12345678', 'Authorization: Bearer 12345678', 'DB_PASS=12345678']
    }

}
