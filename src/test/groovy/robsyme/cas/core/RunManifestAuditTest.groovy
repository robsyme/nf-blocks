package robsyme.cas.core

import spock.lang.PendingFeature
import spock.lang.Specification

/**
 * DESIGN.md §6 and the P3 plan section 1.6: the audit that a copy runs over
 * every RunManifest before it moves a block between stores. It reads the
 * encoded block, so it checks what a reader would see, not what the scrub
 * meant to do.
 */
class RunManifestAuditTest extends Specification {

    static final String USER = System.getProperty('user.name')
    static final Cid CID = DagCbor.cidOf(DagCbor.encode([kind: 'RunManifest']))

    static final String AKIA = 'AKIAIOSFODNN7EXAMPLE'
    static final String ASIA = 'ASIAIOSFODNN7EXAMPLE'
    static final String GHP = 'ghp_' + 'a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5P6q7R8'
    static final String GH_PAT = 'github_pat_' + '11ABCDEFG0abcdefghij_klmnopqrstuvwxyzABCDEFGHIJ0123456789abcdefghij'
    static final String TOWER = 'eyJ0aWQiOjEyMzR9' + 'LmFiY2RlZmdoaWprbG1ub3A'
    static final String PEM = '-----BEGIN RSA PRIVATE KEY-----' + '\nMIIEowIBAAKCAQEA/abc+def\n/ghijklmnop==\n-----END RSA PRIVATE KEY-----'

    /** Params holding every shape the audit looks for, as a pipeline might receive them. */
    private static Map rawParams() {
        return [
            outdir : '/data/results',
            inputs : ['cas://abc', '/mnt/a.fq', 'https://example.org/reads.fq'],
            who    : USER,
            nested : [aws: [id: AKIA, temp: ASIA], gh: GHP, ghpat: GH_PAT, tower: TOWER, pem: PEM],
            github_token: 'abc',
            db_password : 'hunter2',
            extra  : '--ref /data/g.fa',
            mount  : "-v /home/${USER}:/data".toString(),
            fine   : 'results',
            number : 7,
            // Final review item 3: a path after a comma, glued to a short flag or after an `@`,
            // and the user name inside a token.
            csvs   : 'a.csv,/home/someone/b.csv',
            bind   : "--bind a,/scratch/${USER}/x".toString(),
            short  : '-B/home/someone/data',
            argfile: '@/home/someone/args.txt',
            tilde  : "~${USER}/data".toString(),
        ]
    }

    /** Lines 1 to 11 are one shape each, the PEM block takes lines 12 to 15, line 16 is clean, lines 17 to 23 one shape each. */
    private static String rawConfig() {
        return [
            "workDir = '/scratch/work'",
            "process.containerOptions = '--volume=/home/x/data:/data'",
            "docker.registry = 'https://registry.example.org/x'",
            "env.WHO = '${USER}'",
            "env.NOTE1 = '${AKIA}'",
            "env.NOTE2 = '${ASIA}'",
            "env.NOTE3 = '${GHP}'",
            "env.NOTE4 = '${GH_PAT}'",
            "env.NOTE5 = '${TOWER}'",
            "tower.accessToken = 'abc123'",
            "password = 'x'",
            "certificate = '''${PEM}'''",
            "process.cpus = 2",
            "env.LIST = 'a.csv,/home/someone/b.csv'",
            "process.containerOptions = '--bind a,/scratch/${USER}/x'",
            "process.clusterOptions = '-B/home/someone/data'",
            "env.ARGS = '@/home/someone/args.txt'",
            "env.DATA = '~${USER}/data'",
            "params.passwords = ['hunter2', 'x']",
            "process.secrets = ['hunter2']",
        ].join('\n')
    }

    private static Map encoded(Map block) {
        return (Map) DagCbor.decode(DagCbor.encode(block))
    }

    private static Map manifest(Map params, Object config) {
        return new RunManifest([
            assertedBy: 'test', pipeline: 'demo', runName: 'happy_run', nfRunHash: 'h', sessionId: 's',
            nextflowVersion: '26.04.0', params: params, config: config, startedAt: '2026-10-24T00:00:00Z',
        ]).toCbor()
    }

    def 'a RunManifest built through the scrub has nothing for the audit to find'() {
        when:
        final List<RunManifestAudit.Finding> findings = RunManifestAudit.audit(CID, encoded(manifest(rawParams(), rawConfig())))

        then:
        findings == []
    }

    def 'the audit sees every shape in unscrubbed values, and nothing else'() {
        when:
        final List<RunManifestAudit.Finding> findings =
            RunManifestAudit.audit(CID, [kind: 'RunManifest', params: rawParams(), config: rawConfig()])

        then:
        findings.every { it.cid == CID && it.kind == 'RunManifest' }
        findings.collect { "${it.where}|${it.what}".toString() }.sort() == [
            'params.outdir|absolute path',
            'params.inputs[1]|absolute path',
            'params.inputs[2]|uri',
            'params.who|os user',
            'params.nested.aws.id|aws key id',
            'params.nested.aws.temp|aws key id',
            'params.nested.gh|github token',
            'params.nested.ghpat|github token',
            'params.nested.tower|seqera token',
            'params.nested.pem|private key',
            'params.github_token|secret assignment',
            'params.db_password|secret assignment',
            'params.extra|absolute path',
            'params.mount|absolute path',
            'params.csvs|absolute path',
            'params.bind|absolute path',
            'params.short|absolute path',
            'params.argfile|absolute path',
            'params.tilde|absolute path',    // F2: a `~<user>/` path is a path (was `os user`)
            // config line 14 is the PEM body line that starts with a slash: only the key itself is reported
            'config:1|absolute path',
            'config:2|absolute path',
            'config:3|uri',
            'config:4|os user',
            'config:5|aws key id',
            'config:6|aws key id',
            'config:7|github token',
            'config:8|github token',
            'config:9|seqera token',
            'config:10|seqera token',
            'config:11|secret assignment',
            'config:12|private key',
            'config:17|absolute path',
            'config:18|absolute path',
            'config:19|absolute path',
            'config:20|absolute path',
            'config:21|absolute path',       // F2: was `os user`
            'config:22|secret assignment',
            'config:23|secret assignment',
        ].sort()
    }

    def 'a list under a secret-named key is judged element by element; a map in it by its own keys'() {
        expect:
        RunManifestAudit.audit(CID, [params: [tokens: ['abc', '', '[secret]', 7, [inner: 'x'], ['deep']], tower_tokens: ['t'], ok: ['fine']]])
            .collect { "${it.where}|${it.what}".toString() } == [
            'params.tokens[0]|secret assignment',
            'params.tokens[5][0]|secret assignment',
            'params.tower_tokens[0]|seqera token',
        ]

        and: 'what the scrub writes for the same params is clean'
        RunManifestAudit.audit(CID, encoded(manifest([tokens: ['abc', 7, [inner: 'x']], tower_tokens: ['t']], ''))) == []
    }

    def 'an empty value under a secret-named key is no finding'() {
        expect:
        RunManifestAudit.audit(CID, [params: [token: '', nested: [db_password: '']], config: [api_key: '']]) == []
    }

    def 'the scrub keeps the shape of params strings and secret-named params'() {
        when:
        final Map block = encoded(manifest(rawParams(), rawConfig()))
        final Map params = (Map) block.params

        then:
        params.extra == '--ref [redacted-location]'
        params.mount == '-v [redacted-location]:[redacted-location]'
        params.github_token == '[secret]'
        params.db_password == '[secret]'
        params.fine == 'results'
        params.csvs == 'a.csv,[redacted-location]'
        params.bind == '--bind a,[redacted-location]'
        params.short == '-B[redacted-location]'
        params.argfile == '@[redacted-location]'
        params.tilde == '[redacted-location]'    // F2: was '~[redacted-user]/data'
        ((String) block.config).readLines().takeRight(7) == [
            "env.LIST = 'a.csv,[redacted-location]'",
            "process.containerOptions = '--bind a,[redacted-location]'",
            "process.clusterOptions = '-B[redacted-location]'",
            "env.ARGS = '@[redacted-location]'",
            "env.DATA = '[redacted-location]'",
            "params.passwords = ['[secret]', '[secret]']",
            "process.secrets = ['[secret]']",
        ]
    }

    def 'the markers the scrub writes, and cas and lid references, are clean'() {
        given:
        final String config = [
            "workDir = '[redacted-location]'",
            "password = '[secret]'",
            "who = '[redacted-user]'",
            "x = 'cas://lab/abc'",
            "y = 'lid://abc/def'",
            "--volume=[redacted-location]:[redacted-location]",
        ].join('\n')

        expect:
        RunManifestAudit.audit(CID, [params: [a: '[redacted-location]', b: '[redacted-user]', c: 'cas://x', d: 'lid://y'], config: config]) == []
    }

    def 'a config that is a Map (a block from before 2026-09-27) is walked like params'() {
        when:
        final def findings = RunManifestAudit.audit(CID, [params: [:], config: [process: [dir: '/x/y'], list: ['s3://b/k']]])

        then:
        findings*.where == ['config.process.dir', 'config.list[0]']
        findings*.what == ['absolute path', 'uri']
    }

    /** F6 round 2: a bare `name=value` with a secret-named name, in a params string and a quoted config value. */
    private static Map bareParams() {
        return [a: 'user=bob password=hunter2', b: '--db "host=h dbname=d password=secret123"', c: '-Dx=a,password=hunter2',
                d: 'token=abc', e: '-Ddb.props=user=admin,password=hunter2']
    }

    private static String bareConfig() {
        return [
            "env.X = 'token=abc'",
            "env.Y = '-Ddb.props=user=admin,password=hunter2'",
            "params.a = 'user=bob password=hunter2'",
            "params.b = '--db \"host=h dbname=d password=secret123\"'",
            "params.c = '-Dx=a,password=hunter2'",
        ].join('\n')
    }

    def 'a bare secret-named name=value is found in the raw values, and the scrubbed block audits clean'() {
        when:
        final List<String> raw = RunManifestAudit.audit(CID, [kind: 'RunManifest', params: bareParams(), config: bareConfig()])
            .collect { "${it.where}|${it.what}".toString() }
        final Map block = encoded(manifest(bareParams(), bareConfig()))

        then:
        raw.sort() == ['params.a', 'params.b', 'params.c', 'params.d', 'params.e', 'config:1', 'config:2', 'config:3', 'config:4', 'config:5']
            .collect { it + '|secret assignment' }.sort()
        RunManifestAudit.audit(CID, block) == []
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config
    }

    def 'a bare name=value whose name does not end in a secret word is no finding (#kept)'() {
        expect:
        RunManifestAudit.audit(CID, [kind: 'RunManifest', params: [a: kept], config: "process.ext.args = '${kept}'".toString()]) == []

        where:
        kept << ['--trimmomatic-args SLIDINGWINDOW:4:15', '-q 20 --nextseq-trim=20', 'ktrim=r k=23 mink=11 hdist=1 ref=adapters',
                 'SORT_ORDER=coordinate', 'max_tokens=4096', 'key_column=sample tokenizer=bert keyfile=reads.key', 'skip_auth=true',
                 'monkey=1', '-O bam,level=5', 'x=1,y=2;z=3']
    }

    def 'a map key that is a credential, the user name or holds a path is a finding, named as the scrub names it'() {
        given:
        final Map files = [('/home/x/a.fq'): 1, (USER): 2, '--in=/home/x/y': 3, 'a /home/x/b': 4, '~/c.fq': 5,
                           ('ghp_' + 'abcdefghijklmnopqrstuv'): 6, 's3://b/k': 7, sample: 8, 'rel.fq': 9, '7': 10]

        when:
        final List<String> raw = RunManifestAudit.audit(CID, [kind: 'RunManifest', params: [files: files], config: [env: [(USER): 'x']]])
            .collect { "${it.where}|${it.what}".toString() }
        final Map block = encoded(manifest([files: files], [env: [(USER): 'x']]))

        then:
        raw == [
            'params.files.[redacted-location]|absolute path',
            'params.files.[redacted-user]|os user',
            'params.files.[redacted-location]-2|absolute path',
            'params.files.[redacted-location]-3|absolute path',
            'params.files.[redacted-location]-4|absolute path',
            'params.files.[secret]|github token',
            'params.files.[redacted-location]-5|uri',
            'config.env.[redacted-user]|os user',
        ]
        and: 'the scrubbed block audits clean'
        RunManifestAudit.audit(CID, block) == []
    }

    def 'a finding serialises as cid, kind, where and what'() {
        when:
        final def findings = RunManifestAudit.audit(CID, [params: [outdir: '/x']])

        then:
        findings.size() == 1
        findings[0].toMap() == [cid: CID.toString(), kind: 'RunManifest', where: 'params.outdir', what: 'absolute path']
    }

    /** Runs the closure with user.name set to the given name, and restores it. */
    private static <T> T asUser(String name, Closure<T> body) {
        final String saved = System.getProperty('user.name')
        System.setProperty('user.name', name)
        try { return body.call() }
        finally { System.setProperty('user.name', saved) }
    }

    static final String CONTRIBUTORS =
        "manifest { contributors = [[name:'Alice Example', affiliation:'Example Lab', github:'@alice', contribution:['contributor'], orcid:'']] }"

    def 'a github handle equal to the os user name is neither flagged nor scrubbed'() {
        when:
        final List<RunManifestAudit.Finding> findings = asUser('alice') { RunManifestAudit.audit(CID, [params: [:], config: CONTRIBUTORS]) }
        final Map block = asUser('alice') { encoded(manifest([handle: '@alice'], CONTRIBUTORS)) }

        then:
        findings == []
        block.config == CONTRIBUTORS
        ((Map) block.params).handle == '@alice'
    }

    def 'a path after an @ and a user name before an @ are judged as before, with the user name set'() {
        when:
        final List<RunManifestAudit.Finding> findings = asUser('alice') {
            RunManifestAudit.audit(CID, [params: [argfile: '@/home/someone/args.txt', mail: 'alice@example.org', both: '@alice:/home/x']])
        }
        final Map params = (Map) asUser('alice') {
            encoded(manifest([argfile: '@/home/someone/args.txt', mail: 'alice@example.org', both: '@alice:/home/x'], ''))
        }.params

        then:
        findings.collect { "${it.where}|${it.what}".toString() }.sort() ==
            ['params.argfile|absolute path', 'params.both|absolute path', 'params.mail|os user']
        params.argfile == '@[redacted-location]'
        params.mail == '[redacted-user]@example.org'
        params.both == '@alice:[redacted-location]'
    }

    def 'a missing params or config is not an error'() {
        expect:
        RunManifestAudit.audit(CID, [kind: 'RunManifest']) == []
    }

    // ---- The F2 rulings: every row of the integrator's scrub probe (p3-rev-idx/scrub-probe.txt)

    static final String AWS_SECRET = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    static final String JWT = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlX2hlcmU'
    static final String SLACK = 'xoxb-' + '1234567890-abcdefghij'
    static final String GITLAB = 'glpat-' + 'abcdefghij0123456789'
    static final String HF = 'hf_' + 'abcdefghijklmnopqrstuvwxyzABCDEFGH'

    /** The probe's params, plus a row for each rule the probe only reached under a secret-named key. */
    private static Map probeParams() {
        return [
            aws_creds : AWS_SECRET,
            auth      : 'Bearer ' + JWT,
            bearer    : 'Bearer abcdefghij0123456789',
            jwt       : JWT,
            slack     : SLACK,
            gitlab    : GITLAB,
            url       : 'https://user:hunter2@example.org/x',
            win       : 'C:\\Users\\someone\\x.fq',
            unc       : '\\\\fileserver\\share\\x.fq',
            tilde     : '~/data/x.fq',
            tilde_user: '~someone/data/x.fq',
            rel       : "../../home/${USER}/x".toString(),
            hf        : HF,
            extra_args: '--api-key=abc123def --password hunter2',
            json      : '{"token": "abc123"}',
            files     : ['/home/someone/a.fq': 'a'],
            paths     : ['/data/x.fq': 1, '~/y.fq': 2, 'C:\\z.fq': 3, 'rel.fq': 4],
            nested    : [[path: '/data/x']],
            gstring   : "${'/'}data/y".toString(),
            ip        : '10.1.2.3:/export/data',
            sas       : 'sv=2021-08-06&ss=b&sig=abcDEF123%2Bxyz%3D',
            // fix round 1
            flag_bearer : '--token Bearer abcdefghij0123456789',
            hdr_flag    : '--header-auth Bearer abcdefghij0123456789',
            quoted_flag : '--password \'hunter2\' --api-key="abc123"',
            basic       : 'Authorization: Basic dXNlcjpwYXNz',
            bearer_lower: 'bearer abcdefghij0123456789',
            // fix round 2: auth as the last segment takes the next token, unless a negation precedes it
            aflags      : '--auth sometoken --basic-auth u:p --proxy-auth tok --http-auth tok',
            // fix round 3: a secret flag inside a non-secret flag's quoted value, and a -D property
            extra1      : '--extra "foo --password hunter2"',
            extra2      : "--extra 'run --token abc123'",
            extra3      : '--extra="foo --password hunter2"',
            wrapper     : '--wrapper "java -jar x.jar --password hunter2"',
            jvm         : '-Dfoo.password=abc -Xmx2g',
            // fix round 4: Basic judged by decoding, and a secret flag's Basic value
            basic_unpad : 'Authorization: Basic dXNlcjpwYXNzd29yZA',
            basic_admin : 'Basic YWRtaW46YWRtaW4',
            basic_short : 'Basic dTpwYXNz basic YWI6Y2Rl Basic cm9vdDp4',
            hdr_basic   : '--header "Authorization: Basic dXNlcjpwYXNzd29yZA"',
            ab1         : '--auth Basic dTpwYXNz',
            ab2         : "--auth 'Basic dXNlcjpwYXNzd29yZA'",
            ab3         : '--auth Basic Statistics',
            jvm_comma   : '-Dfoo.password=abc,-Dbar.token=def',
        ]
    }

    /**
     * The probe's config lines 1 to 8, then one line per rule the probe left out. Config text
     * writes a backslash doubled, so line 15 holds `C:\\Users\\...` and line 16 `\\\\fileserver\\...`.
     */
    private static String probeConfig() {
        return [
            "process.ext.args = '--password hunter2'",
            "env.API_AUTH = 'abc'",
            "aws.client.endpoint = 'http://10.0.0.5:9000'",
            "params.slack = '${SLACK}'",
            "singularity.cacheDir = '~/sing'",
            "process.clusterOptions = '--account=projX -A projX'",
            "azure.storage.sasToken = 'sv=2021&sig=abc'",
            "params { aws_creds = '${AWS_SECRET}' }",
            "params.json = '{\"token\": \"abc123\"}'",
            "process.ext.args = '--api-key=abc123def'",
            "env.HDR = 'Authorization: Bearer abcdefghij0123456789'",
            "env.GL = '${GITLAB}'",
            "env.HF = '${HF}'",
            "env.JWT = '${JWT}'",
            "params.win = 'C:\\\\Users\\\\someone\\\\x.fq'",
            "params.unc = '\\\\\\\\fileserver\\\\share\\\\x'",
            "params.home = '~someone/data'",
            "env.SAS = '?sv=2021&sig=abcDEF123%2B'",
            "params.endpoint = '10.1.2.3:/export/data'",
            // fix round 1, lines 20 to 29
            "process.ext.args = \"--password 'hunter2'\"",
            "env.FLAGS = '--password=\"hunter2\" --token Bearer abcdefghij0123456789'",
            "params.java = '''",
            "  -Dfoo.password=abc",
            "'''",
            'process.ext.args = """',
            "  -Dapi.token=abc --x 1",
            '"""',
            "env.BASIC = 'Basic dXNlcjpwYXNz'",
            "env.B2 = 'BEARER abcdefghij0123456789'",
            "process.ext.args = '--auth sometoken --basic-auth u:p --proxy-auth tok --http-auth tok'",
            "process.ext.args = '--extra \"foo --password hunter2\"'",
            "process.ext.args = '-Dfoo.password=abc'",
            "env.JAVA_OPTS = '-Dapi.token=abc -Xmx2g'",
            "env.H = 'Authorization: Basic dXNlcjpwYXNzd29yZA'",
            "env.H2 = 'Basic dTpwYXNz'",
            "process.ext.args = '--auth Basic dTpwYXNz --header \"Authorization: Basic YWRtaW46YWRtaW4\"'",
            "process.ext.args = \"--auth 'Basic dXNlcjpwYXNzd29yZA' --auth Basic Statistics\"",
            // F6, lines 38 to 50: a secret glued after a comma and punctuation, and a -D value in a multi-line string
            "x = a,(token=abc)",
            "params.x = '''",
            "  a=1,(token=abc)",
            "'''",
            "opts=x,{token=abc}",
            "opts=a,+token=abc",
            'x = a,$token=abc',
            "params.java = '''",
            "  -Dfoo.password=p,a:ss",
            "  -Dfoo.password=abc,)def",
            "  -Dfoo.password=abc,\"def",
            "  -Dfoo.password=abc,user=bob",
            "'''",
            // F6 round 1, lines 51 to 62: a secret after a non-secret -D value, and a secret named argument in a call
            "params.y = '''",
            "  -Da=1,password=x",
            "  -Dx=a,token=abc",
            "  -Ddb.props=user=admin,password=hunter2",
            "  -Dconn=user=bob;password=hunter2",
            "  -Dx=a,api_key: abc",
            "  -Dx=a,token='abc'",
            "'''",
            "ext.args = { def o = foo(token: 'abc'); o }",
            "ext.args = {f(token:'abc')}",
            "x = f(token=abc)",
            "x = foo(token: 'abc')",
        ].join('\n')
    }

    def 'every probe row is redacted by the scrub, and the scrubbed block audits clean'() {
        when:
        final Map block = encoded(manifest(probeParams(), probeConfig()))

        then:
        block.params == [
            aws_creds : '[secret]',
            auth      : '[secret]',
            bearer    : 'Bearer [secret]',
            jwt       : '[secret]',
            slack     : '[secret]',
            gitlab    : '[secret]',
            url       : '[redacted-location]',
            win       : '[redacted-location]',
            unc       : '[redacted-location]',
            tilde     : '[redacted-location]',
            tilde_user: '[redacted-location]',
            rel       : '../../home/[redacted-user]/x',
            hf        : '[secret]',
            extra_args: '--api-key=[secret] --password [secret]',
            json      : '{"token": "[secret]"}',
            files     : ['[redacted-location]': 'a'],
            paths     : ['[redacted-location]': 1, '[redacted-location]-2': 2, '[redacted-location]-3': 3, 'rel.fq': 4],
            nested    : [[path: '[redacted-location]']],
            gstring   : '[redacted-location]',
            ip        : '10.1.2.3:[redacted-location]',
            sas       : 'sv=2021-08-06&ss=b&sig=[secret]',
            flag_bearer : '--token [secret]',          // round 4: a secret flag takes `Bearer <token>` whole
            hdr_flag    : '--header-auth [secret]',
            quoted_flag : '--password \'[secret]\' --api-key="[secret]"',
            basic       : 'Authorization: Basic [secret]',
            bearer_lower: 'bearer [secret]',
            aflags      : '--auth [secret] --basic-auth [secret] --proxy-auth [secret] --http-auth [secret]',
            extra1      : '--extra "foo --password [secret]"',
            extra2      : "--extra 'run --token [secret]'",
            extra3      : '--extra="foo --password [secret]"',
            wrapper     : '--wrapper "java -jar x.jar --password [secret]"',
            jvm         : '-Dfoo.password=[secret] -Xmx2g',
            basic_unpad : 'Authorization: Basic [secret]',
            basic_admin : 'Basic [secret]',
            basic_short : 'Basic [secret] basic [secret] Basic [secret]',
            hdr_basic   : '--header "Authorization: Basic [secret]"',
            ab1         : '--auth [secret]',
            ab2         : "--auth '[secret]'",
            ab3         : '--auth [secret]',
            jvm_comma   : '-Dfoo.password=[secret],-Dbar.token=[secret]',
        ]
        block.config == [
            "process.ext.args = '--password [secret]'",
            "env.API_AUTH = '[secret]'",
            "aws.client.endpoint = '[redacted-location]'",
            "params.slack = '[secret]'",
            "singularity.cacheDir = '[redacted-location]'",
            "process.clusterOptions = '--account=projX -A projX'",
            "azure.storage.sasToken = '[secret]'",
            "params { aws_creds = '[secret]' }",
            "params.json = '{\"token\": \"[secret]\"}'",
            "process.ext.args = '--api-key=[secret]'",
            "env.HDR = 'Authorization: Bearer [secret]'",
            "env.GL = '[secret]'",
            "env.HF = '[secret]'",
            "env.JWT = '[secret]'",
            "params.win = '[redacted-location]'",
            "params.unc = '[redacted-location]'",
            "params.home = '[redacted-location]'",
            "env.SAS = '?sv=2021&sig=[secret]'",
            "params.endpoint = '10.1.2.3:[redacted-location]'",
            "process.ext.args = \"--password '[secret]'\"",
            "env.FLAGS = '--password=\"[secret]\" --token [secret]'",
            "params.java = '''",
            "  -Dfoo.password='[secret]'",
            "'''",
            'process.ext.args = """',
            "  -Dapi.token='[secret]' --x 1",
            '"""',
            "env.BASIC = 'Basic [secret]'",
            "env.B2 = 'BEARER [secret]'",
            "process.ext.args = '--auth [secret] --basic-auth [secret] --proxy-auth [secret] --http-auth [secret]'",
            "process.ext.args = '--extra \"foo --password [secret]\"'",
            "process.ext.args = '-Dfoo.password=[secret]'",
            "env.JAVA_OPTS = '-Dapi.token=[secret] -Xmx2g'",
            "env.H = 'Authorization: Basic [secret]'",
            "env.H2 = 'Basic [secret]'",
            "process.ext.args = '--auth [secret] --header \"Authorization: Basic [secret]\"'",
            "process.ext.args = \"--auth '[secret]' --auth [secret]\"",
            "x = a,(token='[secret]')",
            "params.x = '''",
            "  a=1,(token='[secret]')",
            "'''",
            "opts=x,{token='[secret]'}",
            "opts=a,+token='[secret]'",
            "x = a,\$token='[secret]'",
            "params.java = '''",
            "  -Dfoo.password='[secret]'",
            "  -Dfoo.password='[secret]'",
            "  -Dfoo.password='[secret]'\"def",
            "  -Dfoo.password='[secret]'",
            "'''",
            "params.y = '''",
            "  -Da=1,password='[secret]'",
            "  -Dx=a,token='[secret]'",
            "  -Ddb.props=user=admin,password='[secret]'",
            "  -Dconn=user=bob;password='[secret]'",
            "  -Dx=a,api_key: '[secret]'",
            "  -Dx=a,token='[secret]'",
            "'''",
            "ext.args = { def o = foo(token: '[secret]'); o }",
            "ext.args = {f(token:'[secret]')}",
            "x = f(token='[secret]')",
            "x = foo(token: '[secret]')",
        ].join('\n')
        RunManifestAudit.audit(CID, block) == []

        and: 'the scrub is idempotent on every row'
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config
    }

    /** Fix round 5: a -D property's value up to another -D property, and a bare value with a comma in a multi-line string. */
    private static Map round5Params() {
        return [
            jvm_tail : '-Dfoo.password=hunter,2 -Xmx2g',
            jvm_lead : '-Dfoo.password=,abc',
            jvm_at   : '-Djavax.net.ssl.keyStorePassword=p@ss,w0rd',
            proxy_arg: '--proxy-auth basic --threads 4',
            q_basic  : '--auth Basic "dTpwYXNz"',
            q_bearer : '--token Bearer "abcdefghij0123456789" --threads 4',
        ]
    }

    private static String round5Config() {
        return [
            "env.X = '-Dfoo.password=hunter,2 -Xmx2g'",
            "params.java = '''",
            "  -Dfoo.password=abc,def",
            "  a=1,password=xyz",
            "'''",
            "process.ext.args = '--proxy-auth basic --threads 4'",
            "process.ext.args = '--token Bearer \"abcdefghij0123456789\"'",
        ].join('\n')
    }

    def 'fix round 5 rows are redacted by the scrub and found by the audit, and the scrubbed block audits clean'() {
        when:
        final Map block = encoded(manifest(round5Params(), round5Config()))
        final List<RunManifestAudit.Finding> raw =
            RunManifestAudit.audit(CID, [kind: 'RunManifest', params: round5Params(), config: round5Config()])

        then:
        block.params == [
            jvm_tail : '-Dfoo.password=[secret] -Xmx2g',
            jvm_lead : '-Dfoo.password=[secret]',
            jvm_at   : '-Djavax.net.ssl.keyStorePassword=[secret]',
            proxy_arg: '--proxy-auth [secret] --threads 4',
            q_basic  : '--auth [secret]',
            q_bearer : '--token [secret] --threads 4',
        ]
        block.config == [
            "env.X = '-Dfoo.password=[secret] -Xmx2g'",
            "params.java = '''",
            "  -Dfoo.password='[secret]'",
            "  a=1,password='[secret]'",
            "'''",
            "process.ext.args = '--proxy-auth [secret] --threads 4'",
            "process.ext.args = '--token [secret]'",
        ].join('\n')
        raw.collect { "${it.where}|${it.what}".toString() }.sort() == [
            'params.jvm_tail|secret assignment',
            'params.jvm_lead|secret assignment',
            'params.jvm_at|secret assignment',
            'config:1|secret assignment',
            'config:3|secret assignment',
            'config:4|secret assignment',
            'params.proxy_arg|secret assignment',
            'params.q_basic|secret assignment',
            'params.q_bearer|secret assignment',
            'config:6|secret assignment',
            'config:7|secret assignment',
        ].sort()
        RunManifestAudit.audit(CID, block) == []
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config
    }

    def 'the audit finds every probe row in the raw values'() {
        when:
        final List<RunManifestAudit.Finding> findings =
            RunManifestAudit.audit(CID, [kind: 'RunManifest', params: probeParams(), config: probeConfig()])

        then:
        findings.collect { "${it.where}|${it.what}".toString() }.sort() == [
            'params.aws_creds|aws secret key',
            'params.auth|secret assignment',
            'params.auth|bearer token',
            'params.bearer|bearer token',
            'params.jwt|jwt',
            'params.slack|slack token',
            'params.gitlab|gitlab token',
            'params.url|uri',
            'params.win|absolute path',
            'params.unc|absolute path',
            'params.tilde|absolute path',
            'params.tilde_user|absolute path',
            'params.rel|os user',
            'params.hf|hugging face token',
            'params.extra_args|secret assignment',
            'params.json|secret assignment',
            'params.files.[redacted-location]|absolute path',
            'params.paths.[redacted-location]|absolute path',
            'params.paths.[redacted-location]-2|absolute path',
            'params.paths.[redacted-location]-3|absolute path',
            'params.nested[0].path|absolute path',
            'params.gstring|absolute path',
            'params.ip|absolute path',
            'params.sas|azure sas signature',
            'config:1|secret assignment',
            'config:2|secret assignment',
            'config:3|uri',
            'config:4|slack token',
            'config:5|absolute path',
            'config:7|secret assignment',
            'config:7|azure sas signature',
            'config:8|aws secret key',
            'config:9|secret assignment',
            'config:10|secret assignment',
            'config:11|bearer token',
            'config:12|gitlab token',
            'config:13|hugging face token',
            'config:14|jwt',
            'config:15|absolute path',
            'config:16|absolute path',
            'config:17|absolute path',
            'config:18|azure sas signature',
            'config:19|absolute path',
            'params.flag_bearer|secret assignment',   // round 4: was bearer token
            'params.hdr_flag|secret assignment',      // round 4: was bearer token
            'params.quoted_flag|secret assignment',
            'params.basic|basic auth',
            'params.bearer_lower|bearer token',
            'config:20|secret assignment',
            'config:21|secret assignment',            // round 4: no longer also bearer token
            'config:23|secret assignment',
            'config:26|secret assignment',
            'config:28|basic auth',
            'config:29|bearer token',
            'params.aflags|secret assignment',
            'config:30|secret assignment',
            'params.extra1|secret assignment',
            'params.extra2|secret assignment',
            'params.extra3|secret assignment',
            'params.wrapper|secret assignment',
            'params.jvm|secret assignment',
            'config:31|secret assignment',
            'config:32|secret assignment',
            'config:33|secret assignment',
            'params.basic_unpad|basic auth',
            'params.basic_admin|basic auth',
            'params.basic_short|basic auth',
            'params.hdr_basic|basic auth',
            'params.ab1|secret assignment',
            'params.ab2|secret assignment',
            'params.ab3|secret assignment',
            'params.jvm_comma|secret assignment',
            'config:34|basic auth',
            'config:35|basic auth',
            'config:36|secret assignment',
            'config:36|basic auth',
            'config:37|secret assignment',
            'config:38|secret assignment',
            'config:40|secret assignment',
            'config:42|secret assignment',
            'config:43|secret assignment',
            'config:44|secret assignment',
            'config:46|secret assignment',
            'config:47|secret assignment',
            'config:48|secret assignment',
            'config:49|secret assignment',
            'config:52|secret assignment',
            'config:53|secret assignment',
            'config:54|secret assignment',
            'config:55|secret assignment',
            'config:56|secret assignment',
            'config:57|secret assignment',
            'config:59|secret assignment',
            'config:60|secret assignment',
            'config:61|secret assignment',
            'config:62|secret assignment',
        ].sort()
    }

    static final String CID_RAW = 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
    static final String CID_CBOR = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
    static final String HEX64 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

    /** What the F2 ruling keeps, and the lead's cases that keep the AWS secret-key shape narrow. */
    private static Map keptParams() {
        return [
            sample : 'SAMPLE_01',
            rel    : 'reference/genome.fa',
            ratio  : '-1/3',
            email  : 'someone@example.org',
            account: '--account=projX',
            git_sha: '9fceb02d0ae598e95dc970b74767f19372d61af8',
            upper  : '9FCEB02D0AE598E95DC970B74767F19372D61AF8',
            md5    : 'd41d8cd98f00b204e9800998ecf8427e',
            cas_uri: 'cas://' + CID_RAW + '/A.bam',
            lid    : 'lid://abc/x',
            cid    : CID_CBOR,
            raw_cid: CID_RAW,
            listed : [CID_CBOR, 'cas://' + CID_CBOR + '/qc/report.html'],
            digest : 'sha256:' + HEX64,
            image  : 'quay.io/biocontainers/samtools:1.17--h00cdaf9_0@sha256:' + HEX64,
            lower40: 'ghijklmnopqrstuvwxyzghijklmnopqrstuvwxyz',
            // 40 characters of both cases with no digit, `/` or `+`: not the AWS secret-key shape
            letters: 'abcdefghijABCDEFGHIJabcdefghijABCDEFGHIJ',
            author : 'Jane Doe',
            prose  : 'the bearer of the token, one hf_ prefix',
            // fix round 1: a secret word inside a flag's name, not its last segment, takes no next token
            opt_sort  : '--sort-keys input.json',
            opt_file  : '--keyfile reads.key',
            opt_col   : '--token-col 3',
            opt_max   : '--max-tokens 4096',
            opt_skip  : '--skip-auth sample.bam',
            opt_no    : '--no-auth x',
            opt_dis   : '--disable-auth y --disabled-auth z --without-auth w',
            // fix round 3: a property with no secret name, and capitalised prose after Basic
            jvm_ok    : '-Dfile.encoding=UTF-8 -Xmx2g --extra "foo --threads 4"',
            prose3    : 'Basic QualityControl and Basic MultiQC1',
            prose4    : 'Basic DataAnalysis, Basic Statistics, Basic SNPsIndelsQC, Basic FastQCReport, Basic StatisticsReport',
            prose2    : 'Basic Statistics and the bearer instruments',
            // fix round 5: capitalised O-words decode to a byte of 0x80 or more, then a colon
            prose5    : 'Basic Overview, Basic Options, Basic Output, Basic Online, Basic Only',
            prose6    : 'Basic Origin, Basic Overall, Basic Ontology',
        ]
    }

    private static String keptConfig() {
        return [
            "params.sample = 'SAMPLE_01'",
            "params.rel = 'reference/genome.fa'",
            "params.ratio = '-1/3'",
            "params.email = 'someone@example.org'",
            "process.clusterOptions = '--account=projX'",
            "params.git_sha = '9fceb02d0ae598e95dc970b74767f19372d61af8'",
            "params.cas = 'cas://${CID_RAW}/A.bam'",
            "params.lid = 'lid://abc/x'",
            "params.cid = '${CID_CBOR}'",
            "process.container = 'quay.io/biocontainers/samtools:1.17--h00cdaf9_0@sha256:${HEX64}'",
            "params.digest = 'sha256:${HEX64}'",
            "params.lower40 = 'ghijklmnopqrstuvwxyzghijklmnopqrstuvwxyz'",
            "manifest.author = 'Jane Doe'",
            "env.AUTHOR = 'Jane'",
            "params.listed = ['${CID_CBOR}', 'cas://${CID_CBOR}/qc/report.html']",
            // fix round 1: regex escapes as config text writes them (backslashes doubled), and flags
            "params.re1 = '\\\\d\\\\d'",
            "params.re2 = '\\\\bword\\\\b'",
            "params.re3 = '\\\\w\\\\s'",
            "process.ext.args = '--sort-keys input.json --keyfile reads.key --token-col 3 --max-tokens 4096 --skip-auth sample.bam'",
            "process.ext.args = '--no-auth x --disable-auth y --disabled-auth z --without-auth w'",
            "env.JAVA_OPTS = '-Dfile.encoding=UTF-8 -Xmx2g'",
            "params.prose3 = 'Basic QualityControl and Basic MultiQC1'",
            "params.prose4 = 'Basic DataAnalysis, Basic Statistics, Basic SNPsIndelsQC, Basic FastQCReport, Basic StatisticsReport'",
            "params.prose2 = 'Basic Statistics and the bearer instruments'",
            "params.prose5 = 'Basic Overview, Basic Options, Basic Output, Basic Online, Basic Only'",
            "params.prose6 = 'Basic Origin, Basic Overall, Basic Ontology'",
        ].join('\n')
    }

    def 'what the ruling keeps survives the scrub unchanged and is no finding'() {
        when:
        final Map block = encoded(manifest(keptParams(), keptConfig()))

        then:
        block.params == keptParams()
        block.config == keptConfig()
        RunManifestAudit.audit(CID, [kind: 'RunManifest', params: keptParams(), config: keptConfig()]) == []
        RunManifestAudit.audit(CID, block) == []
    }

    def 'a 50 KB quoted config value or params string is scrubbed and audited without overflow'() {
        given: 'a single-quoted and a double-quoted 50 KB value, one of 50 KB of escapes, and a 50 KB params string'
        final String a = 'a' * 50000
        final String b = 'b' * 50000
        final String escaped = ('x' + '\\' + "'") * 17000
        final String config = [
            "params.long1 = '${a} /data/x'",
            "params.long2 = \"${b} /data/y\"",
            "params.long3 = '${escaped}'",
        ].join('\n')
        final Map params = [long: ('c ' * 25000) + '/data/z']

        when:
        final long t0 = System.nanoTime()
        final Map block = encoded(manifest(params, config))
        final List<RunManifestAudit.Finding> onRaw = RunManifestAudit.audit(CID, [kind: 'RunManifest', params: params, config: config])
        final List<RunManifestAudit.Finding> onBlock = RunManifestAudit.audit(CID, block)
        final long millis = (System.nanoTime() - t0).intdiv(1000000L)

        then:
        ((String) block.config).readLines() == [
            "params.long1 = '${a} [redacted-location]'".toString(),
            "params.long2 = \"${b} [redacted-location]\"".toString(),
            "params.long3 = '${escaped}'".toString(),
        ]
        ((Map) block.params).long == ('c ' * 25000) + '[redacted-location]'
        onRaw.collect { "${it.where}|${it.what}".toString() }.sort() == ['config:1|absolute path', 'config:2|absolute path', 'params.long|absolute path']
        onBlock == []
        millis < 20000
    }

    /** Milliseconds the closure takes, and its result. */
    private static List timed(Closure<?> work) {
        final long t0 = System.nanoTime()
        final Object result = work.call()
        return [(System.nanoTime() - t0).intdiv(1000000L), result]
    }

    def 'a 50 KB list (#name) is scrubbed and audited in under a second, in params and in config'() {
        given: 'one warm-up pass, so class loading is not timed'
        Records.scrub([v: 'a,b:c=d'])
        RunManifestAudit.audit(CID, [kind: 'RunManifest', params: [v: 'a,b:c=d'], config: "x = 'a,b:c=d'"])

        expect:
        final List scrubbed = timed { Records.scrub([v: list]).v }
        scrubbed[1] == list
        scrubbed[0] < 1000
        final List audited = timed { RunManifestAudit.audit(CID, [kind: 'RunManifest', params: [v: list]]) }
        audited[1] == []
        audited[0] < 1000
        final String config = "params.v = '${list}'\nparams.w = ${list}"
        final List configScrubbed = timed { Records.scrubConfigText(config) }
        configScrubbed[1] == config
        configScrubbed[0] < 1000
        final List configAudited = timed { RunManifestAudit.audit(CID, [kind: 'RunManifest', config: config]) }
        configAudited[1] == []
        configAudited[0] < 1000

        where:
        name                 | list
        "'SAMPLE_0001,' x 4200" | 'SAMPLE_0001,' * 4200
        "'a,' x 25000"          | 'a,' * 25000
        "'a:' x 25000"          | 'a:' * 25000
        "'chr1:100-200,' x 4000" | 'chr1:100-200,' * 4000
        "'a=' x 25000"          | 'a=' * 25000
        "'a;b@' x 12500"        | 'a;b@' * 12500
        "'(' x 50000"           | '(' * 50000
        "'f({a,' x 10000"       | 'f({a,' * 10000
    }

    def 'a 50 KB list of paths or a -D secret with 25000 commas has every part redacted, in under a second'() {
        given: 'a path after each = and a key after each : (a path runs to a colon), and one -D property'
        Records.scrub([v: 'a,b:c=d'])
        final String paths = 'x=/data/a:' * 5000
        final String jvm = '-Dfoo.password=' + ('a,' * 25000)

        when:
        final List scrubbed = timed { Records.scrub([paths: paths, jvm: jvm]) }
        final List audited = timed { RunManifestAudit.audit(CID, [kind: 'RunManifest', params: [paths: paths, jvm: jvm]]) }
        final List configScrubbed = timed { Records.scrubConfigText("params.paths = '${paths}'\nenv.J = '${jvm}'") }

        then:
        scrubbed[1] == [paths: 'x=[redacted-location]:' * 5000, jvm: '-Dfoo.password=[secret]']
        audited[1].collect { "${it.where}|${it.what}".toString() }.sort() == ['params.jvm|secret assignment', 'params.paths|absolute path']
        configScrubbed[1] == "params.paths = '${'x=[redacted-location]:' * 5000}'\nenv.J = '-Dfoo.password=[secret]'".toString()
        scrubbed[0] < 1000
        audited[0] < 1000
        configScrubbed[0] < 1000
    }

    def 'an IP before a path keeps the IP and loses the path'() {
        expect:
        ((Map) encoded(manifest([ip: '10.1.2.3:/export/data'], "nfs = '10.1.2.3:/export/data'")).params).ip == '10.1.2.3:[redacted-location]'
        encoded(manifest([:], "nfs = '10.1.2.3:/export/data'")).config == "nfs = '10.1.2.3:[redacted-location]'"
    }

    // ---- P4 L1: the integrator's second scrub probe (bf3dc76), row by row

    static final String PROBE_PEM = '-----BEGIN PRIVATE KEY-----' + '\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\n-----END PRIVATE KEY-----\n'
    static final String GOOGLE_KEY = 'AIza' + 'SyA1234567890abcdefghijklmnopqrstuv'

    /**
     * [label, params, config text, text that must not survive (null when nothing is secret), text that must
     * survive (or null), the kinds the audit reports on the raw values]. The probe's Google key was 38
     * characters, one short of the shape (`AIza` and 35); this row uses 39.
     */
    static final List<List<Object>> PROBE2 = [
        // secrets in params strings
        ['p --api-key=', [extra: '--api-key=abc123def'], '', 'abc123def', '--api-key=', ['secret assignment']],
        ['p --password sp', [extra: '--password hunter2'], '', 'hunter2', '--password ', ['secret assignment']],
        ['p --token quoted', [extra: "--token 'my secret value'"], '', 'my secret value', '--token ', ['secret assignment']],
        ['p --token dq', [extra: '--token "abc123"'], '', 'abc123', '--token ', ['secret assignment']],
        ['p --api_key', [extra: '--api_key abc123def'], '', 'abc123def', '--api_key ', ['secret assignment']],
        ['p --apiKey=', [extra: '--apiKey=abc123def'], '', 'abc123def', '--apiKey=', ['secret assignment']],
        ['p --passphrase', [extra: '--passphrase hunter2'], '', 'hunter2', '--passphrase ', ['secret assignment']],
        ['p --pass', [extra: '--pass hunter2'], '', 'hunter2', '--pass ', ['secret assignment']],
        ['p --pwd', [extra: '--pwd hunter2'], '', 'hunter2', '--pwd ', ['secret assignment']],
        ['p openssl -pass pass:', [extra: '-pass pass:hunter2'], '', 'hunter2', '-pass pass:', ['secret assignment']],
        ['p password: yaml', [extra: 'password: hunter2'], '', 'hunter2', 'password: ', ['secret assignment']],
        ['p token=bare', [extra: 'host=h token=abc123'], '', 'abc123', 'host=h token=', ['secret assignment']],
        ['p Authorization', [extra: 'Authorization: Bearer abcDEF123456'], '', 'abcDEF123456', 'Authorization: Bearer ', ['bearer token']],
        ['p Authorization short', [extra: 'Authorization: Bearer abc'], '', 'abc', 'Authorization: Bearer ', ['secret assignment']],
        ['p Authorization token', [extra: 'Authorization: token abc'], '', 'abc', 'Authorization: token ', ['secret assignment']],
        ['p --header auth', [extra: "--header 'Authorization: token ghx123456abc'"], '', 'ghx123456abc', "--header 'Authorization: token ", ['secret assignment']],
        ['p x-api-key header', [extra: "-H 'x-api-key: abc123def456'"], '', 'abc123def456', "-H 'x-api-key: ", ['secret assignment']],
        ['p X-Auth-Token header', [extra: '-H "X-Auth-Token: abc"'], '', 'abc', '-H "X-Auth-Token: ', ['secret assignment']],
        ['p --token= then flag', [extra: '--token=abc123 --threads 4'], '', 'abc123', '--threads 4', ['secret assignment']],
        ['p threads after secret', [extra: '--password hunter2 --threads 4'], '', 'hunter2', '--threads 4', ['secret assignment']],
        ['p -Dpassword', [extra: '-Djavax.net.ssl.keyStorePassword=hunter2 -Xmx2g'], '', 'hunter2', '-Xmx2g', ['secret assignment']],
        // credential shapes under innocent keys
        ['aws secret innocent', [x: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'], '', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', null, ['aws secret key']],
        ['aws secret in string', [x: 'creds wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY here'], '', 'wJalrXUtnFEMI', 'creds ', ['aws secret key']],
        ['sas sig in url', [x: 'https://acct.blob.core.windows.net/c/x?sv=2021&sig=abcDEF123%2Bxyz%3D'], '', 'abcDEF123', null, ['azure sas signature', 'uri']],
        ['sas sig bare', [x: 'sv=2021-08-06&ss=b&srt=co&sig=abcDEF123%2Bxyz%3D'], '', 'abcDEF123', 'sv=2021-08-06&ss=b&srt=co&sig=', ['azure sas signature']],
        ['hf token', [x: HF], '', 'hf_abcdefghij', null, ['hugging face token']],
        ['jwt', [x: 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlX2hlcmU'], '', 'c2lnbmF0dXJlX2hlcmU', null, ['jwt']],
        ['slack xoxp', [x: 'xoxp-' + '1234567890-abcdefghij'], '', 'xoxp-' + '1234567890', null, ['slack token']],
        ['github ghp', [x: 'ghp_' + 'abcdefghijklmnopqrstuvwxyz0123456789'], '', 'ghp_abcdef', null, ['github token']],
        ['gitlab', [x: 'glpat-' + 'abcdefghijklmnopqrstu'], '', 'glpat-abcdef', null, ['gitlab token']],
        ['google api key', [x: GOOGLE_KEY], '', 'AIzaSyA1234567890', null, ['google api key']],
        ['openai sk-proj-', [x: 'sk-proj-' + 'abcdefghijklmnopqrstuvwxyz0123456789ABCD'], '', 'abcdefghijklmnopqrstuvwxyz0123456789', null, ['openai key']],
        ['openai sk-', [x: 'key is sk-' + 'aB3dE5gH7jK9mN1pQ3sT5vW7yZ9bC1dF2gH4jK6mN8pQ0'], '', 'aB3dE5gH7jK9', 'key is ', ['openai key']],
        ['anthropic sk-ant', [x: 'sk-ant-' + 'api03-abcdefghijklmnopqrstuvwxyz0123456789'], '', 'abcdefghijklmnopqrstuvwxyz0123456789', null, ['anthropic key']],
        ['gcp sa json string', [x: '{"type": "service_account", "private_key_id": "abc123def456", "private_key": "' + PROBE_PEM.replace('\n', '\\n') + '", "client_email": "x@p.iam.gserviceaccount.com"}'], '', 'abc123def456', 'service_account', ['private key', 'secret assignment']],
        ['gcp sa json pem', [x: '{"type": "service_account", "private_key": "' + PROBE_PEM.replace('\n', '\\n') + '"}'], '', 'MIIEvQIBADANBgkqhkiG9w0BAQEFAASC', 'service_account', ['private key']],
        ['gcp sa map', [sa: [type: 'service_account', private_key_id: 'abc123def456', private_key: PROBE_PEM]], '', 'MIIEvQIBADANBgkqhkiG9w0BAQEFAASC', 'service_account', ['secret assignment', 'private key']],
        ['pem raw innocent', [x: PROBE_PEM], '', 'MIIEvQIBADANBgkqhkiG9w0BAQEFAASC', null, ['private key']],
        ['url userinfo', [x: 'postgres://bob:hunter2@db.example.org/x'], '', 'hunter2', null, ['uri']],
        ['url userinfo in text', [x: 'connect to https://bob:hunter2@h.example.org/x now'], '', 'hunter2', 'connect to ', ['uri']],
        // a URI is a location (§6) and goes whole, so the probe's 'fp tower url' row is that rule
        ['uri goes whole', [x: 'https://raw.githubusercontent.com/nf-core/test-datasets/x.csv'], '', 'raw.githubusercontent.com', null, ['uri']],
        // map keys
        ['key abs path', [files: ['/home/someone/a.fq': 'a']], '', '/home/someone', null, ['absolute path']],
        ['key ~ path', [files: ['~/data/a.fq': 'a']], '', '~/data', null, ['absolute path']],
        ['key windows', [files: ['C:\\data\\a.fq': 'a']], '', 'C:\\data', null, ['absolute path']],
        ['key secret', [m: [('ghp_' + 'abcdefghijklmnopqrstuvwxyz0123456789'): 1]], '', 'ghp_abcdef', null, ['github token']],
        ['key url userinfo', [m: ['https://bob:hunter2@h/x': 1]], '', 'hunter2', null, ['uri']],
        // ~ paths as values
        ['tilde value', [x: '~/data/x.fq'], '', '~/data', null, ['absolute path']],
        ['tilde user value', [x: '~bob/data/x.fq'], '', '~bob/data', null, ['absolute path']],
        // config text
        ['c ext.args', [:], "process.ext.args = '--password hunter2'", 'hunter2', 'process.ext.args', ['secret assignment']],
        ['c withName ext.args', [:], "process {\n  withName: FOO {\n    ext.args = '--api-key=abc123def --threads 4'\n  }\n}", 'abc123def', '--threads 4', ['secret assignment']],
        ['c singularity ~', [:], "singularity.cacheDir = '~/sing'", '~/sing', 'singularity.cacheDir', ['absolute path']],
        ['c env secret', [:], "env {\n  MY_SECRET = 'abc123'\n}", 'abc123', 'MY_SECRET', ['secret assignment']],
        ['c env DB_PASS', [:], "env.DB_PASS = 'hunter2'", 'hunter2', 'env.DB_PASS', ['secret assignment']],
        ['c env MYSQL_PWD', [:], "env.MYSQL_PWD = 'hunter2'", 'hunter2', 'env.MYSQL_PWD', ['secret assignment']],
        ['c env session token', [:], "env.AWS_SESSION_TOKEN = 'abc123'", 'abc123', 'AWS_SESSION_TOKEN', ['secret assignment']],
        ['c aws secret innocent', [:], "params.x = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'", 'wJalrXUtnFEMI', 'params.x', ['aws secret key']],
        ['c url userinfo', [:], "params.db = 'postgres://bob:hunter2@db/x'", 'hunter2', 'params.db', ['uri']],
        ['c --token dq in string', [:], "params.extra = '--token \"abc123\"'", 'abc123', '--token', ['secret assignment']],
        ['c clusterOptions pw', [:], "process.clusterOptions = '--password hunter2'", 'hunter2', '--password', ['secret assignment']],
        ['c header', [:], "process.ext.args = \"-H 'x-api-key: abc123' --retry 3\"", 'abc123', '--retry 3', ['secret assignment']],
        ['c google key', [:], "env.MAPS = '" + GOOGLE_KEY + "'", 'AIzaSy', 'env.MAPS', ['google api key']],
        ['env map secret', [env: [MY_SECRET: 'abc123']], '', 'abc123', 'MY_SECRET', ['secret assignment']],
        ['env map DB_PASSWORD', [env: [DB_PASSWORD: 'abc123']], '', 'abc123', 'DB_PASSWORD', ['secret assignment']],
        ['env map DB_PASS', [env: [DB_PASS: 'abc123']], '', 'abc123', 'DB_PASS', ['secret assignment']],
        ['json spaced', [x: '{"password" : "x y z"}'], '', 'x y z', '"password"', ['secret assignment']],
        // false positives: kept, and no finding
        ['fp --token=$VAR', [extra: '--token=$TOKEN --threads 4'], '', null, '--token=$TOKEN --threads 4', []],
        ['fp sample', [sample: 'SAMPLE_01_tumor'], '', null, 'SAMPLE_01_tumor', []],
        ['fp rel path', [x: 'data/reads/a.fq'], '', null, 'data/reads/a.fq', []],
        ['fp dot rel', [x: './data/a.fq'], '', null, './data/a.fq', []],
        ['fp dotdot rel', [x: '../data/a.fq'], '', null, '../data/a.fq', []],
        ['fp word value', [label: 'password_file_name'], '', null, 'password_file_name', []],
        ['fp key password_file_name', [password_file_name: 'pw_list.txt'], '', null, 'pw_list.txt', []],
        ['fp cid', [x: 'bafyreid3qkbmdxqjuvfsirlsmpabluxgaky5i3vvzfa762sx63equxipwa'], '', null, 'bafyreid3qkbmdxqjuvfsirlsmpabluxgaky5i3vvzfa762sx63equxipwa', []],
        ['fp sha256', [x: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'], '', null, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', []],
        ['fp git sha', [x: '270f03a1b2c3d4e5f60718293a4b5c6d7e8f9012'], '', null, '270f03a1b2c3d4e5f60718293a4b5c6d7e8f9012', []],
        ['fp version', [x: '3.14.0-rc1'], '', null, '3.14.0-rc1', []],
        ['fp genome', [genome: 'GRCh38'], '', null, 'GRCh38', []],
        ['fp seq 40 mixed', [x: 'ACGTacgtNNACGTacgtACGTacgtACGTacgtACGTacgt'], '', null, 'ACGTacgtNNACGTacgtACGTacgtACGTacgtACGTacgt', []],
        ['fp skip-auth', [extra: '--skip-auth x.bam'], '', null, '--skip-auth x.bam', []],
        ['fp sort-keys', [extra: '--sort-keys in.json'], '', null, '--sort-keys in.json', []],
        ['fp max_tokens', [extra: 'max_tokens=10'], '', null, 'max_tokens=10', []],
        ['fp Basic Statistics', [x: 'Basic Statistics'], '', null, 'Basic Statistics', []],
        ['fp key monkey', [monkey_reads: 'sample1'], '', null, 'sample1', []],
        ['fp key keep_tokens', [keep_tokens: 'yes'], '', null, 'yes', []],
        ['fp key keystone', [keystone: 'k1'], '', null, 'k1', []],
        ['fp key turkey', [turkey: 'gobble'], '', null, 'gobble', []],
        ['fp key keyfile', [keyfile: 'r.key'], '', null, 'r.key', []],
        ['fp --bypass', [extra: '--bypass x.bam --low-pass 0.5'], '', null, '--bypass x.bam --low-pass 0.5', []],
        ['fp passage, compass', [x: 'passage 3 compass n'], '', null, 'passage 3 compass n', []],
        ['fp colon prose', [x: 'note: see docs; SLIDINGWINDOW:4:15'], '', null, 'note: see docs; SLIDINGWINDOW:4:15', []],
        ['fp sk- near miss', [x: 'sk-learn task-0123456789abcdefghij0123456789abcdefghij'], '', null, 'sk-learn task-0123456789abcdefghij0123456789abcdefghij', []],
        ['fp AIza short', [x: 'AIza' + 'SyA1234567890abcdefghijklmnopqrstu'], '', null, 'AIza' + 'SyA1234567890abcdefghijklmnopqrstu', []],
        ['fp email', [x: 'someone@example.org'], '', null, 'someone@example.org', []],
        ['fp --pat', [extra: '--pattern *.fq'], '', null, '--pattern *.fq', []],
        ['fp author', [author: 'Rob Syme'], '', null, 'Rob Syme', []],
        ['fp ratio', [x: '1:2:3'], '', null, '1:2:3', []],
        ['fp time', [x: '12:30'], '', null, '12:30', []],
        ['fp c sample', [:], "params.sample = 'SAMPLE_01'", null, "params.sample = 'SAMPLE_01'", []],
        ['fp c version', [:], "manifest.version = '1.2.3'", null, "manifest.version = '1.2.3'", []],
        ['fp c rel', [:], "params.input = 'assets/samplesheet.csv'", null, "params.input = 'assets/samplesheet.csv'", []],
        ['fp c ext.args threads', [:], "process.ext.args = '--threads 4 --min-len 50'", null, "process.ext.args = '--threads 4 --min-len 50'", []],
        ['fp c keep_tokens', [:], "params.keep_tokens = 'yes'\nparams.monkey_reads = 's1'", null, "params.keep_tokens = 'yes'\nparams.monkey_reads = 's1'", []],
        ['fp c bypass', [:], "env.BYPASS_QC = 'yes'\nprocess.ext.args = '--low-pass 0.5'", null, "env.BYPASS_QC = 'yes'\nprocess.ext.args = '--low-pass 0.5'", []],
    ]

    def 'probe 2 row #label: a secret is redacted and reported, anything else is kept and quiet; the stored block audits clean'() {
        when:
        final Map raw = [kind: 'RunManifest', params: params, config: config]
        final Map block = encoded(manifest((Map) params, config))
        final String stored = String.valueOf(block.params) + '\n' + block.config

        then:
        secret == null || !stored.contains((String) secret)
        keep == null || stored.contains((String) keep)
        RunManifestAudit.audit(CID, raw).collect { it.what } as Set == whats as Set
        RunManifestAudit.audit(CID, block) == []
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config

        where:
        [label, params, config, secret, keep, whats] << PROBE2
    }

    /** Shapes the probe found that this round leaves open: a short `-p` flag's value, a bare base64 user-pass and an AWS session token. */
    @PendingFeature(reason = 'P4 L1 leaves these probe rows open: no rule recognises them')
    def 'probe 2 rows still missed (#label) are redacted'() {
        when:
        final Map block = encoded(manifest([x: value], ''))

        then:
        !String.valueOf(block.params).contains((String) secret)

        where:
        label               | value                                                                                                  | secret
        'mysql -p next'     | 'mysql -u bob -p hunter2'                                                                              | 'hunter2'
        'mysql -p glued'    | 'mysql -u bob -phunter2'                                                                               | 'hunter2'
        'bare base64'       | 'dXNlcjpodW50ZXIy'                                                                                     | 'dXNlcjpodW50ZXIy'
        'aws session token' | 'IQoJb3JpZ2luX2Vj' + 'EJr//////////wEaCXVzLWVhc3QtMSJHMEUCIQD6Gq0xZ1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcdEFGH' | 'IQoJb3JpZ2luX2Vj'
    }

    // ---- R2: the integrator's third scrub probe (p4-revB/scrub-probe3.txt), row by row

    static final String PRESIGNED_STS = 'https://bkt.s3.amazonaws.com/x.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA' + 'ABCDEFGHIJKLMNOP%2F20261025%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Security-Token=IQoJb3JpZ2luX2Vj' + 'EJrSECRETTOKENPART&X-Amz-Signature=deadbeefcafe0123'

    /**
     * Rows as {@link #PROBE2}'s: the probe's 62 rows less 'pending mysql -p', which is the pending row
     * 'mysql -p next' above. Three of its false-positive rows are rules this round keeps, and say so in
     * their label: a `key` segment of its own names a secret (`id_key`), the colon form reads prose
     * (`Missing key: sample`), and a URI is a location (`s3://...`). The last three rows are R2's own.
     */
    static final List<List<Object>> PROBE3 = [
        ['google 39 innocent', [x: GOOGLE_KEY], '', 'AIzaSyA1234567890', null, ['google api key']],
        ['google 39 in string', [extra: "--maps-key ${GOOGLE_KEY} --threads 2".toString()], '', 'AIzaSyA1234567890', '--threads 2', ['secret assignment']],
        ['google 39 config', [:], "params.x = '${GOOGLE_KEY}'".toString(), 'AIzaSyA1234567890', 'params.x', ['google api key']],
        ['openai legacy 48', [x: 'sk-' + 'abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKL'], '', 'abcdefghijklmnopqrstuvwxyz0123456789', null, ['openai key']],
        ['sk-ant in string', [extra: 'ANTHROPIC=sk-ant-' + 'api03-abcdefghijklmnopqrstuvwxyz0123'], '', 'abcdefghijklmnopqrstuvwxyz', 'ANTHROPIC=', ['anthropic key']],
        ['--pass=', [extra: '--pass=hunter2'], '', 'hunter2', '--pass=', ['secret assignment']],
        ['--db-password sp', [extra: '--db-password hunter2'], '', 'hunter2', '--db-password ', ['secret assignment']],
        ['--secret-key sp', [extra: '--secret-key hunter2'], '', 'hunter2', '--secret-key ', ['secret assignment']],
        ['PGPASSWORD=', [extra: 'PGPASSWORD=hunter2 psql'], '', 'hunter2', ' psql', ['secret assignment']],
        ['PGPASS=', [extra: 'PGPASS=hunter2 psql'], '', 'hunter2', 'PGPASS=[secret] psql', ['secret assignment']],
        ['sshpass -p', [extra: 'sshpass -p hunter2 scp a b'], '', 'hunter2', 'sshpass -p [secret] scp a b', ['secret assignment']],
        ['Authorization Basic', [extra: "-H 'Authorization: Basic dXNlcjpodW50ZXIy'"], '', 'dXNlcjpodW50ZXIy', 'Authorization: Basic ', ['basic auth']],
        ['Authorization Bearer dq', [extra: '-H "Authorization: Bearer abcDEF123456"'], '', 'abcDEF123456', 'Authorization: Bearer ', ['bearer token']],
        ['Proxy-Authorization', [extra: "-H 'Proxy-Authorization: token abc123def'"], '', 'abc123def', 'Proxy-Authorization: token ', ['secret assignment']],
        ['X-Vault-Token', [extra: "-H 'X-Vault-Token: s.abc123def456'"], '', 'abc123def456', 'X-Vault-Token: ', ['secret assignment']],
        ['Cookie session', [extra: "-H 'Cookie: session=abc123def456'"], '', 'abc123def456', "-H 'Cookie: [secret]'", ['secret assignment']],
        ['api_key: yaml', [extra: 'api_key: abc123def'], '', 'abc123def', 'api_key: ', ['secret assignment']],
        ['token:nospace', [extra: 'token:abc123def'], '', 'abc123def', 'token:', ['secret assignment']],
        ['json nospace', [x: '{"password":"hunter2"}'], '', 'hunter2', '"password":', ['secret assignment']],
        ['client_secret=', [extra: 'client_id=a client_secret=abc123def'], '', 'abc123def', 'client_id=a client_secret=', ['secret assignment']],
        ['npm authToken', [extra: '//registry.npmjs.org/:_authToken=abc123def'], '', 'abc123def', null, ['secret assignment', 'absolute path']],
        ['azure conn AccountKey', [x: 'DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=abc123DEF456+xyz==;EndpointSuffix=core.windows.net'], '', 'abc123DEF456', 'EndpointSuffix=core.windows.net', ['secret assignment']],
        ['azure conn SAS', [x: 'BlobEndpoint=https://a.blob.core.windows.net/;SharedAccessSignature=sv=2021-08-06&ss=b&sig=abcDEF123%2Bxyz%3D'], '', 'abcDEF123', 'BlobEndpoint=', ['azure sas signature', 'uri']],
        ['presigned STS in string', [extra: "--url ${PRESIGNED_STS} --x 1".toString()], '', 'IQoJb3JpZ2lu', '--x 1', ['aws key id', 'secret assignment', 'uri']],
        ['presigned STS value', [x: PRESIGNED_STS], '', 'IQoJb3JpZ2lu', null, ['aws key id', 'secret assignment', 'uri']],
        ['bare X-Amz-Security-Token', [extra: 'X-Amz-Security-Token=IQoJb3JpZ2luX2VjEJr'], '', 'IQoJb3JpZ2lu', 'X-Amz-Security-Token=', ['secret assignment']],
        ['c aws block', [:], "aws {\n  accessKey = 'AKIA" + "ABCDEFGHIJKLMNOP'\n  secretKey = 'abc123secretvalue'\n}", 'abc123secretvalue', 'secretKey', ['aws key id', 'secret assignment']],
        ['c aws sessionToken', [:], "aws.client.sessionToken = 'IQoJb3JpZ2luX2VjEJr'", 'IQoJb3JpZ2lu', 'aws.client.sessionToken', ['secret assignment']],
        ['c tower token', [:], "tower.accessToken = 'eyJ0aWQiOiAxfQ.abcdef'", 'abcdef', 'tower.accessToken', ['seqera token']],
        ['c github token param', [:], "params.github_token = 'gho_abc'", 'gho_abc', 'params.github_token', ['secret assignment']],
        ['env map AWS_SESSION_TOKEN', [env: [AWS_SESSION_TOKEN: 'IQoJb3JpZ2luX2VjEJr']], '', 'IQoJb3JpZ2lu', 'AWS_SESSION_TOKEN', ['secret assignment']],
        ['url pw slash', [x: 'connect https://bob:pa/ss@h.example.org now'], '', 'ss@h', 'connect ', ['uri']],
        // rules this round keeps (see above)
        ['kept rule: id_key is secret-named', [id_key: 'sample'], '', 'sample', 'id_key', ['secret assignment']],
        ['kept rule: the colon form reads prose', [x: 'Missing key: sample'], '', 'sample', 'Missing key: ', ['secret assignment']],
        ['kept rule: a URI is a location', [genome: 'GRCh38', igenomes_base: 's3://ngi-igenomes/igenomes'], '', 'ngi-igenomes', 'GRCh38', ['uri']],
        // false positives: kept, and no finding
        ['fp c secrets ref', [:], 'params.x = secrets.MY_TOKEN', null, 'params.x = secrets.MY_TOKEN', []],
        ['fp nfcore input rel', [input: 'samplesheet.csv'], '', null, 'samplesheet.csv', []],
        ['fp aligner', [aligner: 'star_salmon'], '', null, 'star_salmon', []],
        ['fp salmon args', [extra_salmon_quant_args: '--seqBias --gcBias'], '', null, '--seqBias --gcBias', []],
        ['fp star twopass', [extra_star_align_args: '--twopassMode Basic --outSAMattrIHstart 0'], '', null, '--twopassMode Basic --outSAMattrIHstart 0', []],
        ['fp trimmomatic', [extra: 'SLIDINGWINDOW:4:15 MINLEN:36'], '', null, 'SLIDINGWINDOW:4:15 MINLEN:36', []],
        ['fp bcftools PASS', [extra: "-f PASS -i 'QUAL>30'"], '', null, "-f PASS -i 'QUAL>30'", []],
        ['fp FILTER=PASS', [extra: 'FILTER=PASS'], '', null, 'FILTER=PASS', []],
        ['fp key pass_filter', [pass_filter: 'true'], '', null, 'pass_filter=true', []],
        ['fp key only_pass', [vcf_pass_only: 'yes'], '', null, 'vcf_pass_only=yes', []],
        ['fp key two_pass', [two_pass: 'yes'], '', null, 'two_pass=yes', []],
        ['fp key bypass_qc', [bypass_qc: 'yes'], '', null, 'bypass_qc=yes', []],
        ['fp key keyword', [keyword: 'tumor'], '', null, 'keyword=tumor', []],
        ['fp key passes', [passes: '3'], '', null, 'passes=3', []],
        ['fp --passes', [extra: '--passes 3'], '', null, '--passes 3', []],
        ['fp --pat', [extra: '--pat GATC'], '', null, '--pat GATC', []],
        ['fp --keep-tokens', [extra: '--keep-tokens 3'], '', null, '--keep-tokens 3', []],
        ['fp tokenizer', [extra: '--tokenizer bpe'], '', null, '--tokenizer bpe', []],
        ['fp author: name', [x: 'author: Rob'], '', null, 'author: Rob', []],
        ['fp prose password', [x: 'Set the password in the vault'], '', null, 'Set the password in the vault', []],
        ['fp nf-core multiqc title', [multiqc_title: 'Project: pass/fail summary'], '', null, 'Project: pass/fail summary', []],
        ['fp sk-learn', [x: 'sk-learn-model-v1'], '', null, 'sk-learn-model-v1', []],
        ['fp task-', [x: 'task-abcdefghijklmnopqrstuvwxyz0123456789ABCD'], '', null, 'task-abcdefghijklmnopqrstuvwxyz0123456789ABCD', []],
        ['fp AIza short', [x: 'AIzawords'], '', null, 'AIzawords', []],
        ['fp c ext.args -p threads', [:], "process.ext.args = '-p 8 --very-sensitive'", null, "process.ext.args = '-p 8 --very-sensitive'", []],
        ['fp c withName pass', [:], "process {\n  withName: 'BCFTOOLS_VIEW' {\n    ext.args = '--apply-filters PASS'\n  }\n}", null, "ext.args = '--apply-filters PASS'", []],
        // R2: MinKNOW's output directories, and pass beside a filter word
        ['fp key fastq_pass', [fastq_pass: 'run1/fastq_pass', fastq_fail: 'run1/fastq_fail', bam_pass: 'run1/bam_pass'], '', null, 'bam_pass=run1/bam_pass, fastq_fail=run1/fastq_fail, fastq_pass=run1/fastq_pass', []],
        ['fp --fastq_pass', [extra: '--fastq_pass run1/fastq_pass --pass-only'], '', null, '--fastq_pass run1/fastq_pass --pass-only', []],
        ['fp c fastq_pass', [:], "params.fastq_pass = 'run1/fastq_pass'\nparams.pass_filter = 'yes'", null, "params.fastq_pass = 'run1/fastq_pass'\nparams.pass_filter = 'yes'", []],
    ]

    def 'probe 3 row #label: a secret is redacted and reported, anything else is kept and quiet; the stored block audits clean'() {
        when:
        final Map raw = [kind: 'RunManifest', params: params, config: config]
        final Map block = encoded(manifest((Map) params, config))
        final String stored = String.valueOf(block.params) + '\n' + block.config

        then:
        secret == null || !stored.contains((String) secret)
        keep == null || stored.contains((String) keep)
        RunManifestAudit.audit(CID, raw).collect { it.what } as Set == whats as Set
        RunManifestAudit.audit(CID, block) == []
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config

        where:
        [label, params, config, secret, keep, whats] << PROBE3
    }

    def 'a secret-named key loses its value and is a finding; a key holding a secret word only inside a word keeps it (#name)'() {
        when:
        final Map params = [(name): 'abc123']
        final String config = "${name} = 'abc123'".toString()
        final Map block = encoded(manifest(params, config))
        final List<RunManifestAudit.Finding> raw = RunManifestAudit.audit(CID, [kind: 'RunManifest', params: params, config: config])

        then:
        block.params == [(name): secret ? '[secret]' : 'abc123']
        block.config == "${name} = '${secret ? '[secret]' : 'abc123'}'".toString()
        raw.collect { it.what } as Set == (secret ? ['secret assignment'] : []) as Set
        RunManifestAudit.audit(CID, block) == []

        where:
        name                       | secret
        'api_key'                  | true
        'apiKey'                   | true
        'ssh-key'                  | true
        'key'                      | true
        'apikey'                   | true
        'AWS_SECRET_ACCESS_KEY'    | true
        'db_password'              | true
        'DB_PASS'                  | true
        'MYSQL_PWD'                | true
        'passphrase'               | true
        'keyStorePassword'         | true
        'GITHUB_PAT'               | true
        'Authorization'            | true
        'monkey_reads'             | false
        'keep_tokens'              | false
        'keystone'                 | false
        'turkey'                   | false
        'keyfile'                  | false
        'keyword'                  | false
        'max_tokens'               | false
        'sort_keys'                | false
        'secretome_db'             | false
        'bypass_qc'                | false
        'passage'                  | false
        'compass'                  | false
        'password_file_name'       | false
        'author'                   | false
    }
}
