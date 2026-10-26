package robsyme.cas.core

import spock.lang.Specification

/**
 * DESIGN.md §6: the reviewer's fourth scrub probe (p4f-r1/scrub-probe4.txt), row by row, with the
 * second follow-up round's own rows after it. Each row goes through a RunManifest and a DAG-CBOR
 * round trip, as {@link RunManifestAuditTest}'s probe 2 and 3 tables do.
 */
class ScrubProbe4Test extends Specification {

    static final Cid CID = DagCbor.cidOf(DagCbor.encode([kind: 'RunManifest']))

    /**
     * [label, params, config text, text that must not survive (null when nothing is secret), text that
     * must survive (or null), the kinds the audit reports on the raw values]. The probe's false-positive
     * rows whose value is a path or a URI keep it as a location: the value goes as one, and the audit
     * reports a location, not a secret.
     */
    static final List<List<Object>> PROBE4 = [
        // one-word database and login tool names
        ['PGPASS=', [extra: 'PGPASS=hunter2 psql'], '', 'hunter2', 'PGPASS=[secret] psql', ['secret assignment']],
        ['PGPASS env map', [env: [PGPASS: 'hunter2']], '', 'hunter2', 'PGPASS=[secret]', ['secret assignment']],
        ['c env.PGPASS', [:], "env.PGPASS = 'hunter2'", 'hunter2', "env.PGPASS = '[secret]'", ['secret assignment']],
        ['DBPASS=', [extra: 'DBPASS=hunter2 run'], '', 'hunter2', 'DBPASS=[secret] run', ['secret assignment']],
        ['MYSQLPWD=', [extra: 'MYSQLPWD=hunter2 run'], '', 'hunter2', 'MYSQLPWD=[secret] run', ['secret assignment']],
        ['SSHPASS= (sshpass -e)', [extra: 'SSHPASS=hunter2 sshpass -e scp a b'], '', 'hunter2', 'SSHPASS=[secret] sshpass -e scp a b', ['secret assignment']],
        // a read, only or filter word before pass is no qualifier: secrets beat false positives
        ['DB_READ_PASS=', [extra: 'DB_READ_PASS=hunter2 run'], '', 'hunter2', 'DB_READ_PASS=[secret] run', ['secret assignment']],
        ['READ_ONLY_PASS=', [extra: 'READ_ONLY_PASS=hunter2 run'], '', 'hunter2', 'READ_ONLY_PASS=[secret] run', ['secret assignment']],
        ['key db_read_pass', [db_read_pass: 'hunter2'], '', 'hunter2', 'db_read_pass=[secret]', ['secret assignment']],
        ['key qc_db_pass', [qc_db_pass: 'hunter2'], '', 'hunter2', 'qc_db_pass=[secret]', ['secret assignment']],
        ['FILTER_PASS= env', [extra: 'FILTER_PASS=hunter2'], '', 'hunter2', 'FILTER_PASS=[secret]', ['secret assignment']],
        // sshpass -p
        ['sshpass -p', [extra: 'sshpass -p hunter2 scp a b'], '', 'hunter2', 'sshpass -p [secret] scp a b', ['secret assignment']],
        ['sshpass -phunter2', [extra: 'sshpass -phunter2 scp a b'], '', 'hunter2', 'sshpass -p[secret] scp a b', ['secret assignment']],
        ['sshpass -v -p q', [extra: "sshpass -v -p 'hunter 2' ssh h"], '', 'hunter 2', "sshpass -v -p '[secret]' ssh h", ['secret assignment']],
        ['sshpass -p dq', [extra: 'sshpass -p "hunter2" ssh h'], '', 'hunter2', 'sshpass -p "[secret]" ssh h', ['secret assignment']],
        ['sshpass -P x -p', [extra: 'sshpass -P assword -p hunter2 ssh h'], '', 'hunter2', 'sshpass -P assword -p [secret] ssh h', ['secret assignment']],
        ['sshpass tab -p', [extra: 'sshpass\t-p\thunter2 ssh h'], '', 'hunter2', 'sshpass\t-p\t[secret] ssh h', ['secret assignment']],
        ['c sshpass ext.args', [:], "process.ext.prefix = 'sshpass -p hunter2 rsync'", 'hunter2', "process.ext.prefix = 'sshpass -p [secret] rsync'", ['secret assignment']],
        // cookies
        ['Cookie session', [extra: "-H 'Cookie: session=abc123def456'"], '', 'abc123def456', "-H 'Cookie: [secret]'", ['secret assignment']],
        ['Cookie two pairs', [extra: "-H 'Cookie: a=1; sid=abc123def456'"], '', 'abc123def456', "-H 'Cookie: [secret]'", ['secret assignment']],
        ['cookie lower dq', [extra: '-H "cookie: sid=abc123def456"'], '', 'abc123def456', '-H "cookie: [secret]"', ['secret assignment']],
        ['Set-Cookie', [extra: 'Set-Cookie: id=abc123def456; Path=/; HttpOnly'], '', 'abc123def456', 'Set-Cookie: [secret]', ['secret assignment']],
        ['Cookie value', [x: 'Cookie: session=abc123def456'], '', 'abc123def456', 'Cookie: [secret]', ['secret assignment']],
        ['Cookie json header', [x: '{"Cookie": "session=abc123def456"}'], '', 'abc123def456', '{"Cookie": "[secret]"}', ['secret assignment']],
        ['curl -b', [extra: "curl -b 'session=abc123def456' https://h"], '', 'abc123def456', "curl -b '[secret]' [redacted-location]", ['secret assignment', 'uri']],
        ['curl --cookie', [extra: "curl --cookie 'session=abc123def456' https://h"], '', 'abc123def456', "curl --cookie '[secret]' [redacted-location]", ['secret assignment', 'uri']],
        // pat
        ['--pat token', [extra: '--pat ghp_' + 'abcdefghijklmnopqrstuvwxyz0123456789'], '', 'abcdefghijklmnopqrstuvwxyz0123456789', '--pat [secret]', ['secret assignment']],
        ['--pat abc123', [extra: '--pat abc123def'], '', 'abc123def', '--pat [secret]', ['secret assignment']],
        ['--pat acgtacgtxyz', [extra: '--pat gatcqzx'], '', 'gatcqzx', '--pat [secret]', ['secret assignment']],
        ['github_pat key ACGT', [github_pat: 'ACGT'], '', 'ACGT', 'github_pat=[secret]', ['secret assignment']],
        ['GITHUB_PAT=ACGT', [extra: 'GITHUB_PAT=GATTACA'], '', 'GATTACA', 'GITHUB_PAT=[secret]', ['secret assignment']],
        // false positives the R2 round fixed
        ['fp pass_filter key', [pass_filter: 'yes'], '', null, 'pass_filter=yes', []],
        ['fp vcf_pass_only key', [vcf_pass_only: 'true'], '', null, 'vcf_pass_only=true', []],
        ['fp --pass_filter flag', [extra: '--pass_filter yes --threads 2'], '', null, '--pass_filter yes --threads 2', []],
        ['fp --vcf_pass_only flag', [extra: '--vcf_pass_only true'], '', null, '--vcf_pass_only true', []],
        ['fp --pat GATC', [extra: '--pat GATC'], '', null, '--pat GATC', []],
        ['fp --pat q', [extra: "--pat 'AGATCGGAAGAGC'"], '', null, "--pat 'AGATCGGAAGAGC'", []],
        ['fp --pat=gatc', [extra: '--pat=gatc'], '', null, '--pat=gatc', []],
        ['fp pat key', [pat: 'GATC'], '', null, 'pat=GATC', []],
        ['fp fastq_pass key', [fastq_pass: '/data/run1/fastq_pass'], '', '/data/run1', 'fastq_pass=[redacted-location]', ['absolute path']],
        ['fp fastq_pass= str', [extra: 'fastq_pass=/data/run1/fastq_pass'], '', '/data/run1', 'fastq_pass=[redacted-location]', ['absolute path']],
        ['fp --fastq_pass flag', [extra: '--fastq_pass /data/run1/fastq_pass'], '', '/data/run1', '--fastq_pass [redacted-location]', ['absolute path']],
        ['fp c fastq_pass', [:], "params.fastq_pass = '/data/run1/fastq_pass'", '/data/run1', "params.fastq_pass = '[redacted-location]'", ['absolute path']],
        ['fp bam_pass', [bam_pass: 's3://b/run/bam_pass'], '', 's3://b', 'bam_pass=[redacted-location]', ['uri']],
        ['fp pod5_pass', [pod5_pass: 'pod5_pass/'], '', null, 'pod5_pass=pod5_pass/', []],
        ['fp qc_pass', [qc_pass: '0.9'], '', null, 'qc_pass=0.9', []],
        ['fp pass_reads', [pass_reads: 'reads.fq'], '', null, 'pass_reads=reads.fq', []],
        // earlier false positives, kept
        ['fp low_pass', [low_pass: '0.1'], '', null, 'low_pass=0.1', []],
        ['fp two_pass', [extra: '--two_pass true'], '', null, '--two_pass true', []],
        ['fp bypass_qc', [bypass_qc: 'true'], '', null, 'bypass_qc=true', []],
        ['fp compass', [compass: 'north'], '', null, 'compass=north', []],
        ['fp env PWD', [env: [PWD: '/home/rob/x']], '', '/home/rob', 'PWD=[redacted-location]', ['absolute path']],
        ['fp env OLDPWD', [env: [OLDPWD: '/home/rob/y']], '', '/home/rob', 'OLDPWD=[redacted-location]', ['absolute path']],
        ['fp ssh -p port', [extra: 'ssh -p 2222 host'], '', null, 'ssh -p 2222 host', []],
        ['fp sshpass -f', [extra: 'sshpass -f /home/rob/.pw ssh h'], '', '/home/rob', 'sshpass -f [redacted-location] ssh h', ['absolute path']],
        ['fp sshpass -p $VAR', [extra: 'sshpass -p $SSH_PW ssh h'], '', null, 'sshpass -p $SSH_PW ssh h', []],
        ['fp cookie prose', [x: 'cookie: chocolate chip'], '', null, 'cookie: chocolate chip', []],
        ['fp cookie_cutter', [cookie_cutter: 'gh:org/tpl'], '', null, 'cookie_cutter=gh:org/tpl', []],
        ['fp Cookie: $VAR', [extra: '-H "Cookie: $COOKIE"'], '', null, '-H "Cookie: $COOKIE"', []],
        ['fp pattern', [pattern: '*.fq'], '', null, 'pattern=*.fq', []],
        ['fp --pattern', [extra: '--pattern ACGT'], '', null, '--pattern ACGT', []],
        ['fp path', [path: '/data/x'], '', '/data/x', 'path=[redacted-location]', ['absolute path']],
        ['fp FILTER=PASS', [extra: 'FILTER=PASS'], '', null, 'FILTER=PASS', []],
        ['fp -f PASS', [extra: '-f PASS'], '', null, '-f PASS', []],
        ['fp monkey_reads', [monkey_reads: 'sample1'], '', null, 'monkey_reads=sample1', []],

        // second follow-up: the shapes B1 named, in config text
        ['c params.db_read_pass', [:], "params.db_read_pass = 'hunter2'", 'hunter2', "params.db_read_pass = '[secret]'", ['secret assignment']],
        ['c env READ_ONLY_PASS', [:], "env { READ_ONLY_PASS = 'hunter2' }", 'hunter2', "READ_ONLY_PASS = '[secret]'", ['secret assignment']],
        ['--db-read-pass', [extra: '--db-read-pass hunter2'], '', 'hunter2', '--db-read-pass [secret]', ['secret assignment']],
        // a plain boolean or number is never a secret
        ['fp filter_pass true', [filter_pass: true], '', null, 'filter_pass=true', []],
        ['fp filter_pass str', [filter_pass: 'true'], '', null, 'filter_pass=true', []],
        ['fp only_pass False', [only_pass: 'False'], '', null, 'only_pass=False', []],
        ['fp read_pass 8', [read_pass: 8], '', null, 'read_pass=8', []],
        ['fp db_pass 0.05', [db_pass: 0.05], '', null, 'db_pass=0.05', []],
        ['fp api_key -1 str', [api_key: '-1'], '', null, 'api_key=-1', []],
        ['fp FILTER_PASS=true', [extra: 'FILTER_PASS=true ONLY_PASS=0 --read-pass 1e-5 --password -1'], '', null, 'FILTER_PASS=true ONLY_PASS=0 --read-pass 1e-5 --password -1', []],
        ['fp json number', [x: '{"filter_pass": 0.05, "token": "8"}'], '', null, '{"filter_pass": 0.05, "token": "8"}', []],
        ['fp -D number', [x: '-Dhttp.proxyPassword=12 -Xmx2g'], '', null, '-Dhttp.proxyPassword=12 -Xmx2g', []],
        ['fp c filter_pass', [:], "params.filter_pass = true\nparams.read_only_pass = 'false'\nparams.pass = 3\nparams.tokens = [1, 2.5, true]", null, "params.filter_pass = true\nparams.read_only_pass = 'false'\nparams.pass = 3\nparams.tokens = [1, 2.5, true]", []],
        ['fp tokens list', [tokens: [1, 'true', false, -1]], '', null, 'tokens=[1, true, false, -1]', []],
        ['number next to a secret', [tokens: [1, 'abc']], "params.passwords = [2, 'abc']", 'abc', "params.passwords = [2, '[secret]']", ['secret assignment']],
        // the minors: curl and JSON cookies, sshpass -P, pat as a params key, PWD as a path
        ['curl --cookie bare', [extra: 'curl --cookie session=abc123def456 https://h'], '', 'abc123def456', 'curl --cookie [secret] [redacted-location]', ['secret assignment', 'uri']],
        ['curl --cookie=', [extra: 'curl --cookie=session=abc123def456'], '', 'abc123def456', 'curl --cookie=[secret]', ['secret assignment']],
        ['curl -b two pairs', [extra: 'curl -b "a=1; sid=abc123def456" -o x'], '', 'abc123def456', 'curl -b "[secret]" -o x', ['secret assignment']],
        ['fp curl -b file', [extra: 'curl -b cookies.txt -c jar.txt https://h'], '', null, 'curl -b cookies.txt -c jar.txt [redacted-location]', ['uri']],
        ['fp bedtools -b', [extra: 'bedtools intersect -a a.bed -b b.bed'], '', null, 'bedtools intersect -a a.bed -b b.bed', []],
        ['fp samtools -b', [extra: 'samtools view -b -h x.sam'], '', null, 'samtools view -b -h x.sam', []],
        ['json set-cookie', [x: "{'Set-Cookie': 'id=abc123def456; Path=/'}"], '', 'abc123def456', "{'Set-Cookie': '[secret]'}", ['secret assignment']],
        ['c json cookie', [:], 'params.headers = \'{"Cookie": "sid=abc123def456"}\'', 'abc123def456', '{"Cookie": "[secret]"}', ['secret assignment']],
        ['sshpass -P glued', [extra: "sshpass -Ppassword: -p hunter2 ssh h"], '', 'hunter2', 'sshpass -Ppassword: -p [secret] ssh h', ['secret assignment']],
        ['sshpass -P quoted -v', [extra: "sshpass -P 'pass phrase' -v -p hunter2 ssh h"], '', 'hunter2', "sshpass -P 'pass phrase' -v -p [secret] ssh h", ['secret assignment']],
        ['fp c pat', [:], "params.pat = 'GATC'", null, "params.pat = 'GATC'", []],
        ['fp env PAT', [env: [PAT: 'AGATCGGAAGAGC']], '', null, 'PAT=AGATCGGAAGAGC', []],
        ['pat key token', [pat: 'abc123'], "params.pat = 'abc123'", 'abc123', "params.pat = '[secret]'", ['secret assignment']],
        ['fp c env PWD', [:], "env.PWD = '/home/rob/x'", '/home/rob', "env.PWD = '[redacted-location]'", ['absolute path']],
        ['fp PWD= str', [extra: 'PWD=/home/rob/x'], '', '/home/rob', 'PWD=[redacted-location]', ['absolute path']],
        ['PWD word', [env: [PWD: 'hunter2']], '', 'hunter2', 'PWD=[secret]', ['secret assignment']],
        // a location is kept for the path rule only when that rule takes all of it
        ['path then colon', [password: '/abc:hunter2'], "password = '/abc:hunter2'", 'hunter2', "password = '[secret]'", ['secret assignment', 'absolute path']],
        ['path then space', [password: '/abc hunter2'], "password = '/abc hunter2'", 'hunter2', "password = '[secret]'", ['secret assignment', 'absolute path']],
        ['path in json', [x: '{"password": "/abc:hunter2"}'], '', 'hunter2', '{"password": "[secret]"}', ['secret assignment']],
        ['fp uri under a key', [db_password: 's3://bucket/x'], "params.db_password = 's3://bucket/x'", 'bucket', "params.db_password = '[redacted-location]'", ['uri']],
    ]

    def 'probe 4 row #label: a secret is redacted and reported, anything else is kept and quiet; the stored block audits clean'() {
        when:
        final Map raw = [kind: 'RunManifest', params: params, config: config]
        final Map block = recorded((Map) params, (String) config)
        final String stored = String.valueOf(block.params) + '\n' + block.config

        then:
        secret == null || !stored.contains((String) secret)
        keep == null || stored.contains((String) keep)
        RunManifestAudit.audit(CID, raw).collect { it.what } as Set == whats as Set
        RunManifestAudit.audit(CID, block) == []
        Records.scrub(block.params) == block.params
        Records.scrubConfigText((String) block.config) == block.config

        where:
        [label, params, config, secret, keep, whats] << PROBE4
    }

    /** The params and config text a RunManifest records, after a DAG-CBOR round trip. */
    private static Map recorded(Map params, String config) {
        final RunManifest manifest = new RunManifest(RecordsTest.manifestArgs() + [params: params, config: config])
        return (Map) DagCbor.decode(DagCbor.encode(manifest.toCbor()))
    }
}
