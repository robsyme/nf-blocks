package robsyme.cas.lineage

import java.nio.file.Path
import java.time.LocalDate
import java.time.OffsetDateTime

import nextflow.Session
import nextflow.lineage.model.v1beta1.Checksum
import nextflow.lineage.model.v1beta1.DataPath
import nextflow.lineage.model.v1beta1.FileOutput
import nextflow.lineage.model.v1beta1.Parameter
import nextflow.lineage.model.v1beta1.TaskOutput
import nextflow.lineage.model.v1beta1.TaskRun
import nextflow.lineage.model.v1beta1.Workflow
import nextflow.lineage.model.v1beta1.WorkflowOutput
import nextflow.lineage.model.v1beta1.WorkflowRun
import nextflow.script.ScriptBinding
import nextflow.script.WorkflowMetadata
import robsyme.cas.core.Records
import spock.lang.Specification

/**
 * The location scrub CasLinStore applies to Nextflow's own records before
 * DefaultLinStore writes them under nf/ (DESIGN.md §10). Every field Nextflow
 * reads back to resolve lineage (keys, lid:// and cas:// references,
 * checksums, FileOutput.path) survives; host paths, the home directory and the
 * user name do not.
 */
class NfRecordScrubTest extends Specification {

    static final String LOC = '[redacted-location]'
    static final String USER = '[redacted-user]'
    static final String REPO = 'https://github.com/nf-core/demo'

    static final Checksum SUM = new Checksum('abc123', 'nextflow', 'standard')

    final NfRecordScrub scrub = new NfRecordScrub(REPO, ['/home/rob/project/work', '/home/rob/project', '/home/rob'])

    def 'a workflow run keeps its keys and identity and loses every host location in metadata'() {
        given:
        final run = new WorkflowRun(
            workflow: new Workflow([], REPO, 'c0ffee'),
            sessionId: 'sess-1',
            name: 'nice_curie',
            params: [],
            config: [:],
            metadata: [
                runName    : 'nice_curie',
                homeDir    : '/home/rob',
                launchDir  : '/home/rob/project',
                workDir    : 's3://lab-bucket/work',
                projectDir : '/home/rob/project',
                scriptFile : '/home/rob/project/main.nf',
                commandLine: '/opt/nf/launch.sh run main.nf -c /home/rob/cas.config --input /data/x.csv',
                configFiles: ['file:///home/rob/.nextflow/config', 'file:///home/rob/project/nextflow.config'],
                userName   : 'rob',
                outputDir  : 'cas://lab',
                repository : REPO,
                commitId   : 'c0ffee',
                revision   : 'main',
                profile    : 'test,docker',
                nextflow   : [version: '26.04.6', build: 12646],
            ])

        when:
        final out = scrub.scrub(run) as WorkflowRun
        final Map meta = out.metadata

        then: 'the home, launch, work and project directories, the script and the config files are redacted'
        meta.homeDir == LOC
        meta.launchDir == LOC
        meta.workDir == LOC
        meta.projectDir == LOC
        meta.scriptFile == LOC
        meta.configFiles == [LOC, LOC]
        and: 'the command line keeps its shape without the paths, even when it starts with one'
        meta.commandLine == "${LOC} run main.nf -c ${LOC} --input ${LOC}".toString()
        and: 'the user name goes even when it is not this JVM user'
        meta.userName == USER
        and: 'what names the run and the pipeline stays, as the RunManifest keeps it'
        meta.runName == 'nice_curie'
        meta.outputDir == 'cas://lab'
        meta.repository == REPO
        meta.commitId == 'c0ffee'
        meta.revision == 'main'
        meta.profile == 'test,docker'
        meta.nextflow == [version: '26.04.6', build: 12646]
        out.sessionId == 'sess-1'
        out.name == 'nice_curie'
        out.workflow.repository == REPO
        out.workflow.commitId == 'c0ffee'
    }

    def 'a workflow run config keeps every key and its numbers, and loses its host paths and secrets'() {
        given:
        final run = new WorkflowRun(
            workflow: new Workflow([], null, null),
            config: [
                cas      : [stores: [lab: [location: '/home/rob/store'], org: [location: 's3://org-bucket/cas']]],
                lineage  : [enabled: true, store: [location: 'cas://lab']],
                workDir  : 'work',
                poolSize : 11,
                process  : [executor: 'local', 'withName:ALIGN': [ext: [args: '--ref /data/g.fa --seed 1']]],
                env      : [MY_API_TOKEN: 'abc123', TMPDIR: '/scratch/rob'],
                plugins  : ['nf-blocks@0.9.0-beta.1'],
            ])

        when:
        final Map config = (scrub.scrub(run) as WorkflowRun).config

        then: 'keys stay, so the record keeps Nextflow\'s shape'
        config.keySet() == ['cas', 'lineage', 'workDir', 'poolSize', 'process', 'env', 'plugins'] as Set
        and: 'member locations go, the portable store uri stays'
        config.cas == [stores: [lab: [location: LOC], org: [location: LOC]]]
        config.lineage == [enabled: true, store: [location: 'cas://lab']]
        and: 'a relative work dir and a number are left as they were'
        config.workDir == 'work'
        config.poolSize == 11
        config.poolSize instanceof Integer
        and: 'an absolute path inside a flag string goes, the flag stays'
        config.process == [executor: 'local', 'withName:ALIGN': [ext: [args: "--ref ${LOC} --seed 1".toString()]]]
        and: 'a secret-named key loses its value'
        config.env == [MY_API_TOKEN: '[secret]', TMPDIR: LOC]
        config.plugins == ['nf-blocks@0.9.0-beta.1']
    }

    def 'workflow run params lose host paths and keep lid and cas references'() {
        given:
        final run = new WorkflowRun(
            workflow: new Workflow([
                new DataPath('file:///home/rob/project/main.nf', SUM),
                new DataPath("${REPO}/tree/c0ffee/modules/align/main.nf".toString(), SUM),
            ], REPO, 'c0ffee'),
            params: [
                new Parameter('String', 'input', 'file:///home/rob/samples.csv'),
                new Parameter('String', 'outdir', 's3://lab-bucket/results'),
                new Parameter('String', 'reads', 'lid://8f5496/reads/A.fq'),
                new Parameter('String', 'genome', 'cas://bafkreia/genome.fa'),
                new Parameter('String', 'sample', 'A'),
                new Parameter('Integer', 'depth', 30),
                new Parameter('String', 'db_password', 'hunter2'),
            ])

        when:
        final out = scrub.scrub(run) as WorkflowRun

        then:
        out.params*.value == [LOC, LOC, 'lid://8f5496/reads/A.fq', 'cas://bafkreia/genome.fa', 'A', 30, '[secret]']
        out.params*.name == ['input', 'outdir', 'reads', 'genome', 'sample', 'depth', 'db_password']
        out.params*.type == ['String', 'String', 'String', 'String', 'String', 'Integer', 'String']
        and: 'a local script path goes; one under the run\'s own repository stays, and both keep their checksum'
        out.workflow.scriptFiles*.path == [LOC, "${REPO}/tree/c0ffee/modules/align/main.nf".toString()]
        out.workflow.scriptFiles*.checksum == [SUM, SUM]
    }

    def 'a task run keeps its references and loses host paths, without touching the values the run holds'() {
        given:
        final Map meta = [id: 'A', ref: '/home/rob/refs/g.fa', nested: [kit: 'truseq', dir: '/data/raw']]
        final task = new TaskRun(
            sessionId: 'sess-1',
            name: 'ALIGN (A)',
            codeChecksum: SUM,
            script: "bwa mem /home/rob/project/assets/ref.fa reads.fq | sed 's/.* //' > out.sam\nawk '/^>/{print}' x > /data/elsewhere.txt",
            input: [
                new Parameter('path', 'reads', [new DataPath('file:///home/rob/data/A.fq', SUM), 'lid://3bb53f/out.txt']),
                new Parameter('val', 'meta', meta),
                new Parameter('val', 'note', '/home/rob/notes'),
            ],
            container: 'quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8',
            conda: 'file:///home/rob/project/env.yml',
            spack: 'bwa@0.7.17',
            architecture: 'linux/arm64',
            globalVars: ['task.ext.args': '--tmp /scratch/rob/tmp', 'task.ext.prefix': 'A'],
            binEntries: [new DataPath('file:///home/rob/project/bin/helper.py', SUM)],
            workflowRun: 'lid://8f5496',
            moduleId: 'nf-core/bwa@1.0.0')

        when:
        final out = scrub.scrub(task) as TaskRun

        then: 'lid:// inputs, checksums and the run reference survive'
        out.input[0].value == [new DataPath(LOC, SUM), 'lid://3bb53f/out.txt']
        out.workflowRun == 'lid://8f5496'
        out.codeChecksum == SUM
        out.sessionId == 'sess-1'
        out.name == 'ALIGN (A)'
        out.moduleId == 'nf-core/bwa@1.0.0'
        out.architecture == 'linux/arm64'
        and: 'host paths in val inputs go, the rest of the value stays'
        out.input[1].value == [id: 'A', ref: LOC, nested: [kit: 'truseq', dir: LOC]]
        out.input[2].value == LOC
        and: 'the script loses the run\'s own host locations and nothing else, so its regexes read as they ran'
        out.script == "bwa mem ${LOC}/assets/ref.fa reads.fq | sed 's/.* //' > out.sam\nawk '/^>/{print}' x > /data/elsewhere.txt".toString()
        and: 'an image name stays; a local env file and a host path in a task variable go'
        out.container == 'quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8'
        out.conda == LOC
        out.spack == 'bwa@0.7.17'
        out.globalVars == ['task.ext.args': "--tmp ${LOC}".toString(), 'task.ext.prefix': 'A']
        out.binEntries == [new DataPath(LOC, SUM)]
        and: 'the run\'s own objects are not modified'
        meta == [id: 'A', ref: '/home/rob/refs/g.fa', nested: [kit: 'truseq', dir: '/data/raw']]
    }

    def 'a local container image path goes, a registry image stays'() {
        expect:
        (scrub.scrub(new TaskRun(container: image)) as TaskRun).container == expected

        where:
        image                                                  || expected
        '/home/rob/images/bwa.sif'                             || LOC
        'file:///home/rob/images/bwa.sif'                      || LOC
        'docker.io/library/ubuntu:24.04'                       || 'docker.io/library/ubuntu:24.04'
        'community.wave.seqera.io/library/samtools:1.24--abc'  || 'community.wave.seqera.io/library/samtools:1.24--abc'
        null                                                   || null
    }

    def 'task and workflow outputs keep their lid references and lose host paths'() {
        given:
        final now = OffsetDateTime.now()
        final taskOut = new TaskOutput('lid://3bb53f', 'lid://8f5496', now, [
            new Parameter('path', 'bam', 'lid://3bb53f/A.bam'),
            new Parameter('val', 'where', '/home/rob/project/work/3b/b53f'),
        ], ['bam'])
        final wfOut = new WorkflowOutput(now, 'lid://8f5496', [
            new Parameter('Collection', 'counts', ['lid://8f5496/counts/count.txt']),
            new Parameter('Path', 'stray', Path.of('/home/rob/elsewhere/x.txt')),
        ])

        when:
        final t = scrub.scrub(taskOut) as TaskOutput
        final w = scrub.scrub(wfOut) as WorkflowOutput

        then:
        t.taskRun == 'lid://3bb53f'
        t.workflowRun == 'lid://8f5496'
        t.createdAt == now
        t.labels == ['bam']
        t.output*.value == ['lid://3bb53f/A.bam', LOC]
        w.workflowRun == 'lid://8f5496'
        w.output*.value == [['lid://8f5496/counts/count.txt'], LOC]
    }

    def 'a file output keeps its path: lid:// reads and lineage check open it'() {
        given:
        final output = new FileOutput(path: '/home/rob/project/work/3b/b53f/out.txt', checksum: SUM,
            source: 'lid://3bb53f', workflowRun: 'lid://8f5496', taskRun: 'lid://3bb53f', size: 174L)

        when:
        final out = scrub.scrub(output) as FileOutput

        then:
        out.path == '/home/rob/project/work/3b/b53f/out.txt'
        out.checksum == SUM
        out.source == 'lid://3bb53f'
    }

    def 'the scrub is idempotent'() {
        given:
        final task = new TaskRun(
            script: 'cat /home/rob/project/in.txt',
            input: [new Parameter('val', 'x', [a: '/data/a', b: 'lid://k/f'])],
            workflowRun: 'lid://8f5496')

        when:
        final once = scrub.scrub(task) as TaskRun
        final twice = scrub.scrub(once) as TaskRun

        then:
        twice.script == once.script
        twice.input == once.input
        once.script == "cat ${LOC}/in.txt".toString()
    }

    def 'a host location only matches at a path boundary'() {
        expect:
        scrub.code('ls /home/robert/x /home/rob/project2 /home/rob/project/') == "ls /home/robert/x ${LOC}/project2 ${LOC}/".toString()
    }

    def 'a script loses what the scrub redacted in the run params and in the task\'s own values, and its code survives'() {
        given: 'run params with a reference path, an output bucket and a password'
        final s = new NfRecordScrub(null, ['/home/rob'],
            [genome: '/refs/hg38.fa', outdir: 's3://lab-bucket/results', db_password: 'hunter2xyz', sample: 'A', depth: 30])
        final task = new TaskRun(
            script: [
                "bwa mem /refs/hg38.fa /data/reads/A.fq | sed 's/.* //' > A.sam",
                'kraken2 --db /dbs/k2 --threads 2',
                'aws s3 cp A.sam s3://lab-bucket/results/A.sam',
                'mysql -p hunter2xyz < q.sql',
                "awk '/^>/{print}' /refs/hg38.fa.fai /refs/hg38.fasta",
                'echo A',
            ].join('\n'),
            input: [new Parameter('val', 'reads', '/data/reads/A.fq'), new Parameter('val', 'sample', 'A')],
            globalVars: ['task.ext.args': '--db /dbs/k2'])

        when:
        final out = s.scrub(task) as TaskRun

        then: 'each redacted value is replaced where the script spells it, at a path boundary'
        out.script == [
            "bwa mem ${LOC} ${LOC} | sed 's/.* //' > A.sam",
            "kraken2 --db ${LOC} --threads 2",
            "aws s3 cp A.sam ${LOC}/A.sam",
            'mysql -p [secret] < q.sql',
            "awk '/^>/{print}' ${LOC}.fai /refs/hg38.fasta",
            'echo A',
        ].join('\n')
        and: 'the values themselves are scrubbed as before'
        out.input*.value == [LOC, 'A']
        out.globalVars == ['task.ext.args': "--db ${LOC}".toString()]
    }

    def 'a secret param a task holds goes from its global variables and its val inputs; a val named key keeps its value'() {
        given: 'Nextflow records each params.x a script reads as globalVars[\'params.x\']'
        final s = new NfRecordScrub(null, ['/home/rob'], [db_password: 'Tr0ub4dor&3xyz', sample: 'A'])
        final task = new TaskRun(
            script: 'mysql -p Tr0ub4dor&3xyz < q.sql',
            input: [
                new Parameter('val', 'pw', 'Tr0ub4dor&3xyz'),
                new Parameter('val', 'key', 'sample_17'),
            ],
            globalVars: [
                'params.db_password': 'Tr0ub4dor&3xyz',
                'params.sample': 'A',
                'task.ext.args': '--min-len 30',
                'task.ext.token': 'abc',
            ])

        when:
        final out = s.scrub(task) as TaskRun

        then: 'a secret-named global variable goes, as a RunManifest params key does; the others stay'
        out.globalVars == [
            'params.db_password': '[secret]',
            'params.sample': 'A',
            'task.ext.args': '--min-len 30',
            'task.ext.token': '[secret]',
        ]
        and: 'a val input equal to a secret param\'s value goes; a val named key keeps an ordinary value'
        out.input*.name == ['pw', 'key']
        out.input[0].value == '[secret]'
        out.input[1].value == 'sample_17'
        and: 'the script loses the secret as before'
        out.script == 'mysql -p [secret] < q.sql'
    }

    def 'a short or numeric secret param is not looked for in a script, so ordinary code survives'() {
        given:
        final s = new NfRecordScrub(null, ['/home/rob'],
            [max_tokens: 1000, token_limit: 1234567890L, key_column: 'sample', my_secret: 'secret', db_password: 'hunter2xyz'])

        expect: 'numbers and secret strings under 8 characters stay where the script spells them'
        s.code('head -n 1000 x.txt | split -l 1234567890') == 'head -n 1000 x.txt | split -l 1234567890'
        s.code('samtools view -o sample.bam sample.sam') == 'samtools view -o sample.bam sample.sam'
        s.code("echo secret '[secret]'") == "echo secret '[secret]'"
        and: 'a secret of 8 characters or more still goes'
        s.code('mysql -p hunter2xyz < q.sql') == 'mysql -p [secret] < q.sql'
    }

    def 'a redacted value equal to a marker\'s inner word does not match inside a marker'() {
        given:
        final s = new NfRecordScrub(null, ['/home/rob'], [api_key: 'redacted-location', db_token: 'redacted-user'])

        expect:
        s.code('ls [redacted-location]/x [redacted-user] /home/rob/y') == "ls [redacted-location]/x [redacted-user] ${LOC}/y".toString()
    }

    def 'a redacted path after a closing brace, a closing parenthesis or a dollar stays: it is the tail of an expansion'() {
        given:
        final s = new NfRecordScrub(null, ['/home/rob'], [scratch: '/tmp'])

        expect:
        s.code('cd ${PWD}/tmp && ls $(pwd)/tmp $/tmp && ls /tmp') == "cd \${PWD}/tmp && ls \$(pwd)/tmp \$/tmp && ls ${LOC}".toString()
    }

    def 'a map key that is a location is named as the RunManifest scrub names it, and the script loses it'() {
        given: 'location keys that collide, one already the marker, a Path key and keys that stay'
        final Map files = ['/data/a.fq': 1, '[redacted-location]': 2, 'file:///data/b.fq': 3, 'rel.fq': 4, (Path.of('/data/c.fq')): 5, 7: 'seven']
        final task = new TaskRun(script: 'cat /data/a.fq /data/b.fq rel.fq', input: [new Parameter('val', 'files', files)])

        when:
        final out = scrub.scrub(task) as TaskRun
        final Map value = out.input[0].value as Map

        then: 'a key the map keeps is never renamed; each location key takes the next free number'
        value == [(LOC + '-2'): 1, (LOC): 2, (LOC + '-3'): 3, 'rel.fq': 4, (LOC + '-4'): 5, 7: 'seven']
        value.keySet().collect { String.valueOf(it) } == ((Records.scrub([files: files]) as Map).files as Map).keySet().collect { String.valueOf(it) }
        and: 'a location key\'s text is looked for in the script, as a value\'s is'
        out.script == "cat ${LOC} ${LOC} rel.fq".toString()
    }

    def 'a map key that is a credential, the user name or holds a path is renamed, and the script loses what it held'() {
        given:
        final String user = System.getProperty('user.name')
        final String ghp = 'ghp_' + 'abcdefghijklmnopqrstuv'
        final Map opts = [(ghp): 'a', (user): 'b', '--in=/data/in.txt': 'c', 'a /data/b.txt': 'd', sample: 'e']
        final task = new TaskRun(script: "cat /data/in.txt /data/b.txt ${ghp} sample".toString(), input: [new Parameter('val', 'opts', opts)])
        final run = new WorkflowRun(workflow: new Workflow([], null, null), params: [new Parameter('Map', 'opts', opts)])

        when:
        final out = scrub.scrub(task) as TaskRun
        final again = scrub.scrub(out) as TaskRun
        final runOut = scrub.scrub(run) as WorkflowRun

        then: 'named as the RunManifest scrub names them'
        out.input[0].value == ['[secret]': 'a', '[redacted-user]': 'b', (LOC): 'c', (LOC + '-2'): 'd', sample: 'e']
        (out.input[0].value as Map).keySet().toList() == ((Records.scrub([opts: opts]) as Map).opts as Map).keySet().toList()
        out.script == "cat ${LOC} ${LOC} [secret] sample".toString()
        and: 'in run params, a value under a key renamed to [secret] goes too, as in a RunManifest'
        runOut.params[0].value == ['[secret]': '[secret]', '[redacted-user]': 'b', (LOC): 'c', (LOC + '-2'): 'd', sample: 'e']
        and: 'idempotent'
        again.input == out.input
        again.script == out.script
        (scrub.scrub(runOut) as WorkflowRun).params[0].value == runOut.params[0].value
    }

    def 'a secret a name carried is looked for in the script from 8 characters; one found by its own shape however short'() {
        given:
        final s = new NfRecordScrub(null, ['/home/rob'], [args: '--password hunter2xyz', opt1: '--key sample', opt2: '--auth basic',
            short: '--password hunter2', db: 'mysql://u:hunter2@h/db', hdr: 'Authorization: Basic YTpi'])

        expect: 'a flag-carried secret of 8 characters or more goes'
        s.code('mysql -p hunter2xyz < q.sql') == 'mysql -p [secret] < q.sql'
        and: 'a shorter one is as likely ordinary code, and stays'
        s.code('samtools view -o sample.bam sample.sam') == 'samtools view -o sample.bam sample.sam'
        s.code('curl --basic -u x basic.txt') == 'curl --basic -u x basic.txt'
        s.code('mysql -p hunter2 < q.sql') == 'mysql -p hunter2 < q.sql'
        and: 'a Basic credential, found by its own shape, goes however short'
        s.code('curl -H "Authorization: Basic YTpi" x') == 'curl -H "Authorization: Basic [secret]" x'
        and: 'a URL with a password is a location, and goes whole'
        s.code('mysql mysql://u:hunter2@h/db') == "mysql ${LOC}".toString()
    }

    def 'a top-level parameter name is named as the RunManifest names a top-level params key'() {
        given:
        final String user = System.getProperty('user.name')
        final String ghp = 'ghp_' + 'abcdefghijklmnopqrstuv'
        final run = new WorkflowRun(workflow: new Workflow([], null, null), params: [
            new Parameter('String', ghp, 'a'), new Parameter('String', user, 'b'), new Parameter('String', '/data/x', 'c'),
            new Parameter('String', 'sample', 'd'), new Parameter('String', 'db_password', 'hunter2xyz')])

        when:
        final out = scrub.scrub(run) as WorkflowRun
        final again = scrub.scrub(out) as WorkflowRun

        then:
        out.params*.name == ['[secret]', '[redacted-user]', LOC, 'sample', 'db_password']
        out.params*.value == ['[secret]', 'b', 'c', 'd', '[secret]']
        out.params*.name == ((Records.scrub([(ghp): 'a', (user): 'b', '/data/x': 'c', sample: 'd', db_password: 'hunter2xyz']) as Map).keySet() as List)
        and: 'idempotent'
        again.params*.name == out.params*.name
        again.params*.value == out.params*.value
    }

    def 'a map key under the run\'s repository stays, as a value does, and the script keeps the repository url'() {
        given:
        final String script = "git clone ${REPO} && curl ${REPO}/raw/main/x.nf".toString()
        final task = new TaskRun(script: script, input: [new Parameter('val', 'm', [(REPO + '/raw/main/x.nf'): 1, (REPO): 2, '/data/x': 3])])

        when:
        final out = scrub.scrub(task) as TaskRun

        then:
        out.input[0].value == [(REPO + '/raw/main/x.nf'): 1, (REPO): 2, (LOC): 3]
        out.script == script
    }

    def 'file, uri and url values are judged by their text; numbers, booleans and dates stay'() {
        given:
        final now = OffsetDateTime.now()
        final day = LocalDate.of(2026, 10, 25)
        final task = new TaskRun(input: [new Parameter('val', 'x', [
            file: new File('/data/a.txt'), relative: new File('rel.txt'), uri: new URI('s3://bucket/key'),
            portable: new URI('lid://8f5496/x.txt'), url: new URL('https://example.org/x'),
            n: 3, f: 1.5d, b: true, at: now, day: day])])

        when:
        final Map value = (scrub.scrub(task) as TaskRun).input[0].value as Map

        then:
        value == [file: LOC, relative: 'rel.txt', uri: LOC, portable: 'lid://8f5496/x.txt', url: LOC,
                  n: 3, f: 1.5d, b: true, at: now, day: day]
    }

    def 'forSession takes the run\'s directories as paths and file:// uris, its repository and its params'() {
        given:
        final meta = Mock(WorkflowMetadata) {
            getLaunchDir() >> Path.of('/home/rob/launch')
            getProjectDir() >> Path.of('/home/rob/project')
            getWorkDir() >> Path.of('/home/rob/launch/work')
            getHomeDir() >> Path.of('/home/rob')
            getRepository() >> REPO
        }
        final session = Mock(Session) {
            getWorkflowMetadata() >> meta
            getWorkDir() >> Path.of('/home/rob/launch/work')
            getParams() >> new ScriptBinding.ParamsMap([genome: '/refs/hg38.fa', sample: 'A'])
        }

        when:
        final s = NfRecordScrub.forSession(session)

        then: 'each directory as a plain path and as a file:// uri, longest first'
        s.locations as Set == ['/home/rob/launch', '/home/rob/project', '/home/rob/launch/work', '/home/rob',
                               'file:///home/rob/launch', 'file:///home/rob/project', 'file:///home/rob/launch/work', 'file:///home/rob'] as Set
        s.locations.size() == 8
        s.locations.first() == 'file:///home/rob/launch/work'
        s.locations*.length() == s.locations*.length().sort(false) { a, b -> b <=> a }
        and: 'a script loses the directories and the redacted params'
        s.code('cat file:///home/rob/project/a /home/rob/launch/work/ab/x /refs/hg38.fa A') ==
            "cat ${LOC}/a ${LOC}/ab/x ${LOC} A".toString()
        and: 'a path under the repository is kept'
        (s.scrub(new TaskRun(binEntries: [new DataPath("${REPO}/tree/c0ffee/bin/x.py".toString(), SUM)])) as TaskRun)
            .binEntries*.path == ["${REPO}/tree/c0ffee/bin/x.py".toString()]
    }

    def 'forSession on a bare session has no locations'() {
        expect:
        NfRecordScrub.forSession(Mock(Session)).locations == []
        NfRecordScrub.forSession(null).locations == []
    }
}
