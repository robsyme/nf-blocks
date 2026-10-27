package robsyme.cas.trace

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import nextflow.file.FileHelper
import nextflow.file.FileHolder
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfigV1
import nextflow.script.ProcessConfigV2
import nextflow.script.params.FileInParam
import nextflow.script.params.ValueInParam
import nextflow.script.params.v2.ProcessInput
import nextflow.script.params.v2.ProcessInputsDef
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.DagJson
import robsyme.cas.core.Hashing
import robsyme.cas.core.InputSet
import robsyme.cas.nio.CasFileSystemProvider
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The input recorder's own rules (explorer v0.3 P4 plan 1.3, rulings R3 and
 * R4): which staged files are inputs, their role and sample, and that each
 * distinct file is hashed once, off the caller's thread.
 */
class InputRecorderTest extends Specification {

    @TempDir
    Path tempDir

    private final List<FileSystem> zips = []

    /** A file the relative-param feature makes under the working directory, removed after it. */
    private Path existing

    def cleanup() {
        zips.each { it.close() }
    }

    private Path file(String rel, String text) {
        final Path p = tempDir.resolve(rel)
        Files.createDirectories(p.parent)
        Files.writeString(p, text)
        return p
    }

    /** A path on a file system other than the default one, standing in for s3:// or http(s)://. */
    private Path remote(String name, byte[] bytes) {
        final Path zip = tempDir.resolve("remote-${zips.size()}.zip")
        final FileSystem fs = FileSystems.newFileSystem(zip, [create: 'true'])
        zips << fs
        final Path p = fs.getPath('/' + name)
        Files.write(p, bytes)
        return p
    }

    /**
     * A recorder that counts the files it hashes. Its stat answers from
     * {@code statAnswers} first, waits on {@code statGate} for a path in
     * {@code statBlocks} (real paths, which is what a local stat is given;
     * deaf to interrupts, as a hung mount is), and
     * refuses an http(s) path so no test reaches the network.
     */
    private static class Counting extends InputRecorder {
        final AtomicInteger hashed = new AtomicInteger()
        Path location
        CountDownLatch gate
        final Map<Path, Long> statAnswers = [:]
        final Set<Path> statBlocks = []
        final CountDownLatch statGate = new CountDownLatch(1)
        final CountDownLatch statWaiting = new CountDownLatch(1)

        Counting(Path workDir, Map params, boolean hash) {
            super(workDir, params, hash)
        }

        @Override
        protected Cid hashFile(Path path) {
            hashed.incrementAndGet()
            gate?.await()
            return super.hashFile(path)
        }

        @Override
        protected Path resolveLocation(String text) {
            return location ?: super.resolveLocation(text)
        }

        @Override
        protected BasicFileAttributes statOf(Path path) {
            if( path in statBlocks ) {
                statWaiting.countDown()
                while( statGate.count > 0 ) {
                    try { statGate.await() } catch( InterruptedException ignored ) { }
                }
            }
            if( statAnswers.containsKey(path) )
                return new RemoteFiles.Attributes(statAnswers[path])
            if( path.toUri().scheme in ['http', 'https'] )
                throw new IOException("test: no network for ${path.toUri().host}")
            return super.statOf(path)
        }
    }

    /** Runs {@code body} on a thread of its own; true when it returned within {@code millis}. */
    private static boolean returnsWithin(long millis, Closure body) {
        final Thread t = new Thread(body as Runnable, 'test-caller')
        t.daemon = true
        t.start()
        t.join(millis)
        return !t.alive
    }

    private static InputRecorder.Staged staged(Object source, Path store = null, String sample = null) {
        return new InputRecorder.Staged(source, store ?: (Path) source, sample)
    }

    private static InputSet.Input only(List<InputSet.Input> inputs) {
        assert inputs.size() == 1
        return inputs[0]
    }

    // ------------------------------------------------------------ file params

    def 'file-valued params are the ones whose value looks like a path or URI, by base name; params.input is not one'() {
        given:
        Path local = file('refs/genes.bed', 'x')

        expect:
        InputRecorder.fileParams([
            input        : '/data/samplesheet.csv',
            fasta        : '/ref/genome.fa',
            gtf          : 's3://bucket/k/genes.gtf',
            igenomes_base: 's3://ngi-igenomes/igenomes/',
            bed          : local.toString(),
            outdir       : 'results',
            max_cpus     : 4,
            skip_qc      : true,
            title        : 'My run',
            genome       : null,
            genomes      : [GRCh38: [fasta: '/ref/GRCh38.fa']],
        ]) == [bed: 'genes.bed', fasta: 'genome.fa', gtf: 'genes.gtf', igenomes_base: 'igenomes']
    }

    def 'a relative param value names a file only when that file exists'() {
        given:
        Path here = Path.of('').toAbsolutePath()
        existing = Files.createTempFile(Files.createDirectories(here.resolve('build')), 'param', '.txt')

        expect:
        InputRecorder.fileParams([a: here.relativize(existing).toString(), b: 'no-such-file.txt']) == [a: existing.fileName.toString()]

        cleanup:
        if( existing != null )
            Files.deleteIfExists(existing)
    }

    // ------------------------------------------------------------------ roles

    def "a file the pipeline itself ships (under projectDir) is an asset with no sample, so it never links two runs' samples"() {
        given: 'multiqc_config.yml from the pipeline, staged in a tuple with meta id multiqc_report, beside real reads'
        final Path project = Files.createDirectories(tempDir.resolve('pipeline'))
        final Path asset = file('pipeline/assets/multiqc_config.yml', 'report_comment: x')
        final Path reads = file('data/S1_1.fq', '@r\nACGT\n+\n!!!!\n')
        final Path sheet = file('pipeline/assets/samplesheet.csv', 'sample,fastq_1\n')
        final InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), project, [input: sheet.toString()], true)

        when:
        recorder.recordStaged([staged(asset, asset, 'multiqc_report'), staged(reads, reads, 'S1'), staged(sheet, sheet, null)])
        final Map<String, InputSet.Input> byName = recorder.collect(-1).collectEntries { [(it.name): it] }

        then:
        byName['multiqc_config.yml'].role == 'asset'
        byName['multiqc_config.yml'].sample == null
        byName['multiqc_config.yml'].address == Hashing.hashRaw(asset)
        byName['S1_1.fq'].role == 'task' && byName['S1_1.fq'].sample == 'S1'
        byName['samplesheet.csv'].role == 'samplesheet'
    }

    def 'role precedence is samplesheet, then param:<name> in name order, then task'() {
        expect:
        InputRecorder.roleOf(name, 'samplesheet.csv', [alt: 'genome.fa', fasta: 'genome.fa', gtf: 'genes.gtf']) == role

        where:
        name              | role
        'samplesheet.csv' | 'samplesheet'
        'genome.fa'       | 'param:alt'
        'genes.gtf'       | 'param:gtf'
        'reads_1.fq.gz'   | 'task'
        null              | 'task'
    }

    def 'with no params.input, nothing is a samplesheet by name'() {
        expect:
        InputRecorder.roleOf('samplesheet.csv', null, [:]) == 'task'
    }

    // ---------------------------------------------------------------- samples

    def 'the sample is the id of the tuple or record that holds the file'() {
        expect:
        InputRecorder.sampleOf(group) == sample

        where:
        group                                                              | sample
        [meta: [id: 'S1', single_end: false], reads: [Path.of('/r1.fq')]] | 'S1'
        [sample: [id: 'R1', meta: [id: 'S1'], reads: [Path.of('/r1.fq')]]] | 'S1'
        [sample: [id: 'R1', reads: [Path.of('/r1.fq')]]]                  | 'R1'
        [id: 'S2', reads: [Path.of('/r1.fq')]]                            | 'S2'
        [meta: [id: 7], reads: Path.of('/r1.fq')]                         | '7'
        [id: 'S4', ref_meta: [id: 'x'], reads: Path.of('/r1.fq')]         | 'S4'
        [meta2: [id: 'genome'], meta: [id: 'S5'], reads: Path.of('/r1.fq')] | 'S5'
        [reads: [Path.of('/r1.fq')]]                                       | null
        [index: Path.of('/idx')]                                           | null
        [opts: [name: 'n'], reads: [Path.of('/r1.fq')]]                   | null
        [sample: [meta: [single_end: true], reads: [Path.of('/r1.fq')]]]   | null
    }

    def 'a map component not named meta gives no sample: nf-core reference tuples name the reference, not a sample'() {
        expect:
        InputRecorder.sampleOf(group) == null

        where:
        group << [
            [meta2: [id: 'genome'], fasta: Path.of('/genome.fa')],
            [meta3: [id: 'fasta'], index: Path.of('/bwa')],
            [ref_meta: [id: 'GRCh38'], gtf: Path.of('/genes.gtf')],
            [anything: [id: 'S3'], reads: Path.of('/r1.fq')],
            [sample: [meta2: [id: 'genome'], fasta: Path.of('/genome.fa')]],
        ]
    }

    def 'files of a tuple get its sample; a file from a separate path input of the same task gets null'() {
        given:
        Path r1 = Path.of('/data/S1_1.fq')
        Path r2 = Path.of('/data/S1_2.fq')
        Path idx = Path.of('/ref/index')
        List<FileHolder> holders = [new FileHolder(r1), new FileHolder(r2), new FileHolder(idx)]

        when:
        List<InputRecorder.Staged> staged = InputRecorder.staged(
            [[meta: [id: 'S1'], reads: [r1, r2]], [index: idx]], holders)

        then:
        staged.collect { [it.source, it.sample] } as Set == [[r1, 'S1'], [r2, 'S1'], [idx, null]] as Set
    }

    def 'a v1 tuple holds the FileHolders themselves, and they match by source'() {
        given:
        Path r1 = Path.of('/data/S1_1.fq')
        Path other = Path.of('/data/other.txt')
        FileHolder h1 = new FileHolder(r1)
        FileHolder h2 = new FileHolder(other)

        expect:
        InputRecorder.staged([[meta: [id: 'S1'], reads: [h1]], [extra: [h2]]], [h1, h2])
            .collect { [it.source, it.sample] } as Set == [[r1, 'S1'], [other, null]] as Set
    }

    def 'a file held by two samples is staged once for each; one also in a separate input keeps only the samples'() {
        given:
        Path shared = Path.of('/data/shared.fq')

        expect:
        InputRecorder.staged([[meta: [id: 'A'], reads: shared], [meta: [id: 'B'], reads: shared], [extra: shared]], [new FileHolder(shared)])
            .collect { it.sample } as Set == ['A', 'B'] as Set
    }

    def 'a value a task turned into a file is not a file the run read'() {
        given:
        Path tmp = file('work/tmp/input.1', 'hello')

        expect:
        InputRecorder.staged([[x: 'hello']], [FileHolder.get('hello', tmp)]).isEmpty()
    }

    // -------------------------------------------------------------- placement

    def 'a store path inside the work directory is the run\'s own, except under stage-*'() {
        given:
        Path work = Files.createDirectories(tempDir.resolve('work'))
        List<Path> dirs = InputRecorder.workDirsOf(work)

        expect:
        InputRecorder.placement(work.resolve('ab/123456/out.txt'), dirs) == InputRecorder.Placement.RUN
        InputRecorder.placement(work.resolve('stage-0000-1111/ab/cdef/reads.fq'), dirs) == InputRecorder.Placement.STAGED
        InputRecorder.placement(tempDir.resolve('data/reads.fq'), dirs) == InputRecorder.Placement.OUTSIDE
        InputRecorder.placement(tempDir.resolve('workshop/reads.fq'), dirs) == InputRecorder.Placement.OUTSIDE
        InputRecorder.placement(tempDir.resolve('data/reads.fq'), InputRecorder.workDirsOf(null)) == InputRecorder.Placement.OUTSIDE
    }

    def 'a work directory named through a symlink still contains the real paths Nextflow stages'() {
        given:
        Path real = Files.createDirectories(tempDir.resolve('real-work'))
        Path link = Files.createSymbolicLink(tempDir.resolve('work'), real)

        expect:
        InputRecorder.placement(real.toRealPath().resolve('ab/12/out.txt'), InputRecorder.workDirsOf(link)) == InputRecorder.Placement.RUN
    }

    // ---------------------------------------------------------------- hashing

    def 'a file read outside the work directory is hashed once however many tasks stage it'() {
        given:
        Path work = Files.createDirectories(tempDir.resolve('work'))
        Path reads = file('data/S1_1.fq', '@r1\nACGT\n+\nIIII\n')
        Counting recorder = new Counting(work, [:], true)

        when:
        recorder.recordStaged([staged(reads, reads, 'S1')])
        recorder.recordStaged([staged(reads, reads, 'S1')])
        recorder.recordStaged([staged(reads, reads, 'S2')])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        recorder.hashed.get() == 1
        inputs.collect { it.sample } as Set == ['S1', 'S2'] as Set
        inputs.every { it.role == 'task' && it.name == 'S1_1.fq' && it.address == Hashing.hashRaw(reads) && it.size == Files.size(reads) && it.reason == null }

        cleanup:
        recorder.close()
    }

    def 'a file the run made in its work directory is no input; a foreign file staged under stage-* is hashed from that copy'() {
        given:
        Path work = Files.createDirectories(tempDir.resolve('work'))
        Path made = file('work/ab/123/aligned.bam', 'bam')
        Path source = remote('S1_1.fq', 'remote bytes'.bytes)
        Path copy = file('work/stage-uuid/cd/ef01/S1_1.fq', 'remote bytes')
        Counting recorder = new Counting(work, [:], true)

        when:
        recorder.recordStaged([staged(made), staged(source, copy, 'S1')])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        input.name == 'S1_1.fq'
        input.address == Hashing.hashRaw(copy)
        input.size == 12L
        input.sample == 'S1'

        cleanup:
        recorder.close()
    }

    def 'a file only reachable remotely is listed with reason remote and its size, never read'() {
        given:
        Path source = remote('big.bam', new byte[100])
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(source)])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        recorder.hashed.get() == 0
        input.address == null
        input.reason == 'remote'
        input.size == 100L
        input.name == 'big.bam'

        cleanup:
        recorder.close()
    }

    def 'a task input on a remote file system is never opened: no address, reason remote, the size from a stat on the pool'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Path source = remote.path('s3://bucket/reads/S1_1.fq.gz', 100L)
        Path missing = remote.path('s3://bucket/reads/S2_1.fq.gz')
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when: 'the store path is remote too, as with an s3 work directory'
        recorder.recordStaged([staged(source, source, 'S1'), staged(missing, missing, 'S2')])
        Map<String, InputSet.Input> byName = recorder.collect(-1).collectEntries { [(it.name): it] }

        then:
        remote.reads.get() == 0
        recorder.hashed.get() == 0
        byName['S1_1.fq.gz'].address == null
        byName['S1_1.fq.gz'].reason == 'remote'
        byName['S1_1.fq.gz'].size == 100L
        byName['S1_1.fq.gz'].sample == 'S1'

        and: 'a stat that fails leaves it remote with no size'
        byName['S2_1.fq.gz'].reason == 'remote'
        byName['S2_1.fq.gz'].size == null

        cleanup:
        recorder.close()
    }

    def 'a path under /fusion/ is Fusion\'s mount of remote storage: never hashed, reason remote (as #how)'() {
        given:
        Path fused = Path.of('/fusion/s3/bucket/data/samplesheet.csv')
        Counting recorder = new Counting(tempDir.resolve('work'), [input: fused.toString()], true)
        recorder.statAnswers[fused] = 7L

        when:
        if( how == 'a task input' )
            recorder.recordStaged([staged(fused, fused, 'S1')])
        else
            recorder.recordSamplesheet()
        InputSet.Input input = only(recorder.collect(-1))

        then:
        recorder.hashed.get() == 0
        input.address == null
        input.reason == 'remote'
        input.size == 7L
        input.role == 'samplesheet'

        cleanup:
        recorder.close()

        where:
        how << ['a task input', 'params.input']
    }

    def 'a /fusion/ path whose stat fails is still remote, not unreadable'() {
        given:
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(Path.of('/fusion/s3/bucket/no-such-nf-blocks-test/S1_1.fq.gz'))])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        recorder.hashed.get() == 0
        input.reason == 'remote'
        input.size == null

        cleanup:
        recorder.close()
    }

    def 'the Fusion rule: a path is on Fusion\'s mount when its text, absolute and normalized, is /fusion or under it'() {
        expect:
        InputRecorder.onFusion(Path.of(text)) == fused

        where:
        text                         | fused
        '/data/S1_1.fq'              | false
        'relative/S1_1.fq'           | false
        '/fusion/s3/bucket/x.fq'     | true
        '/fusion'                    | true
        '/fusionx/x.fq'              | false
        '/data/../fusion/s3/b/x.fq'  | true
    }

    def 'a local file Nextflow staged to a remote work directory is hashed from the local source'() {
        given:
        Path reads = file('data/S1_1.fq', 'ACGT')
        Path remoteCopy = remote('S1_1.fq', 'ACGT'.bytes)
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(reads, remoteCopy, 'S1')])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        input.address == Hashing.hashRaw(reads)
        input.reason == null

        cleanup:
        recorder.close()
    }

    def 'a cas:// input keeps its known address without hashing'() {
        given:
        Path copyBytes = file('work/stage-uuid/aa/bb/reads.fq', 'stored')
        Cid cid = Hashing.hashRaw(copyBytes)
        Path source = new CasFileSystemProvider().getPath(URI.create("cas://${cid}/reads.fq"))
        Counting recorder = new Counting(tempDir.resolve('work'), [:], hash)

        when:
        recorder.recordStaged([staged(source, copyBytes, 'S1')])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        recorder.hashed.get() == 0
        input.address == cid
        input.reason == null
        input.size == 6L
        input.name == 'reads.fq'

        cleanup:
        recorder.close()

        where:
        hash << [true, false]
    }

    private Path casPath(Cid cid, String name) {
        return new CasFileSystemProvider().getPath(URI.create("cas://${cid}/${name}"))
    }

    private Cid blockOf(String name, String text) {
        return Hashing.hashRaw(file("blocks/${name}", text))
    }

    def 'a cas:// input from fromStore lists its origins'() {
        given:
        Cid dat = blockOf('dat', 'dev')
        Cid devItem = blockOf('item', 'item')
        Cid peopleColl = blockOf('coll', 'people')
        Cid sel = blockOf('sel', 'select')
        Path path = casPath(dat, 'Dev_Patel.dat')
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), [:], true)
        InputRecorder.Staged s = new InputRecorder.Staged(path, tempDir.resolve('work/ab/cdef/Dev_Patel.dat'), 'Dev', 'Dev_Patel.dat')
        recorder.recordStaged([s])
        recorder.origins = [(path): [new InputSet.Origin(devItem, peopleColl, sel)] as Set]

        when:
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        inputs*.from == [[new InputSet.Origin(devItem, peopleColl, sel)]]
        recorder.collectedByStaged[s].is(inputs[0])

        cleanup:
        recorder.close()
    }

    /** A provider whose every path reads as one directory with a fixed address, as two items holding identical directories do. */
    private static class SameDirectory extends CasFileSystemProvider {
        final Cid address

        SameDirectory(Cid address) {
            this.address = address
        }

        @Override
        <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, java.nio.file.LinkOption... options) {
            return (A) new robsyme.cas.nio.CasAttributes(0L, false, true, false, false, 0L, address.toString())
        }
    }

    def 'two directory paths from two items stay two inputs'() {
        given: 'cas://<item1>/qc and cas://<item2>/qc with the same manifest behind both'
        Cid manifest = blockOf('manifest', 'qc manifest')
        Cid item1 = Cid.of(Cid.DAG_CBOR, new byte[32])
        Cid item2 = Cid.of(Cid.DAG_CBOR, ([1] + [0] * 31) as byte[])
        SameDirectory provider = new SameDirectory(manifest)
        Path p1 = provider.getPath(URI.create("cas://${item1}/qc"))
        Path p2 = provider.getPath(URI.create("cas://${item2}/qc"))
        InputSet.Origin o1 = new InputSet.Origin(item1, null, null)
        InputSet.Origin o2 = new InputSet.Origin(item2, null, null)
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), [:], true)
        recorder.recordStaged([staged(p1, p1, null), staged(p2, p2, null)])
        recorder.origins = [(p1): [o1] as Set, (p2): [o2] as Set]

        when:
        InputSet set = new InputSet('rob', recorder.collect(-1))

        then: 'the inputs differ only in from'
        set.inputs.size() == 2
        set.inputs*.name == ['qc', 'qc']
        set.inputs*.address == [manifest, manifest]
        set.inputs*.from as Set == [[o1], [o2]] as Set

        cleanup:
        recorder.close()
    }

    def 'a file under the run work dir is not in collectedByStaged'() {
        given:
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), [:], true)
        InputRecorder.Staged intermediate = new InputRecorder.Staged(
            tempDir.resolve('work/aa/bbbb/Dev_intro.txt'), tempDir.resolve('work/cc/dddd/Dev_intro.txt'), 'Dev')
        recorder.recordStaged([intermediate])

        expect:
        recorder.collect(-1) == []
        !recorder.collectedByStaged.containsKey(intermediate)

        cleanup:
        recorder.close()
    }

    def 'recordTask returns what it staged'() {
        given:
        Path reads = file('data/S1.fq', 'ACGT')
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), [:], true)
        TaskRun task = storeTask([reads], null)

        expect:
        recorder.recordTask(task) == [new InputRecorder.Staged(reads, reads.toRealPath(), null, reads.fileName.toString())]
        recorder.recordTask(null) == []

        cleanup:
        recorder.close()
    }

    def 'a params map without input is never asked for it'() {
        given:
        Map params = Mock(Map) { containsKey('input') >> false; entrySet() >> ([] as Set) }

        when:
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), null, params, true)
        recorder.recordSamplesheet()

        then:
        0 * params.get('input')

        cleanup:
        recorder.close()
    }

    def 'with cas.inputs.hash = false a local input is listed with its size, unaddressed'() {
        given:
        Path reads = file('data/S1_1.fq', 'ACGT')
        Counting recorder = new Counting(tempDir.resolve('work'), [:], false)

        when:
        recorder.recordStaged([staged(reads, reads, 'S1')])
        InputSet.Input input = only(recorder.collect(-1))

        then:
        recorder.hashed.get() == 0
        input.address == null
        input.reason == 'unaddressed'
        input.size == 4L
        input.sample == 'S1'

        cleanup:
        recorder.close()
    }

    def 'a file that cannot be read is unreadable; a directory is unaddressed'() {
        given:
        Path missing = tempDir.resolve('data/gone.fq')
        Path dir = Files.createDirectories(tempDir.resolve('refs/star_index'))
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(missing), staged(dir)])
        Map<String, InputSet.Input> byName = recorder.collect(-1).collectEntries { [it.name, it] }

        then:
        byName['gone.fq'].reason == 'unreadable'
        byName['gone.fq'].address == null
        byName['star_index'].reason == 'unaddressed'
        byName['star_index'].size == null

        cleanup:
        recorder.close()
    }

    def 'a staged file whose base name is a file param\'s gets that param\'s role'() {
        given:
        Path fasta = file('refs/genome.fa', '>chr1\nACGT\n')
        Counting recorder = new Counting(tempDir.resolve('work'), [fasta: fasta.toString()], true)

        when:
        recorder.recordStaged([staged(fasta)])

        then:
        only(recorder.collect(-1)).role == 'param:fasta'

        cleanup:
        recorder.close()
    }

    // ------------------------------------------------------------ params.input

    def 'params.input is a samplesheet input, and a task\'s copy of it is the same entry'() {
        given:
        Path sheet = file('data/samplesheet.csv', 'sample,fastq_1\nS1,/data/S1_1.fq\n')
        Counting recorder = new Counting(tempDir.resolve('work'), [input: sheet.toString()], true)

        when:
        recorder.recordSamplesheet()
        recorder.recordStaged([staged(sheet)])
        List<InputSet.Input> inputs = new InputSet('t', recorder.collect(-1)).inputs

        then:
        recorder.hashed.get() == 1
        inputs.size() == 1
        inputs[0].role == 'samplesheet'
        inputs[0].name == 'samplesheet.csv'
        inputs[0].address == Hashing.hashRaw(sheet)
        inputs[0].sample == null

        cleanup:
        recorder.close()
    }

    def 'a remote params.input is never read: role samplesheet, its size from a stat, no address, reason remote (hash: #hash)'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Counting recorder = new Counting(tempDir.resolve('work'), [input: 's3://bucket/samplesheet.csv'], hash)
        recorder.location = remote.path('s3://bucket/samplesheet.csv', 42L)

        when:
        recorder.recordSamplesheet()
        InputSet.Input input = only(recorder.collect(-1))

        then:
        remote.reads.get() == 0
        recorder.hashed.get() == 0
        input.role == 'samplesheet'
        input.name == 'samplesheet.csv'
        input.size == 42L
        input.address == null
        input.reason == 'remote'

        cleanup:
        recorder.close()

        where:
        hash << [true, false]
    }

    def 'a remote params.input whose stat fails is still remote, with no size'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Counting recorder = new Counting(tempDir.resolve('work'), [input: 's3://bucket/gone.csv'], true)
        recorder.location = remote.path('s3://bucket/gone.csv')

        when:
        recorder.recordSamplesheet()
        InputSet.Input input = only(recorder.collect(-1))

        then:
        remote.reads.get() == 0
        input.size == null
        input.reason == 'remote'

        cleanup:
        recorder.close()
    }

    def 'params.input that names no file records nothing; one that cannot be resolved is unreadable'() {
        given:
        Counting recorder = new Counting(tempDir.resolve('work'), [input: value], true)

        when:
        recorder.recordSamplesheet()
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        inputs*.reason == reasons

        cleanup:
        recorder.close()

        where:
        value                  | reasons
        null                   | []
        42                     | []
        'not a file'           | []
        '/no/such/sheet.csv'   | ['unreadable']
    }

    // ------------------------------------------------------ names, credentials

    static final String PRESIGNED = 'https://bucket.s3.eu-west-1.amazonaws.com/runs/x.fq.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256' +
        '&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20261025%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date=20261025T000000Z' +
        '&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=0123456789abcdef'
    static final String SAS = 'https://account.blob.core.windows.net/container/runs/x.fq.gz?sv=2022-11-02&ss=b&srt=o&sp=r' +
        '&se=2026-10-26T00:00:00Z&sig=AbCdEf%2BghIjKl%3D'
    static final String BASIC = 'https://user:pass@host.example.org/runs/x.fq.gz'

    /**
     * The InputSet block, encoded, holds no credential fragment: its DAG-CBOR
     * bytes read as text, and (for a lone '?', which a CID's bytes may hold)
     * its DAG-JSON text.
     */
    private static void noCredentials(List<InputSet.Input> inputs) {
        final Map block = new InputSet('t', inputs).toCbor()
        final String cbor = new String(DagCbor.encode(block), 'ISO-8859-1')
        for( String fragment : ['X-Amz', 'sig=', 'user', 'pass', 'AKIA', 'Signature', 'Credential'] )
            assert !cbor.contains(fragment)
        assert !new String(DagJson.encode(block), 'UTF-8').contains('?')
    }

    def 'a base name never keeps a query, fragment or userinfo: #text'() {
        expect:
        InputRecorder.baseName(text) == name

        where:
        text                                              | name
        PRESIGNED                                         | 'x.fq.gz'
        SAS                                               | 'x.fq.gz'
        BASIC                                             | 'x.fq.gz'
        'https://user:pass@host.example.org'              | 'host.example.org'
        'https://user:pass@host.example.org/'             | 'host.example.org'
        'https://host.example.org/x.fq.gz#frag'           | 'x.fq.gz'
        's3://bucket/runs/x.fq.gz?versionId=abc'          | 'x.fq.gz'
        'az://container/runs/x.fq.gz?sig=abc'            | 'x.fq.gz'
        'ftp://user:pass@host/runs/x.fq.gz'               | 'x.fq.gz'
        's3://bucket'                                     | 'bucket'
        '/data/runs/x.fq.gz'                              | 'x.fq.gz'
        'x.fq.gz'                                         | 'x.fq.gz'
    }

    def 'a remote task input named by a credential-bearing URL is recorded by its base name alone (#kind)'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Path source = remote.path(url, 11L)
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(source, source, 'S1')])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == 'x.fq.gz'
        only(inputs).reason == 'remote'
        remote.reads.get() == 0
        noCredentials(inputs)

        cleanup:
        recorder.close()

        where:
        kind              | url
        'presigned S3'    | PRESIGNED
        'Azure SAS'       | SAS
        'http basic auth' | BASIC
    }

    def 'Nextflow\'s own https path (XPath) for a credential-bearing URL is recorded by its base name alone (#kind)'() {
        given:
        Path source = FileHelper.asPath(url)
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(source, source, 'S1')])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == 'x.fq.gz'
        only(inputs).reason == 'remote'
        recorder.hashed.get() == 0
        noCredentials(inputs)

        cleanup:
        recorder.close()

        where:
        kind              | url
        'presigned S3'    | PRESIGNED
        'Azure SAS'       | SAS
        'http basic auth' | BASIC
    }

    def 'params.input named by a credential-bearing URL is recorded by its base name alone, and a task\'s copy matches it by that name (#kind)'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Counting recorder = new Counting(tempDir.resolve('work'), [input: url], true)
        recorder.location = remote.path(url, 12L)
        Path copy = file('work/stage-uuid/ab/cd/x.fq.gz', 'sample\n')

        when:
        recorder.recordSamplesheet()
        recorder.recordStaged([staged(remote.path(url, 12L), copy, null)])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then: 'the staged copy, hashed, stands for the samplesheet; the remote params.input entry it makes redundant is dropped'
        only(inputs).role == 'samplesheet'
        only(inputs).name == 'x.fq.gz'
        only(inputs).address == Hashing.hashRaw(copy)
        only(inputs).reason == null
        remote.reads.get() == 0
        noCredentials(inputs)

        cleanup:
        recorder.close()

        where:
        kind              | url
        'presigned S3'    | PRESIGNED
        'Azure SAS'       | SAS
        'http basic auth' | BASIC
    }

    def 'params.input as Nextflow resolves it (XPath) keeps no credential either (#kind)'() {
        given:
        Counting recorder = new Counting(tempDir.resolve('work'), [input: url], true)

        when:
        recorder.recordSamplesheet()
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == name
        only(inputs).reason == 'remote'
        noCredentials(inputs)

        cleanup:
        recorder.close()

        where:
        kind              | url                                     | name
        'presigned S3'    | PRESIGNED                               | 'x.fq.gz'
        'Azure SAS'       | SAS                                     | 'x.fq.gz'
        'http basic auth' | BASIC                                   | 'x.fq.gz'
        'no path'         | 'https://user:pass@host.example.org/'   | 'host.example.org'
    }

    def 'a file param named by a credential-bearing URL gives its role by the plain base name'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Counting recorder = new Counting(tempDir.resolve('work'), [fasta: SAS.replace('x.fq.gz', 'genome.fa')], true)
        Path source = remote.path(SAS.replace('x.fq.gz', 'genome.fa'), 3L)

        when:
        recorder.recordStaged([staged(source)])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        InputRecorder.fileParams([fasta: PRESIGNED, gtf: BASIC.replace('x.fq.gz', 'genes.gtf')]) == [fasta: 'x.fq.gz', gtf: 'genes.gtf']
        only(inputs).role == 'param:fasta'
        only(inputs).name == 'genome.fa'
        noCredentials(inputs)

        cleanup:
        recorder.close()
    }

    static final String GHP = 'ghp_' + '0123456789abcdefghijklmnopqrstuvwxyz'
    static final String BOX_TOKEN = 'abcd1234efgh5678ijkl9012mnop3456'
    static final String CID = Hashing.hashRaw(new ByteArrayInputStream('x'.bytes), new byte[16]).toString()

    /** The InputSet block, encoded as DAG-CBOR and as DAG-JSON, holds none of the secret texts. */
    private static void holdsNone(List<InputSet.Input> inputs, List<String> secrets) {
        final Map block = new InputSet('t', inputs).toCbor()
        final String cbor = new String(DagCbor.encode(block), 'ISO-8859-1')
        final String json = new String(DagJson.encode(block), 'UTF-8')
        for( String secret : secrets ) {
            assert !cbor.contains(secret)
            assert !json.contains(secret)
        }
    }

    def 'a base name loses path parameters, a password holding a slash, and any credential shape: #text'() {
        expect:
        InputRecorder.baseName(text) == name

        where:
        text                                                                   | name
        'https://h/x.fq;jsessionid=ABC123'                                     | 'x.fq'
        'https://h/dir;v=2/x.fq;jsessionid=ABC123?a=b'                         | 'x.fq'
        "https://raw.githubusercontent.com/o/r/${GHP}/x.fq".toString()         | 'x.fq'
        "https://host.example.org/files/${GHP}".toString()                     | '[secret]'
        "https://app.box.com/shared/static/${BOX_TOKEN}.fastq.gz".toString()   | '[secret].fastq.gz'
        "https://app.box.com/shared/static/${BOX_TOKEN}.csv".toString()        | '[secret].csv'
        'https://user:pa/ss@host.example.org'                                  | 'host.example.org'
        'https://user:pa/ss@host.example.org/runs/x.fq'                        | 'x.fq'
        'https://user:pass@host.example.org/x.fq?token=y#frag'                 | 'x.fq'
        'https://host.example.org/a/b/?X-Amz-Signature=abc'                    | 'b'
        'https://host.example.org/dl?file=x.fq&sig=abc'                        | 'dl'
        'https://host.example.org:8443/data/sample@1.fq'                       | 'sample@1.fq'
        's3://bucket/data/sample@1.fq'                                         | 'sample@1.fq'
        'https://h/runs/0a1b2c3d-4e5f-6789-abcd-ef0123456789.bam'              | '0a1b2c3d-4e5f-6789-abcd-ef0123456789.bam'
        "/data/${GHP}.fq".toString()                                           | '[secret].fq'
        '/data/S1;rep2.fq'                                                     | 'S1;rep2.fq'
        "/data/${BOX_TOKEN}.fq".toString()                                     | '[secret].fq'
        'https://h/d41d8cd98f00b204e9800998ecf8427e.fq'                        | 'd41d8cd98f00b204e9800998ecf8427e.fq'
        "cas://${CID}".toString()                                              | CID
        '/data/GRCh38_primary_assembly_2024.fa'                                | 'GRCh38_primary_assembly_2024.fa'
        '/tmp/param3318408433160109677.txt'                                    | 'param3318408433160109677.txt'
        // In a remote URL path, two letter/digit switches make a 24-character stem a token (a random
        // token has fewer with probability 6.8e-5), so a separator-free remote name with a number
        // inside is redacted too. A local file name needs four (orchestrator ruling): tokens live in
        // URLs, and real FASTQ names such as run20261025combinedreads stay readable on disk.
        "https://host.example.org/files/${TWO_SWITCHES}.csv".toString()        | '[secret].csv'
        'https://host.example.org/data/run20261025combinedreads.fastq.gz'      | '[secret].fastq.gz'
        '/data/run20261025combinedreads.fastq.gz'                              | 'run20261025combinedreads.fastq.gz'
        "/data/${TWO_SWITCHES}.csv".toString()                                 | "${TWO_SWITCHES}.csv".toString()
        '/data/sample1rep2lane3read1chunk4.fastq.gz'                           | '[secret].fastq.gz'
        '/data/sample20261025.fastq.gz'                                        | 'sample20261025.fastq.gz'
        's3://bucket/dir/HG002v1p1chr20q12.vcf.gz'                             | 'HG002v1p1chr20q12.vcf.gz'
        's3://bucket/dir/SRR12345678_1.fastq.gz'                               | 'SRR12345678_1.fastq.gz'
    }

    static final String TWO_SWITCHES = 'abcdefghijk1234567890lmnopqrstuv'

    def 'on a Box shared link the stem of the base name is a token whatever its shape: #text'() {
        expect:
        InputRecorder.baseName(text) == name

        where:
        text                                                                     | name
        'https://app.box.com/s/abcdef'                                           | '[secret]'
        'https://app.box.com/s/abcdef/'                                          | '[secret]'
        'https://app.box.com/shared/static/x1y2.fastq.gz'                        | '[secret].fastq.gz'
        'https://acme.app.box.com/s/q8x2k4m9z7w1v3n5b6c0d2f4g6h8j0kl'           | '[secret]'
        'https://ACME.Box.COM/shared/static/reads.csv'                           | '[secret].csv'
        'https://app.box.com:443/s/abcdef?dl=1'                                  | '[secret]'
        'https://user:pass@app.box.com/s/abcdef'                                 | '[secret]'
        'https://app.box.com/s/q8x2k4m9z7w1v3n5b6c0d2f4g6h8j0kl/file/1234567'   | '1234567'
        'https://app.box.com/files/reads.csv'                                    | 'reads.csv'
        'https://app.box.com/shared/reads.csv'                                   | 'reads.csv'
        'https://notbox.com/s/reads.csv'                                         | 'reads.csv'
        'https://box.com.example.org/s/reads.csv'                                | 'reads.csv'
        'https://host.example.org/s/reads.csv'                                   | 'reads.csv'
    }

    def 'a userinfo whose password holds an unescaped / or @ is dropped whole; an @ after the host stays in the name: #text'() {
        expect:
        InputRecorder.baseName(text) == name

        where:
        text                                                         | name
        'https://user:pa/ss@host.example.org'                        | 'host.example.org'
        'https://user:p@a/ss@host.example.org'                       | 'host.example.org'
        'https://user:p/a/s/s@host.example.org'                      | 'host.example.org'
        'https://user:pa/s@s@host.example.org/dir/x.fq'              | 'x.fq'
        'https://user:pa/ss@host.example.org/dir/a@b.fq'             | 'a@b.fq'
        'https://user:pa/ss@host.example.org/a@b.fq'                 | 'a@b.fq'
        'https://user:pa%2Fss@host.example.org'                      | 'host.example.org'
        'https://host.example.org:8443/data/x@1.fq'                  | 'x@1.fq'
        'https://host.example.org/x.fq?next=a/b@c'                   | 'x.fq'
        'https://user:pass@host.example.org/x.fq?next=a/b@c'         | 'x.fq'
        // Ambiguous, the same shape as user:p@a/ss@host: read as a password holding '@' and '/',
        // so no part of one can become the name. The name loses its text up to the '@'.
        'https://user:pass@host.example.org/data/x@1.fq'             | '1.fq'
        'https://token@host.example.org/data/x@1.fq'                 | '1.fq'
    }

    def 'no secret text in a name reaches the InputSet block, as Nextflow resolves the URL (#kind, #how)'() {
        given:
        Counting recorder = new Counting(tempDir.resolve('work'), how == 'params.input' ? [input: url] : [:], true)

        when:
        if( how == 'params.input' )
            recorder.recordSamplesheet()
        else
            recorder.recordStaged([staged(FileHelper.asPath(url), FileHelper.asPath(url), 'S1')])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == name
        only(inputs).reason == 'remote'
        recorder.hashed.get() == 0
        holdsNone(inputs, secrets)

        cleanup:
        recorder.close()

        where:
        [kind, url, name, secrets, how] << [
            ['path parameter', 'https://h.example.org/x.fq;jsessionid=ABC123', 'x.fq', ['jsessionid', 'ABC123']],
            ['github token segment', "https://raw.githubusercontent.com/o/r/${GHP}/x.fq".toString(), 'x.fq', [GHP, 'ghp_']],
            ['github token name', "https://host.example.org/files/${GHP}".toString(), '[secret]', [GHP, 'ghp_']],
            ['box shared link', "https://app.box.com/shared/static/${BOX_TOKEN}.fastq.gz".toString(), '[secret].fastq.gz', [BOX_TOKEN, BOX_TOKEN.substring(0, 12)]],
            ['password with a slash', 'https://user:pa/ss@host.example.org', 'host.example.org', ['pa/ss', 'user', 'ss@']],
            ['password with an @ and a slash', 'https://user:p@a/ss@host.example.org', 'host.example.org', ['p@a', 'user', 'ss@']],
            ['an @ in the path after a slashed password', 'https://user:pa/ss@host.example.org/dir/a@b.fq', 'a@b.fq', ['pa/ss', 'user', 'ss@']],
            ['box link by /s/', 'https://app.box.com/s/q8x2k4m9z7w1v3n5b6c0d2f4g6h8j0kl', '[secret]', ['q8x2k4m9z7w1v3n5b6c0d2f4g6h8j0kl']],
            ['box link, word-shaped', 'https://app.box.com/shared/static/x1y2.fastq.gz', '[secret].fastq.gz', ['x1y2']],
        ].collectMany { List c -> ['a task input', 'params.input'].collect { String h -> c + [h] } }
    }

    def 'a remote name keeps no path parameter or token-shaped stem, whatever the provider puts in its file name (#url)'() {
        given:
        RemoteFiles remote = new RemoteFiles()
        Path source = remote.path(url, 5L)
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)

        when:
        recorder.recordStaged([staged(source)])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == name
        remote.reads.get() == 0
        holdsNone(inputs, secrets)

        cleanup:
        recorder.close()

        where:
        url                                                                     | name                | secrets
        's3://bucket/x.fq;jsessionid=ABC123'                                    | 'x.fq'              | ['jsessionid', 'ABC123']
        "s3://bucket/files/${GHP}".toString()                                   | '[secret]'          | [GHP]
        "https://app.box.com/shared/static/${BOX_TOKEN}.fastq.gz".toString()    | '[secret].fastq.gz' | [BOX_TOKEN]
        'https://app.box.com/s/abcdef'                                          | '[secret]'          | ['abcdef']
        'https://app.box.com/shared/static/x1y2.fastq.gz'                       | '[secret].fastq.gz' | ['x1y2']
        'https://user:p@a/ss@host.example.org'                                  | 'host.example.org'  | ['p@a', 'user', 'ss@']
        'https://user:pa/ss@host.example.org/dir/a@b.fq'                        | 'a@b.fq'            | ['pa/ss', 'user', 'ss@']
    }

    def 'a local file named with a credential shape is hashed under a scrubbed name'() {
        given:
        Path reads = file("data/${GHP}.fq", 'ACGT')
        Counting recorder = new Counting(tempDir.resolve('work'), [reads: reads.toString()], true)

        when:
        recorder.recordStaged([staged(reads, reads, 'S1')])
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        only(inputs).name == '[secret].fq'
        only(inputs).role == 'param:reads'
        only(inputs).address == Hashing.hashRaw(reads)
        holdsNone(inputs, [GHP, 'ghp_'])

        cleanup:
        recorder.close()
    }

    // ---------------------------------------------------------------- waiting

    def 'collect gives up on hashes still running at its deadline and lists them unfinished'() {
        given:
        Path reads = file('data/slow.fq', 'ACGT')
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)
        recorder.gate = new CountDownLatch(1)

        when:
        recorder.recordStaged([staged(reads)])
        InputSet.Input input = only(recorder.collect(50))

        then: 'the stat answered, so the size is known; the hash did not'
        input.reason == 'unfinished'
        input.address == null
        input.size == 4L
        recorder.leftUnfinished == 1

        cleanup:
        recorder.gate.countDown()
        recorder.close()
    }

    def 'a hash stuck past the deadline, deaf to interrupts as a hung mount is, holds neither close nor the JVM'() {
        given:
        Path reads = file('data/hung.fq', 'ACGT')
        CountDownLatch release = new CountDownLatch(1)
        CountDownLatch started = new CountDownLatch(1)
        Thread[] hasher = new Thread[1]
        InputRecorder recorder = new InputRecorder(tempDir.resolve('work'), [:], true) {
            @Override
            protected Cid hashFile(Path path) {
                hasher[0] = Thread.currentThread()
                started.countDown()
                while( release.count > 0 ) {
                    try { release.await() } catch( InterruptedException ignored ) { }
                }
                return super.hashFile(path)
            }
        }

        when:
        recorder.recordStaged([staged(reads)])
        started.await()
        long before = System.nanoTime()
        InputSet.Input input = only(recorder.collect(50))
        recorder.close()
        long tookMillis = (System.nanoTime() - before).intdiv(1_000_000L)

        then:
        input.reason == 'unfinished'
        tookMillis < 5_000
        hasher[0].daemon
        hasher[0].alive

        cleanup:
        release.countDown()
    }

    def 'a stat still pending at the deadline gives reason unfinished with no size, and collect keeps to the bound (#where_)'() {
        given:
        Path reads = file('data/hung.fq', 'ACGT')
        RemoteFiles remote = new RemoteFiles()
        remote.statGate = new CountDownLatch(1)
        Path far = remote.path('s3://bucket/hung.fq', 4L)
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)
        recorder.statBlocks << reads.toRealPath()

        when:
        boolean returned = returnsWithin(2_000) { recorder.recordStaged([staged(where_ == 'local' ? reads : far)]) }
        boolean statStarted = (where_ == 'local' ? recorder.statWaiting : remote.statWaiting).await(5, TimeUnit.SECONDS)
        long before = System.nanoTime()
        InputSet.Input input = only(recorder.collect(200))
        recorder.close()
        long tookMillis = (System.nanoTime() - before).intdiv(1_000_000L)

        then:
        returned
        statStarted
        input.reason == 'unfinished'
        input.size == null
        input.address == null
        recorder.leftUnfinished == 1
        tookMillis < 5_000
        recorder.hashed.get() == 0

        cleanup:
        recorder.statGate.countDown()
        remote.statGate.countDown()

        where:
        where_ << ['local', 'remote']
    }

    def 'recording a task returns at once when its file\'s stat hangs, as on a hung mount (#where_)'() {
        given:
        Path reads = file('data/hung.fq', 'ACGT')
        RemoteFiles remote = new RemoteFiles()
        remote.statGate = new CountDownLatch(1)
        Path far = remote.path('s3://bucket/hung.fq', 4L)
        Path sheet = file('data/sheet.csv', 'sample\n')
        Counting recorder = new Counting(tempDir.resolve('work'), [input: sheet.toString()], true)
        recorder.statBlocks << reads.toRealPath() << sheet.toRealPath()

        expect: 'neither a task nor params.input waits for the stat'
        returnsWithin(2_000) { recorder.recordStaged([staged(where_ == 'local' ? reads : far)]) }
        returnsWithin(2_000) { recorder.recordSamplesheet() }

        when:
        recorder.statGate.countDown()
        remote.statGate.countDown()
        Map<String, InputSet.Input> byName = recorder.collect(-1).collectEntries { [(it.name): it] }

        then:
        byName['sheet.csv'].address == Hashing.hashRaw(sheet)
        byName['hung.fq'].reason == (where_ == 'local' ? null : 'remote')

        cleanup:
        recorder.close()

        where:
        where_ << ['local', 'remote']
    }

    def 'each distinct file is stat\'ed and hashed once per run, however many tasks and samples name it'() {
        given:
        Path reads = file('data/S1_1.fq', 'ACGT')
        RemoteFiles remote = new RemoteFiles()
        Path far = remote.path('s3://bucket/S2_1.fq', 9L)
        AtomicInteger stats = new AtomicInteger()
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true) {
            @Override
            protected BasicFileAttributes statOf(Path path) {
                if( path == reads.toRealPath() ) stats.incrementAndGet()
                return super.statOf(path)
            }
        }

        when:
        3.times { recorder.recordStaged([staged(reads, reads, 'S1'), staged(reads, reads, 'S9'), staged(far, far, 'S2')]) }
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        inputs.size() == 3
        recorder.hashed.get() == 1
        stats.get() == 1
        remote.stats.get() == 1

        cleanup:
        recorder.close()
    }

    def 'recording a task returns before its files are hashed'() {
        given:
        Path reads = file('data/S1.fq', 'ACGT')
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)
        recorder.gate = new CountDownLatch(1)

        when:
        recorder.recordStaged([staged(reads)])

        then: 'the caller is not held by the gated hash'
        true

        when:
        recorder.gate.countDown()

        then:
        only(recorder.collect(-1)).address == Hashing.hashRaw(reads)
        recorder.leftUnfinished == 0

        cleanup:
        recorder.close()
    }

    // ---------------------------------------------------------------- storeDir

    /** A task with no declared inputs that staged {@code files}, its storeDir {@code storeDir} (or none). */
    private TaskRun storeTask(List<Path> files, Path storeDir) {
        TaskRun task = new TaskRun()
        task.processor = Stub(TaskProcessor) { getConfig() >> Stub(ProcessConfigV1) }
        task.config = new TaskConfig(storeDir != null ? [storeDir: storeDir.toString()] : [:])
        task.inputFiles.addAll(files.collect { new FileHolder(it) })
        return task
    }

    def 'a file under the storeDir of one of the run\'s tasks is no input, whichever task is recorded first (stored first: #storedFirst)'() {
        given:
        Path index = file('db/genome.idx', 'index')
        Path reads = file('data/S1.fq', 'ACGT')
        Counting recorder = new Counting(tempDir.resolve('work'), [:], true)
        TaskRun indexer = storeTask([], tempDir.resolve('db'))
        TaskRun consumer = storeTask([index, reads], null)

        when:
        (storedFirst ? [indexer, consumer] : [consumer, indexer]).each { recorder.recordTask(it) }
        List<InputSet.Input> inputs = recorder.collect(-1)

        then:
        inputs*.name == ['S1.fq']
        recorder.hashed.get() == (storedFirst ? 1 : 2)

        cleanup:
        recorder.close()

        where:
        storedFirst << [true, false]
    }

    // ------------------------------------------------------- Nextflow's inputs

    def 'a typed process: a record input is one group, a destructured tuple another, a Path input a third'() {
        given:
        ProcessInputsDef inputs = new ProcessInputsDef()
        inputs.addParam('sample', Map, false)
        ProcessInput metaIn = new ProcessInput('meta', Map, false)
        ProcessInput readsIn = new ProcessInput('reads', List, false)
        inputs.addTupleParam([metaIn, readsIn], nextflow.script.types.Tuple)
        inputs.addParam('index', Path, false)
        ProcessConfigV2 config = Stub(ProcessConfigV2) { getInputs() >> inputs }
        TaskRun task = new TaskRun()
        task.processor = Stub(TaskProcessor) { getConfig() >> config }
        Map record = [id: 'R1', meta: [id: 'S1'], reads: [Path.of('/a.fq')]]
        task.inputs[inputs.params[0]] = record
        task.inputs[metaIn] = [id: 'S2']
        task.inputs[readsIn] = [Path.of('/b.fq')]
        task.inputs[inputs.params[2]] = Path.of('/idx')

        expect:
        InputRecorder.groupsOf(task) == [[sample: record], [meta: [id: 'S2'], reads: [Path.of('/b.fq')]], [index: Path.of('/idx')]]
    }

    def 'a classic process groups tuple elements by their declaration index'() {
        given: 'as Nextflow declares them: the tuple at index 0 of the inputs, its elements in its own list'
        Binding binding = new Binding()
        List declared = ['the tuple']
        List inner = []
        ValueInParam metaIn = new ValueInParam(binding, inner, (short) 0).bind('meta') as ValueInParam
        FileInParam readsIn = new FileInParam(binding, inner, (short) 0).bind('reads') as FileInParam
        FileInParam indexIn = new FileInParam(binding, declared).bind('index') as FileInParam
        TaskRun task = new TaskRun()
        task.processor = Stub(TaskProcessor) { getConfig() >> Stub(ProcessConfigV1) }
        FileHolder h = new FileHolder(Path.of('/a.fq'))
        task.inputs[metaIn] = [id: 'S1']
        task.inputs[readsIn] = [h]
        task.inputs[indexIn] = [new FileHolder(Path.of('/idx'))]

        when:
        List<Map<String, Object>> groups = InputRecorder.groupsOf(task)

        then:
        groups.size() == 2
        groups.find { it.containsKey('meta') } == [meta: [id: 'S1'], reads: [h]]
        groups.find { it.containsKey('index') }.size() == 1
    }
}
