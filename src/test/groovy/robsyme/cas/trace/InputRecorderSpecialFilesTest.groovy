package robsyme.cas.trace

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import robsyme.cas.core.Cid
import robsyme.cas.core.Hashing
import robsyme.cas.core.InputSet
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/**
 * What the input recorder does with a local path that is not a plain file
 * (a FIFO, a device, a link into Fusion's mount, a dangling link, a file too
 * big to hash in time). It never opens one it cannot address, so it never
 * stalls a run's completion or races the pipeline for a FIFO's bytes.
 * Brought in from the P4 reviewer's probe (ReviewInputProbeTest).
 */
@Requires({ new File('/dev/zero').exists() })
class InputRecorderSpecialFilesTest extends Specification {

    @TempDir
    Path tempDir

    /**
     * Counts the files it hashes and notes which thread resolved each real
     * path. {@code links} stands in for a mount this host does not have: a
     * path in it resolves to its value, as a link into a mounted /fusion
     * would, and {@code sizes} answers that target's stat. A path in
     * {@code unresolvable} has no real path, as Linux's realpath fails for a
     * {@code /dev/fd/N} link to a pipe or socket.
     */
    static class Counting extends InputRecorder {
        final AtomicInteger hashed = new AtomicInteger()
        final Map<Path, Path> links = [:]
        final Set<Path> unresolvable = []
        final Map<Path, Long> sizes = [:]
        final Set<String> resolvingThreads = Collections.synchronizedSet(new HashSet<String>())

        Counting(Path workDir, Map params, boolean hash = true) {
            super(workDir, params, hash)
        }

        @Override
        protected Cid hashFile(Path path) {
            hashed.incrementAndGet()
            return super.hashFile(path)
        }

        @Override
        protected Path realPathOf(Path path) {
            resolvingThreads << Thread.currentThread().name
            if( path in unresolvable )
                throw new NoSuchFileException(path.toString())
            return links.containsKey(path) ? links[path] : super.realPathOf(path)
        }

        @Override
        protected BasicFileAttributes statOf(Path path) {
            return sizes.containsKey(path) ? new RemoteFiles.Attributes(sizes[path]) : super.statOf(path)
        }
    }

    private static InputRecorder.Staged staged(Path p) {
        return new InputRecorder.Staged(p, p, 'S1')
    }

    private Path mkfifo(String name) {
        final Path p = tempDir.resolve(name)
        assert new ProcessBuilder('mkfifo', p.toString()).start().waitFor() == 0
        return p
    }

    private static long millisSince(long nanos) {
        return (System.nanoTime() - nanos).intdiv(1_000_000L)
    }

    def 'a huge local file still hashing at the deadline is unfinished, and collect keeps to the bound'() {
        given:
        Path big = tempDir.resolve('data/huge.fq')
        Files.createDirectories(big.parent)
        new RandomAccessFile(big.toFile(), 'rw').withCloseable { it.setLength(64L * 1024 * 1024 * 1024) }
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        r.recordStaged([staged(big)])
        long t0 = System.nanoTime()
        InputSet.Input input = r.collect(1500).first()
        long ms = millisSince(t0)
        r.close()

        then:
        input.reason == 'unfinished'
        input.size == 64L * 1024 * 1024 * 1024
        ms < 3000
    }

    def 'a FIFO as a task input is never opened: unaddressed at once, with no size (hash: #hash)'() {
        given:
        Path fifo = mkfifo('reads.fq')
        Counting r = new Counting(tempDir.resolve('work'), [:], hash)

        when:
        r.recordStaged([staged(fifo)])
        long t0 = System.nanoTime()
        List<InputSet.Input> out = r.collect(5000)
        long ms = millisSince(t0)
        r.close()

        then:
        out.size() == 1
        out[0].name == 'reads.fq'
        out[0].reason == 'unaddressed'
        out[0].address == null
        out[0].size == null
        r.hashed.get() == 0
        r.leftUnfinished == 0
        ms < 2000

        where:
        hash << [true, false]
    }

    def 'a FIFO as params.input is never opened, so the pipeline keeps every byte its writer sends'() {
        given:
        Path fifo = mkfifo('sheet.csv')
        Process writer = new ProcessBuilder('sh', '-c', "printf 'sample,fastq_1\\nA,a.fq\\n' > '${fifo}'").start()
        Counting r = new Counting(tempDir.resolve('work'), [input: fifo.toString()])

        when:
        r.recordSamplesheet()
        List<InputSet.Input> out = r.collect(5000)
        r.close()
        boolean writerDone = writer.waitFor(500, TimeUnit.MILLISECONDS)

        then: 'nothing opened the FIFO for reading, so the writer is still waiting for its reader'
        !writerDone
        out.size() == 1
        out[0].role == 'samplesheet'
        out[0].name == 'sheet.csv'
        out[0].reason == 'unaddressed'
        out[0].address == null
        r.hashed.get() == 0

        cleanup:
        writer?.destroyForcibly()
    }

    def 'a link to a FIFO, as /dev/stdin and /dev/fd/N from --input <(...) are, is never opened'() {
        given:
        Path fifo = mkfifo('pipe')
        Path link = Files.createSymbolicLink(tempDir.resolve('data/input.csv').tap { Files.createDirectories(it.parent) }, fifo)
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        r.recordStaged([staged(link)])
        long t0 = System.nanoTime()
        List<InputSet.Input> out = r.collect(5000)
        long ms = millisSince(t0)
        r.close()

        then:
        out*.reason == ['unaddressed']
        out[0].name == 'input.csv'
        r.hashed.get() == 0
        ms < 2000
    }

    /**
     * On Linux, {@code /dev/fd/N} from {@code --input <(...)} and a piped
     * {@code /dev/stdin} are links to {@code pipe:[n]} or {@code socket:[n]},
     * which name no file: realpath fails, or a provider could give the bare
     * text. A FIFO here stands for the pipe the kernel's stat sees through
     * the link; opening it with no writer would block.
     */
    def 'a link to a pipe or socket whose real path is #linux, as on Linux, is unaddressed and never opened (#how)'() {
        given:
        Path fd = mkfifo('63')
        Counting r = new Counting(tempDir.resolve('work'), how == 'params.input' ? [input: fd.toString()] : [:])
        if( linux == 'no real path' )
            r.unresolvable << fd
        else
            r.links[fd] = Paths.get(linux)

        when:
        if( how == 'params.input' )
            r.recordSamplesheet()
        else
            r.recordStaged([staged(fd)])
        long t0 = System.nanoTime()
        List<InputSet.Input> out = r.collect(5000)
        long ms = millisSince(t0)
        r.close()

        then:
        out.size() == 1
        out[0].name == '63'
        out[0].reason == 'unaddressed'
        out[0].address == null
        out[0].size == null
        r.hashed.get() == 0
        r.leftUnfinished == 0
        ms < 2000

        where:
        [linux, how] << [['pipe:[123]', 'socket:[45]', 'no real path'], ['a task input', 'params.input']].combinations()
    }

    def 'a device (#device) is never opened: unaddressed, with no size'() {
        given:
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        r.recordStaged([staged(Paths.get(device))])
        long t0 = System.nanoTime()
        List<InputSet.Input> out = r.collect(5000)
        long ms = millisSince(t0)
        r.close()

        then:
        out*.reason == ['unaddressed']
        out[0].size == null
        r.hashed.get() == 0
        ms < 2000

        where:
        device << ['/dev/zero', '/dev/null']
    }

    def 'a link to a regular file is hashed through the link'() {
        given:
        Path target = tempDir.resolve('store/S1_1.fq')
        Files.createDirectories(target.parent)
        Files.writeString(target, 'ACGT')
        Path link = Files.createSymbolicLink(tempDir.resolve('S1_1.fq'), target)
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        List<InputSet.Input> out = r.with { recordStaged([staged(link)]); collect(-1) }
        r.close()

        then:
        out.size() == 1
        out[0].name == 'S1_1.fq'
        out[0].address == Hashing.hashRaw(target)
        out[0].size == 4L
        out[0].reason == null
        r.hashed.get() == 1
    }

    def 'a directory is unaddressed and a missing local file unreadable; neither is hashed'() {
        given:
        Path dir = Files.createDirectories(tempDir.resolve('ref/idx'))
        Path missing = tempDir.resolve('gone/x.fq')
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        List<InputSet.Input> out = r.with { recordStaged([staged(dir), staged(missing)]); collect(2000) }
        r.close()

        then:
        out.collect { [it.name, it.reason, it.size] } as Set == [['idx', 'unaddressed', null], ['x.fq', 'unreadable', null]] as Set
        r.hashed.get() == 0
    }

    def 'a local link into /fusion is judged by its real path: remote, never read, the real path resolved on the pool (#how)'() {
        given:
        Path link = tempDir.resolve('data/reads.fq')
        Files.createDirectories(link.parent)
        Path fused = Paths.get('/fusion/s3/bucket/reads.fq')
        Files.createSymbolicLink(link, fused)
        Counting r = new Counting(tempDir.resolve('work'), how == 'params.input' ? [input: link.toString()] : [:])
        r.links[link] = fused
        r.sizes[fused] = 21L

        when:
        if( how == 'params.input' )
            r.recordSamplesheet()
        else
            r.recordStaged([staged(link)])
        List<InputSet.Input> out = r.collect(-1)
        r.close()

        then:
        out.size() == 1
        out[0].name == 'reads.fq'
        out[0].reason == 'remote'
        out[0].size == 21L
        out[0].address == null
        r.hashed.get() == 0
        !r.resolvingThreads.isEmpty()
        r.resolvingThreads.every { it.startsWith('nf-blocks-inputs-') }

        where:
        how << ['a task input', 'params.input']
    }

    def 'a dangling link is unreadable and never hashed (#target)'() {
        given:
        Path link = tempDir.resolve('data/reads.fq')
        Files.createDirectories(link.parent)
        Files.createSymbolicLink(link, Paths.get(target))
        Counting r = new Counting(tempDir.resolve('work'), [:])

        when:
        List<InputSet.Input> out = r.with { recordStaged([staged(link)]); collect(-1) }
        r.close()

        then:
        out*.reason == ['unreadable']
        out[0].address == null
        r.hashed.get() == 0

        where:
        target << ['/fusion/s3/no-such-nf-blocks-bucket/reads.fq', '/no/such/nf-blocks/reads.fq']
    }
}
