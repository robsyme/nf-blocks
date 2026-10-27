package robsyme.cas.trace

import java.nio.file.Path
import java.security.MessageDigest

import com.google.common.hash.HashCode
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import robsyme.cas.core.Cid
import robsyme.cas.core.InputSet
import robsyme.cas.core.ItemLineage
import robsyme.cas.nio.CasFileSystemProvider
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The walk of cross-run lineage spec §5.2 step 3, on noted facts only: the
 * paths sit under a temp dir and need not exist, since the walk never touches
 * a file system. The consumer run is the spec's: four Introduce tasks each
 * stage one cas:// datafile and publish {@code <name>_intro.txt}; FriendCircle
 * stages the four intros and publishes {@code circle.txt}.
 */
class ItemDerivationTest extends Specification {

    @TempDir
    Path tempDir

    private CasFileSystemProvider provider = new CasFileSystemProvider()

    private Path work(String name) {
        return tempDir.resolve('work').resolve(name)
    }

    private Path casPath(String name) {
        return provider.getPath(URI.create("cas://rob/people/${name}/${name}_Patel.dat"))
    }

    private static byte[] sha256(String text) {
        return MessageDigest.getInstance('SHA-256').digest(text.getBytes('UTF-8'))
    }

    private static Cid raw(String name) {
        return Cid.of(Cid.RAW, sha256(name))
    }

    private static Cid item(String name) {
        return Cid.of(Cid.DAG_CBOR, sha256("item:${name}"))
    }

    private static String hashOf(String name) {
        return MessageDigest.getInstance('MD5').digest(name.getBytes('UTF-8')).encodeHex().toString()
    }

    /** An output as the Join would give it; {@code derive} reads only its item cids and join keys. */
    private static Join.JoinedOutput joined(String name, List<List> entries) {
        return new Join.JoinedOutput(name, [], entries.collect { (Cid) it[0] }, entries.collect { (List<String>) it[1] }, null)
    }

    /** A file the task was given and staged under its own name; Nextflow's storePath is the source itself here. */
    private static InputRecorder.Staged staged(Path source, String sample = null) {
        return new InputRecorder.Staged(source, source, sample, source.fileName.toString())
    }

    private static InputSet.Input input(String name) {
        return new InputSet.Input('task', name, raw(name), 10L, null, null)
    }

    def 'intros name their one datafile and Introduce; circle names all four through the intermediate tasks'() {
        given:
        def d = new ItemDerivation()
        def names = ['Alice', 'Bob', 'Carol', 'Dev']
        def stagedBy = names.collectEntries { n -> [(n): staged(casPath(n), n)] }
        names.each { n ->
            d.noteTask(hashOf(n), work("i${n}"), 'Introduce', n, [stagedBy[n]])
            d.notePublish("cas://rob/intros/${n}_intro.txt", work("i${n}").resolve("${n}_intro.txt"))
        }
        d.noteTask(hashOf('circle'), work('c'), 'FriendCircle', null,
            names.collect { n -> staged(work("i${n}").resolve("${n}_intro.txt")) })
        d.notePublish('cas://rob/circle/circle.txt', work('c').resolve('circle.txt'))
        def inputs = names.collect { n -> new InputSet.Input('task', "${n}_Patel.dat", raw(n), 10L, n, null) }
        def set = new InputSet('rob', inputs)
        def runInputs = names.collectEntries { n -> [(stagedBy[n]): inputs[names.indexOf(n)]] }

        when:
        def derived = d.derive([joined('intros', names.collect { n -> [item(n), ["cas://rob/intros/${n}_intro.txt".toString()]] }),
                                joined('circle', [[item('circle'), ['cas://rob/circle/circle.txt']]])], set, runInputs)

        then:
        derived.size() == 5
        derived.find { it.item == item('Dev') }.inputs == [set.positionOf(inputs[3])]
        derived.find { it.item == item('Dev') }.tasks == [new ItemLineage.TaskRef(hashOf('Dev'), 'Introduce', 'Dev')]
        derived.find { it.item == item('circle') }.inputs == [0, 1, 2, 3]
        derived.find { it.item == item('circle') }.tasks == [new ItemLineage.TaskRef(hashOf('circle'), 'FriendCircle', null)]
    }

    def 'a published staged link maps to that input only'() {
        given: 'task P stages in.dat and ref.fa, both run inputs, and publishes its in.dat link and a made file'
        def d = new ItemDerivation()
        def inDat = staged(tempDir.resolve('data/in.dat'))
        def refFa = staged(tempDir.resolve('data/ref.fa'))
        d.noteTask(hashOf('P'), work('p'), 'P', null, [inDat, refFa])
        d.notePublish('cas://rob/pass/in.dat', work('p').resolve('in.dat'))
        d.notePublish('cas://rob/made/out.txt', work('p').resolve('out.txt'))
        def set = new InputSet('rob', [input('in.dat'), input('ref.fa')])
        def runInputs = [(inDat): input('in.dat'), (refFa): input('ref.fa')]

        when:
        def derived = d.derive([joined('pass', [[item('pass'), ['cas://rob/pass/in.dat']]]),
                                joined('made', [[item('made'), ['cas://rob/made/out.txt']]])], set, runInputs)

        then:
        derived.find { it.item == item('pass') }.inputs == [set.positionOf(input('in.dat'))]
        derived.find { it.item == item('made') }.inputs == [0, 1]
    }

    def 'a published staged link of an intermediate file is followed through that file only'() {
        given: 'Q stages P\'s out.txt and a run input and publishes the out.txt link unchanged'
        def d = new ItemDerivation()
        def inDat = staged(tempDir.resolve('data/in.dat'))
        def other = staged(tempDir.resolve('data/other.dat'))
        d.noteTask(hashOf('P'), work('p'), 'P', null, [inDat])
        d.noteTask(hashOf('Q'), work('q'), 'Q', null, [staged(work('p').resolve('out.txt')), other])
        d.notePublish('cas://rob/q/out.txt', work('q').resolve('out.txt'))
        def set = new InputSet('rob', [input('in.dat'), input('other.dat')])

        when:
        def derived = d.derive([joined('q', [[item('q'), ['cas://rob/q/out.txt']]])], set,
            [(inDat): input('in.dat'), (other): input('other.dat')])

        then: 'the publishing task is Q, the input is what P read'
        derived.find { it.item == item('q') }.inputs == [set.positionOf(input('in.dat'))]
        derived.find { it.item == item('q') }.tasks*.process == ['Q']
    }

    def 'a source outside every task gives no entry; a value-only item gives no entry'() {
        given:
        def d = new ItemDerivation()
        def inDat = staged(tempDir.resolve('data/in.dat'))
        d.noteTask(hashOf('P'), work('p'), 'P', null, [inDat])
        d.notePublish('cas://rob/out/x.txt', tempDir.resolve('elsewhere/x.txt'))
        d.notePublish('cas://rob/out/p.txt', work('p').resolve('p.txt'))
        def set = new InputSet('rob', [input('in.dat')])
        def outsideItem = item('outside')
        def valuesItem = item('values')
        def unpublishedItem = item('unpublished')

        when:
        def derived = d.derive([joined('out', [[outsideItem, ['cas://rob/out/x.txt']],
                                               [valuesItem, []],
                                               [unpublishedItem, ['cas://rob/out/never.txt']],
                                               [item('p'), ['cas://rob/out/p.txt']]])],
            set, [(inDat): input('in.dat')])

        then:
        derived*.item == [item('p')]
    }

    def 'two tasks staging each other end'() {
        given: 'A stages a file under B\'s work dir and the seed; B stages one under A\'s and publishes'
        def d = new ItemDerivation()
        def seed = staged(tempDir.resolve('data/seed.txt'))
        d.noteTask(hashOf('A'), work('a'), 'A', null, [staged(work('b').resolve('b.txt')), seed])
        d.noteTask(hashOf('B'), work('b'), 'B', null, [staged(work('a').resolve('a.txt'))])
        d.notePublish('cas://rob/loop/out.txt', work('b').resolve('out.txt'))
        def set = new InputSet('rob', [input('seed.txt')])

        when:
        def derived = d.derive([joined('loop', [[item('loop'), ['cas://rob/loop/out.txt']]])], set, [(seed): input('seed.txt')])

        then:
        derived.find { it.item == item('loop') }.inputs == [set.positionOf(input('seed.txt'))]
        derived.find { it.item == item('loop') }.tasks*.process == ['B']
    }

    def 'one item in two outputs is one entry with the union'() {
        given:
        def d = new ItemDerivation()
        def x = staged(tempDir.resolve('data/x.dat'))
        def y = staged(tempDir.resolve('data/y.dat'))
        d.noteTask(hashOf('X'), work('x'), 'X', null, [x])
        d.noteTask(hashOf('Y'), work('y'), 'Y', null, [y])
        d.notePublish('cas://rob/one/s.txt', work('x').resolve('s.txt'))
        d.notePublish('cas://rob/two/s.txt', work('y').resolve('s.txt'))
        def set = new InputSet('rob', [input('x.dat'), input('y.dat')])
        def shared = item('shared')

        when:
        def derived = d.derive([joined('one', [[shared, ['cas://rob/one/s.txt']]]),
                                joined('two', [[shared, ['cas://rob/two/s.txt']]])], set,
            [(x): input('x.dat'), (y): input('y.dat')])

        then:
        derived.count { it.item == shared } == 1
        derived.find { it.item == shared }.inputs == [0, 1]
        derived.find { it.item == shared }.tasks*.process == ['X', 'Y'].sort { hashOf(it) }
    }

    def 'the last publish for a key wins and paths are compared normalized'() {
        given:
        def d = new ItemDerivation()
        def a = staged(tempDir.resolve('data/a.dat'))
        def b = staged(tempDir.resolve('data/b.dat'))
        d.noteTask(hashOf('A'), work('a'), 'A', null, [a])
        d.noteTask(hashOf('B'), work('b/./sub/..'), 'B', null, [b])
        d.notePublish('cas://rob/o/f.txt', work('a').resolve('f.txt'))
        d.notePublish('cas://rob/o/f.txt', work('a').resolve('../b/f.txt'))
        def set = new InputSet('rob', [input('a.dat'), input('b.dat')])

        when:
        def derived = d.derive([joined('o', [[item('f'), ['cas://rob/o/f.txt']]])], set, [(a): input('a.dat'), (b): input('b.dat')])

        then:
        derived.find { it.item == item('f') }.inputs == [set.positionOf(input('b.dat'))]
    }

    def 'a task reference carries a scrubbed tag'() {
        given:
        def d = new ItemDerivation()
        def home = "/Users/${System.getProperty('user.name')}/reads/S1.fq"
        d.noteTask(hashOf('T'), work('t'), 'T', "S1 ${home}", [])
        d.notePublish('cas://rob/t/o.txt', work('t').resolve('o.txt'))

        when:
        def derived = d.derive([joined('t', [[item('t'), ['cas://rob/t/o.txt']]])], new InputSet('rob', []), [:])

        then: 'a task with no run input still names its item'
        derived.find { it.item == item('t') }.inputs == []
        def tag = derived.find { it.item == item('t') }.tasks[0].tag
        tag.startsWith('S1 ')
        !tag.contains(System.getProperty('user.name'))
    }

    def 'a task that could not be named is not noted, so the other items keep their lineage'() {
        given: 'a hash that is not 32 lowercase hex, and a blank process'
        def d = new ItemDerivation()
        d.noteTask('NOT-A-HASH', work('bad'), 'Bad', null, [])
        d.noteTask(hashOf('blank'), work('blank'), ' ', null, [])
        d.noteTask(hashOf('T'), work('t'), 'T', null, [])
        ['bad', 'blank', 't'].each { String n -> d.notePublish("cas://rob/${n}/o.txt".toString(), work(n).resolve('o.txt')) }

        when:
        def derived = d.derive([joined('o', ['bad', 'blank', 't'].collect { String n -> [item(n), ["cas://rob/${n}/o.txt".toString()]] })],
            new InputSet('rob', []), [:])

        then:
        d.noted('NOT-A-HASH') == null
        d.noted(hashOf('blank')) == null
        derived*.item == [item('t')]
        derived[0].tasks == [new ItemLineage.TaskRef(hashOf('T'), 'T', null)]
    }

    def 'noteTask(TaskRun) copies hash, work dir, process and tag'() {
        given:
        def d = new ItemDerivation()
        def task = new TaskRun()
        task.processor = Stub(TaskProcessor) { getName() >> 'Introduce' }
        task.hash = HashCode.fromString(hashOf('Dev'))
        task.workDir = work('iDev')
        task.config = new TaskConfig([tag: ' Dev '])

        when:
        d.noteTask(task, [])
        def noted = d.noted(hashOf('Dev'))

        then:
        noted.ref.hash ==~ /[0-9a-f]{32}/
        noted.workDir == work('iDev')
        noted.ref.process == 'Introduce'
        noted.ref.tag == 'Dev'
    }

    def 'noteTask(TaskRun) ignores a task with no hash or no work dir, and reads no tag as null'() {
        given:
        def d = new ItemDerivation()
        def noHash = new TaskRun(processor: Stub(TaskProcessor) { getName() >> 'P' }, workDir: work('n'))
        def noDir = new TaskRun(processor: Stub(TaskProcessor) { getName() >> 'P' }, hash: HashCode.fromString(hashOf('noDir')))
        def untagged = new TaskRun(processor: Stub(TaskProcessor) { getName() >> 'P' }, hash: HashCode.fromString(hashOf('untagged')),
            workDir: work('u'), config: new TaskConfig([:]))

        when:
        d.noteTask(noHash, [])
        d.noteTask(noDir, [])
        d.noteTask(untagged, [])

        then:
        d.noted(hashOf('noDir')) == null
        d.noted(hashOf('untagged')).ref.tag == null
    }
}
