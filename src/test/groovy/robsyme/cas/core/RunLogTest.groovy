package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §5: `runs/<rts>-<cid>`, one empty file per RunCompletion, with
 * `rts = String.format('%013d', 9999999999999L - finishedAtMillis)` so a
 * lexicographic listing is newest first.
 */
class RunLogTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
    }

    static Cid cidOf(String text) {
        return Cid.of(Cid.DAG_CBOR, Hashing.sha256(text.getBytes('UTF-8')))
    }

    private List<String> namesOnDisk() {
        final Path runs = tempDir.resolve('store').resolve('runs')
        return Files.list(runs).map { Path p -> p.fileName.toString() }.sorted().toList()
    }

    def 'append writes an empty file named by the reverse timestamp and the cid'() {
        given:
        def cid = cidOf('one')

        when:
        RunLog.append(store, cid, 1_700_000_000_000L)

        then:
        namesOnDisk() == ['8299999999999-' + cid]
        Files.size(tempDir.resolve('store').resolve('runs').resolve('8299999999999-' + cid)) == 0
    }

    def 'the reverse timestamp is always 13 digits'() {
        when:
        RunLog.append(store, cidOf('early'), 1L)

        then:
        namesOnDisk()[0].split('-')[0].length() == 13
        namesOnDisk()[0].startsWith('9999999999998-')
    }

    def 'appending the same entry twice is success'() {
        given:
        def cid = cidOf('one')

        when:
        RunLog.append(store, cid, 1_700_000_000_000L)
        RunLog.append(store, cid, 1_700_000_000_000L)

        then:
        namesOnDisk().size() == 1
    }

    def 'read lists the newest entry first'() {
        given:
        def older = cidOf('older')
        def newer = cidOf('newer')

        when:
        RunLog.append(store, older, 1_000_000_000_000L)
        RunLog.append(store, newer, 2_000_000_000_000L)
        def entries = RunLog.read(store)

        then:
        entries.size() == 2
        entries[0].cid == newer
        entries[0].finishedAtMillis == 2_000_000_000_000L
        entries[1].cid == older
        entries[1].finishedAtMillis == 1_000_000_000_000L
        entries[0].name < entries[1].name
    }

    def 'ties on the timestamp break by cid'() {
        given:
        def a = cidOf('a')
        def b = cidOf('b')

        when:
        RunLog.append(store, a, 1_000L)
        RunLog.append(store, b, 1_000L)
        def entries = RunLog.read(store)

        then:
        entries.size() == 2
        entries*.cid*.toString() == [a, b]*.toString().sort()
    }

    def 'entriesAfter returns only what is newer than the watermark'() {
        given:
        def one = cidOf('one')
        def two = cidOf('two')
        def three = cidOf('three')
        RunLog.append(store, one, 1_000L)
        RunLog.append(store, two, 2_000L)
        RunLog.append(store, three, 3_000L)
        def log = RunLog.of(store)
        def watermark = log.read().find { it.cid == two }.name

        when:
        def newer = log.entriesAfter(watermark)

        then:
        newer*.cid == [three]

        and: 'a null watermark is everything'
        log.entriesAfter(null)*.cid == [three, two, one]
    }

    def 'an empty log reads as nothing'() {
        expect:
        RunLog.read(store) == []
        RunLog.of(store).entriesAfter('9999999999999-x') == []
    }

    def 'a malformed entry name is ignored'() {
        given:
        def good = cidOf('good')
        RunLog.append(store, good, 1_000L)
        def runs = tempDir.resolve('store').resolve('runs')
        Files.createFile(runs.resolve('not-a-run-log-entry'))
        Files.createFile(runs.resolve('9999999999999-notacid'))
        Files.createFile(runs.resolve('abcdefghijklm-' + cidOf('x')))

        when:
        def entries = RunLog.read(store)

        then:
        entries*.cid == [good]
    }

    def 'the log of a composite store is the log of its writable member'() {
        given:
        def other = new LocalBlockStore(tempDir.resolve('other'), 'archive', false)
        def composite = new CompositeStore([store, other])
        def cid = cidOf('one')

        when:
        RunLog.append(composite, cid, 1_000L)

        then:
        namesOnDisk().size() == 1
        RunLog.read(composite)*.cid == [cid]
    }
}
