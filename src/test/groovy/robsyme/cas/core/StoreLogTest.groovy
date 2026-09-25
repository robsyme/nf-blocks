package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * DESIGN.md §5 as amended: `log/<rts>-<kind>-<cid>`, one empty file per block
 * written outside a run's closure, `rts` the reverse of the moment the entry
 * was written, so a lexicographic listing is newest first.
 */
class StoreLogTest extends Specification {

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
        final Path log = tempDir.resolve('store').resolve('log')
        return Files.list(log).map { Path p -> p.fileName.toString() }.sorted().toList()
    }

    def 'append writes an empty file named by reverse timestamp, kind and cid'() {
        given:
        def cid = cidOf('one')

        when:
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_700_000_000_000L)

        then:
        namesOnDisk() == ["${String.format('%013d', 9999999999999L - 1_700_000_000_000L)}-run-${cid}".toString()]
        Files.size(tempDir.resolve('store/log').resolve(namesOnDisk()[0])) == 0
    }

    def 'nothing is written under runs/'() {
        when:
        StoreLog.append(store, StoreLogKind.RUN, cidOf('one'), 1_000L)

        then:
        !Files.exists(tempDir.resolve('store/runs'))
    }

    def 'every kind round-trips through its token'() {
        expect:
        StoreLogKind.fromToken(kind.token) == kind
        StoreLog.parse(StoreLog.entryName(kind, cidOf('x'), 5_000L)).kind == kind

        where:
        kind << StoreLogKind.values().toList()
    }

    def 'parse recovers the written time and the cid'() {
        given:
        def cid = cidOf('p')

        when:
        def entry = StoreLog.parse(StoreLog.entryName(StoreLogKind.SELECTION, cid, 1_234_567L))

        then:
        entry.writtenAtMillis == 1_234_567L
        entry.cid == cid
        entry.kind == StoreLogKind.SELECTION
    }

    def 'read lists the newest entry first'() {
        given:
        def older = cidOf('older')
        def newer = cidOf('newer')
        StoreLog.append(store, StoreLogKind.RUN, older, 1_000L)
        StoreLog.append(store, StoreLogKind.CLAIM, newer, 2_000L)

        expect:
        StoreLog.read(store)*.cid == [newer, older]
    }

    def 'appending the same entry twice is success'() {
        given:
        def cid = cidOf('twice')

        when:
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_000L)
        StoreLog.append(store, StoreLogKind.RUN, cid, 1_000L)

        then:
        namesOnDisk().size() == 1
    }

    def 'unknown kinds and malformed names are ignored, the rest still reads'() {
        given:
        def good = cidOf('good')
        StoreLog.append(store, StoreLogKind.RUN, good, 1_000L)
        def dir = tempDir.resolve('store/log')
        Files.createFile(dir.resolve("${String.format('%013d', 9999999999999L - 2_000L)}-pin-${cidOf('future')}"))
        Files.createFile(dir.resolve('README'))
        Files.createFile(dir.resolve("123-run-${cidOf('short')}"))
        Files.createFile(dir.resolve("${String.format('%013d', 9999999999999L - 3_000L)}-run-notacid"))

        expect:
        StoreLog.read(store)*.cid == [good]
    }

    def 'entriesSince returns everything newer than the watermark plus the overlap window'() {
        given: 'entries at 0, 20 min, 25 min and 40 min'
        def base = 1_700_000_000_000L
        def e0 = cidOf('e0'); def e20 = cidOf('e20'); def e25 = cidOf('e25'); def e40 = cidOf('e40')
        StoreLog.append(store, StoreLogKind.RUN, e0, base)
        StoreLog.append(store, StoreLogKind.RUN, e20, base + 20 * 60_000L)
        StoreLog.append(store, StoreLogKind.RUN, e25, base + 25 * 60_000L)
        StoreLog.append(store, StoreLogKind.RUN, e40, base + 40 * 60_000L)
        def log = StoreLog.of(store)
        def watermark = StoreLog.entryName(StoreLogKind.RUN, e25, base + 25 * 60_000L)

        expect: 'the 40-minute entry, the watermark itself and the 20-minute entry inside the 10-minute window'
        log.entriesSince(watermark)*.cid == [e40, e25, e20]
    }

    def 'a watermark from a clock running ahead cannot hide entries written by correct clocks'() {
        given: 'now is t, the watermark came from a host one hour fast, a correct host wrote at t - 1 min'
        def now = 1_700_000_000_000L
        def ahead = cidOf('ahead'); def correct = cidOf('correct')
        StoreLog.append(store, StoreLogKind.RUN, ahead, now + 60 * 60_000L)
        StoreLog.append(store, StoreLogKind.RUN, correct, now - 60_000L)
        def watermark = StoreLog.entryName(StoreLogKind.RUN, ahead, now + 60 * 60_000L)

        expect:
        StoreLog.of(store).entriesSince(watermark, now)*.cid.contains(correct)
    }

    def 'entriesSince with no watermark is everything'() {
        given:
        StoreLog.append(store, StoreLogKind.RUN, cidOf('a'), 1_000L)
        StoreLog.append(store, StoreLogKind.RUN, cidOf('b'), 2_000L)

        expect:
        StoreLog.of(store).entriesSince(null).size() == 2
        StoreLog.of(store).entriesSince('').size() == 2
    }

    def 'an empty log reads as nothing'() {
        expect:
        StoreLog.read(store) == []
    }

    def 'the log of a composite store is the log of its writable member'() {
        given:
        def other = new LocalBlockStore(tempDir.resolve('other'), 'other', false)
        def composite = new CompositeStore([store, other] as List<BlockStore>)

        when:
        StoreLog.append(composite, StoreLogKind.RUN, cidOf('c'), 1_000L)

        then:
        StoreLog.read(store).size() == 1
        !Files.exists(tempDir.resolve('other/log'))
    }

    def 'a timestamp outside the encodable range is refused'() {
        when:
        StoreLog.reverseTimestamp(-1L)

        then:
        thrown(IllegalArgumentException)
    }
}
