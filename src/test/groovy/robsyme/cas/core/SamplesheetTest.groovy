package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.TempDir

/** Spec section 10 and decision 18: one row per item, Meta Map columns, file columns by structural position. */
class SamplesheetTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Cid bamA, bamB, chunk1, chunk2, qc

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        bamA = Fixtures.contentCid('A')
        bamB = Fixtures.contentCid('B')
        chunk1 = Fixtures.contentCid('c1')
        chunk2 = Fixtures.contentCid('c2')
        qc = Fixtures.cidOf([kind: 'DirectoryManifest', schema: 1, entries: []])
    }

    private Cid item(Object value) { store.putDagCbor(Fixtures.outputItem(value)) }

    def 'tuple items: Meta Map columns by dotted path, then file columns by tuple position'() {
        given:
        final Cid a = item([[sample: 'A', lane: 1L, single_end: false, nested: [kit: 'truseq', ids: [1L, 2L]]], Fixtures.leaf('A.bam', bamA, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a])
        final List<String> lines = sheet.csv().readLines()

        then:
        // Meta Map columns come out in the item's decoded (canonical DAG-CBOR)
        // key order -- shortest key first, ties broken bytewise -- not the
        // construction order above: 'lane'(4) < 'nested'(6) < 'sample'(6) <
        // 'single_end'(10), and within 'nested', 'ids'(3) < 'kit'(3).
        sheet.columns == ['lane', 'nested.ids', 'nested.kit', 'sample', 'single_end', '1']
        sheet.rows[0].files == ['1': "cas://${bamA}/A.bam".toString()]
        sheet.rows[0].flat['nested.ids'] == [1L, 2L]
        lines == ['lane,nested.ids,nested.kit,sample,single_end,1', "1,\"[1,2]\",truseq,A,false,cas://${bamA}/A.bam".toString()]
    }

    def 'a key first seen in a later row is a later column, blank above it'() {
        given:
        final Cid a = item([[sample: 'A'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B', depth: 1.5d], Fixtures.leaf('B.bam', bamB, 1L)])
        final List<Cid> items = [a, b].sort { it.toString() }

        when:
        final Samplesheet sheet = Samplesheet.of(store, items)

        then:
        sheet.rows*.item == items
        // Every Meta Map column precedes every file column; 'depth' precedes
        // 'sample' because b's item is first by cid string, and within it
        // 'depth'(5 bytes) sorts before 'sample'(6) in canonical DAG-CBOR.
        sheet.columns == ['depth', 'sample', '1']
        parse(sheet.csv()).find { it.sample == 'A' }.depth == ''
        parse(sheet.csv()).find { it.sample == 'B' }.depth == '1.5'
    }

    def 'a list of files is one column per position; a directory leaf is cas://<manifest>; an unaddressed leaf is blank'() {
        given:
        final Cid c = item([[sample: 'C'], [Fixtures.leaf('chunk_1.txt', chunk1, 1L), Fixtures.leaf('chunk_2.txt', chunk2, 1L)],
                            Fixtures.leaf('C_qc', qc, 0L), Fixtures.unaddressedLeaf('gone.txt')])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [c])

        then:
        sheet.columns == ['sample', '1.0', '1.1', '2', '3']
        sheet.rows[0].files == ['1.0': "cas://${chunk1}/chunk_1.txt".toString(), '1.1': "cas://${chunk2}/chunk_2.txt".toString(),
                                '2': "cas://${qc}".toString(), '3': '']
    }

    def 'a record item names its file columns by key'() {
        given:
        final Cid r = item([sample: 'R', bam: Fixtures.leaf('R.bam', bamA, 1L)])

        expect:
        Samplesheet.of(store, [r]).columns == ['sample', 'bam']
    }

    def 'CSV quotes commas, quotes and newlines; blank where a key is absent (Review Focus 3)'() {
        given:
        final Cid a = item([[sample: 'A', note: 'tumour, "batch 2"\nsecond line'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B', extra: 'x'], Fixtures.leaf('B.bam', bamB, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a, b].sort { it.toString() })
        final String csv = sheet.csv()

        then:
        csv.contains('"tumour, ""batch 2""\nsecond line"')
        parse(csv).collect { it.sample } as Set == ['A', 'B'] as Set
        parse(csv).find { it.sample == 'B' }.note == ''
        parse(csv).find { it.sample == 'A' }.extra == ''
    }

    def 'non-ASCII text survives CSV and JSON unchanged (Review Focus 3)'() {
        given:
        final String text = 'café – naïve ✓ 𝄞'
        final Cid a = item([[sample: 'A', note: text], Fixtures.leaf('A.bam', bamA, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a])
        final String csv = sheet.csv()
        final byte[] csvUtf8 = csv.getBytes('UTF-8')

        then:
        // quote() only quotes for a comma, a quote, CR or LF (controller ruling); this text
        // has none of those, so it is written plain, both as a Java String and as UTF-8 bytes.
        csv.contains(text)
        new String(csvUtf8, 'UTF-8').contains(text)
        parse(csv).find { it.sample == 'A' }.note == text
        ((List<Map>) new JsonSlurper().parseText(sheet.json())).find { it.sample == 'A' }.note == text
    }

    def 'a list-valued Meta Map key with non-ASCII text is not unicode-escaped, in the CSV cell or the JSON (final review finding 3)'() {
        given:
        final Cid a = item([[sample: 'A', tags: ['café', 'naïve']], Fixtures.leaf('A.bam', bamA, 1L)])

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a])
        final String csv = sheet.csv()
        final String json = sheet.json()

        then:
        csv.contains('café') && csv.contains('naïve')
        !csv.contains('\\u')
        json.contains('café') && json.contains('naïve')
        !json.contains('\\u')
        parse(csv).find { it.sample == 'A' }.tags == '["café","naïve"]'
        ((List<Map>) new JsonSlurper().parseText(json)).find { it.sample == 'A' }.tags == ['café', 'naïve']
    }

    def 'JSON keeps nesting, types and absence'() {
        given:
        final Cid a = item([[sample: 'A', lane: 1L, depth: 1.5d, nested: [kit: 'truseq']], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid b = item([[sample: 'B'], Fixtures.leaf('B.bam', bamB, 1L)])

        when:
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(Samplesheet.of(store, [a, b].sort { it.toString() }).json())

        then:
        rows.find { it.sample == 'A' } == [sample: 'A', lane: 1, depth: 1.5, nested: [kit: 'truseq'], '1': "cas://${bamA}/A.bam".toString()]
        rows.find { it.sample == 'B' } == [sample: 'B', '1': "cas://${bamB}/B.bam".toString()]
    }

    def 'a file column named like a Meta Map column is written file.<position>'() {
        given:
        final Cid a = item([['1': 'one'], Fixtures.leaf('A.bam', bamA, 1L)])

        expect:
        Samplesheet.of(store, [a]).columns == ['1', 'file.1']
    }

    def 'with occurrences: a leading occurrence column, one per row, in CSV and JSON; without, nothing changes'() {
        given:
        final Cid a = item([[sample: 'A'], Fixtures.leaf('A.bam', bamA, 1L)])
        final String occ = "cas://${Fixtures.cidOf([kind: 'OutputCollection', n: 1])}/${a}".toString()

        when:
        final Samplesheet sheet = Samplesheet.of(store, [a], [occ])

        then:
        sheet.columns == ['occurrence', 'sample', '1']
        sheet.csv().readLines() == ['occurrence,sample,1', "${occ},A,cas://${bamA}/A.bam".toString()]
        new JsonSlurper().parseText(sheet.json()) == [[occurrence: occ, sample: 'A', '1': "cas://${bamA}/A.bam".toString()]]
        Samplesheet.of(store, [a]).columns == ['sample', '1']
    }

    def 'with occurrences, a Meta Map key named occurrence is meta.occurrence and a file position so named is file.occurrence'() {
        given:
        final Cid t = item([[occurrence: 'first'], Fixtures.leaf('A.bam', bamA, 1L)])
        final Cid r = item([sample: 'R', occurrence: Fixtures.leaf('R.bam', bamB, 1L)])
        final Cid coll = Fixtures.cidOf([kind: 'OutputCollection', n: 1])
        final List<Cid> items = [t, r].sort { it.toString() }
        final List<String> occs = items.collect { Cid i -> "cas://${coll}/${i}".toString() }

        when:
        final Samplesheet sheet = Samplesheet.of(store, items, occs)
        final List<Map> rows = (List<Map>) new JsonSlurper().parseText(sheet.json())

        then:
        sheet.columns[0] == 'occurrence'
        sheet.columns as Set == ['occurrence', 'meta.occurrence', 'sample', '1', 'file.occurrence'] as Set
        rows.find { it['meta.occurrence'] == 'first' }.occurrence == "cas://${coll}/${t}".toString()
        rows.find { it.sample == 'R' }['file.occurrence'] == "cas://${bamB}/R.bam".toString()
        Samplesheet.of(store, [t]).columns == ['occurrence', '1']
    }

    def 'occurrences must pair with items one to one'() {
        when:
        Samplesheet.of(store, [item([[sample: 'A']])], [])

        then:
        thrown(IllegalArgumentException)
    }

    /** A small RFC 4180 reader, so the test does not trust the writer. */
    private static List<Map<String, String>> parse(String csv) {
        final List<List<String>> records = []
        List<String> record = []
        StringBuilder field = new StringBuilder()
        boolean quoted = false
        for( int i = 0; i < csv.length(); i++ ) {
            final char ch = csv.charAt(i)
            if( quoted ) {
                if( ch == '"' as char && i + 1 < csv.length() && csv.charAt(i + 1) == '"' as char ) { field.append('"'); i++ }
                else if( ch == '"' as char ) quoted = false
                else field.append(ch)
            }
            else if( ch == '"' as char ) quoted = true
            else if( ch == ',' as char ) { record << field.toString(); field = new StringBuilder() }
            else if( ch == '\n' as char ) { record << field.toString(); records << record; record = []; field = new StringBuilder() }
            else field.append(ch)
        }
        final List<String> header = records[0]
        return records.drop(1).collect { List<String> r -> [header, r].transpose().collectEntries() as Map<String, String> }
    }
}
