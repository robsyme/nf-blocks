package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Cross-run lineage spec §5.4: a run's InputSet gives each `run_input` row its
 * position and one `input_origin` row per Origin, and its ItemLineage one
 * `item_input` row per item and one `item_task` row per task, all keyed by
 * position in that InputSet.
 */
class IndexItemLineageTest extends Specification {

    static final String WHO = LineageFixture.WHO

    @TempDir Path tmp
    RetentionFixture f
    LineageFixture lineageFixture
    Index index

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
        lineageFixture = new LineageFixture(f)
    }

    def cleanup() { index?.close() }

    private Cid getPeople() { lineageFixture.people }
    private Map<String, Cid> getPersons() { lineageFixture.persons }
    private Cid getSel() { lineageFixture.sel }
    private Cid item(String leaf, Cid content) { lineageFixture.item(leaf, content) }
    private InputSet datafiles() { lineageFixture.datafiles() }
    private Map introsRun(InputSet set, Map options = [:]) { lineageFixture.introsRun(set, options) }

    private List<List<Object>> rows(String sql, Object arg) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${index.file}")
        try {
            final PreparedStatement ps = c.prepareStatement(sql)
            if( arg != null )
                ps.setString(1, arg.toString())
            final ResultSet rs = ps.executeQuery()
            final int n = rs.metaData.columnCount
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..n).collect { int i -> rs.getObject(i) }
            return out
        }
        finally {
            c.close()
        }
    }

    private String meta(String key) { index.metaValue(key) }

    private int count(String table) { index.countRows(table) }

    def 'a schema 3 run gives run_input positions, input_origin and item_input rows'() {
        given:
        final Map run = introsRun(datafiles())
        final InputSet set = (InputSet) run.set
        final String c = run.completion.toString()

        when:
        index.ingestRun(f.store, (Cid) run.completion, 'lab')

        then:
        rows('SELECT position, name FROM run_input WHERE completion_cid = ? ORDER BY position', c) ==
            set.inputs.withIndex().collect { InputSet.Input i, int n -> [n, i.name] }
        rows('SELECT position, item_cid, collection_cid, selection_cid FROM input_origin WHERE completion_cid = ? ORDER BY position', c) == [
            [0, persons.Alice.toString(), null, null],
            [1, persons.Bob.toString(), people.toString(), sel.toString()],
            [2, persons.Carol.toString(), people.toString(), sel.toString()],
            [3, persons.Dev.toString(), people.toString(), sel.toString()],
        ]
        rows('SELECT item_cid, positions FROM item_input WHERE completion_cid = ? ORDER BY item_cid', c) as Set ==
            [[run.introDev.toString(), '[3]'], [run.circle.toString(), '[0,1,2,3]']] as Set
        rows('SELECT item_cid, task_hash, process, tag FROM item_task WHERE completion_cid = ? ORDER BY item_cid', c) as Set == [
            [run.introDev.toString(), 'a' * 32, 'Introduce', 'Dev'],
            [run.circle.toString(), 'b' * 32, 'Circle', null],
        ] as Set

        and: 'an ItemLineage, like an InputSet, is never a missing, block or block_ref row'
        rows('SELECT count(*) FROM missing', null) == [[0]]
        rows('SELECT count(*) FROM block WHERE cid = ?', run.lineageCid) == [[0]]
        rows('SELECT count(*) FROM block_ref WHERE cid = ?', run.lineageCid) == [[0]]
    }

    def 'input_origin is keyed by position, so identical directories keep their own item'() {
        given: 'two inputs qc with the same manifest address, one from each item'
        final Cid qc = f.dir(['report.html': f.raw('qc report')])
        final Cid item1 = item('qc', f.raw('qc of 1'))
        final Cid item2 = item('qc', f.raw('qc of 2'))
        final InputSet set = new InputSet(WHO, [item1, item2].collect { Cid i ->
            new InputSet.Input(InputSet.TASK, 'qc', qc, 1L, null, null).withFrom([new InputSet.Origin(i, people, null)])
        })
        final Cid setCid = f.store.putDagCbor(set.toCbor())
        final Map run = f.run('qc', [[x: f.raw('x')]], [inputSet: setCid])
        final String c = run.completion.toString()

        when:
        index.ingestRun(f.store, (Cid) run.completion, 'lab')

        then: 'InputSet order puts the item with the lower CID bytes first'
        set.inputs.size() == 2
        rows('SELECT position, item_cid FROM input_origin WHERE completion_cid = ? ORDER BY position', c) ==
            [item1, item2].sort { Cid a, Cid b -> Arrays.compareUnsigned(a.bytes(), b.bytes()) }.withIndex().collect { Cid i, int n -> [n, i.toString()] }
        rows('SELECT position, name, address FROM run_input WHERE completion_cid = ? ORDER BY position', c) ==
            [[0, 'qc', qc.toString()], [1, 'qc', qc.toString()]]
    }

    def 'an input read from two items has an input_origin row for each'() {
        given:
        final Cid bytes = Fixtures.contentCid('the same bytes')
        final InputSet set = new InputSet(WHO, [new InputSet.Input(InputSet.TASK, 'same.txt', bytes, 1L, null, null)
            .withFrom([new InputSet.Origin(persons.Alice, people, null), new InputSet.Origin(persons.Bob, people, null)])])
        final Map run = f.run('same', [[x: f.raw('x')]], [inputSet: f.store.putDagCbor(set.toCbor())])

        when:
        index.ingestRun(f.store, (Cid) run.completion, 'lab')

        then:
        rows('SELECT position, item_cid FROM input_origin WHERE completion_cid = ? ORDER BY item_cid', run.completion) as Set ==
            [[0, persons.Alice.toString()], [0, persons.Bob.toString()]] as Set
    }

    def 'an ItemLineage that has not arrived waits, and is ingested when it arrives'() {
        given:
        final Map run = introsRun(datafiles(), [storeLineage: false])
        final Cid completion = (Cid) run.completion

        when:
        index.ingestRun(f.store, completion, 'lab')

        then:
        meta("${Index.ITEMLINEAGE_WAIT}${completion}") == run.lineageCid.toString()
        count('item_input') == 0
        count('missing') == 0
        count('input_origin') == 4

        when:
        f.store.putDagCbor((Map) run.lineage)
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then:
        meta("${Index.ITEMLINEAGE_WAIT}${completion}") == null
        count('item_input') == 2
        count('item_task') == 2
        count('missing') == 0
    }

    def 'an ItemLineage that cannot be read for now is retried'() {
        given:
        final Map run = introsRun(datafiles())
        final IndexTest.Unreachable unreachable = new IndexTest.Unreachable(f.store, (Cid) run.lineageCid)

        when:
        index.catchUp(unreachable, StoreLog.of(f.store), 'lab')

        then:
        index.isRunIndexed((Cid) run.completion)
        count('item_input') == 0
        count('run_input') == 4
        meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") == run.lineageCid.toString()

        when:
        unreachable.broken = false
        index.catchUp(unreachable, StoreLog.of(f.store), 'lab')

        then:
        count('item_input') == 2
        meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") == null
    }

    def 'an item lineage naming another input set is skipped'() {
        given:
        final Cid other = f.store.putDagCbor(new InputSet(WHO, []).toCbor())
        final Map run = introsRun(datafiles(), [lineageInputSet: other])

        when:
        index.ingestRun(f.store, (Cid) run.completion, 'lab')

        then:
        count('item_input') == 0
        count('item_task') == 0
        meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") == null

        and: 'the rest of the run is indexed'
        count('input_origin') == 4
        index.isRunIndexed((Cid) run.completion)
    }

    def 'a malformed ItemLineage is skipped with the run still indexed, and is not retried'() {
        given:
        final Cid setCid = f.store.putDagCbor(datafiles().toCbor())
        final Cid bad = f.store.putDagCbor([kind: 'ItemLineage', schema: 1L, asserted_by: WHO, input_set: setCid, items: 'not a list'])
        final Map run = f.run('bad', [[x: f.raw('x')]], [inputSet: setCid, itemLineage: bad])

        when:
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then:
        index.isRunIndexed((Cid) run.completion)
        count('item_input') == 0
        count('run_input') == 4
        count('missing') == 0
        meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") == null
    }

    def 're-ingesting and forgetting a run clear its rows'() {
        given:
        final Map run = introsRun(datafiles())
        final Cid completion = (Cid) run.completion

        when:
        index.ingestRun(f.store, completion, 'lab')
        index.ingestRun(f.store, completion, 'lab')

        then:
        count('input_origin') == 4
        count('item_input') == 2
        count('item_task') == 2

        when:
        index.forget([completion])

        then:
        count('input_origin') == 0
        count('item_input') == 0
        count('item_task') == 0
    }

    def 'forgetting a run whose ItemLineage waits drops its wait record'() {
        given:
        final Map run = introsRun(datafiles(), [storeLineage: false])
        index.ingestRun(f.store, (Cid) run.completion, 'lab')
        assert meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") != null

        when:
        index.forget([(Cid) run.completion])

        then:
        meta("${Index.ITEMLINEAGE_WAIT}${run.completion}") == null
    }

    def 'a schema 2 run (no item_lineage) indexes as before'() {
        given:
        final InputSet set = datafiles()
        final Cid manifest = f.store.putDagCbor(RetentionFixture.manifest('old', f.raw('script of old')))
        final Map block = RetentionFixture.completion(manifest, [], [inputSet: f.store.putDagCbor(set.toCbor())])
        block.schema = 2L
        block.remove('item_lineage')
        final Cid completion = f.store.putDagCbor(block)

        when:
        index.ingestRun(f.store, completion, 'lab')

        then:
        index.isRunIndexed(completion)
        count('item_input') == 0
        count('item_task') == 0
        rows('SELECT count(*) FROM run_input', null) == [[set.inputs.size()]]
        meta("${Index.ITEMLINEAGE_WAIT}${completion}") == null
    }
}
