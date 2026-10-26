package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** P4 plan 1.3 and R2: a run's InputSet fills `run_input` and `consumer`, never `missing`, `block` or `block_ref`. */
class IndexRunInputTest extends Specification {

    static final String WHO = 'p4-test@example.org'

    @TempDir Path tmp
    RetentionFixture f
    Index index

    /** Read bytes the store does not hold: a samplesheet and two FASTQs another lab keeps. */
    final Cid sheet = Fixtures.contentCid('sample,fastq_1\nA,a.fq.gz\nB,b.fq.gz\n')
    final Cid readsA = Fixtures.contentCid('reads of A')
    final Cid readsB = Fixtures.contentCid('reads of B')

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private void catchUp(Index into = index, BlockStore store = f.store) {
        into.catchUp(store, StoreLog.of(store), store.alias())
    }

    private InputSet inputSet() {
        return new InputSet(WHO, [
            new InputSet.Input(InputSet.SAMPLESHEET, 'samplesheet.csv', sheet, 40L, null, null),
            new InputSet.Input(InputSet.TASK, 'a.fq.gz', readsA, 10L, 'A', null),
            new InputSet.Input(InputSet.TASK, 'b.fq.gz', readsB, 10L, 'B', null),
            new InputSet.Input('param:fasta', 'genome.fa', null, null, null, InputSet.UNADDRESSED),
        ])
    }

    /** A run whose RunCompletion links {@code set}, stored unless {@code store} is false. */
    private Map runWith(InputSet set, String name = 'r', boolean store = true) {
        final Cid cid = store ? f.store.putDagCbor(set.toCbor()) : Fixtures.cidOf(set.toCbor())
        final Map run = f.run(name, [[x: f.raw("x of ${name}")]], [inputSet: cid])
        run.inputSet = cid
        return run
    }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
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

    private List<List<Object>> expectedInputs(Cid completion) {
        final String c = completion.toString()
        return [
            [c, 'param:fasta', 'genome.fa', null, null, null],
            [c, 'samplesheet', 'samplesheet.csv', sheet.toString(), 40, null],
            [c, 'task', 'a.fq.gz', readsA.toString(), 10, 'A'],
            [c, 'task', 'b.fq.gz', readsB.toString(), 10, 'B'],
        ]
    }

    private List<List<Object>> expectedConsumers(Cid completion) {
        final String c = completion.toString()
        return [
            [readsA.toString(), c, 'a.fq.gz', 'task'],
            [readsB.toString(), c, 'b.fq.gz', 'task'],
            [sheet.toString(), c, 'samplesheet.csv', 'samplesheet'],
        ].sort { it[0] }
    }

    private static final String INPUTS = 'SELECT * FROM run_input ORDER BY role, name'
    private static final String CONSUMERS = 'SELECT * FROM consumer ORDER BY content_cid'

    def "a run's InputSet gives one run_input row per input and one consumer row per addressed input"() {
        given:
        final Map run = runWith(inputSet())

        when:
        catchUp()

        then:
        rows(index.file, INPUTS) == expectedInputs((Cid) run.completion)
        rows(index.file, CONSUMERS) == expectedConsumers((Cid) run.completion)

        and: 'inputs the store does not hold are no missing rows, and no storage rows'
        index.countRows('missing') == 0
        rows(index.file, "SELECT cid FROM block WHERE cid IN ('${sheet}', '${readsA}', '${readsB}')") == []
        rows(index.file, "SELECT cid FROM block_ref WHERE cid IN ('${sheet}', '${readsA}', '${readsB}')") == []

        and: 'runInputs answers from the rows'
        index.runInputs((Cid) run.completion).collect { RunInputRow r -> [r.role, r.name, r.address?.toString(), r.size, r.sample] } ==
            expectedInputs((Cid) run.completion).collect { it.subList(1, 6) }.collect { [it[0], it[1], it[2], it[3] == null ? null : (it[3] as Long), it[4]] }
        index.runInputs((Cid) run.completion).every { RunInputRow r -> r.completion == run.completion }
    }

    def 'a run with no InputSet writes no input rows, as before'() {
        given:
        final Map run = f.run('old', [[x: f.raw('x')]])

        when:
        catchUp()

        then:
        index.isRunIndexed((Cid) run.completion)
        index.countRows('run_input') == 0
        index.countRows('consumer') == 0
        index.runInputs((Cid) run.completion) == []
    }

    def 'ingesting the run again leaves one set of input rows'() {
        given:
        final Map run = runWith(inputSet())

        when:
        catchUp()
        index.ingestRun(f.store, (Cid) run.completion, 'lab')
        index.ingestRun(f.store, (Cid) run.completion, 'lab')
        index.setMetaValue('store_log_watermark:lab', null)
        catchUp()

        then:
        rows(index.file, INPUTS) == expectedInputs((Cid) run.completion)
        rows(index.file, CONSUMERS) == expectedConsumers((Cid) run.completion)
    }

    def 'an InputSet not yet arrived leaves the run indexed and a wait record, no missing row, retried once it lands'() {
        given:
        final InputSet set = inputSet()
        final Map run = runWith(set, 'r', false)

        when:
        catchUp()

        then:
        index.isRunIndexed((Cid) run.completion)
        index.countRows('run_input') == 0
        index.countRows('missing') == 0
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == run.inputSet.toString()

        when: 'it arrives after the watermark has passed the run'
        f.store.putDagCbor(set.toCbor())
        f.clock += 60 * 60_000L
        f.run('later', [[x: f.raw('later')]])
        catchUp()

        then:
        rows(index.file, INPUTS) == expectedInputs((Cid) run.completion)
        rows(index.file, CONSUMERS) == expectedConsumers((Cid) run.completion)
        index.countRows('missing') == 0
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == null
    }

    def 'a malformed InputSet is skipped with the run still indexed, and is not retried'() {
        given:
        final Cid bad = f.store.putDagCbor([kind: 'InputSet', schema: 1L, asserted_by: WHO, inputs: 'not a list'])
        final Map run = f.run('r', [[x: f.raw('x')]], [inputSet: bad])
        final Cid other = f.dir([:])
        final Map second = f.run('s', [[x: f.raw('y')]], [inputSet: other])

        when:
        catchUp()
        catchUp()

        then:
        index.isRunIndexed((Cid) run.completion)
        index.isRunIndexed((Cid) second.completion)
        index.countRows('run_input') == 0
        index.countRows('consumer') == 0
        index.countRows('missing') == 0
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == null
        index.metaValue(Index.INPUTSET_WAIT + second.completion) == null
    }

    def "forgetting a run removes its run_input and consumer rows"() {
        given:
        final Map kept = runWith(inputSet(), 'kept')
        final Map gone = runWith(new InputSet(WHO, [new InputSet.Input(InputSet.TASK, 'a.fq.gz', readsA, 10L, 'A', null)]), 'gone')
        catchUp()

        when:
        index.forget([(Cid) gone.completion])

        then:
        rows(index.file, INPUTS) == expectedInputs((Cid) kept.completion)
        rows(index.file, CONSUMERS) == expectedConsumers((Cid) kept.completion)
    }

    def 'reset empties run_input and consumer'() {
        given:
        runWith(inputSet())
        catchUp()

        when:
        index.reset()

        then:
        index.countRows('run_input') == 0
        index.countRows('consumer') == 0
    }

    def "a member's snapshot carries its runs' input rows, and a seeded cache has them"() {
        given:
        final LocalBlockStore shared = new LocalBlockStore(tmp.resolve('shared'), 'shared', true)
        final Map run = runWith(inputSet())
        final RetentionFixture g = new RetentionFixture(shared)
        final Cid sharedSet = shared.putDagCbor(new InputSet(WHO, [new InputSet.Input(InputSet.TASK, 'c.fq.gz', readsB, 10L, 'C', null)]).toCbor())
        final Map sharedRun = g.run('shared', [[x: g.raw('s')]], [inputSet: sharedSet])
        catchUp()
        catchUp(index, shared)
        assert index.countRows('run_input') == 5

        when:
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        final Path snapshot = tmp.resolve('lab').resolve(IndexSnapshot.relativePath())

        then:
        rows(snapshot, INPUTS) == expectedInputs((Cid) run.completion)
        rows(snapshot, CONSUMERS) == expectedConsumers((Cid) run.completion)
        !rows(snapshot, 'SELECT completion_cid FROM run_input').contains([sharedRun.completion.toString()])

        when: 'a cold cache seeds from it'
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        rows(cold.file, INPUTS) == expectedInputs((Cid) run.completion)
        rows(cold.file, CONSUMERS) == expectedConsumers((Cid) run.completion)

        cleanup:
        cold?.close()
    }

    def 'an InputSet block that cannot be read for now is retried'() {
        given:
        final Map run = runWith(inputSet())
        final IndexTest.Unreachable unreachable = new IndexTest.Unreachable(f.store, (Cid) run.inputSet)

        when:
        index.catchUp(unreachable, StoreLog.of(f.store), 'lab')

        then:
        index.isRunIndexed((Cid) run.completion)
        index.countRows('run_input') == 0
        index.countRows('missing') == 0
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == run.inputSet.toString()

        when:
        unreachable.broken = false
        index.catchUp(unreachable, StoreLog.of(f.store), 'lab')

        then:
        rows(index.file, INPUTS) == expectedInputs((Cid) run.completion)
        index.countRows('missing') == 0
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == null
    }

    /** Records every block a catch-up asks the store about. */
    static class HasCounting extends IndexTest.Counting {
        final List<Cid> asked = []
        HasCounting(BlockStore delegate) { super(delegate) }
        @Override boolean has(Cid cid) { asked.add(cid); return super.has(cid) }
    }

    def "a catch-up asks only about its member's waiting InputSets, never a run's other missing blocks"() {
        given: 'a run whose OutputCollection never arrives'
        final Cid manifest = f.store.putDagCbor(RetentionFixture.manifest('gone', f.raw('script of gone')))
        final Cid absentCollection = Fixtures.cidOf([kind: 'OutputCollection', name: 'never written'])
        final Cid gone = f.store.putDagCbor(RetentionFixture.completion(manifest, [absentCollection]))
        StoreLog.append(f.store, StoreLogKind.RUN, gone, ++f.clock)

        and: 'a run here and one of another member whose InputSets have not arrived'
        final Map waiting = runWith(inputSet(), 'waiting', false)
        final LocalBlockStore shared = new LocalBlockStore(tmp.resolve('shared'), 'shared', true)
        final RetentionFixture g = new RetentionFixture(shared)
        final Cid sharedSet = Fixtures.cidOf(new InputSet(WHO, [new InputSet.Input(InputSet.TASK, 'c.fq.gz', readsB, 10L, 'C', null)]).toCbor())
        g.run('s', [[x: g.raw('s')]], [inputSet: sharedSet])
        catchUp()
        catchUp(index, shared)
        assert rows(index.file, 'SELECT have_cid, needed_cid FROM missing') == [[gone.toString(), absentCollection.toString()]]

        when:
        final HasCounting counting = new HasCounting(f.store)
        index.catchUp(counting, StoreLog.of(f.store), 'lab')

        then:
        counting.asked.contains((Cid) waiting.inputSet)
        !counting.asked.contains(absentCollection)
        !counting.asked.contains(sharedSet)
    }

    def 'a waiting InputSet does not make the storage figures partial, and a cache seeded from the snapshot still waits for it'() {
        given:
        final InputSet set = inputSet()
        final Map run = runWith(set, 'r', false)
        catchUp()

        when:
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), 0L, null, tmp.resolve('t'))
        final Path snapshot = tmp.resolve('lab').resolve(IndexSnapshot.relativePath())

        then:
        rows(snapshot, 'SELECT partial FROM store_stats') == [[0]]
        rows(snapshot, 'SELECT DISTINCT partial FROM root_stats') == [[0]]

        when: 'a cold cache seeds from it'
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab', new LocalSnapshotStorage(tmp.resolve('lab')), tmp.resolve('t'))

        then:
        cold.metaValue('seeded_from:lab') != null
        cold.metaValue(Index.INPUTSET_WAIT + run.completion) == run.inputSet.toString()

        when: 'the InputSet lands'
        f.store.putDagCbor(set.toCbor())
        cold.catchUp(f.store, StoreLog.of(f.store), 'lab')

        then:
        rows(cold.file, INPUTS) == expectedInputs((Cid) run.completion)
        rows(cold.file, CONSUMERS) == expectedConsumers((Cid) run.completion)
        cold.metaValue(Index.INPUTSET_WAIT + run.completion) == null

        cleanup:
        cold?.close()
    }

    def "forgetting a waiting run drops its wait record"() {
        given:
        final Map run = runWith(inputSet(), 'r', false)
        catchUp()
        assert index.metaValue(Index.INPUTSET_WAIT + run.completion) != null

        when:
        index.forget([(Cid) run.completion])

        then:
        index.metaValue(Index.INPUTSET_WAIT + run.completion) == null
    }
}
