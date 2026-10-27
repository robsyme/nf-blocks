package robsyme.cas.core

/**
 * The runs of the cross-run lineage specs: an upstream `people` run, and a
 * downstream `intros` run that read its four person items (spec 5.4). The
 * intro of Dev read input 3, the circle read all four.
 */
class LineageFixture {

    static final String WHO = 'lineage-test@example.org'
    static final List<String> PEOPLE = ['Alice', 'Bob', 'Carol', 'Dev']

    final RetentionFixture f

    /** The upstream run's `out` collection, its four person items by name, and a Selection of them. */
    final Cid people
    final Map<String, Cid> persons
    final Cid sel = Fixtures.cidOf([kind: 'Selection', name: 'S'])

    LineageFixture(RetentionFixture f) {
        this.f = f
        final Map<String, Cid> raws = PEOPLE.collectEntries { String n -> [(n): f.raw("person ${n}")] }
        people = (Cid) f.run('people', PEOPLE.collect { String n -> [person: raws[n]] }).collection
        persons = PEOPLE.collectEntries { String n -> [(n): item('person', raws[n])] }
    }

    /** The OutputItem RetentionFixture.run writes for one leaf, so a lineage can name it before the run is written. */
    Cid item(String leaf, Cid content) {
        return f.store.putDagCbor(OutputItem.of([(leaf): Leaf.of(leaf, content, f.sizeOf(content))]).toCbor())
    }

    /** Alice..Dev's datafiles, each read from that person's item; Alice's through a pick by query, so no collection and no Selection. Without `withOrigins`, no input says where it came from. */
    InputSet datafiles(boolean withOrigins = true) {
        return new InputSet(WHO, PEOPLE.collect { String n ->
            final InputSet.Origin origin = n == 'Alice'
                ? new InputSet.Origin(persons[n], null, null)
                : new InputSet.Origin(persons[n], people, sel)
            final InputSet.Input input = new InputSet.Input(InputSet.TASK, "${n}.txt".toString(), Fixtures.contentCid("data of ${n}"), 10L, null, null)
            withOrigins ? input.withFrom([origin]) : input
        })
    }

    /** A downstream run: the intro of Dev read input 3, the circle read all four. */
    Map introsRun(InputSet set, Map options = [:]) {
        final Cid setCid = f.store.putDagCbor(set.toCbor())
        final Cid introRaw = f.raw('intro of Dev')
        final Cid circleRaw = f.raw('the circle')
        final Cid introDev = item('intro', introRaw)
        final Cid circle = item('circle', circleRaw)
        final ItemLineage lineage = new ItemLineage(WHO, (Cid) (options.lineageInputSet ?: setCid), [
            ItemLineage.Derived.of(introDev, [3], [new ItemLineage.TaskRef('a' * 32, 'Introduce', 'Dev')]),
            ItemLineage.Derived.of(circle, [0, 1, 2, 3], [new ItemLineage.TaskRef('b' * 32, 'Circle', null)]),
        ])
        final Map lineageBlock = lineage.toCbor()
        final Cid lineageCid = options.storeLineage == false ? Fixtures.cidOf(lineageBlock) : f.store.putDagCbor(lineageBlock)
        final Map run = f.run('intros', [[intro: introRaw], [circle: circleRaw]], [inputSet: setCid, itemLineage: lineageCid])
        return run + [set: set, setCid: setCid, introDev: introDev, circle: circle, lineage: lineageBlock, lineageCid: lineageCid]
    }
}
