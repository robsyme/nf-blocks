package robsyme.cas.core

import spock.lang.Specification
import spock.lang.Timeout

import robsyme.cas.core.StorageStats.Root

/**
 * P2 task 2: per-store reachability and storage aggregates (rulings R1 to R5).
 * Every expected value is worked out by hand in the comments, never by
 * calling the code under test.
 */
class StorageStatsTest extends Specification {

    // The hand-worked fixture, reused by task 3. Raw blocks A..F of 1, 2, 4,
    // 8, 16 and 32 B; items i1..i6 hold A..F, one each; all runs in pipeline p.
    static final Map<String, Long> SIZES = [A: 1L, B: 2L, C: 4L, D: 8L, E: 16L, F: 32L]
    static final Map<String, List<String>> REFS = [
        i1: ['A'], i2: ['B'], i3: ['C'], i4: ['D'], i5: ['E'], i6: ['F'],
    ]
    // D has no extension; B and C share one once lower-cased.
    static final Map<String, List<String>> NAMES = [
        A: ['a.txt'], B: ['b.bam'], C: ['c.BAM'], D: ['d'], E: ['e.txt'], F: ['f.html'],
    ]
    static final Map<String, List<String>> COLLECTIONS = [
        c1: ['i1', 'i2', 'i3'], c2: ['i3', 'i4'], c3: ['i4', 'i5', 'i6'],
    ]
    static final List<Root> ROOTS = [
        new Root(1, 'run1', StorageStats.RUN, 'ana', StorageStats.HELD, 't1', 'p', ['i1', 'i2', 'i3']),
        new Root(2, 'run2', StorageStats.RUN, 'ben', StorageStats.HELD, 't2', 'p', ['i3', 'i4']),
        new Root(3, 'run3', StorageStats.RUN, 'cy', StorageStats.RELEASED, 't3', 'p', ['i4', 'i5', 'i6']),
        new Root(4, 'sel1', StorageStats.SELECTION, 'ana', StorageStats.HELD, 't4', null, ['i2']),
        new Root(5, 'i6', StorageStats.PIN, 'ben', StorageStats.PINNED, 't5', null, ['i6']),
        new Root(6, 'run1', StorageStats.PIN, 'ben', StorageStats.PINNED, 't6', null, ['i1', 'i2', 'i3']),
    ]

    /**
     * The dedup ratio as the page computes it from a store_stats row
     * (web/src/model.js, storeStats): referenced over stored, null when
     * nothing is stored.
     */
    static Double pageDedup(Map storeStats) {
        final long stored = (storeStats.stored_bytes ?: 0L) as long
        stored > 0 ? ((storeStats.referenced_bytes ?: 0L) as long) / (double) stored : null
    }

    static StorageStats fixture() {
        StorageStats.compute(SIZES, REFS, NAMES, ROOTS, COLLECTIONS, [] as Set<String>)
    }

    def 'store totals of the fixture'() {
        expect:
        // stored: A..F = 63 B in 6 blocks. referenced: c1 1+2+4, c2 4+8, c3 8+16+32 = 7+12+56 = 75.
        // pinned: r5 reaches F, r6 reaches A B C: 39. held: r1 r2 r4 reach A B C D, minus pinned: D = 8.
        // releasing: E = 16. shared by two or more run roots: C (r1, r2) and D (r2, r3) = 12.
        fixture().store == [stored_bytes: 63, stored_blocks: 6, referenced_bytes: 75, pinned_bytes: 39,
                            held_bytes: 8, releasing_bytes: 16, shared_bytes: 12, partial: 0]
    }

    def 'root stats of the fixture'() {
        expect:
        // Content roots: r1 {A B C}, r2 {C D}, r4 {B}, r5 {F}, r6 {A B C}. r3 {D E F} is released.
        // Exclusive = blocks no other content root reaches: r2 D, r3 E (D is r2's, F is r5's), r5 F.
        // pinned_bytes = the closure's part of the pinned set {A B C F}.
        fixture().rootStats == [
            [root_id: 1, reachable_bytes: 7, reachable_blocks: 3, exclusive_bytes: 0, exclusive_blocks: 0, pinned_bytes: 7, partial: 0],
            [root_id: 2, reachable_bytes: 12, reachable_blocks: 2, exclusive_bytes: 8, exclusive_blocks: 1, pinned_bytes: 4, partial: 0],
            [root_id: 3, reachable_bytes: 56, reachable_blocks: 3, exclusive_bytes: 16, exclusive_blocks: 1, pinned_bytes: 32, partial: 0],
            [root_id: 4, reachable_bytes: 2, reachable_blocks: 1, exclusive_bytes: 0, exclusive_blocks: 0, pinned_bytes: 2, partial: 0],
            [root_id: 5, reachable_bytes: 32, reachable_blocks: 1, exclusive_bytes: 32, exclusive_blocks: 1, pinned_bytes: 32, partial: 0],
            [root_id: 6, reachable_bytes: 7, reachable_blocks: 3, exclusive_bytes: 0, exclusive_blocks: 0, pinned_bytes: 7, partial: 0],
        ]
    }

    def 'root shared of the fixture, ties broken by other cid then id'() {
        when:
        final List<Map> shared = fixture().rootShared

        then:
        // r1 {A B C}: r6 A+B+C = 7, r2 C = 4, r4 B = 2.
        // r2 {C D}: r1 C = 4 and r6 C = 4; both have cid run1, so id 1 comes first.
        // r4 {B}: r1 2, r6 2. r5 {F}: no other content root. r6 mirrors r1.
        // r3 is released, so it has no rows and is nobody's "other".
        shared.findAll { it.root_id == 2 } == [[root_id: 2, other_root_id: 1, bytes: 4], [root_id: 2, other_root_id: 6, bytes: 4]]
        shared == [
            [root_id: 1, other_root_id: 6, bytes: 7], [root_id: 1, other_root_id: 2, bytes: 4], [root_id: 1, other_root_id: 4, bytes: 2],
            [root_id: 2, other_root_id: 1, bytes: 4], [root_id: 2, other_root_id: 6, bytes: 4],
            [root_id: 4, other_root_id: 1, bytes: 2], [root_id: 4, other_root_id: 6, bytes: 2],
            [root_id: 6, other_root_id: 1, bytes: 7], [root_id: 6, other_root_id: 2, bytes: 4], [root_id: 6, other_root_id: 4, bytes: 2],
        ]
    }

    def 'asserters of the fixture'() {
        expect:
        // ana: r1 and r4 reach A B C = 7, all also reached by ben's r6 or r2: exclusive 0.
        // ben: r2 r5 r6 reach A B C D F = 47; D and F are ben's alone = 40; pins reach A B C F = 39.
        // cy: r3 is released, so reachable 0; it reaches the releasing set {E}: 16.
        fixture().asserters == [
            [asserted_by: 'ana', reachable_bytes: 7, exclusive_bytes: 0, pinned_bytes: 0, releasing_bytes: 0],
            [asserted_by: 'ben', reachable_bytes: 47, exclusive_bytes: 40, pinned_bytes: 39, releasing_bytes: 0],
            [asserted_by: 'cy', reachable_bytes: 0, exclusive_bytes: 0, pinned_bytes: 0, releasing_bytes: 16],
        ]
    }

    def 'pipeline of the fixture follows R5'() {
        expect:
        // runs: three, none deleted. bytes: run1 and run2 hold content, A B C D = 15 in 4 blocks;
        // run3 is released and its cid is not pinned. releasing: run3's part of the releasing set, E = 16.
        fixture().pipelines == [[pipeline: 'p', runs: 3, bytes: 15, blocks: 4, releasing_bytes: 16]]
    }

    def 'collection and item sizes of the fixture'() {
        when:
        final StorageStats s = fixture()

        then:
        s.collectionSizes == [
            [collection_cid: 'c1', bytes: 7, blocks: 3],
            [collection_cid: 'c2', bytes: 12, blocks: 2],
            [collection_cid: 'c3', bytes: 56, blocks: 3],
        ]
        s.itemSizes == [
            [item_cid: 'i1', bytes: 1, blocks: 1], [item_cid: 'i2', bytes: 2, blocks: 1], [item_cid: 'i3', bytes: 4, blocks: 1],
            [item_cid: 'i4', bytes: 8, blocks: 1], [item_cid: 'i5', bytes: 16, blocks: 1], [item_cid: 'i6', bytes: 32, blocks: 1],
        ]
    }

    def 'kinds of the fixture, largest first'() {
        expect:
        // html: F 32. txt: A 1 + E 16 = 17. no extension: D 8. bam: B 2 + C 4 = 6.
        fixture().kinds == [
            [kind: 'html', files: 1, bytes: 32],
            [kind: 'txt', files: 2, bytes: 17],
            [kind: '', files: 1, bytes: 8],
            [kind: 'bam', files: 2, bytes: 6],
        ]
    }

    def 'pinned overlap of the fixture'() {
        expect:
        // Only ben pins: the set {A B C F}. run1 A B C = 7, run2 C = 4, run3 F = 32, sel1 B = 2,
        // collections c1 A B C = 7, c2 C = 4, c3 F = 32, items i1 1, i2 2, i3 4, i6 32
        // (i4 and i5 overlap nothing), and each pinned block at its size.
        fixture().pinnedOverlap as Set == [
            [subject_cid: 'c1', asserted_by: 'ben', bytes: 7],
            [subject_cid: 'c2', asserted_by: 'ben', bytes: 4],
            [subject_cid: 'c3', asserted_by: 'ben', bytes: 32],
            [subject_cid: 'run1', asserted_by: 'ben', bytes: 7],
            [subject_cid: 'run2', asserted_by: 'ben', bytes: 4],
            [subject_cid: 'run3', asserted_by: 'ben', bytes: 32],
            [subject_cid: 'sel1', asserted_by: 'ben', bytes: 2],
            [subject_cid: 'i1', asserted_by: 'ben', bytes: 1],
            [subject_cid: 'i2', asserted_by: 'ben', bytes: 2],
            [subject_cid: 'i3', asserted_by: 'ben', bytes: 4],
            [subject_cid: 'i6', asserted_by: 'ben', bytes: 32],
            [subject_cid: 'A', asserted_by: 'ben', bytes: 1],
            [subject_cid: 'B', asserted_by: 'ben', bytes: 2],
            [subject_cid: 'C', asserted_by: 'ben', bytes: 4],
            [subject_cid: 'F', asserted_by: 'ben', bytes: 32],
        ] as Set
        fixture().pinnedOverlap.size() == 15
    }

    def 'Referenced keeps multiplicity of items and of references'() {
        given:
        // i1 is listed twice in c1 and once in c2: 3 x 5. i2 names B twice: 2 x 3.
        // i3 holds a directory m of A and B: 5 + 3.
        final Map<String, List<String>> collections = [c1: ['i1', 'i1', 'i2'], c2: ['i1', 'i3']]
        final Map<String, List<String>> refs = [i1: ['A'], i2: ['B', 'B'], i3: ['m'], m: ['A', 'B']]
        final List<Root> roots = [new Root(1, 'run', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i1', 'i2', 'i3'])]

        when:
        final StorageStats s = StorageStats.compute([A: 5L, B: 3L], refs, [:], roots, collections, ['m'] as Set)

        then:
        s.store.referenced_bytes == 15 + 6 + 8
        // Stored counts each block once.
        s.store.stored_bytes == 8
        s.store.stored_blocks == 2
        // Collection and item sizes count unique blocks: c1 is A + B, i2 is B once.
        s.collectionSizes == [[collection_cid: 'c1', bytes: 8, blocks: 2], [collection_cid: 'c2', bytes: 8, blocks: 2]]
        s.itemSizes == [[item_cid: 'i1', bytes: 5, blocks: 1], [item_cid: 'i2', bytes: 3, blocks: 1], [item_cid: 'i3', bytes: 8, blocks: 2]]
    }

    def 'a reachable block with a null size counts as 0 and sets partial'() {
        given:
        final Map<String, Long> sizes = [A: null, B: 4L]
        final List<Root> roots = [
            new Root(1, 'run', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i1']),
            new Root(2, 'other', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i2']),
        ]

        when:
        final StorageStats s = StorageStats.compute(sizes, [i1: ['A', 'B'], i2: ['B']], [:], roots, [c: ['i1'], c2: ['i2']], [] as Set)

        then:
        s.store.stored_bytes == 4
        s.store.stored_blocks == 2
        // (c, i1) A 0 + B 4, (c2, i2) B 4.
        s.store.referenced_bytes == 8
        s.store.partial == 1
        s.rootStats[0].reachable_bytes == 4
        s.rootStats[0].reachable_blocks == 2
        s.rootStats[0].partial == 1
        // Root 2 reaches only B, which has a size.
        s.rootStats[1].partial == 0
    }

    def 'a subject pinned by two asserters leaves each pin nothing exclusive'() {
        given:
        final List<Root> roots = [
            new Root(1, 'i1', StorageStats.PIN, 'ana', StorageStats.PINNED, 't', null, ['i1']),
            new Root(2, 'i1', StorageStats.PIN, 'ben', StorageStats.PINNED, 't', null, ['i1']),
        ]

        when:
        final StorageStats s = StorageStats.compute([A: 10L], [i1: ['A']], [:], roots, [:], [] as Set)

        then:
        s.rootStats*.exclusive_bytes == [0, 0]
        s.rootStats*.pinned_bytes == [10, 10]
        s.asserters*.exclusive_bytes == [0, 0]
        s.asserters*.pinned_bytes == [10, 10]
        s.store.pinned_bytes == 10
        s.pinnedOverlap as Set == [
            [subject_cid: 'i1', asserted_by: 'ana', bytes: 10], [subject_cid: 'i1', asserted_by: 'ben', bytes: 10],
            [subject_cid: 'A', asserted_by: 'ana', bytes: 10], [subject_cid: 'A', asserted_by: 'ben', bytes: 10],
        ] as Set
    }

    def 'a manifest cycle terminates'() {
        given:
        // i1 -> m1 -> {A, m2}, m2 -> {B, m1}.
        final Map<String, List<String>> refs = [i1: ['m1'], m1: ['A', 'm2'], m2: ['B', 'm1']]
        final List<Root> roots = [new Root(1, 'run', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i1'])]

        when:
        final StorageStats s = StorageStats.compute([A: 1L, B: 2L], refs, [:], roots, [c: ['i1']], ['m1', 'm2'] as Set)

        then:
        s.store.stored_bytes == 3
        s.store.stored_blocks == 2
        s.store.referenced_bytes == 3
        s.itemSizes == [[item_cid: 'i1', bytes: 3, blocks: 2]]
    }

    def 'Referenced counts every holder that makes a block stored: memberships, Handoff files, and what else a root starts from'() {
        given:
        // A 1, B 2, C 4, D 8, E 16. Run r (released) outputs c = i1, i1 -> A. Selection s holds i1 and x, an item
        // no output collection here lists (another member's run, R8); x -> directory m -> B, C. Handoff h of s
        // has two file Leaves, D and A (a run file copied into it). Pins: C (under x), E (under no holder),
        // x (already a holder), A (under i1).
        final Map<String, List<String>> refs = [i1: ['A'], x: ['m'], m: ['B', 'C'], h: ['D', 'A']]
        final List<Root> roots = [
            new Root(1, 'r', StorageStats.RUN, 'ana', StorageStats.RELEASED, 't', 'p', ['i1']),
            new Root(2, 's', StorageStats.SELECTION, 'ana', StorageStats.HELD, 't', null, ['i1', 'x']),
            new Root(3, 'h', StorageStats.HANDOFF, 'ana', StorageStats.HELD, 't', null, ['h', 'i1', 'x']),
            new Root(4, 'C', StorageStats.PIN, 'ben', StorageStats.PINNED, 't', null, ['C']),
            new Root(5, 'E', StorageStats.PIN, 'ben', StorageStats.PINNED, 't', null, ['E']),
            new Root(6, 'x', StorageStats.PIN, 'cy', StorageStats.PINNED, 't', null, ['x']),
            new Root(7, 'A', StorageStats.PIN, 'cy', StorageStats.PINNED, 't', null, ['A']),
        ]

        when:
        final StorageStats s = StorageStats.compute([A: 1L, B: 2L, C: 4L, D: 8L, E: 16L], refs, [:], roots, [c: ['i1']], ['m'] as Set)

        then: 'stored is A..E once each'
        s.store.stored_bytes == 31
        s.store.stored_blocks == 5

        and: 'referenced: (c, i1) A 1; h D 8 + A 1; x once, B 2 + C 4; E 16. The pins on C, x and A add nothing.'
        s.store.referenced_bytes == 1 + 9 + 6 + 16
        (s.store.referenced_bytes as long) >= (s.store.stored_bytes as long)
    }

    def 'kind comes from the smallest name a block is referenced by'() {
        given:
        final List<Root> roots = [new Root(1, 'run', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i1'])]

        when:
        final StorageStats s = StorageStats.compute([A: 7L], [i1: ['A']], [A: ['b.BAM', 'a.txt']], roots, [:], [] as Set)

        then:
        s.kinds == [[kind: 'txt', files: 1, bytes: 7]]
    }

    def 'extOf mirrors web/src/sizes.js: #name gives "#ext"'() {
        expect:
        StorageStats.extOf(name) == ext

        where:
        name          | ext
        'a.txt'       | 'txt'
        'reads.BAM'   | 'bam'
        'x.tar.GZ'    | 'gz'
        'noext'       | ''
        '.bashrc'     | ''
        'trailing.'   | ''
        ''            | ''
        null          | ''
    }

    def 'an asserter key is trimmed and lower-cased, and null or blank is anonymous'() {
        given:
        final List<Root> roots = [
            new Root(1, 'r1', StorageStats.RUN, ' Ana@X ', StorageStats.HELD, 't', 'p', ['i1']),
            new Root(2, 'r2', StorageStats.RUN, 'ana@x', StorageStats.HELD, 't', 'p', ['i1']),
            new Root(3, 'r3', StorageStats.RUN, null, StorageStats.HELD, 't', 'p', ['i2']),
            new Root(4, 'r4', StorageStats.RUN, '  ', StorageStats.HELD, 't', 'p', ['i2']),
        ]

        when:
        final StorageStats s = StorageStats.compute([A: 1L, B: 2L], [i1: ['A'], i2: ['B']], [:], roots, [:], [] as Set)

        then:
        s.asserters == [
            [asserted_by: 'ana@x', reachable_bytes: 1, exclusive_bytes: 1, pinned_bytes: 0, releasing_bytes: 0],
            [asserted_by: 'anonymous', reachable_bytes: 2, exclusive_bytes: 2, pinned_bytes: 0, releasing_bytes: 0],
        ]
    }

    def 'keys and extensions lower-case the same way on a Turkish-locale JVM, as JS toLowerCase does'() {
        given:
        Locale before = Locale.getDefault()
        Locale.setDefault(new Locale('tr', 'TR'))

        expect:
        // Under tr-TR, String.toLowerCase() turns 'I' into a dotless 'ı'; principalKey in identity.js gives 'i'.
        StorageStats.asserter(' ADMIN@IO.X ') == 'admin@io.x'
        StorageStats.extOf('TRACE.TXT.ZIP') == 'zip'
        StorageStats.extOf('report.HTML') == 'html'
        StorageStats.extOf('LOG.LIST') == 'list'

        cleanup:
        Locale.setDefault(before)
    }

    def 'the asserter key is identity.js principalKey: ASCII whitespace trimmed, ASCII letters lowercased, nothing else'() {
        expect:
        StorageStats.asserter(raw) == key

        where:
        raw                  || key
        null                 || 'anonymous'
        ' \t\n '             || 'anonymous'
        ' Ana@Example.ORG\t'        || 'ana@example.org'
        'ÉLODIE@x'           || 'Élodie@x'
        '\u00a0bob@x'        || '\u00a0bob@x'
    }

    def 'root shared keeps the top five others'() {
        given:
        // Root 1 reaches A..F of 1..32 B. Selections 2..7 reach one block each, so
        // overlaps are 1, 2, 4, 8, 16, 32: root 1 keeps the five largest.
        final List<String> blocks = ['A', 'B', 'C', 'D', 'E', 'F']
        final List<Root> roots = [new Root(1, 'run', StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['i1', 'i2', 'i3', 'i4', 'i5', 'i6'])]
        blocks.eachWithIndex { String b, int i ->
            roots << new Root(i + 2, "s${i + 2}".toString(), StorageStats.SELECTION, 'ana', StorageStats.HELD, 't', null, ["i${i + 1}".toString()])
        }

        when:
        final List<Map> shared = StorageStats.compute(SIZES, REFS, [:], roots, [:], [] as Set).rootShared.findAll { it.root_id == 1 }

        then:
        shared == [
            [root_id: 1, other_root_id: 7, bytes: 32], [root_id: 1, other_root_id: 6, bytes: 16], [root_id: 1, other_root_id: 5, bytes: 8],
            [root_id: 1, other_root_id: 4, bytes: 4], [root_id: 1, other_root_id: 3, bytes: 2],
        ]
    }

    @Timeout(10)
    def 'root shared over 200 runs that share one block'() {
        given:
        // Runs 1..200 (cids run001..run200) each reach the shared block S of 1000 B
        // and a private block of their own. Only S overlaps, so every other run
        // overlaps by 1000 B, and the tie goes to the smaller cid: each run keeps
        // the five lowest-numbered runs other than itself.
        final Map<String, Long> sizes = [S: 1000L]
        final Map<String, List<String>> refs = [s: ['S']]
        final List<Root> roots = []
        for( int id = 1; id <= 200; id++ ) {
            sizes.put("P${id}".toString(), (long) id)
            refs.put("p${id}".toString(), ["P${id}".toString()])
            roots << new Root(id, String.format('run%03d', id), StorageStats.RUN, 'ana', StorageStats.HELD, 't', 'p', ['s', "p${id}".toString()])
        }

        when:
        final List<Map> shared = StorageStats.compute(sizes, refs, [:], roots, [:], [] as Set).rootShared

        then:
        shared.size() == 200 * 5
        (1..200).every { int id ->
            final List<Integer> expected = (1..200).findAll { it != id }.take(5)
            shared.findAll { it.root_id == id } == expected.collect { [root_id: id, other_root_id: it, bytes: 1000] }
        }
        shared.findAll { it.root_id == 1 }*.other_root_id == [2, 3, 4, 5, 6]
        shared.findAll { it.root_id == 4 }*.other_root_id == [1, 2, 3, 5, 6]
        shared.findAll { it.root_id == 200 }*.other_root_id == [1, 2, 3, 4, 5]
    }

    def 'R5: a released run whose cid is pinned holds content, a deleted one is not counted as a run'() {
        given:
        final List<Root> roots = [
            new Root(1, 'run1', StorageStats.RUN, 'ana', StorageStats.RELEASED, 't', 'p', ['i1']),
            new Root(2, 'run1', StorageStats.PIN, 'ben', StorageStats.PINNED, 't', null, ['i1']),
            new Root(3, 'run2', StorageStats.RUN, 'ana', StorageStats.DELETED, 't', 'p', ['i2']),
            new Root(4, 'run3', StorageStats.RUN, 'ana', StorageStats.DELETED, 't', 'p', ['i3']),
            new Root(5, 'run3', StorageStats.PIN, 'ben', StorageStats.PINNED, 't', null, ['i3']),
            new Root(6, 'run4', StorageStats.RUN, 'ana', StorageStats.RELEASED, 't', 'q', ['i4']),
        ]

        when:
        final StorageStats s = StorageStats.compute(SIZES, REFS, [:], roots, [:], [] as Set)

        then:
        // p: runs = run1 (run2 and run3 are delete-hidden) = 1. bytes: run1 (released, pinned) A
        // and run3 (deleted, pinned) C = 5 in 2 blocks. releasing: run1 is pinned, the deleted
        // runs are left out, so 0. q: run4 is released and unpinned: 0 bytes, releasing D = 8.
        s.pipelines == [
            [pipeline: 'p', runs: 1, bytes: 5, blocks: 2, releasing_bytes: 0],
            [pipeline: 'q', runs: 1, bytes: 0, blocks: 0, releasing_bytes: 8],
        ]
        // Store: stored A B C D = 15, pinned A C = 5, held nothing, releasing B D = 10.
        s.store.subMap(['stored_bytes', 'pinned_bytes', 'held_bytes', 'releasing_bytes']) ==
            [stored_bytes: 15, pinned_bytes: 5, held_bytes: 0, releasing_bytes: 10]
    }

    def 'P4 1.4: a project is the union of its members here, split as the store is, with no row when none is here'() {
        given:
        final List<StorageStats.Membership> members = [
            new StorageStats.Membership('X', 'run1'), new StorageStats.Membership('X', 'sel1'),
            new StorageStats.Membership('X', 'i4'), new StorageStats.Membership('X', 'run1'),
            new StorageStats.Membership('Y', 'run3'), new StorageStats.Membership('Y', 'elsewhere'),
            new StorageStats.Membership('Z', 'elsewhere'), new StorageStats.Membership('Z', 'i9'),
        ]

        when:
        final StorageStats s = StorageStats.compute(SIZES, REFS, NAMES, ROOTS, COLLECTIONS, [] as Set<String>, [] as Set<String>, members)
        final StorageStats swept = StorageStats.compute(SIZES, REFS, NAMES, ROOTS, COLLECTIONS, [] as Set<String>, ['E'] as Set<String>, members)

        then:
        // X: run1 {A B C}, sel1 {B}, i4 {D}: B once, run1 once. A B C are pinned by r6 = 7; D is held by run2 = 8. 3 roots.
        // Y: run3 {D E F}; 'elsewhere' is no root here. D held = 8, E releasing = 16, F pinned by r5 = 32. 1 root.
        // Z: neither member is here (i9 is in no output collection), so no row.
        s.projects == [
            [project_cid: 'X', reachable_bytes: 15, pinned_bytes: 7, held_bytes: 8, releasing_bytes: 0, roots: 3],
            [project_cid: 'Y', reachable_bytes: 56, pinned_bytes: 32, held_bytes: 8, releasing_bytes: 16, roots: 1],
        ]
        // A sweep took E: it leaves Y's figures.
        swept.projects[1] == [project_cid: 'Y', reachable_bytes: 40, pinned_bytes: 32, held_bytes: 8, releasing_bytes: 0, roots: 1]
        // Without memberships, no rows.
        fixture().projects == []
    }
}
