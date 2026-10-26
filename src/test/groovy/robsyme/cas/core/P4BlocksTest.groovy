package robsyme.cas.core

import spock.lang.Specification

/** The Project, InputSet and Handoff blocks of the explorer v0.3 P4 plan, sections 1.1, 1.3 and 1.5. */
class P4BlocksTest extends Specification {

    static final Cid SEL = Fixtures.cidOf([kind: 'Selection', n: 1])
    static final Cid PROJ = Fixtures.cidOf([kind: 'Project', n: 1])
    static final Cid A = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
    static final Cid B = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

    private static Map roundTrip(Map block) {
        return (Map) DagCbor.decode(DagCbor.encode(block))
    }

    def 'a Project holds the 1.1 fields and decodes to itself'() {
        given:
        final Project p = new Project('rob@lab', 'Arabidopsis drought', '', '2026-10-25T10:00:00.000Z')

        expect:
        p.toCbor() == [kind: 'Project', schema: 1L, asserted_by: 'rob@lab', name: 'Arabidopsis drought',
                       description: '', created_at: '2026-10-25T10:00:00.000Z']
        Project.fromCbor(roundTrip(p.toCbor())) == p
    }

    def 'a Project refuses a blank or long name and a long description'() {
        when:
        new Project('rob@lab', name, description, '2026-10-25T10:00:00.000Z')

        then:
        thrown(IllegalArgumentException)

        where:
        name         | description
        ' '          | ''
        'x' * 257    | ''
        'ok'         | 'd' * 4097
        'ok'         | null
    }

    def 'an InputSet sorts its inputs, drops repeats, and its address does not depend on the order seen'() {
        given:
        final InputSet.Input reads = new InputSet.Input('task', 'S1_1.fastq.gz', B, 6L, 'S1', null)
        final InputSet.Input sheet = new InputSet.Input('samplesheet', 'samplesheet.csv', A, 0L, null, null)
        final InputSet.Input remote = new InputSet.Input('param:fasta', 'genome.fa', null, null, null, 'remote')
        final InputSet one = new InputSet('rob@lab', [reads, sheet, remote, reads])
        final InputSet two = new InputSet('rob@lab', [remote, sheet, reads])

        expect:
        one.inputs == [remote, sheet, reads]
        Fixtures.cidOf(one.toCbor()) == Fixtures.cidOf(two.toCbor())
        InputSet.fromCbor(roundTrip(one.toCbor())) == one
        one.toCbor().inputs[0] == [role: 'param:fasta', name: 'genome.fa', address: null, size: null, sample: null, reason: 'remote']
    }

    def 'an input has an address or a reason, never both'() {
        when:
        new InputSet('rob@lab', [new InputSet.Input('task', 'x', address, null, null, reason)])

        then:
        thrown(IllegalArgumentException)

        where:
        address | reason
        null    | null
        A       | 'remote'
    }

    def 'a Handoff sorts its files by name, keeps params as strings, and decodes to itself'() {
        given:
        final Handoff h = new Handoff('rob@lab', 'nf-core/differentialabundance', SEL, PROJ,
            [Leaf.of('samplesheet.csv', B, 6L), Leaf.of('contrasts.csv', A, 0L)],
            [input: "cas://${B}/samplesheet.csv".toString(), contrasts: "cas://${A}/contrasts.csv".toString()],
            '2026-10-25T10:00:00.000Z')
        final Map block = h.toCbor()

        expect:
        block.files*.name == ['contrasts.csv', 'samplesheet.csv']
        block.project == PROJ
        Handoff.fromCbor(roundTrip(block)) == h
        Handoff.fromCbor(roundTrip(new Handoff('rob@lab', 't', SEL, null, [], [:], '2026-10-25T10:00:00.000Z').toCbor())).project == null
    }

    def 'a Handoff refuses a file named twice and an unaddressed file'() {
        when:
        new Handoff('rob@lab', 't', SEL, null, files, [:], '2026-10-25T10:00:00.000Z')

        then:
        thrown(IllegalArgumentException)

        where:
        files << [[Leaf.of('a.csv', A, 0L), Leaf.of('a.csv', B, 6L)], [Leaf.without('a.csv', Leaf.UNADDRESSED)]]
    }

    def 'the store log knows project and handoff entries'() {
        expect:
        StoreLogKind.fromToken('project') == StoreLogKind.PROJECT
        StoreLogKind.fromToken('handoff') == StoreLogKind.HANDOFF
    }
}
