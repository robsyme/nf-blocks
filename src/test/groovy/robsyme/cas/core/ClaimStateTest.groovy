package robsyme.cas.core

import java.nio.file.Path

import groovy.json.JsonSlurper
import spock.lang.Specification

/** Decision 5 of the milestone 2 plan: current state, pinned by the vectors claims.js runs too. */
class ClaimStateTest extends Specification {

    static List<Map> vectors() {
        (List<Map>) new JsonSlurper().parse(Path.of('web/test/fixtures/claim-vectors.json').toFile())
    }

    def '#v.name'() {
        given:
        final List<ClaimState.Row> rows = ((List<Map>) v.claims).collect { Map c ->
            new ClaimState.Row((String) c.cid, (String) c.verb, (String) c.attribute, c.value, (List<String>) c.supersedes)
        }

        when:
        final ClaimState state = ClaimState.of(rows)
        final Map expect = (Map) v.expect

        then:
        state.current*.cid == expect.current
        state.names == expect.names
        state.nameConflicted == expect.nameConflicted
        state.deletion == expect.deletion
        state.hidden == (expect.deletion == 'deleted')
        !expect.containsKey('retain') || state.retain == expect.retain
        !expect.containsKey('pins') || state.pinNotes == expect.pins
        !expect.containsKey('pinned') || state.pinned == expect.pinned
        !expect.containsKey('retain') || state.released == (expect.retain == 'released')

        where:
        v << vectors()
    }

    def 'a pin group is never flagged conflicted in claim_current'() {
        given:
        final ClaimState state = ClaimState.of([
            new ClaimState.Row('c1', 'add', 'pin', 'a', []),
            new ClaimState.Row('c2', 'add', 'pin', 'b', []),
            new ClaimState.Row('c3', 'set', 'retain', 'lineage', []),
            new ClaimState.Row('c4', 'set', 'retain', 'lineage', []),
        ])

        expect:
        state.currentRows()*.conflicted == [false, false, true, true]
        state.pinClaims == ['c1', 'c2']
        state.retainClaims == ['c3', 'c4']
        state.retain == ClaimState.CONFLICTED
    }

    def 'currentRows flags every row of a conflicted group'() {
        given:
        final ClaimState state = ClaimState.of([
            new ClaimState.Row('c1', 'set', 'name', 'a', []),
            new ClaimState.Row('c2', 'set', 'name', 'b', []),
            new ClaimState.Row('c3', 'delete', null, null, []),
        ])

        expect:
        state.currentRows() == [
            [cid: 'c1', attribute: 'name', value: 'a', conflicted: true],
            [cid: 'c2', attribute: 'name', value: 'b', conflicted: true],
            [cid: 'c3', attribute: null, value: null, conflicted: false],
        ]
        state.nameClaims == ['c1', 'c2']
        state.deletionClaims == ['c3']
    }
}
