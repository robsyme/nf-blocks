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

        where:
        v << vectors()
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
