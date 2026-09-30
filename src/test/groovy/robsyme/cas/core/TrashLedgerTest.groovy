package robsyme.cas.core

import groovy.json.JsonSlurper
import spock.lang.Specification

class TrashLedgerTest extends Specification {

    def 'a ledger round-trips, sorts its blocks and names itself by deadline'() {
        given:
        final Cid a = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')
        final Cid b = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        final TrashLedger l = new TrashLedger('20260930T120000Z-abcdefgh', 1_791_000_000_000L, '2026-09-30T12:00:00.000Z', [(b): 0L, (a): 6L])

        when:
        final TrashLedger back = TrashLedger.parse(l.name, l.toJson())
        final Map json = (Map) new JsonSlurper().parse(l.toJson())

        then:
        l.name == '1791000000000-20260930T120000Z-abcdefgh'
        back.blocks == l.blocks
        back.deadlineMillis == 1_791_000_000_000L
        json.blocks == [[cid: a.toString(), size: 6], [cid: b.toString(), size: 0]]
        json.deadline == Index.isoMillis(1_791_000_000_000L)
        l.without([a]).blocks.keySet() == [b] as Set
    }

    def 'a ledger whose name and body disagree, or that does not parse, is refused'() {
        when:
        TrashLedger.parse(name, body.bytes)

        then:
        thrown(IllegalArgumentException)

        where:
        name                     | body
        '1791000000000-other'    | '{"sweep":"s","trashed_at":"x","deadline":"2026-10-01T00:00:00.000Z","blocks":[]}'
        '1791000000000-s'        | 'not json'
        'nodigits-s'             | '{"sweep":"s","trashed_at":"x","deadline":"x","blocks":[]}'
    }

    def 'a ledger whose blocks are not cids and sizes is refused'() {
        when:
        TrashLedger.parse('1791000000000-s', body.bytes)

        then:
        thrown(IllegalArgumentException)

        where:
        body << [
            '{"sweep":"s","trashed_at":"x","deadline":"x","blocks":[{"cid":"not a cid","size":1}]}',
            '{"sweep":"s","trashed_at":"x","deadline":"x","blocks":[{"cid":"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"}]}',
            '{"sweep":"s","trashed_at":"x","deadline":"x","blocks":{"a":1}}',
            '[1,2]',
        ]
    }
}
