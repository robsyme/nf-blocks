package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Block explorer spec section 9: deterministic construction, validation, idempotence. */
class PutTest extends Specification {

    static final long NOW = 1_758_000_000_000L                // 2025-09-16T05:20:00.000Z
    static final String TS = '2025-09-16T05:20:00.000Z'

    @TempDir
    Path tempDir

    LocalBlockStore store
    Index index
    Put put
    long now = NOW
    Cid manifest, itemA, itemB, collA, collB

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        index = Index.open(tempDir.resolve('cache/index.sqlite'))
        put = newPut(store)
        manifest = store.putDagCbor(Fixtures.runManifest())
        itemA = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        itemB = store.putDagCbor(Fixtures.outputItem([[sample: 'B'], Fixtures.leaf('B.bam', Fixtures.contentCid('B'), 1L)]))
        collA = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[itemA, ['aligned/A.bam']], [itemB, ['aligned/B.bam']]]))
        final Cid other = store.putDagCbor(Fixtures.runManifest(run_name: 'other', nf_run_hash: 'other'))
        collB = store.putDagCbor(Fixtures.outputCollection(other, 'aligned', [[itemA, ['x/A.bam']]]))
    }

    def cleanup() {
        index?.close()
    }

    LocalBlockStore shared

    private Put newPut(BlockStore s) {
        return new Put(s, s, index, 'ada', { now }, { index.catchUp(store, StoreLog.of(store), 'lab') })
    }

    /** lab writable, shared read-only: the composition the page sees with a Bundle mounted. */
    private Put twoMembers() {
        shared = new LocalBlockStore(tempDir.resolve('shared'), 'shared', false)
        return new Put(new CompositeStore([store, shared]), store, index, 'ada', { now }, catchUpBoth())
    }

    /** Writes into shared's directory, as the host where it was writable once did. */
    private Put seedShared() {
        final LocalBlockStore seed = new LocalBlockStore(tempDir.resolve('shared'), 'shared', true)
        return new Put(new CompositeStore([seed, store]), seed, index, 'ada', { now }, catchUpBoth())
    }

    private Closure catchUpBoth() {
        return {
            index.catchUp(store, StoreLog.of(store), 'lab')
            final LocalBlockStore s = new LocalBlockStore(tempDir.resolve('shared'), 'shared', false)
            index.catchUp(s, StoreLog.of(s), 'shared')
        }
    }

    private static PutResult sendTo(Put p, String json, boolean dry = false) { p.put(json.getBytes('UTF-8'), dry) }

    private static String link(Cid c) { '{"/":"' + c + '"}' }

    private static String links(List<Cid> cs) { '[' + cs.collect { link(it) }.join(',') + ']' }

    private static String selection(String members) { '{"kind":"Selection","members":[' + members + '],"derived_from":[]}' }

    private static String item(Cid address, List<Cid> via) { '{"item":{"address":' + link(address) + ',"via":' + links(via) + '}}' }

    private static String claim(Cid subject, String verb, String attribute, String valueJson, List<Cid> supersedes, String ts = TS) {
        return '{"kind":"Claim","subject":' + link(subject) + ',"verb":"' + verb + '","attribute":' +
            (attribute == null ? 'null' : '"' + attribute + '"') + ',"value":' + valueJson +
            ',"supersedes":' + links(supersedes) + ',"timestamp":"' + ts + '"}'
    }

    private PutResult send(String json, boolean dry = false) { put.put(json.getBytes('UTF-8'), dry) }

    private PutError refused(String json) {
        try {
            send(json)
            return null
        }
        catch( PutError e ) {
            return e
        }
    }

    private static PutError refusedBy(Put p, String json) {
        try {
            p.put(json.getBytes('UTF-8'), false)
        }
        catch( PutError e ) {
            return e
        }
        throw new AssertionError('expected a PutError')
    }

    def 'a Selection is normalised, written, logged and indexed; its address is its canonical block address'() {
        when:
        final PutResult r = send(selection(item(itemB, [collA]) + ',' + item(itemA, [collB, collA])))
        final Map expected = new Selection('ada', [Selection.item(itemA, [collA, collB]), Selection.item(itemB, [collA])], []).toCbor()

        then:
        r.written
        r.address == DagCbor.cidOf(DagCbor.encode(expected))
        store.has(r.address)
        StoreLog.read(store)*.name == [r.entry]
        r.entry == StoreLog.entryName(StoreLogKind.SELECTION, r.address, NOW)
        index.isSelectionIndexed(r.address)
        ((Map) DagJson.decode(r.body())).block == expected
        ((Map) DagJson.decode(r.body())).address == r.address
    }

    def 'an occurrence URI and an item map for one item are one member, in either order (Review Focus 2)'() {
        given:
        final String occurrence = '"cas://' + collA + '/' + itemA + '"'

        expect:
        send(selection(occurrence + ',' + item(itemA, [collB])), true).address ==
            send(selection(item(itemA, [collB]) + ',' + occurrence), true).address
        send(selection(occurrence + ',' + item(itemA, [collB])), true).address ==
            DagCbor.cidOf(DagCbor.encode(new Selection('ada', [Selection.item(itemA, [collA, collB])], []).toCbor()))
    }

    def 'a retried identical request writes no second block and no second entry'() {
        given:
        final String json = selection(item(itemA, [collA]))
        final PutResult first = send(json)
        now += 60_000

        when:
        final PutResult again = send(json)

        then:
        !again.written
        again.address == first.address
        again.entry == first.entry
        StoreLog.read(store).size() == 1
    }

    def 'a dry run writes nothing and reports existence and current names'() {
        given:
        final String json = selection(item(itemA, [collA]))

        when:
        final PutResult dry = send(json, true)

        then:
        dry.dryRun
        !dry.exists
        !dry.here
        dry.names == []
        dry.nameClaims == []
        !store.has(dry.address)
        StoreLog.read(store).isEmpty()

        when:
        final Cid written = send(json).address
        final Cid named = send(claim(written, 'set', 'name', '"first"', [])).address
        final PutResult again = send(json, true)

        then:
        again.exists
        again.here
        again.names == ['first']
        again.nameClaims == [named]
        ((Map) DagJson.decode(again.body())) == [address: written, exists: true, here: true, names: ['first'], name_claims: [named]]
    }

    def 'a dry run says whether the writable member holds the block, and which Claims name it (ticket 09)'() {
        given:
        final Put seed = seedShared()
        final String json = selection(item(itemA, [collA]))
        final Cid s = sendTo(seed, json).address
        final Cid named = sendTo(seed, claim(s, 'set', 'name', '"from-shared"', [])).address
        final Put both = twoMembers()

        when:
        final PutResult dry = sendTo(both, json, true)

        then:
        dry.exists
        !dry.here
        dry.names == ['from-shared']
        dry.nameClaims == [named]
        ((Map) DagJson.decode(dry.body())) == [address: s, exists: true, here: false, names: ['from-shared'], name_claims: [named]]

        when: 'the real write copies the block into the writable member'
        final PutResult copied = sendTo(both, json)

        then:
        copied.written
        store.has(s)
        StoreLog.read(store).any { it.cid == s }

        and: 'held in both members now, so here'
        sendTo(both, json, true).here
    }

    def 'staleness counts the writable member\'s Claims; presence counts every member (ticket 09)'() {
        given: 'a Selection named in lab, renamed by a Claim only shared holds'
        final Put both = twoMembers()
        final Cid s = sendTo(both, selection(item(itemA, [collA]))).address
        final Cid labName = sendTo(both, claim(s, 'set', 'name', '"lab-name"', [])).address
        final Cid sharedName = sendTo(seedShared(), claim(s, 'set', 'name', '"shared-name"', [labName])).address

        when: 'renaming from what lab shows'
        final PutResult renamed = sendTo(both, claim(s, 'set', 'name', '"lab-renamed"', [labName], '2025-09-16T05:20:00.001Z'))

        then: 'written, and the composition reports the disagreement as a conflict'
        renamed.written
        index.claimState(s).names.toSorted() == ['lab-renamed', 'shared-name']
        index.claimState(s).nameClaims.size() == 2

        when: 'a superseder lab holds still refuses'
        final PutError stale = refusedBy(both, claim(s, 'set', 'name', '"again"', [labName], '2025-09-16T05:20:00.002Z'))

        then:
        stale.code == 'stale_supersedes'
        stale.message.contains(renamed.address.toString())

        when: 'one Claim superseding both current names, one of them held only in shared'
        sendTo(both, claim(s, 'set', 'name', '"settled"', [renamed.address, sharedName], '2025-09-16T05:20:00.003Z'))

        then:
        index.claimState(s).names == ['settled']
    }

    def 'rename, delete and undo, with supersedes checked against what the index knows'() {
        given:
        final Cid s = send(selection(item(itemA, [collA]))).address
        final Cid named = send(claim(s, 'set', 'name', '"first"', [])).address

        when:
        final Cid renamed = send(claim(s, 'set', 'name', '"second"', [named])).address
        final Cid deleted = send(claim(s, 'delete', null, 'null', [])).address

        then:
        index.claimState(s).names == ['second']
        index.claimState(s).hidden

        when:
        send(claim(s, 'del', null, 'null', [deleted]))

        then:
        !index.claimState(s).hidden

        when: 'a second rename from a window that still shows the first name'
        final PutError stale = refused(claim(s, 'set', 'name', '"third"', [named], '2025-09-16T05:20:00.001Z'))

        then:
        stale.code == 'stale_supersedes'
        stale.at == '/supersedes/0'
        stale.message.contains(renamed.toString())
    }

    def 'a retried rename after the state moved on still dedupes, even eleven minutes later (Review Focus 1)'() {
        given:
        final Cid s = send(selection(item(itemA, [collA]))).address
        final Cid named = send(claim(s, 'set', 'name', '"first"', [])).address
        final String rename = claim(s, 'set', 'name', '"second"', [named])
        final PutResult first = send(rename)
        send(claim(s, 'set', 'name', '"third"', [first.address], '2025-09-16T05:20:00.002Z'))

        when:
        final PutResult replayed = send(rename)
        now += 11 * 60_000
        final PutResult late = send(rename)

        then:
        !replayed.written && replayed.address == first.address
        !late.written && late.address == first.address
    }

    def 'refused before anything is written, each with its code and where'() {
        given:
        final Cid absentItem = Fixtures.cidOf(Fixtures.outputItem([[sample: 'Z']]))
        final Cid absentSelection = Fixtures.cidOf([kind: 'Selection', n: 99])
        final Cid otherSubjectClaim = send(claim(itemB, 'set', 'name', '"b"', [])).address
        final int blocksBefore = store.listBlocks().withCloseable { it.count() } as int
        final List<List<String>> cases = [
            [selection(''), 'empty', '/members'],
            [selection(item(absentItem, [])), 'not_found', '/members/0/item/address'],
            [selection(item(manifest, [])), 'wrong_kind', '/members/0/item/address'],
            [selection(item(itemB, [collB])), 'not_in_via', '/members/0/item/via/0'],
            [selection(item(itemA, [itemB])), 'wrong_kind', '/members/0/item/via/0'],
            [selection('{"selection":' + link(absentSelection) + '}'), 'not_found', '/members/0/selection'],       // Review Focus 5
            [selection('"cas://' + collA + '/' + itemA + '/A.bam"'), 'invalid', '/members/0'],
            ['{"kind":"Selection","members":[],"extra":1}', 'invalid', '/extra'],
            ['{"kind":"RunCompletion"}', 'wrong_kind', '/kind'],
            ['{"members":[]}', 'invalid', '/kind'],
            ['[1]', 'invalid', ''],
            ['{"kind":"Selection","members":[{"/":5}]}', 'invalid', '/members/0'],
            ['{"kind":', 'invalid', '/kind'],
            [claim(itemA, 'add', 'tag', '"x"', []), 'invalid', '/verb'],
            [claim(itemA, 'rename', 'name', '"x"', []), 'invalid', '/verb'],
            [claim(itemA, 'set', null, '"x"', []), 'invalid', '/attribute'],
            [claim(itemA, 'set', 'name', '""', []), 'invalid', '/value'],
            [claim(itemA, 'set', 'name', '"' + 'x' * 257 + '"', []), 'invalid', '/value'],
            [claim(itemA, 'delete', 'name', 'null', []), 'invalid', '/attribute'],
            [claim(itemA, 'del', null, 'null', []), 'invalid', '/supersedes'],
            [claim(itemA, 'delete', null, 'null', [], '2025-09-16T05:20:00Z'), 'invalid', '/timestamp'],
            [claim(itemA, 'delete', null, 'null', [], '2025-09-16T05:30:00.001Z'), 'clock_skew', '/timestamp'],
            [claim(itemA, 'del', null, 'null', [absentSelection]), 'stale_supersedes', '/supersedes/0'],
            [claim(itemA, 'del', null, 'null', [itemB]), 'wrong_kind', '/supersedes/0'],
            [claim(itemA, 'del', null, 'null', [otherSubjectClaim]), 'wrong_kind', '/supersedes/0'],
            // final review finding 1: a lone surrogate or an out-of-range
            // integer in the request text is refused by DagJson at decode
            // time, with a field, rather than reaching DagCbor.encode.
            [claim(itemA, 'set', 'x', '"\\ud800"', []), 'invalid', '/value'],
            [claim(itemA, 'set', 'x', '184467440737095516160000', []), 'invalid', '/value'],
            ['{"kind":"Claim","subject":' + link(itemA) + ',"verb":"set","attribute":"\\ud800","value":"x","supersedes":[],"timestamp":"' + TS + '"}',
             'invalid', '/attribute'],
        ]

        when:
        final List<String> wrong = cases.findResults { List<String> c ->
            final PutError e = refused(c[0])
            (e?.code == c[1] && e?.at == c[2]) ? null : "${c[0].take(80)}: got ${e?.code} at ${e?.at}, want ${c[1]} at ${c[2]}".toString()
        }

        then:
        wrong == []
        (store.listBlocks().withCloseable { it.count() } as int) == blocksBefore
    }

    def 'a request built directly as a Map, bypassing DagJson, with content dag-cbor cannot encode is invalid (final review finding 1 backstop)'() {
        given:
        final String loneSurrogate = new String(Character.toChars(0xD800))

        when:
        put.put([kind: 'Claim', subject: itemA, verb: 'set', attribute: 'x', value: loneSurrogate, supersedes: [], timestamp: TS], false)

        then:
        final PutError e = thrown()
        e.code == 'invalid'

        when:
        put.put([kind: 'Claim', subject: itemA, verb: 'set', attribute: 'x', value: new BigInteger('184467440737095516160000'),
                 supersedes: [], timestamp: TS], false)

        then:
        final PutError e2 = thrown()
        e2.code == 'invalid'
    }

    def 'an encoded Selection over 1 MiB is too_large, before any member is looked up'() {
        given:
        final Random random = new Random(7)
        final List<Map> members = (1..20_000).collect {
            final byte[] digest = new byte[32]
            random.nextBytes(digest)
            [item: [address: Cid.of(Cid.DAG_CBOR, digest), via: []]] as Map
        }

        when:
        put.put([kind: 'Selection', members: members, derived_from: []], false)

        then:
        final PutError e = thrown()
        e.code == 'too_large'
        e.at == '/members'
    }

    def 'a writable member that is read-only is not_writable, a 409'() {
        given:
        final Put readOnly = newPut(new LocalBlockStore(tempDir.resolve('store'), 'lab', false))

        when:
        readOnly.put(selection(item(itemA, [collA])).getBytes('UTF-8'), false)

        then:
        final PutError e = thrown()
        e.code == 'not_writable'
        e.status == 409
    }

    def 'derived_from takes links or bytes and stores bytes'() {
        given:
        final Cid prior = send(selection(item(itemB, [collA]))).address
        final String asLink = '{"kind":"Selection","members":[' + item(itemA, [collA]) + '],"derived_from":[' + link(prior) + ']}'
        final String asBytes = '{"kind":"Selection","members":[' + item(itemA, [collA]) + '],"derived_from":[{"/":{"bytes":"' +
            Base64.encoder.withoutPadding().encodeToString(prior.bytes()) + '"}}]}'

        expect:
        send(asLink, true).address == send(asBytes, true).address
    }
}
