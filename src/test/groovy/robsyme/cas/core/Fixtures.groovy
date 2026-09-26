package robsyme.cas.core

/**
 * DESIGN.md §6 blocks, encoded straight from the specified maps so the index
 * tests depend on the contract rather than on a builder. Field names, sort
 * orders and the `kind`/`schema` pair are §6 verbatim.
 */
class Fixtures {

    static final String ASSERTED_BY = 'ada'

    /** A raw content address, without storing any bytes for it. */
    static Cid contentCid(String content) {
        return Cid.of(Cid.RAW, Hashing.sha256(content.getBytes('UTF-8')))
    }

    /** The address a §6 map would have, whether or not it is stored. */
    static Cid cidOf(Map block) {
        return DagCbor.cidOf(DagCbor.encode(block))
    }

    static Map leaf(String name, Cid address, long size) {
        return [kind: 'Leaf', name: name, address: address, size: size,
                provider: 'head-node', reason: null]
    }

    static Map unaddressedLeaf(String name) {
        return [kind: 'Leaf', name: name, address: null, size: null,
                provider: null, reason: 'unaddressed']
    }

    static Map runManifest(Map overrides = [:]) {
        final Map block = [kind: 'RunManifest', schema: 1, asserted_by: ASSERTED_BY,
                           pipeline: 'p', repository: null, revision: 'main', commit_id: 'c0ffee',
                           run_name: 'grave_curie', nf_run_hash: 'aa11bb22', session_id: 'sess-1',
                           resumed: false, nextflow_version: '26.04.6',
                           params: [:], config: [:], script: null,
                           started_at: '2026-09-03T10:00:00.000Z']
        block.putAll(overrides)
        return block
    }

    static Map outputItem(Object value) {
        return [kind: 'OutputItem', schema: 1, value: value]
    }

    /**
     * `items` sorted ascending by cid string with `paths` carried along, as §6
     * requires. Each element of `itemsWithPaths` is `[cid, [publish path, ...]]`.
     */
    static Map outputCollection(Cid run, String name, List<List> itemsWithPaths) {
        final List<List> sorted = new ArrayList<List>(itemsWithPaths)
        sorted.sort { List a, List b -> String.valueOf(a[0]) <=> String.valueOf(b[0]) }
        return [kind: 'OutputCollection', schema: 1, asserted_by: ASSERTED_BY,
                run: run, name: name,
                items: sorted.collect { it[0] },
                paths: sorted.collect { it[1] }]
    }

    /** A Claim as §6 specifies it; supersedes sorted by cid string. */
    static Map claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes,
                     String timestamp = '2026-09-25T10:00:00.000Z') {
        return [kind: 'Claim', schema: 1, asserted_by: ASSERTED_BY, subject: subject, verb: verb,
                attribute: attribute, value: value,
                supersedes: new ArrayList<Cid>(supersedes).sort { Cid c -> c.toString() },
                timestamp: timestamp]
    }

    /** A Selection as §6 specifies it; the caller passes members already sorted by address. */
    static Map selection(List<Map> members, List<Cid> derivedFrom = []) {
        return [kind: 'Selection', schema: 1, asserted_by: ASSERTED_BY, members: members,
                derived_from: new ArrayList<Cid>(derivedFrom).sort { Cid c -> c.toString() }.collect { Cid c -> c.bytes() }]
    }

    static Map runCompletion(Cid run, List<Cid> collections, Map overrides = [:]) {
        final Map block = [kind: 'RunCompletion', schema: 1, asserted_by: ASSERTED_BY,
                           run: run, collections: collections, input_set: null,
                           status: 'succeeded', exit_status: 0, possibly_incomplete: false,
                           started_at: '2026-09-03T10:00:00.000Z',
                           finished_at: '2026-09-03T10:05:00.000Z',
                           anomalies: [unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0],
                           error: null]
        block.putAll(overrides)
        return block
    }
}
