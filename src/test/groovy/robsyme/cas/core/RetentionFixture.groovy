package robsyme.cas.core

import java.nio.file.Path

/**
 * A local writable member with runs built from real blocks: each run's items
 * hold file leaves (raw) and optionally a directory leaf, logged like a real run.
 */
class RetentionFixture {

    final LocalBlockStore store
    final Path root
    long clock = 1_790_000_000_000L

    RetentionFixture(Path root) {
        this.root = root
        this.store = new LocalBlockStore(root, 'lab', true)
    }

    Cid raw(String text) { store.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8'))) }

    Cid dir(Map<String, Cid> files) {
        final List<ManifestEntry> entries = files.collect { String n, Cid c -> ManifestEntry.regular(n, c, 1L) }
        return store.putDagCbor(new DirectoryManifest(entries).toCbor())
    }

    /** A run with one output `out` whose items each hold the given leaves; returns [completion, collection, items]. */
    Map run(String name, List<Map<String, Cid>> items) {
        final Cid script = raw("script of ${name}")
        final Cid manifest = store.putDagCbor(RetentionFixture.manifest(name, script))
        final List<Cid> itemCids = items.collect { Map<String, Cid> leaves ->
            store.putDagCbor(OutputItem.of(leaves.collectEntries { String n, Cid c -> [(n): Leaf.of(n, c, 1L)] }).toCbor())
        }.sort { it.toString() }
        final Cid collection = store.putDagCbor(new OutputCollection('test', manifest, 'out', itemCids, itemCids.collect { [] as List<String> }).toCbor())
        final Cid completion = store.putDagCbor(RetentionFixture.completion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, ++clock)
        return [completion: completion, collection: collection, items: itemCids, manifest: manifest, script: script]
    }

    Cid claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = []) {
        final Cid c = store.putDagCbor(new Claim('test', subject, verb, attribute, value, supersedes, Index.isoMillis(++clock)).toCbor())
        StoreLog.append(store, StoreLogKind.CLAIM, c, clock)
        return c
    }

    Cid selection(Cid item, Cid via) {
        final Cid s = store.putDagCbor(new Selection('test', [Selection.item(item, [via])], []).toCbor())
        StoreLog.append(store, StoreLogKind.SELECTION, s, ++clock)
        return s
    }

    /** A Selection block like {@link #selection}, but not logged as its own Store Log root: for nesting under another Selection. */
    Cid selectionBlock(Cid item, Cid via) {
        return store.putDagCbor(new Selection('test', [Selection.item(item, [via])], []).toCbor())
    }

    /** A Selection whose one member nests another Selection (not an item), logged as a Store Log root. */
    Cid nestedSelection(Cid inner) {
        final Cid s = store.putDagCbor(new Selection('test', [Selection.selection(inner)], []).toCbor())
        StoreLog.append(store, StoreLogKind.SELECTION, s, ++clock)
        return s
    }

    List<MemberLog> logs() { [new MemberLog('lab', true, StoreLog.read(store))] }

    static Map manifest(String name, Cid script) {
        return new RunManifest(RecordsTest.manifestArgs() + [runName: name, nfRunHash: "hash-${name}".toString(), script: script]).toCbor()
    }

    static Map completion(Cid manifest, List<Cid> collections) {
        return new RunCompletion([assertedBy: 'test', run: manifest, collections: collections, inputSet: null,
            status: 'succeeded', exitStatus: 0, possiblyIncomplete: false,
            startedAt: '2026-09-28T10:00:00.000Z', finishedAt: '2026-09-28T10:05:00.000Z',
            anomalies: Anomalies.NONE, error: null]).toCbor()
    }
}
