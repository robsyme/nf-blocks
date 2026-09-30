package robsyme.cas.core

import java.nio.file.Path

/**
 * A writable member with runs built from real blocks: each run's items hold
 * file leaves (raw) and optionally a directory leaf, logged like a real run.
 * Local by default; any store that is also a LoggedStore (an S3BlockStore over
 * MemoryS3Ops) through the second constructor.
 */
class RetentionFixture {

    final BlockStore store
    final Path root
    long clock = 1_790_000_000_000L

    RetentionFixture(Path root) {
        this.root = root
        this.store = new LocalBlockStore(root, 'lab', true)
    }

    RetentionFixture(BlockStore store) {
        if( !(store instanceof LoggedStore) )
            throw new IllegalArgumentException("a retention fixture needs a store with a Store Log, not ${store}")
        this.root = null
        this.store = store
    }

    Cid raw(String text) { store.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8'))) }

    Cid dir(Map<String, Cid> files) {
        final List<ManifestEntry> entries = files.collect { String n, Cid c -> ManifestEntry.regular(n, c, 1L) }
        return store.putDagCbor(new DirectoryManifest(entries).toCbor())
    }

    /**
     * A run with one output `out` whose items each hold the given leaves;
     * returns [completion, collection, items]. {@code options} (finishedAt,
     * status, pipeline, possiblyIncomplete, inputSet) are passed through to the
     * completion and manifest builders; a caller that omits them gets the
     * same run as before.
     */
    Map run(String name, List<Map<String, Cid>> items, Map options = [:]) {
        final Cid script = raw("script of ${name}")
        final Cid manifest = store.putDagCbor(RetentionFixture.manifest(name, script, options))
        final List<Cid> itemCids = items.collect { Map<String, Cid> leaves ->
            store.putDagCbor(OutputItem.of(leaves.collectEntries { String n, Cid c -> [(n): Leaf.of(n, c, 1L)] }).toCbor())
        }.sort { it.toString() }
        final Cid collection = store.putDagCbor(new OutputCollection('test', manifest, 'out', itemCids, itemCids.collect { [] as List<String> }).toCbor())
        final Cid completion = store.putDagCbor(RetentionFixture.completion(manifest, [collection], options))
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

    List<MemberLog> logs() { [new MemberLog(store.alias(), true, StoreLog.read(store))] }

    static Map manifest(String name, Cid script, Map options = [:]) {
        final Map args = RecordsTest.manifestArgs() + [runName: name, nfRunHash: "hash-${name}".toString(), script: script]
        if( options.pipeline )
            args.pipeline = options.pipeline
        return new RunManifest(args).toCbor()
    }

    static Map completion(Cid manifest, List<Cid> collections, Map options = [:]) {
        final String status = (options.status ?: 'succeeded') as String
        return new RunCompletion([assertedBy: 'test', run: manifest, collections: collections, inputSet: (Cid) options.inputSet,
            status: status, exitStatus: status == 'succeeded' ? 0 : 1,
            possiblyIncomplete: options.possiblyIncomplete ?: false,
            startedAt: '2026-09-28T10:00:00.000Z', finishedAt: (options.finishedAt ?: '2026-09-28T10:05:00.000Z') as String,
            anomalies: Anomalies.NONE, error: null]).toCbor()
    }
}
