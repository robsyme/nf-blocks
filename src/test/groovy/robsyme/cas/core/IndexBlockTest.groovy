package robsyme.cas.core

import java.nio.file.Path
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Schema 4: ingest records every block an OutputItem reaches through its
 * Leaves and DirectoryManifests (`block`), and which record names it
 * (`block_ref`), so a snapshot can work out per-store reachability.
 */
class IndexBlockTest extends Specification {

    @TempDir
    Path tempDir

    LocalBlockStore store
    Path dbFile
    Index index

    Cid a, x, y
    Map nestedBlock, dirBlock
    Cid nested, dir
    Map itemBlock
    Cid item

    def setup() {
        store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        dbFile = tempDir.resolve('cache').resolve('index.sqlite')
        index = Index.open(dbFile)

        a = raw('a' * 10)
        x = raw('x' * 100)
        y = raw('y' * 5)
        nestedBlock = new DirectoryManifest([ManifestEntry.regular('y.txt', y, 5L)]).toCbor()
        nested = Fixtures.cidOf(nestedBlock)
        dirBlock = new DirectoryManifest([
            ManifestEntry.regular('x.bam', x, 100L),
            ManifestEntry.directory('nested', nested),
            ManifestEntry.symlink('link', 'x.bam'),
        ]).toCbor()
        dir = Fixtures.cidOf(dirBlock)
        itemBlock = Fixtures.outputItem([
            sample: 'A',
            file  : Fixtures.leaf('a.txt', a, 10L),
            reads : Fixtures.leaf('reads', dir, 999L),
            gone  : Fixtures.unaddressedLeaf('gone.txt'),
        ])
        item = Fixtures.cidOf(itemBlock)
    }

    def cleanup() {
        index?.close()
    }

    // ------------------------------------------------------------ fixtures

    private Cid raw(String text) {
        return store.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8')))
    }

    private Cid run(String name, List<Cid> items) {
        final Cid manifest = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: "hash-${name}".toString()))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'out',
            items.collect { Cid c -> [c, ["out/${c}".toString()]] }))
        return store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
    }

    private void storeAll() {
        store.putDagCbor(itemBlock)
        store.putDagCbor(dirBlock)
        store.putDagCbor(nestedBlock)
    }

    private List<List> rows(String sql) {
        final List<List> out = []
        DriverManager.getConnection("jdbc:sqlite:${dbFile}").withCloseable { c ->
            c.createStatement().executeQuery(sql).withCloseable { rs ->
                final int n = rs.metaData.columnCount
                while( rs.next() )
                    out << (1..n).collect { int i ->
                        final Object v = rs.getObject(i)
                        v instanceof Number ? ((Number) v).longValue() : v
                    }
            }
        }
        return out
    }

    private Set<List> blocks() { rows('SELECT cid, size, codec FROM block') as Set }

    private Set<List> refs() { rows('SELECT owner_cid, cid, name FROM block_ref') as Set }

    private static List ref(Cid owner, Cid cid, String name) { [owner.toString(), cid.toString(), name] }

    private static List blk(Cid cid, Long size, String codec) { [cid.toString(), size, codec] }

    private Set<List> allBlocks() {
        [blk(a, 10L, 'raw'), blk(x, 100L, 'raw'), blk(y, 5L, 'raw'),
         blk(dir, store.size(dir), 'dag-cbor'), blk(nested, store.size(nested), 'dag-cbor')] as Set
    }

    private Set<List> allRefs() {
        [ref(item, a, 'a.txt'), ref(item, dir, 'reads'),
         ref(dir, x, 'x.bam'), ref(dir, nested, 'nested'), ref(nested, y, 'y.txt')] as Set
    }

    // ------------------------------------------------------------ the tests

    def 'ingest records each block an item reaches, with its size and codec, and who names it'() {
        given:
        storeAll()
        final Cid completion = run('one', [item])

        when:
        index.ingestRun(store, completion, 'lab')

        then: 'three raw blocks sized from their Leaf and entries, two manifests sized as encoded'
        index.countRows('block') == 5
        blocks() == allBlocks()
        store.size(dir) == DagCbor.encode(dirBlock).length

        and: 'one row per addressed Leaf and per addressed entry; the symlink and the unaddressed Leaf add none'
        index.countRows('block_ref') == 5
        refs() == allRefs()
        index.countRows('missing') == 0
    }

    def 'ingesting a run again leaves the same rows'() {
        given:
        storeAll()
        final Cid completion = run('one', [item])

        when:
        index.ingestRun(store, completion, 'lab')
        index.ingestRun(store, completion, 'lab')

        then:
        index.countRows('block') == 5
        index.countRows('block_ref') == 5
        blocks() == allBlocks()
        refs() == allRefs()
    }

    def 'a second run sharing the item adds no references'() {
        given:
        storeAll()
        final Cid first = run('one', [item])
        final Cid second = run('two', [item])

        when:
        index.ingestRun(store, first, 'lab')
        index.ingestRun(store, second, 'lab')

        then:
        index.countRows('run') == 2
        index.countRows('block_ref') == 5
        index.countRows('block') == 5
    }

    def 'an absent manifest is missing, and catchUp fills the directory once it arrives'() {
        given: 'the item is stored, its directory manifest is not'
        store.putDagCbor(itemBlock)
        final Cid completion = run('one', [item])
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_000L)

        when:
        index.catchUp(store, StoreLog.of(store), 'lab')

        then: 'the file Leaf is indexed, and the directory is missing for the item'
        index.isRunIndexed(completion)
        rows('SELECT have_cid, needed_cid FROM missing') == [[item.toString(), dir.toString()]]
        refs() == [ref(item, a, 'a.txt'), ref(item, dir, 'reads')] as Set
        blocks() == [blk(a, 10L, 'raw')] as Set

        when: 'the manifest arrives, its nested directory does not'
        store.putDagCbor(dirBlock)
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        rows('SELECT have_cid, needed_cid FROM missing') == [[dir.toString(), nested.toString()]]
        refs() == allRefs() - [ref(nested, y, 'y.txt')]

        when: 'the nested manifest arrives'
        store.putDagCbor(nestedBlock)
        index.catchUp(store, StoreLog.of(store), 'lab')

        then:
        index.countRows('missing') == 0
        refs() == allRefs()
        blocks() == allBlocks()
    }

    def 'a manifest that names itself is read once'() {
        given: 'a store that answers for a manifest whose entry is its own address'
        final Cid z = raw('zzz')
        final Cid loop = Cid.of(Cid.DAG_CBOR, Hashing.sha256('a manifest that names itself'.getBytes('UTF-8')))
        final byte[] loopBytes = DagCbor.encode(new DirectoryManifest([
            ManifestEntry.directory('again', loop),
            ManifestEntry.regular('z.txt', z, 3L),
        ]).toCbor())
        int loopReads = 0
        final BlockStore looping = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override boolean has(Cid cid) { cid == loop || super.has(cid) }
            @Override long size(Cid cid) { cid == loop ? loopBytes.length : super.size(cid) }
            @Override InputStream open(Cid cid) {
                if( cid != loop )
                    return super.open(cid)
                loopReads++
                return new ByteArrayInputStream(loopBytes)
            }
        }
        final Cid looped = store.putDagCbor(Fixtures.outputItem([Fixtures.leaf('loop', loop, 1L)]))
        final Cid completion = run('loop', [looped])

        when:
        index.ingestRun(looping, completion, 'lab')

        then:
        loopReads == 1
        refs() == [ref(looped, loop, 'loop'), ref(loop, loop, 'again'), ref(loop, z, 'z.txt')] as Set
        blocks() == [blk(loop, (long) loopBytes.length, 'dag-cbor'), blk(z, 3L, 'raw')] as Set
    }

    def 'a Leaf with no size takes the size of its block when the block is present, else none'() {
        given:
        final Cid present = raw('seven!!')
        final Cid absent = Fixtures.contentCid('never stored')
        final Map unsized = [kind: 'Leaf', name: 'b.txt', address: present, size: null, provider: 'head-node', reason: null]
        final Map unknown = [kind: 'Leaf', name: 'c.txt', address: absent, size: null, provider: 'head-node', reason: null]
        final Cid sized = store.putDagCbor(Fixtures.outputItem([unsized, unknown]))
        final Cid completion = run('sizes', [sized])

        when:
        index.ingestRun(store, completion, 'lab')

        then:
        blocks() == [blk(present, 7L, 'raw'), blk(absent, null, 'raw')] as Set
        refs() == [ref(sized, present, 'b.txt'), ref(sized, absent, 'c.txt')] as Set
    }

    def 'a manifest that fails to read for now is missing, and a later catchUp fills the directory'() {
        given: 'a store whose reads of the directory manifest fail with an IO error while failing is set'
        storeAll()
        final Cid completion = run('one', [item])
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_000L)
        boolean failing = true
        int attempts = 0
        final BlockStore flaky = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override InputStream open(Cid cid) {
                if( cid == dir ) {
                    attempts++
                    if( failing )
                        throw new IOException('connection reset')
                }
                return super.open(cid)
            }
        }

        when:
        index.ingestRun(flaky, completion, 'lab')

        then: 'the manifest is missing for the item, the rest of the run is indexed'
        index.isRunIndexed(completion)
        rows('SELECT have_cid, needed_cid FROM missing') == [[item.toString(), dir.toString()]]
        refs() == [ref(item, a, 'a.txt'), ref(item, dir, 'reads')] as Set

        when: 'the read fails again on the retry'
        attempts = 0
        index.catchUp(flaky, StoreLog.of(flaky), 'lab')

        then: 'it was tried (once by the first block scan, once by the retry), and the row stays'
        attempts == 2
        rows('SELECT have_cid, needed_cid FROM missing') == [[item.toString(), dir.toString()]]

        when: 'the read succeeds'
        failing = false
        index.catchUp(flaky, StoreLog.of(flaky), 'lab')

        then:
        index.countRows('missing') == 0
        refs() == allRefs()
        blocks() == allBlocks()
    }

    def 'a block first named without a size takes the size a later Leaf gives'() {
        given: 'an absent block named by a Leaf with no size, then by a sized Leaf in another item'
        final Cid absent = Fixtures.contentCid('not here yet')
        final Map unsized = [kind: 'Leaf', name: 'u.txt', address: absent, size: null, provider: 'head-node', reason: null]
        final Cid first = store.putDagCbor(Fixtures.outputItem([unsized]))
        final Cid second = store.putDagCbor(Fixtures.outputItem([Fixtures.leaf('s.txt', absent, 42L)]))

        when:
        index.ingestRun(store, run('first', [first]), 'lab')

        then:
        blocks() == [blk(absent, null, 'raw')] as Set

        when:
        index.ingestRun(store, run('second', [second]), 'lab')

        then:
        blocks() == [blk(absent, 42L, 'raw')] as Set
        refs() == [ref(first, absent, 'u.txt'), ref(second, absent, 's.txt')] as Set
    }

    def 'a manifest that names no addressed entry is read once, however many items name it'() {
        given: 'a directory holding only a symlink, named by the items of two runs'
        final Map linksBlock = new DirectoryManifest([ManifestEntry.symlink('link', 'elsewhere')]).toCbor()
        final Cid links = store.putDagCbor(linksBlock)
        int reads = 0
        final BlockStore counting = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override InputStream open(Cid cid) {
                if( cid == links ) reads++
                return super.open(cid)
            }
        }
        final Cid first = store.putDagCbor(Fixtures.outputItem([Fixtures.leaf('links', links, 1L)]))
        final Cid second = store.putDagCbor(Fixtures.outputItem([sample: 'B', dir: Fixtures.leaf('links', links, 1L)]))

        when:
        index.ingestRun(counting, run('one', [first]), 'lab')
        index.ingestRun(counting, run('two', [second]), 'lab')

        then:
        reads == 1
        blocks() == [blk(links, store.size(links), 'dag-cbor')] as Set
        refs() == [ref(first, links, 'links'), ref(second, links, 'links')] as Set
        index.countRows('missing') == 0
    }

    def 'a manifest over the block limit is never read, stays missing for each owner, and is looked at again'() {
        given: 'a store that says the directory manifest is larger than any metadata block, until the limit is lifted'
        storeAll()
        final long huge = 64L * 1024 * 1024 + 1
        boolean over = true
        int reads = 0
        int sizes = 0
        final BlockStore oversize = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override long size(Cid cid) {
                if( cid == dir && over ) { sizes++; return huge }
                return super.size(cid)
            }
            @Override InputStream open(Cid cid) {
                if( cid == dir ) reads++
                return super.open(cid)
            }
        }
        final Cid other = store.putDagCbor(Fixtures.outputItem([sample: 'B', reads: Fixtures.leaf('reads', dir, 999L)]))
        final Cid completion = run('one', [item])
        StoreLog.append(store, StoreLogKind.RUN, completion, 1_000_000L)

        when:
        index.catchUp(oversize, StoreLog.of(oversize), 'lab')

        then: 'no block row says it was read, and the item waits for it'
        reads == 0
        blocks() == [blk(a, 10L, 'raw')] as Set
        rows('SELECT have_cid, needed_cid FROM missing') == [[item.toString(), dir.toString()]]

        when: 'a catch-up with nothing new, then a second item naming the same directory'
        sizes = 0
        index.catchUp(oversize, StoreLog.of(oversize), 'lab')
        final int sizesOnRetry = sizes
        index.ingestRun(oversize, run('two', [other]), 'lab')

        then: 'the retry looks at its size again, never reads it, and the second owner waits too'
        sizesOnRetry > 0
        reads == 0
        rows('SELECT have_cid, needed_cid FROM missing') as Set == [[item.toString(), dir.toString()], [other.toString(), dir.toString()]] as Set

        when: 'the limit no longer applies: the next catch-up reads it and fills the directory'
        over = false
        store.putDagCbor(nestedBlock)
        index.catchUp(oversize, StoreLog.of(oversize), 'lab')

        then:
        reads == 1
        index.countRows('missing') == 0
        blocks() == allBlocks()
    }

    def 'a manifest that reads as nothing is read again by the next owner'() {
        given: 'the first read of the directory manifest loses a race with the store; later reads succeed'
        storeAll()
        int reads = 0
        final BlockStore racing = new LocalBlockStore(tempDir.resolve('store'), 'lab', true) {
            @Override InputStream open(Cid cid) {
                if( cid == dir && reads++ == 0 )
                    throw new NoSuchBlockException(cid, 'lab')
                return super.open(cid)
            }
        }
        final Cid other = store.putDagCbor(Fixtures.outputItem([sample: 'B', reads: Fixtures.leaf('reads', dir, 999L)]))

        when:
        index.ingestRun(racing, run('one', [item]), 'lab')

        then: 'no block row for it'
        reads == 1
        !blocks().any { List b -> b[0] == dir.toString() }

        when:
        index.ingestRun(racing, run('two', [other]), 'lab')

        then: 'the second owner reads it, and its files are recorded'
        reads == 2
        blocks() == allBlocks()
        refs() == allRefs() + [ref(other, dir, 'reads')] as Set
    }

    def 'forgetting the last item that reaches a directory drops the missing rows under it'() {
        given: 'run one and run two share the item; its directory arrived, the nested one did not'
        store.putDagCbor(itemBlock)
        store.putDagCbor(dirBlock)
        final Cid one = run('one', [item])
        final Cid two = run('two', [item])
        index.ingestRun(store, one, 'lab')
        index.ingestRun(store, two, 'lab')
        assert rows('SELECT have_cid, needed_cid FROM missing') == [[dir.toString(), nested.toString()]]
        final Closure<List<Cid>> collectionsOf = { Cid completion ->
            rows("SELECT collection_cid FROM collection WHERE completion_cid = '${completion}'").collect { List r -> Cid.parse((String) r[0]) }
        }

        when: 'run one is forgotten: run two still reaches the directory'
        index.forget([one] + collectionsOf.call(one))

        then:
        rows('SELECT have_cid, needed_cid FROM missing') == [[dir.toString(), nested.toString()]]

        when: 'run two and the item are forgotten too'
        index.forget([two, item] + collectionsOf.call(two))

        then: 'nothing is left to retry, though the block facts stay'
        index.countRows('missing') == 0
        index.countRows('block_ref') > 0
    }
}
