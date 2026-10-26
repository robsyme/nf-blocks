package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/**
 * P3 plan 1.4: the copy closure of the wigshop Selection in the real roblab
 * store, at every depth. Opt-in, since the store is local to one machine:
 *
 *   ./gradlew test --tests robsyme.cas.core.RoblabCopyClosureTest -Droblab.store=/tmp/cas-exploration/store
 *
 * The store's blocks/, log/ and nf/ directories and its store-id are copied
 * to a scratch directory, and everything runs against the copy, with a fresh
 * index: the original is never opened.
 */
@Requires({ System.getProperty('roblab.store') })
class RoblabCopyClosureTest extends Specification {

    static final Cid WIGSHOP = Cid.parse('bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce')

    @TempDir Path tmp

    private static void copyTree(Path from, Path to) {
        if( !Files.exists(from) )
            return
        Files.walk(from).withCloseable { stream ->
            stream.forEach { Path p ->
                final Path target = to.resolve(from.relativize(p).toString())
                if( Files.isDirectory(p) ) Files.createDirectories(target)
                else Files.copy(p, target)
            }
        }
    }

    private static void print(String label, CopyClosure.Result r) {
        println "roblab ${label} ${r.depth}: ${r.totals.blocks} blocks, ${r.totals.bytes} bytes, unsized ${r.totals.unsized}, " +
            "unaddressed ${r.unaddressed}, missing_at_source ${r.missingAtSource.size()}, skipped ${r.skippedClaims}"
        for( String group : CopyClosure.GROUPS ) {
            final List<CopyClosure.Entry> in = r.entries.findAll { it.group == group }
            println "roblab ${label} ${r.depth}   ${group}: ${in.size()} blocks, ${in.sum(0L) { CopyClosure.Entry e -> e.size ?: 0L }} bytes"
        }
        println "roblab ${label} ${r.depth}   runs: ${r.runs.collect { "${it.runName} (${it.pipeline}) lineage=${it.lineage}" }}"
        println "roblab ${label} ${r.depth}   claims: ${r.claims.size()} listed, ${r.claims.count { it.carried }} carried, " +
            "${r.claims.count { it.current }} current"
    }

    def 'the wigshop Selection closure at each depth'() {
        given:
        final Path source = Paths.get(System.getProperty('roblab.store'))
        final Path copy = tmp.resolve('store')
        for( String dir : ['blocks', 'log', 'nf'] )
            copyTree(source.resolve(dir), copy.resolve(dir))
        if( Files.exists(source.resolve('store-id')) )
            Files.copy(source.resolve('store-id'), copy.resolve('store-id'))
        final LocalBlockStore store = new LocalBlockStore(copy, 'roblab', false)
        final Index index = Index.open(tmp.resolve('cache/index.sqlite'))
        index.catchUp(store, StoreLog.of(store), 'roblab')
        final CopyClosure.Root root = new CopyClosure.Root(CopyClosure.SELECTION_ROOT, WIGSHOP)
        final Set<String> claims = [CopyClosure.NAMES, CopyClosure.PROJECTS, CopyClosure.ANNOTATIONS] as Set

        when:
        final long started = System.nanoTime()
        final CopyClosure.Result tasks = CopyClosure.compute(store, index, 'roblab', root, CopyClosure.ALL, claims, new LocalLineageRecords(copy))
        final long millis = (System.nanoTime() - started).intdiv(1_000_000L)
        final CopyClosure.Result joinKey = CopyClosure.compute(store, index, 'roblab', root, CopyClosure.ALL, claims, null)

        then:
        println "roblab closure computed in ${millis} ms"
        for( String depth : CopyClosure.DEPTHS )
            print('tasks', tasks.at(depth))
        print('join_key', joinKey.at(CopyClosure.LINEAGE))
        final Set<Cid> rootItems = tasks.at(CopyClosure.RESULTS).entries.findAll { it.group == 'items' }*.cid as Set
        for( Map.Entry<String, CopyClosure.Result> e : [tasks: tasks, join_key: joinKey] ) {
            final Set<Cid> lineageFiles = e.value.at(CopyClosure.LINEAGE).entries.findAll { it.group == 'lineage' }*.cid as Set
            final Set<Cid> runs = e.value.runs*.completion as Set
            final Set<Cid> added = new LinkedHashSet<Cid>()
            for( Cid file : lineageFiles )
                for( ProducerRow p : index.producersOf(file) )
                    if( runs.contains(p.completionCid) && !rootItems.contains(p.itemCid) )
                        added.add(p.itemCid)
            println "roblab ${e.key}: lineage adds ${added.size()} items (${lineageFiles.size()} top-level and nested files) to ${rootItems.size()} results items"
        }
        tasks.at(CopyClosure.RESULTS).entries.size() > 0

        cleanup:
        index?.close()
    }
}
