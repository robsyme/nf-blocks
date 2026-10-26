package robsyme.cas.cli

import java.nio.file.Path
import java.sql.DriverManager

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Fix round 3, item 3: a seeded_lean cache recovers through
 * `nextflow plugin nf-blocks:reindex --seed false`, driven through the CLI
 * verbs. Then a second persistent cache, seeded lean earlier, writes after
 * the recovery: its lean build must not replace the snapshot with figures
 * (the integrator's ReviewSeedLeanTest).
 */
class CasCommandsSeededLeanTest extends Specification {
    @TempDir Path tempDir
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    private Map config(String cache = 'cache') {
        [lineage: [store: [location: 'cas://lab']],
         cas: [stores: [lab: [location: tempDir.resolve('store').toString()]],
               index: [path: tempDir.resolve("${cache}/index.sqlite").toString()]]]
    }

    private String run(String cmd, List<String> args = [], String cache = 'cache') {
        out.reset(); err.reset()
        final int status = new CasCommands().run(cmd, args, config(cache), new PrintStream(out, true), new PrintStream(err, true))
        assert status == 0 : err.toString()
        return out.toString().trim()
    }

    private Cid logRun(String name) {
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: name], Fixtures.leaf("${name}.bam", Fixtures.contentCid(name), 1L)]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ["aligned/${name}.bam".toString()]]]))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, System.currentTimeMillis())
        return completion
    }

    private Map snapMeta() {
        DriverManager.getConnection("jdbc:sqlite:${tempDir.resolve('store/index/v6.sqlite')}").withCloseable { c ->
            def rs = c.createStatement().executeQuery('SELECT key, value FROM meta'); def m = [:]
            while( rs.next() ) m[rs.getString(1)] = rs.getString(2)
            def r2 = c.createStatement().executeQuery('SELECT (SELECT count(*) FROM block_ref), (SELECT count(*) FROM store_stats), (SELECT count(*) FROM run)')
            r2.next(); m.block_ref = r2.getInt(1); m.store_stats = r2.getInt(2); m.runs = r2.getInt(3)
            m
        }
    }

    private void meta(String cache, String key, String value) {
        DriverManager.getConnection("jdbc:sqlite:${tempDir.resolve("${cache}/index.sqlite")}").withCloseable { c ->
            c.createStatement().executeUpdate("INSERT OR REPLACE INTO meta(key, value) VALUES ('${key}', '${value}')")
        }
    }

    def 'reindex --seed false recovers a lean-seeded cache, and a second lean-seeded cache then keeps the recovered figures'() {
        given: 'a run and a full snapshot'
        logRun('A')
        run('snapshot')
        assert snapMeta().store_stats == 1

        when: 'the figures run out of memory (the cache remembers an OOM at 1 ref and any heap)'
        meta('cache', 'snapshot_out_of_memory:lab', "1 ${Long.MAX_VALUE}")
        logRun('B')
        run('snapshot')

        then:
        snapMeta().storage_omitted == 'out_of_memory'

        when: 'a second, persistent cache (another machine) seeds from it'
        run('snapshot', [], 'cache2')

        then:
        snapMeta().storage_omitted == 'seeded_lean'

        when: 'reindex with the default seed: still lean'
        String o = run('reindex')

        then:
        snapMeta().storage_omitted == 'seeded_lean'
        o.contains('nextflow plugin nf-blocks:reindex --seed false')

        when: 'the documented recovery'
        o = run('reindex', ['--seed', 'false'])
        Map m = snapMeta()

        then: 'figures are back'
        m.storage_omitted == null
        m.store_stats == 1
        m.block_ref > 0
        m.runs == 2

        when: 'the second cache writes after a new run'
        logRun('C')
        o = run('snapshot', [], 'cache2')
        m = snapMeta()

        then: 'the recovered figures survive, and the line says why and what to run'
        m.storage_omitted == null
        m.store_stats == 1
        m.runs == 2
        o.startsWith('nf-blocks:snapshot: not rewritten: seeded_lean_base_full: ')
        o.contains('nextflow plugin nf-blocks:reindex --seed false')

        when: 'reindex of the second cache, with the default seed, now seeds from the full snapshot'
        o = run('reindex', [], 'cache2')
        m = snapMeta()

        then: 'it writes figures, and the new run is in'
        m.storage_omitted == null
        m.store_stats == 1
        m.runs == 3
    }

    def 'a lean-seeded cache\'s snapshot rewrite after a sweep keeps a snapshot with figures, and says why'() {
        given: 'a full snapshot, and a second cache seeded lean from an earlier lean one'
        logRun('A')
        run('snapshot')
        meta('cache', 'snapshot_out_of_memory:lab', "1 ${Long.MAX_VALUE}")
        logRun('B')
        run('snapshot')
        run('snapshot', [], 'cache2')
        assert snapMeta().storage_omitted == 'seeded_lean'
        run('reindex', ['--seed', 'false'])
        assert snapMeta().storage_omitted == null

        when: 'a sweep from the second cache rewrites the snapshot'
        String o = RetentionCommands.forgetAndSnapshot(new robsyme.cas.CasSession(robsyme.cas.CasConfig.fromSession(config('cache2'))),
            new robsyme.cas.core.SweepReport())

        then:
        o.startsWith('not rewritten: seeded_lean_base_full: ')
        o.contains('nextflow plugin nf-blocks:reindex --seed false')
        snapMeta().storage_omitted == null
    }
}
