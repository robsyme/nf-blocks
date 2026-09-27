package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

class CasCommandsTest extends Specification {

    @TempDir
    Path tempDir

    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    private Map config() {
        return [
            lineage: [store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                index: [path: tempDir.resolve('cache/index.sqlite').toString()],
            ],
        ]
    }

    private int run(String cmd, List<String> args = [], Map cfg = config()) {
        return new CasCommands().run(cmd, args, cfg, new PrintStream(out, true), new PrintStream(err, true))
    }

    private Cid logRun() {
        final LocalBlockStore store = new LocalBlockStore(tempDir.resolve('store'), 'lab', true)
        final Cid manifest = store.putDagCbor(Fixtures.runManifest())
        final Cid item = store.putDagCbor(Fixtures.outputItem([[sample: 'A'], Fixtures.leaf('A.bam', Fixtures.contentCid('A'), 1L)]))
        final Cid collection = store.putDagCbor(Fixtures.outputCollection(manifest, 'aligned', [[item, ['aligned/A.bam']]]))
        final Cid completion = store.putDagCbor(Fixtures.runCompletion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, System.currentTimeMillis())
        return completion
    }

    def 'snapshot catches the index up from the Store Log and writes the snapshot at any size'() {
        given:
        final Cid completion = logRun()

        when:
        final int status = run('snapshot')
        final Path file = tempDir.resolve('store/index/v3.sqlite')

        then:
        status == 0
        Files.isRegularFile(file)
        out.toString().contains(file.toString())
        out.toString().contains('1 runs')
        DriverManager.getConnection("jdbc:sqlite:${file}").withCloseable { c ->
            c.createStatement().executeQuery('SELECT completion_cid FROM run').with { next(); getString(1) }
        } == completion.toString()
    }

    def 'an unknown verb is a usage error listing the verbs'() {
        expect:
        run('frobnicate') == 2
        err.toString().contains('explore')
        err.toString().contains('items')
        err.toString().contains('put')
        err.toString().contains('snapshot')
    }

    def 'snapshot takes no flags'() {
        expect:
        run('snapshot', ['--port', '1']) == 2
        err.toString().contains('--port')
    }

    def 'a config without a cas lineage store is reported, not thrown'() {
        expect:
        run('snapshot', [], [:]) == 1
        err.toString().contains('lineage.store.location')
    }
}
