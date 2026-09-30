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

    def 'snapshot says not rewritten: fewer_runs, and exits 0, when the stored snapshot has more runs than the cache'() {
        given: 'a stored snapshot counting three runs, and a Store Log with one'
        logRun()
        final Path file = tempDir.resolve('store/index/v3.sqlite')
        Files.createDirectories(file.parent)
        DriverManager.getConnection("jdbc:sqlite:${file}").withCloseable { c ->
            c.createStatement().withCloseable { st ->
                st.executeUpdate('CREATE TABLE run (id INTEGER PRIMARY KEY)')
                st.executeUpdate('INSERT INTO run VALUES (1), (2), (3)')
            }
        }
        final byte[] before = Files.readAllBytes(file)

        when:
        final int status = run('snapshot')

        then:
        status == 0
        out.toString().trim() == 'nf-blocks:snapshot: not rewritten: fewer_runs'
        Files.readAllBytes(file) == before
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
    // Nextflow >= 26.08.0-edge: the 3-argument exec has no Launcher, so the config is
    // read from the default files alone (DESIGN.md §15).

    private int execWithoutLauncher(String cmd, List<String> args, Path home, Path launchDir, Map<String, String> env = [:]) {
        return new CasCommands().exec('nf-blocks', cmd, args, home, launchDir, env, new PrintStream(out, true), new PrintStream(err, true))
    }

    private void writeConfig(Path file, String text) {
        Files.createDirectories(file.parent)
        Files.writeString(file, text)
    }

    private String storeConfig() {
        return """
            lineage.store.location = 'cas://lab'
            cas.stores.lab.location = '${tempDir.resolve('store')}'
            cas.index.path = '${tempDir.resolve('cache/index.sqlite')}'
        """.stripIndent()
    }

    def 'without a Launcher, the verb reads nextflow.config in the launch directory'() {
        given:
        logRun()
        final Path launchDir = tempDir.resolve('launch')
        writeConfig(launchDir.resolve('nextflow.config'), storeConfig())

        when:
        final int status = execWithoutLauncher('snapshot', [], tempDir.resolve('home'), launchDir)

        then:
        status == 0
        Files.isRegularFile(tempDir.resolve('store/index/v3.sqlite'))
        out.toString().contains('1 runs')
    }

    def 'without a Launcher, the home config is read first and the launch directory config overrides it'() {
        given:
        logRun()
        final Path home = tempDir.resolve('home')
        final Path launchDir = tempDir.resolve('launch')
        writeConfig(home.resolve('config'), storeConfig().replace("'cas://lab'", "'cas://elsewhere'"))
        writeConfig(launchDir.resolve('nextflow.config'), "lineage.store.location = 'cas://lab'\n")

        when:
        final int status = execWithoutLauncher('snapshot', [], home, launchDir)

        then: 'cas.stores comes from the home config, lineage from the launch directory'
        status == 0
        Files.isRegularFile(tempDir.resolve('store/index/v3.sqlite'))
    }

    def 'without a Launcher, NXF_CONFIG_FILE names the launch directory config'() {
        given:
        logRun()
        final Path launchDir = tempDir.resolve('launch')
        writeConfig(tempDir.resolve('other/cas.config'), storeConfig())

        when:
        final int status = execWithoutLauncher('snapshot', [], tempDir.resolve('home'), launchDir,
            [NXF_CONFIG_FILE: tempDir.resolve('other/cas.config').toString()])

        then:
        status == 0
        Files.isRegularFile(tempDir.resolve('store/index/v3.sqlite'))
    }

    def 'without a Launcher and without any config file, the verb reports the missing store'() {
        expect:
        execWithoutLauncher('snapshot', [], tempDir.resolve('home'), tempDir.resolve('launch')) == 1
        err.toString().contains('lineage.store.location')
    }

    def 'without a Launcher, a config that does not parse is reported, exit 1'() {
        given:
        final Path launchDir = tempDir.resolve('launch')
        writeConfig(launchDir.resolve('nextflow.config'), 'cas { stores { \n')

        expect:
        execWithoutLauncher('snapshot', [], tempDir.resolve('home'), launchDir) == 1
        err.toString().contains('could not read the Nextflow config')
    }
}
