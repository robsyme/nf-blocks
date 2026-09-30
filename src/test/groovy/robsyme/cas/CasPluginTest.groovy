package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import org.pf4j.PluginDescriptor
import org.pf4j.PluginWrapper
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The plugin verbs' entry points (DESIGN.md §15). Nextflow 26.04.6 calls the
 * 4-argument {@code exec(Launcher, ...)}; from 26.08.0-edge
 * ({@code nextflow.cli.PluginExecAware} after nextflow 1dc8cf68f) the
 * 3-argument {@code exec(pluginId, cmd, args)}. The home and launch
 * directories and the environment are pointed at a temp dir, so nothing of
 * the developer's own ~/.nextflow/config or working directory is read.
 */
class CasPluginTest extends Specification {

    @TempDir
    Path tempDir

    private final PrintStream stderr = System.err
    private final ByteArrayOutputStream captured = new ByteArrayOutputStream()

    def setup() {
        System.err = new PrintStream(captured, true)
    }

    def cleanup() {
        System.err = stderr
    }

    private CasPlugin plugin() {
        final descriptor = Stub(PluginDescriptor) { getPluginId() >> 'nf-blocks' }
        final wrapper = Stub(PluginWrapper) { getDescriptor() >> descriptor }
        final CasPlugin plugin = new CasPlugin(wrapper)
        plugin.homeDir = tempDir.resolve('home')
        plugin.launchDir = tempDir.resolve('launch')
        plugin.env = [:]
        return plugin
    }

    def 'the 3-argument exec of Nextflow >= 26.08.0-edge reaches the verbs and returns their exit code'() {
        when:
        final int status = plugin().exec('nf-blocks', 'frobnicate', [])

        then: 'the usage error of CasCommands.run, exit 2'
        status == 2
        captured.toString().contains("unknown command 'nf-blocks:frobnicate'")
    }

    def 'an unknown verb is a usage error before any config is read, even an unparseable one'() {
        given:
        Files.createDirectories(tempDir.resolve('home'))
        Files.writeString(tempDir.resolve('home/config'), 'cas { stores { \n')

        expect:
        plugin().exec('nf-blocks', 'frobnicate', []) == 2
        !captured.toString().contains('could not read the Nextflow config')
    }

    def 'the 3-argument exec reads its config from the injected home and launch directories'() {
        given:
        Files.createDirectories(tempDir.resolve('launch'))
        Files.writeString(tempDir.resolve('launch/nextflow.config'), """
            lineage.store.location = 'cas://lab'
            cas.stores.lab.location = '${tempDir.resolve('store')}'
            cas.index.path = '${tempDir.resolve('cache/index.sqlite')}'
        """.stripIndent())

        expect:
        plugin().exec('nf-blocks', 'snapshot', []) == 0
        Files.isRegularFile(tempDir.resolve('store/index/v3.sqlite'))
    }

    def 'the 3-argument exec is a public method a Nextflow >= 26.08.0-edge caller links to'() {
        expect:
        CasPlugin.getMethod('exec', String, String, List).returnType == int.class
    }
}
