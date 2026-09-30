package robsyme.cas

import org.pf4j.PluginDescriptor
import org.pf4j.PluginWrapper
import spock.lang.Specification

/**
 * The plugin verbs' entry points (DESIGN.md §15). Nextflow 26.04.6 calls the
 * 4-argument {@code exec(Launcher, ...)}; from 26.08.0-edge
 * ({@code nextflow.cli.PluginExecAware} after nextflow 1dc8cf68f) the
 * 3-argument {@code exec(pluginId, cmd, args)}.
 */
class CasPluginTest extends Specification {

    private final PrintStream stderr = System.err

    private CasPlugin plugin() {
        final descriptor = Stub(PluginDescriptor) { getPluginId() >> 'nf-blocks' }
        final wrapper = Stub(PluginWrapper) { getDescriptor() >> descriptor }
        return new CasPlugin(wrapper)
    }

    def 'the 3-argument exec of Nextflow >= 26.08.0-edge reaches the verbs and returns their exit code'() {
        given:
        final ByteArrayOutputStream captured = new ByteArrayOutputStream()
        System.err = new PrintStream(captured, true)

        when:
        final int status = plugin().exec('nf-blocks', 'frobnicate', [])

        then: 'the usage error of CasCommands.run, exit 2'
        status == 2
        captured.toString().contains("unknown command 'nf-blocks:frobnicate'")

        cleanup:
        System.err = stderr
    }

    def 'the 3-argument exec is a public method a Nextflow >= 26.08.0-edge caller links to'() {
        expect:
        CasPlugin.getMethod('exec', String, String, List).returnType == int.class
    }
}
