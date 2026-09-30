package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.cli.Launcher
import nextflow.cli.PluginExecAware
import nextflow.file.FileHelper
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper
import robsyme.cas.cli.CasCommands
import robsyme.cas.nio.CasFileSystemProvider
import robsyme.cas.trace.CasObserver

/**
 * Plugin entry point for nf-blocks, the content-addressed lineage store.
 * See DESIGN.md at the repository root for the contract this plugin implements.
 */
@CompileStatic
class CasPlugin extends BasePlugin implements PluginExecAware {

    private static CasFileSystemProvider provider

    CasPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        super.start()
        provider()
    }

    /**
     * Nextflow stops the plugins on main just before it exits
     * (ScriptRunner.shutdown), so an aborted run's RunCompletion, which may
     * still be on its way on the aborting thread, is finished here first
     * (DESIGN.md §11).
     */
    @Override
    void stop() {
        CasObserver.finishPending()
        super.stop()
    }

    /**
     * The one `cas` provider of this JVM. A plugin's own
     * `META-INF/services/java.nio.file.spi.FileSystemProvider` is never seen by
     * `FileSystemProvider.installedProviders()` -- that list is built by the
     * system class loader -- so Nextflow's own installer is used instead.
     */
    static synchronized CasFileSystemProvider provider() {
        if( provider == null )
            provider = FileHelper.getOrInstallProvider(CasFileSystemProvider)
        return provider
    }

    /** `nextflow plugin nf-blocks:<cmd>` (DESIGN.md §15). */
    @Override
    int exec(Launcher launcher, String pluginId, String cmd, List<String> args) {
        return new CasCommands().exec(launcher, pluginId, cmd, args)
    }
}
