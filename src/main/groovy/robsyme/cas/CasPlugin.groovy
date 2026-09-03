package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.file.FileHelper
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper
import robsyme.cas.nio.CasFileSystemProvider

/**
 * Plugin entry point for nf-blocks, the content-addressed lineage store.
 * See DESIGN.md at the repository root for the contract this plugin implements.
 */
@CompileStatic
class CasPlugin extends BasePlugin {

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
}
