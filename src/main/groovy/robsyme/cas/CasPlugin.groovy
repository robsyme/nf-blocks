package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

/**
 * Plugin entry point for nf-blocks, the content-addressed lineage store.
 * See DESIGN.md at the repository root for the contract this plugin implements.
 */
@CompileStatic
class CasPlugin extends BasePlugin {

    CasPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
