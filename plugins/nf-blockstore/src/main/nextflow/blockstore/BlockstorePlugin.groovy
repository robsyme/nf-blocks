package nextflow.blockstore

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.plugin.Scoped
import org.pf4j.PluginWrapper

/**
 * Implements the blockstore plugin
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
class BlockstorePlugin extends BasePlugin {
    BlockstorePlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
} 