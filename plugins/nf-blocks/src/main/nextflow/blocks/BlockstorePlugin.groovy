package nextflow.blocks

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.plugin.Scoped
import org.pf4j.PluginWrapper

/**
 * Implements the blocks plugin
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
    class BlocksPlugin extends BasePlugin {
    BlocksPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
} 