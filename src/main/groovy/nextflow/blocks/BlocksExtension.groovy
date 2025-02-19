package nextflow.blocks

import groovy.transform.CompileStatic
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session

/**
 * Implements the blockstore extension
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@CompileStatic
class BlocksExtension extends PluginExtensionPoint {
    /*
     * A session hold information about current execution of the script
     */
    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
    }
} 