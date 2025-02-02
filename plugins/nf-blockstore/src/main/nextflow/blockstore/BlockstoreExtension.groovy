package nextflow.blockstore

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.plugin.extension.PluginExtensionPoint

/**
 * Implements the blockstore extension
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@CompileStatic
class BlockstoreExtension extends PluginExtensionPoint {
    /*
     * A session hold information about current execution of the script
     */
    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
    }
} 