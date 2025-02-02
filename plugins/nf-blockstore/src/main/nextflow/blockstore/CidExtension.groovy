package nextflow.cid


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint

/**
 * Example plugin extension showing how to implement a basic
 * channel factory method, a channel operator and a custom function.
 *
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@Slf4j
@CompileStatic
class CidExtension extends PluginExtensionPoint {

    /*
     * A session hold information about current execution of the script
     */
    private Session session

    /*
     * nf-core initializes the plugin once loaded and session is ready
     * @param session
     */
    @Override
    protected void init(Session session) {
        this.session = session
    }

}
