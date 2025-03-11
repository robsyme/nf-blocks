package nextflow.blocks

import groovy.util.logging.Slf4j
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

import java.nio.file.spi.FileSystemProvider

/**
 * Implements the blocks plugin
 *
 * @author Rob Syme <rob.syme@gmail.com>
 */
@Slf4j
class BlocksPlugin extends BasePlugin {
    BlocksPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
    
    @Override
    void start() {
        super.start()
    }
} 