package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.blocks.fs.BlocksFileSystemProvider
import nextflow.plugin.extension.PluginExtensionPoint
import org.pf4j.Extension

import java.lang.reflect.Field
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Plugin extension that registers the BlocksFileSystemProvider with Nextflow
 */
@Extension
@CompileStatic
@Slf4j
class BlocksPluginExtension extends PluginExtensionPoint {

    @Override
    protected void init(Session session) {
        log.info "Initializing BlocksFileSystemProvider plugin extension"
    }
} 