package nextflow.blocks

import groovy.transform.CompileStatic
import nextflow.blocks.fs.BlocksFileSystemProvider
import org.pf4j.Plugin
import org.pf4j.PluginWrapper

import java.lang.reflect.Field
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Main plugin class for nf-blocks
 */
@CompileStatic
class BlocksPlugin extends Plugin {

    BlocksPlugin(PluginWrapper wrapper) {
        super(wrapper)
        log.info "BlocksPlugin created"
        // Register FileSystemProvider early
        registerFileSystemProvider()
    }

    @Override
    void start() {
        log.info "BlocksPlugin started"
    }

    @Override
    void stop() {
        log.info "BlocksPlugin stopped"
    }

    /**
     * Register the BlocksFileSystemProvider early in plugin initialization
     * We need to register separate providers for each scheme due to Java's FileSystemProvider limitations
     */
    private void registerFileSystemProvider() {
        try {
            // Get the installedProviders list using reflection
            Field field = FileSystemProvider.class.getDeclaredField("installedProviders")
            field.setAccessible(true)
            
            // Get the current list of providers
            List<FileSystemProvider> providers = field.get(null) as List<FileSystemProvider>
            List<FileSystemProvider> newProviders = new CopyOnWriteArrayList<>(providers)
            
            // Register providers for each supported scheme
            String[] supportedSchemes = ["blocks+file", "blocks+http"]
            
            for (String scheme : supportedSchemes) {
                // Check if a provider for this scheme already exists
                boolean exists = providers.any { it instanceof BlocksFileSystemProvider && it.scheme == scheme }
                
                if (!exists) {
                    // Create a scheme-specific provider
                    BlocksFileSystemProvider provider = new SchemeSpecificProvider(scheme)
                    newProviders.add(provider)
                    log.info "Registered BlocksFileSystemProvider for scheme: ${scheme}"
                }
            }
            
            // Replace the installedProviders list
            field.set(null, newProviders)
            
            // Verify registration
            List<FileSystemProvider> updatedProviders = FileSystemProvider.installedProviders()
            log.info "Registered providers: ${updatedProviders.collect { it.class.simpleName }.join(', ')}"
            
        } catch (Exception e) {
            log.error "Failed to register BlocksFileSystemProvider: ${e.message}", e
        }
    }
    
    /**
     * Scheme-specific wrapper for BlocksFileSystemProvider
     */
    @CompileStatic
    private static class SchemeSpecificProvider extends BlocksFileSystemProvider {
        private final String targetScheme
        
        SchemeSpecificProvider(String scheme) {
            super()
            this.targetScheme = scheme
        }
        
        @Override
        String getScheme() {
            return targetScheme
        }
    }
} 