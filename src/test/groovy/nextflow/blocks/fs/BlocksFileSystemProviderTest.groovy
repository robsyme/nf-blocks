package nextflow.blocks.fs

import spock.lang.Specification

import java.nio.file.FileSystems
import java.nio.file.spi.FileSystemProvider

class BlocksFileSystemProviderTest extends Specification {

    def "should find blocks file system provider"() {
        when:
        def providers = FileSystemProvider.installedProviders()
        def blocksProvider = providers.find { it.scheme == 'blocks' }

        then:
        blocksProvider != null
        blocksProvider instanceof BlocksFileSystemProvider
    }
} 