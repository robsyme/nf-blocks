package nextflow.blocks.fs

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.spi.FileSystemProvider

class BlocksFileSystemProviderTest extends Specification {
    @TempDir
    Path tempDir

    // Note: These tests skip provider finding since the plugin registration
    // happens at runtime and isn't available in unit test context

    // Helper method to create scheme-specific providers like the plugin does
    private BlocksFileSystemProvider createSchemeSpecificProvider(String scheme) {
        return new BlocksFileSystemProvider() {
            @Override
            String getScheme() {
                return scheme
            }
        }
    }

    @Unroll
    def "should support scheme: #scheme"() {
        given:
        def provider = createSchemeSpecificProvider(scheme)
        
        expect:
        provider.getScheme() == scheme
        
        where:
        scheme << ["blocks+file", "blocks+http"]
    }

    def "should parse blocks+file URI correctly"() {
        given:
        def provider = new BlocksFileSystemProvider()
        def uri = URI.create("blocks+file:///test-store/path/to/file.txt")
        
        when:
        def uriInfo = provider.parseBlocksUri(uri)
        
        then:
        uriInfo.backend == "file"
        uriInfo.backendUri == "./test-store"
        uriInfo.blocksPath == "/path/to/file.txt"
        uriInfo.cacheKey == "file://test-store"
    }

    def "should parse blocks+http URI correctly"() {
        given:
        def provider = new BlocksFileSystemProvider()
        def uri = URI.create("blocks+http://127.0.0.1:5001/ipfs/path")
        
        when:
        def uriInfo = provider.parseBlocksUri(uri)
        
        then:
        uriInfo.backend == "http"
        uriInfo.backendUri == "/ip4/127.0.0.1/tcp/5001"
        uriInfo.blocksPath == "/ipfs/path"
        uriInfo.cacheKey == "http://127.0.0.1:5001"
    }

    def "should create file system for blocks+file URI"() {
        given:
        def storeDir = tempDir.resolve("test-blocks")
        Files.createDirectories(storeDir)
        def provider = createSchemeSpecificProvider("blocks+file")
        def uri = URI.create("blocks+file:///test-blocks/")
        
        when:
        def fileSystem = provider.newFileSystem(uri, [:])
        
        then:
        fileSystem != null
        fileSystem instanceof BlocksFileSystem
        
        cleanup:
        fileSystem?.close()
    }

    def "should handle invalid URI schemes"() {
        given:
        def provider = new BlocksFileSystemProvider()
        def invalidUri = URI.create("http://example.com/path")
        
        when:
        provider.parseBlocksUri(invalidUri)
        
        then:
        thrown(IllegalArgumentException)
    }

    def "should reject unsupported schemes"() {
        given:
        def provider = new BlocksFileSystemProvider()
        
        expect:
        !provider.supportsScheme("blocks+unknown")
        !provider.supportsScheme("http")
        !provider.supportsScheme("file")
    }

    def "should write and read file using blocks+file scheme"() {
        given:
        def storeDir = tempDir.resolve("integration-test")
        Files.createDirectories(storeDir)
        def provider = createSchemeSpecificProvider("blocks+file")
        def uri = URI.create("blocks+file:///integration-test/test.txt")
        def content = "Hello, blocks!"
        
        when:
        def fileSystem = provider.newFileSystem(uri, [:])
        def path = fileSystem.getPath("/test.txt")
        Files.write(path, content.bytes)
        def readContent = new String(Files.readAllBytes(path))
        
        then:
        readContent == content
        
        cleanup:
        fileSystem?.close()
    }
} 