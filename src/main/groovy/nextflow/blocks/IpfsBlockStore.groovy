package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.IPFS
import io.ipfs.api.MerkleNode
import io.ipfs.api.NamedStreamable
import io.ipfs.multihash.Multihash
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import io.ipfs.cid.Cid
import java.io.IOException
import java.net.HttpURLConnection
import java.util.Optional
import java.util.Collections
import java.util.ArrayList

/**
 * Implements a block store that uses IPFS as the backend
 */
@Slf4j
@CompileStatic
class IpfsBlockStore implements BlockStore {
    private final IPFS ipfs
    
    /**
     * Create a new IpfsBlockStore
     * @param multiaddr The multiaddr of the IPFS node (e.g. "/ip4/127.0.0.1/tcp/5001")
     */
    IpfsBlockStore(String multiaddr) {
        this.ipfs = new IPFS(multiaddr)
    }

    /**
     * Create a new IpfsBlockStore using default localhost settings
     */
    IpfsBlockStore() {
        this("/ip4/127.0.0.1/tcp/5001")
    }

    /**
     * Create a new IpfsBlockStore with a provided IPFS instance (for testing)
     * @param ipfs The IPFS instance to use
     * @param skipValidation Whether to skip connection validation (for testing)
     */
    IpfsBlockStore(IPFS ipfs, boolean skipValidation) {
        this.ipfs = ipfs
        if (!skipValidation) {
            validateConnection()
        }
    }

    /**
     * Create a new IpfsBlockStore with a provided IPFS instance (for testing)
     * @param ipfs The IPFS instance to use
     */
    IpfsBlockStore(IPFS ipfs) {
        this(ipfs, false)
    }

    /**
     * Validate that we can connect to the IPFS node
     */
    private void validateConnection() {
        try {
            def version = ipfs.version()
            String versionStr = version instanceof Map ? version.get('Version')?.toString() : version?.toString()
            log.debug "Successfully connected to IPFS node, version: ${versionStr}"
            
            // Run diagnostics to check IPFS compatibility
            runIpfsDiagnostics()
        } catch (Exception e) {
            log.error "Failed to connect to IPFS node: ${e.message}"
            if (e.getCause() instanceof java.net.ConnectException) {
                log.error "Connection refused. Make sure the IPFS daemon is running with 'ipfs daemon'"
                log.error getLocalBlockStoreInstructions()
            } else if (e.getCause() instanceof java.net.ProtocolException) {
                log.error "Protocol error. This may be due to an incompatible IPFS version or API change."
                log.error getLocalBlockStoreInstructions()
            }
            throw new RuntimeException("Failed to connect to IPFS node. Is IPFS daemon running? Error: ${e.message}", e)
        }
    }
    
    /**
     * Run diagnostics to check IPFS compatibility
     */
    private void runIpfsDiagnostics() {
        log.debug "Running IPFS compatibility diagnostics..."
        
        // Define test cases with proper type information
        def tests = [
            [name: "Basic connectivity", test: { -> ipfs.id() } as Closure],
            [name: "File operations", test: { -> 
                byte[] testData = "nf-blocks-test".bytes
                def streamable = new NamedStreamable.ByteArrayWrapper(testData)
                ipfs.add(streamable)
            } as Closure],
            [name: "DAG operations", test: { ->
                byte[] testData = '{"test":"data"}'.bytes
                String codecName = formatCodecName(Cid.Codec.DagCbor)
                ipfs.dag.put(codecName, testData, codecName)
            } as Closure]
        ]
        
        // Run each test
        tests.each { testCase ->
            try {
                def testClosure = testCase.test as Closure
                def result = testClosure.call()
                log.debug "IPFS ${testCase.name} test successful"
            } catch (Exception e) {
                log.warn "IPFS ${testCase.name} test failed: ${e.message}"
                if (testCase.name == "DAG operations") {
                    log.error """
                        |Critical IPFS functionality test failed: dag.put operations are not working.
                        |This will prevent blocks from being stored correctly.
                        |
                        |Possible solutions:
                        |1. Update your IPFS daemon to the latest version
                        |2. Run IPFS with: ipfs daemon --enable-pubsub-experiment
                        |3. ${getLocalBlockStoreInstructions()}
                        """.stripMargin()
                }
            }
        }
    }

    /**
     * Get instructions for switching to the local block store
     * @return String with instructions
     */
    private static String getLocalBlockStoreInstructions() {
        return """Consider using the local file system block store instead by adding this to your nextflow.config file:
                |
                |blocks {
                |    type = 'local'
                |    // Optional: specify a directory for the local block store
                |    // dir = '/path/to/blocks'
                |}"""
    }

    private <T> T withIpfs(Closure<T> operation, String errorPrefix = "IOException contacting IPFS daemon") {
        try {
            return operation()
        } catch (IOException e) {
            throw new RuntimeException("${errorPrefix}.\n${e.message}", e)
        }
    }

    String chcid(String path, Map options) {
        withIpfs { ipfs.files.chcid(path) }
    }

    String cp(String source, String dest, boolean parents) {
        withIpfs { ipfs.files.cp(source, dest, parents) }
    }

    Map flush(String path) {
        withIpfs { path ? ipfs.files.flush(path) : ipfs.files.flush() }
    }

    List<Map> ls(String path, Map options) {
        withIpfs { path ? ipfs.files.ls(path) : ipfs.files.ls() }
    }

    String mkdir(String path, boolean parents, Map options) {
        withIpfs { ipfs.files.mkdir(path, parents) }
    }

    String mv(String source, String dest) {
        withIpfs { ipfs.files.mv(source, dest) }
    }

    byte[] read(String path, Map options) {
        withIpfs { ipfs.files.read(path) }
    }

    String rm(String path, boolean recursive, boolean force) {
        withIpfs { ipfs.files.rm(path, recursive, force) }
    }

    Map stat(String path, Map options) {
        withIpfs { ipfs.files.stat(path) }
    }

    String write(String path, byte[] data, Map options) {
        withIpfs {
            def streamable = new NamedStreamable.ByteArrayWrapper(data)
            return ipfs.files.write(path, streamable, true, false)
        }
    }

    @Override
    MerkleNode get(Multihash hash) {
        log.trace "Getting block: ${hash}"
        withIpfs {
            byte[] data = ipfs.get(hash)
            return new MerkleNode(
                hash.toString(),                  // hash as String
                Optional.empty(),                 // name
                Optional.empty(),                 // size
                Optional.empty(),                 // largeSize
                Optional.empty(),                 // type
                Collections.emptyList(),          // links
                Optional.of(data)                 // data
            )
        }
    }

    @Override
    MerkleNode add(byte[] data) {
        return add(data, Cid.Codec.DagCbor)
    }

    @Override
    MerkleNode add(byte[] data, Cid.Codec codec) {
        try {
            String codecName = formatCodecName(codec)
            log.debug "Adding data with codec: ${codecName}"
            
            MerkleNode node = ipfs.dag.put(codecName, data, codecName)
            
            // Log the block addition
            log.info "🔗 IPFS BLOCK STORE: Added raw data block"
            log.info "   CID: ${node.hash}"
            log.info "   Size: ${data.length} bytes"
            log.info "   Codec: ${codecName}"
            
            return node
        } catch (IOException e) {
            handleIpfsError(e, "Failed to add data to IPFS")
        } catch (Exception e) {
            log.error "Unexpected error adding data to IPFS: ${e.message}"
            throw new RuntimeException("Failed to add data to IPFS: ${e.message}", e)
        }
    }

    private void handleIpfsError(IOException e, String message) {
        log.error "${message}: ${e.message}"
        if (e.getCause() instanceof java.net.ProtocolException || 
            e.message?.contains("Server rejected operation")) {
            log.error """
                |IPFS server rejected the operation. This may be due to:
                |1. An incompatible IPFS version (try updating your IPFS daemon)
                |2. IPFS API changes that require client library updates
                |3. IPFS daemon configuration issues
                |
                |Try running IPFS with: ipfs daemon --enable-pubsub-experiment
                |
                |${getLocalBlockStoreInstructions()}
                """.stripMargin()
        }
        throw new RuntimeException("${message}: ${e.message}", e)
    }

    private static final Map<Cid.Codec, String> CODEC_NAMES = [
        (Cid.Codec.Cbor): "cbor",
        (Cid.Codec.Raw): "raw",
        (Cid.Codec.DagProtobuf): "dag-pb",
        (Cid.Codec.DagCbor): "dag-cbor",
        (Cid.Codec.Libp2pKey): "libp2p-key",
        (Cid.Codec.EthereumBlock): "eth-block",
        (Cid.Codec.EthereumTx): "eth-block-list",
        (Cid.Codec.BitcoinBlock): "bitcoin-block",
        (Cid.Codec.BitcoinTx): "bitcoin-tx",
        (Cid.Codec.ZcashBlock): "zcash-block",
        (Cid.Codec.ZcashTx): "zcash-tx"
    ].asImmutable()

    String formatCodecName(Cid.Codec codec) {
        if (!CODEC_NAMES.containsKey(codec)) {
            log.warn "Unknown codec: ${codec}, using dag-cbor instead"
        }
        return CODEC_NAMES.getOrDefault(codec, "dag-cbor")
    }
    
    /**
     * Convert codec name string back to Cid.Codec enum.
     * This is the reverse of formatCodecName().
     */
    Cid.Codec parseCodecName(String codecName) {
        // Create reverse mapping from codec names to enums
        def reverseMapping = CODEC_NAMES.collectEntries { codec, name -> [name, codec] }
        
        if (!reverseMapping.containsKey(codecName)) {
            log.warn "Unknown codec name: ${codecName}, using DagCbor instead"
            return Cid.Codec.DagCbor
        }
        
        return reverseMapping[codecName] as Cid.Codec
    }

    /**
     * Add data to the block store with codec specified via options map.
     * This provides a flexible interface for specifying codecs by name.
     */
    MerkleNode add(byte[] data, Map options) {
        String inputFormat = options.getOrDefault('inputFormat', 'dag-cbor') as String
        
        // Convert string format to Codec enum using our CID-aware parsing
        Cid.Codec codec = parseCodecName(inputFormat)
        
        log.debug "Parsed codec from options: ${inputFormat} → ${codec}"
        return add(data, codec)
    }

    @Override
    MerkleNode addPath(Path path) {
        withIpfs {
            def streamable = Files.isDirectory(path) 
                ? new NamedStreamable.DirWrapper(path.fileName.toString(), createDirWrappers(path))
                : new NamedStreamable.FileWrapper(path.toFile())
            def cids = ipfs.add(streamable)
            MerkleNode node = cids.last()
            
            // Log the path addition
            String type = Files.isDirectory(path) ? "directory" : "file"
            log.info "🗂️  IPFS BLOCK STORE: Added ${type}"
            log.info "   Path: ${path}"
            log.info "   CID: ${node.hash}"
            log.info "   Size: ${node.size.present ? node.size.get() : 'unknown'} bytes"
            log.info "   Links: ${node.links.size()}"
            
            return node
        }
    }

    private List<NamedStreamable> createDirWrappers(Path dirPath) {
        List<NamedStreamable> result = new ArrayList<>()
        Files.list(dirPath).forEach { Path path ->
            if (Files.isDirectory(path)) {
                result.add(new NamedStreamable.DirWrapper(path.fileName.toString(), createDirWrappers(path)))
            } else {
                result.add(new NamedStreamable.FileWrapper(path.toFile()))
            }
        }
        return result
    }
    
    @Override
    void updateOptions(Map options) {
        log.warn "IPFS block store does not support runtime configuration updates"
    }
} 