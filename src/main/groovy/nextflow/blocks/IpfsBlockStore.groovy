package nextflow.blocks

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.ipfs.api.IPFS
import io.ipfs.api.MerkleNode
import io.ipfs.api.NamedStreamable
import io.ipfs.multihash.Multihash
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

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
            ipfs.version()
            log.debug "Successfully connected to IPFS node"
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to IPFS node. Is IPFS daemon running?", e)
        }
    }

    String chcid(String path, Map options) {
        try {
            return ipfs.files.chcid(path)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    String cp(String source, String dest, boolean parents) {
        try {
            return ipfs.files.cp(source, dest, parents)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    Map flush(String path) {
        try {
            return path ? ipfs.files.flush(path) : ipfs.files.flush()
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    List<Map> ls(String path, Map options) {
        try {
            return path ? ipfs.files.ls(path) : ipfs.files.ls()
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    String mkdir(String path, boolean parents, Map options) {
        try {
            return ipfs.files.mkdir(path, parents)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    String mv(String source, String dest) {
        try {
            return ipfs.files.mv(source, dest)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    byte[] read(String path, Map options) {
        try {
            return ipfs.files.read(path)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    String rm(String path, boolean recursive, boolean force) {
        try {
            return ipfs.files.rm(path, recursive, force)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    Map stat(String path, Map options) {
        try {
            return ipfs.files.stat(path)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    String write(String path, byte[] data, Map options) {
        try {
            def streamable = new NamedStreamable.ByteArrayWrapper(data)
            return ipfs.files.write(path, streamable, true, false)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    byte[] get(Multihash hash) {
        try {
            return ipfs.get(hash)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    @Override
    MerkleNode add(byte[] data) {
        return add(data, [:])
    }

    @Override
    MerkleNode add(byte[] data, Map options) {
        try {
            String inputFormat = options.getOrDefault('inputFormat', 'dag-cbor')
            String outputFormat = options.getOrDefault('outputFormat', 'dag-cbor')
            return ipfs.dag.put(inputFormat, data, outputFormat)
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    @Override
    MerkleNode addPath(Path path) {
        try {
            def streamable = Files.isDirectory(path) 
                ? new NamedStreamable.DirWrapper(path.fileName.toString(), createDirWrappers(path))
                : new NamedStreamable.FileWrapper(path.toFile())
            def cids = ipfs.add(streamable)
            return cids.last()
        } catch (IOException e) {
            throw new RuntimeException("IOException contacting IPFS daemon.\n${e.message}", e)
        }
    }

    private List<NamedStreamable> createDirWrappers(Path dirPath) {
        return Files.list(dirPath)
            .map { path ->
                Files.isDirectory(path)
                    ? new NamedStreamable.DirWrapper(path.fileName.toString(), createDirWrappers(path))
                    : new NamedStreamable.FileWrapper(path.toFile())
            }
            .collect(Collectors.toList())
    }
} 