package nextflow.blocks

import io.ipfs.api.IPFS
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import spock.lang.Specification
import spock.lang.Unroll
import java.security.MessageDigest
import java.util.Base64
import java.util.NoSuchElementException

class IpfsBlockStoreTest extends Specification {
    static final int RAW_CODEC = 0x55 // Raw codec type

    class TestBlock extends IPFS.Block {
        private final Map<String, byte[]> blocks = [:]
        private final Map<String, Map> stats = [:]

        TestBlock() {
            super(null)
        }

        @Override
        List<Map> put(List<byte[]> blocks, Optional<String> format) {
            def results = []
            blocks.each { block ->
                def digest = MessageDigest.getInstance("SHA-256")
                def hash = digest.digest(block)
                def mh = new Multihash(Multihash.Type.sha2_256, hash)
                def cidBytes = new byte[mh.toBytes().length + 2]
                cidBytes[0] = 0x01  // Version 1
                cidBytes[1] = Cid.Codec.Raw.type  // Raw codec
                System.arraycopy(mh.toBytes(), 0, cidBytes, 2, mh.toBytes().length)
                def cid = Cid.cast(cidBytes)
                def key = Base64.getEncoder().encodeToString(hash)
                this.blocks[key] = block
                def stat = [Key: key, Size: block.length]
                this.stats[key] = stat
                results << stat
            }
            return results
        }

        @Override
        byte[] get(Multihash hash) {
            def key = Base64.getEncoder().encodeToString(hash.hash)
            if (!blocks.containsKey(key)) {
                throw new NoSuchElementException("Block not found: ${key}")
            }
            return blocks[key]
        }

        @Override
        Map stat(Multihash hash) {
            def key = Base64.getEncoder().encodeToString(hash.hash)
            if (!stats.containsKey(key)) {
                throw new NoSuchElementException("Block not found: ${key}")
            }
            return stats[key]
        }
    }

    class TestRefs extends IPFS.Refs {
        TestRefs() {
            super(null)
        }

        @Override
        List<String> local() {
            return []
        }
    }

    class TestIpfs extends IPFS {
        TestIpfs() {
            super("localhost", 5001, "/api/v0/", false, 0, 0, false)  // Use minimal constructor with no connection attempt
            def field = IPFS.getDeclaredField("block")
            field.setAccessible(true)
            field.set(this, new TestBlock())
            
            field = IPFS.getDeclaredField("refs")
            field.setAccessible(true)
            field.set(this, new TestRefs())
        }

        @Override
        String version() {
            return "0.25.0"  // Return a fixed version to prevent connection attempts
        }

        @Override
        Map commands() {
            return [:]  // Return empty map to prevent connection attempts
        }
    }

    private Cid createCidForBlock(byte[] block, Multihash.Type hashType) {
        def algorithm = switch(hashType) {
            case Multihash.Type.sha1 -> "SHA-1"
            case Multihash.Type.sha2_256 -> "SHA-256"
            case Multihash.Type.sha2_512 -> "SHA-512"
            case Multihash.Type.sha3_224 -> "SHA3-224"
            case Multihash.Type.sha3_256 -> "SHA3-256"
            case Multihash.Type.sha3_512 -> "SHA3-512"
            default -> throw new IllegalArgumentException("Unsupported hash type: ${hashType}")
        }
        
        def digest = MessageDigest.getInstance(algorithm)
        def hash = digest.digest(block)
        def mh = new Multihash(hashType, hash)
        def cidBytes = new byte[mh.toBytes().length + 2]
        cidBytes[0] = 0x01  // Version 1
        cidBytes[1] = Cid.Codec.Raw.type  // Raw codec
        System.arraycopy(mh.toBytes(), 0, cidBytes, 2, mh.toBytes().length)
        return Cid.cast(cidBytes)
    }

    @Unroll
    def "should store and retrieve raw blocks with #hashType hash"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def block = "Hello, World!".bytes
        def cid = createCidForBlock(block, hashType)

        when:
        store.putBlock(cid, block)
        
        then:
        store.hasBlock(cid)
        
        when:
        def result = store.getBlock(cid)
        
        then:
        result == block
        
        where:
        hashType << [
            Multihash.Type.sha2_256  // Start with just one hash type for testing
        ]
    }

    def "should verify block content matches CID"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def block = "Hello, World!".bytes
        def wrongBlock = "Wrong content".bytes
        def cid = createCidForBlock(block, Multihash.Type.sha2_256)

        when:
        store.putBlock(cid, wrongBlock)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw exception for unsupported hash algorithm"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def block = "Hello, World!".bytes
        def cid = createCidForBlock(block, Multihash.Type.sha2_256)
        def mh = new Multihash(Multihash.Type.blake2b_256, cid.hash)
        def cidBytes = new byte[mh.toBytes().length + 2]
        cidBytes[0] = 0x01  // Version 1
        cidBytes[1] = Cid.Codec.Raw.type  // Raw codec
        System.arraycopy(mh.toBytes(), 0, cidBytes, 2, mh.toBytes().length)
        def badCid = Cid.cast(cidBytes)

        when:
        store.putBlock(badCid, block)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw exception for missing block"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def hash = new byte[32] // Create a zero-filled hash
        def mh = new Multihash(Multihash.Type.sha2_256, hash)
        def cidBytes = new byte[mh.toBytes().length + 2]
        cidBytes[0] = 0x01  // Version 1
        cidBytes[1] = Cid.Codec.Raw.type  // Raw codec
        System.arraycopy(mh.toBytes(), 0, cidBytes, 2, mh.toBytes().length)
        def cid = Cid.cast(cidBytes)

        when:
        store.getBlock(cid)

        then:
        thrown(NoSuchElementException)
    }
} 