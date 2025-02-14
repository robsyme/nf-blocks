package nextflow.blocks

import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import nextflow.cbor.CborConverter
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll
import java.nio.file.Path
import java.security.MessageDigest

class FileSystemBlockStoreTest extends Specification {
    @TempDir
    Path tempDir

    private Cid createCidForBlock(byte[] block, Multihash.Type hashType, Cid.Codec codec = Cid.Codec.Raw) {
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
        return Cid.buildCidV1(codec, mh.getType(), hash)
    }

    @Unroll
    def "should store and retrieve raw blocks with #hashType hash"() {
        given:
        def store = new FileSystemBlockStore(tempDir.toString())
        def block = "Hello, World!".bytes
        def cid = createCidForBlock(block, hashType)

        when:
        store.putBlock(cid, block)
        
        then:
        store.hasBlock(cid)
        store.getBlock(cid) == block
        cid.codec == Cid.Codec.Raw
        
        where:
        hashType << [
            Multihash.Type.sha1,
            Multihash.Type.sha2_256,
            Multihash.Type.sha2_512,
            Multihash.Type.sha3_256,
            Multihash.Type.sha3_512
        ]
    }

    def "should store and retrieve DAG-CBOR blocks"() {
        given:
        def store = new FileSystemBlockStore(tempDir.toString())
        def data = [name: "test", value: 42]
        def block = CborConverter.toCbor(data).toByteArray()
        def cid = createCidForBlock(block, Multihash.Type.sha2_256, Cid.Codec.DagCbor)

        when:
        store.putBlock(cid, block)
        
        then:
        store.hasBlock(cid)
        store.getBlock(cid) == block
        cid.codec == Cid.Codec.DagCbor
    }

    def "should verify block content matches CID"() {
        given:
        def store = new FileSystemBlockStore(tempDir.toString())
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
        def store = new FileSystemBlockStore(tempDir.toString())
        def block = "Hello, World!".bytes
        def cid = createCidForBlock(block, Multihash.Type.sha2_256)
        // Modify the CID to use an unsupported hash type
        def badCid = Cid.buildCidV1(cid.codec, Multihash.Type.blake2b_256, cid.hash)

        when:
        store.putBlock(badCid, block)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("Unsupported hash algorithm")
    }

    def "should throw exception for missing block"() {
        given:
        def store = new FileSystemBlockStore(tempDir.toString())
        def badCid = Cid.decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

        when:
        store.getBlock(badCid)

        then:
        thrown(NoSuchElementException)
    }

    def "should create store directory if it doesn't exist"() {
        given:
        def storePath = tempDir.resolve("blocks")

        when:
        new FileSystemBlockStore(storePath.toString())

        then:
        storePath.toFile().exists()
        storePath.toFile().isDirectory()
    }
} 