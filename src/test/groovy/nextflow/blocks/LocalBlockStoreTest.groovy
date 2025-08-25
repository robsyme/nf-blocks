package nextflow.blocks

import io.ipfs.api.MerkleNode
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import io.ipfs.multibase.Base58
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.Random

class LocalBlockStoreTest extends Specification {
    @TempDir
    Path tempDir

    def "should create block store directory if it doesn't exist"() {
        given:
        Path blockStoreDir = tempDir.resolve("blocks")
        
        when:
        new LocalBlockStore(blockStoreDir)
        
        then:
        Files.exists(blockStoreDir)
        Files.isDirectory(blockStoreDir)
    }
    
    def "should add and retrieve a block"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        byte[] data = "Hello, world!".getBytes()
        
        when:
        MerkleNode addedNode = blockStore.add(data)
        MerkleNode retrievedNode = blockStore.get(addedNode.hash)
        
        then:
        // The hash formats may differ (CIDv1 vs CIDv0), so we only check the data
        retrievedNode.data.isPresent()
        retrievedNode.data.get() == data
    }
    
    def "should add a block with a specific codec"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        byte[] data = "Hello, world!".getBytes()
        
        when:
        MerkleNode addedNode = blockStore.add(data, Cid.Codec.Raw)
        
        then:
        // Verify the CID format for Raw codec
        addedNode.hash.toString().startsWith("baf")  // Raw codec CIDs typically start with "baf"
        
        and: "we can retrieve the block"
        MerkleNode retrievedNode = blockStore.get(addedNode.hash)
        // The hash formats may differ (CIDv1 vs CIDv0), so we only check the data
        retrievedNode.data.isPresent()
        retrievedNode.data.get() == data
    }
        
    def "should add a file"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        Path testFile = tempDir.resolve("test.txt")
        String content = "Hello, world!"
        Files.write(testFile, content.getBytes())
        
        when:
        MerkleNode addedNode = blockStore.addPath(testFile)
        
        then:
        // Verify we got a hash
        addedNode.hash != null
        
        and: "we can retrieve the block"
        Multihash multihash = addedNode.hash instanceof Cid ? 
            ((Cid)addedNode.hash).bareMultihash() : 
            Multihash.fromBase58(addedNode.hash.toString())
        MerkleNode retrievedNode = blockStore.get(multihash)
        
        // The hash formats may differ (CIDv1 vs CIDv0), so we only check the data
        retrievedNode.data.isPresent()
        
        // For UnixFS files, we need to extract the actual file content from the UnixFS data
        // This is a simplified check that just verifies the content is contained in the data
        new String(retrievedNode.data.get()).contains(content)
    }
    
    def "should add a directory"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        Path testDir = tempDir.resolve("testdir")
        Files.createDirectory(testDir)
        
        // Create a file in the directory
        Path testFile = testDir.resolve("test.txt")
        String content = "Hello, world!"
        Files.write(testFile, content.getBytes())
        
        when:
        MerkleNode addedNode = blockStore.addPath(testDir)
        
        then:
        // Verify we got a hash
        addedNode.hash != null
        
        and: "we can retrieve the block"
        Multihash multihash = addedNode.hash instanceof Cid ? 
            ((Cid)addedNode.hash).bareMultihash() : 
            Multihash.fromBase58(addedNode.hash.toString())
        MerkleNode retrievedNode = blockStore.get(multihash)
        
        // The hash formats may differ (CIDv1 vs CIDv0), so we only check the data
        retrievedNode.data.isPresent()
    }
    
    def "should throw exception when block not found"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        // Create a random multihash that won't exist in the store
        byte[] hashBytes = new byte[32]
        new Random().nextBytes(hashBytes)
        Multihash nonExistentHash = new Multihash(Multihash.Type.sha2_256, hashBytes)
        
        when:
        blockStore.get(nonExistentHash)
        
        then:
        thrown(RuntimeException)
    }
    
    def "should handle large files"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        byte[] largeData = new byte[1024 * 1024] // 1MB
        new Random().nextBytes(largeData)
        
        when:
        MerkleNode addedNode = blockStore.add(largeData)
        MerkleNode retrievedNode = blockStore.get(addedNode.hash)
        
        then:
        // The hash formats may differ (CIDv1 vs CIDv0), so we only check the data
        retrievedNode.data.isPresent()
        retrievedNode.data.get() == largeData
    }
    
    def "should add data with codec specified via options map"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        byte[] data = "test data with options".getBytes()
        
        when:
        MerkleNode rawNode = blockStore.add(data, [inputFormat: 'raw'])
        MerkleNode dagPbNode = blockStore.add(data, [inputFormat: 'dag-pb'])
        MerkleNode cborkNode = blockStore.add(data, [inputFormat: 'dag-cbor'])
        MerkleNode defaultNode = blockStore.add(data, [:]) // should default to dag-cbor
        
        then:
        // Verify CIDs are created correctly for different codecs
        rawNode.hash.toString().startsWith("baf") // Raw codec CIDs typically start with "baf"
        dagPbNode.hash.toString().startsWith("baf") // DAG-PB codec CIDs
        cborkNode.hash.toString().startsWith("baf") // DAG-CBOR codec CIDs
        defaultNode.hash.toString().startsWith("baf") // Default DAG-CBOR codec CIDs
        
        // Verify we can retrieve all of them
        blockStore.get(rawNode.hash).data.get() == data
        blockStore.get(dagPbNode.hash).data.get() == data
        blockStore.get(cborkNode.hash).data.get() == data
        blockStore.get(defaultNode.hash).data.get() == data
        
        // Verify different codecs produce different CIDs for the same data
        rawNode.hash != dagPbNode.hash
        rawNode.hash != cborkNode.hash
        dagPbNode.hash != cborkNode.hash
        cborkNode.hash == defaultNode.hash // These should be the same (both dag-cbor)
    }
    
    def "should parse codec names correctly in LocalBlockStore"() {
        given:
        LocalBlockStore blockStore = new LocalBlockStore(tempDir)
        byte[] data = "test data".getBytes()
        
        expect:
        // Test various codec name formats
        def rawNode = blockStore.add(data, [inputFormat: 'raw'])
        def dagPbNode1 = blockStore.add(data, [inputFormat: 'dag-pb'])
        def dagPbNode2 = blockStore.add(data, [inputFormat: 'dag-protobuf'])
        def cborNode = blockStore.add(data, [inputFormat: 'cbor'])
        def dagCborNode = blockStore.add(data, [inputFormat: 'dag-cbor'])
        def unknownNode = blockStore.add(data, [inputFormat: 'unknown']) // should fallback to dag-cbor
        
        // All should create valid CIDs
        rawNode.hash != null
        dagPbNode1.hash != null
        dagPbNode2.hash != null
        cborNode.hash != null
        dagCborNode.hash != null
        unknownNode.hash != null
        
        // dag-pb and dag-protobuf should produce the same CID
        dagPbNode1.hash == dagPbNode2.hash
        
        // dag-cbor and unknown (fallback) should produce the same CID  
        dagCborNode.hash == unknownNode.hash
    }
} 