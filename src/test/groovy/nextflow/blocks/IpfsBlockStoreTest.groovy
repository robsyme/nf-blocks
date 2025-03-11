package nextflow.blocks

import io.ipfs.api.IPFS
import io.ipfs.api.MerkleNode
import io.ipfs.api.NamedStreamable
import io.ipfs.cid.Cid
import io.ipfs.multihash.Multihash
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.Base64
import java.util.NoSuchElementException
import spock.lang.Specification
import spock.lang.Unroll

class IpfsBlockStoreTest extends Specification {
    class TestFiles extends IPFS.Files {
        private final Map<String, byte[]> files = [:]
        
        TestFiles() {
            super(null)
        }

        @Override
        String write(String path, NamedStreamable data, boolean create, boolean parents) {
            if (files.containsKey(path) && !create) {
                throw new IOException("File already exists: ${path}")
            }
            files[path] = data.getContents()
            return "ok"
        }

        @Override
        byte[] read(String path) {
            if (!files.containsKey(path)) {
                throw new IOException("File not found: ${path}")
            }
            return files[path]
        }

        @Override
        List<Map> ls(String path) {
            def entries = files.findAll { it.key.startsWith(path) }.collect { k, v ->
                [
                    Name: k.tokenize('/')[-1],
                    Type: 0,
                    Size: v.length,
                    Hash: ""
                ]
            }
            return entries
        }

        @Override
        Map stat(String path) {
            if (!files.containsKey(path)) {
                throw new IOException("File not found: ${path}")
            }
            return [
                Hash: "QmNRQVNpQp8r2N56tMDUpYyLaqHaJeiV9m8Y66FcGM6F4g",  // Example hash
                Size: files[path].length,                                // Capitalized Size
                CumulativeSize: files[path].length + 58,                 // Added CumulativeSize
                Blocks: 1,                                               // Capitalized Blocks
                Type: "file"                                             // Capitalized Type
            ]
        }

        @Override
        String cp(String source, String dest, boolean parents) {
            if (!files.containsKey(source)) {
                throw new IOException("Source file not found: ${source}")
            }
            if (files.containsKey(dest)) {
                throw new IOException("""{"Message":"cp: cannot put node in path ${dest}: directory already has entry by that name","Code":0,"Type":"error"}""")
            }
            files[dest] = files[source].clone() // Use clone to avoid sharing the byte array
            return "ok"
        }

        @Override
        String rm(String path, boolean recursive, boolean force) {
            if (!files.containsKey(path)) {
                throw new IOException("File not found: ${path}")
            }
            files.remove(path)
            return "ok"
        }
    }

    class TestDag extends IPFS.Dag {
        private final Map<String, byte[]> blocks

        TestDag(Map<String, byte[]> blocks) {  // Take blocks map as constructor parameter
            super(null)
            this.blocks = blocks
        }

        @Override
        MerkleNode put(String inputFormat, byte[] object, String outputFormat) {
            def hash = MessageDigest.getInstance("SHA-256").digest(object)
            def multihash = new Multihash(Multihash.Type.sha2_256, hash)
            blocks[multihash.toBase58()] = object
            return new MerkleNode(multihash.toBase58())
        }

        @Override
        byte[] get(Cid cid) {
            def key = cid.hash.toBase58()
            if (!blocks.containsKey(key)) {
                throw new IOException("Block not found: ${key}")
            }
            return blocks[key]
        }
    }

    class TestIpfs extends IPFS {
        final TestFiles files
        final TestDag dag
        private final Map<String, byte[]> blocks = [:]  // Single blocks map shared between TestIpfs and TestDag

        TestIpfs() {
            super("", -1)  // Use invalid host/port to prevent real connection attempts
            this.files = new TestFiles()
            this.dag = new TestDag(blocks)
            // Set the files field in the parent class
            def filesField = IPFS.getDeclaredField("files")
            filesField.setAccessible(true)
            filesField.set(this, files)
            // Set the dag field in the parent class
            def dagField = IPFS.getDeclaredField("dag")
            dagField.setAccessible(true)
            dagField.set(this, dag)
        }

        @Override
        String version() {
            return "0.11.0"
        }

        @Override
        byte[] get(Multihash hash) {
            def key = hash.toBase58()
            if (!blocks.containsKey(key)) {
                throw new IOException("Block not found: ${key}")
            }
            return blocks[key]
        }

        @Override
        List<MerkleNode> add(NamedStreamable data) {
            def content = data.getContents()
            def hash = MessageDigest.getInstance("SHA-256").digest(content)
            def multihash = new Multihash(Multihash.Type.sha2_256, hash)
            blocks[multihash.toBase58()] = content
            return [new MerkleNode(multihash.toBase58())]
        }
    }

    def "should connect to IPFS node"() {
        given:
        def ipfs = new TestIpfs()
        
        when:
        def store = new IpfsBlockStore(ipfs, true)  // Use mocked IPFS and skip validation
        
        then:
        noExceptionThrown()
    }

    def "should put and get data with mocked IPFS"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def data = "test data".bytes
        
        when:
        def node = store.add(data, [:])
        def retrieved = store.get(node.hash)
        
        then:
        retrieved.data.get() == data
    }

    def "should put and get data with explicit codec"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def data = "test data with codec".bytes
        
        when:
        def node = store.add(data, Cid.Codec.DagCbor)
        def retrieved = store.get(node.hash)
        
        then:
        retrieved.data.get() == data
    }

    def "should handle files operations with mocked IPFS"() {
        given:
        def ipfs = new TestIpfs()
        def store = new IpfsBlockStore(ipfs, true)
        def data = "test content".bytes
        
        when: "writing a file"
        def writeResult = store.write("/test.txt", data, [:])
        
        then: "should be able to read it back"
        store.read("/test.txt", [:]) == data
        
        when: "listing files"
        def ls = store.ls("/", [:])
        
        then: "should see our file"
        ls.find { it.Name == "test.txt" }
        
        when: "getting stats"
        def stats = store.stat("/test.txt", [:])
        
        then: "should have correct size"
        stats.Size == data.length
    
        when: "copying file"
        try {
            store.rm("/copy.txt", false, false) // Try to remove if it exists
        } catch (RuntimeException ignored) {
            // Ignore if file doesn't exist
        }
        store.cp("/test.txt", "/copy.txt", false)
        
        then: "both files should exist"
        store.read("/test.txt", [:]) == data
        store.read("/copy.txt", [:]) == data
        
        when: "removing file"
        store.rm("/test.txt", false, false)
        and: "trying to read the removed file"
        store.read("/test.txt", [:])
        
        then: "should throw RuntimeException with IPFS error message"
        def e = thrown(RuntimeException)
        e.message.contains('IOException contacting IPFS daemon') && 
        e.message.contains('File not found: /test.txt')
    }
} 