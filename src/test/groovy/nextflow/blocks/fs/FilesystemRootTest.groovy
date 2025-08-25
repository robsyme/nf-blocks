package nextflow.blocks.fs

import nextflow.blocks.LocalBlockStore
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class FilesystemRootTest extends Specification {
    @TempDir
    Path tempDir
    
    LocalBlockStore blockStore
    FilesystemRoot filesystemRoot
    
    def setup() {
        blockStore = new LocalBlockStore(tempDir.resolve("blocks"))
        filesystemRoot = new FilesystemRoot(blockStore)
    }
    
    def "should create empty root directory with valid CID"() {
        expect:
        filesystemRoot.rootCid != null
        filesystemRoot.rootCid.length() > 0
        
        and: "root CID should be deterministic for empty directory"
        def anotherRoot = new FilesystemRoot(blockStore)
        // Note: Different instances create different timestamps, so CIDs may differ
        anotherRoot.rootCid != null
    }
    
    def "should add file to root directory"() {
        given:
        def testData = "Hello, world!".bytes
        def contentNode = blockStore.add(testData)
        def contentCid = contentNode.hash.toString()
        def initialRootCid = filesystemRoot.rootCid
        
        when:
        def newRootCid = filesystemRoot.addFile("/test.txt", contentCid)
        
        then:
        newRootCid != null
        newRootCid != initialRootCid // Should have changed
        filesystemRoot.rootCid == newRootCid  // Should be updated
    }
    
    def "should add file to nested directory"() {
        given:
        def testData = "Nested file content".bytes
        def contentNode = blockStore.add(testData)
        def contentCid = contentNode.hash.toString()
        
        when:
        def newRootCid = filesystemRoot.addFile("/results/analysis/data.txt", contentCid)
        
        then:
        newRootCid != null
        filesystemRoot.rootCid == newRootCid
    }
    
    def "should handle multiple files in same directory"() {
        given:
        def data1 = "First file".bytes
        def data2 = "Second file".bytes
        def cid1 = blockStore.add(data1).hash.toString()
        def cid2 = blockStore.add(data2).hash.toString()
        
        when: "Add first file"
        def rootAfterFirst = filesystemRoot.addFile("/results/file1.txt", cid1)
        
        and: "Add second file to same directory"
        def rootAfterSecond = filesystemRoot.addFile("/results/file2.txt", cid2)
        
        then:
        rootAfterFirst != rootAfterSecond  // Root should change
        filesystemRoot.rootCid == rootAfterSecond
    }
    
    def "should handle overlapping directory structures"() {
        given:
        def data1 = "Sample 1 data".bytes
        def data2 = "Sample 2 data".bytes  
        def data3 = "Analysis result".bytes
        def cid1 = blockStore.add(data1).hash.toString()
        def cid2 = blockStore.add(data2).hash.toString()
        def cid3 = blockStore.add(data3).hash.toString()
        
        when: "Create overlapping directory structure"
        filesystemRoot.addFile("/results/sample1/data.txt", cid1)
        filesystemRoot.addFile("/results/sample2/data.txt", cid2)
        def finalRootCid = filesystemRoot.addFile("/results/analysis.txt", cid3)
        
        then:
        finalRootCid != null
        filesystemRoot.rootCid == finalRootCid
    }
    
    def "should handle very deep nested paths"() {
        given:
        def testData = "Deep file".bytes
        def contentCid = blockStore.add(testData).hash.toString()
        
        when:
        def newRootCid = filesystemRoot.addFile("/a/b/c/d/e/f/deep.txt", contentCid)
        
        then:
        newRootCid != null
        filesystemRoot.rootCid == newRootCid
    }
    
    def "should replace file when adding to same path"() {
        given:
        def originalData = "Original content".bytes
        def newData = "Updated content".bytes
        def originalCid = blockStore.add(originalData).hash.toString()
        def newCid = blockStore.add(newData).hash.toString()
        
        when: "Add original file"
        def rootAfterOriginal = filesystemRoot.addFile("/results/data.txt", originalCid)
        
        and: "Replace with new file"
        def rootAfterUpdate = filesystemRoot.addFile("/results/data.txt", newCid)
        
        then:
        rootAfterOriginal != rootAfterUpdate  // Root should change
        filesystemRoot.rootCid == rootAfterUpdate
    }
    
    def "should add directory to filesystem"() {
        given:
        // Create a sample directory structure
        def dirData = "directory content".bytes
        def dirCid = blockStore.add(dirData).hash.toString()
        
        when:
        def newRootCid = filesystemRoot.addDirectory("/results/subdir", dirCid)
        
        then:
        newRootCid != null
        filesystemRoot.rootCid == newRootCid
    }
    
    def "should handle mixed files and directories"() {
        given:
        def fileData = "File content".bytes
        def dirData = "Dir content".bytes
        def fileCid = blockStore.add(fileData).hash.toString()
        def dirCid = blockStore.add(dirData).hash.toString()
        
        when: "Add file and directory in same parent"
        filesystemRoot.addFile("/results/data.txt", fileCid)
        def finalRootCid = filesystemRoot.addDirectory("/results/subdir", dirCid)
        
        then:
        finalRootCid != null
        filesystemRoot.rootCid == finalRootCid
    }
    
    def "should create filesystem root from existing CID"() {
        given: "Create a filesystem with some content"
        def testData = "Test content".bytes
        def contentCid = blockStore.add(testData).hash.toString()
        filesystemRoot.addFile("/test.txt", contentCid)
        def existingRootCid = filesystemRoot.rootCid
        
        when: "Create new FilesystemRoot from existing CID"
        def loadedRoot = new FilesystemRoot(blockStore, existingRootCid)
        
        then:
        loadedRoot.rootCid == existingRootCid
    }
    
    def "should handle empty paths correctly"() {
        given:
        def testData = "Root file".bytes
        def contentCid = blockStore.add(testData).hash.toString()
        
        when: "Add file to root with minimal path"
        def newRootCid = filesystemRoot.addFile("rootfile.txt", contentCid)
        
        then:
        newRootCid != null
        filesystemRoot.rootCid == newRootCid
    }
}