/*
 * Copyright 2025, Rob Syme (rob.syme@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin

import robsyme.plugin.config.BlocksConfig
import robsyme.plugin.ipld.CID
import robsyme.plugin.storage.StorageBackendFactory
import robsyme.plugin.storage.impl.FileBlobStore
import robsyme.plugin.storage.impl.FileRefStore
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

/**
 * Test core components of the nf-blocks plugin
 */
class CoreComponentsTest extends Specification {

    @TempDir
    Path tempDir

    def "test configuration parsing"() {
        given:
        def configMap = [
            enabled: true,
            storage: [
                uri: 'blocks+file:///tmp/blocks'
            ],
            readLocations: [
                'blocks+file:///tmp/cache',
                'blocks+file:///tmp/blocks'
            ],
            refs: [
                uri: 'refs+file:///tmp/refs'
            ]
        ]

        when:
        def config = BlocksConfig.parse(configMap)

        then:
        config.enabled == true
        config.storage.uri == 'blocks+file:///tmp/blocks'
        config.refs.uri == 'refs+file:///tmp/refs'
        config.readLocations.size() == 2
    }

    def "test configuration validation"() {
        given:
        def configMap = [
            enabled: true,
            storage: [
                uri: 'blocks+file:///tmp/blocks'
            ],
            refs: [
                uri: 'refs+file:///tmp/refs'
            ]
        ]

        when:
        def config = BlocksConfig.parse(configMap)
        config.validate()

        then:
        noExceptionThrown()
    }

    def "test configuration validation fails without storage URI"() {
        given:
        def configMap = [
            enabled: true,
            refs: [
                uri: 'refs+file:///tmp/refs'
            ]
        ]

        when:
        def config = BlocksConfig.parse(configMap)
        config.validate()

        then:
        thrown(IllegalArgumentException)
    }

    def "test CID generation from hash"() {
        given:
        def hash = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"

        when:
        def cid = CID.fromHash(hash)

        then:
        cid != null
        cid.startsWith('b')  // base32 multibase prefix
        cid.length() > 10
    }

    def "test CID decode"() {
        given:
        def hash = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        def cid = CID.fromHash(hash)

        when:
        def decoded = CID.decode(cid)

        then:
        decoded.version == 1
        decoded.hash == hash
    }

    def "test CID validation"() {
        given:
        def hash = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        def validCid = CID.fromHash(hash)

        expect:
        CID.isValid(validCid) == true
        CID.isValid("invalid") == false
        CID.isValid("") == false
    }

    def "test FileBlobStore creation"() {
        given:
        def uri = "blocks+file://${tempDir}"

        when:
        def store = StorageBackendFactory.createBlobStore(uri)

        then:
        store instanceof FileBlobStore
    }

    def "test FileRefStore creation"() {
        given:
        def uri = "refs+file://${tempDir}"

        when:
        def store = StorageBackendFactory.createRefStore(uri)

        then:
        store instanceof FileRefStore
    }

    def "test FileBlobStore write and read"() {
        given:
        def store = new FileBlobStore(tempDir)
        def hash = "test-hash-123"
        def testData = "Hello, World!".bytes

        when:
        def written = store.writeBlob(hash, new ByteArrayInputStream(testData))
        def exists = store.exists(hash)
        def inputStream = store.readBlob(hash)
        def readData = inputStream?.bytes

        then:
        written == true
        exists == true
        readData == testData

        cleanup:
        inputStream?.close()
        store.close()
    }

    def "test FileBlobStore with outboard"() {
        given:
        def store = new FileBlobStore(tempDir)
        def hash = "test-hash-456"
        def testData = "Test content".bytes
        def outboardData = "Mock outboard".bytes

        when:
        store.writeBlob(hash, new ByteArrayInputStream(testData))
        store.writeOutboard(hash, outboardData)
        def retrievedOutboard = store.readOutboard(hash)

        then:
        retrievedOutboard == outboardData

        cleanup:
        store.close()
    }

    def "test FileRefStore write and read"() {
        given:
        def store = new FileRefStore(tempDir)
        def refPath = "workflows/test/latest"
        def cid = "bafkr4iabcdef123456"

        when:
        def written = store.writeRef(refPath, cid)
        def readCid = store.readRef(refPath)
        def exists = store.exists(refPath)

        then:
        written == true
        readCid == cid
        exists == true

        cleanup:
        store.close()
    }

    def "test FileRefStore list refs"() {
        given:
        def store = new FileRefStore(tempDir)
        store.writeRef("workflows/test1/latest", "cid1")
        store.writeRef("workflows/test2/latest", "cid2")
        store.writeRef("projects/proj1/latest", "cid3")

        when:
        def allRefs = store.listRefs("")
        def workflowRefs = store.listRefs("workflows")

        then:
        allRefs.size() == 3
        workflowRefs.size() == 2
        workflowRefs.every { it.startsWith("workflows") }

        cleanup:
        store.close()
    }

    def "test FileBlobStore delete"() {
        given:
        def store = new FileBlobStore(tempDir)
        def hash = "test-hash-delete"
        def testData = "Delete me".bytes

        when:
        store.writeBlob(hash, new ByteArrayInputStream(testData))
        def existsBefore = store.exists(hash)
        def deleted = store.deleteBlob(hash)
        def existsAfter = store.exists(hash)

        then:
        existsBefore == true
        deleted == true
        existsAfter == false

        cleanup:
        store.close()
    }

    def "test FileRefStore delete"() {
        given:
        def store = new FileRefStore(tempDir)
        def refPath = "workflows/to-delete/latest"

        when:
        store.writeRef(refPath, "cid123")
        def existsBefore = store.exists(refPath)
        def deleted = store.deleteRef(refPath)
        def existsAfter = store.exists(refPath)

        then:
        existsBefore == true
        deleted == true
        existsAfter == false

        cleanup:
        store.close()
    }

    def "test FileBlobStore size"() {
        given:
        def store = new FileBlobStore(tempDir)
        def hash = "test-hash-size"
        def testData = "This is some test data".bytes

        when:
        store.writeBlob(hash, new ByteArrayInputStream(testData))
        def size = store.getBlobSize(hash)

        then:
        size == testData.length

        cleanup:
        store.close()
    }
}
