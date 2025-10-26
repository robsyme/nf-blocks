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

package robsyme.plugin.storage

import java.nio.file.Path

/**
 * Interface for content-addressed blob storage.
 *
 * Implementations can support various backends (S3, local filesystem, etc.)
 */
interface BlobStore {

    /**
     * Write a blob to storage by its content hash
     *
     * @param hash The content hash (CID) of the blob
     * @param data Input stream containing the blob data
     * @return true if written successfully
     */
    boolean writeBlob(String hash, InputStream data)

    /**
     * Write a blob from a file
     *
     * @param hash The content hash (CID) of the blob
     * @param file Path to the file to upload
     * @return true if written successfully
     */
    boolean writeBlob(String hash, Path file)

    /**
     * Write a Bao outboard file for a blob
     *
     * @param hash The content hash (CID) of the blob
     * @param outboard The outboard data
     * @return true if written successfully
     */
    boolean writeOutboard(String hash, byte[] outboard)

    /**
     * Read a blob from storage
     *
     * @param hash The content hash (CID) of the blob
     * @return Input stream of the blob data, or null if not found
     */
    InputStream readBlob(String hash)

    /**
     * Read a Bao outboard file
     *
     * @param hash The content hash (CID) of the blob
     * @return The outboard data, or null if not found
     */
    byte[] readOutboard(String hash)

    /**
     * Check if a blob exists in storage
     *
     * @param hash The content hash (CID) of the blob
     * @return true if the blob exists
     */
    boolean exists(String hash)

    /**
     * Delete a blob from storage
     *
     * @param hash The content hash (CID) of the blob
     * @return true if deleted successfully
     */
    boolean deleteBlob(String hash)

    /**
     * Get the size of a blob
     *
     * @param hash The content hash (CID) of the blob
     * @return The size in bytes, or -1 if not found
     */
    long getBlobSize(String hash)

    /**
     * Close any resources held by this store
     */
    void close()
}
