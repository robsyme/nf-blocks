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

/**
 * Interface for ref (reference) storage.
 *
 * Refs are mutable pointers to immutable content-addressed manifests.
 * Similar to Git refs, they follow a hierarchical path structure:
 * - refs/workflows/{name}/latest
 * - refs/projects/{id}/latest
 * - refs/runs/{run-id}
 */
interface RefStore {

    /**
     * Write a ref (atomic update)
     *
     * @param refPath The ref path (e.g., "workflows/rnaseq/latest")
     * @param cid The CID to point to
     * @return true if written successfully
     */
    boolean writeRef(String refPath, String cid)

    /**
     * Read a ref
     *
     * @param refPath The ref path (e.g., "workflows/rnaseq/latest")
     * @return The CID the ref points to, or null if not found
     */
    String readRef(String refPath)

    /**
     * Check if a ref exists
     *
     * @param refPath The ref path
     * @return true if the ref exists
     */
    boolean exists(String refPath)

    /**
     * List all refs under a prefix
     *
     * @param prefix The prefix to search under (e.g., "workflows/")
     * @return List of ref paths
     */
    List<String> listRefs(String prefix)

    /**
     * Delete a ref
     *
     * @param refPath The ref path
     * @return true if deleted successfully
     */
    boolean deleteRef(String refPath)

    /**
     * Close any resources held by this store
     */
    void close()
}
