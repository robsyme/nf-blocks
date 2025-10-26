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

package robsyme.plugin.ref

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.plugin.ipld.Link
import robsyme.plugin.storage.RefStore

/**
 * Manages refs (mutable pointers to immutable content-addressed manifests).
 *
 * Refs follow a hierarchical structure:
 * - refs/workflows/{workflow-name}/latest - Latest run for a workflow
 * - refs/workflows/{workflow-name}/{branch} - Branch-specific refs
 * - refs/projects/{project-id}/latest - Latest run for a project
 * - refs/runs/{run-id} - Permanent ref for a specific run
 * - refs/samples/{sample-id}/latest - Latest run that processed a sample
 *
 * Supports Git-like history via parent links in manifests.
 */
@Slf4j
@CompileStatic
class RefManager {

    private final RefStore refStore

    RefManager(RefStore refStore) {
        this.refStore = refStore
    }

    /**
     * Get the current CID that a ref points to (for setting parent link)
     *
     * @param refPath The ref path (e.g., "workflows/rnaseq/latest")
     * @return The CID, or null if ref doesn't exist
     */
    String getCurrentCid(String refPath) {
        return refStore.readRef(refPath)
    }

    /**
     * Update a ref to point to a new manifest CID
     *
     * @param refPath The ref path
     * @param manifestCid The new manifest CID
     * @return true if updated successfully
     */
    boolean updateRef(String refPath, String manifestCid) {
        log.debug "Updating ref: $refPath -> $manifestCid"
        return refStore.writeRef(refPath, manifestCid)
    }

    /**
     * Update multiple refs atomically (as much as possible)
     *
     * @param updates Map of ref paths to manifest CIDs
     * @return Map of ref paths to update results (true/false)
     */
    Map<String, Boolean> updateRefs(Map<String, String> updates) {
        def results = [:] as Map<String, Boolean>

        updates.each { refPath, manifestCid ->
            results[refPath] = updateRef(refPath, manifestCid)
        }

        return results
    }

    /**
     * Update workflow ref and return parent CID (for linking)
     *
     * This is a convenience method that:
     * 1. Reads the current ref value (to use as parent)
     * 2. Updates the ref to the new manifest CID
     *
     * @param workflowName The workflow name
     * @param manifestCid The new manifest CID
     * @param branch The branch name (default: "latest")
     * @return Link to parent manifest, or null if no parent
     */
    Link updateWorkflowRef(String workflowName, String manifestCid, String branch = "latest") {
        def refPath = "workflows/${workflowName}/${branch}"

        // Read current value for parent link
        def currentCid = getCurrentCid(refPath)

        // Update ref
        updateRef(refPath, manifestCid)

        // Return parent link
        return currentCid ? Link.of(currentCid) : null
    }

    /**
     * Update project ref
     *
     * @param projectId The project identifier
     * @param manifestCid The new manifest CID
     * @return Link to parent manifest, or null if no parent
     */
    Link updateProjectRef(String projectId, String manifestCid) {
        def refPath = "projects/${projectId}/latest"

        // Read current value for parent link
        def currentCid = getCurrentCid(refPath)

        // Update ref
        updateRef(refPath, manifestCid)

        // Return parent link
        return currentCid ? Link.of(currentCid) : null
    }

    /**
     * Create a permanent ref for a specific run
     *
     * @param runId The run identifier
     * @param manifestCid The manifest CID
     * @return true if created successfully
     */
    boolean createRunRef(String runId, String manifestCid) {
        def refPath = "runs/${runId}"
        return updateRef(refPath, manifestCid)
    }

    /**
     * Update sample ref (tracks latest run that processed this sample)
     *
     * @param sampleId The sample identifier
     * @param manifestCid The manifest CID
     * @return true if updated successfully
     */
    boolean updateSampleRef(String sampleId, String manifestCid) {
        def refPath = "samples/${sampleId}/latest"
        return updateRef(refPath, manifestCid)
    }

    /**
     * List all refs under a prefix
     *
     * @param prefix The prefix (e.g., "workflows/" or "projects/")
     * @return List of ref paths
     */
    List<String> listRefs(String prefix) {
        return refStore.listRefs(prefix)
    }

    /**
     * Get ref history by following parent links
     *
     * Note: This requires fetching manifests from blob storage,
     * so it's not implemented here. The caller should use BlobStore
     * to fetch manifests and follow parent links.
     *
     * @param refPath The starting ref path
     * @param limit Maximum number of history entries
     * @return List of CIDs in history order (newest first)
     */
    List<String> getRefHistory(String refPath, int limit = 10) {
        List<String> history = []
        def currentCid = getCurrentCid(refPath)

        if (currentCid) {
            history << currentCid
        }

        // TODO: Fetch manifests and follow parent links
        // This requires BlobStore access to read manifest objects
        // For now, just return the current CID

        return history
    }

    /**
     * Delete a ref
     *
     * @param refPath The ref path to delete
     * @return true if deleted successfully
     */
    boolean deleteRef(String refPath) {
        log.debug "Deleting ref: $refPath"
        return refStore.deleteRef(refPath)
    }
}
