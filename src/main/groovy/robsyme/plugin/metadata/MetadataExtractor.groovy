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

package robsyme.plugin.metadata

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.script.WorkflowMetadata

/**
 * Extracts metadata from Nextflow Session, TaskRun, and other objects
 * to populate IPLD data structures.
 */
@Slf4j
@CompileStatic
class MetadataExtractor {

    /**
     * Extract workflow metadata from Session
     */
    static Map<String, Object> extractWorkflowMetadata(Session session) {
        def workflow = session.workflowMetadata

        def metadata = [
            name: workflow.scriptName ?: workflow.projectName ?: 'unknown',
            version: workflow.manifest?.version ?: 'unknown',
            repository: workflow.repository ?: workflow.manifest?.homePage ?: null,
            revision: workflow.revision ?: workflow.commitId ?: null,
            container_engine: workflow.containerEngine ?: null,
            command_line: workflow.commandLine ?: null
        ]

        return metadata.findAll { k, v -> v != null } as Map<String, Object>
    }

    /**
     * Extract execution metadata from Session
     */
    static Map<String, Object> extractExecutionMetadata(Session session) {
        def workflow = session.workflowMetadata

        def metadata = [
            nextflow_version: workflow.nextflow?.version?.toString() ?: 'unknown',
            started_at: workflow.start?.toString(),
            completed_at: workflow.complete?.toString(),
            duration_seconds: workflow.duration?.toMillis() ? workflow.duration.toMillis() / 1000 : null,
            status: workflow.success ? 'success' : 'failed',
            exit_code: workflow.exitStatus ?: 0,
            session_id: workflow.sessionId?.toString(),
            run_name: workflow.runName,
            user_name: workflow.userName,
            launch_dir: workflow.launchDir?.toString(),
            work_dir: workflow.workDir?.toString(),
            project_dir: workflow.projectDir?.toString(),
            resume: workflow.resume ?: false,
            error_message: workflow.errorMessage,
            error_report: workflow.errorReport
        ]

        return metadata.findAll { k, v -> v != null } as Map<String, Object>
    }

    /**
     * Extract process metadata from TaskRun
     */
    static Map<String, Object> extractProcessMetadata(TaskRun task) {
        if (!task) {
            return [:] as Map<String, Object>
        }

        def metadata = [
            process_name: task.processor?.name ?: task.name ?: 'unknown',
            task_name: task.name,
            container: task.container ?: null,
            cpus: task.config?.cpus ?: null,
            memory: task.config?.memory?.toString(),
            time: task.config?.time?.toString(),
            disk: task.config?.disk?.toString(),
            queue: task.config?.queue,
            executor: task.config?.executor,
            module: task.config?.module,
            conda: task.config?.conda,
            exit_status: task.exitStatus,
            attempt: task.config?.attempt ?: 1,
            scratch: task.config?.scratch?.toString()
        ]

        return metadata.findAll { k, v -> v != null } as Map<String, Object>
    }

    /**
     * Extract sample metadata from task inputs
     *
     * Looks for 'meta' Map in task inputs and recursively converts to simple types
     */
    static Map<String, Object> extractSampleMetadata(TaskRun task) {
        if (!task) {
            return [:] as Map<String, Object>
        }

        // Try to find 'meta' in task inputs
        def inputs = task.inputs
        if (!inputs) {
            return [:] as Map<String, Object>
        }

        // Look for a parameter named 'meta' or any Map that looks like metadata
        def metaValue = null

        for (entry in inputs.entrySet()) {
            def key = entry.key
            def value = entry.value

            // Check if parameter is named 'meta'
            if (key?.name == 'meta' && value instanceof Map) {
                metaValue = value
                break
            }

            // Check if value itself is a Map (could be tuple with meta)
            if (value instanceof Map && !value.isEmpty()) {
                metaValue = value
                break
            }

            // Check if value is a list/tuple containing a Map
            if (value instanceof List && value.size() > 0) {
                def firstItem = value[0]
                if (firstItem instanceof Map) {
                    metaValue = firstItem
                    break
                }
            }
        }

        if (metaValue instanceof Map) {
            return normalizeMetadata(metaValue)
        }

        return [:] as Map<String, Object>
    }

    /**
     * Normalize metadata to simple types (String, Number, Boolean, List, Map)
     * Recursively processes nested structures
     */
    static Map<String, Object> normalizeMetadata(Map<?, ?> meta) {
        def result = [:] as Map<String, Object>

        meta.each { key, value ->
            def normalizedValue = normalizeValue(value)
            if (normalizedValue != null) {
                result[key.toString()] = normalizedValue
            }
        }

        return result
    }

    /**
     * Normalize a single value to simple types
     */
    private static Object normalizeValue(Object value) {
        if (value == null) {
            return null
        }

        // Already simple types
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value
        }

        // Convert GString to String
        if (value instanceof GString) {
            return value.toString()
        }

        // Recursively handle Maps
        if (value instanceof Map) {
            return normalizeMetadata(value as Map)
        }

        // Recursively handle Lists
        if (value instanceof List || value instanceof Collection) {
            def list = value as Collection
            return list.collect { normalizeValue(it) }.findAll { it != null }
        }

        // Convert other types to String
        return value.toString()
    }

    /**
     * Extract project identifier from Session config
     */
    static String extractProjectId(Session session) {
        // Try various config locations for project ID
        def config = session.config

        try {
            // Check blocks.project
            if (config?.containsKey('blocks')) {
                def blocks = config.blocks
                if (blocks instanceof Map && blocks.containsKey('project')) {
                    return blocks.project.toString()
                }
            }

            // Check top-level project
            if (config?.containsKey('project')) {
                return config.project.toString()
            }

            // Check manifest.name
            if (config?.containsKey('manifest')) {
                def manifest = config.manifest
                if (manifest instanceof Map && manifest.containsKey('name')) {
                    return manifest.name.toString()
                }
            }
        } catch (Exception e) {
            // Ignore and use fallback
        }

        // Use workflow script name as fallback
        return session.workflowMetadata?.scriptName ?: 'default'
    }

    /**
     * Generate a unique run ID
     */
    static String generateRunId(Session session) {
        def workflow = session.workflowMetadata
        def timestamp = workflow.start?.format('yyyy-MM-dd-HHmmss') ?: 'unknown'
        def sessionId = workflow.sessionId?.toString()?.take(8) ?: 'unknown'

        return "run-${timestamp}-${sessionId}"
    }
}
