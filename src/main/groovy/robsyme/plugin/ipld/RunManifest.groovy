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

package robsyme.plugin.ipld

import groovy.transform.CompileStatic

/**
 * IPLD manifest for a complete Nextflow run.
 *
 * Contains:
 * - Run identification and timing
 * - Workflow information
 * - Links to all output metadata objects (preserving path structure!)
 * - Link to parent run (Git-like history)
 * - Execution information
 */
@CompileStatic
class RunManifest {
    /**
     * Type identifier for IPLD
     */
    String type = "NextflowRun"

    /**
     * Unique run identifier
     */
    String runId

    /**
     * Run timestamp (ISO 8601)
     */
    String timestamp

    /**
     * Workflow information
     */
    WorkflowInfo workflow

    /**
     * Link to parameters object
     */
    Link parameters

    /**
     * Map of output paths to their metadata CIDs
     * Preserves the output structure from the Nextflow output DSL
     * Example: {"step1/sample1.bam": Link(...), "step2/results.csv": Link(...)}
     */
    Map<String, Link> outputs

    /**
     * Link to parent run manifest (for Git-like history)
     */
    Link parent

    /**
     * Execution information
     */
    ExecutionInfo execution

    /**
     * Project identifier
     */
    String project

    /**
     * User who ran the workflow
     */
    String user

    /**
     * Convert to a map for serialization
     */
    Map<String, Object> toMap() {
        def map = [
            '@type': type,
            'run_id': runId,
            'timestamp': timestamp,
            'workflow': workflow?.toMap(),
            'project': project,
            'user': user
        ]

        if (parameters) {
            map['parameters'] = parameters.toMap()
        }

        if (outputs) {
            map['outputs'] = outputs.collectEntries { path, link ->
                [(path): link.toMap()]
            }
        }

        if (parent) {
            map['parent'] = parent.toMap()
        }

        if (execution) {
            map['execution'] = execution.toMap()
        }

        return map
    }

    /**
     * Create from a map (for deserialization)
     */
    static RunManifest fromMap(Map<String, Object> map) {
        def manifest = new RunManifest()
        manifest.type = map['@type'] as String ?: "NextflowRun"
        manifest.runId = map['run_id'] as String
        manifest.timestamp = map['timestamp'] as String
        manifest.project = map['project'] as String
        manifest.user = map['user'] as String

        if (map['workflow']) {
            manifest.workflow = WorkflowInfo.fromMap(map['workflow'] as Map<String, Object>)
        }

        if (map['parameters']) {
            manifest.parameters = Link.fromMap(map['parameters'] as Map<String, String>)
        }

        if (map['outputs']) {
            manifest.outputs = (map['outputs'] as Map<String, Object>).collectEntries { path, linkMap ->
                [(path): Link.fromMap(linkMap as Map<String, String>)]
            } as Map<String, Link>
        }

        if (map['parent']) {
            manifest.parent = Link.fromMap(map['parent'] as Map<String, String>)
        }

        if (map['execution']) {
            manifest.execution = ExecutionInfo.fromMap(map['execution'] as Map<String, Object>)
        }

        return manifest
    }

    /**
     * Workflow information
     */
    @CompileStatic
    static class WorkflowInfo {
        String name
        String version
        String repository
        String revision
        Link definition

        Map<String, Object> toMap() {
            Map<String, Object> map = [
                'name': name,
                'version': version,
                'repository': repository,
                'revision': revision
            ] as Map<String, Object>

            if (definition) {
                map['definition'] = definition.toMap() as Object
            }

            return map
        }

        static WorkflowInfo fromMap(Map<String, Object> map) {
            def info = new WorkflowInfo()
            info.name = map['name'] as String
            info.version = map['version'] as String
            info.repository = map['repository'] as String
            info.revision = map['revision'] as String

            if (map['definition']) {
                info.definition = Link.fromMap(map['definition'] as Map<String, String>)
            }

            return info
        }
    }

    /**
     * Execution information
     */
    @CompileStatic
    static class ExecutionInfo {
        String startedAt
        String completedAt
        Long durationSeconds
        String status
        Integer exitCode
        String executor
        String computeEnvironment
        String nextflowVersion
        Double totalCpuHours
        Double totalMemoryGbHours
        Integer peakRunningTasks
        Link resumedFrom
        List<String> cachedProcesses
        List<String> recomputedProcesses

        Map<String, Object> toMap() {
            Map<String, Object> map = [
                'started_at': startedAt,
                'completed_at': completedAt,
                'duration_seconds': durationSeconds,
                'status': status,
                'exit_code': exitCode,
                'executor': executor,
                'compute_environment': computeEnvironment,
                'nextflow_version': nextflowVersion
            ] as Map<String, Object>

            if (totalCpuHours != null) {
                map['total_cpu_hours'] = totalCpuHours
            }

            if (totalMemoryGbHours != null) {
                map['total_memory_gb_hours'] = totalMemoryGbHours
            }

            if (peakRunningTasks != null) {
                map['peak_running_tasks'] = peakRunningTasks
            }

            if (resumedFrom) {
                map['resumed_from'] = resumedFrom.toMap()
            }

            if (cachedProcesses) {
                map['cached_processes'] = cachedProcesses as Object
            }

            if (recomputedProcesses) {
                map['recomputed_processes'] = recomputedProcesses as Object
            }

            return map
        }

        static ExecutionInfo fromMap(Map<String, Object> map) {
            def info = new ExecutionInfo()
            info.startedAt = map['started_at'] as String
            info.completedAt = map['completed_at'] as String
            info.durationSeconds = map['duration_seconds'] as Long
            info.status = map['status'] as String
            info.exitCode = map['exit_code'] as Integer
            info.executor = map['executor'] as String
            info.computeEnvironment = map['compute_environment'] as String
            info.nextflowVersion = map['nextflow_version'] as String
            info.totalCpuHours = map['total_cpu_hours'] as Double
            info.totalMemoryGbHours = map['total_memory_gb_hours'] as Double
            info.peakRunningTasks = map['peak_running_tasks'] as Integer
            info.cachedProcesses = map['cached_processes'] as List<String>
            info.recomputedProcesses = map['recomputed_processes'] as List<String>

            if (map['resumed_from']) {
                info.resumedFrom = Link.fromMap(map['resumed_from'] as Map<String, String>)
            }

            return info
        }
    }
}
