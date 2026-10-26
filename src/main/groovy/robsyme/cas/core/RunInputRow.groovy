package robsyme.cas.core

import groovy.transform.CompileStatic

/** One row of `run_input` (P4 plan 1.3): one input of a run's InputSet. */
@CompileStatic
final class RunInputRow {

    final Cid completion
    /** "samplesheet", "param:<name>" or "task". */
    final String role
    /** The file's base name, or null. */
    final String name
    /** The content's address, or null when the run did not hash it. The store may not hold its bytes. */
    final Cid address
    final Long size
    /** The meta.id of the task input tuple that staged it, or null. */
    final String sample

    RunInputRow(Cid completion, String role, String name, Cid address, Long size, String sample) {
        this.completion = completion
        this.role = role
        this.name = name
        this.address = address
        this.size = size
        this.sample = sample
    }

    @Override
    String toString() { "RunInputRow[$role $name ($address) of $completion]" }
}
