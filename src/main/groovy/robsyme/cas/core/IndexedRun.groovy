package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * One row of `run` (DESIGN.md §12), as {@link Prune} and the CLI verbs need
 * it: the run's completion address, its pipeline, run name, status and
 * whether it finished cleanly.
 */
@Canonical
@CompileStatic
class IndexedRun {
    Cid completion
    String pipeline
    String runName
    String status
    boolean possiblyIncomplete
    String finishedAt

    boolean isSuccessful() { status == 'succeeded' && !possiblyIncomplete }
}
