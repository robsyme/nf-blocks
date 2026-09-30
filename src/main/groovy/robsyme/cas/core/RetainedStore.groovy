package robsyme.cas.core

import groovy.transform.CompileStatic

/** A block store that knows where its member's retention objects live (as LoggedStore is for the Store Log). */
@CompileStatic
interface RetainedStore {
    RetentionStorage retentionStorage()
}
