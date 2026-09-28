package robsyme.cas.core

import groovy.transform.CompileStatic

/** A block store that knows where its member's Store Log lives (DESIGN.md §5). */
@CompileStatic
interface LoggedStore {
    StoreLogStorage storeLogStorage()
}
