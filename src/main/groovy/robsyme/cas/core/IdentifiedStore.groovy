package robsyme.cas.core

import groovy.transform.CompileStatic

/** A block store that knows where its member's `store-id` lives (as LoggedStore is for the Store Log). */
@CompileStatic
interface IdentifiedStore {
    StoreIdStorage storeIdStorage()
}
