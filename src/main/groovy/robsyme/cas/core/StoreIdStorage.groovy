package robsyme.cas.core

import groovy.transform.CompileStatic

/** A member's `store-id` (DESIGN.md §20 decision 1): not a block, written once, never rewritten. */
@CompileStatic
interface StoreIdStorage {
    /** The member's id, or null when there is none or it does not parse. */
    String read()
    /** Writes `id` only when the member has none; the id there afterwards, ours or the winner's. */
    String create(String id)
    String describe()
}
