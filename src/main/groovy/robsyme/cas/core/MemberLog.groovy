package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** One member's Store Log, as the mark reads it: who it is, whether it is swept, and its entries. */
@Canonical
@CompileStatic
class MemberLog {
    String alias
    boolean writable
    List<StoreLogEntry> entries
}
