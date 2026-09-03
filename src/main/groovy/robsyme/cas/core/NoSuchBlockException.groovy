package robsyme.cas.core

import groovy.transform.CompileStatic

/** The store was asked for a block it does not hold. */
@CompileStatic
class NoSuchBlockException extends IOException {

    final Cid cid

    NoSuchBlockException(Cid cid, String where) {
        super("no block $cid in store '$where'")
        this.cid = cid
    }
}
