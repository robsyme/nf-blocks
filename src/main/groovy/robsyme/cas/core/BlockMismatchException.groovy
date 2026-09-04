package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * Bytes offered for an address are not the bytes that address names, either
 * because they hash to something else or because there are not as many of
 * them as the caller announced. The block is never placed.
 */
@CompileStatic
class BlockMismatchException extends IOException {

    final Cid cid

    BlockMismatchException(Cid cid, String detail) {
        super("block $cid was not stored: $detail")
        this.cid = cid
    }
}
