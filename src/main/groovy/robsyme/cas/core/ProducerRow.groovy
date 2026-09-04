package robsyme.cas.core

import groovy.transform.CompileStatic

/** One row of `producer` (DESIGN.md §12): which item of which run carries a content address. */
@CompileStatic
final class ProducerRow {

    final Cid contentCid
    final Cid itemCid
    final Cid collectionCid
    final Cid completionCid
    /** The Leaf name: the file name the content was published under. */
    final String filename

    ProducerRow(Cid contentCid, Cid itemCid, Cid collectionCid, Cid completionCid, String filename) {
        this.contentCid = contentCid
        this.itemCid = itemCid
        this.collectionCid = collectionCid
        this.completionCid = completionCid
        this.filename = filename
    }

    @Override
    String toString() { "ProducerRow[$contentCid as $filename in $itemCid of $completionCid]" }
}
