package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/**
 * One item of one Output Collection, as nf-blocks:items finds it (decision 7
 * of the milestone 3 plan). Its occurrence is the Item Occurrence URI a
 * Selection request may list as a member (Put.groovy, entryOf).
 */
@CompileStatic
@EqualsAndHashCode
final class ItemHit implements Comparable<ItemHit> {

    final Cid collection
    final Cid item

    ItemHit(Cid collection, Cid item) {
        if( collection == null || item == null )
            throw new IllegalArgumentException('an item hit needs a collection and an item')
        this.collection = collection
        this.item = item
    }

    /** cas://<collection>/<item> */
    String occurrence() {
        return ItemOccurrence.PREFIX + collection + '/' + item
    }

    /** By item CID, then collection CID, the order Index.itemHitsByText answers in. */
    @Override
    int compareTo(ItemHit other) {
        final int byItem = item.compareTo(other.item)
        return byItem != 0 ? byItem : collection.compareTo(other.collection)
    }

    @Override
    String toString() { occurrence() }
}
