package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * One item as it appeared in one run's output (glossary: Item Occurrence;
 * DESIGN.md §7): cas://<OutputCollection cid>/<OutputItem cid>[/<leaf name>].
 * Parsing checks only the shape; whether the item is in the collection's
 * items is for the reader holding the collection to check.
 */
@Canonical
@CompileStatic
final class ItemOccurrence {

    static final String PREFIX = 'cas://'

    final Cid collection
    final Cid item
    final String leaf

    /** The occurrence this text names, or null when it is not of that shape. */
    static ItemOccurrence parse(String text) {
        if( text == null || !text.startsWith(PREFIX) )
            return null
        final String[] parts = text.substring(PREFIX.length()).split('/', -1)
        if( parts.length < 2 || parts.length > 3 )
            return null
        if( !Cid.isCid(parts[0]) || !Cid.isCid(parts[1]) )
            return null
        final Cid collection = Cid.parse(parts[0])
        final Cid item = Cid.parse(parts[1])
        if( !collection.isDagCbor() || !item.isDagCbor() )
            return null
        if( parts.length == 3 && parts[2].isEmpty() )
            return null
        return new ItemOccurrence(collection, item, parts.length == 3 ? parts[2] : null)
    }

    @Override
    String toString() {
        return PREFIX + collection + '/' + item + (leaf == null ? '' : '/' + leaf)
    }
}
