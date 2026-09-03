package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * One scalar leaf of an item's metadata view, as it lands in `item_attr`
 * (DESIGN.md §12).
 */
@CompileStatic
final class AttrRow {

    /** Dotted path from the metadata view root; array elements sit under the array's path. */
    final String path
    /** One of `string`, `int`, `float`, `bool`, `null`. */
    final String type
    /** The text form, or `sha256:<hex>` when truncated, or null for the `null` type. */
    final String value
    /** 1 when `value` is a digest standing in for a string past the cap, else 0. */
    final int truncated

    AttrRow(String path, String type, String value, int truncated) {
        this.path = path
        this.type = type
        this.value = value
        this.truncated = truncated
    }

    @Override
    String toString() { "AttrRow[$path $type ${truncated ? "$value (truncated)" : value}]" }

    @Override
    boolean equals(Object other) {
        if( !(other instanceof AttrRow) )
            return false
        final AttrRow that = (AttrRow) other
        return path == that.path && type == that.type && value == that.value && truncated == that.truncated
    }

    @Override
    int hashCode() { Objects.hash(path, type, value, truncated) }
}
