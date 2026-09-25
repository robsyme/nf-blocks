package robsyme.cas.core

import groovy.transform.CompileStatic

/** One Store Log entry: `<rts>-<kind>-<cid>` decomposed (DESIGN.md §5). */
@CompileStatic
final class StoreLogEntry implements Comparable<StoreLogEntry> {

    /** The entry name as it appears in the store; also the watermark form. */
    final String name
    /** The 13-digit reverse timestamp, so lexicographic order is newest first. */
    final String reverseTs
    /** When the entry was written to this member. */
    final long writtenAtMillis
    final StoreLogKind kind
    /** The block this entry announces. */
    final Cid cid

    StoreLogEntry(String name, String reverseTs, long writtenAtMillis, StoreLogKind kind, Cid cid) {
        this.name = name
        this.reverseTs = reverseTs
        this.writtenAtMillis = writtenAtMillis
        this.kind = kind
        this.cid = cid
    }

    @Override
    int compareTo(StoreLogEntry other) { name <=> other.name }

    @Override
    String toString() { "StoreLogEntry[$name]" }

    @Override
    boolean equals(Object other) {
        return other instanceof StoreLogEntry && ((StoreLogEntry) other).name == name
    }

    @Override
    int hashCode() { name.hashCode() }
}
