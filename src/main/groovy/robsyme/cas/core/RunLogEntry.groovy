package robsyme.cas.core

import groovy.transform.CompileStatic

/** One Run Log entry: `<rts>-<cid>` decomposed (DESIGN.md §5). */
@CompileStatic
final class RunLogEntry implements Comparable<RunLogEntry> {

    /** The entry name as it appears in the store; also the watermark form. */
    final String name
    /** The 13-digit reverse timestamp, so lexicographic order is newest first. */
    final String reverseTs
    final long finishedAtMillis
    /** The RunCompletion this entry announces. */
    final Cid cid

    RunLogEntry(String name, String reverseTs, long finishedAtMillis, Cid cid) {
        this.name = name
        this.reverseTs = reverseTs
        this.finishedAtMillis = finishedAtMillis
        this.cid = cid
    }

    @Override
    int compareTo(RunLogEntry other) { name <=> other.name }

    @Override
    String toString() { "RunLogEntry[$name]" }

    @Override
    boolean equals(Object other) {
        return other instanceof RunLogEntry && ((RunLogEntry) other).name == name
    }

    @Override
    int hashCode() { name.hashCode() }
}
