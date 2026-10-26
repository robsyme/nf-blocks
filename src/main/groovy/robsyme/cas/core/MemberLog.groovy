package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * One member's Store Log, as the mark reads it: who it is, whether it is
 * swept, its entries, and the names of its entries of kinds this build does
 * not know (P4 ruling R8: a sweep lists them and refuses to apply).
 */
@Canonical
@CompileStatic
class MemberLog {
    String alias
    boolean writable
    List<StoreLogEntry> entries
    List<String> unknown = []

    /** The member's Store Log as one listing. */
    static MemberLog of(BlockStore member, boolean writable) {
        final StoreLog.Listing l = StoreLog.of(member).list()
        return new MemberLog(member.alias(), writable, l.entries, l.unknown)
    }

    /** The kinds of {@link #unknown}, sorted, each once. */
    List<String> unknownKinds() {
        final Set<String> kinds = new TreeSet<String>()
        for( String name : unknown ?: Collections.<String> emptyList() ) {
            final String k = StoreLog.unknownKindOf(name)
            if( k != null )
                kinds.add(k)
        }
        return new ArrayList<String>(kinds)
    }
}
