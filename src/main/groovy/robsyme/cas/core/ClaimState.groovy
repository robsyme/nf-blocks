package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The current state of one subject from its Claims (block explorer spec
 * section 8; decision 5 of the milestone 2 plan), including the `retain`
 * group and the `pin` set (ticket 21). Current = the Claims no
 * Claim in the set supersedes. They group by attribute, delete and del (no
 * attribute) forming the deletion group; a group of more than one is a
 * conflict, surfaced in claim-address order and never resolved here.
 * web/src/claims.js implements the same rules; web/test/fixtures/claim-vectors.json pins both.
 */
@CompileStatic
final class ClaimState {

    static final String NONE = 'none'
    static final String DELETED = 'deleted'
    static final String CONFLICTED = 'conflicted'
    static final String RELEASED = 'released'

    @Canonical
    @CompileStatic
    static final class Row {
        String cid
        String verb
        String attribute
        Object value
        List<String> supersedes
    }

    final List<Row> current
    final List<String> names
    final List<String> nameClaims
    final boolean nameConflicted
    final String deletion
    final List<String> deletionClaims
    final String retain
    final List<String> retainClaims
    final List<String> pinClaims
    final List<String> pinNotes
    private final Set<String> conflictedGroups

    private ClaimState(List<Row> current, Set<String> conflictedGroups) {
        this.current = Collections.unmodifiableList(current)
        this.conflictedGroups = conflictedGroups
        final List<Row> nameGroup = current.findAll { Row r -> r.attribute == Claim.NAME }
        this.nameClaims = Collections.unmodifiableList(nameGroup*.cid)
        this.names = Collections.unmodifiableList(nameGroup.findAll { Row r -> r.verb == Claim.SET }.collect { Row r -> String.valueOf(r.value) })
        this.nameConflicted = nameGroup.size() > 1
        final List<Row> deletionGroup = current.findAll { Row r -> r.attribute == null && (r.verb == Claim.DELETE || r.verb == Claim.DEL) }
        this.deletionClaims = Collections.unmodifiableList(deletionGroup*.cid)
        this.deletion = deletionGroup.size() > 1 ? CONFLICTED
            : deletionGroup.size() == 1 && deletionGroup[0].verb == Claim.DELETE ? DELETED
            : NONE
        final List<Row> retainGroup = current.findAll { Row r -> r.attribute == Claim.RETAIN }
        this.retainClaims = Collections.unmodifiableList(retainGroup*.cid)
        // Ticket 21 answers 1 and 4: only one clean `set retain "lineage"` releases; a conflict keeps content.
        this.retain = retainGroup.size() > 1 ? CONFLICTED
            : retainGroup.size() == 1 && retainGroup[0].verb == Claim.SET && retainGroup[0].value == Claim.LINEAGE ? RELEASED
            : NONE
        final List<Row> pins = current.findAll { Row r -> r.attribute == Claim.PIN && r.verb == Claim.ADD }
        this.pinClaims = Collections.unmodifiableList(pins*.cid)
        this.pinNotes = Collections.unmodifiableList(pins.collect { Row r -> String.valueOf(r.value) })
    }

    static ClaimState of(Collection<Row> claims) {
        final Set<String> superseded = new HashSet<String>()
        for( Row row : claims )
            superseded.addAll(row.supersedes ?: Collections.<String> emptyList())
        // Ticket 21 answer 2: a group that holds an `add` is a set; its current Claims never conflict.
        final Set<String> addGroups = new HashSet<String>()
        for( Row row : claims )
            if( row.verb == Claim.ADD )
                addGroups.add(groupOf(row))
        final List<Row> current = new ArrayList<Row>(claims.findAll { Row r -> !superseded.contains(r.cid) })
        current.sort { Row a, Row b -> a.cid <=> b.cid }
        final Map<String, Integer> sizes = new HashMap<String, Integer>()
        for( Row row : current )
            sizes.merge(groupOf(row), 1, { Integer a, Integer b -> a + b })
        final Set<String> conflicted = sizes.findAll { String k, Integer n -> n > 1 && !addGroups.contains(k) }.keySet()
        return new ClaimState(current, new HashSet<String>(conflicted))
    }

    /** The attribute, or a marker for the deletion group (attribute null). */
    private static String groupOf(Row row) {
        return row.attribute == null ? '\u0000deletion' : row.attribute
    }

    boolean isHidden() { deletion == DELETED }

    boolean isReleased() { retain == RELEASED }

    boolean isPinned() { !pinClaims.isEmpty() }

    /** One entry per current Claim, for claim_current. */
    List<Map<String, Object>> currentRows() {
        return current.collect { Row r ->
            [cid: r.cid, attribute: r.attribute, value: r.value, conflicted: conflictedGroups.contains(groupOf(r))] as Map<String, Object>
        }
    }
}
