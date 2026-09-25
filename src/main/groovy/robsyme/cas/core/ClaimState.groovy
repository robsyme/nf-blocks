package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The current state of one subject from its Claims (block explorer spec
 * section 8; decision 5 of the milestone 2 plan). Current = the Claims no
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
    }

    static ClaimState of(Collection<Row> claims) {
        final Set<String> superseded = new HashSet<String>()
        for( Row row : claims )
            superseded.addAll(row.supersedes ?: Collections.<String> emptyList())
        final List<Row> current = new ArrayList<Row>(claims.findAll { Row r -> !superseded.contains(r.cid) })
        current.sort { Row a, Row b -> a.cid <=> b.cid }
        final Map<String, Integer> sizes = new HashMap<String, Integer>()
        for( Row row : current )
            sizes.merge(groupOf(row), 1, { Integer a, Integer b -> a + b })
        final Set<String> conflicted = sizes.findAll { String k, Integer n -> n > 1 }.keySet()
        return new ClaimState(current, new HashSet<String>(conflicted))
    }

    /** The attribute, or a marker for the deletion group (attribute null). */
    private static String groupOf(Row row) {
        return row.attribute == null ? '\u0000deletion' : row.attribute
    }

    boolean isHidden() { deletion == DELETED }

    /** One entry per current Claim, for claim_current. */
    List<Map<String, Object>> currentRows() {
        return current.collect { Row r ->
            [cid: r.cid, attribute: r.attribute, value: r.value, conflicted: conflictedGroups.contains(groupOf(r))] as Map<String, Object>
        }
    }
}
