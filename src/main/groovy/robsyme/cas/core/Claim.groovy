package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * A statement about a subject (lineage spec §5, block explorer spec section 8):
 * rename is `set name`, delete is `delete`, undo is `del` superseding the
 * delete. Claims are blocks, not index rows, so they travel. The timestamp is
 * advisory and comes from the request, so a retried request is the same block.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Claim {

    static final String SET = 'set'
    static final String ADD = 'add'
    static final String DEL = 'del'
    static final String DELETE = 'delete'
    static final Set<String> VERBS = [SET, ADD, DEL, DELETE] as Set
    static final String NAME = 'name'
    static final String RETAIN = 'retain'
    static final String PIN = 'pin'
    /** The one `set retain` value that releases a run's content (ticket 21 answer 1). */
    static final String LINEAGE = 'lineage'

    final String assertedBy
    final Cid subject
    final String verb
    final String attribute
    final Object value
    final List<Cid> supersedes
    final String timestamp

    Claim(String assertedBy, Cid subject, String verb, String attribute, Object value, List<Cid> supersedes, String timestamp) {
        if( !assertedBy )
            throw new IllegalArgumentException('a claim needs an asserted_by')
        if( subject == null )
            throw new IllegalArgumentException('a claim needs a subject')
        if( !VERBS.contains(verb) )
            throw new IllegalArgumentException("unknown claim verb '${verb}'; one of ${VERBS.join(', ')}")
        if( !timestamp )
            throw new IllegalArgumentException('a claim needs a timestamp')
        this.assertedBy = assertedBy
        this.subject = subject
        this.verb = verb
        this.attribute = attribute
        this.value = value
        final TreeSet<Cid> sorted = new TreeSet<Cid>(supersedes ?: Collections.<Cid> emptyList())
        this.supersedes = Collections.unmodifiableList(new ArrayList<Cid>(sorted))
        this.timestamp = timestamp
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.CLAIM)
        map.put('asserted_by', assertedBy)
        map.put('subject', subject)
        map.put('verb', verb)
        map.put('attribute', attribute)
        map.put('value', value)
        map.put('supersedes', new ArrayList<Object>(supersedes))
        map.put('timestamp', timestamp)
        return map
    }

    static Claim fromCbor(Map block) {
        Records.expectKind(block, Records.CLAIM)
        final List supersedes = (List) Records.require(block, 'supersedes')
        return new Claim(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.cid(Records.require(block, 'subject'), 'subject'),
            Records.string(Records.require(block, 'verb'), 'verb'),
            Records.string(Records.require(block, 'attribute'), 'attribute'),
            Records.require(block, 'value'),
            supersedes.collect { Object c -> Records.cid(c, 'supersedes') },
            Records.string(Records.require(block, 'timestamp'), 'timestamp'))
    }
}
