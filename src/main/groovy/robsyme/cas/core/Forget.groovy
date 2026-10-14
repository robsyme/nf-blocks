package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * What one sweep batch deleted (DESIGN.md §20 decision 6), so every cache can
 * forget its rows. No timestamp and no sweep id (§6): two batches that delete
 * the same set write one block.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Forget {

    final String assertedBy
    final List<Cid> deleted

    Forget(String assertedBy, Collection<Cid> deleted) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Forget needs an asserted_by')
        if( !deleted )
            throw new IllegalArgumentException('an empty Forget is refused')
        this.assertedBy = assertedBy
        this.deleted = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(deleted)))
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.FORGET)
        map.put('asserted_by', assertedBy)
        map.put('deleted', new ArrayList<Object>(deleted))
        return map
    }

    static Forget fromCbor(Map block) {
        Records.expectKind(block, Records.FORGET)
        final List deleted = (List) Records.require(block, 'deleted')
        return new Forget(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            deleted.collect { Object c -> Records.cid(c, 'deleted') })
    }
}
