package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * A Collection a person assembles (block explorer spec section 7). Identity is
 * asserted_by, members and derived_from, with no timestamp. The constructor is
 * the normalisation: members sort by address string, each address appears
 * once (an item picked twice keeps the union of its via), each via and
 * derived_from sorts likewise. `via` is followed for metadata only;
 * `derived_from` holds weak references as bytes (spec section 7.3).
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Selection {

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Member {
        final Cid address
        /** True for a nested Selection, false for an Output Item. */
        final boolean nested
        /** The Output Collections the item was picked from; empty when chosen by a query, always empty when nested. */
        final List<Cid> via

        private Member(Cid address, boolean nested, Collection<Cid> via) {
            if( address == null )
                throw new IllegalArgumentException('a Selection member needs an address')
            this.address = address
            this.nested = nested
            this.via = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(via ?: Collections.<Cid> emptyList())))
        }
    }

    static Member item(Cid address, List<Cid> via) { new Member(address, false, via) }

    static Member selection(Cid address) { new Member(address, true, Collections.<Cid> emptyList()) }

    final String assertedBy
    final List<Member> members
    final List<Cid> derivedFrom

    Selection(String assertedBy, List<Member> members, List<Cid> derivedFrom) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Selection needs an asserted_by')
        if( !members )
            throw new IllegalArgumentException('an empty Selection is refused (block explorer spec section 7.2)')
        final TreeMap<Cid, Member> merged = new TreeMap<Cid, Member>()
        for( Member m : members ) {
            final Member seen = merged.get(m.address)
            if( seen == null ) {
                merged.put(m.address, m)
                continue
            }
            if( seen.nested != m.nested )
                throw new IllegalArgumentException("${m.address} is given both as an item and as a Selection")
            final List<Cid> via = new ArrayList<Cid>(seen.via)
            via.addAll(m.via)
            merged.put(m.address, new Member(m.address, m.nested, via))
        }
        this.assertedBy = assertedBy
        this.members = Collections.unmodifiableList(new ArrayList<Member>(merged.values()))
        this.derivedFrom = Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(derivedFrom ?: Collections.<Cid> emptyList())))
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.SELECTION)
        map.put('asserted_by', assertedBy)
        map.put('members', members.collect { Member m ->
            m.nested
                ? ([selection: m.address] as Map<String, Object>)
                : ([item: [address: m.address, via: new ArrayList<Object>(m.via)] as Map<String, Object>] as Map<String, Object>)
        })
        map.put('derived_from', derivedFrom.collect { Cid c -> c.bytes() })
        return map
    }

    static Selection fromCbor(Map block) {
        Records.expectKind(block, Records.SELECTION)
        final List<Member> members = ((List) Records.require(block, 'members')).collect { Object entry ->
            if( !(entry instanceof Map) || ((Map) entry).size() != 1 )
                throw new IllegalArgumentException("a Selection member is a map with exactly one key, 'item' or 'selection': ${entry}")
            final Map m = (Map) entry
            if( m.containsKey('selection') )
                return selection(Records.cid(m.get('selection'), 'selection'))
            if( m.containsKey('item') && m.get('item') instanceof Map ) {
                final Map item = (Map) m.get('item')
                return Selection.item(Records.cid(Records.require(item, 'address'), 'address'),
                    ((List) Records.require(item, 'via')).collect { Object v -> Records.cid(v, 'via') })
            }
            throw new IllegalArgumentException("a Selection member is a map with exactly one key, 'item' or 'selection': ${entry}")
        }
        final List<Cid> derived = ((List) Records.require(block, 'derived_from')).collect { Object b ->
            if( !(b instanceof byte[]) )
                throw new IllegalArgumentException("derived_from holds binary cids, got ${b?.getClass()?.simpleName}")
            return Cid.fromBytes((byte[]) b)
        }
        return new Selection(Records.string(Records.require(block, 'asserted_by'), 'asserted_by'), members, derived)
    }
}
