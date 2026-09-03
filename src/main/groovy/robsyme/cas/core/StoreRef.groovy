package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/**
 * A Store URI naming one addressed thing: {@code cas://<cid>/<name>}
 * (DESIGN.md §7). This is the whole content of a Pointer File and the value
 * that replaces a Publish Coordinate in a lineage record.
 *
 * A raw block has no name of its own, so a reference to one must carry the
 * name it was published under; a DirectoryManifest names its own children, so
 * a reference to one may go without.
 */
@CompileStatic
@EqualsAndHashCode
class StoreRef {

    private static final String PREFIX = Coordinates.SCHEME + '://'

    final Cid cid

    final String name

    StoreRef(Cid cid, String name) {
        if( cid == null )
            throw new IllegalArgumentException('a store reference needs a content address')
        if( cid.isRaw() && !name )
            throw new IllegalArgumentException("a reference to the raw block $cid needs the name it was published under")
        this.cid = cid
        this.name = name
    }

    /** True when the address is a DirectoryManifest rather than file content. */
    boolean isDirectory() { cid.isDagCbor() }

    static StoreRef parse(String text) {
        if( !text || !text.startsWith(PREFIX) )
            throw new IllegalArgumentException("not a store uri: ${text == null ? 'null' : "'$text'"}")
        final String rest = text.substring(PREFIX.length())
        final int slash = rest.indexOf('/')
        final String authority = slash < 0 ? rest : rest.substring(0, slash)
        final String path = slash < 0 ? '' : rest.substring(slash + 1)
        if( !Cid.isCid(authority) )
            throw new IllegalArgumentException("'$text' is a publish coordinate, not a store uri: '$authority' is not a content address")
        if( path.contains('/') )
            throw new IllegalArgumentException("a store uri carries at most one segment: '$text'")
        return new StoreRef(Cid.parse(authority), path ?: null)
    }

    @Override
    String toString() {
        return name ? PREFIX + cid + '/' + name : PREFIX + cid
    }
}
