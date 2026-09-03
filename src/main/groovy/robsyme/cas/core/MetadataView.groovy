package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The metadata view of a published channel item and its flattening into
 * `item_attr` rows (DESIGN.md §12, CONTEXT.md "Meta Map").
 *
 * A bare query key resolves against the view: the item itself when it is a
 * map, otherwise the first top-level map in the tuple. Leaf maps
 * (`kind == 'Leaf'`, DESIGN.md §6) are file positions rather than metadata
 * and are skipped wherever they appear.
 */
@CompileStatic
class MetadataView {

    /** Strings longer than this many UTF-8 bytes are stored as a digest. */
    static final int VALUE_CAP_BYTES = 1024

    static final String TYPE_STRING = 'string'
    static final String TYPE_INT = 'int'
    static final String TYPE_FLOAT = 'float'
    static final String TYPE_BOOL = 'bool'
    static final String TYPE_NULL = 'null'

    /**
     * The item's metadata view: the item when it is a map, else the first
     * top-level map in the list, else null. A Leaf map is never the view.
     */
    static Object of(Object itemValue) {
        if( isLeaf(itemValue) )
            return null
        if( itemValue instanceof Map )
            return itemValue
        if( itemValue instanceof List ) {
            for( Object element : (List) itemValue )
                if( element instanceof Map && !isLeaf(element) )
                    return element
        }
        return null
    }

    /** True for the Leaf map of DESIGN.md §6, the decoding rule being `kind == 'Leaf'`. */
    static boolean isLeaf(Object value) {
        return value instanceof Map && ((Map) value).get('kind') == 'Leaf'
    }

    /** Every scalar leaf of the view, typed, under its dotted path. */
    static List<AttrRow> flatten(Object view) {
        final List<AttrRow> rows = new ArrayList<AttrRow>()
        if( view instanceof Map && !isLeaf(view) )
            walkMap((Map) view, null, rows)
        return rows
    }

    private static void walkMap(Map map, String prefix, List<AttrRow> rows) {
        for( Object entry : map.entrySet() ) {
            final Map.Entry e = (Map.Entry) entry
            final String key = String.valueOf(e.key)
            walk(e.value, prefix ? "${prefix}.${key}".toString() : key, rows)
        }
    }

    private static void walk(Object value, String path, List<AttrRow> rows) {
        if( isLeaf(value) ) {
            // A file position, not metadata.
            return
        }
        if( value instanceof Map ) {
            walkMap((Map) value, path, rows)
            return
        }
        if( value instanceof List ) {
            // Array elements sit under the array's own path, so a match on
            // that path is membership.
            for( Object element : (List) value )
                walk(element, path, rows)
            return
        }
        rows.add(scalar(path, value))
    }

    /** The typed row for one scalar. */
    static AttrRow scalar(String path, Object value) {
        if( value == null )
            return new AttrRow(path, TYPE_NULL, null, 0)
        if( value instanceof Boolean )
            return new AttrRow(path, TYPE_BOOL, value.toString(), 0)
        if( value instanceof Double || value instanceof Float || value instanceof BigDecimal )
            return new AttrRow(path, TYPE_FLOAT, value.toString(), 0)
        if( value instanceof Number )
            return new AttrRow(path, TYPE_INT, value.toString(), 0)
        return text(path, value.toString())
    }

    /** The typed row for a string, hashing it when it is past the cap. */
    static AttrRow text(String path, String value) {
        final byte[] bytes = value.getBytes('UTF-8')
        if( bytes.length <= VALUE_CAP_BYTES )
            return new AttrRow(path, TYPE_STRING, value, 0)
        return new AttrRow(path, TYPE_STRING, digestOf(bytes), 1)
    }

    /** The stand-in a string past the cap is stored as. */
    static String digestOf(byte[] bytes) {
        final StringBuilder hex = new StringBuilder(71).append('sha256:')
        for( byte b : Hashing.sha256(bytes) )
            hex.append(Integer.toHexString((b & 0xff) | 0x100).substring(1))
        return hex.toString()
    }
}
