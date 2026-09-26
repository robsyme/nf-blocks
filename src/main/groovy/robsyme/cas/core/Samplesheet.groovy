package robsyme.cas.core

import groovy.json.JsonOutput
import groovy.transform.CompileStatic

/**
 * A Selection's items as a samplesheet (block explorer spec section 10;
 * decision 18 of the milestone 2 plan). One row per item. Meta Map columns
 * by dotted path; file columns by the leaf's structural position (tuple
 * index or record key, never file name). A file cell is cas://<cid>/<name>
 * for a file, cas://<manifest> for a directory, blank for any leaf without
 * an address. CSV is the flattened copy, JSON the lossless one.
 */
@CompileStatic
final class Samplesheet {

    @CompileStatic
    static final class Row {
        final Cid item
        final Map<String, Object> meta
        final Map<String, Object> flat
        final Map<String, String> files

        Row(Cid item, Map<String, Object> meta, Map<String, Object> flat, Map<String, String> files) {
            this.item = item
            this.meta = meta
            this.flat = flat
            this.files = files
        }
    }

    final List<Row> rows
    final List<String> columns
    private final List<String> metaColumns
    private final Map<String, String> fileColumnNames

    private Samplesheet(List<Row> rows, List<String> metaColumns, Map<String, String> fileColumnNames) {
        this.rows = rows
        this.metaColumns = metaColumns
        this.fileColumnNames = fileColumnNames
        this.columns = Collections.unmodifiableList(metaColumns + new ArrayList<String>(fileColumnNames.values()))
    }

    static Samplesheet of(BlockStore store, List<Cid> items) {
        final List<Row> rows = new ArrayList<Row>()
        final LinkedHashSet<String> metaColumns = new LinkedHashSet<String>()
        final LinkedHashSet<String> positions = new LinkedHashSet<String>()
        for( Cid cid : items ) {
            final Object value = OutputItem.fromCbor(load(store, cid)).value
            final Object view = viewOf(value)
            final Map<String, Object> meta = view instanceof Map ? (Map<String, Object>) withoutLeaves(view) : new LinkedHashMap<String, Object>()
            final Map<String, Object> flat = new LinkedHashMap<String, Object>()
            flatten(meta, null, flat)
            final Map<String, String> files = new LinkedHashMap<String, String>()
            collectFiles(value, null, files)
            metaColumns.addAll(flat.keySet())
            positions.addAll(files.keySet())
            rows.add(new Row(cid, meta, flat, files))
        }
        // A position that is also a Meta Map column is written file.<position>.
        final Map<String, String> names = new LinkedHashMap<String, String>()
        for( String position : positions )
            names.put(position, metaColumns.contains(position) ? "file.${position}".toString() : position)
        return new Samplesheet(rows, new ArrayList<String>(metaColumns), names)
    }

    String csv() {
        final StringBuilder out = new StringBuilder()
        out.append(columns.collect { String c -> quote(c) }.join(',')).append('\n')
        for( Row row : rows ) {
            final List<String> cells = new ArrayList<String>()
            for( String column : metaColumns )
                cells.add(quote(text(column, row.flat.get(column))))
            for( Map.Entry<String, String> file : fileColumnNames.entrySet() )
                cells.add(quote(row.files.get(file.key) ?: ''))
            out.append(cells.join(',')).append('\n')
        }
        return out.toString()
    }

    String json() {
        final List<Map<String, Object>> out = new ArrayList<Map<String, Object>>()
        for( Row row : rows ) {
            final Map<String, Object> entry = new LinkedHashMap<String, Object>(row.meta)
            for( Map.Entry<String, String> file : row.files.entrySet() )
                entry.put(fileColumnNames.get(file.key), file.value)
            out.add(entry)
        }
        return JsonOutput.prettyPrint(JsonOutput.toJson(out)) + '\n'
    }

    // ------------------------------------------------------------------ plumbing

    private static Map load(BlockStore store, Cid cid) {
        if( !store.has(cid) )
            throw new IllegalStateException("output item ${cid} is not in any member of this composition")
        final InputStream input = store.open(cid)
        try {
            return (Map) DagCbor.decode(input.readAllBytes())
        }
        finally {
            input.close()
        }
    }

    /** MetadataView.of over a decoded item, whose leaves are Leaf objects rather than maps. */
    private static Object viewOf(Object value) {
        if( value instanceof Map )
            return value
        if( value instanceof List )
            for( Object element : (List) value )
                if( element instanceof Map )
                    return element
        return null
    }

    private static Object withoutLeaves(Object value) {
        if( value instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map) value).entrySet() )
                if( !(e.value instanceof Leaf) )
                    out.put(String.valueOf(e.key), withoutLeaves(e.value))
            return out
        }
        if( value instanceof List )
            return ((List) value).findAll { Object v -> !(v instanceof Leaf) }.collect { Object v -> withoutLeaves(v) }
        if( value instanceof Cid )
            return value.toString()
        return value
    }

    private static void flatten(Map<String, Object> map, String prefix, Map<String, Object> out) {
        for( Map.Entry<String, Object> e : map.entrySet() ) {
            final String path = prefix ? "${prefix}.${e.key}".toString() : e.key
            if( e.value instanceof Map )
                flatten((Map<String, Object>) e.value, path, out)
            else
                out.put(path, e.value)
        }
    }

    private static void collectFiles(Object value, String position, Map<String, String> files) {
        if( value instanceof Leaf ) {
            final Leaf leaf = (Leaf) value
            // A bare file item has no position; its one column is `file`.
            files.put(position ?: 'file', !leaf.addressed ? '' : leaf.address.isRaw()
                ? "cas://${leaf.address}/${leaf.name}".toString()
                : "cas://${leaf.address}".toString())
            return
        }
        if( value instanceof Map ) {
            for( Map.Entry e : ((Map) value).entrySet() )
                collectFiles(e.value, position ? "${position}.${e.key}".toString() : String.valueOf(e.key), files)
            return
        }
        if( value instanceof List ) {
            final List list = (List) value
            for( int i = 0; i < list.size(); i++ )
                collectFiles(list[i], position ? "${position}.${i}".toString() : String.valueOf(i), files)
        }
    }

    /** The index's text form for a scalar (MetadataView.scalar), JSON for a list or map, blank for absent or null. */
    private static String text(String path, Object value) {
        if( value == null )
            return ''
        if( value instanceof String )
            return (String) value
        if( value instanceof List || value instanceof Map )
            return JsonOutput.toJson(value)
        return MetadataView.scalar(path, value).value
    }

    /** RFC 4180: quote a field holding a comma, a quote, CR or LF, doubling its quotes. */
    private static String quote(String field) {
        if( field.indexOf(',') < 0 && field.indexOf('"') < 0 && field.indexOf('\n') < 0 && field.indexOf('\r') < 0 )
            return field
        return '"' + field.replace('"', '""') + '"'
    }
}
