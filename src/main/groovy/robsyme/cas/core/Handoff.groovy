package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * One hand-off to a downstream pipeline (explorer v0.3 P4 plan 1.5, D4,
 * DESIGN.md §22): the Selection handed off, the files generated for it
 * (a samplesheet, contrasts) as raw blocks it roots, and the downstream
 * params with every generated file as `cas://<cid>/<name>`.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Handoff {

    final String assertedBy
    final String template
    final Cid selection
    final Cid project
    /** Sorted by name, names unique, every one addressed. */
    final List<Leaf> files
    final Map<String, String> params
    final String timestamp

    Handoff(String assertedBy, String template, Cid selection, Cid project, List<Leaf> files,
            Map<String, String> params, String timestamp) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Handoff needs an asserted_by')
        if( !template )
            throw new IllegalArgumentException('a Handoff needs a template')
        if( selection == null )
            throw new IllegalArgumentException('a Handoff needs a selection')
        if( !timestamp )
            throw new IllegalArgumentException('a Handoff needs a timestamp')
        final TreeMap<String, Leaf> byName = new TreeMap<String, Leaf>()
        for( Leaf f : files ?: Collections.<Leaf> emptyList() ) {
            if( f == null || !f.name || f.address == null )
                throw new IllegalArgumentException("a Handoff's file has a name and an address, got ${f}")
            if( byName.put(f.name, f) != null )
                throw new IllegalArgumentException("a Handoff names the file '${f.name}' twice")
        }
        for( Map.Entry<String, String> e : (params ?: Collections.<String, String> emptyMap()).entrySet() )
            if( e.key == null || e.value == null )
                throw new IllegalArgumentException("a Handoff's params map strings to strings, got ${e.key}: ${e.value}")
        this.assertedBy = assertedBy
        this.template = template
        this.selection = selection
        this.project = project
        this.files = Collections.unmodifiableList(new ArrayList<Leaf>(byName.values()))
        this.params = Collections.unmodifiableMap(new TreeMap<String, String>(params ?: Collections.<String, String> emptyMap()))
        this.timestamp = timestamp
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.HANDOFF)
        map.put('asserted_by', assertedBy)
        map.put('template', template)
        map.put('selection', selection)
        map.put('project', project)
        map.put('files', files.collect { Leaf f -> (Object) f.toCbor() })
        map.put('params', new LinkedHashMap<String, Object>(params))
        map.put('timestamp', timestamp)
        return map
    }

    static Handoff fromCbor(Map block) {
        Records.expectKind(block, Records.HANDOFF)
        Records.schemaOf(block, [Records.SCHEMA])
        final Object files = Records.require(block, 'files')
        if( !(files instanceof List) )
            throw new IllegalArgumentException("field 'files' should be a list")
        final Object params = Records.require(block, 'params')
        if( !(params instanceof Map) )
            throw new IllegalArgumentException("field 'params' should be a map")
        final Map<String, String> strings = new TreeMap<String, String>()
        for( Map.Entry e : ((Map) params).entrySet() )
            strings.put(Records.string(e.key, 'params'), Records.string(e.value, 'params'))
        return new Handoff(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.string(Records.require(block, 'template'), 'template'),
            Records.cid(Records.require(block, 'selection'), 'selection'),
            Records.cid(Records.require(block, 'project'), 'project'),
            ((List) files).collect { Object f ->
                if( !(f instanceof Map) )
                    throw new IllegalArgumentException("a Handoff file is a Leaf map")
                Leaf.fromCbor((Map) f)
            },
            strings,
            Records.string(Records.require(block, 'timestamp'), 'timestamp'))
    }
}
