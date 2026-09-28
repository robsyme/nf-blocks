package robsyme.cas.trace

import groovy.transform.CompileStatic

/**
 * Installs node-side hashing as a process.afterScript default (spec §14,
 * DESIGN.md §11), ahead of the user's own afterScript in the process scope
 * and in each withName/withLabel selector. A closure afterScript is left
 * alone (its tasks fall back to the head node) and returned for a warning.
 */
@CompileStatic
class NodeHash {

    static final String RESOURCE = '/robsyme/cas/node-hash.sh'

    private static final String SCRIPT = NodeHash.getResourceAsStream(RESOURCE).withCloseable { InputStream in -> in.getText('UTF-8') }.trim()

    static String script() { SCRIPT }

    /** Chains ours into config.process; the selectors whose afterScript is a closure, left alone. */
    static List<String> install(Map config) {
        Object process = config.get('process')
        if( !(process instanceof Map) ) {
            process = new LinkedHashMap<String, Object>()
            config.put('process', process)
        }
        final Map scope = (Map) process
        final List<String> skipped = []
        chain(scope, 'process', skipped, true)
        for( Object key : new ArrayList<Object>(scope.keySet()) ) {
            final String name = String.valueOf(key)
            if( (name.startsWith('withName:') || name.startsWith('withLabel:')) && scope.get(key) instanceof Map )
                chain((Map) scope.get(key), name, skipped, false)
        }
        return skipped
    }

    private static void chain(Map scope, String name, List<String> skipped, boolean top) {
        final Object current = scope.get('afterScript')
        if( current == null ) {
            if( top ) scope.put('afterScript', SCRIPT)
            return
        }
        if( !(current instanceof CharSequence) ) {
            skipped.add(name)
            return
        }
        final String text = current.toString()
        if( !text.startsWith(SCRIPT) )
            scope.put('afterScript', SCRIPT + '\n' + text)
    }
}
