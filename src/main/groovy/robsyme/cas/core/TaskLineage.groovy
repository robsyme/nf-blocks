package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The task-level upstream walk behind `nf-blocks:copy --depth lineage`. A run's
 * published FileOutputs name the work-dir file they came from (`source`, a
 * `lid://<task>/<file>`); that task's TaskRun `input` names the files it read,
 * in the same form. An input that some FileOutput of the same run published is
 * upstream of the address we started from.
 */
@CompileStatic
class TaskLineage {

    private static final String LID = 'lid://'
    private static final String CAS = 'cas://'

    /** available is false when the member holds no records under the run's hash. */
    static class Result {
        final boolean available
        final Set<Cid> upstream

        Result(boolean available, Set<Cid> upstream) {
            this.available = available
            this.upstream = upstream
        }
    }

    static Result upstream(LineageRecords nf, String nfRunHash, Collection<Cid> addresses) {
        final List<String> keys = nf.keysUnder(nfRunHash + '/')
        if( keys.isEmpty() )
            return new Result(false, new LinkedHashSet<Cid>())

        final Map<Cid, Set<String>> sourcesOf = [:]
        final Map<String, Set<Cid>> publishedFrom = [:]
        for( String key : keys ) {
            final Map record = nf.read(key)
            if( record == null || record.kind != 'FileOutput' || !(record.spec instanceof Map) ) continue
            final Map spec = (Map) record.spec
            final Cid cid = addressOf(spec.path)
            final Object source = spec.source
            if( cid == null || !(source instanceof String) || !((String) source).startsWith(LID) ) continue
            sourcesOf.computeIfAbsent(cid) { Cid c -> new LinkedHashSet<String>() }.add((String) source)
            publishedFrom.computeIfAbsent((String) source) { String s -> new LinkedHashSet<Cid>() }.add(cid)
        }

        final Set<Cid> upstream = new LinkedHashSet<Cid>()
        final Set<String> seen = new HashSet<String>()
        final ArrayDeque<String> queue = new ArrayDeque<String>()
        for( Cid a : addresses ) {
            final Set<String> sources = sourcesOf.get(a)
            if( sources != null )
                for( String lid : sources ) enqueue(taskOf(lid), seen, queue)
        }

        while( !queue.isEmpty() ) {
            final String task = queue.poll()
            final Map record = nf.read(task)
            if( record == null || record.kind != 'TaskRun' || !(record.spec instanceof Map) ) continue
            final List<String> inputs = []
            collectLids(((Map) record.spec).get('input'), inputs)
            for( String lid : inputs ) {
                final Set<Cid> published = publishedFrom.get(lid)
                if( published != null ) upstream.addAll(published)
                enqueue(taskOf(lid), seen, queue)
            }
        }
        return new Result(true, upstream)
    }

    private static void enqueue(String task, Set<String> seen, ArrayDeque<String> queue) {
        if( task && seen.add(task) ) queue.add(task)
    }

    /** `lid://<task>/<file>` to `<task>`. */
    private static String taskOf(String lid) {
        final String rest = lid.substring(LID.length())
        final int slash = rest.indexOf('/')
        return slash < 0 ? rest : rest.substring(0, slash)
    }

    /** The cid of a `cas://<cid>/<name>` path; null for any other path or a bad cid. */
    private static Cid addressOf(Object path) {
        if( !(path instanceof String) || !((String) path).startsWith(CAS) ) return null
        final String rest = ((String) path).substring(CAS.length())
        final int slash = rest.indexOf('/')
        try {
            return Cid.parse(slash < 0 ? rest : rest.substring(0, slash))
        }
        catch( IllegalArgumentException e ) {
            return null
        }
    }

    private static void collectLids(Object value, List<String> out) {
        if( value instanceof String ) {
            if( ((String) value).startsWith(LID) ) out.add((String) value)
        }
        else if( value instanceof Map ) {
            for( Object v : ((Map) value).values() ) collectLids(v, out)
        }
        else if( value instanceof Collection ) {
            for( Object v : (Collection) value ) collectLids(v, out)
        }
    }
}
