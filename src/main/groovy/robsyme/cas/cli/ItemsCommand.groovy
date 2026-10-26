package robsyme.cas.cli

import groovy.transform.CompileStatic
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.Conditions
import robsyme.cas.core.DagJson
import robsyme.cas.core.Index
import robsyme.cas.core.ItemHit
import robsyme.cas.core.Records
import robsyme.cas.core.RunRef
import robsyme.cas.core.Samplesheet

/**
 * `nextflow plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...]
 * [--pipeline <id>] [--format csv|json|occurrences|selection]` (decision 7 of the
 * milestone 3 plan; ticket 05). Read-only: it catches the plugin's own index up
 * from every member's Store Log, as put and snapshot do, and writes nothing.
 * Conditions are positionals split at the first '='; several runs are one
 * comma-joined --run, because CmdPlugin keeps one value per flag.
 */
@CompileStatic
class ItemsCommand {

    static final Set<String> FLAGS = ['run', 'pipeline', 'format'] as Set
    static final List<String> FORMATS = ['csv', 'json', 'occurrences', 'selection']

    /** The arguments, checked; every usage error but a malformed run reference is found here. */
    @CompileStatic
    static final class Request {
        final String output
        final Map<String, String> conditions
        final List<String> runs
        final String pipeline
        final String format

        Request(String output, Map<String, String> conditions, List<String> runs, String pipeline, String format) {
            this.output = output
            this.conditions = conditions
            this.runs = runs
            this.pipeline = pipeline
            this.format = format
        }
    }

    static int run(List<String> args, Map config, PrintStream out, PrintStream err) {
        final Request request = parse(args)
        final CasSession cas = CasSession.forCommand(CasConfig.fromSession(config), err)
        final Index index = cas.openIndex()
        try {
            cas.catchUpIndex(index)
            final List<ItemHit> hits = hits(index, cas.store, request)
            if( hits.isEmpty() ) {
                err.println("nf-blocks:items: no item of output '${request.output}' matches " +
                    "${request.conditions ? request.conditions.collect { String k, String v -> final List<String> key = Conditions.splitKey(k); "${key[0]}${key[1]}${v}" }.join(' ') : 'at all'} " +
                    "in ${request.runs.join(', ')}" + (request.format == 'selection' ? '; an empty Selection is refused, so nothing is printed' : ''))
                if( request.format == 'selection' )
                    return 1
            }
            out.print(render(cas.store, hits, request.format))
            out.flush()
            return 0
        }
        finally {
            index.close()
        }
    }

    static Request parse(List<String> args) {
        final Options options = Options.parse(args, FLAGS)
        final List<String> positionals = options.positionals
        if( positionals.isEmpty() )
            throw new UsageException('items takes an output name, then any <path>=<value> conditions')
        final String output = positionals[0]
        if( output.contains('=') )
            throw new UsageException("items takes the output name first, then conditions; got the condition '${output}' where the output name goes")
        final Map<String, String> conditions = new LinkedHashMap<String, String>()
        for( String arg : positionals.drop(1) ) {
            final List<String> parts = Conditions.splitArg(arg)
            if( parts == null )
                throw new UsageException("condition '${arg}' is not <path>=<value>, <path>!=<value> or <path>>=<number> (or >, <, <=)")
            final String path = parts[0], op = parts[1], value = parts[2]
            if( !path )
                throw new UsageException("condition '${arg}' has no path before the '${op}'")
            if( Conditions.RANGE_OPS.contains(op) ) {
                try {
                    Conditions.number(value)
                }
                catch( NumberFormatException ignored ) {
                    throw new UsageException("condition '${arg}' compares with ${op}, so its value must be a number")
                }
            }
            // Keyed as fromStore's where: keys them: the path, or the path and its operator.
            final String key = op == '=' ? path : "${path} ${op}".toString()
            if( conditions.containsKey(key) )
                throw new UsageException("condition '${arg}': the path '${path}' is already given, and each path takes one value")
            conditions.put(key, value)
        }
        final String run = options.flag('run')
        if( !run )
            throw new UsageException('items needs --run <ref>[,<ref>...]: a lid://<hash>, a cas:// RunCompletion or RunManifest, or latest with --pipeline')
        final List<String> runs = Arrays.asList(run.split(',', -1)).collect { String r -> r.trim() }
        if( runs.any { String r -> !r } )
            throw new UsageException("--run '${run}' has an empty run reference")
        final String pipeline = options.flag('pipeline')
        if( pipeline != null && !runs.contains(RunRef.LATEST) )
            throw new UsageException('--pipeline goes with --run latest')
        final String format = options.flag('format') ?: 'csv'
        if( !FORMATS.contains(format) )
            throw new UsageException("--format is one of ${FORMATS.join(', ')}, got '${format}'")
        return new Request(output, conditions, runs, pipeline, format)
    }

    /** The union over every run, each (collection, item) once, sorted by item CID then collection CID. */
    static List<ItemHit> hits(Index index, BlockStore store, Request request) {
        final LinkedHashSet<Cid> completions = new LinkedHashSet<Cid>()
        for( String ref : request.runs ) {
            try {
                completions.add(RunRef.resolve(index, store, ref, request.pipeline))
            }
            catch( IllegalArgumentException e ) {
                throw new UsageException("--run: ${e.message}")
            }
        }
        final TreeSet<ItemHit> union = new TreeSet<ItemHit>()
        for( Cid completion : completions ) {
            final Map<String, Cid> outputs = index.collectionsOf(completion)
            if( !outputs.containsKey(request.output) )
                throw new IllegalStateException("run ${completion} has no output '${request.output}'; its outputs are ${outputs.keySet().join(', ') ?: 'none'}")
            union.addAll(index.itemHitsByText(completion, request.output, request.conditions))
        }
        return new ArrayList<ItemHit>(union)
    }

    static String render(BlockStore store, List<ItemHit> hits, String format) {
        final List<String> occurrences = hits.collect { ItemHit h -> h.occurrence() }
        if( format == 'occurrences' )
            return occurrences.collect { String o -> o + '\n' }.join('')
        if( format == 'selection' ) {
            // The request the page's writer.selection sends: asserted_by is the server's.
            final Map<String, Object> request = new LinkedHashMap<String, Object>()
            request.put('kind', Records.SELECTION)
            request.put('members', occurrences)
            request.put('derived_from', new ArrayList<Object>())
            return DagJson.encodeToString(request) + '\n'
        }
        final Samplesheet sheet = Samplesheet.of(store, hits.collect { ItemHit h -> h.item }, occurrences)
        return format == 'json' ? sheet.json() : sheet.csv()
    }
}
