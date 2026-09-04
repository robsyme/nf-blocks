package robsyme.cas.ext

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.PluginExtensionPoint
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.IndexPaths
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputItem
import robsyme.cas.core.Records
import robsyme.cas.core.StoreRef

/**
 * The read-back channel factory (DESIGN.md §13):
 * {@code channel.fromStore(run: <ref>, output: 'aligned', where: [sample: 'B'])}.
 *
 * {@code run} is a RunManifest/RunCompletion Store URI, a {@code lid://<runHash>},
 * or {@code 'latest'} with {@code pipeline: '<id>'}. Each matching OutputItem is
 * emitted restored to its published structure: a file leaf becomes a
 * {@code cas://<cid>/<name>} path, a declined leaf becomes {@code null}, and an
 * {@code unaddressed} leaf is an error naming the item.
 */
@Slf4j
@CompileStatic
class CasExtension extends PluginExtensionPoint {

    private static final String LID_PREFIX = 'lid://'
    private static final String CAS_PREFIX = 'cas://'
    private static final String LATEST = 'latest'

    private Session session
    private CasSession cas

    @Override
    protected void init(Session session) {
        this.session = session
        this.cas = CasSession.of(session)
    }

    @Factory
    DataflowWriteChannel fromStore(Map opts) {
        final DataflowWriteChannel channel = CH.create()
        // Resolve now, so a bad run reference or an unaddressed item fails fast
        // rather than at ignition; but bind only once the DAG is ignited and a
        // downstream subscriber is attached, since CH.create() is a broadcast
        // that does not buffer for late subscribers.
        final List<Object> items = resolveItems(opts)
        session.addIgniter({ ->
            for( Object item : items )
                channel.bind(item)
            channel.bind(Channel.STOP)
        } as Closure)
        return channel
    }

    private List<Object> resolveItems(Map opts) {
        final String output = opts?.get('output') as String
        if( !output )
            throw new IllegalArgumentException("channel.fromStore needs an 'output' name")
        final Map<String, Object> where = (opts?.get('where') ?: [:]) as Map<String, Object>

        Index index = null
        try {
            index = openIndex()
            final Cid completion = resolveRun(index, opts)
            final List<Object> items = new ArrayList<Object>()
            for( Cid itemCid : index.items(completion, output, where) ) {
                final OutputItem item = loadItem(itemCid)
                if( item == null )
                    throw new IllegalStateException("output item ${itemCid} of output '${output}' is not in the store")
                items.add(restore(item.value, itemCid))
            }
            return items
        }
        finally {
            index?.close()
        }
    }

    /** The RunCompletion address for the run reference in {@code opts.run}. */
    private Cid resolveRun(Index index, Map opts) {
        final String run = opts?.get('run') as String
        if( !run )
            throw new IllegalArgumentException("channel.fromStore needs a 'run' reference")

        if( run == LATEST ) {
            final String pipeline = opts?.get('pipeline') as String
            if( !pipeline )
                throw new IllegalArgumentException("channel.fromStore(run: 'latest', ...) needs a 'pipeline' identity")
            return index.latestSuccessfulRun(pipeline)
                .orElseThrow { new IllegalStateException("no successful run of pipeline '${pipeline}' is recorded") }
        }
        if( run.startsWith(LID_PREFIX) ) {
            final String hash = run.substring(LID_PREFIX.length())
            return index.runByNextflowHash(hash)
                .orElseThrow { new IllegalStateException("no run with nextflow run hash '${hash}' is recorded") }
        }
        if( run.startsWith(CAS_PREFIX) ) {
            final Cid cid = StoreRef.parse(run).cid
            final Map block = loadBlock(cid)
            final String kind = block == null ? null : Records.kindOf(block)
            if( kind == Records.RUN_COMPLETION )
                return cid
            if( kind == Records.RUN_MANIFEST )
                return index.runByManifest(cid)
                    .orElseThrow { new IllegalStateException("run manifest ${cid} has no RunCompletion; the run did not finish") }
            throw new IllegalArgumentException("run reference '${run}' is a ${kind ?: 'unknown'} block, not a run")
        }
        throw new IllegalArgumentException("unrecognised run reference '${run}': expected a cas:// Store URI, a lid://<hash>, or 'latest'")
    }

    // ------------------------------------------------------------- restore

    /** Rebuilds an item's published structure, turning each leaf into a path or null. */
    private Object restore(Object value, Cid itemCid) {
        if( value instanceof Leaf )
            return pathFor((Leaf) value, itemCid)
        if( value instanceof Map ) {
            final Map<Object, Object> out = new LinkedHashMap<Object, Object>()
            for( Map.Entry e : ((Map) value).entrySet() )
                out.put(e.key, restore(e.value, itemCid))
            return out
        }
        if( value instanceof List ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object element : (List) value )
                out.add(restore(element, itemCid))
            return out
        }
        return value
    }

    private Object pathFor(Leaf leaf, Cid itemCid) {
        if( leaf.isAddressed() ) {
            final Cid cid = leaf.address
            // A raw block is presented under its published name; a directory's
            // manifest is presented as a directory by the provider (DESIGN.md §13).
            final String uri = cid.isDagCbor()
                ? CAS_PREFIX + cid
                : CAS_PREFIX + cid + '/' + leaf.name
            return CasPlugin.provider().getPath(URI.create(uri))
        }
        if( leaf.reason == Leaf.DECLINED )
            return null
        throw new IllegalStateException("output item ${itemCid} has a '${leaf.reason}' leaf (${leaf.name ?: 'unnamed'}) that cannot be restored to a path")
    }

    // ------------------------------------------------------------- plumbing

    private OutputItem loadItem(Cid cid) {
        final Map block = loadBlock(cid)
        return block == null ? null : OutputItem.fromCbor(block)
    }

    private Map loadBlock(Cid cid) {
        if( cid == null || !cid.isDagCbor() || !cas.store.has(cid) )
            return null
        final InputStream input = cas.store.open(cid)
        try {
            final Object decoded = DagCbor.decode(input.readAllBytes())
            return decoded instanceof Map ? (Map) decoded : null
        }
        finally {
            input.close()
        }
    }

    private Index openIndex() {
        final List<String> memberLocations = cas.config.members.collect { String alias -> cas.config.locationOf(alias).toString() }
        final String override = navigate('cas.index.path')
        return Index.open(IndexPaths.cachePath(memberLocations, override))
    }

    private String navigate(String dottedKey) {
        Object node = session.config
        for( String segment : dottedKey.split('\\.') ) {
            if( !(node instanceof Map) )
                return null
            node = ((Map) node).get(segment)
        }
        return node == null ? null : node.toString()
    }
}
