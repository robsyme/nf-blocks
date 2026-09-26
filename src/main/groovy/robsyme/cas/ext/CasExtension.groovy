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
import robsyme.cas.core.ClaimState
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputItem
import robsyme.cas.core.Records
import robsyme.cas.core.Selection
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
        if( opts?.containsKey('selection') )
            return resolveSelection(opts)
        final String output = opts?.get('output') as String
        if( !output )
            throw new IllegalArgumentException("channel.fromStore needs an 'output' name")
        final Map<String, Object> where = (opts?.get('where') ?: [:]) as Map<String, Object>

        Index index = null
        try {
            index = openIndex()
            // Read across the whole composition: bring the index up to date from
            // every member's run log, so a run recorded only in a read-only member
            // (the producer's store, mounted read-only here) is visible to `latest`.
            cas.catchUpIndex(index)
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

    /**
     * fromStore(selection:) (block explorer spec section 10): every distinct
     * item the Selection reaches, nested ones included, sorted by item CID,
     * restored as a run's output is. A deleted Selection still emits: its
     * address is an explicit, immutable request.
     */
    private List<Object> resolveSelection(Map opts) {
        final List<String> clashing = ['run', 'output', 'where', 'pipeline'].findAll { String k -> opts.containsKey(k) }
        if( clashing )
            throw new IllegalArgumentException("channel.fromStore(selection: ...) takes no ${clashing.join(', ')}: a Selection names its items itself")
        final Cid selection = selectionCid(opts.get('selection'))
        Index index = null
        try {
            index = openIndex()
            cas.catchUpIndex(index)
            final Map block = loadBlock(selection)
            if( block == null )
                throw new IllegalStateException("selection ${selection} is not in any member of this composition")
            if( Records.kindOf(block) != Records.SELECTION )
                throw new IllegalArgumentException("${selection} is a ${Records.kindOf(block) ?: 'block with no kind'}, not a Selection")
            ensureIndexed(index, selection, new HashSet<Cid>())
            final ClaimState state = index.claimState(selection)
            if( state.hidden )
                log.warn("selection ${selection} is hidden by a current delete Claim (${state.deletionClaims.join(', ')}); emitting its items anyway, as its address asks")
            else if( state.deletion == ClaimState.CONFLICTED )
                log.warn("selection ${selection} has a conflicted deletion (${state.deletionClaims.join(', ')}); emitting its items")
            final List<Object> items = new ArrayList<Object>()
            for( Cid itemCid : index.selectionItems(selection) ) {
                final OutputItem item = loadItem(itemCid)
                if( item == null )
                    throw new IllegalStateException("output item ${itemCid} of selection ${selection} is not in any member of this composition")
                items.add(restore(item.value, itemCid))
            }
            return items
        }
        finally {
            index?.close()
        }
    }

    private static Cid selectionCid(Object value) {
        String text = value?.toString()
        if( text?.startsWith(CAS_PREFIX) )
            text = text.substring(CAS_PREFIX.length())
        if( !text || !Cid.isCid(text) )
            throw new IllegalArgumentException("channel.fromStore(selection: ...) takes a Selection address, cas://<cid> or <cid>, got '${value}'")
        return Cid.parse(text)
    }

    /**
     * A Selection block present in the store but never logged (copied without
     * its Store Log entry) is indexed on the spot, and so is each nested one
     * the store holds; a nested one the store lacks is left for
     * selectionItems to name.
     */
    private void ensureIndexed(Index index, Cid selection, Set<Cid> seen) {
        if( !seen.add(selection) )
            return
        final Map block = loadBlock(selection)
        if( block == null || Records.kindOf(block) != Records.SELECTION )
            return
        if( !index.isSelectionIndexed(selection) )
            index.ingestSelection(cas.store, selection, cas.config.writableAlias)
        for( Selection.Member m : Selection.fromCbor(block).members )
            if( m.nested )
                ensureIndexed(index, m.address, seen)
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
        return cas.openIndex()
    }
}
