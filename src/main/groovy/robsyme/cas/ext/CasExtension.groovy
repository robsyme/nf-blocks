package robsyme.cas.ext

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.script.dsl.Description
import nextflow.util.RecordMap
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.ClaimState
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputItem
import robsyme.cas.core.Records
import robsyme.cas.core.RunRef

/**
 * The read-back channel factory (DESIGN.md §13):
 * {@code channel.fromStore(run: <ref>, output: 'aligned', where: [sample: 'B'])}.
 *
 * {@code run} is a RunManifest/RunCompletion Store URI, a {@code lid://<runHash>},
 * or {@code 'latest'} with {@code pipeline: '<id>'}. Each matching OutputItem is
 * emitted restored to its published structure: a file leaf becomes a
 * {@code cas://<cid>/<name>} path, a declined leaf becomes {@code null}, and an
 * {@code unaddressed} leaf is an error naming the item. With {@code records: true}
 * every non-Leaf map is a {@code nextflow.util.RecordMap} instead, at any depth,
 * for typed processes with record inputs (ticket 09).
 */
@Slf4j
@CompileStatic
class CasExtension extends PluginExtensionPoint {

    private static final String CAS_PREFIX = 'cas://'

    /** What fromStore takes, for a call that names none of it (ticket 07 Q3). */
    static final String USAGE =
        "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output: <name>`; optionally `where: [...]` and `records: true`."

    private static final List<String> ENTRY_KEYS = ['selection', 'run', 'output']

    private Session session
    private CasSession cas

    @Override
    protected void init(Session session) {
        this.session = session
        this.cas = CasSession.of(session)
    }

    @Factory
    @Description('Emits Output Items from the store: selection: <address>, or run: <ref> with output: <name>; optionally where: [...] and records: true.')
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
        if( opts == null || !ENTRY_KEYS.any { String k -> opts.containsKey(k) } )
            throw new IllegalArgumentException(USAGE)
        final boolean records = recordsOpt(opts)
        if( opts.containsKey('selection') )
            return resolveSelection(opts, records)
        final String output = opts.get('output') as String
        if( !output )
            throw new IllegalArgumentException("channel.fromStore needs an 'output' name")
        final Map<String, Object> where = (opts.get('where') ?: [:]) as Map<String, Object>

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
                items.add(restore(item.value, itemCid, records))
            }
            return items
        }
        finally {
            index?.close()
        }
    }

    /**
     * {@code records:} (ticket 09): false when absent, the given Boolean
     * otherwise. Anything else is refused, so a typo such as
     * {@code records: 'true'} is not quietly read as false.
     */
    private static boolean recordsOpt(Map opts) {
        if( !opts.containsKey('records') )
            return false
        final Object value = opts.get('records')
        if( value instanceof Boolean )
            return (Boolean) value
        final String type = value == null ? 'null' : value.getClass().simpleName
        throw new IllegalArgumentException("fromStore's `records` takes true or false, got '${value}' (${type})")
    }

    /** The RunCompletion address for the run reference in {@code opts.run} (the shared resolver, RunRef). */
    private Cid resolveRun(Index index, Map opts) {
        final String run = opts?.get('run') as String
        if( !run )
            throw new IllegalArgumentException("channel.fromStore needs a 'run' reference")
        return RunRef.resolve(index, cas.store, run, opts?.get('pipeline') as String)
    }

    /**
     * fromStore(selection:) (block explorer spec section 10): every distinct
     * item the Selection reaches, nested ones included, sorted by item CID,
     * restored as a run's output is. A deleted Selection still emits: its
     * address is an explicit, immutable request.
     */
    private List<Object> resolveSelection(Map opts, boolean records) {
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
            index.ensureSelectionIndexed(cas.store, selection, cas.config.writableAlias)
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
                items.add(restore(item.value, itemCid, records))
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

    // ------------------------------------------------------------- restore

    /**
     * Rebuilds an item's published structure, turning each leaf into a path or
     * null. With {@code records}, every map (a Leaf is decoded to {@link Leaf}
     * before this, so it is never one) is returned as an immutable RecordMap;
     * lists stay lists either way.
     */
    private Object restore(Object value, Cid itemCid, boolean records) {
        if( value instanceof Leaf )
            return pathFor((Leaf) value, itemCid)
        if( value instanceof Map ) {
            final LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map) value).entrySet() )
                out.put(String.valueOf(e.key), restore(e.value, itemCid, records))
            return records ? new RecordMap(out) : out
        }
        if( value instanceof List ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object element : (List) value )
                out.add(restore(element, itemCid, records))
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
