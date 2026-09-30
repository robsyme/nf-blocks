package robsyme.cas.trace

import java.nio.file.Path

import groovy.transform.CompileStatic
import robsyme.cas.CasSession
import robsyme.cas.core.Anomalies
import robsyme.cas.core.Cid
import robsyme.cas.core.Coordinates
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Leaf
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputIndex
import robsyme.cas.core.OutputItem
import robsyme.cas.core.Providers
import robsyme.cas.core.Records

/**
 * The pure join of DESIGN.md §11: the captured workflow outputs plus a
 * {@link CasSession} become the run's {@link OutputItem}s, {@link OutputCollection}s
 * and the run-wide anomaly totals.
 *
 * "Pure" here means it holds no Nextflow event type: it is handed the output
 * name and the already-normalised published value (whose file leaves are
 * {@code cas://} coordinate {@link Path}s), and it reads addresses out of the
 * session's {@code publishes} map. It stores nothing; the observer writes the
 * blocks whose addresses this computes.
 *
 * An {@code index {}} block does not null the value at 26.04.6
 * ({@code PublishOp.groovy:219-229}); a null value is a value channel that
 * emitted nothing, and its output becomes an empty collection rather than an
 * {@code unaddressed} anomaly (ticket 26).
 */
@CompileStatic
class Join {

    /** One named output: the items to store and the collection that links them. */
    static class JoinedOutput {
        final String name
        /** The items in arrival order, for the observer to store as blocks. */
        final List<OutputItem> items
        /** The collection, its {@code items}/{@code paths} already sorted by cid. */
        final OutputCollection collection

        JoinedOutput(String name, List<OutputItem> items, OutputCollection collection) {
            this.name = name
            this.items = items
            this.collection = collection
        }
    }

    /** Everything the observer needs from the join. */
    static class Result {
        final List<JoinedOutput> outputs
        final Anomalies anomalies
        /** Provider name to every Leaf address the run published under it (DESIGN.md §6, ticket 16, final review I5). */
        final Map<String, List<Cid>> providers
        /** The join keys of every Leaf this join built, addressed or not, and of every index file (ticket 26). */
        final Set<String> joinedKeys

        Result(List<JoinedOutput> outputs, Anomalies anomalies, Map<String, List<Cid>> providers, Set<String> joinedKeys) {
            this.outputs = outputs
            this.anomalies = anomalies
            this.providers = providers
            this.joinedKeys = Collections.unmodifiableSet(joinedKeys)
        }
    }

    /** Mutable anomaly tally, folded once at the end into an {@link Anomalies}. */
    private static class Counters {
        int unresolvable
        int unaddressed
        int declined
        int neverPublished

        Anomalies toAnomalies() {
            return new Anomalies(unresolvable, unaddressed, declined, neverPublished)
        }
    }

    /**
     * @param captured output name -> the normalised published value; a channel
     *                 output's value is a collection of items, a value output's
     *                 value is the single item. A null value is a value channel
     *                 that emitted nothing, and joins to an empty collection.
     * @param session  the run's shared state: {@code publishFor}, {@code uploadAnomaliesFor},
     *                 {@code assertedBy} and the RunManifest address.
     */
    static Result join(Map<String, Object> captured, CasSession session) {
        return join(captured, Collections.<String, Path> emptyMap(), session)
    }

    /**
     * @param captured output name -> the normalised published value (see the
     *                 2-arg {@link #join}).
     * @param indexes  output name -> its Output Index File's {@code cas://}
     *                 coordinate {@link Path}, for an output that declares
     *                 {@code index {}} (glossary; ticket 26).
     * @param session  the run's shared state, as the 2-arg {@link #join}.
     */
    static Result join(Map<String, Object> captured, Map<String, Path> indexes, CasSession session) {
        final Cid run = session.getRunManifest()
        if( run == null )
            throw new IllegalStateException('the run manifest must be written before the outputs are joined')
        final String assertedBy = session.assertedBy
        final Counters counters = new Counters()
        final List<JoinedOutput> outputs = new ArrayList<JoinedOutput>()
        final Map<String, TreeMap<String, Cid>> byProvider = new TreeMap<String, TreeMap<String, Cid>>()
        final Set<String> joined = new LinkedHashSet<String>()

        for( Map.Entry<String, Object> entry : captured.entrySet() ) {
            final String name = entry.key
            final Object value = entry.value
            // A null value is a value channel that emitted nothing (ticket 26);
            // its output is an empty collection, not an unaddressed anomaly.
            final List<Object> rawItems = value == null ? Collections.<Object> emptyList() : itemsOf(value)
            final List<OutputItem> items = new ArrayList<OutputItem>(rawItems.size())
            final List<Cid> itemCids = new ArrayList<Cid>(rawItems.size())
            final List<List<String>> itemPaths = new ArrayList<List<String>>(rawItems.size())
            for( Object raw : rawItems ) {
                final List<String> leafPaths = new ArrayList<String>()
                final Object built = build(raw, leafPaths, counters, session, byProvider, joined)
                final OutputItem item = OutputItem.of(built)
                items.add(item)
                itemCids.add(DagCbor.cidOf(DagCbor.encode(item.toCbor())))
                itemPaths.add(leafPaths)
            }
            final Path indexPath = indexes.get(name)
            final OutputIndex index = indexPath == null ? null : indexFor(indexPath, counters, session, byProvider, joined)
            final OutputCollection collection = new OutputCollection(assertedBy, run, name, itemCids, itemPaths, index)
            outputs.add(new JoinedOutput(name, items, collection))
        }
        final Map<String, List<Cid>> providers = byProvider.collectEntries {
            String k, TreeMap<String, Cid> v -> [(k): new ArrayList<Cid>(v.values())]
        } as Map<String, List<Cid>>
        return new Result(outputs, counters.toAnomalies(), providers, joined)
    }

    /** A channel output is a collection of items; a value output is one item. */
    private static List<Object> itemsOf(Object value) {
        if( value instanceof Collection )
            return new ArrayList<Object>((Collection) value)
        return Collections.singletonList(value)
    }

    /**
     * Rebuilds one item's structure, replacing every file/directory leaf (a
     * {@code cas://} coordinate {@link Path}) and every {@code null} (a declined
     * path) with a {@link Leaf}, and appending each leaf's publish path to
     * {@code leafPaths} in the depth-first order {@link OutputItem#leaves} uses.
     */
    private static Object build(Object raw, List<String> leafPaths, Counters counters, CasSession session,
                                Map<String, TreeMap<String, Cid>> byProvider, Set<String> joined) {
        if( raw instanceof Path )
            return leafFor((Path) raw, leafPaths, counters, session, byProvider, joined)
        if( raw == null ) {
            leafPaths.add(null)
            counters.declined += 1
            return Leaf.declined()
        }
        if( raw instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            for( Map.Entry e : ((Map) raw).entrySet() )
                out.put(String.valueOf(e.key), build(e.value, leafPaths, counters, session, byProvider, joined))
            return out
        }
        if( raw instanceof Collection ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object element : (Collection) raw )
                out.add(build(element, leafPaths, counters, session, byProvider, joined))
            return out
        }
        return raw
    }

    private static Leaf leafFor(Path path, List<String> leafPaths, Counters counters, CasSession session,
                               Map<String, TreeMap<String, Cid>> byProvider, Set<String> joined) {
        final String key = Coordinates.key(path)
        joined.add(key)
        final List<String> segments = segmentsOf(key)
        final String name = segments.isEmpty() ? null : segments.last()
        final String relPath = segments.join('/')
        final CasSession.Publish publish = session.publishFor(key)
        if( publish == null ) {
            // A coordinate that never received a publish event (DESIGN.md §6).
            leafPaths.add(relPath)
            counters.neverPublished += 1
            return Leaf.without(name, Leaf.NEVER_PUBLISHED)
        }
        leafPaths.add(relPath)
        final Cid address = publish.ref.cid
        // A directory's address is its dag-cbor manifest and its Publish.size is
        // the manifest block's byte count, not a content-byte total, so a
        // directory leaf carries no size (Task 6 review).
        final Long size = address.isDagCbor() ? null : (Long) publish.size
        final Anomalies uploaded = session.uploadAnomaliesFor(key)
        if( uploaded != null )
            fold(counters, uploaded)
        final String provider = publish.provider ?: Providers.HEAD_NODE
        // A directory leaf is listed by its manifest alone: listing the files inside would grow
        // the RunCompletion past what the index reads for a big directory (final review I5).
        byProvider.computeIfAbsent(provider, { String k -> new TreeMap<String, Cid>() }).put(address.toString(), address)
        return Leaf.of(name, address, size)
    }

    /**
     * An output's Output Index File: the {@link Leaf} of its publish, addressed
     * only from a publish made in the same run ({@link CasSession#publishFor},
     * never the Pointer File, ticket 26 answer 4), and its publish path.
     */
    private static OutputIndex indexFor(Path indexPath, Counters counters, CasSession session,
                                        Map<String, TreeMap<String, Cid>> byProvider, Set<String> joined) {
        final List<String> ignored = new ArrayList<String>()
        final Leaf leaf = leafFor(indexPath, ignored, counters, session, byProvider, joined)
        return new OutputIndex(leaf, segmentsOf(Coordinates.key(indexPath)).join('/'))
    }

    private static void fold(Counters counters, Anomalies a) {
        counters.unresolvable += a.unresolvable
        counters.unaddressed += a.unaddressed
        counters.declined += a.declined
        counters.neverPublished += a.neverPublished
    }

    /** The path segments of a join key {@code cas://<authority>/<a>/<b>}. */
    private static List<String> segmentsOf(String key) {
        final String prefix = Coordinates.SCHEME + '://'
        final String rest = key.substring(prefix.length())
        final int slash = rest.indexOf('/')
        if( slash < 0 )
            return Collections.<String> emptyList()
        final List<String> out = new ArrayList<String>()
        for( String s : rest.substring(slash + 1).split('/') )
            if( s )
                out.add(s)
        return out
    }
}
