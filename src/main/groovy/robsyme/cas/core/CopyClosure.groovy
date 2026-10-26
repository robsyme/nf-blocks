package robsyme.cas.core

import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * What `nf-blocks:copy` carries from one member to another (P3 plan 1.4 with
 * D7): the blocks, in the groups of 1.3, the runs whose records travel, and
 * the Claims on what travels. One walk computes the `all` closure and tags
 * each block and Claim with the shallowest depth that includes it, so
 * {@link Result#at} answers every depth without a second walk.
 *
 * Blocks are read from the source member's own store, so `missingAtSource`
 * is the source's truth. Lookups (runs by manifest, producers, join keys,
 * Claims and their log entries) go to the copy composition's index, which the
 * caller has caught up. Like Mark (DESIGN.md §19 decisions 1 to 4), a run's
 * metadata is its RunCompletion, RunManifest, script, InputSet, every
 * OutputCollection and every OutputItem; its content is every Leaf address,
 * each collection's `index` Leaf, and everything under a DirectoryManifest.
 * A carried Selection brings the Handoffs that cite it, with their files.
 */
@CompileStatic
class CopyClosure {

    static final String RESULTS = 'results'
    static final String LINEAGE = 'lineage'
    static final String ALL = 'all'
    static final List<String> DEPTHS = Collections.unmodifiableList([RESULTS, LINEAGE, ALL])

    /**
     * The groups of P3 plan 1.3, in order, with `handoffs` (P4): the Handoffs
     * citing a carried Selection and their files. `inputs` is for input
     * content, which no copy carries yet.
     */
    static final List<String> GROUPS = Collections.unmodifiableList(
        ['root', 'items', 'files', 'collections', 'run_records', 'lineage', 'run_outputs', 'inputs', 'handoffs', 'claims'])
    private static final int ROOT = 0, ITEMS = 1, FILES = 2, COLLECTIONS = 3, RUN_RECORDS = 4,
                             LINEAGE_FILES = 5, RUN_OUTPUTS = 6, INPUTS = 7, HANDOFFS = 8, CLAIMS = 9
    private static final int R = 0, L = 1, A = 2

    static final String SELECTION_ROOT = 'selection'
    static final String RUN_ROOT = 'run'

    /** `runs[].lineage`: which rule found a run's upstream items. */
    static final String TASKS = 'tasks'
    static final String JOIN_KEY = 'join_key'
    /** The item metadata path the join-key rule matches on. */
    static final String JOIN_PATH = 'meta.id'
    /** A RunManifest's `nf_run_hash` when Nextflow gave none. */
    static final String UNKNOWN_HASH = 'unknown'
    /** What an `nf_run_hash` the task rule reads by must look like. */
    private static final java.util.regex.Pattern HEX = java.util.regex.Pattern.compile('[0-9a-f]+')

    static final String NAMES = 'names'
    static final String PINS = 'pins'
    static final String PROJECTS = 'projects'
    static final String ANNOTATIONS = 'annotations'
    static final Set<String> CATEGORIES = Collections.unmodifiableSet(new LinkedHashSet<String>([NAMES, PINS, PROJECTS, ANNOTATIONS]))
    /** Claims about the source's own retention, which never travel (1.1). */
    static final String RETAIN = 'retain'
    static final String DELETION = 'deletion'
    /** Project policies (`policy.default_store`, `policy.retention`, `policy.people`) are `projects` (P4 plan 1.8). */
    private static final String POLICY_PREFIX = 'policy.'

    /** The kind of an Entry for a raw block. */
    static final String RAW = 'raw'

    private static final int THREADS = 16

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Root {
        /** {@link #SELECTION_ROOT} or {@link #RUN_ROOT}. */
        final String kind
        /** A Selection, or a RunCompletion. */
        final Cid cid

        Root(String kind, Cid cid) {
            if( kind != SELECTION_ROOT && kind != RUN_ROOT )
                throw new IllegalArgumentException("a copy root is a '${SELECTION_ROOT}' or a '${RUN_ROOT}', got '${kind}'")
            if( cid == null )
                throw new IllegalArgumentException('a copy root needs a cid')
            this.kind = kind
            this.cid = cid
        }
    }

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Entry {
        final Cid cid
        /** One of {@link #GROUPS}: the first group that claims the block. */
        final String group
        /** Bytes, or null when no size could be read. */
        final Long size
        /** The block's `kind`, or {@link #RAW}. */
        final String kind
        /** The shallowest depth that includes the block. */
        final String depth

        Entry(Cid cid, String group, Long size, String kind, String depth) {
            this.cid = cid
            this.group = group
            this.size = size
            this.kind = kind
            this.depth = depth
        }
    }

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Run {
        final Cid completion
        final Cid manifest
        final Cid script
        final String runName
        final String pipeline
        /** {@link #TASKS} or {@link #JOIN_KEY}; null in a results view. */
        final String lineage

        Run(Cid completion, Cid manifest, Cid script, String runName, String pipeline, String lineage) {
            this.completion = completion
            this.manifest = manifest
            this.script = script
            this.runName = runName
            this.pipeline = pipeline
            this.lineage = lineage
        }

        Run withLineage(String rule) { new Run(completion, manifest, script, runName, pipeline, rule) }
    }

    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class ClaimEntry {
        final Cid cid
        /** One of {@link #CATEGORIES}. A superseded Claim takes the category of the current one it is history of. */
        final String category
        final Cid subject
        final String verb
        final String attribute
        final Object value
        final String assertedBy
        final String timestamp
        /** The Claim block's own `supersedes`, sorted; empty when neither the source nor the index holds the Claim. */
        final List<Cid> supersedes
        final String depth
        /** True when the category was asked for, so the Claim is copied (P3 plan 1.3 amendment). */
        final boolean carried
        /** True when current for its subject under the source's own Claims; false for one listed only because a current one supersedes it. */
        final boolean current

        ClaimEntry(Cid cid, String category, Cid subject, String verb, String attribute, Object value,
                   String assertedBy, String timestamp, List<Cid> supersedes, String depth, boolean carried, boolean current) {
            this.cid = cid
            this.category = category
            this.subject = subject
            this.verb = verb
            this.attribute = attribute
            this.value = value
            this.assertedBy = assertedBy
            this.timestamp = timestamp
            this.supersedes = Collections.unmodifiableList(new ArrayList<Cid>(supersedes ?: Collections.<Cid> emptyList()))
            this.depth = depth
            this.carried = carried
            this.current = current
        }
    }

    /** One depth's totals: distinct blocks, the sum of their known sizes, and how many had none. */
    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Totals {
        final int blocks
        final long bytes
        final int unsized

        Totals(int blocks, long bytes, int unsized) {
            this.blocks = blocks
            this.bytes = bytes
            this.unsized = unsized
        }
    }

    /** Everything one walk found, each part tagged with its depth rank. */
    @CompileStatic
    private static final class Full {
        final List<Entry> entries = new ArrayList<Entry>()
        final Map<Cid, Integer> entryRank = new HashMap<Cid, Integer>()
        final List<Run> runs = new ArrayList<Run>()
        final List<ClaimEntry> claims = new ArrayList<ClaimEntry>()
        /** [rank, reason] per current Claim that never travels. */
        final List<Object[]> skipped = new ArrayList<Object[]>()
        /** Unaddressed Leaves first included at each rank. */
        final int[] unaddressed = new int[3]
        /** Closure cids the source lacks, in entry order. */
        final List<Cid> missing = new ArrayList<Cid>()
    }

    /**
     * The closure at one depth. Every field is that depth's view; {@link #at}
     * gives another depth's view of the same walk, and {@link #getDepths} the
     * totals of all three.
     */
    @CompileStatic
    static final class Result {
        final String depth
        private final Full full
        private final int rank

        private Result(String depth, Full full) {
            this.rank = rankOf(depth)
            this.depth = depth
            this.full = full
        }

        Result at(String other) { new Result(other, full) }

        /** Ordered and distinct, groups in 1.3 order. Only carried Claims are blocks here. */
        List<Entry> getEntries() {
            return full.entries.findAll { Entry e -> full.entryRank.get(e.cid) <= rank }
        }

        List<Run> getRuns() {
            return rank < L ? full.runs.collect { Run r -> r.withLineage(null) } : new ArrayList<Run>(full.runs)
        }

        /** Every current Claim of the four categories on closure subjects, and what it supersedes, carried or not. */
        List<ClaimEntry> getClaims() {
            return full.claims.findAll { ClaimEntry c -> rankOf(c.depth) <= rank }
        }

        /** Current Claims on this depth's subjects that never travel, by reason: `retain` and `deletion`. */
        Map<String, Integer> getSkippedClaims() {
            final Map<String, Integer> out = new LinkedHashMap<String, Integer>()
            out.put(RETAIN, 0)
            out.put(DELETION, 0)
            for( Object[] s : full.skipped )
                if( (Integer) s[0] <= rank )
                    out.put((String) s[1], out.get((String) s[1]) + 1)
            return out
        }

        /** Leaves with no address in the items whose files this depth copies. */
        int getUnaddressed() {
            int n = 0
            for( int i = 0; i <= rank; i++ )
                n += full.unaddressed[i]
            return n
        }

        List<Cid> getMissingAtSource() {
            return full.missing.findAll { Cid c -> full.entryRank.get(c) <= rank }
        }

        Totals getTotals() { totalsOf(getEntries()) }

        /** Each depth's totals, from the same walk, in {@link #DEPTHS} order. */
        Map<String, Totals> getDepths() {
            final Map<String, Totals> out = new LinkedHashMap<String, Totals>()
            for( String d : DEPTHS )
                out.put(d, at(d).totals)
            return out
        }

        private static Totals totalsOf(List<Entry> entries) {
            long bytes = 0
            int unsized = 0
            for( Entry e : entries ) {
                if( e.size == null ) unsized++
                else bytes += e.size
            }
            return new Totals(entries.size(), bytes, unsized)
        }
    }

    private static int rankOf(String depth) {
        final int i = DEPTHS.indexOf(depth)
        if( i < 0 )
            throw new IllegalArgumentException("a copy depth is one of ${DEPTHS.join(', ')}, got '${depth}'")
        return i
    }

    /**
     * The closure of {@code root} in {@code source}, viewed at {@code depth}.
     * {@code claimCategories} are those carried (a subset of {@link #CATEGORIES};
     * empty for `--claims none`). {@code nf} reads the source's Nextflow
     * records; null when it has none to offer, which sends every run to the
     * join-key rule.
     */
    static Result compute(BlockStore source, Index index, String fromAlias, Root root, String depth,
                          Set<String> claimCategories, LineageRecords nf) {
        rankOf(depth)
        for( String c : claimCategories ?: Collections.<String> emptySet() )
            if( !CATEGORIES.contains(c) )
                throw new IllegalArgumentException("a claim category is one of ${CATEGORIES.join(', ')}, got '${c}'")
        final Walk walk = new Walk(source, index, fromAlias, nf, claimCategories ?: Collections.<String> emptySet())
        try {
            return new Result(depth, walk.run(root))
        }
        finally {
            walk.close()
        }
    }

    /** A content address and the size its Leaf or directory entry gives (raw blocks only). */
    @CompileStatic
    private static final class Ref {
        final Cid cid
        final Long size

        Ref(Cid cid, Long size) {
            this.cid = cid
            this.size = size
        }
    }

    /** What the walk keeps of an OutputItem: its Leaf addresses and how many Leaves have none. */
    @CompileStatic
    private static final class Item {
        final List<Ref> leaves = new ArrayList<Ref>()
        int unaddressed
    }

    @CompileStatic
    private static final class Placed {
        final long seq
        int group
        int rank
        String kind
        Long hint

        Placed(long seq, int group, int rank, String kind, Long hint) {
            this.seq = seq
            this.group = group
            this.rank = rank
            this.kind = kind
            this.hint = hint
        }
    }

    @CompileStatic
    private static final class Listed {
        final String category
        boolean carried
        boolean current
        int rank

        Listed(String category, boolean carried, boolean current, int rank) {
            this.category = category
            this.carried = carried
            this.current = current
            this.rank = rank
        }
    }

    @CompileStatic
    private static final class Walk implements Closeable {

        private final BlockStore source
        private final Index index
        private final String fromAlias
        private final LineageRecords nf
        private final Set<String> carried
        private ExecutorService pool

        private final Map<Cid, Placed> placed = new LinkedHashMap<Cid, Placed>()
        private long seq
        /** Encoded lengths of the dag-cbor blocks read, and the cids the source lacks. */
        private final Map<Cid, Integer> lengths = new HashMap<Cid, Integer>()
        private final Set<Cid> absent = new HashSet<Cid>()
        /** Decoded records other than items: Selections, collections, completions, manifests. */
        private final Map<Cid, Map> records = new HashMap<Cid, Map>()
        private final Map<Cid, Item> items = new HashMap<Cid, Item>()
        /** Children of each DirectoryManifest read. */
        private final Map<Cid, List<Ref>> children = new HashMap<Cid, List<Ref>>()
        /** The rank at which each item's files enter the closure. */
        private final Map<Cid, Integer> itemContent = new HashMap<Cid, Integer>()
        /** The root Selection and every nested one, in walk order. */
        private final Set<Cid> selections = new LinkedHashSet<Cid>()

        Walk(BlockStore source, Index index, String fromAlias, LineageRecords nf, Set<String> carried) {
            this.source = source
            this.index = index
            this.fromAlias = fromAlias
            this.nf = nf
            this.carried = carried
        }

        Full run(Root root) {
            final Set<Cid> rootItems = new LinkedHashSet<Cid>()
            final Map<Cid, Set<Cid>> itemRuns = new LinkedHashMap<Cid, Set<Cid>>()
            final Set<Cid> runs = new TreeSet<Cid>()
            if( root.kind == SELECTION_ROOT )
                selectionTree(root.cid, rootItems, itemRuns, runs)
            else {
                place(root.cid, ROOT, R, Records.RUN_COMPLETION, null)
                runs.add(root.cid)
            }

            final Map<Cid, Set<Cid>> runItems = new LinkedHashMap<Cid, Set<Cid>>()
            final Map<Cid, List<Ref>> runIndexLeaves = new LinkedHashMap<Cid, List<Ref>>()
            final Map<Cid, Map> manifests = new LinkedHashMap<Cid, Map>()
            final List<Run> found = new ArrayList<Run>()
            for( Cid run : runs )
                found.add(runRecords(run, root.kind == RUN_ROOT, runItems, runIndexLeaves, manifests))
            if( root.kind == RUN_ROOT ) {
                rootItems.addAll(runItems.get(root.cid))
                for( Cid item : rootItems )
                    itemRuns.put(item, Collections.singleton(root.cid))
            }

            // results: the root's files (a run root's index files too)
            for( Cid item : rootItems )
                itemFiles(item, FILES, R)
            if( root.kind == RUN_ROOT )
                content(runIndexLeaves.get(root.cid), FILES, R)

            // lineage, per run, one TaskLineage walk each
            final Full full = new Full()
            for( Run run : found ) {
                final Set<Cid> resultsItems = new LinkedHashSet<Cid>()
                for( Cid item : rootItems )
                    if( itemRuns.get(item)?.contains(run.completion) )
                        resultsItems.add(item)
                final Set<Cid> upstream = new LinkedHashSet<Cid>()
                final String rule = lineage(run, manifests.get(run.completion), resultsItems,
                    runItems.get(run.completion) ?: Collections.<Cid> emptySet(), upstream)
                full.runs.add(run.withLineage(rule))
                readItems(upstream)
                for( Cid item : upstream ) {
                    // An item only the index lists for the run (its collection is not in the source) needs its record too.
                    place(item, RUN_RECORDS, L, Records.OUTPUT_ITEM, null)
                    itemFiles(item, LINEAGE_FILES, L)
                }
            }

            // all: everything else the runs stored
            for( Run run : found ) {
                for( Cid item : runItems.get(run.completion) ?: Collections.<Cid> emptySet() )
                    itemFiles(item, RUN_OUTPUTS, A)
                content(runIndexLeaves.get(run.completion), RUN_OUTPUTS, A)
            }

            for( Map.Entry<Cid, Integer> e : itemContent.entrySet() ) {
                final Item item = items.get(e.key)
                if( item != null )
                    full.unaddressed[e.value] += item.unaddressed
            }
            final Map<Cid, List<ClaimState.Row>> logged = index.claimsLoggedIn(fromAlias)
            handoffs(logged)
            claims(full, logged)
            finish(full)
            return full
        }

        /** The root Selection and every nested one (all in `root`), their items and the collections they were picked through. */
        private void selectionTree(Cid rootCid, Set<Cid> rootItems, Map<Cid, Set<Cid>> itemRuns, Set<Cid> runs) {
            final Map<Cid, Set<Cid>> viaOf = new LinkedHashMap<Cid, Set<Cid>>()
            final Set<Cid> seen = new HashSet<Cid>()
            List<Cid> level = [rootCid]
            while( !level.isEmpty() ) {
                final List<Cid> next = new ArrayList<Cid>()
                readRecords(level)
                for( Cid sel : level ) {
                    if( !seen.add(sel) )
                        continue
                    selections.add(sel)
                    place(sel, ROOT, R, Records.SELECTION, null)
                    final Map block = records.get(sel)
                    if( block == null || Records.kindOf(block) != Records.SELECTION )
                        continue
                    for( Object m : asList(block.get('members')) ) {
                        if( !(m instanceof Map) )
                            continue
                        final Map member = (Map) m
                        if( member.get('item') instanceof Map ) {
                            final Map item = (Map) member.get('item')
                            final Cid address = asCid(item.get('address'))
                            if( address == null )
                                continue
                            rootItems.add(address)
                            final Set<Cid> via = viaOf.computeIfAbsent(address) { Cid c -> new LinkedHashSet<Cid>() }
                            for( Object v : asList(item.get('via')) )
                                if( asCid(v) != null )
                                    via.add(asCid(v))
                        }
                        else if( asCid(member.get('selection')) != null && !seen.contains(asCid(member.get('selection'))) )
                            next.add(asCid(member.get('selection')))
                    }
                }
                level = next
            }

            readItems(rootItems)
            final Set<Cid> collections = new LinkedHashSet<Cid>()
            final Set<Cid> viaManifests = new LinkedHashSet<Cid>()
            for( Cid item : rootItems ) {
                place(item, ITEMS, R, Records.OUTPUT_ITEM, null)
                final Set<Cid> through = itemRuns.computeIfAbsent(item) { Cid c -> new LinkedHashSet<Cid>() }
                final Set<Cid> via = viaOf.get(item)
                if( via.isEmpty() ) {
                    // Picked by a query: the item is reached through every output that lists it.
                    for( Map.Entry<Cid, Cid> e : index.outputCollectionsOf(item).entrySet() ) {
                        collections.add(e.key)
                        through.add(e.value)
                    }
                    continue
                }
                collections.addAll(via)
                readRecords(via)
                for( Cid v : via ) {
                    final Cid run = runOfCollection(v, item)
                    if( run != null )
                        through.add(run)
                    // Also when the index knows the run: the source may lack its RunCompletion, and then
                    // runRecords places nothing of it (final review item 7).
                    if( manifestOf(v) != null )
                        viaManifests.add(manifestOf(v))
                }
            }
            for( Cid c : collections )
                place(c, COLLECTIONS, R, Records.OUTPUT_COLLECTION, null)
            // Mark follows `run` from the collection (META), whether or not a RunCompletion here names the manifest.
            for( Cid m : viaManifests )
                manifestRecords(m)
            for( Set<Cid> through : itemRuns.values() )
                runs.addAll(through)
        }

        /** collection -> `run` (RunManifest) -> Index.runByManifest; the index's row when the source lacks the collection. */
        private Cid runOfCollection(Cid collection, Cid item) {
            final Map block = records.get(collection)
            if( block != null && Records.kindOf(block) == Records.OUTPUT_COLLECTION ) {
                final Cid manifest = asCid(block.get('run'))
                return manifest == null ? null : index.runByManifest(manifest).orElse(null)
            }
            return index.outputCollectionsOf(item).get(collection)
        }

        /** The `run` link of a collection the source holds; null otherwise. */
        private Cid manifestOf(Cid collection) {
            final Map block = records.get(collection)
            return block != null && Records.kindOf(block) == Records.OUTPUT_COLLECTION ? asCid(block.get('run')) : null
        }

        /** Places a RunManifest and its script in run_records; the manifest as read, or null when the source has none. */
        private Map manifestRecords(Cid manifestCid) {
            place(manifestCid, RUN_RECORDS, R, Records.RUN_MANIFEST, null)
            readRecords([manifestCid])
            final Map manifest = records.get(manifestCid)
            if( manifest == null || Records.kindOf(manifest) != Records.RUN_MANIFEST )
                return null
            final Cid script = asCid(manifest.get('script'))
            if( script != null )
                place(script, RUN_RECORDS, R, script.isRaw() ? RAW : null, null)
            return manifest
        }

        /** Places a run's records (D7: every collection and item, records only) and returns what the plan says of it. */
        private Run runRecords(Cid run, boolean isRoot, Map<Cid, Set<Cid>> runItems, Map<Cid, List<Ref>> runIndexLeaves,
                               Map<Cid, Map> manifests) {
            final Set<Cid> allItems = new LinkedHashSet<Cid>()
            final List<Ref> indexLeaves = new ArrayList<Ref>()
            runItems.put(run, allItems)
            runIndexLeaves.put(run, indexLeaves)
            place(run, RUN_RECORDS, R, Records.RUN_COMPLETION, null)
            readRecords([run])
            final Map completion = records.get(run)
            if( completion == null || Records.kindOf(completion) != Records.RUN_COMPLETION )
                return new Run(run, null, null, null, null, null)
            final Cid manifestCid = asCid(completion.get('run'))
            final Map manifest = manifestCid == null ? null : manifestRecords(manifestCid)
            if( manifest != null )
                manifests.put(run, manifest)
            final Cid script = asCid(manifest?.get('script'))
            final Cid inputSet = asCid(completion.get('input_set'))
            if( inputSet != null ) {
                // Run metadata, as Mark has it (META), so it travels at every depth.
                // InputSet is reserved, so nothing under it is followed.
                place(inputSet, RUN_RECORDS, R, null, null)
                readRecords([inputSet])
                final Map block = records.get(inputSet)
                if( block != null )
                    placed.get(inputSet).kind = Records.kindOf(block)
            }
            final List<Cid> collections = asList(completion.get('collections')).collect { Object c -> asCid(c) }.findAll { it != null } as List<Cid>
            readRecords(collections)
            for( Cid c : collections ) {
                place(c, isRoot ? COLLECTIONS : RUN_RECORDS, R, Records.OUTPUT_COLLECTION, null)
                final Map block = records.get(c)
                if( block == null || Records.kindOf(block) != Records.OUTPUT_COLLECTION )
                    continue
                for( Object i : asList(block.get('items')) )
                    if( asCid(i) != null )
                        allItems.add(asCid(i))
                if( block.get('index') instanceof Map )
                    indexLeaves.addAll(refsOf(((Map) block.get('index')).get('leaf'), new Item()))
            }
            readItems(allItems)
            for( Cid item : allItems )
                place(item, isRoot ? ITEMS : RUN_RECORDS, R, Records.OUTPUT_ITEM, null)
            return new Run(run, manifestCid, script, text(manifest?.get('run_name')), text(manifest?.get('pipeline')), null)
        }

        /**
         * The run's items upstream of its results items, into {@code out}, and
         * the rule used. The task rule when the source holds the run's `nf/`
         * records; else the join key, items sharing a `meta.id` value.
         */
        private String lineage(Run run, Map manifest, Set<Cid> resultsItems, Set<Cid> runItems, Set<Cid> out) {
            final String hash = text(manifest?.get('nf_run_hash'))
            // Hex only: the hash comes from block content and names a directory under nf/ (final review item 8).
            final boolean hashed = nf != null && hash && hash != UNKNOWN_HASH && HEX.matcher(hash).matches()
            if( resultsItems.containsAll(runItems) )
                // Nothing left to add (a run root): only which rule would apply.
                return hashed && !nf.keysUnder(hash + '/').isEmpty() ? TASKS : JOIN_KEY
            if( hashed ) {
                final Set<Cid> addresses = new LinkedHashSet<Cid>()
                for( Cid item : resultsItems )
                    for( Ref ref : items.get(item)?.leaves ?: Collections.<Ref> emptyList() )
                        addresses.add(ref.cid)
                final TaskLineage.Result found = TaskLineage.upstream(nf, hash, addresses)
                if( found.available ) {
                    for( Cid address : found.upstream )
                        for( ProducerRow p : index.producersOf(address) )
                            if( p.completionCid == run.completion && !resultsItems.contains(p.itemCid) )
                                out.add(p.itemCid)
                    return TASKS
                }
            }
            final Set<AttrRow> keys = new LinkedHashSet<AttrRow>()
            for( Cid item : resultsItems )
                keys.addAll(index.attrsOf(item, JOIN_PATH))
            for( AttrRow key : keys )
                for( Cid item : index.runItemsWith(run.completion, JOIN_PATH, key.type, key.value) )
                    if( !resultsItems.contains(item) )
                        out.add(item)
            return JOIN_KEY
        }

        private void itemFiles(Cid item, int group, int rank) {
            final Integer had = itemContent.get(item)
            if( had == null || rank < had )
                itemContent.put(item, rank)
            final Item info = items.get(item)
            if( info != null )
                content(info.leaves, group, rank)
        }

        /** Places content level by level, reading each DirectoryManifest once and passing a better placement down to what is under it. */
        private void content(List<Ref> start, int group, int rank) {
            List<Ref> frontier = start ?: Collections.<Ref> emptyList()
            while( !frontier.isEmpty() ) {
                final List<Cid> expand = new ArrayList<Cid>()
                for( Ref ref : frontier ) {
                    final boolean raw = ref.cid.isRaw()
                    if( place(ref.cid, group, rank, raw ? RAW : Records.DIRECTORY_MANIFEST, raw ? ref.size : null) && ref.cid.isDagCbor() )
                        expand.add(ref.cid)
                }
                readManifests(expand.findAll { Cid c -> !children.containsKey(c) })
                final List<Ref> next = new ArrayList<Ref>()
                for( Cid m : expand )
                    next.addAll(children.get(m) ?: Collections.<Ref> emptyList())
                frontier = next
            }
        }

        /**
         * The Handoffs citing a Selection of the tree that the source's own
         * log announced (P4 plan 1.5), each with its files, in `handoffs` at
         * every depth: the Handoff is the Selection's citation, and its
         * command reads its files (`--input cas://<cid>/samplesheet.csv`).
         * One that the source's Claims leave deleted, cleanly and unpinned as
         * Mark reads it, stays behind, since deletions never travel and it
         * would arrive undeleted. Its `project` link is not followed (R5).
         */
        private void handoffs(Map<Cid, List<ClaimState.Row>> logged) {
            final Map<Cid, List<Cid>> citing = index.handoffsLoggedIn(fromAlias)
            final List<Cid> found = new ArrayList<Cid>()
            for( Cid selection : selections )
                for( Cid handoff : citing.get(selection) ?: Collections.<Cid> emptyList() ) {
                    final ClaimState state = ClaimState.of(logged.get(handoff) ?: Collections.<ClaimState.Row> emptyList())
                    if( state.deletion != ClaimState.DELETED || state.pinned )
                        found.add(handoff)
                }
            readRecords(found)
            for( Cid handoff : found ) {
                // A Handoff the source lacks is placed all the same, so it is listed missing at the source.
                place(handoff, HANDOFFS, R, Records.HANDOFF, null)
                final Map block = records.get(handoff)
                if( block == null || Records.kindOf(block) != Records.HANDOFF )
                    continue
                final List<Ref> files = new ArrayList<Ref>()
                for( Object leaf : asList(block.get('files')) )
                    files.addAll(refsOf(leaf, new Item()))
                content(files, HANDOFFS, R)
            }
        }

        /**
         * The Claims on closure subjects. Current per ClaimState over the
         * Claims the source's own log announced; each current Claim of the four
         * categories is listed with what it supersedes, transitively, carried
         * when its category was asked for. retain and deletion are counted, never listed.
         *
         * With `projects` carried, each Project a carried membership names
         * travels too (P4 plan 1.8), in `claims` at its shallowest membership's
         * depth, and is a subject in turn, so a destination opened alone can
         * name the project and show its policies and annotations.
         */
        private void claims(Full full, Map<Cid, List<ClaimState.Row>> logged) {
            final Map<Cid, Listed> listed = new LinkedHashMap<Cid, Listed>()
            final Map<Cid, Claim> known = new HashMap<Cid, Claim>()
            final Map<Cid, Integer> projects = new LinkedHashMap<Cid, Integer>()
            for( Map.Entry<Cid, Placed> e : new ArrayList<Map.Entry<Cid, Placed>>(placed.entrySet()) )
                claimsOn(e.key, e.value.rank, logged, listed, known, full, projects)
            readRecords(projects.keySet())
            for( Map.Entry<Cid, Integer> e : projects.entrySet() ) {
                final Map block = records.get(e.key)
                // A Project the source lacks is placed all the same, so it is listed missing at the source.
                if( !absent.contains(e.key) && (block == null || Records.kindOf(block) != Records.PROJECT) )
                    continue
                if( place(e.key, CLAIMS, e.value, Records.PROJECT, null) )
                    claimsOn(e.key, placed.get(e.key).rank, logged, listed, known, full, null)
            }
            for( Map.Entry<Cid, Listed> e : listed.entrySet() ) {
                final Claim claim = known.get(e.key)
                final Listed l = e.value
                full.claims.add(new ClaimEntry(e.key, l.category, claim?.subject, claim?.verb, claim?.attribute, claim?.value,
                    claim?.assertedBy, claim?.timestamp, claim?.supersedes, DEPTHS[l.rank], l.carried, l.current))
                if( l.carried )
                    place(e.key, CLAIMS, l.rank, Records.CLAIM, null)
            }
        }

        /**
         * Lists the Claims on one subject at {@code rank}. A carried current
         * `add project` whose value links a block goes into {@code projects}
         * (null: not looked for) at the shallowest rank that names it.
         */
        private void claimsOn(Cid subject, int rank, Map<Cid, List<ClaimState.Row>> logged, Map<Cid, Listed> listed,
                              Map<Cid, Claim> known, Full full, Map<Cid, Integer> projects) {
            final List<ClaimState.Row> rows = logged.get(subject)
            if( rows == null )
                return
            for( ClaimState.Row row : ClaimState.of(rows).current ) {
                final String category = categoryOf(row.attribute)
                if( category == RETAIN || category == DELETION ) {
                    full.skipped.add([rank, category] as Object[])
                    continue
                }
                final Cid head = Cid.parse(row.cid)
                // History travels only with its head: a copy that wrote what the current
                // Claim supersedes, without it, would make an older value current there.
                final Claim current = claimAt(head, known)
                final boolean carry = carried.contains(category) && !absent.contains(head)
                if( projects != null && carry && current?.attribute == Claim.PROJECT && current.verb == Claim.ADD
                        && current.value instanceof Cid && ((Cid) current.value).isDagCbor() ) {
                    final Integer had = projects.get((Cid) current.value)
                    projects.put((Cid) current.value, had == null ? rank : Math.min(had, rank))
                }
                final ArrayDeque<Cid> todo = new ArrayDeque<Cid>()
                todo.add(head)
                while( !todo.isEmpty() ) {
                    final Cid c = todo.poll()
                    final boolean isCurrent = c == head
                    final Listed had = listed.get(c)
                    if( had != null && (had.carried || !carry) && (had.current || !isCurrent) && had.rank <= rank )
                        continue
                    if( had == null )
                        listed.put(c, new Listed(category, carry, isCurrent, rank))
                    else {
                        had.carried = had.carried || carry
                        had.current = had.current || isCurrent
                        had.rank = Math.min(had.rank, rank)
                    }
                    final Claim claim = claimAt(c, known)
                    if( claim != null )
                        todo.addAll(claim.supersedes)
                }
            }
        }

        /** A Claim read from the source, else as the index holds it; null when neither has it. */
        private Claim claimAt(Cid cid, Map<Cid, Claim> known) {
            if( known.containsKey(cid) )
                return known.get(cid)
            readRecords([cid])
            Claim claim = null
            final Map block = records.get(cid)
            if( block != null ) {
                try {
                    claim = Claim.fromCbor(block)
                }
                catch( RuntimeException e ) {
                    claim = null
                }
            }
            if( claim == null )
                claim = index.claimOf(cid)
            known.put(cid, claim)
            return claim
        }

        private static String categoryOf(String attribute) {
            switch( attribute ) {
                case null: return DELETION
                case Claim.RETAIN: return RETAIN
                case Claim.PIN: return PINS
                case Claim.NAME: return NAMES
                case Claim.PROJECT: return PROJECTS
                case Claim.SAMPLE:
                case Claim.ANNOTATION:
                case Claim.SAME_SAMPLE: return ANNOTATIONS
                // policy.default_store, policy.retention, policy.people, and any later policy.
                default: return attribute.startsWith(POLICY_PREFIX) ? PROJECTS : ANNOTATIONS
            }
        }

        /** Sizes and presence, then the entries in group order. */
        private void finish(Full full) {
            final List<Cid> unread = new ArrayList<Cid>()
            final Map<Cid, Long> sizes = new HashMap<Cid, Long>()
            for( Map.Entry<Cid, Placed> e : placed.entrySet() ) {
                final Cid cid = e.key
                Long size = index.blockSize(cid)
                if( size == null )
                    size = e.value.hint
                if( size == null && lengths.containsKey(cid) )
                    size = (long) lengths.get(cid)
                if( size != null )
                    sizes.put(cid, size)
                if( !lengths.containsKey(cid) && !absent.contains(cid) )
                    unread.add(cid)
            }
            // Never read: ask the source whether it holds them, and their size when nothing else said.
            final Map<Cid, Future<Long>> pending = new LinkedHashMap<Cid, Future<Long>>()
            for( Cid c : unread ) {
                final Cid cid = c
                final boolean sized = sizes.containsKey(cid)
                pending.put(cid, pool().submit({ -> !source.has(cid) ? -1L : sized ? 0L : source.size(cid) } as Callable<Long>))
            }
            for( Map.Entry<Cid, Future<Long>> e : pending.entrySet() ) {
                final long answer = await(e.value)
                if( answer < 0 )
                    absent.add(e.key)
                else if( !sizes.containsKey(e.key) )
                    sizes.put(e.key, answer)
            }
            final List<Map.Entry<Cid, Placed>> ordered = new ArrayList<Map.Entry<Cid, Placed>>(placed.entrySet())
            ordered.sort { Map.Entry<Cid, Placed> a, Map.Entry<Cid, Placed> b ->
                a.value.group <=> b.value.group ?: a.value.seq <=> b.value.seq
            }
            for( Map.Entry<Cid, Placed> e : ordered ) {
                full.entries.add(new Entry(e.key, GROUPS[e.value.group], sizes.get(e.key), e.value.kind, DEPTHS[e.value.rank]))
                full.entryRank.put(e.key, e.value.rank)
                if( absent.contains(e.key) )
                    full.missing.add(e.key)
            }
        }

        /** Records the block in the first group and shallowest depth that claim it; true when that is new or better. */
        private boolean place(Cid cid, int group, int rank, String kind, Long hint) {
            final Placed had = placed.get(cid)
            if( had == null ) {
                placed.put(cid, new Placed(seq++, group, rank, kind, hint))
                return true
            }
            if( had.hint == null )
                had.hint = hint
            if( group >= had.group && rank >= had.rank )
                return false
            had.group = Math.min(had.group, group)
            had.rank = Math.min(had.rank, rank)
            return true
        }

        private void readItems(Collection<Cid> cids) {
            final Map<Cid, Object> read = readAll(cids.findAll { Cid c -> !items.containsKey(c) && !absent.contains(c) })
            for( Map.Entry<Cid, Object> e : read.entrySet() ) {
                final Item item = new Item()
                if( e.value instanceof Map && Records.kindOf((Map) e.value) == Records.OUTPUT_ITEM )
                    for( Map leaf : Index.leavesOf(((Map) e.value).get('value')) )
                        item.leaves.addAll(refsOf(leaf, item))
                items.put(e.key, item)
            }
        }

        private void readRecords(Collection<Cid> cids) {
            final Map<Cid, Object> read = readAll(cids.findAll { Cid c -> !records.containsKey(c) && !absent.contains(c) })
            for( Map.Entry<Cid, Object> e : read.entrySet() )
                records.put(e.key, e.value instanceof Map ? (Map) e.value : Collections.emptyMap())
        }

        /** Reads each DirectoryManifest's children; one the source lacks has none, so it is never asked for again. */
        private void readManifests(Collection<Cid> cids) {
            for( Cid c : cids )
                if( absent.contains(c) )
                    children.put(c, Collections.<Ref> emptyList())
            final Map<Cid, Object> read = readAll(cids.findAll { Cid c -> !children.containsKey(c) })
            for( Map.Entry<Cid, Object> e : read.entrySet() ) {
                final List<Ref> refs = new ArrayList<Ref>()
                if( e.value instanceof Map ) {
                    final Map block = (Map) e.value
                    final String kind = Records.kindOf(block)
                    if( kind != null )
                        placed.get(e.key)?.kind = kind
                    for( Object each : asList(block.get('entries')) ) {
                        if( !(each instanceof Map) )
                            continue
                        final Map entry = (Map) each
                        final Cid address = asCid(entry.get('address'))
                        if( address != null )
                            refs.add(new Ref(address, address.isRaw() ? asLong(entry.get('size')) : null))
                    }
                }
                children.put(e.key, refs)
            }
            for( Cid c : cids )
                if( !children.containsKey(c) )
                    children.put(c, Collections.<Ref> emptyList())
        }

        /** A Leaf's address as a Ref; an unaddressed Leaf counts on the item instead. */
        private static List<Ref> refsOf(Object leaf, Item item) {
            if( !(leaf instanceof Map) )
                return Collections.<Ref> emptyList()
            final Cid address = asCid(((Map) leaf).get('address'))
            if( address == null ) {
                item.unaddressed++
                return Collections.<Ref> emptyList()
            }
            return [new Ref(address, address.isRaw() ? asLong(((Map) leaf).get('size')) : null)]
        }

        /**
         * Reads and decodes dag-cbor blocks from the source, up to THREADS at
         * once, recording each one's encoded length, or its absence. Any other
         * failure is thrown: a block that is there but unreadable is not missing.
         */
        private Map<Cid, Object> readAll(Collection<Cid> cids) {
            final Map<Cid, Object> out = new LinkedHashMap<Cid, Object>()
            final Map<Cid, Future<byte[]>> pending = new LinkedHashMap<Cid, Future<byte[]>>()
            for( Cid c : cids ) {
                final Cid cid = c
                if( !cid.isDagCbor() || pending.containsKey(cid) )
                    continue
                pending.put(cid, pool().submit({ -> bytesOf(cid) } as Callable<byte[]>))
            }
            for( Map.Entry<Cid, Future<byte[]>> e : pending.entrySet() ) {
                final byte[] bytes = await(e.value)
                if( bytes == null ) {
                    absent.add(e.key)
                    continue
                }
                lengths.put(e.key, bytes.length)
                out.put(e.key, DagCbor.decode(bytes))
            }
            return out
        }

        private byte[] bytesOf(Cid cid) {
            final InputStream in
            try {
                in = source.open(cid)
            }
            catch( NoSuchBlockException e ) {
                return null
            }
            try {
                return in.readAllBytes()
            }
            finally {
                in.close()
            }
        }

        private ExecutorService pool() {
            if( pool == null )
                pool = Executors.newFixedThreadPool(THREADS)
            return pool
        }

        @Override
        void close() {
            if( pool != null ) {
                pool.shutdownNow()
                pool = null
            }
        }

        private static <T> T await(Future<T> future) {
            try {
                return future.get()
            }
            catch( ExecutionException e ) {
                final Throwable cause = e.getCause()
                if( cause instanceof RuntimeException )
                    throw (RuntimeException) cause
                if( cause instanceof Error )
                    throw (Error) cause
                throw new UncheckedIOException(cause instanceof IOException ? (IOException) cause : new IOException(cause))
            }
            catch( InterruptedException e ) {
                Thread.currentThread().interrupt()
                throw new RuntimeException(e)
            }
        }
    }

    private static List asList(Object value) {
        return value instanceof List ? (List) value : Collections.emptyList()
    }

    private static Cid asCid(Object value) {
        return value instanceof Cid ? (Cid) value : null
    }

    private static Long asLong(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : null
    }

    private static String text(Object value) {
        return value == null ? null : value.toString()
    }
}
