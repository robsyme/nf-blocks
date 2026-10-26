package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Per-store storage accounting for the v4 snapshot (P2 rulings R1 to R5).
 * Pure: the snapshot build hands it rows and writes the results, each one a
 * row of a v4 table in {@link Index#ddl}, keyed by that table's column names.
 *
 * A root's closure is the raw cids reachable from its seeds through `refs`,
 * items, manifests and Handoffs all expanding, with a visited set so a
 * manifest cycle ends. Only raw (file) blocks are counted (R1). Each closure
 * is computed once; every aggregate then comes from what each raw cid is
 * reached by.
 */
@CompileStatic
final class StorageStats {

    static final String RUN = 'run'
    static final String SELECTION = 'selection'
    static final String HANDOFF = 'handoff'
    static final String PIN = 'pin'

    static final String HELD = 'held'
    static final String RELEASED = 'released'
    static final String DELETED = 'deleted'
    static final String PINNED = 'pinned'

    /** The asserter key of a root with no `assertedBy`. */
    static final String ANONYMOUS = 'anonymous'

    /** How many other content roots `root_shared` keeps per root. */
    static final int SHARED_TOP = 5

    /**
     * One root (R3). `pipeline` is set on runs only. `seeds` are expanded by
     * the caller: a run's output items, a Selection's items through its child
     * Selections, a Handoff's own cid (an owner of its files in `refs`) and
     * its Selection's items, and for a pin the seeds of what it pins (a raw
     * or manifest cid seeds itself).
     */
    @Canonical
    @CompileStatic
    static class Root {
        int id
        String cid
        String kind
        String assertedBy
        String state
        String since
        String pipeline
        List<String> seeds
    }

    /**
     * One current membership Claim (P4 plan 1.2): `subject`, a RunCompletion,
     * Selection or OutputItem, is in `project`.
     */
    @Canonical
    @CompileStatic
    static class Membership {
        String project
        String subject
    }

    final Map store
    final List<Map> rootStats
    final List<Map> rootShared
    final List<Map> asserters
    final List<Map> pipelines
    final List<Map> collectionSizes
    final List<Map> itemSizes
    final List<Map> kinds
    final List<Map> pinnedOverlap
    final List<Map> projects

    private StorageStats(Map store, List<Map> rootStats, List<Map> rootShared, List<Map> asserters, List<Map> pipelines,
                         List<Map> collectionSizes, List<Map> itemSizes, List<Map> kinds, List<Map> pinnedOverlap, List<Map> projects) {
        this.store = store
        this.rootStats = rootStats
        this.rootShared = rootShared
        this.asserters = asserters
        this.pipelines = pipelines
        this.collectionSizes = collectionSizes
        this.itemSizes = itemSizes
        this.kinds = kinds
        this.pinnedOverlap = pinnedOverlap
        this.projects = projects
    }

    /** What reaches one raw cid. Root numbers are positions in the roots list. */
    @CompileStatic
    private static final class Reach {
        final List<Integer> contentRoots = new ArrayList<>(2)
        final Set<String> contentAsserters = new HashSet<>(2)
        final Set<String> otherAsserters = new HashSet<>(2)
        final Set<String> pinAsserters = new TreeSet<>()
        int runRoots
        boolean held

        boolean isPinned() { !pinAsserters.isEmpty() }
        boolean isHeldOnly() { held && !pinned }
        boolean isReleasing() { !held && !pinned }
    }

    /**
     * @param rawSizes every raw cid with its size, which may be null
     * @param refs owner (item, manifest or Handoff) to the cids it references, multiplicity kept
     * @param names raw cid to the names it is referenced by
     * @param collectionItems output collection to its item cids, multiplicity kept
     * @param manifests the dag-cbor owners in `refs`; every other owner is an item, or a Handoff root's cid
     */
    static StorageStats compute(Map<String, Long> rawSizes, Map<String, List<String>> refs, Map<String, List<String>> names,
                                List<Root> roots, Map<String, List<String>> collectionItems, Set<String> manifests) {
        compute(rawSizes, refs, names, roots, collectionItems, manifests, Collections.<String> emptySet())
    }

    /**
     * As the 6-arg compute, with `absent` the blocks a sweep has taken from
     * the store: raw files, and DirectoryManifests. They are not stored, so
     * every figure but collection_size, which describes what the records
     * name, leaves them out. An absent manifest is a dead end: a warm cache
     * still holds its edges in `refs`, but a walk does not follow them, as
     * the sweep's mark cannot. A root whose walk meets one is partial, and so
     * is the store. item_size is what is stored here (follow-up B2).
     */
    static StorageStats compute(Map<String, Long> allSizes, Map<String, List<String>> refs, Map<String, List<String>> names,
                                List<Root> roots, Map<String, List<String>> collectionItems, Set<String> manifests,
                                Set<String> absent) {
        compute(allSizes, refs, names, roots, collectionItems, manifests, absent, Collections.<Membership> emptyList())
    }

    /** As the 7-arg compute, with the current membership Claims the projects figures come from. */
    static StorageStats compute(Map<String, Long> allSizes, Map<String, List<String>> refs, Map<String, List<String>> names,
                                List<Root> roots, Map<String, List<String>> collectionItems, Set<String> manifests,
                                Set<String> absent, List<Membership> memberships) {
        // A cid missing from rawSizes is no raw block, and no owner either, so a closure stops there.
        final Map<String, Long> rawSizes = absent ? allSizes.findAll { String cid, Long n -> !absent.contains(cid) } : allSizes
        // The absent owners: manifests a sweep took (items are metadata, which a sweep keeps).
        final Set<String> stops = absent ? absent.findAll { String cid -> refs.containsKey(cid) } as Set<String> : Collections.<String> emptySet()
        final boolean[] hits = new boolean[roots.size()]
        final List<Set<String>> closures = new ArrayList<Set<String>>(roots.size())
        roots.eachWithIndex { Root r, int i ->
            final boolean[] hit = new boolean[1]
            closures.add(closure(r.seeds ?: [], rawSizes, refs, stops, hit))
            hits[i] = hit[0]
        }
        final Map<String, Reach> reach = new HashMap<>()
        roots.eachWithIndex { Root root, int i ->
            final String who = asserter(root.assertedBy)
            final boolean content = isContent(root)
            for( String cid : closures[i] ) {
                Reach r = reach.get(cid)
                if( r == null ) { r = new Reach(); reach.put(cid, r) }
                if( content ) {
                    r.contentRoots.add(i)
                    r.contentAsserters.add(who)
                }
                else {
                    r.otherAsserters.add(who)
                }
                if( root.kind == RUN ) r.runRoots++
                if( root.kind == PIN ) r.pinAsserters.add(who)
                if( root.state == HELD ) r.held = true
            }
        }
        final Sizes sizes = new Sizes(rawSizes)
        final SortedSet<String> items = items(refs, collectionItems, manifests, roots)
        new StorageStats(
            storeRow(reach, sizes, referenced(collectionItems, roots, rawSizes, refs, stops), hits.any { boolean b -> b }),
            rootRows(roots, closures, reach, sizes, hits),
            sharedRows(roots, reach, sizes),
            asserterRows(roots, reach, sizes),
            pipelineRows(roots, closures, reach, sizes),
            collectionRows(collectionItems, allSizes, refs, new Sizes(allSizes)),
            itemRows(items, rawSizes, refs, stops, sizes),
            kindRows(reach.keySet(), names, sizes),
            overlapRows(roots, closures, collectionItems, items, reach, rawSizes, refs, stops, sizes),
            projectRows(memberships, roots, closures, collectionItems, reach, rawSizes, refs, stops, sizes))
    }

    /**
     * P4 plan 1.4: per project, the union of the raw blocks its members here
     * reach, each block once however many members reach it, split as
     * store_stats splits the store: pinned when a pin root reaches it, else
     * held when a held root does, else releasing. So reachable is pinned +
     * held + releasing. A member is here when it is a run or Selection root
     * (any state), or an item of an output collection here; it reaches what
     * its root reaches, or for an item what the item reaches, which is what
     * root_stats counts. `roots` is how many of a project's members are here.
     * A project with none here has no row. The member closures are the
     * roots' own, so this adds one walk over them per (project, member) and
     * none over `refs`, except an item member's own closure, walked once.
     */
    private static List<Map> projectRows(List<Membership> memberships, List<Root> roots, List<Set<String>> closures,
                                         Map<String, List<String>> collectionItems, Map<String, Reach> reach,
                                         Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> stops, Sizes sizes) {
        final List<Map> rows = []
        if( !memberships ) return rows
        final Map<String, Set<String>> under = new HashMap<>()
        roots.eachWithIndex { Root root, int i ->
            if( root.kind == RUN || root.kind == SELECTION ) under.put(root.cid, closures[i])
        }
        final Set<String> items = new HashSet<>()
        for( List<String> members : collectionItems.values() ) items.addAll(members)
        final Map<String, Set<String>> membersOf = new TreeMap<>()
        for( Membership m : memberships )
            membersOf.computeIfAbsent(m.project) { new TreeSet<String>() }.add(m.subject)
        final Set<String> union = new HashSet<>()
        membersOf.each { String project, Set<String> subjects ->
            int here = 0
            union.clear()
            for( String subject : subjects ) {
                Set<String> blocks = under.get(subject)
                if( blocks == null && items.contains(subject) ) {
                    blocks = closure([subject], rawSizes, refs, stops, null)
                    under.put(subject, blocks)
                }
                if( blocks == null ) continue
                here++
                union.addAll(blocks)
            }
            if( here == 0 ) return
            long pinned = 0, held = 0, releasing = 0
            for( String cid : union ) {
                final Reach r = reach.get(cid)
                final long n = sizes.of(cid)
                if( r != null && r.pinned ) pinned += n
                else if( r != null && r.held ) held += n
                else releasing += n
            }
            rows << ([project_cid: project, reachable_bytes: pinned + held + releasing, pinned_bytes: pinned, held_bytes: held,
                      releasing_bytes: releasing, roots: here] as Map)
        }
        rows
    }

    /** `web/src/sizes.js` `extOf`: the text after the last '.', lower case; '' for no dot, a leading dot only, or a trailing dot. */
    static String extOf(String name) {
        final String text = name ?: ''
        final int dot = text.lastIndexOf('.')
        dot <= 0 || dot == text.length() - 1 ? '' : text.substring(dot + 1).toLowerCase(Locale.ROOT)
    }

    /**
     * The per-person key, by the page's one principal rule (`principalKey` in
     * web/src/identity.js, SQL's lower(trim(x, char(32,9,10,13,12,11)))): ASCII
     * whitespace off both ends, ASCII letters lowercased, nothing else changed,
     * whatever the JVM's locale; `anonymous` when nothing is left.
     */
    static String asserter(String assertedBy) {
        final String key = asciiLower(asciiTrim(assertedBy ?: ''))
        key.isEmpty() ? ANONYMOUS : key
    }

    private static boolean asciiSpace(char c) { c == (char) 32 || (c >= (char) 9 && c <= (char) 13) }

    private static String asciiTrim(String s) {
        int from = 0, to = s.length()
        while( from < to && asciiSpace(s.charAt(from)) ) from++
        while( to > from && asciiSpace(s.charAt(to - 1)) ) to--
        s.substring(from, to)
    }

    private static String asciiLower(String s) {
        final char[] out = s.toCharArray()
        for( int i = 0; i < out.length; i++ )
            if( out[i] >= (char) 'A' && out[i] <= (char) 'Z' ) out[i] = Character.toLowerCase(out[i])
        new String(out)
    }

    /** Content roots (R3) are held and pinned roots. */
    static boolean isContent(Root root) {
        root.state == HELD || root.state == PINNED
    }

    /** The raw cids reachable from `seeds`. A raw cid is one in `rawSizes`; anything else expands through `refs`. */
    static Set<String> closure(Collection<String> seeds, Map<String, Long> rawSizes, Map<String, List<String>> refs) {
        closure(seeds, rawSizes, refs, Collections.<String> emptySet(), null)
    }

    /**
     * As the 3-arg closure, not expanding a cid in `stops` (a manifest a
     * sweep took). `hit`, when given, has its first element set once the walk
     * meets one.
     */
    static Set<String> closure(Collection<String> seeds, Map<String, Long> rawSizes, Map<String, List<String>> refs,
                               Set<String> stops, boolean[] hit) {
        final Set<String> out = new HashSet<>()
        final Set<String> seen = new HashSet<>()
        final Deque<String> todo = new ArrayDeque<>(seeds)
        while( !todo.isEmpty() ) {
            final String cid = todo.pop()
            if( !seen.add(cid) ) continue
            if( rawSizes.containsKey(cid) ) { out.add(cid); continue }
            if( stops.contains(cid) ) { if( hit != null ) hit[0] = true; continue }
            final List<String> next = refs.get(cid)
            if( next ) todo.addAll(next)
        }
        out
    }

    /**
     * What a sweep may have taken under `seeds`: the raw cids (in `rawSizes`)
     * and the manifests (in `manifests`) a walk reaches, not expanding a cid
     * in `stops`, which is itself in the result when reached.
     */
    static Set<String> content(Collection<String> seeds, Map<String, Long> rawSizes, Map<String, List<String>> refs,
                               Set<String> manifests, Set<String> stops) {
        final Set<String> out = new HashSet<>()
        final Set<String> seen = new HashSet<>()
        final Deque<String> todo = new ArrayDeque<>(seeds)
        while( !todo.isEmpty() ) {
            final String cid = todo.pop()
            if( !seen.add(cid) ) continue
            if( rawSizes.containsKey(cid) ) { out.add(cid); continue }
            if( manifests.contains(cid) ) out.add(cid)
            if( stops.contains(cid) ) continue
            final List<String> next = refs.get(cid)
            if( next ) todo.addAll(next)
        }
        out
    }

    @CompileStatic
    private static final class Sizes {
        final Map<String, Long> raw
        Sizes(Map<String, Long> raw) { this.raw = raw }
        long of(String cid) { final Long s = raw.get(cid); s == null ? 0L : s.longValue() }
        boolean unknown(String cid) { raw.get(cid) == null }
        long sum(Collection<String> cids) { long n = 0; for( String c : cids ) n += of(c); n }
        int partial(Collection<String> cids) { cids.any { String c -> unknown(c) } ? 1 : 0 }
    }

    /**
     * Every raw reference under every holder, multiplicity kept, directories
     * expanded. The holders are each (output collection, item) membership
     * (R2), each Handoff root, whose references are its file Leaves, and,
     * once each, every other cid a root starts from that none of those
     * reaches: an item of another member's run that a Selection here holds
     * (R8), or a pinned cid outside them. A cid under another holder of that
     * last kind is no holder itself, so a pin on a file of such an item adds
     * nothing. Every stored block is under a holder, so referenced is never
     * below stored (follow-up S3).
     */
    private static long referenced(Map<String, List<String>> collectionItems, List<Root> roots, Map<String, Long> rawSizes,
                                   Map<String, List<String>> refs, Set<String> stops) {
        final List<String> holders = new ArrayList<>()
        for( List<String> items : collectionItems.values() ) holders.addAll(items)
        for( Root root : roots )
            if( root.kind == HANDOFF ) holders.add(root.cid)
        final Set<String> covered = walked(holders, rawSizes, refs, stops)
        final Set<String> others = new TreeSet<>()
        for( Root root : roots )
            for( String seed : root.seeds ?: Collections.<String> emptyList() )
                if( !covered.contains(seed) ) others.add(seed)
        final List<String> below = new ArrayList<>()
        for( String other : others )
            if( !rawSizes.containsKey(other) && !stops.contains(other) ) below.addAll(refs.get(other) ?: Collections.<String> emptyList())
        others.removeAll(walked(below, rawSizes, refs, stops))
        holders.addAll(others)

        final Map<String, Long> memo = new HashMap<>()
        final Set<String> path = new HashSet<>()
        long total = 0
        for( String holder : holders )
            total += referencedUnder(holder, rawSizes, refs, stops, memo, path)
        total
    }

    /** Every cid a walk from `starts` meets, owners and raw cids alike, not expanding a raw cid or a cid in `stops`. */
    private static Set<String> walked(Collection<String> starts, Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> stops) {
        final Set<String> seen = new HashSet<>()
        final Deque<String> todo = new ArrayDeque<>(starts)
        while( !todo.isEmpty() ) {
            final String cid = todo.pop()
            if( !seen.add(cid) || rawSizes.containsKey(cid) || stops.contains(cid) ) continue
            final List<String> next = refs.get(cid)
            if( next ) todo.addAll(next)
        }
        seen
    }

    private static long referencedUnder(String cid, Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> stops,
                                        Map<String, Long> memo, Set<String> path) {
        if( rawSizes.containsKey(cid) ) {
            final Long s = rawSizes.get(cid)
            return s == null ? 0L : s.longValue()
        }
        if( stops.contains(cid) ) return 0L
        final Long known = memo.get(cid)
        if( known != null ) return known
        // A cycle back into the current path adds nothing.
        if( !path.add(cid) ) return 0L
        long sum = 0
        for( String next : refs.get(cid) ?: Collections.<String>emptyList() )
            sum += referencedUnder(next, rawSizes, refs, stops, memo, path)
        path.remove(cid)
        memo.put(cid, sum)
        sum
    }

    private static Map storeRow(Map<String, Reach> reach, Sizes sizes, long referencedBytes, boolean deadEnd) {
        long pinned = 0, held = 0, releasing = 0, shared = 0
        reach.each { String cid, Reach r ->
            final long n = sizes.of(cid)
            if( r.pinned ) pinned += n
            else if( r.held ) held += n
            else releasing += n
            if( r.runRoots >= 2 ) shared += n
        }
        [stored_bytes: sizes.sum(reach.keySet()), stored_blocks: reach.size(), referenced_bytes: referencedBytes,
         pinned_bytes: pinned, held_bytes: held, releasing_bytes: releasing, shared_bytes: shared,
         partial: deadEnd ? 1 : sizes.partial(reach.keySet())]
    }

    /** R4: exclusive is what no other content root reaches. */
    private static List<Map> rootRows(List<Root> roots, List<Set<String>> closures, Map<String, Reach> reach, Sizes sizes, boolean[] deadEnds) {
        final List<Map> rows = []
        roots.eachWithIndex { Root root, int i ->
            long exclusive = 0, pinned = 0
            int exclusiveBlocks = 0
            for( String cid : closures[i] ) {
                final Reach r = reach.get(cid)
                if( r.contentRoots.every { Integer other -> other == i } ) {
                    exclusive += sizes.of(cid)
                    exclusiveBlocks++
                }
                if( r.pinned ) pinned += sizes.of(cid)
            }
            rows << [root_id: root.id, reachable_bytes: sizes.sum(closures[i]), reachable_blocks: closures[i].size(),
                     exclusive_bytes: exclusive, exclusive_blocks: exclusiveBlocks, pinned_bytes: pinned,
                     partial: deadEnds[i] ? 1 : sizes.partial(closures[i])]
        }
        rows
    }

    /**
     * Per content root, the top other content roots by overlapping bytes; ties
     * go to the smaller cid, then id. Blocks reached by the same content roots
     * are summed into one group first, so a file shared by many runs costs one
     * pass. Then one root at a time, its groups add into a scratch array that
     * is cleared after use, keeping memory to O(roots) rather than O(roots²).
     */
    private static List<Map> sharedRows(List<Root> roots, Map<String, Reach> reach, Sizes sizes) {
        final Map<List<Integer>, Long> grouped = new HashMap<>()
        reach.each { String cid, Reach r ->
            final long n = sizes.of(cid)
            if( n == 0 || r.contentRoots.size() < 2 ) return
            grouped.put(r.contentRoots, (grouped.get(r.contentRoots) ?: 0L) + n)
        }
        final List<List<Integer>> members = new ArrayList<>(grouped.size())
        final long[] groupBytes = new long[grouped.size()]
        final List<List<Integer>> groupsOf = new ArrayList<>(roots.size())
        for( int i = 0; i < roots.size(); i++ ) groupsOf.add(null)
        grouped.each { List<Integer> list, Long n ->
            final int g = members.size()
            members.add(list)
            groupBytes[g] = n
            for( Integer i : list ) {
                if( groupsOf[i] == null ) groupsOf[i] = new ArrayList<Integer>(1)
                groupsOf[i].add(g)
            }
        }
        final long[] scratch = new long[roots.size()]
        final int[] touched = new int[roots.size()]
        final int[] top = new int[SHARED_TOP]
        final List<Map> rows = []
        for( int i = 0; i < roots.size(); i++ ) {
            if( groupsOf[i] == null ) continue
            int count = 0
            for( Integer g : groupsOf[i] ) {
                for( Integer o : members[g] ) {
                    if( o == i ) continue
                    if( scratch[o] == 0 ) touched[count++] = o
                    scratch[o] += groupBytes[g]
                }
            }
            // An insertion into the few kept, in rank order, instead of sorting every other root.
            int kept = 0
            for( int t = 0; t < count; t++ ) {
                final int o = touched[t]
                int at = kept
                while( at > 0 && ranksBefore(o, top[at - 1], scratch, roots) ) at--
                if( at >= SHARED_TOP ) continue
                for( int k = Math.min(kept, SHARED_TOP - 1); k > at; k-- ) top[k] = top[k - 1]
                top[at] = o
                if( kept < SHARED_TOP ) kept++
            }
            for( int k = 0; k < kept; k++ )
                rows << ([root_id: roots[i].id, other_root_id: roots[top[k]].id, bytes: scratch[top[k]]] as Map)
            for( int t = 0; t < count; t++ ) scratch[touched[t]] = 0
        }
        rows
    }

    private static boolean ranksBefore(int a, int b, long[] bytes, List<Root> roots) {
        final int c = bytes[b] <=> bytes[a] ?: roots[a].cid <=> roots[b].cid ?: roots[a].id <=> roots[b].id
        c < 0
    }

    private static List<Map> asserterRows(List<Root> roots, Map<String, Reach> reach, Sizes sizes) {
        final Set<String> keys = new TreeSet<>(roots.collect { Root r -> asserter(r.assertedBy) })
        keys.collect { String who ->
            long reachable = 0, exclusive = 0, pinned = 0, releasing = 0
            reach.each { String cid, Reach r ->
                final long n = sizes.of(cid)
                if( r.contentAsserters.contains(who) ) {
                    reachable += n
                    if( r.contentAsserters.size() == 1 ) exclusive += n
                }
                if( r.pinAsserters.contains(who) ) pinned += n
                if( r.releasing && r.otherAsserters.contains(who) ) releasing += n
            }
            [asserted_by: who, reachable_bytes: reachable, exclusive_bytes: exclusive, pinned_bytes: pinned,
             releasing_bytes: releasing] as Map
        }
    }

    /**
     * R5: `runs` leaves out delete-hidden runs. `bytes` counts runs that hold
     * content here: held runs, and released or deleted runs whose cid is
     * pinned. `releasing` is what the other released runs reach of the store's
     * releasing part.
     */
    private static List<Map> pipelineRows(List<Root> roots, List<Set<String>> closures, Map<String, Reach> reach, Sizes sizes) {
        final Set<String> pinnedCids = roots.findAll { Root r -> r.kind == PIN }.collect { Root r -> r.cid } as Set<String>
        final Map<String, Integer> runs = new TreeMap<>()
        final Map<String, Set<String>> holding = new HashMap<>()
        final Map<String, Set<String>> released = new HashMap<>()
        roots.eachWithIndex { Root root, int i ->
            if( root.kind != RUN || root.pipeline == null ) return
            final String p = root.pipeline
            final boolean holds = root.state == HELD || pinnedCids.contains(root.cid)
            if( root.state != DELETED ) runs.put(p, (runs.get(p) ?: 0) + 1)
            else if( !holds ) return
            if( !runs.containsKey(p) ) runs.put(p, 0)
            if( holds ) holding.computeIfAbsent(p) { new HashSet<String>() }.addAll(closures[i])
            else if( root.state == RELEASED ) released.computeIfAbsent(p) { new HashSet<String>() }.addAll(closures[i])
        }
        runs.collect { String p, Integer n ->
            final Set<String> held = holding.get(p) ?: Collections.<String>emptySet()
            final Collection<String> releasing = (released.get(p) ?: Collections.<String>emptySet()).findAll { String c -> reach.get(c).releasing }
            [pipeline: p, runs: n, bytes: sizes.sum(held), blocks: held.size(), releasing_bytes: sizes.sum(releasing)] as Map
        }
    }

    private static List<Map> collectionRows(Map<String, List<String>> collectionItems, Map<String, Long> rawSizes,
                                            Map<String, List<String>> refs, Sizes sizes) {
        new TreeSet<String>(collectionItems.keySet()).collect { String c ->
            final Set<String> under = closure(collectionItems.get(c) ?: [], rawSizes, refs)
            [collection_cid: c, bytes: sizes.sum(under), blocks: under.size()] as Map
        }
    }

    /** Items are the owners in `refs` that are neither manifests nor Handoffs, and every collection member. */
    private static SortedSet<String> items(Map<String, List<String>> refs, Map<String, List<String>> collectionItems, Set<String> manifests,
                                           List<Root> roots) {
        final SortedSet<String> out = new TreeSet<>(refs.keySet())
        out.removeAll(manifests)
        for( Root r : roots )
            if( r.kind == HANDOFF ) out.remove(r.cid)
        for( List<String> members : collectionItems.values() ) out.addAll(members)
        out
    }

    /** What each item has stored here: the raw files under it still in the store, an absent manifest a dead end. */
    private static List<Map> itemRows(Set<String> items, Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> stops,
                                      Sizes sizes) {
        items.collect { String item ->
            final Set<String> under = closure([item], rawSizes, refs, stops, null)
            [item_cid: item, bytes: sizes.sum(under), blocks: under.size()] as Map
        }
    }

    /** A block's kind is the extension of the smallest name it is referenced by. Largest first. */
    private static List<Map> kindRows(Set<String> stored, Map<String, List<String>> names, Sizes sizes) {
        final Map<String, long[]> by = new HashMap<>()
        for( String cid : stored ) {
            final List<String> named = names.get(cid)
            final String kind = named ? extOf(named.min()) : ''
            long[] e = by.get(kind)
            if( e == null ) { e = new long[2]; by.put(kind, e) }
            e[0]++
            e[1] += sizes.of(cid)
        }
        final List<String> order = new ArrayList<>(by.keySet())
        order.sort { String a, String b -> by.get(b)[1] <=> by.get(a)[1] ?: a <=> b }
        order.collect { String k -> [kind: k, files: (int) by.get(k)[0], bytes: by.get(k)[1]] as Map }
    }

    /**
     * For each run, Selection, output collection, item and pinned raw cid, the
     * bytes of it that each asserter's pins reach. Rows with no bytes are left out.
     */
    private static List<Map> overlapRows(List<Root> roots, List<Set<String>> closures, Map<String, List<String>> collectionItems,
                                         Set<String> items, Map<String, Reach> reach,
                                         Map<String, Long> rawSizes, Map<String, List<String>> refs, Set<String> stops, Sizes sizes) {
        final List<Map> rows = []
        if( !roots.any { Root r -> r.kind == PIN } ) return rows
        final Set<String> done = new HashSet<>()
        final Closure emit = { String subject, Collection<String> under ->
            if( !done.add(subject) ) return
            final Map<String, Long> by = new TreeMap<>()
            for( String cid : under ) {
                final Reach r = reach.get(cid)
                if( r == null ) continue
                for( String who : r.pinAsserters ) by.put(who, (by.get(who) ?: 0L) + sizes.of(cid))
            }
            by.each { String who, Long n -> if( n > 0 ) rows << [subject_cid: subject, asserted_by: who, bytes: n] }
        }
        roots.eachWithIndex { Root root, int i ->
            if( root.kind == RUN || root.kind == SELECTION ) emit.call(root.cid, closures[i])
        }
        for( String c : new TreeSet<String>(collectionItems.keySet()) )
            emit.call(c, closure(collectionItems.get(c) ?: [], rawSizes, refs, stops, null))
        for( String item : items ) emit.call(item, closure([item], rawSizes, refs, stops, null))
        for( String cid : new TreeSet<String>(reach.keySet()) )
            if( reach.get(cid).pinned ) emit.call(cid, [cid])
        rows
    }
}
