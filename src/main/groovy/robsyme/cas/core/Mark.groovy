package robsyme.cas.core

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * What a sweep may not remove (ticket 20 answer 4, ticket 21 answers 1 to 4;
 * plan decisions 1 to 6). Reads blocks, never the Index: deletion is where
 * correctness beats speed. Level by level, and within a level in windows of
 * about `threads * WINDOW_FACTOR` blocks, so decoded metadata does not pile
 * up across a whole (possibly very wide) level (DESIGN.md §0 rule 2).
 */
@CompileStatic
class Mark {

    static final int SELF = 0
    static final int META = 1
    static final int ALL = 2

    /** How many blocks a window reads and decodes at once, relative to `threads`. */
    private static final int WINDOW_FACTOR = 8

    @Canonical
    @CompileStatic
    static class Roots {
        int runs
        int contentRoots
        int released
        int hidden
        int hiddenButPinned
        int selections
        int deletedSelections
        int pinnedSubjects
        int claims
    }

    @Canonical
    @CompileStatic
    private static class Visit {
        Cid cid
        int want
        boolean content
        Cid parent
    }

    /** Marks that a read found nothing at a cid (NoSuchBlockException), as opposed to any other failure. */
    private static final class BlockAbsent extends RuntimeException {
        BlockAbsent() { super(null, null, false, false) }
    }

    private final BlockStore store
    private final int threads
    /** Every composite member but the writable first one, so a Claim can check whether its subject survives there (ruling 2). */
    private final List<BlockStore> otherMembers
    private final Map<Cid, Integer> seen = new HashMap<Cid, Integer>()
    private final Set<Cid> liveSet = ConcurrentHashMap.newKeySet()
    private final Set<Cid> runs = new LinkedHashSet<Cid>()
    private final Set<Cid> selections = new LinkedHashSet<Cid>()
    private final Map<Cid, Cid> claimSubject = new HashMap<Cid, Cid>()
    private final Map<Cid, List<ClaimState.Row>> claimsOf = new HashMap<Cid, List<ClaimState.Row>>()
    private final Map<Cid, String> writableEntry = new HashMap<Cid, String>()
    private final Set<Cid> rooted = new HashSet<Cid>()
    private final AtomicLong reads = new AtomicLong()
    /** Lazily created per `extend`, shut down when it returns (DESIGN.md §0 rule 2: no pool lives longer than it must). */
    private ExecutorService pool

    final List<String> missingMetadata = new ArrayList<String>()
    final List<Cid> missingContent = new ArrayList<Cid>()
    final List<String> danglingEntries = new ArrayList<String>()
    /** A Claim block that exists but Claim.fromCbor refuses: kept live, never expanded, and listed here (ruling 3). */
    final List<Cid> unreadableClaims = new ArrayList<Cid>()
    final Roots roots = new Roots()

    private Mark(BlockStore store, int threads) {
        this.store = store
        this.threads = Math.max(1, threads)
        this.otherMembers = otherMembersOf(store)
    }

    private static List<BlockStore> otherMembersOf(BlockStore store) {
        if( !(store instanceof CompositeStore) )
            return Collections.<BlockStore> emptyList()
        final List<BlockStore> all = ((CompositeStore) store).getMembers()
        if( all.size() <= 1 )
            return Collections.<BlockStore> emptyList()
        return Collections.unmodifiableList(new ArrayList<BlockStore>(all.subList(1, all.size())))
    }

    static Mark of(BlockStore store, List<MemberLog> logs, int threads) {
        final Mark m = new Mark(store, threads)
        m.extend(logs)
        return m
    }

    Set<Cid> getLive() { Collections.unmodifiableSet(liveSet) }

    boolean isLive(Cid cid) { liveSet.contains(cid) }

    long getBlocksRead() { reads.get() }

    synchronized void extend(List<MemberLog> logs) {
        try {
            final Set<Cid> touched = new LinkedHashSet<Cid>()
            // A LinkedHashSet, not a List: the same claim cid can arrive twice in one
            // call (two members log it, or one log lists it twice), and counting it
            // into claimsOf twice would read a clean delete or release as CONFLICTED
            // (ruling 1).
            final Set<Cid> newClaims = new LinkedHashSet<Cid>()
            for( MemberLog log : logs ) {
                for( StoreLogEntry e : log.entries ) {
                    if( log.writable )
                        writableEntry.putIfAbsent(e.cid, e.name)
                    switch( e.kind ) {
                        case StoreLogKind.RUN:
                            if( runs.add(e.cid) ) touched.add(e.cid)
                            break
                        case StoreLogKind.SELECTION:
                            if( selections.add(e.cid) ) touched.add(e.cid)
                            break
                        case StoreLogKind.CLAIM:
                            if( !claimSubject.containsKey(e.cid) )
                                newClaims.add(e.cid)
                            break
                    }
                }
            }
            final Map<Cid, Object> claimBlocks = readAll(newClaims)
            for( Cid c : newClaims ) {
                if( !claimBlocks.containsKey(c) ) {
                    dangling(c)
                    continue
                }
                final Object raw = claimBlocks.get(c)
                Claim claim = null
                if( raw instanceof Map ) {
                    try {
                        claim = Claim.fromCbor((Map) raw)
                    }
                    catch( IllegalArgumentException e ) {
                        claim = null
                    }
                }
                if( claim == null ) {
                    // Present, but not a Claim this build can read: kept live and never
                    // expanded, rather than dropped and later swept (ruling 3).
                    liveSet.add(c)
                    unreadableClaims.add(c)
                    continue
                }
                claimSubject.put(c, claim.subject)
                claimsOf.computeIfAbsent(claim.subject) { new ArrayList<ClaimState.Row>() }
                    .add(new ClaimState.Row(c.toString(), claim.verb, claim.attribute, claim.value, claim.supersedes*.toString()))
                touched.add(claim.subject)
            }
            root(touched)
            keepClaims()
            keepClaimsHeldElsewhere()
            roots.claims = (int) claimSubject.keySet().count { Cid c -> liveSet.contains(c) }
        }
        finally {
            shutdownPool()
        }
    }

    private ClaimState stateOf(Cid subject) {
        return ClaimState.of(claimsOf.get(subject) ?: Collections.<ClaimState.Row> emptyList())
    }

    /** Roots the touched subjects by the rules of the table above; a subject already rooted at ALL is left alone. */
    private void root(Set<Cid> touched) {
        final List<Visit> frontier = new ArrayList<Visit>()
        for( Cid s : touched ) {
            final ClaimState st = stateOf(s)
            final boolean deleted = st.deletion == ClaimState.DELETED
            if( runs.contains(s) ) {
                if( deleted && !st.pinned ) { count(s) { roots.hidden++ }; continue }
                if( deleted ) { count(s) { roots.hiddenButPinned++ }; frontier.add(new Visit(s, ALL, false, null)); continue }
                if( st.released && !st.pinned ) { count(s) { roots.released++ }; frontier.add(new Visit(s, META, false, null)); continue }
                count(s) { roots.contentRoots++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
            else if( selections.contains(s) ) {
                if( deleted && !st.pinned ) { count(s) { roots.deletedSelections++ }; continue }
                count(s) { roots.selections++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
            else if( st.pinned ) {
                count(s) { roots.pinnedSubjects++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
        }
        roots.runs = runs.size()
        walk(frontier)
    }

    /** Counts a subject under one root heading once, even when extend reconsiders it. */
    private void count(Cid s, Closure tally) {
        if( rooted.add(s) )
            tally.call()
    }

    private void keepClaims() {
        boolean changed = true
        while( changed ) {
            changed = false
            for( Map.Entry<Cid, Cid> e : claimSubject.entrySet() )
                if( !liveSet.contains(e.key) && liveSet.contains(e.value) ) {
                    liveSet.add(e.key)
                    changed = true
                }
        }
    }

    /**
     * A Claim whose subject still exists in a member other than the writable
     * one must survive even when its subject never becomes live here: the
     * sweep only ever touches the writable member, so if this delete (or
     * other) Claim were removed there, a later mark would see the subject's
     * block still sitting in the read-only member and treat it as undeleted
     * (ruling 2, overrides plan decision 4 as written).
     */
    private void keepClaimsHeldElsewhere() {
        if( otherMembers.isEmpty() )
            return
        for( Map.Entry<Cid, Cid> e : claimSubject.entrySet() ) {
            final Cid c = e.key
            if( liveSet.contains(c) )
                continue
            final Cid subject = e.value
            for( BlockStore member : otherMembers ) {
                if( member.has(subject) ) {
                    liveSet.add(c)
                    break
                }
            }
        }
    }

    private void walk(List<Visit> start) {
        List<Visit> frontier = start
        while( !frontier.isEmpty() ) {
            final List<Visit> todo = new ArrayList<Visit>()
            for( Visit v : frontier ) {
                final Integer had = seen.get(v.cid)
                if( had != null && had >= v.want )
                    continue
                seen.put(v.cid, v.want)
                todo.add(v)
            }
            final List<Visit> next = new ArrayList<Visit>()
            final int window = Math.max(1, threads * WINDOW_FACTOR)
            int i = 0
            while( i < todo.size() ) {
                final int end = Math.min(i + window, todo.size())
                processWindow(todo.subList(i, end), next)
                i = end
            }
            frontier = next
        }
    }

    /** Reads and expands one window of a level, then lets its decoded blocks go before the next window is read (ruling 4). */
    private void processWindow(List<Visit> window, List<Visit> next) {
        final Map<Cid, Object> blocks = readAll(window.findAll { Visit v -> !v.cid.isRaw() }*.cid)
        for( Visit v : window ) {
            if( v.cid.isRaw() ) {
                liveSet.add(v.cid)
                continue
            }
            if( !blocks.containsKey(v.cid) ) {
                absent(v)
                continue
            }
            liveSet.add(v.cid)
            final Object value = blocks.get(v.cid)
            // A root (or reached block) that decodes but is not a record map is kept
            // live and left unexpanded, never treated as absent/dangling: only a
            // genuine NoSuchBlockException means the block is not there (ruling 6).
            if( value instanceof Map )
                expand(v, (Map) value, next)
        }
    }

    private void absent(Visit v) {
        if( v.parent == null )
            dangling(v.cid)
        else if( v.content )
            missingContent.add(v.cid)
        else
            missingMetadata.add("${v.cid} (needed by ${v.parent})".toString())
    }

    private void dangling(Cid cid) {
        final String entry = writableEntry.get(cid)
        if( entry != null && !danglingEntries.contains(entry) )
            danglingEntries.add(entry)
    }

    private static void content(Object address, Cid parent, List<Visit> next) {
        if( address instanceof Cid )
            next.add(new Visit((Cid) address, ALL, true, parent))
    }

    private static void meta(Object address, int want, Cid parent, List<Visit> next) {
        if( address instanceof Cid )
            next.add(new Visit((Cid) address, want, false, parent))
    }

    private void expand(Visit v, Map block, List<Visit> next) {
        switch( Records.kindOf(block) ) {
            case Records.RUN_COMPLETION:
                meta(block.get('run'), META, v.cid, next)
                for( Object c : (List) (block.get('collections') ?: []) )
                    meta(c, v.want, v.cid, next)
                break
            case Records.RUN_MANIFEST:
                content(block.get('script'), v.cid, next)
                break
            case Records.OUTPUT_COLLECTION:
                meta(block.get('run'), META, v.cid, next)
                if( v.want >= META )
                    for( Object i : (List) (block.get('items') ?: []) )
                        meta(i, v.want, v.cid, next)
                if( v.want == ALL && block.get('index') instanceof Map )
                    content(((Map) ((Map) block.get('index')).get('leaf'))?.get('address'), v.cid, next)
                break
            case Records.OUTPUT_ITEM:
                if( v.want == ALL )
                    for( Map leaf : Index.leavesOf(block.get('value')) )
                        content(leaf.get('address'), v.cid, next)
                break
            case Records.DIRECTORY_MANIFEST:
                for( Object e : (List) (block.get('entries') ?: []) )
                    content(((Map) e).get('address'), v.cid, next)
                break
            case Records.SELECTION:
                for( Object m : (List) (block.get('members') ?: []) ) {
                    final Map member = (Map) m
                    if( member.get('item') instanceof Map ) {
                        final Map item = (Map) member.get('item')
                        meta(item.get('address'), ALL, v.cid, next)
                        for( Object via : (List) (item.get('via') ?: []) )
                            meta(via, SELF, v.cid, next)
                    }
                    else
                        meta(member.get('selection'), ALL, v.cid, next)
                }
                break
            default:
                break   // a Claim or a kind this build does not know: itself only
        }
    }

    private ExecutorService pool() {
        if( pool == null )
            pool = Executors.newFixedThreadPool(threads)
        return pool
    }

    private void shutdownPool() {
        if( pool != null ) {
            pool.shutdownNow()
            pool = null
        }
    }

    /**
     * Reads and decodes dag-cbor blocks, up to `threads` at once, through the
     * one pool this `extend` call owns. A cid absent everywhere (a genuine
     * NoSuchBlockException) is simply left out of the result; any other
     * failure -- a block that exists but will not decode -- unwraps the
     * Future's ExecutionException and rethrows the real cause, so a corrupt
     * metadata block aborts the mark rather than reading as missing (ruling 5).
     */
    private Map<Cid, Object> readAll(Collection<Cid> cids) {
        final Map<Cid, Object> out = new HashMap<Cid, Object>()
        if( cids.isEmpty() )
            return out
        final ExecutorService exec = pool()
        final Map<Cid, Future<Object>> pending = new LinkedHashMap<Cid, Future<Object>>()
        for( Cid c : cids ) {
            final Cid cid = c
            pending.put(cid, exec.submit({ -> read(cid) } as Callable<Object>))
        }
        for( Map.Entry<Cid, Future<Object>> e : pending.entrySet() ) {
            try {
                out.put(e.key, e.value.get())
            }
            catch( ExecutionException ex ) {
                final Throwable cause = ex.getCause()
                if( cause instanceof BlockAbsent )
                    continue
                rethrow(cause)
            }
            catch( InterruptedException ex ) {
                Thread.currentThread().interrupt()
                throw new RuntimeException(ex)
            }
        }
        return out
    }

    private static void rethrow(Throwable cause) {
        if( cause instanceof RuntimeException )
            throw (RuntimeException) cause
        if( cause instanceof Error )
            throw (Error) cause
        throw new RuntimeException(cause)
    }

    /** The decoded dag-cbor value at `cid`, whatever shape it is; throws BlockAbsent when the store has nothing there. */
    private Object read(Cid cid) {
        final InputStream in
        try {
            in = store.open(cid)
        }
        catch( NoSuchBlockException e ) {
            throw new BlockAbsent()
        }
        try {
            reads.incrementAndGet()
            return DagCbor.decode(in.readAllBytes())
        }
        finally {
            in.close()
        }
    }
}
