package robsyme.cas.core

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * What a sweep may not remove (ticket 20 answer 4, ticket 21 answers 1 to 4;
 * plan decisions 1 to 6). Reads blocks, never the Index: deletion is where
 * correctness beats speed. Level by level, so up to `threads` blocks are read
 * at once; each level is decided before the next is read.
 */
@CompileStatic
class Mark {

    static final int SELF = 0
    static final int META = 1
    static final int ALL = 2

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

    private final BlockStore store
    private final int threads
    private final Map<Cid, Integer> seen = new HashMap<Cid, Integer>()
    private final Set<Cid> liveSet = ConcurrentHashMap.newKeySet()
    private final Set<Cid> runs = new LinkedHashSet<Cid>()
    private final Set<Cid> selections = new LinkedHashSet<Cid>()
    private final Map<Cid, Cid> claimSubject = new HashMap<Cid, Cid>()
    private final Map<Cid, List<ClaimState.Row>> claimsOf = new HashMap<Cid, List<ClaimState.Row>>()
    private final Map<Cid, String> writableEntry = new HashMap<Cid, String>()
    private final Set<Cid> rooted = new HashSet<Cid>()
    private final AtomicLong reads = new AtomicLong()

    final List<String> missingMetadata = new ArrayList<String>()
    final List<Cid> missingContent = new ArrayList<Cid>()
    final List<String> danglingEntries = new ArrayList<String>()
    final Roots roots = new Roots()

    private Mark(BlockStore store, int threads) {
        this.store = store
        this.threads = Math.max(1, threads)
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
        final Set<Cid> touched = new LinkedHashSet<Cid>()
        final List<Cid> newClaims = new ArrayList<Cid>()
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
                        if( !claimSubject.containsKey(e.cid) ) newClaims.add(e.cid)
                        break
                }
            }
        }
        final Map<Cid, Map> claimBlocks = readAll(newClaims)
        for( Cid c : newClaims ) {
            final Map block = claimBlocks.get(c)
            if( block == null ) {
                dangling(c)
                continue
            }
            Claim claim = null
            try {
                claim = Claim.fromCbor(block)
            }
            catch( IllegalArgumentException e ) {
                continue     // not a Claim this build reads: roots nothing
            }
            claimSubject.put(c, claim.subject)
            claimsOf.computeIfAbsent(claim.subject) { new ArrayList<ClaimState.Row>() }
                .add(new ClaimState.Row(c.toString(), claim.verb, claim.attribute, claim.value, claim.supersedes*.toString()))
            touched.add(claim.subject)
        }
        root(touched)
        keepClaims()
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
        roots.claims = (int) claimSubject.keySet().count { Cid c -> liveSet.contains(c) }
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
            final Map<Cid, Map> blocks = readAll(todo.findAll { Visit v -> !v.cid.isRaw() }*.cid)
            final List<Visit> next = new ArrayList<Visit>()
            for( Visit v : todo ) {
                if( v.cid.isRaw() ) {
                    liveSet.add(v.cid)
                    continue
                }
                final Map block = blocks.get(v.cid)
                if( block == null ) {
                    absent(v)
                    continue
                }
                liveSet.add(v.cid)
                expand(v, block, next)
            }
            frontier = next
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

    /** Reads and decodes dag-cbor blocks, up to `threads` at once; an absent block maps to null. */
    private Map<Cid, Map> readAll(Collection<Cid> cids) {
        final Map<Cid, Map> out = new HashMap<Cid, Map>()
        if( cids.isEmpty() )
            return out
        final ExecutorService pool = Executors.newFixedThreadPool(Math.min(threads, cids.size()))
        try {
            final Map<Cid, Future<Map>> pending = new LinkedHashMap<Cid, Future<Map>>()
            for( Cid c : cids ) {
                final Cid cid = c
                pending.put(cid, pool.submit({ -> read(cid) } as Callable<Map>))
            }
            for( Map.Entry<Cid, Future<Map>> e : pending.entrySet() )
                out.put(e.key, e.value.get())
        }
        finally {
            pool.shutdownNow()
        }
        return out
    }

    private Map read(Cid cid) {
        try {
            final InputStream in = store.open(cid)
            try {
                reads.incrementAndGet()
                final Object value = DagCbor.decode(in.readAllBytes())
                return value instanceof Map ? (Map) value : null
            }
            finally {
                in.close()
            }
        }
        catch( NoSuchBlockException e ) {
            return null
        }
    }
}
