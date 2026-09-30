package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * What a sweep would do, from one mark, one listing of the writable member and
 * every ledger (ticket 20 answers 2 and 3). Pure: it reads and writes nothing.
 */
@Canonical
@CompileStatic
class SweepPlan {
    /** Listed and not live. */
    List<BlockStat> dead
    /** Dead and younger than the age floor: never touched. */
    List<BlockStat> young
    /** Dead, old, in no ledger, within the budget: a new ledger holds these. */
    List<BlockStat> trash
    /** Dead, old, in no ledger, past the budget. */
    List<BlockStat> overBudget
    /** Dead, old, in a ledger whose deadline has passed: deleted. */
    List<BlockStat> due
    /** Dead, old, in a ledger whose deadline has not passed. */
    List<BlockStat> waiting
    /** In a ledger and live now, or in a ledger and no longer listed: dropped from its ledger. */
    Set<Cid> rescued

    static SweepPlan of(Mark mark, List<BlockStat> stats, List<TrashLedger> ledgers, long nowMillis, SweepPolicy policy, long budgetBytes) {
        // A block in two ledgers is judged by the later deadline: the one that keeps it longer.
        final Map<Cid, TrashLedger> ledgerOf = new HashMap<Cid, TrashLedger>()
        for( TrashLedger l : ledgers )
            for( Cid c : l.blocks.keySet() )
                if( !ledgerOf.containsKey(c) || l.deadlineMillis > ledgerOf.get(c).deadlineMillis )
                    ledgerOf.put(c, l)
        final Set<Cid> listed = new HashSet<Cid>()
        for( BlockStat s : stats )
            listed.add(s.cid)
        final List<BlockStat> sorted = new ArrayList<BlockStat>(stats)
        sorted.sort { BlockStat a, BlockStat b -> a.cid.toString() <=> b.cid.toString() }
        final List<BlockStat> dead = [], young = [], trash = [], over = [], due = [], waiting = []
        final Set<Cid> rescued = new LinkedHashSet<Cid>()
        long used = 0
        for( BlockStat s : sorted ) {
            final TrashLedger l = ledgerOf.get(s.cid)
            if( mark.isLive(s.cid) ) {
                if( l != null ) rescued.add(s.cid)
                continue
            }
            dead.add(s)
            if( nowMillis - s.lastModifiedMillis < policy.ageFloorMillis ) {
                young.add(s)
                continue
            }
            if( l != null ) {
                (l.deadlineMillis <= nowMillis ? due : waiting).add(s)
                continue
            }
            if( budgetBytes > 0 && used + s.size > budgetBytes ) {
                over.add(s)
                continue
            }
            used += s.size
            trash.add(s)
        }
        for( Cid c : ledgerOf.keySet() )
            if( !listed.contains(c) )
                rescued.add(c)      // gone already: nothing to delete, drop it from the ledger
        return new SweepPlan(dead, young, trash, over, due, waiting, rescued)
    }
}
