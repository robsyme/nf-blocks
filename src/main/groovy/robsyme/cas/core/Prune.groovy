package robsyme.cas.core

import java.time.Instant

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * One decision `Prune.plan` reached about one run (ticket 21 answer 5, plan
 * decision 12). {@code supersedes} is the run's current `retain` Claims: what
 * a release request for it must supersede.
 */
@Canonical
@CompileStatic
class PruneDecision {
    String pipeline
    IndexedRun run
    String action
    String reason
    List<Cid> supersedes
}

/**
 * Plans which runs a `prune --keep-last`/`--keep-newer` should release. Reads
 * the Index only; writing the release Claims is the caller's job (Task 9's
 * CLI verb), through {@link #request}.
 */
@CompileStatic
class Prune {

    private final Index index

    Prune(Index index) { this.index = index }

    /**
     * keepLast null or &gt;= 0; keepNewerMillis null or &gt; 0; at least one
     * set (else IllegalArgumentException). pipeline null: every pipeline.
     *
     * Per run, in this order (ticket 21 answer 5, plan decision 12): a clean
     * `delete` is `skip` ("deleted"); a `released` run is `skip` ("already
     * released"); a conflicted `retain` group is `skip` ("retain Claims in
     * conflict; content kept until one supersedes them"); then `keep` when
     * `--keep-last` keeps it (successful, and among the newest N successful
     * of its pipeline) or `--keep-newer` keeps it (`finished_at` at or after
     * `now - period`), else `release`. A release of a pinned run carries the
     * reason "pinned: its pins still hold". Failed and possibly-incomplete
     * runs are never counted in N, so `--keep-last` releases them.
     *
     * `successfulSeen` counts successful runs in newest-first order whatever
     * their Claims, so a deleted or released recent run still takes one of
     * the N places: a person who pruned to 3 and then deleted one of the 3
     * does not expect a fourth to be released, nor an older one to be kept
     * in its place (DESIGN §19).
     */
    List<PruneDecision> plan(String pipeline, Integer keepLast, Long keepNewerMillis, long nowMillis) {
        if( keepLast == null && keepNewerMillis == null )
            throw new IllegalArgumentException('prune needs --keep-last <n> or --keep-newer <period>, or both')
        final List<PruneDecision> out = []
        for( String p : (pipeline != null ? [pipeline] : index.pipelines()) ) {
            int successfulSeen = 0
            for( IndexedRun run : index.runsOf(p) ) {
                final ClaimState st = index.claimState(run.completion)
                final boolean keptByCount = keepLast != null && run.successful && successfulSeen < keepLast
                if( run.successful ) successfulSeen++
                final boolean keptByAge = keepNewerMillis != null && run.finishedAt != null &&
                    Instant.parse(run.finishedAt).toEpochMilli() >= nowMillis - keepNewerMillis
                final List<Cid> supersedes = st.retainClaims.collect { String c -> Cid.parse(c) }
                if( st.deletion == ClaimState.DELETED )
                    out << new PruneDecision(p, run, 'skip', 'deleted', supersedes)
                else if( st.released )
                    out << new PruneDecision(p, run, 'skip', 'already released', supersedes)
                else if( st.retain == ClaimState.CONFLICTED )
                    out << new PruneDecision(p, run, 'skip', 'retain Claims in conflict; content kept until one supersedes them', supersedes)
                else if( keptByCount || keptByAge )
                    out << new PruneDecision(p, run, 'keep', keptByCount ? "one of the newest ${keepLast} successful".toString() : 'newer than the cutoff', supersedes)
                else
                    out << new PruneDecision(p, run, 'release', st.pinned ? 'pinned: its pins still hold' : (run.successful ? 'older' : 'failed or possibly incomplete'), supersedes)
            }
        }
        return out
    }

    /** The put request for a release decision: set retain "lineage", superseding the run's current retain Claims. */
    static Map<String, Object> request(PruneDecision d, String timestamp) {
        final Map<String, Object> r = new LinkedHashMap<String, Object>()
        r.put('kind', Records.CLAIM)
        r.put('subject', d.run.completion)
        r.put('verb', Claim.SET)
        r.put('attribute', Claim.RETAIN)
        r.put('value', Claim.LINEAGE)
        r.put('supersedes', new ArrayList<Cid>(d.supersedes))
        r.put('timestamp', timestamp)
        return r
    }
}
