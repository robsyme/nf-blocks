package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a dry run found or an applied sweep did, for the `sweep` verb to print (text) or emit (json). */
@CompileStatic
class SweepReport {

    String sweepId
    boolean applied
    String stopped
    /** The swept member's alias and where it lives (a path, or s3://bucket/prefix). */
    String alias
    String location
    long ageFloorMillis
    Mark.Roots roots
    int live
    int blocks
    long bytes
    int dead
    long deadBytes
    int young
    int trashed
    long trashedBytes
    int overBudget
    int due
    long dueBytes
    int waiting
    int rescued
    List<Cid> deleted = []
    long deletedBytes
    int scratch
    List<LiveRegistry.Registration> fresh = []
    int staleRegistrations
    List<String> missingMetadata = []
    List<Cid> missingContent = []
    int dangling
    int unreadableClaims
    String ledger
    long requests
    long blocksRead
    int listingPages
    int ledgersRead
    List<String> warnings = []

    Map toJson() {
        final Map out = new LinkedHashMap()
        out.sweep_id = sweepId
        out.applied = applied
        out.stopped = stopped
        out.alias = alias
        out.location = location
        out.age_floor_millis = ageFloorMillis
        out.roots = roots == null ? null : [runs: roots.runs, content_roots: roots.contentRoots, released: roots.released,
            hidden: roots.hidden, hidden_but_pinned: roots.hiddenButPinned, selections: roots.selections,
            deleted_selections: roots.deletedSelections, pinned_subjects: roots.pinnedSubjects, claims: roots.claims]
        out.live = live
        out.blocks = blocks
        out.bytes = bytes
        out.dead = dead
        out.dead_bytes = deadBytes
        out.young = young
        out.trashed = trashed
        out.trashed_bytes = trashedBytes
        out.over_budget = overBudget
        out.due = due
        out.due_bytes = dueBytes
        out.waiting = waiting
        out.rescued = rescued
        out.deleted = deleted.collect { Cid c -> c.toString() }
        out.deleted_bytes = deletedBytes
        out.scratch = scratch
        out.fresh = fresh.collect { LiveRegistry.Registration r ->
            [session: r.session, run_name: r.runName, pipeline: r.pipeline, age_seconds: r.ageMillis.intdiv(1000L)] }
        out.stale_registrations = staleRegistrations
        out.missing_metadata = missingMetadata
        out.missing_content = missingContent.collect { Cid c -> c.toString() }
        out.dangling = dangling
        out.unreadable_claims = unreadableClaims
        out.ledger = ledger
        out.requests = requests
        out.warnings = warnings
        return out
    }

    String toText() {
        final StringBuilder out = new StringBuilder()
        final String where = "${alias} (${location})"
        // An --apply attempt (it has a sweep id) reports what it did, in the past tense, even when it stopped.
        final boolean acted = sweepId != null
        if( acted )
            out << "sweep ${sweepId} over ${where}${applied ? '' : ', not applied'}\n"
        else
            out << "dry run over ${where}, nothing written\n"
        if( roots != null )
            line(out, 'roots', "${count(roots.runs)} ${plural(roots.runs, 'run')}: ${count(roots.contentRoots)} with content, " +
                "${count(roots.released)} released, ${count(roots.hidden)} hidden (${count(roots.hiddenButPinned)} hidden but pinned); " +
                "${count(roots.selections)} ${plural(roots.selections, 'Selection')}; " +
                "${count(roots.pinnedSubjects)} pinned ${plural(roots.pinnedSubjects, 'subject')}; ${count(roots.claims)} ${plural(roots.claims, 'Claim')} live")
        line(out, 'blocks', "${count(blocks)} in ${alias} (${size(bytes)}); ${count(live)} live")
        final String trashVerb = acted ? 'trashed' : 'would be trashed'
        String deadLine = "${count(dead)} (${size(deadBytes)}): ${count(young)} younger than the age floor (${duration(ageFloorMillis)}), " +
            "${count(trashed)} ${trashVerb} (${size(trashedBytes)}), ${count(due + waiting)} in Trash"
        if( overBudget > 0 )
            deadLine += ", ${count(overBudget)} over the budget"
        line(out, 'dead', deadLine)
        if( acted )
            line(out, 'trash', "${count(deleted.size())} past their deadline deleted (${size(deletedBytes)}), ${count(waiting)} waiting, ${count(rescued)} rescued")
        else
            line(out, 'trash', "${count(due)} past their deadline would be deleted (${size(dueBytes)}), ${count(waiting)} waiting, ${count(rescued)} rescued")
        line(out, 'scratch', "${count(scratch)} upload ${plural(scratch, 'leftover')} older than the age floor${acted ? ' cleared' : ''}")
        if( fresh.isEmpty() )
            line(out, 'live', staleRegistrations > 0 ? "none registered (${count(staleRegistrations)} stale ${acted ? 'removed' : 'would be removed'})" : 'none registered')
        else
            line(out, 'live', "${count(fresh.size())} ${plural(fresh.size(), 'run')} registered: " +
                fresh.collect { LiveRegistry.Registration r -> "${r.runName ?: 'unnamed'} (session ${r.session}), heartbeat ${r.ageMillis.intdiv(1000L)} s ago" }.join('; ') +
                (acted ? '' : ': --apply would refuse'))
        if( missingMetadata || missingContent || dangling || unreadableClaims )
            line(out, 'problems', "${count(missingMetadata.size())} metadata ${plural(missingMetadata.size(), 'block')} missing, " +
                "${count(missingContent.size())} content ${plural(missingContent.size(), 'block')} missing, " +
                "${count(dangling)} dangling log ${plural(dangling, 'entry', 'entries')}, ${count(unreadableClaims)} unreadable ${plural(unreadableClaims, 'Claim')} kept")
        for( String m : missingMetadata.take(20) )
            line(out, 'missing', m)
        line(out, 'requests', "about ${count(requests)} (${count(blocksRead)} block reads, ${count(listingPages)} listing ${plural(listingPages, 'page')}, " +
            "${count(ledgersRead)} ${plural(ledgersRead, 'ledger')}, 2 for the lock and registrations)")
        if( ledger != null )
            line(out, 'ledger', "trash/${ledger}")
        for( String w : warnings )
            line(out, 'warning', w)
        if( stopped != null )
            line(out, 'stopped', stopped)
        return out.toString()
    }

    private static void line(StringBuilder out, String head, Object text) {
        out << head.padRight(10) << " " << String.valueOf(text) << "\n"
    }

    static String count(long n) { String.format(Locale.ROOT, '%,d', n) }

    static String plural(long n, String one, String many = null) { n == 1 ? one : (many ?: one + 's') }

    /** One decimal, powers of 1,000: 0 B, 512 B, 1.5 KB, 5.2 GB. */
    static String size(long bytes) {
        if( bytes < 1000L )
            return "${bytes} B".toString()
        final String[] units = ['KB', 'MB', 'GB', 'TB'] as String[]
        double value = bytes / 1000.0d
        int u = 0
        while( value >= 999.95d && u < units.length - 1 ) {
            value = value / 1000.0d
            u++
        }
        return String.format(Locale.ROOT, '%.1f %s', value, units[u])
    }

    /** 14d, 12h, 10m, 30s: the largest whole unit. */
    static String duration(long millis) {
        if( millis % SweepPolicy.DAY == 0 && millis > 0 ) return "${millis.intdiv(SweepPolicy.DAY)}d".toString()
        if( millis % 3_600_000L == 0 && millis > 0 ) return "${millis.intdiv(3_600_000L)}h".toString()
        if( millis % 60_000L == 0 && millis > 0 ) return "${millis.intdiv(60_000L)}m".toString()
        return "${millis.intdiv(1000L)}s".toString()
    }
}
