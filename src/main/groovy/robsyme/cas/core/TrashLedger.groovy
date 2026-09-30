package robsyme.cas.core

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

/**
 * One sweep's trash/ ledger (ticket 20 answer 2): the blocks it trashed, with
 * their sizes, and the deadline after which a later sweep deletes those still
 * dead. Named `<13-digit deadline>-<sweep id>` so a listing sorts by deadline;
 * the name's deadline, not the body's text, is authoritative.
 */
@CompileStatic
class TrashLedger {

    final String sweepId
    final long deadlineMillis
    final String trashedAt
    final SortedMap<Cid, Long> blocks

    TrashLedger(String sweepId, long deadlineMillis, String trashedAt, Map<Cid, Long> blocks) {
        this.sweepId = sweepId
        this.deadlineMillis = deadlineMillis
        this.trashedAt = trashedAt
        final TreeMap<Cid, Long> sorted = new TreeMap<Cid, Long>({ Cid a, Cid b -> a.toString() <=> b.toString() } as Comparator<Cid>)
        sorted.putAll(blocks)
        this.blocks = Collections.unmodifiableSortedMap(sorted)
    }

    String getName() { String.format('%013d', deadlineMillis) + '-' + sweepId }

    byte[] toJson() {
        return JsonOutput.toJson([sweep: sweepId, trashed_at: trashedAt, deadline: Index.isoMillis(deadlineMillis),
            blocks: blocks.collect { Cid c, Long size -> [cid: c.toString(), size: size] }]).getBytes('UTF-8')
    }

    TrashLedger without(Collection<Cid> cids) {
        final Map<Cid, Long> kept = new LinkedHashMap<Cid, Long>(blocks)
        for( Cid c : cids )
            kept.remove(c)
        return new TrashLedger(sweepId, deadlineMillis, trashedAt, kept)
    }

    static TrashLedger parse(String name, byte[] body) {
        if( name == null || name.length() < 15 || name.charAt(13) != ('-' as char) || !(name.substring(0, 13) ==~ /\d{13}/) )
            throw new IllegalArgumentException("not a Trash ledger name, <13 digits>-<sweep id>: '${name}'")
        Object parsed
        try {
            parsed = new JsonSlurper().parse(body)
        }
        catch( Exception e ) {
            throw new IllegalArgumentException("Trash ledger '${name}' is not JSON: ${e.message}")
        }
        if( !(parsed instanceof Map) )
            throw new IllegalArgumentException("Trash ledger '${name}' is not a JSON object")
        final Map json = (Map) parsed
        final long deadline = Long.parseLong(name.substring(0, 13))
        if( json.sweep != name.substring(14) )
            throw new IllegalArgumentException("Trash ledger '${name}' names sweep '${json.sweep}'")
        final Object listed = json.blocks == null ? [] : json.blocks
        if( !(listed instanceof List) )
            throw new IllegalArgumentException("Trash ledger '${name}' has no list of blocks")
        final Map<Cid, Long> blocks = new LinkedHashMap<Cid, Long>()
        for( Object o : (List) listed ) {
            final Object cid = o instanceof Map ? ((Map) o).cid : null
            final Object size = o instanceof Map ? ((Map) o).size : null
            if( !(cid instanceof String) || !Cid.isCid((String) cid) || !(size instanceof Number) )
                throw new IllegalArgumentException("Trash ledger '${name}' lists a block that is not {cid, size}: ${o}")
            blocks.put(Cid.parse((String) cid), ((Number) size).longValue())
        }
        return new TrashLedger((String) json.sweep, deadline, json.trashed_at as String, blocks)
    }
}
