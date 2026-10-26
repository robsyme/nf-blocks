package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * What one `nf-blocks:copy` put into its destination member (P3 plan
 * section 1.5, D8): the roots asked for, the runs whose records travelled,
 * the members it went between, and how much of the plan it wrote. Written
 * last, after every block and every ordinary log entry the copy made, so a
 * Transfer never names content its member lacks. The copied blocks keep
 * their own asserted_by; this block carries the copier.
 *
 * Roots, runs and claims are sets: the constructor sorts them by string and
 * drops repeats, so the same copy gives one block whatever order it was
 * built in.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Transfer {

    final String assertedBy
    final List<Cid> roots
    final String from
    final String to
    /** results, lineage or all. Not checked here, so a later depth stays readable. */
    final String depth
    /** The claim categories carried (names, pins, projects, annotations). None blank, none holding ',': the index keeps them comma-joined. */
    final List<String> claims
    /** Every RunCompletion whose records the copy carried. */
    final List<Cid> runs
    final long blocks
    final long copied
    final long bytes
    /** ISO-8601 UTC with milliseconds, when the copy finished. */
    final String timestamp

    Transfer(String assertedBy, List<Cid> roots, String from, String to, String depth, List<String> claims,
             List<Cid> runs, long blocks, long copied, long bytes, String timestamp) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Transfer needs an asserted_by')
        if( !roots )
            throw new IllegalArgumentException('a Transfer needs at least one root')
        if( !from || !to )
            throw new IllegalArgumentException('a Transfer needs a from and a to member')
        if( !depth )
            throw new IllegalArgumentException('a Transfer needs a depth')
        if( !timestamp )
            throw new IllegalArgumentException('a Transfer needs a timestamp')
        if( blocks < 0 || copied < 0 || bytes < 0 )
            throw new IllegalArgumentException("a Transfer's blocks, copied and bytes are counts: ${blocks}, ${copied}, ${bytes}")
        for( String c : claims ?: Collections.<String> emptyList() )
            if( c == null || c.trim().isEmpty() || c.contains(',') )
                throw new IllegalArgumentException("a Transfer's claim category is a word with no ',', got ${c == null ? 'null' : "'" + c + "'"}")
        this.assertedBy = assertedBy
        this.roots = sortedCids(roots, 'roots')
        this.from = from
        this.to = to
        this.depth = depth
        this.claims = Collections.unmodifiableList(new ArrayList<String>(new TreeSet<String>(claims ?: Collections.<String> emptyList())))
        this.runs = sortedCids(runs ?: Collections.<Cid> emptyList(), 'runs')
        this.blocks = blocks
        this.copied = copied
        this.bytes = bytes
        this.timestamp = timestamp
    }

    private static List<Cid> sortedCids(Collection<Cid> cids, String field) {
        if( cids.contains(null) )
            throw new IllegalArgumentException("a Transfer's ${field} holds a null address")
        return Collections.unmodifiableList(new ArrayList<Cid>(new TreeSet<Cid>(cids)))
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.TRANSFER)
        map.put('asserted_by', assertedBy)
        map.put('roots', new ArrayList<Object>(roots))
        map.put('from', from)
        map.put('to', to)
        map.put('depth', depth)
        map.put('claims', new ArrayList<Object>(claims))
        map.put('runs', new ArrayList<Object>(runs))
        map.put('blocks', blocks)
        map.put('copied', copied)
        map.put('bytes', bytes)
        map.put('timestamp', timestamp)
        return map
    }

    static Transfer fromCbor(Map block) {
        Records.expectKind(block, Records.TRANSFER)
        Records.schemaOf(block, [Records.SCHEMA])
        return new Transfer(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            list(block, 'roots').collect { Object c -> Records.cid(c, 'roots') },
            Records.string(Records.require(block, 'from'), 'from'),
            Records.string(Records.require(block, 'to'), 'to'),
            Records.string(Records.require(block, 'depth'), 'depth'),
            list(block, 'claims').collect { Object c -> Records.string(c, 'claims') },
            list(block, 'runs').collect { Object c -> Records.cid(c, 'runs') },
            Records.number(Records.require(block, 'blocks'), 'blocks'),
            Records.number(Records.require(block, 'copied'), 'copied'),
            Records.number(Records.require(block, 'bytes'), 'bytes'),
            Records.string(Records.require(block, 'timestamp'), 'timestamp'))
    }

    private static List list(Map block, String field) {
        final Object value = Records.require(block, field)
        if( !(value instanceof List) )
            throw new IllegalArgumentException("field '${field}' should be a list, got ${value == null ? 'null' : value.getClass().simpleName}")
        return (List) value
    }
}
