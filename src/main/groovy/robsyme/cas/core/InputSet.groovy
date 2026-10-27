package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * The files one run read from outside its own work directory and outputs
 * (explorer v0.3 P4 plan 1.3, DESIGN.md §22), linked from
 * `RunCompletion.input_set`. An input's address is computed whether or not
 * the store keeps its bytes, so two runs that read the same bytes share it.
 *
 * Inputs are a set: the constructor drops repeats and sorts by (role, name,
 * address string, sample, reason, size, from), nulls first, so the same reads
 * give one block whatever order they were seen in. `from` compares last: null
 * before non-null, then element-wise by Origin, then the shorter list first.
 *
 * Schema 2 adds `Input.from`, the fromStore items an input was read from.
 * Schema 1 blocks still read, with every `from` null.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class InputSet {

    static final String SAMPLESHEET = 'samplesheet'
    static final String TASK = 'task'
    /** A file the pipeline itself ships, under its projectDir: never a sample's input. */
    static final String ASSET = 'asset'
    static final String PARAM_PREFIX = 'param:'

    static final int SCHEMA = 2
    static final List<Integer> READABLE = Collections.unmodifiableList([1, 2])

    static final String UNADDRESSED = 'unaddressed'
    static final String UNREADABLE = 'unreadable'
    static final String REMOTE = 'remote'
    /**
     * Still being stat'ed or hashed when the run stopped waiting
     * (cas.inputs.waitSeconds): a set is partial exactly when an input
     * carries this reason.
     */
    static final String UNFINISHED = 'unfinished'

    /** The fromStore item an input came from, with the collection and selection it was read through. */
    @Canonical
    @CompileStatic
    static final class Origin implements Comparable<Origin> {
        Cid item
        Cid collection
        Cid selection

        Origin(Cid item, Cid collection, Cid selection) {
            if( item == null )
                throw new IllegalArgumentException('an Origin needs an item')
            this.item = item
            this.collection = collection
            this.selection = selection
        }

        Map<String, Object> toCbor() {
            final Map<String, Object> map = new LinkedHashMap<String, Object>()
            map.put('item', item)
            map.put('collection', collection)
            map.put('selection', selection)
            return map
        }

        static Origin fromCbor(Object value) {
            if( !(value instanceof Map) )
                throw new IllegalArgumentException("an InputSet origin is a map, got ${value == null ? 'null' : value.getClass().simpleName}")
            final Map map = (Map) value
            return new Origin(
                Records.cid(Records.require(map, 'item'), 'item'),
                Records.cid(Records.require(map, 'collection'), 'collection'),
                Records.cid(Records.require(map, 'selection'), 'selection'))
        }

        @Override
        int compareTo(Origin o) {
            int c = compareBytes(item, o.item)
            if( c == 0 ) c = compareBytes(collection, o.collection)
            if( c == 0 ) c = compareBytes(selection, o.selection)
            return c
        }

        private static int compareBytes(Cid a, Cid b) {
            if( a == null ) return b == null ? 0 : -1
            if( b == null ) return 1
            return Arrays.compareUnsigned(a.bytes(), b.bytes())
        }
    }

    /** One file a run read. */
    @Canonical
    @CompileStatic
    static final class Input implements Comparable<Input> {
        String role
        String name
        Cid address
        Long size
        String sample
        String reason
        /** The fromStore items this input was read from; null when none, never empty. */
        List<Origin> from

        /** A copy whose `from` is sorted and without repeats, null when there are none. */
        Input withFrom(Collection<Origin> origins) {
            final List<Origin> sorted = origins ? Collections.unmodifiableList(new ArrayList<Origin>(new TreeSet<Origin>(origins))) : (List<Origin>) null
            return new Input(role, name, address, size, sample, reason, sorted)
        }

        Map<String, Object> toCbor() {
            final Map<String, Object> map = new LinkedHashMap<String, Object>()
            map.put('role', role)
            map.put('name', name)
            map.put('address', address)
            map.put('size', size)
            map.put('sample', sample)
            map.put('reason', reason)
            if( from != null )
                map.put('from', from.collect { Origin o -> (Object) o.toCbor() })
            return map
        }

        static Input fromCbor(Object value, boolean readFrom = true) {
            if( !(value instanceof Map) )
                throw new IllegalArgumentException("an InputSet input is a map, got ${value == null ? 'null' : value.getClass().simpleName}")
            final Map map = (Map) value
            final Object size = Records.require(map, 'size')
            final Object from = readFrom ? map.get('from') : null
            if( from != null && !(from instanceof List) )
                throw new IllegalArgumentException("field 'from' should be a list")
            final Input input = new Input(
                Records.string(Records.require(map, 'role'), 'role'),
                Records.string(Records.require(map, 'name'), 'name'),
                Records.cid(Records.require(map, 'address'), 'address'),
                size == null ? null : (Long) Records.number(size, 'size'),
                Records.string(Records.require(map, 'sample'), 'sample'),
                Records.string(Records.require(map, 'reason'), 'reason'))
            return from == null ? input : input.withFrom(((List) from).collect { Object o -> Origin.fromCbor(o) })
        }

        @Override
        int compareTo(Input o) {
            int c = role <=> o.role
            if( c == 0 ) c = nullsFirst(name, o.name)
            if( c == 0 ) c = nullsFirst(address?.toString(), o.address?.toString())
            if( c == 0 ) c = nullsFirst(sample, o.sample)
            if( c == 0 ) c = nullsFirst(reason, o.reason)
            if( c == 0 ) c = size <=> o.size
            if( c == 0 ) c = compareFrom(from, o.from)
            return c
        }

        private static int compareFrom(List<Origin> a, List<Origin> b) {
            if( a == null ) return b == null ? 0 : -1
            if( b == null ) return 1
            final int n = Math.min(a.size(), b.size())
            for( int i = 0; i < n; i++ ) {
                final int c = a.get(i).compareTo(b.get(i))
                if( c != 0 ) return c
            }
            return a.size() <=> b.size()
        }

        private static int nullsFirst(String a, String b) {
            if( a == null ) return b == null ? 0 : -1
            if( b == null ) return 1
            return a <=> b
        }
    }

    final String assertedBy
    final List<Input> inputs

    InputSet(String assertedBy, Collection<Input> inputs) {
        if( !assertedBy )
            throw new IllegalArgumentException('an InputSet needs an asserted_by')
        for( Input i : inputs ?: Collections.<Input> emptyList() ) {
            if( i == null || !i.role )
                throw new IllegalArgumentException('an InputSet input needs a role')
            if( i.address == null && !i.reason )
                throw new IllegalArgumentException("an input without an address needs a reason (${i.name ?: 'unnamed'})")
            if( i.address != null && i.reason )
                throw new IllegalArgumentException("an input addressed as ${i.address} cannot also carry the reason '${i.reason}'")
        }
        this.assertedBy = assertedBy
        this.inputs = Collections.unmodifiableList(new ArrayList<Input>(new TreeSet<Input>(inputs ?: Collections.<Input> emptyList())))
    }

    /** The index of an input equal to this one in `inputs`, or -1. */
    int positionOf(Input input) {
        final int at = Collections.binarySearch(inputs, input)
        return at >= 0 ? at : -1
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.INPUT_SET, SCHEMA)
        map.put('asserted_by', assertedBy)
        map.put('inputs', inputs.collect { Input i -> (Object) i.toCbor() })
        return map
    }

    static InputSet fromCbor(Map block) {
        Records.expectKind(block, Records.INPUT_SET)
        final int schema = Records.schemaOf(block, READABLE)
        final Object inputs = Records.require(block, 'inputs')
        if( !(inputs instanceof List) )
            throw new IllegalArgumentException("field 'inputs' should be a list")
        return new InputSet(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            ((List) inputs).collect { Object i -> Input.fromCbor(i, schema >= 2) })
    }
}
