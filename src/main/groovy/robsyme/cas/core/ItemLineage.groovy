package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

import java.util.regex.Pattern

/**
 * Where each output item of one run came from (cross-run lineage spec §5.2,
 * DESIGN.md §6), linked from `RunCompletion.item_lineage`. Each entry names
 * an item, the positions in the run's InputSet of the inputs its tasks read,
 * and the tasks that produced it. Positions index `input_set`, so a lineage
 * only means something beside the InputSet it names.
 *
 * Items are sorted by item address bytes and appear once; positions ascend
 * without repeats and tasks sort by hash, so the same facts give one block
 * whatever order they were seen in.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ItemLineage {

    static final int SCHEMA = 1

    /** A task that produced an item, as the explorer shows and links it. */
    @Canonical
    @CompileStatic
    static final class TaskRef implements Comparable<TaskRef> {
        private static final Pattern HASH = Pattern.compile('[0-9a-f]{32}')

        String hash
        String process
        String tag

        TaskRef(String hash, String process, String tag) {
            if( hash == null || !HASH.matcher(hash).matches() )
                throw new IllegalArgumentException("a task hash is 32 lowercase hex digits, got '${hash}'")
            if( !process?.trim() )
                throw new IllegalArgumentException('a task reference needs a process name')
            this.hash = hash
            this.process = process
            this.tag = tag
        }

        Map<String, Object> toCbor() {
            final Map<String, Object> map = new LinkedHashMap<String, Object>()
            map.put('hash', hash)
            map.put('process', process)
            map.put('tag', tag)
            return map
        }

        static TaskRef fromCbor(Object value) {
            if( !(value instanceof Map) )
                throw new IllegalArgumentException("an ItemLineage task is a map, got ${value == null ? 'null' : value.getClass().simpleName}")
            final Map map = (Map) value
            return new TaskRef(
                Records.string(Records.require(map, 'hash'), 'hash'),
                Records.string(Records.require(map, 'process'), 'process'),
                Records.string(Records.require(map, 'tag'), 'tag'))
        }

        @Override
        int compareTo(TaskRef o) {
            int c = hash <=> o.hash
            if( c == 0 ) c = process <=> o.process
            if( c == 0 ) c = nullsFirst(tag, o.tag)
            return c
        }

        private static int nullsFirst(String a, String b) {
            if( a == null ) return b == null ? 0 : -1
            if( b == null ) return 1
            return a <=> b
        }
    }

    /** One output item with the inputs it was derived from and the tasks that made it. */
    @Canonical
    @CompileStatic
    static final class Derived {
        Cid item
        List<Integer> inputs
        List<TaskRef> tasks

        /** Sorts and dedupes both lists. */
        static Derived of(Cid item, Collection<Integer> inputs, Collection<TaskRef> tasks) {
            return new Derived(item,
                Collections.unmodifiableList(new ArrayList<Integer>(new TreeSet<Integer>(inputs ?: Collections.<Integer> emptyList()))),
                Collections.unmodifiableList(new ArrayList<TaskRef>(new TreeSet<TaskRef>(tasks ?: Collections.<TaskRef> emptyList()))))
        }

        Map<String, Object> toCbor() {
            final Map<String, Object> map = new LinkedHashMap<String, Object>()
            map.put('item', item)
            map.put('inputs', inputs.collect { Integer i -> (Object) Long.valueOf(i.longValue()) })
            map.put('tasks', tasks.collect { TaskRef t -> (Object) t.toCbor() })
            return map
        }

        static Derived fromCbor(Object value) {
            if( !(value instanceof Map) )
                throw new IllegalArgumentException("an ItemLineage item is a map, got ${value == null ? 'null' : value.getClass().simpleName}")
            final Map map = (Map) value
            final Object inputs = Records.require(map, 'inputs')
            final Object tasks = Records.require(map, 'tasks')
            if( !(inputs instanceof List) )
                throw new IllegalArgumentException("field 'inputs' should be a list")
            if( !(tasks instanceof List) )
                throw new IllegalArgumentException("field 'tasks' should be a list")
            return of(
                Records.cid(Records.require(map, 'item'), 'item'),
                ((List) inputs).collect { Object i -> Integer.valueOf((int) Records.number(i, 'inputs')) },
                ((List) tasks).collect { Object t -> TaskRef.fromCbor(t) })
        }
    }

    final String assertedBy
    final Cid inputSet
    final List<Derived> items

    ItemLineage(String assertedBy, Cid inputSet, Collection<Derived> items) {
        if( !assertedBy?.trim() )
            throw new IllegalArgumentException('an ItemLineage needs an asserted_by')
        if( inputSet == null )
            throw new IllegalArgumentException('an ItemLineage needs the input_set its positions index')
        final List<Derived> sorted = new ArrayList<Derived>()
        for( Derived d : items ?: Collections.<Derived> emptyList() ) {
            if( d == null || d.item == null )
                throw new IllegalArgumentException('an ItemLineage entry needs an item')
            if( d.inputs.isEmpty() && d.tasks.isEmpty() )
                throw new IllegalArgumentException("an ItemLineage entry needs an input or a task (${d.item})")
            for( Integer p : d.inputs )
                if( p < 0 )
                    throw new IllegalArgumentException("an input position cannot be negative (${d.item}: ${p})")
            sorted.add(d)
        }
        sorted.sort { Derived a, Derived b -> Arrays.compareUnsigned(a.item.bytes(), b.item.bytes()) }
        for( int i = 1; i < sorted.size(); i++ )
            if( sorted.get(i).item == sorted.get(i - 1).item )
                throw new IllegalArgumentException("an ItemLineage names ${sorted.get(i).item} twice")
        this.assertedBy = assertedBy
        this.inputSet = inputSet
        this.items = Collections.unmodifiableList(sorted)
    }

    /** The entry for an item, or null when this lineage does not name it. */
    Derived itemFor(Cid item) {
        return items.find { Derived d -> d.item == item }
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.ITEM_LINEAGE, SCHEMA)
        map.put('asserted_by', assertedBy)
        map.put('input_set', inputSet)
        map.put('items', items.collect { Derived d -> (Object) d.toCbor() })
        return map
    }

    static ItemLineage fromCbor(Map block) {
        Records.expectKind(block, Records.ITEM_LINEAGE)
        Records.schemaOf(block, [SCHEMA])
        final Object items = Records.require(block, 'items')
        if( !(items instanceof List) )
            throw new IllegalArgumentException("field 'items' should be a list")
        return new ItemLineage(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.cid(Records.require(block, 'input_set'), 'input_set'),
            ((List) items).collect { Object d -> Derived.fromCbor(d) })
    }
}
