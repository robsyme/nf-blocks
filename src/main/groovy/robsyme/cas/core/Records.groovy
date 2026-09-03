package robsyme.cas.core

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * The block kinds of DESIGN.md §6, and the two functions that every kind
 * shares: the portability scrub and the kind tag.
 *
 * Each kind is a small immutable class with {@code Map toCbor()} and
 * {@code static X fromCbor(Map)}. The map is the contract, not the class:
 * a record's address is the address of {@code DagCbor.encode(record.toCbor())},
 * so a decoded record must re-encode to the bytes it came from.
 */
@CompileStatic
class Records {

    /** Every block kind is at schema 1 in the Walking Skeleton. */
    static final int SCHEMA = 1

    static final String DIRECTORY_MANIFEST = 'DirectoryManifest'
    static final String OUTPUT_ITEM = 'OutputItem'
    static final String OUTPUT_COLLECTION = 'OutputCollection'
    static final String RUN_MANIFEST = 'RunManifest'
    static final String RUN_COMPLETION = 'RunCompletion'
    static final String LEAF = 'Leaf'

    static final String REDACTED_LOCATION = '[redacted-location]'
    static final String REDACTED_USER = '[redacted-user]'

    /** The kind tag of a decoded block, or null when there is none. */
    static String kindOf(Map block) {
        final Object kind = block?.get('kind')
        return kind instanceof String ? (String) kind : null
    }

    // ---- helpers shared by the kinds ----

    static Map<String, Object> head(String kind) {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('kind', kind)
        map.put('schema', (long) SCHEMA)
        return map
    }

    static void expectKind(Map block, String kind) {
        if( block == null )
            throw new IllegalArgumentException("expected a $kind block, got null")
        final String actual = kindOf(block)
        if( actual != kind )
            throw new IllegalArgumentException("expected a $kind block, got ${actual ?: 'a block with no kind'}")
    }

    static Object require(Map block, String field) {
        if( !block.containsKey(field) )
            throw new IllegalArgumentException("${kindOf(block) ?: 'block'} is missing the field '$field'")
        return block.get(field)
    }

    static String string(Object value, String field) {
        if( value == null )
            return null
        if( !(value instanceof String) )
            throw new IllegalArgumentException("field '$field' should be a string, got ${value.getClass().simpleName}")
        return (String) value
    }

    static long number(Object value, String field) {
        if( value instanceof Number )
            return ((Number) value).longValue()
        throw new IllegalArgumentException("field '$field' should be a number, got ${value == null ? 'null' : value.getClass().simpleName}")
    }

    static Cid cid(Object value, String field) {
        if( value == null )
            return null
        if( !(value instanceof Cid) )
            throw new IllegalArgumentException("field '$field' should be a content address, got ${value.getClass().simpleName}")
        return (Cid) value
    }

    static int utf8Length(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length
    }
}

/**
 * One entry of a DirectoryManifest: name, mode, size and address, and nothing
 * else (DESIGN.md §6). No permission bits beyond the executable one, no mtime:
 * identity must not depend on the umask of whoever ran the pipeline.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ManifestEntry {

    static final String REGULAR = 'regular'
    static final String EXECUTABLE = 'executable'
    static final String SYMLINK = 'symlink'
    static final String DIRECTORY = 'directory'
    static final String UNRESOLVABLE = 'unresolvable'

    private static final Set<String> MODES = [REGULAR, EXECUTABLE, SYMLINK, DIRECTORY, UNRESOLVABLE] as Set

    final String name
    final String mode
    final long size
    final Cid address
    final String target

    ManifestEntry(String name, String mode, long size, Cid address, String target) {
        if( !name || name.contains('/') || name == '.' || name == '..' )
            throw new IllegalArgumentException("a manifest entry name is one path segment: ${name == null ? 'null' : "'$name'"}")
        if( !MODES.contains(mode) )
            throw new IllegalArgumentException("unknown manifest entry mode '$mode' for '$name'")
        if( size < 0 )
            throw new IllegalArgumentException("manifest entry '$name' has a negative size")
        if( mode == REGULAR || mode == EXECUTABLE ) {
            if( address == null || !address.isRaw() )
                throw new IllegalArgumentException("manifest entry '$name' is $mode and needs a raw content address, got ${address ?: 'null'}")
        }
        else if( mode == DIRECTORY ) {
            if( address == null || !address.isDagCbor() )
                throw new IllegalArgumentException("manifest entry '$name' is a directory and needs a dag-cbor manifest address, got ${address ?: 'null'}")
        }
        else if( address != null ) {
            throw new IllegalArgumentException("manifest entry '$name' is $mode and cannot carry an address")
        }
        if( mode == SYMLINK && !target )
            throw new IllegalArgumentException("manifest entry '$name' is a symlink and needs its target text")
        this.name = name
        this.mode = mode
        this.size = size
        this.address = address
        this.target = target
    }

    static ManifestEntry regular(String name, Cid address, long size) {
        return new ManifestEntry(name, REGULAR, size, address, null)
    }

    static ManifestEntry executable(String name, Cid address, long size) {
        return new ManifestEntry(name, EXECUTABLE, size, address, null)
    }

    /** A subdirectory: the address is its own manifest, the size is not the tree's. */
    static ManifestEntry directory(String name, Cid manifest) {
        return new ManifestEntry(name, DIRECTORY, 0L, manifest, null)
    }

    /** A link kept as a link: its content is the target text. */
    static ManifestEntry symlink(String name, String target) {
        return new ManifestEntry(name, SYMLINK, Records.utf8Length(target), null, target)
    }

    /** Something that could not be addressed. Counted, never dropped. */
    static ManifestEntry unresolvable(String name, String target) {
        return new ManifestEntry(name, UNRESOLVABLE, Records.utf8Length(target), null, target)
    }

    boolean isDirectory() { mode == DIRECTORY }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('name', name)
        map.put('mode', mode)
        map.put('size', size)
        map.put('address', address)
        map.put('target', target)
        return map
    }

    static ManifestEntry fromCbor(Map entry) {
        return new ManifestEntry(
            Records.string(Records.require(entry, 'name'), 'name'),
            Records.string(Records.require(entry, 'mode'), 'mode'),
            Records.number(Records.require(entry, 'size'), 'size'),
            Records.cid(Records.require(entry, 'address'), 'address'),
            Records.string(Records.require(entry, 'target'), 'target'))
    }
}

/**
 * A directory as a sorted list of entries (DESIGN.md §6). Sorted by the raw
 * UTF-8 bytes of the name, so the serialisation is canonical and a receiver
 * can verify a manifest by re-encoding it. Carries no {@code asserted_by}:
 * identical content is one block whoever stored it.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class DirectoryManifest {

    final List<ManifestEntry> entries

    DirectoryManifest(List<ManifestEntry> entries) {
        final List<ManifestEntry> sorted = new ArrayList<ManifestEntry>(entries ?: Collections.<ManifestEntry> emptyList())
        sorted.sort { ManifestEntry a, ManifestEntry b -> compareNames(a.name, b.name) }
        for( int i = 1; i < sorted.size(); i++ ) {
            if( sorted.get(i).name == sorted.get(i - 1).name )
                throw new IllegalArgumentException("a directory manifest cannot hold '${sorted.get(i).name}' twice")
        }
        this.entries = Collections.unmodifiableList(sorted)
    }

    /** Ascending on the raw name bytes: Unicode normalisation forms are different names. */
    private static int compareNames(String a, String b) {
        final byte[] left = a.getBytes(StandardCharsets.UTF_8)
        final byte[] right = b.getBytes(StandardCharsets.UTF_8)
        final int n = Math.min(left.length, right.length)
        for( int i = 0; i < n; i++ ) {
            final int diff = (left[i] & 0xff) - (right[i] & 0xff)
            if( diff != 0 )
                return diff
        }
        return left.length - right.length
    }

    ManifestEntry entry(String name) {
        return entries.find { ManifestEntry e -> e.name == name }
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.DIRECTORY_MANIFEST)
        map.put('entries', entries.collect { ManifestEntry e -> e.toCbor() })
        return map
    }

    static DirectoryManifest fromCbor(Map block) {
        Records.expectKind(block, Records.DIRECTORY_MANIFEST)
        final Object entries = Records.require(block, 'entries')
        if( !(entries instanceof List) )
            throw new IllegalArgumentException('a directory manifest needs a list of entries')
        return new DirectoryManifest(((List) entries).collect { Object e -> ManifestEntry.fromCbor((Map) e) })
    }
}

/**
 * What a run could not address, counted rather than hidden (DESIGN.md §6).
 * Directory publishing produces {@code unresolvable}; the output join produces
 * the other three.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Anomalies {

    static final Anomalies NONE = new Anomalies(0, 0, 0, 0)

    final int unresolvable
    final int unaddressed
    final int declined
    final int neverPublished

    Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished) {
        this.unresolvable = unresolvable
        this.unaddressed = unaddressed
        this.declined = declined
        this.neverPublished = neverPublished
    }

    static Anomalies unresolvable(int count) {
        return new Anomalies(count, 0, 0, 0)
    }

    Anomalies plus(Anomalies other) {
        if( other == null )
            return this
        return new Anomalies(
            unresolvable + other.unresolvable,
            unaddressed + other.unaddressed,
            declined + other.declined,
            neverPublished + other.neverPublished)
    }

    boolean isEmpty() {
        return unresolvable == 0 && unaddressed == 0 && declined == 0 && neverPublished == 0
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('unresolvable', (long) unresolvable)
        map.put('unaddressed', (long) unaddressed)
        map.put('declined', (long) declined)
        map.put('never_published', (long) neverPublished)
        return map
    }

    static Anomalies fromCbor(Map counts) {
        if( counts == null )
            throw new IllegalArgumentException('a run completion needs its anomaly counters')
        return new Anomalies(
            (int) Records.number(Records.require(counts, 'unresolvable'), 'unresolvable'),
            (int) Records.number(Records.require(counts, 'unaddressed'), 'unaddressed'),
            (int) Records.number(Records.require(counts, 'declined'), 'declined'),
            (int) Records.number(Records.require(counts, 'never_published'), 'never_published'))
    }
}
