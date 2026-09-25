package robsyme.cas

import java.nio.file.Path
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import nextflow.util.MemoryUnit
import robsyme.cas.core.Cid
import robsyme.cas.core.IndexSnapshot

/**
 * The `cas` configuration scope, as specified in DESIGN.md section 2.
 *
 * <pre>
 * cas {
 *     stores {
 *         lab { location = '/data/cas' }
 *     }
 *     resolve = ['lab']
 *     asserted_by = 'anonymous'
 * }
 * </pre>
 *
 * The scope is not part of {@code nextflow.lineage.config.LineageConfig}, so it
 * is read from the raw session config map.
 */
@CompileStatic
class CasConfig {

    static final String SCHEME = 'cas'

    private static final Pattern ALIAS = ~/^[a-z][a-z0-9_-]{0,31}$/

    private static final Pattern CAS_LOCATION = ~/^cas:\/\/([^\/]*)$/

    static final String DEFAULT_ASSERTED_BY = 'anonymous'

    /** Alias of the writable member, i.e. the authority of `lineage.store.location`. */
    final String writableAlias

    /** Every resolvable member alias, the writable one first. */
    final List<String> members

    private final Map<String,Path> locations

    /** Opaque label recorded in every asserted block. */
    final String assertedBy

    /** `cas.index.path`, or null for the per-user cache path (DESIGN.md §12). */
    final String indexOverride

    /** `cas.snapshot.maxBytes`: a run rewrites the Index Snapshot only while under this (DESIGN.md §15). */
    final long snapshotMaxBytes

    private CasConfig(String writableAlias, List<String> members, Map<String,Path> locations, String assertedBy,
                       String indexOverride, long snapshotMaxBytes) {
        this.writableAlias = writableAlias
        this.members = Collections.unmodifiableList(members)
        this.locations = Collections.unmodifiableMap(locations)
        this.assertedBy = assertedBy
        this.indexOverride = indexOverride
        this.snapshotMaxBytes = snapshotMaxBytes
    }

    Path getWritableLocation() {
        return locations.get(writableAlias)
    }

    Path locationOf(String alias) {
        return locations.get(alias)
    }

    boolean isMember(String alias) {
        return locations.containsKey(alias)
    }

    /**
     * The alias named by a `cas://<alias>` location, or {@code null} when the
     * given string is not a bare cas location.
     */
    static String aliasOf(String location) {
        if( !location )
            return null
        final m = CAS_LOCATION.matcher(location)
        return m.matches() ? m.group(1) : null
    }

    /**
     * Builds the scope from a Nextflow session config map alone, taking the
     * writable alias from `lineage.store.location`.
     */
    static CasConfig fromSession(Map sessionConfig) {
        final lineage = sessionConfig?.get('lineage')
        final store = lineage instanceof Map ? ((Map)lineage).get('store') : null
        final location = store instanceof Map ? ((Map)store).get('location') as String : null
        return from(sessionConfig, location)
    }

    /**
     * @param sessionConfig the raw Nextflow session config map
     * @param lineageLocation the value of `lineage.store.location`
     */
    static CasConfig from(Map sessionConfig, String lineageLocation) {
        final alias = aliasOf(lineageLocation)
        if( alias == null )
            throw new IllegalArgumentException("lineage.store.location must be a 'cas://<alias>' location -- offending value: ${lineageLocation}")
        checkAlias(alias)

        final scope = (sessionConfig?.get(SCHEME) ?: Collections.emptyMap()) as Map
        final stores = (scope.get('stores') ?: Collections.emptyMap()) as Map

        final Map<String,Path> locations = new LinkedHashMap<String,Path>()
        for( Map.Entry entry : stores.entrySet() ) {
            final String name = entry.key as String
            checkAlias(name)
            locations.put(name, locationFor(name, entry.value))
        }
        if( !locations.containsKey(alias) )
            throw new IllegalArgumentException("Missing store configuration 'cas.stores.${alias}' for the writable member '${alias}' named by lineage.store.location")

        final List<String> members = memberList(alias, scope.get('resolve'), locations.keySet())
        final assertedBy = (scope.get('asserted_by') ?: DEFAULT_ASSERTED_BY) as String
        final Object indexScope = scope.get('index')
        final String indexOverride = indexScope instanceof Map ? ((Map) indexScope).get('path') as String : null
        final Object snapshotScope = scope.get('snapshot')
        final long snapshotMaxBytes = bytesOf(snapshotScope instanceof Map ? ((Map) snapshotScope).get('maxBytes') : null)
        return new CasConfig(alias, members, locations, assertedBy, indexOverride, snapshotMaxBytes)
    }

    private static long bytesOf(Object value) {
        if( value == null )
            return IndexSnapshot.DEFAULT_MAX_BYTES
        final long bytes
        if( value instanceof MemoryUnit )
            bytes = ((MemoryUnit) value).toBytes()
        else if( value instanceof Number )
            bytes = ((Number) value).longValue()
        else
            bytes = new MemoryUnit(value.toString()).toBytes()
        if( bytes <= 0 )
            throw new IllegalArgumentException("cas.snapshot.maxBytes must be a positive size, e.g. 64.MB -- offending value: ${value}")
        return bytes
    }

    private static Path locationFor(String alias, Object storeOpts) {
        final location = (storeOpts instanceof Map ? ((Map)storeOpts).get('location') : null) as String
        if( !location )
            throw new IllegalArgumentException("Missing 'cas.stores.${alias}.location'")
        return Path.of(location).toAbsolutePath().normalize()
    }

    private static List<String> memberList(String writable, Object resolve, Set<String> known) {
        if( resolve != null && !(resolve instanceof List) )
            throw new IllegalArgumentException("cas.resolve must be a list of store aliases, e.g. ['lab', 'shared'] -- offending value: ${resolve}")
        final List<String> requested = resolve != null
            ? ((List) resolve).collect { it as String }
            : new ArrayList<String>(known)
        for( String name : requested ) {
            if( !known.contains(name) )
                throw new IllegalArgumentException("Unknown store alias '${name}' in cas.resolve -- configured stores: ${known.join(', ')}")
        }
        final List<String> result = new ArrayList<String>()
        result.add(writable)
        for( String name : requested ) {
            if( name != writable )
                result.add(name)
        }
        return result
    }

    private static void checkAlias(String alias) {
        if( Cid.isCid(alias) )
            throw new IllegalArgumentException("Store alias '${alias}' is a content address; an alias must not parse as one")
        if( !ALIAS.matcher(alias ?: '').matches() )
            throw new IllegalArgumentException("Invalid store alias '${alias}' -- an alias must match ${ALIAS.pattern()}")
    }
}
