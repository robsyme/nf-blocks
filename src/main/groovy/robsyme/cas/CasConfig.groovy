package robsyme.cas

import java.nio.file.Path
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import robsyme.cas.core.Cid

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

    private CasConfig(String writableAlias, List<String> members, Map<String,Path> locations, String assertedBy) {
        this.writableAlias = writableAlias
        this.members = Collections.unmodifiableList(members)
        this.locations = Collections.unmodifiableMap(locations)
        this.assertedBy = assertedBy
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
        return new CasConfig(alias, members, locations, assertedBy)
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
