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

    // The path, if any, excludes '?', '#' and whitespace: those split a URI
    // into path/query/fragment differently than this pattern would validate,
    // so a location that contains them is refused here rather than accepted
    // and then parsed differently by S3MemberFiles.bucketAndPrefix.
    private static final Pattern S3_LOCATION = ~/^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](\/[^?#\s]*)?$/

    static final String DEFAULT_ASSERTED_BY = 'anonymous'

    /** Alias of the writable member, i.e. the authority of `lineage.store.location`. */
    final String writableAlias

    /** Every resolvable member alias, the writable one first. Local aliases only. */
    final List<String> members

    /** Every configured store alias, writable first: what nf-blocks:explore serves. */
    final List<String> configuredAliases

    private final Map<String,Path> locations

    private final Map<String, URI> remotes

    /** Opaque label recorded in every asserted block. */
    final String assertedBy

    /** `cas.index.path`, or null for the per-user cache path (DESIGN.md §12). */
    final String indexOverride

    /** `cas.snapshot.maxBytes`: a run rewrites the Index Snapshot only while under this (DESIGN.md §15). */
    final long snapshotMaxBytes

    private CasConfig(String writableAlias, List<String> members, List<String> configuredAliases,
                       Map<String,Path> locations, Map<String,URI> remotes, String assertedBy,
                       String indexOverride, long snapshotMaxBytes) {
        this.writableAlias = writableAlias
        this.members = Collections.unmodifiableList(members)
        this.configuredAliases = Collections.unmodifiableList(configuredAliases)
        this.locations = Collections.unmodifiableMap(locations)
        this.remotes = Collections.unmodifiableMap(remotes)
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

    boolean isRemote(String alias) { remotes.containsKey(alias) }

    URI remoteLocationOf(String alias) { remotes.get(alias) }

    /** The resolvable members' local paths, as IndexPaths names the cache file by them. */
    List<String> localLocations() { members.collect { String a -> locations.get(a).toString() } }

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

        final Map<String, Path> locations = new LinkedHashMap<String, Path>()
        final Map<String, URI> remotes = new LinkedHashMap<String, URI>()
        for( Map.Entry entry : stores.entrySet() ) {
            final String name = entry.key as String
            checkAlias(name)
            final String location = locationText(name, entry.value)
            if( location.startsWith('s3://') ) {
                if( !S3_LOCATION.matcher(location).matches() )
                    throw new IllegalArgumentException("cas.stores.${name}.location is not an S3 URI of the form s3://<bucket>[/<prefix>] -- offending value: ${location}")
                remotes.put(name, URI.create(location))
            }
            else {
                locations.put(name, Path.of(location).toAbsolutePath().normalize())
            }
        }
        if( remotes.containsKey(alias) )
            throw new IllegalArgumentException("the writable member '${alias}' must be a local directory, not ${remotes.get(alias)}; S3 members are read-only and only nf-blocks:explore reads them")
        if( !locations.containsKey(alias) )
            throw new IllegalArgumentException("Missing store configuration 'cas.stores.${alias}' for the writable member '${alias}' named by lineage.store.location")

        final List<String> members = memberList(alias, scope.get('resolve'), locations.keySet(), remotes)
        final List<String> configured = [alias] + ((locations.keySet() + remotes.keySet()) - alias).toList()
        final assertedBy = (scope.get('asserted_by') ?: DEFAULT_ASSERTED_BY) as String
        final Object indexScope = scope.get('index')
        final String indexOverride = indexScope instanceof Map ? ((Map) indexScope).get('path') as String : null
        final Object snapshotScope = scope.get('snapshot')
        final long snapshotMaxBytes = bytesOf(snapshotScope instanceof Map ? ((Map) snapshotScope).get('maxBytes') : null)
        return new CasConfig(alias, members, configured, locations, remotes, assertedBy, indexOverride, snapshotMaxBytes)
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

    private static String locationText(String alias, Object storeOpts) {
        final location = (storeOpts instanceof Map ? ((Map)storeOpts).get('location') : null) as String
        if( !location )
            throw new IllegalArgumentException("Missing 'cas.stores.${alias}.location'")
        return location
    }

    private static List<String> memberList(String writable, Object resolve, Set<String> known, Map<String, URI> remotes) {
        if( resolve != null && !(resolve instanceof List) )
            throw new IllegalArgumentException("cas.resolve must be a list of store aliases, e.g. ['lab', 'shared'] -- offending value: ${resolve}")
        final List<String> requested = resolve != null
            ? ((List) resolve).collect { it as String }
            : new ArrayList<String>(known)
        for( String name : requested ) {
            if( remotes.containsKey(name) )
                throw new IllegalArgumentException("store '${name}' is an S3 member (${remotes.get(name)}), which only nf-blocks:explore reads so far; leave it out of cas.resolve")
        }
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
