package robsyme.cas

import java.nio.file.Path
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import nextflow.file.FileHelper
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
 *     asserted_by = 'gate'    // optional; see docs/identity.md
 * }
 * </pre>
 *
 * The scope is not part of {@code nextflow.lineage.config.LineageConfig}, so it
 * is read from the raw session config map.
 *
 * A store's location may be local or, since DESIGN.md §2, `s3://<bucket>[/<prefix>]`;
 * an S3 member may be the writable one or named in `cas.resolve` like any other.
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

    // Any other <scheme>:// location is refused: only a local path or an s3:// one is a member.
    private static final Pattern OTHER_SCHEME = ~/^[A-Za-z][A-Za-z0-9+.-]*:\/\//

    static final String DEFAULT_ASSERTED_BY = 'anonymous'

    private static final Set<String> ARCHIVE = ['GLACIER', 'DEEP_ARCHIVE'] as Set
    private static final Set<String> INFREQUENT = ['STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING'] as Set
    // nf-amazon 3.9.2 AwsS3Config.parseStorageClass keeps only these (and REDUCED_REDUNDANCY, STANDARD).
    private static final Set<String> NF_AMAZON_ACCEPTS = ['STANDARD', 'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'REDUCED_REDUNDANCY'] as Set

    /** Alias of the writable member, i.e. the authority of `lineage.store.location`. */
    final String writableAlias

    /** Every resolvable member alias, the writable one first, local or S3. */
    final List<String> members

    /** Every configured store alias, writable first: what nf-blocks:explore serves. */
    final List<String> configuredAliases

    private final Map<String,Path> locations

    private final Map<String, S3Location> remotes

    private final Set<String> readOnlyAliases

    /** The explicit `cas.asserted_by`, or null when the session resolves a principal (docs/identity.md). */
    final String assertedBy

    /** `cas.index.path`, or null for the per-user cache path (DESIGN.md §12). */
    final String indexOverride

    /** `cas.snapshot.maxBytes`: a run rewrites the Index Snapshot only while under this (DESIGN.md §15). */
    final long snapshotMaxBytes

    /** The raw session config map, as given to {@link #from}. */
    final Map rawConfig

    /** `cas.tmpDir`: scratch directory for content of unknown length on its way to an S3 member. */
    final Path tmpDir

    /** `cas.nodeHash` as configured, or null when left to default to `fusion.enabled` (see {@link #nodeHashEnabled}). */
    final Boolean nodeHashSetting

    /** A warning about `aws.client.storageClass` on the writable member, or null (see {@link #judgeStorageClass}). */
    final String storageClassWarning

    /**
     * `cas.inputs.hash` (default true): the observer hashes the files a run
     * read for its InputSet. When false they are listed unaddressed, with no
     * read of their content (explorer v0.3 P4 plan 1.3).
     */
    final boolean inputsHash

    /**
     * `cas.inputs.waitSeconds` (default {@link #DEFAULT_INPUTS_WAIT_SECONDS}):
     * how long a completing run waits for the stats and hashes of its inputs.
     * A failed run waits at most 30 s of it. An input still being read then
     * is listed with reason "unfinished".
     */
    final int inputsWaitSeconds

    static final int DEFAULT_INPUTS_WAIT_SECONDS = 600

    private CasConfig(String writableAlias, List<String> members, List<String> configuredAliases,
                       Map<String,Path> locations, Map<String,S3Location> remotes, Set<String> readOnlyAliases, String assertedBy,
                       String indexOverride, long snapshotMaxBytes, Map rawConfig, Path tmpDir,
                       Boolean nodeHashSetting, String storageClassWarning, boolean inputsHash, int inputsWaitSeconds) {
        this.writableAlias = writableAlias
        this.members = Collections.unmodifiableList(members)
        this.configuredAliases = Collections.unmodifiableList(configuredAliases)
        this.locations = Collections.unmodifiableMap(locations)
        this.remotes = Collections.unmodifiableMap(remotes)
        this.readOnlyAliases = Collections.unmodifiableSet(readOnlyAliases)
        this.assertedBy = assertedBy
        this.indexOverride = indexOverride
        this.snapshotMaxBytes = snapshotMaxBytes
        this.rawConfig = rawConfig
        this.tmpDir = tmpDir
        this.nodeHashSetting = nodeHashSetting
        this.storageClassWarning = storageClassWarning
        this.inputsHash = inputsHash
        this.inputsWaitSeconds = inputsWaitSeconds
    }

    /** The writable member's local path, or null when the writable member is remote. */
    Path getWritableLocation() {
        return locations.get(writableAlias)
    }

    Path locationOf(String alias) {
        return locations.get(alias)
    }

    boolean isMember(String alias) {
        return locations.containsKey(alias)
    }

    /** `cas.stores.<alias>.readOnly`: true when the config marks this member as never to be written, even by a copy. */
    boolean readOnly(String alias) { readOnlyAliases.contains(alias) }

    boolean isRemote(String alias) { remotes.containsKey(alias) }

    S3Location remoteOf(String alias) { remotes.get(alias) }

    URI remoteLocationOf(String alias) { URI.create(remotes.get(alias).toString()) }

    String locationText(String alias) {
        return remotes.containsKey(alias) ? remotes.get(alias).toString() : locations.get(alias)?.toString()
    }

    /** Every resolvable member's location text, writable first: what IndexPaths names the cache file by. */
    List<String> locationTexts() { members.collect { String a -> locationText(a) } }

    /** The member's location as a Path through FileHelper.asPath; an S3 one needs nf-amazon started. */
    Path pathOf(String alias) {
        return remotes.containsKey(alias) ? FileHelper.asPath(remotes.get(alias).toString()) : locations.get(alias)
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

        final Map<String, Path> locations = new LinkedHashMap<String, Path>()
        final Map<String, S3Location> remotes = new LinkedHashMap<String, S3Location>()
        final Set<String> readOnlyAliases = new LinkedHashSet<String>()
        for( Map.Entry entry : stores.entrySet() ) {
            final String name = entry.key as String
            checkAlias(name)
            final String location = requiredLocationText(name, entry.value)
            final Object readOnlyValue = entry.value instanceof Map ? ((Map) entry.value).get('readOnly') : null
            if( readOnlyValue != null && !(readOnlyValue instanceof Boolean) )
                throw new IllegalArgumentException("cas.stores.${name}.readOnly must be true or false -- offending value: ${readOnlyValue}")
            if( readOnlyValue == Boolean.TRUE )
                readOnlyAliases.add(name)
            if( location.startsWith('s3://') ) {
                if( !S3_LOCATION.matcher(location).matches() )
                    throw new IllegalArgumentException("cas.stores.${name}.location is not an S3 URI of the form s3://<bucket>[/<prefix>] -- offending value: ${location}")
                remotes.put(name, S3Location.parse(location))
            }
            else {
                if( OTHER_SCHEME.matcher(location).find() )
                    throw new IllegalArgumentException("cas.stores.${name}.location must be a local directory or s3://<bucket>[/<prefix>] -- offending value: ${location}")
                locations.put(name, FileHelper.asPath(location).toAbsolutePath().normalize())
            }
        }
        if( !locations.containsKey(alias) && !remotes.containsKey(alias) )
            throw new IllegalArgumentException("Missing store configuration 'cas.stores.${alias}' for the writable member '${alias}' named by lineage.store.location")

        if( readOnlyAliases.contains(alias) )
            throw new IllegalArgumentException("cas.stores.${alias}.readOnly = true, but '${alias}' is the writable member named by lineage.store.location")

        final List<String> members = memberList(alias, scope.get('resolve'), locations.keySet() + remotes.keySet())
        final List<String> configured = [alias] + ((locations.keySet() + remotes.keySet()) - alias).toList()
        final String assertedBy = scope.get('asserted_by')?.toString()?.trim() ?: null
        final Object indexScope = scope.get('index')
        final String indexOverride = indexScope instanceof Map ? ((Map) indexScope).get('path') as String : null
        final Object snapshotScope = scope.get('snapshot')
        final long snapshotMaxBytes = bytesOf(snapshotScope instanceof Map ? ((Map) snapshotScope).get('maxBytes') : null)

        final Object tmpDirValue = scope.get('tmpDir')
        final Path tmpDir = tmpDirValue ? FileHelper.asPath(tmpDirValue as String) : Path.of(System.getProperty('java.io.tmpdir'))

        final Object nodeHashValue = scope.get('nodeHash')
        if( nodeHashValue != null && !(nodeHashValue instanceof Boolean) )
            throw new IllegalArgumentException("cas.nodeHash must be true or false -- offending value: ${nodeHashValue}")
        final Boolean nodeHashSetting = (Boolean) nodeHashValue

        final String storageClassWarning = judgeStorageClass(sessionConfig, remotes.containsKey(alias))

        final Object inputsScope = scope.get('inputs')
        if( inputsScope != null && !(inputsScope instanceof Map) )
            throw new IllegalArgumentException("cas.inputs must be a scope, e.g. cas { inputs { hash = false } } -- offending value: ${inputsScope}")
        final Object inputsHashValue = inputsScope instanceof Map ? ((Map) inputsScope).get('hash') : null
        if( inputsHashValue != null && !(inputsHashValue instanceof Boolean) )
            throw new IllegalArgumentException("cas.inputs.hash must be true or false -- offending value: ${inputsHashValue}")
        final boolean inputsHash = inputsHashValue == null || (Boolean) inputsHashValue
        final Object inputsWaitValue = inputsScope instanceof Map ? ((Map) inputsScope).get('waitSeconds') : null
        if( inputsWaitValue != null && !(inputsWaitValue instanceof Integer && (Integer) inputsWaitValue >= 0) )
            throw new IllegalArgumentException("cas.inputs.waitSeconds must be a whole number of seconds, 0 or more -- offending value: ${inputsWaitValue}")
        final int inputsWaitSeconds = inputsWaitValue == null ? DEFAULT_INPUTS_WAIT_SECONDS : (Integer) inputsWaitValue

        return new CasConfig(alias, members, configured, locations, remotes, readOnlyAliases, assertedBy, indexOverride,
            snapshotMaxBytes, sessionConfig, tmpDir, nodeHashSetting, storageClassWarning, inputsHash, inputsWaitSeconds)
    }

    /** Node-side hashing (DESIGN.md §11): cas.nodeHash when set, else fusion.enabled. */
    static boolean nodeHashEnabled(Map sessionConfig) {
        final Object cas = sessionConfig?.get(SCHEME)
        final Object explicit = cas instanceof Map ? ((Map) cas).get('nodeHash') : null
        if( explicit instanceof Boolean )
            return (Boolean) explicit
        final Object fusion = sessionConfig?.get('fusion')
        return fusion instanceof Map && ((Map) fusion).get('enabled') == Boolean.TRUE
    }

    /**
     * Ticket 02 decision 9: blocks inherit aws.client.storageClass. One object
     * per block pays each class's per-object minimum, which packing (spec §3)
     * exists to avoid, so archive classes are refused and infrequent-access
     * ones warn. Judged only when the writable member is on S3.
     */
    private static String judgeStorageClass(Map sessionConfig, boolean writableIsRemote) {
        if( !writableIsRemote ) return null
        final Object aws = sessionConfig?.get('aws')
        final Object client = aws instanceof Map ? ((Map) aws).get('client') : null
        final String cls = client instanceof Map ? (((Map) client).get('storageClass') ?: ((Map) client).get('uploadStorageClass')) as String : null
        if( !cls ) return null
        if( ARCHIVE.contains(cls) )
            throw new IllegalArgumentException("aws.client.storageClass = '${cls}' would archive every block of the writable S3 member; blocks must stay readable, and archive tiers need packing (spec §3), which nf-blocks does not do yet")
        if( INFREQUENT.contains(cls) )
            return "aws.client.storageClass = '${cls}': every block is its own object, so each pays that class's per-object minimum; packing (spec §3), which would avoid it, is not built yet".toString()
        if( !NF_AMAZON_ACCEPTS.contains(cls) )
            return "aws.client.storageClass = '${cls}': nf-amazon ignores this class (AwsS3Config.parseStorageClass), so blocks are written as STANDARD".toString()
        return null
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

    private static String requiredLocationText(String alias, Object storeOpts) {
        final location = (storeOpts instanceof Map ? ((Map)storeOpts).get('location') : null) as String
        if( !location )
            throw new IllegalArgumentException("Missing 'cas.stores.${alias}.location'")
        return location
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
