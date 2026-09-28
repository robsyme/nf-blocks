package robsyme.cas.nio

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.spi.FileSystemProvider
import java.security.MessageDigest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortRunException
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.file.FileSystemTransferAware
import robsyme.cas.CasSession
import robsyme.cas.core.Addressed
import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.BlockStore
import robsyme.cas.core.Cid
import robsyme.cas.core.CompositeStore
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.Coordinates
import robsyme.cas.core.DagCbor
import robsyme.cas.core.DirectoryManifest
import robsyme.cas.core.DirectoryManifestBuilder
import robsyme.cas.core.HashBufferPool
import robsyme.cas.core.Leaf
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.ManifestEntry
import robsyme.cas.core.NoSuchBlockException
import robsyme.cas.core.OutputCollection
import robsyme.cas.core.OutputItem
import robsyme.cas.core.Providers
import robsyme.cas.core.Records
import robsyme.cas.core.StoreRef
import robsyme.cas.s3.S3BlockStore

/**
 * The `cas` scheme, for real (DESIGN.md section 8).
 *
 * A Publish Coordinate {@code cas://<alias>/<a/b/c>} is a write-side name that
 * Nextflow's publisher hands us; a Store URI {@code cas://<cid>[/name]} names a
 * block or a Directory Manifest and is immutable. This provider hashes a
 * published file into the Block Store and leaves a Pointer File at the
 * coordinate, and reads a block, a manifest, or a coordinate back.
 *
 * The provider is a JVM singleton but holds no run state: the store, the
 * coordinate tree and the config all come from {@link CasSession}, keyed by the
 * live {@link nextflow.Session}, so a second run in one JVM never writes into
 * the first run's store.
 */
@Slf4j
@CompileStatic
class CasFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    static final String SCHEME = CasPath.SCHEME

    private final CasFileSystem fileSystem = new CasFileSystem(this)

    @Override String getScheme() { SCHEME }

    // --- run state, always via the session

    protected CasSession session() { CasSession.current() }

    protected BlockStore store() { session().store }

    private CasPath cas(Path path) {
        if( !(path instanceof CasPath) )
            throw new java.nio.file.ProviderMismatchException("not a '${SCHEME}' path: ${path}")
        return (CasPath)path
    }

    /** The coordinate tree of the member a coordinate names. */
    private CoordinateTree coordsFor(CasPath p) {
        return session().coordinatesOf(p.alias())
    }

    /** The coordinate path relative to its coords root, i.e. the join tail. */
    private static String relOf(CasPath p) {
        return p.segments.join('/')
    }

    // ------------------------------------------------------------------ resolve

    /** What a cas path names once followed: a block, a manifest, or nothing. */
    @CompileStatic
    private static class CasNode {
        boolean present
        boolean directory
        // A directory coordinate: a single pointer file in the mutable coordinate
        // namespace that dereferences to a manifest. Not a browsable directory here
        // -- the tree lives under the Store URI cas://<manifest>. See DESIGN section 7.
        boolean dirPointer
        boolean symlink
        boolean executable
        long size
        long mtime
        Cid content           // the raw block of a file, or the manifest of a directory
        String linkTarget
        // An Item Occurrence without a leaf name: a directory of the item's leaves by name (DESIGN.md §7).
        Map<String, Leaf> occurrence

        static CasNode absent() { new CasNode(present: false) }
    }

    private CasNode resolve(CasPath p) {
        if( !p.isAbsolute() )
            throw new IllegalArgumentException("cannot resolve a relative cas path: '${p}'")
        return p.isStoreUri() ? resolveStoreUri(p) : resolveCoordinate(p)
    }

    private CasNode resolveStoreUri(CasPath p) {
        final Cid cid = p.cid()
        final List<String> segs = p.segments
        if( cid.isRaw() ) {
            if( segs.size() > 1 )
                throw new IOException("a raw Store URI carries at most one segment (the file name): '${p}'")
            return fileNode(cid)
        }
        // A dag-cbor root: a Directory Manifest, or an Output Collection naming an Item Occurrence (DESIGN.md §7).
        final Map root = blockOf(cid)
        if( root != null && Records.kindOf(root) == Records.OUTPUT_COLLECTION )
            return occurrence(cid, root, segs, p)
        if( segs.isEmpty() )
            return manifestNode(cid)
        return traverse(cid, segs, p)
    }

    /** A decoded metadata block, or null when the store does not hold it. */
    private Map blockOf(Cid cid) {
        if( !store().has(cid) )
            return null
        final InputStream input = store().open(cid)
        try {
            final Object value = DagCbor.decode(input.readAllBytes())
            return value instanceof Map ? (Map) value : null
        }
        finally {
            input.close()
        }
    }

    /**
     * cas://<collection>/<item>[/<leaf>[/<entry>...]]. The first segment names
     * an occurrence when it is one of the collection's items; publish-path
     * traversal of a collection is not built, so anything else is an error
     * naming both readings.
     */
    private CasNode occurrence(Cid collectionCid, Map root, List<String> segs, CasPath p) {
        final OutputCollection collection = OutputCollection.fromCbor(root)
        if( segs.isEmpty() )
            throw new IOException("cas: ${p} is an Output Collection; name one of its items, cas://${collectionCid}/<item>")
        final String first = segs[0]
        if( !Cid.isCid(first) || !collection.items.contains(Cid.parse(first)) )
            throw new IOException("cas: '${first}' in ${p} is neither an item of collection ${collectionCid} (an Item Occurrence) " +
                'nor a publish path this version can traverse')
        final Cid itemCid = Cid.parse(first)
        final Map itemBlock = blockOf(itemCid)
        if( itemBlock == null )
            return CasNode.absent()
        final Map<String, Leaf> leaves = leavesByName(OutputItem.fromCbor(itemBlock).value, p)
        if( segs.size() == 1 )
            return new CasNode(present: true, directory: true, size: 0L, mtime: store().lastModifiedMillis(itemCid), occurrence: leaves)
        final Leaf leaf = leaves.get(segs[1])
        if( leaf == null || !leaf.addressed )
            return CasNode.absent()
        if( leaf.address.isRaw() )
            return segs.size() == 2 ? fileNode(leaf.address) : CasNode.absent()
        return segs.size() == 2 ? manifestNode(leaf.address) : traverse(leaf.address, segs.subList(2, segs.size()), p)
    }

    /** The item's named leaves by name; a name two leaves share is refused, naming both positions. */
    private static Map<String, Leaf> leavesByName(Object value, CasPath p) {
        final Map<String, Leaf> byName = new LinkedHashMap<String, Leaf>()
        final Map<String, String> positions = new HashMap<String, String>()
        collectLeaves(value, '', byName, positions, p)
        return byName
    }

    private static void collectLeaves(Object value, String position, Map<String, Leaf> byName, Map<String, String> positions, CasPath p) {
        if( value instanceof Leaf ) {
            final Leaf leaf = (Leaf) value
            if( leaf.name == null )
                return
            if( byName.containsKey(leaf.name) )
                throw new IOException("cas: two leaves of the item in ${p} are named '${leaf.name}' (positions ${positions.get(leaf.name)} and ${position}); " +
                    'address one by its content, cas://<cid>/<name>')
            byName.put(leaf.name, leaf)
            positions.put(leaf.name, position)
            return
        }
        if( value instanceof Map ) {
            for( Map.Entry e : ((Map) value).entrySet() )
                collectLeaves(e.value, position ? "${position}.${e.key}".toString() : String.valueOf(e.key), byName, positions, p)
            return
        }
        if( value instanceof List ) {
            final List list = (List) value
            for( int i = 0; i < list.size(); i++ )
                collectLeaves(list[i], position ? "${position}.${i}".toString() : String.valueOf(i), byName, positions, p)
        }
    }

    /** A raw block as a regular file node; absent when the store has no such block. */
    private CasNode fileNode(Cid cid) {
        if( !store().has(cid) )
            return CasNode.absent()
        return new CasNode(present: true, directory: false, size: store().size(cid),
                mtime: store().lastModifiedMillis(cid), content: cid)
    }

    /** A manifest cid as a directory node; absent when the store has no such block. */
    private CasNode manifestNode(Cid cid) {
        if( !store().has(cid) )
            return CasNode.absent()
        return new CasNode(present: true, directory: true, size: 0L,
                mtime: store().lastModifiedMillis(cid), content: cid)
    }

    /** Walks {@code segs} into the manifest {@code cid} by entry name. */
    private CasNode traverse(Cid cid, List<String> segs, CasPath p) {
        Cid here = cid
        for( int i = 0; i < segs.size(); i++ ) {
            final DirectoryManifest manifest = manifestOf(here)
            final ManifestEntry entry = manifest.entry(segs[i])
            if( entry == null )
                return CasNode.absent()
            final boolean last = i == segs.size() - 1
            switch( entry.mode ) {
                case ManifestEntry.DIRECTORY:
                    if( last )
                        return manifestNode(entry.address)
                    here = entry.address
                    break
                case ManifestEntry.REGULAR:
                    if( !last )
                        return CasNode.absent()   // cannot descend into a file
                    return new CasNode(present: true, directory: false, size: entry.size,
                            mtime: store().lastModifiedMillis(entry.address), content: entry.address)
                case ManifestEntry.SYMLINK:
                    if( !last )
                        return CasNode.absent()
                    return new CasNode(present: true, symlink: true, size: entry.size,
                            mtime: store().lastModifiedMillis(here), linkTarget: entry.target)
                default: // unresolvable
                    return CasNode.absent()
            }
        }
        return CasNode.absent()
    }

    private CasNode resolveCoordinate(CasPath p) {
        final CoordinateTree tree = coordsFor(p)
        final String rel = relOf(p)
        if( tree.isDirectory(rel) )
            return new CasNode(present: true, directory: true, size: 0L, mtime: tree.lastModifiedMillis(rel))
        final Optional<StoreRef> refOpt = tree.read(rel)   // throws if the pointer is corrupt
        if( !refOpt.isPresent() )
            return CasNode.absent()
        final StoreRef ref = refOpt.get()
        if( ref.isDirectory() )
            // A published directory is one indivisible pointer file in the coordinate
            // namespace: it has no per-child coordinates to delete, so Nextflow's
            // overwrite-on-republish must remove the whole pointer rather than recurse
            // into the immutable manifest blocks. Present it as a file-like unit.
            return new CasNode(present: true, directory: false, dirPointer: true,
                    size: store().size(ref.cid), mtime: store().lastModifiedMillis(ref.cid),
                    content: ref.cid)
        return fileNode(ref.cid)
    }

    // ------------------------------------------------------------- read helpers

    /** Reads a DAG-CBOR metadata block. Bounded (never file content), so a small array is fine. */
    private DirectoryManifest manifestOf(Cid cid) {
        byte[] bytes = null
        final InputStream input = store().open(cid)   // NoSuchBlockException names the cid
        try { bytes = input.readAllBytes() }
        finally { input.close() }
        return DirectoryManifest.fromCbor((Map) DagCbor.decode(bytes))
    }

    private List<BlockStore> storeMembers() {
        final BlockStore s = store()
        return s instanceof CompositeStore ? ((CompositeStore)s).members : Collections.<BlockStore>singletonList(s)
    }

    /** The on-disk path of a block held by a local member, or null. */
    private Path localBlockPath(Cid cid) {
        for( BlockStore member : storeMembers() ) {
            if( member instanceof LocalBlockStore && member.has(cid) )
                return ((LocalBlockStore)member).blockPath(cid)
        }
        return null
    }

    private CasPath storeUriPath(Cid cid) {
        return new CasPath(fileSystem, cid.toString(), Collections.<String>emptyList())
    }

    // --- file system lookup

    @Override FileSystem newFileSystem(URI uri, Map<String,?> env) { fileSystem }

    @Override FileSystem getFileSystem(URI uri) { fileSystem }

    @Override
    Path getPath(URI uri) {
        if( uri.scheme != SCHEME )
            throw new IllegalArgumentException("Not a ${SCHEME}:// URI: ${uri}")
        final authority = uri.authority
        if( !authority )
            throw new IllegalArgumentException("Missing store alias or content address in URI: ${uri}")
        return new CasPath(fileSystem, authority, CasPath.split(uri.path))
    }

    // ------------------------------------------------------------------ uploads

    @Override
    boolean canUpload(Path source, Path target) {
        return target instanceof CasPath && ((CasPath)target).isCoordinate()
    }

    @Override
    boolean canDownload(Path source, Path target) {
        return source instanceof CasPath
    }

    /**
     * Hashes a published file (or a whole directory) into the store and leaves
     * a Pointer File at the coordinate. Nextflow hands a directory as one call
     * and never recurses, so the directory is walked here; returning normally
     * with a child untransferred would be silent data loss.
     */
    @Override
    void upload(Path source, Path target, CopyOption... options) throws IOException {
        final CasPath dest = cas(target)
        if( dest.isStoreUri() )
            throw new AccessDeniedException("a Store URI is immutable: '${dest}'")
        final String key = Coordinates.key(target)
        final CoordinateTree tree = coordsFor(dest)
        final String rel = relOf(dest)
        final boolean replace = options.toList().contains(StandardCopyOption.REPLACE_EXISTING)
        // The existence check happens before a byte of the source is read: that
        // is what makes -resume cheap and never re-hashes an unchanged output.
        if( !replace && tree.exists(rel) )
            throw new FileAlreadyExistsException(key)

        final String name = dest.getFileName().toString()
        if( Files.isDirectory(source) ) {
            final DirectoryManifestBuilder.Result result = new DirectoryManifestBuilder(store(), session().addresser,
                { String uri -> FileHelper.asPath(uri) } as Closure<Path>).build(source)
            tree.write(rel, new StoreRef(result.cid, name))
            // A directory leaf's address is its manifest, which the head node always builds (silent decision 3);
            // the files inside it carry their own providers into RunCompletion.providers.
            session().recordPublish(key, new CasSession.Publish(new StoreRef(result.cid, name), store().size(result.cid), Providers.HEAD_NODE, result.providers))
            session().recordUploadAnomalies(key, result.anomalies)
            log.debug "cas: published directory ${source} as manifest ${result.cid} at ${key} (${result.anomalies})"
        }
        else {
            final Addressed a = session().addresser.address(source, Files.size(source))
            tree.write(rel, new StoreRef(a.cid, name))
            session().recordPublish(key, new CasSession.Publish(new StoreRef(a.cid, name), a.size, a.provider))
            log.debug "cas: published file ${source} as block ${a.cid} at ${key} (${a.provider})"
        }
    }

    // ---------------------------------------------------------------- downloads

    @Override
    void download(Path source, Path target, CopyOption... options) throws IOException {
        final CasPath src = cas(source)
        final CasNode node = resolve(src)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(src))
        if( target.parent != null )
            Files.createDirectories(target.parent)
        final boolean replace = options.toList().contains(StandardCopyOption.REPLACE_EXISTING)
        if( node.occurrence != null ) {
            materialiseOccurrence(node.occurrence, target)
            return
        }
        // Content staging follows a directory coordinate's pointer to its manifest;
        // only the delete/overwrite path treats it as a single pointer file.
        if( node.directory || node.dirPointer ) {
            materialiseDirectory(node.content, target, node.content, Collections.<String>emptyList(), Collections.<Cid>singleton(node.content))
        }
        else if( node.symlink ) {
            throw new AbortRunException("cas: cannot download a bare symlink '${src}' -> '${node.linkTarget}'")
        }
        else {
            if( replace )
                Files.deleteIfExists(target)
            materialiseFile(node.content, target, true)
        }
    }

    /**
     * A single raw block to {@code target}: a symlink to the read-only block
     * on the same local fs; a server-side S3-to-S3 copy when a member already
     * holds the block in S3, under CopyObject's own single-request limit
     * (silent decision 9); else stream-and-verify. A copy the SDK refuses
     * (over the limit despite the check, throttled, denied) falls back to
     * streaming, exactly as if no S3 member held the block; only a confirmed
     * digest mismatch aborts.
     */
    private void materialiseFile(Cid cid, Path target, boolean allowSymlink) throws IOException {
        final Path block = localBlockPath(cid)
        if( allowSymlink && block != null && target.fileSystem == FileSystems.default ) {
            // Blocks are stored read-only, so an in-place write by a task fails
            // rather than corrupting the store. No hash is needed for a symlink.
            Files.deleteIfExists(target)
            Files.createSymbolicLink(target, block)
            return
        }
        final String targetUri = s3UriOf(target)
        if( targetUri != null ) {
            final BlockStore holder = holderOf(cid)
            if( holder instanceof S3BlockStore ) {
                final S3BlockStore s3holder = (S3BlockStore) holder
                if( holder.size(cid) <= s3holder.singleRequestMax ) {
                    final int slash = targetUri.indexOf('/', 5)
                    final String targetBucket = slash < 0 ? targetUri.substring(5) : targetUri.substring(5, slash)
                    final String targetKey = slash < 0 ? '' : targetUri.substring(slash + 1)
                    try {
                        s3holder.copyOut(cid, targetBucket, targetKey)
                        return
                    }
                    catch( BlockMismatchException e ) {
                        // The AbortRunException is built first so a failing delete never hides the mismatch (Task 10's removeOrRefuse).
                        final AbortRunException abort = new AbortRunException("cas: ${e.message}", e)
                        try {
                            Files.deleteIfExists(target)
                        }
                        catch( IOException deleteFailure ) {
                            abort.addSuppressed(deleteFailure)
                        }
                        throw abort
                    }
                    catch( IOException e ) {
                        // S3BlockStore.copyOut wraps an SDK refusal in an IOException (never a BlockMismatchException):
                        // the head node reads the bytes instead, the same as when no S3 member holds the block.
                        log.warn("server-side copy of ${cid} to ${targetUri} failed (${e.message}); streaming it instead")
                    }
                }
            }
        }
        streamAndVerify(cid, target)
    }

    /** The s3:// URI text {@code target} names, or null; a seam over FilesEx.toUriString for materialiseFile's S3-to-S3 branch. */
    protected String s3UriOf(Path target) {
        final String uri = FilesEx.toUriString(target)
        return uri.startsWith('s3://') ? uri : null
    }

    /** The first member holding cid, or null; a seam over storeMembers() for materialiseFile's S3-to-S3 branch. */
    private BlockStore holderOf(Cid cid) {
        for( BlockStore member : storeMembers() )
            if( member.has(cid) )
                return member
        return null
    }

    /** Streams a block to {@code target}, hashing in flight; a mismatch aborts and removes the partial file. */
    private void streamAndVerify(Cid cid, Path target) throws IOException {
        final InputStream input = store().open(cid)   // NoSuchBlockException names the cid
        try {
            final Cid actual = (Cid) HashBufferPool.shared().withBuffer { byte[] buffer ->
                final MessageDigest digest = MessageDigest.getInstance('SHA-256')
                final OutputStream out = Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                try {
                    int n
                    while( (n = input.read(buffer, 0, buffer.length)) != -1 ) {
                        if( n > 0 ) {
                            digest.update(buffer, 0, n)
                            out.write(buffer, 0, n)
                        }
                    }
                }
                finally { out.close() }
                return Cid.of(Cid.RAW, digest.digest())
            }
            if( actual != cid ) {
                Files.deleteIfExists(target)
                throw new AbortRunException("cas: block ${cid} streamed as ${actual}; refusing to hand back corrupt provenance")
            }
        }
        finally { input.close() }
    }

    /**
     * Materialises a Directory Manifest under {@code dir}. A symlink stays a
     * link when {@code dir} is on the default filesystem; onto any other
     * filesystem (an object store has no links) it is staged as a copy of
     * whatever it names inside the tree (ticket 15 decision 7). {@code root}
     * and {@code at} are the manifest this walk started from and the path
     * segments from that root to {@code dir}, so an in-tree link can be
     * resolved without ever leaving the manifest. {@code ancestors} is every
     * directory cid on the way from {@code root} to {@code dir}, inclusive: a
     * manifest DAG cannot cycle on its own (a directory's address is the hash
     * of its own content), but a symlink's target text is resolved fresh from
     * {@code root} on every hop, so `nested/up -> ..` or `a/self -> .` can
     * name a directory already being materialised, which would otherwise
     * recurse without end.
     */
    private void materialiseDirectory(Cid manifestCid, Path dir, Cid root, List<String> at, Set<Cid> ancestors) throws IOException {
        Files.createDirectories(dir)
        final DirectoryManifest manifest = manifestOf(manifestCid)
        for( ManifestEntry entry : manifest.entries ) {
            final Path child = dir.resolve(entry.name)
            switch( entry.mode ) {
                case ManifestEntry.DIRECTORY:
                    final List<String> deeper = new ArrayList<String>(at)
                    deeper.add(entry.name)
                    materialiseDirectory(entry.address, child, root, deeper, descend(ancestors, entry.address))
                    break
                case ManifestEntry.REGULAR:
                    materialiseFile(entry.address, child, false)
                    break
                case ManifestEntry.SYMLINK:
                    if( child.fileSystem == FileSystems.default ) {
                        // Recreated verbatim: the target text is relative and stays inside the tree.
                        Files.deleteIfExists(child)
                        Files.createSymbolicLink(child, child.fileSystem.getPath(entry.target))
                    }
                    else {
                        // No links on an object store: stage what the link names, from inside the tree (ticket 15 decision 7).
                        final Resolved resolved = resolveInTree(root, at, entry.target, 0)
                        if( resolved == null )
                            throw new AbortRunException("cas: manifest ${manifestCid}: '${entry.name}' -> '${entry.target}' does not resolve inside the tree; cannot stage it as a copy")
                        if( resolved.directory ) {
                            if( ancestors.contains(resolved.address) )
                                throw new AbortRunException("cas: manifest ${manifestCid}: '${entry.name}' -> '${entry.target}' names a directory already being materialised; cannot stage an ancestor link as a copy")
                            materialiseDirectory(resolved.address, child, root, segmentsOf(at, entry.target), descend(ancestors, resolved.address))
                        }
                        else materialiseFile(resolved.address, child, false)
                    }
                    break
                default: // unresolvable
                    throw new AbortRunException("cas: manifest ${manifestCid} has an unresolvable entry '${entry.name}' (was '${entry.target}'); cannot materialise")
            }
        }
    }

    /** ancestors plus one more directory cid, for the next level down's cycle check. */
    private static Set<Cid> descend(Set<Cid> ancestors, Cid next) {
        final Set<Cid> deeper = new LinkedHashSet<Cid>(ancestors)
        deeper.add(next)
        return deeper
    }

    /** What a symlink's target text names inside the tree: a content or a manifest address, and which. */
    @CompileStatic
    private static class Resolved {
        final Cid address
        final boolean directory
        Resolved(Cid address, boolean directory) { this.address = address; this.directory = directory }
    }

    /**
     * What a symlink's target text names, walking the manifest tree from
     * {@code root} entry by entry (ticket 15 decision 7: an object store has
     * no links, so a link staged there is materialised as a copy of what it
     * names). {@code at} is the path segments from {@code root} to the
     * directory the symlink lives in; {@code target} is its (always
     * relative, always in-tree -- DESIGN.md §6) target text. A symlink met on
     * the way is itself followed, one more hop; null past
     * {@link DirectoryManifestBuilder#MAX_DEPTH} hops, or when the path
     * climbs above {@code root}, is missing, or cannot be descended into.
     * Segments that fully cancel out (`nested/up -> ..` from one level down)
     * resolve to {@code root} itself, so a link straight back to an ancestor
     * is a real resolution -- the caller's ancestor check is what refuses it,
     * not a false "does not resolve".
     */
    private Resolved resolveInTree(Cid root, List<String> at, String target, int hops) {
        if( hops > DirectoryManifestBuilder.MAX_DEPTH )
            return null
        final List<String> segments = segmentsOf(at, target)
        if( segments == null )
            return null
        if( segments.isEmpty() )
            return new Resolved(root, true)
        Cid here = root
        for( int i = 0; i < segments.size(); i++ ) {
            final DirectoryManifest manifest = manifestOf(here)
            final ManifestEntry entry = manifest.entry(segments.get(i))
            if( entry == null )
                return null
            final boolean last = i == segments.size() - 1
            if( entry.mode == ManifestEntry.SYMLINK ) {
                final Resolved resolved = resolveInTree(root, segments.subList(0, i), entry.target, hops + 1)
                if( resolved == null )
                    return null
                if( last )
                    return resolved
                if( !resolved.directory )
                    return null   // cannot descend into a file
                here = resolved.address
                continue
            }
            if( entry.isDirectory() ) {
                if( last )
                    return new Resolved(entry.address, true)
                here = entry.address
                continue
            }
            if( !last )
                return null   // cannot descend into a file, or past an unresolvable entry
            return new Resolved(entry.address, false)
        }
        return null   // unreachable: the loop always returns on its last iteration
    }

    /**
     * {@code at + target} normalised into path segments from {@code root}:
     * {@code ..} pops the last segment, refusing to climb above the root
     * (null). A non-link caller never sees this text; only a manifest
     * SYMLINK's target text is normalised this way.
     */
    private static List<String> segmentsOf(List<String> at, String target) {
        final List<String> segments = new ArrayList<String>(at)
        for( String part : target.split('/') ) {
            if( part.isEmpty() || part == '.' )
                continue
            if( part == '..' ) {
                if( segments.isEmpty() )
                    return null
                segments.remove(segments.size() - 1)
            }
            else {
                segments.add(part)
            }
        }
        return segments
    }

    /** Stages each addressed leaf of an occurrence under {@code dir} by its name. */
    private void materialiseOccurrence(Map<String, Leaf> leaves, Path dir) throws IOException {
        Files.createDirectories(dir)
        for( Map.Entry<String, Leaf> e : leaves.entrySet() ) {
            final Leaf leaf = e.value
            if( !leaf.addressed )
                continue
            if( leaf.address.isRaw() )
                materialiseFile(leaf.address, dir.resolve(e.key), true)
            else
                materialiseDirectory(leaf.address, dir.resolve(e.key), leaf.address, Collections.<String>emptyList(), Collections.<Cid>singleton(leaf.address))
        }
    }

    private String namedAbsence(CasPath p) {
        return p.isStoreUri() ? "no block for '${p.cid()}' (${p})".toString() : p.toString()
    }

    // ------------------------------------------------------------------ read side

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        final CasPath p = cas(path)
        if( isWrite(options) ) {
            if( p.isStoreUri() )
                throw new AccessDeniedException("a Store URI is read-only: '${p}'")
            throw new UnsupportedOperationException("cas: a writable byte channel is not supported: '${p}'")
        }
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        if( node.directory || node.dirPointer )
            throw new IOException("is a directory: '${p}'")
        final Path block = localBlockPath(node.content)
        if( block != null )
            return Files.newByteChannel(block, EnumSet.of(StandardOpenOption.READ))
        return new InputStreamByteChannel(store().open(node.content), node.size)
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        final CasPath p = cas(path)
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        if( node.directory || node.dirPointer )
            throw new IOException("is a directory: '${p}'")
        return store().open(node.content)   // streams the block, never buffers it
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        final CasPath p = cas(path)
        if( p.isStoreUri() )
            throw new AccessDeniedException("a Store URI is immutable: '${p}'")
        // Hash-on-close: Nextflow's transfer-aware path never lands here, but an
        // incidental write must still hash into the store and leave a pointer.
        final CoordinateTree tree = coordsFor(p)
        final String rel = relOf(p)
        final String key = Coordinates.key(path)
        final String name = p.getFileName().toString()
        final Path temp = Files.createTempFile('cas-out-', '.tmp')
        final OutputStream out = Files.newOutputStream(temp, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        final CasFileSystemProvider self = this
        return new FilterOutputStream(out) {
            private boolean closed = false
            @Override void write(byte[] b, int off, int len) throws IOException { out.write(b, off, len) }
            @Override
            void close() throws IOException {
                if( closed ) return
                closed = true
                super.close()
                try {
                    Cid cid = null
                    final InputStream input = Files.newInputStream(temp)
                    try { cid = self.store().putStreaming(input) }
                    finally { input.close() }
                    tree.write(rel, new StoreRef(cid, name))
                    self.session().recordPublish(key, new CasSession.Publish(new StoreRef(cid, name), Files.size(temp), Providers.HEAD_NODE))
                }
                finally { Files.deleteIfExists(temp) }
            }
        }
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final CasPath p = cas(dir)
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        if( !node.directory )
            throw new NotDirectoryException(p.toString())

        final List<Path> children = new ArrayList<Path>()
        if( node.occurrence != null ) {
            for( Map.Entry<String, Leaf> e : node.occurrence.entrySet() )
                if( e.value.addressed )
                    children.add(p.resolve(e.key))
        }
        else if( node.content != null ) {
            // A manifest, reached as a Store URI or through a coordinate pointer.
            final CasPath base = p.isStoreUri() ? p : storeUriPath(node.content)
            for( ManifestEntry entry : manifestOf(node.content).entries )
                children.add(base.resolve(entry.name))
        }
        else {
            // A real coordinate directory on the way to a pointer.
            final CoordinateTree tree = coordsFor(p)
            for( String childName : tree.children(relOf(p)) )
                children.add(p.resolve(childName))
        }

        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                final List<Path> out = new ArrayList<Path>()
                for( Path child : children )
                    if( filter == null || filter.accept(child) )
                        out.add(child)
                return out.iterator()
            }
            @Override void close() throws IOException { }
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        final CasPath p = cas(dir)
        if( p.isStoreUri() )
            throw new AccessDeniedException("a Store URI has no directories to create: '${p}'")
        coordsFor(p).createDirectories(relOf(p))
    }

    @Override
    void delete(Path path) throws IOException {
        final CasPath p = cas(path)
        if( p.isStoreUri() )
            throw new AccessDeniedException("a Store URI is immutable; a block is never deleted through the scheme: '${p}'")
        if( !coordsFor(p).delete(relOf(p)) )
            throw new NoSuchFileException(p.toString())
    }

    @Override
    boolean deleteIfExists(Path path) throws IOException {
        final CasPath p = cas(path)
        if( p.isStoreUri() )
            throw new AccessDeniedException("a Store URI is immutable; a block is never deleted through the scheme: '${p}'")
        return coordsFor(p).delete(relOf(p))
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("cas: copy is not supported; publish goes through upload()/download()")
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("cas: move is not supported")
    }

    @Override
    boolean isSameFile(Path a, Path b) throws IOException {
        if( !(a instanceof CasPath) || !(b instanceof CasPath) )
            return false
        final CasPath pa = (CasPath)a
        final CasPath pb = (CasPath)b
        if( pa == pb )
            return true
        if( pa.isCoordinate() && pb.isCoordinate() )
            return Coordinates.key(pa) == Coordinates.key(pb)
        // Otherwise compare what they resolve to: same block or same manifest.
        final CasNode na = resolve(pa)
        final CasNode nb = resolve(pb)
        return na.present && nb.present && na.content != null && na.content == nb.content
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        final name = path.fileName?.toString()
        return name != null && name.startsWith('.')
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("A cas:// path has no file store: ${path}")
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        final CasPath p = cas(path)
        final List<AccessMode> asked = modes.toList()
        if( AccessMode.WRITE in asked && p.isStoreUri() )
            throw new AccessDeniedException("a Store URI is read-only: '${p}'")
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        // READ and EXECUTE are not refused: a block is world-readable, and a
        // coordinate that resolves to a raw block cannot know its exec bit, so
        // denying EXECUTE here would lie in the refusing direction.
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        if( type != null && type.isAssignableFrom(BasicFileAttributeView) ) {
            final CasFileSystemProvider self = this
            return (V) new BasicFileAttributeView() {
                @Override String name() { 'basic' }
                @Override BasicFileAttributes readAttributes() throws IOException {
                    return self.readAttributes(path, BasicFileAttributes, options)
                }
                @Override void setTimes(FileTime m, FileTime a, FileTime c) throws IOException {
                    throw new UnsupportedOperationException("cas: attributes are facts about content, not settable")
                }
            }
        }
        return null
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if( type != null && !type.isAssignableFrom(CasAttributes) )
            throw new UnsupportedOperationException("cas: unsupported attributes type ${type.name} for '${path}'")
        final CasPath p = cas(path)
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        return (A) attributesOf(p, node)
    }

    @Override
    Map<String,Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        final CasPath p = cas(path)
        final CasNode node = resolve(p)
        if( !node.present )
            throw new NoSuchFileException(namedAbsence(p))
        final CasAttributes a = attributesOf(p, node)

        String view = 'basic'
        String names = attributes
        final int colon = attributes.indexOf(':')
        if( colon >= 0 ) {
            view = attributes.substring(0, colon)
            names = attributes.substring(colon + 1)
        }
        final Map<String,Object> all = new LinkedHashMap<String,Object>()
        all.put('size', a.size())
        all.put('creationTime', a.creationTime())
        all.put('lastAccessTime', a.lastAccessTime())
        all.put('lastModifiedTime', a.lastModifiedTime())
        all.put('isRegularFile', a.isRegularFile())
        all.put('isDirectory', a.isDirectory())
        all.put('isSymbolicLink', a.isSymbolicLink())
        all.put('isOther', a.isOther())
        all.put('fileKey', a.fileKey())
        if( view == 'posix' || view == 'unix' ) {
            all.put('permissions', a.isExecutable()
                    ? PosixFilePermissions.fromString('r-xr-xr-x')
                    : PosixFilePermissions.fromString('r--r--r--'))
        }
        if( names == null || names == '*' )
            return all
        final Map<String,Object> out = new LinkedHashMap<String,Object>()
        for( String n : names.split(',') ) {
            final String k = n.trim()
            if( all.containsKey(k) )
                out.put(k, all.get(k))
        }
        return out
    }

    private CasAttributes attributesOf(CasPath p, CasNode node) {
        final Object fileKey = node.content != null ? node.content.toString() : (p.isCoordinate() ? Coordinates.key(p) : null)
        return new CasAttributes(node.size, !node.directory && !node.symlink, node.directory,
                node.symlink, node.executable, node.mtime, fileKey)
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("cas: the attributes of a content-addressed object are facts about its content, not settable: '${path}' ${attribute}")
    }

    @Override
    void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("${SCHEME}:// does not support symbolic links")
    }

    @Override
    void createLink(Path link, Path existing) throws IOException {
        throw new UnsupportedOperationException("${SCHEME}:// does not support hard links")
    }

    @Override
    Path readSymbolicLink(Path link) throws IOException {
        throw new UnsupportedOperationException("${SCHEME}:// does not support symbolic links")
    }

    private static boolean isWrite(Set<? extends OpenOption> options) {
        return options.any { OpenOption o ->
            o == StandardOpenOption.WRITE || o == StandardOpenOption.APPEND ||
            o == StandardOpenOption.CREATE || o == StandardOpenOption.CREATE_NEW ||
            o == StandardOpenOption.DELETE_ON_CLOSE || o == StandardOpenOption.TRUNCATE_EXISTING
        }
    }
}
