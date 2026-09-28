package robsyme.cas.core

import java.nio.file.DirectoryStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NotDirectoryException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Walks a directory into a DirectoryManifest (DESIGN.md §6, §8), on local
 * disk or in an object store (see {@link ObjectWalk}). Nextflow hands a
 * published directory to {@code upload()} once and does not recurse, so this
 * is the only thing that ever sees inside a published tree.
 *
 * Every regular file is addressed through the {@link FileAddresser}, which
 * streams it into the store or finds it already there; nothing is ever read
 * into memory. Manifests are built bottom-up, so a subdirectory's address
 * exists before its parent's entry mentions it, and a manifest is never
 * written referring to a block that is not there.
 *
 * Symlinks follow DESIGN.md §6: a relative target that resolves inside the
 * tree stays a link, anything else is followed and stored as what it points
 * at, and a link that does not resolve becomes an {@code unresolvable} entry.
 * A stored target is only ever a relative in-tree path; an absolute or
 * escaping one is redacted, so a portable block never carries a launch path.
 * Dangling links are counted, never dropped and never fatal: real tools emit
 * them.
 */
@Slf4j
@CompileStatic
class DirectoryManifestBuilder {

    /** DESIGN.md §6. Deeper than this is a mistake or an attack, not a tree. */
    static final int MAX_DEPTH = 64

    /** The address of a walked tree and what could not be addressed in it. */
    static class Result {
        final Cid cid
        final Anomalies anomalies

        Result(Cid cid, Anomalies anomalies) {
            this.cid = cid
            this.anomalies = anomalies
        }

        @Override
        String toString() { "Result[$cid, $anomalies]" }
    }

    private final BlockStore store
    private final FileAddresser addresser
    private final Closure<Path> objectPath

    DirectoryManifestBuilder(BlockStore store) {
        this(store, new HeadNodeAddresser(store), null)
    }

    DirectoryManifestBuilder(BlockStore store, FileAddresser addresser, Closure<Path> objectPath) {
        this.store = store
        this.addresser = addresser
        this.objectPath = objectPath
    }

    /**
     * Stores every file under {@code directory} and returns the address of the
     * root manifest.
     */
    Result build(Path directory) {
        if( !Files.isDirectory(directory) )
            throw new NotDirectoryException(directory.toString())
        final int[] unresolvable = new int[1]
        final Cid cid
        if( directory.fileSystem == FileSystems.default ) {
            final Path root = directory.toRealPath()
            cid = walk(root, root, 1, new LinkedHashSet<Path>([root]), unresolvable)
        }
        else {
            final Path root = realOf(directory)
            cid = new ObjectWalk(root, unresolvable).walk(root, 1, new LinkedHashSet<String>([keyOf(root)]))
        }
        return new Result(cid, Anomalies.unresolvable(unresolvable[0]))
    }

    /**
     * Every file inside the tree is addressed here, in both walks. The run's
     * addresser counts each provider for its summary line; the addresses
     * themselves are not collected (final review I5: RunCompletion.providers
     * lists Leaf addresses only).
     */
    private Cid addressOf(Path file, long size) {
        return addresser.address(file, size).cid
    }

    /** toRealPath where the provider has it; an object store has no links to resolve (ticket 05). */
    static Path realOf(Path p) {
        try {
            return p.toRealPath()
        }
        catch( UnsupportedOperationException e ) {
            return p.toAbsolutePath().normalize()
        }
    }

    /**
     * An object's key as the walk compares it: the path's absolute, normalized
     * string form. Not realOf: an object store has no links to resolve, and a
     * provider that does support toRealPath (the JDK's zip filesystem) throws
     * for a key with no object, which a dangling or escaping target names.
     */
    private static String keyOf(Path p) { p.toAbsolutePath().normalize().toString() }

    /**
     * @param dir       the directory being walked, already a real path
     * @param root      the real path of the tree being published
     * @param depth     1 for the root
     * @param ancestors the real paths on the way here, for cycle detection
     */
    private Cid walk(Path dir, Path root, int depth, LinkedHashSet<Path> ancestors, int[] unresolvable) {
        if( depth > MAX_DEPTH )
            throw new IOException("directory tree is deeper than $MAX_DEPTH levels at $dir")
        final List<ManifestEntry> entries = new ArrayList<ManifestEntry>()
        DirectoryStream<Path> children = null
        try {
            children = Files.newDirectoryStream(dir)
            for( Path child : children )
                entries.add(entryFor(child, root, depth, ancestors, unresolvable))
        }
        finally {
            children?.close()
        }
        return store.putDagCbor(new DirectoryManifest(entries).toCbor())
    }

    private ManifestEntry entryFor(Path child, Path root, int depth, LinkedHashSet<Path> ancestors, int[] unresolvable) {
        final String name = child.fileName.toString()
        if( Files.isSymbolicLink(child) ) {
            final String target = Files.readSymbolicLink(child).toString()
            if( !Files.exists(child) ) {
                // Dangling. The fact of the broken link is recorded, but the
                // target is only ever a relative in-tree path: an absolute or
                // escaping one would leak the launch path into a portable,
                // asserted_by-free block (DESIGN.md §6).
                unresolvable[0]++
                return ManifestEntry.unresolvable(name, portableTarget(child, root, target))
            }
            if( !Files.readSymbolicLink(child).isAbsolute() && resolvesInside(child, root) )
                return ManifestEntry.symlink(name, target)
            // Absolute, or escaping the tree: follow it and store what it holds.
            return contentEntry(name, child, root, depth, ancestors, unresolvable, target)
        }
        return contentEntry(name, child, root, depth, ancestors, unresolvable, null)
    }

    /** The entry for a path read through, whether it was reached by a link or not. */
    private ManifestEntry contentEntry(String name, Path path, Path root, int depth,
                                       LinkedHashSet<Path> ancestors, int[] unresolvable, String linkTarget) {
        final BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes)
        if( attrs.isDirectory() ) {
            final Path real = realOf(path)
            if( ancestors.contains(real) ) {
                // A link back into the tree we are already inside. Only an
                // absolute or escaping link reaches here, so its target is
                // redacted rather than stored.
                unresolvable[0]++
                return ManifestEntry.unresolvable(name, portableTarget(path, root, linkTarget))
            }
            final LinkedHashSet<Path> deeper = new LinkedHashSet<Path>(ancestors)
            deeper.add(real)
            return ManifestEntry.directory(name, walk(real, root, depth + 1, deeper, unresolvable))
        }
        if( !attrs.isRegularFile() ) {
            // A fifo, a socket, a device: real, and not something we can address.
            unresolvable[0]++
            return ManifestEntry.unresolvable(name, portableTarget(path, root, linkTarget))
        }
        final Cid address = addressOf(path, attrs.size())
        return ManifestEntry.regular(name, address, attrs.size())
    }

    /**
     * The target text safe to write into a portable block: a relative in-tree
     * target unchanged, anything absolute or escaping the tree replaced by the
     * same marker {@code scrub} uses. {@code null} (a non-link) stays null.
     */
    private static String portableTarget(Path link, Path root, String rawTarget) {
        if( rawTarget == null )
            return null
        final Path targetPath = Paths.get(rawTarget)
        if( targetPath.isAbsolute() )
            return Records.REDACTED_LOCATION
        final Path parent = link.parent ?: root
        final Path resolved = parent.resolve(targetPath).normalize()
        return resolved.startsWith(root) ? rawTarget : Records.REDACTED_LOCATION
    }

    /**
     * True when a relative link stays inside the published tree, which is the
     * only case where the link text means anything to a receiver.
     */
    private static boolean resolvesInside(Path link, Path root) {
        try {
            return realOf(link).startsWith(root)
        }
        catch( IOException e ) {
            return false
        }
    }

    /**
     * The walk of a directory in an object store (ticket 15). Keys are compared
     * as the paths' string forms, which are absolute on every object store
     * provider nf-amazon and the JDK's zip provider have. A directory holding
     * a parseable `.fusion.symlinks` has its listed names decoded as links; the
     * sidecar never enters the manifest. Every file is regular: an object has
     * no execute bit (nf-amazon's checkAccess(EXECUTE) always throws).
     */
    private class ObjectWalk {
        final Path root
        final String rootKey
        final int[] unresolvable
        final Map<String, Set<String>> linksByDir = new HashMap<String, Set<String>>()

        ObjectWalk(Path root, int[] unresolvable) {
            this.root = root
            this.rootKey = keyOf(root)
            this.unresolvable = unresolvable
        }

        Cid walk(Path dir, int depth, LinkedHashSet<String> ancestors) {
            if( depth > MAX_DEPTH )
                throw new IOException("directory tree is deeper than $MAX_DEPTH levels at $dir")
            final Map<String, Path> children = new TreeMap<String, Path>()
            Files.newDirectoryStream(dir).withCloseable { stream ->
                for( Path child : stream ) children.put(child.fileName.toString().replaceAll('/+$', ''), child)
            }
            final Set<String> links = linksIn(dir, children)
            final List<ManifestEntry> entries = new ArrayList<ManifestEntry>()
            for( String name : links )
                if( !children.containsKey(name) )
                    entries.add(unresolvable(name, null))
            for( Map.Entry<String, Path> e : children.entrySet() ) {
                if( links.contains(e.key) )
                    entries.add(linkEntry(dir, e.key, e.value, depth, ancestors))
                else
                    entries.add(content(e.key, e.value, depth, ancestors))
            }
            return store.putDagCbor(new DirectoryManifest(entries).toCbor())
        }

        /** The decoded link names of a directory being walked; a parsed sidecar leaves `children`. */
        private Set<String> linksIn(Path dir, Map<String, Path> children) {
            final Path sidecar = children.get(FusionLinks.SIDECAR)
            if( sidecar == null || Files.isDirectory(sidecar) )
                return remember(dir, Collections.<String> emptySet())
            final FusionLinks.Parsed parsed = FusionLinks.parse(readAtMost(sidecar, FusionLinks.MAX_SIDECAR_BYTES + 1))
            if( !parsed.ok ) {
                unresolvable[0]++
                log.warn("${sidecar}: ${parsed.problem}; the directory is recorded as its objects, links as files")
                return remember(dir, Collections.<String> emptySet())
            }
            children.remove(FusionLinks.SIDECAR)
            return remember(dir, parsed.names)
        }

        private Set<String> remember(Path dir, Set<String> names) {
            linksByDir.put(keyOf(dir), names)
            return names
        }

        /** Whether p is a decoded link, reading its directory's sidecar once. */
        private boolean isLink(Path p) {
            final Path parent = p.parent
            if( parent == null ) return false
            Set<String> names = linksByDir.get(keyOf(parent))
            if( names == null ) {
                final Path sidecar = parent.resolve(FusionLinks.SIDECAR)
                final FusionLinks.Parsed parsed = Files.isRegularFile(sidecar)
                    ? FusionLinks.parse(readAtMost(sidecar, FusionLinks.MAX_SIDECAR_BYTES + 1)) : null
                names = remember(parent, parsed?.ok ? parsed.names : Collections.<String> emptySet())
            }
            return names.contains(p.fileName.toString())
        }

        private ManifestEntry linkEntry(Path dir, String name, Path child, int depth, LinkedHashSet<String> ancestors) {
            final String target = Files.isDirectory(child) ? null : FusionLinks.target(readAtMost(child, FusionLinks.MAX_TARGET_BYTES + 1))
            if( target == null )
                return unresolvable(name, Files.isDirectory(child) ? null : Records.REDACTED_LOCATION)
            final boolean absolute = target.startsWith('/')
            final boolean textInTree = !absolute && within(dir.resolve(target).normalize())
            final Path found = chase(dir, target, new HashSet<String>([keyOf(child)]), 0)
            if( found == null )
                return unresolvable(name, textInTree ? target : Records.REDACTED_LOCATION)
            if( !absolute && within(found) )
                return ManifestEntry.symlink(name, target)
            return followed(name, found, depth, ancestors)
        }

        /** The non-link a target leads to, or null when it is missing, cyclic or not resolvable by key. */
        private Path chase(Path fromDir, String target, Set<String> seen, int hops) {
            if( hops >= MAX_DEPTH ) return null
            final Path p = target.startsWith('/') ? fusionPath(target) : fromDir.resolve(target).normalize()
            if( p == null ) return null
            if( isLink(p) ) {
                if( !seen.add(keyOf(p)) ) return null
                final String next = FusionLinks.target(readAtMost(p, FusionLinks.MAX_TARGET_BYTES + 1))
                return next == null ? null : chase(p.parent, next, seen, hops + 1)
            }
            return Files.exists(p) ? p : null
        }

        /** `/fusion/s3/<bucket>/<key>` is `s3://<bucket>/<key>`; any other absolute target has no key. */
        private Path fusionPath(String target) {
            final java.util.regex.Matcher m = target =~ /^\/fusion\/s3\/([^\/]+)\/(.+)$/
            return m.matches() && objectPath != null ? objectPath.call("s3://${m.group(1)}/${m.group(2)}".toString()) : null
        }

        private boolean within(Path p) {
            final String key = keyOf(p)
            return key == rootKey || key.startsWith(rootKey + '/')
        }

        private ManifestEntry followed(String name, Path p, int depth, LinkedHashSet<String> ancestors) {
            if( Files.isDirectory(p) && ancestors.contains(keyOf(p)) )
                return unresolvable(name, Records.REDACTED_LOCATION)
            return content(name, p, depth, ancestors)
        }

        private ManifestEntry content(String name, Path p, int depth, LinkedHashSet<String> ancestors) {
            final BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes)
            if( attrs.isDirectory() ) {
                final LinkedHashSet<String> deeper = new LinkedHashSet<String>(ancestors)
                deeper.add(keyOf(p))
                return ManifestEntry.directory(name, walk(p, depth + 1, deeper))
            }
            return ManifestEntry.regular(name, addressOf(p, attrs.size()), attrs.size())
        }

        private ManifestEntry unresolvable(String name, String target) {
            unresolvable[0]++
            return ManifestEntry.unresolvable(name, target)
        }
    }

    /** At most max bytes of a small object: a sidecar or a link body, never file content. */
    private static byte[] readAtMost(Path p, int max) {
        Files.newInputStream(p).withCloseable { InputStream in -> in.readNBytes(max) }
    }
}
