package robsyme.cas.core

import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.NotDirectoryException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFileAttributeView
import java.nio.file.attribute.PosixFilePermission

import groovy.transform.CompileStatic

/**
 * Walks a directory on the default filesystem into a DirectoryManifest
 * (DESIGN.md §6, §8). Nextflow hands a published directory to
 * {@code upload()} once and does not recurse, so this is the only thing that
 * ever sees inside a published tree.
 *
 * Every regular file is streamed into the store through
 * {@link BlockStore#putStreaming}; nothing is ever read into memory. Manifests
 * are built bottom-up, so a subdirectory's address exists before its parent's
 * entry mentions it, and a manifest is never written referring to a block that
 * is not there.
 *
 * Symlinks follow DESIGN.md §6: a relative target that resolves inside the
 * tree stays a link, anything else is followed and stored as what it points
 * at, and a link that does not resolve becomes an {@code unresolvable} entry.
 * A stored target is only ever a relative in-tree path; an absolute or
 * escaping one is redacted, so a portable block never carries a launch path.
 * Dangling links are counted, never dropped and never fatal: real tools emit
 * them.
 */
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

    DirectoryManifestBuilder(BlockStore store) {
        this.store = store
    }

    /**
     * Stores every file under {@code directory} and returns the address of the
     * root manifest.
     */
    Result build(Path directory) {
        if( !Files.isDirectory(directory) )
            throw new NotDirectoryException(directory.toString())
        final Path root = directory.toRealPath()
        final int[] unresolvable = new int[1]
        final Cid cid = walk(root, root, 1, new LinkedHashSet<Path>([root]), unresolvable)
        return new Result(cid, Anomalies.unresolvable(unresolvable[0]))
    }

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
            final Path real = path.toRealPath()
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
        final Cid address = putFile(path)
        return isExecutable(path)
            ? ManifestEntry.executable(name, address, attrs.size())
            : ManifestEntry.regular(name, address, attrs.size())
    }

    private Cid putFile(Path file) {
        InputStream input = null
        try {
            input = Files.newInputStream(file)
            return store.putStreaming(input)
        }
        finally {
            input?.close()
        }
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
            return link.toRealPath().startsWith(root)
        }
        catch( IOException e ) {
            return false
        }
    }

    /**
     * Git's rule: the owner execute bit, and nothing else about permissions.
     * Read through a link, because by here we have decided to follow it.
     */
    private static boolean isExecutable(Path file) {
        final PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView)
        if( view == null )
            return Files.isExecutable(file)
        return view.readAttributes().permissions().contains(PosixFilePermission.OWNER_EXECUTE)
    }
}
