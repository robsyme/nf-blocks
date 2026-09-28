package robsyme.cas.core

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.stream.Collectors
import java.util.stream.Stream

import groovy.transform.CompileStatic

/**
 * The Pointer File tree under {@code coords/} (DESIGN.md §5, §7).
 *
 * Intermediate segments are real directories; a leaf is a text file holding
 * one line, the Store URI of what was published there. Pointer files are not
 * blocks and not roots: the next run to the same coordinate overwrites them,
 * and deleting one never touches content.
 *
 * A published *directory* is itself a leaf, a pointer at a DirectoryManifest,
 * so {@link #children} answers only for the real directories on the way there;
 * the manifest names the children of a published directory.
 */
@CompileStatic
class LocalCoordinateTree implements CoordinateTree {

    private final Path root

    LocalCoordinateTree(Path coordsRoot) {
        this.root = coordsRoot
    }

    Path getRoot() { root }

    /** The pointer file for a coordinate, whether or not it exists. */
    Path pointerPath(String relPath) {
        Path p = root
        for( String segment : segments(relPath) )
            p = p.resolve(segment)
        return p
    }

    @Override
    Optional<StoreRef> read(String relPath) {
        final Path pointer = pointerPath(relPath)
        String text
        try {
            text = Files.readString(pointer, StandardCharsets.UTF_8)
        }
        catch( NoSuchFileException e ) {
            // Absent is the only thing that reads as empty. A permission error
            // or any other IOException is a real failure and propagates, so it
            // is never mistaken for "no coordinate here".
            return Optional.empty()
        }
        try {
            return Optional.of(StoreRef.parse(text.trim()))
        }
        catch( IllegalArgumentException e ) {
            throw new IOException("pointer file for '${key(relPath)}' does not hold a store uri: ${e.message}", e)
        }
    }

    @Override
    void write(String relPath, StoreRef ref) {
        if( ref == null )
            throw new IllegalArgumentException("no store reference for coordinate '${key(relPath)}'")
        final Path pointer = pointerPath(relPath)
        if( pointer == root )
            throw new IllegalArgumentException('the root of the coordinate tree is not a coordinate')
        Files.createDirectories(pointer.parent)
        final Path temp = Files.createTempFile(pointer.parent, '.tmp-', '')
        try {
            Files.writeString(temp, ref.toString() + '\n', StandardCharsets.UTF_8)
            Files.move(temp, pointer, StandardCopyOption.REPLACE_EXISTING)
        }
        finally {
            Files.deleteIfExists(temp)
        }
    }

    @Override
    boolean exists(String relPath) {
        return Files.exists(pointerPath(relPath))
    }

    @Override
    boolean isDirectory(String relPath) { Files.isDirectory(pointerPath(relPath)) }

    @Override
    void createDirectories(String relPath) { Files.createDirectories(pointerPath(relPath)) }

    @Override
    long lastModifiedMillis(String relPath) {
        try { return Files.getLastModifiedTime(pointerPath(relPath)).toMillis() }
        catch( IOException e ) { return 0L }
    }

    /**
     * True when the coordinate is a directory: either a real directory on the
     * way to a pointer, or a pointer at a DirectoryManifest.
     */
    @Override
    boolean isDirectoryCoordinate(String relPath) {
        final Path pointer = pointerPath(relPath)
        if( Files.isDirectory(pointer) )
            return true
        if( !Files.isRegularFile(pointer) )
            return false
        return read(relPath).map { StoreRef ref -> ref.isDirectory() }.orElse(false)
    }

    /** The names directly under a real coordinate directory, sorted; empty for anything else. */
    @Override
    List<String> children(String relPath) {
        final Path dir = pointerPath(relPath)
        if( !Files.isDirectory(dir) )
            return Collections.<String> emptyList()
        Stream<Path> stream = null
        try {
            stream = Files.list(dir)
            return stream
                .map { Path p -> p.fileName.toString() }
                .filter { String name -> !name.startsWith('.tmp-') }
                .sorted()
                .collect(Collectors.toList())
        }
        finally {
            stream?.close()
        }
    }

    /** Removes the pointer file only. Never a directory, never a block. */
    @Override
    boolean delete(String relPath) {
        final Path pointer = pointerPath(relPath)
        if( Files.isDirectory(pointer) )
            throw new IOException("'${key(relPath)}' is a coordinate directory, not a pointer file")
        return Files.deleteIfExists(pointer)
    }

    private String key(String relPath) {
        return segments(relPath).join('/')
    }

    /**
     * Splits and normalises a coordinate path the way {@link Coordinates#key}
     * does, except that a {@code ..} escaping the tree is a caller error here
     * rather than something to silently clamp.
     */
    private static List<String> segments(String relPath) {
        final List<String> out = new ArrayList<String>()
        for( String segment : (relPath ?: '').split('/') ) {
            if( !segment || segment == '.' )
                continue
            if( segment == '..' ) {
                if( out.isEmpty() )
                    throw new IllegalArgumentException("coordinate path escapes the coordinate tree: '$relPath'")
                out.remove(out.size() - 1)
                continue
            }
            out.add(segment)
        }
        return out
    }
}
