package robsyme.cas.explore

import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic

/** A member in a local directory. */
@CompileStatic
class LocalMemberFiles implements MemberFiles {

    private final Path root

    LocalMemberFiles(Path root) {
        this.root = root.toAbsolutePath().normalize()
    }

    /** Refuses anything that is not a plain relative path inside the root. */
    private Path resolve(String rel) {
        if( !rel || rel.startsWith('/') || rel.split('/').any { String s -> s in ['', '.', '..'] } )
            throw new IllegalArgumentException("not a member path: '${rel}'")
        final Path path = root.resolve(rel).normalize()
        if( !path.startsWith(root) )
            throw new IllegalArgumentException("not a member path: '${rel}'")
        return path
    }

    /**
     * Opened between two reads of the path's attributes: when both agree (size,
     * modification time and file key, the inode), the channel is that file.
     * The snapshot writers replace the file by an atomic move, which changes
     * the file key even when the size and time do not.
     */
    @Override
    MemberFiles.Opened open(String rel) {
        final Path path = resolve(rel)
        for( int attempt = 0; attempt < 5; attempt++ ) {
            final BasicFileAttributes before = attributes(path)
            if( before == null || !before.isRegularFile() )
                return null
            final FileChannel channel = openOrNull(path)
            if( channel == null )
                continue
            final BasicFileAttributes after = attributes(path)
            if( after != null && same(before, after) && channel.size() == before.size() )
                return new Opened(channel, before.size(), tagOf(before))
            channel.close()
        }
        throw new IOException("${rel} kept changing while it was opened")
    }

    private static FileChannel openOrNull(Path path) {
        try {
            return FileChannel.open(path, StandardOpenOption.READ)
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes)
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    private static boolean same(BasicFileAttributes a, BasicFileAttributes b) {
        return a.size() == b.size() && a.lastModifiedTime() == b.lastModifiedTime() && a.fileKey() == b.fileKey()
    }

    private static String tagOf(BasicFileAttributes a) {
        final long nanos = a.lastModifiedTime().to(TimeUnit.NANOSECONDS)
        final int key = Objects.hashCode(a.fileKey())
        return "\"${Long.toString(a.size(), 36)}-${Long.toString(nanos, 36)}-${Integer.toUnsignedString(key, 36)}\"".toString()
    }

    @CompileStatic
    private static class Opened implements MemberFiles.Opened {
        private final FileChannel channel
        final long size
        final String tag

        Opened(FileChannel channel, long size, String tag) {
            this.channel = channel
            this.size = size
            this.tag = tag
        }

        @Override
        InputStream read(long start, long length) {
            channel.position(start)
            // Closing the stream must not close the channel: the caller closes the Opened.
            final InputStream stream = Channels.newInputStream(channel)
            return new FilterInputStream(stream) {
                @Override
                void close() {}
            }
        }

        @Override
        void close() { channel.close() }
    }

    @Override
    List<String> list(String dirRel) {
        final Path dir = resolve(dirRel.replaceAll('/+$', ''))
        if( !Files.isDirectory(dir) )
            return []
        return Files.list(dir).withCloseable { stream ->
            stream.toList().collect { Object p -> ((Path) p).fileName.toString() }
        }
    }

    @Override
    String describe() { root.toString() }
}
