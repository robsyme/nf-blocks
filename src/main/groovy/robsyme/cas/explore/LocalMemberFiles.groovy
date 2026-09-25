package robsyme.cas.explore

import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

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

    @Override
    Long size(String rel) {
        final Path path = resolve(rel)
        return Files.isRegularFile(path) ? Files.size(path) : null
    }

    @Override
    InputStream open(String rel, long start, long length) {
        final FileChannel channel = FileChannel.open(resolve(rel), StandardOpenOption.READ)
        channel.position(start)
        return Channels.newInputStream(channel)
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
