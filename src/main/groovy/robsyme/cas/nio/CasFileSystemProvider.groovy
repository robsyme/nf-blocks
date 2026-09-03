package robsyme.cas.nio

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.file.FileSystemTransferAware
import robsyme.cas.CasConfig

/**
 * The `cas` scheme (DESIGN.md section 8).
 *
 * This is the boundary as the walking skeleton needs it: a Publish Coordinate
 * `cas://<alias>/<a/b/c>` is mirrored onto the real directory
 * `<member location>/coords/<a/b/c>`, so a run publishes, reads back and
 * resumes through the scheme before any block store exists. Task 6 replaces
 * the body of these methods with the block store and the Pointer File tree;
 * the shape Nextflow sees does not change.
 */
@Slf4j
@CompileStatic
class CasFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    static final String SCHEME = CasPath.SCHEME

    /** Sub-directory of a member holding the Publish Coordinate tree. */
    static final String COORDS = 'coords'

    private final CasFileSystem fileSystem = new CasFileSystem(this)

    private volatile CasConfig casConfig

    @Override String getScheme() { SCHEME }

    CasConfig getCasConfig() {
        if( casConfig == null ) {
            synchronized (this) {
                if( casConfig == null ) {
                    final session = Global.session as Session
                    casConfig = CasConfig.fromSession(session?.config)
                }
            }
        }
        return casConfig
    }

    /** Test seam: bind the store configuration without a Nextflow session. */
    void setCasConfig(CasConfig config) {
        synchronized (this) {
            this.casConfig = config
        }
    }

    /**
     * The real directory backing a Publish Coordinate.
     */
    protected Path real(Path path) {
        if( !(path instanceof CasPath) )
            throw new IllegalArgumentException("Not a cas:// path: ${path}")
        final casPath = (CasPath)path
        if( casPath.isStoreUri() )
            throw new UnsupportedOperationException("Reading a Store URI is not implemented yet: ${casPath}")
        if( !casPath.isAbsolute() )
            throw new IllegalArgumentException("Cannot resolve a relative cas:// path: ${casPath}")
        final location = getCasConfig().locationOf(casPath.authority)
        if( location == null )
            throw new IllegalArgumentException("Unknown store alias '${casPath.authority}' -- configured stores: ${getCasConfig().members.join(', ')}")
        Path result = location.resolve(COORDS)
        for( String segment : casPath.segments )
            result = result.resolve(segment)
        return result
    }

    // --- file system lookup

    @Override
    FileSystem newFileSystem(URI uri, Map<String,?> env) {
        return fileSystem
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        return fileSystem
    }

    @Override
    Path getPath(URI uri) {
        if( uri.scheme != SCHEME )
            throw new IllegalArgumentException("Not a ${SCHEME}:// URI: ${uri}")
        final authority = uri.authority
        if( !authority )
            throw new IllegalArgumentException("Missing store alias or content address in URI: ${uri}")
        return new CasPath(fileSystem, authority, CasPath.split(uri.path))
    }

    // --- transfers

    @Override
    boolean canUpload(Path source, Path target) {
        return target instanceof CasPath && ((CasPath)target).isCoordinate()
    }

    @Override
    boolean canDownload(Path source, Path target) {
        return source instanceof CasPath
    }

    /**
     * Nextflow hands us a directory as a single call and never recurses, so a
     * directory is walked here. Returning normally with a child untransferred
     * is a silent data loss, measured on the probe.
     */
    @Override
    void upload(Path source, Path target, CopyOption... options) throws IOException {
        final dest = real(target)
        final replace = options.toList().contains(StandardCopyOption.REPLACE_EXISTING)
        if( !replace && Files.exists(dest) )
            throw new FileAlreadyExistsException(target.toString())
        if( dest.parent != null )
            Files.createDirectories(dest.parent)
        transfer(source, dest, replace)
    }

    @Override
    void download(Path source, Path target, CopyOption... options) throws IOException {
        final src = real(source)
        if( !Files.exists(src) )
            throw new NoSuchFileException(source.toString())
        final replace = options.toList().contains(StandardCopyOption.REPLACE_EXISTING)
        if( target.parent != null )
            Files.createDirectories(target.parent)
        transfer(src, target, replace)
    }

    private static void transfer(Path source, Path target, boolean replace) throws IOException {
        if( !Files.isDirectory(source) ) {
            copyFile(source, target, replace)
            return
        }
        Files.createDirectories(target)
        try( DirectoryStream<Path> children = Files.newDirectoryStream(source) ) {
            for( Path child : children )
                transfer(child, target.resolve(child.fileName.toString()), replace)
        }
    }

    private static void copyFile(Path source, Path target, boolean replace) throws IOException {
        if( replace )
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
        else
            Files.copy(source, target)
    }

    // --- read side

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return Files.newByteChannel(real(path), options, attrs)
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        return Files.newInputStream(real(path), options)
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        final target = real(path)
        if( target.parent != null )
            Files.createDirectories(target.parent)
        return Files.newOutputStream(target, options)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final backing = Files.newDirectoryStream(real(dir))
        final casDir = (CasPath)dir
        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                final List<Path> out = new ArrayList<Path>()
                for( Path p : backing ) {
                    final child = casDir.resolve(p.fileName.toString())
                    if( filter == null || filter.accept(child) )
                        out.add(child)
                }
                return out.iterator()
            }

            @Override
            void close() throws IOException { backing.close() }
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        Files.createDirectories(real(dir))
    }

    @Override
    void delete(Path path) throws IOException {
        Files.delete(real(path))
    }

    @Override
    boolean deleteIfExists(Path path) throws IOException {
        return Files.deleteIfExists(real(path))
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        final dest = real(target)
        if( dest.parent != null )
            Files.createDirectories(dest.parent)
        Files.copy(real(source), dest, options)
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        final dest = real(target)
        if( dest.parent != null )
            Files.createDirectories(dest.parent)
        Files.move(real(source), dest, options)
    }

    @Override
    boolean isSameFile(Path a, Path b) throws IOException {
        return a == b
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
        final target = real(path)
        if( !Files.exists(target) )
            throw new NoSuchFileException(path.toString())
        if( AccessMode.WRITE in modes.toList() && ((CasPath)path).isStoreUri() )
            throw new AccessDeniedException(path.toString())
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return Files.getFileAttributeView(real(path), type, options)
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        try {
            return Files.readAttributes(real(path), type, options)
        }
        catch( NoSuchFileException e ) {
            throw new NoSuchFileException(path.toString())
        }
    }

    @Override
    Map<String,Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        try {
            return Files.readAttributes(real(path), attributes, options)
        }
        catch( NoSuchFileException e ) {
            throw new NoSuchFileException(path.toString())
        }
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        // attributes of a content-addressed object are facts about its content, not settable
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
}
