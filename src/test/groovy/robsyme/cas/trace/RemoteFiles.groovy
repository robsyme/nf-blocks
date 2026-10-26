package robsyme.cas.trace

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 * A remote file system for the input recorder's tests, standing in for
 * s3://, gs://, az:// and http(s)://. A stat answers from {@link #sizes}
 * (and waits on {@link #statGate} when one is set, deaf to interrupts as a
 * hung mount is); opening a file's content counts in {@link #reads} and
 * fails, so a test can say no content was read.
 *
 * A path's {@code getFileName} keeps the URI's query, the worst a provider
 * could do, so a test can say the recorded name never carries one.
 */
class RemoteFiles extends FileSystem {

    final Provider provider = new Provider(this)
    final Map<String, Long> sizes = [:]
    final AtomicInteger reads = new AtomicInteger()
    final AtomicInteger stats = new AtomicInteger()
    volatile CountDownLatch statGate
    /** Counted down when a stat starts waiting on the gate. */
    final CountDownLatch statWaiting = new CountDownLatch(1)

    /** A remote path for {@code uri}, its stat answering {@code size} (null: no such file). */
    Path path(String uri, Long size = null) {
        final RemotePath p = new RemotePath(this, URI.create(uri))
        if( size != null )
            sizes.put(uri, size)
        return p
    }

    // ---------------------------------------------------------------- FileSystem

    @Override FileSystemProvider provider() { provider }
    @Override void close() {}
    @Override boolean isOpen() { true }
    @Override boolean isReadOnly() { true }
    @Override String getSeparator() { '/' }
    @Override Iterable<Path> getRootDirectories() { [] }
    @Override Iterable<FileStore> getFileStores() { [] }
    @Override Set<String> supportedFileAttributeViews() { ['basic'] as Set }
    @Override Path getPath(String first, String... more) { throw new UnsupportedOperationException() }
    @Override PathMatcher getPathMatcher(String syntaxAndPattern) { throw new UnsupportedOperationException() }
    @Override UserPrincipalLookupService getUserPrincipalLookupService() { throw new UnsupportedOperationException() }
    @Override WatchService newWatchService() { throw new UnsupportedOperationException() }

    // ---------------------------------------------------------------- Path

    static class RemotePath implements Path {
        final RemoteFiles fs
        final URI uri
        /** A file-name path's text (its last segment and the URI's query); null for a whole path. */
        final String text

        RemotePath(RemoteFiles fs, URI uri, String text = null) {
            this.fs = fs
            this.uri = uri
            this.text = text
        }

        @Override FileSystem getFileSystem() { fs }
        @Override boolean isAbsolute() { text == null }
        @Override Path getRoot() { null }
        @Override Path getFileName() {
            final String raw = uri.rawPath ?: ''
            final String last = raw.substring(raw.lastIndexOf('/') + 1)
            return last ? new RemotePath(fs, uri, last + (uri.rawQuery ? '?' + uri.rawQuery : '')) : null
        }
        @Override Path getParent() { null }
        @Override int getNameCount() { 1 }
        @Override Path getName(int index) { getFileName() }
        @Override Path subpath(int beginIndex, int endIndex) { throw new UnsupportedOperationException() }
        @Override boolean startsWith(Path other) { other == this }
        @Override boolean endsWith(Path other) { other == this }
        @Override Path normalize() { this }
        @Override Path resolve(Path other) { throw new UnsupportedOperationException() }
        @Override Path relativize(Path other) { throw new UnsupportedOperationException() }
        @Override URI toUri() { uri }
        @Override Path toAbsolutePath() { this }
        @Override Path toRealPath(LinkOption... options) { this }
        @Override WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) { throw new UnsupportedOperationException() }
        @Override int compareTo(Path other) { uri.toString() <=> other.toUri().toString() }
        @Override boolean equals(Object o) { o instanceof RemotePath && ((RemotePath) o).fs.is(fs) && ((RemotePath) o).uri == uri && ((RemotePath) o).text == text }
        @Override int hashCode() { uri.hashCode() }
        @Override String toString() { text ?: uri.toString() }
    }

    // ---------------------------------------------------------------- provider

    static class Provider extends FileSystemProvider {
        final RemoteFiles fs

        Provider(RemoteFiles fs) { this.fs = fs }

        @Override String getScheme() { 'remote-test' }
        @Override FileSystem newFileSystem(URI uri, Map<String, ?> env) { fs }
        @Override FileSystem getFileSystem(URI uri) { fs }
        @Override Path getPath(URI uri) { new RemotePath(fs, uri) }

        @Override
        InputStream newInputStream(Path path, OpenOption... options) {
            fs.reads.incrementAndGet()
            throw new IOException("test: the content of remote file ${path} was opened")
        }

        @Override
        SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) {
            fs.reads.incrementAndGet()
            throw new IOException("test: the content of remote file ${path} was opened")
        }

        @Override
        <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) {
            fs.stats.incrementAndGet()
            final CountDownLatch gate = fs.statGate
            if( gate != null ) {
                fs.statWaiting.countDown()
                while( gate.count > 0 ) {
                    try { gate.await() } catch( InterruptedException ignored ) { }
                }
            }
            final Long size = fs.sizes.get(path.toUri().toString())
            if( size == null )
                throw new NoSuchFileException(path.toString())
            return (A) new Attributes(size)
        }

        @Override DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) { throw new UnsupportedOperationException() }
        @Override void createDirectory(Path dir, FileAttribute<?>... attrs) { throw new UnsupportedOperationException() }
        @Override void delete(Path path) { throw new UnsupportedOperationException() }
        @Override void copy(Path source, Path target, CopyOption... options) { throw new UnsupportedOperationException() }
        @Override void move(Path source, Path target, CopyOption... options) { throw new UnsupportedOperationException() }
        @Override boolean isSameFile(Path path, Path path2) { path == path2 }
        @Override boolean isHidden(Path path) { false }
        @Override FileStore getFileStore(Path path) { throw new UnsupportedOperationException() }
        @Override void checkAccess(Path path, AccessMode... modes) { readAttributes(path, BasicFileAttributes) }
        @Override <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) { null }
        @Override Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) { throw new UnsupportedOperationException() }
        @Override void setAttribute(Path path, String attribute, Object value, LinkOption... options) { throw new UnsupportedOperationException() }
    }

    static class Attributes implements BasicFileAttributes {
        final long size

        Attributes(long size) { this.size = size }

        @Override FileTime lastModifiedTime() { FileTime.fromMillis(0L) }
        @Override FileTime lastAccessTime() { FileTime.fromMillis(0L) }
        @Override FileTime creationTime() { FileTime.fromMillis(0L) }
        @Override boolean isRegularFile() { true }
        @Override boolean isDirectory() { false }
        @Override boolean isSymbolicLink() { false }
        @Override boolean isOther() { false }
        @Override long size() { size }
        @Override Object fileKey() { null }
    }
}
