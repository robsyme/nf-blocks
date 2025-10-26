/*
 * Copyright 2025, Rob Syme (rob.syme@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin.fs

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.channels.SeekableByteChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

/**
 * Blocks FileSystem - wraps an underlying filesystem and
 * intercepts operations for content-addressed storage.
 */
@Slf4j
@CompileStatic
class BlocksFileSystem extends FileSystem {

    private final BlocksFileSystemProvider provider
    private final URI blocksUri
    private final FileSystem underlyingFs
    private final Map<String, ?> env
    private final String separator = "/"

    BlocksFileSystem(BlocksFileSystemProvider provider, URI blocksUri, Map<String, ?> env) {
        this.provider = provider
        this.blocksUri = blocksUri
        this.env = env ?: [:]

        // Create or get the underlying filesystem
        def underlyingUri = BlocksFileSystemProvider.buildUnderlyingUri(blocksUri)
        log.debug "Underlying URI: ${underlyingUri}"

        def parsed = BlocksFileSystemProvider.parseBlocksUri(blocksUri)
        def scheme = parsed.scheme as String

        // Special handling for the default 'file' scheme
        if (scheme == 'file') {
            this.underlyingFs = FileSystems.getDefault()
            log.debug "Using default file system"
        } else {
            try {
                // Try to get existing filesystem first
                this.underlyingFs = FileSystems.getFileSystem(underlyingUri)
                log.debug "Using existing underlying filesystem: ${underlyingFs.class.simpleName}"
            } catch (FileSystemNotFoundException e) {
                // Create new filesystem if it doesn't exist
                this.underlyingFs = FileSystems.newFileSystem(underlyingUri, env)
                log.debug "Created new underlying filesystem: ${underlyingFs.class.simpleName}"
            }
        }
    }

    @Override
    FileSystemProvider provider() {
        return provider
    }

    @Override
    void close() throws IOException {
        // Don't close the underlying filesystem - other code might be using it
        log.debug "Blocks filesystem closed (underlying filesystem left open)"
    }

    @Override
    boolean isOpen() {
        return underlyingFs.isOpen()
    }

    @Override
    boolean isReadOnly() {
        return false // Blocks filesystem is writable
    }

    @Override
    String getSeparator() {
        return separator
    }

    @Override
    Iterable<Path> getRootDirectories() {
        List<Path> roots = underlyingFs.rootDirectories.collect { Path root ->
            new BlocksPath(this, root.toString()) as Path
        }
        return roots
    }

    @Override
    Iterable<FileStore> getFileStores() {
        return underlyingFs.fileStores
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return underlyingFs.supportedFileAttributeViews()
    }

    @Override
    Path getPath(String first, String... more) {
        return new BlocksPath(this, first, more)
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        return underlyingFs.getPathMatcher(syntaxAndPattern)
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        return underlyingFs.userPrincipalLookupService
    }

    @Override
    WatchService newWatchService() throws IOException {
        return underlyingFs.newWatchService()
    }

    /**
     * Get the underlying filesystem
     */
    FileSystem getUnderlyingFileSystem() {
        return underlyingFs
    }

    /**
     * Convert a BlocksPath to the underlying real path
     */
    Path toRealPath(BlocksPath blocksPath) {
        return underlyingFs.getPath(blocksPath.toString())
    }

    /**
     * Check if the underlying filesystem is S3
     */
    boolean isS3FileSystem() {
        return underlyingFs.class.simpleName.contains('S3')
    }

    /**
     * Get base directory for content-addressed storage
     *
     * This extracts the path from the original blocks URI.
     * E.g., blocks+file:///tmp/nf-blocks → /tmp/nf-blocks
     */
    Path getBaseStorageDir() {
        // Extract path from the original blocks URI
        def path = blocksUri.path

        if (path && path != '/') {
            // Use the path specified in the blocks URI
            return underlyingFs.getPath(path)
        }

        // Fallback to root if no path specified
        if (underlyingFs.rootDirectories) {
            return underlyingFs.rootDirectories.first() as Path
        }
        return underlyingFs.getPath('/')
    }

    // Delegate operations to underlying filesystem

    OutputStream newOutputStream(BlocksPath path, OpenOption... options) throws IOException {
        // Check if we should intercept this write for hash-addressed storage
        // For now, intercept all writes - could be filtered by path pattern

        if (isS3FileSystem()) {
            return createS3MultipartOutputStream(path)
        } else {
            return createLocalOutputStream(path)
        }
    }

    private OutputStream createLocalOutputStream(BlocksPath path) {
        // Generate unique staging filename
        def uuid = UUID.randomUUID().toString()

        def baseDir = getBaseStorageDir()
        log.debug "Base storage dir: ${baseDir} (class: ${baseDir.class.simpleName})"

        def stagingDir = baseDir.resolve('.staging')
        log.debug "Staging dir: ${stagingDir} (class: ${stagingDir.class.simpleName})"

        def tempPath = stagingDir.resolve("${uuid}.tmp")
        log.debug "Temp path: ${tempPath} (class: ${tempPath.class.simpleName})"

        log.debug "Creating local blocks output stream for ${path} via temp ${tempPath}"

        return new LocalBlocksOutputStream(
            baseDir,
            tempPath,
            'blobs'
        )
    }

    private OutputStream createS3MultipartOutputStream(BlocksPath path) {
        // TODO: Get S3 client from somewhere (need to think about this)
        // For now, throw exception - will implement in next step
        throw new UnsupportedOperationException(
            "S3 multipart upload not yet wired up - need S3Client configuration"
        )
    }

    InputStream newInputStream(BlocksPath path, OpenOption... options) throws IOException {
        // For reads, just delegate to underlying filesystem
        def realPath = toRealPath(path)
        return underlyingFs.provider().newInputStream(realPath, options)
    }

    SeekableByteChannel newByteChannel(BlocksPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        def realPath = toRealPath(path)
        return underlyingFs.provider().newByteChannel(realPath, options, attrs)
    }

    DirectoryStream<Path> newDirectoryStream(BlocksPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        def realPath = toRealPath(dir)
        def realStream = underlyingFs.provider().newDirectoryStream(realPath, filter)

        // Wrap the stream to return BlocksPath instances
        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                def realIterator = realStream.iterator()
                return new Iterator<Path>() {
                    @Override
                    boolean hasNext() {
                        return realIterator.hasNext()
                    }

                    @Override
                    Path next() {
                        def realNext = realIterator.next()
                        // Convert real path back to blocks path
                        def relativePath = realPath.relativize(realNext)
                        return dir.resolve(relativePath.toString())
                    }
                }
            }

            @Override
            void close() throws IOException {
                realStream.close()
            }
        }
    }

    void createDirectory(BlocksPath dir, FileAttribute<?>... attrs) throws IOException {
        def realPath = toRealPath(dir)
        underlyingFs.provider().createDirectory(realPath, attrs)
    }

    void delete(BlocksPath path) throws IOException {
        def realPath = toRealPath(path)
        underlyingFs.provider().delete(realPath)
    }

    void copy(BlocksPath source, BlocksPath target, CopyOption... options) throws IOException {
        def realSource = toRealPath(source)
        def realTarget = toRealPath(target)
        underlyingFs.provider().copy(realSource, realTarget, options)
    }

    void move(BlocksPath source, BlocksPath target, CopyOption... options) throws IOException {
        def realSource = toRealPath(source)
        def realTarget = toRealPath(target)
        underlyingFs.provider().move(realSource, realTarget, options)
    }

    boolean isHidden(BlocksPath path) throws IOException {
        def realPath = toRealPath(path)
        return underlyingFs.provider().isHidden(realPath)
    }

    FileStore getFileStore(BlocksPath path) throws IOException {
        def realPath = toRealPath(path)
        return underlyingFs.provider().getFileStore(realPath)
    }

    void checkAccess(BlocksPath path, AccessMode... modes) throws IOException {
        def realPath = toRealPath(path)
        underlyingFs.provider().checkAccess(realPath, modes)
    }

    <V extends FileAttributeView> V getFileAttributeView(BlocksPath path, Class<V> type, LinkOption... options) {
        def realPath = toRealPath(path)
        return underlyingFs.provider().getFileAttributeView(realPath, type, options)
    }

    <A extends BasicFileAttributes> A readAttributes(BlocksPath path, Class<A> type, LinkOption... options) throws IOException {
        def realPath = toRealPath(path)
        return underlyingFs.provider().readAttributes(realPath, type, options)
    }

    Map<String, Object> readAttributes(BlocksPath path, String attributes, LinkOption... options) throws IOException {
        def realPath = toRealPath(path)
        return underlyingFs.provider().readAttributes(realPath, attributes, options)
    }

    void setAttribute(BlocksPath path, String attribute, Object value, LinkOption... options) throws IOException {
        def realPath = toRealPath(path)
        underlyingFs.provider().setAttribute(realPath, attribute, value, options)
    }
}
