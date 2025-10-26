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
import java.nio.file.spi.FileSystemProvider

/**
 * FileSystemProvider for blocks+<backend>:// URIs
 *
 * Handles composite URIs like:
 * - blocks+file:///tmp/nf-blocks
 * - blocks+s3://bucket/prefix
 *
 * Delegates most operations to the underlying filesystem,
 * but intercepts writes to upload to content-addressed storage.
 */
@Slf4j
@CompileStatic
class BlocksFileSystemProvider extends FileSystemProvider {

    // Map of authority -> filesystem instance
    private final Map<String, BlocksFileSystem> fileSystems = new HashMap<>()

    /**
     * Override in subclasses to return the full scheme (e.g., "blocks+file", "blocks+s3")
     * Base class just returns "blocks" but shouldn't be used directly
     */
    @Override
    String getScheme() {
        return 'blocks'
    }

    /**
     * Parse a blocks URI to extract the underlying scheme and path
     *
     * blocks+file:///tmp/nf-blocks -> [scheme: 'file', path: '/tmp/nf-blocks']
     * blocks+s3://bucket/prefix -> [scheme: 's3', bucket: 'bucket', path: '/prefix']
     */
    static Map<String, String> parseBlocksUri(URI uri) {
        def fullScheme = uri.scheme

        // Handle both "blocks+scheme" and plain "blocks" URIs
        String underlyingScheme
        if (fullScheme?.startsWith('blocks+')) {
            // Extract underlying scheme: blocks+file -> file
            underlyingScheme = fullScheme.substring(7) // "blocks+".length() = 7
        } else if (fullScheme == 'blocks') {
            // Default to 'file' if no specific scheme
            underlyingScheme = 'file'
        } else {
            throw new IllegalArgumentException("Not a blocks URI: ${uri}")
        }

        Map<String, String> result = [
            scheme: underlyingScheme,
            host: uri.host,
            port: uri.port.toString(),
            path: uri.path
        ]

        log.debug "Parsed blocks URI: ${uri} -> ${result}"
        return result
    }

    /**
     * Build underlying URI from blocks URI
     *
     * blocks+file:///tmp/nf-blocks -> file:///tmp/nf-blocks
     * blocks+s3://bucket/prefix -> s3://bucket/prefix
     */
    static URI buildUnderlyingUri(URI blocksUri) {
        def parsed = parseBlocksUri(blocksUri)
        def scheme = parsed.scheme as String
        def host = parsed.host as String
        def portStr = parsed.port as String
        def path = parsed.path as String

        // Build URI for underlying filesystem
        def uriStr = "${scheme}://"
        if (host) {
            uriStr += host
            if (portStr && portStr != '-1') {
                uriStr += ":${portStr}"
            }
        }
        uriStr += path ?: ''

        return new URI(uriStr)
    }

    @Override
    synchronized FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        log.debug "Creating new blocks filesystem for: ${uri}"

        def parsed = parseBlocksUri(uri)
        String key = "${parsed.scheme}:${uri.authority}:${uri.path}".toString()

        if (fileSystems.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("Blocks filesystem already exists: ${key}")
        }

        def fs = new BlocksFileSystem(this, uri, env)
        fileSystems.put(key, fs)

        log.info "Created blocks filesystem: ${key}"
        return fs
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        def parsed = parseBlocksUri(uri)
        String key = "${parsed.scheme}:${uri.authority}:${uri.path}".toString()

        def fs = fileSystems.get(key)
        if (!fs) {
            throw new FileSystemNotFoundException("Blocks filesystem not found: ${key}")
        }

        return fs
    }

    synchronized FileSystem getOrCreateFileSystem(URI uri, Map<String, ?> env = [:]) {
        try {
            return getFileSystem(uri)
        } catch (FileSystemNotFoundException e) {
            return newFileSystem(uri, env)
        }
    }

    @Override
    Path getPath(URI uri) {
        def fs = getOrCreateFileSystem(uri) as BlocksFileSystem
        return fs.getPath(uri.path)
    }

    private BlocksPath toBlocksPath(Path path) {
        if (path !instanceof BlocksPath) {
            throw new ProviderMismatchException("Not a BlocksPath: ${path.class}")
        }
        return path as BlocksPath
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.newOutputStream(blocksPath, options)
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.newInputStream(blocksPath, options)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.newByteChannel(blocksPath, options, attrs)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        def blocksPath = toBlocksPath(dir)
        return blocksPath.fileSystem.newDirectoryStream(blocksPath, filter)
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        def blocksPath = toBlocksPath(dir)
        blocksPath.fileSystem.createDirectory(blocksPath, attrs)
    }

    @Override
    void delete(Path path) throws IOException {
        def blocksPath = toBlocksPath(path)
        blocksPath.fileSystem.delete(blocksPath)
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        def blocksSource = toBlocksPath(source)
        def blocksTarget = toBlocksPath(target)
        blocksSource.fileSystem.copy(blocksSource, blocksTarget, options)
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        def blocksSource = toBlocksPath(source)
        def blocksTarget = toBlocksPath(target)
        blocksSource.fileSystem.move(blocksSource, blocksTarget, options)
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        if (path == path2) {
            return true
        }
        if (!(path instanceof BlocksPath) || !(path2 instanceof BlocksPath)) {
            return false
        }
        def blocksPath1 = path as BlocksPath
        def blocksPath2 = path2 as BlocksPath
        return blocksPath1.toRealPath() == blocksPath2.toRealPath()
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.isHidden(blocksPath)
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.getFileStore(blocksPath)
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        def blocksPath = toBlocksPath(path)
        blocksPath.fileSystem.checkAccess(blocksPath, modes)
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.getFileAttributeView(blocksPath, type, options)
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.readAttributes(blocksPath, type, options)
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        def blocksPath = toBlocksPath(path)
        return blocksPath.fileSystem.readAttributes(blocksPath, attributes, options)
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        def blocksPath = toBlocksPath(path)
        blocksPath.fileSystem.setAttribute(blocksPath, attribute, value, options)
    }
}
