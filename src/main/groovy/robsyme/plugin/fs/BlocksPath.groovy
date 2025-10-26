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

import java.nio.file.*

/**
 * BlocksPath - wraps a path in the blocks filesystem
 */
@Slf4j
@CompileStatic
class BlocksPath implements Path {

    private final BlocksFileSystem fileSystem
    private final String path

    BlocksPath(BlocksFileSystem fileSystem, String first, String... more) {
        this.fileSystem = fileSystem
        if (more && more.length > 0) {
            this.path = [first, *more].join(fileSystem.separator)
        } else {
            this.path = first
        }
    }

    @Override
    BlocksFileSystem getFileSystem() {
        return fileSystem
    }

    @Override
    boolean isAbsolute() {
        return path.startsWith(fileSystem.separator)
    }

    @Override
    Path getRoot() {
        if (isAbsolute()) {
            return new BlocksPath(fileSystem, fileSystem.separator)
        }
        return null
    }

    @Override
    Path getFileName() {
        if (path == fileSystem.separator || path.isEmpty()) {
            return null
        }
        def lastSep = path.lastIndexOf(fileSystem.separator)
        if (lastSep >= 0) {
            return new BlocksPath(fileSystem, path.substring(lastSep + 1))
        }
        return this
    }

    @Override
    Path getParent() {
        def lastSep = path.lastIndexOf(fileSystem.separator)
        if (lastSep < 0) {
            return null
        }
        if (lastSep == 0) {
            return new BlocksPath(fileSystem, fileSystem.separator)
        }
        return new BlocksPath(fileSystem, path.substring(0, lastSep))
    }

    @Override
    int getNameCount() {
        if (path.isEmpty() || path == fileSystem.separator) {
            return 0
        }
        def cleanPath = path
        if (cleanPath.startsWith(fileSystem.separator)) {
            cleanPath = cleanPath.substring(1)
        }
        if (cleanPath.endsWith(fileSystem.separator)) {
            cleanPath = cleanPath.substring(0, cleanPath.length() - 1)
        }
        if (cleanPath.isEmpty()) {
            return 0
        }
        return cleanPath.split(fileSystem.separator).length
    }

    @Override
    Path getName(int index) {
        def count = getNameCount()
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("Invalid index: ${index}")
        }
        def cleanPath = path
        if (cleanPath.startsWith(fileSystem.separator)) {
            cleanPath = cleanPath.substring(1)
        }
        def parts = cleanPath.split(fileSystem.separator)
        return new BlocksPath(fileSystem, parts[index])
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        def count = getNameCount()
        if (beginIndex < 0 || beginIndex >= count || endIndex <= beginIndex || endIndex > count) {
            throw new IllegalArgumentException("Invalid range: ${beginIndex} to ${endIndex}")
        }
        def cleanPath = path
        if (cleanPath.startsWith(fileSystem.separator)) {
            cleanPath = cleanPath.substring(1)
        }
        def parts = cleanPath.split(fileSystem.separator)
        def subParts = parts[beginIndex..<endIndex]
        return new BlocksPath(fileSystem, subParts.join(fileSystem.separator))
    }

    @Override
    boolean startsWith(Path other) {
        if (!(other instanceof BlocksPath)) {
            return false
        }
        return path.startsWith(other.toString())
    }

    @Override
    boolean startsWith(String other) {
        return path.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        if (!(other instanceof BlocksPath)) {
            return false
        }
        return path.endsWith(other.toString())
    }

    @Override
    boolean endsWith(String other) {
        return path.endsWith(other)
    }

    @Override
    Path normalize() {
        // Simple normalization - remove redundant separators and handle . and ..
        def parts = []
        def cleanPath = path
        if (cleanPath.startsWith(fileSystem.separator)) {
            cleanPath = cleanPath.substring(1)
        }

        for (def part : cleanPath.split(fileSystem.separator)) {
            if (part == '.' || part.isEmpty()) {
                continue
            } else if (part == '..') {
                if (parts.size() > 0) {
                    parts.removeLast()
                }
            } else {
                parts.add(part)
            }
        }

        def normalized = parts.join(fileSystem.separator)
        if (path.startsWith(fileSystem.separator)) {
            normalized = fileSystem.separator + normalized
        }
        return new BlocksPath(fileSystem, normalized)
    }

    @Override
    Path resolve(Path other) {
        if (other.isAbsolute()) {
            return other
        }
        if (path.isEmpty() || path == fileSystem.separator) {
            return new BlocksPath(fileSystem, other.toString())
        }
        return new BlocksPath(fileSystem, path + fileSystem.separator + other.toString())
    }

    @Override
    Path resolve(String other) {
        return resolve(new BlocksPath(fileSystem, other))
    }

    @Override
    Path resolveSibling(Path other) {
        def parent = getParent()
        if (parent == null) {
            return other
        }
        return parent.resolve(other)
    }

    @Override
    Path resolveSibling(String other) {
        return resolveSibling(new BlocksPath(fileSystem, other))
    }

    @Override
    Path relativize(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new IllegalArgumentException("Can only relativize BlocksPath")
        }
        if (this.isAbsolute() != other.isAbsolute()) {
            throw new IllegalArgumentException("Cannot relativize absolute and relative paths")
        }

        def thisPath = this.toString()
        def otherPath = other.toString()

        if (!otherPath.startsWith(thisPath)) {
            throw new IllegalArgumentException("Cannot relativize: ${otherPath} does not start with ${thisPath}")
        }

        def relative = otherPath.substring(thisPath.length())
        if (relative.startsWith(fileSystem.separator)) {
            relative = relative.substring(1)
        }

        return new BlocksPath(fileSystem, relative)
    }

    @Override
    URI toUri() {
        // Build blocks+scheme://path URI
        def underlyingUri = fileSystem.underlyingFileSystem.rootDirectories.first().toUri()
        def underlyingScheme = underlyingUri.scheme
        return new URI("blocks+${underlyingScheme}://${path}")
    }

    @Override
    Path toAbsolutePath() {
        if (isAbsolute()) {
            return this
        }
        // Get current working directory from underlying filesystem
        def realPath = fileSystem.toRealPath(this)
        def absoluteReal = realPath.toAbsolutePath()
        return new BlocksPath(fileSystem, absoluteReal.toString())
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        return fileSystem.toRealPath(this).toRealPath(options)
    }

    @Override
    File toFile() {
        // Convert to the underlying real path's file
        return fileSystem.toRealPath(this).toFile()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        def realPath = fileSystem.toRealPath(this)
        return realPath.register(watcher, events, modifiers)
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        def realPath = fileSystem.toRealPath(this)
        return realPath.register(watcher, events)
    }

    @Override
    Iterator<Path> iterator() {
        def count = getNameCount()
        return (0..<count).collect { getName(it) }.iterator()
    }

    @Override
    int compareTo(Path other) {
        if (!(other instanceof BlocksPath)) {
            throw new ClassCastException("Cannot compare BlocksPath with ${other.class}")
        }
        return path.compareTo(other.toString())
    }

    @Override
    String toString() {
        return path
    }

    @Override
    boolean equals(Object other) {
        if (this.is(other)) {
            return true
        }
        if (!(other instanceof BlocksPath)) {
            return false
        }
        def otherPath = other as BlocksPath
        return this.fileSystem == otherPath.fileSystem && this.path == otherPath.path
    }

    @Override
    int hashCode() {
        return path.hashCode()
    }
}
