package robsyme.cas.nio

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

import groovy.transform.CompileStatic

/**
 * The single `cas` file system of the JVM (DESIGN.md section 8). It carries no
 * state of its own: an authority -- a store alias or a content address -- lives
 * on each {@link CasPath}, and the members it resolves against live in the
 * provider.
 */
@CompileStatic
class CasFileSystem extends FileSystem {

    private final CasFileSystemProvider provider

    CasFileSystem(CasFileSystemProvider provider) {
        this.provider = provider
    }

    @Override FileSystemProvider provider() { provider }

    @Override void close() throws IOException { }

    @Override boolean isOpen() { true }

    @Override boolean isReadOnly() { false }

    @Override String getSeparator() { '/' }

    @Override Iterable<Path> getRootDirectories() { Collections.<Path>emptyList() }

    @Override Iterable<FileStore> getFileStores() { Collections.<FileStore>emptyList() }

    @Override Set<String> supportedFileAttributeViews() { ['basic'] as Set }

    @Override
    Path getPath(String first, String... more) {
        final joined = more ? ([first] + (more as List<String>)).join('/') : first
        if( joined.startsWith("${CasPath.SCHEME}://") )
            return provider.getPath(URI.create(joined))
        return new CasPath(this, null, CasPath.split(joined))
    }

    @Override PathMatcher getPathMatcher(String syntaxAndPattern) {
        return FileSystems.getDefault().getPathMatcher(syntaxAndPattern)
    }

    @Override UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException()
    }

    @Override WatchService newWatchService() {
        throw new UnsupportedOperationException()
    }
}
