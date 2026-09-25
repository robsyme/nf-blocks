package robsyme.cas.core

import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.CompileStatic

/** The Store Log of a local store root: empty files under `log/` (DESIGN.md §5). */
@CompileStatic
class LocalStoreLogStorage implements StoreLogStorage {

    private static final String LOG = 'log'

    private final Path root

    LocalStoreLogStorage(Path storeRoot) {
        this.root = storeRoot
    }

    Path getDirectory() { root.resolve(LOG) }

    @Override
    void putEntry(String name) {
        final Path directory = getDirectory()
        Files.createDirectories(directory)
        try {
            Files.createFile(directory.resolve(name))
        }
        catch( FileAlreadyExistsException e ) {
            // Write-once: the same entry written twice is the same entry.
        }
    }

    @Override
    List<String> listEntries() {
        final Path directory = getDirectory()
        if( !Files.isDirectory(directory) )
            return []
        final List<String> names = new ArrayList<String>()
        Files.list(directory).withCloseable { stream ->
            for( Object p : stream.toList() )
                names.add(((Path) p).fileName.toString())
        }
        return names
    }

    @Override
    String toString() { "LocalStoreLogStorage[${getDirectory()}]" }
}
