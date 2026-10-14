package robsyme.cas.core

import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import groovy.transform.CompileStatic

/** `<member>/store-id` on a local filesystem. */
@CompileStatic
class LocalStoreIdStorage implements StoreIdStorage {

    private final Path root

    LocalStoreIdStorage(Path root) { this.root = root }

    private Path file() { root.resolve(StoreIds.FILE) }

    @Override
    String read() {
        try {
            return StoreIds.parse(Files.readAllBytes(file()), file().toString())
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    String create(String id) {
        if( !StoreIds.valid(id) )
            throw new IllegalArgumentException("not a store id: '${id}'")
        Files.createDirectories(root)
        final Path temp = root.resolve(".tmp-store-id-${UUID.randomUUID()}")
        try {
            Files.write(temp, (id + '\n').getBytes('UTF-8'), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
            try {
                Files.createLink(file(), temp)
            }
            catch( FileAlreadyExistsException e ) {
                // Another writer won; its id stands.
            }
        }
        finally {
            Files.deleteIfExists(temp)
        }
        return read()
    }

    @Override
    String describe() { file().toString() }
}
