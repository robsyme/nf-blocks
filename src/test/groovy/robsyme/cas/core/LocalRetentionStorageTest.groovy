package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.TempDir

class LocalRetentionStorageTest extends RetentionStorageContract {

    @TempDir Path root

    private LocalBlockStore blocks() { new LocalBlockStore(root, 'lab', true) }

    @Override RetentionStorage storage() { new LocalRetentionStorage(root) }

    @Override Cid writeBlock(byte[] bytes) { blocks().putStreaming(new ByteArrayInputStream(bytes)) }

    @Override
    void leaveScratch() {
        Files.createDirectories(root.resolve('blocks'))
        Files.write(root.resolve('blocks').resolve('.tmp-x'), 'x'.bytes)
    }

    @Override
    void writeLogEntry(String name) {
        final Path dir = root.resolve('log')
        Files.createDirectories(dir)
        Files.write(dir.resolve(name), new byte[0])
    }

    @Override boolean logEntryExists(String name) { Files.exists(root.resolve('log').resolve(name)) }
}
