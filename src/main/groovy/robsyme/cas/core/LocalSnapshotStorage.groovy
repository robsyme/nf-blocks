package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermissions

import groovy.transform.CompileStatic

/** index/v<N>.sqlite and index.html in a local member: an atomic move, as before (ticket 03 decision 1). */
@CompileStatic
class LocalSnapshotStorage implements SnapshotStorage {

    final Path memberRoot

    LocalSnapshotStorage(Path memberRoot) { this.memberRoot = memberRoot }

    private Path target() { IndexSnapshot.pathIn(memberRoot) }

    @Override
    SnapshotBase base(Path tempDir) {
        if( !Files.isRegularFile(target()) ) return null
        final BasicFileAttributes a = Files.readAttributes(target(), BasicFileAttributes)
        return new SnapshotBase("${a.size()}:${a.lastModifiedTime().toMillis()}:${a.fileKey()}".toString(), a.size(), IndexSnapshot.countRuns(target()))
    }

    @Override
    Path fetch(Path tempDir) {
        if( !Files.isRegularFile(target()) ) return null
        Files.createDirectories(tempDir)
        final Path copy = Files.createTempFile(tempDir, 'nf-blocks-snapshot-', '.sqlite')
        Files.copy(target(), copy, StandardCopyOption.REPLACE_EXISTING)
        return copy
    }

    /** Locally the move is atomic and the count guard is the caller's; base is not re-checked. */
    @Override
    boolean replace(Path built, int runs, SnapshotBase base) {
        Files.createDirectories(target().parent)
        final Path staged = target().resolveSibling(".tmp-${UUID.randomUUID()}.sqlite")
        try {
            Files.copy(built, staged)
            if( FileSystems.default.supportedFileAttributeViews().contains('posix') )
                Files.setPosixFilePermissions(staged, PosixFilePermissions.fromString('rw-r--r--'))
            Files.move(staged, target(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return true
        }
        finally {
            Files.deleteIfExists(staged)
        }
    }

    @Override boolean writePage(byte[] page) { IndexSnapshot.writePage(memberRoot, page) }

    @Override String describe() { target().toString() }
}
