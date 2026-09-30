package robsyme.cas.core

import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest

import groovy.transform.CompileStatic

/**
 * Retention objects in a local member (ticket 20 answer 5): sweep.lock by
 * create-exclusive and compare-then-move, live/<session> rewritten to
 * heartbeat, trash/<name> ledgers. Ages are mtimes against this host's clock,
 * which is the clock that wrote them.
 */
@CompileStatic
class LocalRetentionStorage implements RetentionStorage {

    private final Path root
    private final LocalBlockStore blocks

    LocalRetentionStorage(Path root) {
        this.root = root
        this.blocks = new LocalBlockStore(root, 'retention', true)
    }

    @Override long nowMillis() { System.currentTimeMillis() }

    private Path lock() { root.resolve('sweep.lock') }

    @Override
    Versioned readLock() {
        try {
            final byte[] body = Files.readAllBytes(lock())
            return new Versioned(body, sha(body), Files.getLastModifiedTime(lock()).toMillis())
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    String createLock(byte[] body) {
        try {
            Files.write(lock(), body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
            return sha(body)
        }
        catch( FileAlreadyExistsException e ) {
            return null
        }
    }

    /**
     * Compare, then move a temp file over the lock. Two local processes can
     * still both pass the compare; the loser learns at its next heartbeat,
     * which compares again, and a sweep heartbeats before every batch (plan
     * decision 10). One writer per host is the common case.
     */
    @Override
    synchronized String replaceLock(String version, byte[] body) {
        final Versioned current = readLock()
        if( current == null || current.version != version )
            return null
        final Path temp = root.resolve(".sweep.lock-${UUID.randomUUID()}")
        Files.write(temp, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
        try {
            Files.move(temp, lock(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
        catch( AtomicMoveNotSupportedException e ) {
            Files.move(temp, lock(), StandardCopyOption.REPLACE_EXISTING)
        }
        return sha(body)
    }

    @Override
    void putLive(String session, byte[] body) {
        final Path dir = root.resolve('live')
        Files.createDirectories(dir)
        Files.write(dir.resolve(session), body)
    }

    @Override void deleteLive(String session) { Files.deleteIfExists(root.resolve('live').resolve(session)) }

    @Override
    byte[] readLive(String session) {
        try {
            return Files.readAllBytes(root.resolve('live').resolve(session))
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override List<Stamped> listLive() { stamped(root.resolve('live')) }

    @Override
    List<String> listLedgers() {
        final Path dir = root.resolve('trash')
        if( !Files.isDirectory(dir) )
            return []
        final List<String> names = new ArrayList<String>()
        final java.util.stream.Stream<Path> s = Files.list(dir)
        try {
            for( Path p : s.toList() ) {
                final String n = p.fileName.toString()
                if( !n.startsWith('.') )
                    names.add(n)
            }
        }
        finally {
            s.close()
        }
        return names.sort()
    }

    @Override
    byte[] readLedger(String name) {
        try {
            return Files.readAllBytes(root.resolve('trash').resolve(name))
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    void writeLedger(String name, byte[] body) {
        final Path dir = root.resolve('trash')
        Files.createDirectories(dir)
        final Path temp = dir.resolve(".${name}-${UUID.randomUUID()}")
        Files.write(temp, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
        Files.move(temp, dir.resolve(name), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }

    @Override void deleteLedger(String name) { Files.deleteIfExists(root.resolve('trash').resolve(name)) }

    @Override
    List<BlockStat> listBlockStats() {
        final List<BlockStat> out = new ArrayList<BlockStat>()
        final java.util.stream.Stream<Cid> s = blocks.listBlocks()
        try {
            for( Cid c : s.toList() ) {
                final Path p = blocks.blockPath(c)
                out.add(new BlockStat(c, Files.size(p), Files.getLastModifiedTime(p).toMillis()))
            }
        }
        finally {
            s.close()
        }
        return out
    }

    @Override
    List<Cid> deleteBlocks(Collection<Cid> cids) {
        final List<Cid> failed = new ArrayList<Cid>()
        for( Cid c : cids ) {
            try {
                Files.deleteIfExists(blocks.blockPath(c))
            }
            catch( IOException e ) {
                failed.add(c)
            }
        }
        return failed
    }

    @Override
    List<Stamped> listScratch() {
        final Path dir = root.resolve('blocks')
        if( !Files.isDirectory(dir) )
            return []
        final List<Stamped> out = new ArrayList<Stamped>()
        final java.util.stream.Stream<Path> s = Files.walk(dir, 2)
        try {
            for( Path p : s.toList() ) {
                if( Files.isRegularFile(p) && p.fileName.toString().startsWith('.tmp-') )
                    out.add(new Stamped(root.relativize(p).toString(), Files.getLastModifiedTime(p).toMillis(), null))
            }
        }
        finally {
            s.close()
        }
        return out
    }

    @Override void deleteScratch(Stamped scratch) { Files.deleteIfExists(root.resolve(scratch.name)) }

    @Override void deleteLogEntry(String name) { Files.deleteIfExists(root.resolve('log').resolve(name)) }

    @Override String describe() { root.toString() }

    private static List<Stamped> stamped(Path dir) {
        if( !Files.isDirectory(dir) )
            return []
        final List<Stamped> out = new ArrayList<Stamped>()
        final java.util.stream.Stream<Path> s = Files.list(dir)
        try {
            for( Path p : s.toList() ) {
                if( Files.isRegularFile(p) )
                    out.add(new Stamped(p.fileName.toString(), Files.getLastModifiedTime(p).toMillis(), null))
            }
        }
        finally {
            s.close()
        }
        return out
    }

    private static String sha(byte[] body) {
        return MessageDigest.getInstance('SHA-256').digest(body).encodeHex().toString()
    }
}
