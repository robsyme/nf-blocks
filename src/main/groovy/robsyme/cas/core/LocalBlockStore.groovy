package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.stream.Stream

import groovy.transform.CompileStatic

/**
 * A block store in a local directory (DESIGN.md §5):
 *
 * <pre>
 * blocks/&lt;xx&gt;/&lt;cid&gt;   xx = the last two characters of the cid string
 * </pre>
 *
 * Every write streams through a fixed buffer into a temporary file, fsyncs
 * it, makes it read-only and links it into place with an atomic
 * create-if-absent, then fsyncs the directory. A link that fails because the
 * block is already there is success: the bytes are the same, that is what the
 * address means. Placement never replaces an existing block, so a block's
 * mtime is stable per address as {@link BlockStore#lastModifiedMillis} says.
 */
@CompileStatic
class LocalBlockStore implements BlockStore, LoggedStore {

    private static final String BLOCKS = 'blocks'
    private static final String TEMP_PREFIX = '.tmp-'
    private static final Set<PosixFilePermission> READ_ONLY = PosixFilePermissions.fromString('r--r--r--')
    private static final SecureRandom RANDOM = new SecureRandom()

    private final Path root
    private final String alias
    private final boolean writable

    LocalBlockStore(Path root, String alias, boolean writable) {
        this.root = root
        this.alias = alias
        this.writable = writable
    }

    Path getRoot() { root }

    @Override
    StoreLogStorage storeLogStorage() { new LocalStoreLogStorage(root) }

    @Override
    String alias() { alias }

    @Override
    boolean isWritable() { writable }

    /** Where the block for {@code cid} lives, whether or not it exists. */
    Path blockPath(Cid cid) {
        final String text = cid.toString()
        return root.resolve(BLOCKS).resolve(text.substring(text.length() - 2)).resolve(text)
    }

    @Override
    boolean has(Cid cid) {
        return present(blockPath(cid))
    }

    /**
     * True if a block file is there, false only if it is genuinely missing.
     * {@code Files.isRegularFile} swallows every IOException and answers
     * false, so a permission error would read as absence and let a composite
     * store fall through to another member; this lets anything but
     * {@link NoSuchFileException} propagate instead.
     */
    private static boolean present(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes).isRegularFile()
        }
        catch( NoSuchFileException e ) {
            return false
        }
    }

    // Only a missing file is an absence. A permission error or a full disk
    // must not read as "not here", or a composite store would quietly fall
    // through to another member and answer with the wrong provenance.

    @Override
    long size(Cid cid) {
        try {
            return Files.size(blockPath(cid))
        }
        catch( NoSuchFileException e ) {
            throw new NoSuchBlockException(cid, alias)
        }
    }

    @Override
    InputStream open(Cid cid) {
        try {
            return Files.newInputStream(blockPath(cid))
        }
        catch( NoSuchFileException e ) {
            throw new NoSuchBlockException(cid, alias)
        }
    }

    @Override
    long lastModifiedMillis(Cid cid) {
        try {
            return Files.getLastModifiedTime(blockPath(cid)).toMillis()
        }
        catch( NoSuchFileException e ) {
            throw new NoSuchBlockException(cid, alias)
        }
    }

    @Override
    void put(Cid cid, InputStream input, long expectedSize) {
        checkWritable()
        if( has(cid) )
            return
        final Path target = blockPath(cid)
        Files.createDirectories(target.parent)
        final Path temp = target.parent.resolve(TEMP_PREFIX + token())
        try {
            final long[] count = new long[1]
            final byte[] digest = HashBufferPool.shared().withBuffer { byte[] buffer -> drainTo(input, temp, buffer, count) } as byte[]
            final long written = count[0]
            final Cid actual = Cid.of(cid.codec, digest)
            if( actual != cid )
                throw new BlockMismatchException(cid, "the bytes hash to $actual")
            if( expectedSize >= 0 && written != expectedSize )
                throw new BlockMismatchException(cid, "$written bytes arrived, not the announced $expectedSize")
            place(temp, target)
        }
        finally {
            Files.deleteIfExists(temp)
        }
    }

    @Override
    Cid putStreaming(InputStream input) {
        checkWritable()
        // The temp file lives at blocks/.tmp-<random> rather than in a shard,
        // because the shard is the last two characters of the cid and the cid
        // is only known once the whole stream has been hashed. The move into
        // blocks/<xx>/ stays inside one filesystem, so the link is still atomic.
        final Path staging = root.resolve(BLOCKS)
        Files.createDirectories(staging)
        final Path temp = staging.resolve(TEMP_PREFIX + token())
        try {
            final Cid cid = Cid.of(Cid.RAW, HashBufferPool.shared().withBuffer { byte[] buffer ->
                drainTo(input, temp, buffer, new long[1])
            } as byte[])
            final Path target = blockPath(cid)
            if( !present(target) ) {
                Files.createDirectories(target.parent)
                place(temp, target)
            }
            return cid
        }
        finally {
            Files.deleteIfExists(temp)
        }
    }

    @Override
    Cid putDagCbor(Object value) {
        checkWritable()
        final byte[] encoded = DagCbor.encode(value)
        final Cid cid = DagCbor.cidOf(encoded)
        put(cid, new ByteArrayInputStream(encoded), encoded.length)
        return cid
    }

    @Override
    Stream<Cid> listBlocks() {
        final Path blocks = root.resolve(BLOCKS)
        if( !Files.isDirectory(blocks) )
            return Stream.<Cid> empty()
        return Files.walk(blocks)
            .filter { Path p -> Files.isRegularFile(p) }
            .map { Path p -> p.fileName.toString() }
            .filter { String name -> Cid.isCid(name) }
            .map { String name -> Cid.parse(name) }
    }

    /**
     * Streams the input into {@code temp} through {@code buffer}, hashing as
     * it goes. Nothing is held: the buffer is the only memory this costs.
     */
    private static byte[] drainTo(InputStream input, Path temp, byte[] buffer, long[] written) {
        final MessageDigest digest = MessageDigest.getInstance('SHA-256')
        final ByteBuffer view = ByteBuffer.wrap(buffer)
        final FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        try {
            int n
            // A read may legally return 0 without being at the end.
            while( (n = input.read(buffer, 0, buffer.length)) != -1 ) {
                if( n == 0 ) continue
                digest.update(buffer, 0, n)
                view.limit(n).position(0)
                while( view.hasRemaining() )
                    channel.write(view)
                written[0] += n
            }
            // fsync the bytes before the block becomes visible under its address
            channel.force(true)
        }
        finally {
            channel.close()
        }
        return digest.digest()
    }

    /**
     * Makes the temp file read-only and links it onto the address. A hard
     * link is created only if nothing is there: unlike a rename, it can never
     * replace a block another writer has just placed, so an existing block
     * keeps its identity and its mtime.
     */
    private static void place(Path temp, Path target) {
        Files.setPosixFilePermissions(temp, READ_ONLY)
        try {
            Files.createLink(target, temp)
        }
        catch( FileAlreadyExistsException e ) {
            // Another writer got there first with the same bytes. That is success.
            if( !Files.isRegularFile(target) )
                throw e
            return
        }
        syncDirectory(target.parent)
    }

    /** Makes the new directory entry itself durable, best effort. */
    private static void syncDirectory(Path directory) {
        try {
            final FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)
            try {
                channel.force(true)
            }
            finally {
                channel.close()
            }
        }
        catch( IOException e ) {
            // Not every filesystem lets a directory be opened and synced; the
            // block itself is already on disk either way.
        }
    }

    private void checkWritable() {
        if( !writable )
            throw new IllegalStateException("store '$alias' is read-only")
    }

    private static String token() {
        final byte[] bytes = new byte[9]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }

    @Override
    String toString() { "LocalBlockStore[$alias at $root]" }
}
