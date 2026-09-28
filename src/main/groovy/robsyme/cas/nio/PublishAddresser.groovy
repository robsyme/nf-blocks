package robsyme.cas.nio

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortRunException
import nextflow.file.FileHelper
import nextflow.extension.FilesEx
import robsyme.cas.core.*
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3Copied
import robsyme.cas.s3.S3UnremovedCopyException

/**
 * The Address Provider seam for a run's publishes (spec §3, ticket 16): the
 * node's digest, S3's SHA-256 from a copy, the head node's read, in that order.
 * A node digest and a computed address must agree, or the run aborts (rule 3).
 */
@Slf4j
@CompileStatic
class PublishAddresser implements FileAddresser {

    private final BlockStore store
    private final BlockStore writable
    private final boolean nodeHash
    private final Path workDir
    private volatile String workDirUri
    private final long singleRequestMax
    private final ConcurrentHashMap<String, Map<String, Cid>> digests = new ConcurrentHashMap<>()
    private final AtomicLong headNodeBytes = new AtomicLong()
    private final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>()

    PublishAddresser(BlockStore store, BlockStore writable, boolean nodeHash, Path workDir,
                     long singleRequestMax = S3BlockStore.SINGLE_REQUEST_MAX) {
        this.store = store; this.writable = writable; this.nodeHash = nodeHash
        this.workDir = workDir
        this.singleRequestMax = singleRequestMax
    }

    long getHeadNodeBytes() { headNodeBytes.get() }

    Map<String, Integer> getCounts() { counts.collectEntries { k, v -> [(k): (int) v.get()] } as Map<String, Integer> }

    String summary() {
        final Map<String, Integer> c = getCounts()
        final int files = (int) c.values().sum(0)
        return "nf-blocks: the head node read ${headNodeBytes.get()} bytes to address ${files} file(s); " +
            Providers.ALL.collect { "${it} ${c.get(it) ?: 0}" }.join(', ')
    }

    @Override
    Addressed address(Path file, long size) {
        final Cid node = nodeHash ? nodeDigest(file) : null
        final List<String> source = copySource(file)
        if( writable instanceof S3BlockStore && source != null ) {
            try {
                final S3Copied c = ((S3BlockStore) writable).copyFrom(source[0], source[1], size, node)
                if( c != null ) return counted(new Addressed(c.cid, size, c.provider))
            }
            catch( S3UnremovedCopyException e ) {
                // Rule 3: unconfirmed bytes sit at a block key a later HEAD would accept; never fall back past them.
                throw new AbortRunException("${file}: ${e.message}", e)
            }
            catch( BlockMismatchException e ) {
                // Ticket 16 decision 3, rule 3: the node's digest and S3's disagree.
                throw new AbortRunException("${file}: .command.cas says ${node}, but S3's SHA-256 of the copy disagrees (${e.message}); the output changed after the task hashed it", e)
            }
            catch( IOException e ) {
                log.warn("server-side copy of ${file} failed (${e.message}); the head node reads it instead")
            }
        }
        // The writable member, not the composite: a block held only by a read-only member must still be written (DESIGN §5).
        if( node != null && writable.has(node) )
            return counted(new Addressed(node, size, Providers.FUSION_NODE))
        final Cid cid = headNodeRead(file, source != null)
        if( node != null && node != cid )
            throw new AbortRunException("${file}: .command.cas says ${node}, the head node hashed ${cid}; the output changed after the task hashed it")
        headNodeBytes.addAndGet(size)
        return counted(new Addressed(cid, size, Providers.HEAD_NODE))
    }

    /** [bucket, key] of an S3 source, or null; a seam for tests. */
    protected List<String> copySource(Path file) {
        final String uri = FilesEx.toUriString(file)
        if( !uri.startsWith('s3://') ) return null
        final int slash = uri.indexOf('/', 5)
        return slash < 0 ? null : [uri.substring(5, slash), uri.substring(slash + 1)]
    }

    /** A local file into an S3 member needs no spool; an object-store source is spooled (rule 2). */
    private Cid headNodeRead(Path file, boolean objectSource) {
        try {
            if( writable instanceof S3BlockStore && !objectSource && file.fileSystem == FileSystems.default )
                return ((S3BlockStore) writable).putFile(file)
            final InputStream in = Files.newInputStream(file)
            try { return writable.putStreaming(in) } finally { in.close() }
        }
        catch( IOException e ) {
            if( e.message?.contains('cas.tmpDir') )
                throw new AbortRunException("${file}: ${e.message}; S3 cannot copy it server-side (over ${singleRequestMax} bytes, no node digest), so it passes through cas.tmpDir, which needs ${Files.size(file)} bytes free", e)
            throw e
        }
    }

    private Addressed counted(Addressed a) {
        counts.computeIfAbsent(a.provider, { new AtomicLong() }).incrementAndGet()
        return a
    }

    private Cid nodeDigest(Path file) {
        if( workDir == null ) return null
        // Compared as real paths: a work dir under a symlink (macOS /var) must still match (pre-flight F40).
        // Resolved on first use, when the work dir exists.
        if( workDirUri == null ) workDirUri = FilesEx.toUriString(realOrAbsolute(workDir))
        final Path real = file.parent == null ? file : realOrAbsolute(file.parent).resolve(file.fileName.toString())
        final NodeDigests.TaskPath tp = NodeDigests.taskDirOf(FilesEx.toUriString(real), workDirUri)
        if( tp == null ) return null
        return digests.computeIfAbsent(tp.taskDir, { String dir -> load(dir) }).get(tp.rel)
    }

    private static Path realOrAbsolute(Path p) {
        try {
            return DirectoryManifestBuilder.realOf(p)
        }
        catch( IOException e ) {
            return p.toAbsolutePath().normalize()
        }
    }

    private Map<String, Cid> load(String taskDir) {
        try {
            final InputStream in = openDigests(FileHelper.asPath(taskDir + '/.command.cas'))
            try {
                final Map<String, Cid> d = NodeDigests.parse(in, NodeDigests.MAX_BYTES)
                if( d == null ) {
                    log.warn("${taskDir}/.command.cas is over ${NodeDigests.MAX_BYTES} bytes and is ignored; the head node addresses that task's outputs")
                    return Collections.<String, Cid> emptyMap()
                }
                if( d.isEmpty() ) log.debug("no usable node digests in ${taskDir}/.command.cas")
                return d
            }
            finally { in.close() }
        }
        catch( NoSuchFileException e ) {
            return Collections.emptyMap()
        }
        catch( IOException e ) {
            log.warn("could not read ${taskDir}/.command.cas (${e.message}); the head node addresses that task's outputs")
            return Collections.emptyMap()
        }
    }

    /** A seam: where .command.cas is opened. */
    protected InputStream openDigests(Path file) { Files.newInputStream(file) }
}
