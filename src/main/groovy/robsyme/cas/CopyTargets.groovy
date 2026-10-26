package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.CompileStatic

/**
 * Which members a copy may write (plan decision D1). Each member is judged
 * alone; the source member's exclusion belongs to the request.
 */
@CompileStatic
class CopyTargets {

    static final String READ_ONLY = 'readOnly is set for this store'
    static final String NOT_WRITABLE = 'this process can\'t write the directory'

    @CompileStatic
    static class Judgement {
        final boolean target
        /** Why the member is not a target; null when it is. */
        final String reason

        Judgement(boolean target, String reason) {
            this.target = target
            this.reason = reason
        }
    }

    static Judgement judge(CasConfig config, String alias) {
        if( config.readOnly(alias) )
            return new Judgement(false, READ_ONLY)
        // S3 has no cheap write probe, so a bucket is always offered; a denied write fails the copy itself.
        if( config.isRemote(alias) )
            return new Judgement(true, null)
        return writable(config.locationOf(alias))
            ? new Judgement(true, null)
            : new Judgement(false, NOT_WRITABLE)
    }

    /** The directory when it exists, else its nearest existing ancestor, must be writable. */
    private static boolean writable(Path dir) {
        Path p = dir
        while( p != null && !Files.exists(p) )
            p = p.parent
        return p != null && Files.isDirectory(p) && Files.isWritable(p)
    }
}
