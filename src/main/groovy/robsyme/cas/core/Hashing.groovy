package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import groovy.transform.CompileStatic

/**
 * SHA-256 content identity. Streams are hashed through one caller-supplied
 * buffer and never accumulated: no method here holds file content
 * (DESIGN.md §0 rule 2, §3).
 */
@CompileStatic
class Hashing {

    /** For metadata blocks, which are small and already in memory. */
    static byte[] sha256(byte[] data) {
        return MessageDigest.getInstance('SHA-256').digest(data)
    }

    /**
     * Hashes the stream to the end using exactly {@code buffer}. The stream
     * is not closed; the caller owns it.
     */
    static Cid hashRaw(InputStream input, byte[] buffer) {
        return Cid.of(Cid.RAW, digestOf(input, buffer))
    }

    /** Hashes a file with a buffer borrowed from the shared pool. */
    static Cid hashRaw(Path file) {
        return HashBufferPool.shared().withBuffer { byte[] buffer ->
            Files.newInputStream(file).withStream { InputStream input ->
                hashRaw(input, buffer)
            } as Cid
        }
    }

    private static byte[] digestOf(InputStream input, byte[] buffer) {
        if( buffer == null || buffer.length == 0 )
            throw new IllegalArgumentException('a hash buffer is required')
        final MessageDigest digest = MessageDigest.getInstance('SHA-256')
        int n
        while( (n = input.read(buffer, 0, buffer.length)) > 0 )
            digest.update(buffer, 0, n)
        return digest.digest()
    }
}
