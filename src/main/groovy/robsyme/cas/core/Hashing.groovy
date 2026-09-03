package robsyme.cas.core

import java.security.MessageDigest

import groovy.transform.CompileStatic

/** SHA-256 over bytes and over streams. DESIGN.md §3. */
@CompileStatic
class Hashing {

    static byte[] sha256(byte[] data) {
        return MessageDigest.getInstance('SHA-256').digest(data)
    }
}
