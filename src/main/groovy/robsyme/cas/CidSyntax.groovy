package robsyme.cas

import java.util.regex.Pattern

import groovy.transform.CompileStatic

/**
 * Syntactic recognition of a CIDv1 text form, used only to tell a store alias
 * apart from a content address in a {@code cas://} authority (DESIGN.md section 7).
 *
 * This is deliberately not a parser: {@code robsyme.cas.core.Cid} owns parsing
 * and validation. The check here answers the one question the config and the
 * path factory need before any core class is on the classpath.
 */
@CompileStatic
class CidSyntax {

    /** CIDv1, multibase base32 lower, sha2-256: 59 characters, `bafk…` raw or `bafy…` dag-cbor. */
    private static final Pattern CIDV1_BASE32 = ~/^baf[ky][a-z2-7]{55}$/

    static boolean looksLikeCid(String text) {
        return text != null && CIDV1_BASE32.matcher(text).matches()
    }
}
