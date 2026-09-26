package robsyme.cas.core

import groovy.transform.CompileStatic

/** A refused request (block explorer spec section 9.4; decision 7 of the milestone 2 plan adds `invalid`). */
@CompileStatic
class PutError extends RuntimeException {

    static final String NOT_FOUND = 'not_found'
    static final String WRONG_KIND = 'wrong_kind'
    static final String NOT_IN_VIA = 'not_in_via'
    static final String EMPTY = 'empty'
    static final String STALE_SUPERSEDES = 'stale_supersedes'
    static final String CLOCK_SKEW = 'clock_skew'
    static final String TOO_LARGE = 'too_large'
    static final String NOT_WRITABLE = 'not_writable'
    static final String INVALID = 'invalid'

    final String code
    /** A JSON pointer into the request; '' for the whole request. */
    final String at

    PutError(String code, String message, String at) {
        super(message)
        this.code = code
        this.at = at
    }

    int getStatus() { code == NOT_WRITABLE ? 409 : 400 }

    byte[] body() {
        return DagJson.encode([error: code, message: message, at: at])
    }
}
