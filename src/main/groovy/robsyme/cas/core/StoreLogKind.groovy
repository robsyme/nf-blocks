package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a Store Log entry announces (block explorer spec section 3). */
@CompileStatic
enum StoreLogKind {
    RUN('run'), SELECTION('selection'), CLAIM('claim')

    final String token

    StoreLogKind(String token) { this.token = token }

    /** The kind for a token, or null when the token is not one we know. */
    static StoreLogKind fromToken(String token) {
        for( StoreLogKind kind : values() )
            if( kind.token == token )
                return kind
        return null
    }
}
