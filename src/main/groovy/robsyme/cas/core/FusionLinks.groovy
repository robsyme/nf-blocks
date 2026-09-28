package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Fusion's link encoding in an object store (ticket 06 §6, ticket 15). Read
 * because it is part of a directory's content; no other Fusion internal is.
 */
@CompileStatic
class FusionLinks {

    static final String SIDECAR = '.fusion.symlinks'
    static final int MAX_SIDECAR_BYTES = 1 << 20
    static final int MAX_TARGET_BYTES = 4096

    @Canonical
    static class Parsed {
        boolean ok
        Set<String> names
        String problem
    }

    static Parsed parse(byte[] body) {
        if( body.length > MAX_SIDECAR_BYTES )
            return bad("the listing is over ${MAX_SIDECAR_BYTES} bytes")
        final String text = utf8(body)
        if( text == null )
            return bad('the listing is not UTF-8')
        if( text.indexOf('\u0000') >= 0 )
            return bad('the listing holds NUL')
        final String trimmed = text.endsWith('\n') ? text.substring(0, text.length() - 1) : text
        final Set<String> names = new LinkedHashSet<String>()
        if( trimmed.isEmpty() )
            return new Parsed(true, names, null)
        for( String name : trimmed.split('\n', -1) ) {
            if( name.isEmpty() )
                return bad('the listing has an empty name')
            if( name.contains('/') || name == '.' || name == '..' )
                return bad("'${name}' is not one path segment")
            names.add(name)
        }
        return new Parsed(true, names, null)
    }

    /** A link object's body as a target a manifest may hold, or null. */
    static String target(byte[] body) {
        if( body.length == 0 || body.length > MAX_TARGET_BYTES )
            return null
        final String text = utf8(body)
        return text == null || text.indexOf('\u0000') >= 0 ? null : text
    }

    private static String utf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString()
        }
        catch( CharacterCodingException e ) {
            return null
        }
    }

    private static Parsed bad(String problem) { new Parsed(false, Collections.<String> emptySet(), problem) }
}
