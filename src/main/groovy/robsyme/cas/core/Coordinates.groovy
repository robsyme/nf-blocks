package robsyme.cas.core

import java.nio.file.Path

import groovy.transform.CompileStatic

/**
 * The join key (DESIGN.md §7).
 *
 * A Publish Coordinate {@code cas://<alias>/<a/b/c>} reaches us three ways --
 * as the string Nextflow logs, as a {@code URI}, and as a {@code Path} -- and
 * the three have to agree, because the publish side and the lineage side each
 * see a different one and have to meet in the same map. This class is the one
 * place that decides what "the same coordinate" means. Never {@code Path.equals}.
 *
 * Segments carry the file name as it is on disk. A name with a space or a
 * {@code #} in it is percent-encoded by {@code java.net.URI} and both forms
 * reach us, so the key always holds the decoded, raw segment.
 */
@CompileStatic
class Coordinates {

    static final String SCHEME = 'cas'

    private static final String PREFIX = SCHEME + '://'

    /**
     * The canonical key for a coordinate written as text. The text is not
     * parsed with {@code java.net.URI}: a raw {@code #} in a file name would
     * become a fragment.
     */
    static String key(String uriOrPath) {
        if( !uriOrPath )
            throw new IllegalArgumentException("not a publish coordinate: ${uriOrPath == null ? 'null' : "'$uriOrPath'"}")
        if( !uriOrPath.startsWith(PREFIX) )
            throw new IllegalArgumentException("not a '$SCHEME' uri: '$uriOrPath'")
        final String rest = uriOrPath.substring(PREFIX.length())
        final int slash = rest.indexOf('/')
        final String authority = slash < 0 ? rest : rest.substring(0, slash)
        final String path = slash < 0 ? '' : rest.substring(slash)
        return canonical(authority, path, uriOrPath)
    }

    /** The canonical key for a coordinate that arrived as a {@code URI}. */
    static String key(URI uri) {
        if( uri == null )
            throw new IllegalArgumentException('not a publish coordinate: null')
        if( !SCHEME.equalsIgnoreCase(uri.scheme ?: '') )
            throw new IllegalArgumentException("not a '$SCHEME' uri: '$uri'")
        // getAuthority()/getPath() decode percent escapes, which is what a
        // file name on disk looks like.
        return canonical(uri.authority, uri.path ?: '', uri.toString())
    }

    /** The canonical key for a coordinate that arrived as a {@code Path}. */
    static String key(Path path) {
        if( path == null )
            throw new IllegalArgumentException('not a publish coordinate: null')
        return key(path.toUri())
    }

    private static String canonical(String authority, String path, String original) {
        if( !authority )
            throw new IllegalArgumentException("publish coordinate has no store alias: '$original'")
        if( Cid.isCid(authority) )
            throw new IllegalArgumentException("'$original' is a Store URI, not a publish coordinate: '$authority' is a content address")
        final List<String> segments = normalize(path)
        return segments.isEmpty()
            ? PREFIX + authority
            : PREFIX + authority + '/' + segments.join('/')
    }

    /**
     * Splits on {@code /}, dropping empty and {@code .} segments and resolving
     * {@code ..}. A coordinate is rooted at its store, so a leading {@code ..}
     * has nowhere to go and is dropped rather than escaping.
     */
    private static List<String> normalize(String path) {
        final List<String> out = new ArrayList<String>()
        for( String segment : path.split('/') ) {
            if( !segment || segment == '.' )
                continue
            if( segment == '..' ) {
                if( !out.isEmpty() )
                    out.remove(out.size() - 1)
                continue
            }
            out.add(segment)
        }
        return out
    }
}
