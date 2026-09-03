package robsyme.cas.nio

import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

import groovy.transform.CompileStatic
import robsyme.cas.CidSyntax

/**
 * A path in the `cas` scheme: an authority plus a normalised segment list
 * (DESIGN.md section 8).
 *
 * The authority is either a store alias, in which case the path is a Publish
 * Coordinate, or a content address, in which case it is a Store URI. An
 * absolute path stringifies as its canonical URI `cas://<authority>/<a/b/c>`,
 * which is what Nextflow logs and records. A path with no authority is
 * relative -- what {@code getFileName()} and {@code relativize()} produce --
 * and stringifies as its segments alone.
 */
@CompileStatic
class CasPath implements Path {

    static final String SCHEME = 'cas'

    private final CasFileSystem fs

    private final String authority

    private final List<String> segments

    CasPath(CasFileSystem fs, String authority, List<String> segments) {
        this.fs = fs
        this.authority = authority
        this.segments = Collections.unmodifiableList(normalize0(segments, authority != null))
    }

    static List<String> split(String path) {
        final List<String> result = new ArrayList<String>()
        for( String s : (path ?: '').split('/') ) {
            if( s )
                result.add(s)
        }
        return result
    }

    /** Resolves `.` and `..`; an absolute path cannot escape its root. */
    private static List<String> normalize0(List<String> input, boolean absolute) {
        final List<String> out = new ArrayList<String>()
        for( String s : input ) {
            if( !s || s == '.' )
                continue
            if( s == '..' ) {
                if( out && out.last() != '..' )
                    out.removeLast()
                else if( !absolute )
                    out.add(s)
                continue
            }
            out.add(s)
        }
        return out
    }

    String getAuthority() { authority }

    List<String> getSegments() { segments }

    /** A Publish Coordinate, i.e. an authority that is a store alias rather than a content address. */
    boolean isCoordinate() {
        return authority != null && !CidSyntax.looksLikeCid(authority)
    }

    /** A location-free Store URI, i.e. an authority that is a content address. */
    boolean isStoreUri() {
        return authority != null && CidSyntax.looksLikeCid(authority)
    }

    private CasPath with(String auth, List<String> segs) {
        return new CasPath(fs, auth, segs)
    }

    private static List<String> segmentsOf(Path other) {
        return other instanceof CasPath
            ? ((CasPath)other).segments
            : split(other.toString())
    }

    @Override FileSystem getFileSystem() { fs }

    @Override boolean isAbsolute() { authority != null }

    @Override
    Path getRoot() {
        return authority != null ? with(authority, Collections.<String>emptyList()) : null
    }

    @Override
    Path getFileName() {
        return segments ? with(null, [segments.last()]) : null
    }

    @Override
    Path getParent() {
        if( segments.size() > 1 )
            return with(authority, segments.subList(0, segments.size()-1))
        if( segments.size() == 1 )
            return authority != null ? with(authority, Collections.<String>emptyList()) : null
        return null
    }

    @Override int getNameCount() { segments.size() }

    @Override
    Path getName(int index) {
        return with(null, [segments.get(index)])
    }

    @Override
    Path subpath(int begin, int end) {
        return with(null, segments.subList(begin, end))
    }

    @Override
    boolean startsWith(Path other) {
        if( !(other instanceof CasPath) )
            return false
        final o = (CasPath)other
        if( o.authority != authority )
            return false
        if( o.segments.size() > segments.size() )
            return false
        return segments.subList(0, o.segments.size()) == o.segments
    }

    @Override
    boolean startsWith(String other) {
        return startsWith(fs.getPath(other))
    }

    @Override
    boolean endsWith(Path other) {
        final o = segmentsOf(other)
        if( o.size() > segments.size() )
            return false
        return segments.subList(segments.size()-o.size(), segments.size()) == o
    }

    @Override
    boolean endsWith(String other) {
        return endsWith(with(null, split(other)))
    }

    @Override Path normalize() { this }

    @Override
    Path resolve(Path other) {
        if( other instanceof CasPath && ((CasPath)other).authority != null )
            return other
        final List<String> joined = new ArrayList<String>(segments)
        joined.addAll(segmentsOf(other))
        return with(authority, joined)
    }

    @Override
    Path resolve(String other) {
        return resolve(with(null, split(other)))
    }

    @Override
    Path resolveSibling(Path other) {
        final parent = getParent()
        return parent != null ? parent.resolve(other) : other
    }

    @Override
    Path resolveSibling(String other) {
        return resolveSibling(with(null, split(other)))
    }

    @Override
    Path relativize(Path other) {
        final o = other instanceof CasPath ? (CasPath)other : with(authority, split(other.toString()))
        if( o.authority != authority )
            throw new IllegalArgumentException("Cannot relativize '${other}' against '${this}'")
        int common = 0
        while( common < segments.size() && common < o.segments.size() && segments[common] == o.segments[common] )
            common++
        final List<String> out = new ArrayList<String>()
        for( int i = common; i < segments.size(); i++ )
            out.add('..')
        out.addAll(o.segments.subList(common, o.segments.size()))
        return with(null, out)
    }

    @Override
    URI toUri() {
        try {
            return authority != null
                ? new URI(SCHEME, authority, segments ? '/' + segments.join('/') : null, null, null)
                : new URI(null, null, segments.join('/'), null)
        }
        catch( URISyntaxException e ) {
            throw new IllegalStateException("Cannot render path as a URI: ${toString()}", e)
        }
    }

    @Override
    Path toAbsolutePath() {
        return this
    }

    @Override
    Path toRealPath(LinkOption... options) {
        return this
    }

    @Override
    File toFile() {
        throw new UnsupportedOperationException("A cas:// path has no File representation: ${toString()}")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) {
        throw new UnsupportedOperationException()
    }

    @Override
    Iterator<Path> iterator() {
        final List<Path> out = new ArrayList<Path>(segments.size())
        for( int i = 0; i < segments.size(); i++ )
            out.add(getName(i))
        return out.iterator()
    }

    @Override
    int compareTo(Path other) {
        return toString() <=> other.toString()
    }

    @Override
    boolean equals(Object other) {
        if( !(other instanceof CasPath) )
            return false
        final o = (CasPath)other
        return o.authority == authority && o.segments == segments
    }

    @Override
    int hashCode() {
        return Objects.hash(authority, segments)
    }

    @Override
    String toString() {
        if( authority == null )
            return segments.join('/')
        return segments
            ? "${SCHEME}://${authority}/${segments.join('/')}".toString()
            : "${SCHEME}://${authority}".toString()
    }
}
