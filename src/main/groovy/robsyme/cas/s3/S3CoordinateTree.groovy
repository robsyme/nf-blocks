package robsyme.cas.s3

import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException

import groovy.transform.CompileStatic
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.StoreRef

/**
 * coords/ on S3 (ticket 02 decision 6). A pointer is the object coords/<rel>,
 * body `cas://<cid>/<name>\n` as locally; a directory is any key under
 * coords/<rel>/, no marker objects. S3 would hold both a/ and a/b, so both
 * local conflicts are checked before a write: a HEAD per ancestor, and one
 * LIST with max-keys 1. Two writers can still race past the checks; then the
 * ancestor pointer shadows what is under it (ticket 03 decision 4).
 */
@CompileStatic
class S3CoordinateTree implements CoordinateTree {

    private final S3Ops ops
    private final String root

    S3CoordinateTree(S3Ops ops, String prefix) {
        this.ops = ops
        this.root = "${prefix ?: ''}coords/".toString()
    }

    private static List<String> segments(String rel) {
        final List<String> out = []
        for( String s : (rel ?: '').split('/') ) {
            if( !s || s == '.' ) continue
            if( s == '..' ) {
                if( out.isEmpty() ) throw new IllegalArgumentException("coordinate path escapes the coordinate tree: '$rel'")
                out.remove(out.size() - 1)
                continue
            }
            out.add(s)
        }
        return out
    }

    private String key(String rel) { root + segments(rel).join('/') }

    private boolean hasChildren(String rel) { !ops.list(key(rel) + '/', 1).isEmpty() }

    /** The first ancestor of rel that is itself a pointer, or null. */
    private String ancestorPointer(String rel) {
        final List<String> segs = segments(rel)
        for( int i = 1; i < segs.size(); i++ ) {
            final String up = segs.subList(0, i).join('/')
            if( ops.head(root + up) != null ) return up
        }
        return null
    }

    @Override
    Optional<StoreRef> read(String rel) {
        final InputStream in = ops.get(key(rel), null, 0L, -1L)
        if( in == null ) return Optional.empty()
        final String text = in.withCloseable { it.getText('UTF-8') }
        if( ancestorPointer(rel) != null ) return Optional.empty()     // shadowed
        try {
            return Optional.of(StoreRef.parse(text.trim()))
        }
        catch( IllegalArgumentException e ) {
            throw new IOException("pointer file for '${segments(rel).join('/')}' does not hold a store uri: ${e.message}", e)
        }
    }

    @Override
    void write(String rel, StoreRef ref) {
        if( ref == null ) throw new IllegalArgumentException("no store reference for coordinate '${rel}'")
        if( segments(rel).isEmpty() ) throw new IllegalArgumentException('the root of the coordinate tree is not a coordinate')
        final String blocking = ancestorPointer(rel)
        if( blocking != null ) throw new FileAlreadyExistsException(blocking, rel, 'a Pointer File is there')
        if( hasChildren(rel) ) throw new DirectoryNotEmptyException(segments(rel).join('/'))
        ops.put(key(rel), S3Body.ofBytes((ref.toString() + '\n').getBytes('UTF-8')), S3PutOptions.create().contentType('text/plain; charset=utf-8'))
    }

    @Override boolean exists(String rel) { ops.head(key(rel)) != null || hasChildren(rel) }

    @Override boolean isDirectory(String rel) { segments(rel).isEmpty() || (ops.head(key(rel)) == null && hasChildren(rel)) }

    @Override
    boolean isDirectoryCoordinate(String rel) {
        if( isDirectory(rel) ) return true
        return read(rel).map { StoreRef r -> r.isDirectory() }.orElse(false)
    }

    @Override
    List<String> children(String rel) {
        final String under = segments(rel).isEmpty() ? root : key(rel) + '/'
        final TreeSet<String> names = new TreeSet<String>()
        for( S3Listed o : ops.list(under, 0) ) {
            final String rest = o.key.substring(under.length())
            final String first = rest.contains('/') ? rest.substring(0, rest.indexOf('/')) : rest
            if( first && !first.startsWith('.tmp-') ) names.add(first)
        }
        return new ArrayList<String>(names)
    }

    @Override
    boolean delete(String rel) {
        if( ops.head(key(rel)) == null ) {
            if( hasChildren(rel) ) throw new IOException("'${segments(rel).join('/')}' is a coordinate directory, not a pointer file")
            return false
        }
        ops.delete(key(rel))
        return true
    }

    @Override void createDirectories(String rel) { }   // S3 has no directories to make

    @Override long lastModifiedMillis(String rel) { ops.head(key(rel))?.lastModifiedMillis ?: 0L }

    /** Pointers under another pointer, in key order, at most limit: what explore warns about (silent decision 20). */
    List<String> shadowedPointers(int limit) {
        final List<String> pointers = ops.list(root, 0)*.key.collect { String k -> k.substring(root.length()) }
        final Set<String> all = new HashSet<String>(pointers)
        final List<String> out = []
        for( String p : pointers ) {
            final List<String> segs = p.split('/') as List<String>
            for( int i = 1; i < segs.size(); i++ )
                if( all.contains(segs.subList(0, i).join('/')) ) { out.add(p); break }
            if( out.size() >= limit ) break
        }
        return out
    }
}
