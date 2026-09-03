package robsyme.cas.core

import java.nio.file.Path

import groovy.transform.CompileStatic

/**
 * Where the index file lives (DESIGN.md §12): `cas.index.path` when the user
 * sets one, otherwise a per-user cache file named by the store's member
 * locations, so two people indexing one store each keep their own cache and
 * need no coordination. Never inside the store, because SQLite must not sit
 * on a network filesystem.
 */
@CompileStatic
class IndexPaths {

    private static final String DIRECTORY = 'nf-blocks'
    private static final String SUFFIX = '.sqlite'
    /** Enough to separate the stores one user has, short enough to type. */
    private static final int NAME_HEX_CHARS = 16

    /** The cache path for a store, reading the real environment. */
    static Path cachePath(List<String> memberLocations, String override) {
        return cachePath(memberLocations, override, System.getenv())
    }

    static Path cachePath(List<String> memberLocations, String override, Map<String, String> env) {
        if( override )
            return Path.of(override)
        return cacheRoot(env).resolve(DIRECTORY).resolve(nameFor(memberLocations))
    }

    /** The file name: the first 16 hex of the sha256 of the locations joined by newline. */
    static String nameFor(List<String> memberLocations) {
        final byte[] digest = Hashing.sha256((memberLocations ?: []).join('\n').getBytes('UTF-8'))
        final StringBuilder hex = new StringBuilder(NAME_HEX_CHARS)
        for( int i = 0; i < NAME_HEX_CHARS / 2; i++ )
            hex.append(Integer.toHexString((digest[i] & 0xff) | 0x100).substring(1))
        return hex.toString() + SUFFIX
    }

    private static Path cacheRoot(Map<String, String> env) {
        final String xdg = env?.get('XDG_CACHE_HOME')
        if( xdg )
            return Path.of(xdg)
        final String home = env?.get('HOME')
        if( home )
            return Path.of(home).resolve('.cache')
        throw new IllegalStateException('cannot place the index: neither XDG_CACHE_HOME nor HOME is set, and cas.index.path was not given')
    }
}
