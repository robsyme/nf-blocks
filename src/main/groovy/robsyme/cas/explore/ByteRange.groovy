package robsyme.cas.explore

import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.CompileStatic

/**
 * One `Range: bytes=` range (RFC 9110 §14). Anything else, several ranges
 * included, is ignored and the whole file is served, which the RFC allows;
 * the page only ever asks for one range at a time.
 */
@CompileStatic
final class ByteRange {

    private static final Pattern SINGLE = ~/^bytes=(\d*)-(\d*)$/

    /** Thrown for a range that starts at or past the end: 416. */
    static class Unsatisfiable extends Exception {
        Unsatisfiable() { super('range not satisfiable') }
    }

    final long start
    /** Inclusive. */
    final long end

    private ByteRange(long start, long end) {
        this.start = start
        this.end = end
    }

    long getLength() { end - start + 1 }

    static ByteRange parse(String header, long size) throws Unsatisfiable {
        if( !header )
            return null
        final Matcher m = SINGLE.matcher(header.trim())
        if( !m.matches() )
            return null
        final String first = m.group(1)
        final String last = m.group(2)
        if( !first && !last )
            return null
        try {
            if( !first ) {
                final long suffix = Long.parseLong(last)
                if( suffix == 0 )
                    throw new Unsatisfiable()
                return new ByteRange(Math.max(0L, size - suffix), size - 1)
            }
            final long start = Long.parseLong(first)
            if( start >= size )
                throw new Unsatisfiable()
            final long end = last ? Math.min(Long.parseLong(last), size - 1) : size - 1
            return end < start ? null : new ByteRange(start, end)
        }
        catch( NumberFormatException e ) {
            return null
        }
    }
}
