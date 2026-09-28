package robsyme.cas.core

import java.nio.charset.StandardCharsets

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** The task node's .command.cas (spec §14, silent decision 7). */
@CompileStatic
class NodeDigests {

    static final long MAX_BYTES = 16L << 20
    private static final java.util.regex.Pattern LINE = ~/^(\\?)([0-9a-f]{64}) [ *](.*)$/

    @Canonical
    static class TaskPath { String taskDir; String rel }

    /**
     * rel path to raw CID; unparseable lines skipped. Read line by line, counting
     * bytes, so the file is never held whole (rule 2's spirit for metadata);
     * null once more than maxBytes have arrived: the caller warns and ignores it.
     */
    static Map<String, Cid> parse(InputStream input, long maxBytes) {
        final long[] seen = [0L] as long[]
        final InputStream counted = new FilterInputStream(input) {
            @Override int read() { final int b = super.read(); if( b >= 0 ) seen[0]++; return b }
            @Override int read(byte[] b, int off, int len) { final int n = super.read(b, off, len); if( n > 0 ) seen[0] += n; return n }
        }
        final BufferedReader reader = new BufferedReader(new InputStreamReader(counted, StandardCharsets.UTF_8))
        final Map<String, Cid> out = new HashMap<String, Cid>()
        String line
        while( (line = reader.readLine()) != null ) {
            if( seen[0] > maxBytes ) return null
            final java.util.regex.Matcher m = LINE.matcher(line)
            if( !m.matches() ) continue
            final String name = m.group(1) ? unescape(m.group(3)) : m.group(3)
            if( name ) out.put(name, Cid.of(Cid.RAW, m.group(2).decodeHex()))
        }
        return seen[0] > maxBytes ? null : out
    }

    /** coreutils' escaping in one left-to-right pass: a backslash then n is a newline, two backslashes one backslash. */
    private static String unescape(String s) {
        final StringBuilder out = new StringBuilder(s.length())
        for( int i = 0; i < s.length(); i++ ) {
            final char c = s.charAt(i)
            if( c == (char) '\\' && i + 1 < s.length() ) {
                final char next = s.charAt(++i)
                out.append(next == (char) 'n' ? (char) '\n' : next)
            }
            else {
                out.append(c)
            }
        }
        return out.toString()
    }

    /** The task directory and relative path of a source under workDir, or null. */
    static TaskPath taskDirOf(String sourceUri, String workDirUri) {
        final String base = workDirUri.replaceAll('/+$', '') + '/'
        if( !sourceUri.startsWith(base) ) return null
        final String[] segs = sourceUri.substring(base.length()).split('/', 3)
        if( segs.length < 3 || !segs[2] ) return null
        return new TaskPath(base + segs[0] + '/' + segs[1], segs[2])
    }
}
