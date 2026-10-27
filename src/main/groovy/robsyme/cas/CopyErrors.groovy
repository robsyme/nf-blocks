package robsyme.cas

import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.CompileStatic
import robsyme.cas.core.Leaks
import robsyme.cas.s3.S3Failures

/**
 * The text of a copy failure, as it may leave the process: in `failures[].error`,
 * the snapshot outcome, a 500 body from the explore server, and on stderr.
 * `<ExceptionClass simple name>: <message>`, except an AWS credentials failure,
 * which is told in plain words (see {@link #describe}). Within it a configured
 * member's root (a local directory, or `s3://bucket/prefix`) becomes
 * `<alias>:`, and any other absolute local path becomes `[local path]`, so a
 * message never says where on the operator's disk the stores, the index cache
 * or the spool directory are. A credential in the message goes as it goes from
 * a RunManifest ({@link Leaks#redactCredentials}).
 */
@CompileStatic
class CopyErrors {

    static final String LOCAL_PATH = '[local path]'

    // A POSIX path starts at a word boundary that is not part of a relative path and runs to whitespace, a
    // quote or a separator. It may follow `word:` (`jdbc:sqlite:/x`, `location:/x`); kept() spares
    // `<alias>:/x` and a scheme's `://`. A drive-letter (`C:\x`, `C:/x`) or UNC path is the Windows form.
    private static final Pattern POSIX = Pattern.compile('(?<![\\w/.\\\\-])/[^\\s\'"`<>,;:)\\]}]+')
    private static final Pattern WINDOWS = Pattern.compile('(?<![\\w])(?:[A-Za-z]:(?:\\\\|/(?!/))|\\\\\\\\)[^\\s\'"`<>,;:)\\]}]*')

    private static final Pattern FILE_URI = Pattern.compile('(?<![\\w-])file:(?://)?(?=/)')
    // The rest of a path under a known directory (home, tmp, working directory): everything to a quote, a
    // comma, a semicolon, a parenthesis, a line end, a ": " (a reason follows) or a following scheme:// URL.
    // Such a path may hold spaces and names, so over-redacting prose after it is the lesser harm.
    private static final String HOME_TAIL = '(?:(?! [A-Za-z][A-Za-z0-9+.-]*://)(?!: )[^\'"`\\r\\n,;()])*'
    private static final String BEFORE = '(?<![\\w/.\\\\-])'

    private CopyErrors() {}

    /** Each configured member's alias and its location: absolute for a local directory, `s3://...` as written. */
    static Map<String, String> rootsOf(Map config) {
        final Map<String, String> roots = new LinkedHashMap<String, String>()
        final Object scope = config?.get(CasConfig.SCHEME)
        final Object stores = scope instanceof Map ? ((Map) scope).get('stores') : null
        if( !(stores instanceof Map) )
            return roots
        for( Object entry : ((Map) stores).entrySet() ) {
            final Map.Entry e = (Map.Entry) entry
            final Object member = e.value
            final Object location = member instanceof Map ? ((Map) member).get('location') : null
            if( location == null || location.toString().isEmpty() )
                continue
            final String text = location.toString()
            // Normalized as CasConfig resolves a location, so `/data/./lab` names /data/lab.
            roots.put(e.key.toString(), text.contains('://') ? text : new File(text).toPath().toAbsolutePath().normalize().toString())
        }
        return roots
    }

    /**
     * `<ExceptionClass>: <message>`, redacted; the bare class name when the
     * exception has no message. A credentials failure is told in plain words
     * instead ({@link S3Failures}), since its message is mostly request ids.
     */
    static String describe(Throwable t, Map<String, String> roots) {
        final String credentials = S3Failures.credentials(t)
        if( credentials != null )
            return "${credentials}. Refresh your AWS credentials, then try again".toString()
        // An anonymous class has an empty simple name.
        final String name = t.getClass().simpleName ?: t.getClass().name
        final String message = t.message
        return message ? "${name}: ${redact(Leaks.redactCredentials(message), roots)}".toString() : name
    }

    /** {@code text} with member roots as aliases, longest root first, then every other absolute local path as [local path]. */
    static String redact(String text, Map<String, String> roots) {
        if( text == null )
            return null
        // file:///p and file:/p are the path p.
        String out = FILE_URI.matcher(text).replaceAll('')
        final List<Map.Entry<String, String>> byLength = new ArrayList<Map.Entry<String, String>>(roots.entrySet())
        byLength.sort { Map.Entry<String, String> a, Map.Entry<String, String> b -> b.value.length() <=> a.value.length() }
        for( Map.Entry<String, String> e : byLength ) {
            String root = e.value
            while( root.length() > 1 && root.endsWith('/') )
                root = root.substring(0, root.length() - 1)
            if( root.length() > 1 )
                out = replaceRoot(out, root, e.key)
        }
        final Set<String> aliases = roots.keySet()
        // A drive path first: `C:/Users/rob` is one path, not `C:` and then a home directory.
        out = replacePaths(WINDOWS, out, aliases, false)
        // The operator's own directories may hold spaces (macOS "Application Support"), which the
        // path pattern would stop at: take them as literal prefixes, with the rest of the path.
        for( String prefix : [System.getProperty('user.home'), System.getProperty('java.io.tmpdir'), System.getProperty('user.dir')] ) {
            String dir = prefix
            while( dir != null && dir.length() > 1 && dir.endsWith('/') )
                dir = dir.substring(0, dir.length() - 1)
            if( dir != null && dir.length() > 1 )
                out = replacePaths(Pattern.compile(BEFORE + Pattern.quote(dir) + '(?![\\w-])' + HOME_TAIL), out, aliases, false)
        }
        return replacePaths(POSIX, out, aliases, true)
    }

    /** {@code root} as a whole path: not after a path or URL character, not before one that continues a name. */
    private static String replaceRoot(String text, String root, String alias) {
        final Pattern p = Pattern.compile('(?<![\\w/.\\\\-])' + Pattern.quote(root) + '(?![\\w-])(?!\\.\\w)')
        return p.matcher(text).replaceAll(Matcher.quoteReplacement(alias + ':'))
    }

    /**
     * Each match of {@code pattern} in {@code text} as [local path], unless {@link #kept}. With
     * {@code sentenceStop}, a full stop that ends the match ends the sentence and stays.
     */
    private static String replacePaths(Pattern pattern, String text, Set<String> aliases, boolean sentenceStop) {
        final Matcher m = pattern.matcher(text)
        final StringBuffer out = new StringBuffer()
        while( m.find() ) {
            String path = m.group()
            if( kept(text, m.start(), path, aliases) ) {
                m.appendReplacement(out, Matcher.quoteReplacement(path))
                continue
            }
            String tail = ''
            while( sentenceStop && path.endsWith('.') ) {
                path = path.substring(0, path.length() - 1)
                tail += '.'
            }
            m.appendReplacement(out, Matcher.quoteReplacement(LOCAL_PATH + tail))
        }
        m.appendTail(out)
        return out.toString()
    }

    /**
     * A match the redaction keeps: the `//...` after a scheme's colon (`s3://b/k`), a path after
     * `<alias>:` (what the member-root replacement wrote), or `<alias>:/...` for a one-letter alias,
     * which the drive-path pattern would otherwise take.
     */
    private static boolean kept(String text, int start, String match, Set<String> aliases) {
        if( match.length() >= 2 && match.charAt(1) == (char) ':' && aliases.contains(match.substring(0, 1)) )
            return true
        if( start == 0 || text.charAt(start - 1) != (char) ':' )
            return false
        if( match.startsWith('//') )
            return true
        int word = start - 1
        while( word > 0 && aliasChar(text.charAt(word - 1)) )
            word--
        return aliases.contains(text.substring(word, start - 1))
    }

    private static boolean aliasChar(char c) {
        return Character.isLetterOrDigit(c) || c == (char) '_' || c == (char) '-'
    }
}
