package robsyme.cas.core

import java.util.regex.Matcher

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * Looks for what must not travel between stores in a RunManifest block: an
 * absolute path, a URI other than `cas://` and `lid://`, the OS user name, and
 * credentials (P3 plan section 1.6, DESIGN.md §6). {@link Records} removes these
 * when the block is written; this reads the block as stored, with the same
 * classifier ({@link Leaks}), so a block written by an older version, or by a
 * scrub with a gap, is caught.
 *
 * It covers `params`, recursively, and `config`: line by line and token by
 * token when it is text, like `params` when it is a Map (a block from before
 * 2026-09-27). Strings are judged token by token in both, and a value under a
 * secret-named key must be the secret marker. A map key that holds a credential,
 * a location or the user name is a finding of its own, named by the key the scrub
 * would write (`params.files.[redacted-location]`). Pure: no I/O and no Nextflow types.
 */
@CompileStatic
final class RunManifestAudit {

    /** One thing the audit found: `where` is `params.a.b[2]`, `config.a.b` (Map config) or `config:<line>`. */
    @CompileStatic
    @EqualsAndHashCode
    @ToString(includePackage = false, includeNames = true)
    static final class Finding {
        final Cid cid
        final String kind = 'RunManifest'
        final String where
        final String what

        Finding(Cid cid, String where, String what) {
            this.cid = cid
            this.where = where
            this.what = what
        }

        Map<String, Object> toMap() {
            final Map<String, Object> map = new LinkedHashMap<String, Object>()
            map.put('cid', cid?.toString())
            map.put('kind', kind)
            map.put('where', where)
            map.put('what', what)
            return map
        }
    }

    /** Every finding in the block's `params` and `config`, in the order they are found. Never throws on a missing field. */
    static List<Finding> audit(Cid cid, Map manifestBlock) {
        final List<Finding> out = new ArrayList<Finding>()
        if( manifestBlock == null )
            return out
        final String user = System.getProperty('user.name')
        walk(cid, 'params', manifestBlock.get('params'), user, out)
        final Object config = manifestBlock.get('config')
        if( config instanceof CharSequence )
            auditConfigText(cid, config.toString(), user, out)
        else
            walk(cid, 'config', config, user, out)
        return out
    }

    // ---- params, and a config that is a Map

    private static void walk(Cid cid, String where, Object value, String user, List<Finding> out) {
        if( value instanceof Map ) {
            // A key that is a location is named as the scrub writes it, so the finding does not repeat it.
            final Map<Object, Object> names = Leaks.keyNames(((Map<?, ?>) value).keySet())
            for( Map.Entry entry : ((Map<?, ?>) value).entrySet() ) {
                final String key = String.valueOf(entry.key)
                final Object v = entry.value
                final String here = where + '.' + String.valueOf(names.get(entry.key))
                // A key is judged as a string is: a credential, a location or the user name in it.
                final Set<String> keyWhats = new LinkedHashSet<String>()
                for( Leaks.Hit hit : textHits(key, user) )
                    keyWhats.add(hit.what)
                for( String what : keyWhats )
                    out.add(new Finding(cid, here, what))
                if( Records.isSecretKey(key) )
                    secrets(cid, here, key, v, Leaks.isSeqeraTokenKey(key) ? Leaks.SEQERA_TOKEN : Leaks.SECRET_ASSIGNMENT, out)
                walk(cid, here, v, user, out)
            }
        }
        else if( value instanceof Collection ) {
            int i = 0
            for( Object item : (Collection) value )
                walk(cid, where + '[' + (i++) + ']', item, user, out)
        }
        else if( value instanceof CharSequence ) {
            final Set<String> whats = new LinkedHashSet<String>()
            for( Leaks.Hit hit : textHits(value.toString(), user) )
                whats.add(hit.what)
            for( String what : whats )
                out.add(new Finding(cid, where, what))
        }
    }

    /** A value under a secret-named key, or each element of a list under one, that should have been the secret marker. */
    private static void secrets(Cid cid, String where, String key, Object value, String what, List<Finding> out) {
        if( isSecretValue(key, value) )
            out.add(new Finding(cid, where, what))
        else if( value instanceof Collection ) {
            int i = 0
            for( Object item : (Collection) value )
                secrets(cid, where + '[' + (i++) + ']', key, item, what, out)
        }
    }

    /**
     * A value that should have been the secret marker: a non-empty string the key does not keep
     * ({@link Leaks#keepsValue}: a plain boolean or number, a marker, a location, a nucleotide word under `pat`).
     */
    private static boolean isSecretValue(String key, Object value) {
        return ((value instanceof CharSequence && value.length() > 0) || value instanceof Number) && !Leaks.keepsValue(key, value)
    }

    // ---- text

    /**
     * Secrets first, over the whole text, as the scrub redacts them
     * ({@link Leaks#maskCredentials}: a PEM block, a secret-named flag or JSON
     * key, the token shapes), each replaced by the marker with its line breaks
     * kept so a PEM body is not read as paths; then every line token by token.
     */
    private static List<Leaks.Hit> textHits(String text, String user) {
        final List<Leaks.Hit> hits = new ArrayList<Leaks.Hit>()
        final String body = Leaks.maskCredentials(text, hits)
        // A header with no footer is still a key that was cut short.
        final Matcher header = Leaks.PEM_HEADER.matcher(body)
        while( header.find() )
            hits.add(new Leaks.Hit(lineOf(body, header.start()), Leaks.PRIVATE_KEY))
        final String[] lines = body.split('\n', -1)
        for( int i = 0; i < lines.length; i++ ) {
            final List<String> kinds = new ArrayList<String>()
            for( String token : lines[i].split('\\s+') )
                Leaks.scanToken(token, user, kinds)
            for( String kind : kinds )
                hits.add(new Leaks.Hit(i + 1, kind))
        }
        return hits
    }

    private static void auditConfigText(Cid cid, String text, String user, List<Finding> out) {
        final Set<String> seen = new LinkedHashSet<String>()
        for( Leaks.Hit hit : textHits(text, user) )
            add(cid, hit.line, hit.what, seen, out)
        // Assignments run on the whole text because a value may follow its key on the next line.
        for( java.util.regex.MatchResult m : Records.secretAssignments(text) ) {
            add(cid, lineOf(text, m.start(1)), Leaks.isSeqeraTokenKey(m.group(1)) ? Leaks.SEQERA_TOKEN : Leaks.SECRET_ASSIGNMENT, seen, out)
        }
        // Each element of a list under a secret-named key, as params is judged.
        for( int[] at : Records.secretListLiterals(text) ) {
            if( unquote(text.substring(at[0], at[1])).isEmpty() )
                continue
            final String key = text.substring(at[2], at[3])
            add(cid, lineOf(text, at[0]), Leaks.isSeqeraTokenKey(key) ? Leaks.SEQERA_TOKEN : Leaks.SECRET_ASSIGNMENT, seen, out)
        }
    }

    private static void add(Cid cid, int line, String what, Set<String> seen, List<Finding> out) {
        final String where = 'config:' + line
        if( seen.add(where + '|' + what) )
            out.add(new Finding(cid, where, what))
    }

    private static int lineOf(String text, int offset) {
        return Leaks.lineOf(text, offset)
    }

    private static String unquote(String value) {
        if( value.length() >= 2 ) {
            final char first = value.charAt(0)
            if( (first == (char) '\'' || first == (char) '"') && value.charAt(value.length() - 1) == first )
                return value.substring(1, value.length() - 1)
        }
        return value
    }
}
