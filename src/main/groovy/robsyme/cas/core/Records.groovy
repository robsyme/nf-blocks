package robsyme.cas.core

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * The block kinds of DESIGN.md §6, and the two functions that every kind
 * shares: the portability scrub and the kind tag.
 *
 * Each kind is a small immutable class with {@code Map toCbor()} and
 * {@code static X fromCbor(Map)}. The map is the contract, not the class:
 * a record's address is the address of {@code DagCbor.encode(record.toCbor())},
 * so a decoded record must re-encode to the bytes it came from.
 */
@CompileStatic
class Records {

    /** The schema a kind is written at unless it says otherwise (DESIGN.md §6). */
    static final int SCHEMA = 1

    static final String DIRECTORY_MANIFEST = 'DirectoryManifest'
    static final String OUTPUT_ITEM = 'OutputItem'
    static final String OUTPUT_COLLECTION = 'OutputCollection'
    static final String RUN_MANIFEST = 'RunManifest'
    static final String RUN_COMPLETION = 'RunCompletion'
    static final String LEAF = 'Leaf'
    static final String CLAIM = 'Claim'
    static final String SELECTION = 'Selection'
    static final String FORGET = 'Forget'
    static final String TRANSFER = 'Transfer'
    static final String INPUT_SET = 'InputSet'
    static final String ITEM_LINEAGE = 'ItemLineage'
    static final String PROJECT = 'Project'
    static final String HANDOFF = 'Handoff'

    static final String REDACTED_LOCATION = '[redacted-location]'
    static final String REDACTED_USER = '[redacted-user]'
    static final String REDACTED_SECRET = '[secret]'

    /** The kind tag of a decoded block, or null when there is none. */
    static String kindOf(Map block) {
        final Object kind = block?.get('kind')
        return kind instanceof String ? (String) kind : null
    }

    /**
     * Config scopes that describe the machine rather than the run, dropped
     * from `params` and `config` before either reaches a block (DESIGN.md §6).
     * Measured at v26.04.6: `session.config` carries
     * `cas.stores.<alias>.location`, an absolute host path.
     */
    private static final Set<String> DROPPED_SCOPES = [
        'cas', 'lineage', 'workDir', 'outputDir', 'launchDir', 'projectDir',
        'homeDir', 'configFiles', 'scriptFile', 'commandLine', 'runName', 'resume',
    ] as Set

    /**
     * The portability scrub of DESIGN.md §6. Drops the store-local top-level
     * scopes, turns Paths into strings, and replaces anything that names a
     * place on some particular machine, or the person who ran the pipeline,
     * with a fixed marker. Returns a new structure; the input is untouched.
     *
     * Idempotent, so a record may safely scrub what it is handed without
     * knowing whether the caller already did.
     */
    static Object scrub(Object value) {
        if( !(value instanceof Map) )
            return scrubValue(value)
        final Map<Object, Object> kept = new LinkedHashMap<Object, Object>()
        for( Map.Entry entry : ((Map<?, ?>) value).entrySet() ) {
            if( !DROPPED_SCOPES.contains(String.valueOf(entry.key)) )
                kept.put(entry.key, entry.value)
        }
        return scrubMap(kept)
    }

    /**
     * Each entry scrubbed by its own key and value, and written under the key
     * {@link Leaks#keyNames} gives it: a key the scrub would change, such as
     * `['/home/x/a.fq': 'a']`, becomes the marker for what it held (numbered when two collide).
     */
    private static Map<Object, Object> scrubMap(Map<?, ?> map) {
        final Map<Object, Object> names = Leaks.keyNames(map.keySet())
        final Map<Object, Object> out = new LinkedHashMap<Object, Object>()
        for( Map.Entry entry : map.entrySet() )
            out.put(names.get(entry.key), scrubEntry(entry.key, names.get(entry.key), entry.value))
        return out
    }

    /**
     * A value under a key that names a secret (`github_token`, `db_password`) is
     * replaced by the secret marker, as config text does, and so is each
     * element of a list under one (`tokens = ['a', 'b']`). A value the key keeps
     * ({@link Leaks#keepsValue}: a plain boolean or number, a location, a marker,
     * a nucleotide word under `pat`), an empty string and a map are left to their
     * own contents. A key renamed to `[secret]` names a secret too, so a second
     * scrub changes nothing.
     */
    private static Object scrubEntry(Object key, Object name, Object value) {
        return isSecretKey(String.valueOf(key)) || isSecretKey(String.valueOf(name)) ? scrubSecret(String.valueOf(key), value) : scrubValue(value)
    }

    private static Object scrubSecret(String key, Object value) {
        final Object v = value instanceof Path ? value.toString() : value
        if( Leaks.keepsValue(key, v) )
            return scrubValue(v)
        if( (v instanceof CharSequence && v.length() > 0) || v instanceof Number )
            return REDACTED_SECRET
        if( v instanceof Collection || v instanceof Object[] ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object item : (v instanceof Collection ? (Collection) v : Arrays.asList((Object[]) v)) )
                out.add(scrubSecret(key, item))
            return out
        }
        return scrubValue(v)
    }

    private static Object scrubValue(Object value) {
        if( value instanceof Path )
            return scrubString(value.toString())
        if( value instanceof CharSequence )
            return scrubString(value.toString())
        if( value instanceof Map )
            return scrubMap((Map<?, ?>) value)
        if( value instanceof Collection ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object item : (Collection) value )
                out.add(scrubValue(item))
            return out
        }
        if( value instanceof Object[] ) {
            final List<Object> out = new ArrayList<Object>()
            for( Object item : (Object[]) value )
                out.add(scrubValue(item))
            return out
        }
        if( value == null || value instanceof Boolean || value instanceof Number
                || value instanceof Cid || value instanceof byte[] )
            return value
        // Anything else dag-cbor cannot encode (a MemoryUnit, a Duration, a
        // Closure) is recorded as its text.
        return scrubString(value.toString())
    }

    /**
     * A params string. One that is wholly a location ({@link Leaks#isLocation}:
     * a path, a home, Windows or UNC path, or a non-portable URI) goes entirely
     * (it may hold spaces); anything else is judged token by token, so
     * `--ref /data/g.fa` keeps its flag.
     */
    private static String scrubString(String text) {
        final String credentialFree = Leaks.redactCredentials(text)
        if( Leaks.isLocation(credentialFree) )
            return REDACTED_LOCATION
        return scrubText(credentialFree)
    }

    /**
     * Free-text scrub for a human message such as a failed run's error report,
     * which unlike a config value can carry an absolute path or the user name
     * embedded mid-sentence (`Failed to publish /work/x for rob`). Redacts token
     * by token so the message keeps its shape without the machine-local parts,
     * which a portable, `asserted_by`-free block must not carry (DESIGN.md §6).
     * Returns null unchanged.
     */
    static String scrubText(String text) {
        if( text == null )
            return null
        text = Leaks.redactCredentials(text)
        final String user = System.getProperty('user.name')
        final StringBuilder out = new StringBuilder(text.length())
        final int n = text.length()
        int i = 0
        while( i < n ) {
            final int start = i
            while( i < n && !Character.isWhitespace(text.charAt(i)) )
                i++
            if( i > start )
                out.append(Leaks.scanToken(text.substring(start, i), user, null))
            while( i < n && Character.isWhitespace(text.charAt(i)) ) {
                out.append(text.charAt(i))
                i++
            }
        }
        return out.toString()
    }

    /** Punctuation, then a key and a separator: where a bare value's `,` or `;` ends it. */
    private static final String KEY_AHEAD = /[^\s\w,;]*[A-Za-z_][\w.\-]*\s*[=:]/
    /**
     * The same after a `(` or `{`, possessive and stopping at another one, so a run of them stays
     * linear. A URI's scheme (`(http://x`) is no key.
     */
    private static final String CALL_KEY_AHEAD = /[^\s\w,;({]*+[A-Za-z_][\w.\-]*\s*[=:](?!\/\/)/
    private static final String COMMA = /,(?![\s'"\[\]\})]|\z|-D|/ + KEY_AHEAD + ')'
    private static final String SEMICOLON = ';(?!' + KEY_AHEAD + ')'
    /** A `(...)` or `{...}` with no space, quote, bracket, comma or semicolon in it, and no key at its start. */
    private static final String GROUP = /\((?!/ + CALL_KEY_AHEAD + /)[^\s()'"\[\]{},;]*+\)|\{(?!/ + CALL_KEY_AHEAD + /)[^\s{}'"\[\]\(\),;]*+\}/

    /**
     * An assignment or map entry whose key names a secret, as config text
     * writes one: `FOO_API_KEY = 'x'`, `azure.storage.accountKey = "x"`,
     * `[MY_TOKEN: 'x']`. Group 1 is the key, group 2 the separator, group 3
     * the value (a quoted string or a bare word; a `[` opens a list or map,
     * whose entries are matched on their own), group 4 the value when it takes
     * the -D form below. A bare word, as a multi-line string holds one, takes its
     * commas (`password=hunter,2`), except a comma before a space, a quote, a
     * bracket, the end, another `-D` property or another key and separator,
     * punctuation between them allowed; a `;` ends it only before a key and
     * separator, the same way. After its first character, a `(` or `{` ends it
     * the same way, so a call's named argument is matched on its own
     * (`x = f(token: 'y')`), and a whole `(...)` or `{...}` with no key at its
     * start is part of it (`password=p(a)ss`). The first character is never
     * refused, so a secret's value that starts `(` (`password = (a:b)`) is
     * matched; {@link #secretAssignments} looks inside such a value when the key
     * names no secret (`x = (token=y)`). A URI's scheme is no key, so
     * `token=(http://u:p@h/x)` keeps its value whole. So `[token: x, y: 1]`,
     * `a=1,password=x`, `a=1;password=x` and `x=a,(token=y)` keep their entries
     * apart. Every form is possessive, so a value of any length is matched
     * without recursion. A key right after `--` is a flag (`--api-key=x`), which
     * {@link Leaks#SECRET_FLAG} judges instead; one after a single `-` is a
     * system property (`-Dfoo.password=x`) and is a key whose bare value takes
     * the {@link Leaks#SYSTEM_PROPERTY} form: up to a space or a quote, stopping
     * only before another `-D` property, so `-Dfoo.password=p,a:ss` goes whole
     * in a multi-line string as it does in a params string. That form needs the
     * `=` right after a key of 256 characters or fewer; a longer one gets the
     * bare form. A -D value under a key that names no secret may hold secret
     * assignments of its own (`-Ddb.props=user=a,password=x`), which
     * {@link #secretAssignments} looks for.
     */
    static final java.util.regex.Pattern ASSIGNMENT = java.util.regex.Pattern.compile(
        /(?<![\w.])(?<!--)([A-Za-z_][\w.\-]*)(\s*[=:]\s*)/ +
        /('(?:[^'\\\n]++|\\.)*+'|"(?:[^"\\\n]++|\\.)*+"|/ +
        /(?<=(?<![\w.\-])-[A-Za-z_][\w.\-]{0,255}=)(?!\[)((?:(?!,-D)[^\s'"])++)|/ +
        '(?:' + GROUP + /|[^\s,;\[\]\})]|/ + COMMA + '|' + SEMICOLON + ')' +
        '(?:' + GROUP + /|[^\s,;\[\]\}){(]|[({](?!/ + CALL_KEY_AHEAD + ')|' + COMMA + '|' + SEMICOLON + ')*+)')

    /**
     * Each {@link #ASSIGNMENT} in text whose key names a secret, in text order, less one whose
     * value the key keeps ({@link Leaks#keepsValue}: `filter_pass = true`, `env.PWD = '/x'`). The
     * -D value form of a key that names none is searched again from its start, so `-Dx=a,token=abc`
     * still yields `token=abc`, and so is a bare value that opens with `(` or `{`, from just
     * inside it, so `x = (token=abc)` yields `token=abc`.
     */
    static List<java.util.regex.MatchResult> secretAssignments(String text) {
        final List<java.util.regex.MatchResult> out = new ArrayList<java.util.regex.MatchResult>()
        final java.util.regex.Matcher m = ASSIGNMENT.matcher(text)
        int from = 0
        while( from <= text.length() && m.find(from) ) {
            if( isSecretKey(m.group(1)) ) {
                if( !Leaks.keepsValue(m.group(1), m.group(3)) )
                    out.add(m.toMatchResult())
                from = m.end()
            }
            else if( m.start(4) >= 0 )
                from = m.start(4)
            else if( m.end(3) > m.start(3) && (text.charAt(m.start(3)) == (char) '(' || text.charAt(m.start(3)) == (char) '{') )
                from = m.start(3) + 1
            else
                from = m.end()
        }
        return out
    }

    /** A key that names a secret ({@link Leaks#isSecretKey}). */
    static boolean isSecretKey(String key) {
        return Leaks.isSecretKey(key)
    }

    /**
     * The RunManifest's config text (DESIGN.md §6): every assignment or map
     * entry whose key names a secret ({@link #isSecretKey}) has its value
     * replaced by {@code '[secret]'}, and each string in a list under such a
     * key too, unless the key keeps it ({@link Leaks#keepsValue}: a plain
     * boolean or number, a whole location); then {@link #scrubText} redacts paths,
     * non-portable URIs and the user name. Idempotent; null passes through.
     */
    static String scrubConfigText(String text) {
        if( text == null )
            return null
        final StringBuilder out = new StringBuilder(text.length())
        int copied = 0
        for( java.util.regex.MatchResult a : secretAssignments(text) ) {
            out.append(text, copied, a.start(3)).append("'").append(REDACTED_SECRET).append("'")
            copied = a.end(3)
        }
        out.append(text, copied, text.length())
        // A list under a secret-named key (`passwords = ['a', 'b']`): each element, as params has it.
        final StringBuilder listed = new StringBuilder(out)
        final List<int[]> literals = secretListLiterals(listed.toString())
        for( int i = literals.size() - 1; i >= 0; i-- ) {
            final int[] at = literals[i]
            final String literal = listed.substring(at[0], at[1])
            final char quote = literal.charAt(0)
            final boolean quoted = quote == (char) '\'' || quote == (char) '"'
            if( quoted && literal.length() == 2 )
                continue    // an empty string, as params leaves it
            final String q = quoted ? String.valueOf(quote) : "'"
            listed.replace(at[0], at[1], q + REDACTED_SECRET + q)
        }
        return scrubText(listed.toString())
    }

    /** A key, then `=` or `:`, then a list opening: `passwords = [`, `[TOKENS: [`. */
    private static final java.util.regex.Pattern LIST_ASSIGNMENT = ~/(?<![\w.])([A-Za-z_][\w.\-]*)(\s*[=:]\s*)\[/

    /**
     * The string and number literals of every list that config text assigns
     * to a secret-named key, nested lists and maps in it included, in text
     * order, less those the key keeps ({@link Leaks#keepsValue}: a number, a
     * quoted boolean, a location). Each is {start, end, keyStart, keyEnd}: the
     * literal with its quotes, and the key it sits under.
     */
    static List<int[]> secretListLiterals(String text) {
        final List<int[]> out = new ArrayList<int[]>()
        final java.util.regex.Matcher m = LIST_ASSIGNMENT.matcher(text)
        final int n = text.length()
        int from = 0
        while( from < n && m.find(from) ) {
            int i = m.end()
            if( !isSecretKey(m.group(1)) ) {
                from = i
                continue
            }
            int depth = 1
            while( i < n && depth > 0 ) {
                final char c = text.charAt(i)
                if( c == (char) '\'' || c == (char) '"' ) {
                    int j = i + 1
                    while( j < n && text.charAt(j) != c && text.charAt(j) != (char) '\n' )
                        j += text.charAt(j) == (char) '\\' ? 2 : 1
                    final int end = Math.min(j + 1, n)
                    if( !Leaks.keepsValue(m.group(1), text.substring(i, end)) )
                        out.add([i, end, m.start(1), m.end(1)] as int[])
                    i = end
                    continue
                }
                if( Character.isDigit(c) && !wordChar(text.charAt(i - 1)) ) {
                    int j = i
                    while( j < n && (wordChar(text.charAt(j)) || text.charAt(j) == (char) '.') )
                        j++
                    // A map key (`[1: 'x']`) is not a value.
                    int k = j
                    while( k < n && text.charAt(k) == (char) ' ' )
                        k++
                    if( (k >= n || text.charAt(k) != (char) ':') && !Leaks.keepsValue(m.group(1), text.substring(i, j)) )
                        out.add([i, j, m.start(1), m.end(1)] as int[])
                    i = j
                    continue
                }
                if( c == (char) '[' )
                    depth++
                else if( c == (char) ']' )
                    depth--
                i++
            }
            from = i
        }
        return out
    }

    private static boolean wordChar(char c) {
        return Character.isLetterOrDigit(c) || c == (char) '_'
    }

    // ---- helpers shared by the kinds ----

    static Map<String, Object> head(String kind) {
        return head(kind, SCHEMA)
    }

    static Map<String, Object> head(String kind, int schema) {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('kind', kind)
        map.put('schema', (long) schema)
        return map
    }

    static void expectKind(Map block, String kind) {
        if( block == null )
            throw new IllegalArgumentException("expected a $kind block, got null")
        final String actual = kindOf(block)
        if( actual != kind )
            throw new IllegalArgumentException("expected a $kind block, got ${actual ?: 'a block with no kind'}")
    }

    /** The block's schema, refused unless the reader knows it. */
    static int schemaOf(Map block, Collection<Integer> readable) {
        final int schema = (int) number(require(block, 'schema'), 'schema')
        if( !readable.contains(schema) )
            throw new IllegalArgumentException("${kindOf(block)} schema ${schema} is not one this build reads (${readable.join(', ')})")
        return schema
    }

    static Object require(Map block, String field) {
        if( !block.containsKey(field) )
            throw new IllegalArgumentException("${kindOf(block) ?: 'block'} is missing the field '$field'")
        return block.get(field)
    }

    static String string(Object value, String field) {
        if( value == null )
            return null
        if( !(value instanceof String) )
            throw new IllegalArgumentException("field '$field' should be a string, got ${value.getClass().simpleName}")
        return (String) value
    }

    static long number(Object value, String field) {
        if( value instanceof Number )
            return ((Number) value).longValue()
        throw new IllegalArgumentException("field '$field' should be a number, got ${value == null ? 'null' : value.getClass().simpleName}")
    }

    static Cid cid(Object value, String field) {
        if( value == null )
            return null
        if( !(value instanceof Cid) )
            throw new IllegalArgumentException("field '$field' should be a content address, got ${value.getClass().simpleName}")
        return (Cid) value
    }

    static int utf8Length(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length
    }
}

/**
 * One entry of a DirectoryManifest: name, mode, size and address, and nothing
 * else (DESIGN.md §6). No permission bits at all: identity must not depend on
 * the umask, or on whether the backend keeps an execute bit.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ManifestEntry {

    static final String REGULAR = 'regular'
    /** Written only at schema 1, before 2026-09-28; {@link #fromCbor} reads it as {@link #REGULAR}. */
    static final String EXECUTABLE = 'executable'
    static final String SYMLINK = 'symlink'
    static final String DIRECTORY = 'directory'
    static final String UNRESOLVABLE = 'unresolvable'

    private static final Set<String> MODES = [REGULAR, SYMLINK, DIRECTORY, UNRESOLVABLE] as Set

    final String name
    final String mode
    final long size
    final Cid address
    final String target

    ManifestEntry(String name, String mode, long size, Cid address, String target) {
        if( !name || name.contains('/') || name == '.' || name == '..' )
            throw new IllegalArgumentException("a manifest entry name is one path segment: ${name == null ? 'null' : "'$name'"}")
        if( !MODES.contains(mode) )
            throw new IllegalArgumentException("unknown manifest entry mode '$mode' for '$name'")
        if( size < 0 )
            throw new IllegalArgumentException("manifest entry '$name' has a negative size")
        if( mode == REGULAR ) {
            if( address == null || !address.isRaw() )
                throw new IllegalArgumentException("manifest entry '$name' is $mode and needs a raw content address, got ${address ?: 'null'}")
        }
        else if( mode == DIRECTORY ) {
            if( address == null || !address.isDagCbor() )
                throw new IllegalArgumentException("manifest entry '$name' is a directory and needs a dag-cbor manifest address, got ${address ?: 'null'}")
        }
        else if( address != null ) {
            throw new IllegalArgumentException("manifest entry '$name' is $mode and cannot carry an address")
        }
        if( mode == SYMLINK && !target )
            throw new IllegalArgumentException("manifest entry '$name' is a symlink and needs its target text")
        this.name = name
        this.mode = mode
        this.size = size
        this.address = address
        this.target = target
    }

    static ManifestEntry regular(String name, Cid address, long size) {
        return new ManifestEntry(name, REGULAR, size, address, null)
    }

    /** A subdirectory: the address is its own manifest, the size is not the tree's. */
    static ManifestEntry directory(String name, Cid manifest) {
        return new ManifestEntry(name, DIRECTORY, 0L, manifest, null)
    }

    /** A link kept as a link: its content is the target text. */
    static ManifestEntry symlink(String name, String target) {
        return new ManifestEntry(name, SYMLINK, Records.utf8Length(target), null, target)
    }

    /** Something that could not be addressed. Counted, never dropped. */
    static ManifestEntry unresolvable(String name, String target) {
        return new ManifestEntry(name, UNRESOLVABLE, Records.utf8Length(target), null, target)
    }

    boolean isDirectory() { mode == DIRECTORY }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('name', name)
        map.put('mode', mode)
        map.put('size', size)
        map.put('address', address)
        map.put('target', target)
        return map
    }

    /** A schema-1 manifest may say `executable`; since 2026-09-28 that is `regular` (ticket 15 addendum). */
    static ManifestEntry fromCbor(Map entry, int schema) {
        final String mode = Records.string(Records.require(entry, 'mode'), 'mode')
        return new ManifestEntry(
            Records.string(Records.require(entry, 'name'), 'name'),
            schema == 1 && mode == EXECUTABLE ? REGULAR : mode,
            Records.number(Records.require(entry, 'size'), 'size'),
            Records.cid(Records.require(entry, 'address'), 'address'),
            Records.string(Records.require(entry, 'target'), 'target'))
    }
}

/**
 * A directory as a sorted list of entries (DESIGN.md §6). Sorted by the raw
 * UTF-8 bytes of the name, so the serialisation is canonical and a receiver
 * can verify a manifest by re-encoding it. Carries no {@code asserted_by}:
 * identical content is one block whoever stored it.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class DirectoryManifest {

    /** Written at schema 2 since 2026-09-28 (ticket 15 addendum): no executable mode. */
    static final int SCHEMA = 2
    static final List<Integer> READABLE = [1, 2].asImmutable()

    final List<ManifestEntry> entries

    DirectoryManifest(List<ManifestEntry> entries) {
        final List<ManifestEntry> sorted = new ArrayList<ManifestEntry>(entries ?: Collections.<ManifestEntry> emptyList())
        sorted.sort { ManifestEntry a, ManifestEntry b -> compareNames(a.name, b.name) }
        for( int i = 1; i < sorted.size(); i++ ) {
            if( sorted.get(i).name == sorted.get(i - 1).name )
                throw new IllegalArgumentException("a directory manifest cannot hold '${sorted.get(i).name}' twice")
        }
        this.entries = Collections.unmodifiableList(sorted)
    }

    /** Ascending on the raw name bytes: Unicode normalisation forms are different names. */
    private static int compareNames(String a, String b) {
        final byte[] left = a.getBytes(StandardCharsets.UTF_8)
        final byte[] right = b.getBytes(StandardCharsets.UTF_8)
        final int n = Math.min(left.length, right.length)
        for( int i = 0; i < n; i++ ) {
            final int diff = (left[i] & 0xff) - (right[i] & 0xff)
            if( diff != 0 )
                return diff
        }
        return left.length - right.length
    }

    ManifestEntry entry(String name) {
        return entries.find { ManifestEntry e -> e.name == name }
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.DIRECTORY_MANIFEST, SCHEMA)
        map.put('entries', entries.collect { ManifestEntry e -> e.toCbor() })
        return map
    }

    static DirectoryManifest fromCbor(Map block) {
        Records.expectKind(block, Records.DIRECTORY_MANIFEST)
        final int schema = Records.schemaOf(block, READABLE)
        final Object entries = Records.require(block, 'entries')
        if( !(entries instanceof List) )
            throw new IllegalArgumentException('a directory manifest needs a list of entries')
        return new DirectoryManifest(((List) entries).collect { Object e -> ManifestEntry.fromCbor((Map) e, schema) })
    }
}

/**
 * What a run could not address, counted rather than hidden (DESIGN.md §6).
 * Directory publishing produces {@code unresolvable}; the output join produces
 * the other three.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Anomalies {

    static final Anomalies NONE = new Anomalies(0, 0, 0, 0, 0)

    final int unresolvable
    final int unaddressed
    final int declined
    final int neverPublished
    /** Publishes into the store that joined to no workflow output item (ticket 19): publishDir's files. */
    final int unjoined

    Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished) {
        this(unresolvable, unaddressed, declined, neverPublished, 0)
    }

    Anomalies(int unresolvable, int unaddressed, int declined, int neverPublished, int unjoined) {
        this.unresolvable = unresolvable
        this.unaddressed = unaddressed
        this.declined = declined
        this.neverPublished = neverPublished
        this.unjoined = unjoined
    }

    static Anomalies unresolvable(int count) {
        return new Anomalies(count, 0, 0, 0)
    }

    static Anomalies unjoined(int count) {
        return new Anomalies(0, 0, 0, 0, count)
    }

    Anomalies plus(Anomalies other) {
        if( other == null )
            return this
        return new Anomalies(
            unresolvable + other.unresolvable,
            unaddressed + other.unaddressed,
            declined + other.declined,
            neverPublished + other.neverPublished,
            unjoined + other.unjoined)
    }

    boolean isEmpty() {
        return unresolvable == 0 && unaddressed == 0 && declined == 0 && neverPublished == 0 && unjoined == 0
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('unresolvable', (long) unresolvable)
        map.put('unaddressed', (long) unaddressed)
        map.put('declined', (long) declined)
        map.put('never_published', (long) neverPublished)
        map.put('unjoined', (long) unjoined)
        return map
    }

    static Anomalies fromCbor(Map counts) {
        if( counts == null )
            throw new IllegalArgumentException('a run completion needs its anomaly counters')
        return new Anomalies(
            (int) Records.number(Records.require(counts, 'unresolvable'), 'unresolvable'),
            (int) Records.number(Records.require(counts, 'unaddressed'), 'unaddressed'),
            (int) Records.number(Records.require(counts, 'declined'), 'declined'),
            (int) Records.number(Records.require(counts, 'never_published'), 'never_published'),
            counts.containsKey('unjoined') ? (int) Records.number(counts.get('unjoined'), 'unjoined') : 0)
    }
}

/**
 * A file or directory inside an Output Item's structure (DESIGN.md §6).
 *
 * A leaf holds the name it was published under, because a raw block has no
 * name of its own and a pipeline that is handed the item back stages the file
 * under that name. An address and a reason are exclusive: exactly one of them
 * is always set, and neither is ever an absent field.
 *
 * Written at schema 2 (2026-09-28, ticket 16): a leaf carries no provider, so
 * the same content published by different Address Providers is one
 * OutputItem. Who addressed it is the run's record ({@link RunCompletion#providers}),
 * not the leaf's. A schema-1 leaf still carries `provider`; it is ignored on
 * decoding.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Leaf {

    static final String DECLINED = 'declined'
    static final String NEVER_PUBLISHED = 'never_published'
    static final String UNRESOLVABLE = 'unresolvable'
    static final String UNADDRESSED = 'unaddressed'

    private static final Set<String> REASONS = [DECLINED, NEVER_PUBLISHED, UNRESOLVABLE, UNADDRESSED] as Set

    final String name
    final Cid address
    final Long size
    final String reason

    Leaf(String name, Cid address, Long size, String reason) {
        if( address == null && !reason )
            throw new IllegalArgumentException("a leaf without an address needs a reason (${name ?: 'unnamed'})")
        if( address != null && reason )
            throw new IllegalArgumentException("a leaf addressed as $address cannot also carry the reason '$reason'")
        if( reason && !REASONS.contains(reason) )
            throw new IllegalArgumentException("unknown leaf reason '$reason'")
        this.name = name
        this.address = address
        this.size = size
        this.reason = reason
    }

    /** An addressed leaf. Who addressed it is the run's record (RunCompletion.providers), not the leaf's. */
    static Leaf of(String name, Cid address, Long size) {
        return new Leaf(name, address, size, null)
    }

    /** A leaf with no address, and the reason there is none. */
    static Leaf without(String name, String reason) {
        return new Leaf(name, null, null, reason)
    }

    /** What Nextflow hands us as a null in place of a path. */
    static Leaf declined() {
        return without(null, DECLINED)
    }

    boolean isAddressed() { address != null }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('kind', Records.LEAF)
        map.put('name', name)
        map.put('address', address)
        map.put('size', size)
        map.put('reason', reason)
        return map
    }

    /**
     * True for a map that is a leaf rather than a Meta Map that happens to
     * carry a {@code kind} of its own. DESIGN.md §6 keys the decoding rule on
     * {@code kind == 'Leaf'}; the two exclusive fields are what tell an
     * encoded leaf apart from a user map that borrowed the word.
     */
    static boolean isLeaf(Object value) {
        if( !(value instanceof Map) )
            return false
        final Map map = (Map) value
        return Records.kindOf(map) == Records.LEAF && map.containsKey('address') && map.containsKey('reason')
    }

    /** A schema-1 leaf also carries `provider`; it is ignored (ticket 16). */
    static Leaf fromCbor(Map map) {
        Records.expectKind(map, Records.LEAF)
        final Object size = Records.require(map, 'size')
        return new Leaf(
            Records.string(Records.require(map, 'name'), 'name'),
            Records.cid(Records.require(map, 'address'), 'address'),
            size == null ? null : (Long) Records.number(size, 'size'),
            Records.string(Records.require(map, 'reason'), 'reason'))
    }
}

/**
 * One entry of an Output Collection, as its own block (DESIGN.md §6).
 *
 * The value mirrors the channel item: a Map stays a map, a tuple stays a list,
 * scalars keep their types, and every file leaf is a {@link Leaf}. The item
 * carries no run reference and no publish path, so identical metadata over
 * identical content is one block however many runs produce it -- which is the
 * whole point of addressing items separately.
 *
 * Written at schema 2 (2026-09-28): its leaves carry no provider. A schema-1
 * item decodes to the same value, and re-encodes at schema 2 under a
 * different address; nothing re-encodes a block it read.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class OutputItem {

    static final int SCHEMA = 2
    static final List<Integer> READABLE = [1, 2].asImmutable()

    final Object value

    OutputItem(Object value) {
        this.value = value
    }

    static OutputItem of(Object value) {
        return new OutputItem(value)
    }

    /** The leaves in the order a reader meets them, which is the order `paths` uses. */
    List<Leaf> leaves() {
        final List<Leaf> found = new ArrayList<Leaf>()
        collect(value, found)
        return found
    }

    private static void collect(Object value, List<Leaf> found) {
        if( value instanceof Leaf )
            found.add((Leaf) value)
        else if( value instanceof Map )
            ((Map) value).values().each { Object v -> collect(v, found) }
        else if( value instanceof Collection )
            ((Collection) value).each { Object v -> collect(v, found) }
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.OUTPUT_ITEM, SCHEMA)
        map.put('value', encode(value))
        return map
    }

    private static Object encode(Object value) {
        if( value instanceof Leaf )
            return ((Leaf) value).toCbor()
        if( value instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            ((Map<?, ?>) value).each { Object k, Object v -> out.put(String.valueOf(k), encode(v)) }
            return out
        }
        if( value instanceof Collection )
            return ((Collection) value).collect { Object v -> encode(v) }
        return value
    }

    static OutputItem fromCbor(Map block) {
        Records.expectKind(block, Records.OUTPUT_ITEM)
        Records.schemaOf(block, READABLE)
        return new OutputItem(decode(Records.require(block, 'value')))
    }

    private static Object decode(Object value) {
        if( Leaf.isLeaf(value) )
            return Leaf.fromCbor((Map) value)
        if( value instanceof Map ) {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            ((Map<?, ?>) value).each { Object k, Object v -> out.put(String.valueOf(k), decode(v)) }
            return out
        }
        if( value instanceof Collection )
            return ((Collection) value).collect { Object v -> decode(v) }
        return value
    }
}

/**
 * One named output of one run (DESIGN.md §6): links to its items, and the
 * publish paths those items' leaves went to.
 *
 * Items are sorted by their address, because `publishedValues` is
 * arrival-ordered and position is therefore not reproducible between runs.
 * `paths` is re-aligned to that sort, so `paths[i]` always belongs to
 * `items[i]`.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class OutputCollection {

    final String assertedBy
    final Cid run
    final String name
    final List<Cid> items
    final List<List<String>> paths
    final OutputIndex index

    OutputCollection(String assertedBy, Cid run, String name, List<Cid> items, List<List<String>> paths) {
        this(assertedBy, run, name, items, paths, null)
    }

    OutputCollection(String assertedBy, Cid run, String name, List<Cid> items, List<List<String>> paths, OutputIndex index) {
        if( !assertedBy )
            throw new IllegalArgumentException('an output collection needs an asserted_by')
        if( run == null )
            throw new IllegalArgumentException("output collection '$name' needs its run manifest address")
        if( !name )
            throw new IllegalArgumentException('an output collection needs the name it was declared under')
        final List<Cid> givenItems = items ?: Collections.<Cid> emptyList()
        final List<List<String>> givenPaths = paths ?: Collections.<List<String>> emptyList()
        if( givenItems.size() != givenPaths.size() )
            throw new IllegalArgumentException("output collection '$name' has ${givenItems.size()} items and ${givenPaths.size()} path lists")
        final List<Integer> order = (0..<givenItems.size()).toList()
        order.sort { Integer a, Integer b -> compare(givenItems.get(a), givenPaths.get(a), givenItems.get(b), givenPaths.get(b)) }
        this.assertedBy = assertedBy
        this.run = run
        this.name = name
        this.items = Collections.unmodifiableList(order.collect { Integer i -> givenItems.get(i) })
        this.paths = Collections.unmodifiableList(order.collect { Integer i -> givenPaths.get(i) })
        this.index = index
    }

    /**
     * Order by item address, then by the item's path list when two items share
     * a cid (identical content published under different paths), so the order
     * -- and therefore the collection address -- is reproducible rather than
     * inheriting arrival order. A null item is a hole Nextflow handed us; holes
     * sort first rather than being compacted away.
     */
    private static int compare(Cid a, List<String> pa, Cid b, List<String> pb) {
        if( a == null && b == null ) return comparePaths(pa, pb)
        if( a == null ) return -1
        if( b == null ) return 1
        final int byCid = a.toString() <=> b.toString()
        return byCid != 0 ? byCid : comparePaths(pa, pb)
    }

    /** Lexicographic over the joined path lists; a null path list sorts first. */
    private static int comparePaths(List<String> a, List<String> b) {
        return key(a) <=> key(b)
    }

    private static String key(List<String> paths) {
        return paths == null ? '' : paths.collect { String p -> p == null ? '' : p }.join('\u0000')
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.OUTPUT_COLLECTION)
        map.put('asserted_by', assertedBy)
        map.put('run', run)
        map.put('name', name)
        map.put('items', new ArrayList<Object>(items))
        map.put('paths', paths.collect { List<String> p -> p == null ? null : new ArrayList<Object>(p) })
        if( index != null )
            map.put('index', index.toCbor())
        return map
    }

    static OutputCollection fromCbor(Map block) {
        Records.expectKind(block, Records.OUTPUT_COLLECTION)
        final List items = (List) Records.require(block, 'items')
        final List paths = (List) Records.require(block, 'paths')
        return new OutputCollection(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.cid(Records.require(block, 'run'), 'run'),
            Records.string(Records.require(block, 'name'), 'name'),
            items.collect { Object c -> Records.cid(c, 'items') },
            paths.collect { Object p -> p == null ? null : (List<String>) ((List) p).collect { Object s -> Records.string(s, 'paths') } },
            block.containsKey('index') ? OutputIndex.fromCbor((Map) block.get('index')) : null)
    }
}

/**
 * A workflow output's Output Index File (glossary; ticket 26): the file Nextflow
 * writes for an output that declares `index {}`. Linked from its collection so it
 * is in the closure; its content names Publish Coordinates, so it is a view, never
 * provenance. The leaf is addressed only from a publish made in the same run.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class OutputIndex {
    final Leaf leaf
    /** The index file's publish path, relative to outputDir, '/'-joined. */
    final String path

    OutputIndex(Leaf leaf, String path) {
        if( leaf == null )
            throw new IllegalArgumentException('an output index needs its leaf')
        if( !path )
            throw new IllegalArgumentException('an output index needs its publish path')
        this.leaf = leaf
        this.path = path
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('leaf', leaf.toCbor())
        map.put('path', path)
        return map
    }

    static OutputIndex fromCbor(Map map) {
        return new OutputIndex(
            Leaf.fromCbor((Map) Records.require(map, 'leaf')),
            Records.string(Records.require(map, 'path'), 'path'))
    }
}

/**
 * What was run (DESIGN.md §6). Written at `onFlowBegin`, when the Nextflow run
 * key is known. `params` and `config` are scrubbed here rather than by the
 * caller, so a store-local path cannot reach a block by being forgotten.
 *
 * `config` is the resolved config as text (a String), which keeps closure
 * source and cannot fail to encode. A block written before 2026-09-27 holds
 * it as a Map, which still decodes.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class RunManifest {

    final String assertedBy
    final String pipeline
    final String repository
    final String revision
    final String commitId
    final String runName
    final String nfRunHash
    final String sessionId
    final boolean resumed
    final String nextflowVersion
    final Map params
    final Object config
    final Cid script
    final String startedAt

    RunManifest(Map args) {
        this.assertedBy = req(args, 'assertedBy')
        this.pipeline = req(args, 'pipeline')
        this.repository = (String) args.get('repository')
        this.revision = (String) args.get('revision')
        this.commitId = (String) args.get('commitId')
        this.runName = req(args, 'runName')
        this.nfRunHash = req(args, 'nfRunHash')
        this.sessionId = req(args, 'sessionId')
        this.resumed = args.get('resumed') as boolean
        this.nextflowVersion = req(args, 'nextflowVersion')
        this.params = (Map) Records.scrub((Map) (args.get('params') ?: [:]))
        this.config = scrubConfig(args.get('config'))
        this.script = (Cid) args.get('script')
        this.startedAt = req(args, 'startedAt')
    }

    private static String req(Map args, String field) {
        final Object value = args.get(field)
        if( !value )
            throw new IllegalArgumentException("a run manifest needs '$field'")
        return value.toString()
    }

    private static Object scrubConfig(Object config) {
        if( config == null )
            return ''
        if( config instanceof CharSequence )
            return Records.scrubConfigText(config.toString())
        if( config instanceof Map )
            return Records.scrub((Map) config)
        throw new IllegalArgumentException("a run manifest's config is a String (or, in an old block, a Map), not ${config.getClass().name}")
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.RUN_MANIFEST)
        map.put('asserted_by', assertedBy)
        map.put('pipeline', pipeline)
        map.put('repository', repository)
        map.put('revision', revision)
        map.put('commit_id', commitId)
        map.put('run_name', runName)
        map.put('nf_run_hash', nfRunHash)
        map.put('session_id', sessionId)
        map.put('resumed', resumed)
        map.put('nextflow_version', nextflowVersion)
        map.put('params', params)
        map.put('config', config)
        map.put('script', script)
        map.put('started_at', startedAt)
        return map
    }

    static RunManifest fromCbor(Map block) {
        Records.expectKind(block, Records.RUN_MANIFEST)
        return new RunManifest([
            assertedBy     : Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            pipeline       : Records.string(Records.require(block, 'pipeline'), 'pipeline'),
            repository     : Records.string(Records.require(block, 'repository'), 'repository'),
            revision       : Records.string(Records.require(block, 'revision'), 'revision'),
            commitId       : Records.string(Records.require(block, 'commit_id'), 'commit_id'),
            runName        : Records.string(Records.require(block, 'run_name'), 'run_name'),
            nfRunHash      : Records.string(Records.require(block, 'nf_run_hash'), 'nf_run_hash'),
            sessionId      : Records.string(Records.require(block, 'session_id'), 'session_id'),
            resumed        : Records.require(block, 'resumed'),
            nextflowVersion: Records.string(Records.require(block, 'nextflow_version'), 'nextflow_version'),
            params         : (Map) Records.require(block, 'params'),
            config         : Records.require(block, 'config'),
            script         : Records.cid(Records.require(block, 'script'), 'script'),
            startedAt      : Records.string(Records.require(block, 'started_at'), 'started_at'),
        ])
    }
}

/**
 * How a run ended, and everything it produced (DESIGN.md §6). The Run Log
 * points at this block; the index is built from it.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class RunCompletion {

    static final String SUCCEEDED = 'succeeded'
    static final String FAILED = 'failed'

    private static final Set<String> STATUSES = [SUCCEEDED, FAILED] as Set

    /**
     * Written at schema 3 since 2026-10-26: it links item_lineage. Schema 2
     * (2026-09-28, ticket 16) added {@link #providers}.
     */
    static final int SCHEMA = 3
    static final List<Integer> READABLE = [1, 2, 3].asImmutable()

    final String assertedBy
    final Cid run
    final List<Cid> collections
    final Cid inputSet
    final String status
    final Integer exitStatus
    final boolean possiblyIncomplete
    final String startedAt
    final String finishedAt
    final Anomalies anomalies
    final String error
    /** Provider name to every address of the run it supplied, sorted (DESIGN.md §6, ticket 16). */
    final Map<String, List<Cid>> providers
    /** The ItemLineage block naming where each output item came from, or null when none was written. */
    final Cid itemLineage

    RunCompletion(Map args) {
        this.assertedBy = str(args, 'assertedBy')
        this.run = (Cid) args.get('run')
        if( run == null )
            throw new IllegalArgumentException("a run completion needs its run manifest address")
        this.collections = Collections.unmodifiableList(new ArrayList<Cid>((List<Cid>) (args.get('collections') ?: [])))
        this.inputSet = (Cid) args.get('inputSet')
        this.status = str(args, 'status')
        if( !STATUSES.contains(status) )
            throw new IllegalArgumentException("unknown run status '$status'")
        final Object exit = args.get('exitStatus')
        this.exitStatus = exit == null ? null : ((Number) exit).intValue()
        this.possiblyIncomplete = args.get('possiblyIncomplete') as boolean
        this.startedAt = str(args, 'startedAt')
        this.finishedAt = str(args, 'finishedAt')
        this.anomalies = (Anomalies) args.get('anomalies')
        if( anomalies == null )
            throw new IllegalArgumentException('a run completion needs its anomaly counters')
        // A failed run's error is free text from Nextflow, so it can carry an
        // absolute path or user name mid-message; scrub it here so a RunCompletion
        // never leaks the launch location into a portable block (DESIGN.md §6).
        this.error = Records.scrubText((String) args.get('error'))
        this.providers = normaliseProviders((Map) args.get('providers') ?: Collections.emptyMap())
        this.itemLineage = (Cid) args.get('itemLineage')
    }

    private static String str(Map args, String field) {
        final Object value = args.get(field)
        if( !value )
            throw new IllegalArgumentException("a run completion needs '$field'")
        return value.toString()
    }

    /** Keys in name order, each list sorted by cid string and without repeats; every key a known provider. */
    private static Map<String, List<Cid>> normaliseProviders(Map raw) {
        final TreeMap<String, List<Cid>> out = new TreeMap<String, List<Cid>>()
        for( Object e : raw.entrySet() ) {
            final String name = String.valueOf(((Map.Entry) e).key)
            if( !Providers.isKnown(name) )
                throw new IllegalArgumentException("unknown Address Provider '${name}' (known: ${Providers.ALL.join(', ')})")
            final TreeMap<String, Cid> byText = new TreeMap<String, Cid>()
            for( Object c : (Collection) ((Map.Entry) e).value )
                byText.put(c.toString(), (Cid) c)
            out.put(name, Collections.unmodifiableList(new ArrayList<Cid>(byText.values())))
        }
        return Collections.unmodifiableMap(out)
    }

    boolean isSuccessful() { status == SUCCEEDED && !possiblyIncomplete }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.RUN_COMPLETION, SCHEMA)
        map.put('asserted_by', assertedBy)
        map.put('run', run)
        map.put('collections', new ArrayList<Object>(collections))
        map.put('input_set', inputSet)
        map.put('status', status)
        map.put('exit_status', exitStatus == null ? null : (long) exitStatus.intValue())
        map.put('possibly_incomplete', possiblyIncomplete)
        map.put('started_at', startedAt)
        map.put('finished_at', finishedAt)
        map.put('anomalies', anomalies.toCbor())
        map.put('error', error)
        final Map<String, Object> byProvider = new LinkedHashMap<String, Object>()
        providers.each { String name, List<Cid> cids -> byProvider.put(name, new ArrayList<Object>(cids)) }
        map.put('providers', byProvider)
        map.put('item_lineage', itemLineage)
        return map
    }

    static RunCompletion fromCbor(Map block) {
        Records.expectKind(block, Records.RUN_COMPLETION)
        final int schema = Records.schemaOf(block, READABLE)
        final Map providers = schema == 1 ? Collections.emptyMap() : (Map) Records.require(block, 'providers')
        final List collections = (List) Records.require(block, 'collections')
        return new RunCompletion([
            assertedBy        : Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            run               : Records.cid(Records.require(block, 'run'), 'run'),
            collections       : collections.collect { Object c -> Records.cid(c, 'collections') },
            inputSet          : Records.cid(Records.require(block, 'input_set'), 'input_set'),
            status            : Records.string(Records.require(block, 'status'), 'status'),
            exitStatus        : Records.require(block, 'exit_status'),
            possiblyIncomplete: Records.require(block, 'possibly_incomplete'),
            startedAt         : Records.string(Records.require(block, 'started_at'), 'started_at'),
            finishedAt        : Records.string(Records.require(block, 'finished_at'), 'finished_at'),
            anomalies         : Anomalies.fromCbor((Map) Records.require(block, 'anomalies')),
            error             : Records.string(Records.require(block, 'error'), 'error'),
            providers         : providers.collectEntries { k, v -> [(k): ((List) v).collect { Object c -> Records.cid(c, 'providers') }] },
            itemLineage       : schema < 3 ? null : Records.cid(Records.require(block, 'item_lineage'), 'item_lineage'),
        ])
    }
}
