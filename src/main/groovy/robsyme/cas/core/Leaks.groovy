package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.transform.CompileStatic

/**
 * What must not travel in a RunManifest, and how to recognise it in a token
 * (DESIGN.md §6). One place for the patterns and the classifier: {@link Records}
 * maps a kind to a redaction marker when it scrubs, {@link RunManifestAudit}
 * maps it to a finding when it reads a stored block, so the two cannot drift.
 */
@CompileStatic
final class Leaks {

    private Leaks() {}

    // Kinds, as the audit reports them.
    static final String ABSOLUTE_PATH = 'absolute path'
    static final String URI = 'uri'
    static final String OS_USER = 'os user'
    static final String AWS_KEY_ID = 'aws key id'
    static final String PRIVATE_KEY = 'private key'
    static final String GITHUB_TOKEN = 'github token'
    static final String SEQERA_TOKEN = 'seqera token'
    static final String SECRET_ASSIGNMENT = 'secret assignment'
    static final String AWS_SECRET_KEY = 'aws secret key'
    static final String BEARER_TOKEN = 'bearer token'
    static final String JWT = 'jwt'
    static final String SLACK_TOKEN = 'slack token'
    static final String GITLAB_TOKEN = 'gitlab token'
    static final String HUGGING_FACE_TOKEN = 'hugging face token'
    static final String AZURE_SAS = 'azure sas signature'
    static final String BASIC_AUTH = 'basic auth'
    static final String GOOGLE_API_KEY = 'google api key'
    static final String OPENAI_KEY = 'openai key'
    static final String ANTHROPIC_KEY = 'anthropic key'

    /** Schemes that name content rather than a place, so they survive the scrub. */
    static final Set<String> PORTABLE_SCHEMES = Collections.unmodifiableSet(['lid', 'cas'] as Set<String>)

    static final Pattern SCHEME = Pattern.compile('^([A-Za-z][A-Za-z0-9+.-]*)://')

    static final Pattern PEM_HEADER = Pattern.compile('-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----')
    /** A whole PEM private key block, which spans lines. */
    static final Pattern PEM_BLOCK = Pattern.compile(
        '-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\\s\\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----')
    static final Pattern AWS_KEY = Pattern.compile('(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])')
    static final Pattern GITHUB = Pattern.compile(
        '(?<![A-Za-z0-9_])(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})')
    /** A Seqera Platform token is base64 of `{"tid": ...}`, so it starts `eyJ0aWQi`. */
    static final Pattern SEQERA = Pattern.compile('(?<![A-Za-z0-9_])eyJ0aWQi[A-Za-z0-9+/=_-]{6,}')

    /**
     * A `--flag` and its value, after `=` or as the next token on the line: `--api-key=abc`,
     * `--password hunter2`, `--token 'abc'`. Group 1 is the flag and separator, group 2 the name,
     * group 3 the separator. Which names count is {@link #kindOf}'s job. A quoted value keeps its
     * quotes. A value that starts with `-` is another flag, one that starts with `$` is a variable,
     * and one that starts with `[` is a marker. `Bearer <token>` or `Basic <value>` is one value, so
     * a secret flag takes it whole whatever the token is (`--auth Basic Statistics`), quoted or not.
     * A token is never another flag, a variable or a marker, so a bare `Bearer` or `Basic` is the
     * value on its own: `--proxy-auth basic --threads 4` keeps `--threads 4`.
     */
    static final Pattern SECRET_FLAG = Pattern.compile(
        '(?<![\\w-])(--([A-Za-z0-9][\\w.-]*)(=|[ \\t]+))(?![\"\']?[$\\[])(?:(?<==)|(?![\"\']?-))' +
        '(\'[^\'\\n]+\'|"[^"\\n]+"|' +
        '(?i:bearer|basic)(?:[ \\t]+(?![\"\']?[-$\\[])(?:\'[^\'\\n]+\'|"[^"\\n]+"|[^\\s\'"]+)|(?![^\\s\'"]))|' +
        '[^\\s\'"]+)')
    /**
     * A quoted secret-named key, a `:` and its value, as JSON writes one: `{"token": "abc"}`.
     * Group 1 is the key, quotes and separator, group 3 the name. `true`, `false` and `null` stay.
     */
    static final Pattern JSON_SECRET = Pattern.compile(
        '(([\"\'])([A-Za-z_][\\w.-]*)\\2[ \\t]*:[ \\t]*)(?![\"\']?\\[secret\\])(?!(?:true|false|null)(?![\\w.+/=-]))' +
        '("[^"\\n]+"|\'[^\'\\n]+\'|[A-Za-z0-9._+/=-]+)')
    /**
     * `Bearer <token>` in any case, as an Authorization header writes it. Group 1, the word and
     * the space, stays. The token has 8 or more characters and a digit, `+`, `/`, `=` or an
     * upper-case letter after its first, so prose (`the bearer instruments`) stays.
     */
    static final Pattern BEARER = Pattern.compile(
        '(\\b(?i:bearer)[ \\t]+)(?=[A-Za-z0-9._~+/-]*[0-9+/=]|[A-Za-z0-9._~+/-]+[A-Z])[A-Za-z0-9._~+/-]{8,}=*')
    /**
     * `Basic <value>` in any case, the value 4 or more characters of the base64 alphabet. Group 1,
     * the word and the space, stays. A match counts only when the value decodes to RFC 7617
     * user-pass ({@link #isUserPass}), so `Basic QualityControl` and `Basic Statistics` stay.
     */
    static final Pattern BASIC = Pattern.compile('(\\b(?i:basic)[ \\t]+)[A-Za-z0-9+/]{4,}=*')
    /**
     * A Java system property, `-D<name>=<value>`, as a JVM flag in a params string or a quoted
     * config value (`'-Dfoo.password=abc -Xmx2g'`). Group 1 is `-D`, the name and `=`, group 2
     * the name, which must be secret-named. A quoted value keeps its quotes; a bare one runs to a
     * space or a quote and takes commas (`-Dfoo.password=hunter,2`), except one that starts another
     * property, so `-Dfoo.password=a,-Dbar.token=b` is two properties. The bare form is possessive,
     * so a value of any length is matched without recursion.
     */
    static final Pattern SYSTEM_PROPERTY = Pattern.compile(
        '(?<![\\w-])(-D([A-Za-z_][\\w.-]*)=)(?![\"\']?[\\[$])(\'[^\'\\n]+\'|"[^"\\n]+"|(?:(?!,-D)[^\\s\'"])++)')
    /**
     * The password `sshpass -p` takes, after spaces or glued (`sshpass -phunter2`), with `-v` flags and
     * a `-P <prompt>` option (spaced, glued or quoted) allowed before the `-p`: `sshpass -p hunter2 scp a b`,
     * `sshpass -P assword -p hunter2 ssh h`. Group 1, the command and the flags, stays. A `-p` of any
     * other command is not matched; in nf-core it is mostly a thread count. A value that starts with
     * `-`, `$` or `[` is no value, as for {@link #SECRET_FLAG}.
     */
    static final Pattern SSHPASS = Pattern.compile(
        '(?<![\\w-])(sshpass(?:[ \\t]+(?:-v+|-P[ \\t]*(?:\'[^\'\\n]*\'|"[^"\\n]*"|[^\\s\'"]+)))*[ \\t]+-p[ \\t]*)' +
        '(?![\"\']?[-$\\[])(\'[^\'\\n]+\'|"[^"\\n]+"|[^\\s\'"]+)')
    /**
     * The value of a `Cookie` or `Set-Cookie` header, in any case: its `name=value` pairs and the
     * `;` between them, up to a space not after a `;`, a quote or the end (`Cookie: a=1; b=2`,
     * `Set-Cookie: id=x; Path=/; HttpOnly`). The header may be a quoted JSON key with a quoted value
     * (`{"Cookie": "sid=x"}`). Group 1, the header name, `:` and any quotes, stays. The value must
     * open with `name=`, so prose (`cookie: chocolate chip`) and the marker stay.
     */
    static final Pattern COOKIE = Pattern.compile(
        '(?<![\\w-])((?i:set-)?(?i:cookie)[\"\']?[ \\t]*:[ \\t]*[\"\']?)(?=[^\\s\'";=]+=)[^\\s\'";]+(?:;[ \\t]*[^\\s\'";]+)*+')
    /**
     * The cookie curl sends, `-b <value>` or `--cookie <value>` (`--cookie=<value>` too), when the value
     * opens with `name=`: `curl -b 'sid=x; a=1'`, `curl --cookie sid=x`. Group 1, the flag and its
     * separator, stays; a quoted value keeps its quotes. A value with no `=` before a space or a `;` is
     * a cookie file (`-b cookies.txt`) and stays, and so does another tool's `-b` with a file or no value.
     */
    static final Pattern CURL_COOKIE = Pattern.compile(
        '(?<![\\w-])((?:-b[ \\t]+|--cookie(?:=|[ \\t]+)))(?=[\"\']?[^\\s\'";=]+=)(\'[^\'\\n]+\'|"[^"\\n]+"|[^\\s\'"]+)')
    /**
     * A bare `name=value` or `name: value` inside a string, as a params string or a quoted config
     * value holds one: `user=bob password=hunter2`, a libpq `host=h password=x`, `-Dx=a,token=abc`,
     * `password: hunter2`, a header `x-api-key: abc` or `Authorization: token abc`. Group 1 is the
     * name, the separator and, after a `:`, a `Bearer`, `Basic`, `token` or `Digest` scheme word; group
     * 2 the name, whose last segment must be a secret word ({@link #endsInSecretWord}), so
     * `SLIDINGWINDOW:4:15`, `ktrim=r`, `max_tokens=10` and `monkey: 1` stay. A scheme word is taken
     * whole or not at all, so `Authorization: Bearer [secret]` is left as it is. A name right after a
     * word character, `.`, `-`, `/`, `$` or a marker's `]` is part of something else (a flag, a
     * property, a path, a variable). The pattern itself asks for a secret word at the name's end, so a
     * name that ends in none never has its value scanned (`a=a=a=...` stays linear). A bare value takes
     * a `,` or `;` unless a space, a quote, the end or another `name=` follows it, and a closing bracket
     * only inside it (`(token=x)` keeps its `)`); it is possessive, so a value of any length costs
     * linear time. A value that starts like another flag, a variable, a marker or a location is left to
     * its own rule.
     */
    static final Pattern BARE_SECRET = Pattern.compile(
        '(?<![\\w.\\-/$\\]])((?=[A-Za-z_])([\\w.-]*?(?i:authorization|passphrase|password|passwd|credentials?|secret|token|auth|pass|pwd|pat|key))' +
        '(?:=|[ \\t]*:[ \\t]*(?:(?i:bearer|basic|token|digest)[ \\t]+)?+))' +
        '(?![\"\']?(?:[$\\[/~\\\\]|[A-Za-z]:[\\\\/]|[A-Za-z][\\w+.-]*://))(?:(?<==)|(?![\"\']?-))' +
        '(\'[^\'\\n]+\'|"[^"\\n]+"|' +
        '(?:[^\\s\'",;&)\\]}]|[)\\]}](?![\\s\'",;&)\\]}]|\\z)|[,;](?![\\s\'"]|\\z|[^\\s\\w,;]*+[A-Za-z_][\\w.-]*=))++)')
    /** A JSON Web Token: base64url of `{"`, then three dot-separated segments. */
    static final Pattern JWT_TOKEN = Pattern.compile('(?<![\\w-])eyJ[\\w-]+\\.[\\w-]+\\.[\\w-]*')
    static final Pattern SLACK = Pattern.compile('(?<![\\w-])xox[abpr]-[A-Za-z0-9-]{10,}')
    static final Pattern GITLAB = Pattern.compile('(?<![\\w-])glpat-[\\w-]{20,}')
    static final Pattern HUGGING_FACE = Pattern.compile('(?<![\\w-])hf_[A-Za-z0-9]{30,}')
    /** A Google API key: `AIza` and exactly 35 letters, digits, `_` or `-`. */
    static final Pattern GOOGLE_API = Pattern.compile('(?<![\\w-])AIza[0-9A-Za-z_-]{35}(?![\\w-])')
    /** An Anthropic API key: `sk-ant-` and 20 or more letters, digits, `_` or `-`. */
    static final Pattern ANTHROPIC = Pattern.compile('(?<![\\w-])sk-ant-[A-Za-z0-9_-]{20,}')
    /**
     * An OpenAI API key: `sk-proj-`, `sk-svcacct-` or `sk-admin-` and 20 or more letters, digits, `_`
     * or `-`; or `sk-` and 32 or more letters and digits, with both, standing alone. `sk-learn` and a
     * word ending in `sk` (`task-...`, `risk-...`) stay.
     */
    static final Pattern OPENAI = Pattern.compile(
        '(?<![\\w-])sk-(?:(?:proj|svcacct|admin)-[A-Za-z0-9_-]{20,}|(?=[A-Za-z0-9]*[0-9])(?=[A-Za-z0-9]*[A-Za-z])[A-Za-z0-9]{32,}(?![\\w-]))')
    /** The `sig=` value of an Azure SAS query string (`?sv=...&sig=...`). Group 1, `sig=`, stays. */
    static final Pattern SAS_SIG = Pattern.compile('(?<=[?&])(sig=)[A-Za-z0-9%+/=]+')
    /**
     * The shape of an AWS secret access key: exactly 40 characters of `[A-Za-z0-9/+=]`, with an
     * upper-case letter, a lower-case letter and a digit, `/` or `+`, standing alone. A hex digest
     * (a git SHA, md5, sha1) has one case only, a CID is lower case, so both stay; a run that
     * starts with `/` is an absolute path and is judged as one.
     */
    static final Pattern AWS_SECRET = Pattern.compile(
        '(?<![A-Za-z0-9/+_.-])(?!/)(?=[A-Za-z0-9/+=]{0,39}[A-Z])(?=[A-Za-z0-9/+=]{0,39}[a-z])' +
        '(?=[A-Za-z0-9/+=]{0,39}[0-9/+])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=_.-])')

    /** One shape of secret in text. The match is the secret, less group 1 when `keepsGroup1`. */
    @CompileStatic
    private static final class Rule {
        final Pattern pattern
        final String kind
        final boolean keepsGroup1
        /** The group holding a name that must be secret-named for a match to count, or 0. */
        final int nameGroup
        /** The group holding a flag's separator, or 0: after a space the name's last segment decides. */
        final int separatorGroup
        /** True when the secret counts only if it decodes to user-pass ({@link Leaks#isUserPass}). */
        final boolean userPass
        /** True when the name counts only if its last segment is a secret word, wherever its value is. */
        final boolean lastSegment

        Rule(Pattern pattern, String kind, boolean keepsGroup1, int nameGroup) {
            this(pattern, kind, keepsGroup1, nameGroup, 0, false)
        }

        Rule(Pattern pattern, String kind, boolean keepsGroup1, int nameGroup, int separatorGroup, boolean userPass) {
            this(pattern, kind, keepsGroup1, nameGroup, separatorGroup, userPass, false)
        }

        Rule(Pattern pattern, String kind, boolean keepsGroup1, int nameGroup, int separatorGroup, boolean userPass, boolean lastSegment) {
            this.lastSegment = lastSegment
            this.pattern = pattern
            this.kind = kind
            this.keepsGroup1 = keepsGroup1
            this.nameGroup = nameGroup
            this.separatorGroup = separatorGroup
            this.userPass = userPass
        }
    }

    /**
     * In the order they are applied: a PEM block first, since its body holds what looks like
     * other things, then the secret-named flag, `sshpass -p`, the JSON key, the cookie header and
     * curl's cookie (whose values may have any shape), then the token shapes, the AWS secret-key shape last.
     */
    private static final List<Rule> RULES = [
        new Rule(PEM_BLOCK, PRIVATE_KEY, false, 0),
        new Rule(SECRET_FLAG, SECRET_ASSIGNMENT, true, 2, 3, false),
        new Rule(SSHPASS, SECRET_ASSIGNMENT, true, 0),
        new Rule(SYSTEM_PROPERTY, SECRET_ASSIGNMENT, true, 2),
        new Rule(JSON_SECRET, SECRET_ASSIGNMENT, true, 3),
        new Rule(COOKIE, SECRET_ASSIGNMENT, true, 0),
        new Rule(CURL_COOKIE, SECRET_ASSIGNMENT, true, 0),
        new Rule(AWS_KEY, AWS_KEY_ID, false, 0),
        new Rule(GITHUB, GITHUB_TOKEN, false, 0),
        new Rule(SEQERA, SEQERA_TOKEN, false, 0),
        new Rule(BEARER, BEARER_TOKEN, true, 0),
        new Rule(BASIC, BASIC_AUTH, true, 0, 0, true),
        new Rule(JWT_TOKEN, JWT, false, 0),
        new Rule(SLACK, SLACK_TOKEN, false, 0),
        new Rule(GITLAB, GITLAB_TOKEN, false, 0),
        new Rule(HUGGING_FACE, HUGGING_FACE_TOKEN, false, 0),
        new Rule(GOOGLE_API, GOOGLE_API_KEY, false, 0),
        new Rule(ANTHROPIC, ANTHROPIC_KEY, false, 0),
        new Rule(OPENAI, OPENAI_KEY, false, 0),
        new Rule(SAS_SIG, AZURE_SAS, true, 0),
        new Rule(AWS_SECRET, AWS_SECRET_KEY, false, 0),
        // Last, so a token shape in a value (`token=Bearer x`, `password=AKIA...`) is judged first.
        new Rule(BARE_SECRET, SECRET_ASSIGNMENT, true, 2, 0, false, true),
    ].asImmutable()

    /** Quotes and brackets that close a value, set aside before a token is judged. */
    static final String TRAILING_PUNCT = ':;,.)]}>\'"'
    /** Quotes and brackets that open a value, as in `workDir = '/x'` or `['/a', '/b']`. */
    static final String LEADING_PUNCT = '\'"([{<='

    /** One secret found in text: the line it starts on (from 1) and its kind. */
    @CompileStatic
    static final class Hit {
        final int line
        final String what

        Hit(int line, String what) {
            this.line = line
            this.what = what
        }
    }

    /** Every secret in the text replaced by the secret marker, quoted as the secret was. */
    static String redactCredentials(String text) {
        return redact(text, false, null)
    }

    /**
     * The text as {@link #redactCredentials} leaves it, except that each marker keeps the line
     * breaks of what it replaced, so a line number in the result is one in the original. Adds a
     * {@link Hit} for each secret to `hits`. The audit reads text through this, so it finds exactly
     * what the scrub removes.
     */
    static String maskCredentials(String text, Collection<Hit> hits) {
        return redact(text, true, hits)
    }

    private static String redact(String text, boolean keepLines, Collection<Hit> hits) {
        String out = text
        for( Rule rule : RULES ) {
            final Matcher m = rule.pattern.matcher(out)
            StringBuilder sb = null
            int last = 0
            int from = 0
            while( from <= out.length() && m.find(from) ) {
                final String what = kindOf(rule, m)
                from = resumeAt(rule, m, what)
                if( what == null )
                    continue
                final int start = rule.keepsGroup1 ? m.end(1) : m.start()
                if( sb == null )
                    sb = new StringBuilder(out.length())
                hits?.add(new Hit(lineOf(out, start), what))
                sb.append(out, last, start).append(marker(out.substring(start, m.end()), keepLines))
                last = m.end()
            }
            if( sb != null )
                out = sb.append(out, last, out.length()).toString()
        }
        return out
    }

    /**
     * Where the search goes on after a match. A match whose name is not secret-named
     * (`--extra "foo --password x"`) goes on at its value, so a secret inside the value is seen.
     */
    private static int resumeAt(Rule rule, Matcher m, String what) {
        return what == null && rule.keepsGroup1 ? m.end(1) : m.end()
    }

    /**
     * The kind a match of the rule counts as, or null when it is no secret. A value that is a plain
     * boolean or number ({@link #PLAIN}) is never one, whatever found it. A flag with its value after
     * `=` counts when the name is secret-named ({@link #isSecretKey}); one with its value in the next
     * token only when the name's last segment is a secret word ({@link #endsInSecretWord}), so
     * `--sort-keys in.json` and `--skip-auth x.bam` stay. A value a secret name keeps
     * ({@link #keepsValue}) does not count either: `--pat GATC`, `PWD=/home/x`.
     */
    private static String kindOf(Rule rule, Matcher m) {
        if( rule.userPass )
            return isUserPass(valueOf(m)) ? rule.kind : null
        if( rule.keepsGroup1 && PLAIN.matcher(unquoted(valueOf(m))).matches() )
            return null
        if( rule.nameGroup == 0 )
            return rule.kind
        final String name = m.group(rule.nameGroup)
        final boolean nextToken = rule.lastSegment || (rule.separatorGroup > 0 && m.group(rule.separatorGroup) != '=')
        if( secretWords(name, nextToken).isEmpty() || keepsValue(name, valueOf(m)) )
            return null
        return isSeqeraTokenKey(name) ? SEQERA_TOKEN : rule.kind
    }

    /**
     * True when a value under a secret-named name is no secret all the same, and is judged as any
     * other value is: a plain boolean or number ({@link #PLAIN}: `true`, `8`, `0.05`, `-1`, `1e-5`), a
     * marker the scrub writes, a location the path rule takes whole ({@link #isWholeLocation}:
     * `PWD=/home/x`, `s3://b/x`), or, under a name that is `pat` alone ({@link #isPatternName}), a
     * nucleotide word (`--pat GATC`, `params.pat = 'GATC'`). A quoted string is judged without its
     * quotes; a Boolean or a Number is plain. The scrub and the audit both ask this, of a flag, a
     * bare `name=value`, a JSON key, a params or config key and each element of a list under one.
     */
    static boolean keepsValue(String name, Object value) {
        if( value instanceof Boolean )
            return true
        if( value instanceof Number )
            return PLAIN.matcher(plainText((Number) value)).matches()
        if( !(value instanceof CharSequence) )
            return false
        final String text = unquoted(value.toString())
        return PLAIN.matcher(text).matches() || MARKERS.contains(text) ||
            isWholeLocation(text) ||
            (isPatternName(name) && NUCLEOTIDES.matcher(text).matches())
    }

    /**
     * A plain boolean or number, in any case: `true`, `False`, `0`, `8`, `0.05`, `-1`, `+3`, `1e-5`,
     * `.5`. A whole number has at most three digits: a longer one can be a PIN or a numeric password,
     * and secrets beat false positives (P4 second follow-up), so `password=12345678` still goes.
     */
    private static final Pattern PLAIN = Pattern.compile(
        '(?i:true|false)|[-+]?[0-9]{1,3}|[-+]?[0-9]*\\.[0-9]+|[-+]?(?:[0-9]{1,3}|[0-9]*\\.[0-9]+)[eE][-+]?[0-9]{1,3}')

    /**
     * A Number as the text PLAIN judges: a whole number as its digits; an integral Double, Float or
     * BigDecimal of 1000 or more as its digits too, so `12345678d` and `1.2345678E+7` are as secret
     * as `12345678` (P4 third follow-up, S7); any other in plain decimal or exponent form.
     */
    private static String plainText(Number n) {
        if( n instanceof Integer || n instanceof Long || n instanceof Short || n instanceof Byte || n instanceof BigInteger )
            return n.toString()
        BigDecimal exact = null
        try {
            exact = n instanceof BigDecimal ? (BigDecimal) n : new BigDecimal(n.toString())
        }
        catch( NumberFormatException ignored ) {
            // NaN, Infinity: not a plain number
            return n.toString()
        }
        if( exact.abs() >= 1000G && exact.stripTrailingZeros().scale() <= 0 )
            return exact.toBigInteger().toString()
        final String text = n.toString()
        return text.contains('.') || text.toLowerCase(Locale.ROOT).contains('e') ? text : text + '.0'
    }

    /**
     * True when the path rule replaces all of the text with one location marker: `/home/x`, `~/x`,
     * `s3://b/x`. Text it would judge in parts is not, so a secret after a path keeps its own rule:
     * `/x:hunter2` (a path list), `/x hunter2` (two tokens in config text).
     */
    private static boolean isWholeLocation(String text) {
        return isLocation(text) && !WHITESPACE.matcher(text).find() && scanToken(text, null, null) == Records.REDACTED_LOCATION
    }

    private static final Pattern WHITESPACE = Pattern.compile('\\s')

    private static final Set<String> MARKERS = Collections.unmodifiableSet(
        [Records.REDACTED_SECRET, Records.REDACTED_LOCATION, Records.REDACTED_USER] as Set<String>)

    /**
     * True when the name is `pat` alone, after the last `.` and any leading `-`: `--pat`, `pat`,
     * `params.pat`, `env.PAT`. A longer name says whose token it is (`GITHUB_PAT`, `adapter-pat`).
     */
    private static boolean isPatternName(String name) {
        final String last = name.substring(name.lastIndexOf('.') + 1)
        int start = 0
        while( start < last.length() && last.charAt(start) == (char) '-' )
            start++
        return last.substring(start).equalsIgnoreCase('pat')
    }

    /**
     * A sequence or a motif: a word of upper-case IUPAC nucleotide codes (`GATC`, `AGATCGGAAGAGC`,
     * `RYKM`), or of lower-case `acgtun` (`gatc`). A token has digits, `_`, mixed case or other letters,
     * so `--pat ghp_...`, `--pat abc123` and `--pat abc` go.
     */
    private static final Pattern NUCLEOTIDES = Pattern.compile('[ACGTUNRYKMSWBDHV]+|[acgtun]+')

    /** The match less its group 1: the secret a rule that keeps its group 1 found. */
    private static String valueOf(Matcher m) {
        return m.group().substring(m.end(1) - m.start())
    }

    /** The text without one pair of matching quotes around it. */
    private static String unquoted(String text) {
        final boolean quoted = text.length() >= 2 && (text.charAt(0) == (char) '\'' || text.charAt(0) == (char) '"') &&
            text.charAt(text.length() - 1) == text.charAt(0)
        return quoted ? text.substring(1, text.length() - 1) : text
    }

    /**
     * True when the value decodes as base64 to RFC 7617 user-pass: well-formed UTF-8 with no
     * control character, holding a `:` with at least one character before it. The `=` padding
     * may be missing; a length of 1 more than a multiple of 4 is no base64. Strict UTF-8 keeps
     * capitalised words such as `Overview` and `Options`, whose bytes start 0x80 or more and then
     * a `:`, out, and a non-ASCII credential (`jörg:pässwörd`) in.
     */
    static boolean isUserPass(String value) {
        int end = value.length()
        while( end > 0 && value.charAt(end - 1) == (char) '=' )
            end--
        if( end % 4 == 1 )
            return false
        final StringBuilder padded = new StringBuilder(value.substring(0, end))
        while( padded.length() % 4 != 0 )
            padded.append('=')
        final String text
        try {
            final byte[] bytes = Base64.getDecoder().decode(padded.toString())
            text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString()
        }
        catch( IllegalArgumentException | CharacterCodingException ignored ) {
            return false
        }
        for( int i = 0; i < text.length(); i++ ) {
            final char c = text.charAt(i)
            if( c < (char) 0x20 || c == (char) 0x7f )
                return false
        }
        return text.indexOf(':') > 0
    }

    /**
     * True when the name's last segment (split as {@link #isSecretKey} splits) names a secret, as
     * {@link #isSecretKey} judges a segment, except that a plural does not count: `api-key`, `apiKey`,
     * `password`, `db-pass`, `basic-auth`, `Authorization`, `accesstoken` do, and `sort-keys`,
     * `max-tokens`, `--skip-auth x.bam`, `--low-pass 0.5` and `password-file` do not. This is the rule
     * for a flag whose value is the next token and for a bare `name=value`, where the name alone says
     * what follows.
     */
    static boolean endsInSecretWord(String name) {
        return !secretWords(name, true).isEmpty()
    }

    /** The secret marker, in the quotes the secret had, followed by its line breaks when `keepLines`. */
    private static String marker(String secret, boolean keepLines) {
        final char first = secret.charAt(0)
        final boolean quoted = secret.length() >= 2 && (first == (char) '"' || first == (char) '\'') &&
            secret.charAt(secret.length() - 1) == first
        final String q = quoted ? String.valueOf(first) : ''
        final StringBuilder out = new StringBuilder(q).append(Records.REDACTED_SECRET).append(q)
        if( keepLines ) {
            for( int i = 0; i < secret.length(); i++ ) {
                if( secret.charAt(i) == (char) '\n' )
                    out.append('\n')
            }
        }
        return out.toString()
    }

    /** The line, from 1, that the offset falls on. */
    static int lineOf(String text, int offset) {
        int line = 1
        for( int i = 0; i < offset; i++ ) {
            if( text.charAt(i) == (char) '\n' )
                line++
        }
        return line
    }

    /** The credential kind of a substring found in the text, or null. */
    static String credentialKind(String text) {
        if( PEM_HEADER.matcher(text).find() )
            return PRIVATE_KEY
        for( Rule rule : RULES ) {
            final Matcher m = rule.pattern.matcher(text)
            int from = 0
            while( from <= text.length() && m.find(from) ) {
                final String what = kindOf(rule, m)
                if( what != null )
                    return what
                from = resumeAt(rule, m, what)
            }
        }
        return null
    }

    /**
     * A path that is not `/`-rooted but names a place on one machine all the same: a home path
     * (`~/x`, `~user/x`), a Windows drive path (`C:\x`) or a UNC path (`\\host\share`). In config
     * text a backslash is written doubled, which the pattern also takes. A UNC host and share have
     * two characters or more each, so the regex escapes `\\d\\d`, `\\bword\\b` and `\\w\\s` stay.
     */
    private static final Pattern OTHER_PATH = Pattern.compile(
        '^(?:~(?:[A-Za-z_][\\w.-]*)?/|[A-Za-z]:\\\\|\\\\{2,}[A-Za-z0-9][\\w.-]+\\\\+[^\\\\\\s]{2})')
    private static final Pattern DRIVE = Pattern.compile('^[A-Za-z]:\\\\')

    /**
     * `absolute path` for a piece that starts as a path on one machine (`/x`, `~/x`, `C:\x`,
     * `\\host\share`), `uri` for one in a scheme other than lid/cas, or null.
     */
    static String locationKind(String piece) {
        return locationKindAt(piece, 0)
    }

    /** {@link #locationKind} of the piece from offset `at` to its end, without copying it. */
    private static String locationKindAt(String piece, int at) {
        if( piece.startsWith('/', at) || lookingAt(OTHER_PATH, piece, at) )
            return ABSOLUTE_PATH
        final Matcher m = SCHEME.matcher(piece).region(at, piece.length())
        if( m.lookingAt() && !PORTABLE_SCHEMES.contains(m.group(1).toLowerCase()) )
            return URI
        return null
    }

    /** True when the pattern matches the piece from offset `at`, as it would match the piece's tail on its own. */
    private static boolean lookingAt(Pattern pattern, String piece, int at) {
        return pattern.matcher(piece).region(at, piece.length()).lookingAt()
    }

    /** True when the whole string names a place on some machine, and goes whole. */
    static boolean isLocation(String text) {
        return locationKind(text) != null
    }

    /**
     * The key each entry of a map is written under. A key whose text the scrub would change
     * ({@link #keyMarker}) becomes the marker for what it held, then the marker with `-2`, `-3`
     * and so on, skipping any key the map keeps, so no entry is lost. Every other key is itself.
     * The scrub writes the map with these keys and the audit names a finding by them.
     */
    static Map<Object, Object> keyNames(Collection<?> keys) {
        final Map<Object, String> markers = new LinkedHashMap<Object, String>()
        final Set<String> taken = new HashSet<String>()
        for( Object key : keys ) {
            final String marker = keyMarker(String.valueOf(key))
            markers.put(key, marker)
            if( marker == null )
                taken.add(String.valueOf(key))
        }
        final Map<String, Integer> numbers = new HashMap<String, Integer>()
        final Map<Object, Object> out = new LinkedHashMap<Object, Object>()
        for( Map.Entry<Object, String> entry : markers.entrySet() ) {
            final String marker = entry.value
            if( marker == null ) {
                out.put(entry.key, entry.key)
                continue
            }
            int n = numbers.containsKey(marker) ? numbers.get(marker) : 1
            String name = n == 1 ? marker : marker + '-' + n
            while( taken.contains(name) )
                name = marker + '-' + (++n)
            numbers.put(marker, n)
            taken.add(name)
            out.put(entry.key, name)
        }
        return out
    }

    /**
     * The marker a map key is renamed to, or null when the scrub ({@link Records#scrub}) leaves
     * its text as it is: `[secret]` for a key that held a credential (`ghp_...`), else
     * `[redacted-location]` for one that held a path or URI (`/x/a.fq`, `--in=/x/y`), else
     * `[redacted-user]` for the OS user name.
     */
    static String keyMarker(String key) {
        final String scrubbed = String.valueOf(Records.scrub(key))
        if( scrubbed == key )
            return null
        if( count(scrubbed, Records.REDACTED_SECRET) > count(key, Records.REDACTED_SECRET) )
            return Records.REDACTED_SECRET
        if( count(scrubbed, Records.REDACTED_LOCATION) > count(key, Records.REDACTED_LOCATION) )
            return Records.REDACTED_LOCATION
        return Records.REDACTED_USER
    }

    private static int count(String text, String marker) {
        int n = 0
        for( int at = text.indexOf(marker); at >= 0; at = text.indexOf(marker, at + marker.length()) )
            n++
        return n
    }

    /**
     * The kind of one whole piece: a credential, an absolute path, a URI in a
     * scheme other than lid/cas, the OS user name, or null. Splitting at `=` and
     * `:` is {@link #scanToken}'s job.
     */
    static String classify(String piece, String user) {
        final String credential = credentialKind(piece)
        if( credential != null )
            return credential
        final String location = locationKind(piece)
        if( location != null )
            return location
        if( user != null && !user.isEmpty() && piece == user )
            return OS_USER
        return null
    }

    /**
     * Judges one whitespace-delimited token and returns it with every machine-local
     * or secret part replaced by its marker. The kinds found are added to `kinds`
     * (which may be null). The token's quotes and brackets are set aside, a path
     * list `/a:/b` has each path redacted, a path glued to a short flag (`-B/x`)
     * goes and the flag stays, and anything else is split at its first `=`, `:`,
     * `,`, `;` or `@` and each side judged again, so `--volume=/home/x:/data`,
     * `TMPDIR=/scratch/x`, `a.csv,/x/b.csv`, `@/x/args.txt` and
     * `--account=<user>` keep their shape. The OS user name is redacted where it
     * stands as a word of its own inside a piece (`../home/<user>/x`), except right
     * after an `@`, where it is a handle (`@<user>`) and is neither redacted nor
     * flagged; a path after the `@` is still a path.
     */
    static String scanToken(String token, String user, Collection<String> kinds) {
        int begin = 0
        while( begin < token.length() && LEADING_PUNCT.indexOf((int) token.charAt(begin)) >= 0 )
            begin++
        int end = token.length()
        while( end > begin && TRAILING_PUNCT.indexOf((int) token.charAt(end - 1)) >= 0 )
            end--
        if( begin == end )
            return token
        return token.substring(0, begin) + scanPiece(token.substring(begin, end), user, kinds) + token.substring(end)
    }

    /**
     * The piece judged from its start, then from each place {@link #scanToken} splits it, left
     * to right in one pass, so a long list (`a,b,c,...`) costs time in proportion to its length.
     * A credential is looked for once, in the whole piece: the rules' lookbehinds treat a split
     * character as they treat the start of text, and a rejected flag, property, JSON key or
     * Basic match cannot hide an accepted one, so a part with a credential means a whole with one.
     */
    private static String scanPiece(String piece, String user, Collection<String> kinds) {
        final String credential = credentialKind(piece)
        if( credential != null ) {
            kinds?.add(credential)
            return redactCredentials(piece)
        }
        final int n = piece.length()
        final StringBuilder out = new StringBuilder(n)
        // `afterAt` is true for the part that follows an `@`: a user name leading it is a handle.
        boolean afterAt = false
        int at = 0
        while( at < n ) {
            final String kind = placeKind(piece, at, user)
            if( kind == OS_USER && afterAt )
                return out.append(piece, at, n).toString()
            if( kind == ABSOLUTE_PATH ) {
                kinds?.add(kind)
                out.append(Records.REDACTED_LOCATION)
                // A path list or mount ends the path at a `:`; a drive letter's `:` is part of it.
                final int colon = piece.indexOf(':', lookingAt(DRIVE, piece, at) ? at + 2 : at)
                if( colon < 0 )
                    break
                out.append(':')
                at = colon + 1
                afterAt = false
                continue
            }
            if( kind != null ) {
                kinds?.add(kind)
                return out.append(kind == URI ? Records.REDACTED_LOCATION : Records.REDACTED_USER).toString()
            }
            if( lookingAt(SCHEME, piece, at) )
                return out.append(piece, at, n).toString()    // a cas:// or lid:// reference
            // A path glued to a short flag: -B/home/x, -I/usr/include.
            if( lookingAt(SHORT_FLAG_PATH, piece, at) ) {
                out.append(piece, at, at + 2)
                at += 2
                afterAt = false
                continue
            }
            int cut = at
            while( cut < n && CUTS.indexOf((int) piece.charAt(cut)) < 0 )
                cut++
            if( cut == n )
                return out.append(userIn(piece.substring(at), user, kinds, afterAt)).toString()
            int rest = cut + 1
            while( rest < n && LEADING_PUNCT.indexOf((int) piece.charAt(rest)) >= 0 )
                rest++
            out.append(userIn(piece.substring(at, cut), user, kinds, afterAt)).append(piece, cut, rest)
            afterAt = piece.charAt(cut) == (char) '@'
            at = rest
        }
        return out.toString()
    }

    /** The piece from offset `at` to its end as {@link #classify} judges it, less the credential check. */
    private static String placeKind(String piece, int at, String user) {
        final String location = locationKindAt(piece, at)
        if( location != null )
            return location
        if( user != null && !user.isEmpty() && piece.length() - at == user.length() && piece.startsWith(user, at) )
            return OS_USER
        return null
    }

    /**
     * Where {@link #scanPiece} splits a piece and judges each side again:
     * `--volume=/x`, `/a:/b`, `a.csv,/b.csv`, `x;/y`, `@/args.txt`.
     */
    private static final String CUTS = '=:,;@'

    /** A one-letter flag glued to a path. A digit is no flag: `-1/3` is a number. */
    private static final Pattern SHORT_FLAG_PATH = Pattern.compile('-[A-Za-z]/')

    /**
     * The OS user name where it stands as a word of its own in the piece (`home/rob/x`), replaced by the
     * user marker. A name that leads a piece following an `@` is a handle (`@rob`), and stays.
     */
    private static String userIn(String piece, String user, Collection<String> kinds, boolean afterAt) {
        if( user == null || user.isEmpty() || !piece.contains(user) )
            return piece
        final Matcher m = Pattern.compile('(?<![A-Za-z0-9_])' + Pattern.quote(user) + '(?![A-Za-z0-9_])').matcher(piece)
        final StringBuilder out = new StringBuilder()
        int last = 0
        boolean found = false
        while( m.find() ) {
            if( afterAt && m.start() == 0 )
                continue
            found = true
            out.append(piece, last, m.start()).append(Records.REDACTED_USER)
            last = m.end()
        }
        if( !found )
            return piece
        kinds?.add(OS_USER)
        return out.append(piece, last, piece.length()).toString()
    }

    /**
     * Where a key or flag name splits into segments: at any run of characters other than letters and
     * digits (`.`, `_`, `-`, and the brackets of a `[secret]` key) and at a camelCase boundary.
     */
    private static final String SEGMENT_SPLIT = /[^A-Za-z0-9]+|(?<=[a-z0-9])(?=[A-Z])/

    /** Segments that name a secret on their own. */
    private static final Set<String> SECRET_WORDS = Collections.unmodifiableSet([
        'key', 'secret', 'token', 'password', 'passwd', 'passphrase', 'pass', 'pwd',
        'credential', 'credentials', 'pat', 'auth', 'authorization',
    ] as Set<String>)

    /** Plurals that name a list of secrets in a key (`tokens = ['a', 'b']`), and the word each is the plural of. */
    private static final Map<String, String> PLURALS = Collections.unmodifiableMap([
        keys: 'key', secrets: 'secret', tokens: 'token', passwords: 'password', passphrases: 'passphrase',
    ] as Map<String, String>)

    /**
     * One lower-case word that ends in a secret word: `accesstoken`, `dbpassword`, `clientsecret`,
     * `apikey`. Group 1 is what comes before the word, group 2 the word. `pass`, `pwd`, `pat` and
     * `auth` end too many ordinary words (`bypass`, `compass`) to count this way, and `key` counts
     * only after one of {@link #KEY_COMPOUNDS}, so `monkey`, `turkey` and `hotkey` stay.
     */
    private static final Pattern COMPOUND = Pattern.compile(
        '([a-z0-9]+?)(tokens?|secrets?|passwords?|passwd|passphrases?|credentials?|keys?)')

    /**
     * One lower-case word, a database or login tool's name and `pass` or `pwd`, as an environment
     * variable names a password: `PGPASS`, `SSHPASS` (which `sshpass -e` reads), `MYSQLPWD`, `DBPASS`,
     * `ROOTPWD`. Group 2 is the word. The list is closed, so `compass`, `bypass`, `overpass` and
     * `oldpwd` stay.
     */
    private static final Pattern PASS_COMPOUND = Pattern.compile(
        '(pg|mysql|ssh|db|sql|mongo|redis|ldap|smb|ftp|sftp|rsync|admin|root)(pass|pwd)')

    /**
     * Data words that, right before or after `pass`, make it a QC verdict rather than a password:
     * MinKNOW's `fastq_pass`, `bam_pass` and `pod5_pass` directories, `qc_pass`, `pass_fail`.
     */
    private static final Set<String> PASS_DATA = Collections.unmodifiableSet([
        'fail', 'failed', 'qc', 'fastq', 'fq', 'bam', 'cram', 'sam', 'vcf', 'bcf', 'pod5', 'fast5',
    ] as Set<String>)

    /**
     * Words that, right after `pass`, make it a QC verdict: the data words ({@link #PASS_DATA}), and a
     * filter, only or read word (`pass_filter`, `vcf_pass_only`, `pass_reads`). Before `pass` a filter,
     * only or read word qualifies nothing, so `FILTER_PASS`, `READ_ONLY_PASS` and `DB_READ_PASS` name a
     * password: a secret beats a false positive.
     */
    private static final Set<String> PASS_VERDICT = Collections.unmodifiableSet(
        (PASS_DATA + ['filter', 'filters', 'filtered', 'only', 'read', 'reads']) as Set<String>)

    /** What may come before `key` in one lower-case word that names a key: `apikey`, `sshkey`, `privatekey`. */
    private static final Set<String> KEY_COMPOUNDS = Collections.unmodifiableSet([
        'api', 'access', 'secret', 'private', 'ssh', 'account', 'master', 'client', 'auth', 'encryption',
        'signing', 'license', 'storage', 'shared', 'sas',
    ] as Set<String>)

    /**
     * Segments that, right before a secret word, make the name a switch, a count or an order rather
     * than a secret: `skip_auth`, `max_tokens`, `sort_keys`, `primary_key`, `low_pass`, `two_pass`,
     * and a data word before `pass` ({@link #PASS_DATA}): `fastq_pass`.
     */
    private static final Map<String, Set<String>> QUALIFIERS = Collections.unmodifiableMap([
        auth : ['skip', 'no', 'disable', 'disabled', 'without'] as Set<String>,
        key  : ['sort', 'skip', 'keep', 'max', 'min', 'num', 'n', 'count', 'total', 'join', 'group', 'merge',
                'index', 'primary', 'foreign', 'split'] as Set<String>,
        token: ['sort', 'skip', 'keep', 'max', 'min', 'num', 'n', 'count', 'total', 'join', 'group', 'merge',
                'index', 'primary', 'foreign', 'split'] as Set<String>,
        pass : (['low', 'high', 'band', 'first', 'second', 'two', 'multi', 'single', 'final', 'one'] + PASS_DATA) as Set<String>,
    ] as Map<String, Set<String>>)

    /**
     * Segments that, right after a secret word, make the name say where a secret is kept rather than
     * hold it: `key_file`, `password_file_name`, `ssh_key_path`, `secrets_dir`. The value is then
     * judged as any other (an absolute path still goes as a location).
     */
    private static final Set<String> REFERENCES = Collections.unmodifiableSet([
        'file', 'files', 'filename', 'filepath', 'path', 'paths', 'dir', 'dirs', 'directory',
    ] as Set<String>)

    /**
     * True when a key, a config path or a flag name names a secret: one of its segments (split at
     * `.`, `_`, `-`, any other punctuation and camelCase) names one ({@link #secretWordAt}). So
     * `api_key`, `apiKey`, `ssh-key`, `key`, `DB_PASS`, `MYSQL_PWD`, `PGPASS`, `GITHUB_PAT`,
     * `env.API_AUTH`, `tokens`, `apikey` and `FILTER_PASS` are secret-named, and `monkey_reads`, `keystone`,
     * `turkey`, `keyfile`, `keep_tokens`, `bypass_qc`, `fastq_pass`, `pass_filter`, `author`, `path` and
     * `pattern` are not.
     */
    static boolean isSecretKey(String key) {
        return !secretWords(key, false).isEmpty()
    }

    /**
     * The secret words the name's segments name ({@link #secretWordAt}): its last segment's alone when
     * `lastSegment`, plurals not counting, as {@link #endsInSecretWord} judges; else every segment's, as
     * {@link #isSecretKey} judges. Empty when the name names no secret.
     */
    private static Set<String> secretWords(String name, boolean lastSegment) {
        final String[] segments = name.split(SEGMENT_SPLIT)
        final Set<String> words = new HashSet<String>()
        for( int i = lastSegment ? segments.length - 1 : 0; i >= 0 && i < segments.length; i++ ) {
            final String word = secretWordAt(segments, i, !lastSegment)
            if( word != null )
                words.add(word)
        }
        return words
    }

    /**
     * The secret word segment `i` names, or null: it is a secret word ({@link #SECRET_WORDS}), a plural
     * of one when `plurals` ({@link #PLURALS}), one lower-case word ending in one ({@link #COMPOUND}), or
     * a database or login tool's name and `pass` or `pwd` ({@link #PASS_COMPOUND}). It does not count
     * after a qualifier ({@link #QUALIFIERS}; in a one-word compound, what comes before the word is its
     * qualifier, so `maxtokens` stays), before a reference ({@link #REFERENCES}), or, for `pass`, before
     * a QC word ({@link #PASS_VERDICT}).
     */
    private static String secretWordAt(String[] segments, int i, boolean plurals) {
        final String segment = segments[i].toLowerCase(Locale.ROOT)
        String before = i > 0 ? segments[i - 1].toLowerCase(Locale.ROOT) : null
        String word = null
        if( SECRET_WORDS.contains(segment) )
            word = segment
        else if( PLURALS.containsKey(segment) )
            word = plurals ? PLURALS.get(segment) : null
        else {
            final Matcher m = COMPOUND.matcher(segment)
            if( m.matches() ) {
                final String end = m.group(2)
                final String singular = PLURALS.containsKey(end) ? PLURALS.get(end) : end
                final boolean counts = (plurals || !PLURALS.containsKey(end)) &&
                    (singular != 'key' || KEY_COMPOUNDS.contains(m.group(1)))
                if( counts ) {
                    word = singular
                    before = m.group(1)
                }
            }
            else {
                final Matcher pass = PASS_COMPOUND.matcher(segment)
                if( pass.matches() ) {
                    word = pass.group(2)
                    before = pass.group(1)
                }
            }
        }
        if( word == null )
            return null
        final String after = i + 1 < segments.length ? segments[i + 1].toLowerCase(Locale.ROOT) : null
        if( after != null && (REFERENCES.contains(after) || (word == 'pass' && PASS_VERDICT.contains(after))) )
            return null
        final Set<String> qualifiers = QUALIFIERS.get(word == 'credentials' ? 'credential' : word)
        return before == null || qualifiers == null || !qualifiers.contains(before) ? word : null
    }

    /**
     * A URI's text as scheme, host and path only: its userinfo (`user:pass@`, HTTP basic auth),
     * query and fragment (where a presigned URL's `X-Amz-Signature` and an Azure SAS token's `sig`
     * live) are dropped. Text with no `<scheme>://` is returned as it is. An InputSet name is cut from
     * this (DESIGN.md §22), so it keeps no credential a URI carried.
     */
    static String withoutCredentials(String uri) {
        final Matcher scheme = SCHEME.matcher(uri)
        if( !scheme.lookingAt() )
            return uri
        String rest = uri.substring(scheme.end())
        final int query = firstOf(rest, (char) '?', (char) '#')
        if( query >= 0 )
            rest = rest.substring(0, query)
        final int slash = rest.indexOf('/')
        String authority = slash < 0 ? rest : rest.substring(0, slash)
        final String path = slash < 0 ? '' : rest.substring(slash)
        final int at = authority.lastIndexOf('@')
        if( at >= 0 )
            authority = authority.substring(at + 1)
        return uri.substring(0, scheme.end()) + authority + path
    }

    /** The first index of either character in the text, or -1. */
    private static int firstOf(String text, char a, char b) {
        for( int i = 0; i < text.length(); i++ ) {
            final char c = text.charAt(i)
            if( c == a || c == b )
                return i
        }
        return -1
    }

    /** True when a key names a Seqera Platform token (`tower.accessToken`, `SEQERA_TOKEN`). */
    static boolean isSeqeraTokenKey(String key) {
        final String lower = key.toLowerCase()
        return lower.contains('token') && (lower.contains('tower') || lower.contains('seqera'))
    }
}
