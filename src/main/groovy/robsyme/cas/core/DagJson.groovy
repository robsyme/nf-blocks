// src/main/groovy/robsyme/cas/core/DagJson.groovy
package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

import groovy.transform.CompileStatic

/**
 * DAG-JSON (block explorer spec section 9.1), the write path's wire format:
 * links as {"/": "<cid>"}, bytes as {"/": {"bytes": "<base64>"}}. Strict where
 * a request is untrusted: a map with a "/" key that is neither form, a
 * duplicate key, text that is not UTF-8 and nesting past {@link #MAX_DEPTH}
 * are refused, each naming where (a JSON pointer). Encoding is canonical as
 * @ipld/dag-json 11 writes it: keys by UTF-8 bytes, no whitespace.
 */
@CompileStatic
final class DagJson {

    static final int MAX_DEPTH = 64

    static class DagJsonException extends IllegalArgumentException {
        final String at
        DagJsonException(String message, String at) {
            super(message)
            this.at = at
        }
    }

    private DagJson() {}

    // ------------------------------------------------------------------ decode

    static Object decode(byte[] utf8) {
        final String text
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(utf8)).toString()
        }
        catch( CharacterCodingException e ) {
            throw new DagJsonException('the request is not UTF-8 text', '')
        }
        return decode(text)
    }

    static Object decode(String text) {
        final Parser parser = new Parser(text)
        parser.skipSpace()
        final Object value = parser.value('', 0)
        parser.skipSpace()
        if( parser.pos != text.length() )
            throw new DagJsonException("unexpected text after the document at offset ${parser.pos}", '')
        return value
    }

    @CompileStatic
    private static final class Parser {
        final String s
        int pos = 0

        Parser(String s) { this.s = s }

        void skipSpace() {
            while( pos < s.length() ) {
                final char c = s.charAt(pos)
                if( c == (char) ' ' || c == (char) '\t' || c == (char) '\n' || c == (char) '\r' ) pos++
                else break
            }
        }

        DagJsonException fail(String message, String at) {
            return new DagJsonException("${message} at offset ${pos}".toString(), at)
        }

        Object value(String at, int depth) {
            if( depth > MAX_DEPTH )
                throw new DagJsonException("the request is nested deeper than ${MAX_DEPTH}", at)
            if( pos >= s.length() )
                throw fail('the document ended early', at)
            final char c = s.charAt(pos)
            switch( c ) {
                case (char) '{': return object(at, depth)
                case (char) '[': return array(at, depth)
                case (char) '"': return string(at)
                case (char) 't': return literal('true', Boolean.TRUE, at)
                case (char) 'f': return literal('false', Boolean.FALSE, at)
                case (char) 'n': return literal('null', null, at)
                default:
                    if( c == (char) '-' || (c >= (char) '0' && c <= (char) '9') )
                        return number(at)
                    throw fail("unexpected character '${c}'", at)
            }
        }

        Object literal(String word, Object result, String at) {
            if( !s.startsWith(word, pos) )
                throw fail("expected ${word}", at)
            pos += word.length()
            return result
        }

        Object number(String at) {
            final int start = pos
            if( s.charAt(pos) == (char) '-' ) pos++
            if( pos >= s.length() ) throw fail('a number ended early', at)
            if( s.charAt(pos) == (char) '0' ) {
                pos++
                if( pos < s.length() && Character.isDigit(s.charAt(pos)) )
                    throw fail('a number may not start with 0', at)
            }
            else {
                if( !Character.isDigit(s.charAt(pos)) ) throw fail('expected a digit', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            boolean floating = false
            if( pos < s.length() && s.charAt(pos) == (char) '.' ) {
                floating = true
                pos++
                if( pos >= s.length() || !Character.isDigit(s.charAt(pos)) ) throw fail('expected a digit after the point', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            if( pos < s.length() && (s.charAt(pos) == (char) 'e' || s.charAt(pos) == (char) 'E') ) {
                floating = true
                pos++
                if( pos < s.length() && (s.charAt(pos) == (char) '+' || s.charAt(pos) == (char) '-') ) pos++
                if( pos >= s.length() || !Character.isDigit(s.charAt(pos)) ) throw fail('expected an exponent', at)
                while( pos < s.length() && Character.isDigit(s.charAt(pos)) ) pos++
            }
            final String text = s.substring(start, pos)
            if( floating ) {
                final double d = Double.parseDouble(text)
                if( Double.isInfinite(d) || Double.isNaN(d) )
                    throw fail('a float out of range', at)
                return d
            }
            final BigInteger big = new BigInteger(text)
            return big.bitLength() < 64 ? (Object) big.longValue() : (Object) big
        }

        String string(String at) {
            pos++    // opening quote
            final StringBuilder out = new StringBuilder()
            while( true ) {
                if( pos >= s.length() ) throw fail('a string ended early', at)
                final char c = s.charAt(pos++)
                if( c == (char) '"' ) return out.toString()
                if( c < (char) 0x20 ) throw fail('an unescaped control character in a string', at)
                if( c != (char) '\\' ) {
                    out.append(c)
                    continue
                }
                if( pos >= s.length() ) throw fail('a string ended early', at)
                final char e = s.charAt(pos++)
                switch( e ) {
                    case (char) '"': out.append('"'); break
                    case (char) '\\': out.append('\\'); break
                    case (char) '/': out.append('/'); break
                    case (char) 'b': out.append('\b'); break
                    case (char) 'f': out.append('\f'); break
                    case (char) 'n': out.append('\n'); break
                    case (char) 'r': out.append('\r'); break
                    case (char) 't': out.append('\t'); break
                    case (char) 'u':
                        if( pos + 4 > s.length() ) throw fail('a \\u escape ended early', at)
                        try {
                            out.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16))
                        }
                        catch( NumberFormatException x ) {
                            throw fail('a \\u escape with a non-hex digit', at)
                        }
                        pos += 4
                        break
                    default: throw fail("an unknown escape \\${e}", at)
                }
            }
        }

        List array(String at, int depth) {
            pos++
            final List<Object> out = new ArrayList<Object>()
            skipSpace()
            if( pos < s.length() && s.charAt(pos) == (char) ']' ) {
                pos++
                return out
            }
            while( true ) {
                skipSpace()
                out.add(value("${at}/${out.size()}".toString(), depth + 1))
                skipSpace()
                if( pos >= s.length() ) throw fail('an array ended early', "${at}/${out.size()}".toString())
                final char c = s.charAt(pos++)
                if( c == (char) ']' ) return out
                if( c != (char) ',' ) throw fail("expected , or ] in an array", at)
            }
        }

        Object object(String at, int depth) {
            pos++
            final LinkedHashMap<String, Object> out = new LinkedHashMap<String, Object>()
            skipSpace()
            if( pos < s.length() && s.charAt(pos) == (char) '}' ) {
                pos++
                return out
            }
            while( true ) {
                skipSpace()
                if( pos >= s.length() || s.charAt(pos) != (char) '"' ) throw fail('expected a string key', at)
                final String key = string(at)
                final String here = "${at}/${pointer(key)}".toString()
                if( out.containsKey(key) ) throw fail("the key '${key}' appears twice", here)
                skipSpace()
                if( pos >= s.length() || s.charAt(pos++) != (char) ':' ) throw fail('expected :', here)
                skipSpace()
                out.put(key, value(here, depth + 1))
                skipSpace()
                if( pos >= s.length() ) throw fail('an object ended early', at)
                final char c = s.charAt(pos++)
                if( c == (char) '}' ) break
                if( c != (char) ',' ) throw fail('expected , or } in an object', at)
            }
            return out.containsKey('/') ? special(out, at) : out
        }

        /** {"/": "<cid>"} is a link, {"/": {"bytes": "<base64>"}} bytes; any other "/" map is refused (DESIGN.md §4). */
        Object special(Map<String, Object> map, String at) {
            final Object slash = map.get('/')
            if( map.size() == 1 && slash instanceof String ) {
                if( !Cid.isCid((String) slash) )
                    throw new DagJsonException("'${slash}' is not a CID nf-blocks can hold", at)
                return Cid.parse((String) slash)
            }
            if( map.size() == 1 && slash instanceof Map && ((Map) slash).size() == 1 && ((Map) slash).get('bytes') instanceof String ) {
                try {
                    return Base64.decoder.decode((String) ((Map) slash).get('bytes'))
                }
                catch( IllegalArgumentException e ) {
                    throw new DagJsonException('bytes that are not base64', at)
                }
            }
            throw new DagJsonException('a map with a "/" key must be a link {"/": "<cid>"} or bytes {"/": {"bytes": "..."}}', at)
        }
    }

    /** RFC 6901 escaping of one pointer segment. */
    static String pointer(String key) {
        return key.replace('~', '~0').replace('/', '~1')
    }

    // ------------------------------------------------------------------ encode

    static byte[] encode(Object value) {
        return encodeToString(value).getBytes(StandardCharsets.UTF_8)
    }

    static String encodeToString(Object value) {
        final StringBuilder out = new StringBuilder()
        write(value, out)
        return out.toString()
    }

    private static final Comparator<String> BY_UTF8 = { String a, String b ->
        final byte[] x = a.getBytes(StandardCharsets.UTF_8)
        final byte[] y = b.getBytes(StandardCharsets.UTF_8)
        return Arrays.compareUnsigned(x, y)
    } as Comparator<String>

    private static void write(Object value, StringBuilder out) {
        if( value == null ) { out.append('null'); return }
        if( value instanceof Boolean ) { out.append(value.toString()); return }
        if( value instanceof Integer || value instanceof Long || value instanceof BigInteger || value instanceof Short || value instanceof Byte ) {
            out.append(value.toString()); return
        }
        if( value instanceof Double || value instanceof Float ) {
            final double d = ((Number) value).doubleValue()
            if( Double.isNaN(d) || Double.isInfinite(d) )
                throw new IllegalArgumentException('NaN and Infinity are not DAG-JSON')
            out.append(Double.toString(d)); return
        }
        if( value instanceof CharSequence ) { string(value.toString(), out); return }
        if( value instanceof Cid ) { out.append('{"/":'); string(value.toString(), out); out.append('}'); return }
        if( value instanceof byte[] ) {
            out.append('{"/":{"bytes":')
            string(Base64.encoder.withoutPadding().encodeToString((byte[]) value), out)
            out.append('}}'); return
        }
        if( value instanceof List ) {
            out.append('[')
            boolean first = true
            for( Object element : (List) value ) {
                if( !first ) out.append(',')
                first = false
                write(element, out)
            }
            out.append(']'); return
        }
        if( value instanceof Map ) {
            final Map map = (Map) value
            final List<String> keys = new ArrayList<String>()
            for( Object key : map.keySet() ) {
                if( !(key instanceof String) ) throw new IllegalArgumentException("a DAG-JSON map key must be a string, got ${key?.getClass()?.simpleName}")
                if( key == '/' ) throw new IllegalArgumentException('a map key "/" is reserved (DESIGN.md §4)')
                keys.add((String) key)
            }
            Collections.sort(keys, BY_UTF8)
            out.append('{')
            boolean first = true
            for( String key : keys ) {
                if( !first ) out.append(',')
                first = false
                string(key, out)
                out.append(':')
                write(map.get(key), out)
            }
            out.append('}'); return
        }
        throw new IllegalArgumentException("cannot encode ${value.getClass().name} as DAG-JSON")
    }

    private static void string(String text, StringBuilder out) {
        out.append('"')
        for( int i = 0; i < text.length(); i++ ) {
            final char c = text.charAt(i)
            switch( c ) {
                case (char) '"': out.append('\\"'); break
                case (char) '\\': out.append('\\\\'); break
                case (char) '\n': out.append('\\n'); break
                case (char) '\r': out.append('\\r'); break
                case (char) '\t': out.append('\\t'); break
                case (char) '\b': out.append('\\b'); break
                case (char) '\f': out.append('\\f'); break
                default:
                    if( c < (char) 0x20 )
                        out.append(String.format('\\u%04x', (int) c))
                    else
                        out.append(c)
            }
        }
        out.append('"')
    }
}
