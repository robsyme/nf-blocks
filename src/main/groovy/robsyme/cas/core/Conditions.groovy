package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * Range and not-equal filters (DESIGN.md §12, amended 2026-10-24). A
 * {@code where:} key is a metadata path, or a path followed by {@code >},
 * {@code >=}, {@code <} or {@code <=}, which compares the value as a number,
 * or by {@code !=}, which keeps every item holding no equal value, items
 * without the path included; an {@code items} argument is {@code path=value},
 * {@code path!=value}, {@code path>=value} and so on. A range matches an
 * {@code int} or {@code float} attribute only, never text.
 */
@CompileStatic
final class Conditions {

    /** Longest first, so {@code >=} is found before {@code >}. */
    static final List<String> RANGE_OPS = ['>=', '<=', '>', '<'].asImmutable()

    static final String NOT_EQUAL = '!='

    private Conditions() {}

    /** {@code [path, op]} for a where key; {@code op} is {@code =} when the key ends in no operator. */
    static List<String> splitKey(String key) {
        final String trimmed = key.trim()
        for( String op : [NOT_EQUAL] + RANGE_OPS ) {
            if( trimmed.endsWith(op) ) {
                final String path = trimmed.substring(0, trimmed.length() - op.length()).trim()
                if( path )
                    return [path, op]
            }
        }
        return [trimmed, '=']
    }

    /** {@code [path, op, value]} split at the argument's first operator, or null when it has none. */
    static List<String> splitArg(String arg) {
        for( int i = 0; i < arg.length(); i++ ) {
            final String two = i + 2 <= arg.length() ? arg.substring(i, i + 2) : ''
            if( two == '>=' || two == '<=' || two == NOT_EQUAL )
                return [arg.substring(0, i), two, arg.substring(i + 2)]
            final char c = arg.charAt(i)
            if( c == ('>' as char) || c == ('<' as char) || c == ('=' as char) )
                return [arg.substring(0, i), String.valueOf(c), arg.substring(i + 1)]
        }
        return null
    }

    /** The number a range compares with; throws {@link NumberFormatException} for anything else. */
    static BigDecimal number(String text) {
        return new BigDecimal(text.trim())
    }
}
