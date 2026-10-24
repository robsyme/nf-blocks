package robsyme.cas.core

import spock.lang.Specification

class ConditionsTest extends Specification {

    def 'a where key names its path, and an operator at its end makes it a range (#key)'() {
        expect:
        Conditions.splitKey(key) == expected

        where:
        key                    | expected
        'sample'               | ['sample', '=']
        'meta.depth >='        | ['meta.depth', '>=']
        'meta.depth>='         | ['meta.depth', '>=']
        'lane <'               | ['lane', '<']
        'lane >'               | ['lane', '>']
        'lane <='              | ['lane', '<=']
        '  depth   >  '        | ['depth', '>']
    }

    def 'an items argument splits at its first operator, a longer operator before a shorter (#arg)'() {
        expect:
        Conditions.splitArg(arg) == expected

        where:
        arg            | expected
        'sample=A'     | ['sample', '=', 'A']
        'formula=x=y'  | ['formula', '=', 'x=y']
        'note=a>b'     | ['note', '=', 'a>b']
        'depth>=30'    | ['depth', '>=', '30']
        'depth<=1.5'   | ['depth', '<=', '1.5']
        'lane>2'       | ['lane', '>', '2']
        'lane<2'       | ['lane', '<', '2']
        'sample'       | null
    }

    def 'a range value must be a number'() {
        expect:
        Conditions.number('30') == 30
        Conditions.number('1.5') == 1.5
        Conditions.number('-2e3') == -2000

        when:
        Conditions.number('two')

        then:
        thrown(NumberFormatException)
    }
}
