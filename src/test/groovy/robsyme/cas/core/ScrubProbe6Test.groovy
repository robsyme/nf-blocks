package robsyme.cas.core

import groovy.json.JsonSlurper
import spock.lang.Specification

/**
 * The integrator's probe 6 (P4 third follow-up, p4g-r1), numeric and leading-dash edges, as
 * assertions: each secret is gone from the stored RunManifest, the audit of the raw values
 * reports it, and the plain values beside it stay (B2, S7, S8).
 */
class ScrubProbe6Test extends Specification {

    static final Cid CID = DagCbor.cidOf(DagCbor.encode([kind: 'RunManifest']))

    private static Map stored(Map params, String config) {
        return (Map) DagCbor.decode(DagCbor.encode(new RunManifest(RecordsTest.manifestArgs() + [params: params, config: config]).toCbor()))
    }

    private static List findings(Map params, String config) {
        return RunManifestAudit.audit(CID, [kind: 'RunManifest', params: params, config: config])
    }

    def 'a secret goes and is reported (#label)'() {
        when:
        final Map block = stored(params, config)
        final String text = String.valueOf(block.params) + ' ' + String.valueOf(block.config)

        then:
        gone.every { String secret -> !text.contains(secret) }
        kept.every { String plain -> text.contains(plain) }
        !findings(params, config).isEmpty()

        where:
        label                     | params                                                        | config                                          | gone                                | kept
        'json password'           | (Map) new JsonSlurper().parseText('{"password": 12345678901234567890}') | ''                   | ['12345678901234567890']            | []
        'passwords list'          | [:]                                                           | "params.passwords = [1234, 12, '5678', true, 3.5]" | ['1234', '5678']                 | ['12', 'true', '3.5']
        'tokens list'             | [:]                                                           | "params.tokens = ['12345678', 123, -1234]"      | ['12345678', '-1234']               | ['123']
        'password=-hunter2'       | [extra: 'password=-hunter2']                                  | ''                                              | ['hunter2']                         | ['password=']
        '--password=-hunter2'     | [extra: '--password=-hunter2']                                | ''                                              | ['hunter2']                         | ['--password=']
        'password=-12345678'      | [extra: 'password=-12345678']                                 | ''                                              | ['12345678']                        | ['password=']
        'map password -hunter2'   | [password: '-hunter2']                                        | ''                                              | ['hunter2']                         | []
        'config password -hunter2'| [:]                                                           | "params.password = '-hunter2'"                  | ['hunter2']                         | []
        'exponent 1234e567'       | [api_key: '1234e567']                                         | ''                                              | ['1234e567']                        | []
        'exponent 0123E4567'      | [api_key: '0123E4567']                                        | ''                                              | ['0123E4567']                       | []
        'exponent 12345e678'      | [extra: 'password=12345e678']                                 | ''                                              | ['12345e678']                       | []
        'trailing point'          | [password: '12345678.']                                       | ''                                              | ['12345678']                        | []
        'BigDecimal 12345678'     | [password: new BigDecimal('12345678')]                        | ''                                              | ['12345678']                        | []
        'BigDecimal 1.2345678E+7' | [password: new BigDecimal('1.2345678E+7')]                    | ''                                              | ['12345678', '1.2345678']           | []
        'Float 1234567f'          | [password: 1234567f]                                          | ''                                              | ['1234567']                         | []
        'Double 12345678d'        | [password: 12345678d]                                         | ''                                              | ['12345678']                        | []
    }

    def 'plain numbers stay readable (#value)'() {
        expect:
        Leaks.keepsValue('password', value)

        where:
        value << ['1e-5', '1e-10', '2.5e-8', '0.05', '.5', '-1', '999', 3.5d, 0.25f, new BigDecimal('0.001'), 999d, 12]
    }
}
