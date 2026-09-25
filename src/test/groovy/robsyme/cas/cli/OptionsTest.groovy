package robsyme.cas.cli

import spock.lang.Specification

/** CmdPlugin hands a verb its positionals, then `--name`, `value` pairs (DESIGN.md §15). */
class OptionsTest extends Specification {

    def 'flags arrive as name and value pairs after the positionals'() {
        when:
        final Options o = Options.parse(['file.json', '--port', '8080'], ['port'] as Set)

        then:
        o.positionals == ['file.json']
        o.flag('port') == '8080'
        o.intFlag('port', 0) == 8080
        o.flag('missing') == null
        o.intFlag('missing', 7) == 7
    }

    def 'an unknown flag is a usage error naming it'() {
        when:
        Options.parse(['--prot', '8080'], ['port'] as Set)

        then:
        final UsageException e = thrown()
        e.message.contains('--prot')
    }

    def 'a flag with no value is a usage error'() {
        when:
        Options.parse(['--port'], ['port'] as Set)

        then:
        thrown(UsageException)
    }

    def 'a non-numeric port is a usage error'() {
        when:
        Options.parse(['--port', 'abc'], ['port'] as Set).intFlag('port', 0)

        then:
        thrown(UsageException)
    }
}
