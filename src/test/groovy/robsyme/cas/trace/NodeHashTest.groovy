package robsyme.cas.trace

import spock.lang.Specification

/** Chained ahead of the user's afterScript, in every selector (spec §14). */
class NodeHashTest extends Specification {

    def 'no afterScript anywhere: ours becomes the default'() {
        given:
        final Map config = [process: [cpus: 1]]

        when:
        NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script()
    }

    def "the user's afterScript runs after ours, at the top level and in each selector"() {
        given:
        final Map config = [process: [afterScript: 'echo user', 'withName:FOO': [afterScript: 'echo foo'], 'withLabel:big': [cpus: 8]]]

        when:
        final List<String> skipped = NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script() + '\necho user'
        config.process['withName:FOO'].afterScript == NodeHash.script() + '\necho foo'
        !config.process['withLabel:big'].containsKey('afterScript')
        skipped.isEmpty()
    }

    def 'a closure afterScript is left alone and reported'() {
        given:
        final Closure dynamic = { -> 'echo dyn' }
        final Map config = [process: ['withName:BAR': [afterScript: dynamic]]]

        when:
        final List<String> skipped = NodeHash.install(config)

        then:
        config.process['withName:BAR'].afterScript.is(dynamic)
        skipped == ['withName:BAR']
        config.process.afterScript == NodeHash.script()
    }

    def 'installing twice chains once'() {
        given:
        final Map config = [process: [afterScript: 'echo user']]

        when:
        NodeHash.install(config); NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script() + '\necho user'
    }
}
