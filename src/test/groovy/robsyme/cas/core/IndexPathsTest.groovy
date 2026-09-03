package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification

/**
 * DESIGN.md §12: `cas.index.path` if set, else
 * `${XDG_CACHE_HOME:-$HOME/.cache}/nf-blocks/<first 16 hex of sha256(member
 * locations joined by '\n')>.sqlite`.
 */
class IndexPathsTest extends Specification {

    static final Map<String, String> ENV = [HOME: '/home/ada', XDG_CACHE_HOME: '/var/cache/ada']

    def 'an explicit path wins over the derived one'() {
        expect:
        IndexPaths.cachePath(['/store/one'], '/tmp/mine.sqlite', ENV) == Path.of('/tmp/mine.sqlite')
    }

    def 'the derived path sits under XDG_CACHE_HOME'() {
        when:
        def path = IndexPaths.cachePath(['/store/one'], null, ENV)

        then:
        path.parent == Path.of('/var/cache/ada/nf-blocks')
        path.fileName.toString() ==~ /[0-9a-f]{16}\.sqlite/
    }

    def 'without XDG_CACHE_HOME the default is under HOME/.cache'() {
        when:
        def path = IndexPaths.cachePath(['/store/one'], null, [HOME: '/home/ada'])

        then:
        path.parent == Path.of('/home/ada/.cache/nf-blocks')
    }

    def 'the name is the first 16 hex of the sha256 of the locations joined by newline'() {
        given:
        def digest = Hashing.sha256('/store/one\n/store/two'.getBytes('UTF-8')).encodeHex().toString()

        expect:
        IndexPaths.cachePath(['/store/one', '/store/two'], null, ENV).fileName.toString() ==
            digest.substring(0, 16) + '.sqlite'
    }

    def 'different member lists land on different files'() {
        expect:
        IndexPaths.cachePath(['/store/one'], null, ENV) != IndexPaths.cachePath(['/store/one', '/store/two'], null, ENV)
    }

    def 'the order of the members is part of the identity'() {
        expect:
        IndexPaths.cachePath(['/a', '/b'], null, ENV) != IndexPaths.cachePath(['/b', '/a'], null, ENV)
    }

    def 'an empty override is not an override'() {
        expect:
        IndexPaths.cachePath(['/store/one'], '', ENV) == IndexPaths.cachePath(['/store/one'], null, ENV)
    }

    def 'no HOME and no XDG is an error rather than a path in the wrong place'() {
        when:
        IndexPaths.cachePath(['/store/one'], null, [:])

        then:
        thrown(IllegalStateException)
    }
}
