package robsyme.cas.nio

import java.nio.file.Path
import java.nio.file.Paths

import spock.lang.Specification

/**
 * The canonical `cas://<authority>/<a/b/c>` form is the join key everything
 * else is keyed on, and it is what Nextflow records and logs. DESIGN.md
 * sections 7 and 8.
 */
class CasPathTest extends Specification {

    private CasFileSystemProvider provider = new CasFileSystemProvider()

    private CasPath path(String uri) {
        return (CasPath) provider.getPath(URI.create(uri))
    }

    def 'an absolute path stringifies as its canonical uri'() {
        expect:
        path(uri).toString() == uri
        path(uri).toUri().toString() == uri

        where:
        uri << ['cas://lab', 'cas://lab/aligned', 'cas://lab/aligned/A/A.bam']
    }

    def 'normalises redundant, dot and parent segments'() {
        expect:
        path(uri).toString() == expected

        where:
        uri                            | expected
        'cas://lab//aligned///A'       | 'cas://lab/aligned/A'
        'cas://lab/aligned/./A'        | 'cas://lab/aligned/A'
        'cas://lab/aligned/B/../A'     | 'cas://lab/aligned/A'
        'cas://lab/../aligned'         | 'cas://lab/aligned'
        'cas://lab/aligned/'           | 'cas://lab/aligned'
    }

    def 'resolves a relative path against the authority'() {
        expect:
        path('cas://lab').resolve('aligned/A/A.bam').toString() == 'cas://lab/aligned/A/A.bam'
        path('cas://lab/aligned').resolve('A').resolve('A.bam').toString() == 'cas://lab/aligned/A/A.bam'
    }

    def 'the file name is the bare last segment, not a uri'() {
        given:
        def p = path('cas://lab/aligned/A/A.bam')

        expect:
        p.fileName.toString() == 'A.bam'
        p.parent.toString() == 'cas://lab/aligned/A'
        p.root.toString() == 'cas://lab'
        p.nameCount == 3
        p.getName(0).toString() == 'aligned'
        p.subpath(0, 2).toString() == 'aligned/A'
    }

    def 'the root of a store has no parent'() {
        expect:
        path('cas://lab').parent == null
        path('cas://lab').fileName == null
        path('cas://lab/aligned').parent.toString() == 'cas://lab'
    }

    def 'a Store URI with no segments is named by its content address, so Nextflow can stage it (final review I1)'() {
        given:
        final String cid = 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'

        expect:
        path("cas://${cid}").fileName.toString() == cid
        path("cas://${cid}").parent == null
        path("cas://${cid}/A.bam").fileName.toString() == 'A.bam'
    }

    def 'relativizes within the same store'() {
        expect:
        path('cas://lab').relativize(path('cas://lab/aligned/A/A.bam')).toString() == 'aligned/A/A.bam'
        path('cas://lab/aligned/A').relativize(path('cas://lab/aligned/B/B.bam')).toString() == '../B/B.bam'
    }

    def 'refuses to relativize across stores'() {
        when:
        path('cas://lab').relativize(path('cas://other/x'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'startsWith answers false for a path of another file system rather than throwing'() {
        given:
        Path other = Paths.get('/data/cas/coords/aligned')

        expect:
        !path('cas://lab/aligned/A').startsWith(other)
        path('cas://lab/aligned/A').startsWith(path('cas://lab/aligned'))
        !path('cas://lab/aligned/A').startsWith(path('cas://other/aligned'))
    }

    def 'equality is by authority and segments'() {
        expect:
        path('cas://lab/aligned/A') == path('cas://lab/./aligned/A')
        path('cas://lab/aligned/A').hashCode() == path('cas://lab/aligned/A').hashCode()
        path('cas://lab/aligned/A') != path('cas://other/aligned/A')
    }

    def 'tells a Publish Coordinate apart from a Store URI by its authority'() {
        given:
        def cid = 'bafkreigyhb6gpc5d2r4d2atx2qusiajjw5kmubu3z4njhaacinvyc5qmga'

        expect:
        path('cas://lab/aligned/A').isCoordinate()
        !path('cas://lab/aligned/A').isStoreUri()
        path("cas://$cid/A.bam").isStoreUri()
        !path("cas://$cid/A.bam").isCoordinate()
    }

    def 'a cas uri without an authority is rejected'() {
        when:
        provider.getPath(URI.create('cas:///aligned/A'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'is absolute only when it carries an authority'() {
        expect:
        path('cas://lab/aligned').isAbsolute()
        !path('cas://lab/aligned').fileName.isAbsolute()
    }

    def 'exposes the content address of a Store URI and the alias of a Coordinate'() {
        given:
        def cid = 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'

        expect:
        path("cas://$cid/A.bam").cid().toString() == cid
        path('cas://lab/aligned/A').alias() == 'lab'

        when:
        path('cas://lab/aligned/A').cid()
        then:
        thrown(IllegalStateException)

        when:
        path("cas://$cid/A.bam").alias()
        then:
        thrown(IllegalStateException)
    }

    def 'resolve, endsWith, compareTo and relativize throw on a foreign path'() {
        given:
        Path foreign = Paths.get('/data/cas/coords/aligned')
        def p = path('cas://lab/aligned/A')

        when: p.resolve(foreign)
        then: thrown(java.nio.file.ProviderMismatchException)

        when: p.endsWith(foreign)
        then: thrown(java.nio.file.ProviderMismatchException)

        when: p.compareTo(foreign)
        then: thrown(java.nio.file.ProviderMismatchException)

        when: p.relativize(foreign)
        then: thrown(java.nio.file.ProviderMismatchException)
    }

    def 'resolve of an empty path is this, of an absolute path is that'() {
        given:
        def base = path('cas://lab/aligned')

        expect:
        base.resolve(base.fileSystem.getPath('')) == base
        base.resolve(path('cas://other/x')).toString() == 'cas://other/x'
    }

    def 'toAbsolutePath returns an absolute path or throws for a bare relative one'() {
        expect:
        path('cas://lab/aligned').toAbsolutePath().toString() == 'cas://lab/aligned'

        when:
        path('cas://lab/aligned/A/A.bam').fileName.toAbsolutePath()
        then:
        thrown(IllegalStateException)
    }
}
