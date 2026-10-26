package robsyme.cas

import java.nio.file.Path

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

/** A spec that sets no identity seam of its own sees the ones {@link IdentitySeamsExtension} installs. */
class IdentitySeamsExtensionTest extends Specification {

    @TempDir
    Path tmp

    @Shared
    PlatformIdentity seenInSetupSpec

    def setupSpec() {
        seenInSetupSpec = CasSession.platformIdentity
    }

    def 'every seam is the test double, in setupSpec and in a feature'() {
        expect:
        seenInSetupSpec.is(IdentitySeamsExtension.REFUSING_PLATFORM)
        CasSession.platformIdentity.is(IdentitySeamsExtension.REFUSING_PLATFORM)
        CasSession.gitEmail.is(IdentitySeamsExtension.NO_GIT)
        CasSession.env.keySet().every { !it.startsWith('TOWER_') && it != 'NXF_BLOCKS_USER' }
    }

    def 'a session built without seams is anonymous and never asks Platform'() {
        given:
        final int before = IdentitySeamsExtension.platformCalls.get()
        final CasSession session = new CasSession(CasConfig.from([cas: [stores: [lab: [location: tmp.resolve('s').toString()]],
            index: [path: tmp.resolve('c.sqlite').toString()]]], 'cas://lab'))

        expect:
        session.assertedBy == 'anonymous'
        IdentitySeamsExtension.platformCalls.get() == before
    }

    def 'a feature that changes a seam does not leak it into the next: first'() {
        when:
        CasSession.gitEmail = { -> 'leak@example.org' } as GitEmail

        then:
        CasSession.gitEmail.userEmail() == 'leak@example.org'
    }

    def 'a feature that changes a seam does not leak it into the next: second'() {
        expect:
        CasSession.gitEmail.is(IdentitySeamsExtension.NO_GIT)
    }
}
