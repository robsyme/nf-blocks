package robsyme.cas

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * {@link CasSession} behaviour not tied to a particular store transport
 * (see {@link CasSessionS3Test} for the S3-composition cases).
 */
class CasSessionTest extends Specification {

    @TempDir
    Path tmp

    CasSession cas

    def setup() {
        cas = new CasSession(CasConfig.from([
            cas: [
                stores : [lab: [location: tmp.resolve('store').toString()]],
                index  : [path: tmp.resolve('cache.sqlite').toString()],
            ],
        ], 'cas://lab'))
    }

    def 'a read-only-only session has nothing to register; stop is idempotent'() {
        when:
        cas.stopLiveWriter()
        cas.stopLiveWriter()

        then:
        notThrown(Exception)
    }
}
