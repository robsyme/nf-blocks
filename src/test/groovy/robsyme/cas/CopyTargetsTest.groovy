package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

class CopyTargetsTest extends Specification {

    @TempDir
    Path tempDir

    private CasConfig config(Map stores) {
        return CasConfig.from([cas: [stores: [lab: [location: tempDir.resolve('lab').toString()]] + stores]], 'cas://lab')
    }

    def 'a local directory this process can write is a target'() {
        given:
        Files.createDirectories(tempDir.resolve('shared'))
        final def c = config([shared: [location: tempDir.resolve('shared').toString()]])

        expect:
        CopyTargets.judge(c, 'shared').target
        CopyTargets.judge(c, 'shared').reason == null
    }

    def 'a local directory that does not exist yet is judged by its nearest existing ancestor'() {
        given:
        final def c = config([fresh: [location: tempDir.resolve('a/b/c').toString()]])

        expect:
        CopyTargets.judge(c, 'fresh').target
    }

    @Requires({ !System.getProperty('user.name').equals('root') })
    def 'a local directory this process cannot write is not a target'() {
        given:
        final Path locked = Files.createDirectories(tempDir.resolve('locked'))
        locked.toFile().setWritable(false)
        final def c = config([locked: [location: locked.toString()], below: [location: locked.resolve('x/y').toString()]])

        expect:
        !CopyTargets.judge(c, 'locked').target
        CopyTargets.judge(c, 'locked').reason == 'this process can\'t write the directory'
        !CopyTargets.judge(c, 'below').target
        CopyTargets.judge(c, 'below').reason == 'this process can\'t write the directory'

        cleanup:
        locked.toFile().setWritable(true)
    }

    def 'readOnly wins over a writable directory, and applies to S3'() {
        given:
        Files.createDirectories(tempDir.resolve('shared'))
        final def c = config([shared: [location: tempDir.resolve('shared').toString(), readOnly: true],
                              frozen: [location: 's3://archive/x', readOnly: true]])

        expect:
        !CopyTargets.judge(c, 'shared').target
        CopyTargets.judge(c, 'shared').reason == 'readOnly is set for this store'
        !CopyTargets.judge(c, 'frozen').target
        CopyTargets.judge(c, 'frozen').reason == 'readOnly is set for this store'
    }

    def 'an S3 member is always a target'() {
        given:
        final def c = config([org: [location: 's3://lab-blocks/cas']])

        expect:
        CopyTargets.judge(c, 'org').target
        CopyTargets.judge(c, 'org').reason == null
    }
}
