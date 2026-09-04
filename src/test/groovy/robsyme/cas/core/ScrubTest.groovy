package robsyme.cas.core

import java.nio.file.Path
import java.nio.file.Paths

import spock.lang.Specification

/**
 * DESIGN.md §6, the portability scrub. A run's `params` and `config` describe
 * the run, not the machine it ran on: an absolute path, a bucket or the OS
 * user name in a block would make the block unusable to anyone else and would
 * break Gate assertion 10.
 */
class ScrubTest extends Specification {

    def 'the store-local config scopes are dropped'() {
        given:
        final Map config = [
            cas        : [stores: [lab: [location: '/data/cas']]],
            lineage    : [enabled: true],
            workDir    : '/scratch/work',
            outputDir  : 'cas://lab',
            launchDir  : '/home/x',
            projectDir : '/home/x/pipeline',
            homeDir    : '/home/x',
            configFiles: ['/home/x/nextflow.config'],
            scriptFile : '/home/x/main.nf',
            commandLine: 'nextflow run .',
            runName    : 'cheeky_curie',
            resume     : true,
            process    : [cpus: 2],
        ]

        when:
        final Map scrubbed = (Map) Records.scrub(config)

        then:
        scrubbed.keySet() == ['process'] as Set
        scrubbed.process == [cpus: 2]
    }

    def 'the scopes are dropped only at the top level'() {
        expect:
        Records.scrub([params: [outputDir: 'results', resume: true]]) == [params: [outputDir: 'results', resume: true]]
    }

    def 'a Path value becomes a string'() {
        given:
        final Path path = Paths.get('reference/genome.fa')

        expect:
        Records.scrub([genome: path]) == [genome: 'reference/genome.fa']
    }

    def 'an absolute path is redacted'() {
        expect:
        Records.scrub([genome: '/Users/x/y']) == [genome: '[redacted-location]']
        Records.scrub([genome: Paths.get('/Users/x/y')]) == [genome: '[redacted-location]']
    }

    def 'a location in another scheme is redacted'() {
        expect:
        Records.scrub([genome: 's3://bucket/x']) == [genome: '[redacted-location]']
        Records.scrub([genome: 'az://container/x']) == [genome: '[redacted-location]']
        Records.scrub([genome: 'file:///data/x']) == [genome: '[redacted-location]']
    }

    def 'lineage and store references stay, they are location-free'() {
        given:
        final String cas = 'cas://bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am/A.bam'

        expect:
        Records.scrub([from: 'lid://abc/x']) == [from: 'lid://abc/x']
        Records.scrub([from: cas]) == [from: cas]
    }

    def 'a relative path is left alone'() {
        expect:
        Records.scrub([genome: 'reference/genome.fa']) == [genome: 'reference/genome.fa']
        Records.scrub([sample: 'A']) == [sample: 'A']
    }

    def 'the os user name is redacted wherever it appears'() {
        given:
        final String user = System.getProperty('user.name')

        expect:
        user
        Records.scrub([owner: user]) == [owner: '[redacted-user]']
        Records.scrub([owners: [user, 'someone-else']]) == [owners: ['[redacted-user]', 'someone-else']]
    }

    def 'nested maps and lists are scrubbed all the way down'() {
        given:
        final Map params = [
            samples: [
                [id: 'A', reads: ['/data/A_1.fq', '/data/A_2.fq']],
                [id: 'B', reads: ['reads/B_1.fq']],
            ],
            options: [store: [dir: 's3://bucket/out'], depth: 30],
        ]

        expect:
        Records.scrub(params) == [
            samples: [
                [id: 'A', reads: ['[redacted-location]', '[redacted-location]']],
                [id: 'B', reads: ['reads/B_1.fq']],
            ],
            options: [store: [dir: '[redacted-location]'], depth: 30],
        ]
    }

    def 'numbers, booleans and nulls are untouched'() {
        expect:
        Records.scrub([cpus: 2, ratio: 0.5, single_end: false, missing: null]) == [cpus: 2, ratio: 0.5, single_end: false, missing: null]
    }

    def 'the input is not mutated'() {
        given:
        final Map inner = [dir: '/data/out']
        final List reads = ['/data/A_1.fq']
        final Map params = [workDir: '/scratch', options: inner, reads: reads]

        when:
        Records.scrub(params)

        then:
        params == [workDir: '/scratch', options: [dir: '/data/out'], reads: ['/data/A_1.fq']]
        inner.dir == '/data/out'
        reads == ['/data/A_1.fq']
    }

    def 'scrubbing twice changes nothing more'() {
        given:
        final Map params = [genome: '/data/genome.fa', owner: System.getProperty('user.name'), depth: 30]

        expect:
        Records.scrub(Records.scrub(params)) == Records.scrub(params)
    }

    def 'a value that is not a container is scrubbed on its own'() {
        expect:
        Records.scrub('/data/x') == '[redacted-location]'
        Records.scrub(null) == null
        Records.scrub(7) == 7
    }
}
