package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import groovy.json.JsonOutput
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3LineageRecords
import spock.lang.Specification
import spock.lang.TempDir

class TaskLineageTest extends Specification {

    static final String RUN = 'a' * 32
    static final String ALIGN = '1' * 32
    static final String FILTER = '2' * 32
    static final String COUNT = '3' * 32

    @TempDir Path member

    private static Cid cid(String text) { Cid.of(Cid.RAW, MessageDigest.getInstance('SHA-256').digest(text.bytes)) }

    private static Map record(String kind, Map spec) { [version: 'lineage/v1beta1', kind: kind, spec: spec] }

    private static Map published(Cid c, String name, String source) {
        record('FileOutput', [path: "cas://${c}/${name}".toString(), source: source])
    }

    /** align -> (unpublished) filter -> count; align and count publish. */
    private Map<String, Map> chain() {
        final Map<String, Map> r = [:]
        r[RUN] = record('WorkflowRun', [name: 'x'])
        r["$RUN/bam/a.bam".toString()] = published(cid('bam'), 'a.bam', "lid://$ALIGN/a.bam".toString())
        r["$RUN/counts/c.tsv".toString()] = published(cid('counts'), 'c.tsv', "lid://$COUNT/c.tsv".toString())
        r["$RUN/nested".toString()] = record('FileOutput', [path: 'cas://' + cid('dir') + '/nested', source: null])
        r["$RUN/nested/deep/d.txt".toString()] = published(cid('deep'), 'd.txt', "lid://$FILTER/never.txt".toString())
        r[ALIGN] = record('TaskRun', [input: [[type: 'Path', name: 'reads', value: ['lid://' + ('9' * 32) + '/r.fq']]]])
        r[FILTER] = record('TaskRun', [input: [[type: 'Path', name: 'bam', value: "lid://$ALIGN/a.bam".toString()]]])
        r[COUNT] = record('TaskRun', [input: [[type: 'Record', name: 'sample', value: [meta: [id: 'x'], bams: [["lid://$FILTER/f.bam".toString()]]]]]])
        r["$ALIGN/a.bam".toString()] = record('FileOutput', [path: '/work/aa/a.bam'])
        return r
    }

    private LineageRecords local(Map<String, Map> records) {
        records.each { String key, Map body ->
            final Path dir = member.resolve('nf').resolve(key)
            Files.createDirectories(dir)
            dir.resolve('.data.json').text = JsonOutput.toJson(body)
        }
        Files.writeString(member.resolve('nf/.history'), 'ignored')
        return new LocalLineageRecords(member)
    }

    private LineageRecords s3(Map<String, Map> records) {
        final MemoryS3Ops ops = new MemoryS3Ops('b')
        records.each { String key, Map body -> ops.putText("m/nf/$key/.data.json".toString(), JsonOutput.toJson(body)) }
        ops.putText('m/nf/.history', 'ignored')
        ops.putText('m/blocks/zz', 'not a record')
        return new S3LineageRecords(ops, 'm/')
    }

    def 'local: keysUnder lists nested record keys and read decodes them'() {
        given:
        final LineageRecords nf = local(chain())

        expect:
        nf.keysUnder("$RUN/") == ["$RUN/bam/a.bam", "$RUN/counts/c.tsv", "$RUN/nested", "$RUN/nested/deep/d.txt"]
        nf.keysUnder(RUN) == nf.keysUnder("$RUN/")
        nf.read(COUNT).kind == 'TaskRun'
        nf.read("$RUN/nested/deep/d.txt").spec.source == "lid://$FILTER/never.txt"
        nf.read('nope') == null
        nf.keysUnder('nope/') == []
    }

    def 'S3: the same listing and reads over MemoryS3Ops, with a member prefix'() {
        given:
        final LineageRecords nf = s3(chain())

        expect:
        nf.keysUnder("$RUN/") == ["$RUN/bam/a.bam", "$RUN/counts/c.tsv", "$RUN/nested", "$RUN/nested/deep/d.txt"]
        nf.read(ALIGN).kind == 'TaskRun'
        nf.read('nope') == null
    }

    def 'S3: keysUnder answers in key order, not the order S3 lists the objects in'() {
        given: "'x/a.b/.data.json' lists before 'x/a/.data.json', since '.' sorts before '/'"
        final LineageRecords nf = s3(['x/a': record('FileOutput', [:]), 'x/a.b': record('FileOutput', [:])])

        expect:
        nf.keysUnder('x/') == ['x/a', 'x/a.b']
    }

    def 'a .data.json that is not a JSON object names its key (#kind)'() {
        given:
        final Map<String, Map> run = [(RUN): record('WorkflowRun', [name: 'x'])]
        final LineageRecords nf = kind == 'local' ? local(run) : s3(run)
        if( kind == 'local' ) {
            Files.createDirectories(member.resolve("nf/$RUN/bad"))
            member.resolve("nf/$RUN/bad/.data.json").text = body
        }
        else
            ((S3LineageRecords) nf).ops.putText("m/nf/$RUN/bad/.data.json".toString(), body)

        when:
        nf.read("$RUN/bad")

        then:
        final IllegalStateException e = thrown()
        e.message.contains("$RUN/bad")

        where:
        kind    | body
        'local' | '{not json'
        'local' | '[1, 2]'
        's3'    | '{not json'
        's3'    | '[1, 2]'
    }

    def 'the walk reaches a published upstream through an unpublished task (#kind)'() {
        given:
        final LineageRecords nf = kind == 'local' ? local(chain()) : s3(chain())

        when:
        final TaskLineage.Result r = TaskLineage.upstream(nf, RUN, [cid('counts')])

        then:
        r.available
        r.upstream == [cid('bam')] as Set

        when: 'starting from the upstream file finds nothing further up'
        final TaskLineage.Result up = TaskLineage.upstream(nf, RUN, [cid('bam')])

        then:
        up.available
        up.upstream.isEmpty()

        where:
        kind << ['local', 's3']
    }

    def 'an address the run never published, a null source and a non-cid path are skipped'() {
        given:
        final Map<String, Map> r = chain()
        r["$RUN/odd".toString()] = record('FileOutput', [path: 'cas://not-a-cid/x', source: "lid://$COUNT/x".toString()])
        final LineageRecords nf = local(r)

        expect:
        TaskLineage.upstream(nf, RUN, [cid('stranger'), cid('dir')]).upstream.isEmpty()
        TaskLineage.upstream(nf, RUN, [cid('stranger')]).available
    }

    def 'a run with no records under its hash is not available (#kind)'() {
        given:
        final LineageRecords nf = kind == 'local' ? local(chain()) : s3(chain())

        when:
        final TaskLineage.Result r = TaskLineage.upstream(nf, 'f' * 32, [cid('counts')])

        then:
        !r.available
        r.upstream.isEmpty()

        where:
        kind << ['local', 's3']
    }

    def 'a run key with only its WorkflowRun record is not available'() {
        expect:
        !TaskLineage.upstream(local([(RUN): record('WorkflowRun', [:])]), RUN, [cid('x')]).available
    }

    def 'cycles among TaskRuns terminate, and each TaskRun is read once'() {
        given:
        final Map<String, Map> r = chain()
        r[ALIGN] = record('TaskRun', [input: ["lid://$COUNT/c.tsv".toString()]])    // align -> count -> filter -> align
        final LineageRecords inner = local(r)
        final List<String> reads = []
        final LineageRecords nf = new LineageRecords() {
            List<String> keysUnder(String prefix) { inner.keysUnder(prefix) }
            Map read(String key) { reads.add(key); inner.read(key) }
        }

        when:
        final TaskLineage.Result res = TaskLineage.upstream(nf, RUN, [cid('counts')])

        then:
        res.upstream == [cid('bam'), cid('counts')] as Set
        [ALIGN, FILTER, COUNT].every { String t -> reads.count(t) <= 1 }
    }
}
