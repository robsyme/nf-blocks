package robsyme.cas.core

import spock.lang.Specification

class NodeDigestsTest extends Specification {

    static final String A = 'a' * 64
    static final String B = 'b' * 64

    private static Map<String, Cid> parse(String text, long max = NodeDigests.MAX_BYTES) {
        NodeDigests.parse(new ByteArrayInputStream(text.getBytes('UTF-8')), max)
    }

    def 'sha256sum lines, text and binary mode, with coreutils escaping; garbage is skipped (Review Focus 3)'() {
        when:
        final Map<String, Cid> d = parse("${A}  d/target.txt\n${B} *bin.dat\n\\${A}  back\\\\slash\\nline\n\\${B}  a\\\\nb\nnot a line\n${A}  name with space.txt\n")

        then: 'escapes are read left to right: an escaped backslash before n is a backslash, then n'
        d.keySet() == ['d/target.txt', 'bin.dat', 'back\\slash\nline', 'a\\nb', 'name with space.txt'] as Set
        d['d/target.txt'] == Cid.of(Cid.RAW, A.decodeHex())
        d['bin.dat'].digest == B.decodeHex()
    }

    def 'a file over the cap is ignored whole'() {
        expect:
        parse("${A}  x\n", 10L) == null
    }

    def 'the task directory is the first two segments under workDir'() {
        expect:
        NodeDigests.taskDirOf('s3://w/work/ab/cdef/d/x.txt', 's3://w/work').taskDir == 's3://w/work/ab/cdef'
        NodeDigests.taskDirOf('s3://w/work/ab/cdef/d/x.txt', 's3://w/work/').rel == 'd/x.txt'
        NodeDigests.taskDirOf('/tmp/work/ab/cdef/A.bam', '/tmp/work').rel == 'A.bam'
        NodeDigests.taskDirOf('/data/store/A.bam', '/tmp/work') == null
        NodeDigests.taskDirOf('/tmp/work/ab/A.bam', '/tmp/work') == null
    }
}
