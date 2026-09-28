package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §0 rule 2 on S3: a 256 MiB file through spool, hash and multipart, under a 48 MiB heap. */
class S3MemoryBoundTest extends Specification {

    @TempDir Path tmp

    def 'putStreaming a 256 MiB stream to S3 holds no file content in heap'() {
        given:
        final MemoryS3Ops s3 = new MemoryS3Ops('member')
        s3.discard = true
        // 64 MiB parts: the lowered limit forces the multipart path.
        final S3BlockStore store = new S3BlockStore(s3, 'cas/', 'lab', true, tmp, 64L << 20)
        final InputStream zeros = new InputStream() {
            long left = 256L << 20
            @Override int read() { left-- > 0 ? 0 : -1 }
            @Override int read(byte[] b, int off, int len) {
                if( left <= 0 ) return -1
                final int n = (int) Math.min(len, left); Arrays.fill(b, off, off + n, (byte) 0); left -= n; n
            }
        }

        when:
        store.putStreaming(zeros)

        then:
        s3.pulledBytes == 256L << 20
        s3.calls.count { it.startsWith('PART ') } == 4
        Runtime.runtime.maxMemory() < (256L << 20)
    }
}
