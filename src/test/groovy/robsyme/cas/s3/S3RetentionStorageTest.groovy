package robsyme.cas.s3

import java.nio.file.Path

import robsyme.cas.core.Cid
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.RetentionStorageContract
import robsyme.cas.core.Stamped
import spock.lang.TempDir

class S3RetentionStorageTest extends RetentionStorageContract {

    MemoryS3Ops ops = new MemoryS3Ops('b')
    @TempDir Path tmp

    private S3BlockStore blocks() { new S3BlockStore(ops, 'm/', 'lab', true, tmp) }

    @Override RetentionStorage storage() { new S3RetentionStorage(ops, 'm/') }

    @Override Cid writeBlock(byte[] bytes) { blocks().putStreaming(new ByteArrayInputStream(bytes)) }

    @Override void leaveScratch() { ops.createMultipart('m/tmp/x', S3PutOptions.create()) }

    @Override void writeLogEntry(String name) { ops.putText('m/log/' + name, '') }

    @Override boolean logEntryExists(String name) { ops.objects.containsKey('m/log/' + name) }

    def 'a lock is taken with If-None-Match and replaced with If-Match'() {
        when:
        final String v = storage().createLock('x'.bytes)
        storage().replaceLock(v, 'y'.bytes)

        then:
        ops.calls.findAll { it.startsWith('PUT m/sweep.lock') }.size() == 2
    }

    def 'scratch holds tmp keys and open uploads, and an upload is aborted, not deleted'() {
        given:
        ops.putText('m/tmp/stage-1', 'x')
        final String id = ops.createMultipart('m/blocks/aa/y', S3PutOptions.create())

        when:
        final List<Stamped> scratch = storage().listScratch()
        scratch.each { storage().deleteScratch(it) }

        then:
        scratch*.name.sort() == ['m/blocks/aa/y', 'm/tmp/stage-1']
        scratch.find { it.token == id } != null
        ops.calls.contains('ABORT m/blocks/aa/y')
        ops.listUploads('m/') == []
    }
}
