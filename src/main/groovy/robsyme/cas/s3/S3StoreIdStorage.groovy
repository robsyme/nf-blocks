package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.StoreIdStorage
import robsyme.cas.core.StoreIds

/** `<prefix>store-id` in an S3 member: created with If-None-Match: *, never rewritten. */
@CompileStatic
class S3StoreIdStorage implements StoreIdStorage {

    private final S3Ops ops
    private final String key

    S3StoreIdStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.key = (prefix ?: '') + StoreIds.FILE
    }

    @Override
    String read() {
        final InputStream in = ops.get(key, null, 0L, -1L)
        if( in == null )
            return null
        final byte[] body = in.withCloseable { InputStream s -> s.readAllBytes() }
        return StoreIds.parse(body, describe())
    }

    @Override
    String create(String id) {
        if( !StoreIds.valid(id) )
            throw new IllegalArgumentException("not a store id: '${id}'")
        ops.put(key, S3Body.ofBytes((id + '\n').getBytes('UTF-8')),
            S3PutOptions.create().ifNoneMatch().contentType('text/plain; charset=utf-8').storageClass(S3RetentionStorage.RETENTION_CLASS))
        return read()
    }

    @Override
    String describe() { "${ops.describe()}/${key}".toString() }
}
