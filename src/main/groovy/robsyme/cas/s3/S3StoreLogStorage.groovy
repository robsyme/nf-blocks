package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.StoreLogStorage

/** log/<rts>-<kind>-<cid> as empty objects; ListObjectsV2 is lexicographic, so newest first holds (DESIGN.md §5). */
@CompileStatic
class S3StoreLogStorage implements StoreLogStorage {
    private final S3Ops ops
    private final String under

    S3StoreLogStorage(S3Ops ops, String prefix) { this.ops = ops; this.under = "${prefix ?: ''}log/".toString() }

    /**
     * Write-once: If-None-Match, and a 412 is the same entry already there. A 409 is
     * tried up to S3BlockStore.ATTEMPTS times, as blocks are, then thrown; the
     * observer's appendStoreLog warns and continues (rule 3).
     */
    @Override
    void putEntry(String name) {
        for( int attempt = 1; attempt <= S3BlockStore.ATTEMPTS; attempt++ ) {
            final S3Written w = ops.put(under + name, S3Body.ofBytes(new byte[0]), S3PutOptions.create().ifNoneMatch())
            if( w.status != S3Written.Status.CONFLICT )
                return
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${S3BlockStore.ATTEMPTS} times writing ${ops.describe()}/${under}${name}")
    }

    @Override
    List<String> listEntries() {
        final List<String> names = []
        for( S3Listed o : ops.list(under, 0) ) {
            final String name = o.key.substring(under.length())
            if( name && !name.contains('/') ) names.add(name)
        }
        return names
    }
}
