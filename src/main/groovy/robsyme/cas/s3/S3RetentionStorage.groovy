package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.BlockStat
import robsyme.cas.core.Cid
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.Stamped
import robsyme.cas.core.Versioned

/**
 * Retention objects in an S3 member (ticket 20 answer 5): the lock by
 * conditional PUTs (If-None-Match to take, If-Match to heartbeat and take
 * over), registrations by plain PUT, ledgers as objects. The clock is S3's
 * Date header of the latest response (answer 6).
 */
@CompileStatic
class S3RetentionStorage implements RetentionStorage {

    static final String JSON = 'application/json'

    private final S3Ops ops
    private final String prefix

    S3RetentionStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.prefix = prefix ?: ''
    }

    @Override
    long nowMillis() {
        Long date = ops.lastServerDateMillis()
        if( date == null ) {
            ops.head(prefix + 'sweep.lock')
            date = ops.lastServerDateMillis()
        }
        return date ?: System.currentTimeMillis()
    }

    @Override
    Versioned readLock() {
        for( int attempt = 0; attempt < 3; attempt++ ) {
            final S3Head head = ops.head(prefix + 'sweep.lock')
            if( head == null )
                return null
            try {
                final InputStream in = ops.get(prefix + 'sweep.lock', head.etag, 0L, -1L)
                if( in == null )
                    return null
                final byte[] body = in.withCloseable { InputStream s -> s.readAllBytes() }
                return new Versioned(body, head.etag, head.lastModifiedMillis)
            }
            catch( S3PreconditionFailed e ) {
                // Replaced between the HEAD and the GET: read again.
            }
        }
        throw new IOException("sweep.lock at ${ops.describe()}/${prefix} kept changing while it was read")
    }

    @Override
    String createLock(byte[] body) {
        final S3Written w = ops.put(prefix + 'sweep.lock', S3Body.ofBytes(body), S3PutOptions.create().ifNoneMatch().contentType(JSON))
        return w.status == S3Written.Status.WRITTEN ? w.etag : null
    }

    @Override
    String replaceLock(String version, byte[] body) {
        final S3Written w = ops.put(prefix + 'sweep.lock', S3Body.ofBytes(body), S3PutOptions.create().ifMatch(version).contentType(JSON))
        return w.status == S3Written.Status.WRITTEN ? w.etag : null
    }

    @Override
    void putLive(String session, byte[] body) {
        ops.put(prefix + 'live/' + session, S3Body.ofBytes(body), S3PutOptions.create().contentType(JSON))
    }

    @Override void deleteLive(String session) { ops.delete(prefix + 'live/' + session) }

    @Override
    byte[] readLive(String session) {
        final InputStream in = ops.get(prefix + 'live/' + session, null, 0L, -1L)
        return in == null ? null : in.withCloseable { InputStream s -> s.readAllBytes() }
    }

    @Override
    List<Stamped> listLive() {
        final String under = prefix + 'live/'
        return ops.list(under, 0).collect { S3Listed o -> new Stamped(o.key.substring(under.length()), o.lastModifiedMillis, null) }
    }

    @Override
    List<String> listLedgers() {
        final String under = prefix + 'trash/'
        return ops.list(under, 0).collect { S3Listed o -> o.key.substring(under.length()) }.sort()
    }

    @Override
    byte[] readLedger(String name) {
        final InputStream in = ops.get(prefix + 'trash/' + name, null, 0L, -1L)
        return in == null ? null : in.withCloseable { InputStream s -> s.readAllBytes() }
    }

    @Override
    void writeLedger(String name, byte[] body) {
        ops.put(prefix + 'trash/' + name, S3Body.ofBytes(body), S3PutOptions.create().contentType(JSON))
    }

    @Override void deleteLedger(String name) { ops.delete(prefix + 'trash/' + name) }

    @Override
    List<BlockStat> listBlockStats() {
        final List<BlockStat> out = new ArrayList<BlockStat>()
        for( S3Listed o : ops.list(prefix + 'blocks/', 0) ) {
            final String name = o.key.substring(o.key.lastIndexOf('/') + 1)
            if( Cid.isCid(name) )
                out.add(new BlockStat(Cid.parse(name), o.size, o.lastModifiedMillis))
        }
        return out
    }

    @Override
    List<Cid> deleteBlocks(Collection<Cid> cids) {
        final List<Cid> all = new ArrayList<Cid>(cids)
        final List<Cid> failed = new ArrayList<Cid>()
        for( int from = 0; from < all.size(); from += 1000 ) {
            final List<Cid> batch = all.subList(from, Math.min(all.size(), from + 1000))
            final Map<String, Cid> byKey = batch.collectEntries { Cid c -> [(keyOf(c)): c] }
            for( String key : ops.deleteMany(new ArrayList<String>(byKey.keySet())) )
                failed.add(byKey.get(key))
        }
        return failed
    }

    @Override
    List<Stamped> listScratch() {
        final List<Stamped> out = new ArrayList<Stamped>()
        for( S3Listed o : ops.list(prefix + 'tmp/', 0) )
            out.add(new Stamped(o.key, o.lastModifiedMillis, null))
        for( S3Upload u : ops.listUploads(prefix) )
            out.add(new Stamped(u.key, u.initiatedMillis, u.uploadId))
        return out
    }

    @Override
    void deleteScratch(Stamped scratch) {
        if( scratch.token != null )
            ops.abortMultipart(scratch.name, scratch.token)
        else
            ops.delete(scratch.name)
    }

    @Override void deleteLogEntry(String name) { ops.delete(prefix + 'log/' + name) }

    @Override String describe() { "${ops.describe()}/${prefix}".toString() }

    private String keyOf(Cid cid) {
        final String text = cid.toString()
        return "${prefix}blocks/${text.substring(text.length() - 2)}/${text}".toString()
    }
}
