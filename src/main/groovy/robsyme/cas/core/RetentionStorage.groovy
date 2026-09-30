package robsyme.cas.core

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Retention objects of one member (ticket 20 answer 5): sweep.lock, live/
 * registrations, trash/ ledgers, and the block listings, upload scratch and
 * Store Log entries a sweep acts on. The same contract, local and S3, so a
 * sweep is written once ({@link RetentionStorageContract}).
 */
@CompileStatic
interface RetentionStorage {
    /** The store's clock as of its latest answer: S3's Date header, a local directory's System.currentTimeMillis(). */
    long nowMillis()

    /** sweep.lock, or null when absent. */
    Versioned readLock()
    /** Creates sweep.lock; its version, or null when a lock object already exists (S3 If-None-Match: *; local create-exclusive). */
    String createLock(byte[] body)
    /** Replaces sweep.lock only while it is still `version`; the new version, or null when it is not (S3 If-Match; local compare-and-move). */
    String replaceLock(String version, byte[] body)

    /** live/<session>, created or rewritten (a rewrite is the heartbeat). */
    void putLive(String session, byte[] body)
    void deleteLive(String session)
    /** A registration's bytes, or null when absent. */
    byte[] readLive(String session)
    /** Every registration, name = the session, with its LastModified. */
    List<Stamped> listLive()

    /** Names under trash/, ascending. */
    List<String> listLedgers()
    /** A ledger's bytes, or null when absent. */
    byte[] readLedger(String name)
    void writeLedger(String name, byte[] body)
    void deleteLedger(String name)

    /** Every block of this member with its size and LastModified. */
    List<BlockStat> listBlockStats()
    /** Deletes the blocks; returns those that could not be deleted. A block already gone is not a failure. */
    List<Cid> deleteBlocks(Collection<Cid> cids)

    /** Upload scratch: S3 tmp/ keys and open multipart uploads to keys under blocks/ or tmp/; local blocks/.tmp-* and blocks/<xx>/.tmp-*. */
    List<Stamped> listScratch()
    void deleteScratch(Stamped scratch)

    /** Removes one Store Log entry by name (decision 7). Absent is success. */
    void deleteLogEntry(String name)

    String describe()
}

@Canonical @CompileStatic class Versioned { byte[] body; String version; long lastModifiedMillis }
@Canonical @CompileStatic class Stamped { String name; long lastModifiedMillis; String token }  // token: an S3 upload id, else null
@Canonical @CompileStatic class BlockStat { Cid cid; long size; long lastModifiedMillis }
