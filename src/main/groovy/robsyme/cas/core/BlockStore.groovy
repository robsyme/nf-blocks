package robsyme.cas.core

import java.util.stream.Stream

/**
 * A set of immutable blocks addressed by their content (DESIGN.md §5).
 * "Already present" is success for every write.
 */
interface BlockStore {

    String alias()

    boolean has(Cid cid)

    /** @throws NoSuchBlockException when the block is absent */
    long size(Cid cid)

    /** @throws NoSuchBlockException when the block is absent */
    InputStream open(Cid cid)

    /** Stable per address once written. */
    long lastModifiedMillis(Cid cid)

    /** Write bytes already known to hash to cid. No-op if present. */
    void put(Cid cid, InputStream in, long expectedSize)

    /** Hash while streaming to a temp file, then place under the computed address. */
    Cid putStreaming(InputStream in)

    /** Encode, hash, put; returns the cid. */
    Cid putDagCbor(Object value)

    /** Every block, for rebuild and sweep. The caller must close the stream. */
    Stream<Cid> listBlocks()

    boolean isWritable()
}
