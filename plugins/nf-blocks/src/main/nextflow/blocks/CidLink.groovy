package nextflow.blocks

import io.ipfs.cid.Cid
import nextflow.cbor.CborObject
import nextflow.cbor.Cborable

/**
 * Represents a CID link in the DAG-CBOR format.
 * Links are encoded as tag 42 in DAG-CBOR, containing the binary form of a CID
 * with the raw-binary identity Multibase (0x00) prefix.
 */
class CidLink implements Cborable {
    private final Cid cid

    CidLink(Cid cid) {
        this.cid = Objects.requireNonNull(cid)
    }

    /**
     * Creates a CidLink from a CID byte array
     */
    static CidLink fromCid(byte[] rawCid) {
        if (rawCid.length == 0) {
            throw new IllegalArgumentException("CID bytes cannot be empty")
        }
        // Remove identity multibase prefix if present
        byte[] cidBytes = rawCid[0] == 0x00 ? rawCid[1..-1] : rawCid
        return new CidLink(Cid.cast(cidBytes))
    }

    /**
     * Get the raw CID bytes without the multibase prefix
     */
    byte[] getCidBytes() {
        return cid.toBytes()
    }

    /**
     * Convert to CBOR representation (tag 42 with byte string)
     */
    @Override
    CborObject toCbor() {
        // Add identity multibase prefix for DAG-CBOR encoding
        byte[] withPrefix = new byte[cid.toBytes().length + 1]
        withPrefix[0] = 0x00
        System.arraycopy(cid.toBytes(), 0, withPrefix, 1, cid.toBytes().length)
        return new CborObject.CborTag(42, new CborObject.CborByteArray(withPrefix))
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (!(o instanceof CidLink)) return false
        return cid == o.cid
    }

    int hashCode() {
        return cid.hashCode()
    }

    String toString() {
        return "CidLink(${cid.toString()})"
    }
} 