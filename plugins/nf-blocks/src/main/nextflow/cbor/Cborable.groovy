package nextflow.cbor

/**
 * Interface for objects that can be serialized to CBOR format.
 * Inspired by the Peergos implementation.
 */
interface Cborable {
    /**
     * Serialize this object to a CBOR object
     * @return The CBOR representation of this object
     */
    CborObject toCbor()
} 