package nextflow.cbor

/**
 * Utility class for converting Groovy objects to CBOR objects.
 */
class CborConverter {
    /**
     * Convert a Groovy object to its CBOR representation
     * @param obj The object to convert
     * @return A CborObject representing the input
     */
    static CborObject toCbor(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Cannot convert null to CBOR")
        }
        
        if (obj instanceof Cborable) {
            return obj.toCbor()
        }
        
        switch (obj) {
            case String:
                return new CborObject.CborString(obj)
            case Boolean:
                return new CborObject.CborBoolean(obj)
            case Long:
            case Integer:
            case Short:
            case Byte:
                return new CborObject.CborLong(obj as long)
            case Float:
            case Double:
                return new CborObject.CborFloat(obj as double)
            case byte[]:
                return new CborObject.CborByteArray(obj)
            case Map:
                Map<CborObject.CborString, CborObject> cborMap = [:]
                obj.each { k, v ->
                    def key = k.toString()  // Convert all keys to strings
                    cborMap[new CborObject.CborString(key)] = toCbor(v)
                }
                return new CborObject.CborMap(cborMap)
            case List:
            case Set:
            case Collection:
                return new CborObject.CborList(obj.collect { toCbor(it) })
            default:
                throw new IllegalArgumentException("Unsupported type for CBOR conversion: ${obj.getClass()}")
        }
    }
} 