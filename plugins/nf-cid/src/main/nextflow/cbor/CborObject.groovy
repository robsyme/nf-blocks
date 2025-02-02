package nextflow.cbor

import static nextflow.cbor.CborConstants.*

/**
 * Represents a CBOR-encoded object.
 * This is a simplified version focusing on Groovy data type serialization.
 */
abstract class CborObject implements Cborable {
    abstract byte[] toByteArray()
    
    @Override
    CborObject toCbor() {
        return this
    }
    
    static class CborMap extends CborObject {
        private final Map<CborObject, CborObject> values

        CborMap(Map<CborObject, CborObject> values) {
            this.values = values
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            result.write(encodeLength(MT_MAP, values.size()))
            values.each { key, value ->
                result.write(key.toByteArray())
                result.write(value.toByteArray())
            }
            return result.toByteArray()
        }
    }

    static class CborString extends CborObject {
        private final String value

        CborString(String value) {
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            def bytes = value.getBytes("UTF-8")
            def result = new ByteArrayOutputStream()
            result.write(encodeLength(MT_TEXT_STRING, bytes.length))
            result.write(bytes)
            return result.toByteArray()
        }
    }

    static class CborLong extends CborObject {
        private final long value

        CborLong(long value) {
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            if (value >= 0) {
                return encodeUnsignedInteger(value)
            } else {
                // For negative numbers, encode -1-n
                return encodeUnsignedInteger(-1 - value)
                    .with { bytes ->
                        bytes[0] = initialByte(MT_NEGATIVE_INT, bytes[0] & 0x1f)
                        return bytes
                    }
            }
        }
    }

    static class CborByteArray extends CborObject {
        private final byte[] value

        CborByteArray(byte[] value) {
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            result.write(encodeLength(MT_BYTE_STRING, value.length))
            result.write(value)
            return result.toByteArray()
        }
    }

    static class CborList extends CborObject {
        private final List<CborObject> value

        CborList(List<CborObject> value) {
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            result.write(encodeLength(MT_ARRAY, value.size()))
            value.each { item ->
                result.write(item.toByteArray())
            }
            return result.toByteArray()
        }
    }
} 