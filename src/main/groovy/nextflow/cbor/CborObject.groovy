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
        private final Map<CborString, CborObject> values

        CborMap(Map<CborString, CborObject> values) {
            // Validate all keys are strings
            values.keySet().each { key ->
                if (!(key instanceof CborString)) {
                    throw new IllegalArgumentException("DAG-CBOR map keys must be strings")
                }
            }
            this.values = values
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            result.write(encodeLength(MT_MAP, values.size()))
            
            // Sort keys by length first, then lexicographically
            def sortedKeys = values.keySet().sort { a, b ->
                def aBytes = a.value.getBytes("UTF-8")
                def bBytes = b.value.getBytes("UTF-8")
                if (aBytes.length != bBytes.length) {
                    return aBytes.length <=> bBytes.length
                }
                for (int i = 0; i < aBytes.length; i++) {
                    if (aBytes[i] != bBytes[i]) {
                        return (aBytes[i] & 0xFF) <=> (bBytes[i] & 0xFF)
                    }
                }
                return 0
            }

            sortedKeys.each { key ->
                result.write(key.toByteArray())
                result.write(values[key].toByteArray())
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

    static class CborFloat extends CborObject {
        private final double value

        CborFloat(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException("IEEE 754 special values are not supported in DAG-CBOR")
            }
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            result.write(initialByte(MT_FLOAT_OR_SIMPLE, AI_8_BYTES))
            def bits = Double.doubleToLongBits(value)
            result.write([
                (bits >>> 56) as byte,
                (bits >>> 48) as byte,
                (bits >>> 40) as byte,
                (bits >>> 32) as byte,
                (bits >>> 24) as byte,
                (bits >>> 16) as byte,
                (bits >>> 8) as byte,
                bits as byte
            ] as byte[])
            return result.toByteArray()
        }
    }

    static class CborTag extends CborObject {
        private final int tag
        private final CborObject value

        CborTag(int tag, CborObject value) {
            if (tag != 42) {
                throw new IllegalArgumentException("Only tag 42 (CID) is supported in DAG-CBOR")
            }
            this.tag = tag
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            def result = new ByteArrayOutputStream()
            // Tag 42 is always encoded as 0xd82a in DAG-CBOR
            result.write(0xd8 as byte)
            result.write(0x2a as byte)
            result.write(value.toByteArray())
            return result.toByteArray()
        }
    }

    static class CborBoolean extends CborObject {
        private final boolean value

        CborBoolean(boolean value) {
            this.value = value
        }

        @Override
        byte[] toByteArray() {
            // True is 0xf5 (MT_FLOAT_OR_SIMPLE | 21)
            // False is 0xf4 (MT_FLOAT_OR_SIMPLE | 20)
            return [value ? 0xf5 : 0xf4] as byte[]
        }
    }
} 