package nextflow.blocks.unixfs

import groovy.transform.CompileStatic
import com.google.protobuf.ByteString
import nextflow.blocks.unixfs.proto.Unixfs.Data as ProtoData
import nextflow.blocks.unixfs.proto.Unixfs.UnixTime as ProtoUnixTime
import java.time.Instant

@CompileStatic
class UnixFsCodec {
    /**
     * Encodes a UnixFsData object to protocol buffer format
     */
    static byte[] encode(UnixFsData unixFsData) {
        def builder = ProtoData.newBuilder()
        
        // Set required type
        builder.setType(ProtoData.DataType.forNumber(unixFsData.type.toProto()))
        
        // Set optional data
        if (unixFsData.data) {
            builder.setData(ByteString.copyFrom(unixFsData.data))
        }
        
        // Set filesize if available (for FILE or RAW types)
        if (unixFsData.filesize != null) {
            builder.setFilesize(unixFsData.filesize)
        }
        
        // Set blocksizes if available (for FILE type)
        if (unixFsData.blocksizes) {
            unixFsData.blocksizes.each { size ->
                builder.addBlocksizes(size)
            }
        }
        
        // Set HAMT shard properties if available
        if (unixFsData.hashType != null) {
            builder.setHashType(unixFsData.hashType)
        }
        
        if (unixFsData.fanout != null) {
            builder.setFanout(unixFsData.fanout)
        }
        
        // Set file mode if available
        if (unixFsData.mode != null) {
            builder.setMode(unixFsData.mode)
        }
        
        // Set modification time if available
        if (unixFsData.mtime != null) {
            def timeBuilder = ProtoUnixTime.newBuilder()
            timeBuilder.setSeconds(unixFsData.mtime.epochSecond)
            int nanos = unixFsData.mtime.nano
            if (nanos > 0) {
                timeBuilder.setFractionalNanoseconds(nanos)
            }
            builder.setMtime(timeBuilder)
        }
        
        return builder.build().toByteArray()
    }
    
    /**
     * Decodes a protocol buffer byte array to a UnixFsData object
     */
    static UnixFsData decode(byte[] bytes) {
        def protoData = ProtoData.parseFrom(bytes)
        
        // Create UnixFsData with the appropriate type
        def dataType = UnixFsData.DataType.fromProto(protoData.getType().getNumber())
        def unixFsData = new UnixFsData(dataType)
        
        // Set data if available
        if (protoData.hasData()) {
            unixFsData.data = protoData.getData().toByteArray()
        }
        
        // Set filesize if available
        if (protoData.hasFilesize()) {
            unixFsData.filesize = protoData.getFilesize()
        }
        
        // Set blocksizes if available
        if (protoData.getBlocksizesCount() > 0) {
            unixFsData.blocksizes = protoData.getBlocksizesList() as List<Long>
        }
        
        // Set HAMT shard properties if available
        if (protoData.hasHashType()) {
            unixFsData.hashType = protoData.getHashType()
        }
        
        if (protoData.hasFanout()) {
            unixFsData.fanout = protoData.getFanout()
        }
        
        // Set file mode if available
        if (protoData.hasMode()) {
            unixFsData.mode = protoData.getMode()
        }
        
        // Set modification time if available
        if (protoData.hasMtime()) {
            def mtime = protoData.getMtime()
            long seconds = mtime.getSeconds()
            int nanos = mtime.hasFractionalNanoseconds() ? mtime.getFractionalNanoseconds() : 0
            unixFsData.mtime = Instant.ofEpochSecond(seconds, nanos)
        }
        
        return unixFsData
    }
} 