package nextflow.blocks.unixfs

import groovy.transform.CompileStatic
import groovy.transform.ToString
import java.time.Instant

@CompileStatic
@ToString(includeNames = true)
class UnixFsData {
    enum DataType {
        RAW, 
        DIRECTORY, 
        FILE, 
        METADATA, 
        SYMLINK, 
        HAMT_SHARD
        
        static DataType fromProto(int value) {
            switch (value) {
                case 0: return RAW
                case 1: return DIRECTORY
                case 2: return FILE
                case 3: return METADATA
                case 4: return SYMLINK
                case 5: return HAMT_SHARD
                default: throw new IllegalArgumentException("Unknown UnixFS DataType: ${value}")
            }
        }
        
        int toProto() {
            switch (this) {
                case RAW: return 0
                case DIRECTORY: return 1
                case FILE: return 2
                case METADATA: return 3
                case SYMLINK: return 4
                case HAMT_SHARD: return 5
                default: throw new IllegalArgumentException("Unknown UnixFS DataType: ${this}")
            }
        }
    }
    
    // Required type
    DataType type
    
    // Data contents (optional)
    byte[] data
    
    // File size for FILE or RAW types
    Long filesize
    
    // Block sizes for FILE type
    List<Long> blocksizes = []
    
    // HAMT Shard properties
    Long hashType
    Long fanout
    
    // POSIX file mode
    Integer mode
    
    // Modification time
    Instant mtime
    
    UnixFsData(DataType type) {
        this.type = type
    }
    
    static UnixFsData file(byte[] data = null, Long filesize = null) {
        def unixFsData = new UnixFsData(DataType.FILE)
        unixFsData.data = data
        unixFsData.filesize = filesize
        return unixFsData
    }
    
    static UnixFsData raw(byte[] data, Long filesize = null) {
        def unixFsData = new UnixFsData(DataType.RAW)
        unixFsData.data = data
        unixFsData.filesize = filesize ?: (data ? data.length as Long : null)
        return unixFsData
    }
    
    static UnixFsData directory() {
        return new UnixFsData(DataType.DIRECTORY)
    }
    
    static UnixFsData symlink(byte[] targetPath) {
        def unixFsData = new UnixFsData(DataType.SYMLINK)
        unixFsData.data = targetPath
        return unixFsData
    }
    
    void addBlockSize(long size) {
        if (blocksizes == null) {
            blocksizes = []
        }
        blocksizes.add(size)
    }
} 