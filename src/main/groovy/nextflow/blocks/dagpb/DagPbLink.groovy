package nextflow.blocks.dagpb

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames = true)
class DagPbLink {
    // CID of the target object (required)
    byte[] hash
    
    // UTF-8 string name (optional)
    String name
    
    // cumulative size of target object (optional)
    Long tsize
    
    DagPbLink(byte[] hash, String name = null, Long tsize = null) {
        this.hash = hash
        this.name = name
        this.tsize = tsize
    }
    
    // Compare links by name for sorting
    int compareTo(DagPbLink other) {
        if (!this.name && !other.name) return 0
        if (!this.name) return -1
        if (!other.name) return 1
        return this.name <=> other.name
    }
} 