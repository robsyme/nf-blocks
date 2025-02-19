package nextflow.blocks.dagpb

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames = true)
class DagPbNode {
    // List of links to other objects (required, may be empty)
    List<DagPbLink> links = []
    
    // Optional opaque user data
    byte[] data
    
    DagPbNode(List<DagPbLink> links = [], byte[] data = null) {
        this.links = new ArrayList<>(links)
        this.data = data
        sortLinks()
    }
    
    private void sortLinks() {
        // Sort links by name as required by spec
        links.sort { a, b -> a.compareTo(b) }
    }
    
    void addLink(DagPbLink link) {
        links.add(link)
        sortLinks()
    }
} 