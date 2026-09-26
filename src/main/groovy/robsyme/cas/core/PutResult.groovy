package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a put answered (block explorer spec sections 9.1 and 9.3). */
@CompileStatic
final class PutResult {

    final Cid address
    final boolean dryRun
    final boolean exists
    final List<String> names
    final Map<String, Object> block
    final String entry
    final boolean written

    private PutResult(Cid address, boolean dryRun, boolean exists, List<String> names, Map<String, Object> block, String entry, boolean written) {
        this.address = address
        this.dryRun = dryRun
        this.exists = exists
        this.names = names
        this.block = block
        this.entry = entry
        this.written = written
    }

    static PutResult dryRun(Cid address, boolean exists, List<String> names) {
        return new PutResult(address, true, exists, names, null, null, false)
    }

    static PutResult written(Cid address, Map<String, Object> block, String entry, boolean written) {
        return new PutResult(address, false, true, null, block, entry, written)
    }

    byte[] body() {
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        out.put('address', address)
        if( dryRun ) {
            out.put('exists', exists)
            out.put('names', names)
        }
        else {
            out.put('block', block)
            out.put('entry', entry)
            out.put('written', written)
        }
        return DagJson.encode(out)
    }
}
