package robsyme.cas.core

import groovy.transform.CompileStatic

/** What a put answered (block explorer spec sections 9.1 and 9.3). */
@CompileStatic
final class PutResult {

    final Cid address
    final boolean dryRun
    final boolean exists
    final boolean here
    final List<String> names
    final List<Cid> nameClaims
    final Map<String, Object> block
    final String entry
    final boolean written

    private PutResult(Cid address, boolean dryRun, boolean exists, boolean here, List<String> names, List<Cid> nameClaims,
                       Map<String, Object> block, String entry, boolean written) {
        this.address = address
        this.dryRun = dryRun
        this.exists = exists
        this.here = here
        this.names = names
        this.nameClaims = nameClaims
        this.block = block
        this.entry = entry
        this.written = written
    }

    static PutResult dryRun(Cid address, boolean exists, boolean here, List<String> names, List<Cid> nameClaims) {
        return new PutResult(address, true, exists, here, names, nameClaims, null, null, false)
    }

    static PutResult written(Cid address, Map<String, Object> block, String entry, boolean written) {
        return new PutResult(address, false, true, true, null, null, block, entry, written)
    }

    byte[] body() {
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        out.put('address', address)
        if( dryRun ) {
            out.put('exists', exists)
            out.put('here', here)
            out.put('names', names)
            out.put('name_claims', nameClaims)
        }
        else {
            out.put('block', block)
            out.put('entry', entry)
            out.put('written', written)
        }
        return DagJson.encode(out)
    }
}
