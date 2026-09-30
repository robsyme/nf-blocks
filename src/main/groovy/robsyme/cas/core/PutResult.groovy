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
    final String deletion
    final List<Cid> deletionClaims
    final String retain
    final List<Cid> retainClaims
    final List<Cid> pinClaims
    final List<String> pinNotes
    final Map<String, Object> block
    final String entry
    final boolean written

    private PutResult(Cid address, boolean dryRun, boolean exists, boolean here, List<String> names, List<Cid> nameClaims,
                       String deletion, List<Cid> deletionClaims, String retain, List<Cid> retainClaims,
                       List<Cid> pinClaims, List<String> pinNotes, Map<String, Object> block, String entry, boolean written) {
        this.address = address
        this.dryRun = dryRun
        this.exists = exists
        this.here = here
        this.names = names
        this.nameClaims = nameClaims
        this.deletion = deletion
        this.deletionClaims = deletionClaims
        this.retain = retain
        this.retainClaims = retainClaims
        this.pinClaims = pinClaims
        this.pinNotes = pinNotes
        this.block = block
        this.entry = entry
        this.written = written
    }

    static PutResult dryRun(Cid address, boolean exists, boolean here, ClaimState state) {
        return new PutResult(address, true, exists, here, state.names, state.nameClaims.collect { String c -> Cid.parse(c) },
            state.deletion, state.deletionClaims.collect { String c -> Cid.parse(c) },
            state.retain, state.retainClaims.collect { String c -> Cid.parse(c) },
            state.pinClaims.collect { String c -> Cid.parse(c) }, state.pinNotes, null, null, false)
    }

    static PutResult written(Cid address, Map<String, Object> block, String entry, boolean written) {
        return new PutResult(address, false, true, true, null, null, null, null, null, null, null, null, block, entry, written)
    }

    byte[] body() {
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        out.put('address', address)
        if( dryRun ) {
            out.put('exists', exists)
            out.put('here', here)
            out.put('names', names)
            out.put('name_claims', nameClaims)
            out.put('deletion', deletion)
            out.put('deletion_claims', deletionClaims)
            out.put('retain', retain)
            out.put('retain_claims', retainClaims)
            out.put('pin_claims', pinClaims)
            out.put('pins', pinNotes)
        }
        else {
            out.put('block', block)
            out.put('entry', entry)
            out.put('written', written)
        }
        return DagJson.encode(out)
    }
}
