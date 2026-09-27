package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The one run-reference resolver (decision 6 of the milestone 3 plan; ticket
 * 05 Q2), shared by {@code fromStore(run:)} and {@code nf-blocks:items --run}.
 * A reference is {@code latest} with a pipeline identity, a
 * {@code lid://<nextflow run hash>}, or a {@code cas://} Store URI naming a
 * RunCompletion or a RunManifest. The answer is the RunCompletion address.
 * The caller catches the index up first.
 */
@CompileStatic
final class RunRef {

    static final String LATEST = 'latest'
    static final String LID_PREFIX = 'lid://'
    static final String CAS_PREFIX = 'cas://'

    private RunRef() {}

    /**
     * @throws IllegalArgumentException the reference is malformed, or names a block that is not a run
     * @throws IllegalStateException the index or the composition cannot resolve it
     */
    static Cid resolve(Index index, BlockStore store, String ref, String pipeline) {
        if( !ref )
            throw new IllegalArgumentException("a run reference is needed: a cas:// Store URI, a lid://<hash>, or 'latest'")
        if( ref == LATEST ) {
            if( !pipeline )
                throw new IllegalArgumentException("run reference 'latest' needs a pipeline identity: pipeline: '<id>' in fromStore, --pipeline <id> for nf-blocks:items")
            return index.latestSuccessfulRun(pipeline)
                .orElseThrow { new IllegalStateException("run reference 'latest': no successful run of pipeline '${pipeline}' is recorded") }
        }
        if( ref.startsWith(LID_PREFIX) ) {
            final String hash = ref.substring(LID_PREFIX.length())
            if( !hash )
                throw new IllegalArgumentException("run reference '${ref}' has no run hash after lid://")
            return index.runByNextflowHash(hash)
                .orElseThrow { new IllegalStateException("run reference '${ref}': no run with nextflow run hash '${hash}' is recorded") }
        }
        if( ref.startsWith(CAS_PREFIX) )
            return fromStoreUri(index, store, ref)
        throw new IllegalArgumentException("unrecognised run reference '${ref}': expected a cas:// Store URI, a lid://<hash>, or 'latest'")
    }

    private static Cid fromStoreUri(Index index, BlockStore store, String ref) {
        Cid cid = null
        try {
            cid = StoreRef.parse(ref).cid
        }
        catch( IllegalArgumentException e ) {
            throw new IllegalArgumentException("run reference '${ref}' is not a Store URI: ${e.message}", e)
        }
        if( !store.has(cid) )
            throw new IllegalStateException("run reference '${ref}' names ${cid}, a block this composition does not hold")
        final String kind = kindOf(store, cid)
        if( kind == Records.RUN_COMPLETION )
            return cid
        if( kind == Records.RUN_MANIFEST )
            return index.runByManifest(cid)
                .orElseThrow { new IllegalStateException("run reference '${ref}' is a RunManifest with no RunCompletion; the run did not finish") }
        throw new IllegalArgumentException("run reference '${ref}' is ${kind ? "a ${kind}" : 'not a metadata'} block, not a run")
    }

    private static String kindOf(BlockStore store, Cid cid) {
        if( !cid.isDagCbor() )
            return null
        final InputStream input = store.open(cid)
        try {
            final Object decoded = DagCbor.decode(input.readAllBytes())
            return decoded instanceof Map ? Records.kindOf((Map) decoded) : null
        }
        finally {
            input.close()
        }
    }
}
