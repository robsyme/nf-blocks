package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The Address Providers a run may record (DESIGN.md §6, ticket 16). The
 * provider is recorded by the run, in the RunCompletion, never in a
 * content-derived block, so no choice of provider changes an address.
 */
@CompileStatic
final class Providers {
    /** The head node streamed the bytes and hashed them: self-computed. */
    static final String HEAD_NODE = 'head-node'
    /** The task node hashed its outputs (.command.cas): asserted. */
    static final String FUSION_NODE = 'fusion-node'
    /** S3 computed the SHA-256 during a server-side copy: asserted. */
    static final String S3_COPY = 's3-copy'

    static final List<String> ALL = [HEAD_NODE, FUSION_NODE, S3_COPY].asImmutable()

    private Providers() {}

    static boolean isKnown(String name) { ALL.contains(name) }
}
