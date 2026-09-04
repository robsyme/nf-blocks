package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.PlaceholderName
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

/**
 * Declares the {@code cas} configuration scope so {@code ConfigValidator} stops
 * warning {@code Unrecognized config option} for every {@code cas.*} key on
 * every run (DESIGN.md §2).
 *
 * The validator reflects over this class's fields (never instantiating it
 * beyond the extension-point no-arg constructor): a {@code @ConfigOption} field
 * is an option, a field whose type implements {@link ConfigScope} is a nested
 * scope, and a {@code Map<String, ? extends ConfigScope>} annotated
 * {@code @PlaceholderName} is a scope of arbitrarily-named child scopes -- which
 * is exactly {@code cas.stores.<alias>.location}. The runtime parsing lives in
 * {@link CasConfig}; this class carries no behaviour.
 *
 * At v26.04.6 the live interfaces are {@code nextflow.config.spec.*}; the
 * {@code nextflow.config.schema.*} names DESIGN.md mentions are deprecated
 * aliases the validator no longer scans, so {@code config.spec} is used here.
 */
@ScopeName('cas')
@Description('Content-addressed lineage store (nf-blocks).')
@CompileStatic
class CasConfigScope implements ConfigScope {

    /* required by the extension-point mechanism -- do not remove */
    CasConfigScope() {}

    @PlaceholderName('<alias>')
    @Description('One store member per alias; the alias in lineage.store.location is the writable member.')
    Map<String, CasStoreScope> stores

    @ConfigOption
    @Description('Store aliases to resolve, writable first. Defaults to every configured store.')
    List resolve

    @ConfigOption
    @Description('Opaque label recorded in every asserted block. Defaults to "anonymous".')
    String asserted_by

    @ConfigOption
    @Description('Overrides the Pipeline Identity recorded in the RunManifest.')
    String pipeline

    CasIndexScope index

    /** One configured store member: {@code cas.stores.<alias>.location}. */
    @CompileStatic
    static class CasStoreScope implements ConfigScope {
        CasStoreScope() {}

        @ConfigOption
        @Description('Filesystem path (a local directory in the skeleton) of this store member.')
        String location
    }

    /** {@code cas.index}: where the derived SQLite index lives. */
    @CompileStatic
    static class CasIndexScope implements ConfigScope {
        CasIndexScope() {}

        @ConfigOption
        @Description('Overrides the SQLite index cache path. Defaults to a per-user cache file.')
        String path
    }
}
