package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.PlaceholderName
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description
import nextflow.util.Duration
import nextflow.util.MemoryUnit

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
    @Description('Name recorded in every asserted block. When unset it comes from Seqera Platform, NXF_BLOCKS_USER, git user.email, then "anonymous" (see docs/identity.md).')
    String asserted_by

    @ConfigOption
    @Description('Overrides the Pipeline Identity recorded in the RunManifest.')
    String pipeline

    @ConfigOption
    @Description('Scratch directory for content of unknown length on its way to an S3 member. Defaults to java.io.tmpdir.')
    String tmpDir

    @ConfigOption
    @Description('Hash declared outputs on the task node and publish their addresses from .command.cas. Defaults to fusion.enabled.')
    Boolean nodeHash

    @Description('The derived SQLite index of the composition.')
    CasIndexScope index

    @Description('The Index Snapshot a member carries for nf-blocks:explore.')
    CasSnapshotScope snapshot

    @Description('What nf-blocks:sweep protects (DESIGN.md §19).')
    CasSweepScope sweep

    @Description('The files a run read from outside its work directory, recorded in its InputSet.')
    CasInputsScope inputs

    /** {@code cas.inputs}: how a run records the files it read (explorer v0.3 P4 plan 1.3). */
    @CompileStatic
    static class CasInputsScope implements ConfigScope {
        CasInputsScope() {}

        @ConfigOption
        @Description('Hash each local input file (once per run, off the task threads) so runs that read the same bytes share its address; a remote input is never read. When false, inputs are listed by name, size, role and sample without an address. Defaults to true.')
        Boolean hash

        @ConfigOption
        @Description('How long, in seconds, a completing run waits for the stat and hash of its inputs; a failed run waits at most 30 s of it. Inputs still being read then are listed without an address, with reason unfinished. A whole number, 0 or more. Defaults to 600.')
        Integer waitSeconds
    }

    /** {@code cas.sweep}: the age floor and the Trash grace period (ticket 20; plan decision 8). */
    @CompileStatic
    static class CasSweepScope implements ConfigScope {
        CasSweepScope() {}

        @ConfigOption
        @Description('A block younger than this is never trashed, whatever reaches it. At least 10m. Defaults to 14d.')
        Duration ageFloor

        @ConfigOption
        @Description('How long a trashed block waits before a sweep may delete it. Defaults to 14d.')
        Duration grace
    }

    /** {@code cas.snapshot}: the Index Snapshot a member carries for the explorer (DESIGN.md §15). */
    @CompileStatic
    static class CasSnapshotScope implements ConfigScope {
        CasSnapshotScope() {}

        @ConfigOption
        @Description('A run rewrites its member Index Snapshot only while it is under this size. Defaults to 64 MB.')
        MemoryUnit maxBytes
    }

    /** One configured store member: {@code cas.stores.<alias>.location}. */
    @CompileStatic
    static class CasStoreScope implements ConfigScope {
        CasStoreScope() {}

        @ConfigOption
        @Description('Where this member lives: a local directory or s3://<bucket>[/<prefix>].')
        String location

        @ConfigOption
        @Description('When true, nf-blocks:copy and the explorer never write to this member. Defaults to false; refused on the writable member.')
        Boolean readOnly
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
