// The Gate's second pipeline: read the Test Pipeline's outputs back out of
// the store four ways and hash whatever Nextflow actually staged.
//
// It computes nothing. Each branch stages files and writes `sha256sum` of the
// staged bytes to `hashes/<source>/<staged name>.sha256`. gate/assert.py
// compares those digests against digests it computed itself from pipeline-a's
// work directory, and counts the files under each source, so a wrong file, a
// truncated file, an empty stage or the wrong number of items is caught by the
// bytes rather than by a record.
//
//   fromstore     channel.fromStore(run: 'latest', ..., where: [sample: 'B'])
//   lid           channel.fromPath('lid://<run>/aligned/A/A.bam')
//   cas           channel.fromPath('cas://<cid>/A.bam')
//   fromlineage   channel.fromLineage(workflowRun: '<run lid>', label: 'bam')
//
// gate.sh discovers the URIs from the store after the producer has run and
// passes them as --lid, --cas and --run_lid. The Pipeline Identity is fixed by
// `manifest.name` in gate/gate.config, so fromStore names it literally.

include { fromStore } from 'plugin/nf-blocks'
include { fromLineage } from 'plugin/nf-lineage'

process HASH {
    tag "${source}:${staged.name}"

    input:
    tuple val(source), path(staged)

    output:
    tuple val(source), path("${staged.name}.sha256"), emit: sha

    script:
    // sha256sum on Linux, shasum -a 256 on macOS; identical output format.
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum '${staged}' > '${staged.name}.sha256'
    else
        shasum -a 256 '${staged}' > '${staged.name}.sha256'
    fi
    """
}

workflow {
    main:
    if( !params.lid && !params.cas && !params.run_lid )
        error "consumer needs --lid, --cas and --run_lid (gate.sh reads them from the store)"

    ch_lid = params.lid
        ? channel.fromPath(params.lid).map { f -> tuple('lid', f) }
        : channel.empty()

    ch_cas = params.cas
        ? channel.fromPath(params.cas).map { f -> tuple('cas', f) }
        : channel.empty()

    // The third load-bearing query: aligned outputs where sample == 'B'.
    // Exactly one item must come back, restored to [meta, path].
    ch_store = channel
        .fromStore(run: 'latest', pipeline: 'cas-test-pipeline',
                   output: 'aligned', where: [sample: 'B'])
        .map { meta, bam -> tuple('fromstore', bam) }

    // Native lineage read-back must keep working alongside ours.
    ch_lineage = params.run_lid
        ? channel.fromLineage(workflowRun: params.run_lid, label: 'bam')
            .flatten()
            .map { f -> tuple('fromlineage', f) }
        : channel.empty()

    HASH(ch_lid.mix(ch_cas, ch_store, ch_lineage))

    publish:
    hashes = HASH.out.sha
}

output {
    hashes {
        path { source, _f -> "hashes/${source}" }
        mode 'copy'
    }
}
