// The Gate's second pipeline: read the Test Pipeline's outputs back out of
// the store three ways and hash whatever Nextflow actually staged.
//
// It computes nothing. Each branch stages one file and writes `sha256sum` of
// the staged bytes. gate/assert.py compares those digests against digests it
// computed itself from pipeline-a's work directory, so a wrong file, a
// truncated file or an empty stage is caught by the bytes and not by a record.
//
//   fromstore  channel.fromStore(run: 'latest', ..., where: [sample: 'B'])
//   lid        channel.fromPath('lid://<run>/aligned/A/A.bam')
//   cas        channel.fromPath('cas://<cid>/A.bam')
//
// gate.sh discovers the two URIs from the store after the producer has run
// and passes them as --lid and --cas. The Pipeline Identity is fixed by
// `manifest.name` in gate/gate.config, so fromStore can name it literally.

include { fromStore } from 'plugin/nf-blocks'

process HASH {
    tag "${tag}"

    input:
    tuple val(tag), path(staged)

    output:
    tuple val(tag), path("${tag}.sha256"), emit: sha

    script:
    // sha256sum on Linux, shasum -a 256 on macOS; identical output format.
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum ${staged} > ${tag}.sha256
    else
        shasum -a 256 ${staged} > ${tag}.sha256
    fi
    """
}

workflow {
    main:
    if( !params.lid && !params.cas )
        error "consumer needs --lid and --cas (gate.sh reads them from the store)"

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

    HASH(ch_lid.mix(ch_cas, ch_store))

    publish:
    hashes = HASH.out.sha
}

output {
    hashes {
        path { tag, _f -> "hashes/${tag}" }
        mode 'copy'
    }
}
