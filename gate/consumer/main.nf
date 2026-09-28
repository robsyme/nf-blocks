// The Gate's second pipeline: read the Test Pipeline's outputs back out of
// the store three ways and hash whatever Nextflow actually staged.
//
// It computes nothing. Each branch stages a file and writes `sha256sum` of the
// staged bytes to `hashes/<source>/<staged name>.sha256`. gate/assert.py
// compares those digests against digests it computed itself from pipeline-a's
// work directory, and counts the files under each source, so a wrong file, a
// truncated file, an empty stage or the wrong number of items is caught by the
// bytes rather than by a record.
//
//   fromstore  channel.fromStore(run: 'latest', ..., where: [sample: 'B'])
//   lid        channel.fromPath('lid://<run>/aligned/A/A.bam')
//   cas        channel.fromPath('cas://<cid>/A.bam')
//   dir        channel.fromPath('cas://<collection>/<item>/A_qc', type: 'dir'),
//              an Item Occurrence naming a directory; tier two's T4 only
//
// gate.sh discovers the URIs from the store after the producer has run and
// passes them as --lid and --cas. The Pipeline Identity is fixed by
// `manifest.name` in gate/gate.config, so fromStore names it literally.
//
// channel.fromLineage over this store is a real product property, but it
// cannot be exercised from here: declaring a custom `plugins { id 'nf-blocks' }`
// block stops Nextflow auto-loading nf-lineage, so the call dies at launch with
// "Cannot find latest version of nf-lineage plugin". It is left out of the
// skeleton Gate deliberately.

include { fromStore } from 'plugin/nf-blocks'

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

// A staged directory, hashed file by file with links followed: one
// sha256sum line per file, sorted, in `<dir name>.sha256`.
process HASH_DIR {
    tag "${source}:${staged.name}"

    input:
    tuple val(source), path(staged)

    output:
    tuple val(source), path("${staged.name}.sha256"), emit: sha

    script:
    """
    find -L '${staged}' -type f | LC_ALL=C sort | while read -r f; do sha256sum "\$f" 2>/dev/null || shasum -a 256 "\$f"; done > '${staged.name}.sha256'
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

    // Tier two T4 only: a directory input, to measure what staging a manifest into S3 does (ticket 15 decision 7).
    ch_dir = params.dir
        ? channel.fromPath(params.dir, type: 'dir').map { d -> tuple('dir', d) }
        : channel.empty()

    // The third load-bearing query: aligned outputs where sample == 'B'.
    // Exactly one item must come back, restored to [meta, path].
    ch_store = channel
        .fromStore(run: 'latest', pipeline: 'cas-test-pipeline',
                   output: 'aligned', where: [sample: 'B'])
        .map { meta, bam -> tuple('fromstore', bam) }

    HASH(ch_lid.mix(ch_cas, ch_store))
    HASH_DIR(ch_dir)

    publish:
    hashes = HASH.out.sha.mix(HASH_DIR.out.sha)
}

output {
    hashes {
        path { source, _f -> "hashes/${source}" }
        mode 'copy'
    }
}
