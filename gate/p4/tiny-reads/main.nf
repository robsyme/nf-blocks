// The P4 input sandbox's unrelated run (gate/p4/input-sandbox.sh): a few local
// FASTQ pairs the script writes, named by a samplesheet, counted by one process
// that takes `tuple val(meta), path(reads)`. It shares no input with the rnaseq
// runs, so the explorer must not suggest it alongside them.
params.input = null

process COUNT_READS {
    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}.counts.tsv")

    script:
    """
    for f in ${reads}; do
        printf '%s\\t%s\\n' "\$f" "\$(gzip -dc "\$f" | awk 'END { print NR / 4 }')"
    done > ${meta.id}.counts.tsv
    """
}

workflow {
    main:
    reads = channel.fromPath(params.input)
        .splitCsv(header: true)
        .map { row -> [[id: row.sample], [file(row.fastq_1), file(row.fastq_2)]] }
    counts = COUNT_READS(reads)

    publish:
    counts = counts
}

output {
    counts {
        path { meta, f -> "counts/${meta.id}" }
        index { path 'counts/index.json' }
    }
}
