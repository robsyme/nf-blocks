// Milestone 5's Gate pipeline (plan 2026-09-29): workflow outputs with a JSON and a CSV
// index, one publishDir process whose files no run records, and one collectFile(storeDir:)
// that stores a file with no publish event at all (final review C1, as sarek's csv/ does).
// Its own store, so the Test Pipeline's assertions and the browser tiers never see it.
// Each record also carries a path from outside the store and the work dir (this script),
// which Nextflow never publishes: the join records it as a never_published leaf rather
// than losing the RunCompletion (patch 0.3.0-beta.2, nf-core/rnaseq's https inputs).
params.outdir = 'cas://lab'

process SAMPLE {
    input:
    val sample

    output:
    tuple val(meta), path("${sample}.txt")

    script:
    meta = [id: sample]
    """
    printf 'sample %s\\n' ${sample} > ${sample}.txt
    """
}

process LEGACY {
    publishDir "${params.outdir}/legacy", mode: 'copy'

    input:
    val sample

    output:
    path "${sample}.legacy"

    script:
    """
    printf 'legacy %s\\n' ${sample} > ${sample}.legacy
    """
}

workflow {
    main:
    samples = channel.of('A', 'B')
    tuples = SAMPLE(samples)
    LEGACY(samples)
    samples.collectFile(name: 'samples.txt', storeDir: "${params.outdir}/collected", newLine: true, sort: true)

    publish:
    tuples = tuples
    records = tuples.map { meta, f -> [id: meta.id, file: f, input: file("${projectDir}/main.nf")] }
}

output {
    tuples {
        path { meta, f -> "tuples/${meta.id}" }
        index { path 'tuples/index.json' }
    }
    records {
        path { r -> "records/${r.id}" }
        index { path 'records/index.csv'; header true }
    }
}
