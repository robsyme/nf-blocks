// Milestone 5's Gate pipeline (plan 2026-09-29): workflow outputs with a JSON and a CSV
// index, and one publishDir process whose files no run records. Its own store, so the
// Test Pipeline's assertions and the browser tiers never see it.
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

    publish:
    tuples = tuples
    records = tuples.map { meta, f -> [id: meta.id, file: f] }
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
