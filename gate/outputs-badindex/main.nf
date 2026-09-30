// Milestone 5's Gate pipeline (plan 2026-09-29): an index Nextflow fails to write (a CSV
// header on a tuple channel) still lets the run exit 0 (research output-dsl-meta-maps.md
// §2.2). Its own store, so the Test Pipeline's assertions and the browser tiers never see it.
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

workflow {
    main:
    tuples = SAMPLE(channel.of('A', 'B'))

    publish:
    tuples = tuples
}

output {
    tuples {
        path { meta, f -> "tuples/${meta.id}" }
        // A CSV header needs map records; a tuple channel makes Nextflow's CsvWriter throw
        // while the run still exits 0 (research output-dsl-meta-maps.md §2.2).
        index { path 'tuples/index.csv'; header true }
    }
}
