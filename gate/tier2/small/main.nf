// Gate tier two's small pipeline (T2b): ALIGN and QC_DIR of the Test Pipeline
// (.scratch/content-addressed-lineage/test-pipeline/main.nf), sample A only,
// published into the member T1 filled with Fusion on, so each address comes
// from the task node's .command.cas under `fusion-node`.
//
// The two processes and the two output {} entries are the Test Pipeline's,
// verbatim, and must stay byte-for-byte in step with it: T2b compares this
// run's `aligned` item addresses with T1's and its `qc` item addresses with
// t2's, so if the two drift apart T2b fails loudly rather than passing on
// different bytes. Its Pipeline Identity is cas-tier2-small (nextflow.config),
// so T4's fromStore(run: 'latest', pipeline: 'cas-test-pipeline') still finds t1.

process ALIGN {
    input:
    tuple val(meta), val(depth)

    output:
    tuple val(meta), path("${meta.sample}.bam"), emit: bam

    script:
    """
    printf 'BAM\\nsample\\t%s\\ndepth\\t%s\\n' '${meta.sample}' '${depth}' > ${meta.sample}.bam
    """
}

process QC_DIR {
    input:
    tuple val(meta), val(depth)

    output:
    tuple val(meta), path("${meta.sample}_qc"), emit: qc

    script:
    def dangle = params.dangling ? "ln -s ../nowhere.txt ${meta.sample}_qc/broken.txt" : "true"
    """
    mkdir -p ${meta.sample}_qc/nested
    printf 'summary for %s\\n' '${meta.sample}' > ${meta.sample}_qc/summary.txt
    printf 'detail\\n'                          > ${meta.sample}_qc/nested/detail.txt
    ln -s summary.txt ${meta.sample}_qc/alias.txt
    ${dangle}
    """
}

workflow {
    main:
    ch_in = channel.of(
        [ [sample: 'A', single_end: false, lane: 1, nested: [kit: 'truseq',  ids: [1, 2]]], 10 ],
    )

    ALIGN(ch_in)
    QC_DIR(ch_in)

    publish:
    aligned = ALIGN.out.bam
    qc      = QC_DIR.out.qc
}

output {
    aligned {
        path { meta, _bam -> "aligned/${meta.sample}" }
        mode 'copy'
        label 'bam'
    }
    qc {
        path { meta, _d -> "qc/${meta.sample}" }
        mode params.qc_mode
    }
}
