// F3 sandbox producer: a host-path input file, a val holding an absolute
// path that the script embeds, a work-dir output read by a second task, and a
// published output on cas://lab. STAGE reads params.db_password, so Nextflow
// records it in the TaskRun's globalVars as well as in the script.

params.ref  = null
params.note = 'none'
params.db_password = 'none'

process STAGE {
    input:
    path ref
    val note

    output:
    path 'out.txt'

    script:
    """
    cat ${ref} > out.txt
    echo 'note ${note}' >> out.txt
    # db ${params.db_password}
    """
}

process COUNT {
    input:
    path txt

    output:
    path 'count.txt'

    script:
    """
    wc -l < ${txt} | tr -d ' ' > count.txt
    """
}

workflow {
    main:
    staged = STAGE(channel.fromPath(params.ref), params.note)
    counted = COUNT(staged)

    publish:
    counts = counted
}

output {
    counts {
        path 'counts'
        mode 'copy'
        label 'count'
    }
}
