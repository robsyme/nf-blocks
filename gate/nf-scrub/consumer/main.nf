// F3 sandbox consumer: read records back through lid://, the way a downstream
// pipeline stages a lineage file. --lid is one or more comma-separated lid://
// paths; --run, when set, also tries channel.fromLineage(workflowRun: ...).

params.lid = null
params.run = null

process READ {
    input:
    path f

    output:
    stdout

    script:
    """
    printf '%s: ' '${f.name}'
    tr '\\n' ' ' < ${f}
    echo
    """
}

workflow {
    files = channel.fromList(params.lid.tokenize(',')).map { file(it) }
    if( params.run )
        files = files.mix(channel.fromLineage(workflowRun: params.run))
    READ(files).view { "staged ${it.trim()}" }
}
