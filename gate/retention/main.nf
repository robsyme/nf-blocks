// Milestone 6's Gate pipeline (plan 2026-09-30): two runs (--tag b, then --tag a) that share
// one file and one file inside a directory, and each hold files of their own. Its own store,
// store-retention, so no other assertion or browser tier sees a sweep.
params.tag = 'a'

process FILE {
    input:
    val name

    output:
    tuple val(meta), path("${name}.txt")

    script:
    meta = [id: name]
    """
    printf '%s\\n' ${name} > ${name}.txt
    """
}

process DIR {
    output:
    tuple val(meta), path("dir_${params.tag}")

    script:
    meta = [id: "dir_${params.tag}"]
    """
    mkdir dir_${params.tag}
    printf '%s one\\n' ${params.tag} > dir_${params.tag}/one.txt
    printf 'shared two\\n' > dir_${params.tag}/two.txt
    """
}

workflow {
    main:
    files = FILE(channel.of('shared', "only_${params.tag}", "pin_${params.tag}"))
    dirs = DIR()

    publish:
    files = files
    dirs = dirs
}

output {
    files {
        path { meta, f -> "files/${meta.id}" }
    }
    dirs {
        path { meta, d -> "dirs/${meta.id}" }
    }
}
