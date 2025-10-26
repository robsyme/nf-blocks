// nextflow.preview.output = true

workflow {
    main:
    Channel.of("Paolo", "Ben")
    | MakeFile
    | map { name, greeting -> [name:name, greeting:greeting] }
    | set { mapped }

    publish:
    greetings = mapped
}

process MakeFile {
    input: val(name)
    output: tuple val(name), path("*.txt")
    script: "echo 'Hello $name' > greeting.${name}.txt"
}

output {
    greetings {
        path 'greetings'
    }
}

