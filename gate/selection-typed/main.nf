// gate/selection-typed/main.nf
// Tier B's typed consumer (block explorer spec section 1.3, assertion B18;
// UX map tickets 07 Q6 and 09 Q5): read the Selection S2 in a typed script
// and hand each item to a process whose input is typed with record types.
//
// tier_b.sh copies this file through `browser_b_assert.py consumer`, which
// replaces the line ending in `// @snippet` with the page's
// [data-snippet="typed"] text, verbatim. That text calls
// nextflow.Channel.fromStore(..., records: true): under
// nextflow.enable.types a plugin factory cannot be reached as
// channel.fromStore (nextflow-io/nextflow#7694, DESIGN.md §13).
//
// Every item of S2 is [meta, file]. The process takes the meta map as a
// Sample and its nested map as a Kit, so TaskProcessor checks both (v26.04.6
// TaskProcessor.groovy:1872-1891): a plain LinkedHashMap logs "invalid
// argument type", a RecordMap passes. browser_b_assert.py reads this run's
// .nextflow.log for that warning and compares the published digests with its
// own hashes of pipeline-a's work files.
//
// nextflow.config has no outputDir: unset, it is the writable member's alias
// (DESIGN.md §2).

nextflow.enable.types = true

include { fromStore } from 'plugin/nf-blocks'

params {
    selection: String
}

record Kit {
    kit: String
}

record Sample {
    sample: String
    lane: Integer
    nested: Kit
}

process HASH {
    tag "typed:${staged.name}"

    input:
    tuple(meta: Sample, kit: Kit, staged: Path)

    output:
    file("${staged.name}.sha256")

    script:
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum '${staged}' > '${staged.name}.sha256'
    else
        shasum -a 256 '${staged}' > '${staged.name}.sha256'
    fi
    """
}

workflow {
    main:
    ch_items = nextflow.Channel.fromStore(selection: params.selection, records: true)   // @snippet

    ch_hashes = HASH(ch_items.map { item -> tuple(item[0], item[0].nested, item[1]) })

    publish:
    hashes = ch_hashes
}

output {
    hashes {
        path 'hashes/typed'
        mode 'copy'
    }
}
