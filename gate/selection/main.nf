// gate/selection/main.nf
// Tier B's pipeline (block explorer spec section 1.3 assertions 9 and 13):
// read one Selection two ways and hash whatever Nextflow staged.
//
//   fromstore    the Selection view's untyped snippet: every file of every item
//   samplesheet  the explorer's CSV export, column '1', staged with file()
//
// tier_b.sh copies this file through `browser_b_assert.py consumer`, which
// replaces the line ending in `// @snippet` with the page's
// [data-snippet="untyped"] text, verbatim (DESIGN.md §15): a broken snippet
// fails B9. Run by hand, the line reads params.selection.
//
// It computes nothing: gate/browser_b_assert.py compares the digests with its
// own hashes of pipeline-a's work files.

include { fromStore } from 'plugin/nf-blocks'

process HASH {
    tag "${source}:${staged.name}"

    input:
    tuple val(source), path(staged)

    output:
    tuple val(source), path("${staged.name}.sha256"), emit: sha

    script:
    """
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum '${staged}' > '${staged.name}.sha256'
    else
        shasum -a 256 '${staged}' > '${staged.name}.sha256'
    fi
    """
}

/** Every file in a restored item, wherever it sits in the structure. */
def filesOf(value) {
    if( value instanceof java.nio.file.Path )
        return [value]
    if( value instanceof Map )
        return value.values().collectMany { v -> filesOf(v) }
    if( value instanceof List )
        return value.collectMany { v -> filesOf(v) }
    return []
}

workflow {
    main:
    if( !params.selection || !params.samplesheet )
        error "selection pipeline needs --selection and --samplesheet (tier_b.sh passes both)"

    ch_items = channel.fromStore(selection: params.selection)   // @snippet

    ch_store = ch_items
        .flatMap { item -> filesOf(item).collect { f -> tuple('fromstore', f) } }

    ch_sheet = channel
        .fromPath(params.samplesheet)
        .splitCsv(header: true, quote: '"')   // RFC 4180: list cells are quoted JSON with commas
        .map { row -> tuple('samplesheet', file(row['1'])) }

    HASH(ch_store.mix(ch_sheet))

    publish:
    hashes = HASH.out.sha
}

output {
    hashes {
        path { source, _f -> "hashes/${source}" }
        mode 'copy'
    }
}
