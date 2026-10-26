// Per-pipeline config (P1a): the join key that names an item's sample, the
// metadata fields worth a column of their own, how to describe each output,
// and which outputs a hand-off ticks by default. A field's `path` is a dotted
// path into an item's metadata; with `output` it counts only for that output.

/** Config per pipeline name; `pipelineConfig` fills in the defaults. */
export const PIPELINES = {
  'nf-core/rnaseq': {
    fields: [
      { output: 'preprocessed', path: 'num_trimmed_reads', label: 'Reads (trimmed)', format: 'int' },
      { output: 'aligned', path: 'percent_mapped', label: '% mapped', format: 'percent' },
      { path: 'meta.single_end', label: 'Layout', labels: { true: 'single', false: 'paired' } },
      { path: 'meta.strandedness', label: 'Strand' },
    ],
    outputs: {
      quant_salmon: { label: 'Salmon quant dir', has: 'quant' },
      markdup: { label: 'BAM + index', has: 'BAM' },
      bigwig: { label: 'bigWig', has: 'bigWig' },
      preprocessed: { label: 'FastQC + trim reports' },
      bam_qc: { label: 'BAM QC', has: 'QC' },
      quant_merged: { label: 'Merged count matrices', merged: { matrix: 'salmon.merged.gene_counts.tsv' } },
      multiqc: { label: 'MultiQC report' },
    },
    handoff: ['quant_salmon', 'quant_merged'],
  },
}

/** `{ joinKey, fields, outputs, handoff }` for a pipeline; an unknown one joins on `meta.id`, then `id`, and has nothing else. */
export function pipelineConfig(pipeline) {
  const c = Object.hasOwn(PIPELINES, pipeline ?? '') ? PIPELINES[pipeline] : {}
  return { joinKey: c.joinKey ?? ['meta.id', 'id'], fields: c.fields ?? [], outputs: c.outputs ?? {}, handoff: c.handoff ?? [] }
}

/** A field's key in a where filter and a column: `output:path` when it names an output, else `path`. */
export const fieldKey = (field) => (field.output ? `${field.output}:${field.path}` : field.path)

/** A where key split into its output and path: qualified only when the text before the first `:` is one of `outputs`. */
export function splitKey(key, outputs) {
  const at = key.indexOf(':')
  const output = at > 0 ? key.slice(0, at) : null
  return output !== null && new Set(outputs).has(output) ? { output, path: key.slice(at + 1) } : { output: null, path: key }
}

/** The pipeline's promoted fields: with `output`, those with no output or that one; without, all. */
export const promotedFor = (pipeline, { output } = {}) => pipelineConfig(pipeline).fields
  .filter(f => output === undefined || output === null || !f.output || f.output === output)

/** The last dotted segment of a path. */
export const shortLabel = (path) => path.slice(path.lastIndexOf('.') + 1)

/**
 * For each field, in field order, the path it shows: one equal to the field's
 * path, else the shortest path ending in `.${field.path}`. Fields with no
 * match are left out.
 */
export function matchPromoted(fields, paths) {
  const out = []
  for (const field of fields) {
    let path = paths.find(p => p === field.path)
    if (path === undefined) {
      const tail = `.${field.path}`
      for (const p of paths) if (p.endsWith(tail) && (path === undefined || p.length < path.length)) path = p
    }
    if (path !== undefined) out.push({ field, path })
  }
  return out
}
