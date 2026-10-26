// web/src/templates/differentialabundance.js
// The hand-off template for nf-core/differentialabundance (P4 Task 5,
// mockups/14-handoff-de.html, plan 1.5 and section 3). It declares what the
// pipeline takes from an nf-core/rnaseq run: a samplesheet of `sample` and
// one grouping column (`condition`), a contrasts file, the merged gene count
// matrix and an optional GTF. Pure: the samplesheet and contrasts text (LF
// endings, a header row, a trailing newline), their raw cids, the checks the
// page shows live, the params and the run command. views/handoff-de.js
// draws them.
//
// Section 3 decides the matrix: differentialabundance subsets the matrix to
// the samplesheet's samples itself, so the whole matrix goes with a subset
// samplesheet, and the check that matters is that every samplesheet sample is
// a matrix column. `--gtf` is optional there, so its check only warns.
import { cidFor, RAW } from '../cid.js'
import { formatNumber } from '../format.js'

export const TEMPLATE = 'nf-core/differentialabundance'

/** The template contract (mockup 14's spec): columns, the matrix source, the extra files and the rules. */
export const CONTRACT = Object.freeze({
  template: TEMPLATE,
  /** The pipeline whose runs it starts from. */
  from: 'nf-core/rnaseq',
  samplesheet: { file: 'samplesheet.csv', param: 'input', id: 'sample', grouping: 'condition' },
  contrasts: { file: 'contrasts.csv', param: 'contrasts', columns: ['id', 'variable', 'reference', 'target'] },
  matrix: { output: 'quant_merged', file: 'salmon.merged.gene_counts.tsv', param: 'matrix' },
  gtf: { param: 'gtf', role: 'param:gtf', required: false },
  /** The per-sample outputs the Selection takes when the hand-off names none. */
  outputs: ['quant_salmon'],
  params: { study_type: 'rnaseq' },
  /** `revision` is the tag the defaults and checks were read against (section 3), so the command never runs the default branch. */
  run: { revision: '2.0.0', profile: 'docker', outdir: 'results' },
  minReplicates: 2,
  rules: [
    { key: 'values', blocking: true, rule: 'sample IDs without whitespace (shinyngs ^\\S+$); a condition of letters, digits, _ . and - for each sample' },
    { key: 'contrast', blocking: true, rule: 'two different conditions' },
    { key: 'replicates', blocking: true, rule: 'at least 2 samples on each side of the contrast' },
    { key: 'matrix', blocking: true, rule: 'every samplesheet sample is a column in the matrix' },
    { key: 'gtf', blocking: false, rule: 'a GTF; without one the matrix is the feature table' },
  ],
})

const SHEET = CONTRACT.samplesheet
const BLOCKING = new Set(CONTRACT.rules.filter(r => r.blocking).map(r => r.key))

/** shinyngs reads the observations' id column with `^\S+$`. */
export const sampleIdOk = (id) => /^\S+$/.test(id ?? '')
/** A condition the samplesheet, the contrast id and DESeq2's level names all take as is. */
export const conditionOk = (value) => /^[A-Za-z0-9_.-]+$/.test(value ?? '')

/** A CSV field, quoted (quotes doubled) when it holds a comma, a quote or a line break. */
const field = (v) => (/[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v)
const csv = (rows) => rows.map(r => `${r.map(field).join(',')}\n`).join('')

/** samplesheet.csv: `sample,condition` and one row per `{ sample, condition }`, in order. */
export const samplesheetText = (rows) => csv([[SHEET.id, SHEET.grouping], ...rows.map(r => [r.sample, r.condition ?? ''])])

/** contrasts.csv for `target` against `reference`, or null without two different conditions. */
export function contrastsText({ target, reference }) {
  if (!target || !reference || target === reference) return null
  return csv([CONTRACT.contrasts.columns, [`${target}_vs_${reference}`, SHEET.grouping, reference, target]])
}

/** A file's raw-block address, as the server will store its UTF-8 bytes: CIDv1, raw codec, sha2-256, base32. */
export const rawCidOf = (text) => cidFor(new TextEncoder().encode(text), RAW).toString()

/** The generated files, `[{ name, text, address }]`: samplesheet.csv, then contrasts.csv once there is a contrast. The server sorts them by name in the Handoff. */
export function filesFor(rows, contrast) {
  const contrasts = contrastsText(contrast)
  return [
    { name: SHEET.file, text: samplesheetText(rows) },
    contrasts === null ? null : { name: CONTRACT.contrasts.file, text: contrasts },
  ].filter(Boolean).map(f => ({ ...f, address: rawCidOf(f.text) }))
}

/** The conditions of `rows` (the included samples), each once, in the order the rows first give them; empty ones left out. */
export const levelsOf = (rows) => [...new Set(rows.map(r => r.condition).filter(Boolean))]

/**
 * The contrast for `levels`: each of `previous`'s picks (the reader's) while
 * it is still a level. Otherwise the target is the first level with enough
 * replicates (`count(level)`), else the first level; the reference is the
 * next such level other than the target, else the first other level, else
 * the target's.
 */
export function pickContrast(levels, previous = {}, count = () => 0) {
  const full = levels.filter(l => count(l) >= CONTRACT.minReplicates)
  const target = levels.includes(previous.target) ? previous.target : (full[0] ?? levels[0] ?? '')
  const reference = levels.includes(previous.reference) ? previous.reference
    : (full.find(l => l !== target) ?? levels.find(l => l !== target) ?? levels[0] ?? '')
  return { target, reference }
}

const listed = (names) => (names.length <= 1 ? names.join('') : `${names.slice(0, -1).join(', ')} and ${names.at(-1)}`)
const sampleWord = (n) => (n === 1 ? 'one sample' : `${formatNumber(n)} samples`)

/** What would break the samplesheet: whitespace in a sample, a condition missing or with other characters. Null when nothing does. */
function valuesProblem(rows) {
  const spaced = rows.filter(r => !sampleIdOk(r.sample)).map(r => `"${r.sample}"`)
  const bad = rows.filter(r => r.condition && !conditionOk(r.condition)).map(r => `"${r.condition}" (${r.sample})`)
  const empty = rows.filter(r => !r.condition).map(r => r.sample)
  const parts = [
    spaced.length ? `Sample IDs cannot hold spaces: ${spaced.join(', ')}.` : null,
    bad.length ? `Conditions may use only letters, digits, _ . and -: ${bad.join(', ')}.` : null,
    empty.length ? `${listed(empty)} ${empty.length === 1 ? 'has' : 'have'} no condition.` : null,
  ].filter(Boolean)
  return parts.length ? parts.join(' ') : null
}

/**
 * The checks, live (mockup 14), `[{ key, ok, tone, blocking, text }]` in the
 * contract's order; `values` only when it fails. `rows` are the included
 * samples `{ sample, condition }`; `contrast` `{ target, reference }`;
 * `matrix` `{ state: 'ready', columns }` (matrixSamples of its header),
 * `'reading'`, `'missing'` or `'failed'` with `message`; `gtf` `{ value,
 * label, store }` (value null when none is given, `store` names where it
 * was not found); `filter` `{ text, only }` when a where filter is in force,
 * `only` when the included samples are exactly the ones it passes.
 */
export function checksFor({ rows, contrast, matrix, gtf, filter = null }) {
  const { target, reference } = contrast
  const n = (level) => (level ? rows.filter(r => r.condition === level).length : 0)
  const out = []
  const add = (key, ok, text, tone = ok ? 'ok' : 'warn') => out.push({ key, ok, tone, blocking: BLOCKING.has(key), text })

  const problem = valuesProblem(rows)
  if (problem) add('values', false, problem)

  const two = !!target && !!reference && target !== reference
  add('contrast', two, two ? `Contrast ${target} vs ${reference}` : 'Pick two different conditions')

  // One condition picked twice is one side: the other has none.
  const min = CONTRACT.minReplicates
  const other = two ? reference : ''
  const enough = two && n(target) >= min && n(reference) >= min
  let why = ''
  if (!enough && two && filter?.only) {
    why = n(target) === n(reference) ? ` The ${filter.text} filter leaves ${sampleWord(n(target))} per condition.`
      : ` The ${filter.text} filter leaves ${formatNumber(n(target))} ${target} and ${formatNumber(n(reference))} ${reference}.`
  }
  add('replicates', enough, enough ? `Replicates: ${target} ${n(target)}, ${reference} ${n(reference)}`
    : `Each side needs at least ${min} replicates: ${target || '—'} has ${n(target)}, ${other || '—'} has ${n(other)}.${why}`)

  const m = CONTRACT.matrix
  if (matrix?.state === 'ready') {
    const columns = new Set(matrix.columns)
    const missing = rows.map(r => r.sample).filter(s => !columns.has(s))
    add('matrix', missing.length === 0, missing.length === 0 ? `All ${rows.length} samples are columns in the matrix`
      : `${missing.join(', ')} ${missing.length === 1 ? 'is not a column' : 'are not columns'} in the matrix, `
        + 'and differentialabundance stops on a samplesheet sample the matrix lacks.')
  } else if (matrix?.state === 'missing') add('matrix', false, `This run has no ${m.file} in ${m.output}.`)
  else if (matrix?.state === 'failed') add('matrix', false, `Could not read the matrix's header: ${matrix.message}`)
  else add('matrix', false, "Reading the matrix's header…")

  const given = (gtf?.value ?? '').trim()
  add('gtf', !!given, given ? `GTF: ${gtf.label ?? given}`
    : `GTF not in ${gtf?.store ?? 'this store'}: rnaseq read it by path. Without one, differentialabundance uses the matrix as its feature table, `
      + 'and the annotated results may carry counts for samples left out. Give a path or a cas:// address.')
  return out
}

/** Whether Save may go: every blocking check passes (the GTF only warns). */
export const ready = (checks) => checks.every(c => c.ok || !c.blocking)

/** The request's params (plan 1.5): the generated files by name, the matrix and the GTF as given (none when blank), the study type. */
export function paramsFor({ matrix, gtf = null }) {
  const out = { [SHEET.param]: { file: SHEET.file }, [CONTRACT.contrasts.param]: { file: CONTRACT.contrasts.file }, [CONTRACT.matrix.param]: matrix }
  if ((gtf ?? '').trim()) out[CONTRACT.gtf.param] = gtf.trim()
  return { ...out, ...CONTRACT.params }
}

/** `params` with each `{ file }` as `cas://<cid>/<name>` from `files` (`[{ name, address }]`), or null for a file not made yet. */
export function resolveParams(params, files) {
  const at = new Map(files.map(f => [f.name, f.address]))
  return Object.fromEntries(Object.entries(params).map(([k, v]) => [k, v && typeof v === 'object'
    ? (at.has(v.file) ? `cas://${at.get(v.file)}/${v.file}` : null) : v]))
}

/** A word the shell reads as itself: as is when it is plain, else single-quoted. */
const shellWord = (v) => (/^[A-Za-z0-9_./:@%+=,-]+$/.test(v) ? v : `'${v.replace(/'/g, "'\\''")}'`)
const FILE_PARAMS = [SHEET.param, CONTRACT.contrasts.param, CONTRACT.matrix.param, CONTRACT.gtf.param]
const PLACEHOLDER = { [SHEET.param]: `<${SHEET.file}>`, [CONTRACT.contrasts.param]: `<${CONTRACT.contrasts.file}>`, [CONTRACT.matrix.param]: '<matrix>' }

/**
 * `nextflow run nf-core/differentialabundance` at `-r 2.0.0` with `-profile docker`, the
 * file params aligned in their order (a missing one as a placeholder, the GTF
 * only when given), the other params by name, and `--outdir results`
 * (nf-core pipelines require it). `params` are strings, as resolveParams or
 * the server's answer gives them.
 */
export function commandText(params) {
  const width = Math.max(...FILE_PARAMS.map(p => p.length)) + 3
  const lines = [`nextflow run ${TEMPLATE}`, `-r ${CONTRACT.run.revision}`, `-profile ${CONTRACT.run.profile}`]
  for (const p of FILE_PARAMS) {
    const v = params[p]
    if (v === undefined || (v === null && !PLACEHOLDER[p])) continue
    lines.push(`${`--${p}`.padEnd(width)}${v === null ? PLACEHOLDER[p] : shellWord(String(v))}`)
  }
  for (const [k, v] of Object.entries(params)) {
    if (FILE_PARAMS.includes(k) || v === null || v === undefined || k === 'outdir') continue
    lines.push(`--${k} ${shellWord(String(v))}`)
  }
  lines.push(`--outdir ${shellWord(String(params.outdir ?? CONTRACT.run.outdir))}`)
  return lines.map((l, i) => (i ? `  ${l}` : l)).join(' \\\n')
}

/**
 * The route of a hand-off to this template: `from` the run, `where` the
 * Samples tab's filter text, `project` the active project, `samples` the
 * meta.ids to tick (each percent-encoded, so a comma stays inside its id),
 * `outputs` the per-sample outputs the Selection takes. Empty parts are left out.
 */
export function handoffHref({ from, where = null, project = null, samples = [], outputs = [] }) {
  const q = [
    ['from', from], ['where', where], ['project', project],
    ['samples', samples?.length ? samples.map(encodeURIComponent).join(',') : null],
    ['outputs', outputs?.length ? outputs.map(encodeURIComponent).join(',') : null],
  ].filter(([, v]) => v).map(([k, v]) => `${k}=${k === 'samples' || k === 'outputs' ? v : encodeURIComponent(v)}`)
  return `#/handoff/${encodeURIComponent(TEMPLATE)}?${q.join('&')}`
}
