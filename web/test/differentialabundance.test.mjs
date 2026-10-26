// web/src/templates/differentialabundance.js (P4 Task 5, mockup 14, plan 1.5
// and section 3): the template contract, the samplesheet and contrasts bytes,
// their raw cids, every check passing and failing, condition validation, the
// run command and the route a hand-off link opens.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseHandoffQuery } from '../src/listquery.js'
import {
  CONTRACT, TEMPLATE, checksFor, commandText, conditionOk, contrastsText, filesFor, handoffHref, levelsOf, paramsFor,
  pickContrast, rawCidOf, ready, resolveParams, sampleIdOk, samplesheetText,
} from '../src/templates/differentialabundance.js'

// Independently computed (Python hashlib + base32): CIDv1, raw codec, sha2-256.
const HELLO = 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
const SHEET = 'sample,condition\nRAP1_UNINDUCED_REP2,uninduced\nWT_REP1,wild_type\n'
const SHEET_CID = 'bafkreihfjanx2wpx2qvm3lvkp6dh35mgm527r4idpm4bohejgoreujzwxy'
const CONTRASTS = 'id,variable,reference,target\nuninduced_vs_wild_type,condition,wild_type,uninduced\n'
const CONTRASTS_CID = 'bafkreigwcz4sqgv2imxbwdikshdsjpgtxwtgus36uuvbgciabpthltjlmy'

const TWO = [{ sample: 'RAP1_UNINDUCED_REP2', condition: 'uninduced' }, { sample: 'WT_REP1', condition: 'wild_type' }]
const FOUR = [
  { sample: 'RAP1_UNINDUCED_REP1', condition: 'uninduced' }, { sample: 'RAP1_UNINDUCED_REP2', condition: 'uninduced' },
  { sample: 'WT_REP1', condition: 'wild_type' }, { sample: 'WT_REP2', condition: 'wild_type' },
]
const PAIR = { target: 'uninduced', reference: 'wild_type' }
const MATRIX_ALL = { state: 'ready', columns: ['RAP1_IAA_30M_REP1', 'RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2'] }
const GTF_NONE = { value: null, store: 'roblab' }
const byKey = (list) => Object.fromEntries(list.map(c => [c.key, c]))

test('the contract: samplesheet columns, the matrix source, the extra files and the rules', () => {
  assert.equal(TEMPLATE, 'nf-core/differentialabundance')
  assert.equal(CONTRACT.template, TEMPLATE)
  assert.equal(CONTRACT.from, 'nf-core/rnaseq')
  assert.deepEqual(CONTRACT.samplesheet, { file: 'samplesheet.csv', param: 'input', id: 'sample', grouping: 'condition' })
  assert.deepEqual(CONTRACT.matrix, { output: 'quant_merged', file: 'salmon.merged.gene_counts.tsv', param: 'matrix' })
  assert.deepEqual(CONTRACT.contrasts, { file: 'contrasts.csv', param: 'contrasts', columns: ['id', 'variable', 'reference', 'target'] })
  assert.deepEqual(CONTRACT.gtf, { param: 'gtf', role: 'param:gtf', required: false })
  assert.deepEqual(CONTRACT.outputs, ['quant_salmon'])
  assert.equal(CONTRACT.minReplicates, 2)
  assert.deepEqual(CONTRACT.rules.map(r => r.key), ['values', 'contrast', 'replicates', 'matrix', 'gtf'])
  assert.deepEqual(CONTRACT.rules.filter(r => r.blocking).map(r => r.key), ['values', 'contrast', 'replicates', 'matrix'])
})

test('samplesheet.csv: a header row, one row per included sample, LF endings and a trailing newline', () => {
  assert.equal(samplesheetText(TWO), SHEET)
  assert.equal(samplesheetText([]), 'sample,condition\n')
  // A field with a comma or a quote is quoted, its quotes doubled (RFC 4180).
  assert.equal(samplesheetText([{ sample: 'a,b', condition: 'x"y' }]), 'sample,condition\n"a,b","x""y"\n')
})

test('contrasts.csv: id,variable,reference,target and one row target_vs_reference', () => {
  assert.equal(contrastsText(PAIR), CONTRASTS)
  assert.equal(contrastsText({ target: 'b', reference: 'b' }), null)
  assert.equal(contrastsText({ target: '', reference: 'b' }), null)
})

test('raw cids of the generated files: CIDv1, raw, sha2-256, base32', () => {
  assert.equal(rawCidOf('hello\n'), HELLO)
  assert.equal(rawCidOf(SHEET), SHEET_CID)
  assert.equal(rawCidOf(CONTRASTS), CONTRASTS_CID)
  const files = filesFor(TWO, PAIR)
  assert.deepEqual(files, [
    { name: 'samplesheet.csv', text: SHEET, address: SHEET_CID },
    { name: 'contrasts.csv', text: CONTRASTS, address: CONTRASTS_CID },
  ])
  // No contrast yet: the samplesheet alone.
  assert.deepEqual(filesFor(TWO, { target: 'uninduced', reference: 'uninduced' }).map(f => f.name), ['samplesheet.csv'])
})

test('sample ids and conditions: no whitespace in a sample; conditions of letters, digits, _ . and -', () => {
  assert.equal(sampleIdOk('WT_REP1'), true)
  assert.equal(sampleIdOk('WT REP1'), false)
  assert.equal(sampleIdOk(''), false)
  for (const ok of ['wild_type', 'auxin-30m', 'v1.2', 'A9']) assert.equal(conditionOk(ok), true, ok)
  for (const bad of ['', 'wild type', 'a/b', 'é', 'x,y']) assert.equal(conditionOk(bad), false, bad)
})

test('levels in the order the samples give them, and the contrast pick keeps what is still a level', () => {
  assert.deepEqual(levelsOf([{ sample: 'a', condition: 'auxin_30m' }, ...FOUR, { sample: 'z', condition: '' }]), ['auxin_30m', 'uninduced', 'wild_type'])
  // Mockup: the 50,000-read filter leaves uninduced then wild_type; the target is the first level, the reference the next.
  assert.deepEqual(pickContrast(['uninduced', 'wild_type'], {}), PAIR)
  // Include all 5: both picks are still levels, so they stay.
  assert.deepEqual(pickContrast(['auxin_30m', 'uninduced', 'wild_type'], PAIR), PAIR)
  // A pick that is no longer a level is replaced; the reference avoids the target when it can.
  assert.deepEqual(pickContrast(['auxin_30m', 'wild_type'], PAIR), { target: 'auxin_30m', reference: 'wild_type' })
  assert.deepEqual(pickContrast(['wild_type'], PAIR), { target: 'wild_type', reference: 'wild_type' })
  assert.deepEqual(pickContrast([], PAIR), { target: '', reference: '' })
  // With no picks, levels with two or more samples come first: include all 5 gives uninduced against wild_type.
  const count = (l) => ({ auxin_30m: 1, uninduced: 2, wild_type: 2 })[l] ?? 0
  assert.deepEqual(pickContrast(['auxin_30m', 'uninduced', 'wild_type'], {}, count), PAIR)
  assert.deepEqual(pickContrast(['auxin_30m', 'uninduced', 'wild_type'], { target: 'auxin_30m' }, count), { target: 'auxin_30m', reference: 'uninduced' })
})

test('checks pass: two conditions, two replicates each, every sample a matrix column, a GTF from the store', () => {
  const list = checksFor({ rows: FOUR, contrast: PAIR, matrix: MATRIX_ALL, gtf: { value: 'cas://bafkreigtf/genes.gtf', label: 'genes.gtf from the run\'s inputs' } })
  assert.deepEqual(list.map(c => [c.key, c.tone]), [['contrast', 'ok'], ['replicates', 'ok'], ['matrix', 'ok'], ['gtf', 'ok']])
  const c = byKey(list)
  assert.equal(c.contrast.text, 'Contrast uninduced vs wild_type')
  assert.equal(c.replicates.text, 'Replicates: uninduced 2, wild_type 2')
  assert.equal(c.matrix.text, 'All 4 samples are columns in the matrix')
  assert.equal(c.gtf.text, "GTF: genes.gtf from the run's inputs")
  assert.equal(ready(list), true)
})

test('the contrast check fails on one condition picked twice, or none', () => {
  const same = byKey(checksFor({ rows: FOUR, contrast: { target: 'wild_type', reference: 'wild_type' }, matrix: MATRIX_ALL, gtf: GTF_NONE }))
  assert.equal(same.contrast.ok, false)
  assert.equal(same.contrast.text, 'Pick two different conditions')
  const none = checksFor({ rows: [], contrast: { target: '', reference: '' }, matrix: MATRIX_ALL, gtf: GTF_NONE })
  assert.equal(byKey(none).contrast.ok, false)
  assert.equal(byKey(none).replicates.text, 'Each side needs at least 2 replicates: — has 0, — has 0.')
  assert.equal(ready(none), false)
})

test('the replicate check fails with the mockup message; the filter sentence only when a filter is in force', () => {
  const plain = byKey(checksFor({ rows: TWO, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE }))
  assert.equal(plain.replicates.ok, false)
  assert.equal(plain.replicates.tone, 'warn')
  assert.equal(plain.replicates.text, 'Each side needs at least 2 replicates: uninduced has 1, wild_type has 1.')
  const filtered = checksFor({ rows: TWO, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE, filter: { text: 'reads (trimmed) ≥ 50,000', only: true } })
  assert.equal(byKey(filtered).replicates.text,
    'Each side needs at least 2 replicates: uninduced has 1, wild_type has 1. The reads (trimmed) ≥ 50,000 filter leaves one sample per condition.')
  assert.equal(ready(filtered), false)
  // Ticks changed by hand since the filter: the filter is no longer why.
  const ticked = checksFor({ rows: TWO, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE, filter: { text: 'reads (trimmed) ≥ 50,000', only: false } })
  assert.equal(byKey(ticked).replicates.text, 'Each side needs at least 2 replicates: uninduced has 1, wild_type has 1.')
  // Uneven sides say each count.
  const three = [...TWO, { sample: 'RAP1_UNINDUCED_REP1', condition: 'uninduced' }]
  assert.equal(byKey(checksFor({ rows: three, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE, filter: { text: 'f', only: true } })).replicates.text,
    'Each side needs at least 2 replicates: uninduced has 2, wild_type has 1. The f filter leaves 2 uninduced and 1 wild_type.')
})

test('the matrix check: every included sample a column; a missing one, a matrix being read, none, or unreadable', () => {
  const short = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'ready', columns: ['WT_REP1', 'WT_REP2', 'RAP1_UNINDUCED_REP1'] }, gtf: GTF_NONE }))
  assert.equal(short.matrix.ok, false)
  assert.equal(short.matrix.text, 'RAP1_UNINDUCED_REP2 is not a column in the matrix, and differentialabundance stops on a samplesheet sample the matrix lacks.')
  const two = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'ready', columns: ['WT_REP1', 'WT_REP2'] }, gtf: GTF_NONE }))
  assert.match(two.matrix.text, /^RAP1_UNINDUCED_REP1, RAP1_UNINDUCED_REP2 are not columns in the matrix/)
  const reading = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'reading' }, gtf: GTF_NONE }))
  assert.equal(reading.matrix.ok, false)
  assert.equal(reading.matrix.text, "Reading the matrix's header…")
  const none = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'missing' }, gtf: GTF_NONE }))
  assert.equal(none.matrix.text, 'This run has no salmon.merged.gene_counts.tsv in quant_merged.')
  const failed = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'failed', message: 'HTTP 500' }, gtf: GTF_NONE }))
  assert.equal(failed.matrix.text, "Could not read the matrix's header: HTTP 500")
  assert.equal(ready(checksFor({ rows: FOUR, contrast: PAIR, matrix: { state: 'reading' }, gtf: GTF_NONE })), false)
})

test('the GTF check is a warning, never a blocker, and says what leaving it out changes', () => {
  const list = checksFor({ rows: FOUR, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE })
  const gtf = byKey(list).gtf
  assert.equal(gtf.ok, false)
  assert.equal(gtf.tone, 'warn')
  assert.equal(gtf.blocking, false)
  assert.equal(gtf.text, 'GTF not in roblab: rnaseq read it by path. Without one, differentialabundance uses the matrix as its feature table, '
    + 'and the annotated results may carry counts for samples left out. Give a path or a cas:// address.')
  assert.equal(ready(list), true)
  const typed = byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: MATRIX_ALL, gtf: { value: '/refs/genes.gtf', store: 'roblab' } })).gtf
  assert.deepEqual([typed.ok, typed.text], [true, 'GTF: /refs/genes.gtf'])
})

test('the values check shows only when a sample or a condition would break the samplesheet, and blocks', () => {
  assert.equal(byKey(checksFor({ rows: FOUR, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE })).values, undefined)
  const rows = [{ sample: 'WT REP1', condition: 'wild_type' }, { sample: 'WT_REP2', condition: 'wild type' }, { sample: 'KO_1', condition: '' }, { sample: 'KO_2', condition: '' }]
  const list = checksFor({ rows, contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE })
  assert.equal(list[0].key, 'values')
  assert.equal(list[0].ok, false)
  assert.equal(list[0].blocking, true)
  assert.equal(list[0].text, 'Sample IDs cannot hold spaces: "WT REP1". Conditions may use only letters, digits, _ . and -: "wild type" (WT_REP2). '
    + 'KO_1 and KO_2 have no condition.')
  assert.equal(ready(list), false)
  assert.equal(checksFor({ rows: [rows[2]], contrast: PAIR, matrix: MATRIX_ALL, gtf: GTF_NONE })[0].text, 'KO_1 has no condition.')
})

test('params: the files by name, the matrix and GTF as given, the study type; resolved to cas:// with the files\' cids', () => {
  const matrix = 'cas://bafkreimatrix/salmon.merged.gene_counts.tsv'
  assert.deepEqual(paramsFor({ matrix, gtf: null }),
    { input: { file: 'samplesheet.csv' }, contrasts: { file: 'contrasts.csv' }, matrix, study_type: 'rnaseq' })
  assert.deepEqual(paramsFor({ matrix, gtf: '/refs/genes.gtf' }).gtf, '/refs/genes.gtf')
  assert.equal(paramsFor({ matrix, gtf: '  ' }).gtf, undefined)
  const files = filesFor(TWO, PAIR)
  assert.deepEqual(resolveParams(paramsFor({ matrix, gtf: null }), files), {
    input: `cas://${SHEET_CID}/samplesheet.csv`, contrasts: `cas://${CONTRASTS_CID}/contrasts.csv`, matrix, study_type: 'rnaseq' })
  // A file not generated yet has no address to name.
  assert.equal(resolveParams(paramsFor({ matrix, gtf: null }), files.slice(0, 1)).contrasts, null)
})

test('the run command: the 2.0.0 tag, -profile docker, the file params aligned, the GTF only when given, --outdir', () => {
  const params = { input: 'cas://bafkreiss/samplesheet.csv', contrasts: 'cas://bafkreict/contrasts.csv',
    matrix: 'cas://bafkreimx/salmon.merged.gene_counts.tsv', study_type: 'rnaseq' }
  assert.equal(commandText(params), [
    'nextflow run nf-core/differentialabundance \\',
    '  -r 2.0.0 \\',
    '  -profile docker \\',
    '  --input     cas://bafkreiss/samplesheet.csv \\',
    '  --contrasts cas://bafkreict/contrasts.csv \\',
    '  --matrix    cas://bafkreimx/salmon.merged.gene_counts.tsv \\',
    '  --study_type rnaseq \\',
    '  --outdir results',
  ].join('\n'))
  const withGtf = commandText({ ...params, gtf: '/refs/my genes.gtf' })
  assert.match(withGtf, /\n {2}--gtf {7}'\/refs\/my genes\.gtf' \\\n {2}--study_type rnaseq/)
  // A file param with no address yet reads as a placeholder.
  assert.match(commandText({ ...params, contrasts: null }), /--contrasts <contrasts\.csv> \\/)
  // The defaults were checked against the 2.0.0 tag, so the command runs that tag, never the default branch.
  assert.equal(CONTRACT.run.revision, '2.0.0')
})

test('the hand-off link carries from, where, project, samples and outputs, and parses back', () => {
  const where = JSON.stringify([['preprocessed:num_trimmed_reads', 'int', '50000', '>=']])
  const href = handoffHref({ from: 'bafyrun', where, project: 'bafyproj', samples: ['WT_1', 'KO,1'], outputs: ['quant_salmon', 'quant_merged'] })
  assert.match(href, /^#\/handoff\/nf-core%2Fdifferentialabundance\?/)
  const query = href.slice(href.indexOf('?') + 1)
  assert.deepEqual(parseHandoffQuery(query), { from: 'bafyrun', where, project: 'bafyproj', samples: ['WT_1', 'KO,1'], outputs: ['quant_salmon', 'quant_merged'] })
  assert.equal(handoffHref({ from: 'bafyrun' }), '#/handoff/nf-core%2Fdifferentialabundance?from=bafyrun')
})
