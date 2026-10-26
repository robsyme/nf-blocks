import { test } from 'node:test'
import assert from 'node:assert/strict'
import { fieldKey, matchPromoted, pipelineConfig, promotedFor, shortLabel, splitKey } from '../src/promoted.js'

test('an unknown pipeline has no promoted fields and joins on meta.id, then id', () => {
  assert.deepEqual(promotedFor('main.nf'), [])
  assert.deepEqual(pipelineConfig('main.nf'), { joinKey: ['meta.id', 'id'], fields: [], outputs: {}, handoff: [] })
  assert.deepEqual(pipelineConfig(undefined).joinKey, ['meta.id', 'id'])
})

test('rnaseq promotes reads (preprocessed), % mapped (aligned), layout and strand, in that order', () => {
  const { fields, joinKey, handoff, outputs } = pipelineConfig('nf-core/rnaseq')
  assert.deepEqual(fields.map(fieldKey), ['preprocessed:num_trimmed_reads', 'aligned:percent_mapped', 'meta.single_end', 'meta.strandedness'])
  assert.deepEqual(fields.map(f => f.label), ['Reads (trimmed)', '% mapped', 'Layout', 'Strand'])
  assert.deepEqual(joinKey, ['meta.id', 'id'])
  assert.deepEqual(handoff, ['quant_salmon', 'quant_merged'])
  assert.deepEqual(outputs.quant_merged, { label: 'Merged count matrices', merged: { matrix: 'salmon.merged.gene_counts.tsv' } })
  assert.deepEqual(outputs.quant_salmon, { label: 'Salmon quant dir', has: 'quant' })
})

test('promotedFor with an output keeps the unqualified fields and that output\'s own', () => {
  assert.deepEqual(promotedFor('nf-core/rnaseq', { output: 'bam_qc' }).map(fieldKey),
    ['meta.single_end', 'meta.strandedness'])
  assert.deepEqual(promotedFor('nf-core/rnaseq', { output: 'preprocessed' }).map(fieldKey),
    ['preprocessed:num_trimmed_reads', 'meta.single_end', 'meta.strandedness'])
  assert.equal(promotedFor('nf-core/rnaseq').length, 4, 'with no output, every field')
})

test('rnaseq fields match a path exactly, else by the shortest path ending in .path, in field order', () => {
  const got = matchPromoted(promotedFor('nf-core/rnaseq'), ['id', 'meta.single_end', 'num_trimmed_reads', 'dupradar.meta.strandedness',
    'meta.strandedness', 'qc.meta.single_end', 'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.deepEqual(got.map(g => g.path), ['num_trimmed_reads', 'meta.single_end', 'meta.strandedness'],
    'Strand is meta.strandedness; the inferred strandedness is an ordinary field')
  assert.equal(got[1].field.labels.true, 'single')
  assert.deepEqual(matchPromoted([{ path: 'percent_mapped', label: '% mapped' }], ['x.percent_mapped', 'a.b.percent_mapped']).map(g => g.path),
    ['x.percent_mapped'])
})

test('splitKey qualifies a key only when the text before the first colon names an output', () => {
  const outputs = new Set(['preprocessed', 'aligned'])
  assert.deepEqual(splitKey('preprocessed:num_trimmed_reads', outputs), { output: 'preprocessed', path: 'num_trimmed_reads' })
  assert.deepEqual(splitKey('meta.single_end', outputs), { output: null, path: 'meta.single_end' })
  assert.deepEqual(splitKey('other:x', outputs), { output: null, path: 'other:x' })
  assert.deepEqual(splitKey('aligned:a:b', outputs), { output: 'aligned', path: 'a:b' })
})

test('shortLabel is the last dotted segment', () => {
  assert.equal(shortLabel('meta.salmon_strand_analysis.inferred_strandedness'), 'inferred_strandedness')
  assert.equal(shortLabel('id'), 'id')
})
