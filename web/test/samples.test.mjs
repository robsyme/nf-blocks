// web/test/samples.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { attrRows, metadataView } from '../src/metadata.js'
import { pairsOf } from '../src/pairs.js'
import { Float } from '../src/typed.js'
import { pipelineConfig } from '../src/promoted.js'
import { buildSamples, handoffSet, parseSamplesWhere, sampleKeyOf, sampleMatches, samplesWhereText, valueOf } from '../src/samples.js'

const RNASEQ = pipelineConfig('nf-core/rnaseq')

// A roblab-shaped run (distraught_murdock): five samples, per-sample outputs keyed
// by meta.id (and id), lint_raw keyed by id only, and three run-level outputs.
const READS = { RAP1_IAA_30M_REP1: 49791, RAP1_UNINDUCED_REP2: 99566, RAP1_UNINDUCED_REP1: 49969, WT_REP2: 49760, WT_REP1: 99971 }
const SAMPLES = Object.keys(READS)
const MAPPED = { RAP1_IAA_30M_REP1: 89, RAP1_UNINDUCED_REP2: 91.5, RAP1_UNINDUCED_REP1: 93.2, WT_REP1: 95.1 }
const SINGLE = (id) => id.startsWith('RAP1_IAA')

const meta = (id, extra = {}) => ({ id, single_end: SINGLE(id), strandedness: 'auto', ...extra })
const entry = (output, i, value) => {
  const view = metadataView(value)
  return { item: `bafy-${output}-${i}`, collection: `bafy-${output}`, pairs: pairsOf(view), rows: attrRows(view), slots: [] }
}
function roblab() {
  const outputs = {
    preprocessed: SAMPLES.map(id => ({ id, meta: meta(id, { salmon_strand_analysis: { inferred_strandedness: id.startsWith('WT') ? 'reverse' : 'unstranded' } }),
      num_trimmed_reads: new Float(READS[id]) })),
    aligned: Object.keys(MAPPED).map(id => ({ id, meta: meta(id), percent_mapped: new Float(MAPPED[id]) })),
    quant_salmon: SAMPLES.map(id => ({ id, meta: meta(id), lib_type: 'A' })),
    bam_qc: SAMPLES.map(id => ({ id, meta: meta(id), dupradar: { meta: meta(id) }, qualimap: 'ok' })),
    lint_raw: SAMPLES.map(id => ({ id, lint: 'pass' })),
    quant_merged: [{ id: 'all_samples', meta: { id: 'all_samples' } }],
    multiqc: [{ id: 'multiqc_report' }],
    pipeline_info: [{ versions: 'v1' }],
  }
  return new Map(Object.entries(outputs).map(([o, values]) => [o, values.map((v, i) => entry(o, i, v))]))
}

const kept = (table, where) => table.samples.filter(s => sampleMatches(table, s, where))

test('sampleKeyOf: the first join path present as one string value', () => {
  const pairs = pairsOf({ id: 'A', meta: { id: 'B' } })
  assert.deepEqual(sampleKeyOf(pairs, ['meta.id', 'id']), { path: 'meta.id', value: 'B' })
  assert.deepEqual(sampleKeyOf(pairsOf({ id: 'A' }), ['meta.id', 'id']), { path: 'id', value: 'A' })
  assert.deepEqual(sampleKeyOf(pairsOf({ meta: { id: ['x', 'y'] }, id: 'A' }), ['meta.id', 'id']), { path: 'id', value: 'A' }, 'a list is no key')
  assert.equal(sampleKeyOf(pairsOf({ id: 3 }), ['meta.id', 'id']), null, 'a number is no key')
  assert.equal(sampleKeyOf(pairsOf({ versions: 'v1' }), ['meta.id', 'id']), null)
})

test('buildSamples: samples come from outputs of two or more items; one-item or keyless outputs are run-level (Review Focus 1)', () => {
  const table = buildSamples(roblab(), RNASEQ)
  assert.deepEqual(table.samples, [...SAMPLES].sort())
  assert.equal(table.joinPath, 'meta.id')
  assert.deepEqual(table.perSample, ['aligned', 'bam_qc', 'lint_raw', 'preprocessed', 'quant_salmon'])
  assert.deepEqual(table.runLevel, ['multiqc', 'pipeline_info', 'quant_merged'])
  assert.deepEqual([...table.itemsOf.get('WT_REP1').keys()].sort(), ['aligned', 'bam_qc', 'lint_raw', 'preprocessed', 'quant_salmon'])
  assert.equal(table.itemsOf.get('WT_REP2').has('aligned'), false)
  assert.equal(table.itemsOf.get('WT_REP1').get('lint_raw')[0].item, 'bafy-lint_raw-4')
  assert.deepEqual(table.runLevelItems.get('quant_merged').map(e => e.item), ['bafy-quant_merged-0'])
  assert.equal(table.itemsOf.has('all_samples'), false)
})

test('buildSamples: a run with only one-item outputs has no samples', () => {
  const table = buildSamples(new Map([['a', [entry('a', 0, { id: 'S1' })]], ['b', [entry('b', 0, { versions: 1 })]]]), pipelineConfig('main.nf'))
  assert.deepEqual(table.samples, [])
  assert.deepEqual(table.perSample, [])
  assert.deepEqual(table.runLevel, ['a', 'b'])
  assert.equal(table.joinPath, null)
})

test('buildSamples: a key seen in only one output is no sample, so a contrasts output is run-level and adds no rows', () => {
  const outputs = roblab()
  outputs.set('deseq2_contrasts', ['treated_vs_control', 'ko_vs_wt', 'ko_vs_treated']
    .map((id, i) => entry('deseq2_contrasts', i, { meta: { id }, padj: new Float(0.05) })))
  const table = buildSamples(outputs, RNASEQ)
  assert.deepEqual(table.samples, [...SAMPLES].sort(), 'the five samples, and no contrast')
  assert.deepEqual(table.perSample, ['aligned', 'bam_qc', 'lint_raw', 'preprocessed', 'quant_salmon'])
  assert.deepEqual(table.runLevel, ['deseq2_contrasts', 'multiqc', 'pipeline_info', 'quant_merged'])
})

test('buildSamples: with one keyed output of several items, every key there is a sample', () => {
  const table = buildSamples(new Map([['a', ['S1', 'S2'].map((id, i) => entry('a', i, { id }))], ['b', [entry('b', 0, { versions: 1 })]]]),
    pipelineConfig('main.nf'))
  assert.deepEqual(table.samples, ['S1', 'S2'])
  assert.deepEqual(table.perSample, ['a'])
  assert.deepEqual(table.runLevel, ['b'])
})

test('buildSamples: one keyed output of several items beside a keyed one-item output keeps its samples', () => {
  const table = buildSamples(new Map([['a', ['S1', 'S2'].map((id, i) => entry('a', i, { id }))], ['b', [entry('b', 0, { id: 'S1' })]]]),
    pipelineConfig('main.nf'))
  assert.deepEqual(table.samples, ['S1', 'S2'], 'only a holds two or more items, so every key there is a sample')
  assert.deepEqual(table.perSample, ['a', 'b'])
  assert.deepEqual(table.runLevel, [])
})

test('buildSamples: fastqc of three samples beside a keyed multiqc report gives three samples, multiqc run-level', () => {
  const table = buildSamples(new Map([['fastqc', ['S1', 'S2', 'S3'].map((id, i) => entry('fastqc', i, { meta: { id } }))],
    ['multiqc', [entry('multiqc', 0, { id: 'multiqc_report' })]]]), RNASEQ)
  assert.deepEqual(table.samples, ['S1', 'S2', 'S3'])
  assert.deepEqual(table.perSample, ['fastqc'])
  assert.deepEqual(table.runLevel, ['multiqc'])
})

test('buildSamples: a sample whose later steps failed stays: reads S1-S3 beside bams S1-S2 gives three samples', () => {
  const table = buildSamples(new Map([['reads', ['S1', 'S2', 'S3'].map((id, i) => entry('reads', i, { meta: { id } }))],
    ['bams', ['S1', 'S2'].map((id, i) => entry('bams', i, { meta: { id } }))]]), RNASEQ)
  assert.deepEqual(table.samples, ['S1', 'S2', 'S3'])
  assert.deepEqual(table.perSample, ['bams', 'reads'])
  assert.deepEqual(table.runLevel, [])
  assert.deepEqual([...table.itemsOf.get('S3').keys()], ['reads'])
})

test('buildSamples: contrasts are listed as samples once a contrast key also keys another output, even a one-item one', () => {
  const contrasts = ['treated_vs_control', 'ko_vs_wt']
  const table = buildSamples(new Map([['deseq2', contrasts.map((id, i) => entry('deseq2', i, { meta: { id } }))],
    ['plots', contrasts.map((id, i) => entry('plots', i, { meta: { id } }))]]), RNASEQ)
  assert.deepEqual(table.samples, ['ko_vs_wt', 'treated_vs_control'])
  assert.deepEqual(table.perSample, ['deseq2', 'plots'])
  // One contrast key in a one-item output is enough: the contrasts output is per-sample, and all its keys with it.
  const report = buildSamples(new Map([...roblab(), ['deseq2', contrasts.map((id, i) => entry('deseq2', i, { meta: { id } }))],
    ['report', [entry('report', 0, { meta: { id: 'ko_vs_wt' } })]]]), RNASEQ)
  assert.deepEqual(report.samples, [...SAMPLES, ...contrasts].sort())
  assert.ok(report.perSample.includes('deseq2') && report.perSample.includes('report'))
})

test('buildSamples: an output of several items with one item that has no key is run-level', () => {
  const table = buildSamples(new Map([['reads', [entry('reads', 0, { meta: { id: 'S1' } }), entry('reads', 1, { meta: { id: 'S2' } }), entry('reads', 2, { lane: 1 })]],
    ['bams', ['S1', 'S2'].map((id, i) => entry('bams', i, { meta: { id } }))]]), RNASEQ)
  assert.deepEqual(table.samples, ['S1', 'S2'])
  assert.deepEqual(table.perSample, ['bams'])
  assert.deepEqual(table.runLevel, ['reads'])
})

test('a qualified range keeps the samples whose item in that output matches', () => {
  const table = buildSamples(roblab(), RNASEQ)
  assert.deepEqual(kept(table, [['preprocessed:num_trimmed_reads', 'int', '50000', '>=']]), ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  assert.deepEqual(kept(table, [['preprocessed:num_trimmed_reads', 'float', '99566.0']]), ['RAP1_UNINDUCED_REP2'])
})

test('a qualified filter on an output where the sample has no item: = and ranges fail it, != passes it (Review Focus 2)', () => {
  const table = buildSamples(roblab(), RNASEQ)
  assert.equal(sampleMatches(table, 'WT_REP2', [['aligned:percent_mapped', 'int', '90', '>=']]), false)
  assert.equal(sampleMatches(table, 'WT_REP2', [['aligned:percent_mapped', 'float', '89.0', '!=']]), true)
  assert.equal(sampleMatches(table, 'RAP1_IAA_30M_REP1', [['aligned:percent_mapped', 'float', '89.0', '!=']]), false)
  assert.equal(sampleMatches(table, 'WT_REP1', [['aligned:percent_mapped', 'int', '90', '>=']]), true)
  assert.equal(sampleMatches(table, 'WT_REP2', [['aligned:percent_mapped', 'float', '89.0']]), false)
})

test('an unqualified key matches over every item of the sample; conditions all hold', () => {
  const table = buildSamples(roblab(), RNASEQ)
  assert.deepEqual(kept(table, [['meta.id', 'string', 'WT_REP1']]), ['WT_REP1'])
  assert.deepEqual(kept(table, [['lint', 'string', 'pass']]), [...SAMPLES].sort(), 'lint_raw items count for an unqualified key')
  assert.deepEqual(kept(table, [['meta.single_end', 'bool', 'true'], ['preprocessed:num_trimmed_reads', 'int', '50000', '<']]), ['RAP1_IAA_30M_REP1'])
  assert.deepEqual(kept(table, []), [...SAMPLES].sort())
})

test('!= is the complement of = over the run\'s samples; a run-level output names no sample\'s items (ADR 0001)', () => {
  const table = buildSamples(roblab(), RNASEQ)
  const all = [...SAMPLES].sort()
  const complement = (where) => {
    const eq = kept(table, where.map(([k, t, v]) => [k, t, v]))
    assert.deepEqual(kept(table, where.map(([k, t, v]) => [k, t, v, '!='])), all.filter(s => !eq.includes(s)))
  }
  complement([['aligned:percent_mapped', 'float', '89.0']])
  complement([['meta.id', 'string', 'WT_REP1']])
  // WT_REP1 holds meta.id WT_REP1 on every item; an item that lacks the value does not let it pass !=.
  assert.ok(!kept(table, [['meta.id', 'string', 'WT_REP1', '!=']]).includes('WT_REP1'))
  // quant_merged is run-level: its item belongs to no sample.
  assert.deepEqual(kept(table, [['quant_merged:id', 'string', 'all_samples']]), [])
  assert.deepEqual(kept(table, [['quant_merged:id', 'int', '0', '>=']]), [])
  assert.deepEqual(kept(table, [['quant_merged:id', 'string', 'all_samples', '!=']]), all)
  assert.deepEqual(kept(table, [['id', 'string', 'all_samples']]), [], 'an unqualified key reads per-sample outputs only')
})

test('columns: the promoted keys first in config order, each with its source', () => {
  const table = buildSamples(roblab(), RNASEQ)
  const keys = table.columns.map(c => c.key)
  assert.deepEqual(keys.slice(0, 4), ['preprocessed:num_trimmed_reads', 'aligned:percent_mapped', 'meta.single_end', 'meta.strandedness'])
  const [reads, mapped, layout, strand] = table.columns
  assert.equal(reads.source, 'preprocessed')
  assert.equal(reads.output, 'preprocessed')
  assert.equal(reads.label, 'Reads (trimmed)')
  assert.equal(reads.numeric, true)
  assert.equal(mapped.source, 'aligned')
  assert.equal(layout.source, 'meta.single_end')
  assert.equal(layout.output, null)
  assert.equal(layout.field.labels.true, 'single')
  assert.equal(strand.numeric, false)
  assert.deepEqual([strand.label, strand.source, strand.output], ['Strand', 'meta.strandedness', null],
    'a promoted path the outputs share, with equal values, is one unqualified column')
  assert.ok(!keys.includes('meta.salmon_strand_analysis.inferred_strandedness'), 'one output\'s path waits in the picker')
  assert.ok(!keys.includes('id') && !keys.includes('meta.id'), 'the join key is never a column')
})

test('valueOf: a sample\'s values in a column, over its items there, each once', () => {
  const table = buildSamples(roblab(), RNASEQ)
  const col = (key) => table.columns.find(c => c.key === key)
  assert.deepEqual(valueOf(table, 'WT_REP1', col('preprocessed:num_trimmed_reads')), [{ type: 'float', value: '99971.0' }])
  assert.deepEqual(valueOf(table, 'WT_REP1', col('meta.single_end')), [{ type: 'bool', value: 'false' }])
  assert.deepEqual(valueOf(table, 'WT_REP2', col('aligned:percent_mapped')), [])
})

test('the picker groups the other paths by output, without the join key or itemTable duplicates', () => {
  const table = buildSamples(roblab(), RNASEQ)
  assert.deepEqual([...table.picker.keys()], ['bam_qc', 'lint_raw', 'preprocessed', 'quant_salmon'])
  assert.deepEqual(table.picker.get('preprocessed').map(c => c.key), ['preprocessed:meta.salmon_strand_analysis.inferred_strandedness'],
    'the inferred strandedness is an ordinary field')
  assert.deepEqual(table.picker.get('bam_qc').map(c => c.key), ['bam_qc:qualimap'], 'dupradar.meta.* repeats meta.*')
  assert.deepEqual(table.picker.get('lint_raw').map(c => [c.key, c.output, c.path, c.label, c.source, c.field]),
    [['lint_raw:lint', 'lint_raw', 'lint', 'lint', 'lint_raw', null]])
  assert.deepEqual(table.picker.get('quant_salmon').map(c => c.key), ['quant_salmon:lib_type'])
})

test('two outputs that disagree on a path for one sample split it into per-output columns', () => {
  const two = (o, a, b, lane) => [entry(o, 0, { meta: { id: 'S1', single_end: a }, lane }), entry(o, 1, { meta: { id: 'S2', single_end: b }, lane })]
  const byOutput = new Map([['outA', two('outA', true, false, 1)], ['outB', two('outB', false, false, 2)]])
  const promoted = buildSamples(byOutput, RNASEQ)
  const keys = promoted.columns.map(c => c.key)
  assert.deepEqual(keys, ['outA:meta.single_end', 'outB:meta.single_end', 'outA:lane', 'outB:lane'])
  assert.deepEqual(promoted.columns.slice(0, 2).map(c => [c.label, c.source, c.output, c.path]),
    [['Layout', 'outA', 'outA', 'meta.single_end'], ['Layout', 'outB', 'outB', 'meta.single_end']])
  assert.equal(promoted.columns[2].numeric, true)
  const plain = buildSamples(byOutput, pipelineConfig('main.nf'))
  assert.deepEqual(plain.columns.map(c => c.key), ['outA:lane', 'outB:lane', 'outA:meta.single_end', 'outB:meta.single_end'],
    'with no config, the split paths A-Z, each path\'s outputs together')
  assert.deepEqual(valueOf(plain, 'S1', plain.columns.find(c => c.key === 'outB:meta.single_end')), [{ type: 'bool', value: 'false' }])
})

test('parseSamplesWhere: a JSON list of [key, type, value] or [key, type, value, op]; else bad_route', () => {
  assert.throws(() => parseSamplesWhere('[["x","int","1","~"]]'), e => e.code === 'bad_route')
  assert.throws(() => parseSamplesWhere('not json'), e => e.code === 'bad_route')
  assert.throws(() => parseSamplesWhere('{"a":1}'), e => e.code === 'bad_route')
  assert.throws(() => parseSamplesWhere('[[1,"int","1"]]'), e => e.code === 'bad_route')
  assert.deepEqual(parseSamplesWhere(''), [])
  assert.deepEqual(parseSamplesWhere(null), [])
  const t = '[["preprocessed:num_trimmed_reads","int","50000",">="],["meta.single_end","bool","true"]]'
  assert.equal(samplesWhereText(parseSamplesWhere(t)), t)
})

test('handoffSet: the chosen samples\' items in each per-sample output, and every item of a run-level output', () => {
  const table = buildSamples(roblab(), RNASEQ)
  const { items } = handoffSet(table, ['WT_REP1', 'RAP1_UNINDUCED_REP2'], ['quant_salmon', 'quant_merged'])
  assert.equal(items.length, 3)
  assert.deepEqual(items, [
    { address: 'bafy-quant_salmon-4', via: ['bafy-quant_salmon'], output: 'quant_salmon', sample: 'WT_REP1' },
    { address: 'bafy-quant_salmon-1', via: ['bafy-quant_salmon'], output: 'quant_salmon', sample: 'RAP1_UNINDUCED_REP2' },
    { address: 'bafy-quant_merged-0', via: ['bafy-quant_merged'], output: 'quant_merged', sample: null },
  ])
  assert.equal(handoffSet(table, ['WT_REP2'], ['aligned']).items.length, 0, 'a sample with no item there adds nothing')
})
