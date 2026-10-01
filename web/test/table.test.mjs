// web/test/table.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { cellOf, fileChips, itemTable, withoutDuplicates } from '../src/table.js'

const e = (path, type, value) => ({ path, type, values: [value], types: [type] })
const files = (s) => ['.markdup.sorted.bam.bai', '.markdup.sorted.bam', '.markdup.sorted.metrics.txt',
  '.markdup.sorted.bam.stats', '.markdup.sorted.bam.flagstat', '.markdup.sorted.bam.idxstats'].map(x => s + x)
const item = (id, singleEnd, extra = []) => ({
  pairs: [e('has_genome_bam', 'bool', 'false'), e('has_transcriptome_bam', 'bool', 'false'), e('id', 'string', id), e('meta.id', 'string', id),
    e('single_end', 'bool', singleEnd), e('strandedness', 'string', 'reverse'), ...extra],
  files: files(id),
})
const MARKDUP = [
  item('RAP1_UNINDUCED_REP1', 'true'),
  item('WT_REP1', 'false', [e('inferred_strandedness', 'string', 'reverse')]),
  item('RAP1_IAA_30M_REP1', 'false'),
  item('WT_REP2', 'false'),
  item('RAP1_UNINDUCED_REP2', 'true'),
]

test('markdup: id once, constants on one line, varying keys as columns (strings first, then most distinct)', () => {
  const shape = itemTable(MARKDUP)
  assert.deepEqual(shape.duplicates, new Map([['meta.id', 'id']]))
  assert.deepEqual(shape.constant.map(c => c.path), ['has_genome_bam', 'has_transcriptome_bam', 'strandedness'],
    'two keys with equal values are not one key: only the same key at two paths is a duplicate')
  assert.deepEqual(shape.columns, ['id', 'inferred_strandedness', 'single_end'])
})

test('a key missing on some items is a column, not a constant', () => {
  const shape = itemTable([item('A', 'false'), item('B', 'false', [e('lane', 'int', '1')])])
  assert.ok(shape.columns.includes('lane'))
  assert.ok(!shape.constant.some(c => c.path === 'lane'))
})

test('one item (multiqc): every key is a column and nothing is "same for every item" (P10)', () => {
  const shape = itemTable([{ pairs: [e('id', 'string', 'multiqc_report'), e('meta.id', 'string', 'multiqc_report')], files: ['multiqc_report.html'] }])
  assert.deepEqual(shape.constant, [])
  assert.deepEqual(shape.columns, ['id'])
  assert.deepEqual(shape.duplicates, new Map([['meta.id', 'id']]))
})

test('previews still loading or failed are ignored; none loaded gives an empty shape', () => {
  assert.deepEqual(itemTable([undefined, { error: 'block_missing' }]), { columns: [], constant: [], duplicates: new Map() })
  assert.deepEqual(itemTable([undefined, MARKDUP[0], MARKDUP[1]]).columns, itemTable([MARKDUP[0], MARKDUP[1]]).columns)
})

test('at most maxColumns columns', () => {
  const wide = [0, 1].map(i => ({ pairs: 'abcdefgh'.split('').map(k => e(k, 'string', `${k}${i}`)), files: [] }))
  assert.equal(itemTable(wide, { maxColumns: 6 }).columns.length, 6)
})

test('a list value can be a column, and the same key at two paths equal only on some items is not a duplicate', () => {
  const list = (path, values) => ({ path, type: 'string', values, types: values.map(() => 'string') })
  const shape = itemTable([{ pairs: [list('tags', ['x', 'y']), e('x.k', 'string', '1'), e('y.k', 'string', '1')], files: [] },
    { pairs: [list('tags', ['z']), e('x.k', 'string', '2'), e('y.k', 'string', '3')], files: [] }])
  assert.ok(shape.columns.includes('tags'))
  assert.equal(shape.duplicates.size, 0)
})

test('cellOf and withoutDuplicates', () => {
  assert.equal(cellOf(MARKDUP[0], 'id').values[0], 'RAP1_UNINDUCED_REP1')
  assert.equal(cellOf(MARKDUP[0], 'nope'), null)
  assert.equal(cellOf(undefined, 'id'), null)
  const { duplicates } = itemTable(MARKDUP)
  assert.ok(!withoutDuplicates(MARKDUP[0].pairs, duplicates).some(p => p.path === 'meta.id'))
})

test('file chips drop the prefix an item\'s files share, up to its last dot (P9)', () => {
  assert.deepEqual(fileChips(files('WT_REP1')), { prefix: 'WT_REP1.markdup.sorted',
    chips: ['.bam.bai', '.bam', '.metrics.txt', '.bam.stats', '.bam.flagstat', '.bam.idxstats'] })
  assert.deepEqual(fileChips(['a.bam', 'a.bam.bai']), { prefix: 'a', chips: ['.bam', '.bam.bai'] })
  assert.deepEqual(fileChips(['multiqc_report.html']), { prefix: '', chips: ['multiqc_report.html'] })
  assert.deepEqual(fileChips(['x.tsv', 'y.rds']), { prefix: '', chips: ['x.tsv', 'y.rds'] })
  assert.deepEqual(fileChips([]), { prefix: '', chips: [] })
  assert.deepEqual(fileChips(undefined), { prefix: '', chips: [] })
})
