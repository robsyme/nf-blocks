// web/test/table.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { cellOf, cellSlots, emptyColumns, fieldChoices, fieldPaths, fileColumns, itemTable, outputSuffixes, rankFields, typedCondition, withoutDuplicates } from '../src/table.js'
import { PredicateError } from '../src/metadata.js'

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

test('fieldPaths: every path on any loaded item, a repeated key once, sorted', () => {
  assert.deepEqual(fieldPaths(MARKDUP),
    ['has_genome_bam', 'has_transcriptome_bam', 'id', 'inferred_strandedness', 'single_end', 'strandedness'])
  const person = (first, continent) => ({ pairs: [e('person.firstName', 'string', first), e('person.location.continent', 'string', continent)], files: [] })
  assert.deepEqual(fieldPaths([person('Alice', 'Europe'), { pending: true }, person('Bob', 'Oceania')]),
    ['person.firstName', 'person.location.continent'])
  assert.deepEqual(fieldPaths([{ pairs: [], files: ['Asia.txt'] }]), [])
  assert.deepEqual(fieldPaths([]), [])
})

test('outputSuffixes: each distinct suffix over every loaded item, sorted, a compression suffix with the one before', () => {
  assert.deepEqual(outputSuffixes([{ files: ['Asia.txt'] }, { files: ['Europe.txt'] }]), ['.txt'])
  assert.deepEqual(outputSuffixes(MARKDUP), ['.bai', '.bam', '.flagstat', '.idxstats', '.stats', '.txt'])
  assert.deepEqual(outputSuffixes([{ files: ['A_R1.fastq.gz', 'A_R2.fastq.gz'] }, { files: ['B.vcf.bgz', 'README'] }]),
    ['.fastq.gz', '.vcf.bgz', 'README'])
  assert.deepEqual(outputSuffixes([{ files: ['.hidden', 'x.'] }]), ['.hidden', 'x.'])
  assert.deepEqual(outputSuffixes([{ pending: true }, {}]), [])
})

const slot = (column, path, address = 'bafkstored') => ({ column, path, name: null, size: null, address, dir: false, reason: address ? null : 'declined' })
const none = (column, path) => slot(column, path, null)

test('fileColumns: keys with a stored file in some loaded item, names sorted, then positions by number', () => {
  const a = { slots: [slot('samtools', 'samtools.stats'), slot('bam', 'bam'), none('meta', 'meta.original_genome_bam')] }
  const b = { slots: [slot('bai', 'bai'), slot('[10]', '[10]'), slot('[2]', '[2]'), none('cram', 'cram'), none('samtools', 'samtools.idxstats')] }
  assert.deepEqual(fileColumns([a, b, undefined, { error: 'block_missing' }]), ['bai', 'bam', 'samtools', '[2]', '[10]'])
  assert.deepEqual(fileColumns([]), [])
})

test('emptyColumns: keys whose slots are address-less in every loaded item, sorted the same way', () => {
  const a = { slots: [slot('bam', 'bam'), none('meta', 'meta.original_genome_bam'), none('cram', 'cram'), none('[3]', '[3]')] }
  const b = { slots: [none('bam', 'bam'), none('json_info', 'json_info')] }
  assert.deepEqual(emptyColumns([a, b, undefined]), ['cram', 'json_info', 'meta', '[3]'])
  assert.deepEqual(emptyColumns([]), [])
})

test('cellSlots: stored slots first, at most five, how many more, and the address-less ones apart', () => {
  const rseqc = [none('rseqc', 'rseqc.tin'), none('rseqc', 'rseqc.meta.original_genome_bam'), slot('rseqc', 'rseqc.bamstat'),
    none('rseqc', 'rseqc.innerdistance.pdf'), ...[...Array(6)].map((_, i) => slot('rseqc', `rseqc.junction[${i}]`))]
  const p = { slots: [...rseqc, slot('bam', 'bam')] }
  const { shown, more, empty } = cellSlots(p, 'rseqc')
  assert.deepEqual(shown.map(s => s.path), ['rseqc.bamstat', 'rseqc.junction[0]', 'rseqc.junction[1]', 'rseqc.junction[2]', 'rseqc.junction[3]'])
  assert.equal(more, 2)
  assert.deepEqual(empty.map(s => s.path), ['rseqc.tin', 'rseqc.meta.original_genome_bam', 'rseqc.innerdistance.pdf'])
  assert.deepEqual(cellSlots(p, 'bam'), { shown: [p.slots.at(-1)], more: 0, empty: [] })
  assert.deepEqual(cellSlots(p, 'bai'), { shown: [], more: 0, empty: [] })
})

test('fieldChoices: each metadata path over the loaded items, a repeated key once, values most common first with item counts', () => {
  const p = (...pairs) => ({ pairs })
  const loaded = [
    p(e('id', 'string', 'A'), e('meta.id', 'string', 'A'), e('single_end', 'bool', 'false')),
    p(e('id', 'string', 'B'), e('meta.id', 'string', 'B'), e('single_end', 'bool', 'false')),
    p(e('id', 'string', 'C'), e('meta.id', 'string', 'C'), e('single_end', 'bool', 'true'), { path: 'tags', type: 'string', values: ['x', 'x'], types: ['string', 'string'] }),
    undefined, { error: 'block_missing' },
  ]
  assert.deepEqual(fieldChoices(loaded), [
    { path: 'id', type: 'string', values: [{ type: 'string', value: 'A', count: 1 }, { type: 'string', value: 'B', count: 1 }, { type: 'string', value: 'C', count: 1 }] },
    { path: 'single_end', type: 'bool', values: [{ type: 'bool', value: 'false', count: 2 }, { type: 'bool', value: 'true', count: 1 }] },
    { path: 'tags', type: 'string', values: [{ type: 'string', value: 'x', count: 1 }] },
  ])
  assert.deepEqual(fieldChoices([]), [])
})

test('fieldChoices: shallower paths first, then by text, so nested copies of meta sink to the end', () => {
  const loaded = [{ pairs: [e('dupradar.meta.salmon.inferred', 'string', 'forward'), e('meta.single_end', 'bool', 'true'), e('id', 'string', 'A'), e('meta.salmon.inferred', 'string', 'reverse')] }]
  assert.deepEqual(fieldChoices(loaded).map(f => f.path), ['id', 'meta.single_end', 'meta.salmon.inferred', 'dupradar.meta.salmon.inferred'])
})

test('typedCondition: typed text takes the field\'s type, normalised as the index types it; a mixed field is a string', () => {
  assert.deepEqual(typedCondition({ path: 'lane', type: 'int' }, '007'), ['lane', 'int', '7'])
  assert.deepEqual(typedCondition({ path: 'id', type: 'string' }, 'WT_REP1'), ['id', 'string', 'WT_REP1'])
  assert.deepEqual(typedCondition({ path: 'x', type: 'mixed' }, '2'), ['x', 'string', '2'])
  assert.throws(() => typedCondition({ path: 'lane', type: 'int' }, 'two'), PredicateError)
})

test('typedCondition with an operator: a numeric field compares, an int bound stays int, a decimal bound is float, text is refused', () => {
  assert.deepEqual(typedCondition({ path: 'lane', type: 'int' }, '2', '>='), ['lane', 'int', '2', '>='])
  assert.deepEqual(typedCondition({ path: 'lane', type: 'int' }, '2.5', '<'), ['lane', 'float', '2.5', '<'])
  assert.deepEqual(typedCondition({ path: 'depth', type: 'float' }, '30', '>'), ['depth', 'int', '30', '>'])
  assert.deepEqual(typedCondition({ path: 'depth', type: 'float' }, '30', '='), ['depth', 'float', '30.0'])
  assert.throws(() => typedCondition({ path: 'id', type: 'string' }, '3', '>'), PredicateError)
  assert.throws(() => typedCondition({ path: 'lane', type: 'int' }, 'x', '>='), PredicateError)
})

test('fieldChoices: among equally common values, numbers sort by value, not as text, and come before text', () => {
  const v = (type, value) => ({ path: 'x', type, values: [value], types: [type] })
  const loaded = ['100', '89', '9.5', '-2'].map((n, i) => ({ pairs: [v(i === 2 ? 'float' : 'int', n)] }))
  assert.deepEqual(fieldChoices(loaded)[0].values.map(x => x.value), ['-2', '9.5', '89', '100'])
  const mixed = [{ pairs: [v('string', 'b')] }, { pairs: [v('int', '10')] }, { pairs: [v('string', 'a')] }, { pairs: [v('int', '9')] }]
  assert.deepEqual(fieldChoices(mixed)[0].values.map(x => x.value), ['9', '10', 'a', 'b'])
  const common = [{ pairs: [v('int', '100')] }, { pairs: [v('int', '100')] }, { pairs: [v('int', '9')] }]
  assert.deepEqual(fieldChoices(common)[0].values.map(x => x.value), ['100', '9'])
})

test('rankFields: whole path, whole last segment, its start, a word start in it then earlier, anywhere, then letters in order; ties keep the given order', () => {
  const f = (...paths) => paths.map(path => ({ path }))
  const ranked = (fields, q) => rankFields(fields, q).map(x => x.path)
  const fields = f('spread', 'qc.r_e_a_d', 'qc.thread_id', 'meta.read_count', 'qc.bigRead', 'meta.reads', 'read', 'meta.read')
  assert.deepEqual(ranked(fields, 'read'), ['read', 'meta.read', 'meta.read_count', 'meta.reads', 'qc.bigRead', 'spread', 'qc.thread_id', 'qc.r_e_a_d'])
  assert.deepEqual(ranked(fields, 'READ'), ranked(fields, 'read'), 'case is ignored')
  assert.deepEqual(ranked(fields, ''), fields.map(x => x.path), 'no query lists everything in the given order')
  assert.deepEqual(ranked(fields, 'zzz'), [])
  const rnaseq = f('id', 'single_end', 'meta.id', 'meta.salmon.forwardFragments', 'qc.meta.salmon.forwardFragments', 'qc.fastqc.read_count')
  assert.deepEqual(ranked(rnaseq, 'frag'), ['meta.salmon.forwardFragments', 'qc.meta.salmon.forwardFragments'])
  assert.deepEqual(ranked(rnaseq, 'fwdfrag'), ['meta.salmon.forwardFragments', 'qc.meta.salmon.forwardFragments'], 'letters in order, with gaps')
  assert.deepEqual(ranked(rnaseq, 'count'), ['qc.fastqc.read_count'])
  assert.deepEqual(ranked(rnaseq, 'id'), ['id', 'meta.id'], 'under three letters, no letters-in-order match (single_end has an i then a d)')
  assert.deepEqual(ranked(f('qc.salmon_strand.forwardFragments', 'meta.inferred_strandedness'), 'strand'),
    ['meta.inferred_strandedness', 'qc.salmon_strand.forwardFragments'], 'a word start in the field\'s own name beats one in a parent')
})
