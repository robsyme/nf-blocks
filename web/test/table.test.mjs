// web/test/table.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { cellOf, cellSlots, columnOrder, emptyColumns, fieldChoices, fieldPaths, fileColumns, fileGroups, itemColumns, itemTable, outputSuffixes, rankFields,
  typedCondition, withoutDuplicates } from '../src/table.js'
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
    { path: 'id', type: 'string', items: 3, values: [{ type: 'string', value: 'A', count: 1 }, { type: 'string', value: 'B', count: 1 }, { type: 'string', value: 'C', count: 1 }] },
    { path: 'single_end', type: 'bool', items: 3, values: [{ type: 'bool', value: 'false', count: 2 }, { type: 'bool', value: 'true', count: 1 }] },
    { path: 'tags', type: 'string', items: 3, values: [{ type: 'string', value: 'x', count: 1 }] },
  ])
  assert.deepEqual(fieldChoices([]), [])
})

test('fieldChoices: shallower paths first, then by text, so nested copies of meta sink to the end', () => {
  const loaded = [{ pairs: [e('dupradar.meta.salmon.inferred', 'string', 'forward'), e('meta.single_end', 'bool', 'true'), e('id', 'string', 'A'), e('meta.salmon.inferred', 'string', 'reverse')] }]
  assert.deepEqual(fieldChoices(loaded).map(f => f.path), ['id', 'meta.single_end', 'meta.salmon.inferred', 'dupradar.meta.salmon.inferred'])
})

test('typedCondition: != takes the field\'s type as = does, on any field', () => {
  assert.deepEqual(typedCondition({ path: 'strandedness', type: 'string' }, 'reverse', '!='), ['strandedness', 'string', 'reverse', '!='])
  assert.deepEqual(typedCondition({ path: 'lane', type: 'int' }, '007', '!='), ['lane', 'int', '7', '!='])
  assert.deepEqual(typedCondition({ path: 'x', type: 'mixed' }, '2', '!='), ['x', 'string', '2', '!='])
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

// --- The items grid's columns (P0 polish, Task 5) ---
const RNASEQ = [
  { path: 'num_trimmed_reads', label: 'Reads (trimmed)', format: 'int' },
  { path: 'meta.single_end', label: 'Layout', labels: { true: 'single', false: 'paired' } },
  { path: 'meta.strandedness', label: 'Strandedness' },
  { path: 'meta.salmon_strand_analysis.inferred_strandedness', label: 'Strand' },
  { path: 'percent_mapped', label: '% mapped', format: 'percent' },
]
const PREPROCESSED_PATHS = ['meta.has_genome_bam', 'meta.salmon_strand_analysis.forwardFragments', 'meta.single_end', 'id',
  'meta.salmon_strand_analysis.inferred_strandedness', 'num_trimmed_reads', 'meta.strandedness', 'meta.seq_center']

test('columnOrder: id first, then promoted paths in config order, then the rest A-Z', () => {
  assert.deepEqual(columnOrder(PREPROCESSED_PATHS, RNASEQ), ['id', 'num_trimmed_reads', 'meta.single_end', 'meta.strandedness',
    'meta.salmon_strand_analysis.inferred_strandedness', 'meta.has_genome_bam', 'meta.salmon_strand_analysis.forwardFragments', 'meta.seq_center'])
  assert.deepEqual(columnOrder(['sample', 'meta.id', 'lane'], []), ['meta.id', 'lane', 'sample'], 'meta.id leads when there is no id')
  assert.deepEqual(columnOrder(['zeta', 'alpha', 'id', 'mid'], []), ['id', 'alpha', 'mid', 'zeta'], 'no promoted config: id then A-Z (Review Focus 5)')
  assert.deepEqual(columnOrder(['qc.meta.single_end', 'meta.single_end', 'b'], RNASEQ), ['meta.single_end', 'qc.meta.single_end', 'b'],
    'a path ending in a promoted path sits with it, the exact one first')
  assert.deepEqual(columnOrder([], RNASEQ), [])
})

test('columnOrder: any subset of the paths comes out in the same relative order, so filters never reorder columns (Review Focus 4)', () => {
  const paths = [...PREPROCESSED_PATHS, 'qc.meta.single_end']
  const full = columnOrder(paths, RNASEQ)
  for (let mask = 1; mask < 2 ** paths.length; mask += 7) {
    const subset = paths.filter((_, i) => mask & (1 << i))
    assert.deepEqual(columnOrder(subset, RNASEQ), full.filter(p => subset.includes(p)), `subset ${subset}`)
  }
})

const pre = (id, reads, singleEnd, inferred = null) => ({
  pairs: [e('id', 'string', id), e('meta.id', 'string', id), e('meta.has_genome_bam', 'bool', 'false'), e('meta.strandedness', 'string', 'reverse'),
    e('meta.single_end', 'bool', singleEnd), e('num_trimmed_reads', 'float', reads), e('meta.seq_center', 'string', id.startsWith('WT') ? 'A' : 'B'),
    ...(inferred ? [e('meta.salmon_strand_analysis.inferred_strandedness', 'string', inferred), e('meta.salmon_strand_analysis.forwardFragments', 'float', '10.37')] : [])],
  slots: [],
})
const PRE = [pre('RAP1_IAA_30M_REP1', '49791.0', 'false'), pre('WT_REP1', '99566.0', 'false', 'reverse'), pre('RAP1_UNINDUCED_REP1', '49969.0', 'true'),
  pre('WT_REP2', '49760.0', 'false'), pre('RAP1_UNINDUCED_REP2', '99971.0', 'true')]

test('itemColumns: the sample id, then the varying promoted fields; a constant promoted field joins the strip; varying others wait under "more fields"', () => {
  const shape = itemColumns(PRE, { promoted: RNASEQ })
  assert.equal(shape.sample, 'id')
  assert.deepEqual(shape.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.single_end', 'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.deepEqual(shape.columns.map(c => c.field?.label ?? null), [null, 'Reads (trimmed)', 'Layout', 'Strand'])
  assert.deepEqual(shape.more, ['meta.salmon_strand_analysis.forwardFragments', 'meta.seq_center'])
  assert.deepEqual(shape.constant.map(c => c.path), ['meta.strandedness', 'meta.has_genome_bam'], 'the strip in column order: promoted first, then A-Z')
  assert.ok(!shape.columns.some(c => c.path === 'meta.id'))
})

test('itemColumns: an added field becomes a column in its stable place and leaves "more fields"; an added constant one too', () => {
  const shape = itemColumns(PRE, { promoted: RNASEQ, added: ['meta.seq_center', 'not.here'] })
  assert.deepEqual(shape.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.single_end',
    'meta.salmon_strand_analysis.inferred_strandedness', 'meta.seq_center'])
  assert.deepEqual(shape.more, ['meta.salmon_strand_analysis.forwardFragments'])
  const constant = itemColumns(PRE, { promoted: RNASEQ, added: ['meta.strandedness'] })
  assert.deepEqual(constant.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.single_end', 'meta.strandedness',
    'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.deepEqual(constant.constant.map(c => c.path), ['meta.has_genome_bam'])
})

test('itemColumns: a promoted column with one value moves to the strip when unfiltered, with no memory of it; the rest keep their order (Review Focus 4)', () => {
  const paired = itemColumns([PRE[0], PRE[1], PRE[3]], { promoted: RNASEQ })
  assert.deepEqual(paired.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.deepEqual(paired.constant.map(c => c.path), ['meta.single_end', 'meta.strandedness', 'meta.has_genome_bam'])
})

test('itemColumns: filtering to fewer items keeps id first and the promoted columns, in the same order (Review Focus 4)', () => {
  const all = itemColumns(PRE, { promoted: RNASEQ }).columns.map(c => c.path)
  const filtered = itemColumns([PRE[1], PRE[4]], { promoted: RNASEQ }).columns.map(c => c.path)
  assert.deepEqual(filtered, all)
  const one = itemColumns([PRE[3], PRE[3]], { promoted: RNASEQ }).columns.map(c => c.path)
  assert.equal(one[0], 'id', 'id stays first even when every loaded item holds the same id')
})

test('itemColumns: no promoted config, or none of its fields present, gives id then the varying fields A-Z; never an empty header (Review Focus 5)', () => {
  const plain = itemColumns(PRE, { promoted: [] })
  assert.deepEqual(plain.columns.map(c => c.path), ['id', 'meta.salmon_strand_analysis.forwardFragments',
    'meta.salmon_strand_analysis.inferred_strandedness', 'meta.seq_center', 'meta.single_end', 'num_trimmed_reads'])
  assert.deepEqual(plain.constant.map(c => c.path), ['meta.has_genome_bam', 'meta.strandedness'])
  assert.deepEqual(itemColumns(PRE, { promoted: [{ path: 'nowhere', label: 'Nowhere' }] }).columns.map(c => c.path), plain.columns.map(c => c.path))
  const capped = itemColumns(PRE, { promoted: [], maxColumns: 3 })
  assert.deepEqual(capped.columns.map(c => c.path), ['id', 'meta.salmon_strand_analysis.forwardFragments', 'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.deepEqual(capped.more, ['meta.seq_center', 'meta.single_end', 'num_trimmed_reads'])
  assert.deepEqual(itemColumns([], { promoted: RNASEQ }), { sample: null, columns: [], more: [], constant: [], duplicates: new Map() })
  const noId = itemColumns([{ pairs: [e('lane', 'int', '1')] }, { pairs: [e('lane', 'int', '2')] }], { promoted: [] })
  assert.equal(noId.sample, null)
  assert.deepEqual(noId.columns.map(c => c.path), ['lane'])
})

test('fileGroups: an item\'s stored files by top-level column, in the table\'s column order, with extension counts and bytes', () => {
  const s = (column, name, size, address = `bafk-${name}`) => ({ column, path: `${column}.${name}`, name, size, address, dir: false, reason: address ? null : 'declined' })
  const p = { slots: [s('fastqc', 'A_1.html', 100), s('fastqc', 'A_1.zip', 300), s('fastqc', 'A_2.html', 100), s('fastqc', 'A_2.zip', 300),
    s('trim', 'A.txt', 5), s('bbsplit', 'A.stats.txt', 7), { column: 'reads_cat', path: 'reads_cat', name: null, size: null, address: null, dir: false, reason: 'declined' },
    { column: 'trim', path: 'trim.log', name: null, size: null, address: null, dir: false, reason: 'declined' }] }
  const groups = fileGroups(p, ['bbsplit', 'fastqc', 'trim', 'unzip'])
  assert.deepEqual(groups.map(g => [g.column, g.exts, g.bytes, g.files.length, g.empty.length]), [
    ['bbsplit', [['txt', 1]], 7, 1, 0], ['fastqc', [['zip', 2], ['html', 2]], 800, 4, 0], ['trim', [['txt', 1]], 5, 1, 1]])
  const dir = { slots: [{ column: 'salmon', path: 'salmon', name: 'A', size: null, address: 'bafy-dir', dir: true, reason: null }] }
  assert.deepEqual(fileGroups(dir, ['salmon']).map(g => g.exts), [[['dir', 1]]])
  const declined = { slots: [{ column: 'trim', path: 'trim', name: null, size: null, address: null, dir: false, reason: 'declined' }] }
  assert.deepEqual(fileGroups(declined, ['trim']).map(g => [g.column, g.files.length, g.empty.length]), [['trim', 0, 1]])
  assert.deepEqual(fileGroups(undefined, ['x']), [])
})

test('one list of sample paths: a Meta Map keyed on sample leads the grid as its Sample column', async () => {
  const { SAMPLE_PATHS, columnOrder, itemColumns, fileGroups } = await import('../src/table.js')
  const { itemName } = await import('../src/views/common.js')
  assert.ok(SAMPLE_PATHS.includes('sample'))
  assert.deepEqual(columnOrder(['lane', 'sample', 'depth']), ['sample', 'depth', 'lane'])
  const p = (sample, lane) => ({ pairs: [{ path: 'sample', type: 'string', types: ['string'], values: [sample] }, { path: 'lane', type: 'int', types: ['int'], values: [lane] }] })
  assert.equal(itemColumns([p('A', 1), p('B', 2)]).sample, 'sample')
  assert.equal(itemName(p('A', 1)), 'A')
  const groups = fileGroups({ slots: [{ column: 'docs', name: 'README', address: 'bafkr', size: 3 }, { column: 'docs', name: 'a.txt', address: 'bafkt', size: 1 }] }, ['docs'])
  assert.deepEqual(groups[0].exts, [['txt', 1]], 'README draws no empty extension')
})

// --- p1a task 5: column stability ---

test('itemColumns: a constant promoted field goes to the strip unfiltered, but is a column when filtered or kept', () => {
  const paired = [PRE[0], PRE[1], PRE[3]]
  assert.ok(itemColumns(paired, { promoted: RNASEQ }).constant.some(c => c.path === 'meta.single_end'))
  const filtered = itemColumns(paired, { promoted: RNASEQ, filtered: true })
  assert.deepEqual(filtered.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.single_end', 'meta.strandedness', 'meta.salmon_strand_analysis.inferred_strandedness'])
  assert.ok(!filtered.constant.some(c => c.path === 'meta.single_end'))
  const kept = itemColumns(paired, { promoted: RNASEQ, keep: ['meta.single_end'] })
  assert.deepEqual(kept.columns.map(c => c.path), ['id', 'num_trimmed_reads', 'meta.single_end', 'meta.salmon_strand_analysis.inferred_strandedness'])
})
