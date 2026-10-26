import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { resetFolds } from '../src/folds.js'
import { content } from '../src/views/collection.js'
import { highlightOf, isPreviewable, kindOf, matrixSamples, previewOf } from '../src/filepreview.js'

const enc = (s) => new TextEncoder().encode(s)
const tsv = (rows, cols) => [Array.from({ length: cols }, (_, c) => `c${c}`).join('\t'),
  ...Array.from({ length: rows }, (_, r) => Array.from({ length: cols }, (_, c) => (c < 2 ? `g${r}` : String(r * c))).join('\t'))].join('\n') + '\n'

test('previewOf: a TSV gives the header, the first 50 rows and the true counts', () => {
  const p = previewOf('x.tsv', enc(tsv(126, 7)))
  assert.equal(p.kind, 'table')
  assert.equal(p.head.length, 7)
  assert.equal(p.rows.length, 50)
  assert.equal(p.totalRows, 126)
  assert.equal(p.totalCols, 7)
  assert.equal(p.truncated, false)
  assert.deepEqual(p.rows[1].slice(0, 3), ['g1', 'g1', '2'])
})

test('previewOf: maxRows is honoured and a CSV splits on commas, with quotes', () => {
  const p = previewOf('a.csv', enc('id,note\n1,"a, b"\n2,"say ""hi"""\n3,x\n'), { maxRows: 2 })
  assert.deepEqual(p.head, ['id', 'note'])
  assert.deepEqual(p.rows, [['1', 'a, b'], ['2', 'say "hi"']])
  assert.equal(p.totalRows, 3)
})

test('previewOf: a file over 64 KB is read for its first 64 KB and says it was cut', () => {
  const p = previewOf('big.tsv', enc(tsv(20000, 3)))
  assert.equal(p.truncated, true)
  assert.ok(p.totalRows < 20000 && p.totalRows > 1000)
  assert.ok(p.rows.every(r => r.length === 3), 'the cut last line is dropped')
})

test('previewOf: text-like names give lines; html gives html; the rest is binary', () => {
  for (const name of ['a.txt', 'a.log', 'a.yml', 'a.yaml', 'a.json', 'a.out', 'a.tab']) {
    assert.deepEqual(previewOf(name, enc('one\r\ntwo\n')), { kind: 'text', lines: ['one', 'two'], truncated: false }, name)
  }
  assert.deepEqual(previewOf('r.html', enc('<p>x</p>')), { kind: 'html' })
  assert.deepEqual(previewOf('x.bam', enc('BAM')), { kind: 'binary' })
  assert.deepEqual(previewOf('x.zip', enc('PK')), { kind: 'binary' })
})

test('previewOf: bytes with a NUL or invalid UTF-8 are binary even under a text name', () => {
  assert.deepEqual(previewOf('a.txt', new Uint8Array([104, 0, 105])), { kind: 'binary' })
  assert.deepEqual(previewOf('a.tsv', new Uint8Array([0xff, 0xfe, 0x41])), { kind: 'binary' })
})

test('isPreviewable and kindOf', () => {
  assert.equal(isPreviewable('a.tsv'), true)
  assert.equal(isPreviewable('A.HTML'), true)
  assert.equal(isPreviewable('a.bam'), false)
  assert.equal(isPreviewable(null), false)
  assert.equal(kindOf('salmon.merged.gene_counts.tsv'), 'TSV')
  assert.equal(kindOf('x.fastq.gz'), 'GZ')
  assert.equal(kindOf('README'), 'FILE')
})

test('previewOf: 64 KB with no newline shows the cut text rather than nothing', () => {
  const long = 'x'.repeat(70000)
  for (const name of ['a.txt', 'a.tsv']) {
    const p = previewOf(name, enc(long))
    assert.equal(p.kind, 'text')
    assert.equal(p.truncated, true)
    assert.equal(p.lines.length, 1)
    assert.equal(p.lines[0].length, 65536)
  }
})

// ---- a merged matrix's sample columns (P1a Task 3) ----

const IDS = ['RAP1_IAA_30M_REP1', 'RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2']

test('matrixSamples: the sample ids the header row holds, in header order, each once', () => {
  const text = `gene_id\tgene_name\t${['WT_REP2', 'RAP1_IAA_30M_REP1', 'WT_REP1', 'WT_REP1'].join('\t')}\nENSG1\tg1\t1\t2\t3\t3\n`
  assert.deepEqual(matrixSamples(text, IDS), ['WT_REP2', 'RAP1_IAA_30M_REP1', 'WT_REP1'])
  assert.deepEqual(matrixSamples('"gene_id","WT_REP1","other"\r\n"g",1,2\r\n', IDS), ['WT_REP1'], 'a CSV with quotes and CRLF')
  assert.deepEqual(matrixSamples('gene_id\tgene_name\nx\ty\n', IDS), [])
  assert.deepEqual(matrixSamples('', IDS), [])
  // A header cut before its newline: the last cell may be half a name, so it is not counted.
  assert.deepEqual(matrixSamples('gene_id\tWT_REP1\tWT_REP', ['WT_REP1', 'WT_REP']), ['WT_REP1'])
})

test('highlightOf: a JSON list of strings from the URL, anything else is no highlight', () => {
  assert.deepEqual(highlightOf('["RAP1_UNINDUCED_REP2","WT_REP1"]'), ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  assert.deepEqual(highlightOf('["A", 3, null]'), ['A'])
  assert.deepEqual(highlightOf(null), [])
  assert.deepEqual(highlightOf('not json'), [])
  assert.deepEqual(highlightOf('{"a":1}'), [])
})

test('file page: ?highlight marks those sample columns, header and cells, with class hl and says so', async () => {
  installDom(); resetFolds()
  const TSV = 'bafkreibxyh4xa4v5od634co6m4wcvs3yrh7s4vhsz2hgiybzwkahgz3oym'
  const RUN = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'
  const body = enc(`gene_id\tgene_name\t${IDS.join('\t')}\n${Array.from({ length: 3 }, (_, i) => `g${i}\tn${i}\t1\t2\t3\t4\t5`).join('\n')}\n`)
  const ex = {
    producersOf: async () => [{ content_cid: TSV, item_cid: 'i', collection_cid: 'c', completion_cid: RUN, filename: 'salmon.merged.gene_counts.tsv' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])),
    runIdentity: async (c) => ({ completion: c, pipeline: 'nf-core/rnaseq', run_name: 'distraught_murdock', status: 'succeeded' }),
    collectionLabel: async () => ({ output: 'quant_merged', run_name: 'distraught_murdock' }),
    itemBlock: async () => ({ view: { id: 'all_samples' }, leaves: [] }),
    blocks: { urlFor: (a) => a, partial: async () => ({ bytes: null, size: body.length }), raw: async () => body },
  }
  const ctx = { tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: 'roblab', key: 'a' }, write: { available: false, reason: 'read only', hrefFor: (h) => h } }
  const page = await content(ex, TSV, ctx, { highlight: ['RAP1_UNINDUCED_REP2', 'WT_REP1', 'NOT_A_COLUMN'] })
  await frame()
  const table = page.querySelector('[data-preview=table]')
  const heads = table.querySelectorAll('th')
  assert.deepEqual(heads.filter(t => t.classList.contains('hl')).map(t => t.textContent), ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  const firstRow = table.querySelectorAll('tr')[1].children
  assert.deepEqual(firstRow.map(td => td.classList.contains('hl')), [false, false, false, false, true, true, false])
  assert.match(page.textContent, /2 sample columns highlighted/)
  const plain = await content(ex, TSV, ctx)
  assert.equal(plain.querySelectorAll('.hl').length, 0, 'the old route, no highlight')
  assert.doesNotMatch(plain.textContent, /highlighted/)
})
