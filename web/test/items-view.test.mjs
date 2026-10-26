// web/test/items-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame, strayTexts } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { items, menuPlace } from '../src/views/items.js'
import { rawCid } from './fixture.mjs'

const view = (id, singleEnd) => ({ id, single_end: singleEnd, strandedness: 'reverse' })
const VIEWS = { i1: view('WT_REP1', false), i2: view('WT_REP2', false), i3: view('RAP1_REP1', true) }
const leaf = (name, address, size = 10, reason = null) => ({ kind: 'Leaf', name, address, size, reason })
const BAM = rawCid('bam')
// A markdup-shaped record; i3 has no bai, json_info is a declined optional output on every item, and
// i1's samtools.idxstats is declined while its stats and flagstat are stored.
const valueOf = (i) => {
  const id = VIEWS[i].id
  const value = { id, bam: leaf(`${id}.markdup.sorted.bam`, BAM, 2048), bai: leaf(`${id}.markdup.sorted.bam.bai`, rawCid(`bai-${i}`)),
    samtools: { stats: leaf(`${id}.stats`, rawCid(`stats-${i}`)), flagstat: leaf(`${id}.flagstat`, rawCid(`flagstat-${i}`)) },
    json_info: leaf(null, null, null, 'declined') }
  if (i === 'i3') delete value.bai
  if (i === 'i1') value.samtools.idxstats = leaf(null, null, null, 'declined')
  return value
}
const SIZES = { i1: 5_290_000, i2: 2_640_000, i3: 1_000_000 }

// Every snapshot-backed method throws: the items view and its panel input may
// use only Explorer.items, block reads, sizes and runIdentity (Review Focus 1, plan P1/P2).
function fakeEx({ pipeline = 'nf-core/rnaseq', all = ['i1', 'i2', 'i3'], output = 'markdup' } = {}) {
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot during query 3`) }
  return {
    items: async (completion, out, where) => ({ items: where.length ? all.slice(0, 2) : all, collection: 'coll' }),
    itemBlock: async (c, i) => ({ view: VIEWS[i], leaves: [{ name: `${VIEWS[i].id}.markdup.sorted.bam` }, { name: `${VIEWS[i].id}.markdup.sorted.bam.bai` }], value: valueOf(i) }),
    runIdentity: async () => ({ pipeline, run_name: 'high_jang', lid: 'lid://x' }),
    blocks: { ofKind: async (cid, kind) => {
      if (cid === 'coll' && kind === 'OutputCollection') return { value: { kind: 'OutputCollection', name: output, items: all } }
      throw Object.assign(new Error(`no ${kind} ${cid}`), { code: 'block_missing' })
    } },
    completionOf: async () => ({ collections: ['coll'] }),
    itemFileMap: async (i) => ({ files: new Map([[`raw-${i}`, { size: SIZES[i] ?? 0 }]]), dirs: [], partial: false }),
    itemSize: async (i) => ({ bytes: SIZES[i] ?? 0, blocks: 1, files: 1, kinds: [], partial: false }),
    runLabel: forbidden('runLabel'), runRow: forbidden('runRow'), runPage: forbidden('runPage'), pipelines: forbidden('pipelines'),
    collection: forbidden('collection'), latestSuccessfulRun: forbidden('latestSuccessfulRun'), allItems: forbidden('allItems'),
    producersOf: forbidden('producersOf'), selectionPage: forbidden('selectionPage'),
  }
}
function ctx() {
  const c = { tray: new Tray(null), sources: new Map(), used: [], marked: [], rerender: () => {}, write: { available: false, hrefFor: (h) => h } }
  c.trayChanged = () => {}
  c.use = (v) => c.used.push({ ...v })
  c.mark = (m) => c.marked.push(m)
  return c
}
const settle = async () => { await frame(); await frame(); await frame() }
const heads = (page) => page.querySelectorAll('th').map(t => t.textContent)
const rowsOf = (page) => page.querySelectorAll('[data-item-result]')

// The preprocessed output of an nf-core/rnaseq run, as distraught_murdock stored it.
const PRE = {
  p1: ['RAP1_IAA_30M_REP1', 49791, false, null, 3_000_000], p2: ['WT_REP1', 99566, false, 'reverse', 5_290_000],
  p3: ['RAP1_UNINDUCED_REP1', 49969, true, null, 2_500_000], p4: ['WT_REP2', 49760, false, null, 3_100_000],
  p5: ['RAP1_UNINDUCED_REP2', 99971, true, null, 2_640_000],
}
// num_trimmed_reads arrives as the index types a Groovy float, e.g. "99566.0".
const { Float } = await import('../src/typed.js')
const preView = (i) => {
  const [id, reads, singleEnd, inferred] = PRE[i]
  return { id, num_trimmed_reads: new Float(reads), meta: { id, single_end: singleEnd, strandedness: 'reverse', has_genome_bam: false,
    seq_center: id.startsWith('WT') ? 'A' : 'B', ...(inferred ? { salmon_strand_analysis: { inferred_strandedness: inferred, forwardFragments: new Float(10.37) } } : {}) } }
}
const preValue = (i) => {
  const id = PRE[i][0]
  return { id, fastqc: [1, 2].flatMap(r => [leaf(`${id}_${r}_fastqc.html`, rawCid(`html-${i}-${r}`), 200_000), leaf(`${id}_${r}_fastqc.zip`, rawCid(`zip-${i}-${r}`), 400_000)]),
    trim: leaf(`${id}.trimming_report.txt`, rawCid(`trim-${i}`), 5000), reads_cat: leaf(null, null, null, 'declined') }
}
function preEx({ pipeline = 'nf-core/rnaseq' } = {}) {
  const all = Object.keys(PRE)
  return { ...fakeEx({ pipeline, all, output: 'preprocessed' }),
    items: async (completion, out, where) => ({ items: where.length ? ['p2', 'p5'] : all, collection: 'coll' }),
    itemBlock: async (c, i) => ({ view: preView(i), leaves: [], value: preValue(i) }),
    itemFileMap: async (i) => ({ files: new Map([[`raw-${i}`, { size: PRE[i][4] }]]), dirs: [], partial: false }),
    itemSize: async (i) => ({ bytes: PRE[i][4], blocks: 1, files: 1, kinds: [], partial: false }) }
}
const READS = JSON.stringify([['num_trimmed_reads', 'int', '50000', '>=']])

test('rows, columns from the loaded previews, the constant strip; no snapshot read but the query (Review Focus 1)', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await settle()
  const rows = rowsOf(page)
  assert.deepEqual(rows.map(r => r.dataset.itemResult), ['i1', 'i2', 'i3'])
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'single_end'])
  const strip = page.querySelector('.constants')
  assert.match(strip.textContent, /^Same for every item:/)
  assert.deepEqual(strip.querySelectorAll('.field').map(f => [f.querySelector('.k').textContent, f.querySelector('.v').textContent]), [['strandedness', 'reverse']])
  assert.equal(rows[0].querySelector('[data-pick]').dataset.pick, 'i1')
  assert.equal(rows[0].querySelector('[data-pick]').dataset.via, 'coll')
  assert.ok(rows[0].querySelector('[data-address]'))
  assert.equal(c.used.at(-1).count, 3)
  assert.deepEqual(c.marked, [{ run: 'bafyrun' }])
})

test('the columns: checkbox, Sample, the metadata columns, Files, Size and the row actions', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  // Rows still loading leave cells out; none may be written as the text "null".
  assert.deepEqual(strayTexts(page), [])
  await settle()
  assert.deepEqual(strayTexts(page), [])
  assert.deepEqual(heads(page), ['', 'Sample', 'single_end', 'Files', 'Size', ''])
  const cells = rowsOf(page)[0].querySelectorAll('td')
  assert.equal(cells.length, 6)
  assert.equal(cells[1].querySelector('a[href="#/item/coll/i1"]').textContent, 'WT_REP1')
  assert.equal(cells[2].textContent.trim(), 'false')
  assert.ok(cells[3].querySelector('[data-file-cell]'))
  assert.equal(cells[4].textContent, '5.29 MB')
  assert.ok(cells[5].querySelector('[data-pick]') && cells[5].querySelector('[data-open-item]'))
})

test('file chips group a row\'s files by the item\'s top-level key, labelled with their extensions; one file links to its page, more to the item', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.fileColumns), ['bai', 'bam', 'samtools'])
  const row = rowsOf(page)[0]
  const chips = row.querySelectorAll('[data-file-cell]')
  assert.deepEqual(chips.map(c => c.dataset.fileCell), ['bai', 'bam', 'samtools'])
  const bam = row.querySelector('a[data-pill="bam"]')
  assert.ok(bam.classList.contains('fileb'))
  assert.equal(bam.textContent, 'bam bam×1')
  assert.equal(bam.getAttribute('href'), `#/content/${BAM.toString()}`, 'one file opens its file page')
  assert.equal(bam.getAttribute('download'), null)
  assert.match(bam.getAttribute('title'), /^bam: 1 file, 2.05 KB\nWT_REP1.markdup.sorted.bam$/)
  const samtools = row.querySelector('[data-file-cell="samtools"]')
  assert.equal(samtools.tagName, 'SUMMARY', 'two files open a menu')
  assert.equal(row.querySelector('.filemenu-item').getAttribute('href'), '#/item/coll/i1')
  assert.equal(samtools.textContent, 'samtools flagstat×1 stats×1')
  assert.match(samtools.getAttribute('title'), /^samtools: 2 files, 20 B\n/)
  assert.match(samtools.getAttribute('title'), /WT_REP1.stats/)
  assert.match(samtools.getAttribute('title'), /1 without a stored file: samtools.idxstats/)
  assert.equal(rowsOf(page)[2].querySelector('[data-file-cell="bai"]'), null, 'an item with no file under a key has no chip for it')
})

test('a key with only address-less slots in this row is a dashed chip saying so, not a link', async () => {
  installDom()
  const ex = fakeEx()
  const item = ex.itemBlock
  ex.itemBlock = async (c, i) => {
    const it = await item(c, i)
    if (i === 'i2') it.value.samtools = { stats: leaf(null, null, null, 'declined') }
    return it
  }
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  const chip = rowsOf(page)[1].querySelector('[data-file-cell="samtools"]')
  assert.equal(chip.tagName, 'SPAN')
  assert.equal(chip.dataset.pillEmpty, '1')
  assert.equal(chip.textContent, 'samtools none')
  assert.match(chip.getAttribute('title'), /samtools.stats: no file \(an optional output was empty\)/)
})

test('keys with no stored file in any item get no chip; the constant strip names them', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  assert.equal(page.querySelector('[data-file-cell="json_info"]'), null)
  assert.equal(page.querySelector('[data-empty-columns]').textContent, 'No stored file in any item: json_info')
})

test('many files under one key are one chip opening a menu with the item page, "-" when there is no collection (Review Focus 5)', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: ['i1'], collection: null })
  ex.itemBlock = async () => ({ view: { id: 'S' }, leaves: [], value: { id: 'S', mqc: [leaf(null, null, null, 'declined'),
    ...[...Array(9)].map((_, i) => leaf(`f${i}.html`, rawCid(`f${i}`))), leaf(null, null, null, 'declined')] } })
  const page = await items(ex, 'bafyrun', 'bam_qc', '[]', ctx())
  await settle()
  const chip = page.querySelector('[data-file-cell="mqc"]')
  assert.equal(chip.textContent, 'mqc html×9')
  assert.equal(page.querySelector('.filemenu-item').getAttribute('href'), '#/item/-/i1')
  assert.match(chip.getAttribute('title'), /^mqc: 9 files, 90 B\n/)
  assert.match(chip.getAttribute('title'), /2 without a stored file/)
})

test('loading and failed rows keep the Sample, Size and action cells and span the rest (Review Focus 4)', async () => {
  installDom()
  const ex = fakeEx()
  const item = ex.itemBlock
  ex.itemBlock = async (c, i) => { if (i === 'i3') throw Object.assign(new Error('gone'), { code: 'block_missing' }); return item(c, i) }
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  const columns = JSON.parse(page.querySelector('table').dataset.columns).length
  const failed = rowsOf(page)[2]
  const cells = failed.querySelectorAll('td')
  assert.match(cells[1].textContent, /not in this store/)
  assert.equal(cells[2].getAttribute('colspan'), String(columns - 1 + 1))
  assert.equal(cells.length, 5)
  assert.ok(cells[4].querySelector('[data-pick]'))
})

test('the Sample opens the item; a separate small filter button carries the filter link; the chips bar removes one', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['single_end', 'bool', 'false']]), ctx())
  await settle()
  const chip = page.querySelector('[data-chip="single_end"]')
  assert.match(chip.textContent, /single_end = false/)
  assert.equal(chip.querySelector('a[data-chip-remove]').getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  const row = rowsOf(page)[0]
  const sample = row.querySelectorAll('a').find(a => a.textContent === 'WT_REP1')
  assert.equal(sample.getAttribute('href'), '#/item/coll/i1')
  const filter = row.querySelector('a[data-filter-value="WT_REP1"]')
  assert.equal(filter.getAttribute('aria-label'), 'Only items where id is WT_REP1')
  assert.deepEqual(JSON.parse(decodeURIComponent(filter.getAttribute('href').split('where=')[1])), [['single_end', 'bool', 'false'], ['id', 'string', 'WT_REP1']])
})

test('checkboxes report to the panel, and "Pick these" puts the checked rows in the tray with the collection', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  const box = rowsOf(page)[1].querySelector('input')
  box.checked = false
  await box.fire('change')
  assert.equal(c.used.at(-1).checked, 2)
  c.used.at(-1).pickChecked()
  assert.deepEqual(c.tray.entries().map(e => [e.address, e.via]), [['i1', ['coll']], ['i3', ['coll']]])
})

test('no items says so, names what the filter hides, and still reports the empty output to the panel', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: [], collection: null })
  const c = ctx()
  const page = await items(ex, 'bafyrun', 'markdup', JSON.stringify([['id', 'string', 'nope']]), c)
  await settle()
  assert.match(page.textContent, /No items match/)
  const hidden = page.querySelector('[data-hidden-count]')
  assert.equal(hidden.dataset.hiddenCount, '3')
  assert.match(hidden.textContent, /^3 items hidden by the filter/)
  assert.equal(hidden.getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  assert.equal(c.used.at(-1).count, 0)
})

test('the items view records which run and output its collection is, for the panel', async () => {
  installDom()
  const c = ctx()
  await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  assert.deepEqual(c.sources.get('coll'), { completion: 'bafyrun', output: 'markdup' })
})

test('Pick adds the item with its collection and then reads Picked; Open goes to the item page', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await settle()
  const row = rowsOf(page)[0]
  const pick = row.querySelector('button[data-pick]')
  assert.equal(pick.dataset.pick, 'i1')
  assert.equal(pick.dataset.via, 'coll')
  assert.equal(pick.dataset.kind, 'item')
  assert.equal(pick.textContent, 'Pick')
  await pick.fire('click')
  assert.deepEqual(c.tray.entries().map(e => [e.address, e.via]), [['i1', ['coll']]])
  assert.equal(pick.disabled, true)
  assert.equal(pick.textContent, 'Picked')
  const open = row.querySelector('a[data-open-item]')
  assert.equal(open.textContent, 'Open')
  assert.equal(open.getAttribute('href'), '#/item/coll/i1')
  assert.equal(open.dataset.address, 'i1')
})

test('an item already in the tray draws its Pick button as Picked', async () => {
  installDom()
  const c = ctx()
  c.tray.add({ address: 'i2', via: ['coll'], kind: 'item' })
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await settle()
  const pick = rowsOf(page)[1].querySelector('button[data-pick]')
  assert.equal(pick.disabled, true)
  assert.equal(pick.textContent, 'Picked')
})

test('the filter bar holds the chips, Add filter and Clear all; Pick all sits in the page head and counts the filtered items', async () => {
  installDom()
  const where = [['single_end', 'bool', 'false']]
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify(where), ctx())
  await settle()
  const bar = page.querySelector('[data-filter-bar]')
  assert.ok(bar.querySelector('[data-chip="single_end"]'))
  assert.equal(bar.querySelector('[data-chip="single_end"]').getAttribute('title'), 'single_end = false')
  assert.ok(bar.querySelector('button[data-add-filter]'))
  assert.equal(bar.querySelector('a[data-clear-filters]').getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  const all = page.querySelector('.page-head').querySelector('[data-pick-all]')
  assert.equal(all.textContent, 'Pick all 2')
  assert.equal(all.dataset.count, '2')
  assert.equal(all.dataset.via, 'coll')
  assert.equal(page.querySelector('fieldset'), null)
})

// --- promoted fields, formatting and sizes (P0 polish, Task 5) ---

test('promoted fields lead after Sample, labelled with their path as small text; a constant one joins the strip; numbers and booleans formatted', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'num_trimmed_reads', 'meta.single_end'])
  const reads = page.querySelector('th[data-column="num_trimmed_reads"]')
  assert.equal(reads.textContent, 'Reads (trimmed)num_trimmed_reads')
  assert.equal(reads.querySelector('small').textContent, 'num_trimmed_reads')
  // Strand (meta.strandedness) reads "reverse" on every item: unfiltered, the promoted field joins the strip under its label.
  assert.deepEqual(page.querySelector('.constants').querySelectorAll('.field').map(f => f.textContent),
    ['Strandreverse', 'has_genome_bamfalse'])
  const wt = rowsOf(page)[1]
  assert.equal(wt.querySelector('td[data-column="num_trimmed_reads"]').textContent, '99,566')
  assert.equal(wt.querySelector('td[data-column="meta.single_end"]').textContent, 'paired')
  assert.equal(wt.querySelector('td[data-column="meta.strandedness"]'), null)
  assert.equal(rowsOf(page)[2].querySelector('td[data-column="meta.single_end"]').textContent, 'single')
  assert.equal(page.querySelector('th[data-column="meta.salmon_strand_analysis.inferred_strandedness"]'), null, 'the inferred strandedness is an ordinary field')
  const filter = wt.querySelector('td[data-column="num_trimmed_reads"]').querySelector('a[data-filter-value]')
  assert.equal(filter.dataset.filterValue, '99566.0', 'the filter link keeps the raw value')
  assert.equal(filter.getAttribute('aria-label'), 'Only items where num_trimmed_reads is 99,566')
})

test('fastqc files are one chip reading "fastqc zip×2 html×2"; the Size column and the heading fill from the sizes', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const wt = rowsOf(page)[1]
  assert.equal(wt.querySelector('[data-file-cell="fastqc"]').textContent, 'fastqc zip×2 html×2')
  assert.equal(wt.querySelector('td[data-size]').textContent, '5.29 MB')
  assert.equal(page.querySelector('[data-items-count]').textContent, '5 items · 16.5 MB')
})

test('filtered: the heading reads "2 of 5 items · 7.93 MB", the chip shows the promoted label, and a row names what the filter hides', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', READS, ctx())
  await settle()
  assert.equal(page.querySelector('[data-items-count]').textContent, '2 of 5 items · 7.93 MB')
  const chip = page.querySelector('[data-chip="num_trimmed_reads"]')
  assert.equal(chip.textContent.replace('×', ''), 'Reads (trimmed) ≥ 50,000')
  assert.equal(chip.getAttribute('title'), 'num_trimmed_reads >= 50000', 'the title keeps the raw condition')
  const hidden = page.querySelector('[data-hidden-count]')
  assert.equal(hidden.textContent, '3 items hidden by the filter · RAP1_IAA_30M_REP1 (49,791), RAP1_UNINDUCED_REP1 (49,969), WT_REP2 (49,760)')
  assert.equal(hidden.getAttribute('href'), `#/items/bafyrun/preprocessed?where=${encodeURIComponent('[]')}`)
  assert.deepEqual(rowsOf(page).map(r => r.dataset.itemResult), ['p2', 'p5'], 'the hidden items are not result rows')
})

test('no raw float anywhere on the page: cells, chip, constant strip and the picker\'s values', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', READS, ctx())
  await settle()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  await picker.querySelector('[data-field="num_trimmed_reads"]').fire('click')
  assert.deepEqual(picker.querySelectorAll('a[data-value]').map(a => a.childNodes[0].textContent), ['99,566', '99,971'])
  assert.doesNotMatch(page.textContent, /\d\.0\b/)
  assert.doesNotMatch(page.textContent, /\d{5}\.\d/)
})

test('fields beyond the shown columns are listed under the table; each adds its column for this page view', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const more = page.querySelector('[data-more-fields]')
  assert.match(more.textContent, /^3 more fields: /)
  const buttons = more.querySelectorAll('button[data-add-column]')
  assert.deepEqual(buttons.map(b => b.dataset.addColumn), ['meta.salmon_strand_analysis.forwardFragments',
    'meta.salmon_strand_analysis.inferred_strandedness', 'meta.seq_center'])
  assert.deepEqual(buttons.map(b => b.textContent), ['forwardFragments', 'inferred_strandedness', 'seq_center'])
  await buttons[2].fire('click')
  await settle()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns).at(-1), 'meta.seq_center')
  assert.equal(rowsOf(page)[1].querySelector('td[data-column="meta.seq_center"]').textContent, 'A')
  assert.match(page.querySelector('[data-more-fields]').textContent, /^2 more fields: /)
})

test('a pipeline with no promoted config shows id then the varying fields A-Z (Review Focus 5)', async () => {
  installDom()
  const page = await items(preEx({ pipeline: 'main.nf' }), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'meta.salmon_strand_analysis.forwardFragments',
    'meta.salmon_strand_analysis.inferred_strandedness', 'meta.seq_center', 'meta.single_end', 'num_trimmed_reads'])
  assert.ok(heads(page).every((t, i, all) => t !== '' || i === 0 || i === all.length - 1), 'no empty header but the checkbox and actions')
  assert.equal(rowsOf(page)[1].querySelector('td[data-column="num_trimmed_reads"]').textContent, '99,566')
  assert.equal(rowsOf(page)[1].querySelector('td[data-column="meta.single_end"]').textContent, 'false')
})

test('adding or removing a filter keeps id first and the columns in the same order (Review Focus 4)', async () => {
  installDom()
  const all = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  const some = await items(preEx(), 'bafyrun', 'preprocessed', READS, ctx())
  await settle()
  assert.deepEqual(JSON.parse(some.querySelector('table').dataset.columns), JSON.parse(all.querySelector('table').dataset.columns))
  assert.equal(heads(some)[1], 'Sample')
})

// --- fix round 1 ---

// `n` items shaped like markdup's i1, each its own CID; itemSize counted (and slowed by `delay` ms).
function manyEx(n, { delay = 0 } = {}) {
  const all = [...Array(n)].map((_, i) => `m${i}`)
  const ex = fakeEx({ all })
  ex.items = async () => ({ items: all, collection: 'coll' })
  ex.itemBlock = async (c, i) => ({ view: { id: i, lane: Number(i.slice(1)) }, leaves: [], value: { id: i, bam: leaf(`${i}.bam`, rawCid(i), 1000) } })
  ex.sized = []
  ex.itemSize = async (i) => {
    ex.sized.push(i)
    if (delay) await new Promise(r => setTimeout(r, delay))
    return { bytes: 1000, blocks: 1, files: 1, kinds: [], partial: false }
  }
  ex.itemFileMap = async (i) => ({ files: new Map([[`raw-${i}`, { size: 1000 }]]), dirs: [], partial: false })
  return ex
}

test('sizes are measured only for rows whose previews were asked; the heading says "at least" until every row is; show details measures that row', async () => {
  installDom()
  const ex = manyEx(103)
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  assert.equal(ex.sized.length, 100, 'the first PREVIEW_CAP rows')
  assert.equal(page.querySelector('[data-items-count]').textContent, '103 items · at least 100 KB')
  const late = rowsOf(page)[101]
  assert.equal(late.querySelector('td[data-size]').textContent, '')
  await late.querySelectorAll('button').find(b => b.textContent === 'show details').fire('click')
  await settle()
  assert.ok(ex.sized.includes('m101'))
  assert.equal(late.querySelector('td[data-size]').textContent, '1 KB')
  assert.equal(page.querySelector('[data-items-count]').textContent, '103 items · at least 101 KB')
})

test('once the page has been shown and is gone, the size and preview queues stop', async () => {
  installDom()
  const ex = manyEx(100, { delay: 5 })
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  let connected = true
  Object.defineProperty(page.querySelector('.items-body'), 'isConnected', { get: () => connected })
  await frame()
  connected = false
  await frame()
  const stopped = ex.sized.length
  await new Promise(r => setTimeout(r, 200))
  assert.ok(stopped < 100, `stopped after ${stopped}`)
  assert.ok(ex.sized.length <= stopped + 4, 'at most the lookups already in flight finish')
})

test('a row with no metadata or file columns yet spans nothing: every row has as many cells as the header', async () => {
  installDom()
  const ex = fakeEx()
  ex.itemBlock = async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) }
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  const width = heads(page).length
  assert.equal(width, 4, 'checkbox, Item, Size, actions')
  for (const r of rowsOf(page)) assert.equal(r.querySelectorAll('td').length, width)
  const loading = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  for (const r of rowsOf(loading)) assert.equal(r.querySelectorAll('td').length, heads(loading).length, 'before any preview arrives')
})

test('no hidden-row placeholder when nothing is hidden', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  assert.equal(page.querySelector('tr'), page.querySelector('thead').querySelector('tr'))
  assert.equal(page.querySelectorAll('tr').filter(tr => tr.classList.contains('group')).length, 0)
  const all = await items(fakeEx({ all: ['i1', 'i2'] }), 'bafyrun', 'markdup', JSON.stringify([['single_end', 'bool', 'false']]), ctx())
  await settle()
  assert.equal(all.querySelectorAll('tr').filter(tr => tr.classList.contains('group')).length, 0, 'a filter that hides nothing')
})

test('the picker gives distinct values that format alike more digits, so no two read the same', async () => {
  installDom()
  const ex = fakeEx()
  const { Float: F } = await import('../src/typed.js')
  const depth = { i1: new F(89.62), i2: new F(89.64), i3: new F(12.5) }
  ex.itemBlock = async (c, i) => ({ view: { id: i, depth: depth[i] }, leaves: [], value: { id: i } })
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await settle()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  await picker.querySelector('[data-field="depth"]').fire('click')
  assert.deepEqual(picker.querySelectorAll('a[data-value]').map(a => [a.dataset.value, a.childNodes[0].textContent]),
    [['12.5', '12.5'], ['89.62', '89.62'], ['89.64', '89.64']])
})

test('a numeric-looking string id reads the same in the Sample cell and the hidden row', async () => {
  installDom()
  const ex = fakeEx()
  ex.itemBlock = async (c, i) => ({ view: { id: { i1: '1', i2: '2', i3: '3' }[i], lane: 7 }, leaves: [], value: { id: i } })
  const page = await items(ex, 'bafyrun', 'markdup', JSON.stringify([['lane', 'int', '7']]), ctx())
  await settle()
  assert.equal(rowsOf(page)[0].querySelector('.namecell').querySelector('a').textContent, '"1"')
  assert.equal(page.querySelector('[data-hidden-count]').textContent, '1 item hidden by the filter · "3" (7)')
})

test('with no conditions the bar has no Clear all', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.equal(page.querySelector('[data-clear-filters]'), null)
  assert.equal(page.querySelectorAll('[data-chip]').length, 0)
})

test('Add filter lists the loaded fields; a field lists its values as filter links with item counts; back returns', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const picker = page.querySelector('[data-filter-picker]')
  assert.equal(picker.dataset.open, 'false')
  await page.querySelector('button[data-add-filter]').fire('click')
  assert.equal(picker.dataset.open, 'true')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['id', 'single_end', 'strandedness'])
  assert.match(picker.querySelector('[data-field="single_end"]').textContent, /2 values/)
  await picker.querySelector('[data-field="single_end"]').fire('click')
  const values = picker.querySelectorAll('a[data-value]')
  assert.deepEqual(values.map(a => [a.dataset.value, decodeURIComponent(a.getAttribute('href').split('where=')[1])]), [
    ['false', '[["single_end","bool","false"]]'], ['true', '[["single_end","bool","true"]]']])
  assert.match(values[0].textContent, /2 items/)
  assert.match(values[1].textContent, /1 item$/)
  assert.ok(picker.querySelector('input[data-filter-value]'))
  await picker.querySelector('button[data-filter-back]').fire('click')
  assert.equal(picker.querySelectorAll('[data-field]').length, 3)
})

test('the field search ignores case', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const shown = (q) => { search.value = q; search.dispatchEvent(new Event('input')); return picker.querySelectorAll('[data-field]').map(b => b.dataset.field) }
  assert.deepEqual(shown('Single'), ['single_end'])
  assert.deepEqual(shown('STRAND'), ['strandedness'])
  assert.deepEqual(shown(''), ['id', 'single_end', 'strandedness'])
})

test('arrow keys move a highlight through the listed fields, wrapping at either end; Enter opens the highlighted one', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const active = () => picker.querySelectorAll('[data-field]').filter(b => b.classList.contains('active')).map(b => b.dataset.field)
  assert.deepEqual(active(), [])
  assert.equal(search.press('Enter'), false, 'Enter with nothing highlighted does nothing')
  assert.ok(picker.querySelector('[data-filter-search]'))
  assert.equal(search.press('ArrowDown'), true, 'the caret stays put')
  assert.deepEqual(active(), ['id'])
  const id = picker.querySelector('[data-field="id"]')
  assert.equal(search.getAttribute('aria-activedescendant'), id.getAttribute('id'))
  assert.equal(id.getAttribute('aria-selected'), 'true')
  search.press('ArrowDown'); search.press('ArrowDown')
  assert.deepEqual(active(), ['strandedness'])
  search.press('ArrowDown')
  assert.deepEqual(active(), ['id'], 'down from the last wraps to the first')
  search.press('ArrowUp')
  assert.deepEqual(active(), ['strandedness'], 'up from the first wraps to the last')
  search.value = 's'; search.dispatchEvent(new Event('input'))
  assert.deepEqual(active(), ['single_end'], 'typing highlights the best match')
  assert.equal(search.getAttribute('aria-activedescendant'), picker.querySelector('[data-field="single_end"]').getAttribute('id'))
  search.press('ArrowUp')
  assert.deepEqual(active(), ['strandedness'])
  search.press('Enter')
  assert.equal(picker.querySelector('input[data-filter-value]').getAttribute('aria-label'), 'a value for strandedness')
})

test('typing ranks the best match first and Enter opens it before the name is finished; clearing the box clears the highlight', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const type = (q) => { search.value = q; search.dispatchEvent(new Event('input')) }
  type('d')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['id', 'single_end', 'strandedness'], 'each has a d inside, so the given order holds')
  type('strd')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['strandedness'])
  type('')
  assert.equal(picker.querySelectorAll('[data-field]').filter(b => b.classList.contains('active')).length, 0)
  type('stra')
  assert.equal(search.press('Enter'), true)
  assert.equal(picker.querySelector('input[data-filter-value]').getAttribute('aria-label'), 'a value for strandedness')
})

test('a text field\'s values offer == and != but no range, and no stray "null"', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  await picker.querySelector('[data-field="strandedness"]').fire('click')
  assert.deepEqual(picker.querySelectorAll('[data-op]').map(b => b.dataset.op), ['=', '!='])
  assert.doesNotMatch(picker.textContent, /null/)
})

test('every field offers == and !=; != lists each value with how many loaded items lack it, and its chip reads path != value', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  await picker.querySelector('[data-field="single_end"]').fire('click')
  assert.deepEqual(picker.querySelectorAll('[data-op]').map(b => [b.dataset.op, b.textContent]), [['=', '=='], ['!=', '!=']])
  await picker.querySelector('[data-op="!="]').fire('click')
  const values = picker.querySelectorAll('a[data-value]')
  assert.deepEqual(values.map(a => [a.textContent, decodeURIComponent(a.getAttribute('href').split('where=')[1])]), [
    ['!= false1 item', '[["single_end","bool","false","!="]]'], ['!= true2 items', '[["single_end","bool","true","!="]]']])
  const filtered = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['single_end', 'bool', 'true', '!=']]), ctx())
  assert.equal(filtered.querySelector('[data-chip]').getAttribute('title'), 'single_end != true')
})

test('a range condition in the route has a chip reading path >= value, and its remove link keeps the rest', async () => {
  installDom()
  const where = [['single_end', 'bool', 'false'], ['lane', 'int', '2', '>=']]
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify(where), ctx())
  const chip = page.querySelector('[data-chip="lane"]')
  assert.match(chip.textContent, /lane ≥ 2/)
  assert.equal(chip.getAttribute('title'), 'lane >= 2')
  assert.equal(decodeURIComponent(chip.querySelector('a').getAttribute('href').split('where=')[1]), '[["single_end","bool","false"]]')
  await assert.rejects(items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['lane', 'int', '2', '~=']]), ctx()), (e) => e.code === 'bad_route')
})

test('a numeric field offers == != > >= < <=; choosing a range turns its values into range links; a text field offers == and != only', async () => {
  installDom()
  const ex = fakeEx()
  ex.itemBlock = async (c, i) => ({ view: { id: i, lane: { i1: 1, i2: 2, i3: 2 }[i] }, leaves: [], value: { id: i } })
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const picker = page.querySelector('[data-filter-picker]')
  await page.querySelector('button[data-add-filter]').fire('click')
  await picker.querySelector('[data-field="id"]').fire('click')
  assert.deepEqual(picker.querySelectorAll('[data-op]').map(b => b.dataset.op), ['=', '!='])
  await picker.querySelector('button[data-filter-back]').fire('click')
  await picker.querySelector('[data-field="lane"]').fire('click')
  const ops = picker.querySelectorAll('button[data-op]')
  assert.deepEqual(ops.map(b => b.dataset.op), ['=', '!=', '>', '>=', '<', '<='])
  assert.equal(ops[0].getAttribute('aria-pressed'), 'true')
  const where = (a) => decodeURIComponent(a.getAttribute('href').split('where=')[1])
  assert.equal(where(picker.querySelector('a[data-value="2"]')), '[["lane","int","2"]]')
  await picker.querySelector('button[data-op=">="]').fire('click')
  assert.equal(picker.querySelector('button[data-op=">="]').getAttribute('aria-pressed'), 'true')
  assert.equal(where(picker.querySelector('a[data-value="2"]')), '[["lane","int","2",">="]]')
})

test('a row\'s Pick is the shared pick button: its aria-label, then Picked', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await settle()
  const pick = rowsOf(page)[0].querySelector('[data-pick]')
  assert.equal(pick.getAttribute('aria-label'), 'Pick this item')
  assert.equal(pick.textContent, 'Pick')
  pick.click()
  assert.equal(pick.textContent, 'Picked')
  assert.equal(pick.getAttribute('aria-label'), 'Pick this item (already picked)')
  assert.equal(c.tray.size, 1)
})

// --- p1a task 5: items grid follow-ups ---

test('a filter that makes Layout constant leaves the Layout column where it was (column stability)', async () => {
  installDom()
  const ex = { ...preEx(), items: async (completion, out, where) => ({ items: where.length ? ['p1', 'p2', 'p4'] : Object.keys(PRE), collection: 'coll' }) }
  const before = await items(ex, 'bafystable', 'preprocessed', '[]', ctx())
  await settle()
  const cols = JSON.parse(before.querySelector('table').dataset.columns)
  assert.equal(cols.indexOf('meta.single_end'), 2)
  const where = JSON.stringify([['meta.single_end', 'bool', 'false']])
  const after = await items(ex, 'bafystable', 'preprocessed', where, ctx())
  await settle()
  // Filtered, the constant Strand is a column too (a filtered view never moves a promoted field to the strip); the rest keep their places.
  const afterCols = JSON.parse(after.querySelector('table').dataset.columns)
  assert.deepEqual(afterCols.filter(c => cols.includes(c)), cols)
  assert.equal(afterCols.indexOf('meta.single_end'), 2)
  assert.ok(!after.querySelector('.constants').querySelectorAll('.field').some(f => /Layout/.test(f.textContent)))
})

test('a chip with several files opens a menu of links to each file; one file links straight to it', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const row = rowsOf(page)[1]
  const menu = row.querySelector('details.filemenu')
  assert.ok(menu)
  const links = menu.querySelectorAll('a').filter(a => a.getAttribute('href').startsWith('#/content/'))
  assert.equal(links.length, 4)
  assert.match(links[0].textContent, /fastqc\.(html|zip)/)
  assert.match(links[0].textContent, /KB/)
  const one = row.querySelector('a[data-file-cell="trim"]')
  assert.match(one.getAttribute('href'), /^#\/content\//)
  assert.equal(menu.querySelector('[data-file-cell="trim"]'), null)
})

// The list's place on screen comes from the chip's rect, which the fake DOM
// lacks; menuPlace below covers that arithmetic, these tests the closing.
test('a file menu closes on Escape and on a click outside it, and listens only while open', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const menu = rowsOf(page)[0].querySelector('details.filemenu')
  const open = (m) => { m.setAttribute('open', ''); return m.fire('toggle') }
  const counts = () => ['keydown', 'click', 'scroll'].map(t => document.listenerCount(t))
  // The filter bar's own click listener drops itself on the first click here, where no node is connected.
  document.dispatch('click', { target: page })
  const idle = counts()
  await open(menu)
  assert.deepEqual(counts(), idle.map(n => n + 1))
  assert.ok(menu.hasAttribute('data-placed'))
  document.dispatch('keydown', { key: 'ArrowDown' })
  assert.ok(menu.hasAttribute('open'), 'another key leaves it open')
  document.dispatch('keydown', { key: 'Escape' })
  assert.ok(!menu.hasAttribute('open'))
  assert.ok(!menu.hasAttribute('data-placed'))
  assert.deepEqual(counts(), idle, 'its listeners go when it closes')

  await open(menu)
  document.dispatch('click', { target: menu.querySelector('[data-file-link]') })
  assert.ok(menu.hasAttribute('open'), 'a click inside keeps it open')
  document.dispatch('scroll', { target: menu.querySelector('.filemenu-list') })
  assert.ok(menu.hasAttribute('open'), 'scrolling its own list keeps it open')
  document.dispatch('click', { target: page })
  assert.ok(!menu.hasAttribute('open'))
  assert.deepEqual(counts(), idle)

  await open(menu)
  document.dispatch('scroll', { target: page.querySelector('.tbox') })
  assert.ok(!menu.hasAttribute('open'), 'scrolling the table closes it')
  assert.deepEqual(counts(), idle)
})

test('opening a file menu closes the one already open', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const [a, b] = rowsOf(page).slice(0, 2).map(r => r.querySelector('details.filemenu'))
  const open = (m) => { m.setAttribute('open', ''); return m.fire('toggle') }
  const idle = document.listenerCount('keydown')
  await open(a)
  await open(b)
  assert.ok(!a.hasAttribute('open'))
  assert.ok(b.hasAttribute('open'))
  assert.equal(document.listenerCount('keydown'), idle + 1, 'one open menu, one set of listeners')
  document.dispatch('keydown', { key: 'Escape' })
  assert.ok(!b.hasAttribute('open'))
  assert.equal(document.listenerCount('keydown'), idle)
})

test('menuPlace: below the chip when it fits, above when only the space above does, always inside the viewport', () => {
  const view = { vw: 1000, vh: 800, topLimit: 52 }
  const chip = (top, left = 100) => ({ left, top, bottom: top + 20 })
  assert.deepEqual(menuPlace(chip(100), { width: 240, height: 120 }, view), { left: 100, top: 123, maxHeight: 260 })
  // Near the bottom with room above: the list opens upward, its bottom just over the chip.
  assert.deepEqual(menuPlace(chip(700), { width: 240, height: 200 }, view), { left: 100, bottom: 103, maxHeight: 260 })
  // Near the right edge: pulled back inside, 8px from the edge.
  assert.equal(menuPlace(chip(100, 900), { width: 240, height: 120 }, view).left, 752)
  assert.equal(menuPlace(chip(100, -30), { width: 240, height: 120 }, view).left, 8)
  // Little room either way: the larger side, its height capped to fit.
  const tight = menuPlace(chip(120), { width: 240, height: 400 }, { vw: 1000, vh: 300, topLimit: 52 })
  assert.deepEqual(tight, { left: 100, top: 143, maxHeight: 149 })
})

test('the funnel link is its own flex child beside the sample link', async () => {
  installDom()
  const page = await items(preEx(), 'bafyrun', 'preprocessed', '[]', ctx())
  await settle()
  const cell = rowsOf(page)[1].querySelector('.namecell')
  const link = cell.children.find(k => k.tagName === 'A' && !k.classList.contains('cell-filter'))
  const funnel = cell.children.find(k => k.classList.contains('cell-filter'))
  assert.ok(link && funnel)
  assert.match(funnel.getAttribute('aria-label'), /^Only items where /)
})

test('a promoted column shown under a filter stays when the same output is opened unfiltered and the field is constant (memory only)', async () => {
  installDom()
  const paired = ['p1', 'p2', 'p4']
  const ex = { ...preEx(), items: async (completion, out, where) => ({ items: paired, collection: 'coll' }) }
  const first = await items(ex, 'bafymemory', 'preprocessed', JSON.stringify([['meta.single_end', 'bool', 'false']]), ctx())
  await settle()
  const cols = JSON.parse(first.querySelector('table').dataset.columns)
  assert.ok(cols.includes('meta.single_end'))
  const again = await items(ex, 'bafymemory', 'preprocessed', '[]', ctx())
  await settle()
  assert.deepEqual(JSON.parse(again.querySelector('table').dataset.columns), cols, 'no filter, Layout constant, but it was shown before')
  const fresh = await items(ex, 'bafyfresh', 'preprocessed', '[]', ctx())
  await settle()
  assert.ok(!JSON.parse(fresh.querySelector('table').dataset.columns).includes('meta.single_end'), 'another run has no memory of it')
})

test('the funnel is never absolutely positioned, so it cannot cover cell text', async () => {
  const { APP_CSS } = await import('../src/styles.js')
  const rules = APP_CSS.split('}').filter(r => r.includes('.cell-filter'))
  assert.ok(rules.length > 0)
  assert.ok(!rules.some(r => /position:\s*absolute/.test(r)))
})
