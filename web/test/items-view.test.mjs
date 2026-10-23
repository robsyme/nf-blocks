// web/test/items-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { items } from '../src/views/items.js'
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

// Every snapshot-backed method throws: the items view and its panel input may
// use only Explorer.items, block reads and runIdentity (Review Focus 1, plan P1/P2).
function fakeEx() {
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot during query 3`) }
  return {
    items: async (completion, output, where) => ({ items: where.length ? ['i1', 'i2'] : ['i1', 'i2', 'i3'], collection: 'coll' }),
    item: async (c, i) => ({ view: VIEWS[i], leaves: [{ name: `${VIEWS[i].id}.markdup.sorted.bam` }, { name: `${VIEWS[i].id}.markdup.sorted.bam.bai` }], value: valueOf(i) }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang', lid: 'lid://x' }),
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

test('rows, columns from the loaded previews, the constant line; no snapshot read but the query (Review Focus 1)', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.deepEqual(rows.map(r => r.dataset.itemResult), ['i1', 'i2', 'i3'])
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'single_end'])
  assert.match(page.querySelector('.constant').textContent, /Same for every item: strandedness reverse/)
  assert.equal(rows[0].querySelector('[data-pick]').dataset.pick, 'i1')
  assert.equal(rows[0].querySelector('[data-pick]').dataset.via, 'coll')
  assert.ok(rows[0].querySelector('[data-address]'))
  assert.equal(c.used.at(-1).count, 3)
  assert.deepEqual(c.marked, [{ run: 'bafyrun' }])
})

test('one column per file key; pills link to the block page with no download', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.fileColumns), ['bai', 'bam', 'samtools'])
  assert.deepEqual(page.querySelectorAll('th[data-file-column]').map(t => t.textContent), ['bai', 'bam', 'samtools'])
  const row = page.querySelectorAll('[data-item-result]')[0]
  const bam = row.querySelector('a[data-pill="bam"]')
  assert.equal(bam.getAttribute('href'), `#/block/${BAM.toString()}`)
  assert.equal(bam.getAttribute('download'), null)
  assert.equal(bam.getAttribute('title'), null)
  assert.equal(bam.getAttribute('aria-label'), 'bam\nWT_REP1.markdup.sorted.bam\n2.0 KB')
  assert.ok(bam.querySelector('[data-icon="file"]'))
  assert.deepEqual(row.querySelector('[data-file-cell="samtools"]').querySelectorAll('a[data-pill]').map(p => p.dataset.pill),
    ['samtools.stats', 'samtools.flagstat'])
})

test('a missing key is a dash; a cell\'s address-less slots are one dashed pill after the stored ones, not a link', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.equal(rows[2].querySelector('[data-file-cell="bai"]').textContent, '—')
  const cell = rows[0].querySelector('[data-file-cell="samtools"]')
  const empty = cell.querySelector('[data-pill-empty]')
  assert.equal(empty.tagName, 'SPAN')
  assert.equal(empty.dataset.pillEmpty, '1')
  assert.equal(empty.textContent, '1')
  assert.equal(empty.getAttribute('title'), null)
  assert.equal(empty.getAttribute('aria-label'), '1 without a stored file\nsamtools.idxstats: no file (an optional output was empty)')
  assert.equal(cell.querySelectorAll('[data-pill-empty]').length, 1)
  assert.equal(rows[1].querySelector('[data-file-cell="samtools"]').querySelector('[data-pill-empty]'), null)
})

test('keys with no stored file in any item get no column; one line above the table names them', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.equal(page.querySelector('th[data-file-column="json_info"]'), null)
  assert.equal(page.querySelector('[data-empty-columns]').textContent, 'No stored file in any item: json_info')
})

test('more than five slots shows five and +N to the item page, "-" when there is no collection (Review Focus 5)', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: ['i1'], collection: null })
  ex.item = async () => ({ view: { id: 'S' }, leaves: [], value: { id: 'S', mqc: [leaf(null, null, null, 'declined'),
    ...[...Array(9)].map((_, i) => leaf(`f${i}`, rawCid(`f${i}`))), leaf(null, null, null, 'declined')] } })
  const page = await items(ex, 'bafyrun', 'bam_qc', '[]', ctx())
  await frame(); await frame()
  const cell = page.querySelector('[data-file-cell="mqc"]')
  assert.deepEqual(cell.querySelectorAll('a[data-pill]').map(p => p.dataset.pill), ['mqc[1]', 'mqc[2]', 'mqc[3]', 'mqc[4]', 'mqc[5]'])
  assert.equal(cell.querySelector('.file-pill-more').textContent, '+4')
  assert.equal(cell.querySelector('[data-pill-empty]').dataset.pillEmpty, '2')
  assert.equal(cell.querySelector('.file-pill-more').getAttribute('href'), '#/item/-/i1')
})

test('loading and failed rows span the file columns too (Review Focus 4)', async () => {
  installDom()
  const ex = fakeEx()
  const item = ex.item
  ex.item = async (c, i) => { if (i === 'i3') throw Object.assign(new Error('gone'), { code: 'block_missing' }); return item(c, i) }
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const columns = JSON.parse(page.querySelector('table').dataset.columns).length
  const files = JSON.parse(page.querySelector('table').dataset.fileColumns).length
  const failed = page.querySelectorAll('[data-item-result]')[2]
  assert.equal(failed.querySelectorAll('td')[1].getAttribute('colspan'), String(columns + files))
})

test('a value is a link that adds its filter; the chips bar removes one', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['single_end', 'bool', 'false']]), ctx())
  await frame(); await frame()
  const chip = page.querySelector('[data-chip="single_end"]')
  assert.match(chip.textContent, /single_end = false/)
  assert.equal(chip.querySelector('a').getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  const value = page.querySelectorAll('[data-item-result]')[0].querySelectorAll('a').find(a => a.textContent === 'WT_REP1')
  assert.match(decodeURIComponent(value.getAttribute('href')), /\["id","string","WT_REP1"\]/)
})

test('checkboxes report to the panel, and "Pick these" puts the checked rows in the tray with the collection', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  const box = page.querySelectorAll('[data-item-result]')[1].querySelector('input')
  box.checked = false
  await box.fire('change')
  assert.equal(c.used.at(-1).checked, 2)
  c.used.at(-1).pickChecked()
  assert.deepEqual(c.tray.entries().map(e => [e.address, e.via]), [['i1', ['coll']], ['i3', ['coll']]])
})

test('no items says so, and still reports the empty output to the panel', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: [], collection: null })
  const c = ctx()
  const page = await items(ex, 'bafyrun', 'markdup', JSON.stringify([['id', 'string', 'nope']]), c)
  assert.match(page.textContent, /No items match/)
  assert.equal(c.used.at(-1).count, 0)
})

test('the items view records which run and output its collection is, for the panel', async () => {
  installDom()
  const c = ctx()
  await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  assert.deepEqual(c.sources.get('coll'), { completion: 'bafyrun', output: 'markdup' })
})

test('file columns come first, after the checkbox and before the metadata columns', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(page.querySelectorAll('th').map(t => t.dataset.fileColumn ?? t.textContent), ['', 'bai', 'bam', 'samtools', 'id', 'single_end', ''])
  const cells = page.querySelectorAll('[data-item-result]')[0].querySelectorAll('td')
  assert.deepEqual(cells.slice(1, 4).map(td => td.dataset.fileCell), ['bai', 'bam', 'samtools'])
  assert.equal(cells[4].textContent, 'WT_REP1')
})

test('hovering or focusing a pill opens the page\'s own tooltip at once; leaving closes it', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const tip = page.querySelector('[data-pill-tip]')
  assert.equal(tip.dataset.open, 'false')
  const rows = page.querySelectorAll('[data-item-result]')
  const bam = rows[0].querySelector('a[data-pill="bam"]')
  await bam.fire('mouseenter')
  assert.equal(tip.dataset.open, 'true')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent), ['bam', 'WT_REP1.markdup.sorted.bam', '2.0 KB'])
  await bam.fire('mouseleave')
  assert.equal(tip.dataset.open, 'false')
  const empty = rows[0].querySelector('[data-pill-empty]')
  await empty.fire('focus')
  assert.equal(tip.dataset.open, 'true')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent),
    ['1 without a stored file', 'samtools.idxstats: no file (an optional output was empty)'])
  await empty.fire('blur')
  assert.equal(tip.dataset.open, 'false')
})
