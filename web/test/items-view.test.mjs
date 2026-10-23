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
// A markdup-shaped record; i3 has no bai, and json_info is a declined optional output on every item.
const valueOf = (i) => {
  const id = VIEWS[i].id
  const value = { id, bam: leaf(`${id}.markdup.sorted.bam`, BAM, 2048), bai: leaf(`${id}.markdup.sorted.bam.bai`, rawCid(`bai-${i}`)),
    samtools: { stats: leaf(`${id}.stats`, rawCid(`stats-${i}`)), flagstat: leaf(`${id}.flagstat`, rawCid(`flagstat-${i}`)) },
    json_info: leaf(null, null, null, 'declined') }
  if (i === 'i3') delete value.bai
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

test('one column per file key after the metadata columns; pills link to the block page with no download', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.fileColumns), ['bai', 'bam', 'json_info', 'samtools'])
  assert.deepEqual(page.querySelectorAll('th[data-file-column]').map(t => t.textContent), ['bai', 'bam', 'json_info', 'samtools'])
  const row = page.querySelectorAll('[data-item-result]')[0]
  const bam = row.querySelector('a[data-pill="bam"]')
  assert.equal(bam.getAttribute('href'), `#/block/${BAM.toString()}`)
  assert.equal(bam.getAttribute('download'), null)
  assert.equal(bam.getAttribute('title'), 'bam\nWT_REP1.markdup.sorted.bam\n2.0 KB')
  assert.equal(bam.getAttribute('aria-label'), bam.getAttribute('title'))
  assert.ok(bam.querySelector('[data-icon="file"]'))
  assert.deepEqual(row.querySelector('[data-file-cell="samtools"]').querySelectorAll('[data-pill]').map(p => p.dataset.pill),
    ['samtools.stats', 'samtools.flagstat'])
})

test('a missing key is a dash; an address-less slot is a pill that is not a link', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.equal(rows[2].querySelector('[data-file-cell="bai"]').textContent, '—')
  const empty = rows[0].querySelector('[data-pill="json_info"]')
  assert.equal(empty.tagName, 'SPAN')
  assert.equal(empty.getAttribute('title'), 'json_info\nno file (an optional output was empty)')
})

test('more than five slots shows five and +N to the item page, "-" when there is no collection (Review Focus 5)', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: ['i1'], collection: null })
  ex.item = async () => ({ view: { id: 'S' }, leaves: [], value: { id: 'S', mqc: [...Array(9)].map((_, i) => leaf(`f${i}`, rawCid(`f${i}`))) } })
  const page = await items(ex, 'bafyrun', 'bam_qc', '[]', ctx())
  await frame(); await frame()
  const cell = page.querySelector('[data-file-cell="mqc"]')
  assert.equal(cell.querySelectorAll('[data-pill]').length, 5)
  assert.equal(cell.querySelector('.pill-more').textContent, '+4')
  assert.equal(cell.querySelector('.pill-more').getAttribute('href'), '#/item/-/i1')
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
