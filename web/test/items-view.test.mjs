// web/test/items-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { items } from '../src/views/items.js'

const view = (id, singleEnd) => ({ id, single_end: singleEnd, strandedness: 'reverse' })
const VIEWS = { i1: view('WT_REP1', false), i2: view('WT_REP2', false), i3: view('RAP1_REP1', true) }

// Every snapshot-backed method throws: the items view and its panel input may
// use only Explorer.items, block reads and runIdentity (Review Focus 1, plan P1/P2).
function fakeEx() {
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot during query 3`) }
  return {
    items: async (completion, output, where) => ({ items: where.length ? ['i1', 'i2'] : ['i1', 'i2', 'i3'], collection: 'coll' }),
    item: async (c, i) => ({ view: VIEWS[i], leaves: [{ name: `${VIEWS[i].id}.markdup.sorted.bam` }, { name: `${VIEWS[i].id}.markdup.sorted.bam.bai` }] }),
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

test('rows, columns from the loaded previews, the constant line, file chips; no snapshot read but the query (Review Focus 1)', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.deepEqual(rows.map(r => r.dataset.itemResult), ['i1', 'i2', 'i3'])
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'single_end'])
  assert.match(page.querySelector('.constant').textContent, /Same for every item: strandedness reverse/)
  assert.match(rows[0].textContent, /\.bam/)
  assert.ok(!rows[0].textContent.includes('WT_REP1.markdup.sorted'))
  assert.equal(rows[0].querySelector('[data-pick]').dataset.pick, 'i1')
  assert.equal(rows[0].querySelector('[data-pick]').dataset.via, 'coll')
  assert.equal(c.used.at(-1).count, 3)
  assert.deepEqual(c.marked, [{ run: 'bafyrun' }])
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
