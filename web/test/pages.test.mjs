// web/test/pages.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { selection } from '../src/views/selection.js'
import { pipeline } from '../src/views/pipeline.js'
import { home } from '../src/views/home.js'

const ctx = () => {
  const c = { tray: new Tray(null), used: [], marked: [], rerender: () => {}, trayChanged: () => {},
    write: { available: false, reason: 'read only', served: true, hrefFor: (h) => h, here: false } }
  c.use = (v) => c.used.push(v)
  c.mark = (m) => c.marked.push(m)
  return c
}

test('the item page: a de-duplicated title, sizes, downloads, produced-by, and the panel told its output', async () => {
  installDom()
  const ex = {
    item: async () => ({ view: { id: 'WT_REP1', meta: { id: 'WT_REP1' }, single_end: false }, state: claimState([]),
      leaves: [{ name: 'WT_REP1.bam', size: 412_000_000, address: 'bafybam' }, { name: 'in.fq', size: null, address: null, reason: 'never_published' }] }),
    producersOf: async () => [{ collection_cid: 'coll', completion_cid: 'bafyrun' }, { collection_cid: 'coll2', completion_cid: 'bafyold' }],
    selectionsHolding: async () => [],
    runLabel: async (c) => ({ run_name: c === 'coll' ? 'high_jang' : 'happy_rubens', output: 'markdup', completion: c === 'coll' ? 'bafyrun' : 'bafyold' }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang' }),
    blocks: { urlFor: (a) => `blocks/${a}` },
  }
  const c = ctx()
  const page = await item(ex, 'coll', 'bafyitem', c)
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'WT_REP1')
  assert.match(page.textContent, /412 MB/)
  assert.equal(page.querySelector('a[download]').getAttribute('href'), 'blocks/bafybam')
  assert.match(page.textContent, /not stored here \(such as an input file\)/)
  assert.equal(page.querySelector('[data-pick]').textContent, 'Add to picked')
  assert.deepEqual(c.used.at(-1), { completion: 'bafyrun', output: 'markdup', where: [], count: null })
  assert.ok(page.querySelector('[data-fold="storage"]'))
})

test('the Selection view leaves the call and samplesheets to the panel (Review Focus 5)', async () => {
  installDom()
  const st = { names: ['treated BAMs'], nameClaims: [], current: [], nameConflicted: false, deletion: 'none', deletionClaims: [] }
  const ex = { selection: async () => ({ state: st, members: [{ kind: 'item', address: 'i1', via: ['coll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', item: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null }
  const c = ctx()
  const page = await selection(ex, 'bafysel', c)
  assert.equal(page.querySelectorAll('[data-snippet]').length, 0)
  assert.equal(page.querySelectorAll('[data-samplesheet]').length, 0)
  assert.deepEqual(c.used.at(-1), { selection: 'bafysel', members: 1, names: ['treated BAMs'] })
  assert.equal(page.getAttribute('data-selection-view'), 'bafysel')
})

test('the pipeline page: health in words, rows keep [data-run], the left column expands it', async () => {
  installDom()
  const ex = {
    runPage: async () => ({ rows: [{ completion_cid: 'bafyrun', pipeline: 'p', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' }],
      hidden: 0, first: 1, last: 1, total: 1, prev: null, next: null }),
    completionOf: async () => ({ anomalies: { declined: 3, unjoined: 1 } }),
    stale: [],
  }
  const c = ctx()
  const page = await pipeline(ex, 'p', 0, c)
  await frame()
  assert.equal(page.querySelector('[data-run]').dataset.status, 'succeeded')
  assert.equal(page.querySelector('[data-anomalies-for="bafyrun"]').textContent, '1 published file that is in no output')
  assert.deepEqual(c.marked, [{ pipeline: 'p', expand: true }])
})

test('home links each pipeline to its latest good run', async () => {
  installDom()
  const ex = { pipelines: async () => [{ pipeline: 'p', runs: 2, latest: '2026-09-30T21:10:38.345Z' }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runIdentity: async () => ({ run_name: 'high_jang' }) }
  const page = await home(ex, ctx())
  await frame()
  assert.ok(page.querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/bafyrun' && a.textContent === 'high_jang'))
})

test('home: a failing runIdentity shows ? and nothing is left unhandled', async () => {
  installDom()
  const seen = []
  const on = (e) => seen.push(e)
  process.on('unhandledRejection', on)
  const ex = { pipelines: async () => [{ pipeline: 'p', runs: 1, latest: null }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runIdentity: async () => { throw new Error('x') } }
  const page = await home(ex, ctx())
  await frame(); await new Promise(r => setTimeout(r, 20))
  process.off('unhandledRejection', on)
  assert.equal(seen.length, 0)
  assert.ok(page.textContent.includes('?'))
  assert.ok(!page.textContent.includes('…'))
})

const itemEx = (over) => ({
  item: async () => ({ view: { id: 'A', meta: { id: 'A' } }, state: claimState([]), leaves: [{ name: 'a.bam', size: 1, address: 'bafybam' }] }),
  producersOf: async () => [{ collection_cid: 'c1', completion_cid: 'r1' }, { collection_cid: 'c2', completion_cid: 'r2' }],
  selectionsHolding: async () => [], runLabel: async (c) => ({ run_name: c === 'c1' ? 'one' : 'two', output: 'o', completion: c === 'c1' ? 'r1' : 'r2' }),
  runIdentity: async () => ({}), blocks: { urlFor: (a) => a }, ...over })

test('item with no collection lists every producing run and the query note', async () => {
  installDom()
  const page = await item(itemEx({}), '-', 'bafyitem', ctx())
  await frame()
  assert.match(page.textContent, /one/)
  assert.match(page.textContent, /two/)
  assert.match(page.textContent, /Picked by a query across runs\./)
})

test('item whose runLabel rejects lists its producers without the query note', async () => {
  installDom()
  const page = await item(itemEx({ runLabel: async () => { throw new Error('no') } }), 'c1', 'bafyitem', ctx())
  await frame()
  assert.ok(page.querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/r1'))
  assert.ok(!page.textContent.includes('Picked by a query'))
})
