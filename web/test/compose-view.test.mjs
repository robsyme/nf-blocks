// web/test/compose-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { compose } from '../src/views/compose.js'

const A = 'bafyreibqmh7sof3fghtxiehvghvimlhkmnjr6sg2l6g4l2a6l2f2crpqju'
const B = 'bafyreiewhmblm2hek3ikrou5ks3mqlqokmrnp7iqa444vtdmh24bti75j4'
const C = 'bafyreiey2d5hmr7b56ggm2kw7kg2p4i3anv3cpjonwy5lbwl35zzn6n3ki'

function world({ write = {}, runs = {} } = {}) {
  installDom()
  const tray = new Tray(null)
  const calls = []
  const files = (cid, size) => ({ files: new Map([[cid, { size, name: `${cid}.bw` }]]), dirs: [], partial: false })
  const ex = {
    pipelines: async () => { throw new Error('#/compose must not read the Pipelines table') },
    itemBlock: async (c, i) => ({ view: { id: { [A]: 'WT_REP2', [B]: 'RAP1_REP1', [C]: 'SAMPLE_C' }[i] }, leaves: [{ name: `${i}.bw` }], value: null }),
    itemFileMap: async (i) => files(i, { [A]: 377_000, [B]: 341_000, [C]: 100_000 }[i]),
    runLabel: () => { throw new Error('#/compose names its groups from blocks, not the snapshot') },
    collectionLabel: async (c) => runs[c] ?? { output: 'bigwig', run_name: 'distraught_murdock', pipeline: 'nf-core/rnaseq' },
  }
  const ctx = { tray, trayChanged: () => calls.push('trayChanged'),
    write: { available: true, here: true, served: true, hrefFor: (h) => h, writer: {}, run: async (status, fn) => { await fn() }, ...write } }
  return { ex, ctx, tray, calls }
}

test('compose: a Picked heading with the count and size, groups named run / output, entries by sample id', async () => {
  const { ex, ctx, tray } = world()
  tray.addMany([{ address: A, via: ['coll'] }, { address: B, via: ['coll'] }, { address: C, via: ['coll2'] }])
  const page = await compose(ex, ctx)
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'Picked')
  assert.match(page.querySelector('.subline').textContent, /^3 items · 818 KB$/)
  const headings = page.querySelectorAll('h2').map(e => e.textContent)
  assert.ok(headings.every(t => t === 'distraught_murdock / bigwig' || t === 'Save as Selection'), headings.join('|'))
  assert.equal(headings.filter(t => t === 'distraught_murdock / bigwig').length, 2)
  const entries = page.querySelectorAll('[data-tray-entry]')
  assert.equal(entries.length, 3)
  assert.ok(entries.every(e => e.dataset.kind === 'item'))
  const first = entries.find(e => e.dataset.trayEntry === A)
  assert.equal(first.querySelector('.pname').textContent, 'WT_REP2')
  assert.equal(first.querySelector('code').textContent, 'bafy…crpqju')
  assert.ok(first.querySelectorAll('button').some(b => b.textContent === 'Remove'))
})

test('compose: never renders the Pipelines table, and keeps the save controls and their ids', async () => {
  const { ex, ctx, tray } = world()
  tray.add({ address: A, via: ['coll'] })
  const page = await compose(ex, ctx)
  assert.ok(!page.querySelectorAll('h1, h2').some(e => /Pipelines/.test(e.textContent)))
  for (const id of ['#compose-name', '#compose-save', '#write-status']) assert.ok(page.querySelector(id), id)
  assert.ok(!page.querySelector('#compose-save').disabled)
  assert.ok(page.querySelectorAll('button').some(b => b.textContent === 'Clear'))
})

test('compose: Remove takes one entry out and Clear empties the list', async () => {
  const { ex, ctx, tray, calls } = world()
  tray.addMany([{ address: A, via: ['coll'] }, { address: B, via: ['coll'] }])
  const page = await compose(ex, ctx)
  await page.querySelector(`[data-tray-entry=${A}]`).querySelectorAll('button').find(b => b.textContent === 'Remove').click()
  assert.equal(tray.size, 1)
  assert.ok(!page.querySelector(`[data-tray-entry=${A}]`))
  assert.ok(calls.includes('trayChanged'))
  await page.querySelectorAll('button').find(b => b.textContent === 'Clear').click()
  assert.equal(tray.size, 0)
  assert.match(page.textContent, /Nothing picked yet/)
})

test('compose: writing unavailable says why and disables Save', async () => {
  const { ex, ctx, tray } = world({ write: { available: false, reason: 'read only' } })
  tray.add({ address: A, via: ['coll'] })
  const page = await compose(ex, ctx)
  assert.equal(page.querySelector('[data-unavailable]').textContent, 'read only')
  assert.ok(page.querySelector('#compose-save').disabled)
})

test('compose: Save runs the same dry run as the panel and says when the Selection exists here', async () => {
  const writer = { selection: async () => ({ exists: true, here: true, address: 'bafysel', names: ['wigshop'], deletion: 'none', deletion_claims: [] }) }
  const { ex, ctx, tray } = world({ write: { writer } })
  ctx.write.run = async (status, fn) => { await fn() }
  tray.addMany([{ address: A, via: [B] }])
  const page = await compose(ex, ctx)
  await page.querySelector('#compose-save').click()
  const exists = page.querySelector('[data-exists]')
  assert.equal(exists.dataset.exists, 'bafysel')
  assert.match(exists.textContent, /already exists as wigshop/)
})

test('compose: a picked Selection is named by its name and counted as a Selection', async () => {
  const { ex, ctx, tray } = world()
  ex.selection = async () => ({ state: { names: ['lagos'] } })
  tray.addMany([{ address: A, via: ['coll'] }, { address: 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce', kind: 'selection' }])
  const page = await compose(ex, ctx)
  await frame()
  const sel = page.querySelector('[data-tray-entry=bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce]')
  assert.equal(sel.dataset.kind, 'selection')
  assert.equal(sel.querySelector('.pname').textContent, 'lagos')
})

test('compose: Save does nothing once the tray is empty, and the list redraws when a save clears it', async () => {
  let dry = 0
  const writer = { selection: async () => { dry++; return { exists: false } }, put: async () => ({}) }
  const { ex, ctx, tray } = world({ write: { writer } })
  tray.addMany([{ address: A, via: [B] }])
  const page = await compose(ex, ctx)
  const save = page.querySelector('#compose-save')
  tray.clear()
  await save.click()
  assert.equal(dry, 0, 'no empty dry run')
})

test('compose: a Selection held in another member offers #compose-copy with the name prefilled', async () => {
  const writer = { selection: async () => ({ exists: true, here: false, address: 'bafysel', names: ['from-shared'], name_claims: ['bafyn'], deletion: 'none', deletion_claims: [] }) }
  const { ex, ctx, tray } = world({ write: { writer } })
  tray.addMany([{ address: A, via: [B] }])
  const page = await compose(ex, ctx)
  await page.querySelector('#compose-save').click()
  const elsewhere = page.querySelector('[data-held-elsewhere]')
  assert.equal(elsewhere.dataset.heldElsewhere, 'bafysel')
  assert.deepEqual(JSON.parse(elsewhere.dataset.names), ['from-shared'])
  assert.ok(page.querySelector('#compose-copy'))
  assert.equal(page.querySelector('#compose-name').value, 'from-shared')
})
