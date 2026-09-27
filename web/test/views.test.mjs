// views.js needs a DOM (`document`) to build its nodes, which this Node test
// runner does not have, so only its pure helpers are unit-tested here (the
// page minors of ticket 11, "Add all" and run labels); the rendering itself
// is covered by the Gate's browser tier. The item rows are also drawn over
// the small fake DOM in dom.mjs, to check the pills and the row contract.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { compose, copyOutcome, copyText, itemRows, pickAll, runLabelText, undoNote } from '../src/views.js'
import { Previews } from '../src/previews.js'
import { frame, installDom } from './dom.mjs'
import { Tray, UNSAVED_NOTE } from '../src/tray.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'

test('copyText: a via-less member copies its bare address; a via\'d one copies cas://<via>/<address>', () => {
  assert.equal(copyText('item1', '-'), 'item1')
  assert.equal(copyText('item1', 'coll1'), 'cas://coll1/item1')
})

test('copyOutcome: "Copied" on a successful write, "Copy failed" on a rejection or a missing clipboard', async () => {
  assert.equal(await copyOutcome(undefined, 'x'), 'Copy failed')
  assert.equal(await copyOutcome({ writeText: async () => {} }, 'x'), 'Copied')
  assert.equal(await copyOutcome({ writeText: async () => { throw new Error('nope') } }, 'x'), 'Copy failed')
})

test('undoNote: writing unavailable everywhere gives the reason, and only the reason', () => {
  assert.deepEqual(undoNote(false, false, 'This explore server has no writable member.', 'lab', 'http://h/m/lab/#/selections?deleted=1'),
    { kind: 'unavailable', reason: 'This explore server has no writable member.' })
  // Whatever `here` is, unavailable wins: there is nowhere to send the link.
  assert.deepEqual(undoNote(false, true, 'no token', 'lab', 'href'), { kind: 'unavailable', reason: 'no token' })
})

test('undoNote: available but not viewing the writable member names it and links there', () => {
  assert.deepEqual(undoNote(true, false, null, 'lab', 'http://h/m/lab/#/selections?deleted=1'),
    { kind: 'elsewhere', writable: 'lab', href: 'http://h/m/lab/#/selections?deleted=1' })
})

test('undoNote: available and already viewing the writable member shows no note', () => {
  assert.equal(undoNote(true, true, null, 'lab', 'href'), null)
})

function countingStorage() {
  const m = new Map()
  const s = { writes: 0, getItem: k => m.get(k) ?? null, setItem: (k, v) => { s.writes++; m.set(k, String(v)) }, removeItem: k => m.delete(k) }
  return s
}

test('Add all on a 15,000-item collection puts every item in the tray with its collection, fetches no block, and survives a reload (Review Focus 4)', async () => {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    "INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ('run1', 'big', 'R1', 'succeeded', 0, '2026-09-01T00:00:00.000Z')",
    "INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run1', 'aligned')",
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 15000)
     INSERT INTO collection_item(collection_cid, item_cid) SELECT 'coll', printf('item%05d', i) FROM n`])
  const asked = []
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: async (url) => { asked.push(String(url)); return new Response('', { status: 404 }) } }),
    listFn: async () => ({ names: [], readable: true }) })
  ex.item = () => { throw new Error('Add all must not fetch a preview') }
  const storage = countingStorage()
  const tray = new Tray(storage)
  assert.equal(await pickAll(ex, tray, { items: null, collectionCid: 'coll' }), 15000)
  assert.equal(tray.size, 15000)
  assert.deepEqual(asked, [])
  assert.equal(storage.writes, 1, 'one write to storage, not one per item')
  const reloaded = new Tray(storage)
  assert.equal(reloaded.size, 15000)
  assert.deepEqual(reloaded.entries()[14999], { address: 'item15000', kind: 'item', via: ['coll'] })
})

test('Add all on query results adds the results it is given, each with the run\'s collection, without listing the collection', async () => {
  const ex = { allItems: async () => { throw new Error('query results already are every item') } }
  const tray = new Tray(null)
  assert.equal(await pickAll(ex, tray, { items: ['i2', 'i1'], collectionCid: 'coll' }), 2)
  assert.deepEqual(tray.entries(), [{ address: 'i1', kind: 'item', via: ['coll'] }, { address: 'i2', kind: 'item', via: ['coll'] }])
  const bare = new Tray(null)
  await pickAll(ex, bare, { items: ['i1'], collectionCid: null })
  assert.deepEqual(bare.entries()[0].via, [])
})

test('a run is named `<run_name> / <output>`, or shown by its collection when this member does not know it (decision 11)', () => {
  assert.equal(runLabelText({ run_name: 'cold', output: 'aligned', completion: 'c' }, 'coll'), 'cold / aligned')
  assert.equal(runLabelText({ run_name: null, output: 'aligned', completion: 'c' }, 'coll'), 'unnamed run / aligned')
  assert.equal(runLabelText(null, 'coll'), 'coll')
})

// A model with just what the rows ask: each item's block (a Meta Map, one
// file) and the run a collection came from.
function rowModel({ allItems = null } = {}) {
  const asked = []
  return {
    asked,
    item: async (collectionCid, itemCid) => {
      asked.push(itemCid)
      return { view: { sample: `S-${itemCid}`, lane: 2, ok: true }, leaves: [{ name: `${itemCid}.bam` }] }
    },
    runLabel: async (c) => (c === 'coll' ? { run_name: 'cold', output: 'aligned', completion: 'run1' } : null),
    allItems: allItems ?? (async () => { throw new Error('not asked') }),
  }
}

function rowCtx(tray = new Tray(null)) {
  const ctx = { tray, changed: [], write: { available: false, reason: 'read only' }, rerender: () => {} }
  ctx.trayChanged = () => ctx.changed.push(tray.size)
  return ctx
}

test('a query result row: label, file chips, pills linking to query 3 with the pair added, the pick carrying its collection (decision 9, 12)', async () => {
  installDom()
  const ex = rowModel()
  const ctx = rowCtx()
  const where = [['lane', 'int', '2']]
  const node = itemRows(ex, ctx, { items: ['i1', 'i2'], collectionCid: 'coll', completion: 'run1', output: 'aligned', where,
    previews: new Previews(ex), results: true })
  await frame()
  assert.deepEqual(node.querySelectorAll('[data-item-result]').map(li => li.dataset.itemResult), ['i1', 'i2'])
  const row = node.querySelector('[data-item-result=i1]')
  assert.equal(row.querySelector('.row-label').textContent, 'S-i1 · 2 · true')
  assert.deepEqual(row.querySelectorAll('.chip').map(c => c.textContent), ['i1.bam'])
  const pills = row.querySelector('[data-preview-for=i1]')
  assert.deepEqual(pills.querySelectorAll('.pill-key').map(k => k.textContent), ['lane', 'ok', 'sample'])
  const sample = pills.querySelectorAll('a.pill-val').find(a => a.textContent === 'S-i1')
  assert.equal(sample.getAttribute('href'),
    `#/items/run1/aligned?where=${encodeURIComponent(JSON.stringify([['lane', 'int', '2'], ['sample', 'string', 'S-i1']]))}`)
  assert.ok(pills.querySelector('.pv-true'), 'a bool value is coloured by its value')
  assert.equal(row.querySelector('[data-pick]').dataset.via, 'coll')
  assert.equal(row.querySelector('.row-cid').querySelector('a').getAttribute('href'), '#/item/coll/i1')
  const all = node.querySelector('[data-pick-all]')
  assert.equal(all.dataset.via, 'coll')
  assert.equal(all.dataset.count, '2')
  assert.equal(all.textContent, 'Add all 2 to the tray')
})

test('an item with no Meta Map has an empty pills container and is labelled by its files', async () => {
  installDom()
  const ex = { ...rowModel(), item: async () => ({ view: null, leaves: [{ name: 'A.bam' }] }) }
  const node = itemRows(ex, rowCtx(), { items: ['i1'], collectionCid: 'coll', previews: new Previews(ex) })
  await frame()
  assert.equal(node.querySelector('[data-preview-for=i1]').children.length, 0)
  assert.equal(node.querySelector('.row-label').textContent, 'A.bam')
})

test('Add all on a collection page reads every item, adds them with the collection, then updates the tray count once', async () => {
  installDom()
  const every = Array.from({ length: 5 }, (_, i) => `i${i}`)
  const ex = rowModel({ allItems: async (c) => { assert.equal(c, 'coll'); return every } })
  const ctx = rowCtx()
  const node = itemRows(ex, ctx, { items: every.slice(0, 2), total: 5, collectionCid: 'coll', previews: new Previews(ex) })
  const all = node.querySelector('[data-pick-all]')
  assert.equal(all.dataset.count, '5')
  await all.click()
  assert.equal(ctx.tray.size, 5)
  assert.deepEqual(ctx.tray.entries().map(e => e.via), every.map(() => ['coll']))
  assert.deepEqual(ctx.changed, [5], 'trayChanged after the items are in, so #tray[data-count] shows them')
  assert.ok(node.querySelectorAll('[data-pick]').every(b => b.disabled && b.textContent === 'In the tray'))
})

test('Add all into a tray the browser cannot save says so next to the count added', async () => {
  installDom()
  const every = ['i1', 'i2', 'i3']
  const ex = rowModel()
  const full = { getItem: () => null, setItem: () => { throw new Error('QuotaExceededError') } }
  const ctx = rowCtx(new Tray(full))
  const node = itemRows(ex, ctx, { items: every, collectionCid: 'coll', previews: new Previews(ex) })
  await node.querySelector('[data-pick-all]').click()
  assert.equal(ctx.tray.size, 3)
  const status = node.querySelector('.row-actions').querySelectorAll('span').find(s => s.textContent.startsWith('Added'))
  assert.equal(status.textContent, `Added 3 to the tray. ${UNSAVED_NOTE}`)
})

test('Add checked adds only the checked rows, with the collection as via', async () => {
  installDom()
  const ex = rowModel()
  const ctx = rowCtx()
  const node = itemRows(ex, ctx, { items: ['i1', 'i2', 'i3'], collectionCid: 'coll', previews: new Previews(ex) })
  const boxes = node.querySelectorAll('input[type=checkbox]')
  boxes[0].checked = true; await boxes[0].fire('change')
  boxes[2].checked = true; await boxes[2].fire('change')
  const add = node.querySelectorAll('button').find(b => b.textContent === 'Add checked (2)')
  await add.click()
  assert.deepEqual(ctx.tray.entries(), [{ address: 'i1', kind: 'item', via: ['coll'] }, { address: 'i3', kind: 'item', via: ['coll'] }])
  assert.deepEqual(ctx.changed, [2])
})

test('the compose tray counts only item entries toward the preview cap, so a Selection entry does not push an item past it', async () => {
  installDom()
  const ex = rowModel()
  const tray = new Tray(null)
  tray.add({ address: 'a-selection', kind: 'selection' })
  const items = Array.from({ length: 100 }, (_, i) => `i${String(i).padStart(3, '0')}`)
  tray.addMany(items.map(address => ({ address, via: ['coll'] })))
  const node = compose(ex, rowCtx(tray))
  assert.equal(node.querySelectorAll('[data-tray-entry]').length, 101)
  const shows = node.querySelectorAll('button').filter(b => b.textContent === 'show details')
  assert.equal(shows.length, 0, 'the 100 items are the first 100 item rows')
  await frame()
  assert.equal(ex.asked.length, 100)
  const via = node.querySelector('[data-tray-entry=i000]').querySelector('.row-via')
  assert.match(via.textContent, /^picked from cold \/ aligned/)
})
