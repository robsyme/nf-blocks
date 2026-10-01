// views.js needs a DOM (`document`) to build its nodes, which this Node test
// runner does not have, so only its pure helpers are unit-tested here (the
// page minors of ticket 11, "Add all" and run labels); the rendering itself
// is covered by the Gate's browser tier. The item rows are also drawn over
// the small fake DOM in dom.mjs, to check the pills and the row contract.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { collection, content, copyOutcome, copyText, item, itemRows, pickAll, run, runLabelText, selections, undoNote } from '../src/views/index.js'
import { Previews } from '../src/previews.js'
import { frame, installDom } from './dom.mjs'
import { Tray, UNSAVED_NOTE } from '../src/tray.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { claimState } from '../src/claims.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { memberWithIndexedCollection, memberWithUnjoinedRun } from './fixture.mjs'

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
  // hrefFor is defined even when writing is unavailable (app.js's own write object always has it): retentionPanel
  // builds its unavailable-note href unconditionally, the way actions() already did.
  const ctx = { tray, changed: [], write: { available: false, reason: 'read only', hrefFor: (h) => h }, rerender: () => {} }
  ctx.trayChanged = () => ctx.changed.push(tray.size)
  return ctx
}

/** Installs the fake DOM, then awaits a view call already in flight: safe because
 * every view awaits its own model call before its first `h()`, so `document` is
 * always installed before that happens. */
function render(promise) { installDom(); return promise }

/** A ctx for a view under test that touches no write path (Task 4's collection/run tests). */
const ctx = () => rowCtx()

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

test('a run that published nothing says so under Outputs, with no snippet toggle and no empty list', async () => {
  installDom()
  const completion = { status: 'succeeded', possibly_incomplete: false, finished_at: '2026-09-27T02:26:05.935Z', error: null,
    anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 } }
  const ex = { run: async () => ({ row: { run_name: 'extravagant_boltzmann', pipeline: 'custom.nf', nf_run_hash: 'abc' }, completion, collections: [],
    state: claimState([]) }) }
  const node = await run(ex, 'bafyrun', ctx())
  const empty = node.querySelector('[data-no-outputs]')
  assert.ok(empty, 'an empty-state line')
  assert.equal(empty.textContent, 'This run published no outputs.')
  assert.equal(node.querySelectorAll('[data-snippet-mode]').length, 0)
  assert.equal(node.querySelectorAll('ul').length, 0)
})

test('a collection with an Output Index File offers it as a download', async () => {
  const { ex, ids } = await memberWithIndexedCollection({ indexName: 'index.json', indexPath: 'multiqc/index.json' })
  const page = await render(collection(ex, ids.collection, 0, ctx()))
  const p = page.querySelector('[data-output-index]')
  assert.ok(p, 'the index paragraph is shown')
  assert.equal(p.getAttribute('data-output-index'), ids.indexFile)
  const a = p.querySelector('a[download]')
  assert.equal(a.getAttribute('download'), 'index.json')
  // The member base, not the page: under nf-blocks:explore the member is at m/<alias>/ (final review I3).
  assert.equal(a.getAttribute('href'), `http://h/m/lab/blocks/${ids.indexFile.slice(-2)}/${ids.indexFile}`)
  assert.match(p.textContent, /Nextflow's index/)
})

test('a collection whose index was never written says so', async () => {
  const { ex, ids } = await memberWithIndexedCollection({ indexName: 'index.csv', indexPath: 'bad/index.csv', neverPublished: true })
  const page = await render(collection(ex, ids.collection, 0, ctx()))
  assert.equal(page.querySelector('[data-output-index]'), null)
  assert.equal(page.querySelector('[data-output-index-missing]').getAttribute('data-output-index-missing'), 'bad/index.csv')
})

test('the run page counts unjoined publishes', async () => {
  const { ex, ids } = await memberWithUnjoinedRun(2)
  const page = await render(run(ex, ids.completion, ctx()))
  assert.match(page.textContent, /unjoined 2/)
})

// Task 11: release/restore on a run, pin/unpin on any subject (ticket 21
// answer 6). The fake explorer's loaders return `state` (claimState of the
// claims a test hands it), as model.js's real loaders now do; the fake
// writer records each write as [method, subject, ...args], mirroring the
// Selection-actions write path (ctx.write.run/writer, actions() at views.js).
const RUN = 'bafyrun1'
const COLLECTION = 'bafycoll1'
const ITEM = 'bafyitem1'
const CONTENT = 'bafycontent1'
const C1 = 'bafyclaim1'
const C2 = 'bafyclaim2'

function fakeWriter(calls) {
  return {
    release: (subject, retainClaims) => { calls.push(['release', subject, retainClaims]); return Promise.resolve({}) },
    restore: (subject, retainClaims) => { calls.push(['restore', subject, retainClaims]); return Promise.resolve({}) },
    pin: (subject, note) => { calls.push(['pin', subject, note]); return Promise.resolve({}) },
    unpin: (subject, pinClaim) => { calls.push(['unpin', subject, pinClaim]); return Promise.resolve({}) },
  }
}

/**
 * A fake explorer and ctx for the retention tests. `runClaims`,
 * `collectionClaims`, `itemClaims` and `contentClaims` become each subject's
 * `state` (claimState); `writable: false` puts the page outside the writable
 * member, as the Selection view's `actions()` reads `ctx.write.here`.
 */
function fixture({ runClaims = [], collectionClaims = [], itemClaims = [], contentClaims = [], writable = true } = {}) {
  const calls = []
  const completion = { status: 'succeeded', possibly_incomplete: false, finished_at: '2026-09-01T00:00:00.000Z', error: null,
    anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 } }
  const ex = {
    run: async () => ({ row: { run_name: 'cold', pipeline: 'demo', nf_run_hash: null }, completion, collections: [],
      state: claimState(runClaims) }),
    collection: async () => ({ cid: COLLECTION, output: 'aligned', completion: null, index: null, items: [], total: 0,
      state: claimState(collectionClaims) }),
    item: async () => ({ collection: COLLECTION, cid: ITEM, value: null, view: null, leaves: [], state: claimState(itemClaims) }),
    selectionsHolding: async () => [],
    runLabel: async () => null,
    producersOf: async () => [],
    claimStates: async (cids) => new Map(cids.map(c => [c, claimState(contentClaims)])),
  }
  const ctx = { tray: new Tray(null), rerender: () => {}, progress: () => {},
    // hrefFor is not the identity, so a test can tell the note links through it (fix round 1: shared unavailableNote).
    write: { available: true, here: writable, writable: 'lab', hrefFor: (h) => `http://h/m/lab${h}`, writer: fakeWriter(calls),
      run: async (status, fn) => { await fn() } } }
  ctx.trayChanged = () => {}
  return { ex, ctx, calls }
}

/** Flushes the microtasks a write's onclick starts: the fake writer resolves with no real timer, so a macrotask tick is enough. */
const settle = () => new Promise(resolve => setTimeout(resolve, 0))

test('a released run shows the badge and Restore content; restore supersedes the release', async () => {
  installDom()
  const { ex, ctx, calls } = fixture({ runClaims: [{ cid: C1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [] }] })
  const node = await run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="content-released"]'))
  assert.equal(node.querySelector('#release'), null)
  node.querySelector('#restore').click()
  await settle()
  assert.deepEqual(calls, [['restore', RUN, [C1]]])
})

test('a run not released offers Release content with its current retain claims', async () => {
  installDom()
  const { ex, ctx, calls } = fixture({ runClaims: [] })
  const node = await run(ex, RUN, ctx)
  node.querySelector('#release').click()
  await settle()
  assert.deepEqual(calls, [['release', RUN, []]])
})

test('a hidden run that is pinned says so', async () => {
  installDom()
  const { ex, ctx } = fixture({ runClaims: [
    { cid: C1, verb: 'delete', attribute: null, value: null, supersedes: [] },
    { cid: C2, verb: 'add', attribute: 'pin', value: 'paper', supersedes: [] }] })
  const node = await run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="hidden-but-pinned"]'))
  assert.equal(node.querySelector('[data-pin]').textContent.includes('paper'), true)
})

test('pin asks for a note, and each pin can be removed on its own', async () => {
  installDom()
  const { ex, ctx, calls } = fixture({ itemClaims: [{ cid: C2, verb: 'add', attribute: 'pin', value: 'paper', supersedes: [] }] })
  const node = await item(ex, COLLECTION, ITEM, ctx)
  const button = node.querySelector('#pin')
  assert.equal(button.disabled, true)
  const note = node.querySelector('#pin-note')
  note.value = 'figure 3'; note.dispatchEvent(new Event('input'))
  assert.equal(button.disabled, false)
  button.click()
  node.querySelector(`button[data-unpin="${C2}"]`).click()
  await settle()
  assert.deepEqual(calls, [['pin', ITEM, 'figure 3'], ['unpin', ITEM, C2]])
})

test('outside the writable member the badges show and no write is offered', async () => {
  installDom()
  const { ex, ctx } = fixture({ runClaims: [{ cid: C1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [] }], writable: false })
  const node = await run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="content-released"]'))
  assert.equal(node.querySelector('#restore'), null)
  assert.ok(node.querySelector('[data-unavailable]'))
})

// Fix round 1 (review): the run page's unavailable note is the same shared
// helper the Selection view's actions() uses, so it names its own actions
// and links to itself in the writable member, the way actions() always has.
test('a run page outside the writable member shows the note with a link to the same route in the writable member', async () => {
  installDom()
  const { ex, ctx } = fixture({ writable: false })
  const node = await run(ex, RUN, ctx)
  const note = node.querySelector('[data-unavailable]')
  assert.equal(note.textContent, 'Pin, release and restore write to the writable member, lab. Open this run there.')
  const a = note.querySelector('a')
  assert.equal(a.textContent, 'Open this run there')
  assert.equal(a.getAttribute('href'), ctx.write.hrefFor(`#/run/${RUN}`))
})

test('a collection page pins the same way, below its heading', async () => {
  installDom()
  const { ex, ctx, calls } = fixture({ collectionClaims: [{ cid: C1, verb: 'add', attribute: 'pin', value: 'kept', supersedes: [] }] })
  const node = await collection(ex, COLLECTION, 0, ctx)
  assert.ok(node.querySelector('[data-badge="pinned"]'))
  node.querySelector('#pin-note').value = 'why'
  node.querySelector('#pin-note').dispatchEvent(new Event('input'))
  node.querySelector('#pin').click()
  await settle()
  assert.deepEqual(calls, [['pin', COLLECTION, 'why']])
})

test('a content page shows a pin and can be unpinned', async () => {
  installDom()
  const { ex, ctx, calls } = fixture({ contentClaims: [{ cid: C1, verb: 'add', attribute: 'pin', value: 'figure 1', supersedes: [] }] })
  const node = await content(ex, CONTENT, ctx)
  assert.ok(node.querySelector('[data-badge="pinned"]'))
  node.querySelector(`button[data-unpin="${C1}"]`).click()
  await settle()
  assert.deepEqual(calls, [['unpin', CONTENT, C1]])
})

test('selections: a row with conflicting names or conflicting deletion says so', async () => {
  installDom()
  const state = (over) => ({ names: ['a'], deletion: 'current', nameConflicted: false, ...over })
  const ex = { selectionPage: async () => ({ hiddenCount: 0, first: 1, last: 2, total: 2, prev: null, next: null, rows: [
    { cid: 'sel1', source: 'x', state: state({ nameConflicted: true }) },
    { cid: 'sel2', source: 'x', state: state({ deletion: 'conflicted' }) }] }) }
  const node = await selections(ex, {}, {})
  assert.match(node.textContent, /\(names in conflict\)/)
  assert.match(node.textContent, /\(deletion in conflict\)/)
})
