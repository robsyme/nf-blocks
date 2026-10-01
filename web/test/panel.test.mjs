// web/test/panel.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { createPanel, panelState, useLabel } from '../src/panel.js'

const ITEMS = { completion: 'bafyrun', output: 'markdup', where: [], count: 5, checked: 5 }

test('panelState: every row of spec §6.1', () => {
  assert.deepEqual(panelState({ route: 'run', view: null, picked: 0 }), { state: 'none' })
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'items', view: { ...ITEMS, where: [['single_end', 'bool', 'false']], count: 3, checked: 3 }, picked: 0 }).state, 'filtered')
  assert.equal(panelState({ route: 'item', view: { completion: 'c', output: 'o', where: [], count: null }, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'collection', view: { completion: 'c', output: 'o', where: [], count: 9 }, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'collection', view: { completion: null, output: 'o', where: [], count: 9 }, picked: 0 }).state, 'none')
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 2 }).state, 'picked')
  assert.equal(panelState({ route: 'compose', view: null, picked: 1 }).state, 'picked')
  assert.deepEqual(panelState({ route: 'selection', view: { selection: 'bafysel', members: 3 }, picked: 2 }), { state: 'saved', selection: 'bafysel', members: 3 })
})

test('unchecking rows keeps the call for the whole output and offers to pick the checked ones', () => {
  const s = panelState({ route: 'items', view: { ...ITEMS, checked: 3 }, picked: 0 })
  assert.equal(s.state, 'whole')
  assert.deepEqual(s.partial, { checked: 3, count: 5 })
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 0 }).partial, null)
})

function world({ lid = 'lid://hash-R4', latest = 'bafyrun', write = { available: true } } = {}) {
  installDom()
  const el = document.createElement('div')
  const tray = new Tray(null)
  const asked = []
  const ex = {
    runIdentity: async (c) => { asked.push(['runIdentity', c]); return { completion: c, pipeline: 'nf-core/rnaseq', run_name: c === 'bafyrun' ? 'high_jang' : 'newer', lid } },
    latestSuccessfulRun: async (p) => { asked.push(['latest', p]); return latest },
    runLabel: () => { throw new Error('the panel must not read the snapshot') },
  }
  const sources = new Map([['coll', { completion: 'bafyrun', output: 'markdup' }]])
  const ctx = { tray, sources, write: { served: true, ...write, hrefFor: (h) => h, run: async () => {} }, trayChanged: () => panel.draw() }
  const panel = createPanel({ el, ex, ctx })
  return { el, tray, ex, ctx, panel, asked }
}

test('whole: the run/output call, with no snapshot query (P2)', async () => {
  const { el, panel, asked } = world()
  await panel.draw({ route: 'items', view: ITEMS })
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'whole')
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(run: 'lid://hash-R4', output: 'markdup')")
  assert.match(el.textContent, /All 5 items of markdup/)
  assert.equal(el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.deepEqual(asked, [['runIdentity', 'bafyrun']])
})

test('filtered: where: from the chips, and data-where for the driver', async () => {
  const { el, panel } = world()
  const where = [['single_end', 'bool', 'false']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 3, checked: 3 } })
  const call = el.querySelector('[data-snippet="untyped"]')
  assert.equal(call.textContent, "channel.fromStore(run: 'lid://hash-R4', output: 'markdup', where: [single_end: false])")
  assert.deepEqual(JSON.parse(call.dataset.where), where)
  assert.match(el.textContent, /3 items of markdup, where single_end = false/)
})

test('latest good run: asked only when chosen; a newer run is named; none keeps "this run" (P3)', async () => {
  const w = world({ latest: 'bafynewer' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  assert.ok(!w.asked.some(([k]) => k === 'latest'))
  const latestButton = w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run')
  await latestButton.click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'latest')
  assert.equal(w.el.querySelector('[data-snippet="untyped"]').textContent,
    "channel.fromStore(run: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup')")
  assert.match(w.el.textContent, /latest good run is now newer/)
  const none = world({ latest: null })
  await none.panel.draw({ route: 'items', view: ITEMS })
  await none.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run').click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(none.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.match(none.el.textContent, /no good run/)
})

test('the run switch goes back to "this run" for another output', async () => {
  const w = world({ latest: 'bafyrun' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  await w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run').click()
  await new Promise(r => setTimeout(r, 0))
  await w.panel.draw({ route: 'items', view: { ...ITEMS, output: 'quant' } })
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
})

test('M of N checked offers "Pick these M"', async () => {
  const { el, panel } = world()
  let picked = 0
  await panel.draw({ route: 'items', view: { ...ITEMS, checked: 3, pickChecked: () => { picked++ } } })
  const button = el.querySelector('#pick-checked')
  assert.equal(button.textContent, 'Pick these 3')
  await button.click()
  assert.equal(picked, 1)
})

test('picked: grouped by output and run, each entry keeps [data-tray-entry], and the save controls keep their ids', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['coll'] }, { address: 'sel', kind: 'selection' }])
  await panel.draw({ route: 'compose', view: null })
  await new Promise(r => setTimeout(r, 0))
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  assert.equal(el.querySelectorAll('[data-tray-entry]').length, 3)
  assert.match(el.textContent, /markdup · high_jang · 2/)
  const entries = el.querySelectorAll('[data-tray-entry]')
  assert.ok(entries.every(e => e.dataset.kind), 'kind kept')
  const folds = el.querySelectorAll('details')
  assert.equal(folds.length, 2, 'one closed fold per group')
  assert.ok(folds.every(f => !f.open))
  for (const id of ['#compose-name', '#compose-save', '#write-status']) assert.ok(el.querySelector(id), id)
  assert.ok(!el.querySelector('[data-snippet]'))
})

test('picked: removing a group, and Clear', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['other'] }])
  await panel.draw({ route: 'compose', view: null })
  await el.querySelectorAll('button').find(b => b.getAttribute('aria-label') === 'remove these').click()
  assert.equal(tray.size, 1)
  await el.querySelectorAll('button').find(b => b.textContent === 'Clear').click()
  assert.equal(tray.size, 0)
})

test('picked while writing is unavailable says why and offers no Save', async () => {
  const { el, panel, tray } = world({ write: { available: false, reason: 'read only' } })
  tray.add({ address: 'i1', via: ['coll'] })
  await panel.draw({ route: 'compose', view: null })
  assert.equal(el.querySelector('[data-unavailable]').textContent, 'read only')
  assert.ok(el.querySelector('#compose-save').disabled)
})

test('saved: the Selection call and its samplesheets', async () => {
  const { el, panel } = world()
  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 4, names: ['treated BAMs'] } })
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(selection: 'bafysel')")
  assert.ok(el.querySelector('[data-samplesheet="csv"]'))
  assert.match(el.textContent, /treated BAMs/)
})

test('the panel never carries [data-error], [data-run] or [data-selection] (P14, Review Focus 4)', async () => {
  const { el, panel, ex } = world()
  ex.runIdentity = async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) }
  await panel.draw({ route: 'items', view: ITEMS })
  assert.ok(!el.querySelector('[data-error]'))
  assert.match(el.textContent, /gone/)
  assert.ok(!el.querySelector('[data-run]') && !el.querySelector('[data-selection]'))
})

test('a slower draw that finishes after a newer one does not overwrite it', async () => {
  const { el, panel, ex } = world()
  let release
  ex.runIdentity = (c) => (c === 'slow' ? new Promise(r => { release = () => r({ pipeline: 'p', lid: 'lid://slow' }) }) : Promise.resolve({ pipeline: 'p', lid: 'lid://fast' }))
  const slow = panel.draw({ route: 'items', view: { ...ITEMS, completion: 'slow' } })
  await panel.draw({ route: 'items', view: { ...ITEMS, completion: 'fast' } })
  release()
  await slow
  assert.match(el.querySelector('[data-snippet="untyped"]').textContent, /lid:\/\/fast/)
})

const tick = () => new Promise(r => setTimeout(r, 0))

test('a redraw during a save keeps the same status node when the tray stays picked (i)', async () => {
  const w = world()
  w.tray.add({ address: 'i1', via: ['coll'] })
  await w.panel.draw({ route: 'compose', view: null })
  const status = w.el.querySelector('#write-status')
  w.ctx.write.run = async (st) => {
    w.ctx.trayChanged()
    st.textContent = 'Saved, but the page could not refresh'
  }
  await w.el.querySelector('#compose-save').click()
  await tick()
  assert.equal(w.el.querySelector('#write-status'), status)
  assert.match(w.el.textContent, /Saved, but the page could not refresh/)
})

test('a further draw keeps the held-elsewhere prompt and the prefilled name (ii)', async () => {
  const w = world()
  w.tray.add({ address: 'i1', via: ['coll'] })
  w.tray.toMembers = () => []
  w.ctx.write.run = async (st, fn) => { await fn() }
  w.ctx.write.writer = { selection: async () => ({ exists: true, here: false, address: 'bafysel', names: ['treated BAMs'], name_claims: ['c1'] }) }
  await w.panel.draw({ route: 'compose', view: null })
  await w.el.querySelector('#compose-save').click()
  await tick()
  assert.ok(w.el.querySelector('[data-held-elsewhere]'))
  assert.equal(w.el.querySelector('#compose-name').value, 'treated BAMs')
  await w.panel.draw()
  assert.ok(w.el.querySelector('[data-held-elsewhere]'))
  assert.ok(w.el.querySelector('#compose-copy'))
  assert.equal(w.el.querySelector('#compose-name').value, 'treated BAMs')
})

test('a draw requested during a save renders the latest input once it settles (iii)', async () => {
  const w = world()
  w.tray.add({ address: 'i1', via: ['coll'] })
  await w.panel.draw({ route: 'compose', view: null })
  w.ctx.write.run = async () => {
    w.tray.clear()
    await w.panel.draw({ route: 'items', view: { ...ITEMS, output: 'quant' } })
    assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  }
  await w.el.querySelector('#compose-save').click()
  await tick()
  assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'whole')
  assert.match(w.el.textContent, /All 5 items of quant/)
})

test('a stale "latest good run" click does not change another output', async () => {
  const w = world({ latest: 'bafynewer' })
  let release
  w.ex.latestSuccessfulRun = () => new Promise(r => { release = () => r('bafynewer') })
  await w.panel.draw({ route: 'items', view: ITEMS })
  const click = w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run').click()
  await w.panel.draw({ route: 'items', view: { ...ITEMS, output: 'quant' } })
  release()
  await click
  await tick()
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.ok(!/latest good run is now/.test(w.el.textContent))
})

test('a save that could not refresh keeps its message after the tray empties', async () => {
  const saved = globalThis.location
  globalThis.location = { hash: '#/compose' }
  try {
    const w = world()
    w.tray.add({ address: 'i1', via: ['coll'] })
    await w.panel.draw({ route: 'compose', view: null })
    w.ctx.write.run = async (st) => {
      w.tray.clear()
      w.ctx.trayChanged()
      const p = document.createElement('p')
      p.setAttribute('data-refresh-failed', '')
      p.textContent = 'Saved, but the page could not refresh'
      st.replaceChildren(p)
    }
    await w.el.querySelector('#compose-save').click()
    await tick()
    assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'none')
    assert.match(w.el.textContent, /Saved, but the page could not refresh/)
    assert.ok(w.el.querySelector('[data-refresh-failed]'))
  } finally { globalThis.location = saved }
})

test('a save that navigated empties the stale status', async () => {
  const saved = globalThis.location
  globalThis.location = { hash: '#/compose' }
  try {
    const w = world()
    w.tray.add({ address: 'i1', via: ['coll'] })
    await w.panel.draw({ route: 'compose', view: null })
    w.ctx.write.run = async (st) => {
      st.textContent = 'Saving...'
      w.tray.clear()
      globalThis.location.hash = '#/selection/bafysel'
    }
    await w.el.querySelector('#compose-save').click()
    await tick()
    assert.doesNotMatch(w.el.textContent, /Saving/)
  } finally { globalThis.location = saved }
})

test('a kept status goes when the hash changes, and stays on the same hash', async () => {
  const saved = globalThis.location
  globalThis.location = { hash: '#/compose' }
  try {
    const w = world()
    w.tray.add({ address: 'i1', via: ['coll'] })
    await w.panel.draw({ route: 'compose', view: null })
    w.ctx.write.run = async (st) => { st.textContent = 'Could not save: boom' }
    await w.el.querySelector('#compose-save').click()
    await tick()
    await w.panel.draw()
    assert.match(w.el.textContent, /Could not save: boom/)
    globalThis.location.hash = '#/items/other'
    await w.panel.draw({ route: 'items', view: ITEMS })
    assert.doesNotMatch(w.el.textContent, /Could not save/)
    assert.equal(w.el.querySelector('#write-status').childNodes.length, 0)
  } finally { globalThis.location = saved }
})

test('when write.run returns without running the attempt, saving clears and the panel draws', async () => {
  const w = world()
  w.tray.add({ address: 'i1', via: ['coll'] })
  await w.panel.draw({ route: 'compose', view: null })
  w.ctx.write.run = async () => {}
  await w.el.querySelector('#compose-save').click()
  await tick()
  await w.panel.draw({ route: 'items', view: ITEMS })
  assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  w.tray.clear()
  await w.panel.draw({ route: 'items', view: ITEMS })
  assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'whole')
})

test('a draw during a save that navigated renders at once, before the attempt settles', async () => {
  const saved = globalThis.location
  globalThis.location = { hash: '#/compose' }
  try {
    const w = world()
    w.tray.add({ address: 'i1', via: ['coll'] })
    await w.panel.draw({ route: 'compose', view: null })
    let seen
    w.ctx.write.run = async () => {
      w.tray.clear()
      globalThis.location.hash = '#/selection/bafysel'
      await w.panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 1 } })
      seen = w.el.querySelector('[data-panel-state]').dataset.panelState
    }
    await w.el.querySelector('#compose-save').click()
    await tick()
    assert.equal(seen, 'saved')
  } finally { globalThis.location = saved }
})

test('onState gets the computed state after each draw', async () => {
  const w = world()
  const seen = []
  const panel = createPanel({ el: w.el, ex: w.ex, ctx: w.ctx, onState: (s) => seen.push(s.state) })
  await panel.draw({ route: 'items', view: ITEMS })
  assert.deepEqual(seen, ['whole'])
})

test('useLabel: Use · N from the count, the picked list or the members', () => {
  const tray = { size: 4 }
  assert.equal(useLabel({ state: 'whole', count: 5 }, tray), 'Use · 5')
  assert.equal(useLabel({ state: 'filtered', count: 3 }, tray), 'Use · 3')
  assert.equal(useLabel({ state: 'whole', count: null }, tray), 'Use')
  assert.equal(useLabel({ state: 'picked' }, tray), 'Use · 4')
  assert.equal(useLabel({ state: 'saved', members: 2 }, tray), 'Use · 2')
  assert.equal(useLabel({ state: 'saved', members: null }, tray), 'Use')
  assert.equal(useLabel({ state: 'none' }, tray), 'Use')
})

test('picked: a group is named from ctx.sources and never reads the snapshot; unknown ones say "an output"', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['unseen'] }])
  await panel.draw({ route: 'compose', view: null })
  assert.match(el.textContent, /markdup · high_jang · 1/)
  assert.match(el.textContent, /an output · 1/)
})

test('where repeating a path: no call, a plain reason, and the rows to pick instead', async () => {
  const { el, panel } = world()
  let picked = 0
  const where = [['sample', 'string', 'A'], ['sample', 'string', 'B']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 4, checked: 4, pickChecked: () => { picked++ } } })
  assert.ok(!el.querySelector('[data-snippet]'))
  assert.match(el.textContent, /holds one value per key/)
  const button = el.querySelector('#pick-checked')
  assert.equal(button.textContent, 'Pick these 4')
  await button.click()
  assert.equal(picked, 1)
})

test('where repeating a path with no matches offers nothing to pick', async () => {
  const { el, panel } = world()
  const where = [['sample', 'string', 'A'], ['sample', 'string', 'B']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 0, checked: 0 } })
  assert.ok(!el.querySelector('[data-snippet]'))
  assert.ok(!el.querySelector('#pick-checked'))
})

test('where repeating a path with some rows unchecked offers the checked ones, once', async () => {
  const { el, panel } = world()
  const where = [['sample', 'string', 'A'], ['sample', 'string', 'B']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 4, checked: 2, pickChecked: () => {} } })
  assert.equal(el.querySelectorAll('button').filter(b => /^Pick these/.test(b.textContent)).length, 1)
  assert.equal(el.querySelector('#pick-checked').textContent, 'Pick these 2')
  assert.match(el.textContent, /2 of 4 checked/)
})
