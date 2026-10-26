// web/test/panel.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { createPanel, panelState, useLabel } from '../src/panel.js'
import { APP_CSS } from '../src/styles.js'

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

test('panelState: the run page with samples is the hand-off, even with something picked', () => {
  const samples = { table: {}, selected: ['A'], where: [], manual: new Map() }
  const s = panelState({ route: 'run', view: { completion: 'bafyrun', output: null, samples }, picked: 0 })
  assert.equal(s.state, 'samples')
  assert.equal(s.completion, 'bafyrun')
  assert.equal(s.samples, samples)
  // On the Samples tab the hand-off wins over a non-empty picked list; it links the list instead.
  assert.equal(panelState({ route: 'run', view: { completion: 'bafyrun', output: null, samples }, picked: 1 }).state, 'samples')
  assert.equal(panelState({ route: 'run', view: null, picked: 1 }).state, 'picked')
  assert.equal(panelState({ route: 'items', view: { completion: 'bafyrun', output: null, samples }, picked: 0 }).state, 'none')
  // The toggle counts what the tab's "Use N samples" button counts: the ticked samples.
  assert.equal(useLabel(s, { size: 5 }), 'Use · 1 sample')
  assert.equal(useLabel({ state: 'samples', samples: { ...samples, selected: ['A', 'B'] } }, { size: 0 }), 'Use · 2 samples')
  assert.equal(useLabel({ state: 'samples', samples: { ...samples, selected: Array(1200).fill('x') } }, { size: 0 }), 'Use · 1,200 samples')
  assert.equal(useLabel({ state: 'samples', samples: { ...samples, selected: [] } }, { size: 0 }), 'Use · 0 samples')
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

test('whole: the run/output call names the run by its RunCompletion address, with no snapshot query (P2)', async () => {
  const { el, panel, asked } = world()
  await panel.draw({ route: 'items', view: ITEMS })
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'whole')
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(run: 'cas://bafyrun', output: 'markdup')")
  assert.match(el.textContent, /All 5 items of markdup/)
  assert.equal(el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.deepEqual(asked, [['runIdentity', 'bafyrun']])
  // The kit's rail (mockups 03/04/08): two seg-toggles, then the code block and Copy snippet.
  const toggles = el.querySelectorAll('.seg-toggle')
  assert.deepEqual(toggles.map(t => t.querySelectorAll('button').map(b => b.textContent)), [['This run', 'Latest good run'], ['Untyped', 'Typed']])
  assert.ok(el.querySelector('pre.code'))
  assert.ok(el.querySelectorAll('button').some(b => b.textContent === 'Copy snippet'))
  assert.ok(el.querySelector('h3'), 'the rail section heading')
})

test('filtered: where: from the chips, and data-where for the driver', async () => {
  const { el, panel } = world()
  const where = [['single_end', 'bool', 'false']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 3, checked: 3 } })
  const call = el.querySelector('[data-snippet="untyped"]')
  assert.equal(call.textContent, "channel.fromStore(run: 'cas://bafyrun', output: 'markdup', where: [single_end: false])")
  assert.deepEqual(JSON.parse(call.dataset.where), where)
  assert.match(el.textContent, /3 items of markdup, where single_end = false/)
})

test('latest good run: asked only when chosen; a newer run is named; none keeps "this run" (P3)', async () => {
  const w = world({ latest: 'bafynewer' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  assert.ok(!w.asked.some(([k]) => k === 'latest'))
  const latestButton = w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'Latest good run')
  await latestButton.click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'latest')
  assert.equal(w.el.querySelector('[data-snippet="untyped"]').textContent,
    "channel.fromStore(run: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup')")
  assert.match(w.el.textContent, /latest good run is now newer/)
  const none = world({ latest: null })
  await none.panel.draw({ route: 'items', view: ITEMS })
  await none.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'Latest good run').click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(none.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.match(none.el.textContent, /no good run/)
})

test('the run switch goes back to "this run" for another output', async () => {
  const w = world({ latest: 'bafyrun' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  await w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'Latest good run').click()
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
  await panel.draw({ route: 'items', view: null })
  await new Promise(r => setTimeout(r, 0))
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  assert.equal(el.querySelectorAll('[data-tray-entry]').length, 3)
  assert.match(el.textContent, /high_jang \/ markdup · 2/)
  const entries = el.querySelectorAll('[data-tray-entry]')
  assert.ok(entries.every(e => e.dataset.kind), 'kind kept')
  const folds = el.querySelectorAll('details')
  assert.equal(folds.length, 2, 'one fold per group')
  assert.ok(folds.every(f => f.hasAttribute('open')), 'a short list is open, so the names show')
  for (const id of ['#compose-name', '#compose-save', '#write-status']) assert.ok(el.querySelector(id), id)
  assert.ok(!el.querySelector('[data-snippet]'))
})

test('picked: entries are named by sample id with the short CID beside it, and grouped by run / output', async () => {
  const { el, panel, tray, ex } = world()
  ex.itemBlock = async (c, i) => ({ view: { id: i === 'bafyitemone1234567890' ? 'WT_REP2' : 'RAP1_REP1', single_end: true }, leaves: [{ name: `${i}.bw` }], value: null })
  tray.addMany([{ address: 'bafyitemone1234567890', via: ['coll'] }, { address: 'bafyitemtwo1234567890', via: ['coll'] }])
  await panel.draw({ route: 'items', view: null })
  await new Promise(r => setTimeout(r, 20))
  const [one, two] = el.querySelectorAll('[data-tray-entry]')
  assert.equal(one.querySelector('.pname').textContent, 'WT_REP2')
  assert.equal(two.querySelector('.pname').textContent, 'RAP1_REP1')
  assert.equal(one.querySelector('code').textContent, 'bafy…567890')
  assert.equal(one.querySelector('code').getAttribute('title'), 'bafyitemone1234567890')
  assert.match(el.textContent, /high_jang \/ markdup · 2/)
})

test('on #/compose the panel counts the picked items and leaves the list and Save to the main pane', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }])
  await panel.draw({ route: 'compose', view: null })
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  assert.match(el.textContent, /Picked: 1 item/)
  assert.ok(!el.querySelector('#compose-save'))
  assert.ok(!el.querySelector('[data-tray-entry]'))
})

test('picked: removing a group, and Clear', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['other'] }])
  await panel.draw({ route: 'items', view: null })
  await el.querySelectorAll('button').find(b => b.getAttribute('aria-label') === 'remove these').click()
  assert.equal(tray.size, 1)
  await el.querySelectorAll('button').find(b => b.textContent === 'Clear').click()
  assert.equal(tray.size, 0)
})

test('picked while writing is unavailable says why and offers no Save', async () => {
  const { el, panel, tray } = world({ write: { available: false, reason: 'read only' } })
  tray.add({ address: 'i1', via: ['coll'] })
  await panel.draw({ route: 'items', view: null })
  assert.equal(el.querySelector('[data-unavailable]').textContent, 'read only')
  assert.ok(el.querySelector('#compose-save').disabled)
})

test('saved: the Selection call and its samplesheets', async () => {
  const { el, panel } = world()
  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 4, names: ['treated BAMs'] } })
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(selection: 'bafysel')")
  assert.ok(!el.querySelector('[data-samplesheet]'), 'the samplesheet export moved to the Selection page')
  assert.match(el.textContent, /treated BAMs/)
  assert.match(el.textContent, /4 items/)
  assert.doesNotMatch(el.textContent, /member/)
})

test('saved: a Claims section under Use in a workflow, as mockup 08 draws it', async () => {
  const { el, panel } = world()
  const at = new Date(Date.now() - 3 * 3600_000).toISOString()
  const claims = [{ cid: 'bafyname', assertedBy: 'ada.lovelace@example.org', verb: 'named', value: 'wigshop', at, conflict: false }]
  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 3, names: ['wigshop'], claims, pinned: false, store: 'roblab' } })
  const secs = el.querySelectorAll('.sec')
  assert.deepEqual(secs.map(s => s.querySelector('h3').textContent), ['Use in a workflow', 'Claims'])
  const rows = secs[1].querySelectorAll('[data-rail-claim]')
  assert.deepEqual(rows.map(r => r.dataset.railClaim), ['bafyname'])
  assert.equal(rows[0].getAttribute('data-claim'), null, 'rail rows carry no [data-claim]')
  assert.ok(rows[0].classList.contains('row'))
  assert.equal(rows[0].querySelector('.age').textContent, '3h')
  assert.match(rows[0].textContent, /named/)
  assert.equal(secs[1].querySelector('li'), null, 'no bullet markers')
  assert.equal(secs[1].querySelector('ul'), null)
  assert.equal(secs[1].querySelector('p.muted').textContent, 'No pins in roblab.')

  const old = { cid: 'bafyold', assertedBy: 'gate', verb: 'named', value: 'before', at, conflict: false, superseded: true }
  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 3, names: ['wigshop'], claims: [...claims, old], pinned: true, store: 'roblab' } })
  const [now, was] = el.querySelectorAll('[data-rail-claim]')
  assert.equal(now.dataset.superseded, undefined)
  assert.equal(was.dataset.superseded, '')
  assert.equal(was.getAttribute('data-claim'), null)
  assert.ok(was.querySelector('.claim').classList.contains('old'))
  assert.equal(was.querySelector('.claim').getAttribute('title'), 'Superseded')
  assert.ok(was.querySelector('.av').getAttribute('title') === 'gate' && was.querySelector('.age'))
  assert.match(APP_CSS, /\.claim\.old\s*\{[^}]*color: var\(--ink-3\)/)
  assert.doesNotMatch(APP_CSS, /\.claim\.old\s*\{[^}]*opacity/, 'faded text is too faint in dark mode')

  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 3, names: [], claims: [], pinned: true, store: 'roblab' } })
  assert.equal(el.querySelectorAll('.sec').length, 1, 'no claims and pinned: nothing to say')

  await panel.draw({ route: 'items', view: ITEMS })
  assert.equal(el.querySelector('[data-rail-claims]'), null, 'only a Selection has a Claims section')
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
  assert.match(el.querySelector('[data-snippet="untyped"]').textContent, /cas:\/\/fast/)
})

test('a run with no lineage ID still gets a call for this run, by its address', async () => {
  const { el, panel } = world({ lid: null })
  await panel.draw({ route: 'items', view: ITEMS })
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(run: 'cas://bafyrun', output: 'markdup')")
  assert.doesNotMatch(el.textContent, /lineage ID/)
})

const tick = () => new Promise(r => setTimeout(r, 0))

test('a redraw during a save keeps the same status node when the tray stays picked (i)', async () => {
  const w = world()
  w.tray.add({ address: 'i1', via: ['coll'] })
  await w.panel.draw({ route: 'items', view: null })
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
  await w.panel.draw({ route: 'items', view: null })
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
  await w.panel.draw({ route: 'items', view: null })
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
  const click = w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'Latest good run').click()
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
    await w.panel.draw({ route: 'items', view: null })
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
    await w.panel.draw({ route: 'items', view: null })
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
    await w.panel.draw({ route: 'items', view: null })
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
  await w.panel.draw({ route: 'items', view: null })
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
    await w.panel.draw({ route: 'items', view: null })
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
  await panel.draw({ route: 'items', view: null })
  assert.match(el.textContent, /high_jang \/ markdup · 1/)
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

test('filtered with a range: the call keys it as path and operator, and the text says the operator', async () => {
  const { el, panel } = world()
  const where = [['depth', 'float', '30.0', '>='], ['single_end', 'bool', 'false']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 2, checked: 2 } })
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent,
    "channel.fromStore(run: 'cas://bafyrun', output: 'markdup', where: ['depth >=': 30.0d, single_end: false])")
  // The text formats the number (pairs.valueText, P0 polish Task 5); the snippet keeps 30.0d.
  assert.match(el.textContent, /2 items of markdup, where depth >= 30, single_end = false/)
})

test('picked: a picked Selection is "Selection" with its short CID, and the panel reads neither selection() nor the snapshot', async () => {
  const { el, panel, tray, ex } = world()
  ex.selection = () => { throw new Error('the panel must not read selection()') }
  ex.db = { query: () => { throw new Error('the panel must not query the snapshot') } }
  ex.claimStates = () => { throw new Error('the panel must not read claims') }
  tray.addMany([{ address: 'bafyselection1234567890', kind: 'selection' }, { address: 'i1', via: ['coll'] }])
  await panel.draw({ route: 'items', view: null })
  await new Promise(r => setTimeout(r, 20))
  const sel = el.querySelector('[data-tray-entry=bafyselection1234567890]')
  assert.equal(sel.querySelector('.pname').textContent, 'Selection')
  assert.equal(sel.querySelector('code').textContent, 'bafy…567890')
})

test('picked: after a reload the group is named by its output and run blocks alone, never the snapshot', async () => {
  const { el, panel, tray, ex, ctx } = world()
  ctx.sources = new Map()
  ex.collectionLabel = async () => ({ output: 'bigwig', run_name: 'distraught_murdock', pipeline: 'nf-core/rnaseq' })
  tray.addMany([{ address: 'i1', via: ['coll'] }])
  await panel.draw({ route: 'items', view: null })
  assert.match(el.textContent, /distraught_murdock \/ bigwig · 1/)
  assert.doesNotMatch(el.textContent, /an output/)
})
