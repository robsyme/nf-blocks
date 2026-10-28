// The item page's Lineage panel over a fake Explorer: a group per upstream run, a line per output and Selection, a row per input.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, strayTexts } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { resetFolds } from '../src/folds.js'
import { createPresence } from '../src/presence.js'
import { shortCid } from '../src/icons.js'
import { FAKE_STORAGE } from './helpers.mjs'

const PEOPLE = 'bafyreiabfmoro4h5zrbnzltdc5u4bguxoptvjq2pkn32fkczz3woqtcyju'
const PEOPLE_RUN = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'
const PEOPLE_DEV = 'bafyreicooyl2pyo3ux3ieqj6kyhacb2zsxhijhfpiixexmu4sn3qxuzrty'
const CONSUMER_INTROS = 'bafyreidordqagvbzb63ew3h4xwykbkufpxufq7gyf6l567dbl5bhz7ac3q'
const CONSUMER_CIRCLE = 'bafyreiaedzibppjsllztdb2wqg322harsiohnkmccapz2uemjervj5kcgu'
const CONSUMER = 'bafyreih4m6vqnfj3p4ttk6zd3yqgbnoxl2jqkfk4kjzt3nfxwhpqnvnbbq'
const CONSUMER_2 = 'bafyreigl7ihmajhbxnc3hrmlmgdbpdnbfpxc5m3rnygpyuzqnvq5eyd7ya'
const FRIENDS = 'bafyreic2oz6xcvphmgk5jxs5lxpyqw4yhq5ghjxbpxcqw7nbxtutqjcmb4'
const INTRO_DEV = 'bafyreie5nyxnr6hzcssjvvvx5twtjuw4yc43vpnpk6h7pkpvsmvbnvmwxe'
const CIRCLE = 'bafyreigdmqpxkwn3czt7i4olfskvhtmu3rzmawn25mrtqr3mdlb6zaoz2e'
const DEV_DAT = 'bafkreibr5nkdlnukzyfhxj2rcgtoxsfztlli2fr6lfmyxekn2iow22ol2u'
const SLEEPY_PEOPLE = `bafyreisleepypeople${'x'.repeat(40)}`
const SLEEPY_RUN = `bafyreisleepyrun${'x'.repeat(43)}`
const KEEN_PEOPLE = `bafyreikeenpeople${'x'.repeat(42)}`
const KEEN_RUN = `bafyreikeenrun${'x'.repeat(45)}`
const PEOPLE_ITEMS = ['Dev', 'Ana', 'Bo', 'Cy'].map((name, i) => [name, i === 0 ? PEOPLE_DEV : `bafyreia${name.toLowerCase()}${'x'.repeat(46 - name.length)}`])
const MANY = Array.from({ length: 13 }, (_, i) => [`P${i}`, `bafyreimany${String(i).padStart(2, '0')}${'x'.repeat(46)}`])
const allHeld = Object.fromEntries([PEOPLE_DEV, DEV_DAT, ...PEOPLE_ITEMS.map(([, c]) => c)].map(c => [c, ['roblab']]))

const calls = { itemLineage: [] }
const LABELS = {
  [CONSUMER_INTROS]: { run_name: 'consumer_run', output: 'intros', completion: CONSUMER, status: 'succeeded' },
  [CONSUMER_CIRCLE]: { run_name: 'consumer_run', output: 'circle', completion: CONSUMER, status: 'succeeded' },
  [PEOPLE]: { run_name: 'infallible_spence', output: 'people', completion: PEOPLE_RUN, status: 'succeeded' },
  [SLEEPY_PEOPLE]: { run_name: 'sleepy_williams', output: 'people', completion: SLEEPY_RUN, status: 'succeeded' },
  [KEEN_PEOPLE]: { run_name: 'keen_hopper', output: 'people', completion: KEEN_RUN, status: 'succeeded' },
}
const leaf = (name, address) => ({ kind: 'Leaf', name, size: 10, address })
const TITLES = { [INTRO_DEV]: 'Dev', [CIRCLE]: 'circle', ...Object.fromEntries(PEOPLE_ITEMS.map(([n, c]) => [c, n])), ...Object.fromEntries(MANY.map(([n, c]) => [c, n])) }

const from = (item_cid, collection_cid = PEOPLE, selection_cid = FRIENDS) => ({ item_cid, collection_cid, selection_cid })
const input = (position, name, origins = []) => ({ position, role: 'path', name, address: DEV_DAT, size: 10, sample: null, origins })

/** The W2 shape for the spec's consumer: `intros` items read people items picked by Selection `friends`. */
/** `reads(cid)` says whether this member can read the item block (the open member's probe); `inputs`, when given, are every item's inputs; `neverPublished` names join the consumer items' leaves. */
function lineageEx({ runs = [CONSUMER], collection = PEOPLE, reads = () => true, inputs = null, neverPublished = [] } = {}) {
  const origin = (item_cid) => ({ item_cid, collection_cid: collection, selection_cid: FRIENDS })
  const inputsOf = (cid) => inputs ?? (cid === CIRCLE
    ? PEOPLE_ITEMS.map(([n, c], position) => ({ position, role: 'path', name: `${n}.dat`, address: DEV_DAT, size: 10, sample: null, origins: [origin(c)] }))
    : [{ position: 0, role: 'path', name: 'Dev_Patel.dat', address: DEV_DAT, size: 10, sample: null, origins: [origin(PEOPLE_DEV)] }])
  const unpublished = neverPublished.map(name => ({ kind: 'Leaf', name, size: null, address: null, reason: 'never_published' }))
  const ex = {
    itemLineage: async (cid, completion) => {
      calls.itemLineage.push([cid, completion])
      return runs.filter(r => !completion || r === completion).sort().map(r => ({ completion_cid: r, source: 'snapshot', inputs: inputsOf(cid) }))
    },
    item: async (coll, cid) => {
      if (!reads(cid)) throw Object.assign(new Error('missing'), { code: 'block_missing' })
      const title = TITLES[cid]
      const extra = cid === INTRO_DEV || cid === CIRCLE ? unpublished : []
      return { cid, value: {}, view: { id: title }, state: claimState([]), leaves: [leaf(`${title}.dat`, DEV_DAT), ...extra] }
    },
    runLabel: async (c) => LABELS[c] ?? null,
    runIdentity: async (c) => ({ run_name: c === CONSUMER_2 ? 'second_run' : 'consumer_run', pipeline: 'p' }),
    runRow: async () => ({ finished_at: '2026-10-24T13:29:00Z' }),
    producersOf: async () => [],
    selectionsHolding: async () => [],
    claimStates: async (cids) => new Map(cids.map(c => [c, claimState(c === FRIENDS ? [{ cid: 'n1', attribute: 'name', verb: 'set', value: 'friends' }] : [])])),
    blocks: { urlFor: (a) => `blocks/${a}`, partial: async () => { throw Object.assign(new Error('missing'), { code: 'block_missing' }) } },
    ...FAKE_STORAGE,
  }
  return ex
}

/** `members` in members.json order; `open` is the open member. Presence answers as createPresence does: the open member reads held unless hereHeld is null, then it answers from `held`. */
function ctxWith({ held = {}, hrefIn = (alias, h) => h, members = ['roblab', 'B'], open = 'roblab', explorers = {} } = {}) {
  const fake = lineageEx()
  const stateOf = (cid, { hereHeld = true } = {}) => members.map(alias => {
    const here = alias === open
    const state = here && hereHeld !== null ? (hereHeld === false ? 'not' : 'held') : (held[cid] ?? []).includes(alias) ? 'held' : 'not'
    return { alias, key: alias === 'B' ? 'b' : 'a', here, state }
  })
  return { tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: open, key: 'a', hrefIn },
    members: { primary: open, list: members.map(alias => ({ alias })), explorer: async (alias) => explorers[alias] ?? fake },
    presence: { open, several: members.length > 1, states: async (cid, o) => stateOf(cid, o), now: stateOf, dots: () => globalThis.document.createElement('span') },
    write: { available: true, here: true, writable: open, hrefFor: (h) => h, writer: {}, run: async (status, fn) => { await fn() } } }
}

async function page(ex, collection, itemCid, ctx) {
  installDom(); resetFolds()
  return item(ex, collection, itemCid, ctx)
}
const lineagePanel = (node) => node.querySelectorAll('section.panel').find(p => p.querySelector('h2')?.textContent === 'Lineage')
const metaOf = (node) => lineagePanel(node).querySelector('.ph').querySelector('.muted')?.textContent ?? null
const groupsOf = (node) => lineagePanel(node).querySelectorAll('[data-group]')
const groupKeys = (node) => groupsOf(node).map(g => g.getAttribute('data-group'))
const rowsOf = (el) => el.querySelectorAll('.tree-row')
const cells = (row) => row.children.map(c => c.textContent)
const subOf = (el) => el.querySelector('.tree-sub').children.map(c => c.textContent)
const hereOf = (el) => el.querySelectorAll('.tree-here').map(n => n.children.map(c => c.textContent))
const hrefsIn = (el) => el.querySelectorAll('a').map(a => a.getAttribute('href'))
const headOf = (group) => group.querySelector('.tree-head').children.map(c => c.textContent)
const lineOf = (node) => lineagePanel(node).querySelector('.tree-line')
const withInputs = (ex, map) => {
  const lineage = ex.itemLineage
  ex.itemLineage = async (...a) => (await lineage(...a)).map(r => ({ ...r, inputs: map(r.inputs) }))
  return ex
}

test('intros · Dev: one group under infallible_spence, the people line via "friends", Dev and Dev_Patel.dat, ending on the item made by consumer_run', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`])
  const [group] = groupsOf(node)
  assert.match(group.querySelector('.tree-head').textContent, /infallible_spence/)
  const line = group.querySelector('.tree-line')
  assert.equal(line.getAttribute('data-line'), `${PEOPLE}:${FRIENDS}`)
  assert.deepEqual(subOf(line), ['people', '1 item, via', '"friends"'])
  assert.ok(hrefsIn(line.querySelector('.tree-sub')).includes(`#/collection/${PEOPLE}`))
  assert.ok(hrefsIn(line.querySelector('.tree-sub')).includes(`#/selection/${FRIENDS}`))
  const [row] = rowsOf(line)
  assert.equal(row.getAttribute('data-row'), '0')
  assert.deepEqual(cells(row), ['Dev', 'Dev_Patel.dat', '', '10 B'])
  assert.ok(hrefsIn(row).includes(`#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.ok(hrefsIn(row).includes(`#/content/${DEV_DAT}`))
  assert.equal(row.classList.contains('ext'), false)
  assert.deepEqual(hereOf(lineagePanel(node)), [['intros · Dev', 'made by', 'consumer_run']])
  assert.equal(metaOf(node), '1 upstream run · 1 input')
  assert.deepEqual(strayTexts(lineagePanel(node)), [])
})

test('circle: four rows under one line, by position', async () => {
  const node = await page(lineageEx(), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.equal(groupsOf(node).length, 1)
  const rows = rowsOf(lineagePanel(node))
  assert.deepEqual(rows.map(r => r.getAttribute('data-row')), ['0', '1', '2', '3'])
  assert.deepEqual(rows.map(r => r.children[0].textContent), ['Dev', 'Ana', 'Bo', 'Cy'])
  assert.equal(metaOf(node), '1 upstream run · 4 inputs')
})

/** The open member cannot read people · Dev; member B holds it. */
const inB = (explorers = {}) => ctxWith({ held: { [PEOPLE_DEV]: ['B'] }, hrefIn: (alias, h) => `?member=${alias}${h}`, explorers })

test('an upstream item held only in member B links there, and its group asks B for the pipeline', async () => {
  const ctx = inB({ B: Object.assign(lineageEx(), { runIdentity: async () => ({ pipeline: 'b-pipeline' }) }) })
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(hrefsIn(rowsOf(lineagePanel(node))[0]).includes(`?member=B#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.equal(groupsOf(node)[0].querySelector('.tree-head').children[1].textContent, 'b-pipeline')
})

test('the run chip of an upstream item held only in member B opens the run there', async () => {
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_INTROS, INTRO_DEV, inB())
  assert.equal(groupsOf(node)[0].querySelector('.sealed-fact').getAttribute('href'), `?member=B#/run/${PEOPLE_RUN}`)
})

test('held nowhere: short CID and "not in any store here", unlinked; the run still heads the group, opening here; the row is dashed', async () => {
  const node = await page(lineageEx({ reads: c => c === INTRO_DEV }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {}, hrefIn: (alias, h) => `?member=${alias}${h}` }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`])
  assert.equal(groupsOf(node)[0].querySelector('.sealed-fact').getAttribute('href'), `#/run/${PEOPLE_RUN}`, 'the run was read here, so it opens here')
  const [row] = rowsOf(lineagePanel(node))
  assert.ok(row.children[0].textContent.includes(shortCid(PEOPLE_DEV)))
  assert.match(row.children[0].textContent, /not in any store here/)
  assert.equal(hrefsIn(row).some(h => h.includes('#/item/')), false)
  assert.ok(row.classList.contains('ext'))
})

test('neither the item nor its run read anywhere: a group headed by the collection\'s short CID and "run not in any store here"', async () => {
  const ex = lineageEx({ reads: c => c === INTRO_DEV })
  ex.runLabel = async (c) => (c === PEOPLE ? null : LABELS[c] ?? null)
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.deepEqual(groupKeys(node), [`collection:${PEOPLE}`])
  const [group] = groupsOf(node)
  assert.deepEqual(headOf(group), [shortCid(PEOPLE), 'run not in any store here'])
  assert.equal(group.querySelector('.tree-head').querySelector('code').getAttribute('title'), PEOPLE)
  assert.equal(group.querySelector('.sealed-fact'), null)
  assert.deepEqual(subOf(lineOf(node)), ['1 item, via', '"friends"'])
  assert.equal(metaOf(node), '1 input')
})

test('a Selection pick by query: a group headed by the Selection, "picked by query", no run chip; the row links #/item/-/', async () => {
  const node = await page(lineageEx({ collection: null }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.deepEqual(groupKeys(node), [`query:${FRIENDS}`])
  const [group] = groupsOf(node)
  assert.deepEqual(headOf(group), ['"friends"', 'picked by query'])
  assert.equal(group.querySelector('.sealed-fact'), null)
  assert.equal(group.querySelector('.tree-sub'), null)
  assert.ok(hrefsIn(group).includes(`#/item/-/${PEOPLE_DEV}`))
  assert.equal(metaOf(node), '1 input')
})

test('lineage is scoped to the URL run; - draws a tree per run in its own wrapper', async () => {
  const scoped = await page(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(calls.itemLineage.at(-1), [INTRO_DEV, CONSUMER])
  assert.equal(scoped.querySelectorAll('[data-lineage-run]').length, 0)
  assert.equal(metaOf(scoped), '1 upstream run · 1 input')
  const across = await page(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), '-', INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(calls.itemLineage.at(-1), [INTRO_DEV, null])
  const wrappers = across.querySelectorAll('[data-lineage-run]')
  assert.equal(wrappers.length, 2)
  assert.equal(metaOf(across), '2 runs recorded lineage')
  for (const w of wrappers) {
    assert.equal(w.children.length, 2)
    assert.ok(w.children[0].hasAttribute('data-lineage-counts'))
    assert.equal(w.children[0].textContent, '1 upstream run · 1 input')
    assert.ok(w.children[1].classList.contains('tree'))
  }
  const wrapper = (c) => wrappers.find(w => w.getAttribute('data-lineage-run') === c)
  assert.deepEqual(hereOf(wrapper(CONSUMER)), [['item · Dev', 'made by', 'consumer_run']])
  assert.deepEqual(hereOf(wrapper(CONSUMER_2)), [['item · Dev', 'made by', 'second_run']])
})

test('a run without lineage shows the item alone', async () => {
  const node = await page(lineageEx({ runs: [] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.equal(groupsOf(node).length, 0)
  assert.deepEqual(hereOf(lineagePanel(node)), [['intros · Dev', 'made by', 'consumer_run']])
  assert.equal(metaOf(node), null)
})

test('an Explorer without itemLineage shows the item alone', async () => {
  const ex = lineageEx()
  delete ex.itemLineage
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.equal(groupsOf(node).length, 0)
  assert.deepEqual(hereOf(lineagePanel(node)), [['intros · Dev', 'made by', 'consumer_run']])
  assert.equal(metaOf(node), null)
})

test('an input with no Origin goes under Read by path, file only', async () => {
  const ex = withInputs(lineageEx(), inputs => inputs.map(i => ({ ...i, origins: [] })))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [DEV_DAT]: ['roblab'] } }))
  assert.deepEqual(groupKeys(node), ['path'])
  assert.equal(groupsOf(node)[0].querySelector('.tree-head').textContent, 'Read by path')
  const [row] = rowsOf(lineagePanel(node))
  assert.deepEqual(cells(row), ['', 'Dev_Patel.dat', '', '10 B'])
  assert.ok(lineOf(node).querySelector('.tree-rows').classList.contains('no-item'))
  assert.equal(metaOf(node), '1 input')
})

test('an input with neither name nor address gets a fixed label', async () => {
  const ex = withInputs(lineageEx(), inputs => inputs.map(i => ({ ...i, name: null, address: null, origins: [] })))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  const [row] = rowsOf(lineagePanel(node))
  assert.equal(row.children[1].textContent, 'unnamed input')
  assert.ok(row.classList.contains('ext'))
})

test('a run whose lineage cannot be drawn shows only its item; the page and the other run still render', async () => {
  const ex = lineageEx({ runs: [CONSUMER, CONSUMER_2] })
  const lineage = ex.itemLineage
  ex.itemLineage = async (...a) => (await lineage(...a)).map(r => (r.completion_cid === CONSUMER_2 ? { ...r, inputs: null } : r))
  const node = await page(ex, '-', INTRO_DEV, ctxWith({ held: allHeld }))
  const wrappers = node.querySelectorAll('[data-lineage-run]')
  assert.equal(wrappers.length, 2)
  const wrapper = (c) => wrappers.find(w => w.getAttribute('data-lineage-run') === c)
  assert.deepEqual(wrapper(CONSUMER).querySelectorAll('[data-group]').map(g => g.getAttribute('data-group')), [`run:${PEOPLE_RUN}`])
  assert.equal(wrapper(CONSUMER_2).querySelectorAll('[data-group]').length, 0)
  assert.equal(wrapper(CONSUMER_2).querySelectorAll('[data-lineage-counts]').length, 0)
  assert.deepEqual(hereOf(wrapper(CONSUMER_2)), [['item · Dev', 'made by', 'second_run']])
})

test('both members hold the upstream item: the link stays in the open member even when it is listed second', async () => {
  const ctx = ctxWith({ members: ['B', 'roblab'], held: { [PEOPLE_DEV]: ['B', 'roblab'] }, hrefIn: (alias, h) => (alias === 'roblab' ? h : `?member=${alias}${h}`) })
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(hrefsIn(lineagePanel(node)).includes(`#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.equal(hrefsIn(lineagePanel(node)).some(h => h.startsWith('?member=B#/item/')), false)
})

test('a file only member B holds is not dashed; one no member holds is', async () => {
  const only = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['B'] } }))
  assert.equal(lineagePanel(only).querySelectorAll('.tree-row.ext').length, 0)
  const none = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.equal(lineagePanel(none).querySelectorAll('.tree-row.ext').length, 1)
})

/** The real presence over one member, `lab`, whose HEAD for each block answers `status(cid)`. */
function onePresenceCtx(status) {
  const fetchFn = async (url) => new Response(null, { status: status(String(url).split('/').at(-1)) })
  const ctx = ctxWith({ members: ['lab'], open: 'lab' })
  ctx.members = { primary: 'lab', list: [{ alias: 'lab', key: 'a', base: 'm/lab/' }], explorer: async () => null }
  ctx.presence = createPresence(ctx.members, { fetchFn, pageUrl: 'http://h/index.html' })
  return ctx
}
const fileRow = (node) => rowsOf(lineagePanel(node)).find(r => r.children[1].textContent === 'Dev_Patel.dat')

test('with one member, a file the open member does not hold is dashed (real presence)', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, onePresenceCtx((cid) => (cid === DEV_DAT ? 404 : 200)))
  assert.ok(fileRow(node).classList.contains('ext'))
})

test('with one member, a file the open member holds stays solid (real presence)', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, onePresenceCtx(() => 200))
  assert.equal(fileRow(node).classList.contains('ext'), false)
})

test('an Origin repeated across inputs is read once', async () => {
  const ex = withInputs(lineageEx(), inputs => inputs.concat(inputs.map(i => ({ ...i, position: 1, name: 'again.dat' }))))
  let reads = 0
  const item = ex.item
  ex.item = async (coll, cid) => { if (cid === PEOPLE_DEV) reads++; return item(coll, cid) }
  await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.equal(reads, 1)
})

test('two inputs sharing an Origin are two rows under one line, the item counted once', async () => {
  const ex = withInputs(lineageEx(), inputs => inputs.concat(inputs.map(i => ({ ...i, position: 1, name: 'again.dat' }))))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  const p = lineagePanel(node)
  assert.deepEqual(rowsOf(p).map(r => r.getAttribute('data-row')), ['0', '1'])
  assert.deepEqual(rowsOf(p).map(r => r.children[1].textContent), ['Dev_Patel.dat', 'again.dat'])
  assert.equal(subOf(lineOf(node))[1], '1 item, via')
  assert.equal(metaOf(node), '1 upstream run · 2 inputs')
  // The fake DOM does not move an element appended twice, so check the two positions hold different elements.
  assert.notEqual(rowsOf(p)[0].children[0].children[0], rowsOf(p)[1].children[0].children[0])
})

test('two upstream runs: a group each, by lowest position; a row under both names the other run', async () => {
  const inputs = [input(0, 'Dev.dat', [from(PEOPLE_DEV)]), input(1, 'Ana.dat', [from(PEOPLE_ITEMS[1][1]), from(PEOPLE_ITEMS[1][1], SLEEPY_PEOPLE)]),
    input(2, 'Bo.dat', [from(PEOPLE_ITEMS[2][1])]), input(3, 'Cy.dat', [from(PEOPLE_ITEMS[3][1], SLEEPY_PEOPLE)])]
  const node = await page(lineageEx({ inputs }), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`, `run:${SLEEPY_RUN}`])
  assert.deepEqual(groupsOf(node).map(g => rowsOf(g).map(r => r.getAttribute('data-row'))), [['0', '1', '2'], ['1', '3']])
  const tag = (g, position) => rowsOf(groupsOf(node)[g]).find(r => r.getAttribute('data-row') === position).querySelector('.tree-tag')
  assert.equal(tag(0, '1').children[0].textContent, 'also')
  assert.equal(tag(0, '1').querySelector('a.also').textContent, 'sleepy_williams')
  assert.equal(tag(0, '1').querySelector('a.also').getAttribute('href'), `#/run/${SLEEPY_RUN}`)
  assert.equal(tag(1, '1').querySelector('a.also').textContent, 'infallible_spence')
  assert.equal(tag(0, '0').textContent, '')
  assert.equal(metaOf(node), '2 upstream runs · 4 inputs')
})

test('three runs: the tag counts the two others, their names in its title, and is not a link', async () => {
  const ana = PEOPLE_ITEMS[1][1]
  const inputs = [input(0, 'Dev.dat', [from(PEOPLE_DEV)]), input(1, 'Ana.dat', [from(ana), from(ana, SLEEPY_PEOPLE), from(ana, KEEN_PEOPLE)])]
  const node = await page(lineageEx({ inputs }), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`, `run:${SLEEPY_RUN}`, `run:${KEEN_RUN}`])
  const box = (g) => rowsOf(groupsOf(node)[g]).find(r => r.getAttribute('data-row') === '1').querySelector('.tree-tag')
  assert.equal(box(0).querySelector('a.also'), null)
  assert.equal(box(0).querySelector('span.also').textContent, '2 other runs')
  assert.equal(box(0).querySelector('span.also').getAttribute('title'), 'sleepy_williams, keen_hopper')
  assert.equal(box(2).querySelector('span.also').getAttribute('title'), 'infallible_spence, sleepy_williams')
})

test('the line says k of m from the collection\'s count; without one, k items; never of 0', async () => {
  const circle = async (count) => {
    const ex = lineageEx()
    if (count) ex.collectionItemCount = count
    return subOf(lineOf(await page(ex, CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))))[1]
  }
  assert.equal(await circle(async (c) => (c === PEOPLE ? 9 : null)), '4 of 9 items, via')
  assert.equal(await circle(async () => 0), '4 items, via')
  assert.equal(await circle(async () => { throw new Error('unreadable') }), '4 items, via')
  assert.equal(await circle(null), '4 items, via')
})

test('the count is asked of the member that holds the collection', async () => {
  const ctx = ctxWith({ held: { [PEOPLE_DEV]: ['B'] }, explorers: { B: Object.assign(lineageEx(), { collectionItemCount: async () => 9 }) } })
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.equal(subOf(lineOf(node))[1], '1 of 9 items, via')
})

test('an unnamed Selection on the line shows its short CID', async () => {
  const ex = lineageEx()
  ex.claimStates = async (cids) => new Map(cids.map(c => [c, claimState([])]))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  const line = lineOf(node)
  assert.equal(subOf(line)[2], shortCid(FRIENDS))
  assert.equal(line.querySelector('.tree-sub').querySelector('code').getAttribute('title'), FRIENDS)
  assert.equal(line.querySelector('.tree-sub').textContent.includes('"'), false)
})

test('the group head names the upstream pipeline and the age; without the pipeline, the age alone', async () => {
  const twoDays = { ...LABELS[PEOPLE], finished_at: new Date(Date.now() - 2 * 86_400_000).toISOString() }
  const headMeta = async (runIdentity) => {
    const ex = lineageEx()
    ex.runLabel = async (c) => (c === PEOPLE ? twoDays : LABELS[c] ?? null)
    if (runIdentity) ex.runIdentity = runIdentity
    const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
    return groupsOf(node)[0].querySelector('.tree-head').children[1].textContent
  }
  assert.equal(await headMeta(null), 'p · 2d')
  assert.equal(await headMeta(async (c) => {
    if (c === PEOPLE_RUN) throw new Error('unreadable')
    return { run_name: 'consumer_run', pipeline: 'p' }
  }), '2d')
})

test('never-published leaves: one named like a path input is that row; the others follow, and none counts as an input', async () => {
  const node = await page(lineageEx({ inputs: [input(0, 'Dev_Patel.dat')], neverPublished: ['Dev_Patel.dat', 'extra.txt'] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), ['path'])
  const rows = rowsOf(lineagePanel(node))
  assert.deepEqual(rows.map(r => r.getAttribute('data-row')), ['0', 'leaf'])
  assert.deepEqual(rows.map(r => r.children[1].textContent), ['Dev_Patel.dat', 'extra.txt'])
  assert.ok(rows[1].classList.contains('ext'))
  assert.equal(metaOf(node), '1 input')
  const bare = await page(lineageEx({ runs: [], neverPublished: ['in.fq'] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  const [only, ...rest] = rowsOf(lineagePanel(bare))
  assert.equal(rest.length, 0)
  assert.equal(only.getAttribute('data-row'), 'leaf')
  assert.equal(only.children[1].textContent, 'in.fq')
  assert.equal(metaOf(bare), null)
})

/** A line's rows box: its directly shown rows, and its fold when there is one. */
const shownRows = (line) => line.querySelector('.tree-rows').children.filter(c => c.classList.contains('tree-row'))

test('a line with 13 rows shows 8 and folds 5 behind +5 more; 12 rows fold none', async () => {
  const many = (n) => lineageEx({ inputs: MANY.slice(0, n).map(([name, cid], i) => input(i, `${name}.dat`, [from(cid)])) })
  const thirteen = lineOf(await page(many(13), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld })))
  assert.equal(shownRows(thirteen).length, 8)
  const details = thirteen.querySelector('details')
  assert.equal(details.getAttribute('data-fold'), 'lineage')
  assert.equal(details.querySelector('summary').textContent, '+5 more')
  assert.equal(rowsOf(details).length, 5)
  const twelve = lineOf(await page(many(12), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld })))
  assert.equal(twelve.querySelector('details'), null)
  assert.equal(rowsOf(twelve).length, 12)
})

test('Read by path folds like a line', async () => {
  const ex = lineageEx({ inputs: MANY.map(([name], i) => input(i, `${name}.dat`)) })
  const line = lineOf(await page(ex, CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld })))
  assert.equal(line.getAttribute('data-line'), 'path')
  assert.equal(shownRows(line).length, 8)
  assert.equal(line.querySelector('details').querySelector('summary').textContent, '+5 more')
})

test('rows of one run held in two members each link their own member', async () => {
  const ctx = ctxWith({ held: { ...allHeld, [PEOPLE_DEV]: ['B'] }, hrefIn: (alias, h) => (alias === 'roblab' ? h : `?member=${alias}${h}`) })
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_CIRCLE, CIRCLE, ctx)
  const itemHref = (row) => hrefsIn(row.children[0])[0]
  const rows = rowsOf(lineagePanel(node))
  assert.ok(itemHref(rows[0]).startsWith('?member=B#/item/'))
  assert.equal(itemHref(rows[1]), `#/item/${PEOPLE}/${PEOPLE_ITEMS[1][1]}`)
})

test('an Origin naming neither collection nor Selection: picked by query alone, no stray text', async () => {
  const node = await page(lineageEx({ inputs: [input(0, 'Dev_Patel.dat', [from(PEOPLE_DEV, null, null)])] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), ['query:-'])
  assert.deepEqual(headOf(groupsOf(node)[0]), ['picked by query'])
  assert.deepEqual(strayTexts(lineagePanel(node)), [])
})

const OTHER_PEOPLE = 'bafyreiotherpeoplexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
const CONSUMER_RUN_NAME = 'consumer_run'

/** The W2 shape: the consumer's `intros · Dev` and `circle` were made from `people · Dev` read through Selection `friends`. */
function usedByEx({ rows = null } = {}) {
  const ex = lineageEx()
  const row = (item_cid, collection_cid) => ({ completion_cid: CONSUMER, via_cid: PEOPLE, selection_cid: FRIENDS, item_cid, collection_cid })
  ex.usedBy = async (cid) => cid === PEOPLE_DEV ? rows ?? [row(INTRO_DEV, CONSUMER_INTROS), row(CIRCLE, CONSUMER_CIRCLE)] : []
  return ex
}
const usedByPanel = (node) => [...node.querySelectorAll('section.panel')].find(p => /Used by/.test(p.querySelector('h2')?.textContent))

test('people \u00b7 Dev from infallible_spence is used by intros \u00b7 Dev and circle under the consumer run', async () => {
  const node = await page(usedByEx(), PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  const panel = usedByPanel(node)
  assert.ok(panel)
  assert.match(panel.textContent, /intros \u00b7 Dev/)
  assert.match(panel.textContent, /circle/)
  assert.match(panel.textContent, new RegExp(CONSUMER_RUN_NAME))
  assert.match(panel.textContent, /via Selection "friends"/)
  assert.equal(panel.querySelectorAll('[data-used-by-run]').length, 1, 'one chip for the one consumer run')
  assert.ok(panel.querySelectorAll('a').some(a => a.getAttribute('href') === `#/item/${CONSUMER_INTROS}/${INTRO_DEV}`))
})

test('used by lists only consumers that read this run collection', async () => {
  const node = await page(usedByEx(), OTHER_PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  assert.equal(usedByPanel(node), undefined)
})

test('with - used by keeps every row', async () => {
  const node = await page(usedByEx(), '-', PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  assert.match(usedByPanel(node).textContent, /intros \u00b7 Dev/)
})

test('an item nothing consumed has no Used by panel; neither has an Explorer without usedBy', async () => {
  const none = await page(usedByEx(), PEOPLE, PEOPLE_ITEMS[1][1], ctxWith({ held: allHeld, members: ['roblab'] }))
  assert.equal(usedByPanel(none), undefined)
  const ex = usedByEx()
  delete ex.usedBy
  assert.equal(usedByPanel(await page(ex, PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))), undefined)
})

test('a consumer recorded in another member links there', async () => {
  const other = usedByEx()
  const none = usedByEx()
  none.usedBy = async () => []
  const ctx = ctxWith({ members: ['B', 'A'], open: 'B', held: allHeld, hrefIn: (a, h) => `?member=${a}${h}`, explorers: { A: other, B: none } })
  const node = await page(none, PEOPLE, PEOPLE_DEV, ctx)
  assert.ok(usedByPanel(node).querySelectorAll('a').some(a => a.getAttribute('href').startsWith('?member=A#/item/')))
})

test('the run chip of a consumer recorded in another member opens the run there', async () => {
  const none = usedByEx()
  none.usedBy = async () => []
  const ctx = ctxWith({ members: ['B', 'A'], open: 'B', held: allHeld, hrefIn: (a, h) => `?member=${a}${h}`, explorers: { A: usedByEx(), B: none } })
  const chip = usedByPanel(await page(none, PEOPLE, PEOPLE_DEV, ctx)).querySelector('[data-used-by-run]').querySelector('.sealed-fact')
  assert.equal(chip.getAttribute('href'), `?member=A#/run/${CONSUMER}`)
})

test('with one member the run chips keep their plain href', async () => {
  const lineage = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  assert.equal(groupsOf(lineage)[0].querySelector('.sealed-fact').getAttribute('href'), `#/run/${PEOPLE_RUN}`)
  const used = usedByPanel(await page(usedByEx(), PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] })))
  assert.equal(used.querySelector('[data-used-by-run]').querySelector('.sealed-fact').getAttribute('href'), `#/run/${CONSUMER}`)
})

test('a consumer both members recorded is listed once, linked in the open member', async () => {
  const ctx = ctxWith({ members: ['B', 'A'], open: 'B', held: allHeld, hrefIn: (a, h) => (a === 'B' ? h : `?member=${a}${h}`), explorers: { A: usedByEx(), B: usedByEx() } })
  const panel = usedByPanel(await page(usedByEx(), PEOPLE, PEOPLE_DEV, ctx))
  assert.equal(panel.querySelectorAll('a').filter(a => a.getAttribute('href').includes('#/item/')).length, 2)
  assert.equal(panel.querySelectorAll('a').some(a => a.getAttribute('href').startsWith('?member=')), false)
})

test('two consumer runs give two groups', async () => {
  const row = (completion_cid, item_cid, collection_cid) => ({ completion_cid, via_cid: PEOPLE, selection_cid: FRIENDS, item_cid, collection_cid })
  const ex = usedByEx({ rows: [row(CONSUMER, INTRO_DEV, CONSUMER_INTROS), row(CONSUMER_2, CIRCLE, CONSUMER_CIRCLE)] })
  const panel = usedByPanel(await page(ex, PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] })))
  assert.deepEqual(panel.querySelectorAll('[data-used-by-run]').map(g => g.getAttribute('data-used-by-run')), [CONSUMER, CONSUMER_2])
})

test('a row with no via collection is dropped under a run and kept under -', async () => {
  const rows = [{ completion_cid: CONSUMER, via_cid: null, selection_cid: FRIENDS, item_cid: INTRO_DEV, collection_cid: CONSUMER_INTROS }]
  const ctx = () => ctxWith({ held: allHeld, members: ['roblab'] })
  assert.equal(usedByPanel(await page(usedByEx({ rows }), PEOPLE, PEOPLE_DEV, ctx())), undefined)
  assert.ok(usedByPanel(await page(usedByEx({ rows }), '-', PEOPLE_DEV, ctx())))
})

test('an unnamed Selection shows its short CID', async () => {
  const ex = usedByEx()
  ex.claimStates = async (cids) => new Map(cids.map(c => [c, claimState([])]))
  const panel = usedByPanel(await page(ex, PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] })))
  assert.match(panel.textContent, /via Selection/)
  assert.equal(panel.textContent.includes('"'), false)
  assert.ok(panel.querySelectorAll('code').some(c => c.getAttribute('title') === FRIENDS))
})

test('Lineage spans the column above a grid-2 of Produced by and Used by; without Used by, Produced by alone', async () => {
  const node = await page(usedByEx(), PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  const lineage = lineagePanel(node)
  const grid = node.children.find(c => c.classList.contains('grid-2'))
  assert.ok(node.children.includes(lineage), 'Lineage is a child of the page, not of the grid')
  assert.ok(node.children.indexOf(lineage) < node.children.indexOf(grid))
  assert.deepEqual(grid.children.map(p => p.querySelector('h2').textContent), ['Produced by', 'Used by'])
  const alone = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(alone.children.find(c => c.classList.contains('grid-2')).children.map(p => p.querySelector('h2').textContent), ['Produced by'])
})
