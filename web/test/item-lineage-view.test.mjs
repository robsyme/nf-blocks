// The item page's Lineage panel over a fake Explorer: the upstream item, its run and Selection, above the file.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { resetFolds } from '../src/folds.js'
import { createPresence } from '../src/presence.js'
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
const PEOPLE_ITEMS = ['Dev', 'Ana', 'Bo', 'Cy'].map((name, i) => [name, i === 0 ? PEOPLE_DEV : `bafyreia${name.toLowerCase()}${'x'.repeat(46 - name.length)}`])
const allHeld = Object.fromEntries([PEOPLE_DEV, DEV_DAT, ...PEOPLE_ITEMS.map(([, c]) => c)].map(c => [c, ['roblab']]))

const calls = { itemLineage: [] }
const LABELS = {
  [CONSUMER_INTROS]: { run_name: 'consumer_run', output: 'intros', completion: CONSUMER, status: 'succeeded' },
  [CONSUMER_CIRCLE]: { run_name: 'consumer_run', output: 'circle', completion: CONSUMER, status: 'succeeded' },
  [PEOPLE]: { run_name: 'infallible_spence', output: 'people', completion: PEOPLE_RUN, status: 'succeeded' },
}
const leaf = (name, address) => ({ kind: 'Leaf', name, size: 10, address })
const TITLES = { [INTRO_DEV]: 'Dev', [CIRCLE]: 'circle', ...Object.fromEntries(PEOPLE_ITEMS.map(([n, c]) => [c, n])) }

/** The W2 shape for the spec's consumer: `intros` items read people items picked by Selection `friends`. */
/** `reads(cid)` says whether this member can read the item block (the open member's probe). */
function lineageEx({ runs = [CONSUMER], collection = PEOPLE, reads = () => true } = {}) {
  const origin = (item_cid) => ({ item_cid, collection_cid: collection, selection_cid: FRIENDS })
  const inputsOf = (cid) => cid === CIRCLE
    ? PEOPLE_ITEMS.map(([n, c], position) => ({ position, role: 'path', name: `${n}.dat`, address: DEV_DAT, size: 10, sample: null, origins: [origin(c)] }))
    : [{ position: 0, role: 'path', name: 'Dev_Patel.dat', address: DEV_DAT, size: 10, sample: null, origins: [origin(PEOPLE_DEV)] }]
  const ex = {
    itemLineage: async (cid, completion) => {
      calls.itemLineage.push([cid, completion])
      return runs.filter(r => !completion || r === completion).sort().map(r => ({ completion_cid: r, source: 'snapshot', inputs: inputsOf(cid) }))
    },
    item: async (coll, cid) => {
      if (!reads(cid)) throw Object.assign(new Error('missing'), { code: 'block_missing' })
      const title = TITLES[cid]
      return { cid, value: {}, view: { id: title }, state: claimState([]), leaves: [leaf(`${title}.dat`, DEV_DAT)] }
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
const thread = (node) => node.querySelectorAll('.thread')
const nodesOf = (node) => thread(node).flatMap(t => t.querySelectorAll('.node'))
const anchors = (node) => thread(node).flatMap(t => t.querySelectorAll('a'))
const hrefs = (node) => anchors(node).map(a => a.getAttribute('href'))

test('intros · Dev shows people · Dev, its run and Selection, above Dev_Patel.dat', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  const nodes = nodesOf(node).map(n => n.textContent)
  assert.match(nodes[0], /people · Dev/)
  assert.match(nodes[0], /infallible_spence/)
  assert.match(nodes[0], /via Selection "friends"/)
  assert.match(nodes[1], /Dev_Patel\.dat/)
  assert.match(nodes.at(-1), /intros · Dev/)
  assert.ok(hrefs(node).includes(`#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.ok(hrefs(node).includes(`#/content/${DEV_DAT}`))
  assert.equal(nodesOf(node).filter(n => n.classes().includes('ext')).length, 0)
})

test('circle shows four upstream nodes', async () => {
  const node = await page(lineageEx(), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.equal(nodesOf(node).filter(n => /people ·/.test(n.textContent)).length, 4)
})

test('an upstream item held only in member B links there with ?member=B', async () => {
  const ctx = ctxWith({ held: { [PEOPLE_DEV]: ['B'] }, hrefIn: (alias, h) => `?member=${alias}${h}` })
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(hrefs(node).includes(`?member=B#/item/${PEOPLE}/${PEOPLE_DEV}`))
})

test('the run chip of an upstream item held only in member B opens the run there', async () => {
  const ctx = ctxWith({ held: { [PEOPLE_DEV]: ['B'] }, hrefIn: (alias, h) => `?member=${alias}${h}` })
  const node = await page(lineageEx({ reads: c => c !== PEOPLE_DEV }), CONSUMER_INTROS, INTRO_DEV, ctx)
  const chip = nodesOf(node)[0].querySelector('.sealed-fact')
  assert.equal(chip.getAttribute('href'), `?member=B#/run/${PEOPLE_RUN}`)
})

test('held nowhere: short CID, "not in any store here", no link; the file node is dashed', async () => {
  const node = await page(lineageEx({ reads: c => c === INTRO_DEV }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {}, hrefIn: (alias, h) => `?member=${alias}${h}` }))
  const first = nodesOf(node)[0]
  assert.match(first.textContent, /not in any store here/)
  assert.equal(first.querySelector('.sealed-fact').getAttribute('href'), `#/run/${PEOPLE_RUN}`, 'the run was read here, so it opens here')
  assert.equal(first.querySelectorAll('a').some(a => a.getAttribute('href').includes('#/item/')), false)
  assert.match(first.textContent, /infallible_spence/, 'the run still shows when the open member can read it')
  assert.ok(nodesOf(node).some(n => n.classes().includes('ext')))
})

test('a Selection pick by query links #/item/-/ with no run chip', async () => {
  const node = await page(lineageEx({ collection: null }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.ok(hrefs(node).includes(`#/item/-/${PEOPLE_DEV}`))
  assert.match(nodesOf(node)[0].textContent, /picked by query/)
  assert.match(nodesOf(node)[0].textContent, /via Selection "friends"/)
  assert.equal(nodesOf(node)[0].querySelector('.sealed-fact'), null)
})

test('lineage is scoped to the URL run; - groups every run under its chip', async () => {
  const scoped = await page(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(calls.itemLineage.at(-1), [INTRO_DEV, CONSUMER])
  assert.equal(scoped.querySelectorAll('[data-lineage-run]').length, 0)
  const across = await page(lineageEx({ runs: [CONSUMER, CONSUMER_2] }), '-', INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(calls.itemLineage.at(-1), [INTRO_DEV, null])
  assert.equal(across.querySelectorAll('[data-lineage-run]').length, 2)
})

test('a run without lineage shows the panel as today', async () => {
  const node = await page(lineageEx({ runs: [] }), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.deepEqual(nodesOf(node).map(n => n.textContent), ['intros · Dev'])
})

test('an Explorer without itemLineage shows the panel as today', async () => {
  const ex = lineageEx()
  delete ex.itemLineage
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  assert.deepEqual(nodesOf(node).map(n => n.textContent), ['intros · Dev'])
})

test('an input with no Origin shows its file only', async () => {
  const ex = lineageEx()
  const lineage = ex.itemLineage
  ex.itemLineage = async (...a) => (await lineage(...a)).map(r => ({ ...r, inputs: r.inputs.map(i => ({ ...i, origins: [] })) }))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [DEV_DAT]: ['roblab'] } }))
  assert.deepEqual(nodesOf(node).map(n => n.textContent), ['Dev_Patel.dat', 'intros · Dev'])
})

test('both members hold the upstream item: the link stays in the open member even when it is listed second', async () => {
  const ctx = ctxWith({ members: ['B', 'roblab'], held: { [PEOPLE_DEV]: ['B', 'roblab'] }, hrefIn: (alias, h) => (alias === 'roblab' ? h : `?member=${alias}${h}`) })
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(hrefs(node).includes(`#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.equal(hrefs(node).some(h => h.startsWith('?member=B#/item/')), false)
})

test('a file only member B holds is not dashed; one no member holds is', async () => {
  const only = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['B'] } }))
  assert.equal(nodesOf(only).filter(n => n.classes().includes('ext')).length, 0)
  const none = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.equal(nodesOf(none).filter(n => n.classes().includes('ext')).length, 1)
})

/** The real presence over one member, `lab`, whose HEAD for each block answers `status(cid)`. */
function onePresenceCtx(status) {
  const fetchFn = async (url) => new Response(null, { status: status(String(url).split('/').at(-1)) })
  const ctx = ctxWith({ members: ['lab'], open: 'lab' })
  ctx.members = { primary: 'lab', list: [{ alias: 'lab', key: 'a', base: 'm/lab/' }], explorer: async () => null }
  ctx.presence = createPresence(ctx.members, { fetchFn, pageUrl: 'http://h/index.html' })
  return ctx
}

test('with one member, a file the open member does not hold is dashed (real presence)', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, onePresenceCtx((cid) => (cid === DEV_DAT ? 404 : 200)))
  const file = nodesOf(node).find(n => /Dev_Patel\.dat/.test(n.textContent))
  assert.ok(file.classes().includes('ext'))
})

test('with one member, a file the open member holds stays solid (real presence)', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, onePresenceCtx(() => 200))
  const file = nodesOf(node).find(n => /Dev_Patel\.dat/.test(n.textContent))
  assert.equal(file.classes().includes('ext'), false)
})

test('an Origin repeated across inputs is read once', async () => {
  const ex = lineageEx()
  const lineage = ex.itemLineage
  ex.itemLineage = async (...a) => (await lineage(...a)).map(r => ({ ...r, inputs: r.inputs.concat(r.inputs.map(i => ({ ...i, position: 1, name: 'again.dat' }))) }))
  let reads = 0
  const item = ex.item
  ex.item = async (coll, cid) => { if (cid === PEOPLE_DEV) reads++; return item(coll, cid) }
  await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'] } }))
  assert.equal(reads, 1)
})

test('two inputs sharing an Origin both render the upstream node', async () => {
  const ex = lineageEx()
  const lineage = ex.itemLineage
  ex.itemLineage = async (...a) => (await lineage(...a)).map(r => ({ ...r, inputs: r.inputs.concat(r.inputs.map(i => ({ ...i, position: 1, name: 'again.dat' }))) }))
  const node = await page(ex, CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  assert.deepEqual(nodesOf(node).map(n => n.textContent.split(/infallible|via/)[0]), ['people · Dev', 'Dev_Patel.dat', 'people · Dev', 'again.dat', 'intros · Dev'])
  // The fake DOM does not move an element appended twice, so check the two positions hold different elements.
  assert.notEqual(nodesOf(node)[0].children[0], nodesOf(node)[2].children[0])
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
  assert.equal(nodesOf(lineage)[0].querySelector('.sealed-fact').getAttribute('href'), `#/run/${PEOPLE_RUN}`)
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
