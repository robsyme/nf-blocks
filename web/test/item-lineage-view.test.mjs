// The item page's Lineage panel over a fake Explorer: the upstream item, its run and Selection, above the file.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { resetFolds } from '../src/folds.js'
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
function lineageEx({ runs = [CONSUMER], collection = PEOPLE } = {}) {
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

function ctxWith({ held = {}, hrefIn = (alias, h) => h } = {}) {
  const fake = lineageEx()
  const stateOf = (cid) => ['roblab', 'B'].map(alias => ({ alias, key: alias === 'B' ? 'b' : 'a', here: alias === 'roblab', state: (held[cid] ?? []).includes(alias) ? 'held' : 'not' }))
  return { tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: 'roblab', key: 'a', hrefIn },
    members: { primary: 'roblab', list: [{ alias: 'roblab' }, { alias: 'B' }], explorer: async () => fake },
    presence: { open: 'roblab', several: true, states: async (cid) => stateOf(cid), now: stateOf, dots: () => globalThis.document.createElement('span') },
    write: { available: true, here: true, writable: 'roblab', hrefFor: (h) => h, writer: {}, run: async (status, fn) => { await fn() } } }
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
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctx)
  assert.ok(hrefs(node).includes(`?member=B#/item/${PEOPLE}/${PEOPLE_DEV}`))
})

test('held nowhere: short CID, "not in any store here", no link; the file node is dashed', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: {} }))
  const first = nodesOf(node)[0]
  assert.match(first.textContent, /not in any store here/)
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
