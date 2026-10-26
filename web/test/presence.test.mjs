// web/test/presence.test.mjs
// Presence dots (P3 Task 3, rule D4): the open member from what the page
// knows, every other member from a HEAD of the block's URL under its base.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { IN_FLIGHT, createPresence, elsewhereText, presenceTitle } from '../src/presence.js'
import { createMembers, memberOpener } from '../src/members.js'
import { resolveStore } from '../src/store.js'
import { BlockFetcher } from '../src/blocks.js'
import { Tray } from '../src/tray.js'
import { resetFolds } from '../src/folds.js'
import { item } from '../src/views/item.js'
import { content } from '../src/views/collection.js'
import { block as blockView } from '../src/views/block.js'
import { buildMember, blockFetch } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'

const CID = 'bafkreibxyh4xa4v5od634co6m4wcvs3yrh7s4vhsz2hgiybzwkahgz3oym'
const PAGE = 'http://h/index.html'
const member = (alias, key) => ({ alias, key, base: `m/${alias}/` })
const THREE = { primary: 'roblab', list: [member('roblab', 'a'), member('org-s3', 'b'), member('ana-hpc', 'c')] }
const TWO = { primary: 'lab', list: [member('lab', 'a'), member('org', 'b')] }
const ONE = { primary: 'lab', list: [member('lab', 'a')] }

/** A fetch that answers each HEAD from `status(alias, cid)` and records it. */
function heads(status = () => 200) {
  const asked = []
  const fn = async (url, init = {}) => {
    const [, alias, cid] = String(url).match(/\/m\/([^/]+)\/blocks\/[^/]{2}\/([^/]+)$/)
    asked.push({ url: String(url), method: init.method, alias, cid })
    const s = status(alias, cid)
    if (s instanceof Error) throw s
    return new Response(null, { status: s })
  }
  fn.asked = asked
  return fn
}

const classes = (node) => node.querySelectorAll('.dot').map(d => d.getAttribute('class'))

test('one member: one filled dot named for it, and no request at all', async () => {
  installDom()
  const fetchFn = heads()
  const p = createPresence(ONE, { fetchFn, pageUrl: PAGE })
  const node = p.dots(CID)
  await frame()
  assert.deepEqual(classes(node), ['dot a'])
  assert.equal(node.getAttribute('title'), 'Held in lab')
  assert.deepEqual(await p.states(CID, { hereHeld: null }), [{ alias: 'lab', key: 'a', here: true, state: 'held' }], 'null reads as held with one member')
  assert.equal(p.several, false)
  assert.equal(fetchFn.asked.length, 0)
})

test('no members.json: the one store ctx names, and no request', async () => {
  installDom()
  const fetchFn = heads()
  const p = createPresence(null, { fetchFn, pageUrl: PAGE, here: { name: 'results', key: 'a' } })
  assert.equal(p.dots(CID, { hereHeld: false }).getAttribute('title'), 'Not in results')
  assert.equal(fetchFn.asked.length, 0)
})

test('two members: the open one is held at once, the other is a pending ring, then a HEAD of its block URL fills it', async () => {
  installDom()
  const fetchFn = heads()
  const p = createPresence(TWO, { fetchFn, pageUrl: PAGE })
  const node = p.dots(CID)
  assert.deepEqual(classes(node), ['dot a', 'dot ring pending b'])
  assert.equal(node.getAttribute('title'), 'Held in lab; checking org')
  assert.equal(node.dataset.state, 'pending')
  await frame()
  assert.deepEqual(classes(node), ['dot a', 'dot b'])
  assert.equal(node.getAttribute('title'), 'Held in lab, org')
  assert.equal(node.dataset.state, 'done')
  assert.deepEqual(fetchFn.asked.map(a => [a.method, a.url]), [['HEAD', `http://h/m/org/blocks/${CID.slice(-2)}/${CID}`]], 'only the other member, at BlockFetcher\'s URL')
})

test('404 is a ring, and the tooltip names who holds it and who does not', async () => {
  installDom()
  const p = createPresence(THREE, { fetchFn: heads((alias) => (alias === 'ana-hpc' ? 404 : 200)), pageUrl: PAGE })
  const node = p.dots(CID)
  await frame()
  assert.deepEqual(classes(node), ['dot a', 'dot b', 'dot ring c'])
  assert.equal(node.getAttribute('title'), 'Held in roblab, org-s3; not in ana-hpc')
  assert.equal(node.getAttribute('aria-label'), 'Held in roblab, org-s3; not in ana-hpc')
})

test('any other answer, or no answer, is unknown: no dot, and the tooltip says it could not check', async () => {
  installDom()
  const p = createPresence(THREE, { fetchFn: heads((alias) => (alias === 'org-s3' ? 500 : new TypeError('Failed to fetch'))), pageUrl: PAGE })
  const node = p.dots(CID)
  await frame()
  assert.deepEqual(classes(node), ['dot a'])
  assert.equal(node.getAttribute('title'), "Held in roblab; couldn't check org-s3, ana-hpc")
})

test('the page knowing it is missing here makes the open member a ring without asking it', async () => {
  installDom()
  const fetchFn = heads()
  const p = createPresence(TWO, { fetchFn, pageUrl: PAGE })
  const node = p.dots(CID, { hereHeld: false })
  await frame()
  assert.deepEqual(classes(node), ['dot ring a', 'dot b'])
  assert.equal(node.getAttribute('title'), 'Held in org; not in lab')
  assert.deepEqual(fetchFn.asked.map(a => a.alias), ['org'])
})

test('hereHeld null asks the open member too, with more than one member', async () => {
  installDom()
  const fetchFn = heads((alias) => (alias === 'lab' ? 404 : 200))
  const p = createPresence(TWO, { fetchFn, pageUrl: PAGE })
  const states = await p.states(CID, { hereHeld: null })
  assert.deepEqual(states.map(s => [s.alias, s.here, s.state]), [['lab', true, 'not'], ['org', false, 'held']])
  assert.deepEqual(fetchFn.asked.map(a => a.alias).sort(), ['lab', 'org'])
})

test('one HEAD per member and block per page load: a second dots() is a cache hit; invalidate asks again', async () => {
  installDom()
  const fetchFn = heads()
  const p = createPresence(TWO, { fetchFn, pageUrl: PAGE })
  p.dots(CID)
  await frame()
  const again = p.dots(CID)
  assert.deepEqual(classes(again), ['dot a', 'dot b'], 'drawn from the cache at once')
  await p.states(CID)
  assert.equal(fetchFn.asked.length, 1)
  p.invalidate()
  await p.states(CID)
  assert.equal(fetchFn.asked.length, 2, 'after a copy the member is asked again')
  p.invalidate('bafyother')
  await p.states(CID)
  assert.equal(fetchFn.asked.length, 2, 'forgetting another block keeps this one')
})

test(`at most ${IN_FLIGHT} HEADs in flight, fewer than the browser's six connections per host`, async () => {
  assert.ok(IN_FLIGHT < 6, 'the page keeps a connection for its own reads')
  installDom()
  let open = 0
  let most = 0
  const waiting = []
  const fetchFn = async () => {
    open++
    most = Math.max(most, open)
    await new Promise(resolve => waiting.push(resolve))
    open--
    return new Response(null, { status: 200 })
  }
  const p = createPresence(THREE, { fetchFn, pageUrl: PAGE })
  const all = Promise.all(Array.from({ length: 10 }, (_, i) => p.states(`${CID}${i}`)))
  await frame()
  assert.equal(open, IN_FLIGHT)
  while (waiting.length) { waiting.shift()(); await new Promise(r => setTimeout(r, 0)) }
  const done = await all
  assert.equal(most, IN_FLIGHT)
  assert.equal(done.length, 10)
  assert.ok(done.every(states => states.every(s => s.state === 'held')))
})

test('in all-stores mode the open member is the primary', async () => {
  const listing = { write: true, members: [{ alias: 'org', writable: false, base: 'm/org/' }, { alias: 'lab', writable: true, base: 'm/lab/' }] }
  const store = await resolveStore('http://h/?member=*', { fetchFn: async () => new Response(JSON.stringify(listing), { headers: { 'Content-Type': 'application/json' } }) })
  const members = createMembers(store, { openExplorer: async () => ({}) })
  const fetchFn = heads()
  const p = createPresence(members, { fetchFn, pageUrl: 'http://h/?member=*' })
  assert.equal(p.open, 'lab')
  const states = await p.states(CID)
  assert.deepEqual(states.map(s => [s.alias, s.here]), [['org', false], ['lab', true]])
  assert.deepEqual(fetchFn.asked.map(a => a.alias), ['org'])
})

test('the tooltip words, and what a disabled Download says', () => {
  assert.equal(presenceTitle([{ alias: 'a', state: 'not' }, { alias: 'b', state: 'not' }]), 'Not in a, b')
  const s = (alias, state, here = false) => ({ alias, state, here })
  assert.equal(elsewhereText([s('lab', 'not', true), s('org', 'held'), s('ana', 'not')], 'lab'), 'Not in lab. Held in org.')
  assert.equal(elsewhereText([s('lab', 'not', true), s('org', 'pending')], 'lab'), 'Not in lab. Checking the other stores.')
  assert.equal(elsewhereText([s('lab', 'not', true), s('org', 'unknown')], 'lab'), "Not in lab. Couldn't check org.")
  assert.equal(elsewhereText([s('lab', 'not', true), s('org', 'not')], 'lab'), 'Not in lab. No other store holds it.')
  assert.equal(elsewhereText([s('lab', 'not', true)], 'lab'), 'Not in lab.')
})

// ---- the one-member budget, over real views ---------------------------------

/** One member opened the way app.js opens it, every request recorded in `asked`. */
async function oneMember(built) {
  const asked = []
  const listing = { members: [{ alias: 'lab', writable: true, base: 'm/lab/' }] }
  const blocks = blockFetch(built.blocks)
  const fetchFn = async (url, init = {}) => {
    const u = String(url)
    asked.push(`${init.method ?? 'GET'} ${u}`)
    if (u.endsWith('/members.json')) return new Response(JSON.stringify(listing), { headers: { 'Content-Type': 'application/json' } })
    if (u.endsWith('/log/')) return new Response(JSON.stringify({ entries: built.log }), { headers: { 'Content-Type': 'application/json' } })
    return blocks(url, init)
  }
  const store = await resolveStore(PAGE, { fetchFn })
  const primaryBlocks = new BlockFetcher(store.base, { fetchFn })
  const members = createMembers(store, { openExplorer: memberOpener(store, { pageUrl: PAGE, primaryBlocks,
    openDb: async (url) => { await fetchFn(url); return snapshotDb(built.snapshot) }, fetchFn }) })
  const ex = await members.explorer(store.member)
  return { ex, members, fetchFn, asked }
}

const ctxWith = (presence) => ({ tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
  store: { name: 'lab', key: 'a', label: 'lab', others: () => [] }, ...(presence ? { presence } : {}),
  write: { available: false, here: false, reason: 'read only', hrefFor: (x) => x } })

test('with one member the item, file and block pages make no HEAD and no more requests than without presence', async () => {
  const built = await buildMember({ now: Date.now() })
  const pages = [
    (ex, ctx) => item(ex, built.runs.R1.collection, built.item.A, ctx),
    (ex, ctx) => content(ex, built.content.A, ctx),
    (ex, ctx) => blockView(ex, built.item.A, ctx),
  ]
  for (const draw of pages) {
    installDom(); resetFolds()
    const withDots = await oneMember(built)
    const opened = withDots.asked.length
    const page = await draw(withDots.ex, ctxWith(createPresence(withDots.members, { fetchFn: withDots.fetchFn, pageUrl: PAGE })))
    await frame()
    const drawn = withDots.asked.slice(opened)
    assert.ok(page.querySelectorAll('.presence').length >= 1, 'the page shows presence')
    assert.ok(page.querySelectorAll('.presence').every(p => p.querySelectorAll('.dot').length === 1), 'one dot each')
    assert.deepEqual(drawn.filter(a => a.startsWith('HEAD')), [], 'no HEAD')

    installDom(); resetFolds()
    const before = await oneMember(built)
    const start = before.asked.length
    await draw(before.ex, ctxWith(null))
    await frame()
    assert.deepEqual(drawn, before.asked.slice(start), 'the same requests as the page drew before presence')
  }
})
