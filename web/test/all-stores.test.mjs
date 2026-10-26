// web/test/all-stores.test.mjs
// All-stores lists (P3 Task 5, rule D5): Pipelines, Selections, Search and
// Storage over several members, the list helpers they share, and the gate
// that a page with one member asks for exactly what it asked for before.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { frame, installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { createMembers, fromEach, hrefIn, listedStates, listsAll, mergeRows, unreadableText } from '../src/members.js'
import { createPresence, knownDots } from '../src/presence.js'
import { drawStores } from '../src/nav.js'
import { home } from '../src/views/home.js'
import { selections } from '../src/views/selection.js'
import { search } from '../src/views/search.js'
import { CARDS_CAPTION, storage } from '../src/views/storage.js'
import { SnapshotError } from '../src/db.js'
import { buildMember, blockFetch } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'
import { ROBLAB, openRoblab } from './storage-fixture.mjs'

const PAGE = 'http://h/index.html'
const settle = async () => { await frame(); await frame(); await new Promise(r => setTimeout(r, 10)) }
const text = (el) => el.textContent.replace(/\s+/g, ' ')
const noSnapshot = () => new SnapshotError('no_snapshot', 'no Index Snapshot at http://h/m/ana/index/v6.sqlite (HTTP 404)')

const MEMBERS = [
  { alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null },
  { alias: 'org', writable: false, base: 'm/org/', kind: 's3', location: 's3://org-bucket/blocks' },
  { alias: 'ana', writable: false, base: 'm/ana/', kind: 'local', location: null },
]

/** A page's store, members and ctx over `exes` (alias -> explorer); a member missing from `exes` has no snapshot. */
function world(exes, { all = true, primary = 'lab', list = MEMBERS, viewer = null, heads = () => 404 } = {}) {
  const store = { member: primary, all, members: list }
  const writable = list.find(m => m.writable)?.alias ?? null
  const members = createMembers(store, { openExplorer: async (m) => { if (!exes[m.alias]) throw noSnapshot(); return exes[m.alias] } })
  const asked = []
  const fetchFn = async (url, init = {}) => {
    const [, alias] = String(url).match(/\/m\/([^/]+)\//)
    asked.push({ alias, method: init.method, url: String(url) })
    return new Response(null, { status: heads(alias) })
  }
  const presence = createPresence(members, { fetchFn, pageUrl: PAGE })
  const ctx = {
    tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, mark: () => {}, use: () => {},
    members, presence, viewer, identity: null,
    store: { name: primary, key: 'a', all, hrefIn: (alias, hash) => (alias === primary && !all ? hash : `${PAGE}?member=${alias}${hash}`) },
    write: { available: true, here: primary === writable, writable, reason: null, hrefFor: (h) => h, writer: null, run: () => {} },
  }
  return { ctx, members, presence, asked }
}

// ---- the list helpers ------------------------------------------------------

test('listsAll: only ?member=* with more than one member', () => {
  const two = { list: [{}, {}] }
  assert.equal(listsAll({ store: { all: true }, members: two }), true)
  assert.equal(listsAll({ store: { all: false }, members: two }), false)
  assert.equal(listsAll({ store: { all: true }, members: { list: [{}] } }), false, 'one member lists itself as before')
  assert.equal(listsAll({ store: { all: true } }), false)
  assert.equal(listsAll(null), false)
})

test('fromEach asks every member in members.json order; a member that fails is named, the others answer', async () => {
  const { members } = world({ lab: { n: 1 }, org: { n: 2, broken: true } })
  const answers = await fromEach(members, async (ex) => { if (ex.broken) throw new Error('boom'); return ex.n })
  assert.deepEqual(answers.map(a => [a.member.alias, a.value ?? null, a.state ?? null]), [['lab', 1, null], ['org', null, 'failed'], ['ana', null, 'no_snapshot']])
  assert.equal(unreadableText(answers[2], 'Selections'), 'ana has no snapshot yet, so its Selections are not listed.')
  assert.equal(unreadableText(answers[1], 'results'), 'org could not be read (boom), so its results are not listed.')
})

test('a member whose only snapshot is older than the page reads is named as such, and how to rewrite it', () => {
  const older = Object.assign(new SnapshotError('no_snapshot', 'older'), { older: 'index/v4.sqlite' })
  assert.equal(unreadableText({ member: { alias: 'ana' }, state: 'no_snapshot', error: older }, 'Selections'),
    "ana's snapshot is older than this explorer reads, so its Selections are not listed; nextflow plugin nf-blocks:snapshot rewrites it.")
})

test('mergeRows: one row per key, the first member\'s, with every member that lists it; listedStates draws them', () => {
  const m = (alias, key) => ({ alias, key })
  const answers = [{ member: m('lab', 'a'), ex: 'L', value: [{ k: 1 }, { k: 2 }] }, { member: m('org', 'b'), ex: 'O', value: [{ k: 2, org: true }, { k: 3 }] },
    { member: m('ana', 'c'), state: 'failed', error: new Error('x') }]
  const rows = mergeRows(answers, v => v, r => r.k)
  assert.deepEqual(rows.map(r => [r.key, r.member.alias, r.ex, r.heldIn]), [[1, 'lab', 'L', ['lab']], [2, 'lab', 'L', ['lab', 'org']], [3, 'org', 'O', ['org']]])
  assert.equal(rows[1].row.org, undefined, 'the first member\'s row is kept')
  assert.deepEqual(listedStates(answers, ['org']).map(s => s.state), ['not', 'held', 'unknown'])
  installDom()
  const dots = knownDots(listedStates(answers, ['lab', 'org']))
  assert.deepEqual(dots.querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot a', 'dot b'])
  assert.equal(dots.getAttribute('title'), "Held in lab, org; couldn't check ana")
  assert.equal(knownDots(listedStates(answers, ['lab']), { title: 'counts' }).getAttribute('title'), 'counts')
})

test('hrefIn: ctx.store.hrefIn when the page gives one, else the hash itself', () => {
  assert.equal(hrefIn({ store: { hrefIn: (a, h) => `x?member=${a}${h}` } }, 'org', '#/run/r'), 'x?member=org#/run/r')
  assert.equal(hrefIn({ store: {} }, 'org', '#/run/r'), '#/run/r')
})

test('presence: stores the page knows hold a block are drawn held and never asked', async () => {
  installDom()
  const { presence, asked } = world({ lab: {}, org: {} }, { heads: () => 200 })
  const node = presence.dots('bafkreiabc', { hereHeld: null, held: ['org'] })
  await settle()
  assert.deepEqual(asked.map(a => a.alias).sort(), ['ana', 'lab'], 'only the stores not known')
  assert.deepEqual(node.querySelectorAll('.dot').map(d => d.dataset.held), ['held', 'held', 'held'])
})

test('the Stores section marks no member as open in all-stores mode', () => {
  installDom()
  const el = document.createElement('div')
  const { members } = world({ lab: {} })
  const figure = () => ({ state: 'loading', stored: null })
  drawStores(el, { store: { member: 'lab', all: true }, members: members.list, figure, href: a => a })
  assert.equal(el.querySelectorAll('a.on').length, 0)
  assert.ok(el.querySelectorAll('a').every(a => !a.hasAttribute('aria-current')))
  drawStores(el, { store: { member: 'lab', all: false }, members: members.list, figure, href: a => a })
  assert.deepEqual(el.querySelectorAll('a').map(a => a.classList.contains('on')), [true, false, false])
})

// ---- Pipelines --------------------------------------------------------------

const named = (cid, name, at = '2026-10-24T19:40:16.459Z') =>
  claimState([{ cid: `k-${cid}`, subject: cid, verb: 'set', attribute: 'name', value: name, timestamp: at, asserted_by: 'anonymous', supersedes: [] }])

function homeEx({ pipelines, counts, runs, selections = [], size = 142_000_000 }) {
  return {
    stale: [],
    pipelines: async () => pipelines,
    runCounts: async () => new Map(Object.entries(counts)),
    runsOfPipeline: async (p) => runs.filter(r => r.pipeline === p),
    completionOf: async () => ({ anomalies: {} }),
    latestSuccessfulRun: async (p) => runs.find(r => r.pipeline === p && r.status === 'succeeded')?.completion_cid ?? null,
    runRow: async (cid) => runs.find(r => r.completion_cid === cid),
    pipelineSize: async () => ({ bytes: size, blocks: 3, partial: false }),
    selectionPage: async ({ limit = 25 } = {}) => ({ rows: selections.slice(0, limit), hiddenCount: 0, first: selections.length ? 1 : 0,
      last: selections.length, total: selections.length, prev: null, next: null }),
    selection: async () => ({ members: [{}, {}] }),
    selectionSize: async () => ({ bytes: 1_135_852, blocks: 4, files: 3, partial: false }),
    recentActivity: async () => [],
    storeStats: async () => { throw new Error('the all-stores home never shows one store\'s bytes as everyone\'s') },
  }
}

const run = (cid, pipeline, name, finished, status = 'succeeded') => ({ completion_cid: cid, pipeline, run_name: name, status, finished_at: finished, possibly_incomplete: 0 })
const LAB_RUNS = [run('r3', 'nf-core/rnaseq', 'distraught_murdock', '2026-10-24T17:29:03.967Z'), run('m1', 'main.nf', 'sleepy_heyrovsky', '2026-10-24T17:58:50.649Z')]
const ORG_RUNS = [run('o1', 'nf-core/rnaseq', 'copied_murdock', '2026-10-25T09:00:00.000Z'), run('o2', 'nf-core/sarek', 'brave_curie', '2026-10-20T09:00:00.000Z')]
const sel = (cid, name, firstSeen) => ({ cid, firstSeen, assertedBy: 'anonymous', source: 'snapshot', error: null, state: named(cid, name) })

function homeWorld(options) {
  return world({
    lab: homeEx({ runs: LAB_RUNS, pipelines: [{ pipeline: 'main.nf', runs: 1, latest: LAB_RUNS[1].finished_at }, { pipeline: 'nf-core/rnaseq', runs: 3, latest: LAB_RUNS[0].finished_at }],
      counts: { 'main.nf': { runs: 1, ok: 1, failed: 0 }, 'nf-core/rnaseq': { runs: 3, ok: 2, failed: 1 } }, selections: [sel('sel1', 'wigshop', '2026-10-24T19:40:00.000Z')] }),
    org: homeEx({ runs: ORG_RUNS, size: 9_000_000_000,
      pipelines: [{ pipeline: 'nf-core/rnaseq', runs: 2, latest: ORG_RUNS[0].finished_at }, { pipeline: 'nf-core/sarek', runs: 1, latest: ORG_RUNS[1].finished_at }],
      counts: { 'nf-core/rnaseq': { runs: 2, ok: 2, failed: 0 }, 'nf-core/sarek': { runs: 1, ok: 1, failed: 0 } },
      selections: [sel('sel2', 'tumours', '2026-10-25T10:00:00.000Z'), sel('sel1', 'wigshop', '2026-10-24T19:40:00.000Z')] }),
  }, options)
}

test('home, all stores: one row per pipeline across the members, dots for the stores holding its runs, links into the first', async () => {
  installDom()
  const { ctx, members } = homeWorld()
  const page = await home(await members.explorer('lab'), ctx)
  await settle()
  const rows = page.querySelectorAll('tr[data-pipeline]')
  assert.deepEqual(rows.map(r => r.dataset.pipeline), ['main.nf', 'nf-core/rnaseq', 'nf-core/sarek'], 'newest first by the row\'s own store')
  const rna = rows[1]
  const link = rna.querySelector('.namecell').querySelector('a')
  assert.equal(link.getAttribute('href'), `${PAGE}?member=lab#/pipeline/nf-core%2Frnaseq`)
  assert.equal(link.getAttribute('title'), 'Opens in lab')
  const dots = rna.querySelector('.presence')
  assert.deepEqual(dots.querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot a', 'dot b'], 'ana could not be checked: no dot')
  assert.equal(dots.getAttribute('title'),
    "Runs per store: lab 3 runs (2 ok, 1 failed); org 2 runs (2 ok); ana couldn't be checked. This row shows lab, where its links open.")
  assert.equal(rna.querySelectorAll('td')[1].textContent, '3 · 2 ok · 1 failed', 'the Runs cell is the row\'s store\'s, never a sum')
  assert.equal(rna.querySelectorAll('td')[4].textContent, '142 MB')
  assert.match(rna.querySelectorAll('td')[4].getAttribute('title'), /^Stored in lab\. /, 'the figure names its store')
  assert.ok(rna.querySelectorAll('a').some(a => a.getAttribute('href') === `${PAGE}?member=lab#/run/r3`), 'the latest good run opens in lab too')
  const sarek = rows[2]
  assert.equal(sarek.querySelector('.namecell').querySelector('a').getAttribute('href'), `${PAGE}?member=org#/pipeline/nf-core%2Fsarek`)
  assert.deepEqual(sarek.querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot ring a', 'dot b'])
  assert.equal(sarek.querySelectorAll('td')[4].textContent, '9 GB')
  assert.match(sarek.querySelectorAll('td')[4].getAttribute('title'), /^Stored in org\. /)
  assert.match(sarek.querySelector('.presence').getAttribute('title'), /lab none; org 1 run \(1 ok\)/)
  const gone = page.querySelector('tr[data-member-unreadable=ana]')
  assert.equal(gone.textContent, 'ana has no snapshot yet, so its pipelines are not listed.')
  const head = text(page.querySelector('.page-head'))
  assert.match(head, /All stores/)
  assert.match(head, /3 pipelines/)
  assert.ok(!page.querySelector('[data-store-figures]'), 'no store-wide bytes in all-stores mode')
})

test('home, all stores: Selections from every member, one per cid, each with dots and a link into its first member', async () => {
  installDom()
  const { ctx, members, asked } = homeWorld()
  const page = await home(await members.explorer('lab'), ctx)
  await settle()
  const sels = page.querySelectorAll('[data-home-selection]')
  assert.deepEqual(sels.map(s => s.dataset.homeSelection), ['sel2', 'sel1'], 'newest first seen first, sel1 once')
  assert.equal(sels[0].querySelector('a').getAttribute('href'), `${PAGE}?member=org#/selection/sel2`)
  assert.equal(sels[1].querySelector('a').getAttribute('href'), `${PAGE}?member=lab#/selection/sel1`)
  const held = (s) => s.querySelector('.presence').querySelectorAll('.dot').map(d => `${d.dataset.store}:${d.dataset.held}`)
  assert.deepEqual(held(sels[1]), ['lab:held', 'org:held', 'ana:not'])
  assert.deepEqual(held(sels[0]), ['lab:not', 'org:held', 'ana:not'])
  assert.deepEqual([...new Set(asked.map(a => a.alias))].sort(), ['ana', 'lab'], 'HEADs only where no index listed the Selection')
  assert.match(text(page), /ana has no snapshot yet, so its Selections are not listed\./)
})

test('home with several members but one open (not all stores) is the one member\'s page', async () => {
  installDom()
  const { ctx, members } = homeWorld({ all: false })
  members.explorer('lab').then(ex => { ex.storeStats = async () => null })
  const page = await home(await members.explorer('lab'), ctx)
  await settle()
  assert.deepEqual(page.querySelectorAll('tr[data-pipeline]').map(r => r.dataset.pipeline), ['main.nf', 'nf-core/rnaseq'])
  assert.ok(!page.querySelector('.presence'))
  assert.equal(page.querySelector('[data-home-selection]').querySelector('a').getAttribute('href'), '#/selection/sel1')
})

// ---- Selections ---------------------------------------------------------------

test('Selections, all stores: every member\'s rows, one per cid, with dots; Undo only where the writable member lists it', async () => {
  installDom()
  const deleted = (cid, name) => ({ ...sel(cid, name, '2026-10-24T10:00:00.000Z'),
    state: claimState([{ cid: `d-${cid}`, subject: cid, verb: 'delete', attribute: null, value: null, timestamp: '2026-10-24T11:00:00.000Z', asserted_by: 'anonymous', supersedes: [] }]) })
  const page = (rows) => async () => ({ rows, hiddenCount: 0, first: rows.length ? 1 : 0, last: rows.length, total: rows.length, prev: null, next: null })
  const { ctx, members } = world({
    lab: { selectionPage: page([sel('sel1', 'wigshop', '2026-10-24T19:40:00.000Z')]) },
    org: { selectionPage: page([sel('sel2', 'tumours', '2026-10-25T10:00:00.000Z'), sel('sel1', 'wigshop', '2026-10-24T19:40:00.000Z')]) },
  })
  const node = await selections(await members.explorer('lab'), { offset: 0 }, ctx)
  await settle()
  const rows = node.querySelectorAll('tr[data-selection]')
  assert.deepEqual(rows.map(r => r.dataset.selection), ['sel2', 'sel1'])
  assert.equal(rows[0].querySelector('a').getAttribute('href'), `${PAGE}?member=org#/selection/sel2`)
  assert.equal(rows[1].querySelector('a').getAttribute('title'), 'Opens in lab')
  assert.deepEqual(rows[1].querySelector('.presence').querySelectorAll('.dot').map(d => `${d.dataset.store}:${d.dataset.held}`), ['lab:held', 'org:held', 'ana:not'])
  assert.equal(text(node.querySelector('[data-page]')), 'The newest Selections in each store: lab has 1, org has 2.')
  assert.equal(node.querySelector('[data-member-unreadable=ana]').textContent, 'ana has no snapshot yet, so its Selections are not listed.')

  const d = world({
    lab: { selectionPage: page([deleted('sel1', 'wigshop')]) },
    org: { selectionPage: page([deleted('sel3', 'old')]) },
  })
  d.ctx.write.writer = {}
  const del = await selections(await d.members.explorer('lab'), { offset: 0, deleted: true }, d.ctx)
  const undo = del.querySelectorAll('[data-undo]').map(b => b.dataset.undo)
  assert.deepEqual(undo, ['sel1'], 'sel3 is only in org: its deletion Claims are not the writable member\'s')
})

// ---- Search ---------------------------------------------------------------------

const sample = (item, collection, completion, s, finished, pipeline = 'demo') => ({ item_cid: item, collection_cid: collection, completion_cid: completion,
  sample: s, path: 'sample', paths: ['sample'], output_name: 'aligned', run_name: `run-${completion}`, status: 'succeeded', finished_at: finished, pipeline })

function searchEx({ samples = [], addresses = [], holding = {} }) {
  return {
    searchAddresses: async () => addresses,
    searchSamples: async () => Object.defineProperty([...samples], 'more', { value: false }),
    selectionsHolding: async (item) => holding[item] ?? [],
    claimStates: async (cids) => new Map(cids.map(c => [c, named(c, `name-${c}`)])),
    itemSize: async () => ({ bytes: 1000, blocks: 1, partial: false }),
  }
}

test('search, all stores: results from every member, one per item, with dots and links into the first member', async () => {
  installDom()
  const { ctx, members } = world({
    lab: searchEx({ samples: [sample('i1', 'c1', 'r1', 'A1', '2026-10-24T10:00:00.000Z')], holding: { i1: ['s1'] } }),
    org: searchEx({ samples: [sample('i2', 'c2', 'r2', 'A2', '2026-10-25T10:00:00.000Z'), sample('i1', 'c1', 'r1', 'A1', '2026-10-24T10:00:00.000Z')],
      holding: { i1: ['s1'], i2: ['s2'] } }),
  })
  const node = await search(await members.explorer('lab'), 'A', ctx)
  await settle()
  assert.match(text(node.querySelector('.page-head')), /in all stores/)
  const items = node.querySelector('[data-search-group=items]').querySelectorAll('tr[data-item]')
  assert.deepEqual(items.map(r => r.dataset.item), ['i2', 'i1'], 'latest run first, i1 once')
  assert.equal(items[0].querySelector('a').getAttribute('href'), `${PAGE}?member=org#/item/c2/i2`)
  assert.equal(items[1].querySelector('a').getAttribute('href'), `${PAGE}?member=lab#/item/c1/i1`)
  assert.deepEqual(items[1].querySelector('.presence').querySelectorAll('.dot').map(d => `${d.dataset.store}:${d.dataset.held}`), ['lab:held', 'org:held', 'ana:not'])
  const samples = node.querySelector('[data-search-group=samples]').querySelectorAll('tr[data-sample]')
  assert.deepEqual(samples.map(r => r.dataset.sample), ['A2', 'A1'])
  assert.match(samples[1].querySelector('a').getAttribute('href'), /^http:\/\/h\/index\.html\?member=lab#\/run\/r1\?tab=samples/)
  const sels = node.querySelector('[data-search-group=selections]').querySelectorAll('tr[data-selection]')
  assert.deepEqual(sels.map(r => r.dataset.selection), ['s1', 's2'])
  assert.equal(sels[1].querySelector('a').getAttribute('href'), `${PAGE}?member=org#/selection/s2`)
  assert.deepEqual(sels[0].querySelector('.presence').querySelectorAll('.dot').map(d => `${d.dataset.store}:${d.dataset.held}`), ['lab:held', 'org:held', 'ana:not'])
  assert.equal(node.querySelector('[data-member-unreadable=ana]').textContent, 'ana has no snapshot yet, so its results are not listed.')
})

test('search, all stores: nothing matches, so every tab counts 0, the Selections tab too', async () => {
  installDom()
  const { ctx, members } = world({ lab: searchEx({}), org: searchEx({}) })
  for (const tab of [null, 'selections']) {
    const node = await search(await members.explorer('lab'), 'ZZZ', ctx, { tab })
    await settle()
    const count = (key) => node.querySelector(`[data-tab=${key}]`).querySelectorAll('small').map(s => s.textContent)
    assert.deepEqual(count('selections'), ['0'], `the Selections tab says 0 on ${tab ?? 'All'}`)
    assert.deepEqual(['samples', 'items', 'addresses'].map(count), [['0'], ['0'], ['0']])
  }
})

test('search, all stores: a member whose Selections cannot be read is named, and "No Selections" says it covers only the others', async () => {
  installDom()
  const org = searchEx({ samples: [sample('i1', 'c1', 'r1', 'A1', '2026-10-24T10:00:00.000Z')] })
  org.selectionsHolding = async () => { throw new Error('snapshot query failed') }
  const { ctx, members } = world({ lab: searchEx({ samples: [sample('i1', 'c1', 'r1', 'A1', '2026-10-24T10:00:00.000Z')] }), org })
  const node = await search(await members.explorer('lab'), 'A', ctx)
  await settle()
  const group = node.querySelector('[data-search-group=selections]')
  assert.equal(group.querySelectorAll('tr[data-selection]').length, 0)
  assert.equal(group.querySelector('[data-selections-unreadable=org]').textContent,
    'org could not be read (snapshot query failed), so its Selections are not listed.')
  assert.equal(group.querySelector('[data-selections-none]').textContent, 'No Selections in the other stores contain A.')
  assert.doesNotMatch(group.textContent, /^No Selections contain/)
})

test('search, all stores: an address found in two members is one result, and opens in the first', async () => {
  installDom()
  const cid = 'bafyreiabcdefghijklmnop'
  const found = [{ cid, kind: 'run', label: 'distraught_murdock', href: `#/run/${cid}` }]
  const { ctx, members } = world({ lab: searchEx({}), org: searchEx({ addresses: found }) })
  members.explorer('lab').then(ex => { ex.searchAddresses = async () => found })
  const went = []
  ctx.navigate = (href) => went.push(href)
  const node = await search(await members.explorer('lab'), cid.slice(0, 16), ctx)
  assert.deepEqual(went, [`${PAGE}?member=lab#/run/${cid}`])
  assert.equal(node.dataset.searchJump, `${PAGE}?member=lab#/run/${cid}`)
})

// ---- Storage ----------------------------------------------------------------------

const STORAGE_MEMBERS = [
  { alias: 'roblab', writable: true, base: 'm/roblab/', kind: 'local', location: null },
  { alias: 'org-s3', writable: false, base: 'm/org-s3/', kind: 's3', location: 's3://lab-blocks' },
  { alias: 'ana-hpc', writable: false, base: 'm/ana/', kind: 'posix', location: null },
]
const ORG = { storeStats: async () => ({ stored: 1.82e12, referenced: 5.6e12, dedup: 5.6 / 1.82, pinned: 640e9, held: 1020e9, releasing: 160e9, partial: false }),
  asserterStats: async () => [{ assertedBy: 'rob@example.org', reachable: 212e9 }], storageChanges: () => ({ claims: false, logged: false }), storageOmitted: null }

async function storageWorld({ all = true, viewer = 'rob@example.org', org = ORG } = {}) {
  const { explorer } = await openRoblab()
  const w = world({ roblab: explorer, 'org-s3': org }, { all, primary: 'roblab', list: STORAGE_MEMBERS, viewer })
  return { ...w, explorer }
}

for (const all of [true, false]) {
  test(`storage with three members (${all ? 'all stores' : 'one open'}): one card per store, never a total, and the open member broken down below`, async () => {
    installDom()
    const { ctx, explorer } = await storageWorld({ all })
    const node = await storage(explorer, ctx)
    await settle()
    const cards = node.querySelectorAll('[data-storage-card]')
    assert.deepEqual(cards.map(c => c.dataset.storageCard), ['roblab', 'org-s3', 'ana-hpc'])
    assert.ok(cards[0].classList.contains('on'))
    assert.equal(cards[0].getAttribute('aria-current'), 'true')
    assert.equal(cards[0].tagName, 'DIV', 'the card broken down below is not a link')
    assert.equal(cards[1].getAttribute('href'), `${PAGE}?member=org-s3#/storage`)
    assert.equal(text(cards[0].querySelector('[data-storage-summary]')), '142 MB stored · 220 MB referenced · 1.55×')
    assert.match(text(cards[0]), /local · writable/)
    assert.equal(text(cards[1].querySelector('[data-storage-summary]')), '1.82 TB stored · 5.6 TB referenced · 3.08×')
    assert.match(text(cards[1]), /s3:\/\/lab-blocks/)
    assert.ok(cards[1].querySelector('[data-read-only]'), 'org-s3 is not writable here: a lock')
    assert.match(text(cards[1].querySelector('.sbar')), /Pinned 640 GB.*Held 1\.02 TB.*Releasing 160 GB/)
    assert.match(text(cards[1].querySelector('.mine')), /Yours.*212 GB · 12%/)
    assert.match(text(cards[0].querySelector('.mine')), /Yours.*0 B · 0%/, 'a known viewer with no roots in roblab holds 0 B of it')
    assert.match(text(cards[2]), /ana-hpc.*posix.*View only.*No snapshot yet\.$/)
    assert.doesNotMatch(text(node), /Read only/)
    assert.match(cards[1].querySelector('[data-read-only]').getAttribute('title'), /^This page can't write to org-s3\./)
    assert.equal(node.querySelectorAll('.sbar').length, 2, 'one bar per store with figures; no combined bar')
    assert.equal(node.querySelector('[data-storage-caption]').textContent, CARDS_CAPTION)
    assert.equal(CARDS_CAPTION, 'Each store keeps its own pins and runs its own cleanup.')
    assert.ok(!node.querySelector('.storage-lead'), 'the caption replaces the single-store lead')
    assert.ok(!node.querySelector('.storage-summary'), 'no R9 single summary')
    assert.match(text(node.querySelector('.storage-detail')), /roblab in detail/)
    assert.equal(node.querySelectorAll('[data-storage-run]').length, 4, 'the breakdown is roblab\'s')
    assert.doesNotMatch(text(node), /1\.96 TB|undefined|NaN/)
  })
}

test('storage: a member whose figures fail or that has no figures says so in its card and leaves the others', async () => {
  installDom()
  const { ctx, explorer } = await storageWorld({ org: { ...ORG, storeStats: async () => null, storageOmitted: 'over_cap' } })
  const node = await storage(explorer, ctx)
  await settle()
  const cards = node.querySelectorAll('[data-storage-card]')
  assert.match(text(cards[1]), /Its snapshot leaves out storage figures\./)
  assert.equal(text(cards[0].querySelector('[data-storage-summary]')), '142 MB stored · 220 MB referenced · 1.55×')
})

test('storage: a member whose only snapshot is older than the page reads says so in its card, and how to rewrite it', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const older = Object.assign(new SnapshotError('no_snapshot', 'older'), { older: 'index/v4.sqlite' })
  const { ctx } = world({ roblab: explorer, 'org-s3': ORG, 'ana-hpc': { then: (resolve, reject) => reject(older) } },
    { primary: 'roblab', list: STORAGE_MEMBERS })
  const node = await storage(explorer, ctx)
  await settle()
  assert.match(text(node.querySelectorAll('[data-storage-card]')[2]),
    /Its snapshot is older than this explorer reads; nextflow plugin nf-blocks:snapshot rewrites it\.$/)
})

test('storage with one member keeps the single-store page: no cards, the lead, one summary', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const { ctx } = world({ roblab: explorer }, { primary: 'roblab', list: [STORAGE_MEMBERS[0]] })
  const node = await storage(explorer, ctx)
  await settle()
  assert.ok(!node.querySelector('[data-storage-cards]'))
  assert.ok(node.querySelector('.storage-lead'))
  assert.equal(node.querySelectorAll('.storage-summary').length, 1)
  assert.equal(ROBLAB.stored > 0, true)
})

// ---- the gate: one member asks for no more than before -------------------------------

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const ONE = [{ alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null }]

/**
 * One view over one fixture member, opened fresh with every fetch and query
 * counted: `members` false is the page before P3 (no members, no presence);
 * true gives it ctx.members and ctx.presence for one member, in `all` mode or not.
 */
async function onePage(member, view, { members: withMembers, all = false }) {
  const asked = []
  let queries = 0
  const blocks = blockFetch(member.blocks)
  const fetchFn = async (url, init = {}) => { asked.push(`${init.method ?? 'GET'} ${url}`); return blocks(url, init) }
  const ex = await Explorer.open({
    base: 'http://h/m/lab/', blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    openDb: async () => { const db = await snapshotDb(member.snapshot); return { ...db, query: (sql, p) => { queries++; return db.query(sql, p) } } },
    listFn: async () => ({ names: member.log, readable: true }), now: () => NOW,
  })
  const ctx = { tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, mark: () => {}, use: () => {}, viewer: null, identity: null,
    store: { name: 'lab', key: 'a' }, write: { available: false, here: false, reason: 'read only', hrefFor: (h) => h } }
  if (withMembers) {
    const members = createMembers({ member: 'lab', all, members: ONE }, { openExplorer: async () => ex })
    Object.assign(ctx, { members, presence: createPresence(members, { fetchFn, pageUrl: PAGE }) })
    ctx.store = { ...ctx.store, all, hrefIn: (alias, hash) => (alias === 'lab' && !all ? hash : `${PAGE}?member=${alias}${hash}`) }
  }
  const before = { asked: asked.length, queries }
  installDom()
  const node = await view(ex, ctx)
  await settle()
  return { asked: asked.slice(before.asked), queries: queries - before.queries, text: text(node) }
}

const VIEWS = {
  home: (ex, ctx) => home(ex, ctx),
  selections: (ex, ctx) => selections(ex, { offset: 0 }, ctx),
  search: (ex, ctx) => search(ex, 'A', ctx),
  storage: (ex, ctx) => storage(ex, ctx),
}

test('one member, in either mode: home, Selections, search and Storage ask for exactly what they asked for before', async () => {
  const member = await buildMember({ now: NOW })
  for (const [name, view] of Object.entries(VIEWS)) {
    const before = await onePage(member, view, { members: false })
    for (const all of [false, true]) {
      const now = await onePage(member, view, { members: true, all })
      assert.deepEqual(now.asked, before.asked, `${name} (all ${all}) fetches`)
      assert.equal(now.queries, before.queries, `${name} (all ${all}) snapshot queries`)
      assert.equal(now.text, before.text, `${name} (all ${all}) renders the same page`)
      assert.ok(!now.asked.some(u => u.startsWith('HEAD')), `${name}: no presence HEAD`)
    }
    assert.ok(before.queries > 0, `${name} reads the snapshot`)
  }
})
