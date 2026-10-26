// web/test/storage-view.test.mjs
// The Storage view (#/storage, P2 task 6; mockups/09-storage.html) on the
// roblab-figure snapshot, and the left column's Storage link and store size.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { frame, installDom } from './dom.mjs'
import { ROBLAB, openRoblab, openSnapshots, roblabLeanSnapshot } from './storage-fixture.mjs'
import { buildMember } from './fixture.mjs'
import { dayText } from '../src/words.js'
import { storage } from '../src/views/storage.js'
import { createNav } from '../src/nav.js'
import { showsRail } from '../src/panel.js'
import { APP_CSS } from '../src/styles.js'
import { readFileSync } from 'node:fs'

const READ_ONLY = { available: false, here: false, reason: 'Read only here.', hrefFor: (h) => h }
const WRITABLE = { available: true, here: true, reason: null, hrefFor: (h) => h }
const ctx = ({ write = READ_ONLY, viewer = null } = {}) => ({ store: { name: 'roblab', key: 'a' }, write, viewer,
  mark: () => {}, use: () => {}, rerender: () => {}, trayChanged: () => {} })
const settle = async () => { await frame(); await new Promise(r => setTimeout(r, 10)) }
const text = (el) => el.textContent.replace(/\s+/g, ' ')

async function page(options = {}, c = ctx()) {
  installDom()
  const { explorer } = await openRoblab(options)
  const node = await storage(explorer, c)
  await settle()
  return node
}

test('storage: a dedup ratio drops its trailing zeros', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const stats = await explorer.storeStats()
  for (const [dedup, says] of [[3.1, '3.1×'], [1, '1×']]) {
    explorer.storeStats = async () => ({ ...stats, dedup })
    const node = await storage(explorer, ctx())
    await settle()
    assert.equal(text(node.querySelector('[data-storage-summary]')), `142 MB stored · 220 MB referenced · ${says}`)
  }
})

const runRow = (node, name) => node.querySelectorAll('[data-storage-run]').find(r => r.textContent.includes(name))

test('storage: the head, the store summary and its bar', async () => {
  const node = await page()
  const head = node.querySelector('.page-head')
  assert.match(text(head), /^Storage/)
  assert.match(text(head), /as of snapshot · /)
  assert.match(text(head), /Each store keeps its own pins and runs its own cleanup, so every number here belongs to one store\./)
  assert.equal(text(node.querySelector('[data-storage-summary]')), '142 MB stored · 220 MB referenced · 1.55×')
  const bar = node.querySelector('.sbar')
  assert.ok(bar, 'the kit storage bar')
  assert.match(text(bar), /Pinned 0 B/)
  assert.match(text(bar), /Held 142 MB/)
  assert.match(text(bar), /Releasing 0 B/)
  assert.equal(node.querySelectorAll('.sbar').length, 1, 'one store, one bar, no card row')
  assert.match(text(node), /roblab in detail/)
  assert.doesNotMatch(text(node), /undefined|null|NaN/)
})

test('storage: by run, grouped by pipeline, with reachable and freed bytes', async () => {
  const node = await page({}, ctx({ write: WRITABLE }))
  const groups = node.querySelectorAll('[data-storage-pipeline]')
  assert.deepEqual(groups.map(g => g.dataset.storagePipeline), ['main.nf', 'nf-core/rnaseq'])
  assert.match(text(groups[1]), /nf-core\/rnaseq.*142 MB/)
  assert.match(text(groups[0]), /main\.nf.*0 B/)
  const rows = node.querySelectorAll('[data-storage-run]')
  assert.deepEqual(rows.map(r => r.dataset.storageRun),
    ['sleepy_heyrovsky', 'distraught_murdock', 'sad_yonath', 'sharp_magritte'].map(n => ROBLAB.runs[n].cid))
  assert.match(text(runRow(node, 'distraught_murdock')), /distraught_murdock.*78\.4 MB.*63\.7 MB/)
  assert.match(text(runRow(node, 'sad_yonath')), /sad_yonath.*78\.4 MB.*15\.6 MB/)
  assert.match(text(runRow(node, 'sharp_magritte')), /sharp_magritte.*53\.4 MB.*0 B · all shared/)
  assert.match(text(runRow(node, 'sharp_magritte')), /nf-core\/rnaseq · failed · /)
  assert.match(text(runRow(node, 'sleepy_heyrovsky')), /main\.nf · /)
  assert.doesNotMatch(text(runRow(node, 'sleepy_heyrovsky')), /all shared/)
  assert.match(text(runRow(node, 'distraught_murdock')), /Held/)
  assert.equal(runRow(node, 'distraught_murdock').querySelector('a').getAttribute('href'), `#/run/${ROBLAB.runs.distraught_murdock.cid}`)
  const head = node.querySelector('[data-storage-by-run]')
  assert.match(text(head), /62\.8 MB is shared by more than one run/)
  assert.ok(!node.querySelector('[data-run]') && !node.querySelector('[data-address]'), 'no Gate attributes on this page')
})

test('storage: read only shows a lock and no Release links; writable shows Release on held runs with bytes', async () => {
  const ro = await page()
  assert.equal(ro.querySelectorAll('[data-release]').length, 0)
  const lock = ro.querySelector('[data-read-only]')
  assert.ok(lock, 'a lock')
  assert.equal(lock.textContent, 'View only')
  assert.match(lock.getAttribute('title'), /^This page can't write to /)
  const rw = await page({}, ctx({ write: WRITABLE }))
  assert.ok(!rw.querySelector('[data-read-only]'))
  const links = rw.querySelectorAll('[data-release]')
  assert.deepEqual(links.map(a => a.getAttribute('href')),
    ['distraught_murdock', 'sad_yonath', 'sharp_magritte'].map(n => `#/run/${ROBLAB.runs[n].cid}?tab=storage&release=1`))
  assert.ok(links.every(a => a.textContent === 'Release…'))
  assert.equal(runRow(rw, 'sleepy_heyrovsky').querySelector('[data-release]'), null)
})

test('storage: Yours shows for a known viewer (0 B when they hold nothing here) and hides for an unknown or anonymous one', async () => {
  const nobody = await page({ assertedBy: 'ada@example.org' })
  assert.equal(nobody.querySelector('.mine'), null)
  const rob = await page({ assertedBy: 'ada@example.org' }, ctx({ viewer: ' Ada@Example.org ' }))
  assert.match(text(rob.querySelector('.mine')), /Yours.*142 MB · 100%/)
  const other = await page({ assertedBy: 'ada@example.org' }, ctx({ viewer: 'ana@example.org' }))
  assert.match(text(other.querySelector('.mine')), /Yours.*0 B · 0%/)
})

test('storage: the viewer app.js derives from members.json identity (P1b viewerOf) drives Yours', async () => {
  const { viewerOf } = await import('../src/identity.js')
  const viewer = viewerOf({ principal: 'Ada@Example.org', source: 'env' })
  const rob = await page({ assertedBy: 'ada@example.org' }, ctx({ viewer }))
  assert.match(text(rob.querySelector('.mine')), /Yours.*142 MB · 100%/)
  const anon = await page({ assertedBy: 'ada@example.org' }, ctx({ viewer: viewerOf({ principal: 'anonymous', source: 'anonymous' }) }))
  assert.equal(anon.querySelector('.mine'), null)
})

test('storage: by kind, the top seven by bytes then Other, each with a bar', async () => {
  const node = await page()
  const rows = node.querySelectorAll('[data-storage-kind]')
  assert.deepEqual(rows.map(r => r.dataset.storageKind), ['bam', 'html', 'zip', 'txt', 'png', 'json', 'svg', 'other'])
  assert.match(text(rows[0]), /BAM bam.*10.*38\.9 MB/)
  assert.match(text(rows[1]), /HTML reports html.*60.*32\.7 MB/)
  assert.match(text(rows[4]), /PNG png.*147.*15\.1 MB/)
  assert.match(text(rows[7]), /Other.*271.*5\.11 MB/)
  assert.equal(rows[0].querySelector('.minibar').getAttribute('aria-label'), '100%')
  assert.ok(rows.every(r => r.querySelector('.minibar')))
})

test('storage: no pins is the empty state', async () => {
  const node = await page()
  const pins = node.querySelector('[data-storage-pins]')
  assert.match(text(pins), /Pinned in roblab/)
  assert.match(text(pins.querySelector('.empty')),
    /^Nothing pinned yet ?Pinned items stay in this store even after their runs are released\. Pin from any run, item, file or Selection\.$/)
})

test('storage: pins list who, when, the subject and its bytes', async () => {
  const sad = ROBLAB.runs.sad_yonath
  const node = await page({ extra: [
    `INSERT INTO root VALUES (6, '${sad.cid}', 'pin', 'ana@example.org', 'pinned', '2026-10-24T20:00:00.000Z')`,
    `INSERT INTO root_stats VALUES (6, ${sad.reachable}, ${sad.blocks}, 0, 0, ${sad.reachable}, 0)`] })
  const pins = node.querySelectorAll('[data-storage-pin]')
  assert.equal(pins.length, 1)
  assert.match(text(pins[0]), /sad_yonath/)
  assert.match(text(pins[0]), /ana@example\.org/)
  assert.match(text(pins[0]), /78\.4 MB/)
  assert.equal(pins[0].querySelector('a').getAttribute('href'), `#/run/${sad.cid}`)
  assert.match(text(runRow(node, 'sad_yonath')), /Pinned/)
})

test('storage: each pin links to its subject\'s page, a Selection by its name', async () => {
  const node = await page({ extra: [
    `INSERT INTO root VALUES (7, 'item-1', 'pin', 'ben', 'pinned', '2026-10-25T10:00:00.000Z'), (8, '${ROBLAB.selection.cid}', 'pin', 'ben', 'pinned', '2026-10-25T08:00:00.000Z'),
      (9, 'coll-dm-aligned', 'pin', 'ben', 'pinned', '2026-10-25T07:00:00.000Z'), (10, 'bafkreiexamplefileblock', 'pin', 'ben', 'pinned', '2026-10-25T06:00:00.000Z')`,
    'INSERT INTO root_stats VALUES (7, 100, 1, 100, 1, 100, 0), (8, 1135852, 9, 0, 0, 1135852, 0), (9, 105, 2, 0, 0, 105, 0), (10, 7, 1, 7, 1, 7, 0)'] })
  const href = (cid) => node.querySelector(`[data-storage-pin=${cid}]`).querySelector('a').getAttribute('href')
  assert.equal(href('item-1'), '#/item/coll-dm-aligned/item-1')
  assert.equal(href(ROBLAB.selection.cid), `#/selection/${ROBLAB.selection.cid}`)
  assert.match(text(node.querySelector(`[data-storage-pin=${ROBLAB.selection.cid}]`)), /wigshop/)
  assert.equal(href('coll-dm-aligned'), '#/collection/coll-dm-aligned')
  assert.equal(href('bafkreiexamplefileblock'), '#/block/bafkreiexamplefileblock')
})

test('storage: by person, with the one-writer note', async () => {
  const anon = await page()
  assert.match(text(anon.querySelector('[data-storage-people]')), /written without an identity/)
  const rob = await page({ assertedBy: 'ada@example.org' }, ctx({ write: WRITABLE }))
  const person = rob.querySelector('[data-storage-person]')
  assert.match(text(person), /ada@example\.org.*142 MB · 100%.*142 MB.*0 B/)
  assert.match(text(rob.querySelector('[data-storage-people]')), /Only ada@example\.org has written to roblab\./)
  const you = await page({ assertedBy: 'ada@example.org' }, ctx({ viewer: 'ada@example.org' }))
  const name = you.querySelector('[data-storage-person]').querySelector('td')
  assert.equal(name.querySelector('.av').textContent, 'AD')
  assert.equal(text(name).slice(2), 'You')
  assert.match(text(you.querySelector('[data-storage-people]')), /You're the only person who has written to roblab\./)
})

test('storage: a partial store says its figures are a floor', async () => {
  const node = await page({ extra: ['UPDATE store_stats SET partial = 1'] })
  assert.match(text(node), /Some sizes are missing, so these figures are at least what is shown\./)
  const whole = await page()
  assert.doesNotMatch(text(whole), /Some sizes are missing/)
})

test('nav: a Storage link under Browse, and the store entry shows what it stores', async () => {
  installDom()
  const { explorer } = await openRoblab()
  const el = document.createElement('div')
  const storesEl = document.createElement('div')
  storesEl.append(document.createElement('a'))
  const entry = storesEl.children[0]
  entry.setAttribute('class', 'on')
  const small = document.createElement('small')
  small.textContent = 'writable'
  entry.append(small)
  const nav = createNav(explorer, { el, storesEl })
  await nav.load()
  const link = el.querySelector('[data-nav-link=storage]')
  assert.equal(link.getAttribute('href'), '#/storage')
  assert.match(link.textContent, /Storage/)
  assert.equal(el.children[0], link, 'first in the tree, right under the Browse links')
  assert.equal(entry.querySelector('[data-store-stored]').textContent, '142 MB · writable')
})

test('nav: on a lean snapshot the store entry stays as it was', async () => {
  installDom()
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': await roblabLeanSnapshot('over_cap') })
  const el = document.createElement('div')
  const storesEl = document.createElement('div')
  storesEl.append(document.createElement('a'))
  storesEl.children[0].setAttribute('class', 'on')
  await createNav(explorer, { el, storesEl }).load()
  assert.equal(storesEl.querySelector('[data-store-stored]'), null)
  assert.ok(el.querySelector('[data-nav-link=storage]'))
})

// Final fix wave item 4: the rail took 320 px and clipped By run's Release… buttons; mockup 09 has no rail.
test('the storage route renders without the rail', () => {
  assert.equal(showsRail('storage'), false)
  for (const route of ['home', 'run', 'items', 'selection', 'compose', 'unknown']) assert.equal(showsRail(route), true, route)
  const app = readFileSync(new URL('../src/app.js', import.meta.url), 'utf8')
  assert.match(app, /classList\.toggle\('has-rail', showsRail\(name\)\)/)
  assert.match(APP_CSS, /\.app:not\(\.has-rail\) \.rail[^{]*\{[^}]*display: none/)
  assert.match(APP_CSS, /\.app:not\(\.has-rail\)[^{]*\{[^}]*grid-template-columns: minmax\(0, 1fr\);/)
})

test('storage: "all shared" says what holds the files in every case, a pin included', async () => {
  const node = await page({}, ctx({ write: WRITABLE }))
  const sub = runRow(node, 'sharp_magritte').querySelectorAll('[title]').find(e => e.textContent.includes('all shared'))
  assert.equal(sub.getAttribute('title'), 'Every file is also held by another run, Selection or pin here.')
})

test('storage: the kind chip writes bigWig as the collection chips do', async () => {
  const node = await page({ extra: ["DELETE FROM kind_stats WHERE kind <> 'bigwig'"] })
  const row = node.querySelector('[data-storage-kind=bigwig]')
  assert.equal(row.querySelector('.kind').textContent, 'bigWig')
})

test('storage: Pinned names the pinner in the same form as By person', async () => {
  const sad = ROBLAB.runs.sad_yonath
  const node = await page({ extra: [
    `INSERT INTO root VALUES (6, '${sad.cid}', 'pin', ' Ana@Example.org ', 'pinned', '2026-10-24T20:00:00.000Z')`,
    `INSERT INTO root_stats VALUES (6, ${sad.reachable}, ${sad.blocks}, 0, 0, ${sad.reachable}, 0)`,
    "INSERT INTO asserter_stats VALUES ('ana@example.org', 1, 0, 1, 0)"] })
  const pin = node.querySelector('[data-storage-pin]')
  const person = node.querySelector('[data-storage-person="ana@example.org"]')
  const by = pin.querySelectorAll('td')[1]
  assert.match(text(by), /ana@example\.org$/)
  assert.equal(by.querySelector('.av').getAttribute('title'), 'ana@example.org')
  assert.match(text(person), /ana@example\.org/)
  assert.doesNotMatch(text(pin), /Ana@X/)
})

// Fix round 2 (F3). Freed on release only appears where you can act on it (mockup 09).
test('storage: a read-only store shows no Freed on release, in By run or By person', async () => {
  const ro = await page({ assertedBy: 'ada@example.org' })
  const heads = (sel) => ro.querySelector(sel).querySelectorAll('th').map(t => text(t).trim())
  assert.deepEqual(heads('[data-storage-by-run]'), ['Run', 'Durability', 'Reachableunique bytes'])
  assert.deepEqual(heads('[data-storage-people]'), ['Person', 'Reachablefrom their roots', 'Pinned'])
  assert.deepEqual(runRow(ro, 'distraught_murdock').querySelectorAll('td').map(td => text(td).trim()).slice(1),
    ['Held', '78.4 MB'])
  assert.doesNotMatch(text(ro.querySelector('[data-storage-by-run]')), /63\.7 MB|all shared|Freed/)
  assert.deepEqual(ro.querySelector('[data-storage-person]').querySelectorAll('td').map(td => text(td).trim()).slice(1), ['142 MB · 100%', '0 B'])
  const rw = await page({ assertedBy: 'ada@example.org' }, ctx({ write: WRITABLE }))
  assert.match(text(rw.querySelector('[data-storage-by-run]')), /Freed on release/)
  assert.match(text(rw.querySelector('[data-storage-people]')), /Freed on release/)
})

const ASOF = 'These figures are as of the snapshot; a pin, release or deletion since then is not counted yet.'
const tailPin = { cid: 'bafyreipinnew', entry: null, error: null, value: { subject: ROBLAB.runs.sad_yonath.cid, verb: 'add', attribute: 'pin',
  value: 'keep', supersedes: [], asserted_by: 'ada@example.org', timestamp: '2026-10-25T09:00:00Z' } }

test('storage: after a pin, release or deletion since the snapshot, the page says its figures are as of it and withholds Freed and Release', async () => {
  installDom()
  const { explorer } = await openRoblab()
  explorer.tailClaims = [tailPin]
  const node = await storage(explorer, ctx({ write: WRITABLE }))
  await settle()
  const line = node.querySelector('[data-storage-stale]')
  assert.ok(line, 'one line under the page head')
  assert.equal(text(line), ASOF)
  assert.equal(node.querySelector('.page-head').querySelector('[data-storage-stale]'), line, 'in the page head')
  assert.equal(node.querySelectorAll('[data-release]').length, 0, 'no Release… links')
  for (const name of ['distraught_murdock', 'sad_yonath', 'sharp_magritte']) {
    const cells = runRow(node, name).querySelectorAll('td')
    assert.equal(text(cells[3]).trim(), '—', `${name}: no freed figure`)
  }
  assert.doesNotMatch(text(node.querySelector('[data-storage-by-run]')), /63\.7 MB|all shared/)
  assert.equal(text(node.querySelector('[data-storage-person]').querySelectorAll('td')[2]).trim(), '—')
  assert.equal(text(node.querySelector('[data-storage-bar-asof]')), 'Pinned, Held and Releasing are as of the snapshot.')
  const fresh = await page({}, ctx({ write: WRITABLE }))
  assert.equal(fresh.querySelector('[data-storage-stale]'), null)
  assert.equal(fresh.querySelector('[data-storage-bar-asof]'), null)
  assert.equal(fresh.querySelectorAll('[data-release]').length, 3)
})

test('storage: a run or Selection logged since the snapshot makes the figures stale too, in its own words', async () => {
  installDom()
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
  } }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const node = await storage(explorer, ctx({ write: WRITABLE }))
  await settle()
  assert.equal(text(node.querySelector('[data-storage-stale]')),
    'These figures are as of the snapshot; runs and Selections logged since then are not counted yet.')
  assert.equal(node.querySelectorAll('[data-release]').length, 0)
  explorer.tailClaims = [tailPin]
  const both = await storage(explorer, ctx({ write: WRITABLE }))
  assert.equal(text(both.querySelector('[data-storage-stale]')),
    'These figures are as of the snapshot; a pin, release or deletion since then is not counted yet, nor are runs and Selections logged since.')
})

test('storage: a lean snapshot says why its storage figures are missing', async () => {
  installDom()
  const open = async (reason, options) => (await openSnapshots({ 'index/v6.sqlite': await roblabLeanSnapshot(reason, options) })).explorer
  const over = await storage(await open('over_cap', { cap: 67_108_864 }), ctx())
  const note = over.querySelector('[data-storage-omitted]')
  assert.equal(text(note), "Storage figures are left out of this store's snapshot: with them it would pass cas.snapshot.maxBytes (67.1 MB). "
    + 'Run nextflow plugin nf-blocks:snapshot to write it with the figures at any size, or raise cas.snapshot.maxBytes so runs keep them.')
  assert.equal(note.querySelector('code').textContent, 'nextflow plugin nf-blocks:snapshot')
  assert.equal(over.querySelector('.sbar'), null)
  assert.equal(over.querySelector('[data-storage-run]'), null)
  const noCap = await storage(await open('over_cap'), ctx())
  assert.equal(text(noCap.querySelector('[data-storage-omitted]')), "Storage figures are left out of this store's snapshot: with them it would pass cas.snapshot.maxBytes. "
    + 'Run nextflow plugin nf-blocks:snapshot to write it with the figures at any size, or raise cas.snapshot.maxBytes so runs keep them.')
  const oom = await storage(await open('out_of_memory'), ctx())
  assert.equal(text(oom.querySelector('[data-storage-omitted]')), "Storage figures are left out of this store's snapshot: computing them ran out of memory. "
    + 'Give Nextflow more memory, for example NXF_OPTS=-Xmx4g, and run nextflow plugin nf-blocks:snapshot.')
})

test('storage: a released run whose earliest sweep date has passed reads Released, with no date', async () => {
  const day = 86_400_000
  const old = new Date(Date.now() - 30 * day).toISOString()
  const soon = new Date(Date.now() - 2 * day).toISOString()
  const node = await page({ extra: [
    `UPDATE root SET state = 'released', since = '${old}' WHERE id = 3`,
    `UPDATE root SET state = 'released', since = '${soon}' WHERE id = 2`] })
  assert.equal(text(runRow(node, 'distraught_murdock').querySelector('.state')), 'Released')
  assert.match(text(runRow(node, 'sad_yonath').querySelector('.state')), /^Releasing · [A-Z][a-z]{2} \d{1,2}/)
})

test('storage.js tells anonymous principals apart with identity.js, as every other page does', () => {
  const src = readFileSync(new URL('../src/views/storage.js', import.meta.url), 'utf8')
  assert.doesNotMatch(src, /isAnonymous/)
  assert.match(src, /import \{[^}]*\bisAnon\b[^}]*\} from '\.\.\/identity\.js'/)
})

// Fix round 2 (F3), a third lean reason: the cache was itself seeded from a lean snapshot.
test('storage: a seeded_lean snapshot names reindex --seed false, and a cap of 0 or none is never printed', async () => {
  installDom()
  const open = async (reason, options) => (await openSnapshots({ 'index/v6.sqlite': await roblabLeanSnapshot(reason, options) })).explorer
  for (const options of [{}, { cap: 0 }]) {
    const seeded = await open('seeded_lean', options)
    assert.equal(seeded.storageOmitted, 'seeded_lean')
    const note = (await storage(seeded, ctx())).querySelector('[data-storage-omitted]')
    assert.equal(text(note), "Storage figures are left out of this store's snapshot: the cache that wrote the snapshot was rebuilt from a snapshot without them. "
      + 'Run nextflow plugin nf-blocks:reindex --seed false to rebuild it from the store.')
    assert.equal(note.querySelector('code').textContent, 'nextflow plugin nf-blocks:reindex --seed false')
    assert.doesNotMatch(text(note), /maxBytes|\(0\)/)
  }
  for (const reason of ['over_cap', 'out_of_memory']) {
    const zero = await open(reason, { cap: 0 })
    assert.equal(zero.snapshotCapBytes, null)
    const said = text((await storage(zero, ctx())).querySelector('[data-storage-omitted]'))
    assert.doesNotMatch(said, /\(0\)|\(0 B\)|maxBytes \(/)
  }
  assert.match(text((await storage(await open('over_cap', { cap: 0 }), ctx())).querySelector('[data-storage-omitted]')),
    /would pass cas\.snapshot\.maxBytes\. Run nextflow plugin nf-blocks:snapshot to write it with the figures at any size/)
})

// ---- Fix round 3 (G2) ------------------------------------------------------
const DAY_MS = 86_400_000
const tailClaim = (cid, subject, verb, attribute, value, timestamp, supersedes = []) =>
  ({ cid, entry: null, error: null, value: { subject, verb, attribute, value, supersedes, asserted_by: 'ana@x', timestamp } })

test('storage: after a Claim since the snapshot, Durability is each run\'s Claim state, as the run page draws it', async () => {
  installDom()
  const R = ROBLAB.runs
  const now = Date.now()
  const earlier = new Date(now - 10 * DAY_MS).toISOString()
  const released = now - 3_600_000
  // sharp_magritte was released in the snapshot; sad_yonath was pinned by ana there.
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': await (await import('./storage-fixture.mjs')).roblabSnapshot({ extra: [
    `UPDATE root SET state = 'released', since = '${earlier}' WHERE id = 1`,
    `INSERT INTO claim VALUES ('k-rel', '${R.sharp_magritte.cid}', 'set', 'retain', 'lineage', '${earlier}', 'anonymous')`,
    `INSERT INTO root VALUES (6, '${R.sad_yonath.cid}', 'pin', 'ana@x', 'pinned', '${earlier}')`,
    `INSERT INTO root_stats VALUES (6, ${R.sad_yonath.reachable}, ${R.sad_yonath.blocks}, 0, 0, ${R.sad_yonath.reachable}, 0)`,
    `INSERT INTO claim VALUES ('k-pin', '${R.sad_yonath.cid}', 'add', 'pin', 'keep', '${earlier}', 'ana@x')`] }) }, { now })
  const before = await storage(explorer, ctx({ write: WRITABLE }))
  const state = (node, name) => text(runRow(node, name).querySelector('.state'))
  assert.equal(state(before, 'sad_yonath'), 'Pinned')
  assert.match(state(before, 'sharp_magritte'), /^Releasing · /)
  assert.equal(before.querySelector('[data-storage-pins]').querySelector('.ph').querySelector('.muted'), null, 'fresh: no as-of mark')
  explorer.tailClaims = [
    tailClaim('bafyreiunpin', R.sad_yonath.cid, 'del', 'pin', null, new Date(now).toISOString(), ['k-pin']),
    tailClaim('bafyreirelease', R.distraught_murdock.cid, 'set', 'retain', 'lineage', new Date(released).toISOString()),
    tailClaim('bafyreirestore', R.sharp_magritte.cid, 'del', 'retain', null, new Date(now).toISOString(), ['k-rel']),
    tailClaim('bafyreidelete', R.sleepy_heyrovsky.cid, 'delete', null, null, new Date(now).toISOString()),
  ]
  const after = await storage(explorer, ctx({ write: WRITABLE }))
  await settle()
  assert.equal(state(after, 'sad_yonath'), 'Held', 'unpinned since')
  assert.equal(state(after, 'distraught_murdock'), `Releasing · ${dayText(released + ROBLAB.graceMs, now)}`, 'released since, from its Claim')
  assert.equal(state(after, 'sharp_magritte'), 'Held', 'restored since')
  assert.equal(runRow(after, 'sleepy_heyrovsky'), undefined, 'deleted since: left out, as the run list leaves it out')
  // The Pinned panel keeps the snapshot's rows, and says so.
  const pins = after.querySelector('[data-storage-pins]')
  assert.equal(pins.querySelectorAll('[data-storage-pin]').length, 1)
  assert.equal(text(pins.querySelector('.ph').querySelector('.muted')), 'as of the snapshot')
  // A pin since the snapshot on a run the snapshot held.
  explorer.tailClaims = [tailClaim('bafyreipinnew', R.distraught_murdock.cid, 'add', 'pin', 'keep', new Date(now).toISOString())]
  assert.equal(state(await storage(explorer, ctx({ write: WRITABLE })), 'distraught_murdock'), 'Pinned')
})

test('storage: the empty Pinned panel is marked as of the snapshot after a Claim since it, and not for a run logged since', async () => {
  installDom()
  const { explorer } = await openRoblab()
  explorer.tailClaims = [tailPin]
  const pins = (await storage(explorer, ctx())).querySelector('[data-storage-pins]')
  assert.equal(text(pins.querySelector('.ph').querySelector('.muted')), 'as of the snapshot')
  assert.ok(pins.querySelector('.empty'))
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
  } }) })
  const { explorer: logged } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const quiet = (await storage(logged, ctx())).querySelector('[data-storage-pins]')
  assert.equal(quiet.querySelector('.ph').querySelector('.muted'), null)
})

test('storage: its days are written by the Explorer\'s clock (ex.now), so a date in another year names that year', async () => {
  installDom()
  const now = Date.parse('2027-03-01T12:00:00Z')
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': await (await import('./storage-fixture.mjs')).roblabSnapshot() }, { now })
  const node = await storage(explorer, ctx())
  assert.match(text(runRow(node, 'distraught_murdock')), /Oct 24, 2026/)
})

test('storage.js and run.js share one stale-figure sentence (common.js staleFiguresText)', () => {
  const read = (f) => readFileSync(new URL(`../src/views/${f}`, import.meta.url), 'utf8')
  assert.doesNotMatch(read('storage.js'), /function staleText/)
  for (const f of ['storage.js', 'run.js']) assert.match(read(f), /import \{[^}]*\bstaleFiguresText\b[^}]*\} from '\.\/common\.js'/, f)
  assert.doesNotMatch(read('run.js'), /before the latest pins and releases/)
})
