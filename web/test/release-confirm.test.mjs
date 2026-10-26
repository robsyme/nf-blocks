// Bytes at the decision points (P2 task 7; mockups/10-run-storage.html): the
// run's Storage tab and its inline Release confirmation say what releasing
// frees in this store and what stays; Pin says what it keeps and how much
// others already pin here; Unpin says what only that pin keeps.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { resetFolds } from '../src/folds.js'
import { holds, namesText, retentionPanel } from '../src/views/common.js'
import { dayText } from '../src/words.js'
import { whoText } from '../src/identity.js'
import { run } from '../src/views/run.js'
import { item } from '../src/views/item.js'
import { collection } from '../src/views/collection.js'
import { buildMember } from './fixture.mjs'
import { argsOf, routeOf } from '../src/routes.js'
import { ROBLAB, openRoblab, openSnapshots, roblabSnapshot } from './storage-fixture.mjs'

const R = ROBLAB.runs
const settle = async () => { for (let i = 0; i < 4; i++) { await frame(); await new Promise(r => setTimeout(r, 2)) } }

function ctxFor({ viewer = null, calls = [], writable = true } = {}) {
  const log = (verb) => async (...args) => { calls.push([verb, ...args]); return {} }
  return { tray: new Tray(null), trayChanged: () => {}, rerender: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    sources: new Map(), store: { name: 'roblab', key: 'a' }, viewer,
    write: writable
      ? { available: true, here: true, writable: 'roblab', hrefFor: (h) => h,
        writer: { pin: log('pin'), unpin: log('unpin'), release: log('release'), restore: log('restore') },
        run: async (status, fn) => { await fn() } }
      : { available: false, reason: 'read only', hrefFor: (h) => h } }
}

/**
 * The roblab Explorer with storage figures, with `run()` answered from its run row (the fixture
 * has no blocks) and the header's manifest read switched off, so
 * `fetchFn.asked` holds only what the Storage tab itself fetched.
 */
async function roblabRun(name, { claims = [], extra = [], grace, tail = [], now = Date.now() } = {}) {
  const opened = await openSnapshots({ 'index/v6.sqlite': await roblabSnapshot({ extra, grace }) }, { now })
  // Claims written since the snapshot, as refreshTail leaves them.
  opened.explorer.tailClaims = tail.map(c => ({ cid: c.cid, entry: null, value: c, error: null }))
  const r = R[name]
  const ex = new Proxy(opened.explorer, {
    get(target, key) {
      if (key === 'run') {
        return async (cid) => ({ row: await target.runRow(cid), collections: [],
          completion: { status: r.status, possibly_incomplete: !!r.incomplete, finished_at: r.finished, anomalies: {}, error: null },
          state: { ...claimState(claims), claims } })
      }
      if (key === 'blocks' || key === 'runIdentity') return undefined
      const v = Reflect.get(target, key, target)
      return typeof v === 'function' ? v.bind(target) : v
    },
  })
  return { ...opened, ex, cid: r.cid }
}

const bignum = (el) => el.querySelector('.bignum')?.textContent ?? null
const cellBytes = (confirm) => ['frees', 'shared', 'pinned'].map(k => Number(confirm.querySelector(`[data-release-${k}]`)?.querySelector('.bignum').dataset.bytes ?? 0))
const sum = (xs) => xs.reduce((a, b) => a + b, 0)
const kvOf = (el) => {
  const dts = el.querySelectorAll('dt').map(d => d.textContent)
  const dds = el.querySelectorAll('dd').map(d => d.textContent)
  return Object.fromEntries(dts.map((t, i) => [t, dds[i]]))
}

test('sharp_magritte: releasing frees 0 B in roblab, and its files stay with the runs that share them', async () => {
  installDom(); resetFolds()
  const { ex, cid, fetchFn } = await roblabRun('sharp_magritte')
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.ok(confirm, 'the confirmation')
  assert.equal(confirm.hasAttribute('hidden'), false, 'open with ?release=1')
  assert.equal(confirm.querySelector('h2').textContent, 'Release sharp_magritte in roblab?')
  const frees = confirm.querySelector('[data-release-frees]')
  assert.match(frees.textContent, /^Frees in roblab\s*0 B/)
  assert.equal(bignum(frees), '0 B')
  assert.match(frees.textContent, /after 14-day grace · earliest [A-Z][a-z]{2} \d{1,2}/)
  const shared = confirm.querySelector('[data-release-shared]')
  assert.match(shared.textContent, /^Stays \(shared\)/)
  assert.equal(bignum(shared), '53.4 MB')
  assert.match(confirm.textContent, /sad_yonath/)
  assert.match(confirm.textContent, /distraught_murdock/)
  assert.match(confirm.textContent, /Releasing this run tidies the run list but won't free disk space until those runs are released too\./)
  // Mockup 10 links each sharing run to its page.
  const named = confirm.querySelector('.note').querySelectorAll('a')
  assert.deepEqual(named.map(a => [a.textContent, a.getAttribute('href')]),
    [['sad_yonath', `#/run/${R.sad_yonath.cid}`], ['distraught_murdock', `#/run/${R.distraught_murdock.cid}`]])
  assert.equal(confirm.querySelector('[data-release-pinned]'), null, 'nothing pinned')
  assert.equal(sum(cellBytes(confirm)), R.sharp_magritte.reachable, 'Frees + Stays add up to Reachable')
  assert.match(confirm.querySelector('[data-release-kept]').textContent, /^Always kept\s*Run records & lineage\s*completion, manifest, config$/)
  assert.equal(confirm.querySelector('#release-cancel').textContent, 'Cancel')
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release (frees 0 B)')
  assert.equal(page.querySelector('[data-slot=size]').textContent, '53.4 MB reachable · 528 file blocks')
  const here = page.querySelector('[data-run-storage]')
  assert.equal(here.querySelector('h2').textContent, 'This run in roblab')
  assert.deepEqual(kvOf(here), { Reachable: '53.4 MB · 528 file blocks', 'Only this run': '0 B',
    Shared: '53.4 MB with sad_yonath, distraught_murdock', 'Pinned here': 'nothing' })
  const compare = page.querySelector('[data-release-compare]')
  assert.match(compare.textContent, /what each release would free in roblab/)
  const rows = compare.querySelector('tbody').children.map(tr => tr.children.map(td => td.textContent))
  assert.deepEqual(rows, [['distraught_murdock', '78.4 MB', '63.7 MB'], ['sad_yonath', '78.4 MB', '15.6 MB'], ['sharp_magritte', '53.4 MB', '0 B']])
  assert.ok(compare.querySelector('tbody').children[2].classList.contains('sel'), 'this run is marked')
  assert.deepEqual(fetchFn.asked, [], 'no block walk when the snapshot has storage figures')
})

test('distraught_murdock frees 63.7 MB; what it shares stays, named', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('distraught_murdock')
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release (frees 63.7 MB)')
  assert.equal(bignum(confirm.querySelector('[data-release-frees]')), '63.7 MB')
  assert.equal(bignum(confirm.querySelector('[data-release-shared]')), '14.6 MB')
  assert.match(confirm.textContent, /sad_yonath, sharp_magritte/)
  assert.doesNotMatch(confirm.textContent, /tidies the run list/)
  assert.equal(kvOf(page.querySelector('[data-run-storage]')).Shared, '14.6 MB with sad_yonath, sharp_magritte')
})

test('Release opens the confirmation; Cancel closes it with no write; confirm releases', async () => {
  installDom(); resetFolds()
  const calls = []
  const { ex, cid } = await roblabRun('sad_yonath')
  const page = await run(ex, cid, ctxFor({ calls }), { tab: 'storage' })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(confirm.hasAttribute('hidden'), true, 'closed without ?release=1')
  await page.querySelector('#release').click()
  assert.equal(confirm.hasAttribute('hidden'), false)
  assert.deepEqual(calls, [], 'opening writes nothing')
  await confirm.querySelector('#release-cancel').click()
  assert.equal(confirm.hasAttribute('hidden'), true)
  assert.deepEqual(calls, [])
  await page.querySelector('#release').click()
  await confirm.querySelector('#release-confirm').click()
  assert.deepEqual(calls, [['release', cid, []]])
})

test('the header Release… opens the confirmation through ?release=1, and the route passes it', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath')
  const page = await run(ex, cid, ctxFor(), {})
  const actions = page.querySelector('.page-head').querySelectorAll('.btn')
  assert.equal(actions.find(a => a.textContent === 'Release…').getAttribute('href'), `#/run/${cid}?tab=storage&release=1`)
  const hash = `#/run/${cid}?tab=storage&release=1`
  const route = routeOf(hash)
  assert.deepEqual(argsOf(route, hash), { cid, tab: 'storage', where: null, show: null, release: true })
  // The route's own view opens the confirmation, so a swapped or dropped argument fails here too.
  const routed = await route.view(ex, argsOf(route, hash), ctxFor())
  await settle()
  const confirm = routed.querySelector('[data-release-confirm]')
  assert.ok(confirm, 'the confirmation')
  assert.equal(confirm.hasAttribute('hidden'), false)
})

test('a read-only page never opens the confirmation', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath')
  const page = await run(ex, cid, ctxFor({ writable: false }), { tab: 'storage', release: true })
  assert.equal(page.querySelector('[data-release-confirm]'), null)
})

test('pinned bytes stay, named by who pins them; Frees, Stays (shared) and Stays (pinned) add up to Reachable', async () => {
  installDom(); resetFolds()
  // A pin root is a content root, so the 7 MB it reaches is no longer distraught_murdock's alone (as a real snapshot writes it).
  const { ex, cid } = await roblabRun('distraught_murdock', { extra: [
    `UPDATE root_stats SET pinned_bytes = 7000000, exclusive_bytes = ${R.distraught_murdock.exclusive - 7_000_000} WHERE root_id = 3`,
    `INSERT INTO pinned_overlap VALUES ('${R.distraught_murdock.cid}', 'ana@x', 5000000), ('${R.distraught_murdock.cid}', 'ben@x', 2000000)`] })
  const page = await run(ex, cid, ctxFor({ viewer: 'ana@x' }), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(bignum(confirm.querySelector('[data-release-frees]')), '56.7 MB')
  assert.equal(bignum(confirm.querySelector('[data-release-shared]')), '14.6 MB', 'shared, less what pins keep')
  assert.equal(sum(cellBytes(confirm)), R.distraught_murdock.reachable)
  assert.match(confirm.textContent, /14\.6 MB of it is also held by sad_yonath, sharp_magritte/)
  assert.equal(kvOf(page.querySelector('[data-run-storage]')).Shared, '21.6 MB with sad_yonath, sharp_magritte', 'the kv keeps reachable less exclusive')
  const pinned = page.querySelector('[data-release-pinned]')
  assert.equal(bignum(pinned), '7 MB')
  assert.match(pinned.textContent, /by you, ben@x/)
  assert.equal(kvOf(page.querySelector('[data-run-storage]'))['Pinned here'], 'You 5 MB, ben@x 2 MB')
  assert.equal(page.querySelector('[data-pinned-by]').textContent, '2 MB already pinned here by ben@x')
})

test('Pin says how much others already pin here, leaving out the viewer', async () => {
  installDom()
  const rows = [{ assertedBy: 'ana@x', bytes: 5_000_000 }, { assertedBy: 'ben@x', bytes: 2_000_000 }]
  const size = { bytes: 10_000_000, partial: false }
  const ana = retentionPanel('bafysubject', claimState([]), ctxFor({ viewer: 'ana@x' }), { size, pinnedBy: rows })
  await settle()
  assert.equal(ana.querySelector('#pin').textContent, 'Pin 10 MB in roblab')
  assert.equal(ana.querySelector('[data-pinned-by]').textContent, '2 MB already pinned here by ben@x')
  const nobody = retentionPanel('bafysubject', claimState([]), ctxFor({ viewer: null }), { size, pinnedBy: Promise.resolve(rows) })
  await settle()
  assert.equal(nobody.querySelector('[data-pinned-by]').textContent, 'Already pinned here: ana@x 5 MB, ben@x 2 MB', 'each person\'s own figure, no sum')
  const partial = retentionPanel('bafysubject', claimState([]), ctxFor({ viewer: 'ana@x' }), { size: { bytes: 10_000_000, partial: true }, pinnedBy: rows })
  await settle()
  assert.equal(partial.querySelector('[data-pinned-by]').textContent, 'at least 2 MB already pinned here by ben@x')
  const alone = retentionPanel('bafysubject', claimState([]), ctxFor({ viewer: 'Ana@X ' }), { size, pinnedBy: rows.slice(0, 1) })
  await settle()
  assert.equal(alone.querySelector('[data-pinned-by]').hasAttribute('hidden'), true, 'only the viewer pins it')
})

test('Unpin says what only that pin keeps: 0 B when two people pin the subject', async () => {
  installDom()
  const pin = (cid, who) => ({ cid, subject: 'item-1', verb: 'add', attribute: 'pin', value: 'figure', supersedes: [], asserted_by: who, timestamp: '2026-10-25T09:00:00Z' })
  const claims = [pin('bafyreipina', 'ana@x'), pin('bafyreipinb', 'ben@x')]
  const state = { ...claimState(claims), claims }
  const { explorer } = await openRoblab({ extra: [
    "INSERT INTO root VALUES (6, 'item-1', 'pin', 'ana@x', 'pinned', '2026-10-25T09:00:00Z'), (7, 'item-1', 'pin', 'ben@x', 'pinned', '2026-10-25T09:00:00Z')",
    'INSERT INTO root_stats VALUES (6, 100, 1, 0, 0, 100, 0), (7, 100, 1, 0, 0, 100, 0)'] })
  const node = retentionPanel('item-1', state, ctxFor(), { kind: 'item', size: { bytes: 100, partial: false }, ...holds(explorer, 'item-1') })
  await settle()
  assert.deepEqual(node.querySelectorAll('[data-unpin]').map(b => b.textContent),
    ['Unpin (kept only by this pin in roblab: 0 B)', 'Unpin (kept only by this pin in roblab: 0 B)'])
  const { explorer: alone } = await openRoblab({ extra: [
    "INSERT INTO root VALUES (6, 'item-1', 'pin', 'Ana@X', 'pinned', '2026-10-25T09:00:00Z')", 'INSERT INTO root_stats VALUES (6, 100, 1, 100, 1, 100, 0)'] })
  const one = retentionPanel('item-1', { ...claimState(claims.slice(0, 1)), claims: claims.slice(0, 1) }, ctxFor(),
    { kind: 'item', size: { bytes: 100, partial: false }, ...holds(alone, 'item-1') })
  await settle()
  assert.equal(one.querySelector('[data-unpin]').textContent, 'Unpin (kept only by this pin in roblab: 100 B)')
  const plain = retentionPanel('item-1', state, ctxFor(), { kind: 'item', size: { bytes: 100, partial: false }, roots: null })
  await settle()
  assert.deepEqual(plain.querySelectorAll('[data-unpin]').map(b => b.textContent), ['Unpin', 'Unpin'], 'a lean snapshot or a tail subject')
})

test('a released run: what is still stored here, what of it is releasing and from when (R6)', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const released = now - 86_400_000
  const earliest = dayText(released + ROBLAB.graceMs, now)
  const claims = [{ cid: 'bafyreirel', subject: R.distraught_murdock.cid, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [],
    asserted_by: 'ana@x', timestamp: new Date(released).toISOString() }]
  const { ex, cid } = await roblabRun('distraught_murdock', { claims, now })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.equal(page.querySelector('.page-head').querySelector('.state').textContent, `Releasing · ${earliest}`)
  assert.equal(page.querySelector('[data-slot=size]').textContent, '78.4 MB still stored here · 894 file blocks')
  assert.equal(page.querySelector('[data-run-size]').textContent,
    `This run's files still stored here: 78.4 MB; 63.7 MB of it is releasing and a sweep can reclaim it from ${earliest}. Restore content holds whatever of its files are still here.`)
  assert.equal(page.querySelector('[data-release-confirm]'), null, 'nothing to confirm: Restore is offered instead')
  assert.ok(page.querySelector('#restore'))
  assert.match(page.querySelector('[data-retention]').textContent,
    /Restoring holds whatever of this run's files are still here; files a sweep already deleted don't come back\./)
  assert.doesNotMatch(page.textContent, /Restoring holds this run's files here again/)
  const { ex: noGrace } = await roblabRun('distraught_murdock', { claims, grace: null })
  const later = await run(noGrace, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(later.querySelector('.page-head').querySelector('.state').textContent, 'Releasing')
  assert.match(later.querySelector('[data-run-size]').textContent, /a sweep can reclaim it after the grace period\./)
})

test('a run newer than the snapshot: its size walks, and the confirmation names no freed figure (Review focus 4)', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
  } }) })
  const { explorer, fetchFn } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  fetchFn.asked.length = 0
  const page = await run(explorer, member.runs.R2.completion, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.ok(fetchFn.asked.includes(member.item.C), 'the tail run is walked')
  assert.match(page.querySelector('[data-run-size]').textContent, /^This run holds 20 B \(2 file blocks\) in roblab\./)
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(confirm.hasAttribute('hidden'), false)
  assert.match(confirm.textContent, /Freed bytes are known once the next snapshot is written/)
  assert.equal(confirm.querySelector('.bignum'), null, 'no figure')
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release')
  assert.ok(page.querySelector('#release'), 'the Release button stays')
})

/** buildMember with R1's storage-figure rows and pinned_overlap rows for item A and R1's collection. */
async function memberWithPins() {
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs, item }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
    db.exec({ sql: 'INSERT INTO collection_size VALUES (?, 20, 2)', bind: [runs.R1.collection] })
    db.exec({ sql: 'INSERT INTO item_size VALUES (?, 777, 1)', bind: [item.A.toString()] })
    db.exec({ sql: "INSERT INTO pinned_overlap VALUES (?, 'ben@x', 10), (?, 'ben@x', 20)", bind: [item.A.toString(), runs.R1.collection] })
  } }) })
  const opened = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  return { member, ...opened }
}

test('the item and collection pages say what others already pin here', async () => {
  installDom(); resetFolds()
  const { member, explorer } = await memberWithPins()
  const itemPage = await item(explorer, member.runs.R1.collection, member.item.A, ctxFor())
  await settle()
  assert.equal(itemPage.querySelector('#pin').textContent, 'Pin 777 B in roblab')
  assert.equal(itemPage.querySelector('[data-pinned-by]').textContent, '10 B already pinned here by ben@x')
  const collPage = await collection(explorer, member.runs.R1.collection, 0, ctxFor())
  await settle()
  assert.equal(collPage.querySelector('#pin').textContent, 'Pin 20 B in roblab')
  assert.equal(collPage.querySelector('[data-pinned-by]').textContent, '20 B already pinned here by ben@x')
})

test('a read-only page offers no Pin… or Release… in the header', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath')
  const head = (await run(ex, cid, ctxFor({ writable: false }), {})).querySelector('.page-head')
  // Copy to… shows either way; without explore's copy it is disabled with its reason.
  assert.deepEqual(head.querySelectorAll('.btn').map(b => [b.textContent, b.disabled]), [['Copy to…', true]])
  const writable = (await run(ex, cid, ctxFor(), {})).querySelector('.page-head')
  assert.deepEqual(writable.querySelectorAll('.btn').map(b => b.textContent), ['Pin…', 'Release…', 'Add to project…', 'Copy to…'])
})

test('names past root_shared\'s cap of 5 read "and others"', async () => {
  assert.equal(namesText(['a', 'b', 'c', 'd']), 'a, b, c and 1 more')
  assert.equal(namesText(['a', 'b'], 3, true), 'a, b and others')
  assert.equal(namesText(['a', 'b', 'c', 'd'], 3, true), 'a, b, c and others')
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sharp_magritte', { extra: [
    "INSERT INTO root VALUES (6, 'bafk-x', 'pin', 'ben@x', 'pinned', '2026-10-25T09:00:00Z'), (7, 'bafk-y', 'pin', 'ben@x', 'pinned', '2026-10-25T09:00:00Z')",
    'INSERT INTO root_stats VALUES (6, 7, 1, 0, 0, 7, 0), (7, 7, 1, 0, 0, 7, 0)',
    'INSERT INTO root_shared VALUES (1, 6, 7), (1, 7, 7)'] })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.equal(kvOf(page.querySelector('[data-run-storage]')).Shared, '53.4 MB with sad_yonath, distraught_murdock and others')
  assert.match(page.querySelector('[data-release-confirm]').textContent, /sad_yonath, distraught_murdock and others/)
})

test('confirming drops release=1 from the address before it writes', async () => {
  installDom(); resetFolds()
  const calls = []
  const { ex, cid } = await roblabRun('sad_yonath')
  const page = await run(ex, cid, ctxFor({ calls }), { tab: 'storage', release: true })
  await settle()
  const saved = { location: globalThis.location, history: globalThis.history }
  globalThis.location = { hash: `#/run/${cid}?tab=storage&release=1` }
  globalThis.history = { state: null, replaceState: (state, title, url) => calls.push(['replace', url]) }
  try {
    await page.querySelector('#release-confirm').click()
  } finally {
    Object.assign(globalThis, saved)
  }
  assert.deepEqual(calls, [['replace', `#/run/${cid}?tab=storage`], ['release', cid, []]])
})

test('Unpin waits for its pin roots, then says what only that pin keeps, or plainly Unpin', async () => {
  installDom()
  const claim = { cid: 'bafyreipina', subject: 'item-1', verb: 'add', attribute: 'pin', value: 'figure', supersedes: [], asserted_by: 'ana@x', timestamp: '2026-10-25T09:00:00Z' }
  const state = { ...claimState([claim]), claims: [claim] }
  let answer
  const roots = new Promise((resolve) => { answer = resolve })
  const node = retentionPanel('item-1', state, ctxFor(), { kind: 'item', roots })
  const unpin = node.querySelector('[data-unpin]')
  assert.equal(unpin.disabled, true, 'waiting for the roots')
  answer([{ kind: 'pin', assertedBy: 'ana@x', exclusive: 100, partial: false }])
  await settle()
  assert.equal(unpin.disabled, false)
  assert.equal(unpin.textContent, 'Unpin (kept only by this pin in roblab: 100 B)')
  const failed = retentionPanel('item-1', state, ctxFor(), { kind: 'item', roots: Promise.reject(new Error('gone')) })
  await settle()
  assert.equal(failed.querySelector('[data-unpin]').disabled, false)
  assert.equal(failed.querySelector('[data-unpin]').textContent, 'Unpin')
})

test('a run\'s Release always goes through a confirmation: without one, retentionPanel offers none', () => {
  installDom()
  const node = retentionPanel('bafyrun', claimState([]), ctxFor(), { isRun: true, kind: 'run' })
  assert.equal(node.querySelector('#release'), null)
  assert.ok(node.querySelector('#pin'))
  const confirmable = retentionPanel('bafyrun', claimState([]), ctxFor(), { isRun: true, kind: 'run', onRelease: () => {} })
  assert.ok(confirmable.querySelector('#release'))
})

test('opening the confirmation focuses its heading, never the destructive button', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath')
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  let focused = null
  const heading = confirm.querySelector('h2')
  assert.equal(heading.getAttribute('tabindex'), '-1')
  heading.focus = () => { focused = 'heading' }
  confirm.querySelector('#release-confirm').focus = () => { focused = 'confirm' }
  await page.querySelector('#release').click()
  assert.equal(focused, 'heading')
})

test('when only Selections share the files, the note names them and says they must be deleted', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sharp_magritte', { extra: [
    'DELETE FROM root_shared WHERE root_id = 1',
    "INSERT INTO root VALUES (6, 'bafyreiselection2', 'selection', 'ben@x', 'held', '2026-10-25T09:00:00Z')",
    'INSERT INTO root_stats VALUES (6, 53400488, 528, 0, 0, 0, 0)',
    'INSERT INTO root_shared VALUES (1, 5, 53400488), (1, 6, 53400488)'] })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const text = page.querySelector('[data-release-confirm]').textContent
  assert.match(text, /wigshop, an unnamed Selection, or both/)
  assert.match(text, /until those Selections are deleted too\./)
  assert.doesNotMatch(text, /that Selection/)
})

test('with nothing shared and nothing pinned, Stays (shared) names every kind that could hold the files', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sleepy_heyrovsky')
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.match(page.querySelector('[data-release-shared]').textContent, /no other run, Selection, hand-off or pin holds these files$/)
})

/**
 * sharp_magritte's files held by `others` alone: [root id, kind, cid, handoff template, handed-over Selection] each,
 * every one sharing all its bytes.
 */
const heldOnlyBy = (others) => ['DELETE FROM root_shared WHERE root_id = 1',
  ...others.flatMap(([id, kind, cid, template, selection = 'bafyhandedsel']) => [
    ...(id === 5 ? [] : [`INSERT INTO root VALUES (${id}, '${cid}', '${kind}', 'ben@x', 'held', '2026-10-25T09:00:00Z')`,
      `INSERT INTO root_stats VALUES (${id}, 53400488, 528, 0, 0, 0, 0)`]),
    ...(kind === 'handoff' ? [`INSERT INTO handoff VALUES ('${cid}', ${template ? `'${template}'` : 'NULL'}, ${selection ? `'${selection}'` : 'NULL'}, 'proj-1', 'ben@x', '2026-10-25T09:00:00Z')`] : []),
    `INSERT INTO root_shared VALUES (1, ${id}, 53400488)`])]

test('a run whose files a hand-off also holds names that hand-off, linked to the Selection it handed over', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sharp_magritte', { extra: heldOnlyBy([[6, 'handoff', 'bafyhandoff', 'nf-core/differentialabundance']]) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.equal(kvOf(page.querySelector('[data-run-storage]')).Shared, '53.4 MB with hand-off to nf-core/differentialabundance')
  const confirm = page.querySelector('[data-release-confirm]')
  assert.deepEqual(confirm.querySelector('.note').querySelectorAll('a').map(a => [a.textContent, a.getAttribute('href')]),
    [['hand-off to nf-core/differentialabundance', '#/selection/bafyhandedsel']])
  assert.match(confirm.querySelector('[data-release-shared]').textContent, /every file is also held by a hand-off/)
  assert.match(confirm.textContent, /until that hand-off is deleted too\./)
})

test('a hand-off with no Selection to link to is named unlinked, never sent to the template route', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sharp_magritte', { extra: heldOnlyBy([[6, 'handoff', 'bafyhandoff', 'nf-core/differentialabundance', null]]) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.match(confirm.textContent, /also held by hand-off to nf-core\/differentialabundance\./)
  assert.deepEqual(confirm.querySelector('.note').querySelectorAll('a').map(a => a.getAttribute('href')), [], 'no link')
  assert.ok(!page.querySelectorAll('a').some(a => (a.getAttribute('href') ?? '').startsWith('#/handoff/')), 'no #/handoff/ link anywhere')
})

test('Selections and hand-offs share the files: both are named, and an unnamed hand-off says so', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sharp_magritte', { extra: heldOnlyBy([[5, 'selection', ROBLAB.selection.cid], [6, 'handoff', 'bafyhandoff', null]]) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const text = page.querySelector('[data-release-confirm]').textContent
  // Both hold every byte, so their order is the snapshot's.
  assert.match(text, /(wigshop, an unnamed hand-off|an unnamed hand-off, wigshop), or both/)
  assert.match(text, /held by Selections and hand-offs/)
  assert.match(text, /until those Selections and hand-offs are deleted too\./)
  assert.match(kvOf(page.querySelector('[data-run-storage]')).Shared, /^53\.4 MB with (wigshop, an unnamed hand-off|an unnamed hand-off, wigshop)$/)
})

test('dayText lives in words.js and whoText in identity.js', () => {
  assert.equal(dayText('2026-10-31T12:00:00Z', new Date('2026-01-01T12:00:00Z')), 'Oct 31')
  assert.equal(dayText(Date.parse('2025-10-31T12:00:00Z'), new Date('2026-01-01T12:00:00Z')), 'Oct 31, 2025')
  assert.equal(dayText(undefined, Date.now()), '')
  // The clock is the caller's (ex.now), in ms or as a Date; dayText never reads one of its own.
  assert.equal(dayText('2026-10-31T12:00:00Z', Date.parse('2027-01-01T12:00:00Z')), 'Oct 31, 2026')
  assert.doesNotMatch(readFileSync(new URL('../src/words.js', import.meta.url), 'utf8'), /now = new Date\(\)/)
  assert.equal(whoText('Ana@X', 'ana@x', { capital: true }), 'You')
  assert.equal(whoText(' anonymous ', null), 'anonymous')
  assert.equal(whoText('ben@x', 'ana@x'), 'ben@x')
})

test('the partly-shared note links each sharing run too', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('distraught_murdock')
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const named = page.querySelector('[data-release-confirm]').querySelector('.note').querySelectorAll('a')
  assert.deepEqual(named.map(a => a.getAttribute('href')), [`#/run/${R.sad_yonath.cid}`, `#/run/${R.sharp_magritte.cid}`])
})

// Final fix wave item 1: after a write the page refreshes only the Store Log
// tail, so a pin, release or deletion Claim written since the snapshot leaves
// the snapshot's freed and kept figures out of date. They are left off.
const DM_PINNED = [
  `UPDATE root_stats SET pinned_bytes = ${R.distraught_murdock.reachable}, exclusive_bytes = 0 WHERE root_id = 3`,
  `INSERT INTO root VALUES (6, '${R.distraught_murdock.cid}', 'pin', 'ana@x', 'pinned', '2026-10-25T09:00:00Z')`,
  `INSERT INTO root_stats VALUES (6, ${R.distraught_murdock.reachable}, 894, ${R.distraught_murdock.exclusive}, 391, ${R.distraught_murdock.reachable}, 0)`,
  `INSERT INTO pinned_overlap VALUES ('${R.distraught_murdock.cid}', 'ana@x', ${R.distraught_murdock.reachable})`]
const pinClaim = { cid: 'bafyreipinold', subject: R.distraught_murdock.cid, verb: 'add', attribute: 'pin', value: 'keep', supersedes: [],
  asserted_by: 'ana@x', timestamp: '2026-10-25T09:00:00Z' }

/** The page's stale-figure checks: no freed figure, a plain Release, a plain Unpin, no "already pinned" line, and the as-of line. */
function assertStale(page) {
  const confirm = page.querySelector('[data-release-confirm]')
  assert.match(confirm.textContent, /Freed bytes are known once the next snapshot is written/)
  assert.equal(confirm.querySelector('.bignum'), null, 'no freed figure')
  assert.equal(confirm.querySelector('[data-release-pinned]'), null, 'no pinned figure')
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release')
  for (const b of page.querySelectorAll('[data-unpin]')) assert.equal(b.textContent, 'Unpin')
  assert.equal(page.querySelector('[data-pinned-by]').hasAttribute('hidden'), true, 'no "already pinned here"')
  assert.match(page.querySelector('[data-run-storage-asof]').textContent, /as of the snapshot/)
}

test('an Unpin since the snapshot: Release names no freed figure and the button reads "Release"', async () => {
  installDom(); resetFolds()
  const unpin = { cid: 'bafyreiunpin', subject: R.distraught_murdock.cid, verb: 'del', attribute: 'pin', value: null, supersedes: [pinClaim.cid],
    asserted_by: 'ana@x', timestamp: '2026-10-25T10:00:00Z' }
  const { ex, cid } = await roblabRun('distraught_murdock', { extra: DM_PINNED, claims: [pinClaim, unpin], tail: [unpin] })
  const page = await run(ex, cid, ctxFor({ viewer: 'ben@x' }), { tab: 'storage', release: true })
  await settle()
  assertStale(page)
  assert.doesNotMatch(page.querySelector('[data-release-confirm]').textContent, /0 B/, 'not "Frees in roblab 0 B"')
})

test('a pin since the snapshot: the same, and Unpin is plain', async () => {
  installDom(); resetFolds()
  const pin = { ...pinClaim, cid: 'bafyreipinnew' }
  const { ex, cid } = await roblabRun('distraught_murdock', { claims: [pin], tail: [pin] })
  const page = await run(ex, cid, ctxFor({ viewer: 'ben@x' }), { tab: 'storage', release: true })
  await settle()
  assertStale(page)
  assert.equal(page.querySelectorAll('[data-unpin]').length, 1)
})

test('a release or a deletion since the snapshot on any subject makes every figure stale; a name Claim does not', async () => {
  const other = R.sad_yonath.cid
  const claim = (verb, attribute, value) => ({ cid: `bafyrei${verb}${attribute ?? 'x'}`, subject: other, verb, attribute, value, supersedes: [],
    asserted_by: 'ana@x', timestamp: '2026-10-25T10:00:00Z' })
  for (const tail of [[claim('set', 'retain', 'lineage')], [claim('delete', null, null)], [claim('del', null, null)]]) {
    installDom(); resetFolds()
    const { ex, cid } = await roblabRun('distraught_murdock', { extra: DM_PINNED, claims: [pinClaim], tail })
    assert.equal(ex.storageStale(), true, tail[0].cid)
    const page = await run(ex, cid, ctxFor({ viewer: 'ben@x' }), { tab: 'storage', release: true })
    await settle()
    assertStale(page)
  }
  const { ex: named } = await roblabRun('distraught_murdock', { tail: [claim('set', 'name', 'x')] })
  assert.equal(named.storageStale(), false)
})

test('with no pin, release or deletion since the snapshot, the figures show as before', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('distraught_murdock', { extra: DM_PINNED, claims: [pinClaim] })
  assert.equal(ex.storageStale(), false)
  const page = await run(ex, cid, ctxFor({ viewer: 'ben@x' }), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(bignum(confirm.querySelector('[data-release-frees]')), '0 B')
  assert.equal(bignum(confirm.querySelector('[data-release-pinned]')), '78.4 MB')
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release (frees 0 B)')
  assert.equal(page.querySelector('[data-unpin]').textContent, 'Unpin (kept only by this pin in roblab: 63.7 MB)')
  assert.equal(page.querySelector('[data-pinned-by]').textContent, '78.4 MB already pinned here by ana@x')
  assert.equal(page.querySelector('[data-run-storage-asof]'), null)
})

test('holds() gives no figures once the snapshot\'s are stale, so the item page\'s Unpin is plain', async () => {
  const { explorer } = await openRoblab({ extra: [
    "INSERT INTO root VALUES (6, 'item-1', 'pin', 'ana@x', 'pinned', '2026-10-25T09:00:00Z')", 'INSERT INTO root_stats VALUES (6, 100, 1, 100, 1, 100, 0)',
    "INSERT INTO pinned_overlap VALUES ('item-1', 'ben@x', 100)"] })
  const fresh = holds(explorer, 'item-1')
  assert.equal((await fresh.roots).length, 1)
  assert.equal((await fresh.pinnedBy).length, 1)
  explorer.tailClaims = [{ cid: 'bafyreiunpin', entry: null, error: null, value: { subject: 'item-2', verb: 'del', attribute: 'pin', value: null,
    supersedes: [], asserted_by: 'ana@x', timestamp: 't' } }]
  assert.deepEqual(holds(explorer, 'item-1'), { pinnedBy: null, roots: null })
})

// Final fix wave item 2: writeStorage makes one pin root per (subject, asserter).
test('two pins by the same person on one subject: each Unpin keeps 0 B, since removing one leaves the other', async () => {
  installDom()
  const pin = (cid, who) => ({ cid, subject: 'item-1', verb: 'add', attribute: 'pin', value: 'figure', supersedes: [], asserted_by: who, timestamp: '2026-10-25T09:00:00Z' })
  const claims = [pin('bafyreipina', 'ana@x'), pin('bafyreipinb', 'Ana@X '), pin('bafyreipinc', 'ben@x')]
  const state = { ...claimState(claims), claims }
  const roots = [{ kind: 'pin', assertedBy: 'ana@x', exclusive: 100, partial: false }, { kind: 'pin', assertedBy: 'ben@x', exclusive: 30, partial: false }]
  const node = retentionPanel('item-1', state, ctxFor(), { kind: 'item', size: { bytes: 100, partial: false }, roots })
  const label = (cid) => node.querySelector(`[data-unpin=${cid}]`).textContent
  assert.equal(label('bafyreipina'), 'Unpin (kept only by this pin in roblab: 0 B)')
  assert.equal(label('bafyreipinb'), 'Unpin (kept only by this pin in roblab: 0 B)')
  assert.equal(label('bafyreipinc'), 'Unpin (kept only by this pin in roblab: 30 B)', 'one pin of its own keeps its root\'s figure')
})

// ---- Fix round 2 (F3) --------------------------------------------------------
const DAY_MS = 86_400_000
const retain = (subject, at) => ({ cid: 'bafyreirelease', subject, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [],
  asserted_by: 'ana@x', timestamp: new Date(at).toISOString() })
/**
 * A run root released at `at`. `swept` is what a sweep took, as F1 writes it:
 * absent blocks leave every figure, so reachable and exclusive both fall by
 * the swept bytes and blocks.
 */
const releasedRoot = (id, at, swept = null) => [`UPDATE root SET state = 'released', since = '${new Date(at).toISOString()}' WHERE id = ${id}`,
  ...(swept === null ? [] : [`UPDATE root_stats SET reachable_bytes = reachable_bytes - ${swept.bytes}, reachable_blocks = reachable_blocks - ${swept.blocks},
    exclusive_bytes = max(0, exclusive_bytes - ${swept.bytes}), exclusive_blocks = max(0, exclusive_blocks - ${swept.blocks}) WHERE root_id = ${id}`])]
const SWEPT_ALONE = 'A sweep may already have reclaimed any files only this run held; its records and lineage stay here.'

test('Unpin names the store whose bytes only that pin keeps', async () => {
  installDom()
  const claim = { cid: 'bafyreipina', subject: 'item-1', verb: 'add', attribute: 'pin', value: 'figure', supersedes: [], asserted_by: 'ana@x', timestamp: '2026-10-25T09:00:00Z' }
  const node = retentionPanel('item-1', { ...claimState([claim]), claims: [claim] }, ctxFor(), { kind: 'item',
    roots: [{ kind: 'pin', assertedBy: 'ana@x', exclusive: 0, partial: false }] })
  assert.equal(node.querySelector('[data-unpin]').textContent, 'Unpin (kept only by this pin in roblab: 0 B)')
})

test('a swept released run: past its earliest date with nothing of its own left, it says a sweep may have reclaimed its files', async () => {
  const at = Date.now() - 30 * DAY_MS
  const claims = [retain(R.distraught_murdock.cid, at)]
  const dm = R.distraught_murdock
  // The sweep took what only distraught_murdock held (F1); then, as if every file had been its own, all of it.
  for (const [swept, slot] of [[{ bytes: dm.exclusive, blocks: dm.exclusiveBlocks }, '14.6 MB still stored here · 503 file blocks'],
    [{ bytes: dm.reachable, blocks: dm.blocks }, '0 B still stored here · 0 file blocks']]) {
    installDom(); resetFolds()
    const { ex, cid } = await roblabRun('distraught_murdock', { claims, extra: releasedRoot(3, at, swept) })
    const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
    await settle()
    assert.equal(page.querySelector('.page-head').querySelector('.state').textContent, 'Released')
    assert.equal(page.querySelector('[data-slot=size]').textContent, slot)
    assert.equal(page.querySelector('[data-run-size]').textContent, SWEPT_ALONE)
    assert.doesNotMatch(page.textContent, /references|can reclaim it from/)
  }
})

test('nothing of its own and past its date, or with no grace known: the same "any files" wording (sharp_magritte never held any)', async () => {
  installDom(); resetFolds()
  const old = Date.now() - 30 * DAY_MS
  const { ex, cid } = await roblabRun('sharp_magritte', { claims: [retain(R.sharp_magritte.cid, old)], extra: releasedRoot(1, old) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(page.querySelector('[data-run-size]').textContent, SWEPT_ALONE)
  installDom(); resetFolds()
  const recent = Date.now() - DAY_MS
  const { ex: noGrace } = await roblabRun('sharp_magritte', { claims: [retain(R.sharp_magritte.cid, recent)], extra: releasedRoot(1, recent), grace: null })
  const unknown = await run(noGrace, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(unknown.querySelector('.page-head').querySelector('.state').textContent, 'Releasing')
  assert.equal(unknown.querySelector('[data-run-size]').textContent, SWEPT_ALONE)
})

test('whether the earliest date has passed is the Explorer\'s clock (ex.now), on the run page and on #/storage', async () => {
  installDom(); resetFolds()
  const released = Date.now() - DAY_MS
  const later = released + ROBLAB.graceMs + DAY_MS
  const { ex, cid } = await roblabRun('distraught_murdock', { claims: [retain(R.distraught_murdock.cid, released)], extra: releasedRoot(3, released), now: later })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(page.querySelector('.page-head').querySelector('.state').textContent, 'Released')
  assert.match(page.querySelector('[data-run-size]').textContent, /a sweep can reclaim it now\./)
  const { storage } = await import('../src/views/storage.js')
  const node = await storage(ex, ctxFor())
  const row = node.querySelectorAll('[data-storage-run]').find(r => r.textContent.includes('distraught_murdock'))
  assert.equal(row.querySelector('.state').textContent, 'Released')
})

test('a released run past its earliest date that still holds files of its own: a sweep can reclaim them now', async () => {
  installDom(); resetFolds()
  const at = Date.now() - 30 * DAY_MS
  const { ex, cid } = await roblabRun('distraught_murdock', { claims: [retain(R.distraught_murdock.cid, at)], extra: releasedRoot(3, at) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(page.querySelector('.page-head').querySelector('.state').textContent, 'Released')
  assert.equal(page.querySelector('[data-run-size]').textContent,
    "This run's files still stored here: 78.4 MB; 63.7 MB of it is releasing and a sweep can reclaim it now. Restore content holds whatever of its files are still here.")
})

test('a released run with nothing of its own before its earliest date says none of it is releasing', async () => {
  installDom(); resetFolds()
  const at = Date.now() - DAY_MS
  const { ex, cid } = await roblabRun('sharp_magritte', { claims: [retain(R.sharp_magritte.cid, at)], extra: releasedRoot(1, at) })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.match(page.querySelector('.page-head').querySelector('.state').textContent, /^Releasing · [A-Z][a-z]{2} \d{1,2}/)
  assert.equal(page.querySelector('[data-run-size]').textContent,
    "This run's files still stored here: 53.4 MB; none of it is releasing, since other runs, Selections or pins here hold every file, or a sweep has already reclaimed them. Restore content holds whatever of its files are still here.")
})

test('a run released since the snapshot: its bar and kv are the snapshot\'s, marked as of it, and the releasing figure waits for the next one', async () => {
  installDom(); resetFolds()
  const rel = retain(R.distraught_murdock.cid, Date.now() - 3600_000)
  const { ex, cid } = await roblabRun('distraught_murdock', { claims: [rel], tail: [rel] })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  const line = page.querySelector('[data-run-size]').textContent
  assert.match(line, /^This run's files still stored here, as of the snapshot: 78\.4 MB\. The next snapshot says how much of it is releasing; a sweep can reclaim that from [A-Z][a-z]{2} \d{1,2}(, \d{4})?\. Restore content holds whatever of its files are still here\.$/)
  assert.doesNotMatch(line, /63\.7 MB|0 B/)
  const here = page.querySelector('[data-run-storage]')
  assert.equal(here.querySelector('.ph').querySelector('.muted').textContent, 'as of the snapshot')
  assert.match(here.querySelector('.sbar').querySelector('.legend').textContent, /Held 78\.4 MB.*Releasing 0 B/, 'the snapshot held it')
  assert.deepEqual(kvOf(here)['Only this run'], '63.7 MB')
  assert.ok(page.querySelector('[data-run-storage-asof]'))
})

test('a run logged since the snapshot makes a snapshot run\'s Release figures stale; its own tab still walks (Review focus 4)', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const member = await buildMember({ now, extra: ({ runs }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO root VALUES (1, ?, 'run', 'test', 'held', ?)", bind: [runs.R1.completion, runs.R1.finished] })
    db.exec('INSERT INTO root_stats VALUES (1, 20, 2, 20, 2, 0, 0)')
    db.exec('INSERT INTO store_stats VALUES (20, 2, 20, 0, 20, 0, 0, 0)')
  } }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const snap = await run(explorer, member.runs.R1.completion, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const confirm = snap.querySelector('[data-release-confirm]')
  assert.match(confirm.textContent, /Freed bytes are known once the next snapshot is written/)
  assert.equal(confirm.querySelector('#release-confirm').textContent, 'Release')
  assert.equal(snap.querySelector('[data-run-storage-asof]').textContent,
    'Only this run and Pinned here are as of the snapshot; runs and Selections logged since then are not counted yet.', 'the same sentence #/storage writes')
  const tail = await run(explorer, member.runs.R2.completion, ctxFor(), { tab: 'storage', release: true })
  await settle()
  assert.match(tail.querySelector('[data-run-size]').textContent, /^This run holds 20 B \(2 file blocks\) in roblab\./)
  assert.match(tail.querySelector('[data-release-confirm]').textContent, /Freed bytes are known once the next snapshot is written/)
})

test('the run\'s Storage tab on a lean snapshot says why its figures are missing, and sizes by the walk', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const member = await buildMember({ now, extra: () => ({ log: [], rows: (db) => db.exec("INSERT INTO meta VALUES ('storage_omitted', 'out_of_memory')") }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const page = await run(explorer, member.runs.R1.completion, ctxFor(), { tab: 'storage' })
  await settle()
  assert.match(page.querySelector('[data-storage-omitted]').textContent,
    /^Storage figures are left out of this store's snapshot: computing them ran out of memory\./)
  assert.match(page.querySelector('[data-run-size]').textContent, /^This run holds 20 B \(2 file blocks\) in roblab\./)
})

test('"earliest <date>" stays on one line in the four-column confirmation', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath', { extra: [
    `INSERT INTO root VALUES (6, '${R.sad_yonath.cid}', 'pin', 'ana@x', 'pinned', '2026-10-25T09:00:00Z')`,
    `UPDATE root_stats SET pinned_bytes = 5000000 WHERE root_id = 2`] })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const confirm = page.querySelector('[data-release-confirm]')
  assert.equal(confirm.querySelector('.grid-3').children.length, 4, 'frees, shared, pinned, kept')
  const earliest = confirm.querySelector('[data-release-frees]').querySelector('.nowrap')
  assert.match(earliest.textContent, /^earliest [A-Z][a-z]{2} \d{1,2}(, \d{4})?$/)
  const { APP_CSS } = await import('../src/styles.js')
  assert.match(APP_CSS, /\.release-confirm \.nowrap \{ white-space: nowrap; \}/)
})

// ---- Fix round 2 (F3 follow-up) ----------------------------------------------
test('a lean snapshot keeps the sweep grace: the confirmation names its earliest date', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const member = await buildMember({ now, extra: () => ({ log: [], rows: (db) => db.exec(
    "INSERT INTO meta VALUES ('storage_omitted', 'over_cap'), ('sweep_grace_ms', '1209600000'), ('snapshot_max_bytes', '67108864')") }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  assert.equal(await explorer.storeStats(), null)
  assert.equal(await explorer.sweepGraceMs(), 1_209_600_000)
  const page = await run(explorer, member.runs.R1.completion, ctxFor(), { tab: 'storage', release: true })
  await settle()
  const frees = page.querySelector('[data-release-confirm]').querySelector('[data-release-frees]')
  assert.match(frees.textContent, /after 14-day grace · earliest [A-Z][a-z]{2} \d{1,2}/)
  assert.match(page.querySelector('[data-storage-omitted]').textContent, /cas\.snapshot\.maxBytes \(67\.1 MB\)/)
  const { explorer: noKey } = await openRoblab({ grace: null })
  assert.equal(await noKey.sweepGraceMs(), null, 'no key, no grace')
})

test('a read-only store\'s run tab has no Compare panel: what each release would free shows only where you can release', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('sad_yonath')
  const ro = await run(ex, cid, ctxFor({ writable: false }), { tab: 'storage' })
  await settle()
  assert.equal(ro.querySelector('[data-release-compare]'), null)
  assert.doesNotMatch(ro.textContent, /would free|Frees/)
  assert.ok(ro.querySelector('[data-run-storage]'), 'This run in roblab stays')
  assert.deepEqual(kvOf(ro.querySelector('[data-run-storage]')).Reachable, '78.4 MB · 894 file blocks')
  const rw = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.ok(rw.querySelector('[data-release-compare]'))
})

test('a read-only store\'s run tab leaves out Only this run, the freed-on-release figure by another name', async () => {
  installDom(); resetFolds()
  const { ex, cid } = await roblabRun('distraught_murdock')
  const ro = await run(ex, cid, ctxFor({ writable: false }), { tab: 'storage' })
  await settle()
  assert.deepEqual(Object.keys(kvOf(ro.querySelector('[data-run-storage]'))), ['Reachable', 'Shared', 'Pinned here'])
  assert.doesNotMatch(ro.textContent, /63\.7 MB/)
  const rw = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  assert.equal(kvOf(rw.querySelector('[data-run-storage]'))['Only this run'], '63.7 MB')
  installDom(); resetFolds()
  const pin = { ...pinClaim, cid: 'bafyreipinnew' }
  const { ex: stale } = await roblabRun('distraught_murdock', { claims: [pin], tail: [pin] })
  const asof = await run(stale, cid, ctxFor({ writable: false }), { tab: 'storage' })
  await settle()
  assert.equal(asof.querySelector('[data-run-storage-asof]').textContent, 'Pinned here is as of the snapshot; a pin, release or deletion since then is not counted yet.')
})

test('the run\'s Storage tab on a seeded_lean snapshot names reindex --seed false', async () => {
  installDom(); resetFolds()
  const now = Date.now()
  const member = await buildMember({ now, extra: () => ({ log: [], rows: (db) => db.exec(
    "INSERT INTO meta VALUES ('storage_omitted', 'seeded_lean'), ('snapshot_max_bytes', '0')") }) })
  const { explorer } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const page = await run(explorer, member.runs.R1.completion, ctxFor(), { tab: 'storage' })
  await settle()
  const note = page.querySelector('[data-storage-omitted]')
  assert.equal(note.getAttribute('data-storage-omitted'), 'seeded_lean')
  assert.match(note.textContent, /the cache that wrote the snapshot was rebuilt from a snapshot without them\. Run nextflow plugin nf-blocks:reindex --seed false to rebuild it from the store\.$/)
  assert.match(page.querySelector('[data-run-size]').textContent, /^This run holds 20 B \(2 file blocks\) in roblab\./, 'sizes walk')
})

// ---- Fix round 3 (G2) ------------------------------------------------------
test('the run page writes its days by the Explorer\'s clock (ex.now): a date in the clock\'s own year has no year', async () => {
  installDom(); resetFolds()
  const now = Date.parse('2027-06-01T12:00:00Z')
  const released = now - 2 * DAY_MS
  const earliest = released + ROBLAB.graceMs
  const { ex, cid } = await roblabRun('distraught_murdock', { claims: [retain(R.distraught_murdock.cid, released)], extra: releasedRoot(3, released), now })
  const page = await run(ex, cid, ctxFor(), { tab: 'storage' })
  await settle()
  const day = new Date(earliest).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  assert.equal(page.querySelector('.page-head').querySelector('.state').textContent, `Releasing · ${day}`)
  assert.match(page.querySelector('[data-run-size]').textContent, new RegExp(`a sweep can reclaim it from ${day}\\.`))
  // The header's Finished date reads the same clock: distraught_murdock finished in 2026, so under a 2027 clock it names the year.
  assert.match(page.querySelector('[data-slot=finished]').textContent, /Finished Oct 24, 2026/)
  assert.doesNotMatch(readFileSync(new URL('../src/views/run.js', import.meta.url), 'utf8'), /new Date\(\)/)
  assert.doesNotMatch(page.textContent, /holds it here again/)
  installDom(); resetFolds()
  const { ex: held, cid: sad } = await roblabRun('sad_yonath', { now })
  const confirm = (await run(held, sad, ctxFor(), { tab: 'storage', release: true })).querySelector('[data-release-confirm]')
  await settle()
  const soon = new Date(now + ROBLAB.graceMs).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  assert.equal(confirm.querySelector('[data-release-frees]').querySelector('.nowrap').textContent, `earliest ${soon}`)
})
