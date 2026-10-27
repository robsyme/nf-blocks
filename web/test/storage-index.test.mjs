// web/test/storage-index.test.mjs
// Background store indexing (W2): each Storage card shows its member's index
// state and offers Index locally, Index and push, Push to store and Retry.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { frame, installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { createMembers } from '../src/members.js'
import { IndexError } from '../src/indexing.js'
import { storage } from '../src/views/storage.js'
import { SnapshotError } from '../src/db.js'
import { openRoblab } from './storage-fixture.mjs'

const settle = async () => { await frame(); await frame(); await new Promise(r => setTimeout(r, 10)) }
const text = (el) => el.textContent.replace(/\s+/g, ' ')
const noSnapshot = () => new SnapshotError('no_snapshot', 'no Index Snapshot')

const LIST = [
  { alias: 'roblab', writable: true, base: 'm/roblab/', kind: 'local', location: null },
  { alias: 'cloud', writable: false, base: 'm/cloud/', kind: 's3', location: 's3://lab-blocks' },
  { alias: 'org-s3', writable: false, base: 'm/org/', kind: 's3', location: 's3://org-bucket' },
]
const FIGURES = { storeStats: async () => ({ stored: 1.82e12, referenced: 5.6e12, dedup: 5.6 / 1.82, pinned: 640e9, held: 1020e9, releasing: 160e9, partial: false }),
  asserterStats: async () => [], storageChanges: () => ({ claims: false, logged: false }), storageOmitted: null }

const minutesAgo = (n) => new Date(Date.now() - n * 60_000).toISOString()
const index = (state, fields = {}) => ({ state, at: null, progress: null, reason: null, mode: null, local: false, ...fields })
const none = (reason = 'absent') => index('none', { reason })
const local = (at) => index('local', { at, local: true })

/**
 * A Storage page over roblab, cloud and org-s3 with a fake ctx.indexing: `start`
 * calls are recorded, `running()` is `runningAlias`, and `change(alias, index)`
 * does what W1's createIndexing does on a poll (sets the member's index, tells
 * the listeners). `cloud` opens a snapshot only when `open` is set.
 */
async function page({ cloud = {}, others = {}, runningAlias = null, open = false, startError = null, withIndexing = true } = {}) {
  installDom()
  const { explorer } = await openRoblab()
  const list = LIST.map(m => ({ ...m, ...(m.alias === 'cloud' ? cloud : others[m.alias] ?? {}) }))
  const exes = { roblab: explorer, 'org-s3': FIGURES, ...(open ? { cloud: FIGURES } : {}) }
  const members = createMembers({ member: 'roblab', all: true, members: list }, { openExplorer: async (m) => { if (!exes[m.alias]) throw noSnapshot(); return exes[m.alias] } })
  const started = []
  const listeners = new Set()
  const state = { runningAlias }
  const indexing = {
    stateOf: (alias) => members.list.find(m => m.alias === alias)?.index ?? null,
    running: () => state.runningAlias,
    start: async (alias, mode) => { started.push([alias, mode]); if (startError) throw startError },
    onChange: (fn) => { listeners.add(fn); return () => listeners.delete(fn) },
  }
  const ctx = {
    tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, mark: () => {}, use: () => {},
    members, presence: null, viewer: null, identity: null,
    store: { name: 'roblab', key: 'a', all: true, hrefIn: (alias, hash) => `http://h/index.html?member=${alias}${hash}` },
    write: { available: true, here: true, writable: 'roblab', reason: null, hrefFor: (h) => h, writer: null, run: () => {} },
    ...(withIndexing ? { indexing } : {}),
  }
  const node = await storage(explorer, ctx)
  await settle()
  const cardOf = (alias) => node.querySelectorAll('[data-storage-card]').find(c => c.dataset.storageCard === alias)
  const change = (alias, next) => {
    members.list.find(m => m.alias === alias).index = next
    for (const fn of [...listeners]) fn(alias, next)
  }
  return { node, ctx, started, state, change, cardOf, listeners }
}

const cardOf = async (alias, member, options = {}) => (await page({ cloud: member, ...options })).cardOf(alias)
const modes = (card) => card.querySelectorAll('[data-index-mode]').map(b => [b.textContent, b.dataset.indexMode])

test('a member not indexed offers Index locally and Index and push', async () => {
  const { cardOf, started } = await page({ cloud: { index: none('absent') } })
  const card = cardOf('cloud')
  assert.match(text(card), /Not indexed: its runs and Selections are not listed/)
  assert.doesNotMatch(text(card), /Its own snapshot can't be used/)
  assert.deepEqual(modes(card), [['Index locally', 'local'], ['Index and push', 'push']])
  card.querySelector('[data-index-mode=local]').click()
  assert.deepEqual(started, [['cloud', 'local']])
})

test('a card that opens the store keeps its link, around its head and figures, and its buttons outside it', async () => {
  const card = await cardOf('cloud', { index: none('absent') })
  assert.equal(card.tagName, 'DIV')
  const open = card.querySelector('a[data-storage-card-open]')
  assert.equal(open.getAttribute('href'), 'http://h/index.html?member=cloud#/storage')
  assert.equal(open.getAttribute('title'), 'Open the Storage page in cloud')
  assert.match(text(open), /cloud/)
  assert.equal(open.querySelectorAll('[data-index-mode]').length, 0, 'a button inside a link would open the page when pressed')
  assert.equal(card.querySelector('[data-index-actions]').querySelectorAll('[data-index-mode]').length, 2)
})

test('a member indexed on this machine says so and when, and offers Push to store', async () => {
  const card = await cardOf('cloud', { index: local(minutesAgo(2)) }, { open: true })
  assert.match(text(card), /Indexed on this machine · 2 min ago/)
  assert.match(text(card), /1\.82 TB stored/, 'its figures show when the local snapshot opens')
  assert.deepEqual(modes(card), [['Push to store', 'push']])
})

test('a member with its own snapshot shows figures and no buttons', async () => {
  const card = await cardOf('cloud', { index: index('snapshot', { at: minutesAgo(5) }) }, { open: true })
  assert.match(text(card), /1\.82 TB stored/)
  assert.doesNotMatch(text(card), /Not indexed|Indexed on this machine/)
  assert.deepEqual(modes(card), [])
})

test('push is disabled with the reason on a member a copy cannot write', async () => {
  const card = await cardOf('cloud', { index: none('absent'), copy_target: false, copy_reason: 'readOnly is set for this store' })
  const push = card.querySelector('[data-index-mode=push]')
  assert.equal(push.disabled, true)
  assert.equal(push.getAttribute('title'), 'readOnly is set for this store')
  assert.match(text(card.querySelector('[data-index-why]')), /Can't push here: readOnly is set for this store/)
  assert.equal(card.querySelector('[data-index-mode=local]').disabled, false, 'Index locally needs no write')
})

test('a push that can be written has no reason line and is enabled', async () => {
  const card = await cardOf('cloud', { index: none('absent'), copy_target: true, copy_reason: null })
  assert.equal(card.querySelector('[data-index-mode=push]').disabled, false)
  assert.equal(card.querySelector('[data-index-why]'), null)
})

test('while another store is being indexed, the Index buttons are disabled and say why', async () => {
  const { cardOf, started } = await page({ cloud: { index: none('absent') }, runningAlias: 'org-s3',
    others: { 'org-s3': { index: index('indexing', { mode: 'local', progress: { phase: 'listing', done: 0, total: 0, startedAt: new Date().toISOString() } }) } } })
  const card = cardOf('cloud')
  for (const button of card.querySelectorAll('[data-index-mode]')) {
    assert.equal(button.disabled, true)
    assert.equal(button.getAttribute('title'), 'another store is being indexed')
  }
  card.querySelector('[data-index-mode=local]').click()
  assert.deepEqual(started, [], 'a disabled button starts nothing')
})

test('an indexing member shows its progress and updates it in place', async () => {
  const progress = (done) => ({ phase: 'reading', done, total: 10, startedAt: new Date(Date.now() - 130_000).toISOString() })
  const { cardOf, change, state } = await page({ cloud: { index: index('indexing', { mode: 'local', progress: progress(2) }) }, runningAlias: 'cloud' })
  const card = cardOf('cloud')
  assert.match(text(card.querySelector('[data-index-progress]')), /^Reading 2 of 10 metadata blocks · 2m 1\ds$/)
  assert.deepEqual(modes(card), [])
  change('cloud', index('indexing', { mode: 'local', progress: progress(9) }))
  assert.match(text(card.querySelector('[data-index-progress]')), /^Reading 9 of 10 metadata blocks/)
  assert.equal(cardOf('cloud'), card, 'the card node is the same')
  state.runningAlias = null
  change('cloud', index('snapshot', { at: new Date().toISOString() }))
  assert.equal(card.querySelector('[data-index-progress]'), null)
})

test('the card of a member that is not indexing follows another member\'s job: its buttons are disabled while it runs and enabled after', async () => {
  const { cardOf, change, state } = await page({ cloud: { index: none('absent') } })
  const card = cardOf('cloud')
  assert.equal(card.querySelector('[data-index-mode=local]').disabled, false)
  state.runningAlias = 'org-s3'
  change('org-s3', index('indexing', { mode: 'local', progress: { phase: 'listing', done: 0, total: 0, startedAt: new Date().toISOString() } }))
  assert.equal(card.querySelector('[data-index-mode=local]').disabled, true)
  state.runningAlias = null
  change('org-s3', index('snapshot', { at: new Date().toISOString() }))
  assert.equal(card.querySelector('[data-index-mode=local]').disabled, false)
})

test('a started job shows its progress at once, without waiting for the next poll', async () => {
  const { cardOf, ctx } = await page({ cloud: { index: none('absent') } })
  const card = cardOf('cloud')
  ctx.indexing.start = async () => {
    ctx.members.list.find(m => m.alias === 'cloud').index = index('indexing', { mode: 'local', progress: { phase: 'listing', done: 0, total: 0, startedAt: new Date().toISOString() } })
  }
  await card.querySelector('[data-index-mode=local]').click()
  assert.match(text(card.querySelector('[data-index-progress]')), /^Listing blocks… · \d+s$/)
  assert.deepEqual(modes(card), [])
})

test('a click the server refuses shows its message in the card', async () => {
  const { cardOf } = await page({ cloud: { index: none('absent') }, startError: new IndexError('index_running', 'Another store is already being indexed.', 409) })
  const card = cardOf('cloud')
  await card.querySelector('[data-index-mode=push]').click()
  assert.equal(text(card.querySelector('[data-index-error]')), 'Another store is already being indexed.')
  assert.equal(modes(card).length, 2, 'the buttons stay for another try')
})

test('a failed job shows its reason and a Retry that repeats its mode', async () => {
  const { cardOf, started } = await page({ cloud: { index: index('failed', { mode: 'push', reason: 'the credentials were refused' }) } })
  const card = cardOf('cloud')
  assert.match(text(card), /Indexing failed: the credentials were refused/)
  assert.doesNotMatch(text(card), /Indexed on this machine/)
  assert.deepEqual(modes(card), [['Retry', 'push']])
  card.querySelector('[data-index-mode=push]').click()
  assert.deepEqual(started, [['cloud', 'push']])
})

test('a failed push of a member indexed on this machine says so and shows its figures', async () => {
  const card = await cardOf('cloud', { index: index('failed', { mode: 'push', reason: 'boom', local: true, at: minutesAgo(1) }) }, { open: true })
  assert.match(text(card), /Indexing failed: boom/)
  assert.match(text(card), /Indexed on this machine/)
  assert.match(text(card), /1\.82 TB stored/)
})

test('a failed push to a member a copy cannot write disables its Retry with the reason', async () => {
  const card = await cardOf('cloud', { index: index('failed', { mode: 'push', reason: 'boom' }), copy_target: false, copy_reason: 'readOnly is set for this store' })
  assert.equal(card.querySelector('[data-index-mode=push]').disabled, true)
  assert.match(text(card.querySelector('[data-index-why]')), /Can't push here: readOnly is set for this store/)
})

test('a member outside cas.resolve says so and offers nothing', async () => {
  const card = await cardOf('cloud', { index: index('outside') })
  assert.match(text(card), /Not in cas\.resolve, so the explorer does not index it/)
  assert.deepEqual(modes(card), [])
})

test('a member whose own snapshot cannot be used says why', async () => {
  const reason = 'written for another incarnation of this store'
  const card = await cardOf('cloud', { index: none(reason) })
  assert.match(text(card), /Not indexed: its runs and Selections are not listed\./)
  assert.match(text(card), new RegExp(`Its own snapshot can't be used \\(${reason}\\)\\.`))
  assert.equal(modes(card).length, 2)
})

test('without index objects the cards are as before', async () => {
  for (const withIndexing of [true, false]) {
    const { cardOf } = await page({ cloud: {}, withIndexing })
    const card = cardOf('cloud')
    assert.match(text(card), /No snapshot yet\.$/)
    assert.deepEqual(modes(card), [])
    assert.equal(card.querySelector('[data-index-actions]')?.querySelectorAll('button').length ?? 0, 0)
    assert.ok(card.querySelector('a[data-storage-card-open]'), 'the open link stays')
  }
})

test('a card stops listening once it has been connected and then removed', async () => {
  const { cardOf, listeners, change } = await page({ cloud: { index: none('absent') } })
  const card = cardOf('cloud')
  const before = listeners.size
  assert.ok(before >= 3, 'every card listens')
  let connected = true
  Object.defineProperty(card, 'isConnected', { get: () => connected })
  change('cloud', none('absent'))
  connected = false
  change('cloud', none('absent'))
  assert.equal(listeners.size, before - 1)
})

test('during a refresh of a member indexed on this machine the card keeps its figures beside the progress line', async () => {
  const at = minutesAgo(3)
  const refreshing = (phase) => index('indexing', { mode: 'local', local: true, at, progress: { phase, done: -1, total: -1, startedAt: new Date().toISOString() } })
  const { cardOf, change } = await page({ cloud: { index: refreshing('log') }, runningAlias: 'cloud', open: true })
  const card = cardOf('cloud')
  assert.match(text(card.querySelector('[data-index-progress]')), /^Reading the Store Log…/)
  assert.match(text(card), /1\.82 TB stored/)
  change('cloud', refreshing('snapshot'))
  assert.match(text(card.querySelector('[data-index-progress]')), /^Building the snapshot…/)
  assert.match(text(card), /1\.82 TB stored/, 'a progress tick does not read the figures again')
})

test('a refused click message clears once the card state changes', async () => {
  const { cardOf, change } = await page({ cloud: { index: none('absent') }, startError: new IndexError('index_running', 'Another store is already being indexed.', 409) })
  const card = cardOf('cloud')
  await card.querySelector('[data-index-mode=local]').click()
  assert.ok(card.querySelector('[data-index-error]'))
  change('cloud', index('indexing', { mode: 'local', progress: { phase: 'listing', done: 0, total: 0, startedAt: new Date().toISOString() } }))
  assert.equal(card.querySelector('[data-index-error]'), null)
})

test('a refused click message clears once another job starts or ends', async () => {
  const { cardOf, change, state } = await page({ cloud: { index: none('absent') }, startError: new IndexError('index_running', 'Another store is already being indexed.', 409) })
  const card = cardOf('cloud')
  await card.querySelector('[data-index-mode=local]').click()
  change('org-s3', index('snapshot', { at: minutesAgo(1) }))
  assert.ok(card.querySelector('[data-index-error]'), 'a change elsewhere that leaves the running job alone keeps it')
  state.runningAlias = 'org-s3'
  change('org-s3', index('indexing', { mode: 'local', progress: { phase: 'listing', done: 0, total: 0, startedAt: new Date().toISOString() } }))
  assert.equal(card.querySelector('[data-index-error]'), null)
})

test('a member indexed on this machine whose snapshot does not open says it could not be read', async () => {
  const card = await cardOf('cloud', { index: local(minutesAgo(2)) })
  assert.match(text(card), /Could not be read \(no Index Snapshot\)\./)
  assert.doesNotMatch(text(card), /No snapshot yet/)
})
