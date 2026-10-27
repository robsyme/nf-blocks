// The page's index client, its progress words and the watch that polls while a job runs.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { IndexError, createIndexing, elapsedText, indexChangeHandler, indexClient, progressText } from '../src/indexing.js'

const t0 = Date.parse('2026-10-27T10:00:00.000Z')
const at = (phase, done = null, total = null) => ({ state: 'indexing', at: null, reason: null, mode: 'local', local: false,
  progress: { phase, done, total, startedAt: new Date(t0).toISOString() } })
const settled = (state, extra = {}) => ({ state, at: '2026-10-27T10:05:00.000Z', reason: null, mode: 'local', local: true, progress: null, ...extra })

const json = (status, value) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })

/** A timer the test runs on demand: `tick()` fires the pending callback, `pending()` says whether one waits. */
function manualTimer() {
  let next = null
  let n = 0
  return {
    setTimer: (fn) => { next = { fn, id: ++n }; return next.id },
    clearTimer: (id) => { if (next?.id === id) next = null },
    pending: () => next !== null,
    tick: async () => { const t = next; next = null; await t?.fn() },
  }
}

test('progress text by phase, with the elapsed time', () => {
  const now = t0 + 130_000
  assert.equal(progressText(at('listing'), 'cloud', now), 'Listing blocks… · 2m 10s')
  assert.equal(progressText(at('reading', 1240, 8615), 'cloud', now), 'Reading 1,240 of 8,615 metadata blocks · 2m 10s')
  assert.equal(progressText(at('ingesting', 12, 340), 'cloud', now), 'Indexing 12 of 340 records · 2m 10s')
  assert.equal(progressText(at('log'), 'cloud', now), 'Reading the Store Log… · 2m 10s')
  assert.equal(progressText(at('snapshot'), 'cloud', now), 'Building the snapshot… · 2m 10s')
  assert.equal(progressText(at('push'), 'cloud', now), 'Pushing to cloud… · 2m 10s')
})

test('elapsed text', () => {
  assert.deepEqual([45_000, 130_000, 3_780_000].map(elapsedText), ['45s', '2m 10s', '1h 3m'])
})

test('the watch polls only while a job runs, and invalidates a member once when its job ends', async () => {
  const invalidated = []
  const cloud = { alias: 'cloud', index: at('reading', 1, 9) }
  const members = { list: [cloud, { alias: 'roblab', index: settled('snapshot') }], invalidate: (a) => invalidated.push(a) }
  const answers = [at('reading', 5, 9), settled('local')]
  let polls = 0
  const fetchStatus = async () => ({ members: { cloud: answers[polls++] } })
  const timer = manualTimer()
  const heard = []
  const indexing = createIndexing(members, { client: null, fetchStatus, ...timer })
  indexing.onChange((alias, index) => heard.push([alias, index.state, invalidated.length]))

  assert.equal(indexing.running(), 'cloud')
  indexing.watch()
  indexing.watch() // a second call does not queue a second poll
  await timer.tick()
  await timer.tick()
  await timer.tick()

  assert.deepEqual(invalidated, ['cloud'])
  assert.equal(polls, 2)
  assert.equal(timer.pending(), false)
  assert.equal(indexing.running(), null)
  assert.equal(cloud.index.state, 'local')
  // the member is invalidated before its listeners hear that the job ended
  assert.deepEqual(heard, [['cloud', 'indexing', 0], ['cloud', 'local', 1]])
})

test('the watch does not poll when nothing is indexing, and a server without index reads null', () => {
  const members = { list: [{ alias: 'a' }, { alias: 'b', index: settled('none') }], invalidate() {} }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus: async () => assert.fail('polled'), ...timer })
  indexing.watch()
  assert.equal(timer.pending(), false)
  assert.equal(indexing.stateOf('a'), null)
  assert.equal(indexing.stateOf('b').state, 'none')
  assert.equal(indexing.running(), null)
})

test('a poll that changes nothing does not call the listeners or invalidate', async () => {
  const invalidated = []
  const members = { list: [{ alias: 'cloud', index: at('reading', 5, 9) }], invalidate: (a) => invalidated.push(a) }
  const fetchStatus = async () => ({ members: { cloud: at('reading', 5, 9) } })
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus, ...timer })
  let heard = 0
  indexing.onChange(() => heard++)
  indexing.watch()
  await timer.tick()
  assert.equal(heard, 0)
  assert.deepEqual(invalidated, [])
  assert.equal(timer.pending(), true)
})

test('a job that fails is invalidated too, and an unsubscribed listener hears nothing', async () => {
  const invalidated = []
  const members = { list: [{ alias: 'cloud', index: at('listing') }], invalidate: (a) => invalidated.push(a) }
  const fetchStatus = async () => ({ members: { cloud: settled('failed', { at: null, local: false, reason: 'no access' }) } })
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus, ...timer })
  let heard = 0
  const off = indexing.onChange(() => heard++)
  off()
  indexing.watch()
  await timer.tick()
  assert.deepEqual(invalidated, ['cloud'])
  assert.equal(heard, 0)
})

test('start posts the member and mode with the token; a 409 is index_running', async () => {
  const calls = []
  const fetchFn = async (url, init) => {
    calls.push({ url, init, body: JSON.parse(init.body) })
    return calls.length === 1
      ? json(202, { member: 'cloud', mode: 'push', index: at('listing') })
      : json(409, { error: 'index_running', message: 'another store is being indexed; try again when it has finished' })
  }
  const client = indexClient({ endpoint: 'http://h/api/index', token: 'tok', fetchFn })
  const index = await client.start('cloud', 'push')
  assert.equal(index.state, 'indexing')
  const [call] = calls
  assert.equal(call.url, 'http://h/api/index')
  assert.equal(call.init.method, 'POST')
  assert.equal(call.init.headers['Content-Type'], 'application/json')
  assert.equal(call.init.headers['X-NF-Blocks-Token'], 'tok')
  assert.equal(call.init.body, '{"member":"cloud","mode":"push"}')
  await assert.rejects(client.start('roblab', 'local'), (e) => {
    assert.ok(e instanceof IndexError)
    assert.equal(e.code, 'index_running')
    assert.equal(e.status, 409)
    assert.equal(e.message, 'another store is being indexed; try again when it has finished')
    return true
  })
})

test('a failed start without a server error code is index_failed', async () => {
  const client = indexClient({ endpoint: 'e', token: 't', fetchFn: async () => new Response('boom', { status: 500 }) })
  await assert.rejects(client.start('cloud', 'local'), (e) => e.code === 'index_failed' && e.status === 500)
  const unreachable = indexClient({ endpoint: 'e', token: 't', fetchFn: async () => { throw new Error('offline') } })
  await assert.rejects(unreachable.start('cloud', 'local'), (e) => e.code === 'index_failed' && e.status === 0 && /offline/.test(e.message))
})

test('start stores the answer and begins the watch', async () => {
  const cloud = { alias: 'cloud', index: settled('none') }
  const members = { list: [cloud], invalidate() {} }
  const client = { start: async (alias, mode) => { assert.deepEqual([alias, mode], ['cloud', 'local']); return at('listing') } }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client, fetchStatus: async () => ({ members: {} }), ...timer })
  await indexing.start('cloud', 'local')
  assert.equal(cloud.index.state, 'indexing')
  assert.equal(indexing.running(), 'cloud')
  assert.equal(timer.pending(), true)
})

test('three failed polls in a row stop the watch', async () => {
  const members = { list: [{ alias: 'cloud', index: at('reading', 1, 9) }], invalidate() {} }
  let polls = 0
  const fetchStatus = async () => { polls++; throw new Error('down') }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus, ...timer })
  indexing.watch()
  await timer.tick()
  await timer.tick()
  assert.equal(timer.pending(), true)
  await timer.tick()
  assert.equal(polls, 3)
  assert.equal(timer.pending(), false)
  indexing.watch() // a new watch starts afresh
  assert.equal(timer.pending(), true)
})

test('a successful poll resets the failure count', async () => {
  const members = { list: [{ alias: 'cloud', index: at('reading', 1, 9) }], invalidate() {} }
  const results = [null, null, { cloud: at('reading', 2, 9) }, null, null]
  let polls = 0
  const fetchStatus = async () => { const r = results[polls++]; if (!r) throw new Error('down'); return { members: r } }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus, ...timer })
  indexing.watch()
  for (let i = 0; i < 5; i++) await timer.tick()
  assert.equal(polls, 5)
  assert.equal(timer.pending(), true)
})

test('progress text without a progress object or with a bad start time leaves out what it cannot say', () => {
  assert.equal(progressText({ state: 'indexing', progress: null }, 'cloud'), 'Indexing…')
  const bad = at('listing')
  bad.progress.startedAt = 'not a time'
  assert.equal(progressText(bad, 'cloud', t0), 'Listing blocks…')
})

test('a listener or invalidate that throws is reported, not counted as a failed poll', async () => {
  const members = { list: [{ alias: 'cloud', index: at('reading', 1, 9) }], invalidate: () => { throw new Error('invalidate broke') } }
  const answers = [at('reading', 2, 9), at('reading', 3, 9), at('reading', 4, 9), settled('local')]
  let polls = 0
  const fetchStatus = async () => ({ members: { cloud: answers[polls++] } })
  const timer = manualTimer()
  const errors = []
  const indexing = createIndexing(members, { client: null, fetchStatus, onError: (e) => errors.push(e.message), ...timer })
  let heard = 0
  indexing.onChange(() => { heard++; throw new Error('listener broke') })
  indexing.onChange(() => heard++)
  indexing.watch()
  for (let i = 0; i < 4; i++) await timer.tick()
  assert.equal(polls, 4, 'three throwing polls in a row did not stop the watch')
  assert.equal(heard, 8, 'the second listener heard every change')
  assert.deepEqual(errors, ['listener broke', 'listener broke', 'listener broke', 'invalidate broke', 'listener broke'])
  assert.equal(indexing.running(), null)
})

test('a member indexing that a poll answer leaves out stops being treated as running', async () => {
  const cloud = { alias: 'cloud', index: at('reading', 1, 9) }
  const invalidated = []
  const members = { list: [cloud], invalidate: (a) => invalidated.push(a) }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus: async () => ({ members: {} }), ...timer })
  const heard = []
  indexing.onChange((alias, index) => heard.push([alias, index]))
  indexing.watch()
  await timer.tick()
  assert.equal(indexing.running(), null)
  assert.equal(cloud.index, null)
  assert.deepEqual(heard, [['cloud', null]])
  assert.deepEqual(invalidated, ['cloud'])
  assert.equal(timer.pending(), false)
})

test('track takes the index a copy answered with, and watches the refresh it queued to its end', async () => {
  const cloud = { alias: 'cloud', index: settled('local') }
  const invalidated = []
  const members = { list: [cloud], invalidate: (a) => invalidated.push(a) }
  const timer = manualTimer()
  const indexing = createIndexing(members, { client: null, fetchStatus: async () => ({ members: { cloud: settled('local', { at: '2026-10-27T10:09:00.000Z' }) } }), ...timer })
  const heard = []
  indexing.onChange((alias, index) => heard.push([alias, index.state]))
  indexing.track('cloud', { ...at('log'), local: true })
  assert.equal(cloud.index.state, 'indexing')
  assert.equal(indexing.running(), 'cloud')
  assert.equal(timer.pending(), true)
  await timer.tick()
  assert.deepEqual(heard, [['cloud', 'indexing'], ['cloud', 'local']])
  assert.deepEqual(invalidated, ['cloud'])
  assert.equal(timer.pending(), false)
})

/** indexChangeHandler with recording collaborators; `primary` is the member the page has open. */
function handler({ primary = 'roblab', storage = false } = {}) {
  const did = []
  const fn = indexChangeHandler({
    primary,
    sizes: { forget: (a) => did.push(['forget', a]) },
    redrawStores: () => did.push(['redraw']),
    presence: { invalidate: () => did.push(['presence']) },
    storageShowing: () => storage,
    render: () => did.push(['render']),
    reload: () => did.push(['reload']),
  })
  return { fn, did }
}

test('a progress tick redraws the Stores list and forgets nothing', () => {
  const { fn, did } = handler({ storage: true })
  fn('cloud', at('reading', 2, 9), at('reading', 1, 9))
  assert.deepEqual(did, [['redraw']])
})

test('a job starting forgets the member size; its end also refreshes presence and the Storage page', () => {
  const { fn, did } = handler({ storage: true })
  fn('cloud', at('listing'), settled('none', { at: null, local: false }))
  assert.deepEqual(did, [['forget', 'cloud'], ['redraw']])
  did.length = 0
  fn('cloud', settled('local'), at('snapshot'))
  assert.deepEqual(did, [['forget', 'cloud'], ['redraw'], ['presence'], ['render']])
})

test('the end of a job on the member the page has open reloads the page, which opens it again', () => {
  const { fn, did } = handler({ primary: 'cloud', storage: true })
  fn('cloud', at('reading', 2, 9), at('reading', 1, 9))
  fn('cloud', { ...at('log'), local: true }, settled('local'))
  assert.ok(!did.some(([what]) => what === 'reload'), 'a running job does not reload')
  fn('cloud', settled('snapshot', { local: false }), { ...at('push'), local: true })
  assert.deepEqual(did.at(-1), ['reload'])
  assert.ok(!did.some(([what]) => what === 'render'))
})
