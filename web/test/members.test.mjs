import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createMembers } from '../src/members.js'
import { Explorer } from '../src/model.js'
import { resolveStore } from '../src/store.js'
import { memberOpener } from '../src/members.js'
import { BlockFetcher } from '../src/blocks.js'
import { listLog } from '../src/storelog.js'
import { createSizes } from '../src/views/switcher.js'
import { buildMember, blockFetch } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'

const listing = {
  write: true,
  members: [
    { alias: 'lab', writable: true, base: 'm/lab/' }, { alias: 'org', writable: false, base: 'm/org/' },
    { alias: 'ana', writable: false, base: 'm/ana/' }, { alias: 'fourth', writable: false, base: 'm/fourth/' },
  ],
}
const serve = async () => new Response(JSON.stringify(listing), { headers: { 'Content-Type': 'application/json' } })

async function world(href = 'http://h/?member=org') {
  const store = await resolveStore(href, { fetchFn: serve })
  const opened = []
  const members = createMembers(store, { openExplorer: async (m) => { opened.push(m.alias); return { alias: m.alias } } })
  return { store, members, opened }
}

test('list keeps members.json order and gives each a colour key; primary is the page\'s member', async () => {
  const { members } = await world()
  assert.equal(members.primary, 'org')
  assert.deepEqual(members.list.map(m => [m.alias, m.key]), [['lab', 'a'], ['org', 'b'], ['ana', 'c'], ['fourth', 'a']])
})

test('nothing is opened until asked; then once, cached', async () => {
  const { members, opened } = await world()
  assert.deepEqual(opened, [])
  const first = members.explorer('ana')
  assert.equal(members.explorer('ana'), first)
  assert.deepEqual(await first, { alias: 'ana' })
  assert.deepEqual(opened, ['ana'])
  assert.equal(members.status('ana').state, 'open')
})

test('status is opening while the open is pending', async () => {
  const store = await resolveStore('http://h/', { fetchFn: serve })
  let release
  const members = createMembers(store, { openExplorer: () => new Promise(r => { release = r }) })
  members.explorer('org')
  assert.equal(members.status('org').state, 'opening')
  release({})
  await members.explorer('org')
  assert.equal(members.status('org').state, 'open')
})

test('a member whose snapshot 404s is no_snapshot, and asking does not break the others', async () => {
  const { store } = await world()
  const members = createMembers(store, {
    openExplorer: (m) => (m.alias === 'ana'
      ? Explorer.open({ base: 'http://h/m/ana/', blocks: null, listFn: async () => ({ names: [], readable: true }),
        openDb: async () => { throw Object.assign(new Error('no snapshot'), { code: 'no_snapshot' }) } })
      : Promise.resolve({ alias: m.alias })),
  })
  await assert.rejects(members.explorer('ana'), e => e.code === 'no_snapshot')
  const s = members.status('ana')
  assert.equal(s.state, 'no_snapshot')
  assert.equal(s.error.code, 'no_snapshot')
  assert.deepEqual(await members.explorer('lab'), { alias: 'lab' })
})

test('any other open failure is failed', async () => {
  const { store } = await world()
  const members = createMembers(store, { openExplorer: async () => { throw new Error('boom') } })
  await assert.rejects(members.explorer('lab'), /boom/)
  assert.deepEqual([members.status('lab').state, members.status('lab').error.message], ['failed', 'boom'])
})

test('invalidate drops the cached Explorer, so the next ask opens again', async () => {
  const { members, opened } = await world()
  await members.explorer('lab')
  members.invalidate('lab')
  await members.explorer('lab')
  assert.deepEqual(opened, ['lab', 'lab'])
})

test('an unknown alias throws; a store with no members has an empty list', async () => {
  const { members } = await world()
  assert.throws(() => members.explorer('nope'), /no member named nope/)
  const single = createMembers(await resolveStore('http://h/index.html', { fetchFn: async () => new Response('x', { status: 404 }) }), { openExplorer: async () => ({}) })
  assert.deepEqual([single.primary, single.list], [null, []])
})

test('with one member the page asks for members.json, the snapshot, the log and its own blocks, and for nothing of any other member', async () => {
  const member = await buildMember({ now: Date.now() })
  const asked = []
  const fetchFn = async (url, init) => {
    const u = String(url)
    asked.push(u)
    if (u.endsWith('/members.json')) return new Response(JSON.stringify({ members: [listing.members[0]] }), { headers: { 'Content-Type': 'application/json' } })
    if (u.endsWith('/log/')) return new Response(JSON.stringify({ entries: member.log }), { headers: { 'Content-Type': 'application/json' } })
    return blockFetch(member.blocks)(url, init)
  }
  const openDb = async (url) => { await fetchFn(url); return snapshotDb(member.snapshot) }
  const pageUrl = 'http://h/index.html'
  const store = await resolveStore(pageUrl, { fetchFn })
  const primaryBlocks = new BlockFetcher(store.base, { fetchFn })
  const members = createMembers(store, { openExplorer: memberOpener(store, { pageUrl, primaryBlocks, openDb, fetchFn }) })
  const ex = await members.explorer(store.member)
  assert.equal(ex.base, 'http://h/m/lab/')
  assert.equal(members.status('lab').state, 'open')
  const mine = asked.slice()
  assert.deepEqual(mine.slice(0, 3), ['http://h/members.json', 'http://h/m/lab/index/v6.sqlite', 'http://h/m/lab/log/'])
  // The rest are the tail run's blocks, all under the one member's base.
  assert.ok(mine.slice(3).length > 0 && mine.slice(3).every(u => u.startsWith('http://h/m/lab/blocks/')), mine.join('\n'))

  // The same member opened without members.js (the page before this task) asks for the same things.
  const before = []
  const baseline = async (url, init) => { before.push(String(url)); return fetchFn(url, init) }
  await Explorer.open({ base: store.base, blocks: new BlockFetcher(store.base, { fetchFn: baseline }),
    openDb: async (url) => { await baseline(url); return snapshotDb(member.snapshot) },
    listFn: (b, o) => listLog(b, { ...o, fetchFn: baseline }) })
  assert.deepEqual(mine.slice(1), before.slice(0, mine.length - 1).map(String))
  assert.equal(mine.length - 1, before.length)

  // The Stores section and the switcher's size read ask for this member's figures through the same Explorer: no further fetch.
  const beforeSizes = asked.length
  const sizes = createSizes(members)
  await sizes.loadAll()
  assert.equal(asked.length, beforeSizes, 'sizes for the one member add no request')
})
