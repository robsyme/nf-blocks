import { test } from 'node:test'
import assert from 'node:assert/strict'
import { resolveStore } from '../src/store.js'

const members = { members: [{ alias: 'lab', writable: true, base: 'm/lab/' }, { alias: 'priv', writable: false, base: 'm/priv/' }] }
const explore = async () => new Response(JSON.stringify(members), { headers: { 'Content-Type': 'application/json' } })
const nothing = async () => new Response('no', { status: 404 })

test('?store= wins, with a trailing slash added', async () => {
  const s = await resolveStore('http://127.0.0.1:9/stores/current/index.html?store=https://b.s3.amazonaws.com/x#/', { fetchFn: explore })
  assert.equal(s.base, 'https://b.s3.amazonaws.com/x/')
})

test('under explore, the first member or ?member=', async () => {
  assert.equal((await resolveStore('http://127.0.0.1:9/', { fetchFn: explore })).base, 'http://127.0.0.1:9/m/lab/')
  const s = await resolveStore('http://127.0.0.1:9/?member=priv', { fetchFn: explore })
  assert.deepEqual([s.base, s.member, s.members.length], ['http://127.0.0.1:9/m/priv/', 'priv', 2])
})

test('otherwise the page directory', async () => {
  assert.equal((await resolveStore('http://h/stores/current/index.html#/run/x', { fetchFn: nothing })).base, 'http://h/stores/current/')
})
