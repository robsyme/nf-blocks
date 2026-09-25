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

test('a page opened from file:// is refused with file_protocol, before any fetch (final review finding 4)', async () => {
  let asked = 0
  const counting = async () => { asked++; return nothing() }
  await assert.rejects(resolveStore('file:///Users/me/store/index.html#/', { fetchFn: counting }),
    e => e.code === 'file_protocol' && /static file server/.test(e.message) && /nf-blocks:explore/.test(e.message))
  assert.equal(asked, 0)
  await assert.rejects(resolveStore('http://h/index.html?store=file:///Users/me/store/', { fetchFn: counting }), e => e.code === 'file_protocol')
})

test('a file:// page may still name an http store', async () => {
  const s = await resolveStore('file:///Users/me/index.html?store=https://b.s3.amazonaws.com/x/', { fetchFn: nothing })
  assert.equal(s.base, 'https://b.s3.amazonaws.com/x/')
})
