import { test } from 'node:test'
import assert from 'node:assert/strict'
import { explainFailure, resolveStore } from '../src/store.js'

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

test('?member=* shows every member, and the primary is the writable one, else the first', async () => {
  const s = await resolveStore('http://127.0.0.1:9/?member=*', { fetchFn: async () => new Response(JSON.stringify({ members: [
    { alias: 'ro', writable: false, base: 'm/ro/' }, { alias: 'rw', writable: true, base: 'm/rw/' }] }), { headers: { 'Content-Type': 'application/json' } }) })
  assert.deepEqual([s.all, s.member, s.base], [true, 'rw', 'http://127.0.0.1:9/m/rw/'])
  const none = await resolveStore('http://127.0.0.1:9/?member=*', { fetchFn: explore })
  assert.deepEqual([none.all, none.member], [true, 'lab'])
  assert.equal((await resolveStore('http://127.0.0.1:9/?member=priv', { fetchFn: explore })).all, false)
})

test('new members.json fields are kept; an older server\'s reads as null/false', async () => {
  const body = { copy: true, members: [
    { alias: 'lab', writable: true, base: 'm/lab/', kind: 'local', location: null, copy_target: true, copy_reason: null, future: 1 },
    { alias: 'org', writable: false, base: 'm/org/', kind: 's3', location: 's3://lab-blocks', copy_target: false, copy_reason: 'read-only in this config' }] }
  const s = await resolveStore('http://h/', { fetchFn: async () => new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } }) })
  assert.equal(s.copy, true)
  assert.deepEqual(s.members[1], { alias: 'org', writable: false, base: 'm/org/', kind: 's3', location: 's3://lab-blocks', copy_target: false, copy_reason: 'read-only in this config' })
  assert.equal(s.members[0].kind, 'local')
  const old = await resolveStore('http://h/', { fetchFn: explore })
  assert.equal(old.copy, false)
  assert.deepEqual([old.members[0].kind, old.members[0].location, old.members[0].copy_target, old.members[0].copy_reason], [null, null, false, null])
  assert.equal((await resolveStore('http://h/x.html', { fetchFn: nothing })).copy, false)
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

test('a failure while the page server is down is explained as server_gone, naming explore and the origin', async () => {
  const down = async () => { throw new TypeError('Load failed') }
  const failure = new Error('could not fetch http://127.0.0.1:54905/index/v7.sqlite: Load failed')
  const e = await explainFailure(failure, 'http://127.0.0.1:54905/?member=lab&token=t#/run/x', { fetchFn: down })
  assert.equal(e.code, 'server_gone')
  assert.match(e.message, /http:\/\/127\.0\.0\.1:54905 is not answering/)
  assert.match(e.message, /nextflow plugin nf-blocks:explore/)
  assert.doesNotMatch(e.message, /token/)
})

test('a failure while the page server answers is left as it was, whatever the answer', async () => {
  const failure = new Error('could not fetch block x: Load failed')
  assert.equal(await explainFailure(failure, 'http://127.0.0.1:9/#/', { fetchFn: explore }), failure)
  assert.equal(await explainFailure(failure, 'http://127.0.0.1:9/#/', { fetchFn: nothing }), failure)
})

test('a file:// page never asks', async () => {
  let asked = 0
  const counting = async () => { asked++; throw new TypeError('Failed to fetch') }
  const local = Object.assign(new Error('x'), { code: 'fetch_failed' })
  assert.equal(await explainFailure(local, 'file:///Users/me/index.html', { fetchFn: counting }), local)
  assert.equal(asked, 0)
})
