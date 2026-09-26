import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { writer, WriteError } from '../src/write.js'
import { block } from './fixture.mjs'

const S = block({ s: 1 }).cid
const N = block({ n: 1 }).cid

function fakeFetch(respond) {
  const calls = []
  const fn = async (url, init) => {
    calls.push({ url, init, body: dagJson.decode(init.body) })
    return respond(url, init)
  }
  fn.calls = calls
  return fn
}

const ok = (value) => new Response(dagJson.encode(value), { status: 200, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })

test('a rename posts the DAG-JSON Claim with the token, superseding what was shown', async () => {
  const fetchFn = fakeFetch(() => ok({ address: N, block: {}, entry: 'e', written: true }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't0k', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  const r = await w.rename(S.toString(), 'tumour, "batch 2"', [N.toString()])
  const [call] = fetchFn.calls
  assert.equal(call.url, 'http://h/api/put')
  assert.equal(call.init.method, 'POST')
  assert.equal(call.init.headers['Content-Type'], 'application/vnd.ipld.dag-json')
  assert.equal(call.init.headers['X-NF-Blocks-Token'], 't0k')
  assert.deepEqual(call.body, { kind: 'Claim', subject: S, verb: 'set', attribute: 'name', value: 'tumour, "batch 2"', supersedes: [N], timestamp: '2026-09-25T10:00:00.000Z' })
  assert.equal(r.address, N.toString())
})

test('delete and undo supersede the deletion Claims shown; a dry run asks with ?dry_run=true', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: false, names: [] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date('2026-09-25T10:00:00.000Z') })
  await w.remove(S.toString(), [])
  await w.undo(S.toString(), [N.toString()])
  await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.equal(fetchFn.calls[0].body.verb, 'delete')
  assert.equal(fetchFn.calls[0].body.attribute, null)
  assert.deepEqual(fetchFn.calls[1].body.supersedes, [N])
  assert.equal(fetchFn.calls[1].body.verb, 'del')
  assert.equal(fetchFn.calls[2].url, 'http://h/api/put?dry_run=true')
  assert.deepEqual(fetchFn.calls[2].body, { kind: 'Selection', members: [{ item: { address: N, via: [] } }], derived_from: [] })
})

test('a dry run decodes name_claims (DAG-JSON links) to CID strings', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: true, here: false, names: ['a', 'b'], name_claims: [N, S] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn })
  const r = await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.deepEqual(r.name_claims, [N.toString(), S.toString()])
})

test('a dry run decodes deletion_claims (DAG-JSON links) to CID strings', async () => {
  const fetchFn = fakeFetch(() => ok({ address: S, exists: true, here: false, names: [], name_claims: [], deletion: 'deleted', deletion_claims: [N] }))
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn })
  const r = await w.selection([{ item: { address: N, via: [] } }], { dryRun: true })
  assert.deepEqual(r.deletion_claims, [N.toString()])
})

test('a refusal becomes a WriteError with its code and where; a plain-text refusal keeps its status', async () => {
  const w = writer({ endpoint: 'http://h/api/put', token: 't', fetchFn: fakeFetch(() => new Response(
    dagJson.encode({ error: 'stale_supersedes', message: 'claim is already superseded', at: '/supersedes/0' }),
    { status: 400, headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })) })
  await assert.rejects(w.rename(S.toString(), 'x', [N.toString()]), (e) => e instanceof WriteError && e.code === 'stale_supersedes' && e.at === '/supersedes/0' && e.status === 400)
  const plain = writer({ endpoint: 'http://h/api/put', token: 'wrong', fetchFn: fakeFetch(() => new Response('refused', { status: 403 })) })
  await assert.rejects(plain.remove(S.toString(), []), (e) => e instanceof WriteError && e.code === 'forbidden' && e.status === 403)
})
