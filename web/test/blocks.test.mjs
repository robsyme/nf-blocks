import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagCbor from '@ipld/dag-cbor'
import { BlockFetcher, validKind } from '../src/blocks.js'
import { validBlock } from '../src/schema.js'
import { block, blockFetch, buildMember, rangedFetch, rawCid } from './fixture.mjs'

const served = (...values) => {
  const blocks = new Map(values.map(v => { const b = block(v); return [b.cid.toString(), b.bytes] }))
  return { cids: [...blocks.keys()], fetcher: new BlockFetcher('http://h/', { fetchFn: blockFetch(blocks) }) }
}
const leafV2 = { kind: 'Leaf', name: 'A.bam', address: rawCid('a'), size: 1, reason: null }
const leafV1 = { ...leafV2, provider: 'head-node' }

test('a block is fetched once, verified, decoded and recorded', async () => {
  const { blocks, runs } = await buildMember()
  const fetchFn = blockFetch(blocks)
  const fetcher = new BlockFetcher('http://h/m/lab/', { fetchFn })
  const a = await fetcher.ofKind(runs.R1.completion, 'RunCompletion')
  await fetcher.get(runs.R1.completion)
  assert.equal(a.value.status, 'succeeded')
  assert.deepEqual(fetchFn.asked, [runs.R1.completion])
  assert.deepEqual(fetcher.verified, [runs.R1.completion])
})

test('bytes that do not hash to the address are refused before decoding', async () => {
  const { blocks, runs } = await buildMember()
  const bad = new Map(blocks)
  const bytes = Uint8Array.from(blocks.get(runs.R2.completion))
  bytes[bytes.length - 3] ^= 1
  bad.set(runs.R2.completion, bytes)
  const fetcher = new BlockFetcher('http://h/', { fetchFn: blockFetch(bad) })
  await assert.rejects(fetcher.get(runs.R2.completion), e => e.code === 'hash_mismatch' && e.cid === runs.R2.completion)
  assert.deepEqual(fetcher.verified, [])
})

test('a block not in this member is block_missing, and the wrong kind is schema_invalid', async () => {
  const { blocks, runs } = await buildMember()
  const fetcher = new BlockFetcher('http://h/', { fetchFn: blockFetch(blocks) })
  await assert.rejects(fetcher.get('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'), e => e.code === 'block_missing')
  await assert.rejects(fetcher.ofKind(runs.R1.manifest, 'RunCompletion'), e => e.code === 'schema_invalid')
})

test('a name that looks like a CID but does not parse as one is schema_invalid, not a plain Error (final review finding 5)', async () => {
  const bad = 'b' + 'a'.repeat(58)
  // A server that answers anything: the refusal must not depend on a 404.
  const fetcher = new BlockFetcher('http://h/', { fetchFn: async () => new Response(new Uint8Array([0xa0])) })
  await assert.rejects(fetcher.get(bad), e => e.constructor.name === 'BlockError' && e.code === 'schema_invalid' && e.cid === bad)
})

test('an OutputItem is read at schema 2 with bare leaves and at schema 1 with provider leaves, and no other way (ticket 16)', async () => {
  const { cids, fetcher } = served(
    { kind: 'OutputItem', schema: 2, value: [{ sample: 'A' }, leafV2] },
    { kind: 'OutputItem', schema: 1, value: [{ sample: 'A' }, leafV1] },
    { kind: 'OutputItem', schema: 2, value: [{ sample: 'A' }, leafV1] },
    { kind: 'OutputItem', schema: 3, value: [{ sample: 'A' }, leafV2] })
  assert.equal((await fetcher.get(cids[0])).value.schema, 2)
  assert.equal((await fetcher.get(cids[1])).value.schema, 1)
  await assert.rejects(fetcher.get(cids[2]), e => e.code === 'schema_invalid')
  await assert.rejects(fetcher.get(cids[3]), e => e.code === 'schema_invalid')
})

test('a schema-2 RunCompletion carries providers with known names; schema 1 carries none', async () => {
  const { blocks, runs } = await buildMember()
  const v1 = dagCbor.decode(blocks.get(runs.R1.completion))
  const { cids, fetcher } = served(
    { ...v1, schema: 2, providers: { 'head-node': [rawCid('a')], 's3-copy': [] } },
    { ...v1, schema: 2 },
    { ...v1, schema: 2, providers: { laptop: [] } },
    { ...v1, schema: 1, providers: {} })
  assert.equal((await fetcher.get(cids[0])).value.kind, 'RunCompletion')
  for (const cid of cids.slice(1))
    await assert.rejects(fetcher.get(cid), e => e.code === 'schema_invalid')
})

test('a schema-2 manifest has no executable entry; schema 1 may (ticket 15 addendum)', async () => {
  const e = (mode) => ({ name: 't', mode, size: 1, address: rawCid('t'), target: null })
  const { cids, fetcher } = served(
    { kind: 'DirectoryManifest', schema: 2, entries: [e('regular')] },
    { kind: 'DirectoryManifest', schema: 1, entries: [e('executable')] },
    { kind: 'DirectoryManifest', schema: 2, entries: [e('executable')] })
  assert.equal((await fetcher.get(cids[0])).value.schema, 2)
  assert.equal((await fetcher.get(cids[1])).value.schema, 1)
  await assert.rejects(fetcher.get(cids[2]), e => e.code === 'schema_invalid')
})

test('urlFor is the URL load fetches: the member base, not the page (final review I3)', async () => {
  const { blocks, runs } = await buildMember()
  const urls = []
  const inner = blockFetch(blocks)
  const fetcher = new BlockFetcher('http://h/m/lab/', { fetchFn: async (url) => { urls.push(String(url)); return inner(url) } })
  const cid = runs.R1.completion
  assert.equal(fetcher.urlFor(cid), `http://h/m/lab/blocks/${cid.slice(-2)}/${cid}`)
  await fetcher.get(cid)
  assert.deepEqual(urls, [fetcher.urlFor(cid)])
})

test('raw: the bytes of a raw block, hash-checked and not decoded', async () => {
  const text = 'sample,bam\nA,cas://lab/aligned/A.bam\n'
  const cid = rawCid(text).toString()
  const blocks = new Map([[cid, new TextEncoder().encode(text)]])
  const fetcher = new BlockFetcher('http://h/', { fetchFn: blockFetch(blocks) })
  assert.equal(new TextDecoder().decode(await fetcher.raw(cid)), text)
  const lying = new BlockFetcher('http://h/', { fetchFn: blockFetch(new Map([[cid, new TextEncoder().encode('other')]])) })
  await assert.rejects(lying.raw(cid), (e) => e.code === 'hash_mismatch')
  await assert.rejects(new BlockFetcher('http://h/', { fetchFn: blockFetch(new Map()) }).raw(cid), (e) => e.code === 'block_missing')
})

test('partial: the first n bytes and the whole size by Range, never recorded as verified', async () => {
  const text = 'x'.repeat(10000)
  const cid = rawCid(text).toString()
  const fetcher = new BlockFetcher('http://h/', { fetchFn: rangedFetch(new Map([[cid, new TextEncoder().encode(text)]])) })
  const { bytes, size } = await fetcher.partial(cid, 4096)
  assert.equal(bytes.length, 4096)
  assert.equal(size, 10000)
  assert.equal(fetcher.partialFetches, 1)
  assert.deepEqual(fetcher.verified, [])
})

test('partial: a server that ignores Range gives no bytes, only the size it states (Review Focus 1)', async () => {
  const cid = rawCid('whole').toString()
  let cancelled = false
  const fetchFn = async () => ({ status: 200, ok: true, headers: new Headers({ 'Content-Length': '5' }), body: { cancel: () => { cancelled = true } } })
  const { bytes, size } = await new BlockFetcher('http://h/', { fetchFn }).partial(cid, 4096)
  assert.equal(bytes, null)
  assert.equal(size, 5)
  assert.ok(cancelled, 'the whole body is not read')
})

test('partial: a status that is neither a partial nor a whole body is fetch_failed', async () => {
  const cid = rawCid('boom').toString()
  const fetchFn = async () => ({ status: 500, ok: false, headers: new Headers() })
  await assert.rejects(new BlockFetcher('http://h/', { fetchFn }).partial(cid, 4096), (e) => e.code === 'fetch_failed')
})

test('partial: a 416 is a range past the end, so an empty block has no bytes and size 0', async () => {
  const cid = rawCid('').toString()
  const fetchFn = async () => ({ status: 416, ok: false, headers: new Headers({ 'Content-Range': 'bytes */0' }), body: { cancel() {} } })
  const r = await new BlockFetcher('http://h/', { fetchFn }).partial(cid, 4096)
  assert.equal(r.bytes.length, 0)
  assert.equal(r.size, 0)
})

test('partial: the body is cancelled before a failure or a missing block is thrown', async () => {
  const cid = rawCid('boom').toString()
  for (const [status, code] of [[500, 'fetch_failed'], [404, 'block_missing']]) {
    let cancelled = false
    const fetchFn = async () => ({ status, ok: false, headers: new Headers(), body: { cancel: () => { cancelled = true } } })
    await assert.rejects(new BlockFetcher('http://h/', { fetchFn }).partial(cid, 4096), (e) => e.code === code)
    assert.ok(cancelled, `${status} body cancelled`)
  }
})

const item = block({ kind: 'OutputItem', n: 1 }).cid
const coll = block({ kind: 'OutputCollection', n: 1 }).cid
const sel = block({ kind: 'Selection', n: 1 }).cid
const set = block({ kind: 'InputSet', n: 1 }).cid
const input = (extra = {}) => ({ role: 'task', name: 'Dev_Patel.dat', address: rawCid('dev'), size: 12, sample: 'Dev', reason: null, ...extra })
const inputSet = (schema, inputs) => ({ kind: 'InputSet', schema, asserted_by: 'rob', inputs })
const lineage = { kind: 'ItemLineage', schema: 1, asserted_by: 'rob', input_set: set,
  items: [{ item, inputs: [0], tasks: [{ hash: '0123456789abcdef0123456789abcdef', process: 'Introduce', tag: 'Dev' }] }] }

test('an InputSet input may carry from at schema 2; schema 1 may not', () => {
  const withFrom = input({ from: [{ item, collection: coll, selection: sel }, { item, collection: null, selection: null }] })
  assert.equal(validBlock(inputSet(2, [withFrom])) && validKind(inputSet(2, [withFrom])), true)
  assert.equal(validBlock(inputSet(1, [input()])) && validKind(inputSet(1, [input()])), true)
  assert.equal(validKind(inputSet(1, [withFrom])), false)
})

test('an ItemLineage block validates; a TaskRef without process does not', () => {
  assert.equal(validBlock(lineage), true)
  assert.equal(validBlock({ ...lineage, items: [{ item, inputs: [0], tasks: [{ hash: 'x', tag: null }] }] }), false)
})

test('a RunCompletion carries item_lineage exactly at schema 3, null allowed', () => {
  const rc = (schema, extra) => ({ kind: 'RunCompletion', schema, providers: {}, ...extra })
  assert.equal(validKind(rc(3, { item_lineage: null })), true)
  assert.equal(validKind(rc(3, { item_lineage: block(lineage).cid })), true)
  assert.equal(validKind(rc(3, {})), false)
  assert.equal(validKind(rc(2, { item_lineage: null })), false)
  assert.equal(validKind(rc(2, {})), true)
})

test('a full schema 3 RunCompletion passes validBlock', () => {
  const full = { kind: 'RunCompletion', schema: 3, asserted_by: 'test', run: block({ kind: 'RunManifest', n: 1 }).cid,
    collections: [], input_set: null, status: 'succeeded', exit_status: 0, possibly_incomplete: false,
    started_at: '2026-09-01T00:00:00.000Z', finished_at: '2026-09-01T00:01:00.000Z',
    anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null,
    providers: {}, item_lineage: block(lineage).cid }
  assert.equal(validBlock(full), true)
  assert.equal(validKind(full), true)
})

test('a block read the server explains in plain text is told in its words, whole or partial', async () => {
  const cid = rawCid('boom').toString()
  const said = 'AWS refused the credentials for this store: they may have expired, or may not grant access to it. Refresh your AWS credentials, then reload this page'
  const fetchFn = async () => new Response(`${said}\n`, { status: 503, headers: { 'Content-Type': 'text/plain; charset=utf-8' } })
  const fetcher = new BlockFetcher('http://h/', { fetchFn })
  await assert.rejects(fetcher.get(cid), (e) => e.code === 'fetch_failed' && e.message === `block ${cid}: ${said}`)
  await assert.rejects(fetcher.partial(cid, 4096), (e) => e.code === 'fetch_failed' && e.message === `block ${cid}: ${said}`)
})
