import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagCbor from '@ipld/dag-cbor'
import { BlockFetcher } from '../src/blocks.js'
import { block, blockFetch, buildMember, rawCid } from './fixture.mjs'

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
