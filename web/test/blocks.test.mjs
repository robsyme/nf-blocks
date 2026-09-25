import { test } from 'node:test'
import assert from 'node:assert/strict'
import { BlockFetcher } from '../src/blocks.js'
import { blockFetch, buildMember } from './fixture.mjs'

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
