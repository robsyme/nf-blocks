import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { MemorySource } from '../src/vfs/sources.js'
import { loadSqlite } from './helpers.mjs'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'

let vfsCount = 0
async function snapshotDb(bytes) {
  const sqlite3 = await loadSqlite()
  const name = `model-${++vfsCount}`
  installReadOnlyVfs(sqlite3, name).register('snap', new MemorySource(bytes))
  const db = new sqlite3.oo1.DB({ filename: 'file:snap?immutable=1', flags: 'r', vfs: name })
  return { query: async (sql, params = []) => db.selectObjects(sql, params).map(r => ({ ...r })), close: async () => db.close() }
}

async function open(overrides = {}) {
  const now = Date.now()
  const member = await buildMember({ now })
  const blocks = overrides.blocks ?? member.blocks
  const fetchFn = blockFetch(blocks)
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    listFn: async () => overrides.log ?? member.log,
    now: () => now,
  })
  return { explorer, member, fetchFn }
}

test('stale runs are the logged runs past the watermark the snapshot does not hold, each once (Review Focus 2)', async () => {
  const { explorer, member, fetchFn } = await open()
  assert.deepEqual(explorer.stale.map(s => s.cid).sort(), [member.runs.R2.completion, member.runs.R3.completion].sort())
  assert.equal(explorer.staleCount, 2)
  assert.ok(!fetchFn.asked.includes(member.runs.R1.completion), 'R1 is in the snapshot and must not be fetched')
  assert.equal(fetchFn.asked.filter(c => c === member.runs.R2.completion).length, 1)
  assert.equal(explorer.notice, false)
})

test('the run list merges the snapshot and the tail, newest first', async () => {
  const { explorer, member } = await open()
  const rows = await explorer.runsOfPipeline('demo')
  assert.deepEqual(rows.map(r => [r.completion_cid, r.source]), [
    [member.runs.R3.completion, 'tail'], [member.runs.R2.completion, 'tail'], [member.runs.R1.completion, 'snapshot']])
  assert.deepEqual((await explorer.pipelines()).map(p => [p.pipeline, p.runs]), [['demo', 3]])
})

test('query 1 answers from the snapshot and from stale closures', async () => {
  const { explorer, member } = await open()
  const rows = await explorer.producersOf(member.content.B)
  assert.deepEqual(rows.map(r => [r.completion_cid, r.item_cid, r.filename]).sort(), [
    [member.runs.R1.completion, member.item.B, 'B.bam'], [member.runs.R2.completion, member.item.B, 'B.bam']].sort())
})

test('query 2 prefers a newer successful stale run and ignores a failed one', async () => {
  const { explorer, member } = await open()
  assert.equal(await explorer.latestSuccessfulRun('demo'), member.runs.R2.completion)
  assert.equal(await explorer.latestSuccessfulRun('other'), null)
})

test('query 3 over a snapshot run is the SQL; over a stale run it is the closure, typed the same', async () => {
  const { explorer, member } = await open()
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R1.completion, [['sample', 'string', 'B']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['depth', 'float', '1.5']]), [member.item.A])
  assert.deepEqual(await items(member.runs.R2.completion, [['depth', 'float', '3.5']]), [member.item.C])
  assert.deepEqual(await items(member.runs.R2.completion, [['lane', 'int', '2']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R2.completion, []), [member.item.B, member.item.C].sort())
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'x'.repeat(2000)]]), [])
  assert.equal((await explorer.items(member.runs.R1.completion, 'aligned', [])).collection, member.runs.R1.collection)
  assert.equal((await explorer.items(member.runs.R2.completion, 'aligned', [])).collection, member.runs.R2.collection)
})

test('a tampered stale RunCompletion is an error on that run, not a listed run', async () => {
  const member = await buildMember()
  const bad = new Map(member.blocks)
  const bytes = Uint8Array.from(member.blocks.get(member.runs.R2.completion))
  bytes[bytes.length - 3] ^= 1
  bad.set(member.runs.R2.completion, bytes)
  const { explorer } = await open({ blocks: bad })
  const r2 = explorer.stale.find(s => s.cid === member.runs.R2.completion)
  assert.equal(r2.error.code, 'hash_mismatch')
  assert.equal(r2.row, null)
  assert.ok(!(await explorer.runsOfPipeline('demo')).some(r => r.completion_cid === member.runs.R2.completion))
})

test('a missing stale item keeps its collection membership without its attributes or producers, and the closure records it missing', async () => {
  const member = await buildMember()
  const bad = new Map(member.blocks)
  bad.delete(member.item.C)
  const { explorer } = await open({ blocks: bad })
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R2.completion, []), [member.item.B, member.item.C].sort())
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'B']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'C']]), [])
  const rows = await explorer.producersOf(member.content.A)
  assert.deepEqual(rows.map(r => [r.completion_cid, r.item_cid]), [[member.runs.R1.completion, member.item.A]])
  const closure = explorer.closures.get(member.runs.R2.completion)
  assert.deepEqual(closure.missing, [{ cid: member.item.C, code: 'block_missing' }])
})

test('a tampered stale item is likewise recorded missing rather than failing the query', async () => {
  const member = await buildMember()
  const bad = new Map(member.blocks)
  const bytes = Uint8Array.from(member.blocks.get(member.item.C))
  bytes[bytes.length - 3] ^= 1
  bad.set(member.item.C, bytes)
  const { explorer } = await open({ blocks: bad })
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R2.completion, []), [member.item.B, member.item.C].sort())
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'C']]), [])
  const closure = explorer.closures.get(member.runs.R2.completion)
  assert.deepEqual(closure.missing, [{ cid: member.item.C, code: 'hash_mismatch' }])
})

test('past 20 stale runs the notice is on', async () => {
  const member = await buildMember()
  const missing = Array.from({ length: 21 }, (_, i) => entryName(Date.now() - (i + 1) * 1000, 'run', rawCid(`missing-${i}`).toString()))
  const { explorer } = await open({ log: [...member.log, ...missing] })
  assert.equal(explorer.staleCount, 23)
  assert.ok(explorer.stale.filter(s => s.error?.code === 'block_missing').length === 21)
  assert.equal(explorer.notice, true)
})
