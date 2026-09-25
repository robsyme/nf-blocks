import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { installReadOnlyVfs } from '../src/vfs/httpvfs.js'
import { MemorySource } from '../src/vfs/sources.js'
import { readFileSync } from 'node:fs'
import { loadSqlite, makeDb } from './helpers.mjs'
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
    listFn: async () => ({ names: overrides.log ?? member.log, readable: overrides.readable ?? true }),
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

/** A snapshot with 120 runs of `big` and one collection of 1200 items, and no tail. */
async function openBig() {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 120)
     INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at)
     SELECT printf('run%03d', i), 'big', printf('R%03d', i), 'succeeded', 0, printf('2026-09-01T%02d:%02d:00.000Z', i / 60, i % 60) FROM n`,
    "INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('coll', 'output', 'run001', 'aligned')",
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1200)
     INSERT INTO collection_item(collection_cid, item_cid) SELECT 'coll', printf('item%04d', i) FROM n`])
  return Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: async () => new Response('', { status: 404 }) }),
    listFn: async () => ({ names: [], readable: true }) })
}

test('a pipeline\'s runs come a page at a time, with where the page is in the whole (final review finding 2)', async () => {
  const explorer = await openBig()
  const first = await explorer.runPage('big')
  assert.equal(first.rows.length, 50)
  assert.equal(first.rows[0].completion_cid, 'run120')
  assert.deepEqual([first.first, first.last, first.total, first.next, first.prev], [1, 50, 120, 50, null])
  const last = await explorer.runPage('big', { offset: 100 })
  assert.deepEqual(last.rows.map(r => r.completion_cid), Array.from({ length: 20 }, (_, i) => `run${String(20 - i).padStart(3, '0')}`))
  assert.deepEqual([last.first, last.last, last.total, last.next, last.prev], [101, 120, 120, null, 50])
})

test('the tail\'s runs are on the first page and counted in its span and the total', async () => {
  const { explorer, member } = await open()
  const page = await explorer.runPage('demo', { limit: 1 })
  assert.deepEqual(page.rows.map(r => r.completion_cid), [member.runs.R3.completion, member.runs.R2.completion, member.runs.R1.completion])
  assert.deepEqual([page.first, page.last, page.total, page.next, page.prev], [1, 3, 3, null, null])
})

test('a collection\'s items come a page at a time, with its item count', async () => {
  const explorer = await openBig()
  const first = await explorer.collection('coll')
  assert.equal(first.items.length, 500)
  assert.deepEqual([first.first, first.last, first.total, first.next, first.prev], [1, 500, 1200, 500, null])
  const last = await explorer.collection('coll', { offset: 1000 })
  assert.equal(last.items[0], 'item1001')
  assert.deepEqual([last.first, last.last, last.total, last.next, last.prev], [1001, 1200, 1200, null, 500])
})

test('a stale collection pages its block\'s items the same way', async () => {
  const { explorer, member } = await open()
  const c = await explorer.collection(member.runs.R2.collection, { limit: 1 })
  assert.equal(c.items.length, 1)
  assert.deepEqual([c.first, c.last, c.total, c.next, c.prev], [1, 1, 2, 1, null])
})

test('a Store Log name whose cid does not parse is an unreadable run, and the page still opens (final review finding 5)', async () => {
  const member = await buildMember()
  const bad = entryName(Date.now() - 1000, 'run', 'b' + 'a'.repeat(58))
  const { explorer } = await open({ log: [...member.log, bad], blocks: new Map([...member.blocks, ['b' + 'a'.repeat(58), new Uint8Array([0xa0])]]) })
  const refused = explorer.stale.find(s => s.cid === 'b' + 'a'.repeat(58))
  assert.equal(refused.error.code, 'schema_invalid')
  assert.equal(explorer.staleCount, 3)
})

test('an unreadable Store Log is recorded, so the page says so instead of "0 runs newer" (final review finding 6)', async () => {
  const { explorer } = await open({ log: [], readable: false })
  assert.equal(explorer.logReadable, false)
  assert.equal(explorer.staleCount, 0)
  const ok = await open()
  assert.equal(ok.explorer.logReadable, true)
})

test('concurrent queries share one closure per stale run, so progress counts each item once (final review finding 7)', async () => {
  const { explorer, member, fetchFn } = await open()
  const seen = []
  await Promise.all([member.content.A, member.content.B, member.content.C].map(c => explorer.producersOf(c, (done, total) => seen.push([done, total]))))
  // R2's closure has two items and R3's one: three steps, however many queries asked.
  assert.deepEqual(seen.map(String).sort(), ['1,1', '1,2', '2,2'])
  assert.equal(fetchFn.asked.filter(c => c === member.item.C).length, 1)
})
