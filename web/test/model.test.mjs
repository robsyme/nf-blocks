import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { readFileSync } from 'node:fs'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName, memberWithIndexedCollection, memberWithUnjoinedRun, rangedFetch, rawCid } from './fixture.mjs'
import { Float } from '../src/typed.js'
import { attrRows } from '../src/metadata.js'
import { claimState } from '../src/claims.js'

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
  const closure = await explorer.closing.get(member.runs.R2.completion)
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
  const closure = await explorer.closing.get(member.runs.R2.completion)
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

// A real, fetchable OutputCollection block for the big snapshot's one collection
// (Task 4: Explorer.collection now reads the block even for a snapshot row, to
// answer `index`), addressed at its own hash rather than a placeholder string.
const bigColl = block({ kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: rawCid('big-run-manifest'),
  name: 'aligned', items: [], paths: [] })
const bigCollCid = bigColl.cid.toString()

/** A snapshot with 120 runs of `big` and one collection of 1200 items, and no tail. */

async function openBig() {
  const sqlite3 = await loadSqlite()
  const bytes = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'),
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 120)
     INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at)
     SELECT printf('run%03d', i), 'big', printf('R%03d', i), 'succeeded', 0, printf('2026-09-01T%02d:%02d:00.000Z', i / 60, i % 60) FROM n`,
    `INSERT INTO collection(collection_cid, kind, completion_cid, output_name) VALUES ('${bigCollCid}', 'output', 'run001', 'aligned')`,
    `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1200)
     INSERT INTO collection_item(collection_cid, item_cid) SELECT '${bigCollCid}', printf('item%04d', i) FROM n`])
  return Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(new Map([[bigCollCid, bigColl.bytes]])) }),
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
  const first = await explorer.collection(bigCollCid)
  assert.equal(first.items.length, 500)
  assert.deepEqual([first.first, first.last, first.total, first.next, first.prev], [1, 500, 1200, 500, null])
  const last = await explorer.collection(bigCollCid, { offset: 1000 })
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

test('a run row carries its nf_run_hash, from the snapshot and from a tail run\'s RunManifest (ticket 07 Q4)', async () => {
  const { explorer, member } = await open()
  assert.equal((await explorer.runRow(member.runs.R1.completion)).nf_run_hash, 'hash-R1')
  assert.equal((await explorer.runRow(member.runs.R2.completion)).nf_run_hash, 'hash-R2')
})

test('runLabel names a collection by its run and output, from the snapshot or the tail, and null when neither knows it (decision 11)', async () => {
  const { explorer, member } = await open()
  assert.deepEqual(await explorer.runLabel(member.runs.R1.collection),
    { run_name: 'R1', output: 'aligned', completion: member.runs.R1.completion, status: 'succeeded', possibly_incomplete: false, finished_at: (await explorer.runRow(member.runs.R1.completion)).finished_at })
  assert.deepEqual(await explorer.runLabel(member.runs.R2.collection),
    { run_name: 'R2', output: 'aligned', completion: member.runs.R2.completion, status: 'succeeded', possibly_incomplete: false, finished_at: (await explorer.runRow(member.runs.R2.completion)).finished_at })
  assert.equal(await explorer.runLabel(rawCid('held elsewhere').toString()), null)
  assert.equal(await explorer.runLabel(null), null)
  assert.equal(explorer.runLabel(member.runs.R1.collection), explorer.runLabel(member.runs.R1.collection), 'one lookup per collection')
})

test('allItems lists every item of a collection past the page size without fetching an OutputItem (decision 12)', async () => {
  const explorer = await openBig()
  const all = await explorer.allItems(bigCollCid)
  assert.equal(all.length, 1200)
  assert.deepEqual([all[0], all[1199]], ['item0001', 'item1200'])
  assert.equal(explorer.blocks.fetches, 0)
  const { explorer: tail, member } = await open()
  assert.deepEqual(await tail.allItems(member.runs.R2.collection), [member.item.B, member.item.C].sort())
})

test('collectionItemCount counts a snapshot collection without a block read, a tail collection from its block, and is null for one neither has', async () => {
  const big = await openBig()
  assert.equal(await big.collectionItemCount(bigCollCid), 1200)
  assert.equal(big.blocks.fetches, 0, 'a snapshot collection is counted by the SQL alone')
  const { explorer, member } = await open()
  assert.equal(await explorer.collectionItemCount(member.runs.R1.collection), 2)
  assert.equal(await explorer.collectionItemCount(member.runs.R2.collection), 2, 'a tail collection: the SQL would say 0, the block says 2')
  assert.equal(await explorer.collectionItemCount(rawCid('held elsewhere').toString()), null)
})

test('an item\'s view keeps its floats floats, so its pairs are typed as the index types them', async () => {
  const { explorer, member } = await open()
  const it = await explorer.item(member.runs.R1.collection, member.item.A)
  assert.ok(it.view.depth instanceof Float)
  assert.deepEqual(attrRows(it.view).map(r => [r.path, r.type, r.value]).sort(),
    [['depth', 'float', '1.5'], ['lane', 'int', '1'], ['sample', 'string', 'A']])
  assert.equal(it.leaves[0].name, 'A.bam')
})

// Task 11 (ticket 21 answer 6): run, item and collection each carry the
// subject's own claim state, for the explorer's release/restore/pin controls.
// No Claim is written in this fixture, so an unclaimed subject's state is the
// same claimState([]) every other subject with no Claims gets.
test('run, item and collection each carry the subject\'s own claim state, unclaimed here', async () => {
  const { explorer, member } = await open()
  const unclaimed = { ...claimState([]), claims: [] }
  const run = await explorer.run(member.runs.R1.completion)
  assert.deepEqual(run.state, unclaimed)
  const it = await explorer.item(member.runs.R1.collection, member.item.A)
  assert.deepEqual(it.state, unclaimed)
  const coll = await explorer.collection(member.runs.R1.collection)
  assert.deepEqual(coll.state, unclaimed)
  // A stale (tail-only) collection's block is read from its own branch, and still carries a state.
  const stale = await explorer.collection(member.runs.R2.collection)
  assert.deepEqual(stale.state, unclaimed)
})

test('concurrent queries share one closure per stale run, so progress counts each item once (final review finding 7)', async () => {
  const { explorer, member, fetchFn } = await open()
  const seen = []
  await Promise.all([member.content.A, member.content.B, member.content.C].map(c => explorer.producersOf(c, (done, total) => seen.push([done, total]))))
  // R2's closure has two items and R3's one: three steps, however many queries asked.
  assert.deepEqual(seen.map(String).sort(), ['1,1', '1,2', '2,2'])
  assert.equal(fetchFn.asked.filter(c => c === member.item.C).length, 1)
})

test('runIdentity names a run from its blocks alone, once per run, and asks again after a failure (P2)', async () => {
  const { ex, ids } = await memberWithUnjoinedRun(0)
  const queries = []
  const query = ex.db.query.bind(ex.db)
  ex.db.query = (sql, params) => { queries.push(sql); return query(sql, params) }
  const id = await ex.runIdentity(ids.completion)
  assert.deepEqual({ ...id, config: undefined, manifest: undefined }, {
    completion: ids.completion, manifest: undefined, pipeline: 'demo', run_name: 'R4', nf_run_hash: 'hash-R4', lid: 'lid://hash-R4',
    revision: null, config: undefined, status: 'succeeded', possibly_incomplete: false, finished_at: '2026-09-04T10:00:00.000Z' })
  assert.match(id.manifest, /^bafy/)
  assert.deepEqual(queries, [], 'no snapshot query')
  assert.equal(await ex.runIdentity(ids.completion), id)
  await assert.rejects(ex.runIdentity('bafyreinotarealblockxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'))
  assert.equal(ex.identities.size, 1)
})

test('stableIndex: the Output Index File with each coordinate replaced by the address fromStore emits', async () => {
  const indexText = 'sample,bam\nA,cas://lab/aligned/A.bam\nB,cas://lab/aligned/B.bam\nZ,cas://lab/aligned/Z.bam\n'
  const { ex, ids } = await memberWithIndexedCollection({ indexName: 'index.csv', indexPath: 'index.csv', indexText })
  assert.deepEqual(await ex.stableIndex(ids.collection), {
    name: 'index.csv',
    text: `sample,bam\nA,cas://${ids.content.A}/A.bam\nB,cas://${ids.content.B}/B.bam\nZ,cas://lab/aligned/Z.bam\n`,
    replaced: 2, unmatched: 1,
  })
})

async function openRanged(extraBlocks = []) {
  const now = Date.now()
  const member = await buildMember({ now })
  for (const [cid, bytes] of extraBlocks) member.blocks.set(cid, bytes)
  const blocks = new BlockFetcher('http://h/m/lab/', { fetchFn: rangedFetch(member.blocks) })
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot), blocks,
    listFn: async () => ({ names: member.log, readable: true }), now: () => now })
  return { ex, member, blocks }
}

test('block: a structured block, verified, with its kind and friendly link', async () => {
  const { ex, member } = await openRanged()
  const b = await ex.block(member.runs.R1.completion)
  assert.equal(b.check, 'verified')
  assert.equal(b.parts.codec, 'dag-cbor')
  assert.equal(b.value.kind, 'RunCompletion')
  assert.equal(b.size, b.bytes.length)
  assert.equal(b.friendly.href, `#/run/${member.runs.R1.completion}`)
})

test('block: a small raw block is read whole and verified; a large one is previewed and unchecked', async () => {
  const small = new TextEncoder().encode('Tarik Benali from Casablanca, Africa\n')
  const large = new TextEncoder().encode('y'.repeat(6000))
  const smallCid = rawCid('Tarik Benali from Casablanca, Africa\n').toString()
  const largeCid = rawCid('y'.repeat(6000)).toString()
  const { ex, blocks } = await openRanged([[smallCid, small], [largeCid, large]])
  const s = await ex.block(smallCid)
  assert.equal(s.check, 'verified')
  assert.equal(new TextDecoder().decode(s.bytes), 'Tarik Benali from Casablanca, Africa\n')
  assert.equal(s.friendly.href, `#/content/${smallCid}`)
  const l = await ex.block(largeCid, { rawCap: 5000 })
  assert.equal(l.check, 'unchecked')
  assert.equal(l.size, 6000)
  assert.equal(l.preview.length, 4096)
  assert.equal(l.bytes, null)
  assert.ok(!blocks.verified.includes(largeCid))
})

test('block: missing, refused, and a block that fails the schema is still shown with its reason', async () => {
  const tampered = rawCid('real').toString()
  const odd = block({ kind: 'OutputItem' })   // no schema, no value: decodes, fails validBlock
  const { ex } = await openRanged([[tampered, new TextEncoder().encode('fake')], [odd.cid.toString(), odd.bytes]])
  assert.equal((await ex.block(rawCid('absent').toString())).check, 'missing')
  const refused = await ex.block(tampered)
  assert.equal(refused.check, 'refused')
  assert.equal(refused.bytes, null)
  const shown = await ex.block(odd.cid.toString())
  assert.equal(shown.check, 'verified')
  assert.equal(shown.value.kind, 'OutputItem')
  assert.match(shown.schemaError, /schema/)
})

test('block: a large raw block on a server without Range is never read whole (Review Focus 1)', async () => {
  const big = new TextEncoder().encode('z'.repeat(6000))
  const cid = rawCid('z'.repeat(6000)).toString()
  const { ex, blocks } = await openRanged([[cid, big]])
  blocks.fetchFn = async () => ({ status: 200, ok: true, headers: new Headers({ 'Content-Length': '6000' }), body: { cancel() {} } })
  const before = blocks.fetches   // opening the Explorer already fetched the tail's blocks
  const b = await ex.block(cid, { rawCap: 5000 })
  assert.equal(b.check, 'unchecked')
  assert.equal(b.preview, null)
  assert.equal(blocks.fetches, before, 'raw() was never called')
})

test('block: a URL that is not a CID is schema_invalid (Review Focus 2)', async () => {
  const { ex } = await openRanged()
  await assert.rejects(ex.block('not-a-cid'), (e) => e.code === 'schema_invalid')
})

test('block: an empty raw block is verified with size 0', async () => {
  const cid = rawCid('').toString()
  const { ex } = await openRanged([[cid, new Uint8Array(0)]])
  const b = await ex.block(cid)
  assert.equal(b.check, 'verified')
  assert.equal(b.size, 0)
})

test('!= over a snapshot run is the SQL, over a stale run the matcher; an item without the path matches', async () => {
  const { explorer, member } = await open()
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R1.completion, [['sample', 'string', 'A', '!=']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['lane', 'int', '1', '!=']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['strandedness', 'string', 'reverse', '!=']]), [member.item.A, member.item.B].sort())
  assert.deepEqual(await items(member.runs.R2.completion, [['sample', 'string', 'B', '!=']]), [member.item.C])
  assert.deepEqual(await items(member.runs.R2.completion, [['strandedness', 'string', 'reverse', '!=']]), [member.item.B, member.item.C].sort())
  await assert.rejects(items(member.runs.R1.completion, [['sample', 'string', 'x'.repeat(2000), '!=']]), (e) => e.constructor.name === 'PredicateError')
})

test('a range over a snapshot run is the SQL, over a stale run the matcher; int and float bounds alike', async () => {
  const { explorer, member } = await open()
  const items = async (run, where) => (await explorer.items(run, 'aligned', where)).items
  assert.deepEqual(await items(member.runs.R1.completion, [['depth', 'float', '2.0', '>=']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['lane', 'int', '1', '>']]), [member.item.B])
  assert.deepEqual(await items(member.runs.R1.completion, [['depth', 'float', '2.5', '<=']]), [member.item.A, member.item.B].sort())
  assert.deepEqual(await items(member.runs.R1.completion, [['sample', 'string', 'A'], ['lane', 'int', '2', '<']]), [member.item.A])
  assert.deepEqual(await items(member.runs.R2.completion, [['depth', 'float', '3.0', '>']]), [member.item.C])
  assert.deepEqual(await items(member.runs.R2.completion, [['lane', 'int', '2', '<='], ['depth', 'float', '2.0', '>']]), [member.item.B])
  await assert.rejects(items(member.runs.R1.completion, [['lane', 'int', 'two', '>=']]), (e) => e.constructor.name === 'PredicateError')
})

test('snapshotWrittenAt is meta.snapshot_written_at, and null when the snapshot has no such key', async () => {
  const { explorer } = await open()
  assert.equal(explorer.snapshotWrittenAt, '2026-09-01T10:00:01.000Z')
  const sqlite3 = await loadSqlite()
  const bare = makeDb(sqlite3, [readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8')])
  const without = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(bare), blocks: null,
    listFn: async () => ({ names: [], readable: true }), now: () => Date.now() })
  assert.equal(without.snapshotWrittenAt, null)
})

test('recentActivity merges a stale snapshot\'s rows with the tail\'s, newest first, ties by cid and each entry once', async () => {
  const now = Date.now()
  const iso = (millis) => new Date(millis).toISOString()
  const member = await buildMember({ now, extra: ({ runs, at }) => ({ log: [], rows: (db) => {
    db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'run', NULL, ?)", bind: [runs.R1.completion, iso(at.R1)] })
    // R2 is logged in the snapshot too, though its run rows are not: it is a tail run, listed once.
    db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'run', NULL, ?)", bind: [runs.R2.completion, iso(at.R1 + 1)] })
    // A claim written in the same millisecond as R3's log entry.
    db.exec({ sql: "INSERT INTO log_entry VALUES ('k-tie', 'claim', NULL, ?)", bind: [iso(at.R3)] })
  } }) })
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => now,
  })
  const rows = await explorer.recentActivity(8)
  const r = member.runs
  assert.deepEqual(rows.map(e => [e.kind, e.cid]), [
    ['selection', r.R3.completion], ['run', r.R3.completion], ['claim', 'k-tie'], ['run', r.R2.completion], ['run', r.R1.completion]])
  assert.equal(rows[1].written_at, rows[2].written_at, 'a tie')
  assert.equal(rows.filter(e => e.cid === r.R2.completion).length, 1)
  assert.equal(rows[3].name, 'R2', 'the tail\'s row, which knows the run')
  assert.deepEqual((await explorer.recentActivity(8)).map(e => e.cid), rows.map(e => e.cid), 'the same order every time')
})
