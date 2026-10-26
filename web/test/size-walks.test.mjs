// The size walks and the block-only item read (P0 final fix wave, findings 2
// and 3): no snapshot query per item or per collection, one limit on block
// fetches shared by every size method, work for a page that is gone dropped,
// and any BlockError a partial total rather than a thrown "?".
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'
import { openSnapshots } from './storage-fixture.mjs'

/** An Explorer over a fixture member whose snapshot queries are recorded in `queries`. */
async function open({ member: given = null, wrap = null, now = Date.now(), log = null, options = {}, lean = false } = {}) {
  const member = given ?? await buildMember({ now, lean })
  const fetchFn = wrap ? wrap(blockFetch(member.blocks)) : blockFetch(member.blocks)
  const queries = []
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => {
      const db = await snapshotDb(member.snapshot)
      return { ...db, query: (sql, params) => { queries.push(sql); return db.query(sql, params) } }
    },
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    listFn: async () => ({ names: log ?? member.log, readable: true }),
    now: () => now,
    ...options,
  })
  queries.length = 0
  return { explorer, member, fetchFn, queries }
}

const byText = (a, b) => (a.toString() < b.toString() ? -1 : 1)
const put = (blocks, value) => { const b = block(value); blocks.set(b.cid.toString(), b.bytes); return b.cid }
const leaf = (name, text, size = 1) => ({ kind: 'Leaf', name, address: rawCid(text), size, provider: 'head-node', reason: null })
const selectionOf = (members) => ({ kind: 'Selection', schema: 1, asserted_by: 'test', derived_from: [],
  members: members.sort((a, b) => byText(a.item?.address ?? a.selection, b.item?.address ?? b.selection)) })

/** `n` items of one byte each in one collection, and a Selection of them all, put beside the fixture's blocks. */
function many(member, n) {
  const items = Array.from({ length: n }, (_, i) => put(member.blocks, { kind: 'OutputItem', schema: 1, value: [{ sample: `s${i}` }, leaf(`s${i}.bam`, `s${i}`)] }))
    .sort(byText)
  const m = CID.parse(member.runs.R1.manifest)
  const coll = put(member.blocks, { kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: m, name: 'many', items, paths: items.map((_, i) => [`many/${i}`]) })
  const sel = put(member.blocks, selectionOf(items.map(i => ({ item: { address: i, via: [coll] } }))))
  return { coll: coll.toString(), sel: sel.toString(), items: items.map(String) }
}

/** A fetch that holds each block for `ms` and records how many OutputItem fetches of `items` ran at once. */
function slowItems(items, ms = 3) {
  const watch = new Set(items)
  const seen = { now: 0, most: 0, asked: 0 }
  const wrap = (inner) => async (url, init) => {
    const cid = String(url).split('/').pop()
    if (!watch.has(cid)) return inner(url, init)
    seen.asked++
    seen.now++
    seen.most = Math.max(seen.most, seen.now)
    await new Promise(r => setTimeout(r, ms))
    try { return await inner(url, init) } finally { seen.now-- }
  }
  return { wrap, seen }
}

test('itemBlock reads the item from its block alone; item adds the claim state', async () => {
  const { explorer, member, queries } = await open()
  const it = await explorer.itemBlock(member.runs.R1.collection, member.item.A)
  assert.deepEqual(queries, [], 'no snapshot query')
  assert.equal(it.cid, member.item.A)
  assert.equal(it.leaves[0].name, 'A.bam')
  assert.ok(it.view)
  assert.equal(it.state, undefined)
  const full = await explorer.item(member.runs.R1.collection, member.item.A)
  assert.ok(full.state)
  assert.ok(queries.length > 0, 'the claim state is a snapshot read')
})

test('on a lean snapshot a collection is sized from its OutputCollection block, with no snapshot query', async () => {
  const now = Date.now()
  const member = await buildMember({ now, lean: true })
  const { explorer, queries } = await openSnapshots({ 'index/v6.sqlite': member.snapshot }, { blocks: member.blocks, log: member.log, now })
  const size = await explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual([size.bytes, size.blocks, size.items, size.partial], [20, 2, 2, false])
  assert.equal((await explorer.runSize(member.runs.R1.completion)).bytes, 20)
  assert.deepEqual(queries, [])
})

test('one limit on item fetches holds across every size method at once', async () => {
  const member = await buildMember()
  const ids = many(member, 12)
  const { wrap, seen } = slowItems(ids.items)
  const { explorer } = await open({ member, wrap, options: { sizeConcurrency: 3 } })
  const [c, s, i] = await Promise.all([explorer.collectionSize(ids.coll), explorer.selectionSize(ids.sel), explorer.itemSize(ids.items[11])])
  assert.equal(c.bytes, 12)
  assert.equal(s.bytes, 12)
  assert.equal(i.bytes, 1)
  assert.equal(seen.asked, 12, 'each item block once')
  assert.ok(seen.most <= 3, `at most 3 at once, saw ${seen.most}`)
  assert.ok(seen.most > 1, 'still in parallel')
})

test('a new page drops the size work the last one queued; asking again starts afresh', async () => {
  const member = await buildMember()
  const ids = many(member, 12)
  const { wrap, seen } = slowItems(ids.items, 5)
  const { explorer } = await open({ member, wrap, options: { sizeConcurrency: 2 } })
  const first = explorer.collectionSize(ids.coll)
  await new Promise(r => setTimeout(r, 1))
  explorer.pageChanged()
  await assert.rejects(first, (e) => e.code === 'aborted')
  await new Promise(r => setTimeout(r, 20))
  assert.ok(seen.asked <= 4, `the queue stopped (${seen.asked} of 12 fetched)`)
  const again = await explorer.collectionSize(ids.coll)
  assert.deepEqual([again.bytes, again.items, again.partial], [12, 12, false])
})

test('on a lean snapshot, pipelineSize sums the newest runs only and says "at least" when there are more', async () => {
  const { explorer, queries } = await open({ options: { pipelineSizeRuns: 2 }, lean: true })
  const size = await explorer.pipelineSize('demo')
  // R3 (C) and R2 (B, C): R1's A is left out.
  assert.deepEqual([size.bytes, size.blocks, size.partial, size.runs, size.more], [20, 2, true, 2, true])
  assert.equal(queries.length, 1, 'one page of runs')
  const { explorer: all } = await open({ lean: true })
  const whole = await all.pipelineSize('demo')
  assert.deepEqual([whole.bytes, whole.partial, whole.more], [30, false, false])
})

test('a refused item, a missing collection, a missing completion and a missing nested Selection each give a partial total', async () => {
  const member = await buildMember()
  // Item A's bytes no longer hash to its address: hash_mismatch.
  const a = member.blocks.get(member.item.A)
  member.blocks.set(member.item.A, Uint8Array.from(a, (x, i) => (i === a.length - 1 ? x ^ 1 : x)))
  member.blocks.delete(member.runs.R2.collection)
  const nested = put(member.blocks, selectionOf([{ item: { address: CID.parse(member.item.B), via: [] } }, { selection: block({ kind: 'Selection', gone: true }).cid }]))
  member.blocks.delete(member.runs.R3.completion)
  const { explorer } = await open({ member })
  const col = await explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual([col.bytes, col.partial], [10, true], 'B counted, A refused')
  const r2 = await explorer.runSize(member.runs.R2.completion)
  assert.deepEqual([r2.bytes, r2.partial], [0, true])
  const r3 = await explorer.runSize(member.runs.R3.completion)
  assert.deepEqual([r3.bytes, r3.partial], [0, true])
  const sel = await explorer.selectionSize(nested.toString())
  assert.deepEqual([sel.bytes, sel.files, sel.partial], [10, 1, true])
})

test('a refresh that finds new runs drops the cached pipeline and run sizes', async () => {
  const now = Date.now()
  const member = await buildMember({ now, lean: true })
  const { explorer } = await open({ member, now, log: member.log.filter(n => !n.includes(member.runs.R3.completion)) })
  assert.equal((await explorer.pipelineSize('demo')).bytes, 30)
  const before = explorer.pipelineSize('demo')
  await explorer.refreshTail(async () => ({ names: member.log, readable: true }))
  assert.notEqual(explorer.pipelineSize('demo'), before, 'asked again after the refresh')
  const same = explorer.pipelineSize('demo')
  await explorer.refreshTail(async () => ({ names: member.log, readable: true }))
  assert.equal(explorer.pipelineSize('demo'), same, 'no new run, the cache stays')
})

test('a re-render mid-walk: the gone page\'s workers stop, and the new page gets the whole size', async () => {
  const member = await buildMember()
  const ids = many(member, 40)
  const { wrap, seen } = slowItems(ids.items, 5)
  const { explorer } = await open({ member, wrap, options: { sizeConcurrency: 4 } })
  const first = explorer.collectionSize(ids.coll)
  first.catch(() => {})
  await new Promise(r => setTimeout(r, 12))
  explorer.pageChanged()
  const again = await explorer.collectionSize(ids.coll).then(v => v, e => e)
  assert.ok(!(again instanceof Error), `the new page's size rejected: ${again?.code} ${again?.message}`)
  assert.deepEqual([again.items, again.bytes, again.partial], [40, 40, false])
  assert.equal(seen.asked, 40, 'each item block fetched once across both pages')
  await assert.rejects(first, (e) => e.code === 'aborted')
})
