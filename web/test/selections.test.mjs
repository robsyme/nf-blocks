import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName } from './fixture.mjs'

const claim = (subject, verb, attribute, value, supersedes, timestamp = '2026-09-01T10:00:00.000Z') =>
  ({ kind: 'Claim', schema: 1, asserted_by: 'test', subject, verb, attribute, value, supersedes, timestamp })

/**
 * S1 (items A and B of R1) and its name N1 are in the snapshot. The tail holds
 * N2 renaming S1, S2 (nesting S1 and a Selection this member lacks, plus item
 * C chosen by query), a delete of S2, and a delete of run R2.
 */
async function open() {
  const now = Date.now()
  const ids = {}
  const member = await buildMember({
    now,
    extra: ({ put, runs, item, at }) => {
      // The fixture keeps run addresses as strings; inside a block a link must be a CID.
      const r1coll = runs.R1.collection
      ids.S1 = put({ kind: 'Selection', schema: 1, asserted_by: 'test', derived_from: [],
        members: [item.A, item.B].sort((a, b) => (a.toString() < b.toString() ? -1 : 1)).map(i => ({ item: { address: i, via: [CID.parse(r1coll)] } })) })
      ids.N1 = put(claim(ids.S1, 'set', 'name', 'first', []))
      ids.N2 = put(claim(ids.S1, 'set', 'name', 'second', [ids.N1]))
      ids.Sx = block({ kind: 'Selection', schema: 1, asserted_by: 'elsewhere', members: [], derived_from: [] }).cid     // never stored here
      ids.S2 = put({ kind: 'Selection', schema: 1, asserted_by: 'test', derived_from: [],
        members: [{ selection: ids.S1 }, { selection: ids.Sx }, { item: { address: item.C, via: [] } }]
          .sort((a, b) => { const k = (m) => (m.selection ?? m.item.address).toString(); return k(a) < k(b) ? -1 : 1 }) })
      ids.D = put(claim(ids.S2, 'delete', null, null, []))
      ids.DR = put(claim(CID.parse(runs.R2.completion), 'delete', null, null, []))
      const t = at.R1 + 1
      return {
        log: [entryName(t, 'selection', ids.S1), entryName(t, 'claim', ids.N1),
              entryName(now - 100_000, 'claim', ids.N2), entryName(now - 90_000, 'selection', ids.S2),
              entryName(now - 80_000, 'claim', ids.D), entryName(now - 70_000, 'claim', ids.DR)],
        rows: (db) => {
          const s1 = ids.S1.toString()
          db.exec({ sql: "INSERT INTO collection(collection_cid, kind, asserted_by) VALUES (?, 'selection', 'test')", bind: [s1] })
          for (const i of [item.A, item.B])
            db.exec({ sql: 'INSERT INTO collection_item(collection_cid, item_cid, via_cid) VALUES (?, ?, ?)', bind: [s1, i.toString(), r1coll] })
          db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'selection', NULL, '2026-09-01T10:00:00.000Z')", bind: [s1] })
          db.exec({ sql: "INSERT INTO claim VALUES (?, ?, 'set', 'name', 'first', '2026-09-01T10:00:00.000Z', 'test')", bind: [ids.N1.toString(), s1] })
          db.exec({ sql: "INSERT INTO log_entry VALUES (?, 'claim', NULL, '2026-09-01T10:00:00.000Z')", bind: [ids.N1.toString()] })
          db.exec({ sql: "INSERT INTO claim_current VALUES (?, 'name', 'first', ?, 0)", bind: [s1, ids.N1.toString()] })
        },
      }
    },
  })
  const text = Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()]))
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }),
    now: () => now,
  })
  return { explorer, member, ids: text }
}

test('the tail brings Selections and Claims the snapshot lacks, and only those', async () => {
  const { explorer, ids } = await open()
  assert.deepEqual(explorer.tailSelections.filter(t => t.value).map(t => t.cid), [ids.S2])
  assert.deepEqual(explorer.tailClaims.map(t => t.cid).sort(), [ids.N2, ids.D, ids.DR].sort())
})

test('a tail rename supersedes a snapshot name; a deleted Selection leaves the list for the deleted one', async () => {
  const { explorer, ids } = await open()
  const page = await explorer.selectionPage()
  const s1 = page.rows.find(r => r.cid === ids.S1)
  assert.deepEqual(s1.state.names, ['second'])
  assert.equal(s1.source, 'snapshot')
  assert.ok(!page.rows.some(r => r.cid === ids.S2))
  assert.equal(page.hiddenCount, 1)
  const deleted = await explorer.selectionPage({ showDeleted: true })
  assert.deepEqual(deleted.rows.map(r => [r.cid, r.state.deletion, r.source]), [[ids.S2, 'deleted', 'tail']])
})

test('a Selection view has its members, first-seen time and current state', async () => {
  const { explorer, member, ids } = await open()
  const s = await explorer.selection(ids.S2)
  assert.equal(s.state.deletion, 'deleted')
  assert.deepEqual(s.members.map(m => [m.kind, m.address]).sort(), [
    ['selection', ids.S1], ['selection', ids.Sx], ['item', member.item.C]].sort())
  assert.match(s.firstSeen, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
})

test('a nested Selection this member lacks is held elsewhere, not an error (Review Focus 5)', async () => {
  const { explorer, member, ids } = await open()
  assert.equal(await explorer.held('selection', ids.Sx), 'elsewhere')
  assert.equal(await explorer.held('selection', ids.S1), 'here')
  assert.equal(await explorer.held('item', member.item.C), 'here')
})

test('a run hidden by a tail delete Claim leaves the run list and query 2', async () => {
  const { explorer, member } = await open()
  const page = await explorer.runPage('demo')
  assert.ok(!page.rows.some(r => r.completion_cid === member.runs.R2.completion))
  assert.equal(page.hidden, 1)
  assert.equal(await explorer.latestSuccessfulRun('demo'), member.runs.R1.completion)
})

test('selectionsHolding finds the snapshot Selections an item is in', async () => {
  const { explorer, member, ids } = await open()
  assert.deepEqual(await explorer.selectionsHolding(member.item.A), [ids.S1])
})
