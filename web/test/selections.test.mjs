import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { CID } from 'multiformats/cid'
import * as dagJson from '@ipld/dag-json'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { loadSqlite, snapshotDb } from './helpers.mjs'
import { block, blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'
import SQL from '../src/queries.json' with { type: 'json' }

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

/**
 * A member built just for query 2 (latestSuccessfulRun) fix-round tests: a
 * snapshot's `run` rows, optionally some of its own `claim`/`claim_supersedes`
 * rows (a delete or an undo already in the snapshot), and the tail's own
 * Claim blocks (real, decodable ones, since a real Claim's `supersedes` is a
 * list of links). No watermark, so every tail entry is in scope and no run
 * Store Log entries at all, so `this.stale` is always empty here.
 */
async function openLatest({ pipeline = 'q2', runs = [], preClaims = [], preSupersedes = [], tailClaimDefs = [] } = {}) {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  let bytes
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    db.exec({ sql: 'INSERT INTO schema_version VALUES (3)' })
    for (const r of runs)
      db.exec({ sql: "INSERT INTO run(completion_cid, pipeline, status, possibly_incomplete, finished_at) VALUES (?, ?, 'succeeded', 0, ?)",
        bind: [r.cid, pipeline, r.finishedAt] })
    for (const c of preClaims)
      db.exec({ sql: 'INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)',
        bind: [c.cid, c.subject, c.verb, c.attribute ?? null, c.value ?? null, c.timestamp ?? '2026-09-01T00:00:00.000Z', c.assertedBy ?? 'test'] })
    for (const s of preSupersedes)
      db.exec({ sql: 'INSERT INTO claim_supersedes VALUES (?, ?)', bind: [s.claim, s.superseded] })
    bytes = sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
  const blocks = new Map()
  const put = (value) => { const b = block(value); blocks.set(b.cid.toString(), b.bytes); return b.cid }
  const log = tailClaimDefs.map((c, i) => entryName(Date.now() - i, 'claim', put(c).toString()))
  const calls = []
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => {
      const db = await snapshotDb(bytes)
      return { query: async (sql, params) => { calls.push({ sql, params }); return db.query(sql, params) }, close: db.close }
    },
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(blocks) }),
    listFn: async () => ({ names: log, readable: true }),
  })
  explorer.calls = calls
  return explorer
}

test('query 2: a tail delete of the snapshot\'s best run falls to the next one when there is no successful stale run, or to null (fix round 1, finding 1)', async () => {
  const RA = rawCid('q2-a').toString()
  const RB = rawCid('q2-b').toString()
  const del = (cid) => claim(CID.parse(cid), 'delete', null, null, [])
  const runs = [{ cid: RA, finishedAt: '2026-09-02T10:00:00.000Z' }, { cid: RB, finishedAt: '2026-09-01T10:00:00.000Z' }]

  const onlyBestDeleted = await openLatest({ runs, tailClaimDefs: [del(RA)] })
  assert.equal(await onlyBestDeleted.latestSuccessfulRun('q2'), RB)

  const bothDeleted = await openLatest({ runs, tailClaimDefs: [del(RA), del(RB)] })
  assert.equal(await bothDeleted.latestSuccessfulRun('q2'), null)
})

test('query 2: a tail delete making a snapshot run\'s deletion conflicted excludes it, though the run list still shows it (fix round 1, finding 2)', async () => {
  const RD = rawCid('q2-d').toString()
  const D0 = rawCid('q2-d-delete').toString()
  const U0 = rawCid('q2-d-undo').toString()
  const explorer = await openLatest({
    runs: [{ cid: RD, finishedAt: '2026-09-01T10:00:00.000Z' }],
    // The snapshot already saw the delete undone...
    preClaims: [{ cid: D0, subject: RD, verb: 'delete' }, { cid: U0, subject: RD, verb: 'del' }],
    preSupersedes: [{ claim: U0, superseded: D0 }],
    // ...but the tail holds a second delete, written without seeing the undo: conflicted, not hidden.
    tailClaimDefs: [claim(CID.parse(RD), 'delete', null, null, [])],
  })
  assert.equal(await explorer.latestSuccessfulRun('q2'), null)
  const page = await explorer.runPage('q2')
  assert.ok(page.rows.some(r => r.completion_cid === RD))
  assert.equal(page.hidden, 0)
})

test('query 2: a tail del undoing a snapshot delete brings the run back (fix round 1, finding 3)', async () => {
  const RE = rawCid('q2-e').toString()
  const D0 = rawCid('q2-e-delete').toString()
  const explorer = await openLatest({
    runs: [{ cid: RE, finishedAt: '2026-09-01T10:00:00.000Z' }],
    preClaims: [{ cid: D0, subject: RE, verb: 'delete' }],
    tailClaimDefs: [claim(CID.parse(RE), 'del', null, null, [CID.parse(D0)])],
  })
  assert.equal(await explorer.latestSuccessfulRun('q2'), RE)
})

// Page minors (ticket 11, Q4 a): the latest-run query pages the snapshot's
// unfiltered successful runs, newest first, sizes 1 then 50, so a tail
// deletion of more than 50 runs cannot hide an older live one.

test('query 2 pages the snapshot\'s successful runs, the first page of size 1 (page minors)', async () => {
  const RA = rawCid('q2-page-a').toString()
  const RB = rawCid('q2-page-b').toString()
  const del = (cidText) => claim(CID.parse(cidText), 'delete', null, null, [])
  const runs = [{ cid: RA, finishedAt: '2026-09-02T10:00:00.000Z' }, { cid: RB, finishedAt: '2026-09-01T10:00:00.000Z' }]
  const explorer = await openLatest({ runs, tailClaimDefs: [del(RA)] })
  assert.equal(await explorer.latestSuccessfulRun('q2'), RB)
  const pages = explorer.calls.filter(c => c.sql === SQL.successfulRunsPage).map(c => c.params[1])
  assert.deepEqual(pages, [1, 50])
})

test('query 2: a snapshot delete undone by a tail del is found on the first page (page minors)', async () => {
  const RA = rawCid('q2-page-c').toString()
  const RB = rawCid('q2-page-d').toString()
  const D0 = rawCid('q2-page-c-delete').toString()
  const runs = [{ cid: RA, finishedAt: '2026-09-02T10:00:00.000Z' }, { cid: RB, finishedAt: '2026-09-01T10:00:00.000Z' }]
  const explorer = await openLatest({
    runs,
    preClaims: [{ cid: D0, subject: RA, verb: 'delete' }],
    tailClaimDefs: [claim(CID.parse(RA), 'del', null, null, [CID.parse(D0)])],
  })
  assert.equal(await explorer.latestSuccessfulRun('q2'), RA)
})

test('query 2: a tail deleting the newest 55 of 60 runs still finds the 56th, not null (page minors)', async () => {
  const base = Date.parse('2026-09-01T00:00:00.000Z')
  const runs = Array.from({ length: 60 }, (_, i) => ({
    cid: rawCid(`q2-many-${i}`).toString(),
    finishedAt: new Date(base + (60 - i) * 60_000).toISOString(),
  }))
  const del = (cidText) => claim(CID.parse(cidText), 'delete', null, null, [])
  const explorer = await openLatest({ runs, tailClaimDefs: runs.slice(0, 55).map(r => del(r.cid)) })
  assert.equal(await explorer.latestSuccessfulRun('q2'), runs[55].cid)
})

test('claimsFor normalises a tail Claim\'s value to the snapshot\'s valueText convention: strings as themselves, else DAG-JSON text', async () => {
  const decoder = new TextDecoder()
  const subj = rawCid('q2-value-subject').toString()
  const mapValue = { a: 1 }
  const mapText = decoder.decode(dagJson.encode(mapValue))
  const numberText = decoder.decode(dagJson.encode(5))
  const preClaims = [{ cid: rawCid('q2-value-snap').toString(), subject: subj, verb: 'set', attribute: 'mapSnap', value: mapText }]
  const tailClaimDefs = [
    claim(CID.parse(subj), 'set', 'mapTail', mapValue, []),
    claim(CID.parse(subj), 'set', 'strTail', 'hello', []),
    claim(CID.parse(subj), 'set', 'numTail', 5, []),
    claim(CID.parse(subj), 'set', 'nilTail', null, []),
  ]
  const explorer = await openLatest({ preClaims, tailClaimDefs })
  const claims = (await explorer.claimsFor([subj])).get(subj)
  const byAttr = (a) => claims.find(c => c.attribute === a)
  assert.equal(byAttr('mapSnap').value, mapText)
  assert.equal(byAttr('mapTail').value, mapText)
  assert.equal(byAttr('strTail').value, 'hello')
  assert.equal(byAttr('numTail').value, numberText)
  assert.equal(byAttr('nilTail').value, null)
})

test('runPage issues one Claim query, not supersedesOf, when a run has no Claims (page minors)', async () => {
  const RA = rawCid('q2-noclaims').toString()
  const explorer = await openLatest({ pipeline: 'q2', runs: [{ cid: RA, finishedAt: '2026-09-01T10:00:00.000Z' }] })
  await explorer.runPage('q2')
  assert.ok(explorer.calls.some(c => c.sql === SQL.claimsOf))
  assert.ok(!explorer.calls.some(c => c.sql === SQL.supersedesOf))
})
