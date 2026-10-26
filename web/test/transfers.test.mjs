// Transfer provenance: Explorer.transfersOf (snapshot rows, tail blocks, a snapshot with no transfer tables) and its two places on the page.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import * as dagCbor from '@ipld/dag-cbor'
import { validBlock } from '../src/schema.js'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { snapshotDb, FAKE_STORAGE } from './helpers.mjs'
import { buildMember, blockFetch, entryName } from './fixture.mjs'
import { installDom, frame, strayTexts } from './dom.mjs'
import { transfersNode } from '../src/views/common.js'
import { storage } from '../src/views/storage.js'
import { run } from '../src/views/run.js'
import { selection } from '../src/views/selection.js'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { fullWhenText } from '../src/words.js'

const NOW = Date.parse('2026-10-24T12:00:00.000Z')
const transfer = (roots, runs, extra = {}) => ({ kind: 'Transfer', schema: 1, asserted_by: 'rob', roots, from: 'org-s3', to: 'lab',
  depth: 'results', claims: ['name'], runs, blocks: 12, copied: 9, bytes: 4096, timestamp: '2026-10-24T11:00:00.000Z', ...extra })

// The generated IPLD schema learns Transfer when DESIGN.md section 6 gains it (the plugin lead's edit). Until then BlockFetcher
// refuses the block, so these tests decode it unvalidated; the last test checks the real fetcher once the schema knows it.
class LenientFetcher extends BlockFetcher {
  async get(cid) {
    try { return await super.get(cid) } catch (e) {
      if (e.code !== 'schema_invalid') throw e
      return { cid, value: dagCbor.decode(await this.fetchVerified(cid)) }
    }
  }
}

async function open(member, { log = member.log, strict = false } = {}) {
  const fetchFn = blockFetch(member.blocks)
  const blocks = new (strict ? BlockFetcher : LenientFetcher)('http://h/m/lab/', { fetchFn })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot), blocks,
    listFn: async () => ({ names: log, readable: true }), now: () => NOW })
  return { explorer, fetchFn }
}

test('a snapshot transfer row names the run it carried and the root it was asked for, newest first', async () => {
  const rows = (db, run) => {
    for (const [cid, at] of [['bafyt1', '2026-10-23T10:00:00.000Z'], ['bafyt2', '2026-10-24T10:00:00.000Z']]) {
      db.exec({ sql: "INSERT INTO transfer VALUES (?, 'org-s3', 'lab', 'rob', 'results', 'name,tag', 10, 8, 2048, ?)", bind: [cid, at] })
      db.exec({ sql: "INSERT INTO transfer_root VALUES (?, ?, 'run')", bind: [cid, run] })
    }
    db.exec({ sql: "INSERT INTO transfer_root VALUES ('bafyt2', 'bafysel', 'root')" })
  }
  let r1
  const member = await buildMember({ now: NOW, extra: ({ runs }) => { r1 = runs.R1.completion; return { log: [], rows: db => rows(db, r1) } } })
  const { explorer } = await open(member)
  const ts = await explorer.transfersOf(r1)
  assert.deepEqual(ts.map(t => t.cid), ['bafyt2', 'bafyt1'])
  assert.deepEqual(ts[0], { cid: 'bafyt2', from: 'org-s3', to: 'lab', assertedBy: 'rob', depth: 'results', claims: ['name', 'tag'],
    blocks: 10, copied: 8, bytes: 2048, at: '2026-10-24T10:00:00.000Z', source: 'snapshot' })
  assert.deepEqual((await explorer.transfersOf('bafysel')).map(t => t.cid), ['bafyt2'])
  assert.deepEqual(await explorer.transfersOf('bafynothing'), [])
})

test('a tail-only transfer is read from its block, matched on roots and on runs, and ignored for other cids', async () => {
  const ids = {}
  const member = await buildMember({ now: NOW, extra: ({ put, runs }) => {
    ids.sel = put({ kind: 'Selection', schema: 1, asserted_by: 'rob', derived_from: [], members: [] })
    const t = put(transfer([ids.sel], [CID.parse(runs.R2.completion)]))
    ids.t = t.toString()
    return { log: [entryName(NOW - 60_000, 'transfer', t)], rows: null }
  } })
  const { explorer } = await open(member)
  assert.equal(explorer.tailTransfers.length, 1)
  const viaRun = await explorer.transfersOf(member.runs.R2.completion)
  assert.equal(viaRun.length, 1)
  assert.deepEqual([viaRun[0].cid, viaRun[0].from, viaRun[0].assertedBy, viaRun[0].source], [ids.t, 'org-s3', 'rob', 'tail'])
  assert.equal((await explorer.transfersOf(ids.sel.toString())).length, 1)
  assert.deepEqual(await explorer.transfersOf(member.runs.R1.completion), [])
})

test('a store with no transfers shows nothing and fetches no more blocks', async () => {
  const withTables = await buildMember({ now: NOW })
  const again = await open(withTables)
  const before = again.fetchFn.asked.length
  assert.deepEqual(await again.explorer.transfersOf(withTables.runs.R1.completion), [])
  assert.equal(again.fetchFn.asked.length, before)
})

const t1 = { cid: 'bafytransfer1', from: 'org-s3', to: 'lab', assertedBy: 'rob', depth: 'lineage', claims: [], blocks: 3, copied: 3, bytes: 9, at: '2026-10-24T11:00:00.000Z' }
const t0 = { ...t1, cid: 'bafytransfer0', from: 'old-store', depth: 'results', at: '2026-10-20T11:00:00.000Z' }

test('transfersNode: one line per transfer in the order given, each linking its block; nothing when there are none', async () => {
  installDom()
  const ex = { now: () => NOW, transfersOf: async () => [t1, t0] }
  const node = await transfersNode(ex, 'bafyrun', { viewer: 'rob' })
  const lines = node.querySelectorAll('[data-transfer]')
  assert.deepEqual(lines.map(l => l.dataset.transfer), ['bafytransfer1', 'bafytransfer0'])
  assert.match(lines[0].textContent, /Copied here from org-s3 by .*you/)
  assert.match(lines[0].textContent, /depth lineage/)
  assert.match(lines[1].textContent, /Copied here from old-store by .*you/)
  assert.equal(lines[0].querySelector('a').getAttribute('href'), '#/block/bafytransfer1')
  assert.equal(await transfersNode({ now: () => NOW, transfersOf: async () => [] }, 'x', {}), null)
  assert.equal(await transfersNode({ now: () => NOW }, 'x', {}), null)
  // A query that fails is said, never read as "nothing was copied here".
  const failed = await transfersNode({ now: () => NOW, transfersOf: async () => { throw new Error('database disk image is malformed') } }, 'x', {})
  assert.equal(failed.dataset.transfersUnreadable, 'x')
  assert.ok(failed.classList.contains('muted'))
  assert.equal(failed.textContent, "Couldn't read how this got here.")
  assert.equal(failed.getAttribute('title'), 'database disk image is malformed')
})

const ctx = () => ({ tray: new Tray(null), trayChanged: () => {}, rerender: () => {}, store: { name: 'lab', key: 'a' },
  write: { available: false, reason: 'read only', hrefFor: (h) => h, served: false } })

test('the run Provenance tab and the Selection head show how it got here', async () => {
  installDom()
  const base = { ...FAKE_STORAGE, now: () => NOW, transfersOf: async () => [t1] }
  const runEx = { ...base,
    run: async () => ({ row: { run_name: 'high_jang', pipeline: 'p', manifest_cid: 'bafyman' }, completion: { status: 'succeeded', possibly_incomplete: false,
      started_at: '2026-10-24T10:00:00.000Z', finished_at: '2026-10-24T10:05:00.000Z', error: null, anomalies: {} }, collections: [], state: claimState([]) }),
    collection: async () => ({ total: 0, items: [], index: null }), allItems: async () => [], runSize: async () => ({ bytes: 1, blocks: 1, partial: false }),
    latestSuccessfulRun: async () => null, runIdentity: async () => ({ manifest: 'bafyman' }),
    blocks: { urlFor: (a) => a, ofKind: async () => ({ value: {} }) } }
  const page = await run(runEx, 'bafyrun', ctx(), { tab: 'provenance' })
  await frame()
  assert.equal(page.querySelectorAll('[data-transfer]').length, 1)
  const none = await run({ ...runEx, transfersOf: async () => [] }, 'bafyrun', ctx(), { tab: 'provenance' })
  await frame()
  assert.equal(none.querySelectorAll('[data-transfer]').length, 0)

  const selEx = { ...base, selectionSize: async () => ({ bytes: 1, blocks: 1, files: 1, partial: false }), runIdentity: async () => ({}),
    selection: async () => ({ state: { names: ['x'], nameClaims: [], current: [], nameConflicted: false, deletion: 'none', deletionClaims: [], pins: [], pinned: false },
      members: [], firstSeen: null, block: { asserted_by: 'rob' } }) }
  const sel = await selection(selEx, 'bafysel', ctx())
  assert.equal(sel.querySelectorAll('[data-transfer]').length, 1)
})

test('the real block fetcher accepts a Transfer block once the generated schema knows the kind', async (t) => {
  const block = transfer([], [])
  if (!validBlock(block)) return t.skip('web/src/generated/schema.json has no Transfer yet (DESIGN.md section 6 is the plugin lead\'s edit)')
  const member = await buildMember({ now: NOW, extra: ({ put }) => ({ log: [entryName(NOW - 60_000, 'transfer', put(block))], rows: null }) })
  const { explorer } = await open(member, { strict: true })
  assert.equal(explorer.tailTransfers[0].error, null)
})

test('a tail Transfer the strict fetcher refuses does not stop the open, carries its error, and leaves a trace on the Storage view only', async () => {
  installDom()
  const ids = {}
  const member = await buildMember({ now: NOW, extra: ({ put, runs }) => {
    ids.t = put(transfer([], [CID.parse(runs.R2.completion)])).toString()
    return { log: [entryName(NOW - 60_000, 'transfer', ids.t)], rows: null }
  } })
  const { explorer } = await open(member, { strict: true })
  if (validBlock(transfer([], []))) return          // the schema knows Transfer: the block reads, nothing is degraded
  assert.equal(explorer.tailTransfers.length, 1)
  assert.equal(explorer.tailTransfers[0].value, null)
  assert.equal(explorer.tailTransfers[0].error.code, 'schema_invalid')
  assert.deepEqual(await explorer.transfersOf(member.runs.R2.completion), [])
  assert.deepEqual(explorer.unreadableTransfers().map(t => t.cid), [ids.t])
  const page = await storage({ ...FAKE_STORAGE, snapshotWrittenAt: null, now: () => NOW, storeStats: async () => null, unreadableTransfers: () => explorer.unreadableTransfers() }, { store: { name: 'lab', key: 'a' } })
  const line = page.querySelector('[data-unreadable-transfer]')
  assert.match(line.textContent, /A copy was recorded here but its record could not be read/)
  assert.equal(line.querySelector('a').getAttribute('href'), `#/block/${ids.t}`)
})

test('a transfer in the snapshot and in the tail log is one result, from the snapshot', async () => {
  let tcid
  const member = await buildMember({ now: NOW, extra: ({ put, runs }) => {
    const t = put(transfer([], [CID.parse(runs.R1.completion)]))
    tcid = t.toString()
    return { log: [entryName(NOW - 60_000, 'transfer', t)], rows: (db) => {
      db.exec({ sql: "INSERT INTO transfer VALUES (?, 'org-s3', 'lab', 'rob', 'results', 'name', 12, 9, 4096, '2026-10-24T11:00:00.000Z')", bind: [tcid] })
      db.exec({ sql: "INSERT INTO transfer_root VALUES (?, ?, 'run')", bind: [tcid, runs.R1.completion] })
    } }
  } })
  const { explorer } = await open(member)
  assert.deepEqual(explorer.tailTransfers, [], 'a known entry is not read again')
  const ts = await explorer.transfersOf(member.runs.R1.completion)
  assert.deepEqual(ts.map(t => [t.cid, t.source]), [[tcid, 'snapshot']])
})

test('recentActivity lists Transfers, from the snapshot table and the tail, with who, where from and the depth', async () => {
  let tail
  const member = await buildMember({ now: NOW, extra: ({ put, runs }) => {
    const t = put(transfer([], [CID.parse(runs.R2.completion)], { from: 'lab2', depth: 'all', asserted_by: 'ana@example.org', timestamp: '2026-10-24T11:59:00.000Z' }))
    tail = t.toString()
    return { log: [entryName(NOW - 30_000, 'transfer', t)], rows: (db) => {
      db.exec({ sql: "INSERT INTO transfer VALUES ('bafysnapt', 'org-s3', 'lab', 'rob', 'lineage', 'names', 10, 8, 2048, '2026-10-24T11:58:00.000Z')" })
      db.exec({ sql: "INSERT INTO log_entry VALUES ('bafysnapt', 'transfer', NULL, '2026-10-24T11:58:01.000Z')" })
    } }
  } })
  const { explorer } = await open(member)
  const rows = await explorer.recentActivity(8)
  assert.deepEqual(rows.slice(0, 2).map(r => [r.kind, r.cid]), [['transfer', tail], ['transfer', 'bafysnapt']], 'newest first, ahead of the older runs')
  assert.deepEqual(rows[0].transfer, { from: 'lab2', to: 'lab', depth: 'all', assertedBy: 'ana@example.org', at: '2026-10-24T11:59:00.000Z' })
  assert.deepEqual(rows[1].transfer, { from: 'org-s3', to: 'lab', depth: 'lineage', assertedBy: 'rob', at: '2026-10-24T11:58:00.000Z' })
  assert.equal(rows[1].written_at, '2026-10-24T11:58:01.000Z')
})

test("Home's activity says a Transfer as \"Copied from <from> · <depth>\" with who and when, its seal linking its block", async () => {
  installDom()
  const { activityTag } = await import('../src/views/home.js')
  const tag = activityTag({ kind: 'transfer', cid: 'bafytransfer1', written_at: '2026-10-24T11:00:01.000Z',
    transfer: { from: 'org-s3', to: 'lab', depth: 'lineage', assertedBy: 'ana@example.org', at: '2026-10-24T11:00:00.000Z' } })
  assert.match(tag.textContent, /^Copied fromorg-s3 · lineage/)
  assert.equal(tag.querySelector('.av').getAttribute('title'), 'ana@example.org')
  assert.equal(tag.querySelector(`[title="${fullWhenText('2026-10-24T11:00:00.000Z')}"]`).textContent.length > 0, true, 'when, with the exact time on hover')
  const a = tag.querySelector('a[data-transfer-block]')
  assert.equal(a.getAttribute('href'), '#/block/bafytransfer1')
  assert.ok(a.querySelector('.seal'))
  assert.deepEqual(strayTexts(tag), [])
})
