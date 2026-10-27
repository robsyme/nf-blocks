// web/test/storage-fixture.mjs
// A snapshot holding roblab's storage figures (P2 constraints, real-store
// acceptance), written as IndexSnapshot writes them, with no blocks behind it:
// anything that reads a block fails the test that counts fetches.
import { readFileSync } from 'node:fs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { SCHEMA_VERSION, SNAPSHOT_PATH } from '../src/config.js'
import { SnapshotError } from '../src/db.js'
import { loadSqlite, makeDb, snapshotDb } from './helpers.mjs'
import { blockFetch } from './fixture.mjs'

export const ROBLAB = {
  stored: 142_134_100, storedBlocks: 1285, referenced: 219_874_873, shared: 62_751_397, graceMs: 1_209_600_000,
  runs: {
    sleepy_heyrovsky: { id: 4, cid: 'bafyreiackkkvjk3njmwi4chqgmwse4q6tgburwz3xzxy6hd4in3rykhs54', pipeline: 'main.nf', status: 'succeeded',
      incomplete: 0, finished: '2026-10-24T17:58:50.649Z', reachable: 0, blocks: 0, exclusive: 0, exclusiveBlocks: 0 },
    distraught_murdock: { id: 3, cid: 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq', pipeline: 'nf-core/rnaseq', status: 'succeeded',
      incomplete: 0, finished: '2026-10-24T17:29:03.967Z', reachable: 78_392_535, blocks: 894, exclusive: 63_743_945, exclusiveBlocks: 391 },
    sad_yonath: { id: 2, cid: 'bafyreidordqagvbzb63ew3h4xwykbkufpxufq7gyf6l567dbl5bhz7ac3q', pipeline: 'nf-core/rnaseq', status: 'succeeded',
      incomplete: 0, finished: '2026-10-23T19:01:08.237Z', reachable: 78_390_155, blocks: 894, exclusive: 15_638_758, exclusiveBlocks: 223 },
    sharp_magritte: { id: 1, cid: 'bafyreiaedzibppjsllztdb2wqg322harsiohnkmccapz2uemjervj5kcgu', pipeline: 'nf-core/rnaseq', status: 'failed',
      incomplete: 1, finished: '2026-10-23T18:59:36.352Z', reachable: 53_400_488, blocks: 528, exclusive: 0, exclusiveBlocks: 0 },
  },
  selection: { id: 5, cid: 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce', name: 'wigshop', since: '2026-10-24T19:40:16.451Z',
    reachable: 1_135_852, blocks: 9 },
  // root_shared as the roblab snapshot holds it: [root, other, bytes].
  rootShared: [[1, 2, 53_400_488], [1, 3, 5_297_681], [1, 5, 1_135_852], [2, 1, 53_400_488], [2, 3, 14_648_590], [2, 5, 1_135_852],
    [3, 2, 14_648_590], [3, 1, 5_297_681], [3, 5, 1_135_852], [5, 1, 1_135_852], [5, 3, 1_135_852], [5, 2, 1_135_852]],
  kinds: [['bam', 10, 38_868_638], ['html', 60, 32_697_303], ['zip', 48, 17_516_088], ['txt', 259, 15_530_087], ['png', 147, 15_123_305],
    ['json', 43, 10_238_002], ['svg', 176, 4_519_630], ['pdf', 256, 3_122_289], ['bigwig', 15, 1_982_931]],
  // One output collection of distraught_murdock, two items, and item-1's item_size row.
  collection: { cid: 'coll-dm-aligned', items: ['item-1', 'item-2'], bytes: 105, blocks: 2 },
}

const q = (v) => (v === null ? 'NULL' : typeof v === 'number' ? String(v) : `'${String(v).replaceAll("'", "''")}'`)
const values = (rows) => rows.map(r => `(${r.map(q).join(', ')})`).join(', ')

/** The roblab rows. `assertedBy` is every root's raw principal; `extra` are more statements. */
export function roblabStatements({ assertedBy = 'anonymous', grace = ROBLAB.graceMs, extra = [] } = {}) {
  const runs = Object.entries(ROBLAB.runs)
  const s = ROBLAB.selection
  const c = ROBLAB.collection
  const who = String(assertedBy ?? '').trim().toLowerCase() || 'anonymous'
  return [
    `INSERT INTO schema_version VALUES (${SCHEMA_VERSION})`,
    `INSERT INTO run (completion_cid, manifest_cid, pipeline, run_name, asserted_by, status, possibly_incomplete, finished_at) VALUES ${values(
      runs.map(([name, r]) => [r.cid, `m-${name}`, r.pipeline, name, assertedBy, r.status, r.incomplete, r.finished]))}`,
    `INSERT INTO root VALUES ${values([...runs.map(([, r]) => [r.id, r.cid, 'run', assertedBy, 'held', r.finished]),
      [s.id, s.cid, 'selection', assertedBy, 'held', s.since]])}`,
    `INSERT INTO root_stats VALUES ${values([...runs.map(([, r]) => [r.id, r.reachable, r.blocks, r.exclusive, r.exclusiveBlocks, 0, 0]),
      [s.id, s.reachable, s.blocks, 0, 0, 0, 0]])}`,
    `INSERT INTO root_shared VALUES ${values(ROBLAB.rootShared)}`,
    `INSERT INTO store_stats VALUES (${ROBLAB.stored}, ${ROBLAB.storedBlocks}, ${ROBLAB.referenced}, 0, ${ROBLAB.stored}, 0, ${ROBLAB.shared}, 0)`,
    `INSERT INTO asserter_stats VALUES (${q(who)}, ${ROBLAB.stored}, ${ROBLAB.stored}, 0, 0)`,
    "INSERT INTO pipeline_stats VALUES ('main.nf', 1, 0, 0, 0), ('nf-core/rnaseq', 3, 142134100, 1285, 0)",
    `INSERT INTO kind_stats VALUES ${values(ROBLAB.kinds)}`,
    `INSERT INTO collection (collection_cid, kind, asserted_by) VALUES (${q(s.cid)}, 'selection', ${q(assertedBy)})`,
    `INSERT INTO claim VALUES ('k-name', ${q(s.cid)}, 'set', 'name', ${q(s.name)}, '2026-10-24T19:40:16.459Z', ${q(assertedBy)})`,
    `INSERT INTO claim_current VALUES (${q(s.cid)}, 'name', ${q(s.name)}, 'k-name', 0)`,
    `INSERT INTO collection VALUES (${q(c.cid)}, 'output', ${q(ROBLAB.runs.distraught_murdock.cid)}, 'aligned', ${q(assertedBy)})`,
    `INSERT INTO collection_item (collection_cid, item_cid) VALUES ${values(c.items.map(i => [c.cid, i]))}`,
    `INSERT INTO item VALUES ${values(c.items.map(i => [i]))}`,
    `INSERT INTO collection_size VALUES (${q(c.cid)}, ${c.bytes}, ${c.blocks})`,
    "INSERT INTO item_size VALUES ('item-1', 100, 1)",
    "INSERT INTO meta VALUES ('store_log_watermark', NULL), ('snapshot_written_at', '2026-10-25T00:45:49.070Z')",
    ...(grace === null ? [] : [`INSERT INTO meta VALUES ('sweep_grace_ms', '${grace}')`]),
    ...extra,
  ]
}

const SCHEMA = readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8')

/** The roblab snapshot's bytes. */
export async function roblabSnapshot(options = {}) {
  return makeDb(await loadSqlite(), [SCHEMA, ...roblabStatements(options)])
}

/**
 * The same runs in a lean snapshot (fix round 2, F2's contract): every
 * table, but no storage rows (store_stats has none), and meta
 * `storage_omitted` saying why (`over_cap` or `out_of_memory`). `cap`, when
 * given, is the cas.snapshot.maxBytes the writer had, as `snapshot_max_bytes`.
 */
export async function roblabLeanSnapshot(reason, { cap = null } = {}) {
  const runs = Object.entries(ROBLAB.runs)
  return makeDb(await loadSqlite(), [SCHEMA, `INSERT INTO schema_version VALUES (${SCHEMA_VERSION})`,
    `INSERT INTO run (completion_cid, manifest_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES ${values(
      runs.map(([name, r]) => [r.cid, `m-${name}`, r.pipeline, name, r.status, r.incomplete, r.finished]))}`,
    "INSERT INTO meta VALUES ('store_log_watermark', NULL), ('snapshot_written_at', '2026-10-25T00:45:49.070Z')",
    `INSERT INTO meta VALUES ('sweep_grace_ms', '${ROBLAB.graceMs}'), ('storage_omitted', ${q(reason)})`,
    ...(cap === null ? [] : [`INSERT INTO meta VALUES ('snapshot_max_bytes', '${cap}')`])])
}

/**
 * An Explorer over `snapshots` (`{ 'index/v7.sqlite': bytes, ... }`), opened
 * the way the page opens one: a path with no snapshot answers no_snapshot.
 * Records every snapshot URL asked for (`opened`), every query (`queries`)
 * and every block fetch (`fetchFn.asked`). Every older-snapshot existence
 * check (`hasFile`) goes into the `probed` array passed in; pass `opened` to see the databases opened when the open fails.
 */
export async function openSnapshots(snapshots, { blocks = new Map(), log = [], now = Date.now(), probed = [], opened = [] } = {}) {
  const queries = []
  const fetchFn = blockFetch(blocks)
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async (url) => {
      opened.push(url)
      const path = new URL(url).pathname.replace('/m/lab/', '')
      if (!snapshots[path]) throw new SnapshotError('no_snapshot', `no Index Snapshot at ${url} (HTTP 404)`)
      const db = await snapshotDb(snapshots[path])
      return { ...db, query: (sql, params) => { queries.push(sql); return db.query(sql, params) } }
    },
    hasFile: async (url) => { probed.push(url); return !!snapshots[new URL(url).pathname.replace('/m/lab/', '')] },
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    listFn: async () => ({ names: log, readable: true }),
    now: () => now,
  })
  queries.length = 0
  return { explorer, opened, queries, fetchFn }
}

/** An Explorer over the roblab snapshot. */
export async function openRoblab(options = {}) {
  return openSnapshots({ [SNAPSHOT_PATH]: await roblabSnapshot(options) })
}
