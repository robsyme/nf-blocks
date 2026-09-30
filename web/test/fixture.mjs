// web/test/fixture.mjs
// A small member built the way the plugin builds one: real DAG-CBOR blocks,
// and a snapshot with the index schema holding run R1's rows. R2 and R3 are
// newer and only in the Store Log. Tests may encode; the page never does.
import { readFileSync } from 'node:fs'
import * as dagCbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from '@noble/hashes/sha2.js'
import { loadSqlite } from './helpers.mjs'

const HORIZON = 9999999999999
export const entryName = (millis, kind, cid) => `${String(HORIZON - millis).padStart(13, '0')}-${kind}-${cid}`
export const rawCid = (text) => CID.create(1, 0x55, Digest.create(0x12, sha256(new TextEncoder().encode(text))))

export function block(value) {
  const bytes = dagCbor.encode(value)
  return { cid: CID.create(1, 0x71, Digest.create(0x12, sha256(bytes))), bytes }
}

const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })

function manifest(runName) {
  return { kind: 'RunManifest', schema: 1, asserted_by: 'test', pipeline: 'demo', repository: null, revision: null,
    commit_id: null, run_name: runName, nf_run_hash: `hash-${runName}`, session_id: 's', resumed: false,
    nextflow_version: '26.04.6', params: {}, config: {}, script: null, started_at: '2026-09-01T00:00:00.000Z' }
}

function completion(run, collections, status, finishedAt) {
  return { kind: 'RunCompletion', schema: 1, asserted_by: 'test', run, collections, input_set: null, status,
    exit_status: status === 'succeeded' ? 0 : 1, possibly_incomplete: status !== 'succeeded',
    started_at: '2026-09-01T00:00:00.000Z', finished_at: finishedAt,
    anomalies: { unresolvable: 0, unaddressed: 1, declined: 0, never_published: 0 }, error: null }
}

export async function buildMember({ now = Date.now(), extra = null } = {}) {
  const blocks = new Map()
  const put = (value) => { const b = block(value); blocks.set(b.cid.toString(), b.bytes); return b.cid }
  const content = { A: rawCid('bam-A'), B: rawCid('bam-B'), C: rawCid('bam-C') }
  const item = {
    A: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'A', lane: 1, depth: 1.5 }, leaf('A.bam', content.A)] }),
    B: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'B', lane: 2, depth: 2.5 }, leaf('B.bam', content.B)] }),
    C: put({ kind: 'OutputItem', schema: 1, value: [{ sample: 'C', lane: 3, depth: 3.5 }, leaf('C.bam', content.C)] }),
  }
  const byCid = (ids) => ids.map(k => item[k]).sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
  const runs = {}
  for (const [name, samples, status, finished] of [
    ['R1', ['A', 'B'], 'succeeded', '2026-09-01T10:00:00.000Z'],
    ['R2', ['B', 'C'], 'succeeded', '2026-09-02T10:00:00.000Z'],
    ['R3', ['C'], 'failed', '2026-09-03T10:00:00.000Z'],
  ]) {
    const m = put(manifest(name))
    const items = byCid(samples)
    const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: m, name: 'aligned', items,
      paths: items.map(i => [`aligned/${samples.find(s => item[s].equals(i))}.bam`]) })
    const comp = put(completion(m, [coll], status, finished))
    runs[name] = { manifest: m.toString(), collection: coll.toString(), completion: comp.toString(), samples, status, finished }
  }
  const at = { R0: now - 3 * 3600_000, R1: now - 3600_000, R2: now - 1800_000, R2again: now - 1500_000, R3: now - 1200_000 }
  const more = extra ? extra({ put, runs, item, content, at }) : { log: [], rows: null }
  const log = [
    entryName(at.R0, 'run', runs.R1.completion.replace(/.$/, 'q')),     // outside the overlap, never looked at
    entryName(at.R1, 'run', runs.R1.completion),
    entryName(at.R2, 'run', runs.R2.completion),
    entryName(at.R2again, 'run', runs.R2.completion),                    // the same run logged twice
    entryName(at.R3, 'run', runs.R3.completion),
    // Not a run: an unreadable tail Selection, since a RunCompletion is not a
    // Selection (BlockError schema_invalid); the views list it with its
    // error, as they do an unreadable run.
    entryName(at.R3 + 1, 'selection', runs.R3.completion),
    ...more.log,
  ]
  const watermark = entryName(at.R1, 'run', runs.R1.completion)
  const snapshot = await snapshotOf(runs.R1, item, content, watermark, more.rows)
  /** The first fixture block value of the given kind, decoded. */
  const valueOfKind = (kind) => {
    for (const bytes of blocks.values()) {
      const value = dagCbor.decode(bytes)
      if (value.kind === kind) return value
    }
    return null
  }
  return { blocks, log, watermark, runs, item: Object.fromEntries(Object.entries(item).map(([k, v]) => [k, v.toString()])),
    content: Object.fromEntries(Object.entries(content).map(([k, v]) => [k, v.toString()])), snapshot, valueOfKind }
}

/** R1's rows, as Index.ingestRun and IndexSnapshot.write would leave them. */
async function snapshotOf(r1, item, content, watermark, rows = null) {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    db.exec({ sql: 'INSERT INTO schema_version VALUES (3)' })
    db.exec({ sql: 'INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)', bind: [r1.completion, r1.manifest, 'demo', null, null,
      'hash-R1', 's', 'R1', 'test', 'succeeded', 0, r1.finished] })
    db.exec({ sql: "INSERT INTO collection(collection_cid, kind, completion_cid, output_name, asserted_by) VALUES (?, 'output', ?, ?, 'test')",
      bind: [r1.collection, r1.completion, 'aligned'] })
    for (const [s, lane, depth] of [['A', 1, '1.5'], ['B', 2, '2.5']]) {
      const i = item[s].toString()
      db.exec({ sql: 'INSERT INTO item VALUES (?)', bind: [i] })
      db.exec({ sql: 'INSERT INTO collection_item(collection_cid, item_cid) VALUES (?, ?)', bind: [r1.collection, i] })
      db.exec({ sql: 'INSERT INTO producer VALUES (?,?,?,?,?)', bind: [content[s].toString(), i, r1.collection, r1.completion, `${s}.bam`] })
      for (const [path, type, value] of [['sample', 'string', s], ['lane', 'int', String(lane)], ['depth', 'float', depth]])
        db.exec({ sql: 'INSERT INTO item_attr VALUES (?,?,?,?,0)', bind: [i, path, type, value] })
    }
    if (rows) rows(db)
    db.exec({ sql: "INSERT INTO meta VALUES ('store_log_watermark', ?)", bind: [watermark] })
    db.exec({ sql: "INSERT INTO meta VALUES ('snapshot_written_at', '2026-09-01T10:00:01.000Z')" })
    return sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
}

/** A fetch over the fixture's blocks, counting what is asked for. */
export function blockFetch(blocks) {
  const asked = []
  const fn = async (url) => {
    const cid = String(url).split('/').pop()
    asked.push(cid)
    const bytes = blocks.get(cid)
    return bytes ? new Response(bytes) : new Response('no such block', { status: 404 })
  }
  fn.asked = asked
  return fn
}
