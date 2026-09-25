// The explorer's answers: the snapshot's rows through the plugin's own SQL,
// and, for runs newer than the snapshot, the same answers computed from their
// verified blocks (spec sections 5.3 to 5.5).
import SQL from './queries.json' with { type: 'json' }
import { CLOSURE_FETCH_NOTICE, SNAPSHOT_PATH, STALE_RUNS_NOTICE } from './config.js'
import { BlockError } from './blocks.js'
import { entriesSince } from './storelog.js'
import { typedDecode } from './typed.js'
import { attrRows, leavesOf, matches, metadataView, predicateRow } from './metadata.js'

const byNewest = (a, b) => (a.finished_at > b.finished_at ? -1 : a.finished_at < b.finished_at ? 1
  : a.completion_cid < b.completion_cid ? -1 : a.completion_cid > b.completion_cid ? 1 : 0)
const text = (cid) => (cid === null || cid === undefined ? null : cid.toString())

export class Explorer {
  constructor({ base, db, blocks, now }) {
    this.base = base
    this.db = db
    this.blocks = blocks
    this.now = now
    this.watermark = null
    this.stale = []
    this.closures = new Map()
    this.fetchesForQuery = 0
  }

  static async open({ base, openDb, blocks, listFn, now = () => Date.now() }) {
    const db = await openDb(new URL(SNAPSHOT_PATH, base).href)
    const explorer = new Explorer({ base, db, blocks, now })
    explorer.watermark = (await db.query(SQL.watermark))[0]?.value ?? null
    await explorer.refreshTail(listFn)
    return explorer
  }

  get staleCount() { return this.stale.length }

  get notice() { return this.stale.length > STALE_RUNS_NOTICE || this.fetchesForQuery > CLOSURE_FETCH_NOTICE }

  async refreshTail(listFn) {
    const names = await listFn(this.base, { watermark: this.watermark, nowMillis: this.now() })
    const runs = entriesSince(names, this.watermark, this.now()).filter(e => e.kind === 'run')
    const unique = [...new Map(runs.map(e => [e.cid, e])).values()]
    const known = unique.length === 0 ? new Set()
      : new Set((await this.db.query(SQL.runsKnown, [JSON.stringify(unique.map(e => e.cid))])).map(r => r.completion_cid))
    this.stale = await Promise.all(unique.filter(e => !known.has(e.cid)).map(e => this.staleRun(e)))
  }

  async staleRun(entry) {
    try {
      const completion = (await this.blocks.ofKind(entry.cid, 'RunCompletion')).value
      const manifest = (await this.blocks.ofKind(text(completion.run), 'RunManifest')).value
      const row = { completion_cid: entry.cid, manifest_cid: text(completion.run), pipeline: manifest.pipeline,
        run_name: manifest.run_name, status: completion.status, possibly_incomplete: completion.possibly_incomplete ? 1 : 0,
        finished_at: completion.finished_at, source: 'tail' }
      return { cid: entry.cid, entry, row, completion, manifest, error: null }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      return { cid: entry.cid, entry, row: null, completion: null, manifest: null, error: e }
    }
  }

  staleRow(cid) { return this.stale.find(s => s.cid === cid && s.row) ?? null }

  async pipelines() {
    const out = new Map((await this.db.query(SQL.pipelines)).map(r => [r.pipeline, { ...r }]))
    for (const s of this.stale.filter(s => s.row)) {
      const p = out.get(s.row.pipeline) ?? { pipeline: s.row.pipeline, runs: 0, latest: null }
      p.runs += 1
      if (!p.latest || s.row.finished_at > p.latest) p.latest = s.row.finished_at
      out.set(p.pipeline, p)
    }
    return [...out.values()].sort((a, b) => (a.pipeline < b.pipeline ? -1 : 1))
  }

  async runsOfPipeline(pipeline, { limit = 50, offset = 0 } = {}) {
    const snapshot = (await this.db.query(SQL.runsOfPipeline, [pipeline, limit, offset])).map(r => ({ ...r, source: 'snapshot' }))
    const tail = offset === 0 ? this.stale.filter(s => s.row?.pipeline === pipeline).map(s => s.row) : []
    return [...tail, ...snapshot].sort(byNewest)
  }

  async runRow(cid) {
    const [row] = await this.db.query(SQL.runByCompletion, [cid])
    return row ? { ...row, source: 'snapshot' } : this.staleRow(cid)?.row ?? null
  }

  async completionOf(cid) { return (await this.blocks.ofKind(cid, 'RunCompletion')).value }

  async run(cid) {
    const row = await this.runRow(cid)
    if (!row) throw new BlockError('not_found', cid, `no run ${cid} in this member's snapshot or Store Log tail`)
    const completion = await this.completionOf(cid)
    const collections = row.source === 'snapshot'
      ? (await this.db.query(SQL.collectionsOf, [cid])).map(r => ({ output: r.output_name, cid: r.collection_cid }))
      : await Promise.all(completion.collections.map(async (c) =>
        ({ output: (await this.blocks.ofKind(text(c), 'OutputCollection')).value.name, cid: text(c) })))
    return { row, completion, collections }
  }

  async collection(cid, { limit = 500, offset = 0 } = {}) {
    const [row] = await this.db.query(SQL.collectionByCid, [cid])
    if (row) {
      const items = (await this.db.query(SQL.collectionItems, [cid, limit, offset])).map(r => r.item_cid)
      return { cid, output: row.output_name, completion: row.completion_cid, items }
    }
    const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
    const completion = this.stale.find(s => s.completion?.collections.some(c => text(c) === cid))?.cid ?? null
    return { cid, output: block.name, completion, items: block.items.filter(Boolean).map(text).slice(offset, offset + limit) }
  }

  async item(collectionCid, itemCid) {
    const { value } = (await this.blocks.ofKind(itemCid, 'OutputItem')).value
    return { collection: collectionCid, cid: itemCid, value, view: metadataView(value), leaves: leavesOf(value) }
  }

  /** A stale run's collections and items, fetched once and turned into index-shaped rows. */
  async closure(stale, onProgress = () => {}) {
    if (this.closures.has(stale.cid)) return this.closures.get(stale.cid)
    const collections = await Promise.all(stale.completion.collections.map(async (c) => ({ cid: text(c), block: (await this.blocks.ofKind(text(c), 'OutputCollection')).value })))
    const total = collections.reduce((n, c) => n + c.block.items.filter(Boolean).length, 0)
    this.fetchesForQuery += total
    let done = 0
    const outputs = new Map()
    const producers = []
    for (const c of collections) {
      const items = []
      for (const link of c.block.items.filter(Boolean)) {
        const block = await this.blocks.ofKind(text(link), 'OutputItem')
        const typed = typedDecode(block.bytes).value
        items.push({ cid: text(link), rows: attrRows(metadataView(typed)) })
        for (const leaf of leavesOf(block.value.value))
          if (leaf.address) producers.push({ content_cid: text(leaf.address), item_cid: text(link), collection_cid: c.cid, completion_cid: stale.cid, filename: leaf.name })
        onProgress(++done, total)
      }
      outputs.set(c.block.name, { cid: c.cid, items })
    }
    const result = { outputs, producers }
    this.closures.set(stale.cid, result)
    return result
  }

  async producersOf(contentCid, onProgress) {
    this.fetchesForQuery = 0
    const rows = await this.db.query(SQL.producersOf, [contentCid])
    for (const s of this.stale.filter(s => s.row))
      rows.push(...(await this.closure(s, onProgress)).producers.filter(p => p.content_cid === contentCid))
    return rows
  }

  /**
   * Query 2 over the snapshot and the tail. With no stale candidate the SQL's
   * answer stands, so the snapshot is not read again for its finish time.
   */
  async latestSuccessfulRun(pipeline) {
    const [best] = await this.db.query(SQL.latestSuccessfulRun, [pipeline])
    const stale = this.stale.filter(s => s.row && s.row.pipeline === pipeline && s.row.status === 'succeeded' && s.row.possibly_incomplete === 0)
    if (stale.length === 0) return best?.completion_cid ?? null
    const candidates = stale.map(s => s.row)
    if (best) candidates.push(await this.runRow(best.completion_cid))
    return candidates.sort(byNewest)[0].completion_cid
  }

  /**
   * Query 3 for one run: the index's SQL for a snapshot run, the closure for a
   * stale one. Which one is known from memory, so the snapshot is read only by
   * the query itself (Gate assertion 2 counts every page). Returns the items
   * and the collection they came from.
   */
  async items(completionCid, output, predicates, onProgress) {
    this.fetchesForQuery = 0
    const rows = predicates.map(([path, type, value]) => predicateRow(path, type, value))
    if (rows.some(r => r.truncated)) return { items: [], collection: null }
    const stale = this.staleRow(completionCid)
    if (stale) {
      const { outputs } = await this.closure(stale, onProgress)
      const found = outputs.get(output)
      return { items: (found?.items ?? []).filter(i => matches(i.rows, rows)).map(i => i.cid).sort(), collection: found?.cid ?? null }
    }
    let sql = SQL.itemsBase
    const params = [completionCid, output]
    for (const r of rows) {
      params.push(r.path, r.type)
      if (r.value === null) sql += SQL.itemsPredicateNull
      else { sql += SQL.itemsPredicate; params.push(r.value) }
    }
    const found = await this.db.query(sql + SQL.itemsOrder, params)
    return { items: found.map(r => r.item_cid), collection: found[0]?.collection_cid ?? null }
  }
}
