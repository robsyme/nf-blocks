// The explorer's answers: the snapshot's rows through the plugin's own SQL,
// and, for runs newer than the snapshot, the same answers computed from their
// verified blocks (spec sections 5.3 to 5.5).
import SQL from './queries.json' with { type: 'json' }
import { CLOSURE_FETCH_NOTICE, SELECTIONS_PAGE, SNAPSHOT_PATH, STALE_RUNS_NOTICE } from './config.js'
import { BlockError } from './blocks.js'
import { entriesSince } from './storelog.js'
import { typedDecode } from './typed.js'
import { attrRows, leavesOf, matches, metadataView, predicateRow } from './metadata.js'
import { DELETED, claimState } from './claims.js'

const byNewest = (a, b) => (a.finished_at > b.finished_at ? -1 : a.finished_at < b.finished_at ? 1
  : a.completion_cid < b.completion_cid ? -1 : a.completion_cid > b.completion_cid ? 1 : 0)
/** Where a page of `shown` rows at `offset` sits among `total`, the tail's `tail` rows all on the first page. */
function span({ offset, limit, shown, count, tail = 0 }) {
  const rows = shown + (offset === 0 ? tail : 0)
  return {
    first: rows === 0 ? 0 : offset === 0 ? 1 : tail + offset + 1,
    last: tail + offset + shown,
    total: tail + count,
    next: offset + shown < count ? offset + limit : null,
    prev: offset > 0 ? Math.max(0, offset - limit) : null,
  }
}

export const RUNS_PAGE = 50
export const ITEMS_PAGE = 500

const text = (cid) => (cid === null || cid === undefined ? null : cid.toString())
const isoOf = (millis) => new Date(millis).toISOString()

export class Explorer {
  constructor({ base, db, blocks, now }) {
    this.base = base
    this.db = db
    this.blocks = blocks
    this.now = now
    this.watermark = null
    this.stale = []
    this.tailSelections = []
    this.tailClaims = []
    this.logReadable = true
    this.closing = new Map()
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
    const { names, readable } = await listFn(this.base, { watermark: this.watermark, nowMillis: this.now() })
    this.logReadable = readable
    const since = entriesSince(names, this.watermark, this.now())
    const unique = (kind) => [...new Map(since.filter(e => e.kind === kind).map(e => [e.cid, e])).values()]
    const known = async (sql, entries, column) => (entries.length === 0 ? new Set()
      : new Set((await this.db.query(sql, [JSON.stringify(entries.map(e => e.cid))])).map(r => r[column])))
    const runs = unique('run')
    const selections = unique('selection')
    const claims = unique('claim')
    const knownRuns = await known(SQL.runsKnown, runs, 'completion_cid')
    const knownSelections = await known(SQL.selectionsKnown, selections, 'collection_cid')
    const knownClaims = await known(SQL.claimsKnown, claims, 'claim_cid')
    this.stale = await Promise.all(runs.filter(e => !knownRuns.has(e.cid)).map(e => this.staleRun(e)))
    this.tailSelections = await Promise.all(selections.filter(e => !knownSelections.has(e.cid)).map(e => this.tailBlock(e, 'Selection')))
    this.tailClaims = await Promise.all(claims.filter(e => !knownClaims.has(e.cid)).map(e => this.tailBlock(e, 'Claim')))
  }

  async tailBlock(entry, kind) {
    try {
      return { cid: entry.cid, entry, value: (await this.blocks.ofKind(entry.cid, kind)).value, error: null }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      return { cid: entry.cid, entry, value: null, error: e }
    }
  }

  /** Every Claim this page can see about each subject: the snapshot's rows and the tail's blocks. */
  async claimsFor(subjects) {
    const wanted = [...new Set(subjects)]
    const out = new Map(wanted.map(s => [s, []]))
    if (wanted.length === 0) return out
    const json = JSON.stringify(wanted)
    const byCid = new Map()
    for (const r of await this.db.query(SQL.claimsOf, [json])) {
      const c = { cid: r.claim_cid, subject: r.subject_cid, verb: r.verb, attribute: r.attribute, value: r.value,
        timestamp: r.timestamp, asserted_by: r.asserted_by, supersedes: [], source: 'snapshot' }
      byCid.set(c.cid, c)
      out.get(c.subject).push(c)
    }
    for (const s of await this.db.query(SQL.supersedesOf, [json]))
      byCid.get(s.claim_cid)?.supersedes.push(s.superseded_cid)
    for (const t of this.tailClaims.filter(t => t.value)) {
      const subject = text(t.value.subject)
      if (!out.has(subject)) continue
      out.get(subject).push({ cid: t.cid, subject, verb: t.value.verb, attribute: t.value.attribute, value: t.value.value,
        timestamp: t.value.timestamp, asserted_by: t.value.asserted_by, supersedes: t.value.supersedes.map(text), source: 'tail' })
    }
    return out
  }

  async claimStates(subjects) {
    const claims = await this.claimsFor(subjects)
    return new Map([...claims].map(([subject, list]) => [subject, { ...claimState(list), claims: list }]))
  }

  /** A page of this member's Selections, newest first seen first; the tail's all on the first page. */
  async selectionPage({ offset = 0, limit = SELECTIONS_PAGE, showDeleted = false } = {}) {
    const snapshot = (await this.db.query(SQL.selectionsPage, [limit, offset]))
      .map(r => ({ cid: r.selection_cid, firstSeen: r.written_at, assertedBy: r.asserted_by, source: 'snapshot', error: null }))
    const tail = offset === 0 ? this.tailSelections.map(t => ({ cid: t.cid, firstSeen: isoOf(t.entry.writtenAtMillis),
      assertedBy: t.value?.asserted_by ?? null, source: 'tail', error: t.error })) : []
    const rows = [...tail.sort((a, b) => (a.firstSeen > b.firstSeen ? -1 : 1)), ...snapshot]
    const states = await this.claimStates(rows.map(r => r.cid))
    const all = rows.map(r => ({ ...r, state: states.get(r.cid) }))
    const shown = all.filter(r => (showDeleted ? r.state.deletion === DELETED : !r.state.hidden))
    const [{ n }] = await this.db.query(SQL.selectionCount)
    return { rows: shown, hiddenCount: all.length - shown.length,
      ...span({ offset, limit, shown: snapshot.length, count: n, tail: tail.length }) }
  }

  async selection(cid) {
    const block = (await this.blocks.ofKind(cid, 'Selection')).value
    const state = (await this.claimStates([cid])).get(cid)
    const [seen] = await this.db.query(SQL.firstSeen, [cid])
    const tail = this.tailSelections.find(t => t.cid === cid)
    return {
      cid, block, state,
      firstSeen: seen?.written_at ?? (tail ? isoOf(tail.entry.writtenAtMillis) : null),
      members: block.members.map(m => (m.item
        ? { kind: 'item', address: text(m.item.address), via: m.item.via.map(text) }
        : { kind: 'selection', address: text(m.selection) })),
    }
  }

  /** Whether this member holds a member's block (spec section 4: an item may be held in another member). */
  async held(kind, address) {
    try {
      await this.blocks.ofKind(address, kind === 'selection' ? 'Selection' : 'OutputItem')
      return 'here'
    } catch (e) {
      if (e instanceof BlockError && e.code === 'block_missing') return 'elsewhere'
      throw e
    }
  }

  async selectionsHolding(itemCid) {
    const snapshot = (await this.db.query(SQL.selectionsHolding, [itemCid])).map(r => r.collection_cid)
    const tail = this.tailSelections.filter(t => t.value?.members.some(m => text(m.item?.address) === itemCid)).map(t => t.cid)
    return [...new Set([...snapshot, ...tail])].sort()
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

  /** One page of a pipeline's runs (runsOfPipeline) and where it sits: first, last, total, next and prev offsets. */
  async runPage(pipeline, { limit = RUNS_PAGE, offset = 0 } = {}) {
    const rows = await this.runsOfPipeline(pipeline, { limit, offset })
    const [{ n }] = await this.db.query(SQL.runCount, [pipeline])
    const tail = this.stale.filter(s => s.row?.pipeline === pipeline).length
    const states = await this.claimStates(rows.map(r => r.completion_cid))
    const visible = rows.filter(r => !states.get(r.completion_cid).hidden)
    return { rows: visible, hidden: rows.length - visible.length,
      ...span({ offset, limit, shown: rows.filter(r => r.source === 'snapshot').length, count: n, tail }) }
  }

  async runsOfPipeline(pipeline, { limit = RUNS_PAGE, offset = 0 } = {}) {
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

  /** One page of a collection's items, with first, last, total, next and prev as runPage. */
  async collection(cid, { limit = ITEMS_PAGE, offset = 0 } = {}) {
    const [row] = await this.db.query(SQL.collectionByCid, [cid])
    if (row) {
      const items = (await this.db.query(SQL.collectionItems, [cid, limit, offset])).map(r => r.item_cid)
      const [{ n }] = await this.db.query(SQL.collectionItemCount, [cid])
      return { cid, output: row.output_name, completion: row.completion_cid, items, ...span({ offset, limit, shown: items.length, count: n }) }
    }
    const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
    const completion = this.stale.find(s => s.completion?.collections.some(c => text(c) === cid))?.cid ?? null
    const all = block.items.filter(Boolean).map(text)
    const items = all.slice(offset, offset + limit)
    return { cid, output: block.name, completion, items, ...span({ offset, limit, shown: items.length, count: all.length }) }
  }

  async item(collectionCid, itemCid) {
    const { value } = (await this.blocks.ofKind(itemCid, 'OutputItem')).value
    return { collection: collectionCid, cid: itemCid, value, view: metadataView(value), leaves: leavesOf(value) }
  }

  /**
   * A stale run's collections and items, fetched once and turned into
   * index-shaped rows. A missing or refused block does not fail the query:
   * as Index.ingestCollection/ingestItem do, a missing OutputCollection is
   * skipped (nothing of that output is indexed), and a missing OutputItem
   * keeps its collection_item membership with no attributes and no
   * producers. Each is recorded in the result's `missing` list.
   */
  closure(stale, onProgress = () => {}) {
    // One closure per run however many queries ask at once (views.item asks
    // per leaf): the first caller's progress is the fetch's progress.
    if (!this.closing.has(stale.cid))
      this.closing.set(stale.cid, this.buildClosure(stale, onProgress).catch((e) => { this.closing.delete(stale.cid); throw e }))
    return this.closing.get(stale.cid)
  }

  async buildClosure(stale, onProgress) {
    const settled = await Promise.all(stale.completion.collections.map(async (c) => {
      const cid = text(c)
      try {
        return { ok: true, cid, block: (await this.blocks.ofKind(cid, 'OutputCollection')).value }
      } catch (e) {
        if (!(e instanceof BlockError)) throw e
        return { ok: false, cid, code: e.code }
      }
    }))
    const collections = settled.filter(s => s.ok)
    const missing = settled.filter(s => !s.ok).map(({ cid, code }) => ({ cid, code }))
    const total = collections.reduce((n, c) => n + c.block.items.filter(Boolean).length, 0)
    this.fetchesForQuery += total
    let done = 0
    const outputs = new Map()
    const producers = []
    for (const c of collections) {
      const items = []
      for (const link of c.block.items.filter(Boolean)) {
        const itemCid = text(link)
        try {
          const block = await this.blocks.ofKind(itemCid, 'OutputItem')
          const typed = typedDecode(block.bytes).value
          items.push({ cid: itemCid, rows: attrRows(metadataView(typed)) })
          for (const leaf of leavesOf(block.value.value))
            if (leaf.address) producers.push({ content_cid: text(leaf.address), item_cid: itemCid, collection_cid: c.cid, completion_cid: stale.cid, filename: leaf.name })
        } catch (e) {
          if (!(e instanceof BlockError)) throw e
          // The membership arrived even though the item did not (Index.ingestCollection).
          items.push({ cid: itemCid, rows: [] })
          missing.push({ cid: itemCid, code: e.code })
        }
        onProgress(++done, total)
      }
      outputs.set(c.block.name, { cid: c.cid, items })
    }
    return { outputs, producers, missing }
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
   * answer stands, so the snapshot is not read again for its finish time. A
   * tail delete Claim can still hide a candidate the SQL did not know to
   * exclude; only asked about when the tail holds any Claim at all.
   */
  async latestSuccessfulRun(pipeline) {
    const [best] = await this.db.query(SQL.latestSuccessfulRun, [pipeline])
    const stale = this.stale.filter(s => s.row && s.row.pipeline === pipeline && s.row.status === 'succeeded' && s.row.possibly_incomplete === 0)
    if (stale.length === 0) return best?.completion_cid ?? null
    const candidates = stale.map(s => s.row)
    if (best) candidates.push(await this.runRow(best.completion_cid))
    if (this.tailClaims.length === 0) return candidates.sort(byNewest)[0].completion_cid
    const states = await this.claimStates(candidates.map(c => c.completion_cid))
    const visible = candidates.filter(c => !states.get(c.completion_cid).hidden).sort(byNewest)
    if (visible.length) return visible[0].completion_cid
    const more = await this.db.query(SQL.successfulRunsOfPipeline, [pipeline])
    const moreStates = await this.claimStates(more.map(r => r.completion_cid))
    return more.find(r => !moreStates.get(r.completion_cid).hidden)?.completion_cid ?? null
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
