// The explorer's answers: the snapshot's rows through the plugin's own SQL,
// and, for runs newer than the snapshot, the same answers computed from their
// verified blocks (spec sections 5.3 to 5.5).
import * as dagCbor from '@ipld/dag-cbor'
import * as dagJson from '@ipld/dag-json'
import SQL from './queries.json' with { type: 'json' }
import { BLOCK_PAGE_PREVIEW, BLOCK_PAGE_RAW_CAP, CLOSURE_FETCH_NOTICE, PIPELINE_SIZE_RUNS, OLDER_SNAPSHOT_PATHS, SELECTIONS_PAGE, SIZE_CONCURRENCY, SNAPSHOT_PATH, STABLE_INDEX_CONCURRENCY, STALE_RUNS_NOTICE } from './config.js'
import { SnapshotError } from './db.js'
import { BlockError } from './blocks.js'
import { cidParts, friendlyOf } from './blockinfo.js'
import { entriesSince } from './storelog.js'
import { typedDecode } from './typed.js'
import { NOT_EQUAL, PredicateError, attrRows, leavesOf, matches, metadataView, predicateRow, rangeRow } from './metadata.js'
import { DELETED, claimState } from './claims.js'
import { decodeValue, membershipsOf, projectState } from './projects.js'
import { samePrincipal } from './identity.js'
import { addressesByPath, rewriteIndex } from './outputindex.js'
import { itemFiles, kindsOf, sumUnique } from './sizes.js'
import { SAMPLE_PATHS } from './table.js'

const EMPTY_PARTIAL = () => ({ files: new Map(), dirs: [], partial: true })
/** One map from several: files first-wins by raw CID, directory leaves once per address. */
function mergeMaps(maps) {
  const files = new Map()
  const dirs = new Map()
  for (const m of maps) {
    for (const [c, f] of m.files) if (!files.has(c)) files.set(c, f)
    for (const d of m.dirs ?? []) if (!dirs.has(d.address)) dirs.set(d.address, d)
  }
  return { files, dirs: [...dirs.values()], partial: maps.some(m => m.partial) }
}
/**
 * This member's snapshot at SNAPSHOT_PATH. Without one the failure is
 * no_snapshot; when the member still has a snapshot an older plugin wrote
 * (OLDER_SNAPSHOT_PATHS), which the page does not read, that error names it
 * (`older`) and says how to rewrite it. `hasFile(url)` answers whether a file
 * exists without opening it.
 */
async function openCurrent(base, openDb, hasFile) {
  try {
    return await openDb(new URL(SNAPSHOT_PATH, base).href)
  } catch (e) {
    if (e?.code !== 'no_snapshot') throw e
    for (const older of OLDER_SNAPSHOT_PATHS) {
      if (!await hasFile(new URL(older, base).href).catch(() => false)) continue
      throw Object.assign(new SnapshotError('no_snapshot', `This store's snapshot (${older}) is older than this explorer, which reads ${SNAPSHOT_PATH}. `
        + 'Run nextflow plugin nf-blocks:snapshot to rewrite it.'), { older })
    }
    throw e
  }
}

const rootOf = (r) => ({ id: r.id, kind: r.kind, assertedBy: r.asserted_by, state: r.state, since: r.since,
  reachable: r.reachable_bytes, blocks: r.reachable_blocks, exclusive: r.exclusive_bytes, pinned: r.pinned_bytes, partial: !!r.partial })

/** A walk stopped because the page that asked for it is gone (Explorer.pageChanged). */
const aborted = () => Object.assign(new Error('the page that asked for this size is gone'), { code: 'aborted' })

/**
 * At most `n` tasks at once, the rest queued in order. A task whose page has
 * gone (`page.gone`) when its turn comes is not run: it rejects with code
 * `aborted`, so a page that is no longer shown stops its walks.
 */
class Limiter {
  constructor(n) { this.n = n; this.running = 0; this.queue = [] }
  run(fn, page) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, page, resolve, reject })
      this.pump()
    })
  }
  pump() {
    while (this.running < this.n && this.queue.length) {
      const { fn, page, resolve, reject } = this.queue.shift()
      if (page?.gone) { reject(aborted()); continue }
      this.running++
      Promise.resolve().then(fn).then(resolve, reject).finally(() => { this.running--; this.pump() })
    }
  }
}

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
/** At most this many rows from one sample search, and addresses from one address search. */
export const SEARCH_SAMPLES = 200
/** At most this many distinct sample IDs from one sample search; the typeahead asks for fewer. */
export const SEARCH_SAMPLE_VALUES = 20
// How many walks searchSamples makes for sample IDs, asking for more each time some it found had no rows to show.
const SEARCH_VALUE_ROUNDS = 4
export const SEARCH_ADDRESSES = 20

const text = (cid) => (cid === null || cid === undefined ? null : cid.toString())
const isoOf = (millis) => new Date(millis).toISOString()

const decoder = new TextDecoder()
/** A Claim value as `claim.value` holds it (Index.valueText): a string as itself, null as itself, anything else as DAG-JSON text. */
const valueText = (value) => (value === null || value === undefined || typeof value === 'string' ? value : decoder.decode(dagJson.encode(value)))

const projectRow = (r) => ({ cid: r.project_cid, blockName: r.name, description: r.description, createdBy: r.created_by,
  createdAt: r.created_at, source: 'snapshot', error: null })
const projectBlockRow = (cid, v) => ({ cid, blockName: v?.name ?? null, description: v?.description ?? null,
  createdBy: v?.asserted_by ?? null, createdAt: v?.created_at ?? null })

export class Explorer {
  constructor({ base, db, blocks, now, sizeConcurrency = SIZE_CONCURRENCY, pipelineSizeRuns = PIPELINE_SIZE_RUNS }) {
    this.base = base
    // The open snapshot file; `db` is how every answer reads it (#query).
    this.snapshot = db
    this.db = { query: (sql, params) => this.#query(sql, params) }
    this.reopening = null
    this.memo = new Map()
    this.blocks = blocks
    this.now = now
    this.watermark = null
    this.snapshotWrittenAt = null
    // Why the snapshot was written lean, with no storage rows (meta storage_omitted: 'over_cap', 'out_of_memory' or 'seeded_lean'), else null.
    this.storageOmitted = null
    // The cas.snapshot.maxBytes the writer had (meta snapshot_max_bytes), when it gives one above 0; else null.
    this.snapshotCapBytes = null
    this.stale = []
    this.tailSelections = []
    this.tailClaims = []
    this.tailTransfers = []
    // P4: Project and Handoff blocks the tail announces that the snapshot has no row for.
    this.tailProjects = []
    this.tailHandoffs = []
    this.logReadable = true
    this.closing = new Map()
    this.fetchesForQuery = 0
    this.runLabels = new Map()
    this.identities = new Map()
    this.sizeCache = new Map()
    this.collectionLabels = new Map()
    // Every size walk's item fetches share one limit; each belongs to the page that started it.
    this.sizeSlots = new Limiter(sizeConcurrency)
    // subjectKinds' block reads: as many at once as a size walk's, in their own queue so a walk never holds them up.
    this.kindSlots = new Limiter(sizeConcurrency)
    this.pipelineSizeRuns = pipelineSizeRuns
    this.page = { gone: false, pending: new Set() }
  }

  /**
   * The page changed (app.js render): size work the last page queued is
   * dropped, and its unfinished cache entries with it, so asking again on the
   * new page starts afresh. Finished sizes stay cached.
   */
  pageChanged() {
    const old = this.page
    old.gone = true
    for (const k of old.pending) this.sizeCache.delete(k)
    this.page = { gone: false, pending: new Set() }
  }

  static async open({ base, openDb, hasFile = async () => false, blocks, listFn, now = () => Date.now(), ...options }) {
    const explorer = new Explorer({ base, db: null, blocks, now, ...options })
    explorer.openSnapshot = () => openCurrent(base, openDb, hasFile)
    explorer.listFn = listFn
    await explorer.load(await explorer.openSnapshot())
    return explorer
  }

  /** Reads `db`'s meta and the tail past its watermark, then answers from `db`. */
  async load(db) {
    this.watermark = (await db.query(SQL.watermark))[0]?.value ?? null
    // When the snapshot was written (IndexSnapshot.WRITTEN_AT_KEY), for the left column's "Snapshot 2 min ago".
    this.snapshotWrittenAt = (await db.query("SELECT value FROM meta WHERE key = 'snapshot_written_at'"))[0]?.value ?? null
    const lean = new Map((await db.query(SQL.storageOmitted)).map(r => [r.key, r.value]))
    this.storageOmitted = lean.get('storage_omitted') || null
    const cap = Number(lean.get('snapshot_max_bytes') ?? NaN)
    this.snapshotCapBytes = Number.isFinite(cap) && cap > 0 ? cap : null
    await this.refreshTail(this.listFn, db)
    this.snapshot = db
  }

  /**
   * One snapshot read. A read that meets a rewritten snapshot (explore's push,
   * refresh or copy into this member) re-opens the member and asks once more;
   * a second rewrite during that retry is the caller's snapshot_changed.
   */
  async #query(sql, params) {
    const db = this.snapshot
    try {
      return await db.query(sql, params)
    } catch (e) {
      if (e?.code !== 'snapshot_changed' || !this.openSnapshot) throw e
      await this.reopen(db)
      return this.snapshot.query(sql, params)
    }
  }

  /**
   * The member's snapshot opened afresh in place of `failed`; every read that
   * met the same rewrite waits on one re-open. One that fails is forgotten,
   * so a later read tries again.
   */
  reopen(failed) {
    if (this.snapshot !== failed) return Promise.resolve()
    if (this.reopening?.failed !== failed) {
      const promise = (async () => {
        const db = await this.openSnapshot()
        try {
          await this.load(db)
        } catch (e) {
          db.close().catch(() => {})
          throw e
        }
        this.memo.clear()
        this.sizeCache.clear()
        failed.close().catch(() => {})
      })()
      promise.catch(() => { if (this.reopening?.promise === promise) this.reopening = null })
      this.reopening = { failed, promise }
    }
    return this.reopening.promise
  }

  get staleCount() { return this.stale.length }

  get notice() { return this.stale.length > STALE_RUNS_NOTICE || this.fetchesForQuery > CLOSURE_FETCH_NOTICE }

  async refreshTail(listFn, db = this.db) {
    const { names, readable } = await listFn(this.base, { watermark: this.watermark, nowMillis: this.now() })
    this.logReadable = readable
    const since = entriesSince(names, this.watermark, this.now())
    const unique = (kind) => [...new Map(since.filter(e => e.kind === kind).map(e => [e.cid, e])).values()]
    const known = async (sql, entries, column) => (entries.length === 0 ? new Set()
      : new Set((await db.query(sql, [JSON.stringify(entries.map(e => e.cid))])).map(r => r[column])))
    const runs = unique('run')
    const selections = unique('selection')
    const claims = unique('claim')
    const transfers = unique('transfer')
    const projects = unique('project')
    const handoffs = unique('handoff')
    const knownRuns = await known(SQL.runsKnown, runs, 'completion_cid')
    const knownSelections = await known(SQL.selectionsKnown, selections, 'collection_cid')
    const knownClaims = await known(SQL.claimsKnown, claims, 'claim_cid')
    const before = new Set(this.stale.map(s => s.cid))
    this.stale = await Promise.all(runs.filter(e => !knownRuns.has(e.cid)).map(e => this.staleRun(e)))
    // A new run changes a pipeline's Stored figure, and may be a run whose size was asked before its blocks arrived.
    if (this.stale.some(s => !before.has(s.cid)))
      for (const k of [...this.sizeCache.keys()]) if (k.startsWith('pipelineSize:') || k.startsWith('runSize:')) this.sizeCache.delete(k)
    this.tailSelections = await Promise.all(selections.filter(e => !knownSelections.has(e.cid)).map(e => this.tailBlock(e, 'Selection')))
    // A Transfer, Project or Handoff entry the snapshot already ingested is read from its table.
    const knownTransfers = await known(SQL.transfersKnown, transfers, 'cid')
    this.tailTransfers = await Promise.all(transfers.filter(e => !knownTransfers.has(e.cid)).map(e => this.tailBlock(e, 'Transfer')))
    const knownProjects = await known(SQL.projectsKnown, projects, 'project_cid')
    this.tailProjects = await Promise.all(projects.filter(e => !knownProjects.has(e.cid)).map(e => this.tailBlock(e, 'Project')))
    const knownHandoffs = await known(SQL.handoffsKnown, handoffs, 'cid')
    this.tailHandoffs = await Promise.all(handoffs.filter(e => !knownHandoffs.has(e.cid)).map(e => this.tailBlock(e, 'Handoff')))
    const claimsBefore = new Set(this.tailClaims.map(t => t.cid))
    this.tailClaims = await Promise.all(claims.filter(e => !knownClaims.has(e.cid)).map(e => this.tailBlock(e, 'Claim')))
    // A new deletion Claim may hide a tail run that a pipeline's Stored figure counted.
    if (this.tailClaims.some(t => !claimsBefore.has(t.cid)))
      for (const k of [...this.sizeCache.keys()]) if (k.startsWith('pipelineSize:')) this.sizeCache.delete(k)
  }

  /**
   * How a root (a run's completion or a Selection) came into this store: the
   * Transfers that name it as a root or as a run they carried, from the
   * snapshot and from the tail, newest first. Empty when there are none.
   */
  async transfersOf(cid) {
    const rows = await this.db.query(SQL.transfersOf, [cid])
    const out = rows.map(r => ({ cid: r.cid, from: r.from_member, to: r.to_member, assertedBy: r.asserted_by, depth: r.depth,
      claims: r.claims ? String(r.claims).split(',') : [], blocks: r.blocks, copied: r.copied, bytes: r.bytes, at: r.at, source: 'snapshot' }))
    const seen = new Set(out.map(t => t.cid))
    for (const t of this.tailTransfers) {
      const v = t.value
      if (!v || seen.has(t.cid)) continue
      if (![...(v.roots ?? []), ...(v.runs ?? [])].some(c => text(c) === cid)) continue
      seen.add(t.cid)
      out.push({ cid: t.cid, from: v.from, to: v.to, assertedBy: v.asserted_by, depth: v.depth, claims: v.claims ?? [],
        blocks: v.blocks, copied: v.copied, bytes: v.bytes, at: v.timestamp, source: 'tail' })
    }
    return out.sort((a, b) => (a.at < b.at ? 1 : a.at > b.at ? -1 : a.cid < b.cid ? -1 : 1))
  }

  /** Tail Transfer entries whose block could not be read. Its roots are unknown, so no run or Selection page can claim it; the Storage view lists them. */
  unreadableTransfers() {
    return this.tailTransfers.filter(t => t.error).map(t => ({ cid: t.cid, error: t.error }))
  }

  // ---- P4: projects, inputs and hand-offs (plan 1.2 to 1.5) ----------------

  /**
   * Every project this member knows, newest created first: the snapshot's
   * rows and the tail's Project blocks, as `{ rows, hiddenCount }`. A row is
   * `{ cid, name, blockName, description, createdBy, createdAt, source,
   * error, state }`: `name` is the current name (projects.js projectState,
   * which `state` is), `blockName` the name at creation. Deleted projects
   * are left out, or with `showDeleted` are the only ones; `includeDeleted`
   * gives every row, for a view that needs both (`state.hidden` tells them
   * apart), with hiddenCount 0.
   */
  async projects({ showDeleted = false, includeDeleted = false } = {}) {
    const snapshot = (await this.db.query(SQL.projects)).map(projectRow)
    const seen = new Set(snapshot.map(r => r.cid))
    const tail = this.tailProjects.filter(t => !seen.has(t.cid)).map(t => ({ ...projectBlockRow(t.cid, t.value), source: 'tail', error: t.error }))
    const rows = [...snapshot, ...tail]
    const claims = await this.claimsFor(rows.map(r => r.cid))
    const all = rows.map(r => {
      const state = projectState({ name: r.blockName }, claims.get(r.cid))
      return { ...r, name: state.name, state }
    }).sort((a, b) => ((a.createdAt ?? '') > (b.createdAt ?? '') ? -1 : (a.createdAt ?? '') < (b.createdAt ?? '') ? 1 : a.cid < b.cid ? -1 : 1))
    const shown = includeDeleted ? all : all.filter(r => (showDeleted ? r.state.deletion === DELETED : !r.state.hidden))
    return { rows: shown, hiddenCount: all.length - shown.length }
  }

  /**
   * One project: its snapshot row, else its tail block, else its block from
   * the store (`source: 'block'`, a Project no entry here announces), with
   * every Claim on it (`claims`) and projectState's fields spread in. A cid
   * that is no Project throws BlockError.
   */
  async project(cid) {
    const [r] = await this.db.query(SQL.projectByCid, [cid])
    let row = r ? projectRow(r) : null
    if (!row) {
      const tail = this.tailProjects.find(t => t.cid === cid && t.value)
      row = tail ? { ...projectBlockRow(cid, tail.value), source: 'tail', error: null }
        : { ...projectBlockRow(cid, (await this.blocks.ofKind(cid, 'Project')).value), source: 'block', error: null }
    }
    const claims = (await this.claimsFor([cid])).get(cid)
    return { ...row, ...projectState({ name: row.blockName }, claims), claims }
  }

  /** The snapshot's project_stats row for this store, `{ reachable, pinned, held, releasing, roots }`, or null (a lean snapshot, or a project it has no row for). */
  async projectStats(cid) {
    const [r] = await this.db.query(SQL.projectStats, [cid])
    return r ? { reachable: r.reachable_bytes, pinned: r.pinned_bytes, held: r.held_bytes, releasing: r.releasing_bytes, roots: r.roots } : null
  }

  /**
   * projectStats for several projects in one read: a Map of project cid to
   * its figures, with no entry for a project the snapshot has no row for.
   */
  async projectStatsOf(cids) {
    const wanted = [...new Set(cids)]
    if (!wanted.length) return new Map()
    return new Map((await this.db.query(SQL.projectStatsOf, [JSON.stringify(wanted)])).map(r => [r.project_cid,
      { reachable: r.reachable_bytes, pinned: r.pinned_bytes, held: r.held_bytes, releasing: r.releasing_bytes, roots: r.roots }]))
  }

  /** The subjects of tail Claims about membership: `add project` naming `project` (any project when null), and every `del project`. */
  tailMembershipSubjects(project = null) {
    return this.tailClaims.filter(t => t.value?.attribute === 'project'
      && (t.value.verb !== 'add' || project === null || decodeValue(valueText(t.value.value)) === project)).map(t => text(t.value.subject))
  }

  /** Current memberships of `subjects` (projects.js membershipsOf) over snapshot and tail Claims. */
  async membershipsOfSubjects(subjects) {
    return membershipsOf(await this.claimsFor(subjects))
  }

  /**
   * The current members of a project: `[{ subject, kind, claim: { cid,
   * asserted_by, timestamp }, hidden }]`, `kind` 'run', 'selection', 'item'
   * or null (subjectKinds). Candidates come from claim_current (by its
   * `(attribute, value)` index; the value of a membership is exactly the
   * link's DAG-JSON) and from the tail, and are then decided from all their
   * Claims, so a tail `del project` removes a snapshot member.
   */
  async projectMembers(cid) {
    const snapshot = (await this.db.query(SQL.projectMemberSubjects, [JSON.stringify({ '/': cid })])).map(r => r.subject_cid)
    const rows = (await this.membershipsOfSubjects([...snapshot, ...this.tailMembershipSubjects(cid)])).filter(m => m.project === cid)
    const kinds = await this.subjectKinds(rows.map(r => r.subject))
    return rows.map(({ project, ...m }) => ({ ...m, kind: kinds.get(m.subject) }))
  }

  /** Every current membership this member knows, `[{ subject, project, claim, hidden }]`: for "Not in a project" and the projects list's counts. */
  async projectMemberships() {
    const snapshot = (await this.db.query(SQL.projectMembershipSubjects)).map(r => r.subject_cid)
    return this.membershipsOfSubjects([...snapshot, ...this.tailMembershipSubjects()])
  }

  /**
   * Every membership Claim about project `cid`, current or superseded, for
   * its Activity: `[{ subject, kind, claim }]` with `claim` the full row. An
   * `add project` naming it, and each `del project` that supersedes one.
   * Candidates are its current members, the subjects with a current `del
   * project` (claim_current's (attribute, value) index, value null), and the
   * tail's; each is then read with all its Claims.
   */
  async projectHistory(cid) {
    const [current, removed] = await Promise.all([this.db.query(SQL.projectMemberSubjects, [JSON.stringify({ '/': cid })]),
      this.db.query(SQL.projectRemovedSubjects)])
    const claims = await this.claimsFor([...current, ...removed].map(r => r.subject_cid).concat(this.tailMembershipSubjects(cid)))
    const out = []
    for (const [subject, list] of claims) {
      const adds = new Set(list.filter(c => c.attribute === 'project' && c.verb === 'add' && decodeValue(c.value) === cid).map(c => c.cid))
      for (const c of list) {
        if (adds.has(c.cid) || (c.attribute === 'project' && c.verb === 'del' && c.supersedes.some(s => adds.has(s)))) out.push({ subject, claim: c })
      }
    }
    const kinds = await this.subjectKinds(out.map(r => r.subject))
    return out.map(r => ({ ...r, kind: kinds.get(r.subject) }))
  }

  /**
   * The output collection each item is published in, a Map of item to
   * collection cid (the first, when several hold it): the snapshot's
   * collection_item rows, then the tail runs' OutputCollection blocks. An
   * item no run here publishes has no entry. A project's item member needs
   * it for its run and its page's route.
   */
  async itemCollections(itemCids) {
    const wanted = [...new Set(itemCids)]
    const out = new Map()
    if (!wanted.length) return out
    for (const r of await this.db.query(SQL.itemCollectionsOf, [JSON.stringify(wanted)])) if (!out.has(r.item_cid)) out.set(r.item_cid, r.collection_cid)
    for (const s of this.stale.filter(s => s.completion)) {
      if (wanted.every(i => out.has(i))) break
      for (const c of s.completion.collections.map(text)) {
        let items
        try {
          items = (await this.blocks.ofKind(c, 'OutputCollection')).value.items.filter(Boolean).map(text)
        } catch (e) {
          if (e instanceof BlockError) continue
          throw e
        }
        for (const i of items) if (!out.has(i) && wanted.includes(i)) out.set(i, c)
      }
    }
    return out
  }

  /** Each subject's current memberships, a Map of subject to `[{ project, claim }]` (empty when none): a run, Selection or item page's project tags. */
  async projectsOf(subjects) {
    const out = new Map([...new Set(subjects)].map(s => [s, []]))
    for (const m of await this.membershipsOfSubjects([...out.keys()])) out.get(m.subject).push({ project: m.project, claim: m.claim })
    return out
  }

  /**
   * What each cid is, as a membership subject: 'run', 'selection' or 'item'
   * from the snapshot or the tail, else from its block's kind when the store
   * has it (read SIZE_CONCURRENCY at a time), else null. A Map in the order
   * asked.
   */
  async subjectKinds(cids) {
    const wanted = [...new Set(cids)]
    const found = new Map(wanted.length ? (await this.db.query(SQL.subjectKinds, [JSON.stringify(wanted)])).map(r => [r.cid, r.kind]) : [])
    for (const s of this.stale) if (s.row) found.set(s.cid, 'run')
    for (const t of this.tailSelections) if (t.value) found.set(t.cid, 'selection')
    const BY_BLOCK = { RunCompletion: 'run', Selection: 'selection', OutputItem: 'item' }
    const unknown = wanted.filter(c => !found.has(c))
    const kinds = await Promise.all(unknown.map(c => this.kindSlots.run(() => this.blocks.get(c)).then(b => BY_BLOCK[b.value?.kind] ?? null,
      (e) => { if (e instanceof BlockError) return null; throw e })))
    unknown.forEach((c, i) => found.set(c, kinds[i]))
    return new Map(wanted.map(c => [c, found.get(c) ?? null]))
  }

  /**
   * The inputs of each run, `[{ completion_cid, role, name, address, size,
   * sample, source }]`: run_input rows for snapshot runs, and the InputSet
   * block for tail runs (`source: 'tail'`). Empty for a run with no InputSet
   * or an unreadable one. Whether the store holds an input's bytes is
   * presence's question, as for leaves (plan 1.3).
   */
  async runInputs(completionCids) {
    const wanted = [...new Set(completionCids)]
    const rows = wanted.length
      ? (await this.db.query(SQL.runInputsOf, [JSON.stringify(wanted)])).map(r => ({ ...r, source: 'snapshot' })) : []
    const have = new Set(rows.map(r => r.completion_cid))
    for (const cid of wanted.filter(c => !have.has(c))) {
      // A snapshot run with no rows has none; a tail run's InputSet block says.
      const set = this.staleRow(cid)?.completion?.input_set
      if (!set) continue
      let value
      try {
        value = (await this.blocks.ofKind(text(set), 'InputSet')).value
      } catch (e) {
        if (e instanceof BlockError) continue
        throw e
      }
      for (const i of value.inputs)
        rows.push({ completion_cid: cid, role: i.role, name: i.name ?? null, address: text(i.address), size: i.size ?? null, sample: i.sample ?? null, source: 'tail' })
    }
    return rows
  }

  /**
   * The inputs that made an item, per run that recorded lineage for it:
   * `[{ completion_cid, source, inputs: [{ position, role, name, address,
   * size, sample, origins: [{ item_cid, collection_cid, selection_cid }] }] }]`,
   * runs by completion cid, inputs by position; `[]` when none did. With
   * `completionCid`, only that run. A snapshot run answers from its
   * item_input, run_input and input_origin rows (no rows, no lineage); a
   * tail run reads its ItemLineage and InputSet blocks, and an unreadable
   * block, a lineage naming another InputSet or a position the InputSet does
   * not have leaves that run out.
   */
  async itemLineage(itemCid, completionCid = null) {
    const runs = []
    const snapshot = (await this.db.query(SQL.itemInputsOf, [itemCid])).filter(r => completionCid === null || r.completion_cid === completionCid)
    for (const r of snapshot) {
      const params = [r.completion_cid, r.positions]
      const origins = Map.groupBy(await this.db.query(SQL.originsAt, params), o => o.position)
      const inputs = (await this.db.query(SQL.runInputsAt, params)).map(i => ({ ...i,
        origins: (origins.get(i.position) ?? []).map(o => ({ item_cid: o.item_cid, collection_cid: o.collection_cid, selection_cid: o.selection_cid })) }))
      runs.push({ completion_cid: r.completion_cid, source: 'snapshot', inputs })
    }
    const have = new Set(runs.map(r => r.completion_cid))
    const tail = this.stale.filter(s => s.row && !have.has(s.cid) && (completionCid === null || s.cid === completionCid))
    for (const s of tail) {
      const lineage = s.completion.item_lineage
      if (!lineage) continue
      try {
        const derived = (await this.blocks.ofKind(text(lineage), 'ItemLineage')).value
        const mine = derived.items.find(d => text(d.item) === itemCid)
        if (!mine || text(derived.input_set) !== text(s.completion.input_set)) continue
        const set = (await this.blocks.ofKind(text(derived.input_set), 'InputSet')).value
        // A position the InputSet does not have leaves the run out rather than failing every run's answer.
        if (!mine.inputs.every(position => set.inputs[position])) continue
        const inputs = mine.inputs.map(position => {
          const i = set.inputs[position]
          return { position, role: i.role, name: i.name ?? null, address: text(i.address), size: i.size ?? null, sample: i.sample ?? null,
            origins: (i.from ?? []).map(o => ({ item_cid: text(o.item), collection_cid: text(o.collection), selection_cid: text(o.selection) })) }
        })
        runs.push({ completion_cid: s.cid, source: 'tail', inputs })
      } catch (e) {
        if (e instanceof BlockError) continue
        throw e
      }
    }
    return runs.sort((a, b) => (a.completion_cid < b.completion_cid ? -1 : a.completion_cid > b.completion_cid ? 1 : 0))
  }

  /**
   * The items derived from an item, `[{ completion_cid, via_cid,
   * selection_cid, item_cid, collection_cid }]`: the consumer run, the
   * collection (`via_cid`) and Selection its fromStore read the item
   * through, and the derived item with the collection holding it. The
   * snapshot's rows only (lineage spec 5.4).
   */
  async usedBy(itemCid) {
    return this.db.query(SQL.usedByOf, [itemCid])
  }

  /** Tail Handoff entries whose block could not be read, `[{ cid, error }]`: handoffs() leaves them out, so the Activity tab can say so. */
  unreadableHandoffs() {
    return this.tailHandoffs.filter(t => t.error).map(t => ({ cid: t.cid, error: t.error }))
  }

  /** The snapshot's task inputs with a sample whose address two or more runs read: what sampleid.js runsSharingInputs groups for suggestions. */
  async sharedInputRows() {
    return this.db.query(SQL.sharedInputRows)
  }

  /**
   * Hand-offs, newest first: `[{ cid, template, selection, project,
   * assertedBy, at, source }]` from the snapshot's handoff rows and the
   * tail's Handoff blocks; only `project`'s when it is given.
   */
  async handoffs({ project = null } = {}) {
    const snapshot = (await this.db.query(SQL.handoffs)).map(r => ({ cid: r.cid, template: r.template,
      selection: r.selection_cid, project: r.project_cid, assertedBy: r.asserted_by, at: r.at, source: 'snapshot' }))
    const seen = new Set(snapshot.map(h => h.cid))
    const tail = this.tailHandoffs.filter(t => t.value && !seen.has(t.cid)).map(t => ({ cid: t.cid, template: t.value.template,
      selection: text(t.value.selection), project: text(t.value.project), assertedBy: t.value.asserted_by, at: t.value.timestamp, source: 'tail' }))
    return [...snapshot, ...tail].filter(h => project === null || h.project === project)
      .sort((a, b) => (a.at < b.at ? 1 : a.at > b.at ? -1 : a.cid < b.cid ? -1 : 1))
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
    const rows = await this.db.query(SQL.claimsOf, [json])
    for (const r of rows) {
      const c = { cid: r.claim_cid, subject: r.subject_cid, verb: r.verb, attribute: r.attribute, value: r.value,
        timestamp: r.timestamp, asserted_by: r.asserted_by, supersedes: [], source: 'snapshot' }
      byCid.set(c.cid, c)
      out.get(c.subject).push(c)
    }
    // No Claim, no supersedes: a subject with nothing in `claim` cannot appear
    // in `claim_supersedes` either, so this second query is skippable.
    if (rows.length > 0)
      for (const s of await this.db.query(SQL.supersedesOf, [json]))
        byCid.get(s.claim_cid)?.supersedes.push(s.superseded_cid)
    for (const t of this.tailClaims.filter(t => t.value)) {
      const subject = text(t.value.subject)
      if (!out.has(subject)) continue
      out.get(subject).push({ cid: t.cid, subject, verb: t.value.verb, attribute: t.value.attribute, value: valueText(t.value.value),
        timestamp: t.value.timestamp, asserted_by: t.value.asserted_by, supersedes: t.value.supersedes.map(text), source: 'tail' })
    }
    return out
  }

  async claimStates(subjects) {
    const claims = await this.claimsFor(subjects)
    return new Map([...claims].map(([subject, list]) => [subject, { ...claimState(list), claims: list }]))
  }

  /**
   * claimStates for the few of `subjects` that a Claim in the Store Log tail
   * names; the rest have no entry, since the snapshot's state for them still
   * holds. One read, and none when the tail names none of them (#/storage's
   * Durability after a pin, release or deletion since the snapshot).
   */
  async tailClaimStates(subjects) {
    const named = new Set(this.tailClaims.filter(t => t.value).map(t => text(t.value.subject)))
    const touched = [...new Set(subjects)].filter(s => named.has(s))
    return touched.length ? this.claimStates(touched) : new Map()
  }

  /** A page of this member's Selections, newest first seen first; the tail's all on the first page. */
  async selectionPage({ offset = 0, limit = SELECTIONS_PAGE, showDeleted = false, by = null } = {}) {
    const snapshot = (await this.db.query(by ? SQL.selectionsPageBy : SQL.selectionsPage, by ? [by, limit, offset] : [limit, offset]))
      .map(r => ({ cid: r.selection_cid, firstSeen: r.written_at, assertedBy: r.asserted_by, source: 'snapshot', error: null }))
    const tail = offset === 0 ? this.tailSelections.filter(t => !by || samePrincipal(t.value?.asserted_by, by)).map(t => ({ cid: t.cid, firstSeen: isoOf(t.entry.writtenAtMillis),
      assertedBy: t.value?.asserted_by ?? null, source: 'tail', error: t.error })) : []
    const rows = [...tail.sort((a, b) => (a.firstSeen > b.firstSeen ? -1 : 1)), ...snapshot]
    const states = await this.claimStates(rows.map(r => r.cid))
    const all = rows.map(r => ({ ...r, state: states.get(r.cid) }))
    const shown = all.filter(r => (showDeleted ? r.state.deletion === DELETED : !r.state.hidden))
    const [{ n }] = await this.db.query(by ? SQL.selectionCountBy : SQL.selectionCount, by ? [by] : [])
    return { rows: shown, hiddenCount: all.length - shown.length,
      ...span({ offset, limit, shown: snapshot.length, count: n, tail: tail.length }) }
  }

  /**
   * The newest `limit` store log entries, snapshot and tail merged:
   * `[{ cid, kind: 'run'|'selection'|'claim'|'transfer', written_at, name?, status?, claim?, transfer? }]`.
   * `name` is a run's name or a Selection's current name; `claim` is
   * `{ verb, attribute, value, subject, assertedBy, timestamp }`; `transfer`
   * is `{ from, to, depth, assertedBy, at }`. Transfers come from the
   * snapshot's transfer table and from readable tail Transfer blocks.
   */
  async recentActivity(limit = 8) {
    // The transfer table is read only for the transfer entries among the newest.
    const snapshot = await this.db.query(
      `SELECT l.cid, l.kind, min(l.written_at) AS written_at, r.run_name, r.status, r.possibly_incomplete,
              k.verb, k.attribute, k.value, k.subject_cid, k.timestamp, k.asserted_by,
              CASE WHEN l.kind = 'transfer' THEN (SELECT json_object('from', t.from_member, 'to', t.to_member, 'depth', t.depth, 'assertedBy', t.asserted_by, 'at', t.at)
                FROM transfer t WHERE t.cid = l.cid) END AS transfer
       FROM log_entry l LEFT JOIN run r ON r.completion_cid = l.cid LEFT JOIN claim k ON k.claim_cid = l.cid
       WHERE l.kind IN ('run', 'selection', 'claim', 'transfer') GROUP BY l.cid, l.kind ORDER BY written_at DESC, l.cid ASC LIMIT ?`, [limit])
    const rows = snapshot.filter(r => r.kind !== 'transfer' || r.transfer).map(r => ({ cid: r.cid, kind: r.kind, written_at: r.written_at, name: r.run_name ?? null,
      status: r.status ? { status: r.status, possibly_incomplete: !!r.possibly_incomplete } : null,
      claim: r.kind === 'claim' ? { verb: r.verb, attribute: r.attribute, value: r.value, subject: r.subject_cid, assertedBy: r.asserted_by, timestamp: r.timestamp } : null,
      ...(r.kind === 'transfer' ? { transfer: JSON.parse(r.transfer) } : {}) }))
    for (const s of this.stale.filter(s => s.row))
      rows.push({ cid: s.cid, kind: 'run', written_at: isoOf(s.entry.writtenAtMillis), name: s.row.run_name ?? null,
        status: { status: s.row.status, possibly_incomplete: !!s.row.possibly_incomplete }, claim: null })
    for (const t of this.tailSelections)
      rows.push({ cid: t.cid, kind: 'selection', written_at: isoOf(t.entry.writtenAtMillis), name: null, status: null, claim: null })
    for (const t of this.tailClaims.filter(t => t.value))
      rows.push({ cid: t.cid, kind: 'claim', written_at: isoOf(t.entry.writtenAtMillis), name: null, status: null,
        claim: { verb: t.value.verb, attribute: t.value.attribute, value: valueText(t.value.value), subject: text(t.value.subject),
          assertedBy: t.value.asserted_by, timestamp: t.value.timestamp } })
    for (const t of this.tailTransfers.filter(t => t.value))
      rows.push({ cid: t.cid, kind: 'transfer', written_at: isoOf(t.entry.writtenAtMillis), name: null, status: null, claim: null,
        transfer: { from: t.value.from, to: t.value.to, depth: t.value.depth, assertedBy: t.value.asserted_by, at: t.value.timestamp } })
    const newest = [...new Map(rows.map(r => [`${r.kind}:${r.cid}`, r])).values()]
      .sort((a, b) => (a.written_at > b.written_at ? -1 : a.written_at < b.written_at ? 1 : a.cid < b.cid ? -1 : 1)).slice(0, limit)
    const selections = newest.filter(r => r.kind === 'selection')
    if (selections.length) {
      const states = await this.claimStates(selections.map(r => r.cid))
      for (const r of selections) r.name = states.get(r.cid).names.join(' / ') || null
    }
    return newest
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
        run_name: manifest.run_name, asserted_by: completion.asserted_by ?? null, nf_run_hash: manifest.nf_run_hash ?? null, status: completion.status,
        possibly_incomplete: completion.possibly_incomplete ? 1 : 0, finished_at: completion.finished_at, source: 'tail' }
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

  /**
   * Per pipeline `{ runs, ok, failed }`, snapshot and tail merged, without a walk over the runs.
   * A run is left out exactly when the pipeline page hides it (claimState): it has one current
   * deletion Claim (no attribute, verb delete or del) and that Claim is a `delete`. A conflicted
   * deletion, or one undone by `del`, stays counted. The grouped query applies the snapshot's
   * Claims; the few runs a tail Claim touches are taken out of it and counted from claimStates,
   * which sees snapshot and tail Claims together.
   */
  async runCounts() {
    const out = new Map()
    const add = (r, n = 1) => {
      const c = out.get(r.pipeline) ?? { runs: 0, ok: 0, failed: 0 }
      c.runs += n
      if (r.status === 'succeeded' && !r.possibly_incomplete) c.ok += n
      else if (r.status === 'failed') c.failed += n
      out.set(r.pipeline, c)
    }
    const touched = [...new Set(this.tailClaims.filter(t => t.value).map(t => text(t.value.subject)))]
    const deletions = `SELECT k.verb FROM claim_current cc JOIN claim k ON k.claim_cid = cc.claim_cid
                       WHERE cc.subject_cid = run.completion_cid AND cc.attribute IS NULL AND k.verb IN ('delete', 'del')`
    const rows = await this.db.query(
      `SELECT pipeline, status, possibly_incomplete, count(*) AS n FROM run
       WHERE completion_cid NOT IN (SELECT value FROM json_each(?))
         AND NOT ((SELECT count(*) FROM (${deletions})) = 1 AND EXISTS (SELECT 1 FROM (${deletions}) WHERE verb = 'delete'))
       GROUP BY pipeline, status, possibly_incomplete`, [JSON.stringify(touched)])
    for (const r of rows) add(r, r.n)
    const own = touched.length === 0 ? [] : await this.db.query(
      'SELECT completion_cid, pipeline, status, possibly_incomplete FROM run WHERE completion_cid IN (SELECT value FROM json_each(?))', [JSON.stringify(touched)])
    const tail = this.stale.filter(s => s.row).map(s => ({ cid: s.cid, ...s.row }))
    const all = [...own.map(r => ({ cid: r.completion_cid, ...r })), ...tail]
    const states = await this.claimStates(all.map(r => r.cid))
    for (const r of all.filter(r => !states.get(r.cid).hidden)) add(r)
    return out
  }

  /** One page of a pipeline's runs (runsOfPipeline) and where it sits: first, last, total, next and prev offsets. */
  async runPage(pipeline, { limit = RUNS_PAGE, offset = 0, by = null } = {}) {
    const rows = await this.runsOfPipeline(pipeline, { limit, offset, by })
    const [{ n }] = await this.db.query(by ? SQL.runCountBy : SQL.runCount, by ? [pipeline, by] : [pipeline])
    const tail = this.tailRunsOf(pipeline, by).length
    const states = await this.claimStates(rows.map(r => r.completion_cid))
    const visible = rows.filter(r => !states.get(r.completion_cid).hidden)
    return { rows: visible, hidden: rows.length - visible.length,
      ...span({ offset, limit, shown: rows.filter(r => r.source === 'snapshot').length, count: n, tail }) }
  }

  async runsOfPipeline(pipeline, { limit = RUNS_PAGE, offset = 0, by = null } = {}) {
    const snapshot = (await this.db.query(by ? SQL.runsOfPipelineBy : SQL.runsOfPipeline, by ? [pipeline, by, limit, offset] : [pipeline, limit, offset]))
      .map(r => ({ ...r, source: 'snapshot' }))
    const tail = offset === 0 ? this.tailRunsOf(pipeline, by) : []
    return [...tail, ...snapshot].sort(byNewest)
  }

  /** The tail's runs of a pipeline, only those signed by `by` when it is given. */
  tailRunsOf(pipeline, by = null) {
    return this.stale.filter(s => s.row?.pipeline === pipeline && (!by || samePrincipal(s.row.asserted_by, by))).map(s => s.row)
  }

  async runRow(cid) {
    const [row] = await this.db.query(SQL.runByCompletion, [cid])
    return row ? { ...row, source: 'snapshot' } : this.staleRow(cid)?.row ?? null
  }

  async completionOf(cid) { return (await this.blocks.ofKind(cid, 'RunCompletion')).value }

  /**
   * A run's name, lineage ID and pipeline from its RunCompletion and
   * RunManifest blocks, with no snapshot query, so the panel and breadcrumbs
   * add nothing to query 3's measured cost (Gate assertion 2; layout B plan P2).
   * One lookup per run; a failure is asked again next time.
   */
  runIdentity(completionCid) {
    if (!this.identities.has(completionCid)) {
      const lookup = (async () => {
        const completion = await this.completionOf(completionCid)
        const manifestCid = text(completion.run)
        const manifest = (await this.blocks.ofKind(manifestCid, 'RunManifest')).value
        return { completion: completionCid, manifest: manifestCid, pipeline: manifest.pipeline ?? null, run_name: manifest.run_name ?? null,
          nf_run_hash: manifest.nf_run_hash ?? null, lid: manifest.nf_run_hash ? `lid://${manifest.nf_run_hash}` : null,
          revision: manifest.revision ?? null, config: manifest.config ?? null,
          status: completion.status, possibly_incomplete: !!completion.possibly_incomplete, finished_at: completion.finished_at ?? null }
      })()
      this.identities.set(completionCid, lookup.catch((e) => { this.identities.delete(completionCid); throw e }))
    }
    return this.identities.get(completionCid)
  }

  async run(cid) {
    const row = await this.runRow(cid)
    if (!row) throw new BlockError('not_found', cid, `no run ${cid} in this member's snapshot or Store Log tail`)
    const completion = await this.completionOf(cid)
    const collections = row.source === 'snapshot'
      ? (await this.db.query(SQL.collectionsOf, [cid])).map(r => ({ output: r.output_name, cid: r.collection_cid }))
      : await Promise.all(completion.collections.map(async (c) =>
        ({ output: (await this.blocks.ofKind(text(c), 'OutputCollection')).value.name, cid: text(c) })))
    // Task 11 (ticket 21 answer 6): the run's own retain/pin state, for the explorer's release/restore/pin controls.
    const state = (await this.claimStates([cid])).get(cid)
    return { row, completion, collections, state }
  }

  /**
   * One page of a collection's items, with first, last, total, next and prev
   * as runPage, and its Output Index File (Task 4, DESIGN.md §15) if the
   * OutputCollection block carries one: `{ leaf, path } | null`. The snapshot's
   * `collection` table has no index column, so the block is fetched even for
   * a snapshot row; a tail row reuses the block it already decoded for items.
   */
  async collection(cid, { limit = ITEMS_PAGE, offset = 0 } = {}) {
    // Task 11: the collection's own retain/pin state, read once for either branch below.
    const state = (await this.claimStates([cid])).get(cid)
    const [row] = await this.db.query(SQL.collectionByCid, [cid])
    if (row) {
      const items = (await this.db.query(SQL.collectionItems, [cid, limit, offset])).map(r => r.item_cid)
      const [{ n }] = await this.db.query(SQL.collectionItemCount, [cid])
      const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
      return { cid, output: row.output_name, completion: row.completion_cid, items, index: block.index ?? null, state,
        ...span({ offset, limit, shown: items.length, count: n }) }
    }
    const block = (await this.blocks.ofKind(cid, 'OutputCollection')).value
    const completion = this.stale.find(s => s.completion?.collections.some(c => text(c) === cid))?.cid ?? null
    const all = block.items.filter(Boolean).map(text)
    const items = all.slice(offset, offset + limit)
    return { cid, output: block.name, completion, items, index: block.index ?? null, state,
      ...span({ offset, limit, shown: items.length, count: all.length }) }
  }

  /**
   * The collection's Output Index File with each Publish Coordinate replaced
   * by the address fromStore emits for that leaf (outputindex.js):
   * `{ name, text, replaced, unmatched }`. Reads every item of the
   * collection; an item this member does not hold leaves its paths unmatched.
   */
  async stableIndex(collectionCid) {
    const collection = (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value
    const leaf = collection.index?.leaf
    if (!leaf?.address) throw new BlockError('not_found', collectionCid, `collection ${collectionCid} has no Output Index File`)
    const bytes = await this.blocks.raw(text(leaf.address))
    const values = new Map()
    const queue = collection.items.filter(Boolean).map(text)
    const worker = async () => {
      for (let c = queue.shift(); c !== undefined; c = queue.shift()) {
        try {
          values.set(c, (await this.blocks.ofKind(c, 'OutputItem')).value.value)
        } catch (e) {
          if (!(e instanceof BlockError)) throw e
        }
      }
    }
    await Promise.all(Array.from({ length: STABLE_INDEX_CONCURRENCY }, worker))
    const rewritten = rewriteIndex(new TextDecoder().decode(bytes), addressesByPath(collection, values))
    return { name: leaf.name, ...rewritten }
  }

  /**
   * One block for the block page (visual language spec §4): its CID
   * decoded, the check the browser made, and its value or bytes. A raw block
   * over `rawCap` is previewed by Range and left unchecked; a structured
   * block that fails the schema is still decoded, with the reason. A
   * missing or refused block is reported, not thrown.
   */
  async block(cidText, { rawCap = BLOCK_PAGE_RAW_CAP, previewBytes = BLOCK_PAGE_PREVIEW } = {}) {
    const parts = cidParts(cidText)
    const out = { cid: cidText, parts, check: null, size: null, value: null, schemaError: null, bytes: null, preview: null, friendly: null, message: null }
    try {
      if (parts.codec === 'raw') {
        const head = await this.blocks.partial(cidText, previewBytes)
        out.size = head.size
        if (head.size !== null && head.size <= rawCap) {
          out.bytes = await this.blocks.raw(cidText)
          out.size = out.bytes.length
          out.check = 'verified'
        } else {
          out.preview = head.bytes
          out.check = 'unchecked'
        }
        out.friendly = { kind: 'file', text: "a file's bytes", label: 'Who produced it', href: `#/content/${cidText}` }
      } else {
        try {
          const b = await this.blocks.get(cidText)
          out.bytes = b.bytes
          out.value = b.value
        } catch (e) {
          if (!(e instanceof BlockError) || e.code !== 'schema_invalid') throw e
          out.bytes = await this.blocks.raw(cidText)
          out.schemaError = e.message
          try { out.value = dagCbor.decode(out.bytes) } catch { out.value = null }
        }
        out.size = out.bytes.length
        out.check = 'verified'
        out.friendly = friendlyOf(cidText, out.value, this.stale)
      }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      if (e.code === 'hash_mismatch') Object.assign(out, { check: 'refused', bytes: null, value: null, preview: null, message: e.message })
      else if (e.code === 'block_missing') Object.assign(out, { check: 'missing', message: e.message })
      else throw e
    }
    return out
  }

  /**
   * An item from its OutputItem block alone, no snapshot query: `{ collection,
   * cid, value, view, leaves }`. `view` is read from the typed decoding, so its
   * pairs type floats as the index does (metadata.js). Previews and names use
   * this; only the item page needs the claim state (`item`).
   */
  async itemBlock(collectionCid, itemCid) {
    const block = await this.blocks.ofKind(itemCid, 'OutputItem')
    const { value } = block.value
    return { collection: collectionCid, cid: itemCid, value, view: metadataView(typedDecode(block.bytes).value), leaves: leavesOf(value) }
  }

  /** `itemBlock` with the item's own retain/pin state (Task 11), a snapshot read: for the item page. */
  async item(collectionCid, itemCid) {
    const it = await this.itemBlock(collectionCid, itemCid)
    const state = (await this.claimStates([itemCid])).get(itemCid)
    return { ...it, state }
  }

  /**
   * A collection's output name and its run's name and pipeline from its
   * OutputCollection and RunManifest blocks, no snapshot query: `{ output,
   * run_name, pipeline, manifest }`. The block links the RunManifest, not the
   * RunCompletion, so the run's own page is not known from here. One lookup
   * per collection; a failure is asked again next time.
   */
  collectionLabel(collectionCid) {
    if (!this.collectionLabels.has(collectionCid)) {
      const lookup = (async () => {
        const block = (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value
        const manifestCid = text(block.run)
        let manifest = null
        try { manifest = (await this.blocks.ofKind(manifestCid, 'RunManifest')).value } catch (e) { if (!(e instanceof BlockError)) throw e }
        return { output: block.name, run_name: manifest?.run_name ?? null, pipeline: manifest?.pipeline ?? null, manifest: manifestCid }
      })()
      this.collectionLabels.set(collectionCid, lookup.catch((e) => { this.collectionLabels.delete(collectionCid); throw e }))
    }
    return this.collectionLabels.get(collectionCid)
  }

  /**
   * Every item CID of a collection, for "Add all N to the tray" (DESIGN.md
   * §16 milestone 3 decision 12): the snapshot's rows, or a tail collection's block. No
   * OutputItem is fetched.
   */
  async allItems(collectionCid) {
    const [row] = await this.db.query(SQL.collectionByCid, [collectionCid])
    if (row) return (await this.db.query(SQL.collectionAllItems, [collectionCid])).map(r => r.item_cid)
    return (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value.items.filter(Boolean).map(text)
  }

  /** How many items a collection holds: the snapshot's count, else its OutputCollection block's (a tail collection), else null. Reads no OutputItem. */
  async collectionItemCount(cid) {
    const [row] = await this.db.query(SQL.collectionByCid, [cid])
    if (row) return (await this.db.query(SQL.collectionItemCount, [cid]))[0].n
    return this.blocks.ofKind(cid, 'OutputCollection').then(
      (b) => b.value.items.filter(Boolean).length,
      (e) => { if (e instanceof BlockError) return null; throw e })
  }

  // ---- storage (P2: the snapshot's per-store figures) ------------------------
  // Every figure belongs to this member's store. A lean snapshot
  // (storageOmitted) has none: storeStats and rootStorage are null there, the
  // lists are empty, and the sizes walk as P0's did.

  /** Whether the snapshot opened has storage figures: it was not written lean. */
  get storage() { return !this.storageOmitted }

  /**
   * What the Store Log tail holds that the snapshot's storage figures do not
   * count: `{ claims, logged }`. `claims` is a pin or unpin, a release or
   * restore (retain), or a deletion or its undoing, written since the
   * snapshot. After a write the page refreshes only the tail, so root_stats,
   * root_shared and pinned_overlap keep the snapshot's figures while the
   * page's Claim state has moved on. `logged` is a run or Selection logged
   * since: a new content root, which may share files that the snapshot
   * counts as one root's alone.
   */
  storageChanges() {
    const claims = this.tailClaims.some(({ value: c }) => !!c && (c.attribute === 'pin' || c.attribute === 'retain'
      || ((c.attribute === null || c.attribute === undefined) && (c.verb === 'delete' || c.verb === 'del'))))
    return { claims, logged: this.stale.length > 0 || this.tailSelections.length > 0 }
  }

  /**
   * Whether the snapshot's storage figures are out of date (storageChanges).
   * What a root holds alone depends on every other root in the store, so one
   * such Claim, run or Selection makes every Release, Unpin and store figure
   * on the page stale until the next snapshot.
   */
  storageStale() {
    const { claims, logged } = this.storageChanges()
    return claims || logged
  }

  /** One snapshot read per Explorer for `key`, since the snapshot never changes; a failure is asked again next time. */
  #once(key, read) {
    if (!this.memo.has(key)) this.memo.set(key, read().catch((e) => { this.memo.delete(key); throw e }))
    return this.memo.get(key)
  }

  /**
   * The store's figures: `{ stored, storedBlocks, referenced, dedup, pinned,
   * held, releasing, shared, partial, graceMs }`, or null on a lean snapshot. `dedup` is
   * referenced over stored, null when nothing is stored. `graceMs` is the
   * writer's cas.sweep.grace, null when it did not know it (R7).
   */
  storeStats() {
    if (!this.storage) return Promise.resolve(null)
    return this.#once('storeStats', async () => {
      const [row = {}] = await this.db.query(SQL.storeStats)
      const grace = await this.sweepGraceMs()
      const n = (v) => Number(v ?? 0)
      const stored = n(row.stored_bytes)
      const referenced = n(row.referenced_bytes)
      return { stored, storedBlocks: n(row.stored_blocks), referenced, dedup: stored > 0 ? referenced / stored : null,
        pinned: n(row.pinned_bytes), held: n(row.held_bytes), releasing: n(row.releasing_bytes), shared: n(row.shared_bytes),
        partial: !!row.partial, graceMs: grace }
    })
  }

  /**
   * The writer's cas.sweep.grace in ms (meta sweep_grace_ms, R7), or null when
   * it did not know it. Read on its own, so a lean snapshot, which has no
   * storeStats, still gives Release its earliest date.
   */
  sweepGraceMs() {
    return this.#once('sweepGraceMs', async () => {
      const value = (await this.db.query(SQL.snapshotGrace))[0]?.value ?? null
      const ms = value === null ? NaN : Number(value)
      return Number.isFinite(ms) ? ms : null
    })
  }

  /** Per asserter (normalised, 'anonymous' for none): `[{ assertedBy, reachable, exclusive, pinned, releasing }]`, most reachable first. */
  async asserterStats() {
    if (!this.storage) return []
    return (await this.db.query(SQL.asserterStats)).map(r => ({ assertedBy: r.asserted_by, reachable: r.reachable_bytes,
      exclusive: r.exclusive_bytes, pinned: r.pinned_bytes, releasing: r.releasing_bytes }))
  }

  /**
   * Every run root in the snapshot, newest run first: `[{ id, cid, name,
   * pipeline, status, finishedAt, state, since, reachable, blocks, exclusive,
   * pinned, partial }]`. `state` is held, released or deleted (R3); a
   * delete-hidden run is listed, for the view to leave out. Tail runs are not
   * here: their figures come with the next snapshot.
   */
  async storageRuns() {
    if (!this.storage) return []
    return (await this.db.query(SQL.storageRuns)).map(r => ({ id: r.id, cid: r.cid, name: r.run_name, pipeline: r.pipeline,
      status: r.status, finishedAt: r.finished_at, state: r.state, since: r.since, reachable: r.reachable_bytes,
      blocks: r.reachable_blocks, exclusive: r.exclusive_bytes, pinned: r.pinned_bytes, partial: !!r.partial }))
  }

  /**
   * Every root whose subject is `cid` (its run or Selection root, and one pin
   * root per asserter): `{ roots: [{ id, kind, assertedBy, state, since,
   * reachable, blocks, exclusive, pinned, partial }], shared: [{ cid, kind,
   * name, href, assertedBy, bytes }] }`, or null on a lean snapshot or when it has no
   * root for it (a tail run). `shared` is the run or Selection root's
   * root_shared rows, most bytes first, each named by its run's name, its
   * Selection's current name, "hand-off to <template>" (linked to its Selection) or its pinner;
   * null when a run, Selection or hand-off has no name (the view says "an unnamed ...").
   */
  async rootStorage(cid) {
    if (!this.storage) return null
    const roots = (await this.db.query(SQL.rootsOf, [cid])).map(rootOf)
    if (roots.length === 0) return null
    const content = roots.find(r => r.kind === 'run' || r.kind === 'selection')
    const rows = content ? await this.db.query(SQL.rootShared, [content.id]) : []
    const selections = rows.filter(r => r.kind === 'selection').map(r => r.cid)
    const states = selections.length ? await this.claimStates(selections) : new Map()
    const nameOf = (r) => (r.kind === 'run' ? r.run_name ?? null
      : r.kind === 'selection' ? states.get(r.cid).names.join(' / ') || null
      : r.kind === 'handoff' ? (r.template ? `hand-off to ${r.template}` : null) : r.asserted_by ?? null)
    // A hand-off has no page of its own, so it links to the Selection it handed over.
    const hrefOf = (r) => (r.kind === 'handoff' ? (r.selection_cid ? `#/selection/${r.selection_cid}` : null)
      : r.kind === 'run' || r.kind === 'selection' ? `#/${r.kind}/${r.cid}` : null)
    return { roots, shared: rows.map(r => ({ cid: r.cid, kind: r.kind, name: nameOf(r), href: hrefOf(r), assertedBy: r.asserted_by, bytes: r.bytes })) }
  }

  /** The bytes of `subjectCid` each asserter's pins keep here: `[{ assertedBy, bytes }]`, most first; empty on a lean snapshot. */
  async pinnedBy(subjectCid) {
    if (!this.storage) return []
    return (await this.db.query(SQL.pinnedOverlapOf, [subjectCid])).map(r => ({ assertedBy: r.asserted_by, bytes: r.bytes }))
  }

  /**
   * Every pin root, newest first: `[{ id, cid, subjectKind, name, collection,
   * assertedBy, since, reachable, exclusive }]`. `cid` is the pinned subject
   * and `subjectKind` what it is: 'run', 'selection', 'collection', 'item' or
   * 'block'. `name` is a run's name or a Selection's current names joined
   * with ' / ' (one claimStates read, only when a Selection is pinned), else
   * null. `collection` is a collection holding a pinned item (its page's
   * route needs one), else null. `assertedBy` is the pinner as the Claim gave
   * it, `exclusive` what only this pin keeps. Empty on a lean snapshot.
   */
  async pinRoots() {
    if (!this.storage) return []
    const rows = await this.db.query(SQL.pinRoots)
    const kindOf = (r) => (r.run_cid ? 'run' : r.collection_cid ? (r.collection_kind === 'selection' ? 'selection' : 'collection')
      : r.item_cid ? 'item' : 'block')
    const selections = [...new Set(rows.filter(r => kindOf(r) === 'selection').map(r => r.cid))]
    const states = selections.length ? await this.claimStates(selections) : new Map()
    return rows.map((r) => {
      const subjectKind = kindOf(r)
      const name = subjectKind === 'run' ? r.run_name ?? null
        : subjectKind === 'selection' ? states.get(r.cid)?.names.join(' / ') || null : null
      return { id: r.id, cid: r.cid, subjectKind, name, collection: subjectKind === 'item' ? r.item_collection ?? null : null,
        assertedBy: r.asserted_by, since: r.since, reachable: r.reachable_bytes, exclusive: r.exclusive_bytes }
    })
  }

  /** Stored files by extension: `[{ kind, files, bytes }]`, most bytes first; empty on a lean snapshot. */
  async kindStats() {
    if (!this.storage) return []
    return (await this.db.query(SQL.kindStats)).map(r => ({ kind: r.kind, files: r.files, bytes: r.bytes }))
  }

  /** The subject's root of `kind`, when its size can come from the index: the snapshot has storage figures and it is no tail run. */
  async #indexRoot(cid, kind, page) {
    if (!this.storage || this.staleRow(cid)) return null
    if (page.gone) throw aborted()
    return (await this.db.query(SQL.rootsOf, [cid])).map(rootOf).find(r => r.kind === kind) ?? null
  }

  /**
   * A collection's size from collection_size, or null when the snapshot has no
   * figure for it. `kinds` is null: the chips come from the walk
   * (collectionKinds), asked for separately so the bytes do not wait on it.
   * The table has no partial column, so it is the store's.
   */
  async #indexedCollection(cid, page) {
    if (!this.storage) return null
    if (page.gone) throw aborted()
    const [row] = await this.db.query(SQL.collectionSize, [cid])
    if (!row) return null
    if (page.gone) throw aborted()
    const [{ n }] = await this.db.query(SQL.collectionItemCount, [cid])
    const { partial } = await this.storeStats()
    return { bytes: row.bytes, blocks: row.blocks, items: n, kinds: null, partial }
  }

  /**
   * An item's size from item_size, one primary-key read: `{ bytes, blocks,
   * partial }`, or null on a lean snapshot or with no row for it (a tail
   * item). The bytes and files stored here: a file or directory a sweep took
   * is left out. For the item page; the grids keep itemSize's walk, which
   * also gives their chips. `partial` is the store's.
   */
  async itemStoredSize(itemCid) {
    if (!this.storage) return null
    const [row] = await this.db.query(SQL.itemSize, [itemCid])
    if (!row) return null
    const { partial } = await this.storeStats()
    return { bytes: row.bytes, blocks: row.blocks, partial }
  }

  // ---- sizes -----------------------------------------------------------------
  // With storage figures, a run, Selection, collection or pipeline the
  // snapshot holds is sized from the index, and so is an item on its own page
  // (itemStoredSize). A tail run, a lean snapshot, a collection's kind chips
  // and itemSize walk blocks only: itemSize is asked per row of the grids,
  // which read the item's block anyway. Item fetches share `sizeSlots`, and each
  // belongs to the page that started it, so a page that is gone stops its own.
  // A BlockError anywhere in a walk (block_missing, hash_mismatch,
  // schema_invalid) makes the total partial; it is never thrown.

  /** One cached lookup per (kind, key) for `page`; concurrent callers share the promise and a rejection is dropped so a retry can work. */
  #sized(kind, key, compute, page = this.page) {
    const k = `${kind}:${key}`
    // A page that is gone starts nothing and caches nothing: an entry bound to it would reject the next page's walk.
    if (page.gone) return Promise.reject(aborted())
    if (!this.sizeCache.has(k)) {
      const p = compute(page).catch((e) => { if (this.sizeCache.get(k) === p) this.sizeCache.delete(k); throw e })
      this.sizeCache.set(k, p)
      page.pending.add(k)
      p.then(() => page.pending.delete(k), () => page.pending.delete(k))
    }
    return this.sizeCache.get(k)
  }

  #itemMap(itemCid, page) {
    return this.#sized('item', itemCid, (pg) => this.sizeSlots.run(async () => {
      try {
        const { value } = (await this.blocks.ofKind(itemCid, 'OutputItem')).value
        return await itemFiles(value, (cid, kind) => this.blocks.ofKind(cid, kind))
      } catch (e) {
        if (e instanceof BlockError) return EMPTY_PARTIAL()
        throw e
      }
    }, pg), page)
  }

  /** `{ files, dirs, partial }` of one OutputItem block, cached. A block this member lacks or refuses gives an empty, partial map. */
  itemFileMap(itemCid) { return this.#itemMap(itemCid, this.page) }

  #collectionMap(collectionCid, page) {
    return this.#sized('collection', collectionCid, async (pg) => {
      let items
      try { items = (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value.items.filter(Boolean).map(text) } catch (e) {
        if (e instanceof BlockError) return { ...EMPTY_PARTIAL(), items: 0 }
        throw e
      }
      // A few workers pull items in turn, so a large collection never floods the shared queue.
      const maps = new Array(items.length)
      let next = 0
      // A worker stops once its page is gone, before it takes the next item.
      const worker = async () => {
        while (next < items.length) {
          if (pg.gone) throw aborted()
          const i = next++
          maps[i] = await this.#itemMap(items[i], pg)
        }
      }
      await Promise.all(Array.from({ length: Math.min(this.sizeSlots.n, items.length) }, worker))
      return { ...mergeMaps(maps), items: items.length }
    }, page)
  }

  /** `{ files, dirs, partial, items }` over a collection's items (from its OutputCollection block), cached. */
  collectionFileMap(collectionCid) { return this.#collectionMap(collectionCid, this.page) }

  /** An item's size from its leaves and directory manifests: `{ bytes, blocks, files, kinds, partial }`. */
  itemSize(itemCid) {
    return this.#sized('itemSize', itemCid, async (pg) => {
      const m = await this.#itemMap(itemCid, pg)
      return { ...sumUnique([m]), files: m.files.size, kinds: kindsOf(m.files, m.dirs) }
    })
  }

  /**
   * A collection's size over its unique files: `{ bytes, blocks, items, kinds,
   * partial }`. From the index when it has a row, with `kinds` null until
   * collectionKinds walks; otherwise the walk, with its kinds.
   */
  collectionSize(collectionCid) {
    return this.#sized('collectionSize', collectionCid, async (pg) => {
      const indexed = await this.#indexedCollection(collectionCid, pg)
      if (indexed) return indexed
      const m = await this.#collectionMap(collectionCid, pg)
      return { ...sumUnique([m]), items: m.items, kinds: kindsOf(m.files, m.dirs) }
    })
  }

  /** A collection's kind chips (`[[ext, count, bytes]]`) by P0's walk, cached with it: for an index-backed collectionSize. */
  collectionKinds(collectionCid) {
    return this.#sized('collectionKinds', collectionCid, async (pg) => {
      const m = await this.#collectionMap(collectionCid, pg)
      return kindsOf(m.files, m.dirs)
    })
  }

  async #runMaps(completionCid, page) {
    let completion
    try { completion = await this.completionOf(completionCid) } catch (e) {
      if (e instanceof BlockError) return [EMPTY_PARTIAL()]
      throw e
    }
    return Promise.all(completion.collections.map(c => this.#collectionMap(text(c), page)))
  }

  /** A run's size, unique over every collection it published: `{ bytes, blocks, partial }`, its run root's reachable figures when the index has them. */
  runSize(completionCid) {
    return this.#sized('runSize', completionCid, async (pg) => {
      const root = await this.#indexRoot(completionCid, 'run', pg)
      if (root) return { bytes: root.reachable, blocks: root.blocks, partial: root.partial }
      return sumUnique(await this.#runMaps(completionCid, pg))
    })
  }

  /**
   * A pipeline's size. With storage figures, the snapshot's pipeline_stats row (R5: held
   * runs, and released or deleted runs that are pinned; every pipeline in one
   * query, read once): `{ bytes, blocks, partial, runs, more: false,
   * releasing, source: 'snapshot', newer }`, partial when the store's figures
   * are. The pipeline's runs newer than the snapshot, less those a delete
   * Claim hides, are walked and added (`newer` of them), unique among
   * themselves only: a tail run may hold files the snapshot's runs already
   * count, which the walk cannot know, so the figure is then an upper bound
   * (`upTo`, "up to"). `partial` stays the snapshot's own. On a lean snapshot,
   * P0's walk, unique over its newest `pipelineSizeRuns` runs (runs hidden by
   * a delete Claim included: their blocks are still stored): `{ bytes,
   * blocks, partial, runs, more }`. With more runs than that, `more` is set
   * and the total is partial ("at least").
   */
  pipelineSize(pipeline) {
    return this.#sized('pipelineSize', pipeline, async (pg) => {
      if (this.storage) {
        if (pg.gone) throw aborted()
        const row = (await this.#once('pipelineStats', async () =>
          new Map((await this.db.query(SQL.pipelineStats)).map(r => [r.pipeline, r])))).get(pipeline)
        const tail = this.stale.filter(s => s.row?.pipeline === pipeline).map(s => s.cid)
        const states = tail.length ? await this.claimStates(tail) : new Map()
        const newer = tail.filter(cid => !states.get(cid)?.hidden)
        const added = newer.length ? sumUnique((await Promise.all(newer.map(cid => this.#runMaps(cid, pg)))).flat()) : { bytes: 0, blocks: 0 }
        return { bytes: (row?.bytes ?? 0) + added.bytes, blocks: (row?.blocks ?? 0) + added.blocks, partial: !!row?.partial, upTo: newer.length > 0,
          runs: (row?.runs ?? 0) + newer.length, more: false, releasing: row?.releasing_bytes ?? 0, source: 'snapshot', newer: newer.length }
      }
      const n = this.pipelineSizeRuns
      const rows = await this.runsOfPipeline(pipeline, { limit: n + 1, offset: 0 })
      const newest = rows.slice(0, n)
      const more = rows.length > n
      const sum = sumUnique((await Promise.all(newest.map(r => this.#runMaps(r.completion_cid, pg)))).flat())
      return { ...sum, partial: sum.partial || more, runs: newest.length, more }
    })
  }

  /** A Selection's size over the items it holds, following nested Selections: `{ bytes, blocks, files, partial }`. */
  selectionSize(selectionCid) {
    return this.#sized('selectionSize', selectionCid, async (pg) => {
      const root = await this.#indexRoot(selectionCid, 'selection', pg)
      if (root) return { bytes: root.reachable, blocks: root.blocks, files: root.blocks, partial: root.partial }
      const items = new Set()
      const seen = new Set()
      let missing = false
      const visit = async (cid) => {
        if (seen.has(cid)) return
        seen.add(cid)
        let block
        try { block = (await this.blocks.ofKind(cid, 'Selection')).value } catch (e) {
          // The Selection asked about must be readable; a nested one that is not leaves the total partial.
          if (e instanceof BlockError && cid !== selectionCid) { missing = true; return }
          throw e
        }
        for (const m of block.members) {
          if (m.item) items.add(text(m.item.address))
          else await visit(text(m.selection))
        }
      }
      await visit(selectionCid)
      const maps = await Promise.all([...items].map(i => this.#itemMap(i, pg)))
      const sum = sumUnique(maps)
      return { ...sum, partial: sum.partial || missing, files: sum.blocks }
    })
  }

  /**
   * The run a collection came from, named (DESIGN.md §16 milestone 3 decision 11): one
   * lookup per collection however many rows ask. A collection neither the
   * snapshot nor the tail knows (held in another member) is null, and is
   * asked again next time, since a tail refresh may find it.
   */
  runLabel(collectionCid) {
    if (!collectionCid) return Promise.resolve(null)
    if (!this.runLabels.has(collectionCid)) {
      this.runLabels.set(collectionCid, this.findRunLabel(collectionCid).then(
        (label) => { if (!label) this.runLabels.delete(collectionCid); return label },
        (e) => { this.runLabels.delete(collectionCid); throw e }))
    }
    return this.runLabels.get(collectionCid)
  }

  async findRunLabel(collectionCid) {
    const [row] = await this.db.query(SQL.collectionByCid, [collectionCid])
    let completion = row?.completion_cid ?? null
    let output = row?.output_name ?? null
    if (!row) {
      const stale = this.stale.find(s => s.completion?.collections.some(c => text(c) === collectionCid))
      if (!stale) return null
      completion = stale.cid
      try {
        output = (await this.blocks.ofKind(collectionCid, 'OutputCollection')).value.name
      } catch (e) {
        if (!(e instanceof BlockError)) throw e
        return null
      }
    }
    if (!completion) return null
    const run = await this.runRow(completion)
    return { run_name: run?.run_name ?? null, output, completion, status: run?.status ?? null, possibly_incomplete: run ? !!run.possibly_incomplete : null, finished_at: run?.finished_at ?? null }
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
   * Query 2 over the snapshot and the tail. With no tail Claim at all, the
   * SQL's answer stands and the snapshot is read for at most one more row (as
   * before this task): a run the tail's Claims might hide cannot exist.
   * Once the tail holds any Claim, a snapshot run's exclusion may be stale
   * (a delete Claim written after the snapshot, or a tail `del` undoing a
   * snapshot delete), so the snapshot's successful runs are read a page at a
   * time, newest first (size 1, then 50, then 50, ...), each page's Claims
   * checked until one row's current deletion group holds no `delete`
   * (decision 13: a conflicted deletion still excludes the run; `hidden`
   * stays for the run lists, which do want a conflicted deletion shown, with
   * its ambiguity, rather than hidden) or a page comes back short. That
   * candidate, if any, is compared against the visible stale candidates and
   * the newest of the two wins, so a tail deletion of more than 50 runs
   * cannot hide an older live one behind it.
   */
  async latestSuccessfulRun(pipeline) {
    const stale = this.stale.filter(s => s.row && s.row.pipeline === pipeline && s.row.status === 'succeeded' && s.row.possibly_incomplete === 0).map(s => s.row)
    if (this.tailClaims.length === 0) {
      const [best] = await this.db.query(SQL.latestSuccessfulRun, [pipeline])
      if (stale.length === 0) return best?.completion_cid ?? null
      const candidates = stale.slice()
      if (best) candidates.push(await this.runRow(best.completion_cid))
      return candidates.sort(byNewest)[0].completion_cid
    }
    const excluded = (s) => s.deletionClaims.some(cid => s.claims.find(c => c.cid === cid)?.verb === 'delete')
    const staleStates = await this.claimStates(stale.map(r => r.completion_cid))
    const visibleStale = stale.filter(r => !excluded(staleStates.get(r.completion_cid)))
    let visible = null
    for (let offset = 0, limit = 1; ; offset += limit, limit = RUNS_PAGE) {
      const page = await this.db.query(SQL.successfulRunsPage, [pipeline, limit, offset])
      if (page.length > 0) {
        const states = await this.claimStates(page.map(r => r.completion_cid))
        const found = page.find(r => !excluded(states.get(r.completion_cid)))
        if (found) { visible = found; break }
      }
      if (page.length < limit) break
    }
    const candidates = visible ? [...visibleStale, visible] : visibleStale
    return candidates.sort(byNewest)[0]?.completion_cid ?? null
  }

  /**
   * Query 3 for one run: the index's SQL for a snapshot run, the closure for a
   * stale one. Which one is known from memory, so the snapshot is read only by
   * the query itself (Gate assertion 2 counts every page). Returns the items
   * and the collection they came from.
   */
  async items(completionCid, output, predicates, onProgress) {
    this.fetchesForQuery = 0
    const rows = predicates.map(([path, type, value, op]) => (op === NOT_EQUAL ? { ...predicateRow(path, type, value), op }
      : op ? rangeRow(path, op, value) : predicateRow(path, type, value)))
    const long = rows.find(r => r.truncated && r.op === NOT_EQUAL)
    if (long) throw new PredicateError(`the value for ${long.path} != is too long to compare: values that long are stored as digests`)
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
      if (r.op === NOT_EQUAL) { sql += SQL.itemsNotEqual; params.push(r.path, r.type, r.value); continue }
      if (r.op) { sql += SQL.itemsRange[r.op]; params.push(r.path, r.value); continue }
      params.push(r.path, r.type)
      if (r.value === null) sql += SQL.itemsPredicateNull
      else { sql += SQL.itemsPredicate; params.push(r.value) }
    }
    const found = await this.db.query(sql + SQL.itemsOrder, params)
    return { items: found.map(r => r.item_cid), collection: found[0]?.collection_cid ?? null }
  }

  // ---- search v1 (P1a Task 4): sample IDs and addresses, snapshot only ------
  // Both are ranges on an existing index (prefixRange), never LIKE, so `%`
  // and `_` in the query are plain characters. Names (runs, outputs, files,
  // Selections) have no index until P2's schema bump.

  /**
   * Items whose sample ID (a string at one of SAMPLE_PATHS, not truncated)
   * starts with `prefix`, in output collections of snapshot runs, latest run
   * first: `[{ sample, path, item_cid, collection_cid, output_name,
   * completion_cid, run_name, pipeline, status, possibly_incomplete,
   * finished_at, paths }]`. One row per item in a collection: an item with
   * its ID at `id` and `meta.id` keeps the path SAMPLE_PATHS names first as
   * `path`, and every matched path in `paths`. Runs a current delete Claim
   * hides are left out, as runPage leaves them out.
   *
   * Two bounded queries: searchSampleValues finds distinct IDs in the
   * prefix range from the covering index alone (DISTINCT streams, so LIMIT
   * stops the walk), then searchSampleRows reads each ID's rows latest run
   * first. An ID with no row to show (a truncated value's digest, an item
   * only in Selections, or only in runs a delete Claim hides) is skipped: it
   * takes no share and does not count toward `values`, and more IDs are asked
   * for in its place, for up to SEARCH_VALUE_ROUNDS walks. An ID equal to
   * `prefix` reads first, with all of `limit`; the others read at once, each
   * up to an equal share of what it leaves, then what is left of that,
   * newest first, from the IDs that had more. `more` (not enumerable) says
   * either query had more than it kept.
   */
  async searchSamples(prefix, { limit = SEARCH_SAMPLES, values = SEARCH_SAMPLE_VALUES } = {}) {
    if (!prefix) return []
    const paths = JSON.stringify(SAMPLE_PATHS)
    const [lo, hi] = prefixRange(prefix)
    const rowsOf = (id, n) => this.db.query(SQL.searchSampleRows, [paths, id, n])
    // Whether each run a current delete Claim hides, asked once per run: hiddenOf(runs) makes one claimStates call
    // for those of `runs` not yet known, and answers for all of them.
    const hidden = new Map()
    const hiddenOf = async (runs) => {
      const unknown = [...new Set(runs)].filter(c => !hidden.has(c))
      if (unknown.length) for (const [c, st] of await this.claimStates(unknown)) hidden.set(c, st.hidden)
      return (c) => hidden.get(c)
    }
    // Which of `ids` have a row to show. Every ID's first row is read at once, then one lookup covers their runs. Only
    // an ID whose first row's run is hidden reads further, up to `limit` more rows, and one with more than that is
    // taken to have a row to show.
    const showing = async (ids) => {
      const firsts = await Promise.all(ids.map(id => rowsOf(id, 1)))
      const isHidden = await hiddenOf(firsts.flat().map(r => r.completion_cid))
      const behind = ids.filter((_, i) => firsts[i].length && isHidden(firsts[i][0].completion_cid))
      const deeper = await Promise.all(behind.map(id => rowsOf(id, limit + 1)))
      const deeperHidden = await hiddenOf(deeper.flat().map(r => r.completion_cid))
      const found = new Map(behind.map((id, i) => [id, deeper[i].length > limit || deeper[i].some(r => !deeperHidden(r.completion_cid))]))
      return ids.map((id, i) => (found.has(id) ? found.get(id) : firsts[i].length > 0))
    }
    // The IDs with rows, in walk order: one more than asked for, so `more` says something was cut, never that a limit
    // was merely reached. Each walk asks for as many more values as IDs are still missing.
    const live = []
    const walked = new Set()
    let asked = values + 1
    let exhausted = false
    for (let round = 0; round < SEARCH_VALUE_ROUNDS && live.length <= values; round++) {
      const got = (await this.db.query(SQL.searchSampleValues, [paths, lo, hi, asked])).map(r => r.value)
      exhausted = got.length < asked
      const fresh = got.filter(v => !walked.has(v))
      for (const v of fresh) walked.add(v)
      const has = await showing(fresh)
      live.push(...fresh.filter((_, i) => has[i]))
      if (exhausted) break
      asked += values + 1 - live.length
    }
    let more = !exhausted || live.length > values
    const found = live.slice(0, values)
    // The prefix is the least value in its range, so each path's walk meets it first. With LIMIT hit on an earlier
    // path, a later path's exact match was never reached, so it is asked for too.
    const exactShows = live.includes(prefix) || (!exhausted && !walked.has(prefix) && (await showing([prefix]))[0])
    const ids = exactShows ? [prefix, ...found.filter(v => v !== prefix)] : found
    // An ID equal to `prefix` reads first, with the whole budget. The other IDs each first read their share of what
    // is left, all at once, so one common ID cannot spend it all; the rows left over then go to the IDs that had
    // more, newest first. searchSampleRows orders by run, item, collection and path, a total order, so a longer read
    // starts with the rows a shorter one returned and slicing from `had` is exact.
    const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
    const rows = []
    const cut = []
    const exact = ids[0] === prefix
    if (exact) {
      const got = await rowsOf(prefix, limit + 1)
      rows.push(...got.slice(0, limit))
      if (got.length > limit) cut.push({ id: prefix, had: limit })
    }
    const rest = exact ? ids.slice(1) : ids
    const budget = limit - rows.length
    const share = Math.ceil(budget / Math.max(rest.length, 1))
    // In order, each its share of what the ones before it could take at most, so the reads never pass the budget.
    const firsts = rest.map((id, i) => ({ id, n: Math.min(share, budget - share * i) }))
    const read = await Promise.all(firsts.map(({ id, n }) => (n > 0 ? rowsOf(id, n + 1) : null)))
    firsts.forEach(({ id, n }, i) => {
      if (n <= 0) { cut.push({ id, had: 0 }); return }
      rows.push(...read[i].slice(0, n))
      if (read[i].length > n) cut.push({ id, had: n })
    })
    const left = limit - rows.length
    if (cut.length && left === 0) more = true
    if (cut.length && left > 0) {
      const extra = (await Promise.all(cut.map(({ id, had }) => rowsOf(id, had + left + 1).then(got => got.slice(had))))).flat()
      extra.sort((a, b) => cmp(b.finished_at ?? '', a.finished_at ?? '') || cmp(a.completion_cid, b.completion_cid))
      rows.push(...extra.slice(0, left))
      if (extra.length > left) more = true
    }
    const rank = (path) => SAMPLE_PATHS.indexOf(path)
    const byItem = new Map()
    for (const r of rows) {
      const k = `${r.collection_cid} ${r.item_cid}`
      const seen = byItem.get(k)
      if (!seen) byItem.set(k, { ...r, paths: [r.path] })
      else {
        seen.paths.push(r.path)
        seen.paths.sort((a, b) => rank(a) - rank(b))
        if (rank(r.path) < rank(seen.path)) Object.assign(seen, { path: r.path, sample: r.sample })
      }
    }
    const isHidden = await hiddenOf(rows.map(r => r.completion_cid))
    const out = [...byItem.values()].filter(r => !isHidden(r.completion_cid))
      .sort((a, b) => cmp(b.finished_at ?? '', a.finished_at ?? '') || cmp(a.completion_cid, b.completion_cid)
        || cmp(a.sample, b.sample) || cmp(a.output_name, b.output_name) || cmp(a.item_cid, b.item_cid))
    return Object.defineProperty(out, 'more', { value: more })
  }

  /**
   * What each address that starts with `prefix` is, once `prefix` is long
   * enough to narrow (isAddressQuery): `[{ cid, kind: 'run'|'output'|
   * 'selection'|'item'|'file', label, href }]`, runs first, then outputs,
   * Selections, items and files. Snapshot rows, plus the Store Log tail's runs
   * and Selections, which are already in memory. An address is the reader's
   * own way in, so nothing is hidden here: the page it opens shows any
   * deletion.
   */
  async searchAddresses(prefix, { limit = SEARCH_ADDRESSES } = {}) {
    if (!isAddressQuery(prefix)) return []
    const [lo, hi] = prefixRange(prefix)
    const ask = (sql) => this.db.query(sql, [lo, hi, limit])
    const runs = await ask(SQL.searchRunCids)
    const collections = await ask(SQL.searchCollectionCids)
    const items = await ask(SQL.searchItemCids)
    const files = await ask(SQL.searchContentCids)
    const found = new Map()
    const add = (cid, kind, label, href, extra = {}) => { if (!found.has(cid)) found.set(cid, { cid, kind, label, href, ...extra }) }
    const runRows = [...runs, ...this.stale.filter(s => s.row && s.cid.startsWith(prefix)).map(s => s.row)]
    for (const r of runRows) add(r.completion_cid, 'run', r.run_name ?? 'unnamed run', `#/run/${r.completion_cid}`)
    for (const c of collections.filter(c => c.kind === 'output'))
      add(c.collection_cid, 'output', `${c.run_name ?? 'unnamed run'} / ${c.output_name}`, `#/collection/${c.collection_cid}`)
    const selections = [...collections.filter(c => c.kind === 'selection').map(c => c.collection_cid),
      ...this.tailSelections.filter(t => t.value && t.cid.startsWith(prefix)).map(t => t.cid)]
    if (selections.length) {
      const states = await this.claimStates(selections)
      for (const cid of selections)
        add(cid, 'selection', states.get(cid).names.join(' / ') || 'unnamed Selection', `#/selection/${cid}`, { named: namedBy(states.get(cid).current) })
    }
    // An item in several collections is opened through the first: ORDER BY puts outputs before Selections.
    const firstOf = new Map()
    for (const r of items) if (!firstOf.has(r.item_cid)) firstOf.set(r.item_cid, r)
    // Items are named by sample ID, which only the item's block holds (item_attr has no index on item_cid).
    const samples = await Promise.all([...firstOf.values()].map(r => this.itemBlock(r.collection_cid, r.item_cid).then(
      b => sampleOf(b.view), (e) => { if (e instanceof BlockError) return null; throw e })))
    for (const [i, r] of [...firstOf.values()].entries()) {
      const label = r.kind === 'output' ? [r.output_name, samples[i]].filter(Boolean).join(' · ') : samples[i] ?? 'item'
      add(r.item_cid, 'item', label, `#/item/${r.collection_cid}/${r.item_cid}`)
    }
    for (const f of files) add(f.content_cid, 'file', f.filename ?? 'file', `#/content/${f.content_cid}`)
    return [...found.values()].slice(0, limit)
  }
}

/**
 * `[prefix, prefix + U+10FFFF]`: every string that starts with `prefix` sorts
 * between the two in SQLite's binary order, so a prefix search is a range on
 * an index and no character in it is a wildcard.
 */
export const prefixRange = (prefix) => [prefix, `${prefix}\u{10FFFF}`]

/**
 * A CIDv1 in base32 starts with the same seven characters for every block of
 * one codec (`bafyrei` dag-cbor, `bafkrei` raw: multibase, version, codec,
 * sha2-256), so an address query needs six more before it narrows anything.
 */
export const ADDRESS_HEAD = 7
export const ADDRESS_MIN = 6
export const isAddressQuery = (q) => typeof q === 'string' && /^b[a-z2-7]+$/.test(q) && q.length >= ADDRESS_HEAD + ADDRESS_MIN

/** The current `set name` claims of a claimState's `current`, as `[{ asserted_by, timestamp, conflicted }]`: who named it and when. Empty when unnamed. */
export const namedBy = (current) => current.filter(c => c.attribute === 'name' && c.verb === 'set')
  .map(c => ({ asserted_by: c.asserted_by, timestamp: c.timestamp, conflicted: c.conflicted }))

/** An item's sample ID: the string at the first of SAMPLE_PATHS its Meta Map has, or null. */
function sampleOf(view) {
  for (const path of SAMPLE_PATHS) {
    const v = path.split('.').reduce((o, k) => (o && typeof o === 'object' ? o[k] : undefined), view)
    if (typeof v === 'string' && v !== '') return v
  }
  return null
}
