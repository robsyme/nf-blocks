// Item previews for the page's item rows (DESIGN.md §16 milestone 3 decision 10, ticket
// 03 Q3): OutputItem blocks fetched lazily through Explorer.itemBlock, which
// reads them through BlockFetcher (hash-checked, cached), a few at a time. No
// snapshot query: itemBlock leaves out the claim state (Explorer.item reads
// it), so a preview never costs a range read of the index.
import { PREVIEW_CAP, PREVIEW_CONCURRENCY } from './config.js'
import { DAG_CBOR } from './cid.js'
import { pairsOf } from './pairs.js'
import { attrRows } from './metadata.js'
import { codeOf, fileSlots } from './slots.js'

export class Previews {
  /** `rows`: also keep each item's item_attr rows (metadata.js attrRows), for filters that match as the index does. */
  constructor(ex, { cap = PREVIEW_CAP, concurrency = PREVIEW_CONCURRENCY, rows = false } = {}) {
    this.ex = ex
    this.cap = cap
    this.rows = rows
    this.concurrency = concurrency
    this.done = new Map()
    this.waiting = new Set()
    this.queue = []
    this.listeners = new Set()
    this.inFlight = 0
    this.fetched = 0
    this.failed = 0
  }

  /** Queues one item's preview unless it has arrived or is on its way. `collectionCid` may be null (a member with no via). */
  ask(collectionCid, itemCid) {
    if (this.done.has(itemCid) || this.waiting.has(itemCid)) return
    this.waiting.add(itemCid)
    this.queue.push({ collectionCid, itemCid })
    this.pump()
  }

  /** Asks for the first `cap` of `rows` (`{collection, item}`) and says how many; the rest wait for "show details". */
  askFirst(rows) {
    const first = rows.slice(0, this.cap)
    for (const r of first) this.ask(r.collection, r.item)
    return first.length
  }

  get(itemCid) { return this.done.get(itemCid) }

  onChange(fn) {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  /** Drops what has not started, for a view that is no longer shown; fetches in flight finish and are kept. */
  cancel() {
    for (const q of this.queue) this.waiting.delete(q.itemCid)
    this.queue = []
  }

  get stats() { return { fetched: this.fetched, failed: this.failed, inFlight: this.inFlight, queued: this.queue.length } }

  pump() {
    while (this.inFlight < this.concurrency && this.queue.length) {
      const { collectionCid, itemCid } = this.queue.shift()
      this.inFlight++
      let answer
      try {
        answer = Promise.resolve(this.ex.itemBlock(collectionCid, itemCid))
      } catch (e) {
        answer = Promise.reject(e)
      }
      answer.then(
        (it) => { this.done.set(itemCid, { pairs: pairsOf(it.view), files: it.leaves.map(l => l.name).filter(Boolean),
          dirs: it.leaves.filter(l => l.name && l.address && codeOf(l.address) === DAG_CBOR).map(l => l.name), slots: fileSlots(it.value),
          ...(this.rows ? { rows: attrRows(it.view) } : {}) }); this.fetched++ },
        (e) => { this.done.set(itemCid, { error: e?.code ?? 'query_failed', message: e?.message ?? String(e) }); this.failed++ })
        .finally(() => {
          this.inFlight--
          this.waiting.delete(itemCid)
          for (const fn of this.listeners) {
            try { fn(itemCid) } catch { /* one row's redraw must not stop the queue */ }
          }
          this.pump()
        })
    }
  }
}
