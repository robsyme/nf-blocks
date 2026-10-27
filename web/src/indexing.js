// The page's side of background store indexing: the POST /api/index client, the
// words for a running job, and the watch that polls GET /api/index while one
// runs. Same token and origin as /api/copy (copy.js).
import { formatNumber } from './format.js'

export class IndexError extends Error {
  constructor(code, message, status) { super(message); this.code = code; this.status = status }
}

/** `45s`, `2m 10s`, `1h 3m`. */
export function elapsedText(ms) {
  const s = Math.max(0, Math.floor(ms / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${s % 60}s`
  return `${Math.floor(m / 60)}h ${m % 60}m`
}

/**
 * What a running job is doing, and for how long: `Reading 1,240 of 8,615
 * metadata blocks · 2m 10s`. Without a progress object it says `Indexing…`,
 * and without a start time it leaves the time out.
 */
export function progressText(index, alias, now = Date.now()) {
  if (!index?.progress) return 'Indexing…'
  const { phase, done, total, startedAt } = index.progress
  const words = {
    listing: 'Listing blocks…',
    reading: `Reading ${formatNumber(done)} of ${formatNumber(total)} metadata blocks`,
    ingesting: `Indexing ${formatNumber(done)} of ${formatNumber(total)} records`,
    log: 'Reading the Store Log…',
    snapshot: 'Building the snapshot…',
    push: `Pushing to ${alias}…`,
  }[phase] ?? 'Indexing…'
  const started = Date.parse(startedAt)
  return Number.isFinite(started) ? `${words} · ${elapsedText(now - started)}` : words
}

/** Whether a member's index moved in a way that changes what the page reads: its state, its time or whether it is served locally. */
export const moved = (before, index) => before?.state !== index?.state || before?.at !== index?.at || before?.local !== index?.local

/**
 * What the page does when a member's index changes (app.js wires it to the
 * watch's onChange). The Stores list redraws on every change. A move
 * (`moved`) also forgets the member's size; one to anything but `indexing`
 * is a job's end, which refreshes who holds what and a showing Storage page.
 * The `primary` member is the snapshot the page itself has open, so its
 * job's end reloads the page, which opens it again.
 */
export function indexChangeHandler({ primary, sizes, redrawStores, presence, storageShowing, render, reload }) {
  return (alias, index, before) => {
    const changed = moved(before, index)
    if (changed) sizes?.forget(alias)
    redrawStores?.()
    if (!changed || index?.state === 'indexing') return
    if (alias === primary) { reload(); return }
    presence?.invalidate()
    if (storageShowing()) render()
  }
}

/**
 * `start(alias, mode)` asks the server to index a member (`mode` is 'local' or
 * 'push') and resolves the `index` object it answers with. A failure is an
 * IndexError whose code is the server's `error` (`index_running` for a 409,
 * else index_failed) and whose message is the server's.
 */
export function indexClient({ endpoint, token, fetchFn = (...a) => fetch(...a) }) {
  return {
    async start(alias, mode) {
      let res
      try {
        res = await fetchFn(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-NF-Blocks-Token': token },
          body: JSON.stringify({ member: alias, mode }),
        })
      } catch (e) {
        throw new IndexError('index_failed', `could not reach the explore server: ${e.message}`, 0)
      }
      const text = await res.text()
      let body = null
      try { body = JSON.parse(text) } catch { body = null }
      if (res.ok && body?.index) return body.index
      const code = (typeof body?.error === 'string' && body.error) || 'index_failed'
      const message = (typeof body?.message === 'string' && body.message) || text.trim() || `HTTP ${res.status}`
      throw new IndexError(code, message, res.status)
    },
  }
}

const MAX_FAILED_POLLS = 3

/**
 * The members' index states and the watch over them. `fetchStatus()` resolves
 * `{ members: { alias: index } }` (GET /api/index). The watch polls every
 * `interval` ms while some member is indexing, and stops after three failed
 * polls in a row; `start`, `track` and `watch()` begin it again. A member
 * whose job ended is invalidated before the listeners hear of it, so whoever
 * re-renders reads its new snapshot. Listeners hear `(alias, index, before)`;
 * one that throws, or an invalidate that does, goes to `onError` and is not a
 * failed poll. A member the answer leaves out while it reads `indexing` is
 * no longer known to the server: its index becomes null.
 */
export function createIndexing(members, { client, fetchStatus, interval = 1000, setTimer = setTimeout, clearTimer = clearTimeout,
  onError = (e) => console.error('indexing: a listener failed', e) }) {
  const listeners = new Set()
  let timer = null
  let polling = false
  let failures = 0

  const find = (alias) => members.list.find(m => m.alias === alias)
  const stateOf = (alias) => find(alias)?.index ?? null
  const running = () => members.list.find(m => m.index?.state === 'indexing')?.alias ?? null

  const guarded = (fn) => { try { fn() } catch (e) { onError(e) } }

  function apply(alias, index) {
    const member = find(alias)
    if (!member) return
    const before = member.index ?? null
    if (JSON.stringify(before) === JSON.stringify(index)) return
    member.index = index
    if (moved(before, index) && index?.state !== 'indexing') guarded(() => members.invalidate(alias))
    for (const fn of [...listeners]) guarded(() => fn(alias, index, before))
  }

  async function poll() {
    timer = null
    polling = true
    let answer = null
    try {
      answer = (await fetchStatus())?.members ?? {}
      failures = 0
    } catch {
      failures++
    } finally {
      polling = false
    }
    if (answer) {
      for (const [alias, index] of Object.entries(answer)) if (index) apply(alias, index)
      for (const m of members.list) if (m.index?.state === 'indexing' && !(m.alias in answer)) apply(m.alias, null)
    }
    if (failures < MAX_FAILED_POLLS) watch()
  }

  function watch() {
    if (timer !== null || polling || running() === null) return
    if (failures >= MAX_FAILED_POLLS) return
    timer = setTimer(poll, interval)
  }

  return {
    stateOf,
    running,
    async start(alias, mode) {
      const index = await client.start(alias, mode)
      const member = find(alias)
      if (member) member.index = index
      failures = 0
      watch()
      return index
    },
    /** Takes `index` for `alias` from another answer (a copy's), and watches it when it is indexing. */
    track(alias, index) {
      if (index) apply(alias, index)
      failures = 0
      watch()
    },
    onChange(fn) { listeners.add(fn); return () => listeners.delete(fn) },
    watch() { failures = 0; watch() },
  }
}
