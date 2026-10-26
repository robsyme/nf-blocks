// The page's copy path: JSON requests to nf-blocks:explore's POST /api/copy
// (P3 interface 1.7), a dry run first and then the copy. The plugin computes
// the closure and the plan (1.3, 1.4); the page only shows them. Same token
// and origin as /api/put (write.js).
import { queryOf } from './routequery.js'

export class CopyError extends Error {
  /** `body` is the 1.3 body when the server sent one with the error (a copy that failed part way). */
  constructor(code, message, status, body = null) { super(message); this.code = code; this.status = status; this.body = body }
}

const CODES = { 400: 'bad_request', 403: 'forbidden', 409: 'conflict', 413: 'too_large', 500: 'copy_failed', 503: 'stopping' }

/** A 1.3 body: it has a plan. */
const isPlan = (body) => !!body && typeof body === 'object' && !!body.plan

/** The words for a failed copy whose 1.3 body has no message: how many blocks failed. */
function failedText(body) {
  const n = Array.isArray(body.failures) ? body.failures.length : 0
  return n ? `${n} ${n === 1 ? 'block' : 'blocks'} failed` : 'the copy failed'
}

/**
 * `plan(request)` asks for a dry run, `copy(request)` copies. `request` is
 * `{ root: 'selection:<cid>' | 'run:<cid>', from, to, depth, claims, accept_audit }`;
 * `claims` names at least one category, or is `['none']`.
 * Both resolve with the 1.3 body. A 409 that carries a plan resolves with
 * that plan and its `refused` ('audit' for audit findings and no
 * accept_audit, 'not_kept' when the destination released or deleted a root
 * or run, D9; 'audit' when the server does not say). Every other failure is
 * a CopyError whose code is the server's `error` (`superseded` for a dry run
 * a newer one replaced, D10; `stopping` for a 503 while the server shuts down), else bad_request, forbidden, conflict or
 * copy_failed by status. A 500 with a 1.3 body (a copy that failed part way)
 * keeps the body, and says how many blocks failed when it has no message.
 */
export function copyClient({ endpoint, token, fetchFn = (...a) => fetch(...a) }) {
  async function post(request, dryRun) {
    let res
    try {
      res = await fetchFn(dryRun ? `${endpoint}?dry_run=true` : endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-NF-Blocks-Token': token },
        body: JSON.stringify(request),
      })
    } catch (e) {
      throw new CopyError('copy_failed', `could not reach the explore server: ${e.message}`, 0)
    }
    const type = res.headers.get('Content-Type') ?? ''
    const text = await res.text()
    let body = null
    if (type.includes('json')) {
      try { body = JSON.parse(text) } catch { body = null }
    }
    if (res.ok && isPlan(body)) return body
    if (res.status === 409 && isPlan(body)) return { ...body, refused: body.refused ?? 'audit' }
    const code = (body && typeof body.error === 'string' && body.error) || CODES[res.status] || 'copy_failed'
    // A JSON body is never the message itself: its message, else what a plan says failed, else the status.
    const message = (body && typeof body.message === 'string' && body.message)
      || (res.ok ? 'the explore server answered without a plan'
        : isPlan(body) ? failedText(body)
          : body ? `HTTP ${res.status}` : text.trim() || `HTTP ${res.status}`)
    throw new CopyError(code, message, res.status, isPlan(body) ? body : null)
  }
  return {
    plan: (request) => post(request, true),
    copy: (request) => post(request, false),
  }
}

/**
 * Whether this page can copy, and why not: `{ available, reason, targets }`.
 * `targets` are the members other than the open one that can take a copy
 * (members.json `copy_target`). Needs no request: it reads what resolveStore
 * already has and the page's token.
 */
export function copyAvailability(store, { token = null } = {}) {
  const members = store?.members ?? null
  const others = (members ?? []).filter(m => m.alias !== store.member)
  const targets = others.filter(m => m.copy_target)
  const reason = !members ? 'Copying needs this page opened through `nextflow plugin nf-blocks:explore`.'
    : !token ? 'Open this page with the URL nf-blocks:explore printed; it carries the token copying needs.'
      : !store.copy ? "This explore server can't run copies."
        : others.length === 0 ? 'There is no other store in this configuration to copy to.'
          : targets.length === 0 ? `No other store can take a copy: ${others.map(m => `${m.alias}${m.copy_reason ? ` (${m.copy_reason})` : ''}`).join('; ')}.`
            : null
  return { available: reason === null, reason, targets: reason === null ? targets.map(m => m.alias) : [] }
}

/**
 * What a store switcher says of a member copy can't write to (members.json
 * copy_target false). Not "read-only": the storage cards say "View only" for
 * a store this page can't write to, which is another thing.
 */
export const cantCopyText = (m) => (m.copy_reason ? `Can't copy here: ${m.copy_reason}` : "Can't copy here")

export const DEPTHS = ['results', 'lineage', 'all']
// 1.1's order; the request lists the ticked ones in it.
export const CATEGORIES = ['names', 'pins', 'projects', 'annotations']
export const DEFAULT_CLAIMS = ['names', 'projects', 'annotations']

/** The copy page's query: `to`, `depth` (results when unknown), `claims` (the defaults when absent, none for `none`) and `project` (P4: the project whose default store the page explains). */
export function parseCopyQuery(query) {
  const q = queryOf(query)
  const depth = DEPTHS.includes(q.get('depth')) ? q.get('depth') : 'results'
  const raw = q.get('claims') ?? null
  const claims = raw === null ? DEFAULT_CLAIMS
    : raw === 'none' ? [] : CATEGORIES.filter(c => raw.split(',').map(s => s.trim()).includes(c))
  return { to: q.get('to') || null, depth, claims, project: q.get('project') || null }
}

/** The copy page's address for these choices, with the project its Copy to… came from so a reload still shows that project's note. */
export function copyHref(kind, cid, { to, depth, claims, project = null }) {
  const q = []
  if (to) q.push(`to=${encodeURIComponent(to)}`)
  q.push(`depth=${depth}`)
  q.push(`claims=${claims.length ? CATEGORIES.filter(c => claims.includes(c)).join(',') : 'none'}`)
  // `project` may list several projects, comma-separated (copyToButton): each is encoded, the commas kept.
  if (project) q.push(`project=${String(project).split(',').map(encodeURIComponent).join(',')}`)
  return `#/${kind}/${cid}/copy?${q.join('&')}`
}
