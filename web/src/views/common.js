// web/src/views/common.js
// What more than one view draws (DESIGN.md §15–16): errors, tables, pagers,
// the retention panel, picks, item rows, breadcrumbs. The data-* attributes
// are the contract the Gate reads; the rest is for people.
import { h, link, copyOutcome, saveText } from '../html.js'
import { address, icon, ref, shortCid } from '../icons.js'
import { Previews } from '../previews.js'
import { labelPaths, labelText } from '../pairs.js'
import { trayNote } from '../tray.js'
import { SAMPLE_PATHS, itemTable, withoutDuplicates } from '../table.js'
import { runTone } from '../words.js'
import { ago, avatar, claimTag, durability, note, panel } from '../components.js'
import { seal } from '../seal.js'
import { mineState, samePrincipal, whoText } from '../identity.js'
import { bytes, sizeText } from '../format.js'
import { decodeValue } from '../projects.js'

export const enc = encodeURIComponent

const SNAPSHOT_COMMAND = 'nextflow plugin nf-blocks:snapshot'
const REINDEX_COMMAND = 'nextflow plugin nf-blocks:reindex --seed false'

/**
 * Why a lean snapshot has no storage figures (Explorer.storageOmitted,
 * fix round 2): a note for #/storage and the run's Storage tab, or null when
 * the snapshot has them. The cap is
 * named only when the snapshot gives one above 0 (Explorer.snapshotCapBytes).
 * `seeded_lean`: the cache that wrote it was seeded from a lean snapshot, so
 * it has no content edges to count until it is rebuilt from the store.
 * `over_cap` names the CLI's snapshot first: it ignores the cap, so it
 * writes the figures at any size, while raising the cap is what lets each
 * run's snapshot keep them.
 */
export function omittedNote(ex) {
  const reason = ex.storageOmitted
  if (!reason) return null
  const run = h('code', {}, SNAPSHOT_COMMAND)
  const cap = ex.snapshotCapBytes ? ` (${bytes(ex.snapshotCapBytes)})` : ''
  const lead = "Storage figures are left out of this store's snapshot"
  const body = reason === 'over_cap' ? [`${lead}: with them it would pass cas.snapshot.maxBytes${cap}. Run `, run,
    ' to write it with the figures at any size, or raise cas.snapshot.maxBytes so runs keep them.']
    : reason === 'out_of_memory' ? [`${lead}: computing them ran out of memory. Give Nextflow more memory, for example NXF_OPTS=-Xmx4g, and run `, run, '.']
      : reason === 'seeded_lean' ? [`${lead}: the cache that wrote the snapshot was rebuilt from a snapshot without them. Run `,
        h('code', {}, REINDEX_COMMAND), ' to rebuild it from the store.']
        : [`${lead}. Run `, run, ' to rewrite it.']
  const node = note(body)
  node.setAttribute('data-storage-omitted', reason)
  return node
}

/**
 * The one sentence #/storage and the run's Storage tab write when the
 * snapshot's figures are stale (Explorer.storageChanges): `what` is the
 * figures it covers, with its verb ("These figures are", "Pinned here is"),
 * then what has happened since that they leave out.
 */
export function staleFiguresText(what, { claims, logged }) {
  const lead = `${what} as of the snapshot; `
  if (claims && logged) return `${lead}a pin, release or deletion since then is not counted yet, nor are runs and Selections logged since.`
  if (claims) return `${lead}a pin, release or deletion since then is not counted yet.`
  return `${lead}runs and Selections logged since then are not counted yet.`
}

/**
 * A Claim's verb in the words every page uses (the Selection rail, Home's
 * activity, the retention badges): a name reads "named" (a removed name
 * "unnamed"), a pin "pinned" (an unpin "unpinned"), a release "released", a
 * deletion "deleted", and undoing a release or a deletion "restored". A P4
 * Claim reads as the project page says it (projectClaimWords).
 */
export function claimVerb(c) {
  if (isProjectClaim(c)) return projectClaimWords(c).verb
  if (c.attribute === 'name') return c.verb === 'del' ? 'unnamed' : 'named'
  if (c.attribute === 'pin') return c.verb === 'add' ? 'pinned' : 'unpinned'
  if (c.attribute === 'retain') return c.verb === 'set' ? 'released' : 'restored'
  if (c.attribute === null || c.attribute === undefined) return c.verb === 'delete' ? 'deleted' : 'restored'
  return c.verb ?? 'claimed'
}

/**
 * A Claim's value as its tag shows it, beside claimVerb: a P4 Claim's in the
 * project page's words (projectClaimWords, with `was` the value of a Claim a
 * `del` supersedes, when known), a name's or a pin's note as it is, and
 * nothing for the rest.
 */
export function claimValueOf(c, was = null) {
  if (isProjectClaim(c)) return projectClaimWords(c, was).value
  return c.attribute === 'name' || c.attribute === 'pin' ? c.value : null
}

// The P4 attributes (plan 1.2): a membership on its member, and the rest on the project.
const PROJECT_ATTRIBUTES = new Set(['project', 'sample', 'annotation', 'same_sample', 'policy.default_store', 'policy.retention', 'policy.people'])
export const isProjectClaim = (c) => PROJECT_ATTRIBUTES.has(c?.attribute)

/** A project as a Claim's value: its name linked to its page, its short cid until it is named (nameProjectLinks). */
export const projectLink = (cid, name = null) => h('a', { href: `#/project/${cid}`, 'data-project-name': cid }, name ?? shortCid(cid))

/**
 * Names each projectLink in `links` once `lookup(cids)` (a Promise of a Map
 * of cid to `{ name }`) answers; one it has no name for keeps its short cid.
 * The name is the link's title too, and its claim tag's: claimTag titles a
 * value with the text it had when built, the short cid here. Resolves when
 * they are named; a lookup that fails leaves them as they are.
 */
export function nameProjectLinks(links, lookup) {
  const list = [...links].filter(a => a?.getAttribute?.('data-project-name'))
  if (!list.length) return Promise.resolve()
  return Promise.resolve().then(() => lookup([...new Set(list.map(a => a.getAttribute('data-project-name')))])).then((names) => {
    for (const a of list) {
      const name = names?.get(a.getAttribute('data-project-name'))?.name
      if (!name) continue
      a.textContent = name
      a.setAttribute('title', name)
      const val = a.parentElement
      if (val?.classList?.contains('val')) val.setAttribute('title', val.textContent)
    }
  }, () => {})
}

const words = (text) => (text ? h('span', {}, text) : null)
/** A Claim value as decodeValue reads it, or as it is when it is not DAG-JSON text (a bare cid, say). */
const decoded = (v) => { const d = decodeValue(v); return d === undefined ? v : d }
/** A link's cid: a cid string, or DAG-JSON's `{ "/": cid }` where a value came as it is. */
const linkCid = (v) => (typeof v === 'string' ? v : typeof v?.['/'] === 'string' ? v['/'] : null)

/**
 * A project's Claim, or a membership, in the project page's words (mockup
 * 13: `condition`, `RAP1_IAA_30M_REP1 = auxin_30m`): `{ verb, value }`, the
 * value a node or null. Every page that shows one says it this way: the
 * project's Activity, the Selection's Claims rail, home's activity and the
 * copy page's claims. `was` is the value of a Claim a `del` supersedes, which
 * describes the `del` when the page knows it. Values come as the index keeps
 * them (DAG-JSON text) or decoded.
 */
export function projectClaimWords(c, was = null) {
  const del = c.verb === 'del'
  const value = decoded(c.value)
  const old = was === null || was === undefined ? null : decoded(was)
  switch (c.attribute) {
    case 'name': return del ? { verb: 'unnamed', value: null } : { verb: 'named', value: words(`"${c.value}"`) }
    case null: case undefined: return { verb: c.verb === 'delete' ? 'deleted' : 'restored', value: words('the project') }
    case 'project': {
      const project = linkCid(del ? old : value)
      return { verb: del ? 'removed from' : 'added to', value: project ? projectLink(project) : null }
    }
    case 'sample': return { verb: del ? 'removed sample' : 'added sample', value: words((del ? old : value)?.id) }
    case 'annotation': return del ? { verb: 'cleared', value: words(old?.name ? `${old.name} of ${old.sample?.id}` : 'an annotation') }
      : { verb: value?.name ?? 'annotated', value: words(`${value?.sample?.id ?? ''} = ${value?.value ?? ''}`) }
    case 'same_sample': return del ? { verb: 'undid', value: words(old ? `${old.same ? 'the merge of' : 'the split of'} ${old.a?.id} and ${old.b?.id}` : 'a merge or split') }
      : value?.same ? { verb: 'merged', value: words(`${value.a?.id} and ${value.b?.id}`) } : { verb: 'split', value: words(`${value?.b?.id} from ${value?.a?.id}`) }
    case 'policy.default_store': return del ? { verb: 'cleared', value: words('the default store') } : { verb: 'default store', value: words(c.value) }
    case 'policy.retention': return { verb: 'retention', value: words(c.value === 'pin_selections' ? 'pins every Selection' : 'off') }
    case 'policy.people': return del ? { verb: 'removed person', value: words(typeof old === 'string' ? old : null) } : { verb: 'added person', value: words(c.value) }
    default: return { verb: c.verb ?? 'claimed', value: words(c.attribute) }
  }
}

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
}

/** A write's refusal as errorNode shows it, with what to do about a stale supersedes (runWrite and the hand-off page's steps). */
export function writeFailureNode(e) {
  const node = errorNode(e)
  if (e?.code === 'stale_supersedes') node.append(' Someone changed this since the page loaded; reload to see the current state.')
  return node
}

export function table(head, rows) {
  return h('div', { class: 'scroll' }, h('table', {}, h('tr', {}, head.map(t => h('th', {}, t))), rows))
}

/**
 * The page's one tooltip (spec §6.3, amended): shown at once on hover or
 * focus, where the native `title` waits about a second. `lines` are
 * `[class, text]`, one line each; the items table and the run page's +N use it.
 */
export function pillTip() {
  const el = h('div', { class: 'file-pill-tip', role: 'tooltip', 'data-pill-tip': '', 'data-open': 'false' })
  const show = (target, lines) => {
    el.replaceChildren(...lines.map(([cls, text]) => h('div', { class: cls }, text)))
    el.dataset.open = 'true'
    const r = target.getBoundingClientRect?.()
    if (r) el.setAttribute('style', `left: ${Math.max(8, Math.min(r.left, innerWidth - el.offsetWidth - 8))}px; top: ${r.bottom + 6}px`)
  }
  const hide = () => { el.dataset.open = 'false' }
  /** Hover and focus handlers for `lines`, or for `() => lines` when they change after drawing. */
  const on = (lines) => {
    const get = typeof lines === 'function' ? lines : () => lines
    return { onmouseenter: (e) => show(e.currentTarget, get()), onfocus: (e) => show(e.currentTarget, get()),
      onmouseleave: hide, onblur: hide, 'aria-label': get().map(([, text]) => text).join('\n') }
  }
  return { el, on }
}

/** What Copy on a Selection member copies (spec 7.1a): the bare item address when it has no via, else `cas://<via>/<address>`. */
export const copyText = (address, via) => (via === '-' ? address : `cas://${via}/${address}`)

/**
 * What the deleted Selections list's Undo-unavailable note says, or `null`
 * when Undo is offered here and no note is shown (mirrors the Selection
 * view's `actions()`). `href` is `ctx.write.hrefFor('#/selections?deleted=1')`,
 * computed by the caller since it is `ctx`'s own method.
 */
export const undoNote = (available, here, reason, writable, href) => (!available
  ? { kind: 'unavailable', reason }
  : here ? null
    : { kind: 'elsewhere', writable, href })

/** Store Log runs whose blocks were missing or refused. Their pipeline is
 * unknown (it is in the RunManifest the refused RunCompletion links to), so
 * every view that lists runs shows them. */
export function unreadableRuns(ex) {
  const unreadable = ex.stale.filter(s => s.error)
  return unreadable.length
    ? h('section', {}, h('h2', {}, 'Runs in the Store Log that could not be read'), unreadable.map(s => errorNode(s.error)))
    : null
}

/**
 * The All / Mine toggle of a list head. Nothing without an identity (the page was not opened through
 * nf-blocks:explore); a disabled Mine with the reason for an anonymous one. `allHref` and `mineHref`
 * keep the list's other query params and reset its offset.
 */
export function mineToggle(identity, { allHref, mineHref, on }) {
  if (!identity) return null
  const state = mineState(identity)
  const pressed = on && state.available
  return h('div', { class: 'seg-toggle', 'data-mine-toggle': '', role: 'group', 'aria-label': 'Whose' },
    h('a', { href: allHref, class: pressed ? null : 'on', 'aria-pressed': String(!pressed) }, 'All'),
    state.available
      ? h('a', { href: mineHref, class: pressed ? 'on' : null, 'aria-pressed': String(pressed) }, 'Mine')
      : h('button', { type: 'button', disabled: true, title: state.reason, 'aria-pressed': 'false' }, 'Mine'))
}

/** Where a page sits in the whole list, and links to the pages either side (DESIGN.md §15, [data-page]). A route may already carry a query. */
export function pager(route, page, noun) {
  if (page.total === 0) return null
  const at = (offset) => `${route}${route.includes('?') ? '&' : '?'}offset=${offset}`
  const label = page.first === 0 ? `no ${noun} on this page; there are ${page.total}` : `showing ${page.first}-${page.last} of ${page.total} ${noun}`
  return h('p', { 'data-page': '', 'data-first': page.first, 'data-last': page.last, 'data-total': page.total, class: 'muted' },
    label,
    page.prev !== null ? [' ', h('a', { href: at(page.prev), 'data-page-prev': '' }, 'previous page')] : null,
    page.next !== null ? [' ', h('a', { href: at(page.next), 'data-page-next': '' }, 'next page')] : null)
}

/**
 * The note shown instead of write controls when writing is unavailable, or
 * available but not to this page: unavailable everywhere gives just
 * `ctx.write.reason`; available but elsewhere names what would be offered
 * here (`verbs`, e.g. "Rename, delete and undo" or "Pin, release and
 * restore") and links to this same page in the writable member (`href`,
 * built by the caller with `ctx.write.hrefFor` as the Selection view's
 * `actions()` does, `openLabel` what the link reads). Shared by `actions()`
 * and `retentionPanel`, so every page's unavailable note reads the same way.
 */
export function unavailableNote(ctx, verbs, openLabel, href) {
  if (!ctx.write.available) return h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason)
  return h('p', { 'data-unavailable': '', class: 'muted' }, `${verbs} write to the writable member, ${ctx.write.writable}. `,
    link(href, openLabel), '.')
}

/**
 * `a, b, c and 2 more`: at most `max` names, then how many are left.
 * `capped` says the list itself was cut short (root_shared keeps five rows
 * per root), so the count is unknown: `a, b and others`.
 */
export function namesText(names, max = 3, capped = false) {
  const shown = names.slice(0, max).join(', ')
  if (capped) return `${shown} and others`
  return names.length > max ? `${shown} and ${names.length - max} more` : shown
}

const settledValue = (v, use) => {
  if (v && typeof v.then === 'function') v.then(use, () => {})
  else if (v !== null && v !== undefined) use(v)
}

/** "Already pinned here" for the other pinners' rows: one row is a sentence, more are listed each with its own figure (no sum: pins overlap). */
export function pinnedByText(rows, viewer, partial) {
  const amount = (r) => sizeText({ bytes: r.bytes, partial })
  if (rows.length === 1) return `${amount(rows[0])} already pinned here by ${whoText(rows[0].assertedBy, viewer)}`
  return `Already pinned here: ${rows.map(r => `${whoText(r.assertedBy, viewer, { capital: true })} ${amount(r)}`).join(', ')}`
}

/**
 * What only each pin keeps here (R4): a Map of pin Claim cid to `{ bytes,
 * partial }`, from the subject's pin roots (`roots`, Explorer.rootStorage)
 * matched by who asserted the Claim. `pins` is every current pin on the
 * subject, `[{ cid, assertedBy }]`. There is one pin root per (subject,
 * asserter): when one person pins a subject twice, removing either pin
 * leaves the other, so each keeps 0 B. A pin with no root has no entry.
 */
export function pinKeeps(pins, roots) {
  const matched = pins.map(p => roots?.find(r => r.kind === 'pin' && samePrincipal(r.assertedBy, p.assertedBy)) ?? null)
  const shares = new Map()
  for (const root of matched) if (root) shares.set(root, (shares.get(root) ?? 0) + 1)
  const out = new Map()
  pins.forEach((p, i) => {
    const root = matched[i]
    if (root) out.set(p.cid, shares.get(root) > 1 ? { bytes: 0, partial: false } : { bytes: root.exclusive, partial: root.partial })
  })
  return out
}

/**
 * The pins a state holds, each a claim tag (who, "pinned", the note, how long
 * ago) in a `[data-pin][data-claim]` item; null when there are none. With `can`
 * each also has its own Unpin, calling `onUnpin(cid, event)`; without it the
 * list is read-only. `only` (a Set of pin Claim cids) lists just those, the
 * rest still counting for what each keeps. `roots` (the subject's roots, or a
 * promise of them) names on each Unpin what only that pin keeps in `store`
 * (pinKeeps); Unpin waits for a promise. Shared by `retentionPanel`, the
 * Selection page and a project's policy pins.
 */
export function pinsList(state, { can = false, onUnpin = () => {}, only = null, roots = null, store = 'this store' } = {}) {
  const shown = state.pins.filter(p => !only || only.has(p.cid))
  if (!shown.length) return null
  const current = new Map([...(state.current ?? []), ...(state.claims ?? [])].map(c => [c.cid, c]))
  const buttons = new Map()
  const list = h('ul', { class: 'pins' }, shown.map((p) => {
    const c = current.get(p.cid)
    const unpin = can ? h('button', { type: 'button', class: 'btn sm ghost', 'data-unpin': p.cid, onclick: (e) => onUnpin(p.cid, e) }, 'Unpin') : null
    if (unpin) buttons.set(p.cid, unpin)
    return h('li', { 'data-pin': '', 'data-claim': p.cid },
      claimTag({ assertedBy: c?.asserted_by ?? null, verb: 'pinned', value: p.note, at: c?.timestamp ?? null }), ' ', unpin)
  }))
  const label = (rs) => {
    const keeps = pinKeeps(state.pins.map(p => ({ cid: p.cid, assertedBy: current.get(p.cid)?.asserted_by ?? null })), rs)
    for (const [cid, b] of buttons) {
      const kept = keeps.get(cid)
      if (kept) b.textContent = `Unpin (kept only by this pin in ${store}: ${sizeText(kept)})`
      b.disabled = false
    }
  }
  if (buttons.size && roots && typeof roots.then === 'function') {
    for (const b of buttons.values()) b.disabled = true
    roots.then(label, () => label(null))
  } else if (buttons.size) label(roots)
  return list
}

/**
 * Pin/Unpin on any subject and Release/Restore content on a run (ticket 21
 * answer 6, DESIGN.md §15), one component for the run's Storage tab, the
 * Selection's Pins, and the item, file and collection pages. Each current pin
 * is a claim tag (who, "pinned", the note, how long ago) with its own Unpin;
 * the badges say "pinned", "content released", "hidden but pinned". Pin names
 * the bytes it keeps before the click (`Pin 5.29 MB in roblab`) and waits for
 * both a reason and those bytes. Offered where a Selection's own writes are
 * (`ctx.write.available && ctx.write.here`); elsewhere the badges and pins
 * still show and the controls are `unavailableNote`, naming this page's kind.
 *
 * `size` is the bytes a pin keeps: `{ bytes, partial }`, a promise of one, or
 * null when the page shows no figure (a released run, whose files a sweep may
 * already have taken). A size that cannot be read leaves the figure off and
 * the button usable. `pinnedBy` (Explorer.pinnedBy rows, or a promise of
 * them) puts what others already pin here under Pin, leaving out
 * `ctx.viewer`: "<bytes> already pinned here by <name>" for one person,
 * "Already pinned here: <name> <bytes>, ..." for more (rows overlap, so they
 * are never summed). `roots` (the subject's roots from Explorer.rootStorage, or a
 * promise of them) gives each Unpin what only that pin keeps; null on a lean snapshot or a
 * tail subject, where Unpin is plain, and Unpin waits for a promise as Pin
 * waits for its size. `onRelease` opens the run page's Release confirmation:
 * a run's Release always goes through one, so without it no Release is
 * offered. Ids and data-* (the Gate's B.retain step and the tests):
 * #pin-note, #pin, #release, #restore, [data-retention], [data-pin],
 * [data-claim], [data-unpin], [data-retention-status], [data-badge],
 * [data-pinned-by].
 */
export function retentionPanel(subject, state, ctx, { isRun = false, kind = 'page', href, size = null, intro = null, footer = true,
  pinnedBy = null, roots = null, onRelease = null } = {}) {
  const store = ctx.store?.name ?? 'this store'
  const status = h('p', { class: 'muted', 'data-retention-status': '' })
  const can = ctx.write.available && ctx.write.here
  const badges = [
    isRun && state.released ? h('span', { class: 'badge', 'data-badge': 'content-released' }, claimTag({ assertedBy: state.releaseClaim?.asserted_by, verb: 'released', at: state.releaseClaim?.timestamp })) : null,
    isRun && state.hidden && state.pinned ? h('span', { class: 'badge', 'data-badge': 'hidden-but-pinned' }, 'hidden but pinned') : null,
    state.pinned ? h('span', { class: 'badge', 'data-badge': 'pinned' }, 'pinned') : null,
  ].filter(Boolean)
  // What only each pin keeps (pinKeeps, R4): Unpin waits for the roots, then shows the figure or stays plain.
  const pins = pinsList(state, { can, roots, store, onUnpin: (cid, e) => ctx.write.run(status, () => ctx.write.writer.unpin(subject, cid), e.currentTarget) })
  const note = h('input', { id: 'pin-note', type: 'text', maxlength: '256', placeholder: 'Why keep it? e.g. figure 3 of the paper' })
  // Known once the size has answered (or there is no figure to wait for).
  let known = !size || typeof size.then !== 'function'
  let kept = known ? size : null
  const pinLabel = (s) => `Pin ${s && s.bytes !== null && s.bytes !== undefined ? `${sizeText(s)} ` : ''}in ${store}`
  const pin = h('button', { type: 'button', id: 'pin', class: 'btn primary', disabled: true,
    onclick: (e) => ctx.write.run(status, () => ctx.write.writer.pin(subject, note.value.trim()), e.currentTarget) }, pinLabel(kept))
  const ready = () => { pin.disabled = !(known && (note.value ?? '').trim()) }
  note.addEventListener('input', ready)
  // Others' pins on this subject, the viewer's own left out.
  const others = h('p', { class: 'muted small', 'data-pinned-by': '', hidden: true })
  let pinners = null
  const drawOthers = () => {
    const rows = (pinners ?? []).filter(r => !samePrincipal(r.assertedBy, ctx.viewer))
    if (!rows.length) { others.setAttribute('hidden', ''); return }
    others.textContent = pinnedByText(rows, ctx.viewer, !!kept?.partial)
    others.removeAttribute('hidden')
  }
  settledValue(pinnedBy, (rows) => { pinners = rows; drawOthers() })
  if (!known) {
    size.then((s) => { kept = s; pin.textContent = pinLabel(s) }, () => { pin.textContent = pinLabel(null) })
      .finally(() => { known = true; ready(); if (pinners) drawOthers() })
  }
  const release = !isRun ? null : state.released
    ? h('button', { type: 'button', class: 'btn', id: 'restore', onclick: (e) => ctx.write.run(status, () => ctx.write.writer.restore(subject, state.retainClaims), e.currentTarget) }, 'Restore content')
    : onRelease ? h('button', { type: 'button', class: 'btn', id: 'release', onclick: () => onRelease() }, 'Release content…') : null
  return h('section', { 'data-retention': subject, class: 'retention' },
    intro ? h('p', { class: 'small' }, intro) : null,
    badges.length ? h('p', {}, badges) : null,
    pins,
    can ? [
      isRun ? h('p', { class: 'muted small' }, state.released
        ? "Restoring holds whatever of this run's files are still here; files a sweep already deleted don't come back."
        : "Releasing keeps this run's lineage and lets a sweep reclaim its files after the grace period. Pinned items stay.") : null,
      h('div', { class: 'row pin-form' }, note, pin, release), others]
      : unavailableNote(ctx, isRun ? 'Pin, release and restore' : 'Pin', `Open this ${kind} there`, href),
    status,
    footer ? h('p', { class: 'muted small' }, state.pinned ? `Pinned in ${store}. Pins apply to this store only.`
      : `Nothing here is pinned in ${store} yet. Pins apply to this store only.`) : null)
}

/**
 * What `retentionPanel` shows beside Pin and Unpin for `subject`, from the
 * snapshot's storage figures: `pinnedBy`, who already pins it here and how
 * much, and `roots`, its pin roots for "kept only by this pin". Both are
 * promises; a lean snapshot, a tail subject or a failed read gives
 * none. A pin, release or deletion Claim since the snapshot makes both stale
 * (null); a run or Selection logged since moves what a pin keeps alone, so
 * `roots` is null, but not who pins what (Explorer.storageChanges).
 */
export function holds(ex, subject) {
  const changed = ex.storageChanges()
  return {
    pinnedBy: changed.claims || !ex.pinnedBy ? null : Promise.resolve(ex.pinnedBy(subject)).catch(() => []),
    roots: changed.claims || changed.logged || !ex.rootStorage ? null
      : Promise.resolve(ex.rootStorage(subject)).then(s => s?.roots ?? null, () => null),
  }
}

/** `retentionPanel` in a panel titled `Keep this <kind>`, with a Pinned badge in its head when pinned: the item and file pages. */
export function keepPanel(subject, state, ctx, { size = null, intro = null, kind = 'item', href, title = `Keep this ${kind}`, pinnedBy = null, roots = null } = {}) {
  return panel({ title, actions: state.pinned ? [durability('pinned', 'Pinned')] : [] },
    retentionPanel(subject, state, ctx, { kind, href, size, intro, pinnedBy, roots }))
}

export const flag = (text) => h('span', { class: 'warn' }, ` (${text})`)

/** Shows `el` when `on`, else hides it, by its `hidden` attribute. */
export const show = (el, on) => { if (on) el.removeAttribute('hidden'); else el.setAttribute('hidden', '') }

/** The cids of claim tags (`[{ cid, ... }]`): the Claims an edit or a removal supersedes. */
export const cids = (tags) => tags.map(t => t.cid)

const pickLabel = (kind) => (kind === 'selection' ? 'Pick this Selection' : 'Pick this item')

/** A Pick button once its entry is in the tray: "Picked", disabled, and its aria-label says so. */
export function markPicked(button) {
  button.textContent = 'Picked'
  button.setAttribute('aria-label', `${pickLabel(button.dataset.kind)} (already picked)`)
  button.disabled = true
}

/**
 * Adds an item or a Selection to the tray (spec section 5.6, [data-pick]):
 * "Pick" (or "Pick this Selection"), with an aria-label, then "Picked". The
 * one pick button for the item page, item rows and the items grid; `size:
 * 'sm'` for a table row.
 */
export function pickButton(ctx, { address, via = [], kind = 'item', size = 'md' }) {
  const inTray = ctx.tray.has(address)
  const node = h('button', { type: 'button', class: size === 'sm' ? 'btn sm' : 'btn', 'data-pick': address, 'data-kind': kind, 'data-via': via.join(' '),
    'aria-label': pickLabel(kind),
    onclick: (event) => {
      ctx.tray.add({ address, via, kind })
      ctx.trayChanged()
      markPicked(event.currentTarget)
    } }, kind === 'selection' ? 'Pick this Selection' : 'Pick')
  if (inTray) markPicked(node)
  return node
}

export { shortCid } from '../icons.js'

/**
 * What an item is called: its sample id when its Meta Map has one, else the
 * label an item row leads with (`labelText` over `paths`, or the file names),
 * else null (no preview yet, or the preview failed: the caller shows the short
 * CID). Used by the Picked panel, #/compose and the Selection page.
 */
export function itemName(preview, paths = []) {
  if (!preview || preview.error) return null
  for (const path of SAMPLE_PATHS) {
    const e = preview.pairs?.find(p => p.path === path)
    if (e && e.values.length === 1 && e.values[0] !== null && e.values[0] !== '') return String(e.values[0])
  }
  return labelText(preview, paths) || null
}

/** `<run_name> / <output>` (DESIGN.md §16 milestone 3 decision 11), or the collection CID when this member does not know its run. */
export const runLabelText = (label, collectionCid) => (label ? `${label.run_name ?? 'unnamed run'} / ${label.output}` : collectionCid)

/**
 * A collection shown by its run: the short CID until Explorer.runLabel
 * answers, then `<run_name> / <output>` linking to the run, the collection
 * CID in `title`. With `completionCid` the placeholder already links there.
 */
export function runLabelNode(ex, collectionCid, completionCid = null) {
  const node = h('span', { title: collectionCid }, completionCid
    ? ref('run', h('code', { class: 'cid' }, shortCid(completionCid)), `#/run/${completionCid}`, { cid: completionCid })
    : address(collectionCid))
  ex.runLabel(collectionCid).then((label) => {
    if (!label) return
    const c = label.completion
    node.replaceChildren(
      ref('run', label.run_name ?? 'unnamed run', c ? `#/run/${c}` : null, { tone: runTone(label), cid: c }), ' / ',
      ref('output', label.output, c ? `#/items/${c}/${encodeURIComponent(label.output)}?where=%5B%5D` : null, { cid: collectionCid }))
  }, () => {})
  return node
}

/**
 * "Add all N to the tray" (DESIGN.md §16 milestone 3 decision 12): `items` when the
 * caller holds every one (query results), else every item of the collection
 * from the model; each picked with the collection as via, one storage
 * write, no preview fetched.
 */
export async function pickAll(ex, tray, { items = null, collectionCid }) {
  const all = items ?? await ex.allItems(collectionCid)
  const via = collectionCid ? [collectionCid] : []
  tray.addMany(all.map(address => ({ address, via, kind: 'item' })))
  return all.length
}

/** "Added N to the tray.", and the unsaved note when storage refused the write. */
export const added = (n, tray) => [`Picked ${n}.`, trayNote(tray)].filter(Boolean).join(' ')

export function markInTray(list, addresses) {
  const only = addresses ? new Set(addresses) : null
  for (const button of list.querySelectorAll('[data-pick]')) {
    if (only && !only.has(button.dataset.pick)) continue
    markPicked(button)
  }
}

/** Calls `fn` once on the next frame however often it is asked in between. */
export function nextFrame(fn) {
  let queued = false
  const later = globalThis.requestAnimationFrame ?? ((f) => setTimeout(f, 16))
  return () => {
    if (queued) return
    queued = true
    later(() => { queued = false; fn() })
  }
}

// Each row's parts, kept beside its node so a list can redraw it.
const rowOf = new WeakMap()

/**
 * One item row (visual language spec 5.1): `lead` (a checkbox), the item
 * icon and its label, which links to the item page once the preview
 * arrives, `action` at the right; beneath, its files, a from line when
 * `fromLabel` is set (each via named by its run and output, `perVia(via)`
 * after it), and the item's address. The preview is drawn by `watchRows`; a
 * row at `index` past `previews.cap` waits for "show details".
 */
export function itemRow(ex, ctx, { address: itemCid, via = [], previews, target = null, index = 0, attrs = {}, lead = null, action = null,
  fromLabel = null, noVia = 'a query', perVia = () => null }) {
  const collection = via[0] ?? null
  const row = { address: itemCid, collection, target, drawn: undefined, href: `#/item/${collection ?? '-'}/${itemCid}`,
    label: h('strong', { class: 'row-label' }), files: h('span', { class: 'row-files' }) }
  const loading = () => h('span', { class: 'muted' }, 'loading...')
  row.label.append(index < previews.cap ? loading()
    : h('button', { type: 'button', onclick: () => { row.label.replaceChildren(loading()); previews.ask(collection, itemCid) } }, 'show details'))
  const fromLine = fromLabel === null ? null : h('span', { class: 'row-via muted' }, ` ${fromLabel} `,
    via.length ? via.map((v, i) => [i ? '; ' : null, runLabelNode(ex, v), ' ', perVia(v)]) : [noVia, ' ', perVia('-')])
  const li = h('li', { class: 'row', ...attrs },
    h('div', { class: 'row-head' }, lead, h('a', { href: row.href, class: 'row-open', title: 'Open the item' }, icon('item')), row.label, h('span', { class: 'row-action' }, action)),
    h('div', { class: 'row-sub' }, row.files, fromLine, ' ', address(itemCid)))
  rowOf.set(li, row)
  return li
}

export function fillRow(row, preview, paths) {
  if (preview.error) {
    row.label.replaceChildren(h('span', { class: 'muted', title: preview.message ?? '' },
      preview.error === 'block_missing' ? shortCid(row.address) : `no preview (${preview.error})`))
    return
  }
  row.label.replaceChildren(link(row.href, labelText(preview, paths) || h('span', { class: 'muted' }, 'no Meta Map')))
  const dirs = new Set(preview.dirs ?? [])
  row.files.replaceChildren(...preview.files.flatMap((f, i) => [...(i ? [' '] : []), ref(dirs.has(f) ? 'directory' : 'file', h('code', {}, f))]))
}

/**
 * Asks for the first rows' previews and redraws rows as they arrive, one
 * frame at a time; the label paths are chosen across the list's loaded
 * previews, so every row is relabelled when they change. Once the list has
 * been shown and is gone (another route rendered), the queue is dropped.
 */
export function watchRows(list, previews, lis) {
  const rows = lis.map(li => rowOf.get(li))
  previews.askFirst(rows.map(r => ({ collection: r.collection, item: r.address })))
  let shown = false
  let lastPaths = null
  const redraw = nextFrame(() => {
    if (list.isConnected) shown = true
    else if (shown) { off(); previews.cancel(); return }
    const loaded = rows.map(r => previews.get(r.address)).filter(p => p?.pairs)
    const { duplicates } = itemTable(loaded)
    const paths = labelPaths(loaded.map(p => ({ ...p, pairs: withoutDuplicates(p.pairs, duplicates) })))
    const key = JSON.stringify(paths)
    const relabel = key !== lastPaths
    lastPaths = key
    for (const r of rows) {
      const p = previews.get(r.address)
      if (p === undefined || (p === r.drawn && !relabel)) continue
      r.drawn = p
      fillRow(r, p, paths)
    }
  })
  const off = previews.onChange(redraw)
  redraw()
}

/**
 * The labelled list for query results and collection pages: a checkbox per
 * row with "Add checked (k)", "Add all N to the tray" ([data-pick-all]) for
 * the whole query or collection, and each row's own Add. Every pick records
 * `collectionCid` as via (decision 12). Pills link to query 3 over
 * `completion` and `output` with `where` plus the pair.
 */
export function itemRows(ex, ctx, { items, total = items.length, collectionCid = null, completion = null, output = null, where = [],
  previews, results = false }) {
  const via = collectionCid ? [collectionCid] : []
  const target = { completion, output, where }
  const checked = new Set()
  const status = h('span', { class: 'muted' })
  const addChecked = h('button', { type: 'button', disabled: true }, 'Pick checked (0)')
  const showChecked = () => {
    addChecked.textContent = `Pick checked (${checked.size})`
    addChecked.disabled = checked.size === 0
  }
  const lis = items.map((address, index) => itemRow(ex, ctx, { address, via, previews, target, index,
    attrs: results ? { 'data-item-result': address } : {},
    lead: h('input', { type: 'checkbox', 'aria-label': 'check this item', onchange: (event) => {
      if (event.currentTarget.checked) checked.add(address)
      else checked.delete(address)
      showChecked()
    } }),
    action: pickButton(ctx, { address, via, size: 'sm' }) }))
  const list = h('ol', { class: 'rows' }, lis)
  addChecked.addEventListener('click', () => {
    const picked = [...checked]
    ctx.tray.addMany(picked.map(address => ({ address, via, kind: 'item' })))
    ctx.trayChanged()
    markInTray(list, picked)
    for (const box of list.querySelectorAll('input[type=checkbox]')) box.checked = false
    checked.clear()
    showChecked()
    status.textContent = added(picked.length, ctx.tray)
  })
  const addAll = h('button', { type: 'button', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': total, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    status.textContent = total > items.length ? `Reading all ${total} items...` : ''
    try {
      const n = await pickAll(ex, ctx.tray, { items: total === items.length ? items : null, collectionCid })
      ctx.trayChanged()
      markInTray(list, null)
      status.textContent = added(n, ctx.tray)
    } catch (e) {
      status.replaceChildren(errorNode(e))
    } finally {
      button.disabled = false
    }
  } }, `Pick all ${total}`)
  watchRows(list, previews, lis)
  return h('div', { class: 'item-rows' }, h('p', { class: 'row-actions' }, addChecked, ' ', addAll, ' ', status), list)
}

/** `a / b / c`: each part `{ text, href? }`, linked when it has an href. */
export function crumbs(...parts) {
  return h('nav', { class: 'crumbs', 'aria-label': 'breadcrumb' },
    parts.filter(Boolean).map((p, i) => [i ? ' / ' : null, p.href ? link(p.href, p.text) : h('span', {}, p.text)]))
}

/**
 * `pipeline / run / ...tail` for a run, named from its blocks (Explorer.runIdentity,
 * no snapshot query). Shows `run / ...tail` until the names arrive, and keeps
 * that when they cannot be read.
 */
export function runCrumbs(ex, completionCid, tail = []) {
  const node = crumbs({ text: 'run', href: `#/run/${completionCid}` }, ...tail)
  const lookup = ex.runIdentity?.(completionCid)
  lookup?.then((id) => {
    node.replaceChildren(...crumbs(
      id.pipeline ? { text: id.pipeline, href: `#/pipeline/${enc(id.pipeline)}` } : null,
      { text: id.run_name ?? 'run', href: `#/run/${completionCid}` }, ...tail).childNodes)
  }, () => {})
  return node
}

/**
 * A collection's Output Index File with each Publish Coordinate replaced by
 * its address (Explorer.stableIndex), built and saved on click: the button,
 * then `[data-index-status]` saying what was left as Nextflow wrote it.
 * `ctx.download(name, text)` stands in for the browser's save in tests.
 */
export function stableIndexControl(ex, ctx, collectionCid, label, className = 'link') {
  const status = h('span', { class: 'muted', 'data-index-status': '' })
  const button = h('button', { type: 'button', class: className, 'data-index-stable': '',
    title: 'Each file named by its address, as fromStore gives it',
    onclick: async () => {
      status.className = 'muted'
      status.textContent = ' preparing…'
      try {
        const r = await ex.stableIndex(collectionCid)
        ;(ctx.download ?? saveText)(r.name, r.text)
        status.textContent = r.unmatched ? ` ${r.unmatched} ${r.unmatched === 1 ? 'path' : 'paths'} left as Nextflow wrote ${r.unmatched === 1 ? 'it' : 'them'}` : ''
      } catch (e) {
        status.className = 'warn'
        status.textContent = ` Could not prepare the index file: ${e?.message ?? e}`
      }
    } }, label)
  return [button, status]
}

/**
 * How a run or Selection came into this store (Explorer.transfersOf): one line
 * per Transfer, newest first, each with the Transfer block's seal and short
 * cid linking to the block. Null when nothing was copied here, or when the
 * Explorer has no transfersOf. A query that fails is a muted line saying so (never silence:
 * it is not "nothing was copied here").
 */
export async function transfersNode(ex, cid, ctx) {
  let transfers
  try {
    transfers = await ex.transfersOf?.(cid)
  } catch (e) {
    return h('p', { class: 'muted', 'data-transfers-unreadable': cid, title: String(e?.message ?? e) }, "Couldn't read how this got here.")
  }
  if (!transfers?.length) return null
  const now = ex.now()
  return h('div', { 'data-transfers': cid }, transfers.map(t => h('p', { 'data-transfer': t.cid },
    'Copied here from ', h('b', {}, t.from ?? 'another store'), ' by ', avatar(t.assertedBy), ' ', whoText(t.assertedBy, ctx?.viewer),
    t.at ? [' · ', ago(t.at, now)] : null,
    t.depth ? ` · depth ${t.depth}` : null, ' · ',
    link(`#/block/${t.cid}`, seal(t.cid, { size: 14 }), ' ', shortCid(t.cid)))))
}

/** A muted line per recorded copy whose Transfer block could not be read, each linking its block; null when there are none. */
export function unreadableTransfersNote(ex) {
  const bad = ex.unreadableTransfers?.() ?? []
  if (bad.length === 0) return null
  return h('div', { 'data-unreadable-transfers': '' }, bad.map(t => h('p', { class: 'muted', 'data-unreadable-transfer': t.cid },
    'A copy was recorded here but its record could not be read: ', link(`#/block/${t.cid}`, seal(t.cid, { size: 14 }), ' ', shortCid(t.cid)))))
}
