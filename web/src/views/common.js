// web/src/views/common.js
// What more than one view draws (DESIGN.md §15–16): errors, tables, pagers,
// the retention panel, picks, item rows, breadcrumbs. The data-* attributes
// are the contract the Gate reads; the rest is for people.
import { h, link, copyOutcome, saveText } from '../html.js'
import { address, icon, ref, shortCid } from '../icons.js'
import { Previews } from '../previews.js'
import { labelPaths, labelText } from '../pairs.js'
import { trayNote } from '../tray.js'
import { itemTable, withoutDuplicates } from '../table.js'
import { runTone } from '../words.js'

export const enc = encodeURIComponent

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
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
 * Release/Restore content on a run, and Pin/Unpin on any subject (ticket 21
 * answer 6, DESIGN.md §15): the "pinned", "content released" and "hidden but
 * pinned" badges; a pin note form and each current pin with its own Unpin;
 * Release/Restore content on a run only. Offered where `actions()` offers a
 * Selection's own writes (`ctx.write.available && ctx.write.here`);
 * elsewhere the badges still show and the controls are `unavailableNote`,
 * naming this page's own kind and linking to it in the writable member.
 */
export function retentionPanel(subject, state, ctx, { isRun = false, kind = 'page', href } = {}) {
  const status = h('p', { class: 'muted', 'data-retention-status': '' })
  const can = ctx.write.available && ctx.write.here
  const badges = [
    isRun && state.released ? h('span', { class: 'badge', 'data-badge': 'content-released' }, 'content released') : null,
    isRun && state.hidden && state.pinned ? h('span', { class: 'badge', 'data-badge': 'hidden-but-pinned' }, 'hidden but pinned') : null,
    state.pinned ? h('span', { class: 'badge', 'data-badge': 'pinned' }, 'pinned') : null,
  ]
  const pins = state.pins.length ? h('ul', {}, state.pins.map(p => h('li', { 'data-pin': '', 'data-claim': p.cid }, p.note, ' ',
    can ? h('button', { type: 'button', 'data-unpin': p.cid,
      onclick: (e) => ctx.write.run(status, () => ctx.write.writer.unpin(subject, p.cid), e.currentTarget) }, 'Unpin') : null))) : null
  const note = h('input', { id: 'pin-note', type: 'text', maxlength: '256', placeholder: 'why keep this? e.g. figure 3' })
  const pin = h('button', { type: 'button', id: 'pin', disabled: true,
    onclick: (e) => ctx.write.run(status, () => ctx.write.writer.pin(subject, note.value.trim()), e.currentTarget) }, 'Pin')
  note.addEventListener('input', () => { pin.disabled = !note.value.trim() })
  const release = isRun ? (state.released
    ? h('button', { type: 'button', id: 'restore', onclick: (e) => ctx.write.run(status, () => ctx.write.writer.restore(subject, state.retainClaims), e.currentTarget) }, 'Restore content')
    : h('button', { type: 'button', id: 'release', onclick: (e) => ctx.write.run(status, () => ctx.write.writer.release(subject, state.retainClaims), e.currentTarget) }, 'Release content')) : null
  return h('section', { 'data-retention': subject },
    h('p', {}, badges),
    pins,
    can ? [
      isRun ? h('p', { class: 'muted' }, "Releasing keeps this run's lineage and lets a sweep reclaim its files after the grace period. Pinned items stay.") : null,
      h('p', {}, release, ' ', note, ' ', pin),
      status]
      : unavailableNote(ctx, isRun ? 'Pin, release and restore' : 'Pin', `Open this ${kind} there`, href))
}

export const flag = (text) => h('span', { class: 'warn' }, ` (${text})`)

/** Adds an item or a Selection to the tray (spec section 5.6, [data-pick]). */
export function pickButton(ctx, { address, via = [], kind = 'item' }) {
  const inTray = ctx.tray.has(address)
  return h('button', { type: 'button', 'data-pick': address, 'data-kind': kind, 'data-via': via.join(' '), disabled: inTray,
    onclick: (event) => {
      ctx.tray.add({ address, via, kind })
      ctx.trayChanged()
      event.currentTarget.textContent = 'Picked'
      event.currentTarget.disabled = true
    } }, inTray ? 'Picked' : kind === 'selection' ? 'Add this Selection to picked' : 'Add to picked')
}

export { shortCid } from '../icons.js'

/** `<run_name> / <output>` (DESIGN.md §16 milestone 3 decision 11), or the collection CID when this member does not know its run. */
export const runLabelText = (label, collectionCid) => (label ? `${label.run_name ?? 'unnamed run'} / ${label.output}` : collectionCid)

/**
 * A collection shown by its run: the short CID until Explorer.runLabel
 * answers, then `<run_name> / <output>` linking to the run, the collection
 * CID in `title`. With `completionCid` the placeholder already links there.
 */
export function runLabelNode(ex, collectionCid, completionCid = null) {
  const node = h('span', { title: collectionCid }, completionCid
    ? link(`#/run/${completionCid}`, h('code', { class: 'cid' }, shortCid(completionCid)))
    : address(collectionCid))
  ex.runLabel(collectionCid).then((label) => {
    if (!label) return
    const c = label.completion
    node.replaceChildren(
      ref('run', label.run_name ?? 'unnamed run', c ? `#/run/${c}` : null, { tone: runTone(label) }), ' / ',
      ref('output', label.output, c ? `#/items/${c}/${encodeURIComponent(label.output)}?where=%5B%5D` : null))
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
    button.textContent = 'Picked'
    button.disabled = true
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
    action: pickButton(ctx, { address, via }) }))
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
