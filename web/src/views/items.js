// web/src/views/items.js
// #/items/<completion>/<output>?where=... (explorer layout B spec §5.4): query 3
// as a table whose columns are the Meta Map keys that tell items apart.
// Nothing here reads the snapshot but Explorer.items (Gate assertion 2).
import { h } from '../html.js'
import { icon } from '../icons.js'
import { Previews } from '../previews.js'
import { cellOf, cellSlots, emptyColumns, fieldChoices, fileColumns, itemTable, typedCondition } from '../table.js'
import { filterHref, valueText } from '../pairs.js'
import { humanBytes, leafReasonText } from '../words.js'
import { enc, nextFrame, pickAll, runCrumbs } from './common.js'

function parseWhere(whereText) {
  try {
    const where = JSON.parse(whereText)
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && w.length === 3)) throw new Error('not a list of [path, type, value]')
    return where
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[path, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
}

const routeOf = (target, where) => `#/items/${target.completion}/${enc(target.output)}?where=${enc(JSON.stringify(where))}`

const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`

/**
 * The filter bar (option A of `filters-v1.html`): one chip per condition with
 * a remove link, "Add filter" opening a picker of the loaded items' fields and
 * then their values (each a filter link, with how many items hold it, or a
 * typed value on Enter), "Clear all", and the Pick all button at the end.
 * `choices()` is `fieldChoices` over the previews loaded so far.
 */
function filterBar(target, { choices, end }) {
  const picker = h('div', { class: 'fb-picker', role: 'dialog', 'aria-label': 'add a filter', 'data-filter-picker': '', 'data-open': 'false',
    onclick: (e) => e.stopPropagation?.() })
  const close = () => { picker.dataset.open = 'false' }
  const onKey = (e) => { if (e.key === 'Escape') close() }
  const showFields = () => {
    const all = choices()
    const list = h('div', { class: 'fb-list' })
    const draw = (q) => list.replaceChildren(...all.filter(f => f.path.includes(q)).map(f =>
      h('button', { type: 'button', class: 'fb-item', 'data-field': f.path, onclick: () => showValues(f) },
        h('span', { class: 'fb-path' }, f.path), h('span', { class: 'fb-count' }, plural(f.values.length, 'value')))))
    draw('')
    const search = h('input', { class: 'fb-input', type: 'search', placeholder: 'Search fields…', 'aria-label': 'search fields', 'data-filter-search': '',
      oninput: (e) => draw((e.currentTarget.value ?? '').trim()), onkeydown: onKey })
    picker.replaceChildren(search, all.length ? list : h('p', { class: 'muted fb-note' }, 'No fields yet: the items are still loading.'))
    search.focus?.()
  }
  const showValues = (f) => {
    const error = h('p', { class: 'fb-error', 'data-filter-error': '' })
    const input = h('input', { class: 'fb-input', placeholder: 'Or type a value and press Enter', 'aria-label': `a value for ${f.path}`, 'data-filter-value': '',
      onkeydown: (e) => {
        if (e.key !== 'Enter') return onKey(e)
        try {
          const [path, type, value] = typedCondition(f, e.currentTarget.value ?? '')
          location.hash = filterHref(target, path, type, value)
        } catch (err) {
          error.textContent = err.message
        }
      } })
    picker.replaceChildren(
      h('div', { class: 'fb-head' },
        h('button', { type: 'button', class: 'btn btn-ghost fb-back', 'aria-label': 'back to the fields', 'data-filter-back': '', onclick: showFields }, '‹'),
        h('span', { class: 'fb-path' }, f.path), h('span', { class: 'fb-type' }, f.type)),
      input, error,
      h('div', { class: 'fb-list' }, f.values.map(v => h('a', { class: 'fb-item', 'data-value': String(v.value), href: filterHref(target, f.path, v.type, v.value) },
        h('span', {}, valueText(v.type, v.value)), h('span', { class: 'fb-count' }, plural(v.count, 'item'))))))
    input.focus?.()
  }
  const add = h('button', { type: 'button', class: 'btn', 'aria-haspopup': 'dialog', 'data-add-filter': '', onclick: (e) => {
    e.stopPropagation?.()
    if (picker.dataset.open === 'true') return close()
    showFields()
    picker.dataset.open = 'true'
  } }, h('span', { class: 'btn-icon', 'aria-hidden': 'true' }, '+'), 'Add filter')
  const chip = ([p, t, v], i) => h('span', { class: 'fb-chip', 'data-chip': p, title: `${p} = ${valueText(t, v)}` },
    h('span', { class: 'fb-chip-key' }, `\u200e${p}`), h('span', { class: 'fb-chip-op' }, ' = '), h('span', { class: 'fb-chip-val' }, valueText(t, v)),
    h('a', { class: 'fb-chip-x', href: routeOf(target, target.where.filter((_, j) => j !== i)), 'aria-label': `remove ${p}`, 'data-chip-remove': '' }, '×'))
  const bar = h('div', { class: 'fb-bar', 'data-filter-bar': '' }, ...target.where.map(chip),
    h('span', { class: 'fb-anchor' }, add, picker),
    target.where.length ? h('a', { class: 'btn btn-ghost', href: routeOf(target, []), 'data-clear-filters': '' }, 'Clear all') : null,
    h('span', { class: 'fb-spacer' }), ...end)
  // A click anywhere else closes the picker; the listener goes once the page has.
  const onDoc = () => { if (!bar.isConnected) return document.removeEventListener?.('click', onDoc); close() }
  globalThis.document?.addEventListener?.('click', onDoc)
  return bar
}

function valueNode(entry, target) {
  if (!entry) return h('span', { class: 'muted' }, '—')
  if (entry.values.length !== 1) return h('span', {}, entry.values.map(v => v ?? 'null').join(', '))
  const type = entry.types[0]
  const value = entry.values[0]
  const href = filterHref(target, entry.path, type, value)
  const text = valueText(type, value)
  return href ? h('a', { href, title: `only items where ${entry.path} is ${text}` }, text) : h('span', {}, text)
}

export async function items(ex, completionCid, output, whereText, ctx) {
  const where = parseWhere(whereText)
  let found
  try {
    found = await ex.items(completionCid, output, where, ctx.progress)
  } catch (e) {
    if (e.constructor?.name === 'PredicateError') throw Object.assign(e, { code: 'bad_predicate' })
    throw e
  }
  const { items: results, collection: collectionCid } = found
  const via = collectionCid ? [collectionCid] : []
  if (collectionCid) ctx.sources?.set(collectionCid, { completion: completionCid, output })
  const target = { completion: completionCid, output, where }
  const checked = new Set(results)
  const view = { completion: completionCid, output, where, count: results.length, checked: results.length,
    pickChecked: () => {
      ctx.tray.addMany([...checked].map(address => ({ address, via, kind: 'item' })))
      ctx.trayChanged()
    } }
  ctx.use?.(view)
  ctx.mark?.({ run: completionCid })
  const report = () => { view.checked = checked.size; ctx.use?.(view) }
  const previews = new Previews(ex)
  const status = h('span', { class: 'muted fb-status' })
  const pickAllButton = h('button', { type: 'button', class: 'btn', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': results.length, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    try {
      const n = await pickAll(ex, ctx.tray, { items: results, collectionCid })
      ctx.trayChanged()
      status.textContent = `Picked ${n}.`
    } finally {
      button.disabled = false
    }
  } }, `Pick all ${results.length}`)
  return h('section', {},
    runCrumbs(ex, completionCid, [{ text: output }]),
    h('h1', {}, output, h('span', { class: 'muted' }, ` · ${results.length} item${results.length === 1 ? '' : 's'}`)),
    filterBar(target, { choices: () => fieldChoices(results.map(a => previews.get(a))), end: results.length ? [status, pickAllButton] : [] }),
    results.length === 0 ? h('p', { class: 'muted' }, 'No items match.')
      : itemsTable(ex, ctx, { results, via, target, checked, onCheck: report, previews }))
}

/**
 * The items table's one tooltip (spec §6.3, amended): shown at once on hover or
 * focus, where the native `title` waits about a second. `lines` are
 * `[class, text]`; the first is the field path.
 */
function pillTip() {
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

/** The row's "+" pill (items file columns spec §6.5): adds the item to the tray with its collection, then shows a tick. */
function pickPill(ctx, { address, via }, tip) {
  let picked = ctx.tray.has(address)
  const button = h('button', { type: 'button', class: `file-pill row-pill${picked ? ' picked' : ''}`, 'data-pick': address, 'data-kind': 'item',
    'data-via': via.join(' '), disabled: picked, ...tip.on(() => [['tip-path', picked ? 'Picked' : 'Add to picked']]),
    onclick: (event) => {
      ctx.tray.add({ address, via, kind: 'item' })
      ctx.trayChanged()
      picked = true
      const b = event.currentTarget
      b.textContent = '✓'
      b.disabled = true
      b.classList.add('picked')
      b.setAttribute('aria-label', 'Picked')
    } }, picked ? '✓' : '+')
  return button
}

/** The row's braces pill: opens the item page, with the item's address in the tip. */
function openPill(href, address, tip) {
  return h('a', { class: 'file-pill row-pill', href, 'data-open-item': '', 'data-address': address,
    ...tip.on([['tip-path', 'Open item'], ['tip-name', address]]) }, icon('item'))
}

/** A stored file as a fixed-size file-pill linking to its block page (items file columns spec §6.3); the tip gives path, name and size. */
function pill(s, tip) {
  const lines = [['tip-path', s.path], ['tip-name', s.name && (s.dir ? `${s.name}/` : s.name)], ['tip-size', humanBytes(s.size)]].filter(([, t]) => t)
  return h('a', { class: 'file-pill', href: `#/block/${s.address}`, 'data-pill': s.path, ...tip.on(lines) }, icon(s.dir ? 'directory' : 'file'))
}

/** A cell's address-less slots as one dashed file-pill showing their count (spec §6.2, amended); the tip lists each path and reason. */
function emptyPill(slots, tip) {
  const lines = [['tip-path', `${slots.length} without a stored file`], ...slots.map(s => ['tip-reason', `${s.path}: ${leafReasonText(s.reason)}`])]
  return h('span', { class: 'file-pill file-pill-empty', role: 'img', tabindex: '0', 'data-pill-empty': String(slots.length), ...tip.on(lines) }, String(slots.length))
}

function itemsTable(ex, ctx, { results, via, target, checked, onCheck, previews }) {
  const head = h('tr', {})
  const body = h('tbody', {})
  const constantLine = h('p', { class: 'constant muted' })
  const emptyLine = h('p', { class: 'constant muted', 'data-empty-columns': '' })
  const table = h('table', { class: 'items-table', 'data-columns': '[]' }, h('thead', {}, head), body)
  const all = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check every item', onchange: (event) => {
    const on = event.currentTarget.checked
    for (const r of rows) { r.box.checked = on; if (on) checked.add(r.address); else checked.delete(r.address) }
    onCheck()
  } })
  const rows = results.map((address, index) => {
    const box = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check this item', onchange: (event) => {
      if (event.currentTarget.checked) checked.add(address)
      else checked.delete(address)
      onCheck()
    } })
    return { address, index, box, tr: h('tr', { 'data-item-result': address, 'data-preview-for': address }), drawn: undefined }
  })
  body.append(...rows.map(r => r.tr))
  let shape = null
  let shapeKey = null
  let files = []
  const span = () => String(Math.max(1, shape.columns.length + files.length))
  const tip = pillTip()
  const fileClass = (i) => (i === files.length - 1 ? 'file-cell file-last' : 'file-cell')
  const fileCell = (p, key, i, open) => {
    const { shown, more, empty } = cellSlots(p, key)
    return h('td', { class: fileClass(i), 'data-file-cell': key }, shown.length + empty.length === 0 ? h('span', { class: 'muted' }, '—')
      : [...shown.map(s => pill(s, tip)), more ? h('a', { class: 'file-pill-more', href: open }, `+${more}`) : null, empty.length ? emptyPill(empty, tip) : null])
  }
  const drawRow = (r) => {
    const p = previews.get(r.address)
    r.drawn = p
    const itemHref = `#/item/${via[0] ?? '-'}/${r.address}`
    const actions = h('td', { class: 'item-actions' }, pickPill(ctx, { address: r.address, via }, tip), openPill(itemHref, r.address, tip))
    if (p === undefined) {
      const wait = r.index < previews.cap ? h('span', { class: 'muted' }, 'loading...')
        : h('button', { type: 'button', onclick: () => previews.ask(via[0] ?? null, r.address) }, 'show details')
      r.tr.replaceChildren(h('td', {}, r.box), actions, h('td', { colspan: span() }, wait))
      return
    }
    if (p.error) {
      r.tr.replaceChildren(h('td', {}, r.box), actions, h('td', { class: 'muted', colspan: span() },
        p.error === 'block_missing' ? 'not in this store' : `no preview (${p.error})`))
      return
    }
    r.tr.replaceChildren(h('td', {}, r.box), actions,
      ...files.map((key, i) => fileCell(p, key, i, itemHref)),
      ...shape.columns.map(path => h('td', {}, valueNode(cellOf(p, path), target))))
  }
  const redraw = nextFrame(() => {
    const loaded = rows.map(r => previews.get(r.address))
    const next = itemTable(loaded)
    const nextFiles = fileColumns(loaded)
    const empties = emptyColumns(loaded)
    const key = JSON.stringify([next.columns, next.constant.map(e => [e.path, e.values]), nextFiles, empties])
    const reshaped = key !== shapeKey
    shape = next
    files = nextFiles
    shapeKey = key
    if (reshaped) {
      table.dataset.columns = JSON.stringify(shape.columns)
      table.dataset.fileColumns = JSON.stringify(files)
      head.replaceChildren(h('th', {}, all), h('th', { class: 'item-actions' }), ...files.map((k, i) => h('th', { class: `file-key ${fileClass(i)}`, 'data-file-column': k }, k)),
        ...shape.columns.map(p => h('th', {}, p)))
      constantLine.replaceChildren(...(shape.constant.length ? ['Same for every item: ',
        ...shape.constant.flatMap((e, i) => [i ? ' · ' : '', `${e.path} `, h('strong', {}, valueNode(e, target))])] : []))
      emptyLine.replaceChildren(...(empties.length ? [`No stored file in any item: ${empties.join(' · ')}`] : []))
    }
    for (const r of rows) if (reshaped || previews.get(r.address) !== r.drawn) drawRow(r)
  })
  shape = itemTable([])
  table.dataset.fileColumns = '[]'
  head.replaceChildren(h('th', {}, all), h('th', {}))
  for (const r of rows) drawRow(r)
  previews.onChange(redraw)
  previews.askFirst(rows.map(r => ({ collection: via[0] ?? null, item: r.address })))
  return h('div', {}, constantLine, emptyLine, h('div', { class: 'scroll' }, table), tip.el)
}
