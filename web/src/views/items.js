// web/src/views/items.js
// #/items/<completion>/<output>?where=... (explorer layout B spec §5.4; P0
// polish, mockups/04-output-items.html): query 3 as a grid whose first column
// is the sample, then the pipeline's promoted fields, then any asked for, in
// one stable order; each row's files as chips by tool, its size, Pick and Open.
// Nothing here reads the snapshot but Explorer.items (Gate assertion 2): the
// rest is block reads, sizes and runIdentity.
import { h } from '../html.js'
import { Previews } from '../previews.js'
import { SAMPLE_PATHS, cellOf, emptyColumns, fieldChoices, fileColumns, fileGroups, itemColumns, rankFields, typedCondition } from '../table.js'
import { filterHref, valueText } from '../pairs.js'
import { NOT_EQUAL, RANGE_OPS } from '../metadata.js'
import { leafReasonText } from '../words.js'
import { bytes, sizeText } from '../format.js'
import { fieldChip, fileChip, pageHead, shortCid } from '../components.js'
import { seal } from '../seal.js'
import { sealed } from '../icons.js'
import { matchPromoted, promotedFor, shortLabel } from '../promoted.js'
import { sumUnique } from '../sizes.js'
import { enc, markPicked, nextFrame, pickAll, pickButton } from './common.js'
import { OP_TEXT } from './samples.js'

function parseWhere(whereText) {
  try {
    const where = JSON.parse(whereText)
    const ops = [NOT_EQUAL, ...RANGE_OPS]
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && (w.length === 3 || (w.length === 4 && ops.includes(w[3])))))
      throw new Error(`not a list of [path, type, value] or [path, type, value, op] with op one of ${ops.join(' ')}`)
    return where
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[path, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
}

const routeOf = (target, where) => `#/items/${target.completion}/${enc(target.output)}?where=${enc(JSON.stringify(where))}`

const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`

// Hidden items named in the "hidden by the filter" row, at most; past this the row gives the count.
const HIDDEN_NAMED = 10
// itemSize lookups in flight at once.
const SIZE_CONCURRENCY = 4

const FUNNEL = '<svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true"><path d="M2 3h12l-4.5 5.5V13l-3 1.5V8.5z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>'

/** The small funnel link beside a value: the same page with one more condition. */
function filterLink(target, path, type, value, text) {
  const href = filterHref(target, path, type, value)
  if (!href) return null
  const label = `Only items where ${path} is ${text}`
  const a = h('a', { class: 'cell-filter', href, 'aria-label': label, title: label, 'data-filter-value': String(value) })
  a.innerHTML = FUNNEL
  return a
}

/**
 * The picker's text for each of a field's `values` (`{ type, value }`):
 * valueText, except that distinct numbers which would read alike (`89.62`
 * and `89.64` both `89.6`) get more significant digits until they differ,
 * and, failing that (`7` and `7.0`), their raw text.
 */
function distinctTexts(values, field) {
  const out = new Map(values.map(v => [v, valueText(v.type, v.value, field)]))
  const groups = new Map()
  for (const v of values) groups.set(out.get(v), [...(groups.get(out.get(v)) ?? []), v])
  for (const group of groups.values()) {
    if (group.length < 2) continue
    const numbers = group.filter(v => (v.type === 'int' || v.type === 'float') && Number.isFinite(Number(v.value)))
    let texts = numbers.map(v => String(v.value))
    for (let digits = 4; digits <= 17; digits++) {
      const tried = numbers.map(v => Number(v.value).toLocaleString('en-US', { maximumSignificantDigits: digits }))
      if (new Set(tried).size === numbers.length) { texts = tried; break }
    }
    numbers.forEach((v, i) => out.set(v, field?.format === 'percent' ? `${texts[i]}%` : texts[i]))
  }
  return out
}

/**
 * The filter bar (option A of `filters-v1.html`): one chip per condition with
 * a remove link, "Add filter" opening a picker of the loaded items' fields and
 * then their values (each a filter link, with how many items hold it, or a
 * typed value on Enter), and "Clear all". `choices()` is `fieldChoices` over
 * the previews loaded so far; `fieldFor(path)` a path's promoted field or null.
 */
function filterBar(target, { choices, fieldFor }) {
  const picker = h('div', { class: 'fb-picker', role: 'dialog', 'aria-label': 'add a filter', 'data-filter-picker': '', 'data-open': 'false',
    onclick: (e) => e.stopPropagation?.(), onkeydown: (e) => { if (e.key === 'Escape') close() } })
  const close = () => { picker.dataset.open = 'false' }
  const onKey = (e) => { if (e.key === 'Escape') close() }
  const showFields = () => {
    const all = choices()
    const list = h('div', { class: 'fb-list', role: 'listbox', id: 'fb-fields' })
    // Focus stays in the search box; the arrow keys move `active` through the
    // fields `shown` (a combobox's aria-activedescendant), wrapping at the ends.
    // Typing ranks the fields (rankFields) and highlights the best, so Enter
    // takes it before the name is finished.
    let shown = []
    let active = -1
    const highlight = (i) => {
      const was = list.children[active]
      was?.classList.remove('active')
      was?.setAttribute('aria-selected', 'false')
      active = i
      const b = list.children[i]
      if (!b) { search.removeAttribute('aria-activedescendant'); return }
      b.classList.add('active')
      b.setAttribute('aria-selected', 'true')
      search.setAttribute('aria-activedescendant', b.getAttribute('id'))
      b.scrollIntoView?.({ block: 'nearest' })
    }
    const move = (step) => {
      if (!shown.length) return
      highlight(active < 0 ? (step > 0 ? 0 : shown.length - 1) : (active + step + shown.length) % shown.length)
    }
    const onSearchKey = (e) => {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); move(e.key === 'ArrowDown' ? 1 : -1) }
      else if (e.key === 'Enter' && active >= 0) { e.preventDefault(); showValues(shown[active]) }
      else onKey(e)
    }
    const search = h('input', { class: 'fb-input', type: 'search', placeholder: 'Search fields…', 'aria-label': 'search fields', 'data-filter-search': '',
      role: 'combobox', 'aria-controls': 'fb-fields', 'aria-expanded': 'true', 'aria-autocomplete': 'list',
      oninput: (e) => draw((e.currentTarget.value ?? '').trim()), onkeydown: onSearchKey })
    const draw = (q) => {
      shown = rankFields(all, q)
      active = -1
      list.replaceChildren(...shown.map((f, i) =>
        h('button', { type: 'button', class: 'fb-item', role: 'option', id: `fb-field-${i}`, 'aria-selected': 'false', 'data-field': f.path, onclick: () => showValues(f) },
          h('span', { class: 'fb-path' }, f.path), h('span', { class: 'fb-count' }, plural(f.values.length, 'value')))))
      highlight(q && shown.length ? 0 : -1)
    }
    draw('')
    picker.replaceChildren(search, all.length ? list : h('p', { class: 'muted fb-note' }, 'No fields yet: the items are still loading.'))
    search.focus?.()
  }
  const showValues = (f) => {
    // Every field takes == and !=; a number field also compares. The chosen
    // operator applies to a listed value and to a typed one.
    const numeric = ['int', 'float', 'mixed'].includes(f.type)
    const field = fieldFor(f.path)
    let op = '='
    const error = h('p', { class: 'fb-error', 'data-filter-error': '' })
    const input = h('input', { class: 'fb-input', placeholder: 'Or type a value and press Enter', 'aria-label': `a value for ${f.path}`, 'data-filter-value': '',
      onkeydown: (e) => {
        if (e.key !== 'Enter') return onKey(e)
        try {
          const [path, type, value, rangeOp] = typedCondition(f, e.currentTarget.value ?? '', op)
          location.hash = filterHref(target, path, type, value, rangeOp ?? '=')
        } catch (err) {
          error.textContent = err.message
        }
      } })
    const list = h('div', { class: 'fb-list' })
    // A != count is the loaded items that do not hold the value, those without the field included.
    const texts = distinctTexts(f.values, field)
    const drawValues = () => list.replaceChildren(...f.values.filter(v => op === '=' || op === NOT_EQUAL || v.type === 'int' || v.type === 'float').map((v) =>
      h('a', { class: 'fb-item', 'data-value': String(v.value), href: filterHref(target, f.path, v.type, v.value, op) },
        h('span', {}, `${op === '=' ? '' : `${op} `}${texts.get(v)}`), h('span', { class: 'fb-count' }, plural(op === NOT_EQUAL ? f.items - v.count : v.count, 'item')))))
    const opButtons = ['=', NOT_EQUAL, ...(numeric ? RANGE_OPS : [])].map(o => h('button', { type: 'button', class: 'fb-op', 'data-op': o, 'aria-pressed': String(o === op),
      onclick: () => {
        op = o
        for (const b of opButtons) b.setAttribute('aria-pressed', String(b.dataset.op === op))
        drawValues()
        input.focus?.()
      } }, o === '=' ? '==' : o))
    drawValues()
    // replaceChildren writes a null argument as the text "null"; only h() skips it.
    picker.replaceChildren(...[
      h('div', { class: 'fb-head' },
        h('button', { type: 'button', class: 'btn btn-ghost fb-back', 'aria-label': 'back to the fields', 'data-filter-back': '', onclick: showFields }, '‹'),
        h('span', { class: 'fb-path' }, f.path), h('span', { class: 'fb-type' }, f.type)),
      h('div', { class: 'fb-ops', role: 'group', 'aria-label': 'compare with' }, opButtons),
      input, error, list].filter(Boolean))
    input.focus?.()
  }
  const add = h('button', { type: 'button', class: 'btn ghost sm', 'aria-haspopup': 'dialog', 'data-add-filter': '', onclick: (e) => {
    e.stopPropagation?.()
    if (picker.dataset.open === 'true') return close()
    // Open first: a hidden picker's search box cannot take focus.
    picker.dataset.open = 'true'
    showFields()
  } }, '+ Add filter')
  const chip = ([p, t, v, op = '='], i) => {
    const field = fieldFor(p)
    const text = valueText(t, v, field)
    // The title keeps the condition as the route holds it; the chip shows it formatted.
    return h('span', { class: 'fchip', 'data-chip': p, title: `${p} ${op} ${t === 'null' ? 'null' : v}` },
      h('span', { class: 'fchip-key' }, field ? field.label : `‎${p}`), h('span', { class: 'op' }, ` ${OP_TEXT[op] ?? op} `), h('span', { class: 'fchip-val' }, text),
      h('a', { class: 'x', href: routeOf(target, target.where.filter((_, j) => j !== i)), 'aria-label': `remove ${p}`, 'data-chip-remove': '' }, '×'))
  }
  const bar = h('div', { class: 'filterbar items-filters', 'data-filter-bar': '' }, ...target.where.map(chip),
    h('span', { class: 'fb-anchor' }, add, picker),
    target.where.length ? h('a', { class: 'muted fb-clear', href: routeOf(target, []), 'data-clear-filters': '' }, 'Clear all') : null)
  // A click anywhere else closes the picker; the listener goes once the page has.
  const onDoc = () => { if (!bar.isConnected) return document.removeEventListener?.('click', onDoc); close() }
  globalThis.document?.addEventListener?.('click', onDoc)
  return bar
}

/**
 * Every item of the output the query ran over, from its OutputCollection
 * block (a block read, not the snapshot): the collection the query named, or
 * when it matched nothing the run's collection with this output's name.
 * Null when it cannot be read.
 */
async function outputItems(ex, completionCid, output, collectionCid) {
  const itemsOf = (block) => (block.items ?? []).filter(Boolean).map(String)
  try {
    if (collectionCid) return { cid: collectionCid, items: itemsOf((await ex.blocks.ofKind(collectionCid, 'OutputCollection')).value) }
    for (const c of (await ex.completionOf(completionCid)).collections ?? []) {
      const block = (await ex.blocks.ofKind(String(c), 'OutputCollection')).value
      if (block.name === output) return { cid: String(c), items: itemsOf(block) }
    }
  } catch { /* the heading then gives the query's count alone */ }
  return null
}

export async function items(ex, completionCid, output, whereText, ctx) {
  const where = parseWhere(whereText)
  const identityLookup = Promise.resolve().then(() => ex.runIdentity?.(completionCid) ?? null).catch(() => null)
  let found
  try {
    found = await ex.items(completionCid, output, where, ctx.progress)
  } catch (e) {
    if (e.constructor?.name === 'PredicateError') throw Object.assign(e, { code: 'bad_predicate' })
    throw e
  }
  const identity = await identityLookup
  const promoted = promotedFor(identity?.pipeline, { output })
  const fieldFor = (path) => matchPromoted(promoted, [path])[0]?.field ?? null
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
  const count = h('span', { class: 'muted items-count', 'data-items-count': '' })
  const heading = { total: null, size: null }
  const drawCount = () => {
    const n = results.length
    const of = where.length && heading.total !== null && heading.total !== n ? `${n} of ${plural(heading.total, 'item')}` : plural(n, 'item')
    count.textContent = [of, heading.size ? sizeText(heading.size) : null].filter(Boolean).join(' · ')
  }
  drawCount()
  const grid = itemsGrid(ex, ctx, { results, via, target, checked, onCheck: report, previews, promoted, fieldFor, heading, drawCount })
  const pickAllButton = h('button', { type: 'button', class: 'btn', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': results.length, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    try {
      const n = await pickAll(ex, ctx.tray, { items: results, collectionCid })
      ctx.trayChanged()
      grid.markPicked()
      status.textContent = `Picked ${n}.`
    } finally {
      button.disabled = false
    }
  } }, `Pick all ${results.length}`)
  const crumbs = identity
    ? [identity.pipeline ? { text: identity.pipeline, href: `#/pipeline/${enc(identity.pipeline)}` } : null,
      { text: identity.run_name ?? 'run', href: `#/run/${completionCid}` }, { text: output }].filter(Boolean)
    : [{ text: 'run', href: `#/run/${completionCid}` }, { text: output }]
  // The output's other items, for "N of M" and the hidden row; unfiltered, the query already has them all.
  if (where.length) {
    outputItems(ex, completionCid, output, collectionCid).then((all) => {
      if (!all) return
      heading.total = all.items.length
      drawCount()
      grid.showHidden(all)
    })
  }
  return h('section', { class: 'items-page' },
    pageHead({ crumbs, title: output, after: [count], actions: results.length ? [status, pickAllButton] : [] }),
    filterBar(target, { choices: () => fieldChoices(results.map(a => previews.get(a))), fieldFor }),
    grid.node)
}

/** One file group as a chip: a link to the file page (`#/content/<cid>`) when it holds one file, else a menu with a link to each file; the title names the files and their size. */
function groupChip(g, itemHref) {
  if (!g.files.length) {
    const title = [`${g.column}: no stored file`, ...g.empty.map(s => `${s.path}: ${leafReasonText(s.reason)}`)].join('\n')
    return h('span', { class: 'fileb fileb-empty', 'data-file-cell': g.column, 'data-pill-empty': String(g.empty.length), title },
      g.column, ' ', h('span', { class: 'ext' }, 'none'))
  }
  const names = g.files.map(s => (s.name ? (s.dir ? `${s.name}/` : s.name) : s.path))
  const sized = g.files.every(s => s.size !== null && s.size !== undefined)
  const title = [`${g.column}: ${plural(g.files.length, 'file')}${sized ? `, ${bytes(g.bytes)}` : ''}`, ...names.slice(0, 12),
    names.length > 12 ? `and ${names.length - 12} more` : null,
    g.empty.length ? `${g.empty.length} without a stored file: ${g.empty.map(s => s.path).join(', ')}` : null].filter(Boolean).join('\n')
  if (g.files.length === 1) {
    const chip = fileChip(g.column, g.exts, { href: `#/content/${g.files[0].address}`, title })
    chip.dataset.fileCell = g.column
    chip.dataset.pill = g.column
    return chip
  }
  // Several files: the chip opens a short list, one link per file page, and the item page last.
  const links = g.files.map((s, i) => h('a', { href: `#/content/${s.address}`, title: s.path, 'data-file-link': '' },
    h('span', { class: 'name' }, names[i]), s.size !== null && s.size !== undefined ? h('span', { class: 'size' }, bytes(s.size)) : null))
  const summary = h('summary', { class: 'fileb', title, 'data-file-cell': g.column, 'data-pill': g.column },
    g.column, ' ', h('span', { class: 'ext' }, g.exts.map(([ext, count]) => `${ext}×${count}`).join(' ')))
  return fileMenu(summary, h('div', { class: 'filemenu-list' }, links, h('a', { class: 'filemenu-item', href: itemHref }, 'Open the item')))
}

/**
 * Where a file menu's list goes, in viewport pixels, for the chip's `rect` and
 * the list's natural `width` and `height`: below the chip, or above it when
 * the list does not fit below and there is more room above (under
 * `topLimit`, the top bar's bottom). Kept `margin` inside the viewport, at
 * most `cap` tall. `{ left, top | bottom, maxHeight }`.
 */
export function menuPlace(rect, { width, height }, { vw, vh, topLimit = 0, gap = 3, margin = 8, cap = 260 }) {
  const below = vh - margin - (rect.bottom + gap)
  const above = rect.top - gap - Math.max(margin, topLimit)
  const up = below < Math.min(height, cap) && above > below
  const maxHeight = Math.max(48, Math.min(cap, up ? above : below))
  const left = Math.max(margin, Math.min(rect.left, vw - margin - width))
  return up ? { left, bottom: vh - (rect.top - gap), maxHeight } : { left, top: rect.bottom + gap, maxHeight }
}

/** The file menu that is open, as its close function: opening another closes it. */
let openMenu = null

/**
 * A chip's file list as a `details`. The table box scrolls sideways, which
 * clips anything below it, so the open list is fixed to the viewport by the
 * chip's rect. Escape, a click outside, a scroll or resize, or another menu
 * opening closes it; the listeners are there only while it is open.
 */
function fileMenu(summary, list) {
  const menu = h('details', { class: 'filemenu' }, summary, list)
  let stop = null
  const close = () => { menu.removeAttribute('open'); stop?.() }
  const place = () => {
    const rect = summary.getBoundingClientRect?.()
    if (rect) {
      const at = menuPlace(rect, { width: list.offsetWidth, height: list.scrollHeight }, { vw: document.documentElement.clientWidth,
        vh: innerHeight, topLimit: document.querySelector('.top')?.getBoundingClientRect().bottom ?? 0 })
      Object.assign(list.style, { left: `${at.left}px`, top: at.top === undefined ? 'auto' : `${at.top}px`,
        bottom: at.bottom === undefined ? 'auto' : `${at.bottom}px`, maxHeight: `${at.maxHeight}px` })
    }
    menu.dataset.placed = ''
  }
  const open = () => {
    if (openMenu) openMenu()
    openMenu = close
    place()
    const onKey = (e) => { if (e.key === 'Escape') { close(); summary.focus?.() } }
    const onClick = (e) => { if (!menu.contains(e.target)) close() }
    const onScroll = (e) => { if (!list.contains(e.target)) close() }
    // Scroll events do not bubble; the capture phase sees the table box's and the page's.
    document.addEventListener('keydown', onKey)
    document.addEventListener('click', onClick, true)
    document.addEventListener('scroll', onScroll, true)
    globalThis.addEventListener?.('resize', close)
    globalThis.addEventListener?.('hashchange', close)
    // The box can change size with the window still (the side panels); its first report is the size it opened at.
    const box = menu.closest?.('.tbox')
    let first = true
    const watch = box && globalThis.ResizeObserver ? new ResizeObserver(() => { if (first) first = false; else close() }) : null
    watch?.observe(box)
    stop = () => {
      stop = null
      if (openMenu === close) openMenu = null
      delete menu.dataset.placed
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('click', onClick, true)
      document.removeEventListener('scroll', onScroll, true)
      globalThis.removeEventListener?.('resize', close)
      globalThis.removeEventListener?.('hashchange', close)
      watch?.disconnect()
    }
  }
  // A browser fires toggle after the open attribute changes, by a click on the chip or by close().
  menu.addEventListener('toggle', () => {
    if (!menu.hasAttribute('open')) stop?.()
    else if (!stop) open()
  })
  return menu
}

/** A dotted path's parent, as a header's small text: `meta`, or `…salmon_strand_analysis` when deeper. */
function parentText(path) {
  const at = path.lastIndexOf('.')
  if (at < 0) return null
  const parent = path.slice(0, at)
  return parent.includes('.') ? `…${shortLabel(parent)}` : parent
}

/** A promoted field's path as its header's small text: whole, or `…last` past two segments. */
const pathText = (path) => (path.split('.').length > 2 ? `…${shortLabel(path)}` : path)

/** The paths whose every loaded value is a number: their cells align right. */
function numericPaths(loaded, columns) {
  const out = new Set()
  for (const { path } of columns) {
    const entries = loaded.map(p => cellOf(p, path)).filter(Boolean)
    if (entries.length && entries.every(e => e.types.every(t => t === 'int' || t === 'float'))) out.add(path)
  }
  return out
}

// The promoted columns shown so far for each 'completion/output' in this page session, so a filter never moves one to the strip.
const shownPromoted = new Map()

function itemsGrid(ex, ctx, { results, via, target, checked, onCheck, previews, promoted, fieldFor, heading, drawCount }) {
  const memoryKey = `${target.completion}/${target.output}`
  const constants = h('div', { class: 'constants' })
  const moreLine = h('p', { class: 'muted more-fields', 'data-more-fields': '' })
  const hiddenLink = h('a', { class: 'muted', href: routeOf(target, []), title: 'Clear the filter' })
  const added = new Set()
  if (results.length === 0) {
    const empty = h('div', {}, h('p', { class: 'muted' }, 'No items match.'), h('p', {}, hiddenLink))
    const showHidden = hiddenFor(previews, hiddenLink, { target, results, fieldFor, place: () => {} })
    previews.onChange(nextFrame(showHidden.redraw))
    return { node: empty, markPicked: () => {}, showHidden }
  }
  const head = h('tr', {})
  const body = h('tbody', {})
  const hiddenRow = h('tr', { class: 'group' })
  const table = h('table', { class: 't items-grid', 'data-columns': '[]', 'data-file-columns': '[]' }, h('thead', {}, head), body)
  const all = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check every item', onchange: (event) => {
    const on = event.currentTarget.checked
    for (const r of rows) { r.box.checked = on; if (on) checked.add(r.address); else checked.delete(r.address) }
    onCheck()
  } })
  const itemHref = (address) => `#/item/${via[0] ?? '-'}/${address}`
  const rows = results.map((address, index) => {
    const box = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check this item', onchange: (event) => {
      if (event.currentTarget.checked) checked.add(address)
      else checked.delete(address)
      onCheck()
    } })
    const pick = pickButton(ctx, { address, via, size: 'sm' })
    const actions = h('td', { class: 'nowrap item-actions' }, pick,
      h('a', { class: 'btn sm ghost', href: itemHref(address), 'data-open-item': '', 'data-address': address }, 'Open'))
    return { address, index, box, pick, actions, size: h('td', { class: 'r num nowrap item-size', 'data-size': '' }),
      tr: h('tr', { 'data-item-result': address, 'data-preview-for': address }), drawn: undefined, sized: false }
  })
  body.append(...rows.map(r => r.tr))
  const box = h('div', { class: 'pb tbox' }, table)
  const node = h('div', { class: 'items-body' }, constants, h('section', { class: 'panel flush' }, box), moreLine)
  // Once the page has been shown and is gone (another route rendered), stop asking for previews and sizes, as watchRows does.
  let shown = false
  const alive = () => {
    if (node.isConnected) shown = true
    return !shown || node.isConnected
  }
  const sizes = sizer(ex, { total: results.length, heading, drawCount, alive })
  let shape = itemColumns([], { promoted })
  let files = []
  let numeric = new Set()
  let shapeKey = null
  const others = () => shape.columns.filter(c => c.path !== shape.sample)
  const width = () => others().length + (files.length ? 1 : 0) + 4
  const nameCell = (r, p, note = null) => {
    const entry = p?.pairs && shape.sample ? cellOf(p, shape.sample) : null
    const single = entry && entry.values.length === 1
    const text = entry ? entry.values.map((v, i) => valueText(entry.types[i], v)).join(', ') : shortCid(r.address)
    return h('td', { class: 'nowrap', 'data-column': shape.sample },
      h('div', { class: 'namecell' }, sealed('item', r.address, { size: 18 }), h('a', { href: itemHref(r.address), class: entry ? null : 'mono' }, text),
        single ? filterLink(target, entry.path, entry.types[0], entry.values[0], text) : null, note))
  }
  const valueCell = (p, { path, field }) => {
    const entry = cellOf(p, path)
    const cls = numeric.has(path) ? 'value r num' : 'value'
    if (!entry) return h('td', { class: `${cls} muted`, 'data-column': path }, '—')
    const text = entry.values.map((v, i) => valueText(entry.types[i], v, field)).join(', ')
    return h('td', { class: cls, 'data-column': path }, text,
      entry.values.length === 1 ? filterLink(target, path, entry.types[0], entry.values[0], text) : null)
  }
  const drawRow = (r) => {
    const p = previews.get(r.address)
    r.drawn = p
    if (p === undefined || p.error) {
      const note = p === undefined
        ? (r.index < previews.cap ? h('span', { class: 'muted' }, 'loading...')
          : h('button', { type: 'button', class: 'btn sm ghost', onclick: () => { previews.ask(via[0] ?? null, r.address); sizes.ask(r) } }, 'show details'))
        : h('span', { class: 'muted', title: p.message ?? '' }, p.error === 'block_missing' ? 'not in this store' : `no preview (${p.error})`)
      // The metadata and Files columns as one cell, when the header has any.
      const rest = width() - 4
      // replaceChildren writes null as the text "null": only cells go in.
      r.tr.replaceChildren(...[h('td', {}, r.box), nameCell(r, null, note), rest > 0 ? h('td', { colspan: String(rest) }) : null, r.size, r.actions].filter(Boolean))
      return
    }
    const groups = fileGroups(p, files)
    r.tr.replaceChildren(...[h('td', {}, r.box), nameCell(r, p), ...others().map(c => valueCell(p, c)),
      files.length ? h('td', { class: 'files' }, groups.length ? h('div', { class: 'kinds' }, groups.map(g => groupChip(g, itemHref(r.address)))) : h('span', { class: 'muted' }, '—')) : null,
      r.size, r.actions].filter(Boolean))
  }
  const drawHead = () => {
    // A narrow column wraps its path text only after a `.` or `_`.
    const small = (text) => (text ? h('small', {}, text.split(/(?<=[._])/).flatMap((part, i) => [i ? h('wbr') : null, part])) : null)
    const th = ({ path, field }) => h('th', { class: numeric.has(path) ? 'r' : null, 'data-column': path, title: path },
      field ? field.label : shortLabel(path), small(field ? pathText(path) : parentText(path)))
    head.replaceChildren(...[h('th', {}, all), h('th', { title: shape.sample }, shape.sample ? 'Sample' : 'Item'), ...others().map(th),
      files.length ? h('th', {}, 'Files') : null, h('th', { class: 'r item-size' }, 'Size'), h('th', { class: 'item-actions' })].filter(Boolean))
    table.dataset.columns = JSON.stringify(shape.columns.map(c => c.path))
    table.dataset.fileColumns = JSON.stringify(files)
  }
  const drawStrip = (empties) => {
    const label = (path) => fieldFor(path)?.label ?? shortLabel(path)
    const chips = shape.constant.map(e => fieldChip(label(e.path), e.values.map((v, i) => valueText(e.types[i], v, fieldFor(e.path))).join(', '), { title: e.path }))
    constants.replaceChildren(...[
      chips.length ? 'Same for every item:' : null, ...chips,
      chips.length && empties.length ? h('span', { class: 'muted' }, '·') : null,
      empties.length ? h('span', { class: 'muted', 'data-empty-columns': '' }, 'No stored file in any item: ',
        empties.flatMap((k, i) => [i ? ', ' : null, h('span', { class: 'mono' }, k)])) : null].filter(Boolean))
    // Two paths ending alike are named in full.
    const shorts = shape.more.map(shortLabel)
    const name = (p, i) => (shorts.filter(s => s === shorts[i]).length > 1 ? p : shorts[i])
    moreLine.replaceChildren(...(shape.more.length ? [`${plural(shape.more.length, 'more field')}: `,
      ...shape.more.flatMap((p, i) => [i ? ', ' : null, h('button', { type: 'button', class: 'more-field mono', 'data-add-column': p,
        title: `Show ${p} as a column`, onclick: () => { added.add(p); redraw() } }, name(p, i))].filter(Boolean))] : []))
  }
  const showHidden = hiddenFor(previews, hiddenLink, { target, results, fieldFor,
    place: () => { hiddenRow.replaceChildren(h('td', { colspan: String(width()) }, hiddenLink)); body.append(hiddenRow) } })
  const off = []
  const redraw = nextFrame(() => {
    if (!alive()) { previews.cancel(); for (const f of off) f(); return }
    box.classList[box.scrollWidth > box.clientWidth ? 'add' : 'remove']('overflows')
    const loaded = rows.map(r => previews.get(r.address))
    const memory = shownPromoted.get(memoryKey) ?? new Set()
    const next = itemColumns(loaded, { promoted, added: [...added], filtered: target.where.length > 0, keep: [...memory] })
    for (const c of next.columns) if (c.field) memory.add(c.path)
    shownPromoted.set(memoryKey, memory)
    const nextFiles = fileColumns(loaded)
    const empties = emptyColumns(loaded)
    const nextNumeric = numericPaths(loaded, next.columns)
    const key = JSON.stringify([next.sample, next.columns.map(c => c.path), next.more, next.constant.map(e => [e.path, e.values]), nextFiles, empties, [...nextNumeric]])
    const reshaped = key !== shapeKey
    shape = next
    files = nextFiles
    numeric = nextNumeric
    shapeKey = key
    if (reshaped) {
      drawHead()
      drawStrip(empties)
      if (hiddenRow.children.length) hiddenRow.children[0].setAttribute('colspan', String(width()))
    }
    for (const r of rows) if (reshaped || previews.get(r.address) !== r.drawn) drawRow(r)
    showHidden.redraw()
  })
  drawHead()
  for (const r of rows) drawRow(r)
  off.push(previews.onChange(redraw))
  const first = previews.askFirst(rows.map(r => ({ collection: via[0] ?? null, item: r.address })))
  for (const r of rows.slice(0, first)) sizes.ask(r)
  // A resized window can make the table start or stop overflowing its box.
  if (globalThis.ResizeObserver) {
    const watch = new ResizeObserver(() => { if (!alive()) return watch.disconnect(); redraw() })
    watch.observe(box)
  }
  return {
    node,
    markPicked: () => { for (const r of rows) markPicked(r.pick) },
    showHidden,
  }
}

/**
 * The "N items hidden by the filter" link (it clears the filter), naming the
 * hidden items and their values at the filter's paths once their previews
 * arrive, when there are few. `place()` puts the link on the page. The
 * returned function takes the output's `{ cid, items }`; its `redraw` is
 * called as previews arrive.
 */
function hiddenFor(previews, link, { target, results, fieldFor, place }) {
  let hidden = null
  const draw = () => {
    if (!hidden?.length) return
    const named = hidden.length <= HIDDEN_NAMED ? hidden.map(a => [a, previews.get(a)]) : []
    const ready = named.length > 0 && named.every(([, p]) => p !== undefined)
    const label = ([a, p]) => {
      const id = SAMPLE_PATHS.map(path => cellOf(p, path)).find(Boolean) ?? null
      // Written as the Sample cell writes it (a numeric-looking string id stays quoted).
      const name = id ? id.values.map((v, i) => valueText(id.types[i], v)).join(', ') : shortCid(a)
      const values = target.where.filter(([path]) => path !== id?.path).flatMap(([path]) => {
        const e = cellOf(p, path)
        return e ? [e.values.map((v, i) => valueText(e.types[i], v, fieldFor(path))).join(', ')] : []
      })
      return values.length ? `${name} (${values.join(', ')})` : name
    }
    const names = ready ? named.map(label).sort() : []
    link.textContent = [`${plural(hidden.length, 'item')} hidden by the filter`, names.length ? names.join(', ') : null].filter(Boolean).join(' · ')
    link.dataset.hiddenCount = String(hidden.length)
  }
  const show = (all) => {
    const shown = new Set(results)
    hidden = all.items.filter(a => !shown.has(a))
    if (hidden.length <= HIDDEN_NAMED) for (const a of hidden) previews.ask(all.cid, a)
    if (hidden.length) place()
    draw()
  }
  show.redraw = draw
  return show
}

/**
 * Row sizes on request (P2 replaces this with an index table): `ask(row)`
 * queues a row's itemSize, a few at a time, and fills its Size cell; after
 * each, the heading's total over the measured rows' files, each file once,
 * reads "at least" until every row is measured. Rows are asked as their
 * previews are (the first `PREVIEW_CAP`, then "show details"). `alive()`
 * false (the page was shown and has gone) drops the queue.
 */
function sizer(ex, { total, heading, drawCount, alive }) {
  const queue = []
  const maps = new Map()
  let running = 0
  const pump = () => {
    if (!alive()) { queue.length = 0; return }
    while (running < SIZE_CONCURRENCY && queue.length) {
      const r = queue.shift()
      running++
      ;(async () => {
        try {
          r.size.textContent = sizeText(await ex.itemSize(r.address))
          maps.set(r.address, await ex.itemFileMap(r.address))
        } catch {
          r.size.textContent = '—'
          maps.set(r.address, { files: new Map(), partial: true })
        }
      })().finally(() => {
        running--
        const sum = sumUnique([...maps.values()])
        heading.size = { ...sum, partial: sum.partial || maps.size < total }
        drawCount()
        pump()
      })
    }
  }
  return {
    ask(r) {
      if (!ex.itemSize || !ex.itemFileMap || r.sized) return
      r.sized = true
      queue.push(r)
      pump()
    },
  }
}
