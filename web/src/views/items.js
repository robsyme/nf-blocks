// web/src/views/items.js
// #/items/<completion>/<output>?where=... (explorer layout B spec §5.4): query 3
// as a table whose columns are the Meta Map keys that tell items apart.
// Nothing here reads the snapshot but Explorer.items (Gate assertion 2).
import { h, link } from '../html.js'
import { address, icon } from '../icons.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { cellOf, cellSlots, fileColumns, itemTable } from '../table.js'
import { filterHref, valueText } from '../pairs.js'
import { humanBytes, leafReasonText } from '../words.js'
import { enc, nextFrame, pickAll, pickButton, runCrumbs } from './common.js'

export const TYPES = ['string', 'int', 'float', 'bool', 'null']

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

function chipsBar(target) {
  if (!target.where.length) return h('p', { class: 'muted' }, 'Click any value to filter by it.')
  return h('p', { class: 'chips-bar' }, target.where.map(([p, t, v], i) => h('span', { class: 'chip', 'data-chip': p },
    `${p} = ${valueText(t, v)} `, h('a', { href: routeOf(target, target.where.filter((_, j) => j !== i)), 'aria-label': `remove ${p}` }, '✕'))))
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
  const status = h('span', { class: 'muted' })
  const pickAllButton = h('button', { type: 'button', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': results.length, onclick: async (event) => {
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
    chipsBar(target),
    fold(`filter:${completionCid}/${output}`, '+ filter', whereForm(completionCid, output, where)),
    results.length === 0 ? h('p', { class: 'muted' }, 'No items match.')
      : [h('p', { class: 'row-actions' }, pickAllButton, ' ', status),
        itemsTable(ex, ctx, { results, via, target, checked, onCheck: report })])
}

/** A slot's hover text (items file columns spec §6.3): its field path, then name and size, or why nothing is stored. */
function slotTitle(s) {
  const parts = s.address ? [s.path, s.name, humanBytes(s.size)] : [s.path, leafReasonText(s.reason), s.name]
  return parts.filter(Boolean).join('\n')
}

/** One file slot as a fixed-size pill: a link to its block page, or a dashed pill when nothing is stored. */
function pill(s) {
  const title = slotTitle(s)
  if (!s.address) return h('span', { class: 'pill pill-empty', role: 'img', title, 'aria-label': title, 'data-pill': s.path })
  return h('a', { class: 'pill', href: `#/block/${s.address}`, title, 'aria-label': title, 'data-pill': s.path }, icon(s.dir ? 'directory' : 'file'))
}

function itemsTable(ex, ctx, { results, via, target, checked, onCheck }) {
  const previews = new Previews(ex)
  const head = h('tr', {})
  const body = h('tbody', {})
  const constantLine = h('p', { class: 'constant muted' })
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
  const fileCell = (p, key, open) => {
    const { shown, more } = cellSlots(p, key)
    return h('td', { 'data-file-cell': key }, shown.length === 0 ? h('span', { class: 'muted' }, '—')
      : [...shown.map(pill), more ? h('a', { class: 'pill-more', href: open }, `+${more}`) : null])
  }
  const drawRow = (r) => {
    const p = previews.get(r.address)
    r.drawn = p
    const itemHref = `#/item/${via[0] ?? '-'}/${r.address}`
    const open = link(itemHref, 'Open')
    const pick = pickButton(ctx, { address: r.address, via })
    if (p === undefined) {
      const wait = r.index < previews.cap ? h('span', { class: 'muted' }, 'loading...')
        : h('button', { type: 'button', onclick: () => previews.ask(via[0] ?? null, r.address) }, 'show details')
      r.tr.replaceChildren(h('td', {}, r.box), h('td', { colspan: span() }, wait), h('td', {}, open, ' ', address(r.address), ' ', pick))
      return
    }
    if (p.error) {
      r.tr.replaceChildren(h('td', {}, r.box), h('td', { class: 'muted', colspan: span() },
        p.error === 'block_missing' ? 'not in this store' : `no preview (${p.error})`), h('td', {}, open, ' ', address(r.address), ' ', pick))
      return
    }
    r.tr.replaceChildren(h('td', {}, r.box),
      ...shape.columns.map(path => h('td', {}, valueNode(cellOf(p, path), target))),
      ...files.map(key => fileCell(p, key, itemHref)),
      h('td', {}, open, ' ', address(r.address), ' ', pick))
  }
  const redraw = nextFrame(() => {
    const loaded = rows.map(r => previews.get(r.address))
    const next = itemTable(loaded)
    const nextFiles = fileColumns(loaded)
    const key = JSON.stringify([next.columns, next.constant.map(e => [e.path, e.values]), nextFiles])
    const reshaped = key !== shapeKey
    shape = next
    files = nextFiles
    shapeKey = key
    if (reshaped) {
      table.dataset.columns = JSON.stringify(shape.columns)
      table.dataset.fileColumns = JSON.stringify(files)
      head.replaceChildren(h('th', {}, all), ...shape.columns.map(p => h('th', {}, p)),
        ...files.map(k => h('th', { 'data-file-column': k }, k)), h('th', {}))
      constantLine.replaceChildren(...(shape.constant.length ? ['Same for every item: ',
        ...shape.constant.flatMap((e, i) => [i ? ' · ' : '', `${e.path} `, h('strong', {}, valueNode(e, target))])] : []))
    }
    for (const r of rows) if (reshaped || previews.get(r.address) !== r.drawn) drawRow(r)
  })
  shape = itemTable([])
  table.dataset.fileColumns = '[]'
  head.replaceChildren(h('th', {}, all), h('th', {}))
  for (const r of rows) drawRow(r)
  previews.onChange(redraw)
  previews.askFirst(rows.map(r => ({ collection: via[0] ?? null, item: r.address })))
  return h('div', {}, constantLine, h('div', { class: 'scroll' }, table))
}

function whereForm(completionCid, output, where) {
  const rows = h('div', {})
  const addRow = ([path, type, value] = ['', 'string', '']) => rows.append(h('fieldset', {},
    h('input', { name: 'path', value: path, placeholder: 'Meta Map key, e.g. sample or library.kit' }),
    h('select', { name: 'type' }, TYPES.map(t => h('option', { value: t, selected: t === type }, t))),
    h('input', { name: 'value', value: value ?? '', placeholder: 'value' })))
  ;(where.length ? where : [undefined]).forEach(addRow)
  return h('form', { onsubmit: (event) => {
    event.preventDefault()
    const next = [...rows.querySelectorAll('fieldset')].map(f => [f.elements.path.value.trim(), f.elements.type.value, f.elements.value.value])
      .filter(([path]) => path)
    location.hash = `#/items/${completionCid}/${enc(output)}?where=${enc(JSON.stringify(next))}`
  } }, rows, h('div', {}, h('button', { type: 'button', onclick: () => addRow() }, 'Add a condition'), ' ', h('button', { type: 'submit' }, 'Filter')))
}
