// web/src/views/items.js
// #/items/<completion>/<output> (DESIGN.md §15).
import { h, link } from '../html.js'
import { Previews } from '../previews.js'
import { enc, itemRows } from './common.js'

export async function items(ex, completionCid, output, whereText, ctx) {
  let where
  try {
    where = JSON.parse(whereText)
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && w.length === 3)) throw new Error('not a list of [path, type, value]')
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[path, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
  let found
  try {
    found = await ex.items(completionCid, output, where, ctx.progress)
  } catch (e) {
    if (e.constructor?.name === 'PredicateError') throw Object.assign(e, { code: 'bad_predicate' })
    throw e
  }
  const { items: results, collection: collectionCid } = found
  return h('section', {},
    h('h1', {}, `Items of ${output}`), h('p', {}, 'In ', link(`#/run/${completionCid}`, 'this run'), ', where every condition below holds.'),
    whereForm(completionCid, output, where),
    h('p', {}, `${results.length} item${results.length === 1 ? '' : 's'}`),
    // Decision 12: a per-run query's pick records the run's Output Collection as via.
    results.length === 0 ? null : itemRows(ex, ctx, { items: results, collectionCid, completion: completionCid, output, where,
      previews: new Previews(ex), results: true }))
}

const TYPES = ['string', 'int', 'float', 'bool', 'null']

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
