// web/src/views.js
// One function per route (DESIGN.md §15). Each returns a node; the data-*
// attributes are the contract the Gate reads, the rest is for people.
import { h, link, cid } from './html.js'

const enc = encodeURIComponent

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
}

function table(head, rows) {
  return h('div', { class: 'scroll' }, h('table', {}, h('tr', {}, head.map(t => h('th', {}, t))), rows))
}

const shown = (value) => (value === null ? 'null' : typeof value === 'object' && typeof value.toString === 'function' && value['/']
  ? value.toString() : typeof value === 'object' ? JSON.stringify(value, (k, v) => (v && v['/'] ? v.toString() : v)) : String(value))

export function idle() {
  return h('p', { class: 'muted' }, 'Snapshot open.')
}

/** Store Log runs whose blocks were missing or refused. Their pipeline is
 * unknown (it is in the RunManifest the refused RunCompletion links to), so
 * every view that lists runs shows them. */
function unreadableRuns(ex) {
  const unreadable = ex.stale.filter(s => s.error)
  return unreadable.length
    ? h('section', {}, h('h2', {}, 'Runs in the Store Log that could not be read'), unreadable.map(s => errorNode(s.error)))
    : null
}

export async function home(ex) {
  const pipelines = await ex.pipelines()
  return h('section', {},
    h('h1', {}, 'Pipelines'),
    pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs in this member yet.')
      : table(['pipeline', 'runs', 'latest finish'], pipelines.map(p => h('tr', {},
        h('td', {}, link(`#/pipeline/${enc(p.pipeline)}`, p.pipeline)), h('td', {}, String(p.runs)), h('td', {}, p.latest ?? '')))),
    unreadableRuns(ex))
}

/** Where a page sits in the whole list, and links to the pages either side (DESIGN.md §15, [data-page]). */
function pager(route, page, noun) {
  if (page.total === 0) return null
  return h('p', { 'data-page': '', 'data-first': page.first, 'data-last': page.last, 'data-total': page.total, class: 'muted' },
    `showing ${page.first}-${page.last} of ${page.total} ${noun}`,
    page.prev !== null ? [' ', h('a', { href: `${route}?offset=${page.prev}`, 'data-page-prev': '' }, 'previous page')] : null,
    page.next !== null ? [' ', h('a', { href: `${route}?offset=${page.next}`, 'data-page-next': '' }, 'next page')] : null)
}

export async function pipeline(ex, name, offset = 0) {
  const page = await ex.runPage(name, { offset })
  const rows = page.rows
  const node = h('section', {},
    h('h1', {}, name),
    h('p', {}, link(`#/latest/${enc(name)}`, 'Latest successful run')),
    pager(`#/pipeline/${enc(name)}`, page, 'runs'),
    table(['run', 'status', 'finished', 'anomalies', 'from'], rows.map(r => h('tr', {
      'data-run': r.completion_cid, 'data-pipeline': r.pipeline, 'data-status': r.status, 'data-source': r.source },
    h('td', {}, link(`#/run/${r.completion_cid}`, r.run_name ?? r.completion_cid)),
    h('td', {}, r.possibly_incomplete ? `${r.status}, possibly incomplete` : r.status),
    h('td', {}, r.finished_at ?? ''),
    h('td', { 'data-anomalies-for': r.completion_cid, class: 'muted' }, '...'),
    h('td', { class: 'muted' }, r.source === 'tail' ? 'newer than the snapshot' : 'snapshot')))),
    unreadableRuns(ex))
  // Anomaly counts are only in each RunCompletion; fill them in without holding up the view.
  for (const r of rows) {
    ex.completionOf(r.completion_cid).then(c => {
      const cell = node.querySelector(`[data-anomalies-for="${r.completion_cid}"]`)
      const a = c.anomalies
      if (cell) cell.textContent = Object.entries(a).filter(([, n]) => n).map(([k, n]) => `${k.replace('_', ' ')} ${n}`).join(', ') || 'none'
    }, e => {
      const cell = node.querySelector(`[data-anomalies-for="${r.completion_cid}"]`)
      if (cell) cell.replaceChildren(errorNode(e))
    })
  }
  return node
}

export async function run(ex, completionCid) {
  const { row, completion, collections } = await ex.run(completionCid)
  const a = completion.anomalies
  return h('section', {},
    h('h1', {}, row.run_name ?? 'run'), cid(completionCid),
    h('dl', {},
      h('dt', {}, 'pipeline'), h('dd', {}, link(`#/pipeline/${enc(row.pipeline)}`, row.pipeline)),
      h('dt', {}, 'status'), h('dd', {}, completion.possibly_incomplete ? `${completion.status}, possibly incomplete` : completion.status),
      h('dt', {}, 'finished'), h('dd', {}, completion.finished_at),
      h('dt', {}, 'anomalies'), h('dd', {}, `unresolvable ${a.unresolvable}, unaddressed ${a.unaddressed}, declined ${a.declined}, never published ${a.never_published}`),
      completion.error ? [h('dt', {}, 'error'), h('dd', {}, completion.error)] : null),
    h('h2', {}, 'Outputs'),
    h('ul', {}, collections.map(c => h('li', { 'data-collection': c.cid, 'data-output': c.output },
      link(`#/collection/${c.cid}`, c.output), ' ', link(`#/items/${completionCid}/${enc(c.output)}`, '(filter by metadata)')))))
}

export async function collection(ex, collectionCid, offset = 0) {
  const c = await ex.collection(collectionCid, { offset })
  return h('section', {},
    h('h1', {}, c.output), cid(collectionCid),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    pager(`#/collection/${collectionCid}`, c, 'items'),
    h('ul', {}, c.items.map(i => h('li', {}, link(`#/item/${collectionCid}/${i}`, h('code', { class: 'cid' }, `cas://${collectionCid}/${i}`))))))
}

export async function item(ex, collectionCid, itemCid) {
  const it = await ex.item(collectionCid, itemCid)
  const view = it.view ?? {}
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString())]))
  return h('section', {},
    h('h1', {}, 'Item'), h('p', {}, cid(`cas://${collectionCid}/${itemCid}`)),
    h('h2', {}, 'Meta Map'),
    Object.keys(view).length ? table(['key', 'value'], Object.entries(view).filter(([, v]) => !(v && v.kind === 'Leaf'))
      .map(([k, v]) => h('tr', {}, h('td', {}, k), h('td', {}, shown(v))))) : h('p', { class: 'muted' }, 'none'),
    h('h2', {}, 'Files'),
    table(['name', 'size', 'content', 'produced by'], it.leaves.map(l => h('tr', {},
      h('td', {}, l.name ?? ''), h('td', {}, l.size ?? ''),
      h('td', {}, l.address ? link(`#/content/${l.address}`, cid(l.address.toString())) : h('span', { class: 'muted' }, l.reason)),
      h('td', {}, (producers.find(([leaf]) => leaf === l)?.[1] ?? []).map(p => h('div', {}, link(`#/run/${p.completion_cid}`, p.filename ?? p.completion_cid))))))))
}

export async function content(ex, contentCid, ctx) {
  const rows = await ex.producersOf(contentCid, ctx.progress)
  return h('section', {},
    h('h1', {}, 'Every producer of this content'), cid(contentCid),
    rows.length === 0 ? h('p', { class: 'muted' }, 'No run in this member produced it.')
      : table(['file', 'item', 'run'], rows.map(p => h('tr', {
        'data-producer': '', 'data-content': p.content_cid, 'data-item': p.item_cid, 'data-collection': p.collection_cid,
        'data-completion': p.completion_cid, 'data-filename': p.filename },
      h('td', {}, p.filename ?? ''), h('td', {}, link(`#/item/${p.collection_cid}/${p.item_cid}`, cid(p.item_cid))),
      h('td', {}, link(`#/run/${p.completion_cid}`, cid(p.completion_cid)))))))
}

export async function latest(ex, pipelineName) {
  const best = await ex.latestSuccessfulRun(pipelineName)
  return h('section', {},
    h('h1', {}, `Latest successful run of ${pipelineName}`),
    h('p', { 'data-latest': best ?? '' }, best ? link(`#/run/${best}`, cid(best)) : 'None: no run of this pipeline succeeded without being possibly incomplete.'))
}

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
    h('ul', {}, results.map(i => h('li', { 'data-item-result': i },
      collectionCid ? link(`#/item/${collectionCid}/${i}`, cid(i)) : cid(i)))))
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
