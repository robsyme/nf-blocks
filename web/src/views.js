// web/src/views.js
// One function per route (DESIGN.md §15). Each returns a node; the data-*
// attributes are the contract the Gate reads, the rest is for people.
import { h, link, cid, copyOutcome } from './html.js'
import { saveChoice } from './save-choice.js'
import { saveSequence, retryRestore } from './save-flow.js'
import { Previews } from './previews.js'
import { labelPaths, labelText, pairsNode, pairsOf } from './pairs.js'
import { snippetBlock, snippetToggle } from './snippets.js'

// Re-exported so this module's existing test and callers keep working (P8):
// the Copy outcome text is html.js's, shared with snippets.js.
export { copyOutcome }

const enc = encodeURIComponent

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
}

function table(head, rows) {
  return h('div', { class: 'scroll' }, h('table', {}, h('tr', {}, head.map(t => h('th', {}, t))), rows))
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

/** Where a page sits in the whole list, and links to the pages either side (DESIGN.md §15, [data-page]). A route may already carry a query. */
function pager(route, page, noun) {
  if (page.total === 0) return null
  const at = (offset) => `${route}${route.includes('?') ? '&' : '?'}offset=${offset}`
  const label = page.first === 0 ? `no ${noun} on this page; there are ${page.total}` : `showing ${page.first}-${page.last} of ${page.total} ${noun}`
  return h('p', { 'data-page': '', 'data-first': page.first, 'data-last': page.last, 'data-total': page.total, class: 'muted' },
    label,
    page.prev !== null ? [' ', h('a', { href: at(page.prev), 'data-page-prev': '' }, 'previous page')] : null,
    page.next !== null ? [' ', h('a', { href: at(page.next), 'data-page-next': '' }, 'next page')] : null)
}

export async function pipeline(ex, name, offset = 0) {
  const page = await ex.runPage(name, { offset })
  // Anomaly counts are only in each RunCompletion; each row's cell is filled
  // in when its block arrives, without holding up the view.
  const rows = page.rows.map(r => ({ r, anomalies: h('td', { 'data-anomalies-for': r.completion_cid, class: 'muted' }, '...') }))
  const node = h('section', {},
    h('h1', {}, name),
    h('p', {}, link(`#/latest/${enc(name)}`, 'Latest successful run')),
    pager(`#/pipeline/${enc(name)}`, page, 'runs'),
    page.hidden ? h('p', { 'data-hidden-runs': page.hidden, class: 'muted' }, `${page.hidden} run${page.hidden === 1 ? '' : 's'} on this page hidden by a delete Claim`) : null,
    table(['run', 'status', 'finished', 'anomalies', 'from'], rows.map(({ r, anomalies }) => h('tr', {
      'data-run': r.completion_cid, 'data-pipeline': r.pipeline, 'data-status': r.status, 'data-source': r.source },
    h('td', {}, link(`#/run/${r.completion_cid}`, r.run_name ?? r.completion_cid)),
    h('td', {}, r.possibly_incomplete ? `${r.status}, possibly incomplete` : r.status),
    h('td', {}, r.finished_at ?? ''),
    anomalies,
    h('td', { class: 'muted' }, r.source === 'tail' ? 'newer than the snapshot' : 'snapshot')))),
    unreadableRuns(ex))
  for (const { r, anomalies } of rows) {
    ex.completionOf(r.completion_cid).then(
      c => { anomalies.textContent = Object.entries(c.anomalies).filter(([, n]) => n).map(([k, n]) => `${k.replace('_', ' ')} ${n}`).join(', ') || 'none' },
      e => { anomalies.replaceChildren(errorNode(e)) })
  }
  return node
}

export async function run(ex, completionCid) {
  const { row, completion, collections } = await ex.run(completionCid)
  const a = completion.anomalies
  // Decision 13: the run's lineage ID, which fromStore(run:) and nf-blocks:items --run accept.
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  return h('section', {},
    h('h1', {}, row.run_name ?? 'run'),
    lid ? h('p', {}, h('code', { title: 'this run\'s lineage ID' }, lid)) : null,
    cid(completionCid),
    h('dl', {},
      h('dt', {}, 'pipeline'), h('dd', {}, link(`#/pipeline/${enc(row.pipeline)}`, row.pipeline)),
      h('dt', {}, 'status'), h('dd', {}, completion.possibly_incomplete ? `${completion.status}, possibly incomplete` : completion.status),
      h('dt', {}, 'finished'), h('dd', {}, completion.finished_at),
      h('dt', {}, 'anomalies'), h('dd', {}, `unresolvable ${a.unresolvable}, unaddressed ${a.unaddressed}, declined ${a.declined}, never published ${a.never_published}`),
      completion.error ? [h('dt', {}, 'error'), h('dd', {}, completion.error)] : null),
    h('h2', {}, 'Outputs'),
    lid && collections.length ? h('p', {}, 'Read an output in a downstream workflow. Script kind: ', snippetToggle()) : null,
    h('ul', {}, collections.map(c => h('li', { 'data-collection': c.cid, 'data-output': c.output },
      link(`#/collection/${c.cid}`, c.output), ' ', link(`#/items/${completionCid}/${enc(c.output)}`, '(filter by metadata)'),
      lid ? snippetBlock({ kind: 'run', lid, output: c.output }) : null))))
}

export async function collection(ex, collectionCid, offset = 0, ctx) {
  const c = await ex.collection(collectionCid, { offset })
  return h('section', {},
    h('h1', {}, c.output), cid(collectionCid),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    pager(`#/collection/${collectionCid}`, c, 'items'),
    c.total === 0 ? h('p', { class: 'muted' }, 'This collection has no items.')
      : itemRows(ex, ctx, { items: c.items, total: c.total, collectionCid, completion: c.completion, output: c.output, where: [],
        previews: new Previews(ex) }))
}

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  // Ticket 10 Q2: on the item page a value's pair is the query's only condition.
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const pills = pairsNode(pairsOf(it.view), target, { size: 'page' })
  return h('section', {},
    h('h1', {}, 'Item'),
    // `-` is an item reached with no collection (a Selection member picked by a query).
    h('p', {}, collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
    from ? h('p', { title: collectionCid }, 'From ', from.completion ? link(`#/run/${from.completion}`, runLabelText(from, collectionCid)) : runLabelText(from, collectionCid)) : null,
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
    h('h2', {}, 'Meta Map'),
    pills ? [pills, target ? h('p', { class: 'muted' }, 'Click a value to list the items of ', h('code', {}, target.output), ' in this run that share it.') : null]
      : h('p', { class: 'muted' }, 'none'),
    h('h2', {}, 'Files'),
    table(['name', 'size', 'content', 'produced by'], it.leaves.map(l => h('tr', {},
      h('td', {}, l.name ?? ''), h('td', {}, l.size ?? ''),
      h('td', {}, l.address ? link(`#/content/${l.address}`, cid(l.address.toString())) : h('span', { class: 'muted' }, l.reason)),
      h('td', {}, (producers.find(([leaf]) => leaf === l)?.[1] ?? []).map(p => h('div', {}, runLabelNode(ex, p.collection_cid, p.completion_cid))))))))
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

const flag = (text) => h('span', { class: 'warn' }, ` (${text})`)

/** Adds an item or a Selection to the tray (spec section 5.6, [data-pick]). */
export function pickButton(ctx, { address, via = [], kind = 'item' }) {
  const inTray = ctx.tray.has(address)
  return h('button', { type: 'button', 'data-pick': address, 'data-kind': kind, 'data-via': via.join(' '), disabled: inTray,
    onclick: (event) => {
      ctx.tray.add({ address, via, kind })
      ctx.trayChanged()
      event.currentTarget.textContent = 'In the tray'
      event.currentTarget.disabled = true
    } }, inTray ? 'In the tray' : kind === 'selection' ? 'Add this Selection to the tray' : 'Add to the tray')
}

const shortCid = (text) => (text.length > 20 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text)

/** `<run_name> / <output>` (DESIGN.md §16 milestone 3 decision 11), or the collection CID when this member does not know its run. */
export const runLabelText = (label, collectionCid) => (label ? `${label.run_name ?? 'unnamed run'} / ${label.output}` : collectionCid)

/**
 * A collection shown by its run: the short CID until Explorer.runLabel
 * answers, then `<run_name> / <output>` linking to the run, the collection
 * CID in `title`. With `completionCid` the placeholder already links there.
 */
function runLabelNode(ex, collectionCid, completionCid = null) {
  const placeholder = h('code', { class: 'cid' }, shortCid(completionCid ?? collectionCid))
  const node = h('span', { title: collectionCid }, completionCid ? link(`#/run/${completionCid}`, placeholder) : placeholder)
  ex.runLabel(collectionCid).then((label) => {
    if (!label) return
    const text = runLabelText(label, collectionCid)
    node.replaceChildren(label.completion ? link(`#/run/${label.completion}`, text) : text)
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

function markInTray(list, addresses) {
  const only = addresses ? new Set(addresses) : null
  for (const button of list.querySelectorAll('[data-pick]')) {
    if (only && !only.has(button.dataset.pick)) continue
    button.textContent = 'In the tray'
    button.disabled = true
  }
}

/** Calls `fn` once on the next frame however often it is asked in between. */
function nextFrame(fn) {
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
 * One item row (DESIGN.md §16 milestone 3 decision 9): `lead` (a checkbox) and the label,
 * file-name chips, `action` at the right; the Meta Map pills beneath in
 * `[data-preview-for]`; a via line when `viaLabel` is set, each via named by
 * its run with `perVia(via)` after it; the short CID last, linking to the
 * item page. The preview is drawn by `watchRows`; a row at `index` past
 * `previews.cap` waits for "show details".
 */
export function itemRow(ex, ctx, { address, via = [], previews, target = null, index = 0, attrs = {}, lead = null, action = null,
  viaLabel = null, noVia = 'a query', perVia = () => null }) {
  const collection = via[0] ?? null
  const row = { address, collection, target, drawn: undefined,
    label: h('strong', { class: 'row-label' }), files: h('span', { class: 'row-files' }),
    pairs: h('div', { class: 'row-pairs', 'data-preview-for': address }) }
  const loading = () => h('span', { class: 'muted' }, 'loading...')
  row.label.append(index < previews.cap ? loading()
    : h('button', { type: 'button', onclick: () => { row.label.replaceChildren(loading()); previews.ask(collection, address) } }, 'show details'))
  const viaLine = viaLabel === null ? null : h('div', { class: 'row-via muted' }, `${viaLabel} `,
    via.length ? via.map((v, i) => [i ? '; ' : null, runLabelNode(ex, v), ' ', perVia(v)]) : [noVia, ' ', perVia('-')])
  const li = h('li', { class: 'row', ...attrs },
    h('div', { class: 'row-head' }, lead, row.label, row.files, h('span', { class: 'row-action' }, action)),
    row.pairs, viaLine,
    h('div', { class: 'row-cid' }, link(`#/item/${collection ?? '-'}/${address}`, h('code', { class: 'cid', title: address }, shortCid(address)))))
  rowOf.set(li, row)
  return li
}

function fillRow(row, preview, paths) {
  if (preview.error) {
    row.label.replaceChildren(h('span', { class: 'muted', title: preview.message ?? '' },
      preview.error === 'block_missing' ? 'not held in this member' : `no preview (${preview.error})`))
    return
  }
  row.label.replaceChildren(labelText(preview, paths) || h('span', { class: 'muted' }, 'no Meta Map'))
  row.files.replaceChildren(...preview.files.map(f => h('span', { class: 'chip' }, f)))
  const pills = pairsNode(preview.pairs, row.target, { size: 'row' })
  row.pairs.replaceChildren(...(pills ? [pills] : []))
}

/**
 * Asks for the first rows' previews and redraws rows as they arrive, one
 * frame at a time; the label paths are chosen across the list's loaded
 * previews, so every row is relabelled when they change. Once the list has
 * been shown and is gone (another route rendered), the queue is dropped.
 */
function watchRows(list, previews, lis) {
  const rows = lis.map(li => rowOf.get(li))
  previews.askFirst(rows.map(r => ({ collection: r.collection, item: r.address })))
  let shown = false
  let lastPaths = null
  const redraw = nextFrame(() => {
    if (list.isConnected) shown = true
    else if (shown) { off(); previews.cancel(); return }
    const paths = labelPaths(rows.map(r => previews.get(r.address)).filter(p => p?.pairs))
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
  const addChecked = h('button', { type: 'button', disabled: true }, 'Add checked (0)')
  const showChecked = () => {
    addChecked.textContent = `Add checked (${checked.size})`
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
    status.textContent = `Added ${picked.length} to the tray.`
  })
  const addAll = h('button', { type: 'button', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': total, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    status.textContent = total > items.length ? `Reading all ${total} items...` : ''
    try {
      const n = await pickAll(ex, ctx.tray, { items: total === items.length ? items : null, collectionCid })
      ctx.trayChanged()
      markInTray(list, null)
      status.textContent = `Added ${n} to the tray.`
    } catch (e) {
      status.replaceChildren(errorNode(e))
    } finally {
      button.disabled = false
    }
  } }, `Add all ${total} to the tray`)
  watchRows(list, previews, lis)
  return h('div', { class: 'item-rows' }, h('p', { class: 'row-actions' }, addChecked, ' ', addAll, ' ', status), list)
}

/** Why Undo is unavailable on the deleted list, mirroring the Selection view's `actions()`; null renders nothing. */
function undoUnavailableNote(ctx) {
  const note = undoNote(ctx.write.available, ctx.write.here, ctx.write.reason, ctx.write.writable, ctx.write.hrefFor('#/selections?deleted=1'))
  if (!note) return null
  return h('p', { 'data-unavailable': '', class: 'muted' }, note.kind === 'unavailable' ? note.reason
    : [`Undo writes to the writable member, ${note.writable}. `, link(note.href, 'Open this list there'), '.'])
}

export async function selections(ex, { offset = 0, deleted = false }, ctx) {
  const page = await ex.selectionPage({ offset, showDeleted: deleted })
  const route = deleted ? '#/selections?deleted=1' : '#/selections'
  return h('section', {},
    h('h1', {}, deleted ? 'Deleted Selections' : 'Selections'),
    // With showDeleted, hiddenCount counts the current rows left out, so it is shown only in the current view.
    h('p', {}, deleted ? link('#/selections', 'Show current Selections')
      : [link('#/selections?deleted=1', 'Show deleted'), page.hiddenCount ? ` (${page.hiddenCount} on this page)` : '']),
    pager(route, page, 'Selections'),
    deleted ? undoUnavailableNote(ctx) : null,
    page.rows.length === 0 ? h('p', { class: 'muted' }, deleted ? 'No deleted Selections.' : 'No Selections in this member yet.')
      : table(['name', 'first seen', 'by', 'state', ''], page.rows.map(r => h('tr', {
        'data-selection': r.cid, 'data-deletion': r.state.deletion, 'data-source': r.source, 'data-names': JSON.stringify(r.state.names) },
      h('td', {}, link(`#/selection/${r.cid}`, r.state.names.length ? r.state.names.join(' / ') : h('span', { class: 'muted' }, 'unnamed')),
        r.state.nameConflicted ? flag('names in conflict') : null, r.error ? errorNode(r.error) : null),
      h('td', {}, r.firstSeen ?? ''), h('td', {}, r.assertedBy ?? ''),
      h('td', {}, r.state.deletion === 'conflicted' ? flag('deletion in conflict') : r.state.deletion === 'deleted' ? 'deleted' : ''),
      h('td', {}, deleted ? undoButton(r.cid, r.state, ctx) : null)))))
}

function undoButton(cid, state, ctx) {
  if (!ctx.write.available || !ctx.write.here) return null
  const status = h('span', {})
  return [h('button', { type: 'button', 'data-undo': cid, onclick: (event) => ctx.write.run(status, async () => {
    await ctx.write.writer.undo(cid, state.deletionClaims)
    return { href: `#/selection/${cid}` }
  }, event.currentTarget) }, 'Undo'), status]
}

export async function selection(ex, selectionCid, ctx) {
  let s
  try {
    s = await ex.selection(selectionCid)
  } catch (e) {
    if (e.code === 'block_missing') e.message = `Selection ${selectionCid} is not in this member; it may be held in another one`
    throw e
  }
  const st = s.state
  const status = h('div', { id: 'write-status' })
  const byCid = new Map(st.current.map(c => [c.cid, c]))
  const previews = new Previews(ex)
  const copy = (text) => h('button', { type: 'button', onclick: async (event) => {
    event.currentTarget.textContent = await copyOutcome(navigator.clipboard, text)
  } }, 'Copy')
  // A Selection may have thousands of members: the list renders at once and
  // each row's "held in" fills in when its own lookup answers, so one slow or
  // failing member (a hash mismatch, say) never holds up the view or fails it
  // outright (final review finding 2). Spec section 7.1a: each via's Copy
  // copies the member as an Item Occurrence. A row's index counts item
  // members only, so the preview cap and "show details" agree with watchRows.
  let itemIndex = 0
  const members = s.members.map((m) => {
    const held = h('span', { 'data-held-for': m.address, class: 'muted' }, '...')
    const node = m.kind === 'selection'
      ? h('li', { class: 'row', 'data-member': m.address, 'data-kind': m.kind },
        h('div', { class: 'row-head' }, h('strong', {}, 'Selection'), link(`#/selection/${m.address}`, cid(m.address)), h('span', { class: 'row-action' }, held)))
      : itemRow(ex, ctx, { address: m.address, via: m.via, previews, index: itemIndex++, attrs: { 'data-member': m.address, 'data-kind': m.kind },
        action: held, viaLabel: 'via', noVia: 'no run (picked by a query across runs)', perVia: (v) => copy(copyText(m.address, v)) })
    return { m, held, node }
  })
  for (const { m, held, node } of members) {
    ex.held(m.kind, m.address).then(
      (state) => {
        node.dataset.held = state
        held.textContent = state === 'here' ? 'held in this member' : 'held in another member'
        held.classList.remove('muted')
      },
      (e) => {
        const error = errorNode(e)
        error.textContent = `unavailable (${error.textContent})`
        held.replaceChildren(error)
        held.classList.remove('muted')
      })
  }
  const memberList = h('ol', { class: 'rows' }, members.map(({ node }) => node))
  watchRows(memberList, previews, members.filter(({ m }) => m.kind === 'item').map(({ node }) => node))
  return h('section', { 'data-selection-view': selectionCid, 'data-deletion': st.deletion },
    h('h1', {}, st.names.length ? st.names.join(' / ') : 'Unnamed Selection'), cid(selectionCid),
    st.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    h('ul', {}, st.nameClaims.map(c => byCid.get(c)).filter(c => c.verb === 'set').map(c => h('li', {
      'data-name': c.value, 'data-claim': c.cid, 'data-conflicted': c.conflicted ? '' : null },
    String(c.value), ' ', h('span', { class: 'muted' }, `named by ${c.asserted_by ?? 'unknown'} at ${c.timestamp ?? 'unknown'}`)))),
    st.deletion === 'none' ? null : h('div', { class: 'warn' },
      h('p', {}, st.deletion === 'deleted' ? 'Deleted: hidden from the list of Selections until undone.'
        : 'Deletion in conflict: these Claims are all current, so it stays visible.'),
      h('ul', {}, st.deletionClaims.map(c => byCid.get(c)).map(c => h('li', { 'data-deletion-claim': c.cid, 'data-verb': c.verb },
        `${c.verb} by ${c.asserted_by ?? 'unknown'} at ${c.timestamp ?? 'unknown'}`)))),
    h('dl', {}, h('dt', {}, 'first seen in this member'), h('dd', {}, s.firstSeen ?? 'unknown'),
      h('dt', {}, 'assembled by'), h('dd', {}, s.block.asserted_by)),
    actions(selectionCid, st, ctx, status),
    status,
    h('h2', {}, `Members (${s.members.length})`),
    memberList,
    ctx.write.served ? h('p', {}, 'Samplesheet: ',
      h('a', { href: `api/samplesheet/${selectionCid}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
      h('a', { href: `api/samplesheet/${selectionCid}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null,
    h('h2', {}, 'Use it in a workflow'),
    h('p', {}, 'Read this Selection in a downstream workflow. Script kind: ', snippetToggle()),
    snippetBlock({ kind: 'selection', cid: selectionCid }))
}

function actions(selectionCid, st, ctx, status) {
  if (!ctx.write.available) return h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason)
  if (!ctx.write.here) {
    return h('div', {},
      h('p', { 'data-unavailable': '', class: 'muted' }, `Rename, delete and undo write to the writable member, ${ctx.write.writable}. `,
        link(ctx.write.hrefFor(`#/selection/${selectionCid}`), 'Open this Selection there'), '.'),
      h('p', {}, pickButton(ctx, { address: selectionCid, kind: 'selection' })))
  }
  const name = h('input', { id: 'rename-name', placeholder: 'New name', value: '' })
  return h('div', {},
    h('p', {}, name, ' ', h('button', { type: 'button', id: 'rename-save', onclick: (event) => ctx.write.run(status, async () => {
      if (!name.value.trim()) throw Object.assign(new Error('type a name first'), { code: 'invalid' })
      await ctx.write.writer.rename(selectionCid, name.value.trim(), st.nameClaims)
      return { href: `#/selection/${selectionCid}` }
    }, event.currentTarget) }, 'Rename')),
    h('p', {},
      st.deletion === 'deleted' ? null : h('button', { type: 'button', id: 'delete', onclick: (event) => ctx.write.run(status, async () => {
        await ctx.write.writer.remove(selectionCid, st.deletionClaims)
        return { href: `#/selection/${selectionCid}` }
      }, event.currentTarget) }, 'Delete'),
      st.deletion === 'none' ? null : [' ', h('button', { type: 'button', id: 'undo', onclick: (event) => ctx.write.run(status, async () => {
        await ctx.write.writer.undo(selectionCid, st.deletionClaims)
        return { href: `#/selection/${selectionCid}` }
      }, event.currentTarget) }, 'Undo delete')],
      ' ', pickButton(ctx, { address: selectionCid, kind: 'selection' })))
}

/**
 * The banner on the saved Selection's page after a save whose naming or
 * restoring failed (DESIGN.md §16 decision 23). A naming failure needs no
 * button, since Rename is on the page below; a restoring one offers Retry.
 */
export function failureBanner(address, failures, ctx) {
  const status = h('div', { id: 'retry-status' })
  return h('div', { class: 'warn', 'data-write-failed': failures.map(f => f.step).join(' '), 'data-banner-for': address },
    failures.map(f => h('div', {},
      h('p', {}, f.step === 'restoring' ? 'Saved, but restoring it failed:' : 'Saved, but naming it failed:'),
      errorNode({ code: f.code, message: f.message }),
      f.step === 'restoring' ? h('p', {}, h('button', { type: 'button', id: 'retry-restore', onclick: (e) => ctx.write.run(status, async () => {
        await retryRestore(ctx.write.writer, address, f.retry)
        return { address, href: `#/selection/${address}` }
      }, e.currentTarget) }, 'Retry restore'), status) : null)))
}

const deletedNote = (deletion, where) => deletion === 'deleted' ? ` It is deleted ${where}.`
  : deletion === 'conflicted' ? ` Its deletion is in conflict ${where}.` : ''

export function compose(ex, ctx) {
  const entries = ctx.tray.entries()
  const status = h('div', { id: 'write-status' })
  const name = h('input', { id: 'compose-name', placeholder: 'A name for this Selection' })
  const blocked = !ctx.write.available || entries.length === 0 || ctx.tray.onlyOneSelection()
  const save = h('button', { type: 'button', id: 'compose-save', disabled: blocked, onclick: (event) => ctx.write.run(status, async () => {
    const members = ctx.tray.toMembers()
    const saveAndName = async (choice) => {
      const { address, failures } = await saveSequence(ctx.write.writer, members, choice, name.value,
        { onSaved: () => { ctx.tray.clear(); ctx.trayChanged() } })
      return { address, href: `#/selection/${address}`, failures }
    }
    const dry = await ctx.write.writer.selection(members, { dryRun: true })
    const choice = saveChoice(dry)
    const cancel = h('button', { type: 'button', onclick: () => status.replaceChildren() }, 'cancel')
    if (choice.state === 'here') {
      const named = dry.names.length === 0 ? ', unnamed'
        : dry.names.length === 1 ? ` as ${dry.names[0]}` : ` as ${dry.names.join(', ')} (in conflict)`
      status.replaceChildren(h('p', { 'data-exists': dry.address, 'data-deletion': choice.deletion, 'data-names': JSON.stringify(dry.names) },
        `This Selection already exists${named}.${deletedNote(choice.deletion, 'in this composition')} `,
        link(ctx.write.hrefFor(`#/selection/${dry.address}`), 'Open it to rename it'),
        choice.restore.length ? [', ', h('button', { type: 'button', id: 'exists-restore', onclick: (e) => ctx.write.run(status, async () => {
          await ctx.write.writer.undo(dry.address, choice.restore)
          return { address: dry.address, href: `#/selection/${dry.address}` }
        }, e.currentTarget) }, 'Restore')] : null,
        ' or ', cancel, '.'))
      return { outcome: 'exists' }
    }
    if (choice.state === 'elsewhere') {
      if (!name.value.trim() && choice.prefill) name.value = choice.prefill
      const named = choice.names.length === 0 ? ', unnamed'
        : choice.names.length === 1 ? ` as ${choice.names[0]}` : ` as ${choice.names.join(', ')} (in conflict)`
      status.replaceChildren(h('p', { 'data-held-elsewhere': dry.address, 'data-deletion': choice.deletion, 'data-names': JSON.stringify(choice.names) },
        `This Selection is already held in another member${named}.${deletedNote(choice.deletion, 'in this composition')} `,
        h('button', { type: 'button', id: 'compose-copy', onclick: (e) => ctx.write.run(status, () => saveAndName(choice), e.currentTarget) },
          choice.restore.length ? 'Restore a copy here' : 'Save a copy here'), ' or ', cancel, '.'))
      return { outcome: 'elsewhere' }
    }
    return saveAndName(choice)
  }, event.currentTarget) }, 'Save')
  const previews = new Previews(ex)
  // As in the Selection view, a row's index counts item entries only.
  let itemIndex = 0
  const entryNodes = entries.map((e) => {
    const remove = h('button', { type: 'button', onclick: () => { ctx.tray.remove(e.address); ctx.trayChanged(); ctx.rerender() } }, 'Remove')
    return e.kind === 'selection'
      ? h('li', { class: 'row', 'data-tray-entry': e.address, 'data-kind': e.kind },
        h('div', { class: 'row-head' }, h('strong', {}, 'Selection'), link(`#/selection/${e.address}`, cid(e.address)), h('span', { class: 'row-action' }, remove)))
      : itemRow(ex, ctx, { address: e.address, via: e.via, previews, index: itemIndex++, attrs: { 'data-tray-entry': e.address, 'data-kind': e.kind },
        action: remove, viaLabel: 'picked from', noVia: 'a query' })
  })
  const entryList = h('ol', { class: 'rows' }, entryNodes)
  watchRows(entryList, previews, entryNodes.filter((node, i) => entries[i].kind === 'item'))
  return h('section', {},
    h('h1', {}, 'Compose a Selection'),
    ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
    entries.length === 0 ? h('p', { class: 'muted' }, 'The tray is empty. Add items from a run, a collection, an item or a query.') : entryList,
    ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only member is another Selection is legal, but the explorer does not make one: add an item too.') : null,
    h('p', {}, name, ' ', save),
    status)
}
