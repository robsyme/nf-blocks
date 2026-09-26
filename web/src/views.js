// web/src/views.js
// One function per route (DESIGN.md §15). Each returns a node; the data-*
// attributes are the contract the Gate reads, the rest is for people.
import { h, link, cid } from './html.js'
import { saveChoice } from './save-choice.js'
import { saveSequence, retryRestore } from './save-flow.js'

const enc = encodeURIComponent

export function errorNode(e) {
  return h('p', { 'data-error': e?.code ?? 'query_failed', 'data-cid': e?.cid ?? null }, e?.message ?? String(e))
}

function table(head, rows) {
  return h('div', { class: 'scroll' }, h('table', {}, h('tr', {}, head.map(t => h('th', {}, t))), rows))
}

/** What Copy on a Selection member copies (spec 7.1a): the bare item address when it has no via, else `cas://<via>/<address>`. */
export const copyText = (address, via) => (via === '-' ? address : `cas://${via}/${address}`)

/** What the Copy button should say after `clipboard.writeText(text)`; a missing clipboard counts as a rejection (page minors, ticket 11). */
export async function copyOutcome(clipboard, text) {
  if (!clipboard) return 'Copy failed'
  try {
    await clipboard.writeText(text)
    return 'Copied'
  } catch {
    return 'Copy failed'
  }
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

export async function collection(ex, collectionCid, offset = 0, ctx) {
  const c = await ex.collection(collectionCid, { offset })
  return h('section', {},
    h('h1', {}, c.output), cid(collectionCid),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    pager(`#/collection/${collectionCid}`, c, 'items'),
    h('ul', {}, c.items.map(i => h('li', {}, link(`#/item/${collectionCid}/${i}`, h('code', { class: 'cid' }, `cas://${collectionCid}/${i}`)),
      ' ', pickButton(ctx, { address: i, via: [collectionCid] })))))
}

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const view = it.view ?? {}
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  return h('section', {},
    h('h1', {}, 'Item'),
    // `-` is an item reached with no collection (a Selection member picked by a query).
    h('p', {}, collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
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
      collectionCid ? link(`#/item/${collectionCid}/${i}`, cid(i)) : cid(i),
      // Spec section 5.6: an item chosen by a query is picked with no via.
      ' ', pickButton(ctx, { address: i, via: [] })))))
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

/** Why Undo is unavailable on the deleted list, mirroring the Selection view's `actions()`. */
function undoUnavailableNote(ctx) {
  if (!ctx.write.available) return h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason)
  return h('p', { 'data-unavailable': '', class: 'muted' }, `Undo writes to the writable member, ${ctx.write.writable}. `,
    link(ctx.write.hrefFor('#/selections?deleted=1'), 'Open this list there'), '.')
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
    deleted && (!ctx.write.available || !ctx.write.here) ? undoUnavailableNote(ctx) : null,
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
  // A Selection may have thousands of members: the table renders at once and
  // each row's "held in" cell fills in when its own lookup answers, so one
  // slow or failing member (a hash mismatch, say) never holds up the view or
  // fails it outright (final review finding 2).
  const members = s.members.map((m) => {
    // Spec section 7.1a: members shown as Item Occurrences, copyable as links.
    const copy = (text) => h('button', { type: 'button', onclick: async (event) => {
      event.currentTarget.textContent = await copyOutcome(navigator.clipboard, text)
    } }, 'Copy')
    const where = m.kind === 'selection'
      ? link(`#/selection/${m.address}`, cid(m.address))
      : (m.via.length ? m.via : ['-']).map(v => h('div', {}, v === '-' ? [link(`#/item/-/${m.address}`, cid(m.address)), ' ', copy(copyText(m.address, v))]
        : [link(`#/item/${v}/${m.address}`, h('code', { class: 'cid' }, `cas://${v}/${m.address}`)), ' ', copy(copyText(m.address, v))]))
    const held = h('td', { 'data-held-for': m.address, class: 'muted' }, '...')
    const tr = h('tr', { 'data-member': m.address, 'data-kind': m.kind }, h('td', {}, where), h('td', {}, m.kind), held)
    return { m, held, tr }
  })
  for (const { m, held, tr } of members) {
    ex.held(m.kind, m.address).then(
      (state) => {
        tr.dataset.held = state
        held.textContent = state === 'here' ? 'this member' : 'another member'
        held.classList.remove('muted')
      },
      (e) => {
        const node = errorNode(e)
        node.textContent = `unavailable (${node.textContent})`
        held.replaceChildren(node)
        held.classList.remove('muted')
      })
  }
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
    table(['member', 'kind', 'held in'], members.map(({ tr }) => tr)),
    ctx.write.served ? h('p', {}, 'Samplesheet: ',
      h('a', { href: `api/samplesheet/${selectionCid}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
      h('a', { href: `api/samplesheet/${selectionCid}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null)
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
  return h('section', {},
    h('h1', {}, 'Compose a Selection'),
    ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
    entries.length === 0 ? h('p', { class: 'muted' }, 'The tray is empty. Add items from a run, a collection, an item or a query.')
      : table(['member', 'kind', 'picked from', ''], entries.map(e => h('tr', { 'data-tray-entry': e.address, 'data-kind': e.kind },
        h('td', {}, cid(e.address)), h('td', {}, e.kind), h('td', {}, e.via.length ? e.via.map(v => h('div', {}, cid(v))) : h('span', { class: 'muted' }, 'a query')),
        h('td', {}, h('button', { type: 'button', onclick: () => { ctx.tray.remove(e.address); ctx.trayChanged(); ctx.rerender() } }, 'Remove'))))),
    ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only member is another Selection is legal, but the explorer does not make one: add an item too.') : null,
    h('p', {}, name, ' ', save),
    status)
}
