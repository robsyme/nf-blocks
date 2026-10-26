// web/src/views/selection.js
// #/selections and #/selection/<cid> (DESIGN.md §15; the tray composer is #/compose, compose.js).
import { h, link, copyOutcome } from '../html.js'
import { address, ref } from '../icons.js'
import { Previews } from '../previews.js'
import { retryRestore } from '../save-flow.js'
import { fold } from '../folds.js'
import { whenText } from '../words.js'
import { addrChip, avatar, button, claimTag, durability, empty, pageHead, panel, runFact, shortCid } from '../components.js'
import { presenceOf } from '../presence.js'
import { formatValue, sizeText } from '../format.js'
import { seal } from '../seal.js'
import { labelPaths } from '../pairs.js'
import { matchPromoted, promotedFor } from '../promoted.js'
import { counted } from '../picked.js'
import { extLabel, extOf } from '../sizes.js'
import { copyToButton } from './copy.js'
import { aimCopyTo, projectControl, projectNames, projectsCache } from './addproject.js'
import { fromEach, hrefIn, listsAll, mergeRows, unreadableText } from '../members.js'
import { claimValueOf, claimVerb, copyText, crumbs, errorNode, flag, holds, itemName, mineToggle, nextFrame, pager, nameProjectLinks, pickButton, pinsList, retentionPanel, runLabelNode, table, transfersNode, undoNote, unavailableNote } from './common.js'

const when = (iso) => (iso ? h('span', { title: iso }, whenText(iso)) : 'an unknown time')

/** Why Undo is unavailable on the deleted list, mirroring the Selection view's `actions()`; null renders nothing. */
function undoUnavailableNote(ctx) {
  const note = undoNote(ctx.write.available, ctx.write.here, ctx.write.reason, ctx.write.writable, ctx.write.hrefFor('#/selections?deleted=1'))
  if (!note) return null
  return h('p', { 'data-unavailable': '', class: 'muted' }, note.kind === 'unavailable' ? note.reason
    : [`Undo writes to the writable member, ${note.writable}. `, link(note.href, 'Open this list there'), '.'])
}

export async function selections(ex, { offset = 0, deleted = false, mine = false }, ctx) {
  // Filter by ctx.viewer, the one seam for "who is looking"; an unavailable Mine (anonymous, no identity) shows everything.
  const by = mine && ctx?.viewer ? ctx.viewer : null
  const options = by ? { offset, showDeleted: deleted, by } : { offset, showDeleted: deleted }
  const all = listsAll(ctx)
  const list = all ? await everyMember(ctx, options) : null
  const page = all ? null : await ex.selectionPage(options)
  const query = (...parts) => { const q = parts.filter(Boolean).join('&'); return q ? `#/selections?${q}` : '#/selections' }
  const del = deleted ? 'deleted=1' : null
  const route = query(del, by ? 'mine=1' : null)
  const rows = all ? list.rows.map(m => selectionRow(m.row, ctx, deleted, {
    href: hrefIn(ctx, m.member.alias, `#/selection/${m.row.cid}`), opensIn: m.member.alias,
    dots: presenceOf(ctx).dots(m.row.cid, { hereHeld: null, held: m.heldIn }), undo: list.writableRow(m.row.cid) }))
    : page.rows.map(r => selectionRow(r, ctx, deleted, { undo: r }))
  const none = all ? 'No Selections in any store yet.' : 'No Selections in this member yet.'
  return h('section', all ? { 'data-all-stores': '' } : {},
    crumbs({ text: 'Selections' }),
    h('h1', {}, deleted ? 'Deleted Selections' : 'Selections'),
    mineToggle(ctx?.identity, { allHref: query(del), mineHref: query(del, 'mine=1'), on: !!by }),
    // With showDeleted, hiddenCount counts the current rows left out, so it is shown only in the current view.
    h('p', {}, deleted ? link(query(by ? 'mine=1' : null), 'Show current Selections')
      : [link(query('deleted=1', by ? 'mine=1' : null), 'Show deleted'), page?.hiddenCount ? ` (${page.hiddenCount} on this page)` : '']),
    all ? pagerAll(route, list, offset) : pager(route, page, 'Selections'),
    deleted ? undoUnavailableNote(ctx) : null,
    rows.length === 0 ? (by ? empty(`No Selections signed by ${by}.`, '')
      : h('p', { class: 'muted' }, deleted ? 'No deleted Selections.' : none))
      : table(['name', 'first seen', 'by', 'state', ''], rows),
    all ? list.unreadable.map(a => h('p', { class: 'muted', 'data-member-unreadable': a.member.alias }, unreadableText(a, 'Selections'))) : null)
}

/** One Selections row. In all-stores mode `href` opens it in its first member and `dots` say which stores hold it; `undo` is the writable member's row. */
function selectionRow(r, ctx, deleted, { href = `#/selection/${r.cid}`, opensIn = null, dots = null, undo = null }) {
  const name = link(href, r.state.names.length ? r.state.names.join(' / ') : h('span', { class: 'muted' }, 'unnamed'))
  if (opensIn) name.setAttribute('title', `Opens in ${opensIn}`)
  return h('tr', {
    'data-selection': r.cid, 'data-deletion': r.state.deletion, 'data-source': r.source, 'data-names': JSON.stringify(r.state.names) },
  h('td', {}, name, dots ? [' ', dots] : null,
    r.state.nameConflicted ? flag('names in conflict') : null, r.error ? errorNode(r.error) : null),
  h('td', { style: 'white-space:nowrap' }, r.firstSeen ? when(r.firstSeen) : ''),
  h('td', {}, h('span', { class: 'namecell' }, avatar(r.assertedBy), r.assertedBy ? [' ', h('span', { class: 'muted' }, r.assertedBy)] : null)),
  h('td', {}, r.state.deletion === 'conflicted' ? flag('deletion in conflict') : r.state.deletion === 'deleted' ? 'deleted' : ''),
  h('td', {}, deleted && undo ? undoButton(r.cid, undo.state, ctx) : null))
}

const newestSeen = (a, b) => ((a.row.firstSeen ?? '') < (b.row.firstSeen ?? '') ? 1 : (a.row.firstSeen ?? '') > (b.row.firstSeen ?? '') ? -1 : 0)

/**
 * The same page of every member's Selections (?member=*), one row per cid,
 * newest first seen first. Undo writes beside the writable member's own
 * deletion Claims, so it is offered only on rows that member lists.
 */
async function everyMember(ctx, options) {
  const answers = await fromEach(ctx.members, ex => ex.selectionPage(options))
  const writable = answers.find(a => 'value' in a && a.member.alias === ctx.write?.writable)
  return {
    answers,
    rows: mergeRows(answers, page => page.rows, r => r.cid).sort(newestSeen),
    unreadable: answers.filter(a => !('value' in a)),
    writableRow: (cid) => writable?.value.rows.find(r => r.cid === cid) ?? null,
  }
}

/**
 * The pager for every member's Selections: each store's page holds the same
 * offsets, so a Selection held in several stores may show on more than one
 * page. Each store's total is named, never added up.
 */
function pagerAll(route, list, offset) {
  const pages = list.answers.filter(a => 'value' in a)
  if (pages.every(a => a.value.total === 0)) return null
  const at = (o) => `${route}${route.includes('?') ? '&' : '?'}offset=${o}`
  const prev = pages.map(a => a.value.prev).find(p => p !== null) ?? null
  const next = pages.map(a => a.value.next).filter(n => n !== null)
  const totals = pages.map(a => `${a.member.alias} has ${a.value.total}`).join(', ')
  return h('p', { 'data-page': '', 'data-offset': String(offset), class: 'muted' },
    offset === 0 ? `The newest Selections in each store: ${totals}.` : `Each store's Selections from number ${offset + 1} on: ${totals}.`,
    prev !== null ? [' ', h('a', { href: at(prev), 'data-page-prev': '' }, 'previous page')] : null,
    next.length ? [' ', h('a', { href: at(Math.min(...next)), 'data-page-next': '' }, 'next page')] : null)
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
  const title = st.names.length ? st.names.join(' / ') : 'Unnamed Selection'
  const copy = (text) => h('button', { type: 'button', class: 'btn sm ghost', title: `Copy ${text}`, onclick: async (event) => {
    event.currentTarget.textContent = await copyOutcome(navigator.clipboard, text)
  } }, 'Copy')
  // A Selection may have thousands of members: the table renders at once and
  // each row fills in when its own lookup answers, so one slow or failing
  // member (a hash mismatch, say) never holds up the view or fails it
  // outright (final review finding 2). Spec section 7.1a: each via's Copy
  // copies the member as an Item Occurrence. A row's index counts item
  // members only, so the preview cap and "show details" agree with the items.
  let itemIndex = 0
  const members = s.members.map(m => memberRow(ex, ctx, m, m.kind === 'item' ? itemIndex++ : 0, previews, copy))
  for (const row of members) {
    const { m, held, tr } = row
    ex.held(m.kind, m.address).then(
      (state) => {
        tr.dataset.held = state
        held.textContent = state === 'here' ? '' : 'not in this store'
      },
      (e) => {
        const error = errorNode(e)
        error.textContent = `unavailable (${error.textContent})`
        held.replaceChildren(error)
      })
  }
  const grid = itemsGrid(ex, members, previews, selectionCid)
  const itemCount = s.members.filter(m => m.kind === 'item').length
  const nested = s.members.length - itemCount
  const vias = [...new Set(s.members.flatMap(m => m.via ?? []))]
  const totals = h('span', {})
  totals.replaceChildren(h('b', {}, nested ? `${counted(itemCount, 'item')} and ${counted(nested, 'Selection')}` : counted(itemCount, 'item')))
  ex.selectionSize(selectionCid).then((size) => {
    totals.replaceChildren(h('b', {}, nested ? `${counted(itemCount, 'item')} and ${counted(nested, 'Selection')}` : counted(itemCount, 'item')),
      ` · ${counted(size.files, 'file')} · `, h('b', {}, sizeText(size)))
  }, () => {})
  const claims = railClaims(st)
  ctx.use?.({ selection: selectionCid, members: s.members.length, names: st.names,
    claims, pinned: st.pinned, store: ctx.store?.name ?? null })
  const nameClaims = st.nameClaims.map(c => byCid.get(c)).filter(c => c.verb === 'set')
  const pin = pinArea(ex, selectionCid, st, ctx)
  // Add to project (P4): its memberships from this Selection's Claims (st), so the subline's tags cost no query.
  const cache = projectsCache(ex)
  const projects = projectControl(ex, ctx, { subject: selectionCid, kind: 'selection', claims: st.claims ?? null, state: st, href: `#/selection/${selectionCid}`, cache })
  // A membership in the rail reads as its project's short cid until the projects read (shared with the tags) names it.
  nameProjectLinks(claims.map(r => r.value), (cids) => projectNames(ex, cache, cids))
  const head = headActions(selectionCid, st, ctx, status, pin, projects)
  const transfers = await transfersNode(ex, selectionCid, ctx)
  const deletion = st.deletion === 'deleted' ? durability('releasing', 'Deleted')
    : st.deletion === 'conflicted' ? durability('releasing', 'Deletion in conflict')
      : st.pinned ? durability('pinned', 'Pinned') : durability('held', 'Held')
  return h('section', { 'data-selection-view': selectionCid, 'data-deletion': st.deletion },
    pageHead({
      crumbs: [{ text: 'Selections', href: '#/selections' }, { text: title }],
      seal: selectionCid, title,
      after: [
        nameClaims.map(c => nameClaim(c, st.names.length > 1)),
        deletion, presenceOf(ctx).dots(selectionCid),
      ],
      actions: head.actions,
      subline: [
        totals,
        vias.length === 1 ? h('span', {}, 'from ', runLabelNode(ex, vias[0]))
          : vias.length > 1 ? `from ${vias.length} outputs` : '',
        addrChip(selectionCid),
        projects.tags,
      ].filter(x => x !== '' && x !== null),
    }),
    transfers,
    head.note,
    projects.node,
    st.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    st.deletion === 'none' ? null : h('div', { class: 'warn' },
      h('p', {}, st.deletion === 'deleted' ? 'Deleted: hidden from the list of Selections until undone.'
        : 'Deletion in conflict: these Claims are all current, so it stays visible.'),
      h('ul', {}, st.deletionClaims.map(c => byCid.get(c)).map(c => h('li', { 'data-deletion-claim': c.cid, 'data-verb': c.verb },
        claimTag({ assertedBy: c.asserted_by, verb: c.verb === 'delete' ? 'deleted' : c.verb, at: c.timestamp, conflict: c.conflicted }))))),
    fold(`details:${selectionCid}`, 'Details',
      h('dl', {}, h('dt', {}, 'first seen in this store'), h('dd', {}, s.firstSeen ? when(s.firstSeen) : 'unknown'),
        h('dt', {}, 'assembled by'), h('dd', { 'data-assembled-by': '' }, avatar(s.block.asserted_by), ' ', s.block.asserted_by))),
    pin,
    status,
    panel({ title: s.members.some(m => m.kind === 'selection') ? 'Items and Selections' : 'Items', meta: "fixed list · can't change", flush: true },
      h('div', { class: 'tbox' }, grid.table)),
    ctx.write.served ? panel({ title: 'Samplesheet',
      actions: [h('a', { class: 'btn sm', href: `api/samplesheet/${selectionCid}.csv`, download: '', 'data-samplesheet': 'csv' }, 'Download CSV'),
        h('a', { class: 'btn sm ghost', href: `api/samplesheet/${selectionCid}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')] }, h('p', { class: 'muted' }, 'One row per item, with its metadata and the address of each file.')) : null)
}

/**
 * The Claims section's rows (panel.js draws them as claim tags with relative
 * ages): the current claims, then every superseded one, newest first. A
 * current pin and a current delete have their own panels (the Pins panel and
 * the deleted banner), so they are left out of the current rows. A current
 * reversal (an undo, an unpin) stays, so the rail says who undid it and when;
 * a superseded pin or deletion still shows, muted, as history.
 */
function railClaims(st) {
  const byCid = new Map((st.claims ?? []).map(c => [c.cid, c]))
  // A del is described by the value of a Claim it supersedes (a removed membership names its project).
  const was = (c) => (c.supersedes ?? []).map(s => byCid.get(s)).find(Boolean)?.value ?? null
  const row = (c, superseded) => ({ cid: c.cid, assertedBy: c.asserted_by, verb: claimVerb(c),
    value: claimValueOf(c, was(c)), at: c.timestamp, conflict: !superseded && c.conflicted, superseded })
  const hasOwnPanel = (c) => (c.attribute === 'pin' ? c.verb === 'add' : (c.attribute === null || c.attribute === undefined) && c.verb === 'delete')
  const current = st.current.filter(c => !hasOwnPanel(c)).map(c => row(c, false))
  const replaced = new Set((st.claims ?? []).flatMap(c => c.supersedes ?? []))
  const old = (st.claims ?? []).filter(c => replaced.has(c.cid))
    .sort((a, b) => (a.timestamp < b.timestamp ? 1 : a.timestamp > b.timestamp ? -1 : a.cid < b.cid ? -1 : 1))
    .map(c => row(c, true))
  return [...current, ...old]
}

/** The name as a claim tag: its author's avatar (the anonymous state when it says anonymous), "named", its age; [data-name] and [data-claim] stay for the Gate. */
function nameClaim(c, withValue) {
  const tag = claimTag({ assertedBy: c.asserted_by, verb: 'named', value: withValue ? c.value : null, at: c.timestamp, conflict: c.conflicted, absolute: true })
  tag.dataset.name = c.value
  tag.dataset.claim = c.cid
  if (c.conflicted) tag.dataset.conflicted = ''
  return tag
}

/**
 * One member as a table row, with the parts its cells are rebuilt from. An
 * item row's Sample cell leads with its seal and links to the item (its name
 * is filled in when the preview arrives); a nested Selection reads its name.
 */
function memberRow(ex, ctx, m, index, previews, copy) {
  const held = h('span', { 'data-held-for': m.address, class: 'sub' })
  const tr = h('tr', { 'data-member': m.address, 'data-kind': m.kind })
  if (m.kind === 'selection') {
    const name = h('span', {}, 'Selection')
    ex.selection(m.address).then((s) => { name.textContent = s.state.names.length ? s.state.names.join(' / ') : 'unnamed Selection' }, () => {})
    const size = h('td', { class: 'r num muted nowrap' })
    ex.selectionSize(m.address).then((z) => size.replaceChildren(sizeText(z)), () => {})
    return { m, tr, held, nested: true, size,
      sample: h('td', {}, h('div', { class: 'namecell' }, ref('selection', name, `#/selection/${m.address}`)), held, h('div', { class: 'sub' }, address(m.address))), address: addrChip(m.address) }
  }
  const collection = m.via[0] ?? null
  const href = `#/item/${collection ?? '-'}/${m.address}`
  const label = h('a', { href }, h('span', { class: 'muted' }, index < previews.cap ? 'loading...' : shortCid(m.address)))
  const more = index < previews.cap ? null : h('button', { type: 'button', class: 'btn sm ghost', onclick: (event) => {
    event.currentTarget.remove()
    label.replaceChildren(h('span', { class: 'muted' }, 'loading...'))
    previews.ask(collection, m.address)
  } }, 'show details')
  const size = h('td', { class: 'r num nowrap' })
  return { m, tr, held, label, size, href, collection, drawn: undefined,
    sample: h('td', {}, h('div', { class: 'namecell' }, seal(m.address, { size: 18 }), label, more), held, h('div', { class: 'sub' }, address(m.address))),
    from: h('td', { class: 'muted from-cell' }, fromNode(ex, m.address, m.via, copy)) }
}

/** Each via as a run chip and its output once the run is known, the short collection CID before; a member with no via says so. */
function fromNode(ex, item, via, copy) {
  if (via.length === 0) return h('div', { class: 'from' }, 'a query across runs ', copy(copyText(item, '-')))
  // `run · output` may wrap after the dot, so the table keeps its Size column in view at 1440 (lead's visual finding 10).
  return via.map((v) => {
    const node = h('span', { title: v }, address(v))
    ex.runLabel(v).then((label) => {
      if (label) node.replaceChildren(runFact(label, label.completion), ` · ${label.output}`)
    }, () => {})
    return h('div', { class: 'from' }, node, ' ', copy(copyText(item, v)))
  })
}

/** `forward bigWig`: a file slot as a chip, named by its column, else by its file name; a name with no extension gets no extension text. */
function slotChip(slot) {
  const file = slot.name ?? ''
  const ext = extOf(file)
  const lead = /^\[\d+\]$/.test(slot.column) ? (ext ? file.slice(0, -(ext.length + 1)) : file) : slot.column
  return h('span', { class: 'fileb', title: file }, lead, ext ? [' ', h('span', { class: 'ext' }, extLabel(ext))] : null)
}

const MAX_CHIPS = 3

/**
 * The items table: Sample, one column per promoted field the loaded items
 * carry, Files, From, Size. The promoted fields are those of the pipeline of
 * the first item's run; columns appear when a preview has the field's path.
 */
function itemsGrid(ex, rows, previews, selectionCid) {
  const table = h('table', { class: 't sel-grid' })
  const head = h('thead')
  const body = h('tbody', {}, rows.map(r => r.tr))
  table.append(head, body)
  let fields = []
  let key = null
  let shown = []
  const items = rows.filter(r => !r.nested)

  const cells = (r) => {
    if (r.nested) return [r.sample, ...shown.map(() => h('td')), h('td'), h('td', {}, r.address), r.size]
    const p = previews.get(r.m.address)
    const entries = new Map((p?.pairs ?? []).map(e => [e.path, e]))
    const chips = (p?.slots ?? []).filter(sl => sl.address || sl.name)
    return [r.sample,
      ...shown.map(({ field, path }) => {
        const e = entries.get(path)
        return h('td', {}, e ? e.values.map((v, i) => formatValue(e.types[i], v, field)).join(', ') : '')
      }),
      h('td', { class: 'files' }, h('div', { class: 'kinds' }, chips.slice(0, MAX_CHIPS).map(slotChip), chips.length > MAX_CHIPS ? h('span', { class: 'more' }, `+${chips.length - MAX_CHIPS}`) : null)),
      r.from, r.size]
  }
  const drawHead = () => head.replaceChildren(h('tr', {}, ['Sample', ...shown.map(({ field }) => field.label), 'Files', 'From', 'Size']
    .map((t, i, all) => h('th', { class: i === all.length - 1 ? 'r' : null }, t))))
  const draw = (r) => r.tr.replaceChildren(...cells(r))
  const fill = (r) => {
    const p = previews.get(r.m.address)
    if (p === undefined) return
    r.drawn = p
    if (p.error) {
      r.label.replaceChildren(h('span', { class: 'muted', title: p.message ?? '' }, p.error === 'block_missing' ? shortCid(r.m.address) : `no preview (${p.error})`))
      return
    }
    const paths = labelPaths(items.map(i => previews.get(i.m.address)).filter(x => x?.pairs))
    r.label.replaceChildren(itemName(p, paths) ?? h('span', { class: 'muted' }, 'no Meta Map'))
    if (!r.sized) {
      r.sized = true
      ex.itemSize(r.m.address).then((z) => r.size.replaceChildren(sizeText(z)), () => {})
    }
  }
  const redraw = nextFrame(() => {
    const loaded = items.map(r => previews.get(r.m.address)).filter(p => p?.pairs)
    const next = matchPromoted(fields, [...new Set(loaded.flatMap(p => p.pairs.map(e => e.path)))])
    const nextKey = JSON.stringify(next.map(x => x.path))
    const wide = nextKey !== key
    if (wide) { key = nextKey; shown = next; drawHead() }
    for (const r of rows) {
      if (r.nested) { if (wide) draw(r); continue }
      const p = previews.get(r.m.address)
      if (p === undefined || (p === r.drawn && !wide)) continue
      fill(r)
      draw(r)
    }
  })
  previews.onChange(redraw)
  // The promoted fields come from the first item's pipeline; until they are known the table has its plain columns.
  const first = items.find(r => r.collection)
  if (first) {
    ex.runLabel(first.collection).then(async (label) => {
      if (!label?.completion) return
      fields = promotedFor((await ex.runIdentity(label.completion)).pipeline)
      redraw()
    }).catch(() => {})
  }
  drawHead()
  for (const r of rows) draw(r)
  previews.askFirst(items.map(r => ({ collection: r.collection, item: r.m.address })))
  redraw()
  return { table }
}

/**
 * Rename, Pin…, Delete or Undo delete, Add to project…, Add to picked and
 * Copy to… as the title row's actions; what this page cannot offer it says in `note`, below the head.
 */
function headActions(selectionCid, st, ctx, status, pin, projects) {
  const pick = pickButton(ctx, { address: selectionCid, kind: 'selection' })
  // Copy to… (mockup 08's primary): a link when this page can copy, else disabled with the reason. No request either way.
  const copyTo = copyToButton(ctx, 'selection', selectionCid, { primary: true })
  // It points at the Selection's project's default store once its projects are read (P4 plan 1.7).
  aimCopyTo(copyTo, projects, (choice) => copyToButton(ctx, 'selection', selectionCid, { primary: true, ...choice }))
  if (!ctx.write.available) return { actions: [copyTo], note: unavailableNote(ctx) }
  if (!ctx.write.here) {
    return { actions: [pick, copyTo],
      note: unavailableNote(ctx, 'Rename, delete and undo', 'Open this Selection there', ctx.write.hrefFor(`#/selection/${selectionCid}`)) }
  }
  const name = h('input', { id: 'rename-name', type: 'text', placeholder: 'New name', value: '' })
  const go = (event, fn) => ctx.write.run(status, async () => { await fn(); return { href: `#/selection/${selectionCid}` } }, event.currentTarget)
  return { note: null, actions: [
    name,
    button('Rename', { id: 'rename-save', onclick: (event) => go(event, async () => {
      if (!name.value.trim()) throw Object.assign(new Error('type a name first'), { code: 'invalid' })
      await ctx.write.writer.rename(selectionCid, name.value.trim(), st.nameClaims)
    }) }),
    button('Pin…', { id: 'pin-open', onclick: () => { pin.hidden = !pin.hidden } }),
    st.deletion === 'deleted' ? null : button('Delete', { id: 'delete', kind: 'danger', onclick: (event) => go(event, () => ctx.write.writer.remove(selectionCid, st.deletionClaims)) }),
    st.deletion === 'none' ? null : button('Undo delete', { id: 'undo', onclick: (event) => go(event, () => ctx.write.writer.undo(selectionCid, st.deletionClaims)) }),
    projects.button,
    pick,
    copyTo,
  ] }
}

/**
 * Where Pin… opens: the pins this Selection has and a note form whose button
 * names the bytes it keeps (`Pin 1.14 MB in roblab`) before it is pressed.
 * Shown from the start when it already has pins.
 */
function pinArea(ex, selectionCid, st, ctx) {
  if (!ctx.write.available || !ctx.write.here) {
    // Read-only viewers still see who pinned what, and when.
    const list = pinsList(st, { can: false })
    return list ? panel({ title: 'Pins', meta: `in ${ctx.store.name}` }, list) : null
  }
  const area = h('div', { id: 'pin-area', hidden: !st.pinned })
  // The Pin label learns the bytes in place, so a typed note and the status node survive.
  area.replaceChildren(panel({ title: 'Pins', meta: `in ${ctx.store.name}` },
    retentionPanel(selectionCid, st, ctx, { kind: 'Selection', href: ctx.write.hrefFor(`#/selection/${selectionCid}`), size: ex.selectionSize(selectionCid), ...holds(ex, selectionCid) })))
  return area
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
