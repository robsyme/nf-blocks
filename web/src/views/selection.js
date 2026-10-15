// web/src/views/selection.js
// #/selections, #/selection/<cid> and the tray composer (DESIGN.md §15).
import { h, link, copyOutcome } from '../html.js'
import { address, icon, ref } from '../icons.js'
import { Previews } from '../previews.js'
import { retryRestore } from '../save-flow.js'
import { fold } from '../folds.js'
import { whenText } from '../words.js'
import { copyText, crumbs, errorNode, flag, itemRow, pager, pickButton, table, undoNote, unavailableNote, watchRows } from './common.js'

const when = (iso) => (iso ? h('span', { title: iso }, whenText(iso)) : 'an unknown time')

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
    crumbs({ text: 'Selections' }),
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
  const copy = (text) => h('button', { type: 'button', class: 'quiet', title: `Copy ${text}`, onclick: async (event) => {
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
    const held = h('span', { 'data-held-for': m.address, class: 'muted' })
    const node = m.kind === 'selection'
      ? nestedRow(ex, m, held)
      : itemRow(ex, ctx, { address: m.address, via: m.via, previews, index: itemIndex++, attrs: { 'data-member': m.address, 'data-kind': m.kind },
        action: held, fromLabel: 'from', noVia: 'no run (picked by a query across runs)', perVia: (v) => copy(copyText(m.address, v)) })
    return { m, held, node }
  })
  for (const { m, held, node } of members) {
    ex.held(m.kind, m.address).then(
      (state) => {
        node.dataset.held = state
        held.textContent = state === 'here' ? '' : 'not in this store'
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
  ctx.use?.({ selection: selectionCid, members: s.members.length, names: st.names })
  return h('section', { 'data-selection-view': selectionCid, 'data-deletion': st.deletion },
    crumbs({ text: 'Selections', href: '#/selections' }, { text: st.names.length ? st.names.join(' / ') : 'Unnamed Selection' }),
    h('h1', {}, icon('selection'), st.names.length ? st.names.join(' / ') : 'Unnamed Selection'),
    st.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    h('p', { class: 'selection-meta muted' },
      st.nameClaims.map(c => byCid.get(c)).filter(c => c.verb === 'set').map((c, i) => h('span', {
        'data-name': c.value, 'data-claim': c.cid, 'data-conflicted': c.conflicted ? '' : null },
      i ? ' · ' : null, icon('claim'), st.names.length > 1 ? `${c.value}: ` : '', `named by ${c.asserted_by ?? 'unknown'} · `, when(c.timestamp))),
      ' ', address(selectionCid)),
    st.deletion === 'none' ? null : h('div', { class: 'warn' },
      h('p', {}, st.deletion === 'deleted' ? 'Deleted: hidden from the list of Selections until undone.'
        : 'Deletion in conflict: these Claims are all current, so it stays visible.'),
      h('ul', {}, st.deletionClaims.map(c => byCid.get(c)).map(c => h('li', { 'data-deletion-claim': c.cid, 'data-verb': c.verb },
        `${c.verb} by ${c.asserted_by ?? 'unknown'}, `, when(c.timestamp))))),
    fold(`details:${selectionCid}`, 'Details',
      h('dl', {}, h('dt', {}, 'first seen in this store'), h('dd', {}, s.firstSeen ? when(s.firstSeen) : 'unknown'),
        h('dt', {}, 'assembled by'), h('dd', {}, s.block.asserted_by))),
    actions(selectionCid, st, ctx, status),
    status,
    h('h2', {}, `${s.members.some(m => m.kind === 'selection') ? 'Items and Selections' : 'Items'} (${s.members.length})`),
    memberList)
}

/** A nested Selection as a row: its icon and name (read lazily), where it is held, its address. */
function nestedRow(ex, m, held) {
  const name = h('span', {}, 'Selection')
  ex.selection(m.address).then((s) => { name.textContent = s.state.names.length ? s.state.names.join(' / ') : 'unnamed Selection' }, () => {})
  return h('li', { class: 'row', 'data-member': m.address, 'data-kind': m.kind },
    h('div', { class: 'row-head' }, ref('selection', name, `#/selection/${m.address}`), h('span', { class: 'row-action' }, held)),
    h('div', { class: 'row-sub' }, address(m.address)))
}

function actions(selectionCid, st, ctx, status) {
  if (!ctx.write.available) return unavailableNote(ctx)
  if (!ctx.write.here) {
    return h('div', {},
      unavailableNote(ctx, 'Rename, delete and undo', 'Open this Selection there', ctx.write.hrefFor(`#/selection/${selectionCid}`)),
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
