// web/src/views/selection.js
// #/selections, #/selection/<cid> and the tray composer (DESIGN.md §15).
import { h, link, cid, copyOutcome } from '../html.js'
import { Previews } from '../previews.js'
import { saveChoice } from '../save-choice.js'
import { saveSequence, retryRestore } from '../save-flow.js'
import { snippetBlock, snippetToggle } from '../snippets.js'
import { copyText, errorNode, flag, itemRow, pager, pickButton, table, undoNote, unavailableNote, watchRows } from './common.js'

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
