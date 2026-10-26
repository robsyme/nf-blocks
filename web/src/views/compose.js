// web/src/views/compose.js
// #/compose: the picked items in the main pane (they used to open the
// Pipelines table), grouped by the run and output they came from, each named
// by its sample id, then Save as Selection and Clear. The save is the panel's
// own flow (picked.js saveFlow), so the dry run, "exists here", "held
// elsewhere" and failure states read the same from either place.
import { h } from '../html.js'
import { sizeText } from '../format.js'
import { button, empty, pageHead, panel } from '../components.js'
import { Previews } from '../previews.js'
import { trayNote } from '../tray.js'
import { counted, entryRow, groupEntries, nameEntries, pickedSize, saveFlow, sourceLabel } from '../picked.js'
import { ref, shortCid } from '../icons.js'
import { enc } from './common.js'

/**
 * A group's heading, `run / output`, from blocks and what the views recorded
 * (picked.js sourceLabel, no snapshot query): the short collection CID until
 * it arrives, the run linked when its RunCompletion is known.
 */
function sourceHead(ex, ctx, collectionCid) {
  const node = h('span', { title: collectionCid }, h('code', { class: 'cid' }, shortCid(collectionCid)))
  sourceLabel(ex, ctx, collectionCid).then((label) => {
    if (!label) return
    const c = label.completion
    node.replaceChildren(...[
      label.run_name ? ref('run', label.run_name, c ? `#/run/${c}` : null) : null, label.run_name ? ' / ' : null,
      ref('output', label.output, c ? `#/items/${c}/${enc(label.output)}?where=%5B%5D` : null)].filter(Boolean))
  }, () => {})
  return node
}

export async function compose(ex, ctx) {
  const previews = new Previews(ex)
  const name = h('input', { id: 'compose-name', type: 'text', placeholder: 'A name, e.g. treated BAMs' })
  name.value = ''
  const status = h('div', { id: 'write-status' })
  const body = h('div', { class: 'compose-body' })
  let stopNaming = null
  let drawn = 0

  // The write goes through ctx.write.run, which records its own outcome and
  // disables the button while it runs; this page keeps `status` and `name`
  // across redraws, so a failure message outlives a Remove.
  const guarded = async (control, fn) => {
    const before = ctx.tray.size
    await ctx.write.run(status, fn, control)
    // A save that cleared the tray redraws the list, so a second Save cannot send an empty dry run.
    if (ctx.tray.size !== before) draw()
  }
  const removeAll = (list) => { for (const e of list) ctx.tray.remove(e.address); ctx.trayChanged(); draw() }

  function draw() {
    const mine = ++drawn
    stopNaming?.()
    const entries = ctx.tray.entries()
    if (entries.length === 0) {
      body.replaceChildren(pageHead({ title: 'Picked', subline: ['Nothing picked yet'] }),
        empty('Nothing picked yet', 'Pick items on an output or item page, then save them here as a Selection.'), status)
      return
    }
    const size = h('span', { 'data-picked-size': '' })
    pickedSize(ex, entries).then((s) => {
      if (mine !== drawn) return
      size.replaceChildren(' · ', h('b', {}, sizeText(s)))
    }, () => {})
    const rows = []
    const groups = [...groupEntries(entries)].map(([key, members]) => {
      const entryRows = members.map(e => entryRow(e, { tag: 'div', sealSize: 18, remove: () => removeAll([e]) }))
      rows.push(...entryRows)
      const head = key.startsWith('selection:') ? 'Selection' : key === 'via:-' ? 'Picked by a query across runs' : sourceHead(ex, ctx, key.slice(4))
      return panel({ title: head, meta: counted(members.length, key.startsWith('selection:') ? 'Selection' : 'item'),
        actions: [button('Remove these', { kind: 'ghost', size: 'sm', 'aria-label': 'remove these', onclick: () => removeAll(members) })] },
      h('div', { class: 'picked-list' }, entryRows.map(r => r.row)))
    })
    stopNaming = nameEntries(ex, rows, previews)
    const blocked = !ctx.write.available || ctx.tray.onlyOneSelection()
    const save = button('Save as Selection', { kind: 'primary', id: 'compose-save', disabled: blocked,
      onclick: (event) => { if (ctx.tray.size > 0) return guarded(event.currentTarget, () => saveFlow(ctx, status, name, guarded)) } })
    const clear = button('Clear', { onclick: () => { ctx.tray.clear(); ctx.trayChanged(); draw() } })
    body.replaceChildren(
      pageHead({ title: 'Picked',
        subline: [h('span', {}, h('b', {}, counted(entries.length, 'item')), size)] }),
      ...groups,
      panel({ title: 'Save as Selection' },
        trayNote(ctx.tray) ? h('p', { class: 'warn' }, trayNote(ctx.tray)) : null,
        ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only entry is another Selection is legal, but the explorer does not make one: add an item too.') : null,
        h('p', { class: 'muted' }, 'A filter cannot describe these, so save them as a Selection: a fixed list of these exact items.'),
        ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
        h('div', { class: 'compose-actions' }, name, save, clear),
        status))
  }

  draw()
  return h('section', { class: 'compose', 'data-compose': '' }, body)
}
