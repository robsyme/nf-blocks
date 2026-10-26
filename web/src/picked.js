// web/src/picked.js
// What the Picked panel (panel.js) and #/compose (views/compose.js) share: the
// entries grouped by source, each entry drawn as a seal, its sample name and
// its short CID, the names filled in as previews arrive, the picked size, and
// the save flow (dry run, then "exists here", "held elsewhere", or save and
// name; DESIGN.md §16 decisions 9 and 23). Both surfaces call the same saveFlow.
import { h, link } from './html.js'
import { shortCid } from './components.js'
import { seal } from './seal.js'
import { icon } from './icons.js'
import { labelPaths } from './pairs.js'
import { saveChoice } from './save-choice.js'
import { saveSequence } from './save-flow.js'
import { sumUnique } from './sizes.js'
import { itemName } from './views/common.js'

export const counted = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`

/**
 * The run and output a picked group came from, with no snapshot query:
 * `{ run_name, output, completion }`. What a view recorded in `ctx.sources`
 * (the run's RunCompletion, so its page can be linked) named through
 * Explorer.runIdentity; else, after a reload, the OutputCollection block and
 * its RunManifest (Explorer.collectionLabel), which name the run but not its
 * RunCompletion. Null when neither can be read.
 */
export async function sourceLabel(ex, ctx, collectionCid) {
  const source = ctx.sources?.get(collectionCid)
  if (source) {
    let run = null
    try { run = (await ex.runIdentity(source.completion))?.run_name ?? null } catch { /* the output alone */ }
    return { run_name: run, output: source.output, completion: source.completion }
  }
  try {
    const label = await ex.collectionLabel(collectionCid)
    return { run_name: label.run_name, output: label.output, completion: null }
  } catch {
    return null
  }
}

/** `run / output` for a group heading, or the output alone when the run is not known. */
export const sourceText = (label) => (label.run_name ? `${label.run_name} / ${label.output}` : label.output)

/** Entries by source: a nested Selection each its own group, items by the collection they were picked from (`via:-` when from a query). */
export function groupEntries(entries) {
  const groups = new Map()
  for (const e of entries) {
    const key = e.kind === 'selection' ? `selection:${e.address}` : `via:${e.via[0] ?? '-'}`
    groups.set(key, [...(groups.get(key) ?? []), e])
  }
  return groups
}

/** The unique bytes over the picked items, `{ bytes, partial }`; a picked Selection adds nothing here (it is counted when it is opened). */
export async function pickedSize(ex, entries) {
  const items = entries.filter(e => e.kind !== 'selection')
  const maps = await Promise.all(items.map(e => ex.itemFileMap(e.address)))
  const { bytes, partial } = sumUnique(maps)
  return { bytes, partial: partial || entries.length !== items.length }
}

/**
 * One picked entry as a list item: its seal, its name (filled by `nameEntries`),
 * its short CID as secondary mono text. `remove`, when given, draws a Remove
 * button. Keeps [data-tray-entry] and [data-kind].
 */
export function entryRow(entry, { remove = null, tag = 'li', sealSize = 16 } = {}) {
  const name = h('span', { class: 'pname' })
  const row = h(tag, { 'data-tray-entry': entry.address, 'data-kind': entry.kind, class: 'pentry' },
    entry.kind === 'selection' ? icon('selection') : seal(entry.address, { size: sealSize }),
    name, h('code', { class: 'cid', title: entry.address }, shortCid(entry.address)),
    remove ? h('button', { type: 'button', class: 'btn sm ghost', 'aria-label': `Remove ${shortCid(entry.address)}`, onclick: remove }, 'Remove') : null)
  return { row, name, entry }
}

/**
 * Names `rows` (from `entryRow`): an item by its sample id once its preview
 * arrives (Previews reads the item's block through Explorer.itemBlock, no
 * snapshot query), a Selection by its name, which is a Claim and so a snapshot
 * read (`selections: false` skips it). `previews` is a Previews over `ex`.
 * Returns a function that stops listening, for a list that is being redrawn.
 */
export function nameEntries(ex, rows, previews, { selections = true } = {}) {
  const items = rows.filter(r => r.entry.kind !== 'selection')
  // The panel never reads the snapshot, and a Selection's name is a claim read from it: there it stays "Selection" beside its short CID.
  for (const r of rows.filter(r => r.entry.kind === 'selection')) {
    if (!selections) { r.name.textContent = 'Selection'; continue }
    Promise.resolve(ex.selection?.(r.entry.address)).then((s) => {
      if (s) r.name.textContent = s.state.names.length ? s.state.names.join(' / ') : 'Unnamed Selection'
    }, () => {})
  }
  const fill = () => {
    const loaded = items.map(r => previews.get(r.entry.address)).filter(p => p?.pairs)
    const paths = labelPaths(loaded)
    for (const r of items) {
      const text = itemName(previews.get(r.entry.address), paths)
      if (text) r.name.textContent = text
    }
  }
  const off = previews.onChange(fill)
  previews.askFirst(items.map(r => ({ collection: r.entry.via[0] ?? null, item: r.entry.address })))
  fill()
  return off
}

const deletedNote = (deletion, where) => deletion === 'deleted' ? ` It is deleted ${where}.`
  : deletion === 'conflicted' ? ` Its deletion is in conflict ${where}.` : ''

/**
 * Save the picked entries as a Selection: dry run, then "exists here",
 * "held elsewhere", or save and name. `guarded(button, fn)` runs a write for
 * the caller's surface (it knows how to keep its own redraws from clobbering
 * the status); `status` and `name` are that surface's own nodes. `members`
 * defaults to the picked list, which empties once the Selection is written;
 * `onSaved(address)` replaces that step for other members.
 */
export async function saveFlow(ctx, status, name, guarded, {
  members = ctx.tray.toMembers(),
  onSaved = () => { ctx.tray.clear(); ctx.trayChanged() },
} = {}) {
  const saveAndName = async (choice) => {
    const { address, failures } = await saveSequence(ctx.write.writer, members, choice, name.value, { onSaved })
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
      choice.restore.length ? [', ', h('button', { type: 'button', id: 'exists-restore', onclick: (e) => guarded(e.currentTarget, async () => {
        await ctx.write.writer.undo(dry.address, choice.restore)
        return { address: dry.address, href: `#/selection/${dry.address}` }
      }) }, 'Restore')] : null,
      ' or ', cancel, '.'))
    return { outcome: 'exists' }
  }
  if (choice.state === 'elsewhere') {
    if (!name.value.trim() && choice.prefill) name.value = choice.prefill
    const named = choice.names.length === 0 ? ', unnamed'
      : choice.names.length === 1 ? ` as ${choice.names[0]}` : ` as ${choice.names.join(', ')} (in conflict)`
    status.replaceChildren(h('p', { 'data-held-elsewhere': dry.address, 'data-deletion': choice.deletion, 'data-names': JSON.stringify(choice.names) },
      `This Selection is already held in another member${named}.${deletedNote(choice.deletion, 'in this composition')} `,
      h('button', { type: 'button', id: 'compose-copy', onclick: (e) => guarded(e.currentTarget, () => saveAndName(choice)) },
        choice.restore.length ? 'Restore a copy here' : 'Save a copy here'), ' or ', cancel, '.'))
    return { outcome: 'elsewhere' }
  }
  return saveAndName(choice)
}
