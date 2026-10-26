// web/src/panel.js
// The "Use in a workflow" panel (explorer layout B spec §6). panelState picks
// one state from the route, what the view reported (ctx.use) and the picked
// list; createPanel draws it. A call on screen always returns what it says;
// the Samples tab's proposed call says it is one (handoff.js).
// The panel reads runs from their blocks only (Explorer.runIdentity), never
// the snapshot (a picked group is named from ctx.sources), and shows failures
// without [data-error] (plan P2, P14).
import { h, link } from './html.js'
import { createHandoff } from './handoff.js'
import { claimTag } from './components.js'
import { pressSeg, snippetBlock, snippetToggle, whereRepeatsPath } from './snippets.js'
import { fold } from './folds.js'
import { trayNote } from './tray.js'
import { valueText } from './pairs.js'
import { Previews } from './previews.js'
import { plural } from './format.js'
import { counted, entryRow, groupEntries, nameEntries, saveFlow, sourceLabel, sourceText } from './picked.js'

const OUTPUT_ROUTES = new Set(['items', 'item', 'collection'])
// Pages drawn without the rail: Storage (mockup 09) has nothing to use in a workflow, and its tables need the width; Copy (mockup 11) neither,
// nor the Projects list and a project's page (mockup 13), whose Samples table needs the width for its Where column.
const RAILLESS_ROUTES = new Set(['storage', 'copy', 'projects', 'project'])

/** Whether the page for `route` has the "Use in a workflow" rail (app.js toggles `.app.has-rail` by it). */
export const showsRail = (route) => !RAILLESS_ROUTES.has(route)

const whereText = (where) => where.map(([p, t, v, op = '=']) => `${p} ${op} ${valueText(t, v)}`).join(', ')

/**
 * The page a hash shows, for keeping a write's status: the Samples tab
 * rewrites its own `where` and `show` with replaceState as the reader types,
 * which is no move to another page.
 */
function pageOf(hash) {
  const text = String(hash ?? '')
  const at = text.indexOf('?')
  if (!text.startsWith('#/run/') || at < 0) return hash
  const q = new URLSearchParams(text.slice(at + 1))
  q.delete('where')
  q.delete('show')
  return `${text.slice(0, at)}?${q}`
}

/** Spec §6.1. `view` is what the current view passed to ctx.use; `picked` the tray's size. */
export function panelState({ route, view = null, picked = 0 }) {
  if (route === 'selection' && view?.selection) return { state: 'saved', selection: view.selection, members: view.members ?? null }
  // The run's Samples tab (P1a): the hand-off rail (handoff.js), which links a non-empty picked list.
  if (route === 'run' && view?.samples) return { state: 'samples', completion: view.completion, samples: view.samples }
  // A hand-off template's page (P4, mockup 14): its own Saves and Run command sections, drawn by the view.
  if (route === 'handoff' && view?.rail) return { state: 'template', rail: view.rail }
  if (picked > 0) return { state: 'picked' }
  if (OUTPUT_ROUTES.has(route) && view?.completion && view.output) {
    const where = view.where ?? []
    const count = view.count ?? null
    const checked = view.checked ?? count
    return { state: where.length ? 'filtered' : 'whole', completion: view.completion, output: view.output, where, count,
      partial: count !== null && checked !== null && checked < count ? { checked, count } : null }
  }
  return { state: 'none' }
}


/**
 * The label of the narrow-screen toggle (spec §3.1): "Use · N", N the items the panel would use. On the Samples
 * tab it counts the ticked samples, as the tab's "Use N samples" button does: "Use · 2 samples".
 */
export function useLabel(s, tray) {
  if (s.state === 'samples') return `Use · ${plural(s.samples.selected.length, 'sample')}`
  const n = s.state === 'whole' || s.state === 'filtered' ? s.count
    : s.state === 'picked' ? tray.size
    : s.state === 'saved' ? s.members : null
  return n === null || n === undefined ? 'Use' : `Use · ${n}`
}

export function createPanel({ el, ex, ctx, onState = null }) {
  let drawn = 0
  let last = { route: 'open', view: null }
  let mode = 'this'
  let modeKey = null
  let note = null
  let saving = false
  let saveHash // the hash the running save started on
  let statusHash // the page the status was written on; it goes when the reader moves on
  let previews = null // names the picked items; one per panel, so a redraw keeps what it has read
  let stopNaming = null
  // Made once and re-appended on every picked draw, so a redraw never detaches
  // the save feedback or loses the typed name.
  const name = h('input', { id: 'compose-name', type: 'text', placeholder: 'A name, e.g. treated BAMs' })
  name.value = ''
  const status = h('div', { id: 'write-status' })

  const here = () => pageOf(globalThis.location?.hash)

  // Every write the panel starts goes through here: no rebuild while it runs,
  // one draw when it settles.
  async function guarded(button, fn) {
    saving = true
    const hash = here()
    saveHash = hash
    try {
      await ctx.write.run(status, fn, button)
    } finally {
      saving = false
      // A save that navigated leaves stale "Saving..." text; one that could not
      // refresh leaves its message, which must outlive the picked state.
      if (here() !== hash) status.replaceChildren()
      statusHash = here()
      await draw()
    }
  }
  const handoff = createHandoff({ ex, ctx, status, guarded, redraw: () => draw() })

  async function draw(input = last) {
    last = input
    // Held only while the page is still where the save began: a save that navigated is done writing.
    if (saving && here() === saveHash) return
    if (status.childNodes.length && here() !== statusHash) status.replaceChildren()
    const mine = ++drawn
    const s = panelState({ ...input, picked: ctx.tray.size })
    const key = s.completion ? `${s.completion}/${s.output}` : null
    if (key !== modeKey) { mode = 'this'; note = null; modeKey = key }
    let body
    let failed = false
    try {
      body = await bodyOf(s, input.view)
    } catch (e) {
      failed = true
      body = h('p', { class: 'warn' }, `Could not prepare the call: ${e?.message ?? e}`)
    }
    if (mine !== drawn) return
    if ((s.state === 'samples' || s.state === 'template') && !failed) {
      // The hand-off's own sections (mockups 07 and 14), with the write status in their save section.
      el.replaceChildren(h('div', { 'data-panel-state': s.state, class: 'handoff' }, body))
    } else {
      const feedback = s.state !== 'picked' && status.childNodes.length ? status : null
      const claims = s.state === 'saved' ? claimsSection(input.view) : null
      el.replaceChildren(...[h('section', { 'data-panel-state': s.state, class: 'sec' }, h('h3', {}, 'Use in a workflow'), body, feedback), claims].filter(Boolean))
    }
    try { onState?.(s) } catch { /* a label failure must not break the panel */ }
  }

  async function bodyOf(s, view) {
    if (s.state === 'saved') return saved(s, view)
    if (s.state === 'samples') return handoff.body(s)
    if (s.state === 'template') return s.rail
    if (s.state === 'picked') return picked(last.route)
    if (s.state === 'none') return h('p', { class: 'muted' }, 'Open an output to use it in a workflow.')
    return output(s, view)
  }

  async function output(s, view) {
    const id = await ex.runIdentity(s.completion)
    const pickButton = (n, disabled) => h('button', { type: 'button', id: 'pick-checked', disabled, onclick: () => view?.pickChecked?.() }, `Pick these ${n}`)
    if (whereRepeatsPath(s.where)) {
      const checked = s.partial ? s.partial.checked : s.count
      return [
        h('p', {}, `${counted(s.count ?? 0, 'item')} of ${s.output}, where ${whereText(s.where)}`),
        h('p', { class: 'muted' }, 'A fromStore filter holds one value per key, so this filter, with a key used twice, cannot be copied as a call.'),
        s.count > 0 ? h('p', {}, s.partial ? `${checked} of ${s.count} checked. ` : '', pickButton(checked, checked === 0), ' to use them as a Selection.') : null,
      ]
    }
    const what = s.state === 'filtered' ? `${counted(s.count ?? 0, 'item')} of ${s.output}, where ${whereText(s.where)}`
      : s.count === null ? `Every item of ${s.output}` : `All ${counted(s.count, 'item')} of ${s.output}`
    // This run by its RunCompletion address, not its lid://: a lid:// is looked
    // up in the reader's composition and the newest run carrying it wins.
    const spec = mode === 'latest' ? { kind: 'latest', pipeline: id.pipeline, output: s.output, where: s.where }
      : { kind: 'run', run: `cas://${s.completion}`, output: s.output, where: s.where }
    return [
      h('p', { class: 'muted small' }, what),
      h('div', { class: 'row seg-row' }, id.pipeline ? runSwitch(id) : null, snippetToggle()),
      snippetBlock(spec),
      mode === 'latest' ? h('p', { class: 'muted' }, '"latest" may match different items after the next run.') : null,
      note ? h('p', { class: 'muted' }, note) : null,
      s.partial ? h('p', {}, `${s.partial.checked} of ${s.partial.count} checked. `,
        pickButton(s.partial.checked, s.partial.checked === 0)) : null,
    ]
  }

  function runSwitch(id) {
    const choose = async (next) => {
      const key = modeKey
      try {
        if (next === 'latest') {
          const best = await ex.latestSuccessfulRun(id.pipeline)
          if (modeKey !== key) return
          if (!best) { note = `${id.pipeline} has no good run, so "latest" would find nothing.`; return draw() }
          const newer = best === id.completion ? null : (await ex.runIdentity(best)).run_name ?? best
          if (modeKey !== key) return
          note = newer === null ? null : `The latest good run is now ${newer}, not this one.`
        } else {
          note = null
        }
        mode = next
      } catch (e) {
        if (modeKey !== key) return
        note = `Could not find the latest good run: ${e?.message ?? e}`
      }
      return draw()
    }
    const buttons = ['this', 'latest'].map(m => h('button', { type: 'button', class: 'seg', 'data-mode': m, onclick: () => choose(m) },
      m === 'this' ? 'This run' : 'Latest good run'))
    pressSeg(buttons, b => b.dataset.mode === mode)
    return h('div', { class: 'seg-toggle run-switch', role: 'group', 'aria-label': 'which run', 'data-run-mode': mode }, buttons)
  }

  /** Mockup 08's rail Claims: one claim tag per row, then "No pins in <store>." when nothing pins it. */
  function claimsSection(view) {
    const rows = (view?.claims ?? []).map(c => h('div', { class: 'row', 'data-rail-claim': c.cid, 'data-superseded': c.superseded ? '' : null }, claimTag(c)))
    const noPins = view?.pinned === false ? h('p', { class: 'muted' }, `No pins in ${view.store ?? 'this store'}.`) : null
    if (!rows.length && !noPins) return null
    return h('section', { class: 'sec', 'data-rail-claims': '' }, h('h3', {}, 'Claims'), rows, noPins)
  }

  function saved(s, view) {
    const name = view?.names?.length ? `Selection "${view.names.join(' / ')}"` : 'This Selection'
    return [
      h('p', { class: 'muted small' }, name, s.members === null ? '' : ` · ${counted(s.members, 'item')}`),
      h('div', { class: 'row seg-row' }, snippetToggle()),
      snippetBlock({ kind: 'selection', cid: s.selection }),
    ]
  }

  async function picked(route) {
    const entries = ctx.tray.entries()
    // #/compose draws the full list and the save controls in the main pane; the
    // panel says so rather than repeating them (one #compose-save on the page).
    if (route === 'compose') {
      return [
        h('p', {}, `Picked: ${counted(entries.length, 'item')}`),
        h('p', { class: 'muted' }, 'The list and Save as Selection are in the main pane.'),
      ]
    }
    const groups = groupEntries(entries)
    // Named from what the views recorded (ctx.sources), plus the run's block; never the snapshot.
    const names = new Map()
    for (const key of groups.keys()) {
      if (!key.startsWith('via:') || key === 'via:-') continue
      const label = await sourceLabel(ex, ctx, key.slice(4))
      if (label) names.set(key, sourceText(label))
    }
    const removeAll = (list) => { for (const e of list) ctx.tray.remove(e.address); ctx.trayChanged() }
    const rows = []
    previews ??= new Previews(ex)
    const list = h('ul', { class: 'picked' }, [...groups].map(([key, members]) => {
      const label = h('span', {}, key.startsWith('selection:') ? 'Selection' : key === 'via:-' ? 'picked by a query across runs' : names.get(key) ?? 'an output')
      const entryRows = members.map(e => entryRow(e, { remove: () => removeAll([e]) }))
      rows.push(...entryRows)
      return h('li', {}, label, ` · ${members.length} `,
        h('button', { type: 'button', 'aria-label': 'remove these', onclick: () => removeAll(members) }, '✕'),
        fold(`picked:${key}`, 'show items', { open: entries.length <= 6 }, h('ul', { class: 'picked-entries' }, entryRows.map(r => r.row))))
    }))
    stopNaming?.()
    stopNaming = nameEntries(ex, rows, previews, { selections: false })
    const blocked = !ctx.write.available || entries.length === 0 || ctx.tray.onlyOneSelection()
    const save = h('button', { type: 'button', id: 'compose-save', class: 'primary', disabled: blocked,
      onclick: (event) => guarded(event.currentTarget, () => saveFlow(ctx, status, name, guarded)) }, 'Save as Selection')
    const clear = h('button', { type: 'button', onclick: () => { ctx.tray.clear(); ctx.trayChanged() } }, 'Clear')
    return [
      h('p', {}, `Picked: ${counted(entries.length, 'item')}`),
      list,
      trayNote(ctx.tray) ? h('p', { class: 'warn' }, trayNote(ctx.tray)) : null,
      ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only entry is another Selection is legal, but the explorer does not make one: add an item too.') : null,
      h('p', { class: 'muted' }, 'A filter cannot describe these, so save them as a Selection: a fixed list of these exact items.'),
      ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
      h('p', {}, name), h('p', {}, save, ' ', clear),
      status,
    ]
  }

  return { draw }
}
