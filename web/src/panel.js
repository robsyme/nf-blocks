// web/src/panel.js
// The "Use in a workflow" panel (explorer layout B spec §6). panelState picks
// one state from the route, what the view reported (ctx.use) and the picked
// list; createPanel draws it. A call on screen always returns what it says.
// The panel reads runs from their blocks only (Explorer.runIdentity), and
// shows failures without [data-error] (plan P2, P14).
import { h, link } from './html.js'
import { snippetBlock, snippetToggle } from './snippets.js'
import { saveChoice } from './save-choice.js'
import { saveSequence } from './save-flow.js'
import { trayNote } from './tray.js'
import { valueText } from './pairs.js'

const OUTPUT_ROUTES = new Set(['items', 'item', 'collection'])
const counted = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`
const short = (text) => (text.length > 20 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text)
const whereText = (where) => where.map(([p, t, v]) => `${p} = ${valueText(t, v)}`).join(', ')

/** Spec §6.1. `view` is what the current view passed to ctx.use; `picked` the tray's size. */
export function panelState({ route, view = null, picked = 0 }) {
  if (route === 'selection' && view?.selection) return { state: 'saved', selection: view.selection, members: view.members ?? null }
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

const deletedNote = (deletion, where) => deletion === 'deleted' ? ` It is deleted ${where}.`
  : deletion === 'conflicted' ? ` Its deletion is in conflict ${where}.` : ''

export function createPanel({ el, ex, ctx }) {
  let drawn = 0
  let last = { route: 'open', view: null }
  let mode = 'this'
  let modeKey = null
  let note = null

  async function draw(input = last) {
    last = input
    const mine = ++drawn
    const s = panelState({ ...input, picked: ctx.tray.size })
    const key = s.completion ? `${s.completion}/${s.output}` : null
    if (key !== modeKey) { mode = 'this'; note = null; modeKey = key }
    let body
    try {
      body = await bodyOf(s, input.view)
    } catch (e) {
      body = h('p', { class: 'warn' }, `Could not prepare the call: ${e?.message ?? e}`)
    }
    if (mine !== drawn) return
    el.replaceChildren(h('section', { 'data-panel-state': s.state }, h('h2', {}, 'Use in a workflow'), body))
  }

  async function bodyOf(s, view) {
    if (s.state === 'saved') return saved(s, view)
    if (s.state === 'picked') return picked()
    if (s.state === 'none') return h('p', { class: 'muted' }, 'Open an output to use it in a workflow.')
    return output(s, view)
  }

  async function output(s, view) {
    const id = await ex.runIdentity(s.completion)
    const what = s.state === 'filtered' ? `${counted(s.count ?? 0, 'item')} of ${s.output}, where ${whereText(s.where)}`
      : s.count === null ? `Every item of ${s.output}` : `All ${counted(s.count, 'item')} of ${s.output}`
    const spec = mode === 'latest' ? { kind: 'latest', pipeline: id.pipeline, output: s.output, where: s.where }
      : id.lid ? { kind: 'run', lid: id.lid, output: s.output, where: s.where } : null
    return [
      h('p', {}, what),
      id.pipeline ? runSwitch(id) : null,
      spec ? [snippetBlock(spec), h('p', { class: 'muted' }, 'Script kind: ', snippetToggle())]
        : h('p', { class: 'muted' }, 'This run has no lineage ID; read it as the latest good run instead.'),
      mode === 'latest' ? h('p', { class: 'muted' }, '"latest" may match different items after the next run.') : null,
      note ? h('p', { class: 'muted' }, note) : null,
      s.partial ? h('p', {}, `${s.partial.checked} of ${s.partial.count} checked. `,
        h('button', { type: 'button', id: 'pick-checked', disabled: s.partial.checked === 0, onclick: () => view?.pickChecked?.() },
          `Pick these ${s.partial.checked}`)) : null,
    ]
  }

  function runSwitch(id) {
    const choose = async (next) => {
      try {
        if (next === 'latest') {
          const best = await ex.latestSuccessfulRun(id.pipeline)
          if (!best) { note = `${id.pipeline} has no good run, so "latest" would find nothing.`; return draw() }
          note = best === id.completion ? null : `The latest good run is now ${(await ex.runIdentity(best)).run_name ?? best}, not this one.`
        } else {
          note = null
        }
        mode = next
      } catch (e) {
        note = `Could not find the latest good run: ${e?.message ?? e}`
      }
      return draw()
    }
    return h('p', { class: 'run-switch', role: 'group', 'aria-label': 'which run', 'data-run-mode': mode },
      ['this', 'latest'].map(m => h('button', { type: 'button', 'aria-pressed': String(mode === m), onclick: () => choose(m) },
        m === 'this' ? 'this run' : 'latest good run')))
  }

  function saved(s, view) {
    const name = view?.names?.length ? `Selection "${view.names.join(' / ')}"` : 'This Selection'
    return [
      h('p', {}, name, s.members === null ? '' : ` · ${counted(s.members, 'member')}`),
      snippetBlock({ kind: 'selection', cid: s.selection }),
      h('p', { class: 'muted' }, 'Script kind: ', snippetToggle()),
      ctx.write.served ? h('p', {}, 'Samplesheet: ',
        h('a', { href: `api/samplesheet/${s.selection}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
        h('a', { href: `api/samplesheet/${s.selection}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null,
    ]
  }

  function picked() {
    const entries = ctx.tray.entries()
    const groups = new Map()
    for (const e of entries) {
      const key = e.kind === 'selection' ? `selection:${e.address}` : `via:${e.via[0] ?? '-'}`
      groups.set(key, [...(groups.get(key) ?? []), e])
    }
    const removeAll = (list) => { for (const e of list) ctx.tray.remove(e.address); ctx.trayChanged() }
    const list = h('ul', { class: 'picked' }, [...groups].map(([key, members]) => {
      const label = h('span', {}, key.startsWith('selection:') ? 'Selection' : key === 'via:-' ? 'picked by a query across runs' : 'an output')
      if (key.startsWith('via:') && key !== 'via:-') {
        ex.runLabel(key.slice(4)).then((l) => { if (l) label.textContent = `${l.output} · ${l.run_name ?? 'unnamed run'}` }, () => {})
      }
      return h('li', {}, label, ` · ${members.length} `,
        h('button', { type: 'button', 'aria-label': 'remove these', onclick: () => removeAll(members) }, '✕'),
        h('ul', { class: 'picked-entries' }, members.map(e => h('li', { 'data-tray-entry': e.address, 'data-kind': e.kind },
          h('code', { class: 'cid', title: e.address }, short(e.address))))))
    }))
    const status = h('div', { id: 'write-status' })
    const name = h('input', { id: 'compose-name', placeholder: 'A name, e.g. treated BAMs' })
    const blocked = !ctx.write.available || entries.length === 0 || ctx.tray.onlyOneSelection()
    const save = h('button', { type: 'button', id: 'compose-save', class: 'primary', disabled: blocked,
      onclick: (event) => ctx.write.run(status, () => saveFlow(status, name), event.currentTarget) }, 'Save as Selection')
    const clear = h('button', { type: 'button', onclick: () => { ctx.tray.clear(); ctx.trayChanged() } }, 'Clear')
    return [
      h('p', {}, `Picked: ${counted(entries.length, 'item')}`),
      list,
      trayNote(ctx.tray) ? h('p', { class: 'warn' }, trayNote(ctx.tray)) : null,
      ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only member is another Selection is legal, but the explorer does not make one: add an item too.') : null,
      h('p', { class: 'muted' }, 'A filter cannot describe these, so save them as a Selection: a fixed list of these exact items.'),
      ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
      h('p', {}, name), h('p', {}, save, ' ', clear),
      status,
    ]
  }

  // compose()'s save, moved (DESIGN.md §16 decisions 9, 23): dry run, then
  // "exists here", "held elsewhere", or save and name.
  async function saveFlow(status, name) {
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
  }

  return { draw }
}
