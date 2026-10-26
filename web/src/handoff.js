// web/src/handoff.js
// The run page's hand-off rail (P1a, mockups/07-run-samples.html): for the
// samples the Samples tab has ticked, the outputs to take with their counts
// and unique bytes, "Pick N items", Save as Selection, the call that would
// read them, and a hand-off template's link (P4). The `samples:` call is a
// proposal (docs/design/adr/0001-fromstore-samples.md), so it never goes
// where the Gate runs snippets. Block reads only: the run's identity, the items' file
// maps and the first bytes of a merged matrix.
import { h, link, copyOutcome } from './html.js'
import { formatNumber, sizeText } from './format.js'
import { PREVIEW_BYTES, matrixSamples } from './filepreview.js'
import { counted, saveFlow } from './picked.js'
import { pipelineConfig } from './promoted.js'
import { handoffSet, samplesWhereText } from './samples.js'
import { sumUnique } from './sizes.js'
import { samplesSnippet, snippetBlock, snippetToggle, whereRepeatsPath } from './snippets.js'
import { Tray } from './tray.js'
import { readHead } from './views/block.js'
import { CONTRACT as DE, TEMPLATE as DE_TEMPLATE, handoffHref } from './templates/differentialabundance.js'

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const ONE_ITEM = 'One item for the whole run.'

/** "Holds 5 samples as columns; your 2 are among them.": `found` columns, `mine` of the `selected` samples among them. */
export function mergedText(found, mine, selected) {
  const holds = `Holds ${found} ${found === 1 ? 'sample as a column' : 'samples as columns'}`
  const verb = (n) => (n === 1 ? 'is' : 'are')
  if (!selected) return `${holds}.`
  if (mine === selected) return `${holds}; your ${selected} ${verb(selected)} among them.`
  if (mine === 0) return `${holds}; none of your ${selected} is among them.`
  return `${holds}; ${mine} of your ${selected} ${verb(mine)} among them.`
}

/** The table's outputs in the rail's order: the ones the config describes in its order, then the rest by name. */
export function railOutputs(table, config) {
  const rank = Object.keys(config.outputs)
  const at = (o) => (rank.includes(o) ? rank.indexOf(o) : rank.length)
  const order = (list) => [...list].sort((a, b) => (at(a) - at(b)) || byText(a, b))
  return { perSample: order(table.perSample), runLevel: order(table.runLevel) }
}

/**
 * Where a run-level output's items hold `file` (the merged matrix): `{ cid,
 * item, collection }` for the first item that names it, from its slots, else
 * (inside a directory) from its file map; null when none does. `entries` are
 * the output's sample-table entries (samples.js `runLevelItems`).
 */
export async function matrixFile(ex, entries, file) {
  for (const e of entries) {
    const cid = (e.slots ?? []).find(s => s.name === file && s.address && !s.dir)?.address ?? null
    if (cid) return { cid, item: e.item, collection: e.collection }
  }
  for (const e of entries) {
    for (const [address, f] of (await ex.itemFileMap(e.item)).files) if (f.name === file) return { cid: address, item: e.item, collection: e.collection }
  }
  return null
}

/** Which of `samples` the matrix at `cid` holds as columns, from its first bytes (matrixSamples); null when the server sent none. */
export async function matrixColumns(ex, cid, samples) {
  const head = await readHead(ex, cid, PREVIEW_BYTES)
  if (!head.bytes) return null
  return matrixSamples(new TextDecoder().decode(head.bytes), samples)
}

/** A sample id list for `?highlight=`: JSON, with only what would break the query escaped. */
const highlightParam = (ids) => JSON.stringify(ids).replace(/[%&#+ ]/g, c => encodeURIComponent(c))

/**
 * The rail for the panel's `samples` state. `status` is the panel's write
 * status, `guarded(button, fn)` runs a write as the panel does, `redraw()`
 * draws the panel again. It remembers the ticked outputs per run, the typed
 * name, and the last Selection saved for exactly these items.
 */
export function createHandoff({ ex, ctx, status, guarded, redraw }) {
  const ticksByRun = new Map()
  const matrices = new Map()
  const name = h('input', { type: 'text', class: 'handoff-name', 'data-handoff-name': '', 'aria-label': 'Selection name',
    placeholder: 'A name, e.g. rap1-wt-50k' })
  name.value = ''
  let saved = null // { completion, key, address }: shown while the same items are chosen
  let statusKey = null // the items the status was written for
  let drawnKey = null // the items of the last draw
  let saving = false
  let boxes = new Map()

  const ticksOf = (completion, config, { perSample, runLevel }) => {
    if (!ticksByRun.has(completion)) {
      const all = [...perSample, ...runLevel]
      // An unknown pipeline names no hand-off, so its per-sample outputs start ticked.
      ticksByRun.set(completion, new Set(config.handoff.length ? config.handoff.filter(o => all.includes(o)) : perSample))
    }
    return ticksByRun.get(completion)
  }

  const toggle = async (completion, output, on) => {
    const ticks = ticksByRun.get(completion)
    if (on) ticks.add(output)
    else ticks.delete(output)
    await redraw()
    boxes.get(output)?.focus?.()
  }

  /**
   * `node` gets `fmt(size text)` once the items' file maps are read, or
   * `fmt(failed)` when a read fails. A walk stopped because the page went
   * (`aborted`) writes nothing. Resolves either way: true when it wrote.
   */
  const fillSize = (node, items, fmt, failed = '—') => Promise.resolve()
    .then(() => Promise.all(items.map(i => ex.itemFileMap(i.address))))
    .then(maps => { node.textContent = fmt(sizeText(sumUnique(maps))); return true },
      (e) => { if (e?.code === 'aborted') return false; node.textContent = fmt(failed); return true })

  /** The merged matrix's address and the run's samples its header holds, or null; one read per output's items. */
  const matrixOf = (entries, file, samples) => {
    const key = JSON.stringify([entries.map(e => e.item), file])
    if (!matrices.has(key)) {
      const read = (async () => {
        const at = await matrixFile(ex, entries, file)
        if (!at) return null
        const found = await matrixColumns(ex, at.cid, samples)
        return found === null ? null : { cid: at.cid, found }
      })()
      matrices.set(key, read.catch(() => { matrices.delete(key); return null }))
    }
    return matrices.get(key)
  }

  /** A run-level output's note: the matrix's sample columns when the config names one and its header shows them. */
  const mergedNote = (output, config, table, selected) => {
    const entries = table.runLevelItems.get(output) ?? []
    const plain = entries.length === 1 ? ONE_ITEM : `${counted(entries.length, 'item')} for the whole run.`
    const file = config.outputs[output]?.merged?.matrix
    const node = h('span', { class: 'note2', 'data-merged-note': output }, file ? 'Reading the matrix…' : plain)
    if (!file) return node
    matrixOf(entries, file, table.samples).then((m) => {
      if (!m?.found.length) { node.textContent = plain; return }
      const mine = selected.filter(s => m.found.includes(s))
      node.replaceChildren(`${mergedText(m.found.length, mine.length, selected.length)} `,
        link(`#/content/${m.cid}${mine.length ? `?highlight=${highlightParam(mine)}` : ''}`, 'Preview'))
    })
    return node
  }

  /** The rail's sections for panel state `s`. */
  async function body(s) {
    const { table, selected, where, manual } = s.samples
    const id = await Promise.resolve(ex.runIdentity(s.completion)).catch(() => null)
    const config = pipelineConfig(id?.pipeline)
    const outputs = railOutputs(table, config)
    const ticks = ticksOf(s.completion, config, outputs)
    const chosen = [...outputs.perSample, ...outputs.runLevel].filter(o => ticks.has(o))
    const items = handoffSet(table, selected, chosen).items
    const key = JSON.stringify(items.map(i => i.address).sort())
    drawnKey = key
    if (!saving && statusKey !== null && statusKey !== key) { status.replaceChildren(); statusKey = null }

    boxes = new Map()
    const option = (output, runLevel) => {
      const set = handoffSet(table, selected, [output]).items
      const box = h('input', { type: 'checkbox', 'data-out': output, onchange: (e) => toggle(s.completion, output, !!e.currentTarget.checked) })
      box.checked = ticks.has(output)
      if (box.checked) box.setAttribute('checked', '')
      boxes.set(output, box)
      const size = h('span', { class: 'sz', 'data-sz': output }, `${set.length} · …`)
      fillSize(size, set, text => `${set.length} · ${text}`)
      const label = config.outputs[output]?.label
      return h('label', { class: 'opt', title: label ?? output }, box,
        h('span', { class: 'what' }, h('b', {}, output), label ? [' ', h('span', { class: 'd' }, label)] : null), size,
        runLevel ? mergedNote(output, config, table, selected) : null)
    }
    // Nothing to hand off without a sample, even with a run-level output ticked.
    const empty = items.length === 0 || selected.length === 0
    const totalText = h('b', { class: 'num', 'data-total': '' }, `${counted(items.length, 'item')} · …`)
    const pick = h('button', { type: 'button', class: 'btn sm', 'data-handoff-pick': '', disabled: true, onclick: () => {
      ctx.tray.addMany(items.map(i => ({ address: i.address, via: i.via, kind: 'item' })))
      ctx.trayChanged()
    } }, `Pick ${counted(items.length, 'item')}`)

    const save = h('button', { type: 'button', class: 'btn primary', 'data-handoff-save': '', disabled: true,
      onclick: async (e) => {
        // The writer takes members as the picked list makes them.
        const members = new Tray(null)
        members.addMany(items.map(i => ({ address: i.address, via: i.via, kind: 'item' })))
        statusKey = key
        saving = true
        try {
          await guarded(e.currentTarget, () => saveFlow(ctx, status, name, guarded, { members: members.toMembers(),
            onSaved: (address) => { saved = { completion: s.completion, key, address } } }))
        } finally {
          saving = false
          // guarded's own last draw ran while this save still counted as running, so it kept the
          // status; if the tab reported other items meanwhile, draw again to drop it, and with it
          // any "Save a copy here" that would write the old members.
          if (drawnKey !== statusKey) await redraw()
        }
      } }, 'Save')
    // Pick and Save wait until the total shows its bytes, or says it could not read them; a page that went leaves them off.
    fillSize(totalText, items, text => `${counted(items.length, 'item')} · ${text}`, 'size unknown').then((wrote) => {
      if (!wrote) return
      pick.disabled = empty
      save.disabled = empty || !ctx.write.available
    })

    const done = saved?.completion === s.completion && saved.key === key ? saved.address : null
    const proposed = done ? null : samplesSnippet({ completion: s.completion, where,
      ids: manual?.size || whereRepeatsPath(where) ? selected : null,
      outputs: chosen.filter(o => outputs.perSample.includes(o)), runLevel: chosen.filter(o => outputs.runLevel.includes(o)) })
    const copy = h('button', { type: 'button', class: 'btn sm', onclick: async (e) => {
      const button = e.currentTarget
      button.textContent = await copyOutcome(globalThis.navigator?.clipboard, proposed)
    } }, 'Copy snippet')

    // P4: the differentialabundance template, for an nf-core/rnaseq run with a merged matrix. It takes this
    // filter, the ticks when some were made by hand, and the outputs ticked here.
    const template = id?.pipeline === DE.from && table.runLevelItems.has(DE.matrix.output)
      ? h('section', { class: 'sec', 'data-handoff-template': DE_TEMPLATE }, h('h3', {}, 'Hand off to a pipeline'),
        h('p', {}, link(handoffHref({ from: s.completion, where: where.length ? samplesWhereText(where) : null,
          samples: manual?.size ? selected : [], outputs: chosen }), `Hand off to ${DE_TEMPLATE}`)),
        h('p', { class: 'muted small' }, 'Asks for a condition per sample and a contrast, then writes the samplesheet and contrasts as blocks.'))
      : null

    return [
      h('section', { class: 'sec' }, h('h3', {}, `Hand off ${counted(selected.length, 'sample')}`),
        h('span', { class: 'eyebrow' }, 'Per sample'),
        outputs.perSample.map(o => option(o, false)),
        outputs.runLevel.length ? [h('span', { class: 'eyebrow' }, 'Run-level'), outputs.runLevel.map(o => option(o, true))] : null,
        h('div', { class: 'row handoff-total' }, h('span', { class: 'muted' }, 'Total'), totalText),
        h('div', { class: 'row' }, pick),
        ctx.tray.size ? h('p', { class: 'muted small', 'data-handoff-picked': '' },
          `${formatNumber(ctx.tray.size)} picked · `, link('#/compose', 'Open the picked list')) : null),
      h('section', { class: 'sec' }, h('h3', {}, 'Save as Selection'),
        h('div', { class: 'row handoff-save' }, name, save),
        h('p', { class: 'muted small' }, "A Selection is a fixed list of these exact items. You'll get an address you can cite."),
        ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
        status),
      done
        ? h('section', { class: 'sec' }, h('h3', {}, 'Use in a workflow'),
          h('p', { class: 'muted small' }, `The Selection you saved · ${counted(items.length, 'item')}`),
          h('div', { class: 'row seg-row' }, snippetToggle()), snippetBlock({ kind: 'selection', cid: done }))
        : h('section', { class: 'sec' }, h('h3', {}, 'Use in a workflow'),
          h('pre', { class: 'code', 'data-proposed-snippet': '' }, proposed),
          h('div', { class: 'row' }, copy)),
      template,
    ]
  }

  return { body }
}
