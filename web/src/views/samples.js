// web/src/views/samples.js
// The run page's Samples tab (P1a, mockups/07-run-samples.html): every
// output's items joined on the pipeline's join key (samples.js), one row per
// sample. Filters hold across outputs; a tick overrides the filter for its
// row and shows a "manual" mark; the rail gets the selection through ctx.use.
// Each row's small folder link opens Add to project for that sample (P4).
// Block reads only: allItems, the item blocks through Previews and the size
// walks. Typing in a filter rewrites the URL with history.replaceState, so a
// reload keeps it without the page drawing again.
import { h, link } from '../html.js'
import { Previews } from '../previews.js'
import { SAMPLES_ITEM_CAP } from '../config.js'
import { NOT_EQUAL, RANGE_OPS } from '../metadata.js'
import { pairsOf, valueText } from '../pairs.js'
import { pipelineConfig, shortLabel } from '../promoted.js'
import { buildSamples, sampleKeyOf, sampleMatches, samplesWhereText, splitKey, valueOf } from '../samples.js'
import { rankFields, typedCondition } from '../table.js'
import { sumUnique } from '../sizes.js'
import { formatNumber, sizeText } from '../format.js'
import { note, panel } from '../components.js'
import { enc } from './common.js'
import { projectControl, projectsCache } from './addproject.js'

const plural = (n, one, many = `${one}s`) => `${formatNumber(n)} ${n === 1 ? one : many}`
// How a chip writes each operator; the where JSON keeps the typed form. Shared with items.js and the hand-off page.
export const OP_TEXT = { '=': '=', [NOT_EQUAL]: '≠', '>': '>', '>=': '≥', '<': '<', '<=': '≤' }
const SHARED = 'Shared'
// The row action's folder, the left nav's Projects shape at the funnel's size (items.js's filter link).
const FOLDER = '<svg viewBox="0 0 24 24" width="12" height="12" aria-hidden="true"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/></svg>'

/** replaceChildren that skips null, undefined and false, as h() does. */
const put = (el, ...parts) => el.replaceChildren(...parts.flat(Infinity).filter(p => p !== null && p !== undefined && p !== false))

/** A source as a header's small text: an output whole, a path whole up to two segments, else `…last`. */
const sourceText = (source) => (source.split('.').length > 2 ? `…${shortLabel(source)}` : source)

/**
 * Whether the Samples tab is worth showing on another tab of the run page:
 * an output of two or more items whose first item has a sample key. One page
 * of one item per collection, then that item's block; a read that fails
 * counts as no, and so does a run over SAMPLES_ITEM_CAP or a page that is
 * gone (`alive()` false). Cheaper than joinRun and may say yes where the
 * join finds no samples; the tab's empty state covers that.
 */
export async function firstItemKeyed(ex, collections, { joinKey = pipelineConfig(null).joinKey, alive = () => true } = {}) {
  const pages = await Promise.all(collections.map(c => ex.collection(c.cid, { limit: 1 }).catch(() => null)))
  if (pages.reduce((n, coll) => n + (coll?.total ?? 0), 0) > SAMPLES_ITEM_CAP || !alive()) return false
  const keyed = await Promise.all(collections.map(async (c, i) => {
    const coll = pages[i]
    if (!coll || coll.total < 2 || !coll.items.length) return false
    try {
      return sampleKeyOf(pairsOf((await ex.itemBlock(c.cid, coll.items[0])).view), joinKey) !== null
    } catch {
      return false
    }
  }))
  return keyed.some(Boolean)
}

/**
 * The run's items read and joined as the Samples tab joins them (load, then
 * buildSamples by `config`): `{ table, failed }`, `{ over: n }` past
 * SAMPLES_ITEM_CAP, or null when `alive()` turned false first.
 * `onProgress(done, total)` hears each item block read, as it is read. The run page
 * asks this for its default tab, opens on Samples when `table.samples` is not
 * empty, and hands the result to samplesTab so no item is read twice.
 */
export async function joinRun(ex, collections, config, { alive = () => true, onProgress = () => {} } = {}) {
  const loaded = await load(ex, collections, { alive, onProgress })
  if (!loaded) return null
  if (loaded.over !== undefined) return { over: loaded.over }
  return { table: buildSamples(loaded.byOutput, config), failed: loaded.failed }
}

/**
 * The Samples tab. It reads every item of the run (up to SAMPLES_ITEM_CAP),
 * joins them (buildSamples) and draws the filter bar, the table and the
 * column picker line. `where` is the parsed URL filter (parseSamplesWhere),
 * `showExcluded` whether the rows the filter drops show dimmed, `counted(n)`
 * puts the sample count on the tab. `alive()` false means the page that
 * asked is gone (the run page passes its Explorer page's state): the tab
 * then stops reading, as it does once its node has been shown and removed.
 * `joined`, when given, is joinRun's answer for these collections (the run
 * page's default-tab read), drawn without reading the items again.
 *
 * Once the table is drawn, and again whenever the selection, the filters or
 * the ticks change, it calls `ctx.use(view)` for the rail with:
 *
 *     {
 *       completion,        // the RunCompletion CID
 *       output: null,
 *       samples: {
 *         table,           // buildSamples' result: samples, perSample, runLevel, itemsOf, runLevelItems, columns, picker
 *         selected,        // the ticked sample ids, in table.samples order
 *         where,           // the conditions in force: the URL's, less those with no value or one that does not read
 *         manual,          // Map<sample, boolean>: ticks that override the filter (true kept, false dropped)
 *       },
 *       sizeOf(sample, output),  // Promise<{ bytes, blocks, partial }>: unique over the sample's items in
 *                                // `output` (every output when null); a run-level output ignores `sample`
 *     }
 */
export function samplesTab(ex, completionCid, ctx, { pipeline, collections, where = [], showExcluded = false, counted = () => {}, alive: pageAlive = () => true, joined = null }) {
  const node = h('div', { class: 'samples-tab' })
  const status = h('p', { class: 'muted', 'data-samples-loading': '' }, "Reading the run's items…")
  node.append(status)
  let shown = false
  const alive = () => {
    if (node.isConnected) shown = true
    return pageAlive() && (!shown || node.isConnected)
  }
  const config = pipelineConfig(pipeline)
  ;(joined ? Promise.resolve(joined) : joinRun(ex, collections, config, { alive,
    onProgress: (done, total) => { status.textContent = `Reading ${formatNumber(done)} of ${plural(total, 'item')}…` } })).then((loaded) => {
    if (!loaded || !alive()) return
    if (loaded.over !== undefined) {
      put(node, note(`This run has ${formatNumber(loaded.over)} items. The Samples tab joins up to ${formatNumber(SAMPLES_ITEM_CAP)}; use the Outputs tab.`))
      return
    }
    const { table } = loaded
    const unread = loaded.failed ? h('p', { class: 'muted samples-unread', 'data-samples-unread': '' },
      `${plural(loaded.failed, 'item')} could not be read and ${loaded.failed === 1 ? 'is' : 'are'} left out.`) : null
    if (!table.samples.length) {
      put(node, h('div', { class: 'empty', 'data-samples-empty': '' }, h('b', {}, 'No samples to join'),
        h('span', {}, 'No output holds two or more items, each with a sample id, or none of those ids appears in a second output. ',
          link(`#/run/${completionCid}?tab=outputs`, 'See its outputs'))), unread)
      return
    }
    counted(table.samples.length)
    // Which run and output each collection is, for naming what the rail hands off to the picked list.
    for (const c of collections) ctx.sources?.set(c.cid, { completion: completionCid, output: c.output })
    put(node, samplesView(ex, completionCid, ctx, { table, config, pipeline, where, showExcluded, alive }), unread)
  }, (e) => {
    if (alive()) put(node, note(`Could not read the run's items: ${e?.message ?? e}`, 'bad'))
  })
  return node
}

/**
 * One run's sample table for a page other than the run's own (a project's
 * Samples tab): the run's items read and joined exactly as this tab does,
 * by its pipeline's config. `{ row, table, config, failed }`, `{ row, over }`
 * past SAMPLES_ITEM_CAP, or null when `alive()` turned false first.
 */
export async function runSampleTable(ex, completionCid, { alive = () => true } = {}) {
  const { row, collections } = await ex.run(completionCid)
  const config = pipelineConfig(row.pipeline)
  const joined = await joinRun(ex, collections, config, { alive })
  return joined && { row, config, ...joined }
}

/**
 * Every item of the run's collections as `{ byOutput, failed }` (byOutput as
 * buildSamples takes it, without the items whose block could not be read),
 * `{ over: n }` past the cap, or null when the page went away first.
 * `onProgress(done, total)` hears how many item blocks have been read.
 */
async function load(ex, collections, { alive, onProgress }) {
  const totals = await Promise.all(collections.map(c => ex.collection(c.cid, { limit: 0 }).then(coll => coll.total, () => 0)))
  const total = totals.reduce((a, b) => a + b, 0)
  if (total > SAMPLES_ITEM_CAP) return { over: total }
  if (!alive()) return null
  const lists = await Promise.all(collections.map((c, i) => (totals[i] ? ex.allItems(c.cid) : [])))
  if (!alive()) return null
  const wanted = collections.flatMap((c, i) => lists[i].map(item => ({ output: c.output, collection: c.cid, item })))
  const previews = new Previews(ex, { cap: SAMPLES_ITEM_CAP, rows: true })
  const finished = await new Promise((resolve) => {
    const check = () => {
      if (!alive()) { previews.cancel(); resolve(false); return }
      const done = wanted.filter(w => previews.get(w.item) !== undefined).length
      onProgress(done, wanted.length)
      if (done === wanted.length) resolve(true)
    }
    previews.onChange(check)
    for (const w of wanted) previews.ask(w.collection, w.item)
    check()
  })
  if (!finished) return null
  const byOutput = new Map(collections.map(c => [c.output, []]))
  let failed = 0
  for (const w of wanted) {
    const p = previews.get(w.item)
    if (p.error) { failed++; continue }
    byOutput.get(w.output).push({ item: w.item, collection: w.collection, pairs: p.pairs, rows: p.rows, slots: p.slots })
  }
  return { byOutput, failed }
}

/** The filter bar, the table and the column picker line over a table with samples. */
function samplesView(ex, completionCid, ctx, { table, config, pipeline, where: initial, showExcluded, alive }) {
  const { samples } = table
  const where = initial.map(w => [...w])
  const manual = new Map()
  const added = new Set()
  const sizes = new Map()
  let showX = showExcluded
  let passing = new Set()
  let selected = []
  let inForce = []
  // The first draw keeps the URL as it came; every change after it rewrites it.
  let drawn = false

  const outputs = [...table.perSample, ...table.runLevel]
  const offered = [...table.columns.filter(c => c.field === null), ...[...table.picker.values()].flat()]
  const all = [...table.columns, ...[...table.picker.values()].flat()]
  const columnOf = (key) => all.find(c => c.key === key) ?? null
  const shownColumns = () => [...table.columns.filter(c => c.field !== null), ...offered.filter(c => added.has(c.key))]
  // The outputs a column reads: its own, or for a shared path every per-sample output that carries it.
  const outputsOf = (c) => (c.output ? [c.output] : table.perSample.filter(o => samples.some(s =>
    (table.itemsOf.get(s).get(o) ?? []).some(e => (e.pairs ?? []).some(p => p.path === c.path)))))
  const typeOf = (c) => {
    const types = new Set(samples.flatMap(s => valueOf(table, s, c).map(v => v.type)))
    return types.size === 1 ? [...types][0] : 'mixed'
  }
  /** A where key's label and source for its chip: the column's, else the path's own (the join key reads "Sample"). */
  const describe = (key) => {
    const c = columnOf(key)
    if (c) return { label: c.label, source: c.output ?? c.source }
    const { output, path } = splitKey(key, outputs)
    return { label: config.joinKey.includes(path) ? 'Sample' : shortLabel(path), source: output ?? path }
  }

  const kept = (s) => (manual.has(s) ? manual.get(s) : passing.has(s))
  const report = () => ctx.use?.({ completion: completionCid, output: null,
    samples: { table, selected: [...selected], where: inForce.map(w => [...w]), manual: new Map(manual) }, sizeOf })

  function replaceUrl() {
    const q = ['tab=samples', where.length ? `where=${enc(samplesWhereText(where))}` : null, showX ? 'show=excluded' : null].filter(Boolean)
    try { globalThis.history?.replaceState?.(globalThis.history.state ?? null, '', `#/run/${completionCid}?${q.join('&')}`) } catch { /* the URL stays as it was */ }
  }

  // ---- sizes ----
  const entriesOf = (sample, output) => {
    if (output && table.runLevelItems.has(output)) return table.runLevelItems.get(output)
    const mine = table.itemsOf.get(sample)
    if (!mine) return []
    return output ? (mine.get(output) ?? []) : [...mine.values()].flat()
  }
  const sized = new Map()
  function sizeOf(sample, output) {
    const key = JSON.stringify([table.runLevelItems.has(output) ? null : sample, output])
    if (!sized.has(key)) {
      const p = ex.itemFileMap
        ? Promise.all(entriesOf(sample, output).map(e => ex.itemFileMap(e.item))).then(maps => sumUnique(maps))
        : Promise.reject(new Error('sizes are not available here'))
      sized.set(key, p.catch((e) => { sized.delete(key); throw e }))
    }
    return sized.get(key)
  }

  // ---- the filter bar ----
  const chips = h('span', { class: 'samples-chips' })
  const match = h('span', { class: 'muted samples-match', 'data-match': '' })
  const showButton = h('button', { type: 'button', class: 'btn sm', 'data-show-excluded': '', onclick: () => { showX = !showX; update() } })
  const useButton = h('button', { type: 'button', class: 'btn primary sm', 'data-use-samples': '', onclick: () => {
    if (globalThis.document?.body) globalThis.document.body.dataset.panel = 'open'
    // A heading takes focus only with a tabindex; -1 keeps it out of the tab order.
    const heading = globalThis.document?.querySelector?.('#panel h3')
    if (heading && !heading.hasAttribute?.('tabindex')) heading.setAttribute?.('tabindex', '-1')
    heading?.focus?.()
  } })
  const chipOf = (cond, i) => {
    const [key, , value, op = '='] = cond
    const { label, source } = describe(key)
    const range = RANGE_OPS.includes(op)
    const input = h('input', { type: range ? 'number' : 'text', step: range ? 'any' : null, placeholder: 'any',
      'aria-label': `${label} ${OP_TEXT[op] ?? op}`, oninput: (e) => {
        const text = String(e.currentTarget.value ?? '')
        where[i] = range ? [key, /^\s*-?\d+\s*$/.test(text) ? 'int' : 'float', text.trim(), op] : (op === '=' ? [key, cond[1], text] : [key, cond[1], text, op])
        manual.clear()
        update()
      } })
    input.value = value
    return h('span', { class: 'fchip', 'data-chip': key, title: key },
      h('span', {}, label), source !== label ? h('span', { class: 'src' }, sourceText(source)) : null,
      h('span', { class: 'op' }, OP_TEXT[op] ?? op), input,
      h('button', { type: 'button', class: 'x', 'aria-label': `Remove the ${label} filter`, onclick: () => {
        where.splice(i, 1)
        manual.clear()
        drawChips()
        update()
      } }, '×'))
  }
  const drawChips = () => put(chips, where.map(chipOf))
  const addCondition = (cond) => {
    where.push(cond)
    manual.clear()
    drawChips()
    update()
  }
  const filterPicker = picker({ attr: 'data-filter-picker', label: 'add a filter', outputs: table.perSample })
  const addButton = h('button', { type: 'button', class: 'btn ghost sm', 'aria-haspopup': 'dialog', 'data-add-filter': '', onclick: (e) => {
    e.stopPropagation?.()
    filterPicker.toggle(showFields)
  } }, '+ Add filter')

  /** The picker's first step: every column, grouped by output, ranked by the search. */
  function showFields() {
    filterPicker.fields(all, {
      item: (c) => [h('span', { class: 'fb-path' }, c.field ? `${c.label} · ${c.path}` : c.path),
        h('span', { class: 'fb-count' }, plural(new Set(samples.flatMap(s => valueOf(table, s, c).map(v => `${v.type}:${v.value}`))).size, 'value'))],
      attrs: (c) => ({ 'data-field': c.key }),
      pick: showValues,
      empty: 'No fields to filter on.',
    })
  }
  /** The second step: an operator, then a listed value or a typed one. */
  function showValues(c) {
    const type = typeOf(c)
    const numeric = ['int', 'float', 'mixed'].includes(type) && c.numeric
    let op = numeric ? '>=' : '='
    const error = h('p', { class: 'fb-error', 'data-filter-error': '' })
    const add = (cond) => { filterPicker.close(); addCondition(cond) }
    const input = h('input', { class: 'fb-input', placeholder: 'Or type a value and press Enter', 'aria-label': `a value for ${c.label}`, 'data-filter-value': '',
      onkeydown: (e) => {
        if (e.key === 'Escape') return filterPicker.close()
        if (e.key !== 'Enter') return
        e.preventDefault?.()
        try {
          add(typedCondition({ path: c.key, type }, String(e.currentTarget.value ?? ''), op))
        } catch (err) {
          error.textContent = err.message
        }
      } })
    const counts = new Map()
    for (const s of samples) for (const v of valueOf(table, s, c)) {
      const k = JSON.stringify([v.type, v.value])
      counts.set(k, { ...v, n: (counts.get(k)?.n ?? 0) + 1 })
    }
    const values = [...counts.values()].sort((a, b) => (b.n - a.n) || (Number(a.value) - Number(b.value)) || String(a.value).localeCompare(String(b.value)))
    const list = h('div', { class: 'fb-list' })
    const drawValues = () => put(list, values.filter(v => !RANGE_OPS.includes(op) || v.type === 'int' || v.type === 'float').map(v =>
      h('button', { type: 'button', class: 'fb-item', 'data-value': String(v.value),
        onclick: () => add(op === '=' ? [c.key, v.type, v.value] : [c.key, v.type, v.value, op]) },
      h('span', {}, `${op === '=' ? '' : `${OP_TEXT[op]} `}${valueText(v.type, v.value, c.field)}`),
      h('span', { class: 'fb-count' }, plural(op === NOT_EQUAL ? samples.length - v.n : v.n, 'sample')))))
    const opButtons = ['=', NOT_EQUAL, ...(numeric ? RANGE_OPS : [])].map(o => h('button', { type: 'button', class: 'fb-op', 'data-op': o,
      'aria-pressed': String(o === op), onclick: () => {
        op = o
        for (const b of opButtons) b.setAttribute('aria-pressed', String(b.dataset.op === op))
        drawValues()
        input.focus?.()
      } }, OP_TEXT[o]))
    drawValues()
    filterPicker.show(
      h('div', { class: 'fb-head' },
        h('button', { type: 'button', class: 'btn btn-ghost fb-back', 'aria-label': 'back to the fields', 'data-filter-back': '', onclick: showFields }, '‹'),
        h('span', { class: 'fb-path' }, c.label), h('span', { class: 'fb-type' }, sourceText(c.output ?? c.source))),
      h('div', { class: 'fb-ops', role: 'group', 'aria-label': 'compare with' }, opButtons),
      input, error, list)
    input.focus?.()
  }

  const bar = h('div', { class: 'filterbar samples-filters', 'data-filter-bar': '' }, chips,
    h('span', { class: 'fb-anchor' }, addButton, filterPicker.el),
    h('span', { class: 'spacer' }), match, showButton, useButton)

  // ---- a sample's projects (P4) ----
  // A small row action beside the sample's name opens one control above the table for that sample (its SampleRef is
  // this run's pipeline and its id). Every opening shares one read of the projects, made on the first.
  const projectsRead = projectsCache(ex)
  const projectArea = h('div', { class: 'samples-projects', 'data-sample-projects': '', hidden: true })
  let projectsFor = null
  const openProjects = (s) => {
    if (projectsFor === s && !projectArea.hasAttribute('hidden')) {
      projectArea.setAttribute('hidden', '')
      return Promise.resolve()
    }
    projectsFor = s
    const control = projectControl(ex, ctx, { sample: { pipeline, id: s }, cache: projectsRead })
    projectArea.replaceChildren(control.node)
    projectArea.removeAttribute('hidden')
    return control.open()
  }
  const canWrite = !!(ctx.write?.available && ctx.write?.here)
  const projectLink = (s) => {
    if (!pipeline) return null
    const label = canWrite ? `Add ${s} to a project` : `Projects with ${s}`
    const a = h('a', { class: 'cell-filter', href: '#', role: 'button', 'aria-label': label, title: label, 'data-sample-project': s,
      onclick: (e) => { e.preventDefault?.(); return openProjects(s) } })
    a.innerHTML = FOLDER
    return a
  }

  // ---- the table ----
  const head = h('tr', {})
  const body = h('tbody', {})
  const tableEl = h('table', { class: 't samples-grid', 'data-samples-table': '' }, h('thead', {}, head), body)
  const selectAll = h('input', { type: 'checkbox', 'aria-label': 'Select every row shown', onchange: (e) => {
    const on = !!e.currentTarget.checked
    for (const s of samples.filter(s => showX || kept(s))) setManual(s, on)
    update()
  } })
  const setManual = (s, on) => { if (on === passing.has(s)) manual.delete(s); else manual.set(s, on) }
  const drawHead = () => {
    const th = (c) => h('th', { class: c.numeric ? 'r' : null, 'data-column': c.key, title: c.output ? `${c.output}: ${c.path}` : c.path },
      c.label, h('small', {}, sourceText(c.output ?? c.source)))
    put(head, h('th', {}, selectAll), h('th', {}, 'Sample'), shownColumns().map(th), h('th', {}, 'Outputs'),
      h('th', { class: 'r' }, 'Size', h('small', {}, 'all outputs')))
  }
  const firstItem = (s) => {
    const mine = table.itemsOf.get(s)
    for (const o of table.perSample) if (mine.get(o)?.length) return mine.get(o)[0]
    return null
  }
  const outputsCell = (s) => {
    const mine = table.perSample.filter(o => table.itemsOf.get(s).get(o)?.length)
    // The config's has-chips, in its order (quant, BAM, bigWig, QC for rnaseq).
    const has = []
    for (const [o, { has: tag }] of Object.entries(config.outputs)) {
      if (tag && mine.includes(o) && !has.some(x => x.tag === tag)) has.push({ tag, o })
    }
    return h('td', { 'data-outputs': '' }, has.length
      ? h('div', { class: 'kinds' }, has.map(({ tag, o }) => h('span', { class: 'kind', title: [o, config.outputs[o]?.label].filter(Boolean).join(': ') }, tag)))
      : h('span', { class: 'muted' }, plural(mine.length, 'output')))
  }
  const rowOf = (s) => {
    const on = kept(s)
    const box = h('input', { type: 'checkbox', 'aria-label': `Select ${s}`, onchange: (e) => { setManual(s, !!e.currentTarget.checked); update() } })
    box.checked = on
    if (on) box.setAttribute('checked', '')
    const first = firstItem(s)
    const cell = (c) => {
      const values = valueOf(table, s, c)
      const cls = c.numeric ? 'r num' : null
      if (!values.length) return h('td', { class: [cls, 'muted'].filter(Boolean).join(' '), 'data-column': c.key }, '—')
      return h('td', { class: cls, 'data-column': c.key }, values.map(v => valueText(v.type, v.value, c.field)).join(', '))
    }
    return h('tr', { 'data-sample': s, class: on ? null : 'dim' },
      h('td', {}, box),
      h('td', { class: 'nowrap' }, h('div', { class: 'namecell' },
        first ? link(`#/item/${first.collection}/${first.item}`, s) : h('b', {}, s),
        projectLink(s),
        manual.has(s) ? h('span', { class: 'kind', 'data-manual': '', title: 'Ticked by hand: this overrides the filter' }, 'manual') : null)),
      shownColumns().map(cell),
      outputsCell(s),
      h('td', { class: 'r num nowrap', 'data-size': '' }, sizes.get(s) ?? h('span', { class: 'muted' }, '…')))
  }
  const drawBody = () => {
    const rows = samples.filter(s => showX || kept(s))
    put(body, rows.map(rowOf))
    selectAll.checked = rows.length > 0 && rows.every(kept)
  }

  // ---- the column picker line ----
  const columnPicker = picker({ attr: 'data-column-picker', label: 'add a column', outputs: table.perSample })
  const columnsButton = h('button', { type: 'button', class: 'btn sm', 'aria-haspopup': 'dialog', 'data-columns-button': '', onclick: (e) => {
    e.stopPropagation?.()
    columnPicker.toggle(showColumns)
  } }, 'Columns')
  const moreText = h('span', {})
  function showColumns() {
    columnPicker.fields(offered, {
      item: (c) => [h('span', { class: 'fb-path' }, c.path), h('span', { class: 'fb-count' }, added.has(c.key) ? 'shown' : '')],
      attrs: (c) => ({ 'data-add-column': c.key, 'aria-pressed': String(added.has(c.key)) }),
      pick: (c) => {
        if (added.has(c.key)) added.delete(c.key)
        else added.add(c.key)
        columnPicker.close()
        drawMore()
        drawHead()
        drawBody()
      },
      empty: 'No more fields.',
    })
  }
  const more = h('p', { class: 'muted samples-more', 'data-more-fields': '', hidden: offered.length ? null : true },
    moreText, ' ', h('span', { class: 'fb-anchor' }, columnsButton, columnPicker.el))
  const drawMore = () => {
    const rest = offered.filter(c => !added.has(c.key))
    const from = new Set(rest.flatMap(outputsOf))
    moreText.textContent = rest.length
      ? `${plural(rest.length, 'more field')} from ${plural(from.size, 'output')} in the column picker`
      : `Every field from the column picker is shown`
  }

  function update() {
    inForce = []
    for (const [i, cond] of where.entries()) {
      const el = chips.children[i]
      let bad = null
      if (cond[2] !== '') {
        try {
          sampleMatches(table, samples[0], [cond])
          inForce.push(cond)
        } catch (e) {
          bad = e.message
        }
      }
      if (!el) continue
      if (bad) { el.classList.add('bad'); el.setAttribute('title', bad) } else { el.classList.remove('bad'); el.setAttribute('title', cond[0]) }
    }
    passing = new Set(samples.filter(s => sampleMatches(table, s, inForce)))
    selected = samples.filter(kept)
    match.textContent = `${selected.length} of ${plural(samples.length, 'sample')}`
    const excluded = samples.length - selected.length
    showButton.textContent = showX ? 'Hide excluded' : `Show ${excluded} excluded`
    if (!showX && excluded === 0) showButton.setAttribute('hidden', ''); else showButton.removeAttribute('hidden')
    useButton.textContent = `Use ${plural(selected.length, 'sample')}`
    useButton.disabled = selected.length === 0
    drawBody()
    if (drawn) replaceUrl()
    report()
  }

  // Sizes fill in as they arrive; a page that has gone stops asking.
  const fillSize = (s, text) => {
    if (!alive()) return
    sizes.set(s, text)
    Array.from(body.children).find(r => r.dataset.sample === s)?.querySelector('[data-size]')?.replaceChildren(text)
  }
  for (const s of samples) sizeOf(s, null).then(size => fillSize(s, sizeText(size)), () => fillSize(s, '—'))

  drawChips()
  drawHead()
  drawMore()
  update()
  drawn = true
  return h('div', { class: 'samples-body' }, bar, projectArea,
    panel({ flush: true }, h('div', { class: 'tbox' }, tableEl)),
    more)
}

/**
 * A popover list of columns (the filter bar's picker, `.fb-picker`): a search
 * box ranking the columns by path (rankFields), grouped by output with the
 * shared paths first. `toggle(draw)` opens it with `draw()` or closes it;
 * `fields(columns, { item, attrs, pick, empty })` lists `columns`; `show(...)`
 * puts other content in it.
 */
function picker({ attr, label, outputs }) {
  const el = h('div', { class: 'fb-picker', role: 'dialog', 'aria-label': label, [attr]: '', 'data-open': 'false',
    onclick: (e) => e.stopPropagation?.(), onkeydown: (e) => { if (e.key === 'Escape') close() } })
  // A click anywhere else closes it; the page listener is there only while it is open.
  const close = () => {
    el.dataset.open = 'false'
    globalThis.document?.removeEventListener?.('click', close)
  }
  const groupRank = (g) => (g === SHARED ? -1 : outputs.indexOf(g))
  return {
    el,
    close,
    toggle(draw) {
      if (el.dataset.open === 'true') return close()
      // Open first: a hidden picker's search box cannot take focus.
      el.dataset.open = 'true'
      globalThis.document?.addEventListener?.('click', close)
      draw()
    },
    show(...parts) { put(el, parts) },
    fields(columns, { item, attrs, pick, empty }) {
      const list = h('div', { class: 'fb-list' })
      const draw = (q) => {
        const ranked = rankFields(columns.map(c => ({ path: c.path, c })), q).map(r => r.c)
        const groups = new Map()
        for (const c of ranked) {
          const g = c.output ?? SHARED
          groups.set(g, [...(groups.get(g) ?? []), c])
        }
        // With a search, the group holding the best match comes first; without, Shared then the outputs in order.
        const order = [...groups.keys()]
        if (!q) order.sort((a, b) => groupRank(a) - groupRank(b))
        put(list, order.map(g => h('div', { class: 'fb-group', 'data-group': g }, h('div', { class: 'fb-group-name' }, g),
          groups.get(g).map(c => h('button', { type: 'button', class: 'fb-item', ...attrs(c), onclick: () => pick(c) }, item(c))))))
        if (!ranked.length) put(list, h('p', { class: 'muted fb-note' }, q ? 'No field matches.' : empty))
      }
      const search = h('input', { class: 'fb-input', type: 'search', placeholder: 'Search fields…', 'aria-label': 'search fields', 'data-filter-search': '',
        oninput: (e) => draw(String(e.currentTarget.value ?? '').trim()) })
      draw('')
      put(el, search, list)
      search.focus?.()
    },
  }
}
