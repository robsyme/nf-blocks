// web/src/views/project-samples.js
// A project's Samples tab (P4 Task 3, mockups/13-project.html, plan 1.6):
// every occurrence of a sample in the project, one run's meta.id each,
// grouped into samples by sampleid.js, one row per sample. The occurrences
// are the whole-run members' samples, the member items' and the member
// Selections' items', and the samples added by name (matchRefs). Per-run
// values come from each run's own sample table, read and joined as the run
// page's Samples tab does (views/samples.js runSampleTable). Annotation
// cells are project Claims edited in place; Merge… and Split… write
// same_sample Claims. Every write re-reads the page, so a merge or a split
// shows at once from the page's own tail.
import { h, link } from '../html.js'
import { avatar, button, note, panel } from '../components.js'
import { seal } from '../seal.js'
import { presenceOf } from '../presence.js'
import { valueText, pairsOf } from '../pairs.js'
import { pipelineConfig } from '../promoted.js'
import { sampleKeyOf, valueOf } from '../samples.js'
import { inputAddresses, matchRefs, occurrenceKey, sampleGroups } from '../sampleid.js'
import { sampleAnnotations } from '../projects.js'
import { formatNumber, plural } from '../format.js'
import { PROJECT_RUNS_AT_ONCE, PROJECT_SAMPLES_ITEM_BUDGET, SAMPLES_ITEM_CAP, SIZE_CONCURRENCY } from '../config.js'
import { runSampleTable } from './samples.js'
import { samplesHref } from './search.js'
import { cids, namesText, show } from './common.js'
import { whenText } from '../words.js'
import { CONTRACT as DE, handoffHref } from '../templates/differentialabundance.js'

const put = (el, ...parts) => el.replaceChildren(...parts.flat(Infinity).filter(p => p !== null && p !== undefined && p !== false))
const mono = (text) => h('span', { class: 'mono' }, text)
/** The annotation column every project has, before the names its Claims use. */
export const DEFAULT_ANNOTATION = 'condition'
const SEALS = 4
/** A claim cell's title: who wrote it and when (also the hand-off page's condition cells). */
export const whoWhen = (c) => (c ? `${c.asserted_by ?? 'anonymous'}, ${whenText(c.timestamp) || 'at an unknown time'}` : null)

/**
 * The items of the member items and Selections, each once, as `[{ item,
 * collection }]`: a Selection's member item through its first via, an item
 * member (or a via-less one) through Explorer.itemCollections. Nested
 * Selections are followed, each once; one that cannot be read is skipped.
 */
async function memberItems(ex, members) {
  const found = new Map()
  const seen = new Set()
  const walk = async (selection) => {
    if (seen.has(selection)) return
    seen.add(selection)
    let s
    try { s = await ex.selection(selection) } catch { return }
    for (const m of s.members) {
      if (m.kind === 'selection') await walk(m.address)
      else if (!found.has(m.address)) found.set(m.address, m.via[0] ?? null)
    }
  }
  for (const m of members.filter(m => m.kind === 'item')) found.set(m.subject, null)
  for (const m of members.filter(m => m.kind === 'selection')) await walk(m.subject)
  const missing = [...found].filter(([, c]) => !c).map(([i]) => i)
  const origins = missing.length ? await ex.itemCollections(missing) : new Map()
  return [...found].map(([item, collection]) => ({ item, collection: collection ?? origins.get(item) ?? null })).filter(i => i.collection)
}

/** Each item of a run's sample table, as its sample: a Map of item cid to meta.id. */
function samplesByItem(table) {
  const out = new Map()
  for (const [sample, byOutput] of table.itemsOf) for (const entries of byOutput.values()) for (const e of entries) out.set(e.item, sample)
  return out
}

/** `fn` over `list`, at most `n` at once, the answers in `list` order (the worker queue Explorer.stableIndex uses). */
async function inTurn(list, n, fn) {
  const out = new Array(list.length)
  let next = 0
  const worker = async () => { for (let i = next++; i < list.length; i = next++) out[i] = await fn(list[i]) }
  await Promise.all(Array.from({ length: Math.min(n, list.length) }, worker))
  return out
}

/** How many items a run's outputs hold, read as the run's Samples tab counts them (no item read). */
async function itemCount(ex, cid) {
  const { collections } = await ex.run(cid)
  const totals = await Promise.all(collections.map(c => ex.collection(c.cid, { limit: 0 }).then(coll => coll.total, () => 0)))
  return totals.reduce((a, b) => a + b, 0)
}

/**
 * What the tab draws, read once: `{ runs, groups, automatic, inputs,
 * unmatched }`. `runs` is a Map of run cid to what was read of it, newest
 * finished first:
 *
 * - `{ row, table, config }`: the whole run's sample table (runSampleTable),
 *   for a whole-run member and for a run of a pipeline a sample added by
 *   name belongs to, since matching it needs every meta.id of the run;
 * - `{ row, config, items }`: only its member items (directly, or in a
 *   member Selection), each `{ id, keyPath, pairs, output, item }` from its
 *   own block;
 * - `over` (the run passes SAMPLES_ITEM_CAP), `budget` (reading it would
 *   pass `budget` items for the project) or `error`, with the member items
 *   still read one by one.
 *
 * Whole runs are read `PROJECT_RUNS_AT_ONCE` at a time, whole-run members
 * first, newest first. `groups` are sampleGroups' samples with the
 * project's same_sample Claims, `automatic` the same without them (a split
 * picks its `a` there). null when the page went away first.
 */
export async function projectSamples(ex, p, members, { alive = () => true, budget = PROJECT_SAMPLES_ITEM_BUDGET } = {}) {
  const wholeRuns = members.filter(m => m.kind === 'run').map(m => m.subject)
  const items = await memberItems(ex, members)
  const labels = await Promise.all(items.map(i => ex.runLabel(i.collection).catch(() => null)))
  const itemRuns = items.map((i, k) => ({ ...i, run: labels[k]?.completion ?? null, output: labels[k]?.output ?? null })).filter(i => i.run)
  const runCids = [...new Set([...wholeRuns, ...itemRuns.map(i => i.run)])]
  const rows = new Map(await Promise.all(runCids.map(async c => [c, await ex.runRow(c).catch(() => null)])))
  const finished = (c) => rows.get(c)?.finished_at ?? ''
  const newest = (a, b) => (finished(b) > finished(a) ? 1 : finished(b) < finished(a) ? -1 : a < b ? -1 : 1)
  const named = new Set(p.samples.map(s => s.ref.pipeline))
  const whole = [...wholeRuns.sort(newest), ...runCids.filter(c => !wholeRuns.includes(c) && named.has(rows.get(c)?.pipeline)).sort(newest)]
  const runs = new Map()
  // The budget is spent in that order on item counts, before any item is read.
  const counts = await inTurn(whole, PROJECT_RUNS_AT_ONCE, c => (alive() ? itemCount(ex, c).catch(error => ({ error })) : null))
  if (!alive()) return null
  let spent = 0
  const toRead = []
  whole.forEach((c, i) => {
    const n = counts[i]
    if (n?.error) runs.set(c, { row: rows.get(c), error: n.error })
    else if (n > SAMPLES_ITEM_CAP) runs.set(c, { row: rows.get(c), over: n })
    else if (spent + n > budget) runs.set(c, { row: rows.get(c), budget: n })
    else { spent += n; toRead.push(c) }
  })
  const read = await inTurn(toRead, PROJECT_RUNS_AT_ONCE, c => (alive() ? runSampleTable(ex, c, { alive }).catch(error => ({ error })) : null))
  if (!alive() || read.some(r => r === null)) return null
  toRead.forEach((c, i) => runs.set(c, read[i].error ? { row: rows.get(c), error: read[i].error } : read[i]))
  // Every other run gives only its member items, each read from its block.
  const loose = itemRuns.filter(i => !runs.get(i.run)?.table && rows.get(i.run))
  const blocks = await inTurn(loose, SIZE_CONCURRENCY, i => ex.itemBlock(i.collection, i.item).then(b => b.view, () => null))
  loose.forEach((i, k) => {
    const row = rows.get(i.run)
    const entry = runs.get(i.run) ?? { row, config: pipelineConfig(row.pipeline) }
    if (!runs.has(i.run)) runs.set(i.run, entry)
    entry.config ??= pipelineConfig(row.pipeline)
    entry.items ??= new Map()
    const pairs = blocks[k] ? pairsOf(blocks[k]) : null
    const key = pairs ? sampleKeyOf(pairs, entry.config.joinKey) : null
    if (key) entry.items.set(key.value, { id: key.value, keyPath: key.path, pairs, output: i.output, item: i.item })
  })
  const ordered = new Map([...runs].sort(([a], [b]) => newest(a, b)))
  const occurrences = []
  for (const c of wholeRuns) {
    const r = ordered.get(c)
    if (r?.table) for (const id of r.table.samples) occurrences.push({ pipeline: r.row.pipeline, id, run: c })
  }
  const byItem = new Map([...ordered].filter(([, r]) => r.table).map(([c, r]) => [c, samplesByItem(r.table)]))
  for (const i of itemRuns) {
    const r = ordered.get(i.run)
    const id = byItem.get(i.run)?.get(i.item) ?? [...(r?.items?.values() ?? [])].find(x => x.item === i.item)?.id ?? null
    if (id && r?.row) occurrences.push({ pipeline: r.row.pipeline, id, run: i.run })
  }
  const known = [...ordered].filter(([, r]) => r.table).map(([run, r]) => ({ run, pipeline: r.row.pipeline, ids: r.table.samples }))
  const { occurrences: byName, unmatched } = matchRefs(p.samples.map(s => s.ref), known)
  occurrences.push(...byName)
  const inputs = await ex.runInputs(runCids)
  return { runs: ordered, inputs, unmatched, budget,
    groups: sampleGroups(occurrences, { inputs, claims: p.sameSample }), automatic: sampleGroups(occurrences, { inputs }) }
}

/**
 * One sample in one run, as its row shows it: the promoted field `label`'s
 * values, the item its presence dots ask about, and the join path its link
 * filters on; from the run's table, else from the member item's own block.
 */
function sampleIn(run, id, label) {
  const field = label ? run?.config?.fields.find(f => f.label === label) ?? null : null
  if (run?.table) {
    const t = run.table
    const column = field ? t.columns.find(c => c.field?.label === label) : null
    const first = t.perSample.map(o => t.itemsOf.get(id)?.get(o)?.[0]).find(Boolean)
    return { field, values: column ? valueOf(t, id, column) : [], item: first?.item ?? null, joinPath: t.joinPath ?? 'meta.id' }
  }
  const it = run?.items?.get(id)
  if (!it) return { field, values: [], item: null, joinPath: 'meta.id' }
  const pair = field && (!field.output || field.output === it.output) ? it.pairs.find(x => x.path === field.path) : null
  return { field, values: pair ? pair.values.map((value, i) => ({ type: pair.types[i], value })) : [], item: it.item, joinPath: it.keyPath }
}

/** The occurrence of `g` in its newest run (runs is newest first). */
const latestOf = (g, runs) => [...runs.keys()].map(run => g.occurrences.find(o => o.run === run)).find(Boolean) ?? g.occurrences[0]

/** The first promoted field's label among the project's pipelines ("Reads (trimmed)" for nf-core/rnaseq), or null. */
function fieldLabel(runs) {
  for (const r of runs.values()) if (r.config?.fields?.length) return r.config.fields[0].label
  return null
}

/** The basis line under the table (plan 1.6 step 6), in the mockup's words when no run records its inputs. */
function basisNote(d) {
  const byInputs = d.groups.filter(g => g.basis === 'input reads').length
  if (!d.inputs.length) {
    return ['Grouped by ', mono('meta.id'), '. Grouping by input reads needs the plugin to record input addresses (', mono('input_set'), ' is empty in these runs).']
  }
  if (byInputs === d.groups.length) return 'Grouped by input reads: occurrences that read the same input files are one sample, whatever their meta.id says.'
  if (byInputs === 0) return ['Grouped by ', mono('meta.id'), ': these runs record their input files, but none of them is one sample\'s reads.']
  return ['Grouped by input reads where a run records them, and by ', mono('meta.id'), ' otherwise.']
}

const basisMeta = (d) => (d.groups.every(g => g.basis === 'input reads') && d.groups.length ? 'grouped by input reads'
  : d.groups.some(g => g.basis === 'input reads') ? 'grouped by input reads and sample ID' : 'grouped by sample ID')

/**
 * The Samples tab: a node at once, filled when every run's table is read.
 * `can` offers the edits; `write((writer) => promise, button)` runs one
 * through ctx.write.run and re-reads this tab; `counted(n)` puts the sample count on
 * the tab; `aside` (storage and recent activity) sits beside the table, as
 * in the mockup.
 */
export function samplesTab(ex, p, ctx, { members, can, write, counted = () => {}, aside = null, alive = () => true, budget = PROJECT_SAMPLES_ITEM_BUDGET }) {
  const node = h('div', { class: 'grid-2 project-samples' }, h('p', { class: 'muted', 'data-samples-loading': '' }, "Reading the project's runs…"), aside)
  projectSamples(ex, p, members, { alive, budget }).then((d) => {
    if (!d) return
    counted(d.groups.length)
    put(node, samplesPanel(ex, p, ctx, d, { can, write }), aside)
  }, (e) => put(node, note(`Could not read the project's samples: ${e?.message ?? e}`, 'bad'), aside))
  return node
}

function samplesPanel(ex, p, ctx, d, { can, write }) {
  const runName = (run) => d.runs.get(run)?.row?.run_name ?? 'unnamed run'
  const label = fieldLabel(d.runs)
  const names = [...new Set([DEFAULT_ANNOTATION, ...p.annotations.map(a => a.name).sort()])]
  const withInputs = d.groups.some(g => g.basis === 'input reads')
  const addresses = withInputs ? inputAddresses(d.inputs) : new Map()
  const merge = can && d.groups.length > 1 ? mergeForm(p, d, write) : null
  const split = can && d.groups.some(g => g.occurrences.length > 1) ? splitForm(p, d, write, runName) : null
  const head = h('tr', {}, h('th', {}, 'Sample'), h('th', { class: 'r' }, 'Runs'),
    label ? h('th', { class: 'r' }, label, h('small', {}, 'latest run')) : null,
    names.map(n => h('th', { 'data-annotation': n }, n, h('small', {}, 'project claim'))),
    withInputs ? h('th', {}, 'Input reads') : null, h('th', {}, 'Where'))
  const rows = d.groups.map((g) => {
    const latest = latestOf(g, d.runs)
    const here = sampleIn(d.runs.get(latest.run), latest.id, label)
    const others = [...new Set(g.occurrences.map(o => o.id))].filter(id => id !== g.label)
    const runs = [...new Set(g.occurrences.map(o => o.run))]
    const cells = sampleAnnotations(g.refs, p.annotations)
    const reads = withInputs ? [...new Set(g.occurrences.flatMap(o => [...(addresses.get(occurrenceKey(o)) ?? [])]))].sort() : []
    return h('tr', { 'data-sample-group': g.label, 'data-basis': g.basis, 'data-occurrences': g.occurrences.map(occurrenceKey).join(' ') },
      h('td', { class: 'nowrap' }, link(samplesHref(latest.run, here.joinPath, latest.id), h('b', {}, g.label)),
        others.length ? h('div', { class: 'sub' }, `also ${namesText(others)}`) : null),
      h('td', { class: 'r num', title: namesText(runs.map(runName), 5), 'data-runs': String(runs.length) }, formatNumber(runs.length)),
      label ? h('td', { class: here.values.length ? 'r num' : 'r num muted', 'data-field': label },
        here.values.length ? here.values.map(v => valueText(v.type, v.value, here.field)).join(', ') : '—') : null,
      names.map(n => annotationCell(p, ctx, g, n, cells.get(n) ?? null, { can, write })),
      withInputs ? h('td', { 'data-input-reads': String(reads.length) }, reads.slice(0, SEALS).map(a => h('span', { title: a, 'data-input': a }, seal(a, { size: 14 }))),
        reads.length > SEALS ? h('span', { class: 'muted' }, ` +${reads.length - SEALS}`) : reads.length ? null : h('span', { class: 'muted' }, '—')) : null,
      h('td', {}, here.item ? presenceOf(ctx).dots(here.item) : null))
  })
  const unread = [...d.runs].filter(([, r]) => r.over !== undefined || r.budget !== undefined || r.error)
  return h('section', { class: 'panel flush', 'data-samples-panel': '' },
    h('div', { class: 'ph' }, h('h2', {}, 'Samples'), h('span', { class: 'muted', 'data-basis-meta': '' }, basisMeta(d)),
      h('span', { class: 'spacer' }), handoffLink(p, d), merge?.open, split?.open),
    merge?.form, split?.form,
    d.groups.length ? h('div', { class: 'pb tbox' }, h('table', { class: 't', 'data-project-samples': '' }, h('thead', {}, head), h('tbody', {}, rows)))
      : h('div', { class: 'pb' }, h('p', { class: 'muted', 'data-samples-empty': '' }, 'No samples yet. Add a run, a Selection or an item to the project.')),
    h('div', { class: 'pb project-basis' }, h('p', { class: 'note', 'data-basis-note': '' }, h('span', {}, basisNote(d))),
      d.unmatched.length ? h('p', { class: 'muted', 'data-unmatched': '' },
        `${plural(d.unmatched.length, 'sample')} added by name ${d.unmatched.length === 1 ? 'is' : 'are'} in none of these runs: ${namesText(d.unmatched.map(r => r.id))}.`) : null,
      unread.length ? h('p', { class: 'muted', 'data-runs-unread': '' }, `Left out: ${leftOut(unread.map(([, r]) => r), d.budget)}.${unread.some(([, r]) => r.items?.size) ? ' Their member items still count.' : ''}`) : null))
}

/**
 * "Hand off…" (P4 Task 5): the differentialabundance hand-off from the newest
 * run of its source pipeline here (one with the merged matrix first), with
 * this project active and the project's samples in that run ticked; null
 * when no run is of that pipeline.
 */
function handoffLink(p, d) {
  const runs = [...d.runs].filter(([, r]) => r.row?.pipeline === DE.from)
  const [from] = runs.find(([, r]) => r.table?.runLevelItems?.has(DE.matrix.output)) ?? runs[0] ?? []
  if (!from) return null
  const samples = [...new Set(d.groups.flatMap(g => g.occurrences.filter(o => o.run === from).map(o => o.id)))].sort()
  return h('a', { class: 'btn sm', href: handoffHref({ from, project: p.cid, samples }), 'data-handoff-link': '' }, 'Hand off…')
}

/** The runs not read whole, in words: each over the item cap, those past the project's budget together, each that failed. */
function leftOut(runs, budget) {
  const name = (r) => r.row?.run_name ?? 'a run'
  const past = runs.filter(r => r.budget !== undefined)
  return [
    ...runs.filter(r => r.over !== undefined).map(r => `${name(r)} has ${formatNumber(r.over)} items, more than a sample table joins`),
    past.length ? `${namesText(past.map(r => `${name(r)} (${plural(r.budget, 'item')})`), past.length)} would pass the ${formatNumber(budget)} items this page reads for a project` : null,
    ...runs.filter(r => r.error).map(r => `${name(r)} could not be read (${r.error?.message ?? r.error})`),
  ].filter(Boolean).join('; ')
}

/**
 * One annotation cell. Read-only, the value as a claim tag. Editable, the
 * value in an input inside the tag: a change writes `add annotation`
 * superseding the cell's current Claims (an empty value clears them), on the
 * sample's first occurrence's SampleRef; typing puts the writer's avatar on
 * the cell. A conflicted cell shows each value, and picking one (or typing
 * another) writes one value that supersedes them all; emptying it writes
 * nothing, and its Clear clears them all. Each value's title says who wrote
 * it and when.
 */
function annotationCell(p, ctx, g, name, cell, { can, write }) {
  const claims = cids(cell?.claims ?? [])
  const last = cell?.claims.at(-1) ?? null
  const by = last?.asserted_by ?? null
  const td = h('td', { 'data-annotation': name, 'data-value': cell?.value ?? null, 'data-conflicted': cell?.conflicted ? '' : null })
  const ref = { pipeline: g.occurrences[0].pipeline, id: g.occurrences[0].id }
  const save = (value, el) => write((w) => (value === '' ? w.clearAnnotation(p.cid, claims) : w.annotate(p.cid, ref, name, value, claims)), el)
  const entryTitle = (e) => whoWhen(e.claim)
  const entryTag = (e) => h('span', { class: 'claim conflict', title: entryTitle(e) }, avatar(e.claim.asserted_by), e.value)
  if (!can) {
    if (!cell) return td
    td.append(cell.conflicted ? h('span', { class: 'row' }, cell.entries.map(entryTag))
      : h('span', { class: 'claim', title: whoWhen(last) }, avatar(by), cell.value))
    return td
  }
  const face = h('span', {}, cell ? avatar(by) : null)
  const input = h('input', { type: 'text', class: 'cell-input', maxlength: '256', value: cell?.value ?? '', placeholder: cell?.conflicted ? 'or type one' : '—',
    'aria-label': `${name} for ${g.label}`,
    oninput: () => face.replaceChildren(avatar(ctx.viewer ?? 'anonymous')),
    onchange: (e) => {
      const value = String(e.currentTarget.value ?? '').trim()
      // Nothing to write, or (in a conflicted cell) an emptied input, which never clears every value: Clear does.
      if (value === '' && (!cell || cell.conflicted)) return
      if (cell && !cell.conflicted && value === cell.value) return
      save(value, e.currentTarget)
    } })
  input.value = cell?.value ?? ''
  if (!cell?.conflicted) {
    td.append(h('span', { class: cell ? 'claim cell-claim' : 'cell-claim', title: cell ? whoWhen(last) : null }, face, input))
    return td
  }
  td.append(h('div', { class: 'cell-conflict', title: 'More than one value is current. Pick one, or type another: it replaces them all.' },
    cell.entries.map(e => h('button', { type: 'button', class: 'claim conflict', 'data-pick-value': e.value, title: entryTitle(e),
      onclick: (ev) => save(e.value, ev.currentTarget) }, avatar(e.claim.asserted_by), e.value)),
    h('span', { class: 'cell-claim' }, face, input),
    button('Clear', { size: 'sm', kind: 'ghost', 'data-clear-annotation': '', title: 'Clears every value of this cell',
      onclick: (ev) => save('', ev.currentTarget) })))
  return td
}

const sameKeys = (c, a, b) => {
  const x = [occurrenceKey(c.a), occurrenceKey(c.b)].sort().join(' ')
  return x === [occurrenceKey(a), occurrenceKey(b)].sort().join(' ')
}

/** A form opened by its button in the panel head: `{ open, form }`. */
function opener(label, attr, ...body) {
  const form = h('div', { class: 'pb project-form', [attr]: '', hidden: true }, ...body)
  const open = button(label, { size: 'sm', [`${attr}-open`]: '', onclick: () => show(form, form.hasAttribute('hidden')) })
  return { open, form }
}

/**
 * Merge…: two samples, and one same:true Claim between an occurrence of
 * each. It supersedes a split between exactly those two occurrences.
 */
function mergeForm(p, d, write) {
  const options = () => d.groups.map((g, i) => h('option', { value: String(i) }, g.label))
  const one = h('select', { 'aria-label': 'First sample', 'data-merge-a': '' }, options())
  const two = h('select', { 'aria-label': 'Second sample', 'data-merge-b': '' }, options())
  one.value = '0'
  two.value = '1'
  const said = h('p', { class: 'muted small' })
  return opener('Merge…', 'data-merge',
    h('p', { class: 'small' }, 'Merging says two samples are one: a same-sample Claim on this project, which Split can undo.'),
    h('div', { class: 'row' }, one, h('span', {}, 'and'), two,
      button('Merge', { kind: 'primary', size: 'sm', 'data-merge-go': '', onclick: (e) => {
        const [ga, gb] = [d.groups[Number(one.value)], d.groups[Number(two.value)]]
        if (!ga || !gb || ga === gb) { said.textContent = 'Pick two different samples.'; return }
        const [a, b] = [ga.occurrences[0], gb.occurrences[0]]
        const splits = p.sameSample.filter(c => c.same === false && sameKeys(c, a, b)).map(c => c.claim.cid)
        write((w) => w.sameSample(p.cid, a, b, true, splits), e.currentTarget)
      } })), said)
}

/**
 * Split…: one occurrence of a sample of several, and a same:false Claim
 * between it and another occurrence of its sample, from its automatic group
 * when it has one (plan 1.6 step 3 removes the edges to that whole group).
 * Merges apply after splits, so the Claim supersedes every current merge
 * between it and the rest of its sample, or the merge would keep it in.
 */
function splitForm(p, d, write, runName) {
  const choices = d.groups.filter(g => g.occurrences.length > 1)
    .flatMap(g => g.occurrences.map(o => ({ g, o })))
  const pick = h('select', { 'aria-label': 'Occurrence to split off', 'data-split-pick': '' },
    choices.map((c, i) => h('option', { value: String(i), 'data-occurrence': occurrenceKey(c.o) }, `${c.g.label}: ${c.o.id} in ${runName(c.o.run)}`)))
  pick.value = '0'
  return opener('Split…', 'data-split',
    h('p', { class: 'small' }, 'Splitting says one run\'s sample is not the same as the rest: it leaves its sample and becomes its own.'),
    h('div', { class: 'row' }, pick,
      button('Split off', { kind: 'primary', size: 'sm', 'data-split-go': '', onclick: (e) => {
        const { g, o: b } = choices[Number(pick.value)]
        const key = occurrenceKey(b)
        const inGroup = new Set(g.occurrences.map(occurrenceKey))
        const auto = d.automatic.find(ag => ag.occurrences.some(o => occurrenceKey(o) === key))
        const a = auto?.occurrences.find(o => occurrenceKey(o) !== key && inGroup.has(occurrenceKey(o)))
          ?? g.occurrences.find(o => occurrenceKey(o) !== key)
        const merges = p.sameSample.filter(c => c.same === true && [c.a, c.b].some(o => occurrenceKey(o) === key)
          && [c.a, c.b].every(o => inGroup.has(occurrenceKey(o)))).map(c => c.claim.cid)
        write((w) => w.sameSample(p.cid, a, b, false, merges), e.currentTarget)
      } })))
}
