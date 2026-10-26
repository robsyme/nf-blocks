// web/src/samples.js
// The run's sample table (P1a): every output's items joined on the
// pipeline's join key, the columns the Samples tab can show and where each
// value comes from, the where filter with output-qualified keys, and the
// items a hand-off takes. Filters go through metadata.matches over the
// items' item_attr rows, so a sample passes here when Index.items would
// find its item. Pure: views/samples.js draws what it decides.
import { NOT_EQUAL, PredicateError, RANGE_OPS, matches, predicateRow, rangeRow } from './metadata.js'
import { shortLabel, splitKey } from './promoted.js'
import { itemTable } from './table.js'

export { splitKey }

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

/** The item's sample key: the first `joinKey` path its pairs hold as one string value, as `{ path, value }`, or null. */
export function sampleKeyOf(pairs, joinKey) {
  for (const path of joinKey) {
    const e = (pairs ?? []).find(p => p.path === path)
    if (e && e.values.length === 1 && e.types[0] === 'string') return { path, value: e.values[0] }
  }
  return null
}

/**
 * The sample table for one run. `byOutput`: each output's items as `{ item,
 * collection, pairs, rows, slots }` (`pairs` from pairs.js `pairsOf`, `rows`
 * from metadata.js `attrRows`). The rule is decided per output. An output of
 * two or more items, every one keyed, is per-sample when at least one of its
 * keys also keys an item of another output (one-item outputs count), or when
 * it is the only output of two or more items that carries keys; then all its
 * keys are samples, so a sample that dropped out of later steps stays. A
 * one-item output is per-sample when its key is a sample. Any other output
 * with items is run-level. So a contrasts output beside per-sample outputs is
 * run-level (no contrast keys another output), while they are listed as
 * samples once one contrast key also keys another output, even a one-item one.
 * `itemsOf` and `runLevelItems` hold those same entries. `joinPath` is the
 * join path most per-sample items are keyed by (ties: `joinKey` order).
 *
 * `columns`: the promoted fields present, in config order, then each path
 * two or more per-sample outputs carry. A path is one column keyed by the
 * path when, for every sample, those outputs give it the same values;
 * otherwise it splits into one column per output, keyed `output:path`. An
 * output-qualified field is always its output's column. `picker`: per
 * per-sample output, the paths only that output carries, keyed
 * `output:path`. Neither holds a join key path or a path `itemTable` finds
 * repeating another in that output.
 */
export function buildSamples(byOutput, config) {
  const joinKey = config?.joinKey ?? ['meta.id', 'id']
  const keyed = new Map([...byOutput].map(([output, entries]) => [output, entries.map(e => ({ e, key: sampleKeyOf(e.pairs, joinKey) }))]))
  const outputsOf = new Map()
  for (const [output, list] of keyed) {
    for (const { key } of list) if (key) outputsOf.set(key.value, (outputsOf.get(key.value) ?? new Set()).add(output))
  }
  const keyedMulti = [...keyed].filter(([, list]) => list.length >= 2 && list.some(k => k.key))
  const multiPerSample = keyedMulti.filter(([, list]) => list.every(k => k.key)
    && (keyedMulti.length === 1 || list.some(k => outputsOf.get(k.key.value).size >= 2)))
  const sampleSet = new Set(multiPerSample.flatMap(([, list]) => list.map(k => k.key.value)))
  const samples = [...sampleSet].sort(byText)
  const perSample = []
  const runLevel = []
  for (const [output, list] of keyed) {
    if (list.length) (list.every(k => k.key && sampleSet.has(k.key.value)) ? perSample : runLevel).push(output)
  }
  perSample.sort(byText)
  runLevel.sort(byText)

  const itemsOf = new Map(samples.map(s => [s, new Map()]))
  const keyedBy = new Map()
  for (const output of perSample) {
    for (const { e, key } of keyed.get(output)) {
      const mine = itemsOf.get(key.value)
      mine.set(output, [...(mine.get(output) ?? []), e])
      keyedBy.set(key.path, (keyedBy.get(key.path) ?? 0) + 1)
    }
  }
  const joinPath = [...joinKey].sort((a, b) => (keyedBy.get(b) ?? 0) - (keyedBy.get(a) ?? 0))
    .find(p => keyedBy.has(p)) ?? null
  const table = { joinPath, samples, perSample, runLevel, itemsOf,
    runLevelItems: new Map(runLevel.map(o => [o, byOutput.get(o)])), columns: [], picker: new Map() }

  const column = (output, path, field = null) => ({ key: output ? `${output}:${path}` : path, output, path,
    label: field?.label ?? shortLabel(path), source: output ?? path, field, numeric: false })
  const carries = (output, path) => byOutput.get(output).some(e => (e.pairs ?? []).some(p => p.path === path))
  const sigOf = (sample, output, path) => {
    const values = valueOf(table, sample, { output, path })
    return values.length ? JSON.stringify(values.map(v => [v.type, v.value]).sort()) : null
  }
  const agree = (path, outputs) => samples.every(s => new Set(outputs.map(o => sigOf(s, o, path)).filter(x => x !== null)).size <= 1)
  const across = (path, outputs, field) => (outputs.length === 1 || agree(path, outputs)
    ? [column(null, path, field)] : outputs.map(o => column(o, path, field)))

  const taken = new Set()
  const columns = []
  for (const field of config?.fields ?? []) {
    if (field.output) {
      if (!perSample.includes(field.output) || !carries(field.output, field.path)) continue
      columns.push(column(field.output, field.path, field))
      taken.add(`${field.output}:${field.path}`)
    } else {
      const outputs = perSample.filter(o => carries(o, field.path))
      if (!outputs.length) continue
      columns.push(...across(field.path, outputs, field))
      for (const o of outputs) taken.add(`${o}:${field.path}`)
    }
  }

  const skip = new Set(joinKey)
  const offered = new Map()
  for (const output of perSample) {
    const entries = byOutput.get(output)
    const { duplicates } = itemTable(entries)
    const paths = new Set()
    for (const e of entries) {
      for (const p of e.pairs ?? []) if (!duplicates.has(p.path) && !skip.has(p.path) && !taken.has(`${output}:${p.path}`)) paths.add(p.path)
    }
    offered.set(output, paths)
  }
  const picker = new Map()
  for (const path of [...new Set([...offered.values()].flatMap(s => [...s]))].sort(byText)) {
    const outputs = perSample.filter(o => offered.get(o).has(path))
    if (outputs.length >= 2) columns.push(...across(path, outputs, null))
    else picker.set(outputs[0], [...(picker.get(outputs[0]) ?? []), column(outputs[0], path)])
  }

  table.columns = columns
  table.picker = new Map(perSample.filter(o => picker.has(o)).map(o => [o, picker.get(o)]))
  for (const c of [...columns, ...[...table.picker.values()].flat()]) {
    const values = samples.flatMap(s => valueOf(table, s, c))
    c.numeric = values.length > 0 && values.every(v => v.type === 'int' || v.type === 'float')
  }
  return table
}

/** A sample's items a column reads: its items in the column's output, or in every output for an unqualified column. */
const entriesFor = (table, sample, output) => {
  const mine = table.itemsOf.get(sample)
  if (!mine) return []
  return output ? (mine.get(output) ?? []) : [...mine.values()].flat()
}

/** The sample's values in `column` over its items there, each `{ type, value }` once, in item order. */
export function valueOf(table, sample, column) {
  const seen = new Set()
  const out = []
  for (const e of entriesFor(table, sample, column.output)) {
    for (const p of e.pairs ?? []) {
      if (p.path !== column.path) continue
      p.values.forEach((value, i) => {
        const k = JSON.stringify([p.types[i], value])
        if (!seen.has(k)) { seen.add(k); out.push({ type: p.types[i], value }) }
      })
    }
  }
  return out
}

/** One where entry as a predicate row, the way Explorer.items builds its own; throws PredicateError. */
function predicateOf([path, type, value, op]) {
  const row = op === NOT_EQUAL ? { ...predicateRow(path, type, value), op } : op ? rangeRow(path, op, value) : predicateRow(path, type, value)
  if (row.truncated && row.op === NOT_EQUAL) throw new PredicateError(`the value for ${path} != is too long to compare: values that long are stored as digests`)
  return row
}

/**
 * Whether `sample` passes every condition of `where`. A qualified key
 * (`output:path`) reads the sample's items in that output, an unqualified one
 * all its items; the condition holds when one of them matches, and `!=` when
 * none holds the value, so a sample with no item there fails `=` and ranges
 * and passes `!=`. Throws PredicateError for a value its type cannot read.
 */
export function sampleMatches(table, sample, where) {
  const outputs = new Set([...table.perSample, ...table.runLevel])
  return where.every(([key, ...rest]) => {
    const { output, path } = splitKey(key, outputs)
    return matches(entriesFor(table, sample, output).flatMap(e => e.rows ?? []), [predicateOf([path, ...rest])])
  })
}

const OPS = [NOT_EQUAL, ...RANGE_OPS]

/** The Samples tab's `where` from the URL: a JSON list of `[key, type, value]` or `[key, type, value, op]`; empty text is no filter. Throws `{ code: 'bad_route' }`. */
export function parseSamplesWhere(text) {
  if (text === null || text === undefined || text === '') return []
  try {
    const where = JSON.parse(text)
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && typeof w[0] === 'string'
      && (w.length === 3 || (w.length === 4 && OPS.includes(w[3])))))
      throw new Error(`not a list of [key, type, value] or [key, type, value, op] with op one of ${OPS.join(' ')}`)
    return where
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[key, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
}

/** `where` as the URL carries it. */
export const samplesWhereText = (where) => JSON.stringify(where)

/**
 * The items a hand-off takes, in `outputs` order: for a per-sample output,
 * each of `samples`' items there, in `samples` order; for a run-level one,
 * all its items, with `sample` null. Each address once.
 */
export function handoffSet(table, samples, outputs) {
  const items = []
  const seen = new Set()
  const add = (e, output, sample) => {
    if (seen.has(e.item)) return
    seen.add(e.item)
    items.push({ address: e.item, via: e.collection ? [e.collection] : [], output, sample })
  }
  for (const output of outputs) {
    if (table.runLevelItems.has(output)) for (const e of table.runLevelItems.get(output)) add(e, output, null)
    else for (const s of samples) for (const e of table.itemsOf.get(s)?.get(output) ?? []) add(e, output, s)
  }
  return { items }
}
