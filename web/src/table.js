// web/src/table.js
// The items table's shape (explorer layout B spec §5.4), decided from the
// previews loaded so far: which Meta Map paths are columns, which collapse into
// the "same for every item" line, which repeat another path's values, the
// file columns (items file columns spec §6) and an output's file suffixes.
// Pure: the views draw what it decides.
import { predicateRow } from './metadata.js'

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const sig = (entry) => JSON.stringify([entry.types, entry.values])
const keyOf = (path) => path.split('.').at(-1)

/** The entry at `path` in one preview, or null. */
export const cellOf = (preview, path) => preview?.pairs?.find(e => e.path === path) ?? null

/**
 * `previews`: previews in any state; only those with pairs count. The same key
 * at two paths (`id`, `meta.id`), present on every loaded item with equal values
 * on each, is one column, kept under the shorter path (ties: sort order); two
 * different keys are never merged, whatever their values. A path with one value on every
 * loaded item is "constant" once two or more items have loaded (P10). The
 * rest are columns: strings first, then the most distinct values, then by path.
 */
export function itemTable(previews, { maxColumns = 6 } = {}) {
  const items = (previews ?? []).filter(p => p?.pairs)
  const n = items.length
  const stats = new Map()
  for (const p of items) {
    for (const entry of p.pairs) {
      const s = stats.get(entry.path) ?? { present: 0, sigs: new Set(), string: true, first: entry }
      s.present++
      s.sigs.add(sig(entry))
      if (entry.type !== 'string') s.string = false
      stats.set(entry.path, s)
    }
  }
  const everywhere = (path) => stats.get(path).present === n
  const paths = [...stats.keys()].sort((a, b) => (a.length - b.length) || byText(a, b))
  const duplicates = new Map()
  for (let i = 0; i < paths.length; i++) {
    const keep = paths[i]
    if (duplicates.has(keep) || !everywhere(keep)) continue
    for (const other of paths.slice(i + 1)) {
      if (duplicates.has(other) || !everywhere(other) || keyOf(other) !== keyOf(keep)) continue
      if (items.every(p => sig(cellOf(p, keep)) === sig(cellOf(p, other)))) duplicates.set(other, keep)
    }
  }
  const kept = paths.filter(p => !duplicates.has(p))
  const constantPaths = new Set(n >= 2 ? kept.filter(p => everywhere(p) && stats.get(p).sigs.size === 1) : [])
  const constant = [...constantPaths].sort(byText).map(p => stats.get(p).first)
  const columns = kept.filter(p => !constantPaths.has(p))
    .sort((a, b) => (Number(stats.get(b).string) - Number(stats.get(a).string))
      || (stats.get(b).sigs.size - stats.get(a).sigs.size) || byText(a, b))
    .slice(0, maxColumns)
  return { columns, constant, duplicates }
}

/**
 * The fields an output's items carry, for the run page: every path on any
 * loaded item, sorted, with a key `itemTable` found at two paths listed once.
 */
export function fieldPaths(previews) {
  const { duplicates } = itemTable(previews)
  const paths = new Set()
  for (const p of previews ?? []) for (const e of p?.pairs ?? []) if (!duplicates.has(e.path)) paths.add(e.path)
  return [...paths].sort(byText)
}

/**
 * What the filter picker offers (filter bar, option A of `filters-v1.html`):
 * each metadata path over the loaded items, a key `itemTable` found at two
 * paths listed once, shallower paths first, with its values and how many
 * loaded items hold each, most common first. `type` is the values' one type,
 * or `mixed`.
 */
export function fieldChoices(previews) {
  const loaded = (previews ?? []).filter(p => p?.pairs)
  const { duplicates } = itemTable(loaded)
  const fields = new Map()
  for (const p of loaded) {
    for (const e of p.pairs) {
      if (duplicates.has(e.path)) continue
      const f = fields.get(e.path) ?? { path: e.path, types: new Set(), values: new Map() }
      const seen = new Set()
      e.values.forEach((value, i) => {
        const type = e.types[i]
        const key = JSON.stringify([type, value])
        f.types.add(type)
        if (seen.has(key)) return
        seen.add(key)
        const v = f.values.get(key) ?? { type, value, count: 0 }
        v.count++
        f.values.set(key, v)
      })
      fields.set(e.path, f)
    }
  }
  const depth = (path) => path.split('.').length
  return [...fields.values()].sort((a, b) => (depth(a.path) - depth(b.path)) || byText(a.path, b.path)).map(f => ({
    path: f.path,
    type: f.types.size === 1 ? [...f.types][0] : 'mixed',
    values: [...f.values.values()].sort((a, b) => (b.count - a.count) || byText(String(a.value), String(b.value))),
  }))
}

/** A typed-in value as a condition `[path, type, value]`: the field's type (a mixed field's is string), normalised by `predicateRow`, which throws `PredicateError`. */
export function typedCondition(field, text) {
  const row = predicateRow(field.path, field.type === 'mixed' ? 'string' : field.type, text)
  return [row.path, row.type, row.value]
}

const positionOf = (column) => /^\[(\d+)\]$/.exec(column)?.[1]

/** Column keys sorted the file columns' way: record keys by text, then tuple positions `[i]` by number. */
function columnOrder(keys) {
  const names = [...keys].filter(k => positionOf(k) === undefined).sort(byText)
  const positions = [...keys].filter(k => positionOf(k) !== undefined).sort((a, b) => positionOf(a) - positionOf(b))
  return [...names, ...positions]
}

/** Every column key over the loaded previews, split by whether any item stores a file under it. */
function columnKeys(previews) {
  const all = new Set()
  const stored = new Set()
  for (const p of previews ?? []) for (const s of p?.slots ?? []) { all.add(s.column); if (s.address) stored.add(s.column) }
  return { all, stored }
}

/** The file columns over the loaded previews (items file columns spec §6.1): keys with a stored file in some item. */
export function fileColumns(previews) {
  return columnOrder(columnKeys(previews).stored)
}

/** The keys whose slots are address-less in every loaded item (spec §6.1, amended): listed above the table, not drawn as columns. */
export function emptyColumns(previews) {
  const { all, stored } = columnKeys(previews)
  return columnOrder([...all].filter(k => !stored.has(k)))
}

/** One cell's slots (spec §6.2): stored slots in order, the first `max` shown and a count of the rest; address-less slots apart. */
export function cellSlots(preview, column, max = 5) {
  const all = (preview?.slots ?? []).filter(s => s.column === column)
  const stored = all.filter(s => s.address)
  return { shown: stored.slice(0, max), more: Math.max(0, stored.length - max), empty: all.filter(s => !s.address) }
}

const COMPRESSED = new Set(['gz', 'bgz', 'bz2', 'xz', 'zst'])

/** A file name's suffix from its last '.', with the one before when that is a compression suffix; a name with no '.' after its first character is kept whole. */
function suffixOf(name) {
  const last = name.lastIndexOf('.')
  if (last <= 0 || last === name.length - 1) return name
  const prev = name.lastIndexOf('.', last - 1)
  return COMPRESSED.has(name.slice(last + 1)) && prev > 0 ? name.slice(prev) : name.slice(last)
}

/** The distinct file suffixes over every loaded item, sorted: what kinds of file an output holds. */
export function outputSuffixes(previews) {
  const out = new Set()
  for (const p of previews ?? []) for (const f of p?.files ?? []) out.add(suffixOf(f))
  return [...out].sort(byText)
}

/** `pairs` without the paths `itemTable` found to repeat another. */
export const withoutDuplicates = (pairs, duplicates) => (pairs ?? []).filter(e => !duplicates.has(e.path))
