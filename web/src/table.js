// web/src/table.js
// The items table's shape (explorer layout B spec §5.4), decided from the
// previews loaded so far: which Meta Map paths are columns, which collapse into
// the "same for every item" line, which repeat another path's values, and each
// item's file suffixes. Pure: the views draw what it decides.

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

/** `pairs` without the paths `itemTable` found to repeat another. */
export const withoutDuplicates = (pairs, duplicates) => (pairs ?? []).filter(e => !duplicates.has(e.path))

/**
 * An item's files as chips (P9): with two or more, the prefix they share up to
 * its last '.' is dropped, each chip keeping its leading '.'; one file, or no
 * shared prefix with a '.', keeps the whole names.
 */
export function fileChips(files) {
  if (!files?.length) return { prefix: '', chips: [] }
  if (files.length === 1) return { prefix: '', chips: [files[0]] }
  let common = files[0]
  for (const f of files.slice(1)) {
    let i = 0
    while (i < common.length && i < f.length && common[i] === f[i]) i++
    common = common.slice(0, i)
  }
  const cut = common.lastIndexOf('.')
  if (cut <= 0) return { prefix: '', chips: [...files] }
  return { prefix: common.slice(0, cut), chips: files.map(f => f.slice(cut)) }
}
