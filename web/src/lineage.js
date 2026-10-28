// web/src/lineage.js
// Groups an item's inputs and their Origin reads for the Lineage panel: one group per upstream run,
// a line per output and Selection, a row per input position (lineage provenance tree spec §5.3, §5.6).
// No DOM, so it is tested alone.

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`

/** The key readOrigin's answers are cached under: `${item_cid} ${collection_cid} ${selection_cid}`. */
export const originKey = (o) => `${o.item_cid} ${o.collection_cid} ${o.selection_cid}`

/** The Read of an Origin nothing was read for. */
const unread = (o) => ({ itemCid: o.item_cid, collection: o.collection_cid, selection: o.selection_cid, holder: null, reader: null, title: null, label: null, names: [] })

/** The group an Origin's Read belongs to: its kind and key. */
function groupOf(read) {
  if (read.label?.completion) return { kind: 'run', key: `run:${read.label.completion}` }
  if (read.collection) return { kind: 'collection', key: `collection:${read.collection}` }
  return { kind: 'query', key: `query:${read.selection ?? '-'}` }
}

const newGroup = (kind, key, read) => ({
  kind, key,
  completion: kind === 'run' ? read.label.completion : null,
  collection: kind === 'collection' ? read.collection : null, selection: null, names: null,
  label: kind === 'run' ? read.label : null,
  holder: read.holder, reader: read.reader, lines: [], lineByKey: new Map(),
  ...(kind === 'query' ? { selection: read.selection, names: read.names } : {}),
})

const newLine = (key, read) => ({ key, collection: read.collection, selection: read.selection, names: read.names, label: read.label, holder: read.holder, reader: read.reader, items: 0, rows: [], seen: new Set() })

/** Inputs and their Origin reads as run groups, lines and rows (lineage provenance tree spec §5.3, §5.6). */
export function groupLineage(inputs, reads, leaves = []) {
  const sorted = [...inputs].sort((a, b) => a.position - b.position)
  const groups = new Map()
  const loose = []
  for (const input of sorted) {
    if (!input.origins?.length) { loose.push(input); continue }
    for (const origin of input.origins) {
      const read = reads.get(originKey(origin)) ?? unread(origin)
      const { kind, key } = groupOf(read)
      if (!groups.has(key)) groups.set(key, newGroup(kind, key, read))
      const group = groups.get(key)
      const lineKey = `${read.collection ?? '-'}:${read.selection ?? '-'}`
      let line = group.lineByKey.get(lineKey)
      if (!line) {
        group.lines.push(line = newLine(lineKey, read))
        group.lineByKey.set(lineKey, line)
      }
      const rowKey = `${input.position} ${read.itemCid}`
      if (line.seen.has(rowKey)) continue
      line.seen.add(rowKey)
      line.rows.push({ position: input.position, input, leaf: null, read, also: [] })
    }
  }
  const ordered = [...groups.values()]
  const result = [...ordered.filter(g => g.kind === 'run'), ...ordered.filter(g => g.kind !== 'run')]
  const runGroups = result.filter(g => g.kind === 'run')
  const holding = new Map()
  for (const g of runGroups) {
    for (const line of g.lines) for (const row of line.rows) {
      if (!holding.has(row.position)) holding.set(row.position, new Set())
      holding.get(row.position).add(g)
    }
  }
  for (const g of result) {
    for (const line of g.lines) {
      line.rows.sort((a, b) => a.position - b.position || byText(a.read.itemCid, b.read.itemCid))
      line.items = new Set(line.rows.map(r => r.read.itemCid)).size
      for (const row of line.rows) {
        row.also = runGroups.filter(o => o !== g && holding.get(row.position)?.has(o))
          .map(o => ({ completion: o.completion, label: o.label, holder: o.holder }))
      }
      delete line.seen
    }
    delete g.lineByKey
  }
  const path = pathGroup(loose, leaves)
  if (path) result.push(path)
  return { groups: result, runs: runGroups.length, inputs: new Set(inputs.map(i => i.position)).size }
}

/** The group of Origin-less inputs and never-published leaves, or null when there are none. A leaf named like an Origin-less input is that input's row. */
function pathGroup(loose, leaves) {
  const row = (position, input, leaf) => ({ position, input, leaf, read: null, also: [] })
  const taken = new Set()
  const rows = loose.map(input => {
    const i = leaves.findIndex((n, j) => n === input.name && !taken.has(j))
    if (i >= 0) taken.add(i)
    return row(input.position, input, i >= 0 ? leaves[i] : null)
  })
  const rest = leaves.filter((_, i) => !taken.has(i)).map(name => row(null, null, name))
  const all = [...rows, ...rest]
  if (!all.length) return null
  const line = { key: 'path', collection: null, selection: null, names: null, label: null, holder: null, reader: null, items: 0, rows: all }
  return { kind: 'path', key: 'path', completion: null, collection: null, selection: null, names: null, label: null, holder: null, reader: null, lines: [line] }
}

/** The panel's or a tree's meta: `<R> upstream run(s) · <N> input(s)`, `<N> input(s)` when R is 0, null when N is 0. */
export function countsText(runs, inputs) {
  if (!inputs) return null
  return runs ? `${plural(runs, 'upstream run')} · ${plural(inputs, 'input')}` : plural(inputs, 'input')
}

/** A line's count: `<k> of <m> item(s)` when m is a positive number, else `<k> item(s)`. Never "of 0". */
export function itemsText(k, m) {
  return m > 0 ? `${k} of ${plural(m, 'item')}` : plural(k, 'item')
}
