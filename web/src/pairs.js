// A Meta Map drawn as `key | value` pills, and the label an item row leads
// with (DESIGN.md §16 milestone 3 decision 9, tickets 03 and 10). Pairs are the index's
// own item_attr rows (metadata.js attrRows), so a pill's filter link carries
// the type query 3 matches.
import { h } from './html.js'
import { attrRows } from './metadata.js'

const enc = encodeURIComponent
const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const DOT = ' · '

/** The view's item_attr rows grouped by path, sorted by path; truncated values are left out. */
export function pairsOf(view) {
  const out = new Map()
  for (const r of attrRows(view ?? null)) {
    if (r.truncated) continue
    const e = out.get(r.path) ?? { path: r.path, type: r.type, values: [], types: [] }
    e.values.push(r.value)
    e.types.push(r.type)
    if (e.type !== r.type) e.type = 'mixed'
    out.set(r.path, e)
  }
  return [...out.values()].sort((a, b) => byText(a.path, b.path))
}

/**
 * Up to `n` label paths over the previews loaded so far (ticket 10 Q3):
 * a path that is a list in any of them is skipped; string paths come before
 * the rest, then the most distinct values, ties by path.
 */
export function labelPaths(previews, n = 3) {
  const stats = new Map()
  for (const p of previews) {
    for (const e of p.pairs ?? []) {
      const s = stats.get(e.path) ?? { path: e.path, list: false, string: true, distinct: new Set() }
      if (e.values.length > 1) s.list = true
      if (e.type !== 'string') s.string = false
      s.distinct.add(JSON.stringify([e.type, e.values]))
      stats.set(e.path, s)
    }
  }
  return [...stats.values()].filter(s => !s.list)
    .sort((a, b) => (Number(b.string) - Number(a.string)) || (b.distinct.size - a.distinct.size) || byText(a.path, b.path))
    .slice(0, n).map(s => s.path)
}

/** The row's label: the preview's values at `paths`, or its file names when it has none of them. */
export function labelText(preview, paths) {
  const byPath = new Map((preview?.pairs ?? []).map(e => [e.path, e]))
  const values = paths.map(p => byPath.get(p)).filter(Boolean).map(e => e.values.map(v => v ?? 'null').join(', '))
  return values.length ? values.join(DOT) : (preview?.files ?? []).join(', ')
}

const NUMERIC = /^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/

/** A value as a pill shows it: a string that reads as a number is quoted, since the where form is typed (ticket 10 Q4). */
export const valueText = (type, value) => (type === 'null' ? 'null' : type === 'string' && NUMERIC.test(value) ? `"${value}"` : value)

const valueClass = (type, value) => (type === 'int' || type === 'float' ? 'pv-num'
  : type === 'bool' ? `pv-${value}` : type === 'null' ? 'pv-null' : 'pv-str')

/**
 * Query 3 over `target`'s run and output with one more condition (ticket 10
 * Q2): the pair is added to `target.where` unless it is already there. Null
 * when the run or the output is not known, so the pill is not a link.
 */
export function filterHref(target, path, type, value) {
  if (!target?.completion || !target.output) return null
  const kept = (target.where ?? []).filter(([p, t, v]) => !(p === path && t === type && v === value))
  const where = [...kept, [path, type, value]]
  return `#/items/${target.completion}/${enc(target.output)}?where=${enc(JSON.stringify(where))}`
}

/** What pairsNode draws, without a DOM. */
export function pillsOf(pairs, target) {
  return pairs.map(e => ({
    path: e.path,
    key: e.path.split('.').at(-1),
    values: e.values.map((value, i) => {
      const type = e.types[i]
      const text = valueText(type, value)
      const href = filterHref(target, e.path, type, value)
      return { text, type, cls: valueClass(type, value), href,
        title: href ? `list the items of ${target.output} in this run where ${e.path} is ${text}` : `${e.path} is ${text}` }
    }),
  }))
}

/** The pills, or null when there are no pairs (an item with no Meta Map shows none, not an empty box). */
export function pairsNode(pairs, target, { size = 'row' } = {}) {
  if (!pairs?.length) return null
  return h('span', { class: `pills pills-${size}` }, pillsOf(pairs, target).map(p => h('span', { class: 'pill', title: p.path },
    h('span', { class: 'pill-key' }, p.key),
    p.values.map(v => (v.href
      ? h('a', { class: `pill-val ${v.cls}`, href: v.href, title: v.title }, v.text)
      : h('span', { class: `pill-val ${v.cls}`, title: v.title }, v.text))))))
}

export const PAIRS_CSS = `
  .pills { display: inline-flex; flex-wrap: wrap; gap: 4px; }
  .pill { display: inline-flex; max-width: 100%; border: 1px solid var(--line); border-radius: 999px; overflow: hidden; font-size: 12px; }
  .pill-key { background: var(--pill-key); color: var(--muted); padding: 0 6px; }
  .pill-val { padding: 0 6px; border-left: 1px solid var(--line); color: inherit; text-decoration: none;
    max-width: 24rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  a.pill-val:hover { text-decoration: underline; }
  .pv-num { color: var(--num); font-variant-numeric: tabular-nums; } .pv-true { color: var(--true); }
  .pv-false { color: var(--false); } .pv-null { color: var(--muted); font-style: italic; }
  .pills-page .pill { font-size: 14px; } .pills-page .pill-key, .pills-page .pill-val { padding: 1px 8px; }
`
