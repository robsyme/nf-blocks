// web/src/views/item.js
// #/item/<collection>/<item> (mockups/05-item.html): the item by its id, its
// fields as chips, its files grouped by the record's own keys, where it came
// from and what keeping it costs. Also the preview drawing the file page shares.
// Its size, in the Files header and on Pin, is what is stored here (item_size,
// when the snapshot has storage figures): a swept item says "2 files · 5.55 KB stored here of
// 5.27 MB", the files being those stored here (a directory counts its files)
// and the Leaves' sum what the record names.
import { h, link } from '../html.js'
import { ref, sealed, shortCid } from '../icons.js'
import { runTone, statusText } from '../words.js'
import { addrChip, ago, button, claimTag, durability, fieldChip, kv, pageHead, panel, runFact, status, tree } from '../components.js'
import { downloadElsewhere, presenceOf } from '../presence.js'
import { bytes, formatNumber, formatValue, sizeText } from '../format.js'
import { fold } from '../folds.js'
import { PREVIEW_BYTES, isPreviewable, kindOf, previewOf } from '../filepreview.js'
import { labelPaths, labelText, pairsNode, pairsOf } from '../pairs.js'
import { matchPromoted, promotedFor } from '../promoted.js'
import { seal } from '../seal.js'
import { fileSlots } from '../slots.js'
import { itemTable, withoutDuplicates } from '../table.js'
import { leafReasonText } from '../words.js'
import { readHead } from './block.js'
import { holds, keepPanel, pickButton } from './common.js'
import { projectControl } from './addproject.js'
import { fromEach, hrefIn } from '../members.js'
import { countsText, groupLineage, itemsText, originKey } from '../lineage.js'

const SHOWN_PAIRS = 8
// A fold that would hide fewer pills than this costs more than it saves: show them all.
const MIN_FOLDED = 5
// An HTML report is opened whole from a local copy; past this size it is download only.
const HTML_CAP = 32_000_000
const enc = encodeURIComponent
const NUMBER = /^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/

/** The member the page reads, as ctx names it (tests and older callers may give none). */
const storeOf = (ctx) => ctx.store ?? { name: 'this store', key: 'a' }

/** A run's name as a link, with its status icon and word; the run is a label from Explorer.runLabel. */
export function runStatus(label, completion = label?.completion) {
  const name = label?.run_name ?? 'unnamed run'
  const tone = runTone(label)
  const word = tone === 'bad' ? h('span', { class: 'muted' }, ` ${statusText(label).toLowerCase()}`) : null
  const node = h('span', { class: 'ref', 'data-run-status': label?.status ?? '' },
    sealed('run', completion, { tone }), completion ? link(`#/run/${completion}`, name) : name, word)
  node.setAttribute('title', `${name}: ${statusText(label ?? {})}`)
  return node
}

/** A run as a chip (runFact) and how long ago it finished, the full time on hover. */
function runChip(label, completion, { extra = null, now = Date.now() } = {}) {
  return h('span', { class: 'row', style: 'gap:8px' }, runFact(label, completion, extra), ago(label?.finished_at, now, { class: 'muted small' }))
}

/**
 * A run by its name: the short CID in mono until Explorer.runLabel answers,
 * then the name with its status. `build(label)` makes the node for a label.
 */
export function runName(ex, collectionCid, completion, build) {
  const node = h('span', { class: 'runname', title: completion }, ref('run', h('code', { class: 'cid' }, shortCid(completion)), `#/run/${completion}`, { cid: completion }))
  ex.runLabel(collectionCid).then((label) => { if (label) node.replaceChildren(build(label)) }, () => {})
  return node
}

// ---- preview -------------------------------------------------------------

/** A table preview; the columns whose header is in `highlight` (sample ids from the Samples tab) carry class hl. */
function tableNode(p, highlight = new Set()) {
  const numeric = p.head.map((_, c) => p.rows.length > 0 && p.rows.every(r => r[c] === undefined || r[c] === '' || NUMBER.test(r[c])))
  const cls = (i, base) => [base, highlight.has(p.head[i]) ? 'hl' : null].filter(Boolean).join(' ') || null
  return h('div', { class: 'tbox preview-box' }, h('table', { class: 't', 'data-preview': 'table' },
    h('thead', {}, h('tr', {}, p.head.map((c, i) => h('th', { class: cls(i, numeric[i] ? 'r' : null) }, c)))),
    h('tbody', {}, p.rows.map(r => h('tr', {}, p.head.map((_, i) => numeric[i]
      ? h('td', { class: cls(i, 'r num') }, r[i] === undefined || r[i] === '' ? '' : formatNumber(r[i]))
      : h('td', { class: cls(i, 'mono') }, r[i] ?? '')))))))
}

function textNode(p) {
  return h('pre', { class: 'preview-lines', 'data-preview': 'text' }, p.lines.map(l => h('code', {}, l)))
}

/** An HTML report: nothing is loaded until the click, then a frame with no script rights, from a local copy of the bytes. */
function htmlNode(ex, { address: cid, name, size }) {
  const slot = h('div', { class: 'preview-html', 'data-preview': 'html' })
  if (size !== null && size !== undefined && size > HTML_CAP) {
    slot.append(h('p', { class: 'muted pad' }, `${name} is ${bytes(size)}, too large to open here. Download it to read it.`))
    return slot
  }
  const open = button('Open preview', { size: 'sm', onclick: async (e) => {
    const trigger = e.currentTarget
    trigger.disabled = true
    trigger.textContent = 'Opening...'
    try {
      const data = await ex.blocks.raw(cid)
      const url = URL.createObjectURL(new Blob([data], { type: 'text/html' }))
      const frame = h('iframe', { sandbox: '', src: url, title: `Preview of ${name}`, class: 'preview-frame', referrerpolicy: 'no-referrer' })
      // The frame holds its own copy once loaded: free the Blob.
      frame.addEventListener('load', () => URL.revokeObjectURL(url))
      slot.replaceChildren(frame)
    } catch (err) {
      trigger.disabled = false
      trigger.textContent = 'Open preview'
      slot.append(h('p', { class: 'warn' }, `Could not read ${name}: ${err?.message ?? err}`))
    }
  } })
  slot.append(h('p', { class: 'muted pad' }, 'This page runs no scripts, so some charts may not draw. ', open))
  return slot
}

/**
 * A file's preview, loaded when called: `{ node, meta }`. `file` is
 * `{ address, name, size, highlight?, missing?, error? }`; `size` may be
 * null, a missing block reads as a note, and `error` (a read that failed for
 * another reason) says the file could not be read. A table marks the columns
 * named in `highlight`.
 */
export async function previewBody(ex, file) {
  const ext = previewOf(file.name, new Uint8Array(0))
  if (file.missing) return { node: h('p', { class: 'warn pad' }, 'This file is not in this store.'), meta: '' }
  if (file.error) return { node: unreadNode(file.error), meta: '' }
  if (ext.kind === 'binary') return { node: h('p', { class: 'muted pad', 'data-preview': 'binary' }, `No preview for ${kindOf(file.name)} files${file.size == null ? '' : ` (${bytes(file.size)})`}.`), meta: '' }
  if (ext.kind === 'html') return { node: htmlNode(ex, file), meta: 'HTML' }
  let head = file.head
  try { head ??= await readHead(ex, file.address, PREVIEW_BYTES) } catch (e) {
    return { node: e?.code === 'block_missing' ? h('p', { class: 'warn pad' }, 'This file is not in this store.') : unreadNode(e), meta: '' }
  }
  if (!head.bytes) return { node: h('p', { class: 'muted pad' }, 'This server cannot send part of a file, so there is no preview.'), meta: '' }
  const p = previewOf(file.name, head.bytes)
  const more = head.truncated ? ' (first 64 KB)' : ''
  if (p.kind === 'table') {
    const shown = p.rows.length
    const total = `${formatNumber(p.totalRows)}${p.truncated ? '+' : ''}`
    const highlight = new Set(file.highlight ?? [])
    const lit = p.head.filter(c => highlight.has(c)).length
    const marked = lit ? ` · ${lit} sample ${lit === 1 ? 'column' : 'columns'} highlighted` : ''
    return { node: tableNode(p, highlight), meta: `rows 1-${formatNumber(shown)} of ${total} · ${formatNumber(p.totalCols)} columns${more}${marked}`, table: p }
  }
  if (p.kind === 'text') return { node: textNode(p), meta: `${formatNumber(p.lines.length)} lines${more}` }
  return { node: h('p', { class: 'muted pad', 'data-preview': 'binary' }, `This looks like binary data${file.size == null ? '' : `, ${bytes(file.size)}`}.`), meta: '' }
}

const unreadNode = (e) => h('p', { class: 'warn pad', 'data-read-error': e?.code ?? '' }, `Could not read the file: ${e?.message ?? e}`)

/** A panel for one file's preview, with `onClose` when given (the item page's sheet). */
export function previewPanel(ex, file, { onClose = null } = {}) {
  const body = h('div', {}, h('p', { class: 'muted pad' }, 'Loading...'))
  const meta = h('span', { class: 'muted' }, '')
  const box = h('section', { class: 'panel flush', 'data-preview-of': file.address },
    h('div', { class: 'ph' }, h('h2', {}, file.title ?? 'Preview'), meta, h('span', { class: 'spacer' }),
      onClose ? button('Close', { size: 'sm', kind: 'ghost', onclick: onClose }) : null),
    h('div', { class: 'pb' }, body))
  previewBody(ex, file).then(({ node, meta: text }) => { body.replaceChildren(node); meta.textContent = text },
    (e) => body.replaceChildren(h('p', { class: 'warn pad' }, String(e?.message ?? e))))
  return box
}

// ---- item page -----------------------------------------------------------

/** The leaves as table rows with their group: from the record's own slots when the value is there, else by leaf. */
function fileRows(it) {
  const slots = it.value ? fileSlots(it.value) : []
  if (slots.length) {
    return slots.map(s => ({ group: s.column, sub: s.path.slice(s.column.length).replace(/^\./, ''),
      name: s.name, size: s.size, address: s.address, dir: s.dir, reason: s.reason }))
  }
  return it.leaves.map(l => ({ group: null, sub: '', name: l.name ?? null, size: l.size ?? null, address: l.address ? String(l.address) : null,
    dir: false, reason: l.reason }))
}

/** Groups by total bytes, largest first, ties by key A-Z; files keep their slot order (the block's map keys are sorted, so record order is not stored). */
export function orderRows(rows) {
  const totals = new Map()
  for (const r of rows) totals.set(r.group, (totals.get(r.group) ?? 0) + (r.size ?? 0))
  const keys = [...totals.keys()].sort((a, b) => (totals.get(b) - totals.get(a)) || (String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0))
  return keys.flatMap(k => rows.filter(r => r.group === k))
}

const sumOf = (rows) => rows.reduce((n, r) => n + (r.size ?? 0), 0)

export { keepPanel }

/** The item's heading: its label fields, else its file names, else 'Item'. */
export function itemTitle(it) {
  const files = it.leaves.map(l => l.name).filter(Boolean)
  const all = pairsOf(it.view)
  const { duplicates } = itemTable([{ pairs: all, files }])
  const pairs = withoutDuplicates(all, duplicates)
  return labelText({ pairs, files }, labelPaths([{ pairs }], 1)) || 'Item'
}

/**
 * What the members say about one Origin: the upstream item read from the first
 * member that holds its block (`reader`, that member's Explorer), the run that
 * made it and the Selection's names.
 */
async function readOrigin(ex, ctx, pres, origin) {
  const { item_cid: itemCid, collection_cid: collection, selection_cid: selection } = origin
  const coll = collection ?? '-'
  const probe = await Promise.resolve(ex.item(coll, itemCid)).catch(() => null)
  const states = await pres.states(itemCid, { hereHeld: probe !== null }).catch(() => [])
  const holder = (states.find(s => s.here && s.state === 'held') ?? states.find(s => s.state === 'held'))?.alias ?? null
  const reader = !holder || holder === pres.open ? ex : await Promise.resolve(ctx.members?.explorer?.(holder)).catch(() => null) ?? ex
  const it = !holder ? null : reader === ex ? probe : await Promise.resolve(reader.item(coll, itemCid)).catch(() => null)
  const label = collection ? await Promise.resolve(reader.runLabel(collection)).catch(() => null) : null
  const names = selection ? (await Promise.resolve(reader.claimStates?.([selection])).catch(() => null))?.get(selection)?.names ?? [] : []
  return { itemCid, collection, selection, holder, reader, title: it ? itemTitle(it) : null, label, names }
}

/** ` via Selection "<names>"`, or its short CID when no Claim names it. */
const viaSelection = (selection, names) => h('span', { class: 'muted small' }, ' via Selection ',
  names.length ? `"${names.join(' / ')}"` : h('code', { class: 'cid', title: selection }, shortCid(selection)))

/** An item link's text: `<output> · <title>`, the short CID until the item reads. */
const itemLinkText = (label, title, itemCid) => `${label?.output ?? 'item'} \u00b7 ${title ?? shortCid(itemCid)}`

/** A hash opened in the member that holds it, or here when none does. */
const inHolder = (ctx, holder, hash) => (holder ? hrefIn(ctx, holder, hash) : hash)

/** `ask()` once per `key` in `answers`, a failure read as null. */
const askOnce = (answers, key, ask) => {
  if (!answers.has(key)) answers.set(key, Promise.resolve().then(ask).catch(() => null))
  return answers.get(key)
}

/** A Selection as a ref: `"<names>"` from its Claims, else its short CID. */
const selectionRef = (ctx, holder, selection, names) => ref('selection',
  names.length ? `"${names.join(' / ')}"` : h('code', { class: 'cid', title: selection }, shortCid(selection)),
  inHolder(ctx, holder, `#/selection/${selection}`), { cid: selection })

/**
 * One consumer run's Lineage tree, ending on `here`: `{ node, runs, inputs }`. Its inputs are
 * grouped by upstream run (lineage.js); each run's pipeline and each collection's item count
 * are asked of the member that holds it. `cache` holds the reads every tree on the page shares.
 * A null `run`, or one whose lineage cannot be drawn, draws the never-published `leaves` alone.
 */
async function lineageTree(ex, ctx, pres, run, { leaves, here, cache, now, itemCid }) {
  // The same (item, run, Selection) is read once, however many inputs and trees name it.
  const readOnce = (o) => {
    const key = originKey(o)
    if (!cache.origins.has(key)) cache.origins.set(key, readOrigin(ex, ctx, pres, o))
    return cache.origins.get(key)
  }
  let grouped
  try {
    const origins = run ? run.inputs.flatMap(i => i.origins ?? []) : []
    const reads = new Map(await Promise.all(origins.map(async o => [originKey(o), await readOnce(o)])))
    grouped = groupLineage(run?.inputs ?? [], reads, leaves)
  } catch {
    grouped = groupLineage([], new Map(), leaves)
  }
  const { groups, runs, inputs } = grouped
  const lines = groups.flatMap(g => g.lines)
  const addresses = [...new Set(lines.flatMap(l => l.rows.map(r => r.input?.address).filter(Boolean)))]
  const [identities, counts, held] = await Promise.all([
    Promise.all(groups.filter(g => g.kind === 'run').map(async g =>
      [g, await askOnce(cache.identities, `${g.holder} ${g.completion}`, () => g.reader?.runIdentity?.(g.completion))])),
    Promise.all(lines.filter(l => l.collection).map(async l =>
      [l, await askOnce(cache.counts, `${l.holder} ${l.collection}`, () => l.reader?.collectionItemCount?.(l.collection))])),
    Promise.all(addresses.map(async a =>
      [a, await askOnce(cache.held, a, async () => (await pres.states(a, { hereHeld: null })).some(s => s.state === 'held'))])),
  ]).then(answers => answers.map(pairs => new Map(pairs)))

  const head = (g) => {
    if (g.kind === 'run') {
      const pipeline = identities.get(g)?.pipeline
      const age = ago(g.label?.finished_at, now)
      return [runFact(g.label, g.completion, null, { href: inHolder(ctx, g.holder, `#/run/${g.completion}`) }),
        pipeline || age ? h('span', { class: 'muted small' }, pipeline, pipeline && age ? ' \u00b7 ' : null, age) : null]
    }
    if (g.kind === 'collection') return [h('code', { class: 'cid', title: g.collection }, shortCid(g.collection)), h('span', { class: 'muted small' }, 'run not in any store here')]
    if (g.kind === 'query') return [g.selection ? selectionRef(ctx, g.holder, g.selection, g.names) : null, h('span', { class: 'muted small' }, 'picked by query')]
    return h('span', { class: 'muted' }, 'Read by path')
  }
  // Query and path lines have no head: their group's head says it all.
  const sub = (g, line) => (g.kind === 'run' || g.kind === 'collection' ? [
    line.label?.output ? ref('output', line.label.output, inHolder(ctx, line.holder, `#/collection/${line.collection}`), { cid: line.collection }) : null,
    h('span', { class: 'muted small' }, `${itemsText(line.items, counts.get(line))}${line.selection ? ', via' : ''}`),
    line.selection ? selectionRef(ctx, line.holder, line.selection, line.names) : null] : null)
  // What no member can read is not named beyond its short CID.
  const itemCell = (read) => !read ? null : read.holder
    ? ref('item', read.title ?? shortCid(read.itemCid), inHolder(ctx, read.holder, `#/item/${read.collection ?? '-'}/${read.itemCid}`), { cid: read.itemCid })
    : [ref('item', h('code', { class: 'cid', title: read.itemCid }, shortCid(read.itemCid)), null, { cid: read.itemCid }), ' ',
      h('span', { class: 'muted small' }, 'not in any store here')]
  const fileCell = (input, leaf) => input?.address
    ? ref('file', input.name ?? shortCid(input.address), `#/content/${input.address}`, { cid: input.address })
    : input?.name ?? leaf ?? 'unnamed input'
  const tagCell = (also) => !also.length ? null : [h('span', { class: 'muted small' }, 'also'), also.length === 1
    ? h('a', { class: 'also', href: inHolder(ctx, also[0].holder, `#/run/${also[0].completion}`) }, seal(also[0].completion, { size: 12 }), also[0].label?.run_name ?? 'unnamed run')
    : h('span', { class: 'also', title: also.map(a => a.label?.run_name ?? 'unnamed run').join(', ') }, `${also.length} other runs`)]
  // Each row builds its own elements: the same Origin may sit under several positions.
  const row = ({ input, leaf, read, also }) => ({ key: input ? String(input.position) : 'leaf',
    cells: [itemCell(read), fileCell(input, leaf), tagCell(also), input?.size == null ? null : bytes(input.size)],
    ext: !input?.address || !held.get(input.address) })
  // A line folds as the metadata does.
  const treeLine = (g, line) => {
    const rows = line.rows.map(row)
    const folds = rows.length - SHOWN_PAIRS >= MIN_FOLDED
    return { key: line.key, head: sub(g, line), rows: folds ? rows.slice(0, SHOWN_PAIRS) : rows, folded: folds ? rows.slice(SHOWN_PAIRS) : [],
      foldKey: g.kind === 'path' ? `lineage:path:${itemCid}` : `lineage:${line.key}` }
  }
  return { node: tree(groups.map(g => ({ key: g.key, head: head(g), lines: g.lines.map(l => treeLine(g, l)) })), here), runs, inputs }
}

/**
 * The downstream items made from this one, from the open member then every other:
 * `[{ alias, row, title, label, names }]`, a derived item listed once (the open member's
 * copy wins). Over a run's collection only the consumers that read that collection
 * count; `-` keeps every row.
 */
async function usedByRows(ex, ctx, pres, itemCid, collectionCid) {
  const several = ctx.members?.list?.length > 1
  const answers = several
    ? (await fromEach(ctx.members, (e) => e.usedBy?.(itemCid) ?? [])).filter(a => 'value' in a).map(a => ({ alias: a.member.alias, reader: a.ex, rows: a.value }))
    : [{ alias: pres.open, reader: ex, rows: await Promise.resolve(ex.usedBy?.(itemCid)).catch(() => []) ?? [] }]
  answers.sort((a, b) => (b.alias === pres.open) - (a.alias === pres.open))
  const listed = new Map()
  for (const { alias, reader, rows } of answers) {
    for (const row of rows) {
      if (collectionCid !== '-' && row.via_cid !== collectionCid) continue
      const key = `${row.completion_cid} ${row.collection_cid} ${row.item_cid} ${row.selection_cid}`
      if (!listed.has(key)) listed.set(key, { alias, reader, row })
    }
  }
  const labels = new Map()
  const labelOf = (reader, coll) => {
    if (!labels.has(reader)) labels.set(reader, new Map())
    const mine = labels.get(reader)
    if (!mine.has(coll)) mine.set(coll, Promise.resolve(reader.runLabel(coll)).catch(() => null))
    return mine.get(coll)
  }
  return Promise.all([...listed.values()].map(async ({ alias, reader, row }) => {
    const [it, label, claims] = await Promise.all([
      Promise.resolve(reader.item(row.collection_cid, row.item_cid)).catch(() => null),
      labelOf(reader, row.collection_cid),
      row.selection_cid ? Promise.resolve(reader.claimStates?.([row.selection_cid])).catch(() => null) : null])
    return { alias, row, title: it ? itemTitle(it) : null, label, names: claims?.get(row.selection_cid)?.names ?? [] }
  }))
}

/** The Used by panel: derived items as links, grouped under the consumer run's chip. Null when nothing used the item. */
function usedByPanel(ctx, used) {
  if (!used.length) return null
  const byRun = new Map()
  for (const u of used) byRun.set(u.row.completion_cid, [...(byRun.get(u.row.completion_cid) ?? []), u])
  return panel({ title: 'Used by' }, [...byRun].map(([completion, list]) => h('div', { 'data-used-by-run': completion },
    // The first row's member: the open member's when it recorded this run.
    runFact(list.find(u => u.label)?.label ?? null, completion, null, { href: hrefIn(ctx, list[0].alias, `#/run/${completion}`) }),
    h('ul', {}, list.map(({ alias, row, title, label, names }) => h('li', {},
      link(hrefIn(ctx, alias, `#/item/${row.collection_cid}/${row.item_cid}`), itemLinkText(label, title, row.item_cid)),
      row.selection_cid ? viaSelection(row.selection_cid, names) : null))))))
}

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  if (from?.completion) {
    ctx.sources?.set(collectionCid, { completion: from.completion, output: from.output })
    ctx.use?.({ completion: from.completion, output: from.output, where: [], count: null })
    ctx.mark?.({ run: from.completion })
  }
  const identity = from?.completion ? await ex.runIdentity?.(from.completion)?.catch(() => null) : null
  const runRow = from?.completion ? await ex.runRow?.(from.completion)?.catch(() => null) : null
  const runState = from?.completion ? (await ex.claimStates?.([from.completion])?.catch(() => null))?.get(from.completion) : null
  const rows = orderRows(fileRows(it))
  // The index's figure when the snapshot has one (no walk), else the walk's, else the leaves' own sizes.
  const indexed = await Promise.resolve(ex.itemStoredSize?.(itemCid)).catch(() => null)
  const size = (indexed ? { ...indexed, files: indexed.blocks } : null)
    ?? await Promise.resolve(ex.itemSize?.(itemCid)).catch(() => null) ?? { bytes: sumOf(rows), files: rows.filter(r => r.address).length, partial: false }
  const files = it.leaves.map(l => l.name).filter(Boolean)
  const all = pairsOf(it.view)
  const { duplicates } = itemTable([{ pairs: all, files }])
  const pairs = withoutDuplicates(all, duplicates)
  const title = itemTitle(it)
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const store = storeOf(ctx)
  const pres = presenceOf(ctx)
  // The item's block was read here, so it is held here.
  const here = pres.dots(itemCid)
  // item_size is what is stored here (a sweep may have taken some files); the Leaves say what the record names.
  const recorded = sumOf(rows)
  // When fewer bytes are stored here than the record names, the page cannot tell which files went: the open member is asked too (D4).
  const fileHere = indexed && Number(indexed.bytes) < recorded ? null : true
  const itemHref = ctx.write.hrefFor(`#/item/${collectionCid}/${itemCid}`)

  const matched = matchPromoted(promotedFor(identity?.pipeline, { output: from?.output }), all.map(p => p.path))
  const chips = matched.map(({ field, path }) => {
    const e = all.find(p => p.path === path)
    return fieldChip(field.label, e.values.map((v, i) => formatValue(e.types[i], v, field)).join(', '), { title: path })
  })
  const fields = chips.length
    ? [h('div', { class: 'row', 'data-fields': '' }, chips),
      all.length > matched.length ? fold(`meta:${itemCid}`, `All ${all.length} fields`, pairsNode(all, target, { size: 'page' })) : null]
    : pairs.length ? h('div', {}, pairsNode(pairs.length - SHOWN_PAIRS < MIN_FOLDED ? pairs : pairs.slice(0, SHOWN_PAIRS), target, { size: 'page' }),
      pairs.length - SHOWN_PAIRS >= MIN_FOLDED ? fold(`meta:${itemCid}`, `+${pairs.length - SHOWN_PAIRS} more`, pairsNode(pairs.slice(SHOWN_PAIRS), target, { size: 'page' })) : null)
      : h('p', { class: 'muted' }, 'No Meta Map.')

  const crumbs = from?.completion
    ? [identity?.pipeline ? { text: identity.pipeline, href: `#/pipeline/${enc(identity.pipeline)}` } : null,
      { text: from.run_name ?? 'run', href: `#/run/${from.completion}` },
      { text: from.output, href: `#/items/${from.completion}/${enc(from.output)}?where=%5B%5D` }, { text: title }].filter(Boolean)
    : [{ text: title }]
  const releasedRun = runState?.released
  // Add to project (P4): its memberships from the item's own Claims (it.state), so the subline's tags cost no query.
  const projects = projectControl(ex, ctx, { subject: itemCid, kind: 'item', claims: it.state?.claims ?? null, href: `#/item/${collectionCid}/${itemCid}` })
  const head = pageHead({ crumbs, seal: itemCid, kind: 'item', title,
    after: [from?.output ? h('span', { class: 'muted', style: 'font-size:15px' }, `${from.output} item`) : null,
      it.state.pinned ? durability('pinned', 'Pinned') : from ? (releasedRun ? [durability('releasing', 'Run released'), claimTag({ assertedBy: runState.releaseClaim?.asserted_by, verb: 'released', at: runState.releaseClaim?.timestamp })] : durability('held', 'Held by run')) : null, here],
    actions: [projects.button, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })].filter(Boolean),
    subline: projects.tags ? [projects.tags] : [] })
  head.append(...[fields].flat().filter(Boolean))

  // The preview sheet sits under the files; one file at a time.
  const sheet = h('div', { class: 'preview-sheet', 'data-preview-sheet': '' })
  const showPreview = (r) => sheet.replaceChildren(previewPanel(ex, { address: r.address, name: r.name, size: r.size, title: `Preview: ${r.name}` },
    { onClose: () => sheet.replaceChildren() }))
  const grouped = new Set(rows.map(r => r.group)).size > 1
  let last = Symbol('none')
  const body = rows.flatMap((r) => {
    const heading = grouped && r.group !== last ? h('tr', { class: 'group' }, h('td', { colspan: '5' }, h('b', { style: 'color:var(--ink-2)' }, r.group))) : null
    last = r.group
    const stored = !!r.address
    const where = h('td', {}, stored ? pres.dots(r.address, { hereHeld: fileHere }) : null)
    const actions = h('td', { class: 'nowrap r' }, stored
      ? [!r.dir && isPreviewable(r.name) ? button('Preview', { size: 'sm', kind: 'ghost', 'data-preview-button': '', onclick: () => showPreview(r) }) : null,
        r.dir ? link(`#/block/${r.address}`, 'Directory') : h('a', { class: 'btn sm ghost', href: ex.blocks.urlFor(r.address), download: r.name ?? '' }, 'Download')]
      : h('span', { class: 'muted' }, leafReasonText(r.reason)))
    const row = h('tr', { 'data-file': r.address ?? '' },
      h('td', {}, h('div', { class: 'namecell' }, stored ? sealed(r.dir ? 'directory' : 'file', r.address, { size: 18 }) : null,
        stored ? h('a', { href: `#/content/${r.address}`, class: 'mono', style: 'font-weight:500;font-size:12px', title: 'Which runs produced this file' }, r.name ?? r.address)
          : h('span', { class: 'mono', style: 'font-size:12px' }, r.name ?? ''))),
      h('td', { class: 'mono muted', style: 'font-size:11.5px' }, r.sub),
      h('td', { class: 'r num nowrap' }, r.size === null || r.size === undefined ? '' : bytes(r.size)),
      where, actions)
    if (stored && fileHere === null) {
      pres.states(r.address, { hereHeld: null }).then((states) => {
        if (states.find(s => s.here)?.state !== 'not') return
        row.dataset.missing = ''
        where.append(durability('missing', 'Missing here'))
        actions.replaceChildren(r.dir ? link(`#/block/${r.address}`, 'Directory') : downloadElsewhere(pres, r.address, { size: 'sm', kind: 'ghost' }))
      })
    }
    return [heading, row]
  })
  const named = rows.filter(r => r.address).length
  const filesMeta = indexed && Number(indexed.bytes) < recorded
    ? `${size.files} files · ${sizeText(size)} stored here of ${bytes(recorded)}`
    : `${size.files ?? named} files · ${sizeText(size)}`
  const filesPanel = panel({ title: 'Files', meta: filesMeta, flush: true },
    h('div', { class: 'tbox' }, h('table', { class: 't', id: 'files' },
      h('thead', {}, h('tr', {}, h('th', {}, 'File'), h('th', {}, 'Field'), h('th', { class: 'r' }, 'Size'), h('th', {}, 'Where'), h('th', {}))),
      h('tbody', {}, body))))

  const leaves = it.leaves.filter(l => l.reason === 'never_published' && l.name).map(l => l.name)
  // Started with the Lineage reads, awaited after them.
  const usedByRead = usedByRows(ex, ctx, pres, itemCid, collectionCid)
  // Lineage is one run's fact: the URL's run, or every run that recorded one when the URL says `-`.
  const lineageRuns = collectionCid === '-' || from?.completion
    ? await Promise.resolve(ex.itemLineage?.(itemCid, collectionCid === '-' ? null : from.completion)).catch(() => []) ?? []
    : []
  const now = ex.now?.() ?? Date.now()
  const cache = { origins: new Map(), identities: new Map(), counts: new Map(), held: new Map() }
  // The item ending a tree, made by the URL's run, or in the `-` view by the tree's own run; fresh for each tree.
  const hereNode = async (run) => {
    const [label, completion] = from?.completion ? [from, from.completion]
      : run ? [await Promise.resolve(ex.runIdentity?.(run.completion_cid)).catch(() => null), run.completion_cid] : [null, null]
    return [ref('item', h('b', {}, `${from?.output ?? 'item'} \u00b7 ${title}`), null, { cid: itemCid }),
      completion ? [h('span', { class: 'muted small' }, 'made by'), runFact(label, completion)] : []]
  }
  const treeOf = async (run) => lineageTree(ex, ctx, pres, run, { leaves, here: await hereNode(run), cache, now, itemCid })
  let lineage
  if (lineageRuns.length < 2) {
    const { node, runs, inputs } = await treeOf(lineageRuns[0] ?? null)
    lineage = panel({ title: 'Lineage', meta: countsText(runs, inputs) }, node)
  } else {
    const trees = await Promise.all(lineageRuns.map(async (run) => {
      const { node, runs, inputs } = await treeOf(run)
      const counts = countsText(runs, inputs)
      return h('div', { 'data-lineage-run': run.completion_cid }, counts ? h('p', { class: 'muted small', 'data-lineage-counts': '' }, counts) : null, node)
    }))
    lineage = panel({ title: 'Lineage', meta: `${lineageRuns.length} runs recorded lineage` }, h('div', {}, trees))
  }

  const usedBy = usedByPanel(ctx, await usedByRead)

  const runs = new Map()
  // How many of this item's stored files each run produced.
  const made = new Map()
  for (const [leaf, list] of producers) {
    for (const p of list) {
      runs.set(p.completion_cid, p.collection_cid)
      made.set(p.completion_cid, (made.get(p.completion_cid) ?? new Set()).add(String(leaf.address)))
    }
  }
  const storedFiles = new Set(producers.map(([leaf]) => String(leaf.address))).size
  const elsewhere = [...runs].filter(([completion]) => completion !== from?.completion)
  const allOf = elsewhere.every(([completion]) => made.get(completion).size === storedFiles)
  const producedBy = panel({ title: 'Produced by' },
    from ? h('div', { class: 'row' }, runChip({ ...from, finished_at: runRow?.finished_at ?? from.finished_at }, from.completion, { now }))
      : runs.size ? h('div', { class: 'row' }, [...runs].map(([completion, coll]) => runName(ex, coll, completion, (l) => runChip(l, completion, { now }))))
        : h('p', { class: 'muted' }, 'No run in this member produced it.'),
    collectionCid === '-' ? h('p', { class: 'muted small' }, 'Picked by a query across runs.') : null,
    elsewhere.length ? [h('p', { class: 'muted small' }, allOf ? `The same ${storedFiles} ${storedFiles === 1 ? 'file was' : 'files were'} also produced by`
      : 'Some of these files were also produced by'),
      h('div', { class: 'row' }, elsewhere.map(([completion, coll]) => runName(ex, coll, completion,
        (l) => runChip(l, completion, { now, extra: allOf ? null : h('span', { class: 'muted' }, ` · ${made.get(completion).size} of ${storedFiles}`) })))),
      h('p', { class: 'muted small' }, 'Stored once. Content addressing keeps one copy for every run.')] : null)

  // Each Selection by its name (a Claim: this main pane may read them), its short CID beside it.
  const selectionStates = holding.length ? await Promise.resolve(ex.claimStates?.(holding)).catch(() => null) : null
  const inSelections = holding.length ? panel({ title: 'In Selections' }, h('ul', {}, holding.map((s) => {
    const names = selectionStates?.get(s)?.names ?? []
    return h('li', {}, link(`#/selection/${s}`, names.length ? names.join(' / ') : 'Unnamed Selection'), ' ', h('code', { class: 'cid', title: s }, shortCid(s)))
  }))) : null

  return h('section', {},
    head, projects.node, filesPanel, sheet,
    lineage,
    h('div', { class: 'grid-2' }, producedBy, usedBy),
    inSelections,
    keepPanel(itemCid, it.state, ctx, { size, kind: 'item', href: itemHref, ...holds(ex, itemCid),
      intro: from?.run_name ? ['It stays while ', h('b', {}, from.run_name), ' is held. Pinning keeps it in ', h('b', {}, store.name), ' even if the run is released.']
        : ['Pinning keeps it in ', h('b', {}, store.name), ' even if the run that wrote it is released.'] }),
    fold(`details:${itemCid}`, 'Address',
      kv([['Item', addrChip(itemCid)],
        ...(collectionCid !== '-' ? [['In output', addrChip(collectionCid, { ref: `cas://${collectionCid}/${itemCid}` })]] : []),
        ...it.leaves.filter(l => l.address).map(l => [l.name ?? 'file', addrChip(String(l.address))])]),
      all.length ? pairsNode(all, target, { size: 'row' }) : null))
}
