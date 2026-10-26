// web/src/views/item.js
// #/item/<collection>/<item> (mockups/05-item.html): the item by its id, its
// fields as chips, its files grouped by the record's own keys, where it came
// from and what keeping it costs. Also the preview drawing the file page shares.
// Its size, in the Files header and on Pin, is what is stored here (item_size,
// when the snapshot has storage figures): a swept item says "2 files · 5.55 KB stored here of
// 5.27 MB", the files being those stored here (a directory counts its files)
// and the Leaves' sum what the record names.
import { h, link } from '../html.js'
import { shortCid } from '../icons.js'
import { addrChip, age, button, claimTag, durability, factTag, fieldChip, kv, pageHead, panel, status, thread } from '../components.js'
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
  const node = status(label ?? {})
  node.replaceChildren(node.children[0], completion ? link(`#/run/${completion}`, name) : name)
  node.setAttribute('title', `${name}: ${status(label ?? {}).textContent}`)
  return node
}

/**
 * A run by its name: the short CID in mono until Explorer.runLabel answers,
 * then the name with its status. `build(label)` makes the node for a label.
 */
export function runName(ex, collectionCid, completion, build) {
  const node = h('span', { class: 'runname', title: completion }, link(`#/run/${completion}`, h('code', { class: 'cid' }, shortCid(completion))))
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
  const title = labelText({ pairs, files }, labelPaths([{ pairs }], 1)) || 'Item'
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
  const head = pageHead({ crumbs, seal: itemCid, title,
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
      h('td', {}, h('div', { class: 'namecell' }, stored ? seal(r.address, { size: 18 }) : null,
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

  const inputs = it.leaves.filter(l => l.reason === 'never_published' && l.name).map(l => l.name)
  const lineage = panel({ title: 'Lineage' }, thread([
    ...(inputs.length ? [{ text: `${inputs.join(', ')} · input, referenced by path`, ext: true }] : []),
    { text: `${from?.output ?? 'item'} · ${title}`, here: true }]))

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
    from ? h('div', { class: 'row' }, runStatus(from), runRow?.finished_at ? h('span', { class: 'muted' }, age(runRow.finished_at)) : null)
      : runs.size ? h('div', { class: 'row' }, [...runs].map(([completion, coll]) => runName(ex, coll, completion, (l) => runStatus(l, completion))))
        : h('p', { class: 'muted' }, 'No run in this member produced it.'),
    collectionCid === '-' ? h('p', { class: 'muted small' }, 'Picked by a query across runs.') : null,
    elsewhere.length ? [h('p', { class: 'muted small' }, allOf ? `The same ${storedFiles} ${storedFiles === 1 ? 'file was' : 'files were'} also produced by`
      : 'Some of these files were also produced by'),
      h('div', { class: 'row' }, elsewhere.map(([completion, coll]) => runName(ex, coll, completion,
        (l) => factTag('run', [l.run_name ?? 'unnamed run', l.status && l.status !== 'succeeded' ? h('span', { class: 'muted' }, ` ${l.status}`) : null,
          allOf ? null : h('span', { class: 'muted' }, ` · ${made.get(completion).size} of ${storedFiles}`)], completion, `#/run/${completion}`)))),
      h('p', { class: 'muted small' }, 'Stored once. Content addressing keeps one copy for every run.')] : null)

  // Each Selection by its name (a Claim: this main pane may read them), its short CID beside it.
  const selectionStates = holding.length ? await Promise.resolve(ex.claimStates?.(holding)).catch(() => null) : null
  const inSelections = holding.length ? panel({ title: 'In Selections' }, h('ul', {}, holding.map((s) => {
    const names = selectionStates?.get(s)?.names ?? []
    return h('li', {}, link(`#/selection/${s}`, names.length ? names.join(' / ') : 'Unnamed Selection'), ' ', h('code', { class: 'cid', title: s }, shortCid(s)))
  }))) : null

  return h('section', {},
    head, projects.node, filesPanel, sheet,
    h('div', { class: 'grid-2' }, lineage, producedBy),
    inSelections,
    keepPanel(itemCid, it.state, ctx, { size, kind: 'item', href: itemHref, ...holds(ex, itemCid),
      intro: from?.run_name ? ['It stays while ', h('b', {}, from.run_name), ' is held. Pinning keeps it in ', h('b', {}, store.name), ' even if the run is released.']
        : ['Pinning keeps it in ', h('b', {}, store.name), ' even if the run that wrote it is released.'] }),
    fold(`details:${itemCid}`, 'Address',
      kv([['Item', addrChip(itemCid)],
        ...(collectionCid !== '-' ? [['In output', addrChip(collectionCid, { ref: `cas://${collectionCid}/${itemCid}`, seal: false })]] : []),
        ...it.leaves.filter(l => l.address).map(l => [l.name ?? 'file', addrChip(String(l.address))])]),
      all.length ? pairsNode(all, target, { size: 'row' }) : null))
}
