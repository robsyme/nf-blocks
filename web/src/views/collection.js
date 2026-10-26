// web/src/views/collection.js
// #/collection/<cid> and #/content/<cid> (DESIGN.md §15).
import { h, link } from '../html.js'
import { address, icon, shortCid } from '../icons.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { addrChip, durability, kindChip, kv, pageHead, panel } from '../components.js'
import { downloadElsewhere, presenceOf } from '../presence.js'
import { bytes, formatNumber, sizeText } from '../format.js'
import { kindOf, isPreviewable } from '../filepreview.js'
import { labelPaths, labelText, pairsOf } from '../pairs.js'
import { whenText } from '../words.js'
import { crumbs, holds, itemRows, keepPanel, pager, runCrumbs, retentionPanel, stableIndexControl } from './common.js'
import { previewBody, runStatus } from './item.js'
import { readHead } from './block.js'

/** The collection's Output Index File line (Task 4, DESIGN.md §15): the file
 * with addresses in place of its Publish Coordinates, or a note naming its
 * publish path when it was never written. */
function indexLine(ex, ctx, collectionCid, idx) {
  if (!idx) return null
  return idx.leaf.address
    ? h('p', {}, "Nextflow's index file for this output, each file named by its address: ",
        ...stableIndexControl(ex, ctx, collectionCid, idx.leaf.name))
    : h('p', { 'data-output-index-missing': idx.path },
        `Nextflow's index file for this output (${idx.path}) was never written.`)
}

/** Nextflow's own Output Index File, under Details: its paths are Publish Coordinates (DESIGN.md §7). */
function indexAsWritten(ex, idx) {
  if (!idx?.leaf.address) return null
  return h('p', { 'data-output-index': String(idx.leaf.address) },
    "Nextflow's index file as it wrote it: ",
    h('a', { href: ex.blocks.urlFor(String(idx.leaf.address)), download: idx.leaf.name }, idx.leaf.name),
    ` (published at ${idx.path}). Its cas://<store>/<path> names are where files were published; a later run to the same path replaces what they name.`)
}

/** `N items · 21.1 MB · bam×5 ...`, filled in when the collection's files have been sized (a large collection takes a moment). */
function sizeLine(ex, collectionCid) {
  const line = h('p', { class: 'muted', 'data-size-line': '' })
  // replaceChildren takes nodes and strings only: an array would be written out as text.
  const draw = (s, kinds) => line.replaceChildren(`${formatNumber(s.items)} items · ${sizeText(s)}`,
    ...(kinds?.length ? [' · ', ...kinds.slice(0, 4).flatMap(([ext, count]) => [kindChip(ext, count), ' '])] : []))
  Promise.resolve(ex.collectionSize?.(collectionCid)).then((s) => {
    if (!s) return
    draw(s, s.kinds)
    // An index-backed size comes without its chips; they follow from the walk.
    if (!s.kinds && ex.collectionKinds) ex.collectionKinds(collectionCid).then(kinds => draw(s, kinds), () => {})
  }, () => {})
  return line
}

export async function collection(ex, collectionCid, offset = 0, ctx) {
  const c = await ex.collection(collectionCid, { offset })
  if (c.completion) ctx.sources?.set(collectionCid, { completion: c.completion, output: c.output })
  if (c.completion) ctx.use?.({ completion: c.completion, output: c.output, where: [], count: c.total })
  return h('section', {},
    c.completion ? runCrumbs(ex, c.completion, [{ text: c.output }]) : crumbs({ text: c.output }),
    h('h1', {}, icon('output'), c.output),
    sizeLine(ex, collectionCid),
    fold(`storage:${collectionCid}`, 'Storage', retentionPanel(collectionCid, c.state, ctx, { kind: 'collection', href: ctx.write.hrefFor(`#/collection/${collectionCid}`),
      size: ex.collectionSize ? Promise.resolve(ex.collectionSize(collectionCid)) : null, ...holds(ex, collectionCid) })),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    indexLine(ex, ctx, collectionCid, c.index),
    pager(`#/collection/${collectionCid}`, c, 'items'),
    c.total === 0 ? h('p', { class: 'muted' }, 'This collection has no items.')
      : itemRows(ex, ctx, { items: c.items, total: c.total, collectionCid, completion: c.completion, output: c.output, where: [],
        previews: new Previews(ex) }),
    fold(`details:${collectionCid}`, 'Details', h('p', {}, address(collectionCid)), indexAsWritten(ex, c.index)))
}

const LABELLED = 25

/** The most common filename among producer rows; the first seen wins a tie. */
function commonName(rows) {
  const counts = new Map()
  for (const r of rows) if (r.filename) counts.set(r.filename, (counts.get(r.filename) ?? 0) + 1)
  let best = null
  for (const [name, n] of counts) if (best === null || n > best[1]) best = [name, n]
  return best?.[0] ?? null
}

/**
 * `output · item` for a producer row: the short item CID until the output's
 * name (its OutputCollection block) and, for the first rows, the item's own
 * label (its OutputItem block) arrive. Block reads only (Gate assertion 2).
 */
function itemCell(ex, p, withLabel) {
  const href = `#/item/${p.collection_cid}/${p.item_cid}`
  const node = h('span', {}, h('a', { href, class: 'mono', title: p.item_cid }, shortCid(p.item_cid)))
  Promise.resolve(ex.collectionLabel(p.collection_cid)).then(async (label) => {
    let name = shortCid(p.item_cid)
    let mono = true
    if (withLabel) {
      try {
        const it = await ex.itemBlock(p.collection_cid, p.item_cid)
        const pairs = pairsOf(it.view)
        const text = labelText({ pairs, files: [] }, labelPaths([{ pairs }], 1))
        if (text) { name = text; mono = false }
      } catch { /* keep the short CID */ }
    }
    node.replaceChildren(`${label.output} · `, h('a', { href, class: mono ? 'mono' : null, title: p.item_cid }, name))
  }, () => {})
  return node
}

/** The size of this content from the Leaf that names it in a producing item (the first few are tried, from their blocks); null when none says. */
async function leafSize(ex, rows, contentCid) {
  for (const p of rows.slice(0, 3)) {
    try {
      const it = await ex.itemBlock(p.collection_cid, p.item_cid)
      const leaf = it.leaves.find(l => l.address && String(l.address) === contentCid)
      if (leaf && leaf.size !== null && leaf.size !== undefined) return Number(leaf.size)
    } catch { /* try the next producer */ }
  }
  return null
}

const good = (id) => id?.status === 'succeeded' && !id.possibly_incomplete

/**
 * Producer rows with their runs read from blocks (Explorer.runIdentity: name,
 * status and finished_at from the RunManifest and RunCompletion), in the order
 * the page lists them: good runs first, then newest first; a run whose blocks
 * cannot be read goes last. Returns `[{ p, id }]`, `id` null when unread.
 */
async function producerRuns(ex, rows) {
  const ids = new Map()
  await Promise.all([...new Set(rows.map(r => r.completion_cid))].map(async (c) => {
    ids.set(c, await Promise.resolve(ex.runIdentity(c)).catch(() => null))
  }))
  const when = (id) => id?.finished_at ?? ''
  return rows.map((p, i) => ({ p, id: ids.get(p.completion_cid), i }))
    .sort((a, b) => (Number(!a.id) - Number(!b.id)) || (Number(good(b.id)) - Number(good(a.id)))
      || (when(a.id) > when(b.id) ? -1 : when(a.id) < when(b.id) ? 1 : a.i - b.i))
}

/**
 * #/content/<cid>: which runs wrote these bytes, a preview and a pin. The
 * snapshot is read twice, for the producers (query 1) and this content's own
 * Claims; every name, date and size after that is a block read, so the route
 * stays inside Gate assertion 2's point budget. A table preview marks the
 * columns named in `highlight`, sample ids from the Samples tab's merged note.
 */
export async function content(ex, contentCid, ctx, { highlight = [] } = {}) {
  const rows = await ex.producersOf(contentCid, ctx.progress)
  // The content's own retain/pin state; there is no dedicated loader for a content page.
  const state = (await ex.claimStates([contentCid])).get(contentCid)
  const ordered = await producerRuns(ex, rows)
  const name = commonName(rows)
  const kind = name ? kindOf(name) : 'FILE'
  const wantsBytes = !!name && isPreviewable(name)
  let head = null
  let missing = false
  let unread = null
  // Only a previewable file is read, and once; any other size comes from the producing item's Leaf.
  // Missing here only when the store says the block is not there; any other failure is a read that failed.
  if (wantsBytes) {
    try { head = await readHead(ex, contentCid) } catch (e) { if (e?.code === 'block_missing') missing = true; else unread = e }
  }
  const size = head?.size ?? await leafSize(ex, ordered.map(o => o.p), contentCid)
  const first = ordered[0] ?? null
  const firstOutput = first ? await Promise.resolve(ex.collectionLabel(first.p.collection_cid)).then(l => l.output, () => null) : null
  const runs = new Set(rows.map(r => r.completion_cid))
  const preview = name ? await previewBody(ex, { address: contentCid, name, size, head: head ?? undefined, missing, error: unread, highlight }) : null
  const dims = preview?.table ? `${formatNumber(preview.table.totalRows)}${preview.table.truncated ? '+' : ''} rows × ${formatNumber(preview.table.totalCols)} columns` : null
  // A file missing here says so in place of what holds it, and its Download names the stores that do (mockup 05 spec).
  const holder = missing ? durability('missing', 'Missing here') : state.pinned ? durability('pinned', 'Pinned')
    : runs.size ? durability('held', `Held by ${runs.size} ${runs.size === 1 ? 'run' : 'runs'}`) : null
  const pres = presenceOf(ctx)
  const runHref = first ? `#/run/${first.p.completion_cid}` : null
  const page = pageHead({
    crumbs: first ? [
      first.id?.pipeline ? { text: first.id.pipeline, href: `#/pipeline/${encodeURIComponent(first.id.pipeline)}` } : null,
      { text: first.id?.run_name ?? 'run', href: runHref },
      firstOutput ? { text: firstOutput, href: `#/items/${first.p.completion_cid}/${encodeURIComponent(firstOutput)}?where=%5B%5D` } : null,
      { text: name ?? 'File' }].filter(Boolean) : [{ text: name ?? 'File' }],
    seal: contentCid,
    title: h('span', { class: 'mono', 'data-file-title': '' }, name ?? shortCid(contentCid)),
    after: [holder, pres.dots(contentCid, { hereHeld: missing ? false : unread ? null : true })],
    actions: [missing ? downloadElsewhere(pres, contentCid) : h('a', { class: 'btn', href: ex.blocks.urlFor(contentCid), download: name ?? '' }, 'Download')],
    subline: [h('span', {}, h('b', {}, size === null ? '—' : bytes(size)), ` · ${kind}`),
      dims ? dims : null,
      addrChip(contentCid, { ref: `cas://${contentCid}/${name ?? ''}`, seal: false })].filter(Boolean),
  })
  const produced = rows.length === 0 ? h('p', { class: 'muted' }, 'No run in this member produced it.')
    : h('div', { class: 'tbox' }, h('table', { class: 't' },
      h('thead', {}, h('tr', {}, ['Run', 'Output · item', 'Known as', 'Finished'].map(t => h('th', {}, t)))),
      h('tbody', {}, ordered.map(({ p, id }, i) => h('tr', {
        'data-producer': '', 'data-content': p.content_cid, 'data-item': p.item_cid, 'data-collection': p.collection_cid,
        'data-completion': p.completion_cid, 'data-filename': p.filename },
      h('td', {}, id ? runStatus(id, p.completion_cid)
        : h('span', { class: 'runname', title: p.completion_cid }, link(`#/run/${p.completion_cid}`, h('code', { class: 'cid' }, shortCid(p.completion_cid))))),
      h('td', {}, itemCell(ex, p, i < LABELLED)),
      h('td', { class: 'mono', style: 'font-size:12px' }, p.filename ?? ''),
      h('td', { class: 'muted' }, id?.finished_at ? whenText(id.finished_at) : ''))))))
  return h('section', {},
    page,
    preview ? panel({ title: 'Preview', meta: preview.meta, flush: true }, preview.node) : null,
    panel({ title: 'Produced by', meta: runs.size ? `${runs.size} ${runs.size === 1 ? 'run' : 'runs'} wrote these exact bytes` : null, flush: rows.length > 0 }, produced),
    // No pinnedBy or pin roots here: Gate assertion 2 holds this page to its producers and its own claims.
    keepPanel(contentCid, state, ctx, { size: size === null ? null : { bytes: size, partial: false }, kind: 'file', title: 'Keep it', href: ctx.write.hrefFor(`#/content/${contentCid}`),
      intro: runs.size > 1 ? `Releasing any one run will not remove it: the other ${runs.size - 1} still ${runs.size === 2 ? 'holds' : 'hold'} it.` : null }),
    fold(`details:${contentCid}`, 'Address',
      h('p', { class: 'muted small' }, 'Computed from the bytes. It resolves in any store that holds this file, so the link survives copies and moves.'),
      kv([['File', addrChip(contentCid)], ['Reference', h('span', { class: 'mono' }, `cas://${contentCid}/${name ?? ''}`)]])))
}
