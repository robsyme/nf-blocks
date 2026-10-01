// web/src/views/collection.js
// #/collection/<cid> and #/content/<cid> (DESIGN.md §15).
import { h, link, cid } from '../html.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { crumbs, itemRows, pager, runCrumbs, retentionPanel, table } from './common.js'

/** The collection's Output Index File line (Task 4, DESIGN.md §15): a download
 * link when it was published, or a note naming its publish path when it was not. */
function indexLine(ex, idx) {
  if (!idx) return null
  return idx.leaf.address
    ? h('p', { 'data-output-index': String(idx.leaf.address) },
        "Nextflow's index file for this output: ",
        h('a', { href: ex.blocks.urlFor(String(idx.leaf.address)), download: idx.leaf.name }, idx.leaf.name),
        ` (published at ${idx.path})`)
    : h('p', { 'data-output-index-missing': idx.path },
        `Nextflow's index file for this output (${idx.path}) was never written.`)
}

export async function collection(ex, collectionCid, offset = 0, ctx) {
  const c = await ex.collection(collectionCid, { offset })
  if (c.completion) ctx.use?.({ completion: c.completion, output: c.output, where: [], count: c.total })
  return h('section', {},
    c.completion ? runCrumbs(ex, c.completion, [{ text: c.output }]) : crumbs({ text: c.output }),
    h('h1', {}, c.output),
    fold(`storage:${collectionCid}`, 'Storage', retentionPanel(collectionCid, c.state, ctx, { kind: 'collection', href: ctx.write.hrefFor(`#/collection/${collectionCid}`) })),
    c.completion ? h('p', {}, 'Output of ', link(`#/run/${c.completion}`, 'this run')) : null,
    indexLine(ex, c.index),
    pager(`#/collection/${collectionCid}`, c, 'items'),
    c.total === 0 ? h('p', { class: 'muted' }, 'This collection has no items.')
      : itemRows(ex, ctx, { items: c.items, total: c.total, collectionCid, completion: c.completion, output: c.output, where: [],
        previews: new Previews(ex) }),
    fold(`details:${collectionCid}`, 'Details', h('p', {}, cid(collectionCid))))
}

export async function content(ex, contentCid, ctx) {
  const rows = await ex.producersOf(contentCid, ctx.progress)
  // Task 11: the content's own retain/pin state; there is no dedicated model.js loader for a
  // content page, so this reads Explorer's already-public claimStates directly.
  const state = (await ex.claimStates([contentCid])).get(contentCid)
  return h('section', {},
    crumbs({ text: 'File' }),
    h('h1', {}, 'Every run that produced this file'), cid(contentCid),
    fold(`storage:${contentCid}`, 'Storage', retentionPanel(contentCid, state, ctx, { kind: 'content', href: ctx.write.hrefFor(`#/content/${contentCid}`) })),
    rows.length === 0 ? h('p', { class: 'muted' }, 'No run in this member produced it.')
      : table(['file', 'item', 'run'], rows.map(p => h('tr', {
        'data-producer': '', 'data-content': p.content_cid, 'data-item': p.item_cid, 'data-collection': p.collection_cid,
        'data-completion': p.completion_cid, 'data-filename': p.filename },
      h('td', {}, p.filename ?? ''), h('td', {}, link(`#/item/${p.collection_cid}/${p.item_cid}`, cid(p.item_cid))),
      h('td', {}, link(`#/run/${p.completion_cid}`, cid(p.completion_cid)))))))
}
