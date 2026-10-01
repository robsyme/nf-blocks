// web/src/views/item.js
// #/item/<collection>/<item> (explorer layout B spec §5.5).
import { h, link, cid } from '../html.js'
import { fold } from '../folds.js'
import { labelPaths, labelText, pairsNode, pairsOf } from '../pairs.js'
import { itemTable, withoutDuplicates } from '../table.js'
import { humanBytes, leafReasonText } from '../words.js'
import { crumbs, pickButton, retentionPanel, runCrumbs, runLabelNode, table } from './common.js'

const SHOWN_PAIRS = 8

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  if (from?.completion) {
    ctx.use?.({ completion: from.completion, output: from.output, where: [], count: null })
    ctx.mark?.({ run: from.completion })
  }
  const files = it.leaves.map(l => l.name).filter(Boolean)
  const all = pairsOf(it.view)
  const { duplicates } = itemTable([{ pairs: all, files }])
  const pairs = withoutDuplicates(all, duplicates)
  const title = labelText({ pairs, files }, labelPaths([{ pairs }], 1)) || 'Item'
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const runs = new Map()
  for (const [, rows] of producers) for (const p of rows) runs.set(p.completion_cid, p.collection_cid)
  const elsewhere = [...runs].filter(([completion]) => completion !== from?.completion)
  return h('section', {},
    from?.completion ? runCrumbs(ex, from.completion, [{ text: from.output, href: `#/items/${from.completion}/${encodeURIComponent(from.output)}?where=%5B%5D` }, { text: title }])
      : crumbs({ text: title }),
    h('h1', {}, title),
    pairs.length ? h('div', {}, pairsNode(pairs.slice(0, SHOWN_PAIRS), target, { size: 'page' }),
      pairs.length > SHOWN_PAIRS ? fold(`meta:${itemCid}`, `+${pairs.length - SHOWN_PAIRS} more`, pairsNode(pairs.slice(SHOWN_PAIRS), target, { size: 'page' })) : null)
      : h('p', { class: 'muted' }, 'No Meta Map.'),
    h('h2', {}, 'Files'),
    table(['file', 'size', ''], it.leaves.map(l => h('tr', {},
      h('td', {}, h('code', {}, l.name ?? '')),
      h('td', {}, humanBytes(l.size)),
      h('td', {}, l.address
        ? [h('a', { href: ex.blocks.urlFor(String(l.address)), download: l.name ?? '' }, 'Download'), ' ', link(`#/content/${l.address}`, 'where else')]
        : h('span', { class: 'muted' }, leafReasonText(l.reason)))))),
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    h('h2', {}, 'Produced by'),
    from ? h('p', {}, from.completion ? link(`#/run/${from.completion}`, `${from.run_name ?? 'this run'}`) : from.run_name ?? 'this run',
      elsewhere.length ? [h('span', { class: 'muted' }, ' · same files also in: '),
        elsewhere.map(([completion, coll], i) => [i ? ', ' : null, runLabelNode(ex, coll, completion)])] : null)
      : [runs.size ? h('p', {}, [...runs].map(([completion, coll], i) => [i ? ', ' : null, runLabelNode(ex, coll, completion)]))
          : h('p', { class: 'muted' }, 'No run in this member produced it.'),
        collectionCid === '-' ? h('p', { class: 'muted' }, 'Picked by a query across runs.') : null],
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
    fold(`storage:${itemCid}`, 'Storage', retentionPanel(itemCid, it.state, ctx, { kind: 'item', href: ctx.write.hrefFor(`#/item/${collectionCid}/${itemCid}`) })),
    fold(`details:${itemCid}`, 'Details',
      h('p', {}, 'Address: ', collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
      h('ul', {}, it.leaves.filter(l => l.address).map(l => h('li', {}, l.name ?? '', ': ', cid(String(l.address))))),
      pairsNode(all, target, { size: 'row' })))
}
