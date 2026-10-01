// web/src/views/item.js
// #/item/<collection>/<item> (DESIGN.md §15).
import { h, link, cid } from '../html.js'
import { pairsNode, pairsOf } from '../pairs.js'
import { pickButton, retentionPanel, runLabelNode, runLabelText, table } from './common.js'

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  // Ticket 10 Q2: on the item page a value's pair is the query's only condition.
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const pills = pairsNode(pairsOf(it.view), target, { size: 'page' })
  return h('section', {},
    h('h1', {}, 'Item'),
    // `-` is an item reached with no collection (a Selection member picked by a query).
    h('p', {}, collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
    retentionPanel(itemCid, it.state, ctx, { kind: 'item', href: ctx.write.hrefFor(`#/item/${collectionCid}/${itemCid}`) }),
    from ? h('p', { title: collectionCid }, 'From ', from.completion ? link(`#/run/${from.completion}`, runLabelText(from, collectionCid)) : runLabelText(from, collectionCid)) : null,
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
    h('h2', {}, 'Meta Map'),
    pills ? [pills, target ? h('p', { class: 'muted' }, 'Click a value to list the items of ', h('code', {}, target.output), ' in this run that share it.') : null]
      : h('p', { class: 'muted' }, 'none'),
    h('h2', {}, 'Files'),
    table(['name', 'size', 'content', 'produced by'], it.leaves.map(l => h('tr', {},
      h('td', {}, l.name ?? ''), h('td', {}, l.size ?? ''),
      h('td', {}, l.address ? link(`#/content/${l.address}`, cid(l.address.toString())) : h('span', { class: 'muted' }, l.reason)),
      h('td', {}, (producers.find(([leaf]) => leaf === l)?.[1] ?? []).map(p => h('div', {}, runLabelNode(ex, p.collection_cid, p.completion_cid))))))))
}
