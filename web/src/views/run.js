// web/src/views/run.js
// #/run/<completion> (DESIGN.md §15).
import { h, link, cid } from '../html.js'
import { snippetBlock, snippetToggle } from '../snippets.js'
import { enc, retentionPanel } from './common.js'

export async function run(ex, completionCid, ctx) {
  const { row, completion, collections, state } = await ex.run(completionCid)
  const a = completion.anomalies
  // Decision 13: the run's lineage ID, which fromStore(run:) and nf-blocks:items --run accept.
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  return h('section', {},
    h('h1', {}, row.run_name ?? 'run'),
    lid ? h('p', {}, h('code', { title: 'this run\'s lineage ID' }, lid)) : null,
    cid(completionCid),
    h('dl', {},
      h('dt', {}, 'pipeline'), h('dd', {}, link(`#/pipeline/${enc(row.pipeline)}`, row.pipeline)),
      h('dt', {}, 'status'), h('dd', {}, completion.possibly_incomplete ? `${completion.status}, possibly incomplete` : completion.status),
      h('dt', {}, 'finished'), h('dd', {}, completion.finished_at),
      h('dt', {}, 'anomalies'), h('dd', {}, `unresolvable ${a.unresolvable}, unaddressed ${a.unaddressed}, declined ${a.declined}, never published ${a.never_published}, unjoined ${a.unjoined ?? 0}`),
      completion.error ? [h('dt', {}, 'error'), h('dd', {}, completion.error)] : null),
    retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href: ctx.write.hrefFor(`#/run/${completionCid}`) }),
    h('h2', {}, 'Outputs'),
    // A run with no workflow outputs (a consumer that only reads, say) records no Output Collections.
    collections.length === 0 ? h('p', { class: 'muted', 'data-no-outputs': '' }, 'This run published no outputs.') : [
      lid ? h('p', {}, 'Read an output in a downstream workflow. Script kind: ', snippetToggle()) : null,
      h('ul', {}, collections.map(c => h('li', { 'data-collection': c.cid, 'data-output': c.output },
        link(`#/collection/${c.cid}`, c.output), ' ', link(`#/items/${completionCid}/${enc(c.output)}`, '(filter by metadata)'),
        lid ? snippetBlock({ kind: 'run', lid, output: c.output }) : null)))])
}
