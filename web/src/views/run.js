// web/src/views/run.js
// #/run/<completion> (explorer layout B spec §5.3): what the run made, A–Z,
// and its details, storage and lineage check folded away beneath.
import { h, link, cid } from '../html.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { fileChips, itemTable } from '../table.js'
import { anomalyLines, healthSummary, statusText, statusTone, whenText } from '../words.js'
import { crumbs, enc, retentionPanel, table } from './common.js'

const SAMPLE = 3

export async function run(ex, completionCid, ctx) {
  const { row, completion, collections, state } = await ex.run(completionCid)
  ctx.mark?.({ pipeline: row.pipeline, run: completionCid, expand: true })
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  const latestNote = h('span', { class: 'muted' })
  ex.latestSuccessfulRun?.(row.pipeline).then((best) => { if (best === completionCid) latestNote.textContent = ` · latest good run of ${row.pipeline}` }, () => {})
  const previews = new Previews(ex)
  const outputs = [...collections].sort((a, b) => (a.output < b.output ? -1 : a.output > b.output ? 1 : 0))
  const rows = outputs.map(c => ({ c, count: h('td', { class: 'muted' }, '…'), keys: h('td', { class: 'muted' }), files: h('td', {}), items: [] }))
  const redraw = () => {
    for (const r of rows) {
      const loaded = r.items.map(i => previews.get(i)).filter(p => p?.pairs)
      if (!loaded.length) continue
      const { columns } = itemTable(loaded)
      r.keys.textContent = columns.slice(0, 2).join(', ')
      const chips = fileChips(loaded[0].files).chips
      r.files.replaceChildren(...chips.slice(0, 2).map(t => h('span', { class: 'ext' }, t)),
        chips.length > 2 ? h('span', { class: 'ext' }, `+${chips.length - 2}`) : null, r.indexLink ?? null)
    }
  }
  previews.onChange(redraw)
  for (const r of rows) {
    ex.collection(r.c.cid, { limit: SAMPLE }).then((coll) => {
      r.count.textContent = String(coll.total)
      r.count.classList.remove('muted')
      r.items = coll.items
      if (coll.index?.leaf?.address) {
        r.indexLink = h('a', { href: ex.blocks.urlFor(String(coll.index.leaf.address)), download: coll.index.leaf.name, 'data-output-index': String(coll.index.leaf.address) }, ' index file')
        r.files.append(r.indexLink)
      }
      for (const i of coll.items) previews.ask(r.c.cid, i)
    }, () => { r.count.textContent = '?' })
  }
  const details = h('div', {},
    lid ? h('p', {}, 'Run reference: ', h('code', {}, lid)) : null,
    h('p', {}, 'RunCompletion: ', cid(completionCid)),
    row.manifest_cid ? h('p', {}, 'RunManifest: ', cid(row.manifest_cid)) : null)
  ex.runIdentity?.(completionCid).then((id) => {
    if (id.revision) details.prepend(h('p', {}, 'Revision: ', h('code', {}, id.revision)))
    if (id.config) details.append(fold(`config:${completionCid}`, 'Configuration', h('pre', { class: 'scroll' }, id.config)))
  }, () => {})
  const lines = anomalyLines(completion.anomalies)
  return h('section', {},
    crumbs({ text: row.pipeline, href: `#/pipeline/${enc(row.pipeline)}` }, { text: row.run_name ?? 'run' }),
    h('h1', {}, row.run_name ?? 'run'),
    h('p', {}, h('span', { class: `dot-${statusTone({ status: completion.status, possibly_incomplete: completion.possibly_incomplete })}` }, '● '),
      statusText(completion), ' · finished ', whenText(completion.finished_at), latestNote),
    completion.error ? h('p', { class: 'warn' }, completion.error) : null,
    h('h2', {}, 'Outputs'),
    collections.length === 0 ? h('p', { class: 'muted', 'data-no-outputs': '' }, 'This run published no outputs.')
      : table(['output', 'items', 'keyed by', 'files'], rows.map(r => h('tr', { 'data-collection': r.c.cid, 'data-output': r.c.output },
        h('td', {}, link(`#/items/${completionCid}/${enc(r.c.output)}`, h('strong', {}, r.c.output))), r.count, r.keys, r.files))),
    fold(`details:${completionCid}`, 'Run details', details),
    fold(`storage:${completionCid}`, 'Storage', retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href: ctx.write.hrefFor(`#/run/${completionCid}`) })),
    fold(`lineage:${completionCid}`, `Lineage check: ${healthSummary(completion.anomalies)}`,
      lines.length === 0 ? h('p', { class: 'muted' }, 'Every file is accounted for.')
        : h('ul', {}, lines.map(l => h('li', { 'data-anomaly': l.key, class: l.concern ? 'warn' : 'muted' }, l.text)))))
}
